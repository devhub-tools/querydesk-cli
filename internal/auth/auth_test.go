package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"

	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

type memory struct {
	mu     sync.Mutex
	stored map[string]*Stored
}

func (m *memory) Get(server string) (*Stored, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.stored[server]; ok {
		copied := *s
		return &copied, nil
	}
	return nil, nil
}

func (m *memory) Set(server string, stored *Stored) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stored[server] = stored
	return nil
}

func (m *memory) Delete(server string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.stored, server)
	return nil
}

// oauth is QueryDesk's OAuth server: refresh tokens rotate, and a spent one is refused.
type oauth struct {
	*httptest.Server
	mu         sync.Mutex
	live       string
	issued     int
	challenge  string
	clientName string
}

func newOAuth(t *testing.T) *oauth {
	o := &oauth{live: "refresh-0"}
	o.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		defer o.mu.Unlock()
		switch r.URL.Path {
		case "/.well-known/oauth-authorization-server":
			json.NewEncoder(w).Encode(map[string]string{
				"authorization_endpoint": o.URL + "/oauth/authorize",
				"token_endpoint":         o.URL + "/oauth/token",
				"registration_endpoint":  o.URL + "/oauth/register",
			})
		case "/.well-known/oauth-protected-resource/api/v1/querydesk":
			json.NewEncoder(w).Encode(map[string]string{"resource": o.URL + "/api/v1/querydesk"})
		case "/oauth/register":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			o.clientName, _ = body["client_name"].(string)
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]string{"client_id": "cli_1"})
		case "/oauth/token":
			r.ParseForm()
			switch r.Form.Get("grant_type") {
			case "refresh_token":
				if r.Form.Get("refresh_token") != o.live {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
			case "authorization_code":
				sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
				if r.Form.Get("code") != "code-1" || base64.RawURLEncoding.EncodeToString(sum[:]) != o.challenge {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
			}
			o.issued++
			o.live = "refresh-" + string(rune('0'+o.issued))
			json.NewEncoder(w).Encode(map[string]any{"access_token": "access-" + string(rune('0'+o.issued)), "refresh_token": o.live, "expires_in": 3600})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(o.Close)
	return o
}

func newAuth(t *testing.T, o *oauth, store Store) *Auth {
	return &Auth{Server: o.URL, Store: store, LockPath: filepath.Join(t.TempDir(), "refresh.lock")}
}

func TestSignInRegistersAndKeepsTheRefreshToken(t *testing.T) {
	o := newOAuth(t)
	store := &memory{stored: map[string]*Stored{}}
	a := newAuth(t, o, store)

	// the browser: QueryDesk sends it back to the CLI's listener with a code
	browse := func(target string) error {
		u, _ := url.Parse(target)
		q := u.Query()
		o.mu.Lock()
		o.challenge = q.Get("code_challenge")
		o.mu.Unlock()
		if q.Get("scope") != "querydesk" || q.Get("resource") != o.URL+"/api/v1/querydesk" {
			t.Errorf("authorize with %v", q)
		}
		go http.Get(q.Get("redirect_uri") + "?code=code-1&state=" + q.Get("state"))
		return nil
	}
	if err := a.SignIn(context.Background(), browse); err != nil {
		t.Fatal(err)
	}
	if o.clientName != "QueryDesk CLI" {
		t.Errorf("registered as %q", o.clientName)
	}
	stored, _ := store.Get(o.URL)
	if stored == nil || stored.ClientID != "cli_1" || stored.RefreshToken != "refresh-1" {
		t.Fatalf("stored %+v", stored)
	}
	if token, _ := a.Token(context.Background()); token != "access-1" {
		t.Fatalf("token %q", token)
	}
}

func TestATokenIsRenewedOnceWithTheRotatedRefreshToken(t *testing.T) {
	o := newOAuth(t)
	store := &memory{stored: map[string]*Stored{}}
	store.Set(o.URL, &Stored{ClientID: "cli_1", RefreshToken: "refresh-0"})
	a := newAuth(t, o, store)

	token, err := a.Token(context.Background())
	if err != nil || token != "access-1" {
		t.Fatalf("%q %v", token, err)
	}
	// a token two requests both saw rejected is renewed once
	renewed, _ := a.Renew(context.Background(), token)
	again, _ := a.Renew(context.Background(), token)
	if renewed != "access-2" || again != "access-2" {
		t.Fatalf("%q %q", renewed, again)
	}
	if stored, _ := store.Get(o.URL); stored.RefreshToken != "refresh-2" {
		t.Fatalf("stored %+v", stored)
	}
}

func TestAnotherProcessesRotationIsPickedUpFromTheKeychain(t *testing.T) {
	o := newOAuth(t)
	store := &memory{stored: map[string]*Stored{}}
	store.Set(o.URL, &Stored{ClientID: "cli_1", RefreshToken: "refresh-0"})
	first, second := newAuth(t, o, store), newAuth(t, o, store)
	second.LockPath = first.LockPath

	if _, err := first.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	// second never saw refresh-1, but reads it under the lock rather than replaying refresh-0
	if token, err := second.Token(context.Background()); err != nil || token != "access-2" {
		t.Fatalf("%q %v", token, err)
	}
}

func TestARefusedRefreshSignsOut(t *testing.T) {
	o := newOAuth(t)
	store := &memory{stored: map[string]*Stored{}}
	store.Set(o.URL, &Stored{ClientID: "cli_1", RefreshToken: "revoked"})
	a := newAuth(t, o, store)

	if _, err := a.Token(context.Background()); !errors.Is(err, querydesk.ErrSignedOut) {
		t.Fatalf("got %v", err)
	}
	if stored, _ := store.Get(o.URL); stored != nil {
		t.Fatalf("still stored %+v", stored)
	}
	if _, err := newAuth(t, o, store).Token(context.Background()); !errors.Is(err, querydesk.ErrSignedOut) {
		t.Fatalf("signed out: %v", err)
	}
}
