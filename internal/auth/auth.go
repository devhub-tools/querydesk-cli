// Package auth signs in to QueryDesk with OAuth in the browser (authorization code with
// PKCE, back to a one-off listener on 127.0.0.1) and hands out fresh access tokens.
//
// The client id and refresh token are kept in the OS keychain. The refresh token rotates on
// every use and QueryDesk revokes a sign-in whose old one is replayed, so renewing happens
// under a file lock and rereads the keychain first: another querydesk process may have
// rotated it already.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/zalando/go-keyring"

	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

const (
	keychainService = "QueryDesk CLI"
	clientName      = "QueryDesk CLI"
	// the scope database clients sign in with: QueryDesk shows it as a database client, not an AI app
	scope        = "querydesk"
	callbackPath = "/callback"
)

// Store keeps a server's sign-in. The keychain by default; tests swap it.
type Store interface {
	Get(server string) (*Stored, error)
	Set(server string, stored *Stored) error
	Delete(server string) error
}

type Stored struct {
	ClientID     string `json:"client_id"`
	RefreshToken string `json:"refresh_token"`
}

// Auth hands out access tokens for one server.
type Auth struct {
	Server   string
	Store    Store
	LockPath string
	HTTP     *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

var _ querydesk.Tokens = (*Auth)(nil)

// Token is a valid access token, refreshed when it's close to expiring.
func (a *Auth) Token(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Now().Before(a.expiresAt.Add(-time.Minute)) {
		return a.token, nil
	}
	return a.refresh(ctx)
}

// Renew replaces a token QueryDesk rejected, unless another caller already has.
func (a *Auth) Renew(ctx context.Context, rejected string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && a.token != rejected {
		return a.token, nil
	}
	return a.refresh(ctx)
}

func (a *Auth) refresh(ctx context.Context) (string, error) {
	lock := flock.New(a.LockPath)
	if err := lock.Lock(); err != nil {
		return "", fmt.Errorf("couldn't lock %s: %w", a.LockPath, err)
	}
	defer lock.Unlock()

	stored, err := a.Store.Get(a.Server)
	if err != nil {
		return "", err
	}
	if stored == nil {
		return "", querydesk.ErrSignedOut
	}
	endpoints, err := a.endpoints(ctx)
	if err != nil {
		return "", err
	}
	response, err := a.postForm(ctx, endpoints.Token, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {stored.RefreshToken},
		"client_id":     {stored.ClientID},
	})
	if err != nil {
		return "", err
	}
	if response == nil {
		// refused: the sign-in was revoked or expired
		_ = a.Store.Delete(a.Server)
		return "", querydesk.ErrSignedOut
	}
	return a.keep(stored.ClientID, response)
}

// SignIn runs the browser sign-in, calling open with the URL to show. It returns once the
// browser comes back or ctx ends.
func (a *Auth) SignIn(ctx context.Context, open func(url string) error) error {
	endpoints, err := a.endpoints(ctx)
	if err != nil {
		return err
	}
	// RFC 9728: the metadata of a resource with a path sits under that path
	var resource struct {
		Resource string `json:"resource"`
	}
	if err := a.getJSON(ctx, a.Server+"/.well-known/oauth-protected-resource/api/v1/querydesk", &resource); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	redirectURI := fmt.Sprintf("http://127.0.0.1:%d%s", listener.Addr().(*net.TCPAddr).Port, callbackPath)

	clientID := ""
	if stored, _ := a.Store.Get(a.Server); stored != nil {
		clientID = stored.ClientID
	}
	if clientID == "" {
		if clientID, err = a.register(ctx, endpoints.Registration); err != nil {
			return err
		}
	}

	verifier, state := randomURLSafe(32), randomURLSafe(16)
	challenge := sha256.Sum256([]byte(verifier))
	query := url.Values{
		"response_type":         {"code"},
		"client_id":             {clientID},
		"redirect_uri":          {redirectURI},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(challenge[:])},
		"code_challenge_method": {"S256"},
		"scope":                 {scope},
		"resource":              {resource.Resource},
		"state":                 {state},
	}
	if err := open(endpoints.Authorization + "?" + query.Encode()); err != nil {
		return err
	}

	callback, err := awaitCallback(ctx, listener)
	if err != nil {
		return err
	}
	if callback.Get("state") != state {
		return errors.New("the sign-in response didn't match this request")
	}
	if e := callback.Get("error"); e != "" {
		return fmt.Errorf("QueryDesk refused the sign-in: %s", e)
	}

	response, err := a.postForm(ctx, endpoints.Token, url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {callback.Get("code")},
		"redirect_uri":  {redirectURI},
		"client_id":     {clientID},
		"code_verifier": {verifier},
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("QueryDesk refused the sign-in")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err = a.keep(clientID, response)
	return err
}

func (a *Auth) SignOut() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.token = ""
	return a.Store.Delete(a.Server)
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

func (a *Auth) keep(clientID string, response *tokenResponse) (string, error) {
	if response.RefreshToken != "" {
		if err := a.Store.Set(a.Server, &Stored{ClientID: clientID, RefreshToken: response.RefreshToken}); err != nil {
			return "", fmt.Errorf("couldn't save the sign-in to the keychain: %w", err)
		}
	}
	expiresIn := response.ExpiresIn
	if expiresIn == 0 {
		expiresIn = 3600
	}
	a.token, a.expiresAt = response.AccessToken, time.Now().Add(time.Duration(expiresIn)*time.Second)
	return a.token, nil
}

type endpoints struct {
	Authorization string `json:"authorization_endpoint"`
	Token         string `json:"token_endpoint"`
	Registration  string `json:"registration_endpoint"`
}

func (a *Auth) endpoints(ctx context.Context) (*endpoints, error) {
	var metadata endpoints
	if err := a.getJSON(ctx, a.Server+"/.well-known/oauth-authorization-server", &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

func (a *Auth) register(ctx context.Context, endpoint string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"client_name": clientName,
		// QueryDesk accepts any port on a registered loopback redirect (RFC 8252)
		"redirect_uris":              []string{"http://127.0.0.1" + callbackPath},
		"grant_types":                []string{"authorization_code", "refresh_token"},
		"response_types":             []string{"code"},
		"token_endpoint_auth_method": "none",
		"scope":                      scope,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := a.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("couldn't reach QueryDesk: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("QueryDesk refused to register the CLI (%d)", resp.StatusCode)
	}
	var registered struct {
		ClientID string `json:"client_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&registered); err != nil {
		return "", err
	}
	return registered.ClientID, nil
}

func (a *Auth) getJSON(ctx context.Context, target string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := a.client().Do(req)
	if err != nil {
		return fmt.Errorf("couldn't reach QueryDesk: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %d", target, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

// postForm returns nil for a refused grant, so a revoked refresh token sends the member back to sign in.
func (a *Auth) postForm(ctx context.Context, target string, form url.Values) (*tokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("accept", "application/json")
	resp, err := a.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("couldn't reach QueryDesk: %w", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		var response tokenResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return nil, err
		}
		return &response, nil
	case resp.StatusCode < 500:
		return nil, nil
	default:
		return nil, fmt.Errorf("QueryDesk's token endpoint answered %d", resp.StatusCode)
	}
}

func (a *Auth) client() *http.Client {
	if a.HTTP != nil {
		return a.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// awaitCallback waits for the browser to come back with the code, answering with a page saying it can close.
func awaitCallback(ctx context.Context, listener net.Listener) (url.Values, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result := make(chan url.Values, 1)
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != callbackPath {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("content-type", "text/html; charset=utf-8")
			io.WriteString(w, `<!doctype html><title>QueryDesk</title><body style="font-family:system-ui;padding:3rem">`+
				`<h1>Signed in to QueryDesk</h1><p>You can close this tab and go back to your terminal.</p>`)
			select {
			case result <- r.URL.Query():
			default:
			}
		}),
	}
	go server.Serve(listener)
	defer server.Close()

	select {
	case values := <-result:
		return values, nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, errors.New("the sign-in didn't finish within 5 minutes")
		}
		return nil, ctx.Err()
	}
}

func randomURLSafe(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// Keychain stores sign-ins in the OS keychain: Keychain on macOS, Credential Manager on
// Windows and the Secret Service on Linux.
type Keychain struct{}

func (Keychain) Get(server string) (*Stored, error) {
	secret, err := keyring.Get(keychainService, server)
	if errors.Is(err, keyring.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't read the keychain: %w", err)
	}
	var stored Stored
	if err := json.Unmarshal([]byte(secret), &stored); err != nil {
		return nil, nil
	}
	return &stored, nil
}

func (Keychain) Set(server string, stored *Stored) error {
	secret, _ := json.Marshal(stored)
	return keyring.Set(keychainService, server, string(secret))
}

func (Keychain) Delete(server string) error {
	err := keyring.Delete(keychainService, server)
	if errors.Is(err, keyring.ErrNotFound) {
		return nil
	}
	return err
}
