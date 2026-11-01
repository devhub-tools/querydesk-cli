// Package fake is a QueryDesk for tests: its statements API answers from a table of SQL.
package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

// Answer is what the fake sends for a statement: NDJSON lines, or a status and JSON body.
type Answer struct {
	Lines  []any
	Status int
	Body   any
	// Block holds the response until the client goes away, as a long query does
	Block bool
}

type QueryDesk struct {
	*httptest.Server
	Token     string
	Databases []querydesk.Database
	Answers   map[string]Answer

	// Sessions turns on the sessions API; End makes the next statement find its session gone
	Sessions bool
	End      bool

	mu        sync.Mutex
	Ran       []string
	RanOn     []string
	Opened    []string
	Closed    []string
	Cancelled chan string
}

func New(token string, databases []querydesk.Database, answers map[string]Answer) *QueryDesk {
	qd := &QueryDesk{Token: token, Databases: databases, Answers: answers, Cancelled: make(chan string, 10)}
	qd.Server = httptest.NewServer(http.HandlerFunc(qd.serve))
	return qd
}

func (qd *QueryDesk) Statements() []string {
	qd.mu.Lock()
	defer qd.mu.Unlock()
	return append([]string(nil), qd.Ran...)
}

func (qd *QueryDesk) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("authorization") != "Bearer "+qd.Token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/api/v1/querydesk/credentials":
		json.NewEncoder(w).Encode(map[string]any{"databases": qd.Databases})
	case "/api/v1/querydesk/sessions":
		if !qd.Sessions {
			http.NotFound(w, r)
			return
		}
		qd.mu.Lock()
		id := fmt.Sprintf("ses_%d", len(qd.Opened)+1)
		qd.Opened = append(qd.Opened, id)
		qd.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"session_id": id})
	case "/api/v1/querydesk/statements":
		var body struct {
			SQL       string `json:"sql"`
			SessionID string `json:"session_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		qd.mu.Lock()
		end := qd.End && body.SessionID != ""
		qd.End = qd.End && !end
		if !end {
			qd.Ran = append(qd.Ran, body.SQL)
			qd.RanOn = append(qd.RanOn, body.SessionID)
		}
		qd.mu.Unlock()
		if end {
			w.WriteHeader(http.StatusGone)
			json.NewEncoder(w).Encode(map[string]any{"error": "This session has ended. Start a new one."})
			return
		}

		answer, ok := qd.Answers[strings.TrimSpace(body.SQL)]
		if !ok {
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]any{"error": "no answer for " + body.SQL})
			return
		}
		if answer.Status != 0 {
			w.WriteHeader(answer.Status)
			json.NewEncoder(w).Encode(answer.Body)
			return
		}
		w.Header().Set("content-type", "application/x-ndjson")
		for _, line := range answer.Lines {
			json.NewEncoder(w).Encode(line)
			w.(http.Flusher).Flush()
		}
		if answer.Block {
			<-r.Context().Done()
			qd.Cancelled <- body.SQL
		}
	default:
		if strings.HasPrefix(r.URL.Path, "/api/v1/querydesk/sessions/") && r.Method == http.MethodDelete {
			qd.mu.Lock()
			qd.Closed = append(qd.Closed, strings.TrimPrefix(r.URL.Path, "/api/v1/querydesk/sessions/"))
			qd.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	}
}

// Seen is what the fake recorded, read safely while a client may still be talking to it.
func (qd *QueryDesk) Seen() (ranOn, opened, closed []string) {
	qd.mu.Lock()
	defer qd.mu.Unlock()
	return append([]string(nil), qd.RanOn...), append([]string(nil), qd.Opened...), append([]string(nil), qd.Closed...)
}

// Rows is a streamed result: its columns, its rows in one chunk, then done.
func Rows(columns []querydesk.Column, rows ...[]any) Answer {
	lines := []any{map[string]any{"columns": columns}}
	if len(rows) > 0 {
		lines = append(lines, map[string]any{"rows": rows})
	}
	return Answer{Lines: append(lines, map[string]any{"done": true})}
}

// StaticTokens hands out one token.
type StaticTokens string

func (t StaticTokens) Token(context.Context) (string, error) { return string(t), nil }

func (t StaticTokens) Renew(context.Context, string) (string, error) {
	return "", querydesk.ErrSignedOut
}

// Col is a result's column.
func Col(name, typ string) querydesk.Column { return querydesk.Column{Name: name, Type: typ} }
