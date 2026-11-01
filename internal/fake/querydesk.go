// Package fake is a QueryDesk for tests: its statements API answers from a table of SQL.
package fake

import (
	"context"
	"encoding/json"
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

	mu        sync.Mutex
	Ran       []string
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
	case "/api/v1/querydesk/statements":
		var body struct {
			SQL string `json:"sql"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		qd.mu.Lock()
		qd.Ran = append(qd.Ran, body.SQL)
		qd.mu.Unlock()

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
		http.NotFound(w, r)
	}
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
