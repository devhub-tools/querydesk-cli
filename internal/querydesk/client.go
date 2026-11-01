// Package querydesk calls QueryDesk's statements and credentials APIs, which run SQL as the
// member the token belongs to, with QueryDesk's reviews, data protection and audit log applied.
package querydesk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Tokens hands out access tokens. Renew is called with a token QueryDesk rejected and
// returns a fresh one, or an error when the member has to sign in again.
type Tokens interface {
	Token(ctx context.Context) (string, error)
	Renew(ctx context.Context, rejected string) (string, error)
}

type Client struct {
	Server string
	Tokens Tokens
	HTTP   *http.Client
	// Trace, when set, hears of each statement: how long QueryDesk took to start answering
	Trace func(sql string, status int, firstByte time.Duration)
}

// one client for every request, so each statement reuses an open HTTP/2 connection instead of
// paying for a new TCP and TLS handshake
var shared = func() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Minute
	return &http.Client{Transport: transport}
}()

type Database struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Group       string       `json:"group"`
	Adapter     string       `json:"adapter"`
	Credentials []Credential `json:"credentials"`
}

type Credential struct {
	ID              string `json:"id"`
	Username        string `json:"username"`
	Default         bool   `json:"default_credential"`
	ReviewsRequired int    `json:"reviews_required"`
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Error is QueryDesk refusing or failing a statement. QueryID is set for one waiting on review.
type Error struct {
	Status  int
	Message string
	QueryID string
}

func (e *Error) Error() string { return e.Message }

// ErrSignedOut means the member has to sign in again.
var ErrSignedOut = errors.New("signed out of QueryDesk; run `querydesk login`")

// Result is one statement's outcome: rows (Columns set), a command's count, or several
// statements' summaries. Rows stream: Next reads the next batch and returns io.EOF after the last.
type Result struct {
	Columns   []Column
	Command   string
	NumRows   int64
	Summaries []string

	decoder *json.Decoder
	body    io.Closer
	pending [][]any
	done    bool
}

func (c *Client) Databases(ctx context.Context) ([]Database, error) {
	resp, err := c.do(ctx, func(token string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Server+"/api/v1/querydesk/credentials", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("authorization", "Bearer "+token)
		req.Header.Set("accept", "application/json")
		return req, nil
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errorFrom(resp)
	}
	var body struct {
		Databases []Database `json:"databases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("couldn't read QueryDesk's databases: %w", err)
	}
	return body.Databases, nil
}

// Execute runs sql with the credential, on a connection of its own. Cancelling ctx, or
// closing the result, stops the query in QueryDesk.
func (c *Client) Execute(ctx context.Context, credentialID, sql string) (*Result, error) {
	return c.execute(ctx, credentialID, "", sql)
}

func (c *Client) execute(ctx context.Context, credentialID, sessionID, sql string) (*Result, error) {
	body := map[string]any{"credential_id": credentialID, "sql": sql, "max_rows": 0}
	if sessionID != "" {
		body["session_id"] = sessionID
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	resp, err := c.do(ctx, func(token string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Server+"/api/v1/querydesk/statements", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("authorization", "Bearer "+token)
		req.Header.Set("content-type", "application/json")
		// a statement's rows stream back as they're read; several statements come back whole
		req.Header.Set("accept", "application/x-ndjson, application/json")
		return req, nil
	})
	if err != nil {
		return nil, err
	}
	if c.Trace != nil {
		c.Trace(sql, resp.StatusCode, time.Since(started))
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, errorFrom(resp)
	}

	decoder := json.NewDecoder(bufio.NewReaderSize(resp.Body, 64<<10))
	decoder.UseNumber()
	result := &Result{decoder: decoder, body: resp.Body}

	if !strings.HasPrefix(resp.Header.Get("content-type"), "application/x-ndjson") {
		defer result.Close()
		var whole struct {
			Columns []Column `json:"columns"`
			Rows    [][]any  `json:"rows"`
			Command string   `json:"command"`
			NumRows int64    `json:"num_rows"`
			Results []string `json:"results"`
		}
		if err := decoder.Decode(&whole); err != nil {
			return nil, fmt.Errorf("couldn't read QueryDesk's result: %w", err)
		}
		result.Columns, result.Command, result.NumRows, result.Summaries = whole.Columns, whole.Command, whole.NumRows, whole.Results
		result.pending, result.done = whole.Rows, true
		return result, nil
	}

	// the first line names the columns, or is a write's count
	line, err := result.line()
	if err != nil {
		result.Close()
		return nil, err
	}
	if columns, ok := line["columns"]; ok {
		if err := json.Unmarshal(columns, &result.Columns); err != nil {
			result.Close()
			return nil, err
		}
		return result, nil
	}
	_ = json.Unmarshal(line["command"], &result.Command)
	_ = json.Unmarshal(line["num_rows"], &result.NumRows)
	// then done
	_, err = result.Next()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return result, nil
}

// Next returns the next batch of rows, or io.EOF once the result has ended.
func (r *Result) Next() ([][]any, error) {
	if r.pending != nil {
		rows := r.pending
		r.pending = nil
		return rows, nil
	}
	for !r.done {
		line, err := r.line()
		if err != nil {
			r.Close()
			return nil, err
		}
		switch {
		case line["rows"] != nil:
			var rows [][]any
			decoder := json.NewDecoder(bytes.NewReader(line["rows"]))
			decoder.UseNumber()
			if err := decoder.Decode(&rows); err != nil {
				r.Close()
				return nil, err
			}
			return rows, nil
		case line["done"] != nil:
			r.Close()
		case line["error"] != nil:
			r.Close()
			var message string
			_ = json.Unmarshal(line["error"], &message)
			return nil, &Error{Status: http.StatusUnprocessableEntity, Message: message}
		}
	}
	return nil, io.EOF
}

func (r *Result) Close() {
	r.done = true
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
}

func (r *Result) line() (map[string]json.RawMessage, error) {
	var line map[string]json.RawMessage
	if err := r.decoder.Decode(&line); err != nil {
		// a stream ends with done or error, so running out means the result was cut short
		return nil, fmt.Errorf("lost QueryDesk's result partway: %w", err)
	}
	return line, nil
}

// do sends the request, renewing a rejected token once: a request refused for its token never ran.
func (c *Client) do(ctx context.Context, build func(token string) (*http.Request, error)) (*http.Response, error) {
	token, err := c.Tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := c.send(build, token)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	resp.Body.Close()
	if token, err = c.Tokens.Renew(ctx, token); err != nil {
		return nil, err
	}
	if resp, err = c.send(build, token); err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		return nil, ErrSignedOut
	}
	return resp, nil
}

func (c *Client) send(build func(token string) (*http.Request, error), token string) (*http.Response, error) {
	req, err := build(token)
	if err != nil {
		return nil, err
	}
	resp, err := c.http().Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		return nil, fmt.Errorf("couldn't reach QueryDesk at %s: %w", req.URL.Host, err)
	}
	return resp, nil
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return shared
}

func errorFrom(resp *http.Response) error {
	var body struct {
		Error   string `json:"error"`
		QueryID string `json:"query_id"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if json.Unmarshal(raw, &body) != nil || body.Error == "" {
		body.Error = fmt.Sprintf("QueryDesk answered %d", resp.StatusCode)
	}
	return &Error{Status: resp.StatusCode, Message: body.Error, QueryID: body.QueryID}
}

// Session runs a client connection's statements on one database connection QueryDesk holds
// open, so each skips connecting to the database. It opens with its first statement, starts
// over when QueryDesk has ended it, and runs without one on a QueryDesk that has no sessions.
type Session struct {
	client       *Client
	credentialID string

	mu          sync.Mutex
	id          string
	unsupported bool
}

func (c *Client) Session(credentialID string) *Session {
	return &Session{client: c, credentialID: credentialID}
}

// Execute runs sql on the session's connection; see Client.Execute.
func (s *Session) Execute(ctx context.Context, sql string) (*Result, error) {
	id, err := s.open(ctx, false)
	if err != nil {
		return nil, err
	}
	result, err := s.client.execute(ctx, s.credentialID, id, sql)
	var qd *Error
	// ended (idle, or QueryDesk restarted): nothing ran, so a new session runs it
	if errors.As(err, &qd) && qd.Status == http.StatusGone {
		if id, err = s.open(ctx, true); err != nil {
			return nil, err
		}
		return s.client.execute(ctx, s.credentialID, id, sql)
	}
	return result, err
}

// Close ends the session, closing its database connection.
func (s *Session) Close() {
	s.mu.Lock()
	id := s.id
	s.id = ""
	s.mu.Unlock()
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := s.client.do(ctx, func(token string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.client.Server+"/api/v1/querydesk/sessions/"+id, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("authorization", "Bearer "+token)
		return req, nil
	})
	if err == nil {
		resp.Body.Close()
	}
}

func (s *Session) open(ctx context.Context, renew bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unsupported {
		return "", nil
	}
	if s.id != "" && !renew {
		return s.id, nil
	}
	payload, _ := json.Marshal(map[string]string{"credential_id": s.credentialID})
	resp, err := s.client.do(ctx, func(token string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.client.Server+"/api/v1/querydesk/sessions", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("authorization", "Bearer "+token)
		req.Header.Set("content-type", "application/json")
		req.Header.Set("accept", "application/json")
		return req, nil
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		// a QueryDesk from before sessions; a credential that's gone fails its statement instead
		s.unsupported, s.id = true, ""
		return "", nil
	}
	var opened struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&opened); err != nil {
		return "", err
	}
	s.id = opened.SessionID
	return s.id, nil
}
