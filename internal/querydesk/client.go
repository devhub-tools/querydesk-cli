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
}

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

// Execute runs sql with the credential. Cancelling ctx, or closing the result, stops the
// query in QueryDesk.
func (c *Client) Execute(ctx context.Context, credentialID, sql string) (*Result, error) {
	payload, err := json.Marshal(map[string]any{"credential_id": credentialID, "sql": sql, "max_rows": 0})
	if err != nil {
		return nil, err
	}
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
	return &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, ResponseHeaderTimeout: 10 * time.Minute}}
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
