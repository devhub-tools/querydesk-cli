package pgwire

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/devhub-tools/querydesk-cli/internal/directory"
	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

type session struct {
	server  *Server
	backend *pgproto3.Backend
	conn    net.Conn
	user    string
	target  *directory.Target
	// QueryDesk's session for this connection, holding one database connection open for it
	qd     *querydesk.Session
	pid    uint32
	secret []byte

	// SET values, answered by SHOW: each statement runs on its own in QueryDesk, so they can't reach the database
	settings   map[string]string
	statements map[string]*statement
	portals    map[string]*portal
	// after an extended-protocol error, messages are skipped until Sync
	failed bool

	mu     sync.Mutex
	cancel context.CancelFunc
}

type statement struct {
	sql  string
	oids []uint32
}

// portal is a bound statement; its result is read as far as Execute asks.
type portal struct {
	sql       string
	ran       bool
	cancel    context.CancelFunc
	local     *outcome
	result    *querydesk.Result
	buffered  [][]any
	sent      int64
	exhausted bool
}

// outcome is a statement answered here, without QueryDesk.
type outcome struct {
	tag     string
	columns []querydesk.Column
	rows    [][]any
	notice  string
}

// pgError is an error the client sees, with its SQLSTATE.
type pgError struct {
	code    string
	message string
}

func (e *pgError) Error() string { return e.message }

func (sess *session) serve() error {
	for {
		message, err := sess.backend.Receive()
		if err != nil {
			return err
		}
		if sess.failed {
			if _, ok := message.(*pgproto3.Sync); !ok {
				continue
			}
		}
		switch m := message.(type) {
		case *pgproto3.Query:
			sess.simpleQuery(m.String)
		case *pgproto3.Parse:
			sess.statements[m.Name] = &statement{sql: m.Query, oids: m.ParameterOIDs}
			sess.backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			sess.extended(sess.bind(m))
		case *pgproto3.Describe:
			sess.extended(sess.describe(m))
		case *pgproto3.Execute:
			sess.extended(sess.execute(m))
		case *pgproto3.Close:
			if m.ObjectType == 'S' {
				delete(sess.statements, m.Name)
			} else {
				sess.closePortal(m.Name)
			}
			sess.backend.Send(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			sess.failed = false
			sess.closePortal("")
			sess.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Flush:
		case *pgproto3.Terminate:
			return nil
		default:
			sess.sendError(&pgError{"0A000", fmt.Sprintf("%T isn't supported through QueryDesk", m)})
			sess.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
		if err := sess.backend.Flush(); err != nil {
			return err
		}
	}
}

func (sess *session) simpleQuery(sql string) {
	defer sess.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	statements := split(sql)
	if len(statements) == 0 {
		sess.backend.Send(&pgproto3.EmptyQueryResponse{})
		return
	}
	// a string of statements runs one at a time, stopping at the first error, as Postgres does
	for _, sql := range statements {
		p := &portal{sql: sql}
		err := sess.run(p)
		if err == nil {
			if columns := p.columns(); columns != nil {
				sess.backend.Send(rowDescription(columns))
			}
			err = sess.send(p, 0)
		}
		p.close()
		if err != nil {
			sess.sendError(err)
			return
		}
		if err := sess.backend.Flush(); err != nil {
			return
		}
	}
}

func (sess *session) extended(err error) {
	if err != nil {
		sess.sendError(err)
		sess.failed = true
	}
}

func (sess *session) bind(m *pgproto3.Bind) error {
	stmt, ok := sess.statements[m.PreparedStatement]
	if !ok {
		return &pgError{"26000", fmt.Sprintf("prepared statement %q does not exist", m.PreparedStatement)}
	}
	values := make([]*string, len(m.Parameters))
	for i, raw := range m.Parameters {
		if raw == nil {
			continue
		}
		value, err := paramText(raw, format(m.ParameterFormatCodes, i), paramOID(stmt.oids, i))
		if err != nil {
			return err
		}
		values[i] = &value
	}
	sess.closePortal(m.DestinationPortal)
	sess.portals[m.DestinationPortal] = &portal{sql: bind(stmt.sql, values)}
	sess.backend.Send(&pgproto3.BindComplete{})
	return nil
}

func (sess *session) describe(m *pgproto3.Describe) error {
	if m.ObjectType == 'S' {
		stmt, ok := sess.statements[m.Name]
		if !ok {
			return &pgError{"26000", fmt.Sprintf("prepared statement %q does not exist", m.Name)}
		}
		oids := make([]uint32, paramCount(stmt.sql))
		for i := range oids {
			// unknown parameters are asked for as text, which QueryDesk's SQL takes
			if oids[i] = paramOID(stmt.oids, i); oids[i] == 0 {
				oids[i] = textOID
			}
		}
		sess.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: oids})
		// a statement's columns are only known by running it, which waits for its portal
		sess.backend.Send(&pgproto3.NoData{})
		return nil
	}

	p, ok := sess.portals[m.Name]
	if !ok {
		return &pgError{"34000", fmt.Sprintf("portal %q does not exist", m.Name)}
	}
	if err := sess.run(p); err != nil {
		return err
	}
	if columns := p.columns(); columns != nil {
		sess.backend.Send(rowDescription(columns))
	} else {
		sess.backend.Send(&pgproto3.NoData{})
	}
	return nil
}

func (sess *session) execute(m *pgproto3.Execute) error {
	p, ok := sess.portals[m.Portal]
	if !ok {
		return &pgError{"34000", fmt.Sprintf("portal %q does not exist", m.Portal)}
	}
	if blank(p.sql) {
		sess.backend.Send(&pgproto3.EmptyQueryResponse{})
		return nil
	}
	if err := sess.run(p); err != nil {
		return err
	}
	return sess.send(p, int64(m.MaxRows))
}

func (sess *session) closePortal(name string) {
	if p, ok := sess.portals[name]; ok {
		p.close()
		delete(sess.portals, name)
	}
}

// run starts the portal's statement, once: answered here, or sent to QueryDesk.
func (sess *session) run(p *portal) error {
	if p.ran {
		return nil
	}
	p.ran = true
	local, err := sess.answer(p.sql)
	if err != nil || local != nil {
		p.local = local
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	sess.mu.Lock()
	sess.cancel = cancel
	sess.mu.Unlock()

	result, err := sess.qd.Execute(ctx, p.sql)
	if err != nil {
		err = sess.translate(ctx, err)
		cancel()
		return err
	}
	p.result, p.cancel = result, cancel
	return nil
}

// send writes the portal's rows, at most limit of them when it's above 0, then its
// completion, or PortalSuspended when rows remain.
func (sess *session) send(p *portal, limit int64) error {
	if p.local != nil {
		if p.local.notice != "" {
			sess.backend.Send(&pgproto3.NoticeResponse{Severity: "WARNING", SeverityUnlocalized: "WARNING", Code: "25P01", Message: p.local.notice})
		}
		for _, row := range p.local.rows {
			sess.backend.Send(dataRow(row, p.local.columns))
		}
		sess.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(p.local.tag)})
		return nil
	}

	result := p.result
	for _, summary := range result.Summaries {
		sess.backend.Send(&pgproto3.NoticeResponse{Severity: "NOTICE", SeverityUnlocalized: "NOTICE", Code: "00000", Message: summary})
	}
	if result.Columns == nil {
		sess.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag(result.Command, result.NumRows))})
		return nil
	}

	var sentNow int64
	for {
		if len(p.buffered) == 0 && !p.exhausted {
			rows, err := result.Next()
			if errors.Is(err, io.EOF) {
				p.exhausted = true
			} else if err != nil {
				return sess.translate(context.Background(), err)
			} else {
				p.buffered = rows
			}
		}
		if len(p.buffered) == 0 && p.exhausted {
			break
		}
		if limit > 0 && sentNow == limit {
			sess.backend.Send(&pgproto3.PortalSuspended{})
			return nil
		}
		if len(p.buffered) > 0 {
			sess.backend.Send(dataRow(p.buffered[0], result.Columns))
			p.buffered = p.buffered[1:]
			p.sent++
			sentNow++
			// a large result goes out as it comes in
			if p.sent%1000 == 0 {
				if err := sess.backend.Flush(); err != nil {
					return err
				}
			}
		}
	}
	sess.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", p.sent))})
	return nil
}

func (p *portal) columns() []querydesk.Column {
	switch {
	case p.local != nil:
		return p.local.columns
	case p.result != nil:
		return p.result.Columns
	}
	return nil
}

func (p *portal) close() {
	if p.result != nil {
		p.result.Close()
	}
	if p.cancel != nil {
		p.cancel()
	}
}

func (sess *session) cancelIf(secret []byte) {
	if subtle.ConstantTimeCompare(secret, sess.secret) != 1 {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.cancel != nil {
		sess.cancel()
	}
}

func (sess *session) translate(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return &pgError{"57014", "canceling statement due to user request"}
	}
	var qd *querydesk.Error
	switch {
	case errors.As(err, &qd) && qd.QueryID != "":
		return &pgError{"42501", fmt.Sprintf("%s Query %s is waiting for approval in QueryDesk.", qd.Message, qd.QueryID)}
	case errors.As(err, &qd) && qd.Status == 404:
		return &pgError{"3D000", qd.Message}
	case errors.As(err, &qd) && qd.Status == 403:
		return &pgError{"42501", qd.Message}
	case errors.As(err, &qd):
		return &pgError{"42000", qd.Message}
	case errors.Is(err, querydesk.ErrSignedOut):
		return &pgError{"28000", err.Error()}
	}
	var pe *pgError
	if errors.As(err, &pe) {
		return pe
	}
	return &pgError{"08006", err.Error()}
}

func (sess *session) sendError(err error) {
	pe := sess.translate(context.Background(), err).(*pgError)
	sess.backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: pe.code, Message: pe.message})
}

// answer handles what a session does to itself, which never reaches the database: each
// statement runs on its own in QueryDesk. Nil for a statement for QueryDesk.
func (sess *session) answer(sql string) (*outcome, error) {
	words := strings.Fields(strings.ToLower(strings.TrimRight(stripComments(sql), "; \t\n")))
	if len(words) == 0 {
		return nil, nil
	}
	switch words[0] {
	case "begin", "start":
		return nil, &pgError{"0A000", "QueryDesk runs each statement on its own, so it can't hold a transaction open. Use autocommit."}
	case "commit", "end":
		return &outcome{tag: "COMMIT", notice: "there is no transaction in progress"}, nil
	case "rollback", "abort":
		return &outcome{tag: "ROLLBACK", notice: "there is no transaction in progress"}, nil
	case "set":
		if name, value, ok := setting(sql); ok {
			sess.settings[name] = value
		}
		return &outcome{tag: "SET"}, nil
	case "reset":
		if len(words) > 1 {
			delete(sess.settings, words[1])
		}
		return &outcome{tag: "RESET"}, nil
	case "discard":
		sess.settings = map[string]string{}
		return &outcome{tag: "DISCARD ALL"}, nil
	case "show":
		if len(words) == 2 {
			if value, ok := sess.settings[words[1]]; ok {
				return &outcome{tag: "SHOW", columns: []querydesk.Column{{Name: words[1], Type: "text"}}, rows: [][]any{{value}}}, nil
			}
		}
	case "listen", "unlisten", "notify":
		return nil, &pgError{"0A000", strings.ToUpper(words[0]) + " isn't supported through QueryDesk"}
	case "copy":
		if strings.Contains(strings.Join(words, " "), "stdin") || strings.Contains(strings.Join(words, " "), "stdout") {
			return nil, &pgError{"0A000", "COPY to or from the client isn't supported through QueryDesk"}
		}
	}
	return nil, nil
}

// setting is SET [SESSION | LOCAL] name {TO | =} value's name and value.
func setting(sql string) (string, string, bool) {
	words := strings.Fields(strings.TrimRight(strings.TrimSpace(sql), ";"))
	words = words[1:]
	if len(words) > 0 && (strings.EqualFold(words[0], "session") || strings.EqualFold(words[0], "local")) {
		words = words[1:]
	}
	if len(words) < 2 {
		return "", "", false
	}
	name := strings.ToLower(words[0])
	rest := words[1:]
	if strings.EqualFold(rest[0], "to") || rest[0] == "=" {
		rest = rest[1:]
	} else if strings.Contains(name, "=") {
		parts := strings.SplitN(name, "=", 2)
		name, rest = parts[0], append([]string{parts[1]}, rest...)
	}
	return name, strings.Trim(strings.Join(rest, " "), "'"), true
}

// every column goes out in text format, which is what libpq clients ask for
func rowDescription(columns []querydesk.Column) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(columns))
	for i, column := range columns {
		fields[i] = pgproto3.FieldDescription{Name: []byte(column.Name), DataTypeOID: oid(column), DataTypeSize: -1, TypeModifier: -1}
	}
	return &pgproto3.RowDescription{Fields: fields}
}

func dataRow(row []any, columns []querydesk.Column) *pgproto3.DataRow {
	values := make([][]byte, len(row))
	for i, value := range row {
		column := querydesk.Column{Type: "text"}
		if i < len(columns) {
			column = columns[i]
		}
		values[i] = text(value, column)
	}
	return &pgproto3.DataRow{Values: values}
}

func format(codes []int16, i int) int16 {
	switch {
	case len(codes) == 0:
		return 0
	case len(codes) == 1:
		return codes[0]
	case i < len(codes):
		return codes[i]
	}
	return 0
}

func paramOID(oids []uint32, i int) uint32 {
	if i < len(oids) {
		return oids[i]
	}
	return 0
}

// paramText is a parameter as text; a binary one is decoded by its type.
func paramText(raw []byte, format int16, oid uint32) (string, error) {
	if format == 0 {
		return string(raw), nil
	}
	switch {
	case oid == 16 && len(raw) == 1:
		return strconv.FormatBool(raw[0] != 0), nil
	case oid == 21 && len(raw) == 2:
		return strconv.Itoa(int(int16(binary.BigEndian.Uint16(raw)))), nil
	case oid == 23 && len(raw) == 4:
		return strconv.Itoa(int(int32(binary.BigEndian.Uint32(raw)))), nil
	case oid == 20 && len(raw) == 8:
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10), nil
	case oid == 700 && len(raw) == 4:
		return strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(raw))), 'g', -1, 32), nil
	case oid == 701 && len(raw) == 8:
		return strconv.FormatFloat(math.Float64frombits(binary.BigEndian.Uint64(raw)), 'g', -1, 64), nil
	case oid == 2950 && len(raw) == 16:
		return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
	case oid == 25 || oid == 1043 || oid == 19 || oid == 0:
		return string(raw), nil
	}
	return "", &pgError{"0A000", fmt.Sprintf("a binary parameter of type %d isn't supported; send it as text", oid)}
}
