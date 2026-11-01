// Package mysqlwire lets a MySQL client (mysql, Sequel Ace, TablePlus) reach QueryDesk: it
// speaks the MySQL protocol on localhost and runs each statement through QueryDesk's HTTPS
// API, as the signed-in member.
package mysqlwire

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/devhub-tools/querydesk-cli/internal/directory"
	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

// Adapters are the QueryDesk databases a MySQL client can reach.
var Adapters = []string{"mysql", "tidb"}

type Server struct {
	Client    *querydesk.Client
	Directory *directory.Directory
	Password  string
	Log       *log.Logger

	once     sync.Once
	conf     *server.Server
	sessions sync.Map // connection id → *session, for KILL QUERY
	names    sync.Map // credential id → the database's own name, which a client may USE
}

func (s *Server) Serve(listener net.Listener) error {
	s.once.Do(s.configure)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handle(conn)
	}
}

// configure sets up caching_sha2_password, which MySQL 8 and later clients sign in with by
// default. Its first sign-in needs TLS or RSA, so the server has a throwaway self-signed key:
// the connection never leaves the machine.
func (s *Server) configure() {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "querydesk-cli"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
	s.conf = server.NewServer("8.0.11-QueryDesk", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, key, tlsConfig)
}

func (s *Server) handle(netConn net.Conn) {
	// one client's trouble mustn't take the others' connections down
	defer func() {
		if r := recover(); r != nil {
			s.logf("%s: %v", netConn.RemoteAddr(), r)
		}
		netConn.Close()
	}()
	sess := &session{server: s}
	conn, err := s.conf.NewCustomizedConn(netConn, &authenticator{sess}, sess)
	if err != nil {
		return
	}
	sess.conn = conn
	s.sessions.Store(conn.ConnectionID(), sess)
	defer s.sessions.Delete(conn.ConnectionID())
	if sess.target != nil {
		s.logf("%s connected to %s as %s", netConn.RemoteAddr(), sess.target.Database.Name, sess.target.Credential.Username)
	}

	for {
		if err := conn.HandleCommand(); err != nil {
			// a client that hangs up without COM_QUIT reads as EOF
			if !errors.Is(err, io.EOF) && !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "connection closed") {
				s.logf("%s: %v", netConn.RemoteAddr(), err)
			}
			return
		}
		if conn.Closed() {
			return
		}
	}
}

func (s *Server) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log.Printf(format, args...)
	}
}

// authenticator takes any user with the local password; the user picks the credential.
type authenticator struct{ sess *session }

func (a *authenticator) GetCredential(username string) (server.Credential, bool, error) {
	return server.Credential{Passwords: []string{a.sess.server.Password}, AuthPluginName: mysql.AUTH_CACHING_SHA2_PASSWORD}, true, nil
}

// the database named at connect is looked up once the password is right, so a wrong one learns nothing
func (a *authenticator) OnAuthSuccess(conn *server.Conn) error {
	a.sess.user, a.sess.authed = conn.GetUser(), true
	if a.sess.pending != "" {
		return a.sess.use(a.sess.pending)
	}
	return nil
}

func (a *authenticator) OnAuthFailure(*server.Conn, error) {}

type session struct {
	server  *Server
	conn    *server.Conn
	user    string
	authed  bool
	pending string
	target  *directory.Target

	mu     sync.Mutex
	cancel context.CancelFunc
}

var (
	useStatement  = regexp.MustCompile("(?is)^\\s*use\\s+`?([^`;]+?)`?\\s*;?\\s*$")
	killStatement = regexp.MustCompile(`(?is)^\s*kill\s+(?:query\s+|connection\s+)?(\d+)\s*;?\s*$`)
)

func (sess *session) UseDB(name string) error {
	if !sess.authed {
		sess.pending = name
		return nil
	}
	return sess.use(name)
}

func (sess *session) use(name string) error {
	// the name the database has for itself, which a client reads with DATABASE() and may USE
	if sess.target != nil && name == sess.ownName() {
		return nil
	}
	target, err := sess.server.Directory.Find(context.Background(), name, sess.user, Adapters)
	if err != nil {
		return mysql.NewError(mysql.ER_BAD_DB_ERROR, err.Error())
	}
	sess.target = target
	return nil
}

func (sess *session) ownName() string {
	id := sess.target.Credential.ID
	if name, ok := sess.server.names.Load(id); ok {
		return name.(string)
	}
	name := ""
	if result, err := sess.server.Client.Execute(context.Background(), id, "SELECT DATABASE()"); err == nil {
		if rows, err := result.Next(); err == nil && len(rows) == 1 && len(rows[0]) == 1 {
			name, _ = rows[0][0].(string)
		}
		result.Close()
	}
	sess.server.names.Store(id, name)
	return name
}

func (sess *session) HandleQuery(query string) (*mysql.Result, error) {
	if result, err, ok := sess.answer(query); ok {
		return result, err
	}
	if sess.target == nil {
		return nil, mysql.NewError(mysql.ER_NO_DB_ERROR, "No database selected: connect with, or USE, one SHOW DATABASES lists")
	}
	return sess.run(query, false)
}

// answer handles what a session does to itself, which never reaches the database: each
// statement runs on its own in QueryDesk.
func (sess *session) answer(query string) (*mysql.Result, error, bool) {
	if m := useStatement.FindStringSubmatch(query); m != nil {
		return nil, sess.use(m[1]), true
	}
	if m := killStatement.FindStringSubmatch(query); m != nil {
		id, _ := strconv.ParseUint(m[1], 10, 32)
		if found, ok := sess.server.sessions.Load(uint32(id)); ok {
			found.(*session).interrupt()
		}
		return nil, nil, true
	}
	words := strings.Fields(strings.ToLower(strings.TrimRight(query, "; \t\n")))
	if len(words) == 0 {
		return nil, mysql.NewError(mysql.ER_EMPTY_QUERY, "Query was empty"), true
	}
	switch words[0] {
	case "begin", "start":
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "QueryDesk runs each statement on its own, so it can't hold a transaction open. Use autocommit."), true
	case "commit", "rollback":
		return nil, nil, true
	case "set":
		// a session setting can't reach the database; autocommit=0 would be a transaction
		if strings.Contains(strings.Join(words, ""), "autocommit=0") {
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "QueryDesk runs each statement on its own, so autocommit stays on."), true
		}
		return nil, nil, true
	case "show":
		if len(words) >= 2 && (words[1] == "databases" || words[1] == "schemas") {
			result, err := sess.databases()
			return result, err, true
		}
	}
	return nil, nil, false
}

// databases lists the QueryDesk databases a MySQL client can reach, so a client's picker shows them.
func (sess *session) databases() (*mysql.Result, error) {
	databases, err := sess.server.Directory.List(context.Background())
	if err != nil {
		return nil, translate(context.Background(), err)
	}
	var rows [][]any
	for _, db := range databases {
		for _, adapter := range Adapters {
			if db.Adapter == adapter {
				rows = append(rows, []any{db.Name})
			}
		}
	}
	resultset, err := mysql.BuildSimpleTextResultset([]string{"Database"}, rows)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(resultset), nil
}

func (sess *session) interrupt() {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.cancel != nil {
		sess.cancel()
	}
}

// run sends the statement to QueryDesk, streaming its rows: in the text protocol for a
// query, the binary one for a prepared statement's execution.
func (sess *session) run(query string, binary bool) (*mysql.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	sess.mu.Lock()
	sess.cancel = cancel
	sess.mu.Unlock()

	result, err := sess.server.Client.Execute(ctx, sess.target.Credential.ID, query)
	if err != nil {
		err = translate(ctx, err)
		cancel()
		return nil, err
	}
	if result.Columns == nil {
		defer cancel()
		return &mysql.Result{AffectedRows: uint64(max(result.NumRows, 0))}, nil
	}

	fields := make([]*mysql.Field, len(result.Columns))
	for i, column := range result.Columns {
		fields[i] = field(column, binary)
	}
	stream := mysql.NewStreamResult(fields, 256, binary)
	go func() {
		defer cancel()
		defer result.Close()
		defer stream.Close()
		for {
			rows, err := result.Next()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				stream.SetError(translate(ctx, err))
				return
			}
			for _, row := range rows {
				// false once the client has gone, which stops the query in QueryDesk
				if !stream.WriteRow(ctx, values(row, result.Columns, binary)) {
					return
				}
			}
		}
	}()
	return stream.AsResult(), nil
}

func (sess *session) HandleFieldList(table string, wildcard string) ([]*mysql.Field, error) {
	// the mysql client asks for each table's fields to complete names; it can do without them
	return nil, nil
}

func (sess *session) HandleStmtPrepare(query string) (int, int, any, error) {
	if sess.target == nil {
		return 0, 0, nil, mysql.NewError(mysql.ER_NO_DB_ERROR, "No database selected")
	}
	// the columns are only known by running it, which they're sent with
	return placeholders(query), 0, nil, nil
}

func (sess *session) HandleStmtExecute(_ any, query string, args []any) (*mysql.Result, error) {
	bound, err := bind(query, args)
	if err != nil {
		return nil, err
	}
	if result, err, ok := sess.answer(bound); ok {
		return result, err
	}
	return sess.run(bound, true)
}

func (sess *session) HandleStmtClose(any) error { return nil }

func (sess *session) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("command %d isn't supported through QueryDesk", cmd))
}

// QueryDesk names a column's type from its values; each takes the MySQL type it's sent as.
// The binary protocol sends all but numbers as strings.
func field(column querydesk.Column, binary bool) *mysql.Field {
	f := &mysql.Field{Name: []byte(column.Name), Charset: uint16(mysql.DEFAULT_COLLATION_ID), Type: mysql.MYSQL_TYPE_VAR_STRING}
	switch column.Type {
	case "integer", "boolean":
		f.Type, f.Charset, f.Flag = mysql.MYSQL_TYPE_LONGLONG, 63, mysql.BINARY_FLAG
	case "float":
		f.Type, f.Charset, f.Flag = mysql.MYSQL_TYPE_DOUBLE, 63, mysql.BINARY_FLAG
	}
	if binary {
		// the binary protocol has its own layout for a date and a datetime, from a time.Time
		switch column.Type {
		case "date":
			f.Type, f.Charset, f.Flag = mysql.MYSQL_TYPE_DATE, 63, mysql.BINARY_FLAG
		case "timestamp", "timestamptz":
			f.Type, f.Charset, f.Flag = mysql.MYSQL_TYPE_DATETIME, 63, mysql.BINARY_FLAG
		}
		return f
	}
	switch column.Type {
	case "decimal":
		f.Type = mysql.MYSQL_TYPE_NEWDECIMAL
	case "date":
		f.Type = mysql.MYSQL_TYPE_DATE
	case "time":
		f.Type = mysql.MYSQL_TYPE_TIME
	case "timestamp", "timestamptz":
		f.Type = mysql.MYSQL_TYPE_DATETIME
	case "json":
		f.Type = mysql.MYSQL_TYPE_JSON
	case "binary":
		f.Type, f.Charset, f.Flag = mysql.MYSQL_TYPE_BLOB, 63, mysql.BINARY_FLAG
	}
	return f
}

// values renders a row as MySQL sends it: numbers as numbers, everything else as text, and
// bytes raw.
func values(row []any, columns []querydesk.Column, binary bool) []any {
	out := make([]any, len(row))
	for i, value := range row {
		columnType := "text"
		if i < len(columns) {
			columnType = columns[i].Type
		}
		out[i] = mysqlValue(value, columnType, binary)
	}
	return out
}

func mysqlValue(value any, columnType string, binary bool) any {
	switch v := value.(type) {
	case nil:
		return nil
	case bool:
		if v {
			return int64(1)
		}
		return int64(0)
	case json.Number:
		if columnType == "integer" || columnType == "boolean" {
			if n, err := v.Int64(); err == nil {
				return n
			}
		}
		if columnType == "float" {
			if f, err := v.Float64(); err == nil {
				return f
			}
		}
		return v.String()
	case string:
		switch columnType {
		// QueryDesk sends ISO 8601; MySQL puts a space between date and time
		case "timestamp", "timestamptz":
			text := strings.TrimSuffix(strings.Replace(v, "T", " ", 1), "Z")
			if binary {
				if t, err := time.Parse("2006-01-02 15:04:05.999999", text); err == nil {
					return t
				}
			}
			return text
		case "date":
			if binary {
				if t, err := time.Parse("2006-01-02", v); err == nil {
					return t
				}
			}
		case "binary":
			if raw, err := base64.StdEncoding.DecodeString(v); err == nil {
				if binary {
					return string(raw)
				}
				return raw
			}
		}
		return v
	default:
		raw, _ := json.Marshal(v)
		return string(raw)
	}
}

func translate(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return mysql.NewError(mysql.ER_QUERY_INTERRUPTED, "Query execution was interrupted")
	}
	var qd *querydesk.Error
	switch {
	case errors.As(err, &qd) && qd.QueryID != "":
		return mysql.NewError(mysql.ER_SPECIFIC_ACCESS_DENIED_ERROR, fmt.Sprintf("%s Query %s is waiting for approval in QueryDesk.", qd.Message, qd.QueryID))
	case errors.As(err, &qd) && qd.Status == 404:
		return mysql.NewError(mysql.ER_BAD_DB_ERROR, qd.Message)
	case errors.As(err, &qd) && qd.Status == 403:
		return mysql.NewError(mysql.ER_SPECIFIC_ACCESS_DENIED_ERROR, qd.Message)
	case errors.As(err, &qd):
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, qd.Message)
	case errors.Is(err, querydesk.ErrSignedOut):
		return mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, err.Error())
	}
	var my *mysql.MyError
	if errors.As(err, &my) {
		return my
	}
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
}
