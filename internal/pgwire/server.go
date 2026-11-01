// Package pgwire lets a Postgres client (psql, Postico, TablePlus) reach QueryDesk: it
// speaks the Postgres wire protocol on localhost and runs each statement through
// QueryDesk's HTTPS API, as the signed-in member.
package pgwire

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/devhub-tools/querydesk-cli/internal/directory"
	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

// Adapters are the QueryDesk databases a Postgres client can reach.
var Adapters = []string{"postgres", "cockroachdb"}

type Server struct {
	Client    *querydesk.Client
	Directory *directory.Directory
	Password  string
	Log       *log.Logger

	sessions sync.Map // process id → *session, for a cancel request
	versions sync.Map // credential id → server_version
}

func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	// one client's trouble mustn't take the others' connections down
	defer func() {
		if r := recover(); r != nil {
			s.logf("%s: %v", conn.RemoteAddr(), r)
		}
		conn.Close()
	}()
	backend := pgproto3.NewBackend(conn, conn)

	startup, err := s.startup(conn, backend)
	if err != nil || startup == nil {
		return
	}

	sess := &session{server: s, backend: backend, conn: conn, user: startup.Parameters["user"],
		settings: map[string]string{}, statements: map[string]*statement{}, portals: map[string]*portal{}}

	if err := sess.authenticate(); err != nil {
		return
	}
	target, err := s.Directory.Find(context.Background(), startup.Parameters["database"], sess.user, Adapters)
	if err != nil {
		sess.fatal("3D000", err.Error())
		return
	}
	sess.target = target

	sess.pid, sess.secret = s.register(sess)
	defer s.sessions.Delete(sess.pid)

	backend.Send(&pgproto3.AuthenticationOk{})
	for name, value := range s.parameters(target, startup.Parameters) {
		backend.Send(&pgproto3.ParameterStatus{Name: name, Value: value})
	}
	backend.Send(&pgproto3.BackendKeyData{ProcessID: sess.pid, SecretKey: sess.secret})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	s.logf("%s connected to %s as %s", conn.RemoteAddr(), target.Database.Name, target.Credential.Username)

	if err := sess.serve(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		s.logf("%s: %v", conn.RemoteAddr(), err)
	}
}

// startup answers SSL and GSS requests with no (the connection never leaves the machine)
// and a cancel request by cancelling, returning the startup message of a connection to serve.
func (s *Server) startup(conn net.Conn, backend *pgproto3.Backend) (*pgproto3.StartupMessage, error) {
	for {
		message, err := backend.ReceiveStartupMessage()
		if err != nil {
			return nil, err
		}
		switch m := message.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return nil, err
			}
		case *pgproto3.CancelRequest:
			if found, ok := s.sessions.Load(m.ProcessID); ok {
				found.(*session).cancelIf(m.SecretKey)
			}
			return nil, nil
		case *pgproto3.StartupMessage:
			return m, nil
		default:
			return nil, fmt.Errorf("unexpected startup message %T", m)
		}
	}
}

func (s *Server) register(sess *session) (uint32, []byte) {
	for {
		var b [8]byte
		_, _ = rand.Read(b[:])
		pid := binary.BigEndian.Uint32(b[:4]) &^ (1 << 31)
		if _, taken := s.sessions.LoadOrStore(pid, sess); !taken {
			return pid, b[4:]
		}
	}
}

// parameters are what a session reports at startup, its server version the database's own,
// which a client picks its catalog queries by.
func (s *Server) parameters(target *directory.Target, startup map[string]string) map[string]string {
	parameters := map[string]string{
		"server_version":              s.version(target),
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"IntervalStyle":               "postgres",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
		"is_superuser":                "off",
		"session_authorization":       target.Credential.Username,
	}
	if name := startup["application_name"]; name != "" {
		parameters["application_name"] = name
	}
	return parameters
}

func (s *Server) version(target *directory.Target) string {
	if version, ok := s.versions.Load(target.Credential.ID); ok {
		return version.(string)
	}
	version := "16.0"
	if result, err := s.Client.Execute(context.Background(), target.Credential.ID, "SHOW server_version"); err == nil {
		if rows, err := result.Next(); err == nil && len(rows) == 1 && len(rows[0]) == 1 {
			if v, ok := rows[0][0].(string); ok {
				version = v
			}
		}
		result.Close()
	}
	s.versions.Store(target.Credential.ID, version)
	return version
}

func (s *Server) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log.Printf(format, args...)
	}
}

func (sess *session) authenticate() error {
	sess.backend.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := sess.backend.Flush(); err != nil {
		return err
	}
	if err := sess.backend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return err
	}
	message, err := sess.backend.Receive()
	if err != nil {
		return err
	}
	password, ok := message.(*pgproto3.PasswordMessage)
	if !ok || subtle.ConstantTimeCompare([]byte(password.Password), []byte(sess.server.Password)) != 1 {
		sess.fatal("28P01", "password authentication failed: use the password `querydesk databases` shows")
		return errors.New("wrong password")
	}
	return nil
}

func (sess *session) fatal(code, message string) {
	sess.backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: message})
	_ = sess.backend.Flush()
}
