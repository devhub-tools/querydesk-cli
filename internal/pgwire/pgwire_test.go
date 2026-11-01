package pgwire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/devhub-tools/querydesk-cli/internal/directory"
	"github.com/devhub-tools/querydesk-cli/internal/fake"
	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

var databases = []querydesk.Database{{
	ID: "db_1", Name: "payments-prod", Adapter: "postgres",
	Credentials: []querydesk.Credential{{ID: "crd_ro", Username: "reader", Default: true}, {ID: "crd_rw", Username: "writer"}},
}, {ID: "db_2", Name: "shop", Adapter: "mongodb", Credentials: []querydesk.Credential{{ID: "crd_m", Username: "app", Default: true}}}}

var version = fake.Rows([]querydesk.Column{{Name: "server_version", Type: "text"}}, []any{"16.4"})

func start(t *testing.T, answers map[string]fake.Answer) (*fake.QueryDesk, string) {
	t.Helper()
	answers["SHOW server_version"] = version
	qd := fake.New("token", databases, answers)
	t.Cleanup(qd.Close)

	client := &querydesk.Client{Server: qd.URL, Tokens: fake.StaticTokens("token")}
	server := &Server{Client: client, Directory: &directory.Directory{Client: client}, Password: "local"}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go server.Serve(listener)
	return qd, fmt.Sprintf("postgres://reader:local@%s/payments-prod?sslmode=disable", listener.Addr())
}

func connect(t *testing.T, url string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = mode
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func sqlstate(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestValuesComeBackTyped(t *testing.T) {
	columns := []querydesk.Column{fake.Col("id", "integer"), fake.Col("name", "text"), fake.Col("placed_at", "timestamptz"), fake.Col("paid", "boolean"),
		fake.Col("tags", "array"), fake.Col("notes", "json"), fake.Col("receipt", "binary"), fake.Col("total", "decimal"), fake.Col("missing", "null")}
	row := []any{7, "Ada", "2026-10-01T09:30:00Z", true, []any{"a b", "c"}, map[string]any{"gift": true}, "3q2+7w==", "42.50", nil}
	_, url := start(t, map[string]fake.Answer{"SELECT * FROM orders": fake.Rows(columns, row)})

	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
		conn := connect(t, url, mode)
		var (
			id          int64
			name, total string
			placedAt    time.Time
			paid        bool
			tags        []string
			notes       map[string]any
			receipt     []byte
			missing     *string
		)
		err := conn.QueryRow(context.Background(), "SELECT * FROM orders").
			Scan(&id, &name, &placedAt, &paid, &tags, &notes, &receipt, &total, &missing)
		if err != nil {
			t.Fatalf("mode %v: %v", mode, err)
		}
		if id != 7 || name != "Ada" || !placedAt.Equal(time.Date(2026, 10, 1, 9, 30, 0, 0, time.UTC)) || !paid ||
			fmt.Sprint(tags) != "[a b c]" || notes["gift"] != true || fmt.Sprintf("%x", receipt) != "deadbeef" || total != "42.50" || missing != nil {
			t.Fatalf("mode %v: got %v %v %v %v %v %v %x %v %v", mode, id, name, placedAt, paid, tags, notes, receipt, total, missing)
		}
	}
}

func TestStartupReportsTheDatabasesVersion(t *testing.T) {
	_, url := start(t, map[string]fake.Answer{})
	conn := connect(t, url, pgx.QueryExecModeSimpleProtocol)
	if got := conn.PgConn().ParameterStatus("server_version"); got != "16.4" {
		t.Fatalf("server_version %q", got)
	}
}

func TestParametersAreBoundAsLiterals(t *testing.T) {
	qd, url := start(t, map[string]fake.Answer{
		"SELECT name FROM customers WHERE id = '7' AND note = 'it''s $1'": fake.Rows([]querydesk.Column{fake.Col("name", "text")}, []any{"Ada"}),
	})
	conn := connect(t, url, pgx.QueryExecModeExec)
	var name string
	if err := conn.QueryRow(context.Background(), "SELECT name FROM customers WHERE id = $1 AND note = $2", 7, "it's $1").Scan(&name); err != nil {
		t.Fatal(err, qd.Statements())
	}
	if name != "Ada" {
		t.Fatalf("got %q", name)
	}
}

func TestAStringOfStatementsRunsOneAtATime(t *testing.T) {
	qd, url := start(t, map[string]fake.Answer{
		"SELECT 1 AS one":                 fake.Rows([]querydesk.Column{fake.Col("one", "integer")}, []any{1}),
		"SELECT ';' AS semi":              fake.Rows([]querydesk.Column{fake.Col("semi", "text")}, []any{";"}),
		"-- trailing\nUPDATE t SET a = a": {Lines: []any{map[string]any{"command": "update", "num_rows": 3}, map[string]any{"done": true}}},
	})
	conn := connect(t, url, pgx.QueryExecModeSimpleProtocol)
	results, err := conn.PgConn().Exec(context.Background(), "SELECT 1 AS one; SELECT ';' AS semi; -- trailing\nUPDATE t SET a = a").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 || string(results[1].Rows[0][0]) != ";" || results[2].CommandTag.String() != "UPDATE 3" {
		t.Fatalf("got %+v", results)
	}
	if got := qd.Statements(); len(got) != 4 {
		t.Fatalf("ran %v", got)
	}
}

func TestSessionStatementsAreAnsweredHere(t *testing.T) {
	qd, url := start(t, map[string]fake.Answer{})
	conn := connect(t, url, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, "BEGIN"); sqlstate(err) != "0A000" {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := conn.Exec(ctx, "SET search_path TO reporting"); err != nil {
		t.Fatal(err)
	}
	var path string
	if err := conn.QueryRow(ctx, "SHOW search_path").Scan(&path); err != nil || path != "reporting" {
		t.Fatalf("SHOW: %q %v", path, err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	// only the startup's version query reached QueryDesk
	if got := qd.Statements(); len(got) != 1 {
		t.Fatalf("ran %v", got)
	}
}

func TestQueryDesksRefusalsKeepTheirMessage(t *testing.T) {
	_, url := start(t, map[string]fake.Answer{
		"DELETE FROM customers": {Status: 409, Body: map[string]any{"error": "This query needs review before it runs.", "query_id": "qry_1"}},
		"SELECT nope":           {Status: 422, Body: map[string]any{"error": `column "nope" does not exist`}},
	})
	conn := connect(t, url, pgx.QueryExecModeSimpleProtocol)

	_, err := conn.Exec(context.Background(), "DELETE FROM customers")
	if sqlstate(err) != "42501" || !errors.Is(err, err) || err.Error() != "ERROR: This query needs review before it runs. Query qry_1 is waiting for approval in QueryDesk. (SQLSTATE 42501)" {
		t.Fatalf("review: %v", err)
	}
	if _, err := conn.Exec(context.Background(), "SELECT nope"); sqlstate(err) != "42000" {
		t.Fatalf("error: %v", err)
	}
	// the session carries on
	if _, err := conn.Exec(context.Background(), "COMMIT"); err != nil {
		t.Fatal(err)
	}
}

func TestConnectingTakesTheLocalPasswordAndAKnownDatabase(t *testing.T) {
	_, url := start(t, map[string]fake.Answer{})
	cfg, _ := pgx.ParseConfig(url)

	wrong := cfg.Copy()
	wrong.Password = "nope"
	if _, err := pgx.ConnectConfig(context.Background(), wrong); sqlstate(err) != "28P01" {
		t.Fatalf("wrong password: %v", err)
	}

	unknown := cfg.Copy()
	unknown.Database = "shop"
	_, err := pgx.ConnectConfig(context.Background(), unknown)
	if sqlstate(err) != "3D000" {
		t.Fatalf("unknown database: %v", err)
	}
}

func TestTheUserPicksTheCredential(t *testing.T) {
	qd, url := start(t, map[string]fake.Answer{})
	cfg, _ := pgx.ParseConfig(url)
	cfg.User = "writer"
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if got := conn.PgConn().ParameterStatus("session_authorization"); got != "writer" {
		t.Fatalf("got %q (%v)", got, qd.Statements())
	}
}

func TestCancellingStopsTheQueryInQueryDesk(t *testing.T) {
	qd, url := start(t, map[string]fake.Answer{
		"SELECT pg_sleep(60)": {Lines: []any{map[string]any{"columns": []querydesk.Column{fake.Col("pg_sleep", "null")}}}, Block: true},
	})
	conn := connect(t, url, pgx.QueryExecModeSimpleProtocol)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := conn.Exec(ctx, "SELECT pg_sleep(60)")
	if err == nil {
		t.Fatal("no error")
	}
	select {
	case sql := <-qd.Cancelled:
		if sql != "SELECT pg_sleep(60)" {
			t.Fatalf("cancelled %q", sql)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("QueryDesk's request wasn't cancelled")
	}
}
