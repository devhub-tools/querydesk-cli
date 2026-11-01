package mysqlwire

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/devhub-tools/querydesk-cli/internal/directory"
	"github.com/devhub-tools/querydesk-cli/internal/fake"
	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

var databases = []querydesk.Database{{
	ID: "db_1", Name: "orders", Adapter: "mysql",
	Credentials: []querydesk.Credential{{ID: "crd_1", Username: "app", Default: true}},
}, {ID: "db_2", Name: "payments", Adapter: "postgres", Credentials: []querydesk.Credential{{ID: "crd_2", Username: "app", Default: true}}}}

var ownName = fake.Rows([]querydesk.Column{{Name: "DATABASE()", Type: "text"}}, []any{"orders_db"})

func start(t *testing.T, answers map[string]fake.Answer) (*fake.QueryDesk, string) {
	qd, addr, _ := startServer(t, answers)
	return qd, addr
}

func startServer(t *testing.T, answers map[string]fake.Answer) (*fake.QueryDesk, string, *Server) {
	t.Helper()
	answers["SELECT DATABASE()"] = ownName
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
	return qd, listener.Addr().String(), server
}

func open(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func number(err error) uint16 {
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		return my.Number
	}
	return 0
}

func TestValuesComeBackTyped(t *testing.T) {
	columns := []querydesk.Column{fake.Col("id", "integer"), fake.Col("customer", "text"), fake.Col("placed_at", "timestamp"), fake.Col("paid", "integer"),
		fake.Col("notes", "json"), fake.Col("receipt", "binary"), fake.Col("total", "decimal"), fake.Col("missing", "null")}
	row := []any{7, "Ada", "2026-10-01T09:30:00", 1, map[string]any{"gift": true}, "3q2+7w==", "42.50", nil}
	_, addr := start(t, map[string]fake.Answer{
		"SELECT * FROM orders":             fake.Rows(columns, row),
		"SELECT * FROM orders WHERE 1 = 1": fake.Rows(columns, row),
	})

	// the text protocol, then a prepared statement's binary one
	for _, interpolate := range []bool{true, false} {
		db := open(t, fmt.Sprintf("app:local@tcp(%s)/orders?parseTime=true&interpolateParams=%t", addr, interpolate))
		var (
			id, paid        int64
			customer, total string
			placedAt        time.Time
			notes, receipt  []byte
			missing         sql.NullString
		)
		args := []any{}
		query := "SELECT * FROM orders"
		if !interpolate {
			// a placeholder makes the driver prepare it
			query, args = "SELECT * FROM orders WHERE ? = 1", []any{1}
		}
		err := db.QueryRow(query, args...).Scan(&id, &customer, &placedAt, &paid, &notes, &receipt, &total, &missing)
		if err != nil {
			t.Fatal(err)
		}
		if id != 7 || customer != "Ada" || !placedAt.Equal(time.Date(2026, 10, 1, 9, 30, 0, 0, time.UTC)) || paid != 1 ||
			string(notes) != `{"gift":true}` || fmt.Sprintf("%x", receipt) != "deadbeef" || total != "42.50" || missing.Valid {
			t.Fatalf("got %v %v %v %v %s %x %v %v", id, customer, placedAt, paid, notes, receipt, total, missing)
		}
	}
}

func TestAPreparedStatementRunsWithItsArgumentsBound(t *testing.T) {
	qd, addr := start(t, map[string]fake.Answer{
		`SELECT name FROM customers WHERE id = 7 AND note = 'it\'s ?'`: fake.Rows([]querydesk.Column{fake.Col("name", "text")}, []any{"Ada"}),
	})
	db := open(t, fmt.Sprintf("app:local@tcp(%s)/orders", addr))
	var name string
	if err := db.QueryRow("SELECT name FROM customers WHERE id = ? AND note = ?", 7, "it's ?").Scan(&name); err != nil {
		t.Fatal(err, qd.Statements())
	}
	if name != "Ada" {
		t.Fatalf("got %q", name)
	}
}

func TestAWriteReturnsItsCount(t *testing.T) {
	_, addr := start(t, map[string]fake.Answer{
		"UPDATE orders SET paid = 1": {Lines: []any{map[string]any{"command": nil, "num_rows": 3}, map[string]any{"done": true}}},
	})
	db := open(t, fmt.Sprintf("app:local@tcp(%s)/orders", addr))
	result, err := db.Exec("UPDATE orders SET paid = 1")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := result.RowsAffected(); n != 3 {
		t.Fatalf("affected %d", n)
	}
}

func TestSessionStatementsAreAnsweredHere(t *testing.T) {
	qd, addr := start(t, map[string]fake.Answer{})
	db := open(t, fmt.Sprintf("app:local@tcp(%s)/", addr))
	db.SetMaxOpenConns(1)

	var name string
	if err := db.QueryRow("SHOW DATABASES").Scan(&name); err != nil || name != "orders" {
		t.Fatalf("SHOW DATABASES: %q %v", name, err)
	}
	if _, err := db.Exec("SELECT 1"); number(err) != 1046 {
		t.Fatalf("no database: %v", err)
	}
	if _, err := db.Exec("USE `orders`"); err != nil {
		t.Fatal(err)
	}
	// the database's own name, as DATABASE() gives it, is the same database
	if _, err := db.Exec("USE orders_db"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET NAMES utf8mb4"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("BEGIN"); number(err) != 1235 {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := db.Exec("SET autocommit = 0"); number(err) != 1235 {
		t.Fatalf("autocommit: %v", err)
	}
	if got := qd.Statements(); len(got) != 1 {
		t.Fatalf("ran %v", got)
	}
}

func TestQueryDesksRefusalsKeepTheirMessage(t *testing.T) {
	_, addr := start(t, map[string]fake.Answer{
		"DELETE FROM orders": {Status: 409, Body: map[string]any{"error": "This query needs review before it runs.", "query_id": "qry_1"}},
	})
	db := open(t, fmt.Sprintf("app:local@tcp(%s)/orders", addr))
	_, err := db.Exec("DELETE FROM orders")
	var my *mysql.MySQLError
	if !errors.As(err, &my) || my.Message != "This query needs review before it runs. Query qry_1 is waiting for approval in QueryDesk." {
		t.Fatalf("got %v", err)
	}
}

func TestConnectingTakesTheLocalPasswordAndAKnownDatabase(t *testing.T) {
	_, addr := start(t, map[string]fake.Answer{})
	if err := open(t, fmt.Sprintf("app:nope@tcp(%s)/orders", addr)).Ping(); number(err) != 1045 {
		t.Fatalf("wrong password: %v", err)
	}
	if err := open(t, fmt.Sprintf("app:local@tcp(%s)/payments", addr)).Ping(); number(err) != 1049 {
		t.Fatalf("a Postgres database: %v", err)
	}
}

func TestKillQueryStopsTheQueryInQueryDesk(t *testing.T) {
	qd, addr, server := startServer(t, map[string]fake.Answer{
		"SELECT SLEEP(60)": {Lines: []any{map[string]any{"columns": []querydesk.Column{fake.Col("SLEEP(60)", "null")}}}, Block: true},
	})
	db := open(t, fmt.Sprintf("app:local@tcp(%s)/orders", addr))

	done := make(chan error, 1)
	go func() {
		rows, err := db.Query("SELECT SLEEP(60)")
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
		}
		done <- err
	}()
	// a GUI knows its connection's id from the handshake; this test reads it from the server
	var id uint32
	for id == 0 {
		time.Sleep(20 * time.Millisecond)
		server.sessions.Range(func(key, _ any) bool { id = key.(uint32); return false })
	}
	time.Sleep(100 * time.Millisecond)

	if _, err := db.Exec(fmt.Sprintf("KILL QUERY %d", id)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-qd.Cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("QueryDesk's request wasn't cancelled")
	}
	if err := <-done; number(err) != 1317 {
		t.Fatalf("got %v", err)
	}
}

func TestAConnectionsStatementsShareOneSession(t *testing.T) {
	qd, addr := start(t, map[string]fake.Answer{"SELECT 1": fake.Rows([]querydesk.Column{fake.Col("one", "integer")}, []any{1})})
	qd.Sessions = true
	db := open(t, fmt.Sprintf("app:local@tcp(%s)/orders", addr))
	db.SetMaxOpenConns(1)

	for range 2 {
		if _, err := db.Exec("SELECT 1"); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		ranOn, opened, closed := qd.Seen()
		if len(closed) == 1 || time.Now().After(deadline) {
			if fmt.Sprint(ranOn) != "[ses_1 ses_1]" || fmt.Sprint(opened) != "[ses_1]" || fmt.Sprint(closed) != "[ses_1]" {
				t.Fatalf("ran on %q, opened %q, closed %q", ranOn, opened, closed)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
