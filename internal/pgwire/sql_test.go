package pgwire

import (
	"reflect"
	"testing"

	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

func TestSplitKeepsSemicolonsInsideStringsAndComments(t *testing.T) {
	sql := `SELECT 'a;b', "c;d", E'it\'s;' ; /* x; /* nested; */ */ SELECT $$e;f$$ ; SELECT $tag$g;$tag$ -- h;
	;; SELECT 1`
	want := []string{`SELECT 'a;b', "c;d", E'it\'s;'`, `/* x; /* nested; */ */ SELECT $$e;f$$`, "SELECT $tag$g;$tag$ -- h;", "SELECT 1"}
	if got := split(sql); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q", got)
	}
	if got := split(" ; -- only a comment\n"); got != nil {
		t.Fatalf("got %q", got)
	}
}

func TestBindReplacesOnlyParameters(t *testing.T) {
	value, quote := "7", "it's"
	got := bind(`SELECT $1, '$1', "$2", $2, $3, a$1 FROM t -- $1`, []*string{&value, &quote, nil})
	want := `SELECT '7', '$1', "$2", 'it''s', NULL, a$1 FROM t -- $1`
	if got != want {
		t.Fatalf("got %s", got)
	}
	if n := paramCount("SELECT $1, $12, '$40'"); n != 12 {
		t.Fatalf("paramCount %d", n)
	}
}

func TestValuesAreSentInPostgresTextForm(t *testing.T) {
	cases := []struct {
		value any
		typ   string
		want  string
	}{
		{"2026-10-01T09:30:00", "timestamp", "2026-10-01 09:30:00"},
		{"2026-10-01T09:30:00Z", "timestamptz", "2026-10-01 09:30:00+00"},
		{[]any{[]any{"a", nil}, []any{"{", "null"}}, "array", `{{a,NULL},{"{","null"}}`},
		{false, "boolean", "f"},
		{"3q2+7w==", "binary", `\xdeadbeef`},
	}
	for _, c := range cases {
		if got := string(text(c.value, querydesk.Column{Type: c.typ})); got != c.want {
			t.Errorf("%v as %s: got %s", c.value, c.typ, got)
		}
	}
	if got := commandTag("insert", 2); got != "INSERT 0 2" {
		t.Errorf("tag %s", got)
	}
	if got := commandTag("create_table", 0); got != "CREATE TABLE" {
		t.Errorf("tag %s", got)
	}
}
