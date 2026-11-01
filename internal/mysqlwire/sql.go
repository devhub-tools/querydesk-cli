package mysqlwire

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// placeholders counts a prepared statement's ? marks outside strings, identifiers and comments.
func placeholders(query string) int {
	count := 0
	walk(query, func(i int) { count++ })
	return count
}

// bind writes each ? as its argument's literal: QueryDesk runs SQL text, not prepared statements.
func bind(query string, args []any) (string, error) {
	var out strings.Builder
	last, n := 0, 0
	var err error
	walk(query, func(i int) {
		out.WriteString(query[last:i])
		last = i + 1
		if n >= len(args) {
			err = mysql.NewError(mysql.ER_WRONG_ARGUMENTS, "too few arguments for the statement's placeholders")
			return
		}
		out.WriteString(literal(args[n]))
		n++
	})
	out.WriteString(query[last:])
	return out.String(), err
}

// walk calls mark with the index of each ? in code.
func walk(query string, mark func(i int)) {
	for i := 0; i < len(query); i++ {
		switch c := query[i]; {
		case c == '\'' || c == '"' || c == '`':
			i = closing(query, i, c)
		case c == '#' || (c == '-' && strings.HasPrefix(query[i:], "-- ")):
			if nl := strings.IndexByte(query[i:], '\n'); nl >= 0 {
				i += nl
			} else {
				i = len(query)
			}
		case c == '/' && strings.HasPrefix(query[i:], "/*"):
			if end := strings.Index(query[i+2:], "*/"); end >= 0 {
				i += end + 3
			} else {
				i = len(query)
			}
		case c == '?':
			mark(i)
		}
	}
}

func closing(query string, start int, quote byte) int {
	for i := start + 1; i < len(query); i++ {
		switch {
		case query[i] == '\\' && quote != '`':
			i++
		case query[i] == quote:
			if i+1 < len(query) && query[i+1] == quote {
				i++
				continue
			}
			return i
		}
	}
	return len(query)
}

func literal(arg any) string {
	switch v := arg.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(v, 10)
	case int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
		return fmt.Sprint(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		return "'" + v.Format("2006-01-02 15:04:05.999999") + "'"
	case []byte:
		return quote(string(v))
	// a string, a decimal or a date, as the client sent it
	case mysql.TypedBytes:
		return quote(string(v.Bytes))
	case string:
		return quote(v)
	}
	return quote(fmt.Sprint(arg))
}

func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\x00", `\0`, "\n", `\n`, "\r", `\r`, "\x1a", `\Z`).Replace(s) + "'"
}
