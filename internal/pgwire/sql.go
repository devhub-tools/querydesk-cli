package pgwire

import (
	"strconv"
	"strings"
)

type kind int

const (
	code kind = iota
	literalText
	comment
)

// scan walks Postgres SQL in runs of code, literals (strings, quoted identifiers, dollar
// quotes) and comments, writing what each run's callback returns for it.
func scan(sql string, visit func(run string, k kind) string) string {
	var out strings.Builder
	start := 0
	emit := func(end int, k kind) {
		if end > start {
			out.WriteString(visit(sql[start:end], k))
		}
		start = end
	}
	for i := 0; i < len(sql); {
		end, k := 0, code
		switch c := sql[i]; {
		case c == '\'':
			escapes := i > 0 && (sql[i-1] == 'E' || sql[i-1] == 'e') && (i < 2 || !identChar(sql[i-2]))
			end, k = quoted(sql, i, '\'', escapes), literalText
		case c == '"':
			end, k = quoted(sql, i, '"', false), literalText
		case c == '-' && strings.HasPrefix(sql[i:], "--"):
			end, k = len(sql), comment
			if nl := strings.IndexByte(sql[i:], '\n'); nl >= 0 {
				end = i + nl + 1
			}
		case c == '/' && strings.HasPrefix(sql[i:], "/*"):
			end, k = blockComment(sql, i), comment
		case c == '$' && (i == 0 || !identChar(sql[i-1])):
			end, k = dollarQuoted(sql, i), literalText
		}
		if end > i {
			emit(i, code)
			emit(end, k)
			i = end
			continue
		}
		i++
	}
	emit(len(sql), code)
	return out.String()
}

func quoted(sql string, start int, quote byte, escapes bool) int {
	for i := start + 1; i < len(sql); i++ {
		switch {
		case escapes && sql[i] == '\\':
			i++
		case sql[i] == quote:
			if i+1 < len(sql) && sql[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(sql)
}

func blockComment(sql string, start int) int {
	depth := 0
	for i := start; i+1 < len(sql); i++ {
		switch {
		case sql[i] == '/' && sql[i+1] == '*':
			depth++
			i++
		case sql[i] == '*' && sql[i+1] == '/':
			depth--
			i++
			if depth == 0 {
				return i + 1
			}
		}
	}
	return len(sql)
}

// dollarQuoted is a $tag$...$tag$ string's end, or start for a $1 parameter: a tag can't start with a digit.
func dollarQuoted(sql string, start int) int {
	i := start + 1
	if i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
		return start
	}
	for i < len(sql) && identChar(sql[i]) {
		i++
	}
	if i >= len(sql) || sql[i] != '$' {
		return start
	}
	tag := sql[start : i+1]
	body := strings.Index(sql[i+1:], tag)
	if body < 0 {
		return len(sql)
	}
	return i + 1 + body + len(tag)
}

func identChar(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}

// split cuts a query string into its statements, dropping empty ones.
func split(sql string) []string {
	var statements []string
	var current strings.Builder
	cut := func() {
		if statement := current.String(); !blank(statement) {
			statements = append(statements, strings.TrimSpace(statement))
		}
		current.Reset()
	}
	scan(sql, func(run string, k kind) string {
		if k != code {
			current.WriteString(run)
			return run
		}
		for {
			semicolon := strings.IndexByte(run, ';')
			if semicolon < 0 {
				current.WriteString(run)
				return run
			}
			current.WriteString(run[:semicolon])
			cut()
			run = run[semicolon+1:]
		}
	})
	cut()
	return statements
}

// blank is whether sql holds nothing but whitespace and comments.
func blank(sql string) bool {
	empty := true
	scan(sql, func(run string, k kind) string {
		if k != comment && strings.TrimSpace(run) != "" {
			empty = false
		}
		return run
	})
	return empty
}

// params calls found with each $n in sql's code, writing what it returns in its place.
func params(sql string, found func(n int, placeholder string) string) string {
	return scan(sql, func(run string, k kind) string {
		if k != code {
			return run
		}
		var out strings.Builder
		for i := 0; i < len(run); i++ {
			j := i + 1
			for j < len(run) && run[j] >= '0' && run[j] <= '9' {
				j++
			}
			n, err := strconv.Atoi(run[i+1 : j])
			if run[i] != '$' || (i > 0 && identChar(run[i-1])) || err != nil {
				out.WriteByte(run[i])
				continue
			}
			out.WriteString(found(n, run[i:j]))
			i = j - 1
		}
		return out.String()
	})
}

// bind writes each $n as its parameter's literal: QueryDesk runs SQL text, not prepared statements.
func bind(sql string, values []*string) string {
	return params(sql, func(n int, placeholder string) string {
		if n < 1 || n > len(values) {
			return placeholder
		}
		return literal(values[n-1])
	})
}

// paramCount is the highest $n in sql.
func paramCount(sql string) int {
	highest := 0
	params(sql, func(n int, placeholder string) string {
		highest = max(highest, n)
		return placeholder
	})
	return highest
}

// literal is an untyped string literal, which Postgres casts to the type its place needs, as
// it does a text parameter.
func literal(value *string) string {
	if value == nil {
		return "NULL"
	}
	return "'" + strings.ReplaceAll(*value, "'", "''") + "'"
}

// stripComments is sql without its comments.
func stripComments(sql string) string {
	return scan(sql, func(run string, k kind) string {
		if k == comment {
			return " "
		}
		return run
	})
}
