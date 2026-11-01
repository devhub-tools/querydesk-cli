package pgwire

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/devhub-tools/querydesk-cli/internal/querydesk"
)

// QueryDesk names a column's type from its values; each takes the Postgres type whose text
// form it's sent in. A client reads the real column types from the catalog.
var typeOIDs = map[string]uint32{
	"boolean":     16,
	"binary":      17,
	"integer":     20,
	"json":        114,
	"float":       701,
	"date":        1082,
	"time":        1083,
	"timestamp":   1114,
	"timestamptz": 1184,
	"decimal":     1700,
	"uuid":        2950,
	"array":       1009,
}

const textOID = 25

func oid(column querydesk.Column) uint32 {
	if oid, ok := typeOIDs[column.Type]; ok {
		return oid
	}
	return textOID
}

// text renders a value as Postgres sends it in text format; nil is SQL NULL.
func text(value any, column querydesk.Column) []byte {
	if value == nil {
		return nil
	}
	switch column.Type {
	case "array":
		return []byte(arrayLiteral(value))
	case "json":
		raw, _ := json.Marshal(value)
		return raw
	}
	switch v := value.(type) {
	case bool:
		if v {
			return []byte("t")
		}
		return []byte("f")
	case json.Number:
		return []byte(v.String())
	case string:
		switch column.Type {
		// QueryDesk sends ISO 8601; Postgres puts a space between date and time
		case "timestamp":
			return []byte(strings.Replace(v, "T", " ", 1))
		case "timestamptz":
			return []byte(timestamptz(v))
		case "binary":
			raw, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return []byte(v)
			}
			return []byte(`\x` + hex.EncodeToString(raw))
		}
		return []byte(v)
	default:
		raw, _ := json.Marshal(v)
		return raw
	}
}

func timestamptz(v string) string {
	v = strings.Replace(v, "T", " ", 1)
	if strings.HasSuffix(v, "Z") {
		return strings.TrimSuffix(v, "Z") + "+00"
	}
	return v
}

// arrayLiteral writes a Postgres array literal: {1,2}, {"a b",NULL}, {{1},{2}}.
func arrayLiteral(value any) string {
	items, ok := value.([]any)
	if !ok {
		return element(value)
	}
	parts := make([]string, len(items))
	for i, item := range items {
		if nested, ok := item.([]any); ok {
			parts[i] = arrayLiteral(nested)
		} else {
			parts[i] = element(item)
		}
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func element(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return "t"
		}
		return "f"
	case json.Number:
		return v.String()
	case string:
		return quote(v)
	default:
		raw, _ := json.Marshal(v)
		return quote(string(raw))
	}
}

func quote(s string) string {
	if s != "" && !strings.ContainsAny(s, "{},\"\\ \t\n\r") && !strings.EqualFold(s, "null") {
		return s
	}
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

// commandTag is CommandComplete's tag for a write QueryDesk names, such as "insert".
func commandTag(command string, rows int64) string {
	verb := strings.ToUpper(strings.ReplaceAll(command, "_", " "))
	switch verb {
	case "INSERT":
		return fmt.Sprintf("INSERT 0 %d", rows)
	case "UPDATE", "DELETE", "SELECT", "MERGE", "FETCH", "MOVE", "COPY":
		return fmt.Sprintf("%s %d", verb, rows)
	case "":
		return "OK"
	}
	return verb
}
