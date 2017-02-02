package hood

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
	"time"
)

func init() {
	RegisterDialect("cockroachdb", NewCockroachDB())
}

type cockroachdb struct {
	base
}

func NewCockroachDB() Dialect {
	d := &cockroachdb{}
	d.base.Dialect = d
	return d
}

func (d *cockroachdb) SqlType(f interface{}, size int) string {
	switch f.(type) {
	case Id:
		return "bigserial"
	case time.Time, Created, Updated:
		return "timestamp with time zone"
	case TimeUTC, CreatedUTC, UpdatedUTC:
		return "timestamp"
	case bool, sql.NullBool, *bool:
		return "boolean"
	case int, int8, int16, int32, uint, uint8, uint16, uint32, *int, *int8, *int16, *int32, *uint, *uint8, *uint16, *uint32:
		return "integer"
	case int64, uint64, *int64, *uint64, sql.NullInt64:
		return "bigint"
	case float32, float64, *float32, *float64, sql.NullFloat64:
		return "double precision"
	case []byte:
		return "bytea"
	case string, *string, sql.NullString:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("varchar(%d)", size)
		}
		return "text"
	default:
		panic("invalid sql type" )
	}

}

func (d *cockroachdb) Insert(hood *Hood, model *Model) (Id, error) {
	sqlStr, args := d.Dialect.InsertSql(model)
	var id int64
	err := hood.QueryRow(sqlStr, args...).Scan(&id)
	return Id(id), err
}

func (d *cockroachdb) InsertSql(model *Model) (string, []interface{}) {
	m := 0
	columns, markers, values := columnsMarkersAndValuesForModel(d.Dialect, model, &m)
	quotedColumns := make([]string, 0, len(columns))
	for _, c := range columns {
		quotedColumns = append(quotedColumns, d.Dialect.Quote(c))
	}
	sqlStr := fmt.Sprintf(
		"INSERT INTO %v (%v) VALUES (%v) RETURNING %v",
		d.Dialect.Quote(model.Table),
		strings.Join(quotedColumns, ", "),
		strings.Join(markers, ", "),
		d.Dialect.Quote(model.Pk.Name),
	)
	return sqlStr, values
}

func (d *cockroachdb) KeywordAutoIncrement() string {
	// cockroachdb has not auto increment keyword, uses SERIAL type
	return ""
}

// CockroachDB has no support for FK actions yet.
func (d *cockroachdb) ForeignKey(fk *ForeignKey) string {
	return fmt.Sprintf(
		"CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v(%v)", // ON UPDATE %v ON DELETE %v",
		d.Dialect.Quote(fk.Name),
		d.Dialect.Quote(fk.Column),
		d.Dialect.Quote(fk.ReferenceTable),
		d.Dialect.Quote(fk.ReferenceColumn),
		//d.Dialect.ReferentialAction(fk.OnUpdate),
		//d.Dialect.ReferentialAction(fk.OnDelete),
	)
}
// IF NOT EXISTS  added
func (d *cockroachdb) CreateIndexSql(name, table string, unique bool, columns ...string) string {
	a := []string{}
	quotedColumns := make([]string, 0, len(columns))
	for _, c := range columns {
		quotedColumns = append(quotedColumns, d.Dialect.Quote(c))
	}

	if unique {
		//a = append(a, "UNIQUE")
		a = append(a, fmt.Sprintf(
			"ALTER TABLE %v ADD CONSTRAINT %v UNIQUE(%v)",
			d.Dialect.Quote(table),
			d.Dialect.Quote(name),
			strings.Join(quotedColumns, ", "),
		))
	}else {
		a = append(a, fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS %v ON %v (%v)",
			d.Dialect.Quote(name),
			d.Dialect.Quote(table),
			strings.Join(quotedColumns, ", "),
		))
	}
	fmt.Println(strings.Join(a, " "))
	return strings.Join(a, " ")
}