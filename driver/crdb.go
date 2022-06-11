//go:generate go-bindata -nometadata -pkg driver -prefix override override/...
package driver

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/glerchundi/sqlboiler-crdb/v4/driver/override"
	_ "github.com/lib/pq" // Side-effect import sql driver
	"github.com/pkg/errors"
	"github.com/volatiletech/sqlboiler/v4/drivers"
	"github.com/volatiletech/sqlboiler/v4/importers"
	"github.com/volatiletech/strmangle"
)

var re = regexp.MustCompile(`\(([^\)]+)\)`)

// Assemble is more useful for calling into the library so you don't
// have to instantiate an empty type.
func Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	driver := &CockroachDBDriver{}
	return driver.Assemble(config)
}

// CockroachDBDriver holds the database connection string and a handle
// to the database connection.
type CockroachDBDriver struct {
	connStr string
	conn    *sql.DB
}

// Templates that should be added/overridden
func (d *CockroachDBDriver) Templates() (map[string]string, error) {
	names, err := override.TemplateFilenames()
	if err != nil {
		return nil, err
	}
	tpls := make(map[string]string)
	for _, n := range names {
		b, err := override.Template(n)
		if err != nil {
			return nil, err
		}

		tpls[n] = base64.StdEncoding.EncodeToString(b)
	}

	return tpls, nil
}

// Assemble all the information we need to provide back to the driver
func (d *CockroachDBDriver) Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			dbinfo = nil
			err = r.(error)
		}
	}()

	user := config.MustString(drivers.ConfigUser)
	pass, _ := config.String(drivers.ConfigPass)
	dbname := config.MustString(drivers.ConfigDBName)
	host := config.MustString(drivers.ConfigHost)
	port := config.DefaultInt(drivers.ConfigPort, 26257)
	sslmode := config.DefaultString(drivers.ConfigSSLMode, "disable")
	schema := config.DefaultString(drivers.ConfigSchema, "public")
	whitelist, _ := config.StringSlice(drivers.ConfigWhitelist)
	blacklist, _ := config.StringSlice(drivers.ConfigBlacklist)

	useSchema := schema != "public"

	d.connStr = buildQueryString(user, pass, dbname, host, port, sslmode)
	d.conn, err = sql.Open("postgres", d.connStr)
	if err != nil {
		return nil, errors.Wrap(err, "sqlboiler-crdb failed to connect to database")
	}

	defer func() {
		if e := d.conn.Close(); e != nil {
			dbinfo = nil
			err = e
		}
	}()

	dbinfo = &drivers.DBInfo{
		Schema: schema,
		Dialect: drivers.Dialect{
			LQ: '"',
			RQ: '"',

			UseIndexPlaceholders: true,
			UseSchema:            useSchema,
			UseDefaultKeyword:    true,
		},
	}

	dbinfo.Tables, err = drivers.Tables(d, schema, whitelist, blacklist)
	if err != nil {
		return nil, err
	}

	return dbinfo, err
}

// TableNames connects to the CockroachDB database and
// retrieves all table names from the information_schema where the
// table schema is schema. It uses a whitelist and blacklist.
func (d *CockroachDBDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := fmt.Sprintf(`select table_name from information_schema.tables where table_schema = $1`)
	args := []interface{}{schema}
	if len(whitelist) > 0 {
		tables := drivers.TablesFromList(whitelist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name in (%s);", strmangle.Placeholders(true, len(tables), 2, 1))
			for _, w := range tables {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		tables := drivers.TablesFromList(blacklist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s);", strmangle.Placeholders(true, len(tables), 2, 1))
			for _, b := range tables {
				args = append(args, b)
			}
		}
	}

	rows, err := d.conn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil
}

// Columns takes a table name and attempts to retrieve the table information
// from the database information_schema.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (d *CockroachDBDriver) Columns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	makeQuery := func(dataTypeCol, whereClause string) string {
		return fmt.Sprintf(`SELECT
		DISTINCT
		c.column_name,
		c.ordinal_position,
		max(%s) AS data_type,
		max(c.column_default) AS column_default,
		bool_or(
			CASE
			WHEN c.is_nullable = 'NO' THEN false
			ELSE true
			END
		)
			AS is_nullable,
		bool_or(
			CASE
			WHEN pc.count < 2 AND pgc.contype IN ('p', 'u')
			THEN true
			ELSE false
			END
		)
			AS is_unique
	FROM
		information_schema.columns AS c
		LEFT JOIN
			(
				SELECT
					DISTINCT
					c.column_name,
					pgc.conname AS conname,
					pgc.contype AS contype
				FROM
					information_schema.columns AS c
					LEFT JOIN
						information_schema.key_column_usage
							AS kcu
					ON
						c.table_name = kcu.table_name
						AND c.table_schema = kcu.table_schema
						AND c.column_name = kcu.column_name
					LEFT JOIN pg_constraint AS pgc
					ON kcu.constraint_name = pgc.conname
				WHERE
					c.table_schema = $1 AND c.table_name = $2
			)
				AS pgc
		ON c.column_name = pgc.column_name
		LEFT JOIN
			(
				SELECT
					kcu.table_schema,
					kcu.table_name,
					kcu.constraint_name,
					count(*)
				FROM
					information_schema.key_column_usage AS kcu
				GROUP BY
					kcu.table_schema,
					kcu.table_name,
					kcu.constraint_name
			)
				AS pc
		ON
			c.table_schema = pc.table_schema
			AND c.table_name = pc.table_name
			AND pgc.conname = pc.constraint_name
	WHERE
		c.table_schema = $1 AND c.table_name = $2 %s
	GROUP BY
		c.ordinal_position, c.column_name
	ORDER BY
		c.ordinal_position ASC;`, dataTypeCol, whereClause)
	}

	var columns []drivers.Column
	args := []interface{}{schema, tableName}

	var whereClause string
	if len(whitelist) > 0 {
		cols := drivers.ColumnsFromList(whitelist, tableName)
		if len(cols) > 0 {
			whereClause += fmt.Sprintf(" and c.column_name in (%s)", strmangle.Placeholders(true, len(cols), 3, 1))
			for _, w := range cols {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		cols := drivers.ColumnsFromList(blacklist, tableName)
		if len(cols) > 0 {
			whereClause += fmt.Sprintf(" and c.column_name not in (%s)", strmangle.Placeholders(true, len(cols), 3, 1))
			for _, w := range cols {
				args = append(args, w)
			}
		}
	}

	rows, err := d.conn.Query(makeQuery("c.crdb_sql_type", whereClause+` and c.is_hidden = 'NO'`), args...)
	if err != nil {
		// TODO(g.lerchundi): Remove this fallback logic post-2.2.
		// Ref: https://github.com/cockroachdb/cockroach/pull/28945
		if strings.Contains(err.Error(), "column \"crdb_sql_type\" does not exist") {
			rows, err = d.conn.Query(makeQuery("c.data_type", whereClause+` and c.is_hidden = 'NO'`), args...)
		}
		if strings.Contains(err.Error(), "column \"is_hidden\" does not exist") {
			rows, err = d.conn.Query(makeQuery("c.data_type", whereClause), args...)
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colType, udtName string
		var ordinalPos int32
		var defaultValue, arrayType *string
		var nullable, unique bool
		if err := rows.Scan(&colName, &ordinalPos, &colType, &defaultValue, &nullable, &unique); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}

		// To prevent marking nullable columns as not having a default value
		// Techinically, every nullable column is "DEFAULT NULL"
		if nullable && defaultValue == nil {
			null := "NULL"
			defaultValue = &null
		}

		// TODO(glerchundi): find a better way to infer this.
		dbType := strings.ToLower(re.ReplaceAllString(colType, ""))
		tmp := strings.Replace(dbType, "[]", "", 1)
		if dbType != tmp {
			arrayType = &tmp
			dbType = "array"
		}

		column := drivers.Column{
			Name:     colName,
			DBType:   dbType,
			ArrType:  arrayType,
			UDTName:  udtName,
			Nullable: nullable,
			Unique:   unique,
		}
		if defaultValue != nil {
			column.Default = *defaultValue
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (d *CockroachDBDriver) PrimaryKeyInfo(schema, tableName string) (*drivers.PrimaryKey, error) {
	pkey := &drivers.PrimaryKey{}
	var err error

	query := `SELECT
    tc.constraint_name
FROM
    information_schema.table_constraints AS tc
WHERE
    tc.table_name = $1
    AND tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = $2;`

	row := d.conn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	queryColumns := `SELECT
    kcu.column_name
FROM
    information_schema.key_column_usage AS kcu
WHERE
    constraint_name = $1
    AND table_schema = $2
    AND table_name = $3;`

	var rows *sql.Rows
	if rows, err = d.conn.Query(queryColumns, pkey.Name, schema, tableName); err != nil {
		return nil, err
	}

	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string

		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	pkey.Columns = columns

	return pkey, nil
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (d *CockroachDBDriver) ForeignKeyInfo(schema, tableName string) ([]drivers.ForeignKey, error) {
	var fkeys []drivers.ForeignKey

	query := `SELECT
    DISTINCT
    pgcon.conname,
    pgc.relname AS source_table,
    kcu.column_name AS source_column,
    dstlookupname.relname AS dest_table,
    pgadst.attname AS dest_column
FROM
    pg_namespace AS pgn
    INNER JOIN pg_class AS pgc
    ON pgn.oid = pgc.relnamespace AND pgc.relkind = 'r'
    INNER JOIN pg_constraint AS pgcon
    ON
        pgn.oid = pgcon.connamespace
        AND pgc.oid = pgcon.conrelid
    INNER JOIN pg_class AS dstlookupname
    ON pgcon.confrelid = dstlookupname.oid
    LEFT JOIN information_schema.key_column_usage AS kcu
    ON
        pgcon.conname = kcu.constraint_name
        AND pgc.relname = kcu.table_name
    LEFT JOIN information_schema.key_column_usage AS kcudst
    ON
        pgcon.conname = kcu.constraint_name
        AND dstlookupname.relname = kcu.table_name
    INNER JOIN pg_attribute AS pgadst
    ON
        pgcon.confrelid = pgadst.attrelid
        AND pgadst.attnum = ANY pgcon.confkey
WHERE
    pgn.nspname = $2
    AND pgc.relname = $1
    AND pgcon.contype = 'f'
ORDER BY
    pgcon.conname DESC;`

	var rows *sql.Rows
	var err error
	if rows, err = d.conn.Query(query, tableName, schema); err != nil {
		return nil, err
	}

	for rows.Next() {
		var fkey drivers.ForeignKey
		var sourceTable string

		fkey.Table = tableName
		err = rows.Scan(&fkey.Name, &sourceTable, &fkey.Column, &fkey.ForeignTable, &fkey.ForeignColumn)
		if err != nil {
			return nil, err
		}

		fkeys = append(fkeys, fkey)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return fkeys, nil
}

// TranslateColumnType converts Cockroach database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
func (d *CockroachDBDriver) TranslateColumnType(c drivers.Column) drivers.Column {
	// parse DB type
	if c.Nullable {
		switch c.DBType {
		case "int8", "bigint", "bigserial":
			c.Type = "null.Int64"
		case "int4", "int", "integer", "serial":
			c.Type = "null.Int"
		case "int2", "smallint", "smallserial":
			c.Type = "null.Int16"
		case "decimal", "numeric":
			c.Type = "types.NullDecimal"
		case "float8", "float", "double precision":
			c.Type = "null.Float64"
		case "real":
			c.Type = "null.Float32"
		case "string", "collate", "bit", "interval", "bit varying", "character", "character varying", "char", "varchar", "inet", "uuid", "text":
			c.Type = "null.String"
		case `"char"`:
			c.Type = "null.Byte"
		case "bytes", "bytea":
			c.Type = "null.Bytes"
		case "json", "jsonb":
			c.Type = "null.JSON"
		case "bool", "boolean":
			c.Type = "null.Bool"
		case "date", "time", "timestamp", "timestamp without time zone", "timestamptz", "timestamp with time zone":
			c.Type = "null.Time"
		case "array", "ARRAY":
			if c.ArrType == nil {
				panic("unable to get CockroachDB ARRAY underlying type")
			}
			c.Type = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType = strings.ToUpper(c.DBType) + *c.ArrType
		default:
			fmt.Fprintf(os.Stderr, "Warning: Unhandled nullable data type %s, falling back to null.String\n", c.DBType)
			c.Type = "null.String"
		}
	} else {
		switch c.DBType {
		case "int8", "bigint", "bigserial":
			c.Type = "int64"
		case "int4", "int", "integer", "serial":
			c.Type = "int"
		case "int2", "smallint", "smallserial":
			c.Type = "int16"
		case "decimal", "numeric":
			c.Type = "types.Decimal"
		case "float8", "float", "double precision":
			c.Type = "float64"
		case "real":
			c.Type = "float32"
		case "string", "collate", "bit", "interval", "bit varying", "character", "character varying", "char", "varchar", "inet", "uuid", "text":
			c.Type = "string"
		case `"char"`:
			c.Type = "types.Byte"
		case "bytes", "bytea":
			c.Type = "[]byte"
		case "json", "jsonb":
			c.Type = "types.JSON"
		case "bool", "boolean":
			c.Type = "bool"
		case "date", "time", "timestamp", "timestamp without time zone", "timestamptz", "timestamp with time zone":
			c.Type = "time.Time"
		case "array", "ARRAY":
			if c.ArrType == nil {
				panic("unable to get CockroachDB ARRAY underlying type")
			}
			c.Type = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType = strings.ToUpper(c.DBType) + *c.ArrType
		default:
			fmt.Fprintf(os.Stderr, "Warning: Unhandled data type %s, falling back to string\n", c.DBType)
			c.Type = "string"
		}
	}
	return c
}

// getArrayType returns the correct boil.Array type for each database type
func getArrayType(c drivers.Column) string {
	switch *c.ArrType {
	case "int2", "int4", "int8", "int", "integer", "serial", "smallint", "smallserial", "bigint", "bigserial":
		return "types.Int64Array"
	case "bytes", "bytea":
		return "types.BytesArray"
	case "string", "collate", "bit", "interval", "bit varying", "character", "character varying", "char", "varchar", "inet", "text", "uuid":
		return "types.StringArray"
	case "bool", "boolean":
		return "types.BoolArray"
	case "decimal", "numeric":
		return "types.DecimalArray"
	case "float8", "float", "double precision", "real":
		return "types.Float64Array"
	default:
		fmt.Fprintf(os.Stderr, "Warning: Unhandled array data type %s, falling back to types.StringArray\n", *c.ArrType)
		return "types.StringArray"
	}
}

// Imports for the CockroachDB driver
func (d *CockroachDBDriver) Imports() (importers.Collection, error) {
	var col importers.Collection

	col.All = importers.Set{
		Standard: importers.List{
			`"strconv"`,
		},
	}
	col.Singleton = importers.Map{
		"crdb_upsert": {
			Standard: importers.List{
				`"fmt"`,
				`"strings"`,
			},
			ThirdParty: importers.List{
				`"github.com/volatiletech/strmangle"`,
				`"github.com/volatiletech/sqlboiler/v4/drivers"`,
			},
		},
	}
	col.TestSingleton = importers.Map{
		"crdb_suites_test": {
			Standard: importers.List{
				`"testing"`,
			},
		},
		"crdb_main_test": {
			Standard: importers.List{
				`"bytes"`,
				`"database/sql"`,
				`"fmt"`,
				`"io"`,
				`"os"`,
				`"os/exec"`,
				`"regexp"`,
				`"strings"`,
			},
			ThirdParty: importers.List{
				`"github.com/pkg/errors"`,
				`"github.com/spf13/viper"`,
				`"github.com/volatiletech/randomize"`,
				`_ "github.com/lib/pq"`,
			},
		},
	}
	col.BasedOnType = importers.Map{
		"null.Float32": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Float64": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int8": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int16": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int32": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Int64": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint8": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint16": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint32": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Uint64": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.String": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Bool": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Time": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.JSON": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"null.Bytes": {
			ThirdParty: importers.List{`"github.com/volatiletech/null/v8"`},
		},
		"time.Time": {
			Standard: importers.List{`"time"`},
		},
		"types.JSON": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.Decimal": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.BytesArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.Int64Array": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.Float64Array": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.BoolArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.StringArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.DecimalArray": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
		"types.NullDecimal": {
			ThirdParty: importers.List{`"github.com/volatiletech/sqlboiler/v4/types"`},
		},
	}

	return col, nil
}

func buildQueryString(user, pass, dbname, host string, port int, sslmode string) string {
	var up string
	if user != "" {
		up = user
	}
	if pass != "" {
		up = fmt.Sprintf("%s:%s", up, pass)
	}

	return fmt.Sprintf("postgresql://%s@%s:%d/%s?sslmode=%s", up, host, port, dbname, sslmode)
}
