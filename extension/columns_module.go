package extension

import (
	"github.com/walterwanderley/sqlite"
)

type ColumnsModule struct {
}

func (m *ColumnsModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	dbName := "main"
	if len(args) > 3 {
		dbName = args[3]
	}
	return &ColumnsVirtualTable{conn: conn, dbName: dbName},
		declare("CREATE TABLE x(table_catalog TEXT, table_schema TEXT, table_name TEXT, column_name TEXT, ordinal_position INTEGER, column_default TEXT, is_nullable TEXT, data_type TEXT, character_maximum_length INTEGER, character_octet_length INTEGER, numeric_precision INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, character_set_name TEXT, collation_name TEXT, column_type TEXT, column_key TEXT, extra TEXT, privileges TEXT, column_comment TEXT, generation_expression TEXT, srs_id INTEGER)")
}

type ColumnsVirtualTable struct {
	conn   *sqlite.Conn
	dbName string
}

func (vt *ColumnsVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{}, nil
}

func (vt *ColumnsVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newColumnsCursor(vt.conn, vt.dbName), nil
}

func (vt *ColumnsVirtualTable) Disconnect() error {
	return nil
}

func (vt *ColumnsVirtualTable) Destroy() error {
	return nil
}

// table_catalog TEXT, table_schema TEXT, table_name TEXT, column_name TEXT, ordinal_position INTEGER, column_default TEXT, is_nullable TEXT, data_type TEXT, character_maximum_length INTEGER, character_octet_length INTEGER, numeric_precision INTEGER, numeric_scale INTEGER, datetime_precision INTEGER, character_set_name TEXT, collation_name TEXT, column_type TEXT, column_key TEXT, extra TEXT, privileges TEXT, column_comment TEXT, generation_expression TEXT, srs_id INTEGER
type Column struct {
	TableSchema     string // 1
	TableName       string // 2
	ColumnName      string // 3
	OrdinalPosition int64  // 4
	Default         string // 5
	IsNullable      string // 6
	DataType        string // 7
	Key             string // 16
}

func (c Column) MySqlType() string {
	return c.DataType
}

type columnsCursor struct {
	conn    *sqlite.Conn
	dbName  string
	data    []Column
	current Column // current row that the cursor points to
	rowid   int64  // current rowid .. negative for EOF
}

func newColumnsCursor(conn *sqlite.Conn, dbName string) *columnsCursor {
	return &columnsCursor{
		conn:   conn,
		dbName: dbName,
	}
}

func (c *columnsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *columnsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText("def") // table_catalog
	case 1:
		ctx.ResultText(c.current.TableSchema)
	case 2:
		ctx.ResultText(c.current.TableName)
	case 3:
		ctx.ResultText(c.current.ColumnName)
	case 4:
		ctx.ResultInt(int(c.current.OrdinalPosition))
	case 5:
		ctx.ResultText(c.current.Default)
	case 6:
		ctx.ResultText(c.current.IsNullable)
	case 7:
		ctx.ResultText(c.current.DataType)
	case 8:
		ctx.ResultNull() // character_maximum_length
	case 9:
		ctx.ResultNull() // character_octet_length
	case 10:
		ctx.ResultNull() // numeric_precision
	case 11:
		ctx.ResultNull() // numeric_scale
	case 12:
		ctx.ResultNull() // datetime_precision
	case 13:
		ctx.ResultText("utf8mb4") // character_set_name
	case 14:
		ctx.ResultText("utf8mb4_general_ci") // collation_name
	case 15:
		ctx.ResultText(c.current.MySqlType()) // column_type
	case 16:
		ctx.ResultText(c.current.Key) // column_key
	case 17:
		ctx.ResultText("") // extra
	case 18:
		ctx.ResultText("select,insert,update,references") // privileges
	case 19:
		ctx.ResultText("") // comment
	}
	return nil
}

func (c *columnsCursor) Filter(i int, x string, values ...sqlite.Value) error {
	c.data = []Column{}
	err := c.conn.Exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
		func(stmt *sqlite.Stmt) error {
			tableName := stmt.GetText("name")
			columns, err := c.tableColumns(tableName)
			if err != nil {
				return err
			}
			for _, col := range columns {
				c.data = append(c.data, col)
			}
			return nil
		})
	if err != nil {
		return err
	}
	c.rowid = 0

	return c.Next()
}

func (c *columnsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *columnsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *columnsCursor) Close() error {
	return nil
}

func (c *columnsCursor) tableColumns(tableName string) ([]Column, error) {
	var columns []Column

	err := c.conn.Exec("PRAGMA table_info('"+tableName+"')",
		func(stmt *sqlite.Stmt) error {
			col := Column{
				TableSchema:     c.dbName,
				TableName:       tableName,
				ColumnName:      stmt.GetText("name"),
				OrdinalPosition: stmt.GetInt64("cid") + 1,
				Default:         stmt.GetText("dflt_value"),
				IsNullable:      "YES",
				DataType:        stmt.GetText("type"),
			}
			if stmt.GetInt64("notnull") == 1 {
				col.IsNullable = "NO"
			}
			if stmt.GetInt64("pk") == 1 {
				col.Key = "PRI"
			}
			columns = append(columns, col)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return columns, nil
}
