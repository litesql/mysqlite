package extension

import (
	"github.com/walterwanderley/sqlite"
)

type TablesModule struct {
}

func (m *TablesModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	dbName := "main"
	if len(args) > 3 {
		dbName = args[3]
	}
	return &TablesVirtualTable{conn: conn, dbName: dbName},
		declare("CREATE TABLE x(table_catalog TEXT, table_schema TEXT, table_name TEXT, table_type TEXT, engine TEXT, version INTEGER, row_format TEXT, table_rows INTEGER, avg_row_length INTEGER, data_length INTEGER, max_data_length INTEGER, index_length INTEGER, data_free INTEGER, auto_increment INTEGER, create_time TEXT, update_time TEXT, check_time TEXT, table_collation TEXT, checksum TEXT, create_options TEXT, table_comment TEXT)")
}

type TablesVirtualTable struct {
	conn   *sqlite.Conn
	dbName string
}

func (vt *TablesVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{}, nil
}

func (vt *TablesVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newTablesCursor(vt.conn, vt.dbName), nil
}

func (vt *TablesVirtualTable) Disconnect() error {
	return nil
}

func (vt *TablesVirtualTable) Destroy() error {
	return nil
}

type Table struct {
	Name string
	Type string
}

type tablesCursor struct {
	conn    *sqlite.Conn
	dbName  string
	data    []Table
	current Table // current row that the cursor points to
	rowid   int64 // current rowid .. negative for EOF
}

func newTablesCursor(conn *sqlite.Conn, dbName string) *tablesCursor {
	return &tablesCursor{
		conn:   conn,
		dbName: dbName,
	}
}

func (c *tablesCursor) Next() error {
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

func (c *tablesCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText("def") // table_catalog
	case 1:
		ctx.ResultText(c.dbName) // table_schema
	case 2:
		ctx.ResultText(c.current.Name)
	case 3:
		ctx.ResultText(c.current.Type)
	case 4:
		ctx.ResultText("SQLite") // engine
	case 5:
		ctx.ResultInt(10) // version
	case 6:
		ctx.ResultText("Dynamic") // row_format
	case 7:
		ctx.ResultNull() // table_rows
	case 8:
		ctx.ResultNull() // avg_row_length
	case 9:
		ctx.ResultNull() // data_length
	case 10:
		ctx.ResultNull() // max_data_length
	case 11:
		ctx.ResultNull() // index_length
	case 12:
		ctx.ResultNull() // data_free
	case 13:
		ctx.ResultNull() // auto_increment
	case 14:
		ctx.ResultNull() // create_time
	case 15:
		ctx.ResultNull() // update_time
	case 16:
		ctx.ResultNull() // check_time
	case 17:
		ctx.ResultText("utf8mb4_general_ci") // table_collation
	case 18:
		ctx.ResultNull() // checksum
	case 19:
		ctx.ResultText("") // create_options
	case 20:
		ctx.ResultText("") // table_comment
	}
	return nil
}

func (c *tablesCursor) Filter(i int, x string, values ...sqlite.Value) error {
	c.data = []Table{}
	err := c.conn.Exec("SELECT name, upper(type) FROM sqlite_master WHERE (type='table' OR type='view') AND name NOT LIKE 'sqlite_%'",
		func(stmt *sqlite.Stmt) error {
			tableName := stmt.GetText("name")
			tableType := stmt.GetText("type")
			if tableType == "TABLE" {
				tableType = "BASE TABLE"
			}
			c.data = append(c.data, Table{
				Name: tableName,
				Type: tableType,
			})
			return nil
		})
	if err != nil {
		return err
	}
	c.rowid = 0

	return c.Next()
}

func (c *tablesCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *tablesCursor) Eof() bool {
	return c.rowid < 0
}

func (c *tablesCursor) Close() error {
	return nil
}
