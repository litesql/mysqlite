package extension

import (
	"github.com/walterwanderley/sqlite"
)

func registerFunc(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
	if err := api.CreateModule("information_schema.tables", &TablesModule{}, sqlite.EponymousOnly(true), sqlite.ReadOnly(true)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateModule("information_schema.columns", &ColumnsModule{}, sqlite.EponymousOnly(true), sqlite.ReadOnly(true)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("mysqlite_info", &Info{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}

	return sqlite.SQLITE_OK, nil
}
