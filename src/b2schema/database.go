package b2schema

import (
	rdb "github.com/tecbot/gorocksdb"
)

// B2Database babydb数据库结构体
type B2Database struct {
	Database    string
	TableList   []string
	DatabaseID  string
	RocksDbConn rdb.DB
}

func NewDatabase(name string, tables []string) (*B2Database, error) {
	// TODO
	return nil, nil
}
