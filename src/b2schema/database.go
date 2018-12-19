package b2schema

import (
	rdb "github.com/tecbot/gorocksdb"
)

// B2Database babydb数据库结构体
type B2Database struct {
	Database    string   `json:"Database"`
	TableList   []string `json:"TableList,omitempty"`
	DatabaseID  string   `json:"DatabaseID"`
	RocksDbConn rdb.DB   `json:"-"`
}

func NewDatabase(name string, tables []string) (*B2Database, error) {
	// TODO
	return nil, nil
}
