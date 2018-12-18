package b2schema

import (
	"encoding/json"
	"log"

	rdb "github.com/tecbot/gorocksdb"
)

// 元数据库名称常量
const (
	METADB = "B2META"
)

// OpenMetaConn 打开元数据库连接，主程序应该保存这个连接
func OpenMetaConn() *rdb.DB {
	opts := rdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	meta, err := rdb.OpenDb(opts, METADB)
	if err != nil {
		log.Panic("无法打开Meta元数据库连接，此服务器已经存在严重错误，服务即将退出: %v\n", err)
	}
	return meta
}

// GetDatabase 在元数据中查找某个名称的数据库
func GetDatabase(dbname string, meta *rdb.DB) (*B2Database, error) {
	opts = rdb.NewDefaultReadOptions()
	slice, err := meta.Get(opts, []byte(dbname))
	if err != nil {
		log.Printf("找不到名称为 %s 的数据库: %v\n", dbname, err)
		return nil, err
	}
	db := new(B2Database)
	if err = json.Unmarshal(slice.Data(), &db); err != nil {
		log.Printf("数据库元数据结构有错误: %v\n", err)
		return nil, err
	}
	return db, nil
}
