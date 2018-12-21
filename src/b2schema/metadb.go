package b2schema

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	rdb "github.com/tecbot/gorocksdb"
)

// TODO: - implement unit test

// 元数据库名称常量
const (
	METADB = "B2META"
)

// MetaDBSource 元数据库连接结构体
type MetaDBSource struct {
	rocksDB  *rdb.TransactionDB
	OpenTime time.Time
	SyncTime time.Time
	Mu       *sync.Mutex
}

// OpenMetaConn 打开元数据库连接，主程序应该保存这个连接
func OpenMetaConn() *MetaDBSource {
	opts := rdb.NewDefaultOptions()
	topts := rdb.NewDefaultTransactionDBOptions()
	opts.SetCreateIfMissing(true)
	meta, err := rdb.OpenTransactionDb(opts, topts, METADB)
	if err != nil {
		log.Panicf("无法打开Meta元数据库连接，此服务器已经存在严重错误，服务即将退出: %v\n", err)
	}
	nt := time.Now()
	mutex := &sync.Mutex{}
	return &MetaDBSource{
		rocksDB:  meta,
		OpenTime: nt,
		SyncTime: nt,
		Mu:       mutex,
	}
}

// GetDatabase 在元数据中查找某个名称的数据库
func (c *MetaDBSource) GetDatabase(dbname string) (*B2Database, error) {
	opts := rdb.NewDefaultReadOptions()
	slice, err := c.rocksDB.Get(opts, []byte(dbname))
	if err != nil {
		log.Printf("读取名称为 %s 的数据库META时发生错误: %v\n", dbname, err)
		return nil, err
	}
	if slice.Size() == 0 {
		return nil, errors.New("database not exists")
	}
	db := B2Database{}
	if err = json.Unmarshal(slice.Data(), &db); err != nil {
		log.Printf("数据库元数据结构有错误: %v\n", err)
		return nil, err
	}
	return &db, nil
}

// PutDatabase 将某个数据库PUT更新到元数据中
func (c *MetaDBSource) PutDatabase(d2db *B2Database) error {
	opts := rdb.NewDefaultWriteOptions()
	opts.SetSync(true)
	dbContent, err := json.Marshal(d2db)
	if err != nil {
		log.Fatalf("数据库META转换为JSON时发生错误: %v\n", err)
		return err
	}
	if err = c.rocksDB.Put(opts, []byte(d2db.Database), dbContent); err != nil {
		log.Fatalf("将数据库META写入rocksdb时发生错误: %v\n", err)
		return err
	}
	c.SyncTime = time.Now()
	return nil
}

// DelDatabase 在元数据库中删除数据库
func (c *MetaDBSource) DelDatabase(dbname string) error {
	opts := rdb.NewDefaultWriteOptions()
	opts.SetSync(true)
	if err := c.rocksDB.Delete(opts, []byte(dbname)); err != nil {
		log.Fatalf("删除数据库时发生错误: %v\n", err)
		return err
	}
	c.SyncTime = time.Now()
	return nil
}

func (c *MetaDBSource) getTable(dbname, tableName string) (*B2Table, error) {
	opts := rdb.NewDefaultReadOptions()
	slice, err := c.rocksDB.Get(opts, []byte(dbname+"/"+tableName))
	if err != nil {
		log.Printf("找不到数据库 %s 中的表 %s: %v\n", dbname, tableName, err)
		return nil, err
	}
	var table B2Table
	if err = json.Unmarshal(slice.Data(), &table); err != nil {
		log.Printf("数据库表元数据结构有错误: %v\n", err)
		return nil, err
	}
	return &table, nil
}

func (c *MetaDBSource) putTable(dbname string, table *B2Table) error {
	opts := rdb.NewDefaultWriteOptions()
	opts.SetSync(true)
	tableContent, err := json.Marshal(table)
	if err != nil {
		log.Fatalf("将数据库表 %s 的META转换为JSON时发生错误: %v\n", table.TableName, err)
		return err
	}
	key := []byte(dbname + "/" + table.TableName)
	if err = c.rocksDB.Put(opts, key, tableContent); err != nil {
		log.Fatalf("将数据库表 %s META写入rocksdb时发生错误: %v", table.TableName, err)
		return err
	}
	return nil
}

// Close 关闭META数据库连接
func (c *MetaDBSource) Close() {
	c.rocksDB.Close()
}
