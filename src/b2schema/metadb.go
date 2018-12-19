package b2schema

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	rdb "github.com/tecbot/gorocksdb"
)

// 元数据库名称常量
const (
	METADB = "B2META"
)

// MetaDBSource 元数据库连接结构体
type MetaDBSource struct {
	rocksDB  *rdb.DB
	OpenTime time.Time
	SyncTime time.Time
	Mu       *sync.Mutex
}

// OpenMetaConn 打开元数据库连接，主程序应该保存这个连接
func OpenMetaConn() *MetaDBSource {
	opts := rdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	meta, err := rdb.OpenDb(opts, METADB)
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

// PutDatabase 将某个数据库PUT更新到元数据中
func (c *MetaDBSource) PutDatabase(dbname string, dbstruct *B2Database) error {
	opts := rdb.NewDefaultWriteOptions()
	opts.SetSync(true)
	dbContent, err := json.Marshal(dbstruct)
	if err != nil {
		log.Fatalf("数据库META转换为JSON时发生错误: %v\n", err)
		return err
	}
	if err = c.rocksDB.Put(opts, []byte(dbname), dbContent); err != nil {
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
