package b2schema

import (
	"errors"
	"log"
	"time"

	rdb "github.com/tecbot/gorocksdb"
)

// B2Database babydb数据库结构体
type B2Database struct {
	// 数据库名称
	Database string `json:"Database"`
	// 数据库表列表
	TableList []string `json:"TableList,omitempty"`
	// 数据库全局唯一ID，用于作为在rocksdb中的真实数据库ID
	DatabaseID string `json:"DatabaseID"`
	// rocksdb连接结构体
	RocksDbConn *rdb.DB `json:"-"`
	// 创建时间
	CreateTime *time.Time `json:"CreateTime,omitempty"`
	// 打开时间
	OpenTime *time.Time `json:"OpenTime,omitempty"`
}

// NewDatabase 创建一个新的数据库
// name 数据库名称
// meta 元数据库连接结构体指针
func NewDatabase(name string, meta *MetaDBSource) (*B2Database, error) {
	// 加锁，保证修改元数据时是原子操作
	meta.Mu.Lock()
	defer meta.Mu.Unlock()
	db, err := meta.GetDatabase(name)
	if err == nil {
		log.Fatalf("数据库名称已经存在: %s\n", name)
		return nil, errors.New("database name duplicated")
	}
	db = &B2Database{
		Database:    name,
		TableList:   nil,
		DatabaseID:  "UniqueID", // TODO: Global ID Generator to be called
		RocksDbConn: nil,
		CreateTime:  nil,
		OpenTime:    nil,
	}
	if err = meta.PutDatabase(name, db); err != nil {
		log.Fatalf("创建数据库时发生错误: %v\n", err)
		return nil, err
	}
	return db, nil
}

// NewDatabaseAndOpen 新建数据库并打开
func NewDatabaseAndOpen(name string, meta *MetaDBSource) (*B2Database, error) {
	b2db, err := NewDatabase(name, meta)
	if err != nil {
		log.Fatalf("新建数据库时发生错误: %v\n", err)
		return nil, err
	}
	b2db, err = b2db.OpenConnection()
	if err != nil {
		log.Fatalf("打开数据库时发生错误: %v\n", err)
		return nil, err
	}
	return b2db, nil
}

// OpenConnection 打开B2DB数据库连接
func (b2db *B2Database) OpenConnection() (*B2Database, error) {
	opts := rdb.NewDefaultOptions()
	if b2db.CreateTime == nil {
		opts.SetCreateIfMissing(true)
		opts.SetErrorIfExists(true)
	}
	rocks, err := rdb.OpenDb(opts, b2db.DatabaseID)
	if err != nil {
		log.Fatalf("打开数据库连接时发生错误: %v\n", err)
		return nil, err
	}
	nt := time.Now()
	b2db.RocksDbConn = rocks
	b2db.CreateTime = &nt
	b2db.OpenTime = &nt
	return b2db, nil
}
