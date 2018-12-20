package b2schema

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/thoas/go-funk"

	"github.com/rs/xid"
	rdb "github.com/tecbot/gorocksdb"
)

// TODO: - implement unit test - up broadcast to global index - grpc notify whole clusters

// B2Database babydb数据库结构体
type B2Database struct {
	// 数据库名称
	Database string `json:"Database"`
	// 数据库表列表
	TableList []string `json:"TableList,omitempty"`
	// 数据库全局唯一ID，用于作为在rocksdb中的真实数据库ID
	DatabaseID string `json:"DatabaseID"`
	// rocksdb只读连接结构体
	RocksDbReadConn *rdb.DB `json:"-"`
	// rocksdb事务连接结构体
	RocksDbWriteConn *rdb.TransactionDB `json:"-"`
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
	guid := xid.New()
	db = &B2Database{
		Database:         name,
		TableList:        nil,
		DatabaseID:       guid.String(),
		RocksDbReadConn:  nil,
		RocksDbWriteConn: nil,
		CreateTime:       nil,
		OpenTime:         nil,
	}
	if err = meta.PutDatabase(db); err != nil {
		log.Fatalf("创建数据库时发生错误: %v\n", err)
		return nil, err
	}
	// TODO: up broadcast meta data to global index
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
	topts := rdb.NewDefaultTransactionDBOptions()
	if b2db.CreateTime == nil {
		opts.SetCreateIfMissing(true)
		opts.SetErrorIfExists(true)
	}
	write, err := rdb.OpenTransactionDb(opts, topts, b2db.DatabaseID)
	if err != nil {
		log.Fatalf("打开数据库连接时发生错误: %v\n", err)
		return nil, err
	}
	opts.SetCreateIfMissing(false)
	opts.SetErrorIfExists(false)
	read, err := rdb.OpenDbForReadOnly(opts, b2db.DatabaseID, false)
	nt := time.Now()
	b2db.RocksDbReadConn = read
	b2db.RocksDbWriteConn = write
	b2db.CreateTime = &nt
	b2db.OpenTime = &nt
	return b2db, nil
}

// DropDatabase 删除数据库
func DropDatabase(name string, meta *MetaDBSource) error {
	meta.Mu.Lock()
	defer meta.Mu.Unlock()
	b2db, err := meta.GetDatabase(name)
	if err != nil {
		log.Fatalf("找不到要删除的数据库: %s\n", name)
		return err
	}
	b2db.RocksDbReadConn.Close()
	b2db.RocksDbWriteConn.Close()
	opts := rdb.NewDefaultOptions()
	if err = rdb.DestroyDb(b2db.DatabaseID, opts); err != nil {
		log.Fatalf("删除数据库文件时发生错误: %v\n", err)
		return err
	}
	if err = meta.DelDatabase(name); err != nil {
		log.Fatalf("删除数据库META记录时发生错误，数据一致性可能已经破坏: %v\n", err)
		return err
	}
	return nil
}

// GetTable 在元数据中获取某个数据库表META内容
func (b2db *B2Database) GetTable(tableName string, meta *MetaDBSource) (*B2Table, error) {
	return meta.getTable(b2db.Database, tableName)
}

// AddTable 在数据库中添加表
func (b2db *B2Database) AddTable(table *B2Table, meta *MetaDBSource) error {
	if !table.validate() {
		log.Printf("数据库表 %s META内容不完整或有错误\n", table.TableName)
		return errors.New("invalid table structure data")
	}
	wopts := rdb.NewDefaultWriteOptions()
	topts := rdb.NewDefaultTransactionOptions()
	meta.Mu.Lock()
	defer meta.Mu.Unlock()
	txn := meta.rocksDB.TransactionBegin(wopts, topts, nil)
	key := []byte(b2db.Database + "/" + table.TableName)
	value, err := json.Marshal(table)
	if err != nil {
		log.Fatalf("将表 %s META数据转换为json时发生错误: %v\n", table.TableName, err)
		return err
	}
	err = txn.Put(key, value)
	if err != nil {
		log.Fatalf("在数据库 %s 中创建表 %s 时发生错误: %v\n", b2db.Database, table.TableName, err)
		_ = txn.Rollback()
		return err
	}
	b2db.TableList = append(b2db.TableList, table.TableName)
	key = []byte(b2db.Database)
	value, err = json.Marshal(b2db)
	if err != nil {
		log.Fatalf("将数据库 %s META数据转换为json时发生错误: %v\n", b2db.Database, err)
		_ = txn.Rollback()
		b2db.TableList = b2db.TableList[:len(b2db.TableList)-1]
		return err
	}
	if err = txn.Put(key, value); err != nil {
		log.Fatalf("更新数据库 %s 的META时出错: %v\n", b2db.Database, err)
		_ = txn.Rollback()
		b2db.TableList = b2db.TableList[:len(b2db.TableList)-1]
		return err
	}
	_ = txn.Commit()
	// TODO: up broadcast meta data to global index
	return nil
}

// RemoveTable 从数据库中移除表
func (b2db *B2Database) RemoveTable(tableName string, meta *MetaDBSource) error {
	wopts := rdb.NewDefaultWriteOptions()
	topts := rdb.NewDefaultTransactionOptions()
	meta.Mu.Lock()
	defer meta.Mu.Unlock()
	txn := meta.rocksDB.TransactionBegin(wopts, topts, nil)

	// TODO: remove all KV storage owned by the table

	key := []byte(b2db.Database + "/" + tableName)
	if err := txn.Delete(key); err != nil {
		log.Fatalf("删除数据库 %s 中的表 %s 的元数据时发生错误: %v\n", b2db.Database, tableName, err)
		_ = txn.Rollback()
		return err
	}
	pos := funk.IndexOf(b2db.TableList, tableName)
	if pos == -1 {
		log.Printf("数据库表 %s 在数据库 %s META数据中已经被移除\n", tableName, b2db.Database)
		_ = txn.Commit()
		return nil
	}
	restore := make([]string, len(b2db.TableList))
	copy(restore, b2db.TableList)
	copy(b2db.TableList[pos:], b2db.TableList[pos+1:])
	b2db.TableList = b2db.TableList[:len(b2db.TableList)-1]
	key = []byte(b2db.Database)
	value, err := json.Marshal(b2db)
	if err != nil {
		log.Fatalf("数据库 %s META数据转换json时发生错误: %v\n", b2db.Database, err)
		_ = txn.Rollback()
		b2db.TableList = restore
		return err
	}
	if err = txn.Put(key, value); err != nil {
		log.Fatalf("更新数据库 %s META数据时发生错误: %v\n", b2db.Database, err)
		_ = txn.Rollback()
		b2db.TableList = restore
		return err
	}
	_ = txn.Commit()
	// TODO: up broadcast meta data to global index
	return nil
}
