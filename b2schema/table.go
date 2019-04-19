package b2schema

import (
	"errors"
	"log"
	"time"

	"github.com/rs/xid"
	rdb "github.com/tecbot/gorocksdb"
)

// B2Table 数据库表结构体
type B2Table struct {
	// TableName 表名
	TableName string `json:"TableName"`
	// Columns 字段列表
	Columns []B2Column `json:"Columns"`
	// CreateTime 创建时间
	CreateTime time.Time `json:"CreateTime"`
	// TableID 全局唯一表ID
	TableID string `json:"TableID"`
}

// NewTable 新建一张数据库表
func NewTable(name string, cols []B2Column,
	b2db *B2Database, meta *MetaDBSource) (*B2Table, error) {
	var t B2Table
	t.TableName = name
	t.TableID = xid.New().String()
	t.CreateTime = time.Now()
	t.Columns = cols
	if err := b2db.AddTable(&t, meta); err != nil {
		return nil, err
	}
	return &t, nil
}

func (t *B2Table) validate() bool {
	if len(t.TableName) == 0 || len(t.TableID) == 0 || (t.CreateTime == time.Time{}) {
		return false
	}
	for _, col := range t.Columns {
		if len(col.ColumnID) == 0 || len(col.ColumnName) == 0 || len(col.DataType) == 0 {
			return false
		}
	}
	return true
}

// InsertByValues 向表中插入一行数据
func (t *B2Table) InsertByValues(db *B2Database, values ...interface{}) (string, error) {
	rowKey := xid.New().String()
	wopts := rdb.NewDefaultWriteOptions()
	topts := rdb.NewDefaultTransactionOptions()
	if len(t.Columns) != len(values) {
		log.Printf("表字段个数与值个数不相符，字段数: %d，值个数: %d\n", len(t.Columns), len(values))
		return "", errors.New("fields and values mismatch")
	}
	txn := db.RocksDbWriteConn.TransactionBegin(wopts, topts, nil)
	for i, col := range t.Columns {
		colValue, err := col.FormatBytes(values[i])
		if err != nil {
			log.Printf("字段定义与值类型转换出错: %v\n", err)
			txn.Rollback()
			return "", err
		}
		k := rowKey + "/" + col.ColumnID
		err = writeKV([]byte(k), colValue, txn)
		if err != nil {
			log.Printf("写入字段数据时发生错误: %v\n", err)
			_ = txn.Rollback()
			return "", err
		}
	}
	err := txn.Commit()
	if err != nil {
		log.Printf("提交事务时发生错误: %v\n", err)
		_ = txn.Rollback()
		return "", err
	}
	return rowKey, nil
}

// InsertByMap 使用KV对向表中插入一行数据
func (t *B2Table) InsertByMap(db *B2Database, values map[string]interface{}) (string, error) {
	rowKey := xid.New().String()
	wopts := rdb.NewDefaultWriteOptions()
	topts := rdb.NewDefaultTransactionOptions()
	if len(values) > len(t.Columns) {
		log.Printf("数据个数与字段个数不符，values: %d，columns: %d\n", len(values), len(t.Columns))
		return "", errors.New("values more than fields")
	}
	txn := db.RocksDbWriteConn.TransactionBegin(wopts, topts, nil)
	for _, col := range t.Columns {
		if _, ok := values[col.ColumnName]; ok {
			colValue, err := col.FormatBytes(values[col.ColumnName])
			if err != nil {
				log.Printf("字段定义与值类型转换出错: %v\n", err)
				_ = txn.Rollback()
				return "", err
			}
			if err = writeKV([]byte(col.ColumnName), colValue, txn); err != nil {
				log.Printf("写入字段数据时发生错误: %v\n", err)
				_ = txn.Rollback()
				return "", err
			}
			delete(values, col.ColumnName)
		}
	}
	if len(values) > 0 {
		log.Println("数据值中存在与字段定义名称不符的部分")
		txn.Rollback()
		return "", errors.New("values map and columns definition mismatched")
	}
	err := txn.Commit()
	if err != nil {
		log.Printf("提交事务时发生错误: %v\n", err)
		_ = txn.Rollback()
		return "", err
	}
	return rowKey, nil
}

func writeKV(key, value []byte, txn *rdb.Transaction) error {
	return txn.Put(key, value)
}
