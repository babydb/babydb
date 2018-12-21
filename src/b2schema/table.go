package b2schema

import (
	"time"

	"github.com/rs/xid"
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
