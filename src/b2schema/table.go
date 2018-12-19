package b2schema

import "time"

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
