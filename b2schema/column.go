package b2schema

import (
	"encoding/binary"
	"errors"
	"log"
	"math"

	"github.com/rs/xid"
)

// TODO: - add more unit tests

// B2Column 字段结构体
type B2Column struct {
	// ColumnName 字段名称
	ColumnName string `json:"ColumnName"`
	// DataType 以字符串表示数据类型
	DataType string `json:"DataType"`
	// DataLength 字段长度
	DataLength int `json:"DataLength,omitempty"`
	// Indexing 是否为索引字段
	Indexing bool `json:"Indexing,omitempty"`
	// ColumnID 字段全局唯一IDtable
	ColumnID string `json:"ColumnID"`
	// IndexID 索引全局唯一ID
	IndexID string `json:"IndexID"`
}

// NewColumn 创建一个新的字段，仅包括基本字段名称和数据类型
func NewColumn(name, dt string) *B2Column {
	return &B2Column{
		ColumnName: name,
		DataType:   dt,
		ColumnID:   xid.New().String(),
	}
}

// Length 设置字段数据长度
func (col *B2Column) Length(l int) *B2Column {
	col.DataLength = l
	return col
}

// Index 设置字段索引
func (col *B2Column) Index(i bool) *B2Column {
	col.Indexing = i
	if i {
		col.IndexID = xid.New().String()
	}
	return col
}

// FormatBytes 将一个值按照字段数据类型定义转换为一个字节数组值
func (col *B2Column) FormatBytes(value interface{}) ([]byte, error) {
	t, err := NameAsType(col.DataType)
	if err != nil {
		log.Fatalf("字段 %s 数据类型定义 %s 有错误: %v\n", col.ColumnName, col.DataType, err)
		return nil, err
	}
	if v, ok := value.(int32); t.Dtype == DtInt32 && ok {
		return int32ToBytes(v), nil
	}
	if v, ok := value.(int64); t.Dtype == DtInt64 && ok {
		return int64ToBytes(v), nil
	}
	if v, ok := value.(float32); t.Dtype == DtFloat32 && ok {
		return float32ToBytes(v), nil
	}
	if v, ok := value.(float64); t.Dtype == DtFloat64 && ok {
		return float64ToBytes(v), nil
	}
	if v, ok := value.(string); t.Dtype == DtString && ok {
		return []byte(v), nil
	}
	if v, ok := value.([]byte); t.Dtype == DtBytes && ok {
		return v, nil
	}
	if v, ok := value.(int64); t.Dtype == DtBytes && ok {
		return int64ToBytes(v), nil
	}
	log.Printf("值 %v 与字段数据类型 %s 定义不相符，无法转换\n", value, col.DataType)
	return nil, errors.New("value and column data type mismatched")
}

// ParseMap 将一个字节数组值按照字段定义转换为一个map
func (col *B2Column) ParseMap(value []byte) (map[string]interface{}, error) {
	t, err := NameAsType(col.DataType)
	if err != nil {
		log.Fatalf("字段 %s 数据类型定义 %s 有错误: %v\n", col.ColumnName, col.DataType, err)
		return nil, err
	}
	out := make(map[string]interface{})
	switch t.Dtype {
	case DtInt32:
		// 是否需要检查字节数组长度？
		out[col.ColumnName] = bytesToInt32(value)
	case DtInt64:
		out[col.ColumnName] = bytesToInt64(value)
	case DtFloat32:
		out[col.ColumnName] = bytesToFloat32(value)
	case DtFloat64:
		out[col.ColumnName] = bytesToFloat64(value)
	case DtString:
		out[col.ColumnName] = string(value)
	case DtBytes:
		out[col.ColumnName] = value
	case DtTimestamp:
		out[col.ColumnName] = bytesToInt64(value)
	}
	return out, nil
}

// ParseInt32 将一个字节数组值按照字段定义转换为int32
func (col *B2Column) ParseInt32(value []byte) (int32, bool) {
	m, err := col.ParseMap(value)
	if err != nil {
		return 0, false
	}
	v, ok := m[col.ColumnName].(int32)
	if !ok {
		log.Printf("值 %v 与字段定义 %s 不相符，转换失败\n", value, col.DataType)
		return 0, false
	}
	return v, true
}

// ParseInt64 将一个字节数组值按照字段定义转换为int64
func (col *B2Column) ParseInt64(value []byte) (int64, bool) {
	m, err := col.ParseMap(value)
	if err != nil {
		return 0, false
	}
	v, ok := m[col.ColumnName].(int64)
	if !ok {
		log.Printf("值 %v 与字段定义 %s 不相符，转换失败\n", value, col.DataType)
		return 0, false
	}
	return v, true
}

// ParseFloat32 将一个字节数组值按照字段定义转换为float32
func (col *B2Column) ParseFloat32(value []byte) (float32, bool) {
	m, err := col.ParseMap(value)
	if err != nil {
		return 0.0, false
	}
	v, ok := m[col.ColumnName].(float32)
	if !ok {
		log.Printf("值 %v 与字段定义 %s 不相符，转换失败\n", value, col.DataType)
		return 0.0, false
	}
	return v, true
}

// ParseFloat64 将一个字节数组值按照字段定义转换为float64
func (col *B2Column) ParseFloat64(value []byte) (float64, bool) {
	m, err := col.ParseMap(value)
	if err != nil {
		return 0.0, false
	}
	v, ok := m[col.ColumnName].(float64)
	if !ok {
		log.Printf("值 %v 与字段定义 %s 不相符，转换失败\n", value, col.DataType)
		return 0.0, false
	}
	return v, true
}

// ParseString 将一个字节数组值按照字段定义转换为string
func (col *B2Column) ParseString(value []byte) (string, bool) {
	if col.DataType == "string" && len(value) <= col.DataLength {
		return string(value), true
	}
	return "", false
}

// 下面是一些二进制转换工具函数

// Int32ToBytes 将一个int32值转换为字节数组
func Int32ToBytes(i int32) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(i))
	return bs
}

// BytesToInt32 将一个字节数组转换为int32值
func BytesToInt32(bs []byte) int32 {
	return int32(binary.LittleEndian.Uint32(bs))
}

// Int64ToBytes 将一个int64值转换为字节数组
func Int64ToBytes(i int64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(i))
	return bs
}

// BytesToInt64 将一个字节数组转换为int64值
func BytesToInt64(bs []byte) int64 {
	return int64(binary.LittleEndian.Uint64(bs))
}

// Float32ToBytes 将一个float32值转换为字节数组
func Float32ToBytes(f float32) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, math.Float32bits(f))
	return bs
}

// BytesToFloat32 将一个字节数组转换为float32值
func BytesToFloat32(bs []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(bs))
}

// Float64ToBytes 将一个float64值转换为字节数组
func Float64ToBytes(f float64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, math.Float64bits(f))
	return bs
}

// BytesToFloat64 将一个字节数组转换为字节数组
func BytesToFloat64(bs []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(bs))
}
