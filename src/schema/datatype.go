package schema

import "errors"

// 数据类型枚举
const (
	DtInt32     = iota // int32 -> 0, and so on.
	DtInt64            // int64
	DtFloat32          // float32
	DtFloat64          // float64
	DtString           // string
	DtBytes            // []byte
	DtTimestamp        // time
)

// DataType 数据类型结构体
type DataType struct {
	Dtype    int    // 数据类型枚举值
	TypeName string // 数据类型名称
}

var (
	// B2Int32 int32结构体
	B2Int32 = DataType{Dtype: DtInt32, TypeName: "int32"}
	// B2Int64 int64结构体
	B2Int64 = DataType{Dtype: DtInt64, TypeName: "int64"}
	// B2Float32 float32结构体
	B2Float32 = DataType{Dtype: DtFloat32, TypeName: "float32"}
	// B2Float64 float64结构体
	B2Float64 = DataType{Dtype: DtFloat64, TypeName: "float64"}
	// B2String string结构体
	B2String = DataType{Dtype: DtString, TypeName: "string"}
	// B2Bytes []byte结构体
	B2Bytes = DataType{Dtype: DtBytes, TypeName: "bytes"}
	// B2Timestamp time结构体
	B2Timestamp = DataType{Dtype: DtTimestamp, TypeName: "timestamp"}
)

// NameAsType 通过类型名称获取类型结构体
func NameAsType(typeName string) (*DataType, error) {
	switch typeName {
	case B2Int32.TypeName:
		return &B2Int32, nil
	case B2Int64.TypeName:
		return &B2Int64, nil
	case B2Float32.TypeName:
		return &B2Float32, nil
	case B2Float64.TypeName:
		return &B2Float64, nil
	case B2String.TypeName:
		return &B2String, nil
	case B2Bytes.TypeName:
		return &B2Bytes, nil
	case B2Timestamp.TypeName:
		return &B2Timestamp, nil
	default:
		return nil, errors.New("no such type name")
	}
}

// DtAsType 通过类型枚举值获取类型结构体
func DtAsType(dataType int) (*DataType, error) {
	switch dataType {
	case B2Int32.Dtype:
		return &B2Int32, nil
	case B2Int64.Dtype:
		return &B2Int64, nil
	case B2Float32.Dtype:
		return &B2Float32, nil
	case B2Float64.Dtype:
		return &B2Float64, nil
	case B2String.Dtype:
		return &B2String, nil
	case B2Bytes.Dtype:
		return &B2Bytes, nil
	case B2Timestamp.Dtype:
		return &B2Timestamp, nil
	default:
		return nil, errors.New("no such data type")
	}
}
