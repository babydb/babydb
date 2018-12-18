package schema

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
