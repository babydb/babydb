package b2schema

import "testing"

func TestCasting(t *testing.T) {
	int32Col := B2Column{
		ColumnName: "an_int32_col",
		DataType:   "int32",
	}
	i32bytes := int32ToBytes(1234)
	conv, _ := int32Col.ParseInt32(i32bytes)
	if conv != 1234 {
		t.Errorf("%d casting failed\n", 1234)
	}
	int64Col := B2Column{
		ColumnName: "an_int64_col",
		DataType:   "int64",
	}
	i64bytes := int64ToBytes(123456789012345)
	conv1, _ := int64Col.ParseInt64(i64bytes)
	if conv1 != 123456789012345 {
		t.Errorf("%d casting failed\n", 123456789012345)
	}
	float32Col := B2Column{
		DataType: "float32",
	}
	f32bytes := float32ToBytes(0.12345)
	conv2, _ := float32Col.ParseFloat32(f32bytes)
	if conv2 != 0.12345 {
		t.Errorf("%f casting failed\n", 0.12345)
	}
	float64Col := B2Column{
		DataType: "float64",
	}
	f64bytes := float64ToBytes(0.123457890123456789012345)
	conv3, _ := float64Col.ParseFloat64(f64bytes)
	if conv3 != 0.123457890123456789012345 {
		t.Errorf("%f casting failed\n", 0.123457890123456789012345)
	}
	stringCol := B2Column{
		DataType:   "string",
		DataLength: 11,
	}
	conv4, _ := stringCol.ParseString([]byte("hello world"))
	if conv4 != "hello world" {
		t.Errorf("hello world casting failed\n")
	}
}
