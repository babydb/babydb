package core

import (
	"github.com/google/btree"
)

// IDIndex ID字段索引类型
type IDIndex string

// Less IDIndex实现btree Item接口
func (id IDIndex) Less(item btree.Item) bool {
	return id < item.(IDIndex)
}

// NormalIndex 普通字段索引类型
type NormalIndex struct {
	Value interface{}
	UID   string
}

// Less NormalIndex实现btree Item接口
func (a NormalIndex) Less(b btree.Item) bool {
	bi := b.(NormalIndex)
	switch bi.Value.(type) {
	case int32:
		return a.Value.(int32) < bi.Value.(int32)
	case int64:
		return a.Value.(int64) < bi.Value.(int64)
	case float32:
		return a.Value.(float32) < bi.Value.(float32)
	case float64:
		return a.Value.(float64) < bi.Value.(float64)
	case string:
		return a.Value.(string) < bi.Value.(string)
	case []byte:
		return byteLess(a.Value.([]byte), bi.Value.([]byte))
	}
	return false
}

func byteLess(a, b []byte) bool {
	var i int
	for i = 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	if i < len(b) {
		return true
	}
	return false
}
