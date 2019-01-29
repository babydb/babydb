package core

import (
	"bytes"

	"github.com/google/btree"
)

// IDIndex ID字段索引类型
type IDIndex []byte

// Less IDIndex实现btree Item接口
func (id IDIndex) Less(item btree.Item) bool {
	return byteLess(id, item.(IDIndex))
}

// NormalIndex 普通字段索引类型
type NormalIndex struct {
	Value interface{}
	UID   []string
}

// Index 索引的接口
type Index interface {
	Less(item btree.Item) bool
	InsertOpIndexing(ID string)
	DeleteOpIndexing(ID string)
	Serialize(ID string) []byte
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

// IDIndice row key ID的索引map
var IDIndice = make(map[string]*btree.BTree, 10)

// NormalIndice 普通字段的索引map
var NormalIndice = make(map[string]*btree.BTree, 10)

// InsertOpIndexing 插入数据时更新ID字段索引
func (id IDIndex) InsertOpIndexing(tableID string) {
	tree := IDIndice[tableID]
	if tree == nil {
		tree = btree.New(64)
	}
	tree.ReplaceOrInsert(id)
	IDIndice[tableID] = tree
}

// InsertOpIndexing 插入数据时更新普通字段索引
func (a NormalIndex) InsertOpIndexing(indexID string) {
	tree := NormalIndice[indexID]
	if tree == nil {
		tree = btree.New(16)
	}
	if item := tree.Get(a); item != nil {
		node := item.(*NormalIndex)
		node.UID = append(node.UID, a.UID...)
	} else {
		tree.ReplaceOrInsert(a)
	}
}

// DeleteOpIndexing 删除数据时更新ID字段索引
func (id IDIndex) DeleteOpIndexing(tableID string) {
	if IDIndice[tableID] == nil {
		return
	}
	IDIndice[tableID].Delete(id)
}

// DeleteOpIndexing 删除数据时跟新普通字段索引
func (a NormalIndex) DeleteOpIndexing(indexID string) {
	if NormalIndice[indexID] == nil {
		return
	}
	NormalIndice[indexID].Delete(a)
}

// Serialize 将ID索引的Btree序列化为byte数组
func (id IDIndex) Serialize(tableID string) []byte {
	tree, ok := IDIndice[tableID]
	if !ok {
		return nil
	}
	var buf bytes.Buffer
	tree.Ascend(traverse(&buf))
	treeBytes := make([]byte, buf.Len())
	buf.Read(treeBytes)
	return treeBytes
}

// Serialize 将普通字段索引的Btree序列化为byte数组
func (a NormalIndex) Serialize(indexID string) []byte {
	tree, ok := NormalIndice[indexID]
	if !ok {
		return nil
	}
	var buf bytes.Buffer
	tree.Ascend(traverse(&buf))
	treeBytes := make([]byte, buf.Len())
	buf.Read(treeBytes)
	return treeBytes
}

func traverse(buf *bytes.Buffer) btree.ItemIterator {
	return func(i btree.Item) bool {
		buf.Write(i.(IDIndex))
		return true
	}
}
