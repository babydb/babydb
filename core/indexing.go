package core

import (
	"bytes"
	"errors"
	"log"
	"encoding/binary"

	"github.com/google/btree"
	schema "github.com/babydb/babydb/b2schema"
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
	tree, ok := IDIndice[tableID]
	if !ok {
		tree = btree.New(64)
	}
	tree.ReplaceOrInsert(id)
	IDIndice[tableID] = tree
}

// InsertOpIndexing 插入数据时更新普通字段索引
func (a NormalIndex) InsertOpIndexing(indexID string) {
	tree, ok := NormalIndice[indexID]
	if !ok {
		tree = btree.New(64)
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
func (id IDIndex) Serialize(tableID string) ([]byte, error) {
	tree, ok := IDIndice[tableID]
	if !ok {
		return nil, errors.New("table ID not found")
	}
	var buf bytes.Buffer
	tree.Ascend(idTraverse(&buf))
	treeBytes := make([]byte, buf.Len())
	buf.Read(treeBytes)
	return treeBytes, nil
}

// Serialize 将普通字段索引的Btree序列化为byte数组
func (a NormalIndex) Serialize(indexID string) ([]byte, error) {
	tree, ok := NormalIndice[indexID]
	if !ok {
		return nil, errors.New("index ID not found")
	}
	var buf bytes.Buffer
	tree.Ascend(normalTraverse(&buf))
	treeBytes := make([]byte, buf.Len())
	buf.Read(treeBytes)
	return treeBytes, nil
}

// IdIndexDeserialize 将一个byte数组反序列化未一个ID索引
func IdIndexDeserialize(treeBytes []byte) (*btree.BTree, error) {
	bsLen := len(treeBytes)
	if bsLen == 0 {
		return nil, errors.New("empty bytes to deserialize")
	}
	tree := btree.New(64)
	for p:=0; p<bsLen; {
		hl, size := binary.Varint(treeBytes[p:])
		if size == 0 {
			log.Fatalf("读取Varint时发生错误，字节缓冲区长度不足\n")
			break
		}
		if size < 0 {
			log.Printf("读取到的Varint长度值超过64bit，该长度忽略")
			p += size
			continue
		}
		p += size
		if p+hl>bsLen {
			log.Fatalf("ID数据长度范围超出字节总长度范围: %d > %d\n", p+hl, bsLen)
			break
		}
		id := treeBytes[p: p+hl].(IDIndex)
		tree.ReplaceOrInsert(id)
		p += hl
	}
	return tree, nil
}

func NormalIndexDeserialize(treeBytes []byte) (*btree.BTree, error) {
	bsLen := len(treeBytes)
	if bsLen == 0 {
		return nil, errors.New("empty bytes to deserialize")
	}
	tree := btree.New(64)
	for p:=0; p<bsLen; {
		hl, size := binary.Varint(treeBytes[p:])
		if size == 0 {
			log.Fatalf("读取Varint时发生错误，字节缓冲区长度不足\n")
			break
		}
		if size < 0 {
			log.Printf("读取到的Varint长度值超过64bit，该长度忽略")
			p += size
			continue
		}
		
	}
}

func idTraverse(buf *bytes.Buffer) btree.ItemIterator {
	return func(i btree.Item) bool {
		lenBuf := make([]byte, 8)
		hl := binary.PutVarint(lenBuf, len(i.(IDIndex)))
		buf.Write(lenBuf[:hl])
		buf.Write(i.(IDIndex))
		return true
	}
}

func normalTraverse(buf *bytes.Buffer) btree.ItemIterator {
	return func(i btree.Item) bool {
		data, ok := i.(NormalIndex)
		if !ok {
			log.Fatalf("节点不是普通索引，btree类型错误。")
			return false
		}
		lenBuf := make([]byte, 8)
		var hl int
		switch data.Value.(type) {
		case int32:
			hl = binary.PutVarint(lenBuf, (int64) 4)
			buf.Write(lenBuf[:hl])
			buf.Write(schema.Int32ToBytes(data.Value.(int32)))
		case int64:
			hl = binary.PutVarint(lenBuf, (int64) 8)
			buf.Write(lenBuf[:hl])
			buf.Write(schema.Int64ToBytes(data.Value.(int64)))
		case float32:
			hl = binary.PutVarint(lenBuf, (int64) 4)
			buf.Write(lenBuf[:hl])
			buf.Write(schema.Float32ToBytes(data.Value.(float32)))
		case float64:
			hl = binary.PutVarint(lenBuf, (int64) 8)
			buf.Write(lenBuf[:hl])
			buf.Write(schema.Float64ToBytes(data.Value.(float64)))
		case string:
			hl = binary.PutVarint(lenBuf, (int64) len(([]byte) data.Value.(string)))
			buf.Write(lenBuf[:hl])
			buf.Write(([]byte)data.Value.(string))
		case []byte:
			hl = binary.PutVarint(lenBuf, (int64) len(data.Value.([]byte)))
			buf.Write(lenBuf[:hl])
			buf.Write(data.Value.([]byte))
		default:
			log.Printf("节点数据类型不可识别：%t\n", data.Value)
			return false
		}
		hl = binary.PutVarint(lenBuf, (int64) len(data.UID))
		buf.Write(lenBuf[:hl])
		for _, id := range data.UID {
			hl = binary.PutVarint(lenBuf, (int64) len(([]byte)id))
			buf.Write(lenBuf[:hl])
			buf.Write(([]byte)id)
		}
	}
}

