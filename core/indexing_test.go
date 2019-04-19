package core

import (
	"bytes"
	"testing"

	"github.com/rs/xid"

	"github.com/google/btree"
)

func TestIDIndexing(t *testing.T) {
	for i := 0; i < 10000000; i++ {
		a := IDIndex(xid.New().String())
		a.InsertOpIndexing("testTable")
	}
	// var b bytes.Buffer
	// IDIndice["testTable"].Ascend(tTraverse(&b))
	// fmt.Println(b.String())
}

func tTraverse(buf *bytes.Buffer) btree.ItemIterator {
	return func(i btree.Item) bool {
		buf.WriteString(string(i.(IDIndex)))
		return true
	}
}
