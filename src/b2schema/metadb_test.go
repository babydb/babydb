package b2schema

import (
	"fmt"
	"os"
	"testing"
)

var meta *MetaDBSource

func TestMain(t *testing.M) {
	meta = OpenMetaConn()
	ret := t.Run()
	meta.Close()
	os.Exit(ret)
}

func TestDeleteDatabase(t *testing.T) {
	err := meta.DelDatabase("testDB")
	if err != nil {
		t.Errorf("Delete testDB failed")
	}
}
func TestNewDatabase(t *testing.T) {
	_, err := NewDatabaseAndOpen("testDB", meta)
	if err != nil {
		t.Errorf("creating testDB failed")
		return
	}
	b2db, err := meta.GetDatabase("testDB")
	if err != nil {
		t.Errorf("get testDB from META failed")
		return
	}
	fmt.Println(b2db)
}
