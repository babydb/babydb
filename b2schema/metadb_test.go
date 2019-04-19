package b2schema

import (
	"os"
	"testing"
)

var meta *MetaDBSource
var db *B2Database

func TestMain(t *testing.M) {
	meta = OpenMetaConn()
	ret := t.Run()
	meta.Close()
	os.Exit(ret)
}

func TestNewDatabase(t *testing.T) {
	var err error
	db, err = NewDatabaseAndOpen("testDB", meta)
	if err != nil {
		t.Error("creating testDB failed")
		return
	}
	_, err = meta.GetDatabase("testDB")
	if err != nil {
		t.Error("get testDB from META failed")
		return
	}
}

func TestNewTable(t *testing.T) {
	cols := make([]B2Column, 2)
	cols[0] = *NewColumn("username", "string").Length(100).Index(true)
	cols[1] = *NewColumn("age", "int32")
	_, err := NewTable("testTable", cols, db, meta)
	if err != nil {
		t.Error("creating testDb.testTable failed")
	}
	_, err = db.GetTable("testTable", meta)
	if err != nil {
		t.Error("get testDB.testTable META failed")
	}
}

func TestDeleteTable(t *testing.T) {
	err := db.RemoveTable("testTable", meta)
	if err != nil {
		t.Error("deleting testDB.testTable failed")
	}
}

func TestDeleteDatabase(t *testing.T) {
	db.Close()
	err := DropDatabase("testDB", meta)
	// err = meta.DelDatabase("testDB")
	if err != nil {
		t.Error("Delete testDB failed")
	}
}
