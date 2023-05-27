package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	const outFileSize int64 = 200

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, db.segmentName+strconv.Itoa((db.segmentNumber))))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	pairs2 := [][]string{
		{"keyA", "valueA"},
		{"keyB", "valueB"},
		{"keyC", "valueC"},
		{"keyD", "valueD"},
		{"keyA", "newA"},
		{"keyB", "newB"},
		{"keyC", "newC"},
	}
	t.Run("create new out file, when previous file approximately reached expected size", func(t *testing.T) {
		db.segmentSize = outFileSize
		for _, pair := range pairs2 {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}

		f, err := os.Open(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer f.Close()
		filesNames, err := f.Readdirnames(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		n := len(filesNames)
		if n != 2 {
			t.Errorf("Expected 2 files in the directory, got %v", n)
		}
	})

	t.Run("get, if db has more than one files, ", func(t *testing.T) {
		value, err := db.Get(pairs2[5][0])
		if err != nil {
			t.Errorf("Cannot get %s: %s", pairs2[5], err)
		}
		if value != pairs2[5][1] {
			t.Errorf("Bad value returned expected %s, got %s", pairs2[5], value)
		}

		value, err = db.Get(pairs[0][0])
		if err != nil {
			t.Errorf("Cannot get %s: %s", pairs2[5], err)
		}
		if value != pairs[0][1] {
			t.Errorf("Bad value returned expected %s, got %s", pairs[0], value)
		}
	})

	t.Run("merge", func(t *testing.T) {
		for _, pair := range pairs2 {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		for _, pair := range pairs2 {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}

		f, err := os.Open(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer f.Close()
		filesNames, err := f.Readdirnames(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		n := len(filesNames)
		if n != 2 {
			t.Errorf("Expected 2 files in the directory, got %v", n)
		}
	})
}
