package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", ToByte("string"), "value"}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{"key", ToByte("string"), "test-value"}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v.value != "test-value" {
		t.Errorf("Got bad value [%s]", v)
	}
	if v.vType != "string" {
		t.Errorf("Got bad value type [%s]", v)
	}
}

func TestReadValueInt64(t *testing.T) {
	e := entry{"key", ToByte("int64"), "-12"}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v.value != e.value {
		t.Errorf("Got bad value [%s]", v)
	}
	if v.vType != "int64" {
		t.Errorf("Got bad value type [%s]", v)
	}
}
