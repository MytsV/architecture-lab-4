package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", "string", "value"}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	e := entry{"key", "string", "test-value"}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "test-value" {
		t.Errorf("Got bad value [%s]", v)
	}
}

func TestReadType(t *testing.T) {
	e := entry{"key", "int64", "test-value"}
	data := e.Encode()
	vt, err := readType(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if vt != "int64" {
		t.Errorf("Got bad value type [%s]", vt)
	}
}
