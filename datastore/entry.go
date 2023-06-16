package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key   string
	vType byte
	value string
}

type typeOperator interface {
	Encode(*entry) []byte
	Decode([]byte, *entry)
	Read(*bufio.Reader) (string, error)
}

type stringOperator struct{}

func (s stringOperator) Encode(e *entry) []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + TYPE_SIZE + vl + 12
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	res[kl+8] = STRING_TYPE
	binary.LittleEndian.PutUint32(res[kl+TYPE_SIZE+8:], uint32(vl))
	copy(res[kl+TYPE_SIZE+12:], e.value)
	return res
}

func (s stringOperator) Decode(input []byte, e *entry) {
	kl := len(e.key)
	vl := binary.LittleEndian.Uint32(input[kl+TYPE_SIZE+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+TYPE_SIZE+12:kl+TYPE_SIZE+12+int(vl)])
	e.value = string(valBuf)
}

func (s stringOperator) Read(in *bufio.Reader) (string, error) {
	header, err := in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), nil
}

var typeToByte map[string]byte = map[string]byte{
	"string": STRING_TYPE,
	"int64":  INT64_TYPE,
}

func ToByte(vType string) byte {
	return typeToByte[vType]
}

func ToType(value byte) string {
	for k, v := range typeToByte {
		if v == value {
			return k
		}
	}
	return ""
}

var operators map[byte]typeOperator = map[byte]typeOperator{
	0: stringOperator{},
}

const (
	TYPE_SIZE        = 1
	STRING_TYPE byte = 0
	INT64_TYPE  byte = 1
)

func (e *entry) Encode() []byte {
	operator := operators[e.vType]
	return operator.Encode(e)
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	typeValue := input[kl+8]
	operator := operators[typeValue]

	operator.Decode(input, e)
}

type output struct {
	vType string
	value string
}

func readValue(in *bufio.Reader) (output, error) {
	header, err := in.Peek(8)
	if err != nil {
		return output{}, err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return output{}, err
	}

	vType, err := in.Peek(1)
	if err != nil {
		return output{}, err
	}
	_, err = in.Discard(1)
	if err != nil {
		return output{}, err
	}

	operator := operators[vType[0]]
	data, err := operator.Read(in)
	if err != nil {
		return output{}, err
	}
	return output{ToType(vType[0]), data}, nil
}
