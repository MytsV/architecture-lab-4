package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key   string
	vType string
	value string
}

var encoders map[string]func(*entry) []byte = map[string]func(*entry) []byte{
	"string": encodeString,
}

var decoders map[byte]func([]byte, *entry) = map[byte]func([]byte, *entry){
	0: decodeString,
}

var readers map[byte]func(in *bufio.Reader) (string, error) = map[byte]func(in *bufio.Reader) (string, error){
	0: readString,
}

var types map[byte]string = map[byte]string{
	0: "string",
}

const (
	TYPE_SIZE        = 1
	STRING_TYPE byte = 0
	INT64_TYPE  byte = 1
)

func (e *entry) Encode() []byte {
	encode := encoders[e.vType]
	return encode(e)
}

func encodeString(e *entry) []byte {
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

func decodeString(input []byte, e *entry) {
	kl := len(e.key)
	vl := binary.LittleEndian.Uint32(input[kl+TYPE_SIZE+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+TYPE_SIZE+12:kl+TYPE_SIZE+12+int(vl)])
	e.value = string(valBuf)
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	typeValue := input[kl+8]
	decode := decoders[typeValue]

	decode(input, e)
}

type pair struct {
	vType string
	value string
}

func readValue(in *bufio.Reader) (pair, error) {
	header, err := in.Peek(8)
	if err != nil {
		return pair{}, err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return pair{}, err
	}

	vType, err := in.Peek(1)
	if err != nil {
		return pair{}, err
	}
	_, err = in.Discard(1)
	if err != nil {
		return pair{}, err
	}

	read := readers[vType[0]]
	data, err := read(in)
	if err != nil {
		return pair{}, err
	}
	return pair{types[vType[0]], data}, nil
}

func readString(in *bufio.Reader) (string, error) {
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
