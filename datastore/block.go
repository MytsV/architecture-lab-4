package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type block struct {
	index   hashIndex
	segment *os.File

	outPath   string
	outOffset int64
}

func newBlock(dir string, outFileName string, outFileSize int64) (*block, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	bl := &block{
		index:   make(hashIndex),
		segment: f,

		outPath: outputPath,
	}
	err = bl.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return bl, nil
}

const bufSize = 8192

func (b *block) recover() error {
	input, err := os.Open(b.outPath)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			b.index[e.key] = b.outOffset
			b.outOffset += int64(n)
		}
	}
	return err
}

func (b *block) close() error {
	return b.segment.Close()
}

func (b *block) get(key string) (string, error) {
	position, ok := b.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(b.outPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (b *block) put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}

	n, err := b.segment.Write(e.Encode())
	if err == nil {
		b.index[key] = b.outOffset
		b.outOffset += int64(n)
	}
	return err
}

func (b *block) size() (int64, error) {
	info, err := os.Stat(b.outPath)
	if err != nil {
		return 0, err
	}
	currentSize := info.Size()
	return currentSize, nil
}

func compactAndMergeBlocksIntoOne(blocks []*block) (*block, error) {
	//видалити дуплікати e b2
	if len(blocks) == 0 {
		return nil, fmt.Errorf("empty array of blocks")
	}
	newBlock, err := newBlock(blocks[0].outPath+"-temp", "", 0)
	if err != nil {
		return nil, err
	}
	for j := len(blocks) - 1; j >= 0; j = j - 1 {
		err = merge2blocks(newBlock, blocks[j])
		if err != nil {
			return nil, err
		}
	}
	return newBlock, nil
}

func merge2blocks(destBlock, srcBlock *block) error {
	for key := range srcBlock.index {
		_, ok := destBlock.index[key]
		if !ok {
			val, err := srcBlock.get(key)
			if err != nil {
				return err
			}
			destBlock.put(key, val)
		}
	}
	return nil
}

func (b *block) delete() error {
	err := os.Remove(b.segment.Name())
	if err != nil {
		return err
	}
	return nil
}
