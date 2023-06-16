package datastore

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type block struct {
	index   hashIndex
	segment *os.File

	outPath   string
	outOffset int64
	mu        sync.RWMutex

	writeCh chan writeArgument

	cancel context.CancelFunc
}

func newBlock(dir string, outFileName string) (*block, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	bl := &block{
		index:   make(hashIndex),
		segment: f,

		outPath: outputPath,
		writeCh: make(chan writeArgument),
	}
	ctx, cancel := context.WithCancel(context.Background())
	bl.cancel = cancel
	go bl.write(ctx)
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
	b.cancel()
	close(b.writeCh)
	return b.segment.Close()
}

func (b *block) get(key string) (string, string, error) {
	b.mu.RLock()
	position, ok := b.index[key]
	b.mu.RUnlock()
	if !ok {
		return "", "", ErrNotFound
	}

	file, err := os.Open(b.outPath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", "", err
	}

	reader := bufio.NewReader(file)
	pair, err := readValue(reader)
	if err != nil {
		return "", "", err
	}

	return pair.value, pair.vType, nil
}

func (b *block) put(key, vType, value string) error {
	e := entry{
		key:   key,
		vType: ToByte(vType),
		value: value,
	}

	resultCh := make(chan writeResult)
	b.writeCh <- writeArgument{resultCh, e.Encode()}
	result := <-resultCh
	close(resultCh)

	if result.err == nil {
		b.mu.Lock()
		b.index[key] = b.outOffset
		b.outOffset += int64(result.n)
		b.mu.Unlock()
	}

	return result.err
}

type writeArgument struct {
	resultCh chan writeResult
	data     []byte
}

type writeResult struct {
	n   int
	err error
}

func (b *block) write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case arg := <-b.writeCh:
			n, err := b.segment.Write(arg.data)
			arg.resultCh <- writeResult{n, err}
		}
	}
}

func (b *block) size() (int64, error) {
	info, err := os.Stat(b.outPath)
	if err != nil {
		return 0, err
	}
	currentSize := info.Size()
	return currentSize, nil
}

func mergeAll(blocks []*block) (*block, error) {
	if len(blocks) == 0 {
		return nil, fmt.Errorf("empty array of blocks")
	}
	newBlock, err := newBlock(blocks[0].outPath+"-temp", "")
	if err != nil {
		return nil, err
	}
	for j := len(blocks) - 1; j >= 0; j = j - 1 {
		err = mergePair(newBlock, blocks[j])
		if err != nil {
			return nil, err
		}
	}
	return newBlock, nil
}

func mergePair(destBlock, srcBlock *block) error {
	for key := range srcBlock.index {
		_, ok := destBlock.index[key]
		if !ok {
			val, vType, err := srcBlock.get(key)
			if err != nil {
				return err
			}
			destBlock.put(key, vType, val)
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
