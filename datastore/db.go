package datastore

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

const outFileName = "segment-"

// 10 MB = 10000000 Bytes (in decimal)
// 10 MB = 10485760 Bytes (in binary)
const outFileSize int64 = 10000000

type Db struct {
	blocks []*block
	//директорія, де зберігатимуться всі сегменти
	dir           string
	segmentName   string
	segmentNumber int
	segmentSize   int64
}

func NewDb(dir string) (*Db, error) {
	db := &Db{
		dir:         dir,
		segmentName: outFileName,
		segmentSize: outFileSize,
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	filesNames, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	//якщо директорія не порожня -> викликаємо рекавер
	if len(filesNames) != 0 {
		err := db.recover(filesNames)
		if err != nil {
			return nil, err
		}
	} else {
		// директорія порожня -> створюємо перший блок
		err = db.addNewBlockToDb()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *Db) addNewBlockToDb() error {
	db.segmentNumber++
	b, err := newBlock(db.dir,
		db.segmentName+strconv.Itoa((db.segmentNumber)))
	if err != nil {
		return err
	}
	db.blocks = append(db.blocks, b)
	return nil
}

func (db *Db) recover(filesNames []string) error {
	//сортуємо за зростанням
	sort.Strings(filesNames)
	//регексп для перевірки назв фалів
	r, _ := regexp.Compile(db.segmentName + "[0-9]+")
	for _, fileName := range filesNames {
		match := r.MatchString(fileName)

		if match {
			b, err := newBlock(db.dir, fileName)
			if err != nil {
				return err
			}
			db.blocks = append(db.blocks, b)
			reg, _ := regexp.Compile("[0-9]+")
			db.segmentNumber, err = strconv.Atoi(reg.FindString(fileName))
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("wrongly named file in the working directory: %v. Current file neme pattern: %v + int number", fileName, db.segmentName)
		}
	}
	return nil
}

func (db *Db) Close() error {
	for _, block := range db.blocks {
		block.close()
	}
	return nil
}

func (db *Db) getType(key string) (string, string, error) {
	var val, vType string
	var err error
	for j := len(db.blocks) - 1; j >= 0; j = j - 1 {
		val, vType, err = db.blocks[j].get(key)
		if err != nil && err != ErrNotFound {
			return "", "", err
		}
		if val != "" {
			return val, vType, nil
		}
	}
	return "", "", err
}

func (db *Db) putType(key, vType, value string) error {
	actBlock := db.blocks[len(db.blocks)-1]
	curSize, err := actBlock.size()
	if err != nil {
		return err
	}
	if curSize <= db.segmentSize {
		err := actBlock.put(key, vType, value)
		if err != nil {
			return err
		}
		return err
	}

	//якщо нема вже куди писати, то створюємо новий блок
	err = db.addNewBlockToDb()
	if err != nil {
		return err
	}
	err = db.blocks[len(db.blocks)-1].put(key, vType, value)
	if err != nil {
		return err
	}

	//запускаємо мердж, якщо достатньо файлів
	if len(db.blocks) > 2 {
		err = db.merge()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	val, vType, err := db.getType(key)
	if err != nil {
		return "", err
	}
	if vType != "string" {
		return "", fmt.Errorf("wrong type of value")
	}
	return val, nil
}

func (db *Db) Put(key, value string) error {
	err := db.putType(key, "string", value)
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) GetInt64(key string) (int64, error) {
	val, vType, err := db.getType(key)
	if err != nil {
		return 0, err
	}
	if vType != "int64" {
		return 0, fmt.Errorf("wrong type of value")
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (db *Db) PutInt64(key string, value int64) error {
	err := db.putType(key, "int64", strconv.FormatInt(value, 10))
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) merge() error {
	tempBlock, err := mergeAll(db.blocks[:len(db.blocks)-1])
	if err != nil {
		return err
	}

	//додаємо блок до масиву блоків
	db.blocks = append(db.blocks[:1], db.blocks[:]...)
	db.blocks[0] = tempBlock

	//видаляємо вже непотрібні блоки
	for _, block := range db.blocks[1 : len(db.blocks)-1] {
		err := block.delete()
		if err != nil {
			return err
		}
	}

	//видалимо рештки з масиву
	db.blocks = append(db.blocks[:1], db.blocks[len(db.blocks)-1])
	err = os.Rename(tempBlock.segment.Name(), filepath.Join(db.dir, db.segmentName+"0"))
	tempBlock.outPath = tempBlock.segment.Name()
	if err != nil {
		return err
	}
	return nil
}
