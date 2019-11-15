// +build cleveldb

package db

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/jmhodges/levigo"
	"github.com/pkg/errors"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewCLevelDB(name, dir)
	}
	registerDBCreator(CLevelDBBackend, dbCreator, false)
}

var _ DB = (*CLevelDB)(nil)

type CLevelDB struct {
	db     *levigo.DB
	ro     *levigo.ReadOptions
	wo     *levigo.WriteOptions
	woSync *levigo.WriteOptions
}

func NewCLevelDB(name string, dir string) (*CLevelDB, error) {
	dbPath := filepath.Join(dir, name+".db")

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1 << 30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	woSync := levigo.NewWriteOptions()
	woSync.SetSync(true)
	database := &CLevelDB{
		db:     db,
		ro:     ro,
		wo:     wo,
		woSync: woSync,
	}
	return database, nil
}

// Implements DB.
func (db *CLevelDB) Get(key []byte) ([]byte, error) {
	key = nonNilBytes(key)
	res, err := db.db.Get(db.ro, key)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Implements DB.
func (db *CLevelDB) Has(key []byte) bool {
	bytes, err := db.Get(key)
	if err != nil {
		return false
	}
	return bytes != nil
}

// Implements DB.
func (db *CLevelDB) Set(key []byte, value []byte) error {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.wo, key, value)
	if err != nil {
		return err
	}
	return nil
}

// Implements DB.
func (db *CLevelDB) SetSync(key []byte, value []byte) error {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.woSync, key, value)
	if err != nil {
		return err
	}
	return nil
}

// Implements DB.
func (db *CLevelDB) Delete(key []byte) error {
	key = nonNilBytes(key)
	err := db.db.Delete(db.wo, key)
	if err != nil {
		return err
	}
	return nil
}

// Implements DB.
func (db *CLevelDB) DeleteSync(key []byte) error {
	key = nonNilBytes(key)
	err := db.db.Delete(db.woSync, key)
	if err != nil {
		return err
	}
	return nil
}

func (db *CLevelDB) DB() *levigo.DB {
	return db.db
}

// Implements DB.
func (db *CLevelDB) Close() error {
	db.db.Close()
	db.ro.Close()
	db.wo.Close()
	db.woSync.Close()
	return nil
}

// Implements DB.
func (db *CLevelDB) Print() error {
	itr := db.Iterator(nil, nil)
	defer itr.Close()
	var err error
	for ; itr.Valid(); err = itr.Next() {
		if err != nil {
			return err
		}
		key, err := itr.Key()
		if err != nil {
			return err
		}
		value, err := itr.Value()
		if err != nil {
			return err
		}
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Implements DB.
func (db *CLevelDB) Stats() map[string]string {
	keys := []string{
		"leveldb.aliveiters",
		"leveldb.alivesnaps",
		"leveldb.blockpool",
		"leveldb.cachedblock",
		"leveldb.num-files-at-level{n}",
		"leveldb.openedtables",
		"leveldb.sstables",
		"leveldb.stats",
	}

	stats := make(map[string]string, len(keys))
	for _, key := range keys {
		str := db.db.PropertyValue(key)
		stats[key] = str
	}
	return stats
}

//----------------------------------------
// Batch

// Implements DB.
func (db *CLevelDB) NewBatch() Batch {
	batch := levigo.NewWriteBatch()
	return &cLevelDBBatch{db, batch}
}

type cLevelDBBatch struct {
	db    *CLevelDB
	batch *levigo.WriteBatch
}

// Implements Batch.
func (mBatch *cLevelDBBatch) Set(key, value []byte) {
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *cLevelDBBatch) Delete(key []byte) {
	mBatch.batch.Delete(key)
}

// Implements Batch.
func (mBatch *cLevelDBBatch) Write() error {
	err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)
	if err != nil {
		return err
	}
	return nil
}

// Implements Batch.
func (mBatch *cLevelDBBatch) WriteSync() error {
	err := mBatch.db.db.Write(mBatch.db.woSync, mBatch.batch)
	if err != nil {
		return err
	}
	return nil
}

// Implements Batch.
func (mBatch *cLevelDBBatch) Close() {
	mBatch.batch.Close()
}

//----------------------------------------
// Iterator
// NOTE This is almost identical to db/go_level_db.Iterator
// Before creating a third version, refactor.

func (db *CLevelDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCLevelDBIterator(itr, start, end, false)
}

func (db *CLevelDB) ReverseIterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newCLevelDBIterator(itr, start, end, true)
}

var _ Iterator = (*cLevelDBIterator)(nil)

type cLevelDBIterator struct {
	source     *levigo.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

func newCLevelDBIterator(source *levigo.Iterator, start, end []byte, isReverse bool) *cLevelDBIterator {
	if isReverse {
		if end == nil {
			source.SeekToLast()
		} else {
			source.Seek(end)
			if source.Valid() {
				eoakey := source.Key() // end or after key
				if bytes.Compare(end, eoakey) <= 0 {
					source.Prev()
				}
			} else {
				source.SeekToLast()
			}
		}
	} else {
		if start == nil {
			source.SeekToFirst()
		} else {
			source.Seek(start)
		}
	}
	return &cLevelDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

func (itr cLevelDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr cLevelDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	itr.assertNoError()

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var start = itr.start
	var end = itr.end
	var key = itr.source.Key()
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// It's valid.
	return true
}

func (itr cLevelDBIterator) Key() ([]byte, error) {
	err := itr.assertNoError()
	if err != nil {
		return nil, err
	}
	err = itr.assertIsValid()
	if err != nil {
		return nil, err
	}
	return itr.source.Key(), nil
}

func (itr cLevelDBIterator) Value() ([]byte, error) {
	err := itr.assertNoError()
	if err != nil {
		return nil, err
	}
	err = itr.assertIsValid()
	if err != nil {
		return nil, err
	}
	return itr.source.Value(), nil
}

func (itr cLevelDBIterator) Next() error {
	err := itr.assertNoError()
	if err != nil {
		return err
	}
	err = itr.assertIsValid()
	if err != nil {
		return err
	}
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
	return nil
}

func (itr cLevelDBIterator) Close() {
	itr.source.Close()
}

func (itr cLevelDBIterator) assertNoError() error {
	if err := itr.source.GetError(); err != nil {
		return err
	}
	return nil
}

func (itr cLevelDBIterator) assertIsValid() error {
	if !itr.Valid() {
		return errors.New("cLevelDBIterator is invalid")
	}
	return nil
}
