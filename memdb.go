package db

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/tidwall/btree"
)

func init() {
	registerDBCreator(MemDBBackend, func(name, dir string) (DB, error) {
		return NewMemDB(), nil
	}, false)
}

// item is a btree.Item with byte slices as keys and values
type item struct {
	key   []byte
	value []byte
}

type itemRef *item

func less(a, b itemRef) bool {
	return bytes.Compare((a.key), b.key) == -1
}

// newKey creates a new key item.
func newKey(key []byte) itemRef {
	return &item{key: key}
}

// newPair creates a new pair item.
func newPair(key, value []byte) itemRef {
	return &item{key: key, value: value}
}

// MemDB is an in-memory database backend using a B-tree for storage.
//
// For performance reasons, all given and returned keys and values are pointers to the in-memory
// database, so modifying them will cause the stored values to be modified as well. All DB methods
// already specify that keys and values should be considered read-only, but this is especially
// important with MemDB.
type MemDB struct {
	mtx   sync.RWMutex
	btree *btree.Generic[itemRef]
}

var _ DB = (*MemDB)(nil)

// NewMemDB creates a new in-memory database.
func NewMemDB() *MemDB {
	database := &MemDB{
		btree: btree.NewGeneric[itemRef](less),
	}

	return database
}

// Get implements DB.
func (db *MemDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	i, found := db.btree.Get(newKey(key))
	if found {
		return i.value, nil
	}
	return nil, nil
}

// Has implements DB.
func (db *MemDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}

	_, found := db.btree.Get(newKey(key))

	return found, nil
}

// Set implements DB.
func (db *MemDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	db.set(key, value)
	return nil
}

// set sets a value without locking the mutex.
func (db *MemDB) set(key []byte, value []byte) {
	db.btree.Set(newPair(key, value))
}

// SetSync implements DB.
func (db *MemDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Delete implements DB.
func (db *MemDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.delete(key)
	return nil
}

// delete deletes a key without locking the mutex.
func (db *MemDB) delete(key []byte) {
	db.btree.Delete(newKey(key))
}

// DeleteSync implements DB.
func (db *MemDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Close implements DB.
func (db *MemDB) Close() error {
	// Close is a noop since for an in-memory database, we don't have a destination to flush
	// contents to nor do we want any data loss on invoking Close().
	// See the discussion in https://github.com/tendermint/tendermint/libs/pull/56
	return nil
}

// Print implements DB.
func (db *MemDB) Print() error {
	db.btree.Ascend(nil, func(i itemRef) bool {
		pitem := i
		fmt.Printf("[%X]:\t[%X]\n", pitem.key, pitem.value)
		return true
	})
	return nil
}

// Stats implements DB.
func (db *MemDB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["database.type"] = "memDB"
	stats["database.size"] = fmt.Sprintf("%d", db.btree.Len())
	return stats
}

// NewBatch implements DB.
func (db *MemDB) NewBatch() Batch {
	return newMemDBBatch(db)
}

// Iterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *MemDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newMemDBIterator(db, start, end, false), nil
}

// ReverseIterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *MemDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newMemDBIterator(db, start, end, true), nil
}

// IteratorNoMtx makes an iterator with no mutex.
func (db *MemDB) IteratorNoMtx(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newMemDBIteratorMtxChoice(db, start, end, false, false), nil
}

// ReverseIteratorNoMtx makes an iterator with no mutex.
func (db *MemDB) ReverseIteratorNoMtx(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newMemDBIteratorMtxChoice(db, start, end, true, false), nil
}
