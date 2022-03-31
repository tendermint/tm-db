//go:build pebble
// +build pebble

package db

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewPebble(name, dir)
	}
	registerDBCreator(PebbleBackend, dbCreator, false)
}

// Pebble is a Pebble backend.
type Pebble struct {
	db *pebble.DB
}

var _ DB = (*Pebble)(nil)

func NewPebble(name string, dir string) (*pebble.DB, error) {
	// default rocksdb option, good enough for most cases, including heavy workloads.
	// 1GB table cache, 512MB write buffer(may use 50% more on heavy workloads).
	// compression: snappy as default, need to -lsnappy to enable.

	chicken := pebble.Options{
		MaxOpenFiles: 4096,
	}
	// SetMaxOpenFiles to 4096 seems to provide a reliable performance boost

	return NewPebbleWithOptions(name, dir, &chicken)
}

func NewPebbleWithOptions(name string, dir string, opts *pebble.Options) (*pebble.DB, error) {
	dbPath := filepath.Join(dir, name+".db")
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Get implements DB.
func (db *Pebble) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	return moveSliceToBytes(res), nil
}

// Has implements DB.
func (db *Pebble) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *Pebble) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *Pebble) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.SetSync(key, value)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *Pebble) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db *Pebble) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.DeleteSync(key)
	if err != nil {
		return nil
	}
	return nil
}

func (db *Pebble) DB() *pebble.DB {
	return db.db
}

// Close implements DB.
func (db *Pebble) Close() error {
	db.Close()
	return nil
}

// Print implements DB.
func (db *Pebble) Print() error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *Pebble) Stats() map[string]string {
	keys := []string{"rocksdb.stats"}
	stats := make(map[string]string, len(keys))
	for _, key := range keys {
		stats[key] = db.(key)
	}
	return stats
}

// NewBatch implements DB.
func (db *Pebble) NewBatch() Batch {
	return newPebbleBatch(db)
}

// Iterator implements DB.
func (db *Pebble) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr := db.NewIterator()
	return newPebbleIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *Pebble) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr := db.NewIterator()
	return newPebbleIterator(itr, start, end, true), nil
}
