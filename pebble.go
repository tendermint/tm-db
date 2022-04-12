//go:build pebbledb

package db

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewPebbleDB(name, dir)
	}
	registerDBCreator(PebbleDBBackend, dbCreator, false)
}

// PebbleDB is a PebbleDB backend.
type PebbleDB struct {
	db *pebble.DB
}

var _ DB = (*PebbleDB)(nil)

func NewPebbleDB(name string, dir string) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")
	//	cache := pebble.NewCache(1024 * 1024 * 32)
	//	defer cache.Unref()
	opts := &pebble.Options{
		//		Cache:                       cache,
		//		FormatMajorVersion:          pebble.FormatNewest,
		//		L0CompactionThreshold:       2,
		//		L0StopWritesThreshold:       1000,
		//		LBaseMaxBytes:               64 << 20, // 64 MB
		//		Levels:                      make([]pebble.LevelOptions, 7),
		//		MaxConcurrentCompactions:    3,
		//		MaxOpenFiles:                1024,
		//		MemTableSize:                64 << 20,
		//		MemTableStopWritesThreshold: 4,
	}
	/*
		for i := 0; i < len(opts.Levels); i++ {
			l := &opts.Levels[i]
			l.BlockSize = 32 << 10       // 32 KB
			l.IndexBlockSize = 256 << 10 // 256 KB
			l.FilterPolicy = bloom.FilterPolicy(10)
			l.FilterType = pebble.TableFilter
			if i > 0 {
				l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
			}
			l.EnsureDefaults()
		}
	*/
	//	opts.Levels[6].FilterPolicy = nil
	//	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize

	opts.EnsureDefaults()

	p, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleDB{
		db: p,
	}, err
}

// Get implements DB.
func (db *PebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, closer, err := db.db.Get(key)
	if err != nil {
		return res, nil
	}
	closer.Close()
	return res, nil
}

// Has implements DB.
func (db *PebbleDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *PebbleDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.db.Set(key, value, pebble.NoSync)
	if err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *PebbleDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.db.Set(key, value, pebble.Sync)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *PebbleDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(key, pebble.NoSync)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db PebbleDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(key, pebble.Sync)
	if err != nil {
		return nil
	}
	return nil
}

func (db *PebbleDB) DB() *pebble.DB {
	return db.db
}

// Close implements DB.
func (db PebbleDB) Close() error {
	db.db.Close()
	return nil
}

// Print implements DB.
func (db *PebbleDB) Print() error {
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
func (db *PebbleDB) Stats() map[string]string {
	/*
		keys := []string{"rocksdb.stats"}
		stats := make(map[string]string, len(keys))
		for _, key := range keys {
			stats[key] = db.(key)
		}
	*/
	return nil
}

// NewBatch implements DB.
func (db *PebbleDB) NewBatch() Batch {
	return newPebbleDBBatch(db)
}

// Iterator implements DB.
func (db *PebbleDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	itr := db.db.NewIter(&o)
	itr.First()

	return newPebbleDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *PebbleDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	itr := db.db.NewIter(&o)
	itr.Last()
	return newPebbleDBIterator(itr, start, end, true), nil
}
