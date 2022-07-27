//go:build pebbledb

package db

import (
	"fmt"
	"log"

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

func (db *PebbleDB) DB() *pebble.DB {
	return db.db
}

// NB:  A lot of the code in this file is sourced from here: https://github.com/cockroachdb/pebble/blob/master/cmd/pebble/db.go
// NB: This was my best working commit from today July 27 2022: https://github.com/notional-labs/tm-db/tree/6a41b774b9c362cac7d22156fa1021780d801f8a

// NewPebbleDB makes and configures a new instance of PebbleDB.
func NewPebbleDB(name string, dir string) (DB, error) {

	// This is config that we could use later.  When it is enabled, we hit the error more rapidly and frequently as can be seen in this test run:

	/*
		cache := pebble.NewCache(1024 * 1024 * 32)
		defer cache.Unref()
		opts := &pebble.Options{
			Cache:                       cache,
			DisableWAL:                  false,
			FormatMajorVersion:          pebble.FormatNewest,
			L0CompactionThreshold:       2,
			L0StopWritesThreshold:       1000,
			LBaseMaxBytes:               64 << 20, // 64 MB
			Levels:                      make([]pebble.LevelOptions, 7),
			MaxConcurrentCompactions:    3,
			MaxOpenFiles:                16384, // lowering this value can cause the db to use less disk space, and this can matter when running the tests in github actions.
			MemTableSize:                64 << 20,
			MemTableStopWritesThreshold: 4,
		}

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
		opts.Levels[6].FilterPolicy = nil
		opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	*/

	opts := &pebble.Options{}
	opts.EnsureDefaults()

	p, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
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

// The stats section was contributed by ValNodes.  Toss e'm some stake!
// Stats implements DB.
func (db *PebbleDB) Stats() map[string]string {
	stats := make(map[string]string, 1)
	stats["Metrics"] = fmt.Sprint(db.db.Metrics())
	return stats
}

// NewBatch implements DB.
func (db *PebbleDB) NewBatch() Batch {
	return newPebbleDBBatch(db)
}

// NB For the reverse iterator and the iterator, this seems to make some sense: https://github.com/cockroachdb/pebble/blob/7b78c71e40558c8d6cc1c673b5075376609ff4ea/cmd/pebble/db.go#L120

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
