//go:build pebble
// +build pebble

package db

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
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

func newPebble(dir string) DB {
	cache := pebble.NewCache(1024 * 1024 * 1024)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		Comparer:                    mvccComparer,
		DisableWAL:                  disableWAL,
		FormatMajorVersion:          pebble.FormatNewest,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxConcurrentCompactions:    3,
		MaxOpenFiles:                16384,
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

	opts.EnsureDefaults()

	if verbose {
		opts.EventListener = pebble.MakeLoggingEventListener(nil)
		opts.EventListener.TableDeleted = nil
		opts.EventListener.TableIngested = nil
		opts.EventListener.WALCreated = nil
		opts.EventListener.WALDeleted = nil
	}

	p, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	return pebbleDB{
		d:       p,
		ballast: make([]byte, 1<<30),
	}
}

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
