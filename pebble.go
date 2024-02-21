package db

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewPebbleDB(name, dir)
	}
	registerDBCreator(PebbleDBBackend, dbCreator)
}

// PebbleDB is a PebbleDB backend.
type PebbleDB struct {
	db *pebble.DB
}

var _ DB = (*PebbleDB)(nil)

func NewPebbleDB(name string, dir string) (*PebbleDB, error) {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	return NewPebbleDBWithOpts(name, dir, opts)
}

func NewPebbleDBWithOpts(name string, dir string, opts *pebble.Options) (*PebbleDB, error) {
	// configure pebble
	opts.MemTableSize = 256 << 20           // 256 MB
	opts.Cache = pebble.NewCache(256 << 20) // 256 MB
	opts.LBaseMaxBytes = 64 << 20           // 64 MB
	dbPath := filepath.Join(dir, name+".db")
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
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return cp(res), nil
}

// Has implements DB.
func (db *PebbleDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}

	bytesPeb, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytesPeb != nil, nil
}

// Set implements DB.
func (db *PebbleDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	wopts := pebble.NoSync
	err := db.db.Set(key, value, wopts)
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

	wopts := pebble.NoSync
	err := db.db.Delete(key, wopts)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db *PebbleDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(key, pebble.Sync)
	if err != nil {
		return err
	}
	return nil
}

func (db *PebbleDB) DB() *pebble.DB {
	return db.db
}

func (db *PebbleDB) Compact(start, end []byte) (err error) {
	// Currently nil,nil is an invalid range in Pebble.
	// This was taken from https://github.com/cockroachdb/pebble/issues/1474
	// In case the start and end keys are the same
	// pebbleDB will throw an error that it cannot compact.
	if start != nil && end != nil {
		return db.db.Compact(start, end, true)
	}
	iter, err := db.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() {
		err2 := iter.Close()
		if err2 != nil {
			err = err2
		}
	}()
	if start == nil && iter.First() {
		start = append(start, iter.Key()...)
	}
	if end == nil && iter.Last() {
		end = append(end, iter.Key()...)
	}
	err = db.db.Compact(start, end, true)
	return err
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
	stats := make(map[string]string)
	metrics := db.db.Metrics()

	// Convert metrics to a map of strings
	stats["BlockCacheSize"] = fmt.Sprintf("%d", metrics.BlockCache.Size)
	stats["BlockCacheCount"] = fmt.Sprintf("%d", metrics.BlockCache.Count)
	stats["BlockCacheHits"] = fmt.Sprintf("%d", metrics.BlockCache.Hits)
	stats["BlockCacheMisses"] = fmt.Sprintf("%d", metrics.BlockCache.Misses)

	stats["MemTableSize"] = fmt.Sprintf("%d", metrics.MemTable.Size)
	stats["MemTableCount"] = fmt.Sprintf("%d", metrics.MemTable.Count)

	stats["TableCacheSize"] = fmt.Sprintf("%d", metrics.TableCache.Size)
	stats["TableCacheCount"] = fmt.Sprintf("%d", metrics.TableCache.Count)
	stats["TableCacheHits"] = fmt.Sprintf("%d", metrics.TableCache.Hits)
	stats["TableCacheMisses"] = fmt.Sprintf("%d", metrics.TableCache.Misses)

	// Add more metrics as needed

	return stats
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
	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}
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
	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}
	itr.Last()
	return newPebbleDBIterator(itr, start, end, true), nil
}

var _ Batch = (*pebbleDBBatch)(nil)

type pebbleDBBatch struct {
	db    *PebbleDB
	batch *pebble.Batch
}

var _ Batch = (*pebbleDBBatch)(nil)

func newPebbleDBBatch(db *PebbleDB) *pebbleDBBatch {
	return &pebbleDBBatch{
		// For regular batch operations batch.db is going to be set to db
		// and it is not needed to initialize the DB here.
		// This is set to enable general DB operations like compaction
		// (e.x. a call do pebbleDBBatch.db.Compact() would throw a nil pointer exception)
		db:    db,
		batch: db.db.NewBatch(),
	}
}

// Set implements Batch.
func (b *pebbleDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.batch == nil {
		return errBatchClosed
	}

	return b.batch.Set(key, value, nil)
}

// Delete implements Batch.
func (b *pebbleDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.batch == nil {
		return errBatchClosed
	}

	return b.batch.Delete(key, nil)
}

// Write implements Batch.
func (b *pebbleDBBatch) Write() error {
	if b.batch == nil {
		return errBatchClosed
	}

	wopts := pebble.NoSync
	err := b.batch.Commit(wopts)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.

	return b.Close()
}

// WriteSync implements Batch.
func (b *pebbleDBBatch) WriteSync() error {
	if b.batch == nil {
		return errBatchClosed
	}
	err := b.batch.Commit(pebble.Sync)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *pebbleDBBatch) Close() error {
	if b.batch != nil {
		err := b.batch.Close()
		if err != nil {
			return err
		}
		b.batch = nil
	}

	return nil
}

type pebbleDBIterator struct {
	source     *pebble.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

var _ Iterator = (*pebbleDBIterator)(nil)

func newPebbleDBIterator(source *pebble.Iterator, start, end []byte, isReverse bool) *pebbleDBIterator {
	if isReverse {
		if end == nil {
			source.Last()
		}
	} else {
		if start == nil {
			source.First()
		}
	}
	return &pebbleDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (itr *pebbleDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.

// Valid implements Iterator.
func (itr *pebbleDBIterator) Valid() bool {
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source has error, invalid.
	if err := itr.source.Error(); err != nil {
		itr.isInvalid = true
		return false
	}

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is out of bounds, invalid.
	key := itr.source.Key()
	if itr.isReverse {
		if itr.start != nil && bytes.Compare(key, itr.start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if itr.end != nil && bytes.Compare(itr.end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// It's valid.
	return true
}

// Key implements Iterator.
func (itr *pebbleDBIterator) Key() []byte {
	// Key returns a copy of the current key.
	// See https://github.com/cockroachdb/pebble/blob/v1.0.0/iterator.go#L2106
	itr.assertIsValid()
	return cp(itr.source.Key())
}

// Value implements Iterator.
func (itr *pebbleDBIterator) Value() []byte {
	// Value returns a copy of the current value.
	// See https://github.com/cockroachdb/pebble/blob/v1.0.0/iterator.go#L2116
	itr.assertIsValid()
	return cp(itr.source.Value())
}

// Next implements Iterator.
// Next implements Iterator.
func (itr *pebbleDBIterator) Next() {
	// Before moving the iterator, we need to check if it's valid.
	if !itr.Valid() {
		panic("iterator is invalid")
	}

	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}

	// After moving the iterator, we need to check if it's still valid.
	if !itr.source.Valid() {
		itr.isInvalid = true
	}
}

// Error implements Iterator.
func (itr *pebbleDBIterator) Error() error {
	return itr.source.Error()
}

// Close implements Iterator.
func (itr *pebbleDBIterator) Close() error {
	err := itr.source.Close()
	if err != nil {
		return err
	}
	return nil
}

func (itr *pebbleDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
