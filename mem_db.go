package db

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/pkg/errors"
)

const (
	// The approximate number of items and children per B-tree node. Tuned with benchmarks.
	bTreeDegree = 32

	// Size of the channel buffer between traversal goroutine and iterator. Using an unbuffered
	// channel causes two context switches per item sent, while buffering allows more work per
	// context switch. Tuned with benchmarks.
	chBufferSize = 64
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

// Less implements btree.Item.
func (i *item) Less(other btree.Item) bool {
	// this considers nil == []byte{}, but that's ok since we handle nil endpoints
	// in iterators specially anyway
	return bytes.Compare(i.key, other.(*item).key) == -1
}

// newKey creates a new key item
func newKey(key []byte) *item {
	return &item{key: nonNilBytes(key)}
}

// newPair creates a new pair item
func newPair(key, value []byte) *item {
	return &item{key: nonNilBytes(key), value: nonNilBytes(value)}
}

// MemDB is an in-memory database backend using a B-tree for storage.
type MemDB struct {
	mtx   sync.RWMutex
	btree *btree.BTree
}

var _ DB = (*MemDB)(nil)

// NewMemDB creates a new in-memory database.
func NewMemDB() *MemDB {
	database := &MemDB{
		btree: btree.New(bTreeDegree),
	}
	return database
}

// Implements DB.
func (db *MemDB) Get(key []byte) ([]byte, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	i := db.btree.Get(newKey(key))
	if i != nil {
		return i.(*item).value, nil
	}
	return nil, nil
}

// Implements DB.
func (db *MemDB) Has(key []byte) (bool, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	return db.btree.Has(newKey(key)), nil
}

// Implements DB.
func (db *MemDB) Set(key []byte, value []byte) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.set(key, value)
	return nil
}

func (db *MemDB) set(key []byte, value []byte) {
	db.btree.ReplaceOrInsert(newPair(key, value))
}

// Implements DB.
func (db *MemDB) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Implements DB.
func (db *MemDB) Delete(key []byte) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.delete(key)
	return nil
}

func (db *MemDB) delete(key []byte) {
	db.btree.Delete(newKey(key))
}

// Implements DB.
func (db *MemDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Implements DB.
func (db *MemDB) Close() error {
	// Close is a noop since for an in-memory database, we don't have a destination to flush
	// contents to nor do we want any data loss on invoking Close().
	// See the discussion in https://github.com/tendermint/tendermint/libs/pull/56
	return nil
}

// Implements DB.
func (db *MemDB) Print() error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.btree.Ascend(func(i btree.Item) bool {
		item := i.(*item)
		fmt.Printf("[%X]:\t[%X]\n", item.key, item.value)
		return true
	})
	return nil
}

// Implements DB.
func (db *MemDB) Stats() map[string]string {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	stats := make(map[string]string)
	stats["database.type"] = "memDB"
	stats["database.size"] = fmt.Sprintf("%d", db.btree.Len())
	return stats
}

// Implements DB.
func (db *MemDB) NewBatch() Batch {
	return &memBatch{db, nil}
}

// Implements DB.
func (db *MemDB) Iterator(start, end []byte) (Iterator, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return newMemDBIterator(db.btree, start, end, false), nil
}

// Implements DB.
func (db *MemDB) ReverseIterator(start, end []byte) (Iterator, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return newMemDBIterator(db.btree, start, end, true), nil
}

//----------------------------------------
// Iterator

// memDBIterator is an in-memory iterator
type memDBIterator struct {
	ch     <-chan *item
	cancel context.CancelFunc
	item   *item
	start  []byte
	end    []byte
}

var _ Iterator = (*memDBIterator)(nil)

// newMemDBIterator creates a new memDBIterator
func newMemDBIterator(bt *btree.BTree, start []byte, end []byte, reverse bool) *memDBIterator {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *item, chBufferSize)
	iter := &memDBIterator{
		ch:     ch,
		cancel: cancel,
		start:  start,
		end:    end,
	}

	go func() {
		// Because we use [start, end) for reverse ranges, while btree uses (start, end], we need
		// the following variables to handle some reverse iteration conditions ourselves.
		var (
			skipEqual     []byte
			abortLessThan []byte
		)
		visitor := func(i btree.Item) bool {
			item := i.(*item)
			if skipEqual != nil && bytes.Equal(item.key, skipEqual) {
				skipEqual = nil
				return true
			}
			if abortLessThan != nil && bytes.Compare(item.key, abortLessThan) == -1 {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case ch <- item:
				return true
			}
		}
		switch {
		case start == nil && end == nil && !reverse:
			bt.Ascend(visitor)
		case start == nil && end == nil && reverse:
			bt.Descend(visitor)
		case end == nil && !reverse:
			// must handle this specially, since nil is considered less than anything else
			bt.AscendGreaterOrEqual(newKey(start), visitor)
		case !reverse:
			bt.AscendRange(newKey(start), newKey(end), visitor)
		case end == nil:
			// abort after start, since we use [start, end) while btree uses (start, end]
			abortLessThan = start
			bt.Descend(visitor)
		default:
			// skip end and abort after start, since we use [start, end) while btree uses (start, end]
			skipEqual = end
			abortLessThan = start
			bt.DescendLessOrEqual(newKey(end), visitor)
		}
		close(ch)
	}()

	// prime the iterator with the first value, if any
	if item, ok := <-ch; ok {
		iter.item = item
	}

	return iter
}

// Close implements Iterator.
func (i *memDBIterator) Close() {
	i.cancel()
	for range i.ch { // drain channel
	}
	i.item = nil
}

// Domain implements Iterator.
func (i *memDBIterator) Domain() ([]byte, []byte) {
	return i.start, i.end
}

// Valid implements Iterator.
func (i *memDBIterator) Valid() bool {
	return i.item != nil
}

// Next implements Iterator.
func (i *memDBIterator) Next() {
	item, ok := <-i.ch
	switch {
	case ok:
		i.item = item
	case i.item == nil:
		panic("called Next() on invalid iterator")
	default:
		i.item = nil
	}
}

// Error implements Iterator.
func (i *memDBIterator) Error() error {
	return nil // famous last words
}

// Key implements Iterator.
func (i *memDBIterator) Key() []byte {
	if i.item == nil {
		panic("called Key() on invalid iterator")
	}
	return i.item.key
}

// Value implements Iterator.
func (i *memDBIterator) Value() []byte {
	if i.item == nil {
		panic("called Value() on invalid iterator")
	}
	return i.item.value
}

// memBatch operations
type opType int

const (
	opTypeSet opType = iota
	opTypeDelete
)

type operation struct {
	opType
	key   []byte
	value []byte
}

// memBatch handles in-memory batching
type memBatch struct {
	db  *MemDB
	ops []operation
}

// Set implements Batch.
func (mBatch *memBatch) Set(key, value []byte) {
	mBatch.ops = append(mBatch.ops, operation{opTypeSet, key, value})
}

// Delete implements Batch.
func (mBatch *memBatch) Delete(key []byte) {
	mBatch.ops = append(mBatch.ops, operation{opTypeDelete, key, nil})
}

// Write implements Batch.
func (mBatch *memBatch) Write() error {
	mBatch.db.mtx.Lock()
	defer mBatch.db.mtx.Unlock()

	for _, op := range mBatch.ops {
		switch op.opType {
		case opTypeSet:
			mBatch.db.set(op.key, op.value)
		case opTypeDelete:
			mBatch.db.delete(op.key)
		default:
			return errors.Errorf("unknown operation %T", op)
		}
	}
	return nil
}

// WriteSync implements Batch.
func (mBatch *memBatch) WriteSync() error {
	return mBatch.Write()
}

// Close implements Batch.
func (mBatch *memBatch) Close() {
	mBatch.ops = nil
}
