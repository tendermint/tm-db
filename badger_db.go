package db

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v2"
)

func init() { registerDBCreator(BadgerDBBackend, badgerDBCreator, true) }

func badgerDBCreator(dbName, dir string) (DB, error) {
	return NewBadgerDB(dbName, dir)
}

// NewBadgerDB creates a Badger key-value store backed to the
// directory dir supplied. If dir does not exist, we create it.
func NewBadgerDB(dbName, dir string) (*BadgerDB, error) {
	// Since Badger doesn't support database names, we join both to obtain
	// the final directory to use for the database.
	path := filepath.Join(dir, dbName)

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(path)
	return NewBadgerDBWithOptions(opts)
}

// NewBadgerDBWithOptions creates a BadgerDB key value store
// gives the flexibility of initializing a database with the
// respective options.
func NewBadgerDBWithOptions(opts badger.Options) (*BadgerDB, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerDB{db: db}, nil
}

type BadgerDB struct {
	db *badger.DB
}

var _ DB = (*BadgerDB)(nil)

func (b *BadgerDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	var val []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err == nil && val == nil {
			val = []byte{}
		}
		return err
	})
	return val, err
}

func (b *BadgerDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	var found bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		found = (err != badger.ErrKeyNotFound)
		return nil
	})
	return found, err
}

func (b *BadgerDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b *BadgerDB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b *BadgerDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *BadgerDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *BadgerDB) Close() error {
	return b.db.Close()
}

func (b *BadgerDB) Print() error {
	return nil
}

func (b *BadgerDB) iteratorOpts(start, end []byte, opts badger.IteratorOptions) (*badgerDBIterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	txn := b.db.NewTransaction(false)
	iter := txn.NewIterator(opts)
	iter.Rewind()
	iter.Seek(start)
	if opts.Reverse && iter.Valid() && bytes.Equal(iter.Item().Key(), start) {
		// If we're going in reverse, our starting point was "end",
		// which is exclusive.
		iter.Next()
	}
	return &badgerDBIterator{
		reverse: opts.Reverse,
		start:   start,
		end:     end,

		txn:  txn,
		iter: iter,
	}, nil
}

func (b *BadgerDB) Iterator(start, end []byte) (Iterator, error) {
	opts := badger.DefaultIteratorOptions
	return b.iteratorOpts(start, end, opts)
}

func (b *BadgerDB) ReverseIterator(start, end []byte) (Iterator, error) {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	return b.iteratorOpts(end, start, opts)
}

func (b *BadgerDB) Stats() map[string]string {
	return nil
}

func (b *BadgerDB) NewBatch() Batch {
	return &badgerDBBatch{db: b}
}

var _ Batch = (*badgerDBBatch)(nil)

type badgerDBBatch struct {
	entriesMu sync.Mutex
	entries   []*badger.Entry

	db *BadgerDB
}

func (bb *badgerDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	bb.entriesMu.Lock()
	bb.entries = append(bb.entries, &badger.Entry{
		Key:   key,
		Value: value,
	})
	bb.entriesMu.Unlock()
	return nil
}

// Unfortunately Badger doesn't have a batch delete
// The closest that we can do is do a delete from the DB.
// Hesitant to do DeleteAsync because that changes the
// expected ordering
func (bb *badgerDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	// bb.db.Delete(key)
	return nil
}

// Write commits all batch sets to the DB
func (bb *badgerDBBatch) Write() error {
	bb.entriesMu.Lock()
	entries := bb.entries
	bb.entries = nil
	bb.entriesMu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	return bb.db.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
}

func (bb *badgerDBBatch) WriteSync() error {
	bb.entriesMu.Lock()
	entries := bb.entries
	bb.entries = nil
	bb.entriesMu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	return bb.db.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
}

func (bb *badgerDBBatch) Close() error {
	return nil // TODO
}

type badgerDBIterator struct {
	reverse    bool
	start, end []byte

	txn  *badger.Txn
	iter *badger.Iterator

	lastErr error
}

func (i *badgerDBIterator) Close() error {
	i.iter.Close()
	i.txn.Discard()
	return nil
}

func (i *badgerDBIterator) Domain() (start, end []byte) { return i.start, i.end }
func (i *badgerDBIterator) Error() error                { return i.lastErr }

func (i *badgerDBIterator) Next() {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	i.iter.Next()
}

func (i *badgerDBIterator) Valid() bool {
	if !i.iter.Valid() {
		return false
	}
	if len(i.end) > 0 {
		key := i.iter.Item().Key()
		if c := bytes.Compare(key, i.end); (!i.reverse && c >= 0) || (i.reverse && c < 0) {
			// We're at the end key, or past the end.
			return false
		}
	}
	return true
}

func (i *badgerDBIterator) Key() []byte {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	// Note that we don't use KeyCopy, so this is only valid until the next
	// call to Next.
	return i.iter.Item().KeyCopy(nil)
}

func (i *badgerDBIterator) Value() []byte {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	val, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		i.lastErr = err
	}
	return val
}
