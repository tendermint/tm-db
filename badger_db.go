package db

import (
	"bufio"
	"io"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v2"
)

func init() {
	registerDBCreator(BadgerDBBackend, badgerDBCreator, true)
}

type Options badger.Options

func badgerDBCreator(dbName, dir string) (DB, error) {
	return NewBadgerDB(dbName, dir)
}

var (
	_KB = int64(1024)
	_MB = 1024 * _KB
	_GB = 1024 * _MB
)

// NewBadgerDB creates a Badger key-value store backed to the
// directory dir supplied. If dir does not exist, we create it.
func NewBadgerDB(dbName, dir string) (*BadgerDB, error) {
	// BadgerDB doesn't expose a way for us to
	// create  a DB with the user's supplied name.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	// // Arbitrary size given that at Tendermint
	// // we'll need huge KeyValue stores.
	opts.ValueLogFileSize = 1 * _GB
	// opts.SyncWrites = false
	opts.Dir = dir
	opts.ValueDir = dir

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
	var val []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

func (b *BadgerDB) Has(key []byte) (bool, error) {
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
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b *BadgerDB) SetSync(key, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b *BadgerDB) Delete(key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *BadgerDB) DeleteSync(key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *BadgerDB) Close() error {
	return b.db.Close()
}

func (b *BadgerDB) Fprint(w io.Writer) {
	// bIter := b.Iterator()
	// defer bIter.Release()

	// var bw *bufio.Writer
	// if bbw, ok := w.(*bufio.Writer); ok {
	// 	bw = bbw
	// } else {
	// 	bw = bufio.NewWriter(w)
	// }
	// defer bw.Flush()

	// i := uint64(0)
	// for bIter.rewind(); bIter.valid(); bIter.Next() {
	// 	k, v := bIter.kv()
	// 	fmt.Fprintf(bw, "[%X]:\t[%X]\n", k, v)
	// 	i += 1
	// 	if i%1024 == 0 {
	// 		bw.Flush()
	// 		i = 0
	// 	}
	// }
}

func (b *BadgerDB) Print() error {
	bw := bufio.NewWriter(os.Stdout)
	b.Fprint(bw)
	return nil
}

func (b *BadgerDB) Iterator(start, end []byte) (Iterator, error) {
	// dbIter := b.db.NewIterator(badger.IteratorOptions{
	// 	PrefetchValues: true,

	// 	// Arbitrary PrefetchSize
	// 	PrefetchSize: 10,
	// })
	// // Ensure that we are always at the zeroth item
	// dbIter.Rewind()
	return nil, nil
}

func (b *BadgerDB) ReverseIterator(start, end []byte) (Iterator, error) {
	return nil, nil
}

func (b *BadgerDB) IteratorPrefix(prefix []byte) (Iterator, error) {
	return b.Iterator(prefix, nil)
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
	// var buf *bytes.Buffer // It'll be lazily allocated when needed
	// for i, entry := range entries {
	// 	if err := entry.Error; err != nil {
	// 		if buf == nil {
	// 			buf = new(bytes.Buffer)
	// 		}
	// 		fmt.Fprintf(buf, "#%d: entry err: %v\n", i, err)
	// 	}
	// }
	// if buf != nil {
	// 	panic(string(buf.Bytes()))
	// }
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

	// var buf *bytes.Buffer // It'll be lazily allocated when needed
	// for i, entry := range entries {
	// 	if err := entry.Error; err != nil {
	// 		if buf == nil {
	// 			buf = new(bytes.Buffer)
	// 		}
	// 		fmt.Fprintf(buf, "#%d: entry err: %v\n", i, err)
	// 	}
	// }
	// if buf != nil {
	// 	panic(string(buf.Bytes()))
	// }
}

func (bb *badgerDBBatch) Close() error {
	return nil // TODO
}

type badgerDBIterator struct {
	mu sync.RWMutex

	iter *badger.Iterator
}
