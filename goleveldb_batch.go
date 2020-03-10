package db

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type goLevelDBBatch struct {
	db    *GoLevelDB
	batch *leveldb.Batch
}

var _ Batch = (*goLevelDBBatch)(nil)

func newGoLevelDBBatch(db *GoLevelDB) *goLevelDBBatch {
	return &goLevelDBBatch{
		db:    db,
		batch: new(leveldb.Batch),
	}
}

// Set implements Batch.
func (b *goLevelDBBatch) Set(key, value []byte) error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	b.batch.Put(key, value)
	return nil
}

// Delete implements Batch.
func (b *goLevelDBBatch) Delete(key []byte) error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	b.batch.Delete(key)
	return nil
}

// Write implements Batch.
func (b *goLevelDBBatch) Write() error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	err := b.db.db.Write(b.batch, &opt.WriteOptions{Sync: false})
	if err != nil {
		return err
	}
	return b.Close()
}

// WriteSync implements Batch.
func (b *goLevelDBBatch) WriteSync() error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	err := b.db.db.Write(b.batch, &opt.WriteOptions{Sync: true})
	if err != nil {
		return err
	}
	return b.Close()
}

// Close implements Batch.
func (b *goLevelDBBatch) Close() error {
	if b.batch != nil {
		b.batch.Reset()
		b.batch = nil
	}
	return nil
}
