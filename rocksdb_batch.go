// +build rocksdb

package db

import "github.com/tecbot/gorocksdb"

type rocksDBBatch struct {
	db    *RocksDB
	batch *gorocksdb.WriteBatch
}

var _ Batch = (*rocksDBBatch)(nil)

func newRocksDBBatch(db *RocksDB) *rocksDBBatch {
	return &rocksDBBatch{
		db:    db,
		batch: gorocksdb.NewWriteBatch(),
	}
}

// Set implements Batch.
func (b *rocksDBBatch) Set(key, value []byte) error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	b.batch.Put(key, value)
	return nil
}

// Delete implements Batch.
func (b *rocksDBBatch) Delete(key []byte) error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	b.batch.Delete(key)
	return nil
}

// Write implements Batch.
func (b *rocksDBBatch) Write() error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	err := b.db.db.Write(b.db.wo, b.batch)
	if err != nil {
		return err
	}
	return b.Close()
}

// WriteSync mplements Batch.
func (b *rocksDBBatch) WriteSync() error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	err := b.db.db.Write(b.db.woSync, b.batch)
	if err != nil {
		return err
	}
	return b.Close()
}

// Close implements Batch.
func (b *rocksDBBatch) Close() error {
	if b.batch != nil {
		b.batch.Destroy()
		b.batch = nil
	}
	return nil
}
