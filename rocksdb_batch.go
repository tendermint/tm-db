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
		batch: nil,
	}
}

func (b *rocksDBBatch) ensureBatch() {
	if b.batch == nil {
		b.batch = gorocksdb.NewWriteBatch()
	}
}

// Set implements Batch.
func (b *rocksDBBatch) Set(key, value []byte) {
	b.ensureBatch()
	b.batch.Put(key, value)
}

// Delete implements Batch.
func (b *rocksDBBatch) Delete(key []byte) {
	b.ensureBatch()
	b.batch.Delete(key)
}

// Write implements Batch.
func (b *rocksDBBatch) Write() error {
	if b.batch == nil {
		return nil
	}
	return b.db.db.Write(b.db.wo, b.batch)
}

// WriteSync mplements Batch.
func (b *rocksDBBatch) WriteSync() error {
	if b.batch == nil {
		return nil
	}
	return b.db.db.Write(b.db.woSync, b.batch)
}

// Close implements Batch.
func (b *rocksDBBatch) Close() {
	if b.batch != nil {
		b.batch.Destroy()
	}
	b.batch = nil
}
