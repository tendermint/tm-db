// +build rocksdb

package db

import "github.com/tecbot/gorocksdb"

type rocksDBBatch struct {
	db    *RocksDB
	batch *gorocksdb.WriteBatch
}

var _ Batch = (*rocksDBBatch)(nil)

// Set implements Batch.
func (mBatch *rocksDBBatch) Set(key, value []byte) error {
	mBatch.batch.Put(key, value)
	return nil
}

// Delete implements Batch.
func (mBatch *rocksDBBatch) Delete(key []byte) error {
	mBatch.batch.Delete(key)
	return nil
}

// Write implements Batch.
func (mBatch *rocksDBBatch) Write() error {
	return mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)
}

// WriteSync mplements Batch.
func (mBatch *rocksDBBatch) WriteSync() error {
	return mBatch.db.db.Write(mBatch.db.woSync, mBatch.batch)
}

// Close implements Batch.
func (mBatch *rocksDBBatch) Close() error {
	mBatch.batch.Destroy()
	return nil
}
