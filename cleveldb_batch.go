// +build cleveldb

package db

import "github.com/jmhodges/levigo"

// cLevelDBBatch is a LevelDB batch.
type cLevelDBBatch struct {
	db    *CLevelDB
	batch *levigo.WriteBatch
}

// Set implements Batch.
func (b *cLevelDBBatch) Set(key, value []byte) error {
	b.batch.Put(key, value)
	return nil
}

// Delete implements Batch.
func (b *cLevelDBBatch) Delete(key []byte) error {
	b.batch.Delete(key)
	return nil
}

// Write implements Batch.
func (b *cLevelDBBatch) Write() error {
	if err := b.db.db.Write(b.db.wo, b.batch); err != nil {
		return err
	}
	return nil
}

// WriteSync implements Batch.
func (b *cLevelDBBatch) WriteSync() error {
	if err := b.db.db.Write(b.db.woSync, b.batch); err != nil {
		return err
	}
	return nil
}

// Close implements Batch.
func (b *cLevelDBBatch) Close() error {
	b.batch.Close()
	return nil
}
