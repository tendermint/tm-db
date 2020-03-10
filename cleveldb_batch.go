// +build cleveldb

package db

import "github.com/jmhodges/levigo"

// cLevelDBBatch is a LevelDB batch.
type cLevelDBBatch struct {
	db    *CLevelDB
	batch *levigo.WriteBatch
}

func newCLevelDBBatch(db *CLevelDB) *cLevelDBBatch {
	return &cLevelDBBatch{
		db:    db,
		batch: levigo.NewWriteBatch(),
	}
}

// Set implements Batch.
func (b *cLevelDBBatch) Set(key, value []byte) error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	b.batch.Put(key, value)
	return nil
}

// Delete implements Batch.
func (b *cLevelDBBatch) Delete(key []byte) error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	b.batch.Delete(key)
	return nil
}

// Write implements Batch.
func (b *cLevelDBBatch) Write() error {
	if b.batch == nil {
		return ErrBatchClosed
	}
	err := b.db.db.Write(b.db.wo, b.batch)
	if err != nil {
		return err
	}
	return b.Close()
}

// WriteSync implements Batch.
func (b *cLevelDBBatch) WriteSync() error {
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
func (b *cLevelDBBatch) Close() error {
	if b.batch != nil {
		b.batch.Close()
		b.batch = nil
	}
	return nil
}
