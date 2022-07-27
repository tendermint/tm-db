//go:build pebbledb

package db

import "github.com/cockroachdb/pebble"

type pebbleDBBatch struct {
	db    *PebbleDB
	batch *pebble.Batch
}

var _ Batch = (*pebbleDBBatch)(nil)

// new PebbleDBBatch returns a new PebbleDBBatch.
func newPebbleDBBatch(db *PebbleDB) *pebbleDBBatch {
	return &pebbleDBBatch{
		batch: db.db.NewBatch(),
	}
}

// Set implements Batch.
// 1) we make sure that the key is not empty, and if it is we return errKeyEmpty.
// 2) Then we check if the value is nil, If the value is nil, we return an error.
// 3) Then we check if the batch is nil value, and return errBatchClosed if it is.
// 4) We set a new value in the batch.
// 5) We return nil.
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
	b.batch.Set(key, value, nil)
	return nil
}

// Delete implements Batch.
func (b *pebbleDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.batch == nil {
		return errBatchClosed
	}
	b.batch.Delete(key, nil)
	return nil
}

// Write implements Batch.
func (b *pebbleDBBatch) Write() error {
	if b.batch == nil {
		return errBatchClosed
	}
	err := b.batch.Commit(pebble.NoSync)
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
