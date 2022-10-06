//go:build mdbx
// +build mdbx

package db

import "github.com/torquem-ch/mdbx-go/mdbx"

// MDBXBatch stores operations internally and dumps them to MDBX on Write().
type MDBXBatch struct {
	db  *MDBX
	ops []operation
}

var _ Batch = (*MDBXBatch)(nil)

func newMDBXBatch(db *MDBX) *MDBXBatch {
	return &MDBXBatch{
		db:  db,
		ops: []operation{},
	}
}

// Set implements Batch.
func (b *MDBXBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.ops = append(b.ops, operation{opTypeSet, key, value})
	return nil
}

// Delete implements Batch.
func (b *MDBXBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.ops == nil {
		return errBatchClosed
	}
	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
	return nil
}

// Write implements Batch.
func (b *MDBXBatch) Write() error {
	if b.ops == nil {
		return errBatchClosed
	}
	err := b.db.Env.Update(func(txn *mdbx.Txn) error {
		for _, op := range b.ops {
			switch op.opType {
			case opTypeSet:
				if err := txn.Put(b.db.DBI, op.key, op.value, 0); err != nil {
					return err
				}
			case opTypeDelete:
				if err := txn.Del(b.db.DBI, op.key, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// WriteSync implements Batch.
func (b *MDBXBatch) WriteSync() error {
	return b.Write()
}

// Close implements Batch.
func (b *MDBXBatch) Close() error {
	b.ops = nil
	return nil
}
