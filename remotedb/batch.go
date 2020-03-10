package remotedb

import (
	"github.com/pkg/errors"

	db "github.com/tendermint/tm-db"
	protodb "github.com/tendermint/tm-db/remotedb/proto"
)

type batch struct {
	db  *RemoteDB
	ops []*protodb.Operation
}

var _ db.Batch = (*batch)(nil)

// Set implements Batch.
func (b *batch) Set(key, value []byte) error {
	op := &protodb.Operation{
		Entity: &protodb.Entity{Key: key, Value: value},
		Type:   protodb.Operation_SET,
	}
	b.ops = append(b.ops, op)
	return nil
}

// Delete implements Batch.
func (b *batch) Delete(key []byte) error {
	op := &protodb.Operation{
		Entity: &protodb.Entity{Key: key},
		Type:   protodb.Operation_DELETE,
	}
	b.ops = append(b.ops, op)
	return nil
}

// Write implements Batch.
func (b *batch) Write() error {
	if _, err := b.db.dc.BatchWrite(b.db.ctx, &protodb.Batch{Ops: b.ops}); err != nil {
		return errors.Errorf("remoteDB.BatchWrite: %v", err)
	}
	return nil
}

// WriteSync implements Batch.
func (b *batch) WriteSync() error {
	if _, err := b.db.dc.BatchWriteSync(b.db.ctx, &protodb.Batch{Ops: b.ops}); err != nil {
		return errors.Errorf("RemoteDB.BatchWriteSync: %v", err)
	}
	return nil
}

// Close implements Batch.
func (b *batch) Close() error {
	b.ops = nil
	return nil
}
