//go:build mdbx
// +build mdbx

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/c2h5oh/datasize"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

func init() {
	registerDBCreator(MDBXBackend, NewMDBX, false)
}

type MDBX struct {
	Env *mdbx.Env
	DBI mdbx.DBI
}

var _ DB = (*MDBX)(nil)

func NewMDBX(name string, dir string) (DB, error) {
	path := filepath.Join(dir, name) + ".db"
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, err
	}
	env.SetGeometry(-1, -1, int(3*datasize.TB), int(2*datasize.GB), -1, int(DefaultPageSize()))
	if err := env.Open(path, 0, 0644); err != nil {
		return nil, err
	}
	var dbi mdbx.DBI
	if err := env.View(func(txn *mdbx.Txn) error {
		dbi, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &MDBX{Env: env, DBI: dbi}, nil
}

func (db *MDBX) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	var value []byte
	if err := db.Env.View(func(txn *mdbx.Txn) error {
		v, err := txn.Get(db.DBI, key)
		if err != nil {
			if mdbx.IsNotFound(err) {
				return nil
			}
			return err
		}
		value = v
		return nil
	}); err != nil {
		return nil, err
	}
	return value, nil
}

func (db *MDBX) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}

	result := false
	if err := db.Env.View(func(txn *mdbx.Txn) error {
		// zero-copy
		txn.RawRead = true

		itr, err := txn.OpenCursor(db.DBI)
		if err != nil {
			return err
		}
		defer itr.Close()

		_, _, err = itr.Get(key, nil, mdbx.Set)
		if err != nil {
			if mdbx.IsNotFound(err) {
				return nil
			}
			return err
		}
		result = true
		return nil
	}); err != nil {
		return false, err
	}
	return result, nil
}

func (db *MDBX) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	return db.Env.Update(func(txn *mdbx.Txn) error {
		return txn.Put(db.DBI, key, value, 0)
	})
}

func (db *MDBX) SetSync(key []byte, value []byte) error {
	if err := db.Set(key, value); err != nil {
		return err
	}
	return db.Env.Sync(true, false)
}

func (db *MDBX) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	err := db.Env.Update(func(txn *mdbx.Txn) error {
		return txn.Del(db.DBI, key, nil)
	})
	if err != nil && mdbx.IsNotFound(err) {
		return nil
	}
	return err
}

func (db *MDBX) DeleteSync(key []byte) error {
	if err := db.Delete(key); err != nil {
		return err
	}
	return db.Env.Sync(true, false)
}

func (db *MDBX) Close() error {
	db.Env.CloseDBI(db.DBI)
	db.Env.Close()
	return nil
}

func (db *MDBX) Print() error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

func (db *MDBX) Stats() map[string]string {
	stat, err := db.Env.Stat()
	if err != nil {
		return nil
	}
	return map[string]string{
		"mdbx.psize":          strconv.FormatUint(uint64(stat.PSize), 10),
		"mdbx.depth":          strconv.FormatUint(uint64(stat.Depth), 10),
		"mdbx.branch_pages":   strconv.FormatUint(stat.BranchPages, 10),
		"mdbx.leaf_pages":     strconv.FormatUint(stat.LeafPages, 10),
		"mdbx.overflow_pages": strconv.FormatUint(stat.OverflowPages, 10),
		"mdbx.entries":        strconv.FormatUint(stat.Entries, 10),
		"mdbx.last_tx_id":     strconv.FormatUint(stat.LastTxId, 10),
	}
}

func (db *MDBX) NewBatch() Batch {
	return newMDBXBatch(db)
}

func (db *MDBX) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	txn, err := db.Env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, err
	}
	return newMDBXIterator(db.DBI, txn, start, end, false)
}

func (db *MDBX) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	txn, err := db.Env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, err
	}
	return newMDBXIterator(db.DBI, txn, start, end, true)
}

func DefaultPageSize() uint64 {
	osPageSize := os.Getpagesize()
	if osPageSize < 4096 { // reduce further may lead to errors (because some data is just big)
		osPageSize = 4096
	} else if osPageSize > mdbx.MaxPageSize {
		osPageSize = mdbx.MaxPageSize
	}
	osPageSize = osPageSize / 4096 * 4096 // ensure it's rounded
	return uint64(osPageSize)
}
