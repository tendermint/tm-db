//go:build mdbx
// +build mdbx

package db

import (
	"os"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/torquem-ch/mdbx-go/mdbx"
)

func init() {
	registerDBCreator(MDBXBackend, NewMDBX, false)
}

type MDBX struct {
	env *mdbx.Env
	dbi mdbx.DBI
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
	return &MDBX{env, dbi}, nil
}

func (db *MDBX) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	var value []byte
	if err := db.env.View(func(txn *mdbx.Txn) error {
		v, err := txn.Get(db.dbi, key)
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
	if err := db.env.View(func(txn *mdbx.Txn) error {
		// zero-copy
		txn.RawRead = true

		itr, err := txn.OpenCursor(db.dbi)
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

	return db.env.Update(func(txn *mdbx.Txn) error {
		return txn.Put(db.dbi, key, value, 0)
	})
}

func (db *MDBX) SetSync(key []byte, value []byte) error {
	// TODO
	return db.Set(key, value)
}

func (db *MDBX) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	err := db.env.Update(func(txn *mdbx.Txn) error {
		return txn.Del(db.dbi, key, nil)
	})
	if err != nil && mdbx.IsNotFound(err) {
		return nil
	}
	return err
}

func (db *MDBX) DeleteSync(key []byte) error {
	// TODO
	return db.Delete(key)
}

func (db *MDBX) Close() error {
	db.env.CloseDBI(db.dbi)
	db.env.Close()
	return nil
}

func (db *MDBX) Print() error {
	// TODO
	return nil
}

func (db *MDBX) Stats() map[string]string {
	_, err := db.env.Stat()
	if err != nil {
		return nil
	}
	// TODO
	return nil
}

func (db *MDBX) NewBatch() Batch {
	return newMDBXBatch(db)
}

func (db *MDBX) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	txn, err := db.env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, err
	}
	return newMDBXIterator(db.dbi, txn, start, end, false)
}

func (db *MDBX) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	txn, err := db.env.BeginTxn(nil, mdbx.Readonly)
	if err != nil {
		return nil, err
	}
	return newMDBXIterator(db.dbi, txn, start, end, true)
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
