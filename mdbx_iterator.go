//go:build mdbx
// +build mdbx

package db

import (
	"bytes"

	"github.com/torquem-ch/mdbx-go/mdbx"
)

// mdbxIterator allows you to iterate on range of keys/values given some
// start / end keys (nil & nil will result in doing full scan).
type mdbxIterator struct {
	tx *mdbx.Txn

	itr   *mdbx.Cursor
	start []byte
	end   []byte

	currentKey   []byte
	currentValue []byte

	isInvalid bool
	isReverse bool

	err error
}

var _ Iterator = (*mdbxIterator)(nil)

// newMDBXIterator creates a new mdbxIterator.
func newMDBXIterator(dbi mdbx.DBI, tx *mdbx.Txn, start, end []byte, isReverse bool) (*mdbxIterator, error) {
	itr, err := tx.OpenCursor(dbi)
	if err != nil {
		return nil, err
	}

	var ck, cv []byte
	if isReverse {
		switch {
		case end == nil:
			ck, cv, err = itr.Get(nil, nil, mdbx.Last)
		default:
			_, _, err = itr.Get(end, nil, mdbx.SetRange)
			if err == nil {
				ck, cv, err = itr.Get(nil, nil, mdbx.Prev)
			} else if mdbx.IsNotFound(err) {
				ck, cv, err = itr.Get(nil, nil, mdbx.Last)
			}
		}
	} else {
		switch {
		case start == nil:
			ck, cv, err = itr.Get(nil, nil, mdbx.First)
		default:
			ck, cv, err = itr.Get(start, nil, mdbx.SetRange)
		}
	}
	if mdbx.IsNotFound(err) {
		err = nil
	}

	return &mdbxIterator{
		tx:           tx,
		itr:          itr,
		start:        start,
		end:          end,
		currentKey:   ck,
		currentValue: cv,
		isReverse:    isReverse,
		isInvalid:    false,
		err:          err,
	}, nil
}

// Domain implements Iterator.
func (itr *mdbxIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *mdbxIterator) Valid() bool {
	if itr.isInvalid {
		return false
	}

	if itr.Error() != nil {
		itr.isInvalid = true
		return false
	}

	// iterated to the end of the cursor
	if itr.currentKey == nil {
		itr.isInvalid = true
		return false
	}

	if itr.isReverse {
		if itr.start != nil && bytes.Compare(itr.currentKey, itr.start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if itr.end != nil && bytes.Compare(itr.end, itr.currentKey) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// Valid
	return true
}

// Next implements Iterator.
func (itr *mdbxIterator) Next() {
	var err error
	itr.assertIsValid()
	if itr.isReverse {
		itr.currentKey, itr.currentValue, err = itr.itr.Get(nil, nil, mdbx.Prev)
	} else {
		itr.currentKey, itr.currentValue, err = itr.itr.Get(nil, nil, mdbx.Next)
	}
	if !mdbx.IsNotFound(err) {
		itr.err = err
	}
}

// Key implements Iterator.
func (itr *mdbxIterator) Key() []byte {
	itr.assertIsValid()
	return append([]byte{}, itr.currentKey...)
}

// Value implements Iterator.
func (itr *mdbxIterator) Value() []byte {
	itr.assertIsValid()
	var value []byte
	if itr.currentValue != nil {
		value = append([]byte{}, itr.currentValue...)
	}
	return value
}

// Error implements Iterator.
func (itr *mdbxIterator) Error() error {
	return itr.err
}

// Close implements Iterator.
func (itr *mdbxIterator) Close() error {
	itr.tx.Abort()
	return nil
}

func (itr *mdbxIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
