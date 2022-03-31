//go:build pebble

package db

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

type pebbleIterator struct {
	source     *pebble.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

var _ Iterator = (*pebbleIterator)(nil)

func newPebbleIterator(source *pebble.Iterator, start, end []byte, isReverse bool) *pebbleIterator {
	if isReverse {
		if end == nil {
			source.Last()
		} else {
			source.SetBounds(start, end)
		}
	} else {
		if start == nil {
			source.First()
		} else {
			source.SetBounds(start, end)
		}
	}
	return &pebbleIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (itr *pebbleIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *pebbleIterator) Valid() bool {
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source has error, invalid.
	if err := itr.source.Error(); err != nil {
		itr.isInvalid = true
		return false
	}

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	start := itr.start
	end := itr.end
	key := itr.source.Key()
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// It's valid.
	return true
}

// Key implements Iterator.
func (itr *pebbleIterator) Key() []byte {
	itr.assertIsValid()
	return itr.source.Value()
}

// Value implements Iterator.
func (itr *pebbleIterator) Value() []byte {
	itr.assertIsValid()
	return itr.source.Value()
}

// Next implements Iterator.
func (itr pebbleIterator) Next() {
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

// Error implements Iterator.
func (itr *pebbleIterator) Error() error {
	return itr.source.Error()
}

// Close implements Iterator.
func (itr *pebbleIterator) Close() error {
	err := itr.source.Close()
	if err != nil {
		return err
	}
	return nil
}

func (itr *pebbleIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
