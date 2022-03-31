//go:build pebble
// +build pebble

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
			source.SeekLT(end)
			if source.Valid() {
				eoakey := moveSliceToBytes(source.Key()) // end or after key
				if bytes.Compare(end, eoakey) <= 0 {
					source.Prev()
				}
			} else {
				source.Last()
			}
		}
	} else {
		if start == nil {
			source.First()
		} else {
			source.Seek(start)
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
	if err := itr.source.Err(); err != nil {
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
	key := moveSliceToBytes(itr.source.Key())
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
	return moveSliceToBytes(itr.source.Key())
}

// Value implements Iterator.
func (itr *pebbleIterator) Value() []byte {
	itr.assertIsValid()
	return moveSliceToBytes(itr.source.Value())
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
	return itr.source.Err()
}

// Close implements Iterator.
func (itr *pebbleIterator) Close() error {
	itr.source.Close()
	return nil
}

func (itr *pebbleIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

// moveSliceToBytes will free the slice and copy out a go []byte
// This function can be applied on *Slice returned from Key() and Value()
// of an Iterator, because they are marked as freed.
func moveSliceToBytes(s *pebble.Slice) []byte {
	defer s.Free()
	if !s.Exists() {
		return nil
	}
	v := make([]byte, len(s.Data()))
	copy(v, s.Data())
	return v
}
