package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE: These helper functions are placeholders for external functions (randStr, int642Bytes, bytes2Int64)
// that were not included in the original snippet but are required for the tests to compile and run.

// randStr generates a random alphanumeric string of a given length.
func randStr(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

// int642Bytes converts an int64 to a byte slice.
func int642Bytes(i int64) []byte {
	// Simple placeholder implementation for testing purposes
	return []byte(fmt.Sprintf("%d", i))
}

// bytes2Int64 converts a byte slice back to an int64.
func bytes2Int64(b []byte) int64 {
	var i int64
	fmt.Sscanf(string(b), "%d", &i)
	return i
}

// Register a test backend for PrefixDB as well, with some unrelated junk data
func init() {
	// nolint: errcheck
	registerDBCreator("prefixdb", func(name, dir string) (DB, error) {
		mdb := NewMemDB()
		mdb.Set([]byte("a"), []byte{1})
		mdb.Set([]byte("b"), []byte{2})
		mdb.Set([]byte("t"), []byte{20})
		mdb.Set([]byte("test"), []byte{0})
		mdb.Set([]byte("u"), []byte{21})
		mdb.Set([]byte("z"), []byte{26})
		// The key "test/" should be excluded from the PrefixDB's view.
		return NewPrefixDB(mdb, []byte("test/")), nil
	}, false)
}

// cleanupDBDir removes the database directory after the test finishes.
// Uses require.NoError to ensure cleanup doesn't panic and reports failure through testing.T
func cleanupDBDir(t *testing.T, dir, name string) {
	dbPath := filepath.Join(dir, name) + ".db"
	err := os.RemoveAll(dbPath)
	require.NoError(t, err, "Failed to remove DB directory: %s", dbPath)
}

// testBackendGetSetDelete tests the fundamental Get, Set, and Delete operations.
func testBackendGetSetDelete(t *testing.T, backend BackendType) {
	// Use os.MkdirTemp instead of deprecated ioutil.TempDir
	dirname, err := os.MkdirTemp("", fmt.Sprintf("test_backend_%s_", backend))
	require.NoError(t, err)

	db, err := NewDB("testdb", backend, dirname)
	require.NoError(t, err)
	defer cleanupDBDir(t, dirname, "testdb")

	// A nonexistent key should return nil.
	value, err := db.Get([]byte("a"))
	require.NoError(t, err)
	require.Nil(t, value)

	ok, err := db.Has([]byte("a"))
	require.NoError(t, err)
	require.False(t, ok)

	// Set and get a value.
	err = db.Set([]byte("a"), []byte{0x01})
	require.NoError(t, err)

	ok, err = db.Has([]byte("a"))
	require.NoError(t, err)
	require.True(t, ok)

	value, err = db.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x01}, value)

	// SetSync and retrieve a value.
	err = db.SetSync([]byte("b"), []byte{0x02})
	require.NoError(t, err)

	value, err = db.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte{0x02}, value)

	// Deleting a non-existent value is fine.
	err = db.Delete([]byte("x"))
	require.NoError(t, err)

	err = db.DeleteSync([]byte("x"))
	require.NoError(t, err)

	// Delete a value.
	err = db.Delete([]byte("a"))
	require.NoError(t, err)

	value, err = db.Get([]byte("a"))
	require.NoError(t, err)
	require.Nil(t, value)

	err = db.DeleteSync([]byte("b"))
	require.NoError(t, err)

	value, err = db.Get([]byte("b"))
	require.NoError(t, err)
	require.Nil(t, value)

	// Setting, getting, and deleting an empty key or nil key should error.
	_, err = db.Get([]byte{})
	require.Equal(t, errKeyEmpty, err)
	_, err = db.Get(nil)
	require.Equal(t, errKeyEmpty, err)

	_, err = db.Has([]byte{})
	require.Equal(t, errKeyEmpty, err)
	_, err = db.Has(nil)
	require.Equal(t, errKeyEmpty, err)

	err = db.Set([]byte{}, []byte{0x01})
	require.Equal(t, errKeyEmpty, err)
	err = db.Set(nil, []byte{0x01})
	require.Equal(t, errKeyEmpty, err)
	err = db.SetSync([]byte{}, []byte{0x01})
	require.Equal(t, errKeyEmpty, err)
	err = db.SetSync(nil, []byte{0x01})
	require.Equal(t, errKeyEmpty, err)

	err = db.Delete([]byte{})
	require.Equal(t, errKeyEmpty, err)
	err = db.Delete(nil)
	require.Equal(t, errKeyEmpty, err)
	err = db.DeleteSync([]byte{})
	require.Equal(t, errKeyEmpty, err)
	err = db.DeleteSync(nil)
	require.Equal(t, errKeyEmpty, err)

	// Setting a nil value should error, but an empty byte slice value is fine.
	err = db.Set([]byte("x"), nil)
	require.Equal(t, errValueNil, err)
	err = db.SetSync([]byte("x"), nil)
	require.Equal(t, errValueNil, err)

	err = db.Set([]byte("x"), []byte{})
	require.NoError(t, err)
	err = db.SetSync([]byte("x"), []byte{})
	require.NoError(t, err)
	value, err = db.Get([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte{}, value)
}

func TestBackendsGetSetDelete(t *testing.T) {
	for dbType := range backends {
		// Use t.Parallel() to run tests faster if backends support concurrent access
		dbType := dbType // capture range variable
		t.Run(string(dbType), func(t *testing.T) {
			testBackendGetSetDelete(t, dbType)
		})
	}
}

func TestGoLevelDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	// An empty directory path is provided to use a default location like os.TempDir()
	db, err := NewDB(name, GoLevelDBBackend, "")
	require.NoError(t, err)
	defer cleanupDBDir(t, "", name) // Note: cleanupDBDir is resilient to empty dir

	// Check if the returned interface is indeed the expected concrete type
	_, ok := db.(*GoLevelDB)
	assert.True(t, ok)
}

func TestDBIterator(t *testing.T) {
	for dbType := range backends {
		dbType := dbType // capture range variable
		t.Run(string(dbType), func(t *testing.T) {
			testDBIterator(t, dbType)
		})
	}
}

// testDBIterator validates forward and reverse iteration, including boundary conditions.
func testDBIterator(t *testing.T, backend BackendType) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir, err := os.MkdirTemp("", "tm-db-test")
	require.NoError(t, err)

	db, err := NewDB(name, backend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(t, dir, name)

	// Populate the database, skipping key '6' to test gap handling.
	for i := 0; i < 10; i++ {
		if i != 6 {
			err := db.Set(int642Bytes(int64(i)), []byte{})
			require.NoError(t, err)
		}
	}

	// Blank iterator keys should error (start or end key must not be empty/nil)
	_, err = db.Iterator([]byte{}, nil)
	require.Equal(t, errKeyEmpty, err)
	_, err = db.Iterator(nil, []byte{})
	require.Equal(t, errKeyEmpty, err)
	_, err = db.ReverseIterator([]byte{}, nil)
	require.Equal(t, errKeyEmpty, err)
	_, err = db.ReverseIterator(nil, []byte{})
	require.Equal(t, errKeyEmpty, err)

	// Full forward iteration (nil, nil)
	itr, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	verifyIterator(t, itr, []int64{0, 1, 2, 3, 4, 5, 7, 8, 9}, "full forward iteration")

	// Full reverse iteration (nil, nil)
	ritr, err := db.ReverseIterator(nil, nil)
	require.NoError(t, err)
	verifyIterator(t, ritr, []int64{9, 8, 7, 5, 4, 3, 2, 1, 0}, "full reverse iteration")

	// Forward iterator start key is exclusive, end key is exclusive.
	// Iterator(nil, int642Bytes(0)) means start after nil (start of DB) up to but excluding '0'. Should be empty.
	itr, err = db.Iterator(nil, int642Bytes(0))
	require.NoError(t, err)
	verifyIterator(t, itr, []int64(nil), "forward iterator from start to 0 (exclusive)")

	// Reverse Iterator start key is exclusive, end key is inclusive.
	// ReverseIterator(int642Bytes(10), nil) means reverse from 10 (exclusive) up to the start of DB. Should be empty.
	ritr, err = db.ReverseIterator(int642Bytes(10), nil)
	require.NoError(t, err)
	verifyIterator(t, ritr, []int64(nil), "reverse iterator from 10 (exclusive) to end")

	// Forward iterator from 0 (inclusive) to end (exclusive of infinity)
	itr, err = db.Iterator(int642Bytes(0), nil)
	require.NoError(t, err)
	verifyIterator(t, itr, []int64{0, 1, 2, 3, 4, 5, 7, 8, 9}, "forward iterator from 0 (inclusive)")

	// Forward iterator from 1 (inclusive) to end
	itr, err = db.Iterator(int642Bytes(1), nil)
	require.NoError(t, err)
	verifyIterator(t, itr, []int64{1, 2, 3, 4, 5, 7, 8, 9}, "forward iterator from 1 (inclusive)")

	// Reverse iterator from end (exclusive) to 10 (exclusive)
	ritr, err = db.ReverseIterator(nil, int642Bytes(10))
	require.NoError(t, err)
	verifyIterator(t, ritr,
		[]int64{9, 8, 7, 5, 4, 3, 2, 1, 0}, "reverse iterator from end to 10 (exclusive)")

	// Reverse iterator from end (exclusive) to 9 (exclusive)
	ritr, err = db.ReverseIterator(nil, int642Bytes(9))
	require.NoError(t, err)
	verifyIterator(t, ritr,
		[]int64{8, 7, 5, 4, 3, 2, 1, 0}, "reverse iterator from end to 9 (exclusive)")

	// Forward iteration with explicit bounds (inclusive start, exclusive end)
	itr, err = db.Iterator(int642Bytes(5), int642Bytes(6))
	require.NoError(t, err)
	verifyIterator(t, itr, []int64{5}, "forward iterator from 5 (incl) to 6 (excl)")

	itr, err = db.Iterator(int642Bytes(6), int642Bytes(7))
	require.NoError(t, err)
	verifyIterator(t, itr, []int64(nil), "forward iterator from 6 (gap) to 7 (excl)")

	itr, err = db.Iterator(int642Bytes(6), int642Bytes(8))
	require.NoError(t, err)
	verifyIterator(t, itr, []int64{7}, "forward iterator from 6 (gap) to 8 (excl)")

	// Reverse iteration with explicit bounds (exclusive start, inclusive end)
	ritr, err = db.ReverseIterator(int642Bytes(4), int642Bytes(5))
	require.NoError(t, err)
	verifyIterator(t, ritr, []int64{4}, "reverse iterator from 5 (excl) to 4 (incl)")

	ritr, err = db.ReverseIterator(int642Bytes(4), int642Bytes(6))
	require.NoError(t, err)
	verifyIterator(t, ritr,
		[]int64{5, 4}, "reverse iterator from 6 (excl) to 4 (incl)")

	ritr, err = db.ReverseIterator(int642Bytes(6), int642Bytes(7))
	require.NoError(t, err)
	verifyIterator(t, ritr,
		[]int64(nil), "reverse iterator from 7 (excl) to 6 (gap)")

	// Reverse iterator from end (exclusive) to 5 (inclusive)
	ritr, err = db.ReverseIterator(int642Bytes(5), nil)
	require.NoError(t, err)
	verifyIterator(t, ritr, []int64{9, 8, 7, 5}, "reverse iterator to 5 (inclusive)")

	// Reverse iterator from 9 (exclusive) to 8 (inclusive)
	ritr, err = db.ReverseIterator(int642Bytes(8), int642Bytes(9))
	require.NoError(t, err)
	verifyIterator(t, ritr, []int64{8}, "reverse iterator from 9 (excl) to 8 (incl)")


	// Ensure that the iterators don't panic with an empty database.
	dir2, err := os.MkdirTemp("", "tm-db-test-empty")
	require.NoError(t, err)
	db2, err := NewDB(name, backend, dir2)
	require.NoError(t, err)
	defer cleanupDBDir(t, dir2, name)

	itr, err = db2.Iterator(nil, nil)
	require.NoError(t, err)
	verifyIterator(t, itr, nil, "forward iterator with empty db")

	ritr, err = db2.ReverseIterator(nil, nil)
	require.NoError(t, err)
	verifyIterator(t, ritr, nil, "reverse iterator with empty db")
}

// verifyIterator drains the iterator and compares the keys to the expected list of int64 values.
func verifyIterator(t *testing.T, itr Iterator, expected []int64, msg string) {
	defer itr.Close()
	var list []int64
	for itr.Valid() {
		key := itr.Key()
		list = append(list, bytes2Int64(key))
		itr.Next()
	}
	require.NoError(t, itr.Error(), "Iterator error check failed")
	assert.Equal(t, expected, list, msg)
}

func TestDBBatch(t *testing.T) {
	for dbType := range backends {
		dbType := dbType // capture range variable
		t.Run(fmt.Sprintf("%v", dbType), func(t *testing.T) {
			testDBBatch(t, dbType)
		})
	}
}

// testDBBatch validates the atomic write capability of the database batch feature.
func testDBBatch(t *testing.T, backend BackendType) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir, err := os.MkdirTemp("", "tm-db-batch")
	require.NoError(t, err)
	db, err := NewDB(name, backend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(t, dir, name)

	// Create a new batch, and some items - they should not be visible until we write
	batch := db.NewBatch()
	require.NoError(t, batch.Set([]byte("a"), []byte{1}))
	require.NoError(t, batch.Set([]byte("b"), []byte{2}))
	require.NoError(t, batch.Set([]byte("c"), []byte{3}))
	assertKeyValues(t, db, map[string][]byte{})

	err = batch.Write()
	require.NoError(t, err)
	assertKeyValues(t, db, map[string][]byte{"a": {1}, "b": {2}, "c": {3}})

	// Operations on a written batch should error, but closing it should work
	require.Error(t, batch.Set([]byte("a"), []byte{9}))
	require.Error(t, batch.Delete([]byte("a")))
	require.Error(t, batch.Write())
	require.Error(t, batch.WriteSync())
	require.NoError(t, batch.Close())

	// Batches should write changes in order (last operation wins)
	batch = db.NewBatch()
	require.NoError(t, batch.Delete([]byte("a")))         // Delete 'a'
	require.NoError(t, batch.Set([]byte("a"), []byte{1})) // Set 'a' back to {1}
	require.NoError(t, batch.Set([]byte("b"), []byte{1}))
	require.NoError(t, batch.Set([]byte("b"), []byte{2})) // Overwrite 'b' to {2}
	require.NoError(t, batch.Set([]byte("c"), []byte{3}))
	require.NoError(t, batch.Delete([]byte("c"))) // Delete 'c'
	require.NoError(t, batch.Write())
	require.NoError(t, batch.Close())
	assertKeyValues(t, db, map[string][]byte{"a": {1}, "b": {2}}) // 'c' is gone

	// Empty and nil keys, as well as nil values, should be disallowed in batches
	batch = db.NewBatch()
	err = batch.Set([]byte{}, []byte{0x01})
	require.Equal(t, errKeyEmpty, err)
	err = batch.Set(nil, []byte{0x01})
	require.Equal(t, errKeyEmpty, err)
	err = batch.Set([]byte("a"), nil)
	require.Equal(t, errValueNil, err)

	err = batch.Delete([]byte{})
	require.Equal(t, errKeyEmpty, err)
	err = batch.Delete(nil)
	require.Equal(t, errKeyEmpty, err)

	err = batch.Close()
	require.NoError(t, err)

	// It should be possible to write and close an empty batch without errors
	batch = db.NewBatch()
	err = batch.Write()
	require.NoError(t, err)
	assertKeyValues(t, db, map[string][]byte{"a": {1}, "b": {2}})
	batch.Close()

	// All other operations on a closed batch should error
	batch.Close() // Re-closing is fine, as checked in original code

	require.Error(t, batch.Set([]byte("a"), []byte{9}), "Set on closed batch should fail")
	require.Error(t, batch.Delete([]byte("a")), "Delete on closed batch should fail")
	require.Error(t, batch.Write(), "Write on closed batch should fail")
	require.Error(t, batch.WriteSync(), "WriteSync on closed batch should fail")
}

// assertKeyValues checks the current state of the database against an expected map.
func assertKeyValues(t *testing.T, db DB, expect map[string][]byte) {
	// Full iteration is used for verification, checking all keys.
	iter, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	actual := make(map[string][]byte)
	for ; iter.Valid(); iter.Next() {
		require.NoError(t, iter.Error(), "Iterator encountered an internal error")
		// Copying the key/value is good practice if the iterator's memory might be reused
		actual[string(iter.Key())] = append([]byte(nil), iter.Value()...)
	}

	assert.Equal(t, expect, actual, "Database state mismatch after batch operation")
}
