package db

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPebbleDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	_, ok := db.(*PebbleDB)
	assert.True(t, ok)
}

func BenchmarkPebbleDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = db.Close()
		require.NoError(b, err)

		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}

func TestPebbleDB_Iterator(t *testing.T) {
	db, cleanup := newTestPebbleDB(t)
	defer cleanup()

	// Insert test data
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	for i, key := range keys {
		require.NoError(t, db.Set(key, values[i]))
	}

	// Test full range iteration
	testFullRangeIterator(t, db, keys, values)

	// Test partial range iteration
	testPartialRangeIterator(t, db, keys, values)

	// Test reverse iterator
	testReverseIterator(t, db, keys, values)

	// Test edge cases: empty database, empty range, etc.
	testIteratorEdgeCases(t, db)
}

func testFullRangeIterator(t *testing.T, db *PebbleDB, keys, values [][]byte) {
	t.Helper()
	itr, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	defer itr.Close()

	i := 0
	for itr.Valid() {
		require.Less(t, i, len(keys), "Iterator returned more keys than expected")
		assert.Equal(t, keys[i], itr.Key(), "Key mismatch")
		assert.Equal(t, values[i], itr.Value(), "Value mismatch")
		itr.Next()
		i++
	}
	assert.Equal(t, len(keys), i, "Iterator did not iterate over all keys")
}

func testPartialRangeIterator(t *testing.T, db *PebbleDB, keys, values [][]byte) {
	t.Helper()
	// Assuming keys are sorted, iterate from the second key to the third key
	startKey := keys[1]
	endKey := keys[2]
	itr, err := db.Iterator(startKey, endKey)
	require.NoError(t, err)
	defer itr.Close()

	if itr.Valid() {
		assert.Equal(t, startKey, itr.Key(), "Partial range iterator key mismatch")
		assert.Equal(t, values[1], itr.Value(), "Partial range iterator value mismatch")
		itr.Next()
	}
	assert.False(t, itr.Valid(), "Partial range iterator expected to have only one valid entry")
}

func testReverseIterator(t *testing.T, db *PebbleDB, keys, values [][]byte) {
	t.Helper()
	itr, err := db.ReverseIterator(nil, nil)
	require.NoError(t, err)
	defer itr.Close()

	i := len(keys) - 1
	for itr.Valid() {
		require.GreaterOrEqual(t, i, 0, "Reverse iterator returned more keys than expected")
		assert.Equal(t, keys[i], itr.Key(), "Reverse iterator key mismatch")
		assert.Equal(t, values[i], itr.Value(), "Reverse iterator value mismatch")
		itr.Next()
		i--
	}
	assert.Equal(t, -1, i, "Reverse iterator did not iterate over all keys")
}

func testIteratorEdgeCases(t *testing.T, db *PebbleDB) {
	t.Helper()
	// Test iterator with empty start or end key
	_, err := db.Iterator([]byte{}, nil)
	assert.Error(t, err, "Expected error for empty start key")

	_, err = db.Iterator(nil, []byte{})
	assert.Error(t, err, "Expected error for empty end key")

	// Test reverse iterator with empty start or end key
	_, err = db.ReverseIterator([]byte{}, nil)
	assert.Error(t, err, "Expected error for empty start key in reverse iterator")

	_, err = db.ReverseIterator(nil, []byte{})
	assert.Error(t, err, "Expected error for empty end key in reverse iterator")

	// Test iterator on an empty database
	emptyDB, cleanup := newTestPebbleDB(t)
	defer cleanup()

	itr, err := emptyDB.Iterator(nil, nil)
	require.NoError(t, err)
	defer itr.Close()
	assert.False(t, itr.Valid(), "Iterator on an empty database should be invalid")
}

func TestPebbleDBSetGet(t *testing.T) {
	db, cleanup := newTestPebbleDB(t)
	defer cleanup()

	key := []byte("key")
	value := []byte("value")
	updatedValue := []byte("updatedValue")

	// Test Set operation
	require.NoError(t, db.Set(key, value))

	// Test Get operation
	gotValue, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, gotValue)

	// Test Update operation
	require.NoError(t, db.Set(key, updatedValue))
	gotValue, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, updatedValue, gotValue)

	// Test Delete operation
	require.NoError(t, db.Delete(key))
	gotValue, err = db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, gotValue)

	// Test Get non-existent key
	gotValue, err = db.Get([]byte("non-existent"))
	require.NoError(t, err)
	assert.Nil(t, gotValue)
}

// newTestPebbleDB creates a new PebbleDB instance for testing purposes.
// It returns the PebbleDB instance and a cleanup function to be called
// by the tests, typically deferred right after calling this function.
func newTestPebbleDB(t *testing.T) (*PebbleDB, func()) {
	t.Helper()
	// Create a temporary directory for the database.
	dir, err := os.MkdirTemp("", "pebble_test")
	require.NoError(t, err)

	// Create a new PebbleDB instance in the temporary directory.
	db, err := NewPebbleDB("testdb", dir)
	require.NoError(t, err)

	// Return the database and a cleanup function.
	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Logf("Failed to close database: %v", err)
		}
		if err := os.RemoveAll(dir); err != nil { // Handle the error returned by os.RemoveAll
			t.Logf("Failed to remove temporary directory: %v", err)
		}
	}

	return db, cleanup
}

func TestPebbleDBCompaction(t *testing.T) {
	db, cleanup := newTestPebbleDB(t)
	defer cleanup()

	// Insert some key-value pairs into the database.
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}
	for i, key := range keys {
		err := db.Set([]byte(key), []byte(values[i]))
		require.NoError(t, err)
	}

	// Perform a compaction over the entire key range.
	err := db.Compact(nil, nil)
	require.NoError(t, err)

	// Verify that all the key-value pairs are still correctly stored in the database.
	for i, key := range keys {
		value, err := db.Get([]byte(key))
		require.NoError(t, err)
		assert.Equal(t, values[i], string(value))
	}
}

func TestPebbleDBConcurrency(t *testing.T) {
	db, cleanup := newTestPebbleDB(t)
	defer cleanup()

	var wg sync.WaitGroup
	goroutines := 10 // Number of goroutines
	keyPrefix := "key_"
	valuePrefix := "value_"

	// Use a map to track all the keys and their expected values for later verification
	expectedValues := make(map[string][]byte)
	var mu sync.Mutex // Protects expectedValues

	// Perform concurrent writes with randomized write counts
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			writeCount := rand.Intn(100) + 50 // Randomize number of writes between 50 and 149
			for j := 0; j < writeCount; j++ {
				key := []byte(fmt.Sprintf("%s%d_%d", keyPrefix, goroutineID, j))
				value := []byte(fmt.Sprintf("%s%d", valuePrefix, j))
				require.NoError(t, db.Set(key, value))

				mu.Lock()
				expectedValues[string(key)] = value
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait() // Wait for all writes to complete

	// Verify data integrity
	mu.Lock()
	defer mu.Unlock()
	for key, expectedValue := range expectedValues {
		value, err := db.Get([]byte(key))
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}
