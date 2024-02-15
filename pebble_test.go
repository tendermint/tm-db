package db

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
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

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}

	for i, key := range keys {
		if err := db.Set(key, values[i]); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	itr, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer itr.Close()

	for i := 0; itr.Valid(); itr.Next() {
		if !reflect.DeepEqual(itr.Key(), keys[i]) || !reflect.DeepEqual(itr.Value(), values[i]) {
			t.Errorf("Iterator key/value mismatch: got %v/%v, want %v/%v", itr.Key(), itr.Value(), keys[i], values[i])
		}
		i++
	}
}

func TestPebbleDBSetGet(t *testing.T) {
	db, cleanup := newTestPebbleDB(t)
	defer cleanup()

	key := []byte("key")
	value := []byte("value")

	// Test Set operation
	require.NoError(t, db.Set(key, value))

	// Test Get operation
	gotValue, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, gotValue)

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
	writeCount := 100 // Number of writes per goroutine
	goroutines := 10  // Number of goroutines

	// Perform concurrent writes
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < writeCount; j++ {
				key := []byte("key_" + strconv.Itoa(goroutineID) + "_" + strconv.Itoa(j))
				value := []byte("value_" + strconv.Itoa(j))
				require.NoError(t, db.Set(key, value))
			}
		}(i)
	}

	wg.Wait() // Wait for all writes to complete

	// Verify data integrity
	for i := 0; i < goroutines; i++ {
		for j := 0; j < writeCount; j++ {
			key := []byte("key_" + strconv.Itoa(i) + "_" + strconv.Itoa(j))
			expectedValue := []byte("value_" + strconv.Itoa(j))
			value, err := db.Get(key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)
		}
	}
}
