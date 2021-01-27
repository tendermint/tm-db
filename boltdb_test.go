// +build boltdb

package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoltDBNewBoltDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	defer cleanupDBDir(dir, name)

	db, err := NewBoltDB(name, dir)
	require.NoError(t, err)
	db.Close()
}

func TestBoltDBBatchSmallNumberFails(t *testing.T) {
	var txs int64 = 10
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	defer cleanupDBDir(dir, name)

	db, err := NewBoltDB(name, dir)
	require.NoError(t, err)

	for i := int64(1); i < txs; i++ {
		require.NoError(t, db.SetSync(int642Bytes(i), int642Bytes(i)))
	}

	iter, err := db.ReverseIterator(int642Bytes(int64(1)), int642Bytes(txs))
	require.NoError(t, err)
	defer iter.Close()

	deleteBatch := db.NewBatch()
	defer deleteBatch.Close()

	for ;iter.Valid(); iter.Next() {
		err := deleteBatch.Delete(iter.Key())
		require.NoError(t, err)
	}
	require.NoError(t, iter.Error())

	require.NoError(t, deleteBatch.WriteSync())
}

func TestBoltDBBatchLargeNumberPasses(t *testing.T) {
	var txs int64 = 1000
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	defer cleanupDBDir(dir, name)

	db, err := NewBoltDB(name, dir)
	require.NoError(t, err)

	for i := int64(1); i < txs; i++ {
		require.NoError(t, db.SetSync(int642Bytes(i), int642Bytes(i)))
	}

	iter, err := db.ReverseIterator(int642Bytes(int64(1)), int642Bytes(txs))
	require.NoError(t, err)
	defer iter.Close()

	deleteBatch := db.NewBatch()
	defer deleteBatch.Close()

	for ;iter.Valid(); iter.Next() {
		err := deleteBatch.Delete(iter.Key())
		require.NoError(t, err)
	}
	require.NoError(t, iter.Error())

	require.NoError(t, deleteBatch.WriteSync())
}

func TestBoltDBBatchNoIteratorPasses(t *testing.T) {
	var txs int64 = 10
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	defer cleanupDBDir(dir, name)

	db, err := NewBoltDB(name, dir)
	require.NoError(t, err)

	for i := int64(1); i < txs; i++ {
		require.NoError(t, db.SetSync(int642Bytes(i), int642Bytes(i)))
	}

	deleteBatch := db.NewBatch()
	defer deleteBatch.Close()

	for i := int64(1); i < txs; i++ {
		err := deleteBatch.Delete(int642Bytes(i))
		require.NoError(t, err)
	}

	require.NoError(t, deleteBatch.WriteSync())
}

func BenchmarkBoltDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewBoltDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}
