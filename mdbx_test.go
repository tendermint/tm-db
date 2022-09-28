//go:build mdbx
// +build mdbx

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMDBXNewMDBX(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	defer cleanupDBDir(dir, name)

	db, err := NewMDBX(name, dir)
	require.NoError(t, err)
	db.Close()
}

func TestWithMDBX(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mdbx")

	db, err := NewMDBX(path, "")
	require.NoError(t, err)

	t.Run("MDBX", func(t *testing.T) { Run(t, db) })
}

func BenchmarkMDBXRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewMDBX(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}
