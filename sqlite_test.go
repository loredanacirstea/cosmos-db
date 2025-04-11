package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDb(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)
	// defer func() {
	// 	dir, _ := os.Getwd()
	// 	os.Remove(path.Join(dir, "testdb.db"))
	// }()
	db, err := NewSqliteDb(name, "", nil)
	require.NoError(t, err)

	// Set
	err = db.Set([]byte{1, 2, 4}, []byte{1, 1, 1})
	require.NoError(t, err)
	value, err := db.Get([]byte{1, 2, 4})
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1, 1}, value)

	// Delete
	err = db.Delete([]byte{1, 2, 4})
	require.NoError(t, err)
	value, err = db.Get([]byte{1, 2, 4})
	require.NoError(t, err)
	require.Nil(t, value)

	// Batch
	batch := db.NewBatchWithSize(100000)
	err = batch.Set([]byte{1, 2, 3}, []byte{2, 2, 2})
	require.NoError(t, err)
	err = batch.Write()
	require.NoError(t, err)
	err = batch.Close()
	require.NoError(t, err)
	value, err = db.Get([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, []byte{2, 2, 2}, value)
}
