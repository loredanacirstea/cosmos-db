package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgreSQLDb(t *testing.T) {
	db, err := NewPostgreSQLDb("testdb", "", nil)
	require.NoError(t, err)
	defer db.Close()

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

func TestPostgreSQLIterator(t *testing.T) {
	// Skip test if PostgreSQL connection string not provided
	connString := os.Getenv("POSTGRES_TEST_URL")
	if connString == "" {
		t.Skip("Skipping PostgreSQL test: POSTGRES_TEST_URL environment variable not set")
	}

	db, err := NewPostgreSQLDb("testdb", connString, nil)
	require.NoError(t, err)
	defer db.Close()

	// Set up test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}

	for k, v := range testData {
		err = db.Set([]byte(k), []byte(v))
		require.NoError(t, err)
	}

	// Test forward iterator
	itr, err := db.Iterator([]byte("key1"), []byte("key4"))
	require.NoError(t, err)
	defer itr.Close()

	count := 0
	for ; itr.Valid(); itr.Next() {
		key := string(itr.Key())
		value := string(itr.Value())
		expectedValue, exists := testData[key]
		require.True(t, exists)
		require.Equal(t, expectedValue, value)
		count++
	}
	require.Equal(t, 3, count) // key1, key2, key3 (key4 is exclusive)

	// Test reverse iterator
	reverseItr, err := db.ReverseIterator([]byte("key1"), []byte("key4"))
	require.NoError(t, err)
	defer reverseItr.Close()

	reverseCount := 0
	for ; reverseItr.Valid(); reverseItr.Next() {
		reverseCount++
	}
	require.Equal(t, 3, reverseCount)
}
