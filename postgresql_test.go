package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLDb(t *testing.T) {
	db, err := NewPostgreSQLDb("testdb", "", nil)
	require.NoError(t, err)
	defer cleanupPostgresDB("testdb", "", db)

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
	db, err := NewPostgreSQLDb("testdb", "", nil)
	require.NoError(t, err)
	defer cleanupPostgresDB("testdb", "", db)

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

func cleanupPostgresDB(name string, dir string, db *PostgreSQLDb) error {
	db.Close()
	return RemovePostgreSQLDb(name, dir, nil)
}

func TestPostgreSQLEmbeddings(t *testing.T) {
	requirePgVector(t)

	opts := OptionsMap{
		optionEnableEmbeddings:   true,
		optionEmbeddingDimension: 3,
		optionEmbeddingMetric:    string(embeddingMetricCosine),
	}

	db, err := NewPostgreSQLDb("testdb_embeddings", "", opts)
	if err != nil {
		t.Skipf("skipping pgvector test: %v", err)
		return
	}
	defer cleanupPostgresDB("testdb_embeddings", "", db)

	err = db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	err = db.Set([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	err = db.UpsertEmbedding([]byte("key1"), []float32{0.9, 0.1, 0})
	require.NoError(t, err)
	err = db.UpsertEmbedding([]byte("key2"), []float32{0.1, 0.9, 0})
	require.NoError(t, err)

	var embeddingCount int
	err = db.Pool().QueryRow(context.Background(), `SELECT count(*) FROM state_embeddings`).Scan(&embeddingCount)
	require.NoError(t, err)
	require.Equal(t, 2, embeddingCount)

	vec, err := db.GetEmbedding([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []float32{0.9, 0.1, 0}, vec)

	results, err := db.SimilaritySearch([]float32{0.85, 0.15, 0}, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, []byte("key1"), results[0].Key)

	err = db.Delete([]byte("key1"))
	require.NoError(t, err)
	vec, err = db.GetEmbedding([]byte("key1"))
	require.NoError(t, err)
	require.Nil(t, vec)
}

func requirePgVector(t *testing.T) {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), "postgresql://localhost:5432/postgres")
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
		return
	}
	defer pool.Close()

	if _, err := pool.Exec(context.Background(), `CREATE EXTENSION IF NOT EXISTS vector;`); err != nil {
		t.Skipf("pgvector extension is not available: %v", err)
		return
	}
}
