package db

import (
	"errors"
	"fmt"

	pgx "github.com/jackc/pgx/v5"
	pgvector "github.com/pgvector/pgvector-go"
)

var errEmbeddingEmpty = errors.New("embedding cannot be empty")

// EmbeddingResult represents the nearest neighbor search output.
type EmbeddingResult struct {
	Key      []byte
	Distance float32
}

func (p *PostgreSQLDb) ensureEmbeddingsEnabled() error {
	if !p.enableEmbeddings {
		return errEmbeddingsDisabled
	}
	return nil
}

// UpsertEmbedding stores or updates a vector embedding keyed by the same key used in state_storage.
func (p *PostgreSQLDb) UpsertEmbedding(key []byte, embedding []float32) error {
	if err := p.ensureEmbeddingsEnabled(); err != nil {
		return err
	}
	if len(key) == 0 {
		return errKeyEmpty
	}
	if len(embedding) == 0 {
		return errEmbeddingEmpty
	}
	if len(embedding) != p.embeddingDimension {
		return fmt.Errorf("embedding dimension mismatch: expected %d, got %d", p.embeddingDimension, len(embedding))
	}

	vector := pgvector.NewVector(embedding)
	if _, err := p.pool.Exec(p.ctx, postgresqlUpsertEmbeddingStmt, key, vector); err != nil {
		return fmt.Errorf("failed to upsert PostgreSQL embedding: %w", err)
	}
	return nil
}

// GetEmbedding retrieves the stored embedding for a key. Returns nil if no embedding is present.
func (p *PostgreSQLDb) GetEmbedding(key []byte) ([]float32, error) {
	if err := p.ensureEmbeddingsEnabled(); err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	var vector pgvector.Vector
	err := p.pool.QueryRow(p.ctx, postgresqlGetEmbeddingStmt, key).Scan(&vector)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query PostgreSQL embedding: %w", err)
	}

	return vector.Slice(), nil
}

// DeleteEmbedding removes the embedding for a given key.
func (p *PostgreSQLDb) DeleteEmbedding(key []byte) error {
	if err := p.ensureEmbeddingsEnabled(); err != nil {
		return err
	}
	if len(key) == 0 {
		return errKeyEmpty
	}
	if _, err := p.pool.Exec(p.ctx, postgresqlDelEmbeddingStmt, key); err != nil {
		return fmt.Errorf("failed to execute PostgreSQL embedding delete statement: %w", err)
	}
	return nil
}

// SimilaritySearch returns the closest embeddings ordered by the configured distance metric.
func (p *PostgreSQLDb) SimilaritySearch(query []float32, limit int) ([]EmbeddingResult, error) {
	if err := p.ensureEmbeddingsEnabled(); err != nil {
		return nil, err
	}
	if len(query) == 0 {
		return nil, errEmbeddingEmpty
	}
	if len(query) != p.embeddingDimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", p.embeddingDimension, len(query))
	}
	if limit <= 0 {
		limit = 10
	}

	op := p.embeddingMetric.operator()
	rows, err := p.pool.Query(
		p.ctx,
		fmt.Sprintf(`
			SELECT key, embedding %s $1 AS distance
			FROM state_embeddings
			ORDER BY embedding %s $1
			LIMIT $2;
		`, op, op),
		pgvector.NewVector(query),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL embeddings: %w", err)
	}
	defer rows.Close()

	results := make([]EmbeddingResult, 0, limit)
	for rows.Next() {
		var key []byte
		var distance float32
		if err := rows.Scan(&key, &distance); err != nil {
			return nil, fmt.Errorf("failed to scan embedding search result: %w", err)
		}
		results = append(results, EmbeddingResult{Key: key, Distance: distance})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate embedding search results: %w", err)
	}
	return results, nil
}
