package db

import (
	"context"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ Batch = (*postgresqlBatch)(nil)

type postgresqlBatchOp struct {
	action     batchAction
	key, value []byte
}

type postgresqlBatch struct {
	db    *PostgreSQLDb
	index int64
	pool  *pgxpool.Pool
	ctx   context.Context
	tx    pgx.Tx
	ops   []postgresqlBatchOp
	size  int
}

func NewPostgreSQLBatch(pool *pgxpool.Pool, ctx context.Context, db *PostgreSQLDb, index int64) (*postgresqlBatch, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL transaction: %w", err)
	}

	return &postgresqlBatch{
		db:    db,
		index: index,
		pool:  pool,
		ctx:   ctx,
		tx:    tx,
		ops:   make([]postgresqlBatchOp, 0),
	}, nil
}

func (b *postgresqlBatch) Size() int {
	return b.size
}

func (b *postgresqlBatch) Reset() error {
	b.ops = nil
	b.ops = make([]postgresqlBatchOp, 0)
	b.size = 0

	if b.tx != nil {
		_ = b.tx.Rollback(b.ctx)
	}
	tx, err := b.pool.Begin(b.ctx)
	if err != nil {
		return err
	}
	b.tx = tx
	return nil
}

func (b *postgresqlBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.tx == nil {
		return errBatchClosed
	}
	b.size += len(key) + len(value)
	b.ops = append(b.ops, postgresqlBatchOp{action: batchActionSet, key: key, value: value})
	return nil
}

func (b *postgresqlBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.tx == nil {
		return errBatchClosed
	}
	b.size += len(key)
	b.ops = append(b.ops, postgresqlBatchOp{action: batchActionDel, key: key})
	return nil
}

func (b *postgresqlBatch) Write() (err error) {
	if b.tx == nil {
		return errBatchClosed
	}

	defer func() {
		if err != nil && b.tx != nil {
			_ = b.tx.Rollback(b.ctx)
			b.tx = nil
		}
	}()

	if len(b.ops) == 0 {
		return nil
	}

	var (
		setOps []postgresqlBatchOp
		delOps []postgresqlBatchOp
	)

	for _, op := range b.ops {
		switch op.action {
		case batchActionSet:
			setOps = append(setOps, op)
		case batchActionDel:
			delOps = append(delOps, op)
		}
	}

	// Bulk insert/update for sets
	if len(setOps) > 0 {
		valueStrings := make([]string, 0, len(setOps))
		valueArgs := make([]any, 0, len(setOps)*2)

		for i, op := range setOps {
			// ($1,$2), ($3,$4), ...
			idx := i*2 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d)", idx, idx+1))
			valueArgs = append(valueArgs, op.key, op.value)
		}

		stmt := fmt.Sprintf(`
            INSERT INTO state_storage (key, value)
            VALUES %s
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        `, strings.Join(valueStrings, ","))

		if _, err = b.tx.Exec(b.ctx, stmt, valueArgs...); err != nil {
			return fmt.Errorf("failed to bulk upsert: %w", err)
		}
	}

	// Bulk delete for dels
	if len(delOps) > 0 {
		valueStrings := make([]string, 0, len(delOps))
		valueArgs := make([]any, 0, len(delOps))

		for i, op := range delOps {
			idx := i + 1
			valueStrings = append(valueStrings, fmt.Sprintf("$%d", idx))
			valueArgs = append(valueArgs, op.key)
		}

		stmt := fmt.Sprintf(`DELETE FROM state_storage WHERE key IN (%s)`, strings.Join(valueStrings, ","))
		if _, err = b.tx.Exec(b.ctx, stmt, valueArgs...); err != nil {
			return fmt.Errorf("failed to bulk delete: %w", err)
		}
	}

	if err = b.tx.Commit(b.ctx); err != nil {
		return fmt.Errorf("failed to commit PostgreSQL transaction: %w", err)
	}
	b.tx = nil
	return nil
}

func (b *postgresqlBatch) Close() error {
	if b.tx != nil {
		err := b.tx.Rollback(b.ctx)
		if err != nil {
			return err
		}
		b.tx = nil
		b.db.RemoveBatch(b.index)
	}
	return nil
}

func (b *postgresqlBatch) GetByteSize() (int, error) {
	if b.tx == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}

func (b *postgresqlBatch) WriteSync() error {
	if b.tx == nil {
		return errBatchClosed
	}
	err := b.Write()
	if err != nil {
		return err
	}
	return b.Close()
}
