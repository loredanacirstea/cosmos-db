package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ Batch = (*postgresqlBatch)(nil)
var _ PostgreSqlBatch = (*postgresqlBatch)(nil)

type PostgreSqlBatch interface {
	Batch
	Tx() pgx.Tx
}

type postgresqlBatchOp struct {
	action     batchAction
	key, value []byte
}

type postgresqlBatch struct {
	db     *PostgreSQLDb
	index  int64
	pool   *pgxpool.Pool
	ctx    context.Context
	tx     pgx.Tx
	ops    []postgresqlBatchOp
	size   int
	closed bool
}

func NewPostgreSQLBatch(pool *pgxpool.Pool, ctx context.Context, db *PostgreSQLDb, index int64) (*postgresqlBatch, error) {
	return &postgresqlBatch{
		db:    db,
		index: index,
		pool:  pool,
		ctx:   ctx,
		ops:   make([]postgresqlBatchOp, 0),
	}, nil
}

func (b *postgresqlBatch) Size() int {
	return b.size
}

func (b *postgresqlBatch) Reset() error {
	if b.closed {
		return errBatchClosed
	}
	if b.tx != nil {
		_ = b.tx.Rollback(b.ctx)
		b.tx = nil
	}
	b.ops = nil
	b.ops = make([]postgresqlBatchOp, 0)
	b.size = 0
	return nil
}

func (b *postgresqlBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.closed {
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
	if b.closed {
		return errBatchClosed
	}
	b.size += len(key)
	b.ops = append(b.ops, postgresqlBatchOp{action: batchActionDel, key: key})
	return nil
}

func (b *postgresqlBatch) Write() (err error) {
	if b.closed {
		return errBatchClosed
	}
	if len(b.ops) == 0 {
		return nil
	}
	if err := b.ensureTx(); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = b.tx.Rollback(b.ctx)
			b.tx = nil
			b.finalize()
		}
	}()

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
	b.finalize()
	return nil
}

func (b *postgresqlBatch) Close() error {
	if b.closed {
		return nil
	}
	if b.tx != nil {
		if err := b.tx.Rollback(b.ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			return err
		}
		b.tx = nil
	}
	b.finalize()
	return nil
}

func (b *postgresqlBatch) GetByteSize() (int, error) {
	if b.closed {
		return 0, errBatchClosed
	}
	return b.size, nil
}

func (b *postgresqlBatch) WriteSync() error {
	if b.closed {
		return errBatchClosed
	}
	return b.Write()
}

func (b *postgresqlBatch) Tx() pgx.Tx {
	if err := b.ensureTx(); err != nil {
		panic(err)
	}
	return b.tx
}

func (b *postgresqlBatch) finalize() {
	if b.closed {
		return
	}
	b.closed = true
	b.ops = nil
	b.size = 0
	b.db.RemoveBatch(b.index)
}

// ensure a transaction (and therefore a connection) is only opened the moment
// you first touch Tx() or call Write()
func (b *postgresqlBatch) ensureTx() error {
	if b.closed {
		return errBatchClosed
	}
	if b.tx != nil {
		return nil
	}
	tx, err := b.pool.BeginTx(b.ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL transaction: %w", err)
	}
	b.tx = tx
	return nil
}
