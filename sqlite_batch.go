package db

import (
	"database/sql"
	"fmt"
)

var _ Batch = (*sqliteBatch)(nil)

type batchAction int

const (
	batchActionSet batchAction = 0
	batchActionDel batchAction = 1
)

type sqliteBatchOp struct {
	action     batchAction
	key, value []byte
}

type sqliteBatch struct {
	db   *sql.DB
	tx   *sql.Tx
	ops  []sqliteBatchOp
	size int
}

func NewBatch(db *sql.DB) (*sqliteBatch, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL transaction: %w", err)
	}

	return &sqliteBatch{
		db:  db,
		tx:  tx,
		ops: make([]sqliteBatchOp, 0),
	}, nil
}

func (b *sqliteBatch) Size() int {
	return b.size
}

func (b *sqliteBatch) Reset() error {
	b.ops = nil
	b.ops = make([]sqliteBatchOp, 0)
	b.size = 0

	tx, err := b.db.Begin()
	if err != nil {
		return err
	}

	b.tx = tx
	return nil
}

func (b *sqliteBatch) Set(key, value []byte) error {
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
	b.ops = append(b.ops, sqliteBatchOp{action: batchActionSet, key: key, value: value})
	return nil
}

func (b *sqliteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.tx == nil {
		return errBatchClosed
	}
	b.size += len(key)
	b.ops = append(b.ops, sqliteBatchOp{action: batchActionDel, key: key})
	return nil
}

func (b *sqliteBatch) Write() error {
	if b.tx == nil {
		return errBatchClosed
	}
	for _, op := range b.ops {
		switch op.action {
		case batchActionSet:
			_, err := b.tx.Exec(upsertStmt, op.key, op.value, op.value)
			if err != nil {
				return fmt.Errorf("failed to exec batch set SQL statement: %w", err)
			}

		case batchActionDel:
			_, err := b.tx.Exec(delStmt, op.key)
			if err != nil {
				return fmt.Errorf("failed to exec batch del SQL statement: %w", err)
			}
		}
	}

	if err := b.tx.Commit(); err != nil {
		return fmt.Errorf("failed to write SQL transaction: %w", err)
	}
	b.tx = nil

	return nil
}

// Close implements Batch.
func (b *sqliteBatch) Close() error {
	if b.tx != nil {
		err := b.tx.Rollback()
		if err != nil {
			return err
		}
		b.tx = nil
	}
	return nil
}

func (b *sqliteBatch) GetByteSize() (int, error) {
	if b.tx == nil {
		return 0, errBatchClosed
	}
	return b.size, nil
}

// WriteSync implements Batch.
func (b *sqliteBatch) WriteSync() error {
	if b.tx == nil {
		return errBatchClosed
	}
	// err := b.db.db.Write(b.db.woSync, b.batch)
	err := b.Write()
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}
