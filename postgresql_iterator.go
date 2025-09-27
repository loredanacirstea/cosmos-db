package db

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresqlIterator struct {
	ctx       context.Context
	tx        pgx.Tx
	cursor    string
	pool      *pgxpool.Pool
	key       []byte
	val       []byte
	valid     bool
	err       error
	reverse   bool
	start     []byte
	end       []byte
	buffer    []kvPair
	exhausted bool
}

const fetchSize = 1000 // number of rows to fetch per round-trip

type kvPair struct {
	key []byte
	val []byte
}

var cursorCounter uint64

func newPostgreSQLIterator(db *PostgreSQLDb, start, end []byte, reverse bool) (*postgresqlIterator, error) {
	ctx := db.ctx

	// Use a read-only transaction to avoid lock contention with batch writes
	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin read-only tx for cursor: %w", err)
	}

	order := "ASC"
	if reverse {
		order = "DESC"
	}

	where := "TRUE"
	args := []any{}
	argPos := 1

	if start != nil {
		where += fmt.Sprintf(" AND key >= $%d", argPos)
		args = append(args, start)
		argPos++
	}
	if end != nil {
		where += fmt.Sprintf(" AND key < $%d", argPos)
		args = append(args, end)
		argPos++
	}

	cursorName := fmt.Sprintf("c_iter_%d", atomic.AddUint64(&cursorCounter, 1))
	declare := fmt.Sprintf(`DECLARE %s CURSOR FOR
		SELECT key, value
		FROM state_storage
		WHERE %s
		ORDER BY key %s`, cursorName, where, order)

	_, err = tx.Exec(ctx, declare, args...)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("failed to declare cursor: %w", err)
	}

	itr := &postgresqlIterator{
		ctx:     ctx,
		tx:      tx,
		cursor:  cursorName,
		pool:    db.pool,
		reverse: reverse,
		start:   start,
		end:     end,
	}
	itr.fetchNext()
	return itr, nil
}

func (itr *postgresqlIterator) fetchNext() {
	if itr.err != nil {
		itr.valid = false
		return
	}

	if len(itr.buffer) == 0 && !itr.exhausted {
		rows, err := itr.tx.Query(itr.ctx, fmt.Sprintf("FETCH %d FROM %s", fetchSize, itr.cursor))
		if err != nil {
			itr.err = fmt.Errorf("failed to fetch from cursor: %w", err)
			itr.valid = false
			return
		}
		for rows.Next() {
			var k, v []byte
			if err := rows.Scan(&k, &v); err != nil {
				itr.err = err
				itr.valid = false
				rows.Close()
				return
			}
			itr.buffer = append(itr.buffer, kvPair{key: slices.Clone(k), val: slices.Clone(v)})
		}
		if err := rows.Err(); err != nil {
			itr.err = err
			itr.valid = false
			rows.Close()
			return
		}
		rows.Close()
		if len(itr.buffer) == 0 {
			itr.exhausted = true
		}
	}

	if len(itr.buffer) == 0 {
		itr.valid = false
		return
	}

	next := itr.buffer[0]
	itr.buffer = itr.buffer[1:]
	itr.key = next.key
	itr.val = next.val
	itr.valid = true
}

func (itr *postgresqlIterator) Close() error {
	if itr.tx != nil {
		if _, err := itr.tx.Exec(itr.ctx, fmt.Sprintf("CLOSE %s", itr.cursor)); err != nil {
			return err
		}
		return itr.tx.Commit(itr.ctx)
	}
	return nil
}

func (itr *postgresqlIterator) Domain() ([]byte, []byte) { return itr.start, itr.end }
func (itr *postgresqlIterator) Key() []byte              { itr.assertIsValid(); return slices.Clone(itr.key) }
func (itr *postgresqlIterator) Value() []byte            { itr.assertIsValid(); return slices.Clone(itr.val) }
func (itr *postgresqlIterator) Valid() bool              { return itr.valid }
func (itr *postgresqlIterator) Error() error             { return itr.err }

func (itr *postgresqlIterator) Next() {
	itr.assertIsValid()
	itr.fetchNext()
}

func (itr *postgresqlIterator) assertIsValid() {
	if !itr.valid {
		panic("iterator is invalid")
	}
}
