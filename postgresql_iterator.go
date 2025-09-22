package db

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ Iterator = (*postgresqlIterator)(nil)

type postgresqlIterator struct {
	pool       *pgxpool.Pool
	ctx        context.Context
	rows       pgx.Rows
	key, val   []byte
	start, end []byte
	valid      bool
	err        error
}

func newPostgreSQLIterator(db *PostgreSQLDb, start, end []byte, reverse bool) (*postgresqlIterator, error) {
	var (
		keyClause = []string{}
		queryArgs = []any{}
	)

	switch {
	case start != nil && end != nil:
		keyClause = append(keyClause, "key >= $1", "key < $2")
		queryArgs = []any{start, end}

	case start != nil && end == nil:
		keyClause = append(keyClause, "key >= $1")
		queryArgs = []any{start}

	case start == nil && end != nil:
		keyClause = append(keyClause, "key < $1")
		queryArgs = []any{end}

	default:
		queryArgs = []any{}
	}

	orderBy := "ASC"
	if reverse {
		orderBy = "DESC"
	}

	whereClause := "1=1"
	if len(keyClause) > 0 {
		whereClause = strings.Join(keyClause, " AND ")
	}

	cmd := fmt.Sprintf(`SELECT key, value
FROM state_storage
WHERE %s
ORDER BY key %s;`, whereClause, orderBy)

	rows, err := db.pool.Query(db.ctx, cmd, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute iterator PostgreSQL query: %w", err)
	}

	itr := &postgresqlIterator{
		pool:  db.pool,
		ctx:   db.ctx,
		rows:  rows,
		start: start,
		end:   end,
		valid: rows.Next(),
	}

	if !itr.valid {
		return itr, nil
	}

	itr.parseRow()
	if !itr.valid {
		return itr, nil
	}
	return itr, nil
}

func (itr *postgresqlIterator) Close() (err error) {
	if itr.rows != nil {
		itr.rows.Close()
	}

	itr.valid = false
	itr.rows = nil
	return nil
}

func (itr *postgresqlIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *postgresqlIterator) Key() []byte {
	itr.assertIsValid()
	return slices.Clone(itr.key)
}

func (itr *postgresqlIterator) Value() []byte {
	itr.assertIsValid()
	return slices.Clone(itr.val)
}

func (itr *postgresqlIterator) Valid() bool {
	if !itr.valid || itr.rows.Err() != nil {
		itr.valid = false
		return itr.valid
	}
	key := itr.Key()
	if end := itr.end; end != nil && bytes.Compare(key, end) >= 0 {
		itr.valid = false
		return itr.valid
	}

	if start := itr.start; start != nil && bytes.Compare(key, start) < 0 {
		itr.valid = false
		return itr.valid
	}

	return true
}

func (itr *postgresqlIterator) Next() {
	itr.assertIsValid()
	if itr.rows.Next() {
		itr.parseRow()
		return
	}

	itr.valid = false
}

func (itr *postgresqlIterator) Error() error {
	if err := itr.rows.Err(); err != nil {
		return err
	}

	return itr.err
}

func (itr *postgresqlIterator) parseRow() {
	var (
		key   []byte
		value []byte
	)
	if err := itr.rows.Scan(&key, &value); err != nil {
		itr.err = fmt.Errorf("failed to scan row: %w", err)
		itr.valid = false
		return
	}

	itr.key = key
	itr.val = value
}

func (itr *postgresqlIterator) assertIsValid() {
	if !itr.valid {
		panic("iterator is invalid")
	}
}
