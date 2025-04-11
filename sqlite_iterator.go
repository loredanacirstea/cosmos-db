package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

var _ Iterator = (*sqliteIterator)(nil)

type sqliteIterator struct {
	statement  *sql.Stmt
	rows       *sql.Rows
	key, val   []byte
	start, end []byte
	valid      bool
	err        error
}

func newSqliteIterator(db *SqliteDb, start, end []byte, reverse bool) (*sqliteIterator, error) {
	var (
		keyClause = []string{}
		queryArgs = []any{}
	)

	switch {
	case start != nil && end != nil:
		if reverse {
			keyClause = append(keyClause, "key >= ?", "key < ?")
		} else {
			keyClause = append(keyClause, "key >= ?", "key < ?")
		}
		queryArgs = []any{start, end}

	case start != nil && end == nil:
		if reverse {
			keyClause = append(keyClause, "key >= ?")
		} else {
			keyClause = append(keyClause, "key >= ?")
		}
		queryArgs = []any{start}

	case start == nil && end != nil:
		if reverse {
			keyClause = append(keyClause, "key < ?") // <=
		} else {
			keyClause = append(keyClause, "key < ?")
		}
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

	// Note, this is not susceptible to SQL injection because placeholders are used
	// for parts of the query outside the store's direct control.
	cmd := fmt.Sprintf(`
	SELECT x.key, x.value
	FROM (
		SELECT key, value,
			row_number() OVER (PARTITION BY key) AS _rn
			FROM state_storage WHERE %s
		) x
	WHERE x._rn = 1 ORDER BY x.key %s;
	`, whereClause, orderBy)
	stmt, err := db.db.Prepare(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare iterator SQL statement: %w", err)
	}

	rows, err := stmt.Query(queryArgs...)
	if err != nil {
		_ = stmt.Close()
		return nil, fmt.Errorf("failed to execute iterator SQL query: %w", err)
	}

	itr := &sqliteIterator{
		statement: stmt,
		rows:      rows,
		start:     start,
		end:       end,
		valid:     rows.Next(),
	}

	if !itr.valid {
		return itr, nil
	}

	// read the first row
	itr.parseRow()
	if !itr.valid {
		return itr, nil
	}

	return itr, nil
}

func (itr *sqliteIterator) Close() (err error) {
	if itr.statement != nil {
		err = itr.statement.Close()
	}

	itr.valid = false
	itr.statement = nil
	itr.rows = nil

	return err
}

// Domain returns the domain of the iterator. The caller must not modify the
// return values.
func (itr *sqliteIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *sqliteIterator) Key() []byte {
	itr.assertIsValid()
	return slices.Clone(itr.key)
}

func (itr *sqliteIterator) Value() []byte {
	itr.assertIsValid()
	return slices.Clone(itr.val)
}

func (itr *sqliteIterator) Valid() bool {
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

func (itr *sqliteIterator) Next() {
	itr.assertIsValid()
	if itr.rows.Next() {
		itr.parseRow()
		return
	}

	itr.valid = false
}

func (itr *sqliteIterator) Error() error {
	if err := itr.rows.Err(); err != nil {
		return err
	}

	return itr.err
}

func (itr *sqliteIterator) parseRow() {
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

func (itr *sqliteIterator) assertIsValid() {
	if !itr.valid {
		panic("iterator is invalid")
	}
}
