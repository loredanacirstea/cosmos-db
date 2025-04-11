package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

func init() {
	dbCreator := func(name string, dir string, opts Options) (DB, error) {
		return NewSqliteDb(name, dir, opts)
	}
	registerDBCreator(SqliteBackend, dbCreator, false)
}

type SqliteDb struct {
	db *sql.DB
}

var _ DB = (*SqliteDb)(nil)

const (
	driverName = "sqlite3"
	// dbName     = "ss.db?cache=shared&mode=rwc&_journal_mode=WAL"

	reservedUpsertStmt = `
	INSERT INTO state_storage(key, value)
    VALUES(?, ?)
  ON CONFLICT(key) DO UPDATE SET
    value = ?;
	`
	upsertStmt = `
	INSERT INTO state_storage(key, value)
    VALUES(?, ?)
  ON CONFLICT(key) DO UPDATE SET
    value = ?;
	`
	delStmt = `DELETE FROM state_storage WHERE key = ?;`
)

func NewSqliteDb(name string, dir string, opts Options) (*SqliteDb, error) {
	return NewSqliteDbWithOpts(name, dir, opts)
}

func NewSqliteDbWithOpts(name string, dir string, opts Options) (*SqliteDb, error) {
	dbPath := filepath.Join(dir, name+DBFileSuffix)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create DB directory '%s': %w", dir, err)
	}

	db, err := sql.Open(driverName, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite DB '%s': %w", dbPath, err)
	}

	stmt := `
	CREATE TABLE IF NOT EXISTS state_storage (
		id integer not null primary key,
		key varchar not null,
		value varchar not null,
		unique (key)
	);

	CREATE UNIQUE INDEX IF NOT EXISTS idx_key ON state_storage (key);
	`
	// stmt := `
	// CREATE TABLE IF NOT EXISTS state_storage (
	// 	id integer not null primary key,
	// 	key BLOB,
	// 	value    BLOB,
	// );

	// CREATE UNIQUE INDEX IF NOT EXISTS idx_key ON state_storage (key);
	// `
	_, err = db.Exec(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to exec SQL statement: %w", err)
	}
	return &SqliteDb{db: db}, nil
}

func (s *SqliteDb) Close() error {
	var err error
	if s.db != nil {
		err = s.db.Close()
	}
	s.db = nil
	return err
}
func (s *SqliteDb) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	_, err := s.db.Exec(delStmt, key)
	if err != nil {
		return fmt.Errorf("failed to prepare SQL delete statement: %w", err)
	}
	return err
}

// Get([]byte) ([]byte, error)
func (s *SqliteDb) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	stmt, err := s.db.Prepare(`
	SELECT value FROM state_storage
	WHERE key = ?
	LIMIT 1;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}

	defer stmt.Close()

	var (
		value []byte
	)
	if err := stmt.QueryRow(key).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to query row: %w", err)
	}
	return value, nil
}

// Has(key []byte) (bool, error)
func (s *SqliteDb) Has(key []byte) (bool, error) {
	value, err := s.Get(key)
	if err != nil {
		return false, err
	}
	return value != nil, nil
}
func (s *SqliteDb) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	_, err := s.db.Exec(upsertStmt, key, value, value)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqliteDb) SetSync(key []byte, value []byte) error {
	return s.Set(key, value)
}

func (s *SqliteDb) DeleteSync(key []byte) error {
	return s.Delete(key)
}

func (s *SqliteDb) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	return newSqliteIterator(s, start, end, false)
}

func (s *SqliteDb) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	return newSqliteIterator(s, start, end, true)
}

func (s *SqliteDb) NewBatch() Batch {
	batch, err := NewBatch(s.db)
	if err != nil {
		panic(err)
	}
	return batch
}

func (s *SqliteDb) NewBatchWithSize(size int) Batch {
	return s.NewBatch()
}

func (s *SqliteDb) Print() error {
	itr, err := s.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

func (s *SqliteDb) Stats() map[string]string {
	// _stats := s.db.Stats()
	stats := make(map[string]string, 0)
	// for _, key := range keys {
	// 	stats[key] = s.db.Stats() // s.db.GetProperty(key)
	// }
	return stats
}
