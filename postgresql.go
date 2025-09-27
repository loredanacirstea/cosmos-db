package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cast"
)

func init() {
	dbCreator := func(name string, dir string, opts Options) (DB, error) {
		return NewPostgreSQLDb(name, dir, opts)
	}
	registerDBCreator(PostgreSQLBackend, dbCreator, false)
}

type PostgreSQLDb struct {
	pool       *pgxpool.Pool
	ctx        context.Context
	ctxCancel  context.CancelFunc
	batchesMap map[int64]*postgresqlBatch
	counter    int64
}

var _ DB = (*PostgreSQLDb)(nil)

const (
	postgresqlUpsertStmt = `
		INSERT INTO state_storage(key, value)
		VALUES($1, $2)
		ON CONFLICT(key) DO UPDATE SET
			value = EXCLUDED.value;
	`
	postgresqlDelStmt = `DELETE FROM state_storage WHERE key = $1;`
)

func NewPostgreSQLDb(name string, dir string, opts Options) (*PostgreSQLDb, error) {
	return NewPostgreSQLDbWithOpts(name, dir, opts)
}

func NewPostgreSQLDbWithOpts(name string, dir string, opts Options) (*PostgreSQLDb, error) {
	dbname := strings.ReplaceAll(dir, "/", "_") + "_" + name
	connection := `postgresql://localhost:5432/postgres`
	if opts != nil {
		conn := cast.ToString(opts.Get("connection"))
		if conn != "" {
			connection = conn
		}
	}
	return NewPostgreSQLDbWithCtx(context.Background(), dbname, connection, opts)
}

func NewPostgreSQLDbWithCtx(parentCtx context.Context, dbname string, connection string, opts Options) (*PostgreSQLDb, error) {
	ctx, ctxCancel := context.WithCancel(parentCtx)
	config, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL config: %w", err)
	}

	// Set pool configuration
	config.MaxConns = 100
	config.MinConns = 5

	// TODO rest of config
	if opts != nil {
		maxconns := cast.ToInt(opts.Get("maxconns"))
		if maxconns > 0 {
			config.MaxConns = int32(maxconns)
		}
		minconns := cast.ToInt(opts.Get("minconns"))
		if minconns > 0 {
			config.MinConns = int32(minconns)
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	if dbname != "" {
		if err := ensureDatabase(ctx, pool, dbname); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create PostgreSQL database: %w", err)
		}
		// switch to our database
		config.ConnConfig.Database = dbname
		pool.Close()
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
		}
		// Test connection
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to ping PostgreSQL database: %w", err)
		}
	}

	// Main storage table
	createTableStmt := `
	CREATE TABLE IF NOT EXISTS state_storage (
		id SERIAL PRIMARY KEY,
		key BYTEA NOT NULL,
		value BYTEA NOT NULL,
		UNIQUE (key)
	);`
	if _, err = pool.Exec(ctx, createTableStmt); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create PostgreSQL table: %w", err)
	}

	createIndexStmt := `CREATE UNIQUE INDEX IF NOT EXISTS idx_key_unique ON state_storage (key);`
	if _, err = pool.Exec(ctx, createIndexStmt); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create PostgreSQL unique index: %w", err)
	}

	return &PostgreSQLDb{
		pool:       pool,
		ctx:        ctx,
		ctxCancel:  ctxCancel,
		counter:    0,
		batchesMap: make(map[int64]*postgresqlBatch),
	}, nil
}

func ensureDatabase(ctx context.Context, pool *pgxpool.Pool, dbName string) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName))
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42P04" {
			// 42P04 = duplicate_database, ignore
			log.Printf("database %s already exists", dbName)
		} else {
			return err
		}
	}
	return nil
}

func (p *PostgreSQLDb) Pool() *pgxpool.Pool {
	return p.pool
}

func (p *PostgreSQLDb) Close() error {
	if len(p.batchesMap) > 0 {
		fmt.Println("postgresql closing unclosed batches:", len(p.batchesMap))
	}
	for _, b := range p.batchesMap {
		b.Close()
	}
	if p.pool != nil {
		stats := p.pool.Stat()
		conns := stats.AcquiredConns()
		if conns > 0 {
			fmt.Println("postgresql unclosed connections:", conns)
			fmt.Println("postgres stats:", p.Stats())
		}
		p.pool.Close()
	}
	p.pool = nil
	p.ctxCancel()
	return nil
}

func (p *PostgreSQLDb) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	_, err := p.pool.Exec(p.ctx, postgresqlDelStmt, key)
	if err != nil {
		return fmt.Errorf("failed to execute PostgreSQL delete statement: %w", err)
	}
	return nil
}

func (p *PostgreSQLDb) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	var value []byte
	err := p.pool.QueryRow(p.ctx, `
		SELECT value FROM state_storage
		WHERE key = $1
		LIMIT 1;
	`, key).Scan(&value)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query row: %w", err)
	}
	return value, nil
}

func (p *PostgreSQLDb) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	var exists bool
	err := p.pool.QueryRow(p.ctx, `
		SELECT EXISTS (
			SELECT 1 FROM state_storage WHERE key = $1
		);
	`, key).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check key existence: %w", err)
	}
	if !exists {
		return false, nil
	}
	return true, nil
}

func (p *PostgreSQLDb) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	_, err := p.pool.Exec(p.ctx, postgresqlUpsertStmt, key, value)
	if err != nil {
		return fmt.Errorf("failed to execute PostgreSQL upsert statement: %w", err)
	}
	return nil
}

func (p *PostgreSQLDb) SetSync(key []byte, value []byte) error {
	return p.Set(key, value)
}

func (p *PostgreSQLDb) DeleteSync(key []byte) error {
	return p.Delete(key)
}

func (p *PostgreSQLDb) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	return newPostgreSQLIterator(p, start, end, false)
}

func (p *PostgreSQLDb) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}

	return newPostgreSQLIterator(p, start, end, true)
}

func (p *PostgreSQLDb) NewBatch() Batch {
	p.counter += 1
	batch, err := NewPostgreSQLBatch(p.pool, p.ctx, p, p.counter)
	if err != nil {
		panic(err)
	}
	p.batchesMap[p.counter] = batch
	return batch
}

func (p *PostgreSQLDb) NewBatchWithError() (PostgreSqlBatch, error) {
	p.counter += 1
	batch, err := NewPostgreSQLBatch(p.pool, p.ctx, p, p.counter)
	if err != nil {
		return nil, err
	}
	p.batchesMap[p.counter] = batch
	return batch, nil
}

func (p *PostgreSQLDb) NewBatchWithSize(size int) Batch {
	return p.NewBatch()
}

func (p *PostgreSQLDb) RemoveBatch(index int64) {
	delete(p.batchesMap, index)
}

func (p *PostgreSQLDb) Print() error {
	itr, err := p.Iterator(nil, nil)
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

func (p *PostgreSQLDb) Stats() map[string]string {
	stats := make(map[string]string)
	if p.pool != nil {
		poolStats := p.pool.Stat()
		stats["total_conns"] = fmt.Sprintf("%d", poolStats.TotalConns())
		stats["acquired_conns"] = fmt.Sprintf("%d", poolStats.AcquiredConns())
		stats["idle_conns"] = fmt.Sprintf("%d", poolStats.IdleConns())
		stats["max_conns"] = fmt.Sprintf("%d", poolStats.MaxConns())
	}
	return stats
}

// VacuumAnalyze triggers PostgreSQL to clean up and refresh planner statistics.
func (p *PostgreSQLDb) VacuumAnalyze() error {
	_, err := p.pool.Exec(p.ctx, `VACUUM ANALYZE state_storage;`)
	if err != nil {
		return fmt.Errorf("failed to run VACUUM ANALYZE: %w", err)
	}
	return nil
}
