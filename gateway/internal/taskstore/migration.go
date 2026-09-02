package taskstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// CurrentSchemaVersion is deliberately exported for diagnostics and release
	// tooling. A newer on-disk schema is never opened by an older binary.
	CurrentSchemaVersion = 2
	databasePragmas      = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
`
)

func migrate(db *sql.DB, path string, existing bool) error {
	version, err := schemaVersionDB(context.Background(), db)
	if err != nil {
		return fmt.Errorf("taskstore: read schema version: %w", err)
	}
	if version > CurrentSchemaVersion {
		return fmt.Errorf("taskstore: database schema version %d is newer than supported version %d; upgrade IndexQube", version, CurrentSchemaVersion)
	}
	if version == CurrentSchemaVersion {
		return nil
	}

	hasSchema, err := tableExists(db, "tasks")
	if err != nil {
		return fmt.Errorf("taskstore: inspect legacy schema: %w", err)
	}
	if existing && hasSchema {
		backupPath := migrationBackupPath(path, version, time.Now().UTC())
		if err := backupDatabase(context.Background(), db, backupPath); err != nil {
			return fmt.Errorf("taskstore: back up schema version %d before migration: %w", version, err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("taskstore: begin migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(schema); err != nil {
		return fmt.Errorf("taskstore: apply schema version %d: %w", CurrentSchemaVersion, err)
	}
	// Version zero includes databases created before migrations were explicit.
	// Keep its only historical additive change idempotent here.
	if err := ensureColumn(tx, "route_attempts", "fallback_eligible",
		`ALTER TABLE route_attempts ADD COLUMN fallback_eligible INTEGER NOT NULL DEFAULT 0`); err != nil {
		return fmt.Errorf("taskstore: migrate fallback eligibility: %w", err)
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", CurrentSchemaVersion)); err != nil {
		return fmt.Errorf("taskstore: record schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("taskstore: commit migration: %w", err)
	}
	return nil
}

func tableExists(db *sql.DB, table string) (bool, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count)
	return count != 0, err
}

func schemaVersionDB(ctx context.Context, db *sql.DB) (int, error) {
	var version int
	err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	return schemaVersionDB(ctx, s.db)
}

func integrityCheckDB(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `PRAGMA quick_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var problems []string
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			problems = append(problems, result)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(problems) != 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (s *Store) IntegrityCheck(ctx context.Context) error {
	return integrityCheckDB(ctx, s.db)
}

// Backup writes a consistent SQLite snapshot without copying live WAL files.
// The destination must not exist, preventing accidental backup replacement.
func (s *Store) Backup(ctx context.Context, destination string) error {
	return backupDatabase(ctx, s.db, destination)
}

func backupDatabase(ctx context.Context, db *sql.DB, destination string) error {
	if strings.TrimSpace(destination) == "" {
		return fmt.Errorf("empty backup destination")
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("backup destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	// VACUUM INTO produces a transactionally consistent standalone database.
	quoted := strings.ReplaceAll(destination, "'", "''")
	if _, err := db.ExecContext(ctx, `VACUUM INTO '`+quoted+`'`); err != nil {
		return err
	}
	if err := os.Chmod(destination, 0o600); err != nil {
		return err
	}
	return nil
}

func migrationBackupPath(path string, version int, now time.Time) string {
	stamp := now.Format("20060102T150405.000000000Z")
	return fmt.Sprintf("%s.backup-v%d-%s", path, version, stamp)
}

type DatabaseDiagnostics struct {
	SchemaVersion int
	Integrity     string
}

// Diagnose performs read-only checks and never creates, migrates, or backs up
// the target database. It is safe for doctor and support tooling.
func Diagnose(ctx context.Context, path string) (DatabaseDiagnostics, error) {
	u := &url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return DatabaseDiagnostics{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	version, err := schemaVersionDB(ctx, db)
	if err != nil {
		return DatabaseDiagnostics{}, err
	}
	if err := integrityCheckDB(ctx, db); err != nil {
		return DatabaseDiagnostics{SchemaVersion: version, Integrity: "corrupt"}, err
	}
	return DatabaseDiagnostics{SchemaVersion: version, Integrity: "ok"}, nil
}
