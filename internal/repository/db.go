package repository

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// InitDB opens (or creates) a SQLite database at the given path and ensures
// all required tables exist. Pass ":memory:" for an in-memory database.
func InitDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// Enable WAL mode for better concurrent read performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set wal mode: %w", err)
	}

	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	if err := createTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}

	return db, nil
}

func createTables(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS transactions (
			id TEXT PRIMARY KEY,
			processor_reference TEXT NOT NULL,
			processor TEXT NOT NULL,
			merchant_id TEXT NOT NULL,
			customer_country TEXT NOT NULL,
			merchant_country TEXT NOT NULL,
			amount REAL NOT NULL,
			currency TEXT NOT NULL,
			usd_amount REAL NOT NULL,
			status TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			captured_at DATETIME,
			settled_at DATETIME
		)`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_processor ON transactions(processor)`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_processor_ref ON transactions(processor_reference)`,
		`CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)`,

		`CREATE TABLE IF NOT EXISTS settlement_reports (
			id TEXT PRIMARY KEY,
			processor TEXT NOT NULL,
			report_date DATETIME NOT NULL,
			batch_id TEXT NOT NULL,
			file_hash TEXT UNIQUE NOT NULL,
			record_count INTEGER NOT NULL,
			ingested_at DATETIME NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_settlement_reports_processor ON settlement_reports(processor)`,

		`CREATE TABLE IF NOT EXISTS settlement_records (
			id TEXT PRIMARY KEY,
			report_id TEXT NOT NULL,
			processor TEXT NOT NULL,
			processor_transaction_id TEXT NOT NULL,
			wakala_transaction_id TEXT,
			gross_amount REAL NOT NULL,
			fee_amount REAL NOT NULL,
			net_amount REAL NOT NULL,
			currency TEXT NOT NULL,
			usd_gross_amount REAL NOT NULL DEFAULT 0,
			usd_net_amount REAL NOT NULL,
			settlement_date DATETIME NOT NULL,
			batch_id TEXT NOT NULL,
			FOREIGN KEY (report_id) REFERENCES settlement_reports(id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_settlement_records_report ON settlement_records(report_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settlement_records_proc_txn ON settlement_records(processor_transaction_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settlement_records_wakala_txn ON settlement_records(wakala_transaction_id)`,

		`CREATE TABLE IF NOT EXISTS discrepancies (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			transaction_id TEXT,
			settlement_id TEXT,
			processor TEXT NOT NULL,
			expected_usd REAL NOT NULL,
			actual_usd REAL NOT NULL,
			difference_usd REAL NOT NULL,
			currency TEXT NOT NULL,
			severity TEXT NOT NULL,
			description TEXT NOT NULL,
			detected_at DATETIME NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_discrepancies_type ON discrepancies(type)`,
		`CREATE INDEX IF NOT EXISTS idx_discrepancies_severity ON discrepancies(severity)`,
		`CREATE INDEX IF NOT EXISTS idx_discrepancies_processor ON discrepancies(processor)`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt[:60], err)
		}
	}

	return nil
}
