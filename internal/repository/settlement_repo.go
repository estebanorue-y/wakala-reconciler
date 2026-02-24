package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/wakala/reconciler/internal/domain"
)

type SettlementRepo struct {
	db *sql.DB
}

func NewSettlementRepo(db *sql.DB) *SettlementRepo {
	return &SettlementRepo{db: db}
}

// ReportExistsByHash checks whether a report with the given file hash has
// already been ingested (idempotency check).
func (r *SettlementRepo) ReportExistsByHash(hash string) (bool, error) {
	var count int
	err := r.db.QueryRow(
		"SELECT COUNT(*) FROM settlement_reports WHERE file_hash = ?", hash,
	).Scan(&count)
	return count > 0, err
}

func (r *SettlementRepo) InsertReport(rpt *domain.SettlementReport) error {
	_, err := r.db.Exec(
		`INSERT INTO settlement_reports
		(id, processor, report_date, batch_id, file_hash, record_count, ingested_at)
		VALUES (?,?,?,?,?,?,?)`,
		rpt.ID, string(rpt.Processor), rpt.ReportDate.Format(time.RFC3339),
		rpt.BatchID, rpt.FileHash, rpt.RecordCount, rpt.IngestedAt.Format(time.RFC3339),
	)
	return err
}

func (r *SettlementRepo) InsertRecords(records []domain.SettlementRecord) (int, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		`INSERT OR IGNORE INTO settlement_records
		(id, report_id, processor, processor_transaction_id, wakala_transaction_id,
		 gross_amount, fee_amount, net_amount, currency, usd_gross_amount, usd_net_amount,
		 settlement_date, batch_id)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return 0, fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	inserted := 0
	for i := range records {
		rec := &records[i]
		var wakalaID any
		if rec.WakalaTransactionID != "" {
			wakalaID = rec.WakalaTransactionID
		}
		res, err := stmt.Exec(
			rec.ID, rec.ReportID, string(rec.Processor), rec.ProcessorTransactionID,
			wakalaID, rec.GrossAmount, rec.FeeAmount, rec.NetAmount, rec.Currency,
			rec.USDGrossAmount, rec.USDNetAmount, rec.SettlementDate.Format(time.RFC3339), rec.BatchID,
		)
		if err != nil {
			return inserted, fmt.Errorf("insert record %d: %w", i, err)
		}
		ra, _ := res.RowsAffected()
		inserted += int(ra)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return inserted, nil
}

// GetUnmatchedRecords returns settlement records that have not been matched
// to a Wakala transaction yet.
func (r *SettlementRepo) GetUnmatchedRecords() ([]domain.SettlementRecord, error) {
	rows, err := r.db.Query(
		"SELECT * FROM settlement_records WHERE wakala_transaction_id IS NULL",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []domain.SettlementRecord
	for rows.Next() {
		rec, err := scanSettlementRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}
	return records, rows.Err()
}

// GetMatchedRecords returns settlement records that have been matched
// to a Wakala transaction.
func (r *SettlementRepo) GetMatchedRecords() ([]domain.SettlementRecord, error) {
	rows, err := r.db.Query(
		"SELECT * FROM settlement_records WHERE wakala_transaction_id IS NOT NULL",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []domain.SettlementRecord
	for rows.Next() {
		rec, err := scanSettlementRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}
	return records, rows.Err()
}

// UpdateWakalaTransactionID sets the matched Wakala transaction ID on a
// settlement record.
func (r *SettlementRepo) UpdateWakalaTransactionID(recordID, txnID string) error {
	_, err := r.db.Exec(
		"UPDATE settlement_records SET wakala_transaction_id = ? WHERE id = ?",
		txnID, recordID,
	)
	return err
}

// GetByTransactionID returns settlement records matched to the given txn.
func (r *SettlementRepo) GetByTransactionID(txnID string) ([]domain.SettlementRecord, error) {
	rows, err := r.db.Query(
		"SELECT * FROM settlement_records WHERE wakala_transaction_id = ?", txnID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []domain.SettlementRecord
	for rows.Next() {
		rec, err := scanSettlementRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}
	return records, rows.Err()
}

type SettlementFilter struct {
	Processor string
	From      *time.Time
	To        *time.Time
	Page      int
	Limit     int
}

func (r *SettlementRepo) ListRecords(f SettlementFilter) ([]domain.SettlementRecord, int, error) {
	where, args := buildSettlementWhere(f)

	var total int
	if err := r.db.QueryRow("SELECT COUNT(*) FROM settlement_records"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	q := "SELECT * FROM settlement_records" + where + " ORDER BY settlement_date DESC LIMIT ? OFFSET ?"
	args = append(args, f.Limit, offset)

	rows, err := r.db.Query(q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var records []domain.SettlementRecord
	for rows.Next() {
		rec, err := scanSettlementRecord(rows)
		if err != nil {
			return nil, 0, err
		}
		records = append(records, *rec)
	}
	return records, total, rows.Err()
}

func buildSettlementWhere(f SettlementFilter) (string, []any) {
	var clauses []string
	var args []any

	if f.Processor != "" {
		clauses = append(clauses, "processor = ?")
		args = append(args, f.Processor)
	}
	if f.From != nil {
		clauses = append(clauses, "settlement_date >= ?")
		args = append(args, f.From.Format(time.RFC3339))
	}
	if f.To != nil {
		clauses = append(clauses, "settlement_date <= ?")
		args = append(args, f.To.Format(time.RFC3339))
	}

	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func scanSettlementRecord(rows *sql.Rows) (*domain.SettlementRecord, error) {
	var rec domain.SettlementRecord
	var proc, settleDateStr string
	var wakalaIDNull sql.NullString

	err := rows.Scan(
		&rec.ID, &rec.ReportID, &proc, &rec.ProcessorTransactionID,
		&wakalaIDNull, &rec.GrossAmount, &rec.FeeAmount, &rec.NetAmount,
		&rec.Currency, &rec.USDGrossAmount, &rec.USDNetAmount, &settleDateStr, &rec.BatchID,
	)
	if err != nil {
		return nil, err
	}

	rec.Processor = domain.Processor(proc)
	rec.SettlementDate, _ = time.Parse(time.RFC3339, settleDateStr)
	if wakalaIDNull.Valid {
		rec.WakalaTransactionID = wakalaIDNull.String
	}

	return &rec, nil
}
