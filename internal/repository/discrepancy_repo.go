package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/wakala/reconciler/internal/domain"
)

type DiscrepancyRepo struct {
	db *sql.DB
}

func NewDiscrepancyRepo(db *sql.DB) *DiscrepancyRepo {
	return &DiscrepancyRepo{db: db}
}

func (r *DiscrepancyRepo) Insert(d *domain.Discrepancy) error {
	var txnID, settID any
	if d.TransactionID != "" {
		txnID = d.TransactionID
	}
	if d.SettlementID != "" {
		settID = d.SettlementID
	}

	_, err := r.db.Exec(
		`INSERT INTO discrepancies
		(id, type, transaction_id, settlement_id, processor, expected_usd,
		 actual_usd, difference_usd, currency, severity, description, detected_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		d.ID, string(d.Type), txnID, settID, string(d.Processor),
		d.ExpectedUSD, d.ActualUSD, d.DifferenceUSD, d.Currency,
		string(d.Severity), d.Description, d.DetectedAt.Format(time.RFC3339),
	)
	return err
}

func (r *DiscrepancyRepo) BulkInsert(discs []domain.Discrepancy) (int, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		`INSERT OR IGNORE INTO discrepancies
		(id, type, transaction_id, settlement_id, processor, expected_usd,
		 actual_usd, difference_usd, currency, severity, description, detected_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return 0, fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	inserted := 0
	for i := range discs {
		d := &discs[i]
		var txnID, settID any
		if d.TransactionID != "" {
			txnID = d.TransactionID
		}
		if d.SettlementID != "" {
			settID = d.SettlementID
		}
		res, err := stmt.Exec(
			d.ID, string(d.Type), txnID, settID, string(d.Processor),
			d.ExpectedUSD, d.ActualUSD, d.DifferenceUSD, d.Currency,
			string(d.Severity), d.Description, d.DetectedAt.Format(time.RFC3339),
		)
		if err != nil {
			return inserted, fmt.Errorf("insert %d: %w", i, err)
		}
		ra, _ := res.RowsAffected()
		inserted += int(ra)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return inserted, nil
}

// GetByTransactionID returns all discrepancies related to a transaction.
func (r *DiscrepancyRepo) GetByTransactionID(txnID string) ([]domain.Discrepancy, error) {
	rows, err := r.db.Query(
		"SELECT * FROM discrepancies WHERE transaction_id = ? ORDER BY detected_at DESC", txnID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDiscrepancies(rows)
}

type DiscrepancyFilter struct {
	Type      string
	Severity  string
	Processor string
	From      *time.Time
	To        *time.Time
	Page      int
	Limit     int
}

func (r *DiscrepancyRepo) List(f DiscrepancyFilter) ([]domain.Discrepancy, int, error) {
	where, args := buildDiscrepancyWhere(f)

	var total int
	if err := r.db.QueryRow("SELECT COUNT(*) FROM discrepancies"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	q := "SELECT * FROM discrepancies" + where + " ORDER BY detected_at DESC LIMIT ? OFFSET ?"
	args = append(args, f.Limit, offset)

	rows, err := r.db.Query(q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	discs, err := scanDiscrepancies(rows)
	return discs, total, err
}

type DiscrepancySummary struct {
	TotalCount    int                `json:"total_count"`
	TotalImpact   float64            `json:"total_impact_usd"`
	ByType        map[string]int     `json:"by_type"`
	BySeverity    map[string]int     `json:"by_severity"`
	ByProcessor   map[string]int     `json:"by_processor"`
	ImpactByProc  map[string]float64 `json:"impact_by_processor"`
}

func (r *DiscrepancyRepo) GetSummary() (*DiscrepancySummary, error) {
	s := &DiscrepancySummary{
		ByType:       make(map[string]int),
		BySeverity:   make(map[string]int),
		ByProcessor:  make(map[string]int),
		ImpactByProc: make(map[string]float64),
	}

	if err := r.db.QueryRow(
		"SELECT COUNT(*), COALESCE(SUM(ABS(difference_usd)),0) FROM discrepancies",
	).Scan(&s.TotalCount, &s.TotalImpact); err != nil {
		return nil, err
	}

	if err := scanGroupCount(r.db, "type", s.ByType); err != nil {
		return nil, err
	}
	if err := scanGroupCount(r.db, "severity", s.BySeverity); err != nil {
		return nil, err
	}
	if err := scanGroupCount(r.db, "processor", s.ByProcessor); err != nil {
		return nil, err
	}

	rows, err := r.db.Query(
		"SELECT processor, COALESCE(SUM(ABS(difference_usd)),0) FROM discrepancies GROUP BY processor",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p string
		var v float64
		if err := rows.Scan(&p, &v); err != nil {
			return nil, err
		}
		s.ImpactByProc[p] = v
	}

	return s, rows.Err()
}

// ClearAll removes all discrepancies (useful before re-running reconciliation).
func (r *DiscrepancyRepo) ClearAll() error {
	_, err := r.db.Exec("DELETE FROM discrepancies")
	return err
}

type ProcessorDiscrepancyStat struct {
	Processor        string  `json:"processor"`
	DiscrepancyCount int     `json:"discrepancy_count"`
	ImpactUSD        float64 `json:"discrepancy_impact_usd"`
}

func (r *DiscrepancyRepo) GetStatsByProcessor() ([]ProcessorDiscrepancyStat, error) {
	rows, err := r.db.Query(`
		SELECT processor, COUNT(*), COALESCE(SUM(ABS(difference_usd)),0)
		FROM discrepancies GROUP BY processor
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []ProcessorDiscrepancyStat
	for rows.Next() {
		var s ProcessorDiscrepancyStat
		if err := rows.Scan(&s.Processor, &s.DiscrepancyCount, &s.ImpactUSD); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

// --- helpers ---

func buildDiscrepancyWhere(f DiscrepancyFilter) (string, []any) {
	var clauses []string
	var args []any

	if f.Type != "" {
		clauses = append(clauses, "type = ?")
		args = append(args, f.Type)
	}
	if f.Severity != "" {
		clauses = append(clauses, "severity = ?")
		args = append(args, f.Severity)
	}
	if f.Processor != "" {
		clauses = append(clauses, "processor = ?")
		args = append(args, f.Processor)
	}
	if f.From != nil {
		clauses = append(clauses, "detected_at >= ?")
		args = append(args, f.From.Format(time.RFC3339))
	}
	if f.To != nil {
		clauses = append(clauses, "detected_at <= ?")
		args = append(args, f.To.Format(time.RFC3339))
	}

	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func scanGroupCount(db *sql.DB, col string, m map[string]int) error {
	rows, err := db.Query(
		"SELECT " + col + ", COUNT(*) FROM discrepancies GROUP BY " + col,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v int
		if err := rows.Scan(&k, &v); err != nil {
			return err
		}
		m[k] = v
	}
	return rows.Err()
}

func scanDiscrepancies(rows *sql.Rows) ([]domain.Discrepancy, error) {
	var discs []domain.Discrepancy
	for rows.Next() {
		var d domain.Discrepancy
		var dtype, proc, sev, detectedAt string
		var txnIDNull, settIDNull sql.NullString

		err := rows.Scan(
			&d.ID, &dtype, &txnIDNull, &settIDNull, &proc,
			&d.ExpectedUSD, &d.ActualUSD, &d.DifferenceUSD,
			&d.Currency, &sev, &d.Description, &detectedAt,
		)
		if err != nil {
			return nil, err
		}

		d.Type = domain.DiscrepancyType(dtype)
		d.Processor = domain.Processor(proc)
		d.Severity = domain.Severity(sev)
		d.DetectedAt, _ = time.Parse(time.RFC3339, detectedAt)
		if txnIDNull.Valid {
			d.TransactionID = txnIDNull.String
		}
		if settIDNull.Valid {
			d.SettlementID = settIDNull.String
		}

		discs = append(discs, d)
	}
	return discs, rows.Err()
}
