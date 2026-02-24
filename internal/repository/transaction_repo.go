package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/wakala/reconciler/internal/domain"
)

type TransactionRepo struct {
	db *sql.DB
}

func NewTransactionRepo(db *sql.DB) *TransactionRepo {
	return &TransactionRepo{db: db}
}

func (r *TransactionRepo) Insert(tx *domain.Transaction) error {
	_, err := r.db.Exec(
		`INSERT OR IGNORE INTO transactions
		(id, processor_reference, processor, merchant_id, customer_country,
		 merchant_country, amount, currency, usd_amount, status, created_at,
		 captured_at, settled_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tx.ID, tx.ProcessorReference, string(tx.Processor), tx.MerchantID,
		tx.CustomerCountry, tx.MerchantCountry, tx.Amount, tx.Currency,
		tx.USDAmount, string(tx.Status), tx.CreatedAt.Format(time.RFC3339),
		formatNullableTime(tx.CapturedAt), formatNullableTime(tx.SettledAt),
	)
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}
	return nil
}

func (r *TransactionRepo) BulkInsert(txns []domain.Transaction) (int, error) {
	inserted := 0
	sqlTx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer sqlTx.Rollback()

	stmt, err := sqlTx.Prepare(
		`INSERT OR IGNORE INTO transactions
		(id, processor_reference, processor, merchant_id, customer_country,
		 merchant_country, amount, currency, usd_amount, status, created_at,
		 captured_at, settled_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return 0, fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for i := range txns {
		tx := &txns[i]
		res, err := stmt.Exec(
			tx.ID, tx.ProcessorReference, string(tx.Processor), tx.MerchantID,
			tx.CustomerCountry, tx.MerchantCountry, tx.Amount, tx.Currency,
			tx.USDAmount, string(tx.Status), tx.CreatedAt.Format(time.RFC3339),
			formatNullableTime(tx.CapturedAt), formatNullableTime(tx.SettledAt),
		)
		if err != nil {
			return inserted, fmt.Errorf("insert row %d: %w", i, err)
		}
		ra, _ := res.RowsAffected()
		inserted += int(ra)
	}

	if err := sqlTx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return inserted, nil
}

func (r *TransactionRepo) Count() (int, error) {
	var count int
	err := r.db.QueryRow("SELECT COUNT(*) FROM transactions").Scan(&count)
	return count, err
}

func (r *TransactionRepo) GetByID(id string) (*domain.Transaction, error) {
	row := r.db.QueryRow("SELECT * FROM transactions WHERE id = ?", id)
	return scanTransaction(row)
}

func (r *TransactionRepo) GetByProcessorRef(processor, ref string) (*domain.Transaction, error) {
	row := r.db.QueryRow(
		"SELECT * FROM transactions WHERE processor = ? AND processor_reference = ?",
		processor, ref,
	)
	return scanTransaction(row)
}

type TransactionFilter struct {
	Processor string
	Status    string
	Currency  string
	From      *time.Time
	To        *time.Time
	Page      int
	Limit     int
}

func (r *TransactionRepo) List(f TransactionFilter) ([]domain.Transaction, int, error) {
	where, args := buildTransactionWhere(f)

	var total int
	countSQL := "SELECT COUNT(*) FROM transactions" + where
	if err := r.db.QueryRow(countSQL, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count: %w", err)
	}

	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	querySQL := "SELECT * FROM transactions" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, f.Limit, offset)

	rows, err := r.db.Query(querySQL, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var txns []domain.Transaction
	for rows.Next() {
		tx, err := scanTransactionRows(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan: %w", err)
		}
		txns = append(txns, *tx)
	}
	return txns, total, rows.Err()
}

// UpdateStatusToSettled marks a transaction as settled.
func (r *TransactionRepo) UpdateStatusToSettled(id string, settledAt time.Time) error {
	_, err := r.db.Exec(
		"UPDATE transactions SET status = ?, settled_at = ? WHERE id = ?",
		string(domain.StatusSettled), settledAt.Format(time.RFC3339), id,
	)
	return err
}

// GetCapturedWithoutSettlement returns captured transactions older than the
// given cutoff that have no matching settlement record.
func (r *TransactionRepo) GetCapturedWithoutSettlement(cutoff time.Time) ([]domain.Transaction, error) {
	query := `
		SELECT t.* FROM transactions t
		LEFT JOIN settlement_records sr ON sr.wakala_transaction_id = t.id
		WHERE t.status = 'captured'
		  AND t.captured_at < ?
		  AND sr.id IS NULL
		ORDER BY t.created_at
	`
	rows, err := r.db.Query(query, cutoff.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var txns []domain.Transaction
	for rows.Next() {
		tx, err := scanTransactionRows(rows)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		txns = append(txns, *tx)
	}
	return txns, rows.Err()
}

// DashboardStats holds aggregate transaction statistics.
type DashboardStats struct {
	Total             int
	Captured          int
	Settled           int
	PendingSettlement int
	TotalUSD          float64
	SettledUSD        float64
	UnsettledUSD      float64
}

func (r *TransactionRepo) GetDashboardStats() (*DashboardStats, error) {
	s := &DashboardStats{}
	err := r.db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN status='captured' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status='settled' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status IN ('authorized','captured') THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(usd_amount), 0),
			COALESCE(SUM(CASE WHEN status='settled' THEN usd_amount ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status IN ('authorized','captured') THEN usd_amount ELSE 0 END), 0)
		FROM transactions
	`).Scan(&s.Total, &s.Captured, &s.Settled, &s.PendingSettlement,
		&s.TotalUSD, &s.SettledUSD, &s.UnsettledUSD)
	return s, err
}

type ProcessorVolume struct {
	Processor  string  `json:"processor"`
	SettledUSD float64 `json:"settled_usd"`
}

func (r *TransactionRepo) GetVolumeByProcessor() ([]ProcessorVolume, error) {
	rows, err := r.db.Query(`
		SELECT processor, COALESCE(SUM(CASE WHEN status='settled' THEN usd_amount ELSE 0 END), 0)
		FROM transactions GROUP BY processor
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ProcessorVolume
	for rows.Next() {
		var pv ProcessorVolume
		if err := rows.Scan(&pv.Processor, &pv.SettledUSD); err != nil {
			return nil, err
		}
		result = append(result, pv)
	}
	return result, rows.Err()
}

type CurrencyVolume struct {
	Currency      string  `json:"currency"`
	Volume        float64 `json:"volume"`
	SettledVolume float64 `json:"settled_volume"`
}

func (r *TransactionRepo) GetVolumeByCurrency() ([]CurrencyVolume, error) {
	rows, err := r.db.Query(`
		SELECT currency,
			COALESCE(SUM(usd_amount), 0),
			COALESCE(SUM(CASE WHEN status='settled' THEN usd_amount ELSE 0 END), 0)
		FROM transactions GROUP BY currency
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []CurrencyVolume
	for rows.Next() {
		var cv CurrencyVolume
		if err := rows.Scan(&cv.Currency, &cv.Volume, &cv.SettledVolume); err != nil {
			return nil, err
		}
		result = append(result, cv)
	}
	return result, rows.Err()
}

// --- helpers ---

func buildTransactionWhere(f TransactionFilter) (string, []any) {
	var clauses []string
	var args []any

	if f.Processor != "" {
		clauses = append(clauses, "processor = ?")
		args = append(args, f.Processor)
	}
	if f.Status != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, f.Status)
	}
	if f.Currency != "" {
		clauses = append(clauses, "currency = ?")
		args = append(args, f.Currency)
	}
	if f.From != nil {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, f.From.Format(time.RFC3339))
	}
	if f.To != nil {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, f.To.Format(time.RFC3339))
	}

	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func formatNullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339)
}

func scanTransaction(row *sql.Row) (*domain.Transaction, error) {
	var tx domain.Transaction
	var proc, status, capturedAt, settledAt, createdAt string
	var capturedAtNull, settledAtNull sql.NullString

	err := row.Scan(
		&tx.ID, &tx.ProcessorReference, &proc, &tx.MerchantID,
		&tx.CustomerCountry, &tx.MerchantCountry, &tx.Amount, &tx.Currency,
		&tx.USDAmount, &status, &createdAt, &capturedAtNull, &settledAtNull,
	)
	if err != nil {
		return nil, err
	}

	tx.Processor = domain.Processor(proc)
	tx.Status = domain.TransactionStatus(status)
	tx.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)

	if capturedAtNull.Valid {
		capturedAt = capturedAtNull.String
		t, _ := time.Parse(time.RFC3339, capturedAt)
		tx.CapturedAt = &t
	}
	if settledAtNull.Valid {
		settledAt = settledAtNull.String
		t, _ := time.Parse(time.RFC3339, settledAt)
		tx.SettledAt = &t
	}

	return &tx, nil
}

func scanTransactionRows(rows *sql.Rows) (*domain.Transaction, error) {
	var tx domain.Transaction
	var proc, status, createdAt string
	var capturedAtNull, settledAtNull sql.NullString

	err := rows.Scan(
		&tx.ID, &tx.ProcessorReference, &proc, &tx.MerchantID,
		&tx.CustomerCountry, &tx.MerchantCountry, &tx.Amount, &tx.Currency,
		&tx.USDAmount, &status, &createdAt, &capturedAtNull, &settledAtNull,
	)
	if err != nil {
		return nil, err
	}

	tx.Processor = domain.Processor(proc)
	tx.Status = domain.TransactionStatus(status)
	tx.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)

	if capturedAtNull.Valid {
		t, _ := time.Parse(time.RFC3339, capturedAtNull.String)
		tx.CapturedAt = &t
	}
	if settledAtNull.Valid {
		t, _ := time.Parse(time.RFC3339, settledAtNull.String)
		tx.SettledAt = &t
	}

	return &tx, nil
}
