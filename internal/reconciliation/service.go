package reconciliation

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/wakala/reconciler/internal/domain"
	"github.com/wakala/reconciler/internal/repository"
)

// ReconciliationResult summarises a full reconciliation run.
type ReconciliationResult struct {
	MatchedCount        int `json:"matched_count"`
	MissingSettlements  int `json:"missing_settlements"`
	AmountMismatches    int `json:"amount_mismatches"`
	OrphanedSettlements int `json:"orphaned_settlements"`
	TotalDiscrepancies  int `json:"total_discrepancies"`
}

// Service performs settlement reconciliation against known transactions.
type Service struct {
	txnRepo  *repository.TransactionRepo
	settRepo *repository.SettlementRepo
	discRepo *repository.DiscrepancyRepo
}

// NewService creates a new reconciliation service.
func NewService(
	txnRepo *repository.TransactionRepo,
	settRepo *repository.SettlementRepo,
	discRepo *repository.DiscrepancyRepo,
) *Service {
	return &Service{
		txnRepo:  txnRepo,
		settRepo: settRepo,
		discRepo: discRepo,
	}
}

// RunFullReconciliation clears previous discrepancies and runs all detection
// steps from scratch. This ensures a consistent view.
func (s *Service) RunFullReconciliation() (*ReconciliationResult, error) {
	if err := s.discRepo.ClearAll(); err != nil {
		return nil, fmt.Errorf("clear discrepancies: %w", err)
	}

	matched, err := s.MatchSettlements()
	if err != nil {
		return nil, fmt.Errorf("match settlements: %w", err)
	}

	missing, err := s.DetectMissingSettlements()
	if err != nil {
		return nil, fmt.Errorf("detect missing: %w", err)
	}

	mismatches, err := s.DetectAmountMismatches()
	if err != nil {
		return nil, fmt.Errorf("detect mismatches: %w", err)
	}

	orphaned, err := s.DetectOrphanedSettlements()
	if err != nil {
		return nil, fmt.Errorf("detect orphaned: %w", err)
	}

	result := &ReconciliationResult{
		MatchedCount:        matched,
		MissingSettlements:  missing,
		AmountMismatches:    mismatches,
		OrphanedSettlements: orphaned,
		TotalDiscrepancies:  missing + mismatches + orphaned,
	}

	log.Printf("[reconciliation] Results: matched=%d, missing=%d, mismatches=%d, orphaned=%d",
		matched, missing, mismatches, orphaned)

	return result, nil
}

// MatchSettlements tries to match unmatched settlement records to transactions
// by processor_reference. On match, the settlement record is updated with the
// wakala transaction ID and the transaction status is set to "settled".
func (s *Service) MatchSettlements() (int, error) {
	unmatched, err := s.settRepo.GetUnmatchedRecords()
	if err != nil {
		return 0, fmt.Errorf("get unmatched: %w", err)
	}

	matched := 0
	for _, rec := range unmatched {
		txn, err := s.txnRepo.GetByProcessorRef(string(rec.Processor), rec.ProcessorTransactionID)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Printf("[reconciliation] WARNING: db error matching %s/%s: %v",
					rec.Processor, rec.ProcessorTransactionID, err)
			}
			continue
		}
		if txn == nil {
			continue
		}

		// Update the settlement record with the Wakala transaction ID.
		if err := s.settRepo.UpdateWakalaTransactionID(rec.ID, txn.ID); err != nil {
			log.Printf("[reconciliation] WARNING: failed to update match for %s: %v",
				rec.ID, err)
			continue
		}

		// Mark the transaction as settled.
		if err := s.txnRepo.UpdateStatusToSettled(txn.ID, rec.SettlementDate); err != nil {
			log.Printf("[reconciliation] WARNING: failed to update txn status for %s: %v",
				txn.ID, err)
		}

		// Log the confidence score.
		confidence := calculateConfidence(txn, &rec)
		log.Printf("[reconciliation] Matched %s -> %s (confidence=%.2f, gross_usd_diff=%.4f)",
			rec.ProcessorTransactionID, txn.ID, confidence,
			math.Abs(txn.USDAmount-rec.USDGrossAmount))

		matched++
	}

	return matched, nil
}

// calculateConfidence returns a score (0-1) indicating how well the settlement
// record matches the transaction.
func calculateConfidence(txn *domain.Transaction, rec *domain.SettlementRecord) float64 {
	if txn.USDAmount == 0 {
		return 0.5
	}
	// Compare gross amounts — fee deduction is expected and not a confidence penalty.
	pctDiff := math.Abs(txn.USDAmount-rec.USDGrossAmount) / txn.USDAmount

	switch {
	case pctDiff <= 0.001:
		return 1.0 // exact match (< 0.1%)
	case pctDiff <= 0.01:
		return 0.95 // within 1%
	case pctDiff <= 0.02:
		return 0.90 // within 2%
	case pctDiff <= 0.05:
		return 0.80
	default:
		return 0.60
	}
}

// settlementWindowHours returns the configured settlement window from the
// SETTLEMENT_WINDOW_HOURS environment variable, defaulting to 48.
func settlementWindowHours() time.Duration {
	if v := os.Getenv("SETTLEMENT_WINDOW_HOURS"); v != "" {
		if h, err := strconv.Atoi(v); err == nil && h > 0 {
			return time.Duration(h) * time.Hour
		}
	}
	return 48 * time.Hour
}

// DetectMissingSettlements finds captured transactions older than the
// settlement window (default 48h, configurable via SETTLEMENT_WINDOW_HOURS)
// that have no matching settlement record.
func (s *Service) DetectMissingSettlements() (int, error) {
	cutoff := time.Now().Add(-settlementWindowHours())

	txns, err := s.txnRepo.GetCapturedWithoutSettlement(cutoff)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}

	var discs []domain.Discrepancy
	for _, txn := range txns {
		sev := severityByAmount(txn.USDAmount)

		d := domain.Discrepancy{
			ID:            fmt.Sprintf("DISC-MS-%s", txn.ID),
			Type:          domain.DiscrepancyMissingSettlement,
			TransactionID: txn.ID,
			Processor:     txn.Processor,
			ExpectedUSD:   txn.USDAmount,
			ActualUSD:     0,
			DifferenceUSD: txn.USDAmount,
			Currency:      txn.Currency,
			Severity:      sev,
			Description: fmt.Sprintf(
				"Transaction %s (%.2f USD) captured but no settlement found from %s",
				txn.ID, txn.USDAmount, txn.Processor,
			),
			DetectedAt: time.Now(),
		}
		discs = append(discs, d)
	}

	if len(discs) > 0 {
		n, err := s.discRepo.BulkInsert(discs)
		if err != nil {
			return 0, fmt.Errorf("insert discrepancies: %w", err)
		}
		log.Printf("[reconciliation] Detected %d MISSING_SETTLEMENT discrepancies", n)
		return n, nil
	}
	return 0, nil
}

// DetectAmountMismatches checks matched settlement records for USD amount
// differences beyond the tolerance threshold.
func (s *Service) DetectAmountMismatches() (int, error) {
	matched, err := s.settRepo.GetMatchedRecords()
	if err != nil {
		return 0, fmt.Errorf("get matched: %w", err)
	}

	var discs []domain.Discrepancy

	for _, rec := range matched {
		txn, err := s.txnRepo.GetByID(rec.WakalaTransactionID)
		if err != nil || txn == nil {
			continue
		}

		// Compare gross USD amount (before fees) against the original transaction
		// amount. Normal fee deductions are expected and do not constitute a
		// mismatch; only a difference in the gross charged amount does.
		diff := rec.USDGrossAmount - txn.USDAmount
		absDiff := math.Abs(diff)

		// Skip clean matches (< 0.5% difference — FX rounding tolerance).
		if txn.USDAmount > 0 && absDiff/txn.USDAmount <= 0.005 {
			continue
		}
		// Also skip tiny absolute differences (< 0.10 USD).
		if absDiff < 0.10 {
			continue
		}

		pctDiff := absDiff / txn.USDAmount
		sev := mismatchSeverity(pctDiff, absDiff)

		d := domain.Discrepancy{
			ID:            fmt.Sprintf("DISC-AM-%s", rec.ID),
			Type:          domain.DiscrepancyAmountMismatch,
			TransactionID: txn.ID,
			SettlementID:  rec.ID,
			Processor:     rec.Processor,
			ExpectedUSD:   txn.USDAmount,
			ActualUSD:     rec.USDGrossAmount,
			DifferenceUSD: diff,
			Currency:      rec.Currency,
			Severity:      sev,
			Description: fmt.Sprintf(
				"Gross amount mismatch for %s: expected %.2f USD, reported gross %.2f USD (%.1f%% diff)",
				txn.ID, txn.USDAmount, rec.USDGrossAmount, pctDiff*100,
			),
			DetectedAt: time.Now(),
		}
		discs = append(discs, d)
	}

	if len(discs) > 0 {
		n, err := s.discRepo.BulkInsert(discs)
		if err != nil {
			return 0, fmt.Errorf("insert discrepancies: %w", err)
		}
		log.Printf("[reconciliation] Detected %d AMOUNT_MISMATCH discrepancies", n)
		return n, nil
	}
	return 0, nil
}

// DetectOrphanedSettlements finds settlement records that could not be matched
// to any known Wakala transaction.
func (s *Service) DetectOrphanedSettlements() (int, error) {
	unmatched, err := s.settRepo.GetUnmatchedRecords()
	if err != nil {
		return 0, fmt.Errorf("get unmatched: %w", err)
	}

	var discs []domain.Discrepancy

	for _, rec := range unmatched {
		d := domain.Discrepancy{
			ID:           fmt.Sprintf("DISC-OS-%s", rec.ID),
			Type:         domain.DiscrepancyOrphaned,
			SettlementID: rec.ID,
			Processor:    rec.Processor,
			ExpectedUSD:  0,
			ActualUSD:    rec.USDNetAmount,
			DifferenceUSD: rec.USDNetAmount,
			Currency:     rec.Currency,
			Severity:     domain.SeverityHigh,
			Description: fmt.Sprintf(
				"Orphaned settlement %s from %s: %.2f USD with no matching transaction (proc_ref=%s)",
				rec.ID, rec.Processor, rec.USDNetAmount, rec.ProcessorTransactionID,
			),
			DetectedAt: time.Now(),
		}
		discs = append(discs, d)
	}

	if len(discs) > 0 {
		n, err := s.discRepo.BulkInsert(discs)
		if err != nil {
			return 0, fmt.Errorf("insert discrepancies: %w", err)
		}
		log.Printf("[reconciliation] Detected %d ORPHANED_SETTLEMENT discrepancies", n)
		return n, nil
	}
	return 0, nil
}

// --- helpers ---

func severityByAmount(usdAmount float64) domain.Severity {
	switch {
	case usdAmount > 500:
		return domain.SeverityHigh
	case usdAmount > 100:
		return domain.SeverityMedium
	default:
		return domain.SeverityLow
	}
}

func mismatchSeverity(pctDiff, absDiff float64) domain.Severity {
	if absDiff > 500 {
		return domain.SeverityCritical
	}
	if pctDiff > 0.02 {
		return domain.SeverityHigh
	}
	return domain.SeverityMedium
}
