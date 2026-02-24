package ingestion

import (
	"crypto/sha256"
	"fmt"
	"log"
	"time"

	"github.com/wakala/reconciler/internal/domain"
	"github.com/wakala/reconciler/internal/reconciliation"
	"github.com/wakala/reconciler/internal/repository"
)

// IngestResult is returned from a successful ingestion.
type IngestResult struct {
	ReportID             string `json:"report_id"`
	RecordsIngested      int    `json:"records_ingested"`
	DuplicatesSkipped    int    `json:"duplicates_skipped"`
	DiscrepanciesDetected int   `json:"discrepancies_detected"`
}

// Service handles ingestion of settlement reports from various processors.
type Service struct {
	settlementRepo *repository.SettlementRepo
	txnRepo        *repository.TransactionRepo
	discRepo       *repository.DiscrepancyRepo
	reconSvc       *reconciliation.Service
}

// NewService creates a new ingestion service.
func NewService(
	settlementRepo *repository.SettlementRepo,
	txnRepo *repository.TransactionRepo,
	discRepo *repository.DiscrepancyRepo,
	reconSvc *reconciliation.Service,
) *Service {
	return &Service{
		settlementRepo: settlementRepo,
		txnRepo:        txnRepo,
		discRepo:       discRepo,
		reconSvc:       reconSvc,
	}
}

// IngestReport parses a settlement report file and stores the records.
// It also triggers reconciliation after ingestion.
//
// format must be one of: csv_a, json_b, csv_c
func (s *Service) IngestReport(data []byte, processor string, format string) (*IngestResult, error) {
	// Idempotency check via file hash.
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	exists, err := s.settlementRepo.ReportExistsByHash(hash)
	if err != nil {
		return nil, fmt.Errorf("check hash: %w", err)
	}
	if exists {
		return &IngestResult{
			ReportID:          "already-ingested",
			RecordsIngested:   0,
			DuplicatesSkipped: 0,
		}, nil
	}

	reportID := fmt.Sprintf("RPT-%s-%d", processor, time.Now().UnixNano())
	proc := domain.Processor(processor)

	var records []domain.SettlementRecord
	var batchID string

	switch format {
	case "csv_a":
		records, batchID, err = ParseAfriPayCSV(data, reportID)
	case "json_b":
		records, batchID, err = ParseNairaGatewayJSON(data, reportID)
	case "csv_c":
		records, batchID, err = ParseCapePayCSV(data, reportID)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", format, err)
	}

	if batchID == "" {
		batchID = fmt.Sprintf("BATCH-%d", time.Now().UnixNano())
	}

	// Store the report.
	report := &domain.SettlementReport{
		ID:          reportID,
		Processor:   proc,
		ReportDate:  time.Now(),
		BatchID:     batchID,
		FileHash:    hash,
		RecordCount: len(records),
		IngestedAt:  time.Now(),
	}
	if err := s.settlementRepo.InsertReport(report); err != nil {
		return nil, fmt.Errorf("insert report: %w", err)
	}

	// Store the records.
	inserted, err := s.settlementRepo.InsertRecords(records)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}

	log.Printf("[ingestion] Ingested report %s: %d records (%d new) from %s",
		reportID, len(records), inserted, processor)

	// Run reconciliation.
	reconResult, err := s.reconSvc.RunFullReconciliation()
	if err != nil {
		log.Printf("[ingestion] WARNING: reconciliation failed: %v", err)
		// Do not fail ingestion if reconciliation has issues.
	}

	discrepanciesDetected := 0
	if reconResult != nil {
		discrepanciesDetected = reconResult.TotalDiscrepancies
	}

	return &IngestResult{
		ReportID:              reportID,
		RecordsIngested:       inserted,
		DuplicatesSkipped:     len(records) - inserted,
		DiscrepanciesDetected: discrepanciesDetected,
	}, nil
}
