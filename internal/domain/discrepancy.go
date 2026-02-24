package domain

import "time"

type DiscrepancyType string

const (
	DiscrepancyMissingSettlement DiscrepancyType = "MISSING_SETTLEMENT"
	DiscrepancyAmountMismatch    DiscrepancyType = "AMOUNT_MISMATCH"
	DiscrepancyOrphaned          DiscrepancyType = "ORPHANED_SETTLEMENT"
)

type Severity string

const (
	SeverityLow      Severity = "LOW"
	SeverityMedium   Severity = "MEDIUM"
	SeverityHigh     Severity = "HIGH"
	SeverityCritical Severity = "CRITICAL"
)

type Discrepancy struct {
	ID            string          `json:"id"`
	Type          DiscrepancyType `json:"type"`
	TransactionID string          `json:"transaction_id,omitempty"`
	SettlementID  string          `json:"settlement_id,omitempty"`
	Processor     Processor       `json:"processor"`
	ExpectedUSD   float64         `json:"expected_usd"`
	ActualUSD     float64         `json:"actual_usd"`
	DifferenceUSD float64         `json:"difference_usd"`
	Currency      string          `json:"currency"`
	Severity      Severity        `json:"severity"`
	Description   string          `json:"description"`
	DetectedAt    time.Time       `json:"detected_at"`
}
