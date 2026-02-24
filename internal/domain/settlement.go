package domain

import "time"

type SettlementReport struct {
	ID          string    `json:"id"`
	Processor   Processor `json:"processor"`
	ReportDate  time.Time `json:"report_date"`
	BatchID     string    `json:"batch_id"`
	FileHash    string    `json:"file_hash"`
	RecordCount int       `json:"record_count"`
	IngestedAt  time.Time `json:"ingested_at"`
}

type SettlementRecord struct {
	ID                     string    `json:"id"`
	ReportID               string    `json:"report_id"`
	Processor              Processor `json:"processor"`
	ProcessorTransactionID string    `json:"processor_transaction_id"`
	WakalaTransactionID    string    `json:"wakala_transaction_id,omitempty"`
	GrossAmount            float64   `json:"gross_amount"`
	FeeAmount              float64   `json:"fee_amount"`
	NetAmount              float64   `json:"net_amount"`
	Currency               string    `json:"currency"`
	USDGrossAmount         float64   `json:"usd_gross_amount"`
	USDNetAmount           float64   `json:"usd_net_amount"`
	SettlementDate         time.Time `json:"settlement_date"`
	BatchID                string    `json:"batch_id"`
}
