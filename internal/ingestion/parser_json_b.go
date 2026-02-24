package ingestion

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/wakala/reconciler/internal/currency"
	"github.com/wakala/reconciler/internal/domain"
)

// nairaGatewayFile represents the top-level JSON structure from NairaGateway.
type nairaGatewayFile struct {
	BatchID        string              `json:"batch_id"`
	SettlementDate string              `json:"settlement_date"`
	Records        []nairaGatewayEntry `json:"records"`
}

type nairaGatewayEntry struct {
	Ref            string  `json:"ref"`
	MerchantID     string  `json:"merchant_id"`
	AmountNGN      float64 `json:"amount_ngn"`
	ProcessingFee  float64 `json:"processing_fee_ngn"`
	PayoutNGN      float64 `json:"payout_ngn"`
	SettledAt      string  `json:"settled_at"`
}

// ParseNairaGatewayJSON parses the NairaGateway Nigeria JSON settlement format.
func ParseNairaGatewayJSON(data []byte, reportID string) ([]domain.SettlementRecord, string, error) {
	var file nairaGatewayFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, "", fmt.Errorf("unmarshal: %w", err)
	}

	var records []domain.SettlementRecord

	for i, entry := range file.Records {
		settledAt, err := time.Parse(time.RFC3339, entry.SettledAt)
		if err != nil {
			// Try alternative format with timezone offset.
			settledAt, err = time.Parse("2006-01-02T15:04:05-07:00", entry.SettledAt)
			if err != nil {
				return nil, "", fmt.Errorf("record %d date: %w", i, err)
			}
		}

		usdGross, err := currency.ToUSD(entry.AmountNGN, "NGN")
		if err != nil {
			return nil, "", fmt.Errorf("record %d currency gross: %w", i, err)
		}
		usdNet, err := currency.ToUSD(entry.PayoutNGN, "NGN")
		if err != nil {
			return nil, "", fmt.Errorf("record %d currency net: %w", i, err)
		}

		rec := domain.SettlementRecord{
			ID:                     fmt.Sprintf("SR-NG-%s-%d", entry.Ref, i),
			ReportID:               reportID,
			Processor:              domain.ProcessorNairaGateway,
			ProcessorTransactionID: entry.Ref,
			GrossAmount:            entry.AmountNGN,
			FeeAmount:              entry.ProcessingFee,
			NetAmount:              entry.PayoutNGN,
			Currency:               "NGN",
			USDGrossAmount:         usdGross,
			USDNetAmount:           usdNet,
			SettlementDate:         settledAt,
			BatchID:                file.BatchID,
		}
		records = append(records, rec)
	}

	return records, file.BatchID, nil
}
