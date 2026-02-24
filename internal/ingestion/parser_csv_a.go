package ingestion

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/wakala/reconciler/internal/currency"
	"github.com/wakala/reconciler/internal/domain"
)

// ParseAfriPayCSV parses the AfriPay Kenya CSV settlement format.
//
// Expected header:
//
//	transaction_id,merchant_ref,settlement_date,gross_amount_kes,fee_kes,net_kes,batch_id
func ParseAfriPayCSV(data []byte, reportID string) ([]domain.SettlementRecord, string, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, "", fmt.Errorf("read header: %w", err)
	}
	if len(header) < 7 {
		return nil, "", fmt.Errorf("expected 7 columns, got %d", len(header))
	}

	var records []domain.SettlementRecord
	var batchID string
	lineNum := 1

	for {
		lineNum++
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", fmt.Errorf("line %d: %w", lineNum, err)
		}
		if len(row) < 7 {
			continue
		}

		txnID := strings.TrimSpace(row[0])
		settleDateStr := strings.TrimSpace(row[2])
		grossStr := strings.TrimSpace(row[3])
		feeStr := strings.TrimSpace(row[4])
		netStr := strings.TrimSpace(row[5])
		batchID = strings.TrimSpace(row[6])

		gross, err := strconv.ParseFloat(grossStr, 64)
		if err != nil {
			return nil, "", fmt.Errorf("line %d gross: %w", lineNum, err)
		}
		fee, err := strconv.ParseFloat(feeStr, 64)
		if err != nil {
			return nil, "", fmt.Errorf("line %d fee: %w", lineNum, err)
		}
		net, err := strconv.ParseFloat(netStr, 64)
		if err != nil {
			return nil, "", fmt.Errorf("line %d net: %w", lineNum, err)
		}

		settleDate, err := time.Parse("2006-01-02", settleDateStr)
		if err != nil {
			settleDate, err = time.Parse(time.RFC3339, settleDateStr)
			if err != nil {
				return nil, "", fmt.Errorf("line %d date: %w", lineNum, err)
			}
		}

		usdGross, err := currency.ToUSD(gross, "KES")
		if err != nil {
			return nil, "", fmt.Errorf("line %d currency gross: %w", lineNum, err)
		}
		usdNet, err := currency.ToUSD(net, "KES")
		if err != nil {
			return nil, "", fmt.Errorf("line %d currency net: %w", lineNum, err)
		}

		rec := domain.SettlementRecord{
			ID:                     fmt.Sprintf("SR-AP-%s-%d", txnID, lineNum),
			ReportID:               reportID,
			Processor:              domain.ProcessorAfriPay,
			ProcessorTransactionID: txnID,
			GrossAmount:            gross,
			FeeAmount:              fee,
			NetAmount:              net,
			Currency:               "KES",
			USDGrossAmount:         usdGross,
			USDNetAmount:           usdNet,
			SettlementDate:         settleDate,
			BatchID:                batchID,
		}
		records = append(records, rec)
	}

	return records, batchID, nil
}
