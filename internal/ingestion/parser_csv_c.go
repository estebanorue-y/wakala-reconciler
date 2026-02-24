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

// ParseCapePayCSV parses the CapePay South Africa pipe-delimited CSV format.
//
// Expected header:
//
//	TXREF|MERCHANT|SETTLE_DATE|AMOUNT_ZAR|DEDUCTIONS_ZAR|NET_ZAR|BATCH
func ParseCapePayCSV(data []byte, reportID string) ([]domain.SettlementRecord, string, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.Comma = '|'
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

		txRef := strings.TrimSpace(row[0])
		settleDateStr := strings.TrimSpace(row[2])
		amountStr := strings.TrimSpace(row[3])
		deductionsStr := strings.TrimSpace(row[4])
		netStr := strings.TrimSpace(row[5])
		batchID = strings.TrimSpace(row[6])

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return nil, "", fmt.Errorf("line %d amount: %w", lineNum, err)
		}
		deductions, err := strconv.ParseFloat(deductionsStr, 64)
		if err != nil {
			return nil, "", fmt.Errorf("line %d deductions: %w", lineNum, err)
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

		usdGross, err := currency.ToUSD(amount, "ZAR")
		if err != nil {
			return nil, "", fmt.Errorf("line %d currency gross: %w", lineNum, err)
		}
		usdNet, err := currency.ToUSD(net, "ZAR")
		if err != nil {
			return nil, "", fmt.Errorf("line %d currency net: %w", lineNum, err)
		}

		rec := domain.SettlementRecord{
			ID:                     fmt.Sprintf("SR-CP-%s-%d", txRef, lineNum),
			ReportID:               reportID,
			Processor:              domain.ProcessorCapePay,
			ProcessorTransactionID: txRef,
			GrossAmount:            amount,
			FeeAmount:              deductions,
			NetAmount:              net,
			Currency:               "ZAR",
			USDGrossAmount:         usdGross,
			USDNetAmount:           usdNet,
			SettlementDate:         settleDate,
			BatchID:                batchID,
		}
		records = append(records, rec)
	}

	return records, batchID, nil
}
