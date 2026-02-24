package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wakala/reconciler/internal/currency"
	"github.com/wakala/reconciler/internal/domain"
)

func main() {
	rng := rand.New(rand.NewSource(42))
	baseDir := findTestdataDir()

	// Date range: 2024-01-08 to 2024-01-21.
	startDate := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, 1, 21, 0, 0, 0, 0, time.UTC)
	dayRange := int(endDate.Sub(startDate).Hours() / 24)

	merchants := make([]string, 20)
	for i := range merchants {
		merchants[i] = fmt.Sprintf("M%03d", i+1)
	}

	type txnGroup struct {
		processor domain.Processor
		currency  string
		country   string
		prefix    string
		count     int
	}

	groups := []txnGroup{
		{domain.ProcessorAfriPay, "KES", "KE", "AP-TXN", 50},
		{domain.ProcessorNairaGateway, "NGN", "NG", "NG-TXN", 55},
		{domain.ProcessorCapePay, "ZAR", "ZA", "CP-TXN", 50},
	}

	var allTxns []domain.Transaction

	for _, g := range groups {
		for i := 1; i <= g.count; i++ {
			txnID := fmt.Sprintf("WKL-%s-%03d", strings.ToUpper(string(g.processor)), i)
			procRef := fmt.Sprintf("%s-%03d", g.prefix, i)

			// Random day within range.
			day := rng.Intn(dayRange)
			hour := rng.Intn(24)
			minute := rng.Intn(60)
			createdAt := startDate.AddDate(0, 0, day).Add(
				time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute,
			)

			// USD amount between 5 and 500.
			usdAmount := 5 + rng.Float64()*495
			usdAmount = math.Round(usdAmount*100) / 100

			localAmount, _ := currency.FromUSD(usdAmount, g.currency)
			localAmount = math.Round(localAmount*100) / 100

			// Status distribution: 85% captured, 10% authorized, 5% failed.
			var status domain.TransactionStatus
			var capturedAt *time.Time
			roll := rng.Float64()
			switch {
			case roll < 0.85:
				status = domain.StatusCaptured
				t := createdAt.Add(time.Duration(rng.Intn(120)+1) * time.Minute)
				capturedAt = &t
			case roll < 0.95:
				status = domain.StatusAuthorized
			default:
				status = domain.StatusFailed
			}

			txn := domain.Transaction{
				ID:                 txnID,
				ProcessorReference: procRef,
				Processor:          g.processor,
				MerchantID:         merchants[rng.Intn(len(merchants))],
				CustomerCountry:    g.country,
				MerchantCountry:    g.country,
				Amount:             localAmount,
				Currency:           g.currency,
				USDAmount:          usdAmount,
				Status:             status,
				CreatedAt:          createdAt,
				CapturedAt:         capturedAt,
			}
			allTxns = append(allTxns, txn)
		}
	}

	// Write transactions.json.
	writeJSONFile(filepath.Join(baseDir, "transactions.json"), allTxns)
	fmt.Printf("Generated %d transactions -> transactions.json\n", len(allTxns))

	// Generate settlement files.
	generateAfriPayCSV(rng, allTxns, baseDir)
	generateNairaGatewayJSON(rng, allTxns, baseDir)
	generateCapePayCSV(rng, allTxns, baseDir)

	fmt.Println("Test data generation complete.")
}

func generateAfriPayCSV(rng *rand.Rand, txns []domain.Transaction, baseDir string) {
	// Filter captured AfriPay transactions.
	var captured []domain.Transaction
	for _, t := range txns {
		if t.Processor == domain.ProcessorAfriPay && t.Status == domain.StatusCaptured {
			captured = append(captured, t)
		}
	}

	filePath := filepath.Join(baseDir, "processor_a_afripay.csv")
	f, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{
		"transaction_id", "merchant_ref", "settlement_date",
		"gross_amount_kes", "fee_kes", "net_kes", "batch_id",
	})

	count := 0
	batchID := "KE-BATCH-001"

	for i, txn := range captured {
		roll := rng.Float64()

		// 8% missing: skip this transaction.
		if roll > 0.92 && roll <= 1.0 {
			continue
		}

		settleDate := txn.CreatedAt.AddDate(0, 0, 1).Format("2006-01-02")
		gross := txn.Amount
		fee := math.Round(gross*0.015*100) / 100 // 1.5% fee
		net := math.Round((gross-fee)*100) / 100

		// 4% amount mismatch: adjust gross (and recalculate fee/net) by 3-5%.
		// This simulates a processor reporting a different charged amount.
		if roll > 0.88 && roll <= 0.92 {
			mismatchPct := 0.03 + rng.Float64()*0.02
			gross = math.Round(gross*(1+mismatchPct)*100) / 100
			fee = math.Round(gross*0.015*100) / 100
			net = math.Round((gross-fee)*100) / 100
		}

		procRef := txn.ProcessorReference

		// 2% orphaned: use a fake transaction ID.
		if i < 2 {
			procRef = fmt.Sprintf("FAKE-AP-%03d", i+1)
		}

		w.Write([]string{
			procRef,
			txn.MerchantID,
			settleDate,
			fmt.Sprintf("%.2f", gross),
			fmt.Sprintf("%.2f", fee),
			fmt.Sprintf("%.2f", net),
			batchID,
		})
		count++
	}

	fmt.Printf("Generated %d AfriPay CSV records -> processor_a_afripay.csv\n", count)
}

func generateNairaGatewayJSON(rng *rand.Rand, txns []domain.Transaction, baseDir string) {
	var captured []domain.Transaction
	for _, t := range txns {
		if t.Processor == domain.ProcessorNairaGateway && t.Status == domain.StatusCaptured {
			captured = append(captured, t)
		}
	}

	type record struct {
		Ref           string  `json:"ref"`
		MerchantID    string  `json:"merchant_id"`
		AmountNGN     float64 `json:"amount_ngn"`
		ProcessingFee float64 `json:"processing_fee_ngn"`
		PayoutNGN     float64 `json:"payout_ngn"`
		SettledAt     string  `json:"settled_at"`
	}

	type fileFormat struct {
		BatchID        string   `json:"batch_id"`
		SettlementDate string   `json:"settlement_date"`
		Records        []record `json:"records"`
	}

	batchID := "NG-BATCH-001"
	var records []record

	for i, txn := range captured {
		roll := rng.Float64()

		// 8% missing.
		if roll > 0.92 && roll <= 1.0 {
			continue
		}

		settleDate := txn.CreatedAt.AddDate(0, 0, 1)
		settleDateStr := settleDate.Format("2006-01-02") + "T23:59:59+01:00"

		gross := txn.Amount
		fee := math.Round(gross*0.01*100) / 100 // 1% fee
		payout := math.Round((gross-fee)*100) / 100

		// 4% amount mismatch: adjust gross by 3-5%.
		if roll > 0.88 && roll <= 0.92 {
			mismatchPct := 0.03 + rng.Float64()*0.02
			gross = math.Round(gross*(1+mismatchPct)*100) / 100
			fee = math.Round(gross*0.01*100) / 100
			payout = math.Round((gross-fee)*100) / 100
		}

		ref := txn.ProcessorReference

		// 2% orphaned.
		if i < 2 {
			ref = fmt.Sprintf("FAKE-NG-%03d", i+1)
		}

		records = append(records, record{
			Ref:           ref,
			MerchantID:    txn.MerchantID,
			AmountNGN:     gross,
			ProcessingFee: fee,
			PayoutNGN:     payout,
			SettledAt:     settleDateStr,
		})
	}

	output := fileFormat{
		BatchID:        batchID,
		SettlementDate: "2024-01-15T23:59:59+01:00",
		Records:        records,
	}

	writeJSONFile(filepath.Join(baseDir, "processor_b_nairagateway.json"), output)
	fmt.Printf("Generated %d NairaGateway JSON records -> processor_b_nairagateway.json\n", len(records))
}

func generateCapePayCSV(rng *rand.Rand, txns []domain.Transaction, baseDir string) {
	var captured []domain.Transaction
	for _, t := range txns {
		if t.Processor == domain.ProcessorCapePay && t.Status == domain.StatusCaptured {
			captured = append(captured, t)
		}
	}

	filePath := filepath.Join(baseDir, "processor_c_capepay.csv")
	f, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Comma = '|'
	defer w.Flush()

	w.Write([]string{
		"TXREF", "MERCHANT", "SETTLE_DATE",
		"AMOUNT_ZAR", "DEDUCTIONS_ZAR", "NET_ZAR", "BATCH",
	})

	count := 0
	batchID := "ZA-BATCH-001"

	for i, txn := range captured {
		roll := rng.Float64()

		// 8% missing.
		if roll > 0.92 && roll <= 1.0 {
			continue
		}

		settleDate := txn.CreatedAt.AddDate(0, 0, 1).Format("2006-01-02")
		amount := txn.Amount
		deductions := math.Round(amount*0.02*100) / 100 // 2% fee
		net := math.Round((amount-deductions)*100) / 100

		// 4% amount mismatch: adjust gross amount by 3-5%.
		if roll > 0.88 && roll <= 0.92 {
			mismatchPct := 0.03 + rng.Float64()*0.02
			amount = math.Round(amount*(1+mismatchPct)*100) / 100
			deductions = math.Round(amount*0.02*100) / 100
			net = math.Round((amount-deductions)*100) / 100
		}

		txRef := txn.ProcessorReference

		// 2% orphaned.
		if i < 2 {
			txRef = fmt.Sprintf("FAKE-CP-%03d", i+1)
		}

		w.Write([]string{
			txRef,
			txn.MerchantID,
			settleDate,
			fmt.Sprintf("%.2f", amount),
			fmt.Sprintf("%.2f", deductions),
			fmt.Sprintf("%.2f", net),
			batchID,
		})
		count++
	}

	fmt.Printf("Generated %d CapePay CSV records -> processor_c_capepay.csv\n", count)
}

func writeJSONFile(path string, v any) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
}

func findTestdataDir() string {
	// Look for the testdata directory relative to common locations.
	candidates := []string{
		"testdata",
		"./testdata",
		"/Users/estebanorue/wakala-reconciler/testdata",
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
	}
	// Fallback.
	return "testdata"
}
