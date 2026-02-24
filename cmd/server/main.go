package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/wakala/reconciler/internal/api"
	"github.com/wakala/reconciler/internal/domain"
	"github.com/wakala/reconciler/internal/ingestion"
	"github.com/wakala/reconciler/internal/reconciliation"
	"github.com/wakala/reconciler/internal/repository"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "wakala.db"
	}

	log.Printf("Initializing database at %s", dbPath)
	db, err := repository.InitDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to init DB: %v", err)
	}
	defer db.Close()

	// Create repositories.
	txnRepo := repository.NewTransactionRepo(db)
	settRepo := repository.NewSettlementRepo(db)
	discRepo := repository.NewDiscrepancyRepo(db)

	// Create services.
	reconSvc := reconciliation.NewService(txnRepo, settRepo, discRepo)
	ingestionSvc := ingestion.NewService(settRepo, txnRepo, discRepo, reconSvc)

	// Seed transactions if DB is empty.
	count, err := txnRepo.Count()
	if err != nil {
		log.Fatalf("Failed to count transactions: %v", err)
	}
	if count == 0 {
		log.Println("Database is empty, seeding transactions from testdata...")
		if err := seedTransactions(txnRepo); err != nil {
			log.Printf("WARNING: Failed to seed transactions: %v", err)
		}
	} else {
		log.Printf("Database already has %d transactions, skipping seed", count)
	}

	// Create router.
	router := api.NewRouter(txnRepo, settRepo, discRepo, ingestionSvc)

	log.Printf("Wakala Cross-Border Settlement Reconciler")
	log.Printf("Listening on http://localhost:%s", port)
	log.Printf("API base: http://localhost:%s/api/v1", port)
	log.Printf("")
	log.Printf("Endpoints:")
	log.Printf("  POST   /api/v1/reports/ingest")
	log.Printf("  GET    /api/v1/transactions")
	log.Printf("  GET    /api/v1/transactions/{id}/settlement-status")
	log.Printf("  GET    /api/v1/discrepancies")
	log.Printf("  GET    /api/v1/discrepancies/summary")
	log.Printf("  GET    /api/v1/settlements")
	log.Printf("  GET    /api/v1/dashboard")

	if err := http.ListenAndServe(":"+port, router); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func seedTransactions(repo *repository.TransactionRepo) error {
	// Try multiple possible locations for testdata.
	candidates := []string{
		"testdata/transactions.json",
		filepath.Join(".", "testdata", "transactions.json"),
	}

	// Also try to find relative to the executable.
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(dir, "testdata", "transactions.json"),
			filepath.Join(dir, "..", "..", "testdata", "transactions.json"),
		)
	}

	var data []byte
	var loadErr error
	for _, path := range candidates {
		data, loadErr = os.ReadFile(path)
		if loadErr == nil {
			log.Printf("Loaded transactions from %s", path)
			break
		}
	}
	if loadErr != nil {
		return fmt.Errorf("could not find transactions.json in any candidate path: %w", loadErr)
	}

	var txns []domain.Transaction
	if err := json.Unmarshal(data, &txns); err != nil {
		return fmt.Errorf("unmarshal transactions: %w", err)
	}

	inserted, err := repo.BulkInsert(txns)
	if err != nil {
		return fmt.Errorf("bulk insert: %w", err)
	}

	log.Printf("Seeded %d transactions (out of %d in file)", inserted, len(txns))
	return nil
}
