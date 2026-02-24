package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/wakala/reconciler/internal/ingestion"
	"github.com/wakala/reconciler/internal/repository"
)

// NewRouter creates the Chi router with all API routes mounted.
func NewRouter(
	txnRepo *repository.TransactionRepo,
	settRepo *repository.SettlementRepo,
	discRepo *repository.DiscrepancyRepo,
	ingestionSvc *ingestion.Service,
) http.Handler {
	h := &Handlers{
		txnRepo:      txnRepo,
		settRepo:     settRepo,
		discRepo:     discRepo,
		ingestionSvc: ingestionSvc,
	}

	r := chi.NewRouter()

	// Middleware.
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.SetHeader("Content-Type", "application/json"))

	r.Route("/api/v1", func(r chi.Router) {
		// Ingestion.
		r.Post("/reports/ingest", h.IngestReport)

		// Transactions.
		r.Get("/transactions", h.ListTransactions)
		r.Get("/transactions/{id}/settlement-status", h.GetTransactionSettlementStatus)

		// Discrepancies.
		r.Get("/discrepancies", h.ListDiscrepancies)
		r.Get("/discrepancies/summary", h.GetDiscrepancySummary)

		// Settlements.
		r.Get("/settlements", h.ListSettlements)

		// Dashboard.
		r.Get("/dashboard", h.GetDashboard)
	})

	return r
}
