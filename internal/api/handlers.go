package api

import (
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/wakala/reconciler/internal/ingestion"
	"github.com/wakala/reconciler/internal/repository"
)

// Handlers groups all HTTP handler methods and their dependencies.
type Handlers struct {
	txnRepo      *repository.TransactionRepo
	settRepo     *repository.SettlementRepo
	discRepo     *repository.DiscrepancyRepo
	ingestionSvc *ingestion.Service
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[api] encode error: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parseTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse("2006-01-02", s)
		if err != nil {
			return nil
		}
	}
	return &t
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return def
	}
	return v
}

func roundUSD(v float64) float64 {
	return math.Round(v*100) / 100
}

// --- IngestReport ---

func (h *Handlers) IngestReport(w http.ResponseWriter, r *http.Request) {
	// Accept multipart form.
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form: "+err.Error())
		return
	}

	processor := r.FormValue("processor")
	format := r.FormValue("format")
	if processor == "" || format == "" {
		writeError(w, http.StatusBadRequest, "processor and format are required")
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "file field is required: "+err.Error())
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read file: "+err.Error())
		return
	}

	result, err := h.ingestionSvc.IngestReport(data, processor, format)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// --- ListTransactions ---

func (h *Handlers) ListTransactions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := repository.TransactionFilter{
		Processor: q.Get("processor"),
		Status:    q.Get("status"),
		Currency:  q.Get("currency"),
		From:      parseTime(q.Get("from")),
		To:        parseTime(q.Get("to")),
		Page:      parseIntDefault(q.Get("page"), 1),
		Limit:     parseIntDefault(q.Get("limit"), 50),
	}

	txns, total, err := h.txnRepo.List(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"transactions": txns,
		"total":        total,
		"page":         filter.Page,
		"limit":        filter.Limit,
	})
}

// --- GetTransactionSettlementStatus ---

func (h *Handlers) GetTransactionSettlementStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	txn, err := h.txnRepo.GetByID(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "transaction not found")
		return
	}

	settlements, err := h.settRepo.GetByTransactionID(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	discrepancies, err := h.discRepo.GetByTransactionID(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"transaction":   txn,
		"settlements":   settlements,
		"discrepancies": discrepancies,
	})
}

// --- ListDiscrepancies ---

func (h *Handlers) ListDiscrepancies(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := repository.DiscrepancyFilter{
		Type:      q.Get("type"),
		Severity:  q.Get("severity"),
		Processor: q.Get("processor"),
		From:      parseTime(q.Get("from")),
		To:        parseTime(q.Get("to")),
		Page:      parseIntDefault(q.Get("page"), 1),
		Limit:     parseIntDefault(q.Get("limit"), 50),
	}

	discs, total, err := h.discRepo.List(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Calculate total impact for the result set.
	var totalImpact float64
	for _, d := range discs {
		totalImpact += math.Abs(d.DifferenceUSD)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"discrepancies":   discs,
		"total":           total,
		"page":            filter.Page,
		"limit":           filter.Limit,
		"total_impact_usd": roundUSD(totalImpact),
	})
}

// --- GetDiscrepancySummary ---

func (h *Handlers) GetDiscrepancySummary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.discRepo.GetSummary()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

// --- GetDashboard ---

func (h *Handlers) GetDashboard(w http.ResponseWriter, r *http.Request) {
	stats, err := h.txnRepo.GetDashboardStats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	discSummary, err := h.discRepo.GetSummary()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	processorVols, err := h.txnRepo.GetVolumeByProcessor()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	discStats, err := h.discRepo.GetStatsByProcessor()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	currencyVols, err := h.txnRepo.GetVolumeByCurrency()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Merge processor volumes with discrepancy stats.
	type procEntry struct {
		Processor        string  `json:"processor"`
		SettledUSD       float64 `json:"settled_usd"`
		DiscrepancyCount int     `json:"discrepancy_count"`
		ImpactUSD        float64 `json:"discrepancy_impact_usd"`
	}

	discMap := make(map[string]repository.ProcessorDiscrepancyStat)
	for _, ds := range discStats {
		discMap[ds.Processor] = ds
	}

	var byProcessor []procEntry
	for _, pv := range processorVols {
		entry := procEntry{
			Processor:  pv.Processor,
			SettledUSD: roundUSD(pv.SettledUSD),
		}
		if ds, ok := discMap[pv.Processor]; ok {
			entry.DiscrepancyCount = ds.DiscrepancyCount
			entry.ImpactUSD = roundUSD(ds.ImpactUSD)
		}
		byProcessor = append(byProcessor, entry)
	}

	dashboard := map[string]any{
		"period": map[string]string{
			"from": "2024-01-08",
			"to":   "2024-01-21",
		},
		"transactions": map[string]int{
			"total":              stats.Total,
			"captured":           stats.Captured,
			"settled":            stats.Settled,
			"pending_settlement": stats.PendingSettlement,
		},
		"volume": map[string]float64{
			"total_usd":     roundUSD(stats.TotalUSD),
			"settled_usd":   roundUSD(stats.SettledUSD),
			"unsettled_usd": roundUSD(stats.UnsettledUSD),
		},
		"discrepancies": map[string]any{
			"total":          discSummary.TotalCount,
			"critical":       discSummary.BySeverity["CRITICAL"],
			"high":           discSummary.BySeverity["HIGH"],
			"medium":         discSummary.BySeverity["MEDIUM"],
			"low":            discSummary.BySeverity["LOW"],
			"total_impact_usd": roundUSD(discSummary.TotalImpact),
		},
		"by_processor": byProcessor,
		"by_currency":  currencyVols,
	}

	writeJSON(w, http.StatusOK, dashboard)
}

// --- ListSettlements ---

func (h *Handlers) ListSettlements(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := repository.SettlementFilter{
		Processor: q.Get("processor"),
		From:      parseTime(q.Get("from")),
		To:        parseTime(q.Get("to")),
		Page:      parseIntDefault(q.Get("page"), 1),
		Limit:     parseIntDefault(q.Get("limit"), 50),
	}

	records, total, err := h.settRepo.ListRecords(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"settlements": records,
		"total":       total,
		"page":        filter.Page,
		"limit":       filter.Limit,
	})
}
