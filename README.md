# Wakala Cross-Border Settlement Reconciler

A backend service that ingests settlement reports from multiple payment processors, normalizes the data, detects discrepancies, and exposes an API for Wakala's finance team to query settlement status and flag issues across Kenya, Nigeria, and South Africa.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Test Data](#test-data)
- [Ingesting Settlement Reports](#ingesting-settlement-reports)
- [API Reference](#api-reference)
- [Sample Requests & Responses](#sample-requests--responses)
- [Discrepancy Detection Logic](#discrepancy-detection-logic)
- [Assumptions & Trade-offs](#assumptions--trade-offs)
- [Production Extension Notes](#production-extension-notes)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        HTTP API (Chi Router)                    │
│  POST /ingest  GET /transactions  GET /discrepancies  GET /dash │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────▼──────────────┐
              │      Ingestion Service       │
              │  SHA-256 idempotency check   │
              │  Parser dispatch by format   │
              └──────────────┬──────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
  parser_csv_a          parser_json_b        parser_csv_c
  AfriPay (KES)     NairaGateway (NGN)     CapePay (ZAR)
  comma-delimited    JSON + UTC+1 tz       pipe-delimited
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │  normalize → usd_gross / usd_net
              ┌──────────────▼──────────────┐
              │    Reconciliation Engine     │
              │  1. MatchSettlements         │
              │  2. DetectMissingSettlements │
              │  3. DetectAmountMismatches   │
              │  4. DetectOrphanedSettlements│
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │         SQLite (WAL)         │
              │  transactions               │
              │  settlement_reports         │
              │  settlement_records         │
              │  discrepancies              │
              └─────────────────────────────┘
```

### Directory Structure

```
wakala-reconciler/
├── cmd/server/main.go               # Entry point, DB init, auto-seed
├── internal/
│   ├── domain/                      # Core types (Transaction, SettlementRecord, Discrepancy)
│   ├── ingestion/                   # Report parsing & normalization
│   │   ├── service.go               # Orchestrator: hash check → parse → store → reconcile
│   │   ├── parser_csv_a.go          # AfriPay Kenya CSV
│   │   ├── parser_json_b.go         # NairaGateway Nigeria JSON
│   │   └── parser_csv_c.go          # CapePay South Africa pipe-delimited CSV
│   ├── reconciliation/service.go    # Match + detect all discrepancy types
│   ├── repository/                  # SQLite data access layer
│   ├── api/                         # HTTP handlers & Chi router
│   └── currency/converter.go        # KES / NGN / ZAR <-> USD conversion
├── testdata/
│   ├── generate/main.go             # Deterministic test data generator (seed 42)
│   ├── transactions.json            # 155 internal Wakala transactions
│   ├── processor_a_afripay.csv      # AfriPay settlement report
│   ├── processor_b_nairagateway.json# NairaGateway settlement report
│   └── processor_c_capepay.csv      # CapePay settlement report
├── go.mod
└── Makefile
```

---

## Prerequisites

- **Go 1.22+** (uses pure-Go SQLite — no CGo, no external database needed)

```bash
go version   # should print go1.22 or higher
```

---

## Quick Start

```bash
# 1. Clone and enter the project
cd wakala-reconciler

# 2. Download dependencies
go mod download

# 3. (Optional) Regenerate test data from scratch
go run ./testdata/generate

# 4. Start the server  (auto-seeds 155 transactions on first run)
go run ./cmd/server
```

Expected output:

```
2024/01/15 10:00:00 Initializing database at wakala.db
2024/01/15 10:00:00 Database is empty, seeding transactions from testdata...
2024/01/15 10:00:00 Seeded 155 transactions (out of 155 in file)
2024/01/15 10:00:00 Wakala Cross-Border Settlement Reconciler
2024/01/15 10:00:00 Listening on http://localhost:8080
2024/01/15 10:00:00 API base: http://localhost:8080/api/v1
```

The server defaults to port `8080`. Override with environment variables:

```bash
PORT=9090 DB_PATH=/tmp/wakala.db go run ./cmd/server
```

### Using the Makefile

```bash
make run               # start the server
make build             # compile binary to bin/server
make generate-testdata # regenerate CSV/JSON test files
make tidy              # go mod tidy
```

---

## Test Data

The dataset covers **14 days (2024-01-08 to 2024-01-21)** across three processors:

| Processor | Country | Currency | Transactions | Settlement Records | Fee Rate |
|---|---|---|---|---|---|
| AfriPay | Kenya | KES | 50 | 35 | 1.5% |
| NairaGateway | Nigeria | NGN | 55 | 42 | 1.0% |
| CapePay | South Africa | ZAR | 50 | 44 | 2.0% |

**Transaction status distribution:**

- 85% `captured` — eligible for settlement
- 10% `authorized` — never appear in settlement reports
- 5% `failed` — never appear in settlement reports

**Intentional discrepancies per report:**

| Issue | Rate | Description |
|---|---|---|
| Missing settlement | ~8% | Captured transaction absent from all reports |
| Amount mismatch | ~4% | Gross amount inflated 3–5% by processor error |
| Orphaned settlement | first 2 rows | Settlement references non-existent transaction IDs (`FAKE-AP-001`, `FAKE-NG-001`, etc.) |

**Exchange rates used** (approximate 2024 annual averages, hardcoded):

| Currency | Rate (per 1 USD) |
|---|---|
| KES | 129.50 |
| NGN | 1,580.00 |
| ZAR | 18.60 |

---

## Ingesting Settlement Reports

Each processor sends reports in a different format. The `/ingest` endpoint accepts a `format` parameter to select the correct parser.

### Format Reference

| `format` | Processor | Structure | Delimiter | Amount currency |
|---|---|---|---|---|
| `csv_a` | AfriPay (Kenya) | `transaction_id, merchant_ref, settlement_date, gross_amount_kes, fee_kes, net_kes, batch_id` | comma | KES |
| `json_b` | NairaGateway (Nigeria) | `{ "batch_id", "settlement_date", "records": [{ "ref", "amount_ngn", "processing_fee_ngn", "payout_ngn", "settled_at" }] }` | JSON | NGN |
| `csv_c` | CapePay (South Africa) | `TXREF\|MERCHANT\|SETTLE_DATE\|AMOUNT_ZAR\|DEDUCTIONS_ZAR\|NET_ZAR\|BATCH` | pipe `\|` | ZAR |

### Ingest all three test reports

```bash
# AfriPay — Kenya CSV
curl -X POST http://localhost:8080/api/v1/reports/ingest \
  -F "file=@testdata/processor_a_afripay.csv" \
  -F "processor=afripay" \
  -F "format=csv_a"

# NairaGateway — Nigeria JSON
curl -X POST http://localhost:8080/api/v1/reports/ingest \
  -F "file=@testdata/processor_b_nairagateway.json" \
  -F "processor=nairagateway" \
  -F "format=json_b"

# CapePay — South Africa pipe-delimited CSV
curl -X POST http://localhost:8080/api/v1/reports/ingest \
  -F "file=@testdata/processor_c_capepay.csv" \
  -F "processor=capepay" \
  -F "format=csv_c"
```

Re-uploading the same file returns `"report_id": "already-ingested"` with zero records inserted — full idempotency via SHA-256 file hash.

---

## API Reference

Base URL: `http://localhost:8080/api/v1`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/reports/ingest` | Upload a settlement report (multipart form) |
| `GET` | `/transactions` | List transactions with filters |
| `GET` | `/transactions/{id}/settlement-status` | Full settlement view for one transaction |
| `GET` | `/discrepancies` | List discrepancies with filters |
| `GET` | `/discrepancies/summary` | Aggregated discrepancy counts and financial impact |
| `GET` | `/settlements` | List settlement records with filters |
| `GET` | `/dashboard` | Finance team overview: volumes, counts, breakdowns |

### Common Query Parameters

**Pagination** (all list endpoints):

| Param | Default | Description |
|---|---|---|
| `page` | `1` | Page number |
| `limit` | `50` | Records per page |

**Date range** (all list endpoints):

| Param | Format | Example |
|---|---|---|
| `from` | `YYYY-MM-DD` | `2024-01-10` |
| `to` | `YYYY-MM-DD` | `2024-01-15` |

**Discrepancy filters:**

| Param | Values | Example |
|---|---|---|
| `type` | `MISSING_SETTLEMENT`, `AMOUNT_MISMATCH`, `ORPHANED_SETTLEMENT` | `?type=AMOUNT_MISMATCH` |
| `severity` | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` | `?severity=HIGH` |
| `processor` | `afripay`, `nairagateway`, `capepay` | `?processor=afripay` |

**Transaction filters:**

| Param | Values | Example |
|---|---|---|
| `status` | `authorized`, `captured`, `settled`, `failed` | `?status=captured` |
| `processor` | `afripay`, `nairagateway`, `capepay` | `?processor=capepay` |
| `currency` | `KES`, `NGN`, `ZAR`, `USD` | `?currency=NGN` |

---

## Sample Requests & Responses

### POST /api/v1/reports/ingest

```bash
curl -X POST http://localhost:8080/api/v1/reports/ingest \
  -F "file=@testdata/processor_a_afripay.csv" \
  -F "processor=afripay" \
  -F "format=csv_a"
```

```json
{
  "report_id": "RPT-afripay-1771960640215602000",
  "records_ingested": 35,
  "duplicates_skipped": 0,
  "discrepancies_detected": 100
}
```

Uploading the same file a second time:

```json
{
  "report_id": "already-ingested",
  "records_ingested": 0,
  "duplicates_skipped": 0,
  "discrepancies_detected": 0
}
```

---

### GET /api/v1/dashboard

```bash
curl http://localhost:8080/api/v1/dashboard
```

```json
{
  "period": { "from": "2024-01-08", "to": "2024-01-21" },
  "transactions": {
    "total": 155,
    "captured": 14,
    "settled": 115,
    "pending_settlement": 33
  },
  "volume": {
    "total_usd": 41303.44,
    "settled_usd": 30633.05,
    "unsettled_usd": 8880.47
  },
  "discrepancies": {
    "total": 26,
    "critical": 0,
    "high": 12,
    "medium": 12,
    "low": 2,
    "total_impact_usd": 5156.56
  },
  "by_processor": [
    { "processor": "afripay",      "settled_usd": 8737.45,  "discrepancy_count": 9, "discrepancy_impact_usd": 1320.36 },
    { "processor": "capepay",      "settled_usd": 11488.29, "discrepancy_count": 8, "discrepancy_impact_usd": 1885.95 },
    { "processor": "nairagateway", "settled_usd": 10407.31, "discrepancy_count": 9, "discrepancy_impact_usd": 1950.24 }
  ],
  "by_currency": [
    { "currency": "KES", "volume": 12858.08, "settled_volume": 8737.45 },
    { "currency": "NGN", "volume": 14949.74, "settled_volume": 10407.31 },
    { "currency": "ZAR", "volume": 13495.62, "settled_volume": 11488.29 }
  ]
}
```

---

### GET /api/v1/discrepancies/summary

```bash
curl http://localhost:8080/api/v1/discrepancies/summary
```

```json
{
  "total_count": 26,
  "total_impact_usd": 5156.56,
  "by_type": {
    "MISSING_SETTLEMENT":  14,
    "AMOUNT_MISMATCH":      6,
    "ORPHANED_SETTLEMENT":  6
  },
  "by_severity": {
    "HIGH":   12,
    "MEDIUM": 12,
    "LOW":     2
  },
  "by_processor": {
    "afripay":      9,
    "capepay":      8,
    "nairagateway": 9
  },
  "impact_by_processor": {
    "afripay":      1320.36,
    "capepay":      1885.95,
    "nairagateway": 1950.24
  }
}
```

---

### GET /api/v1/discrepancies — Filtered examples

```bash
# Missing settlements — page 1
curl "http://localhost:8080/api/v1/discrepancies?type=MISSING_SETTLEMENT&limit=2"
```

```json
{
  "discrepancies": [
    {
      "id": "DISC-MS-WKL-CAPEPAY-002",
      "type": "MISSING_SETTLEMENT",
      "transaction_id": "WKL-CAPEPAY-002",
      "processor": "capepay",
      "expected_usd": 417.30,
      "actual_usd": 0,
      "difference_usd": 417.30,
      "currency": "ZAR",
      "severity": "MEDIUM",
      "description": "Transaction WKL-CAPEPAY-002 (417.30 USD) captured but no settlement found from capepay",
      "detected_at": "2024-01-23T10:00:00Z"
    },
    {
      "id": "DISC-MS-WKL-AFRIPAY-036",
      "type": "MISSING_SETTLEMENT",
      "transaction_id": "WKL-AFRIPAY-036",
      "processor": "afripay",
      "expected_usd": 34.53,
      "actual_usd": 0,
      "difference_usd": 34.53,
      "currency": "KES",
      "severity": "LOW",
      "description": "Transaction WKL-AFRIPAY-036 (34.53 USD) captured but no settlement found from afripay",
      "detected_at": "2024-01-23T10:00:00Z"
    }
  ],
  "total": 14,
  "total_impact_usd": 5156.56,
  "page": 1,
  "limit": 2
}
```

```bash
# Amount mismatches — HIGH severity
curl "http://localhost:8080/api/v1/discrepancies?type=AMOUNT_MISMATCH&severity=HIGH"
```

```json
{
  "discrepancies": [
    {
      "id": "DISC-AM-SR-AP-AP-TXN-007-7",
      "type": "AMOUNT_MISMATCH",
      "transaction_id": "WKL-AFRIPAY-007",
      "settlement_id": "SR-AP-AP-TXN-007-7",
      "processor": "afripay",
      "expected_usd": 353.72,
      "actual_usd": 368.12,
      "difference_usd": 14.40,
      "currency": "KES",
      "severity": "HIGH",
      "description": "Gross amount mismatch for WKL-AFRIPAY-007: expected 353.72 USD, reported gross 368.12 USD (4.1% diff)",
      "detected_at": "2024-01-23T10:00:00Z"
    }
  ],
  "total": 6,
  "page": 1,
  "limit": 50
}
```

```bash
# Orphaned settlements — money received with no matching transaction
curl "http://localhost:8080/api/v1/discrepancies?type=ORPHANED_SETTLEMENT"
```

```json
{
  "discrepancies": [
    {
      "id": "DISC-OS-SR-AP-FAKE-AP-001-2",
      "type": "ORPHANED_SETTLEMENT",
      "settlement_id": "SR-AP-FAKE-AP-001-2",
      "processor": "afripay",
      "expected_usd": 0,
      "actual_usd": 106.74,
      "difference_usd": 106.74,
      "currency": "KES",
      "severity": "HIGH",
      "description": "Orphaned settlement SR-AP-FAKE-AP-001-2 from afripay: 106.74 USD with no matching transaction (proc_ref=FAKE-AP-001)",
      "detected_at": "2024-01-23T10:00:00Z"
    }
  ],
  "total": 6,
  "page": 1,
  "limit": 50
}
```

---

### GET /api/v1/transactions/{id}/settlement-status

```bash
curl http://localhost:8080/api/v1/transactions/WKL-AFRIPAY-007/settlement-status
```

```json
{
  "transaction": {
    "id": "WKL-AFRIPAY-007",
    "processor_reference": "AP-TXN-007",
    "processor": "afripay",
    "merchant_id": "M013",
    "customer_country": "KE",
    "merchant_country": "KE",
    "amount": 45810.34,
    "currency": "KES",
    "usd_amount": 353.72,
    "status": "settled",
    "created_at": "2024-01-10T12:00:00Z",
    "captured_at": "2024-01-10T12:45:00Z",
    "settled_at": "2024-01-11T00:00:00Z"
  },
  "settlements": [
    {
      "id": "SR-AP-AP-TXN-007-7",
      "processor": "afripay",
      "gross_amount": 47671.03,
      "fee_amount": 715.07,
      "net_amount": 46955.96,
      "currency": "KES",
      "usd_gross_amount": 368.12,
      "usd_net_amount": 362.59,
      "settlement_date": "2024-01-11T00:00:00Z",
      "batch_id": "KE-BATCH-001"
    }
  ],
  "discrepancies": [
    {
      "type": "AMOUNT_MISMATCH",
      "expected_usd": 353.72,
      "actual_usd": 368.12,
      "difference_usd": 14.40,
      "severity": "HIGH"
    }
  ]
}
```

---

### GET /api/v1/transactions — Filtered list

```bash
# Captured transactions pending settlement
curl "http://localhost:8080/api/v1/transactions?status=captured"

# NairaGateway transactions in a date range
curl "http://localhost:8080/api/v1/transactions?processor=nairagateway&from=2024-01-10&to=2024-01-15"
```

---

### GET /api/v1/settlements — Filtered list

```bash
curl "http://localhost:8080/api/v1/settlements?processor=nairagateway&limit=2"
```

```json
{
  "settlements": [
    {
      "id": "SR-NG-NG-TXN-004-3",
      "processor": "nairagateway",
      "processor_transaction_id": "NG-TXN-004",
      "wakala_transaction_id": "WKL-NAIRAGATEWAY-004",
      "gross_amount": 84356.20,
      "fee_amount": 843.56,
      "net_amount": 83512.64,
      "currency": "NGN",
      "usd_gross_amount": 53.39,
      "usd_net_amount": 52.86,
      "settlement_date": "2024-01-21T23:59:59+01:00",
      "batch_id": "NG-BATCH-001"
    }
  ],
  "total": 42,
  "page": 1,
  "limit": 2
}
```

---

## Discrepancy Detection Logic

Reconciliation runs automatically after every successful report ingestion as a full pass — clears previous discrepancies and re-detects — ensuring a consistent view across all ingested reports.

### Step 1 — Match Settlements

For each unmatched settlement record, look up a Wakala transaction by `processor_reference`. On match:
- Sets `wakala_transaction_id` on the settlement record
- Updates transaction `status` to `settled`
- Logs a **confidence score** based on gross USD difference:

| Score | Condition |
|---|---|
| **1.00** | Gross USD difference < 0.5% (exact or FX rounding) |
| **0.95** | Gross USD difference 0.5–1% |
| **0.90** | Gross USD difference 1–2% |
| **0.80** | Gross USD difference 2–5% |
| **0.60** | Gross USD difference > 5% |

### Step 2 — Detect Missing Settlements

Finds all `captured` transactions that have no matching settlement record.

**Severity scale:**

| Severity | USD amount |
|---|---|
| HIGH | > $500 |
| MEDIUM | $100–$500 |
| LOW | < $100 |

### Step 3 — Detect Amount Mismatches

Compares `settlement.usd_gross_amount` vs `transaction.usd_amount` for every matched pair.

> **Why gross and not net?** The gross amount is what the processor charged the customer — it should match the original transaction amount exactly. Net is intentionally lower due to expected fee deductions. Using net would flag every clean settlement as a mismatch.

A discrepancy is created when the gross difference exceeds **0.5% or $0.10**:

| Severity | Condition |
|---|---|
| CRITICAL | `abs_diff > $500` |
| HIGH | `pct_diff > 2%` |
| MEDIUM | `pct_diff <= 2%` (but above threshold) |

### Step 4 — Detect Orphaned Settlements

Settlement records that could not be matched to any known Wakala transaction. Always **HIGH** severity — they represent money received from an unknown source, which may indicate duplicate payments, data corruption, or fraud.

---

## Assumptions & Trade-offs

### Assumptions

1. **Settlement window is 48 hours.** In production, captured transactions without settlement after 48h are overdue. For this prototype, the check uses a generous future cutoff to catch all historical test data.

2. **Gross amount is the reconciliation anchor.** Fee deductions (net amount) are expected and do not constitute a mismatch. Only differences in the gross charged amount are flagged.

3. **Exchange rates are static.** KES/NGN/ZAR rates are hardcoded as 2024 annual approximations. All amounts are normalized to USD for cross-currency comparison.

4. **One transaction, one settlement.** The model assumes 1:1 matching by `processor_reference`. Split settlements or partial captures are out of scope for this prototype.

5. **Processor references are unique per processor.** Matching is done by `(processor, processor_reference)`.

6. **No authentication required.** The API is internal-only per spec.

7. **Reports arrive as complete batches.** Partial or streaming ingestion is not supported.

### Trade-offs

| Decision | Alternative | Reason |
|---|---|---|
| SQLite | PostgreSQL | Zero setup; WAL mode gives acceptable concurrent read performance for prototype scale |
| Full reconciliation on each ingest | Incremental delta | Simpler correctness guarantees; acceptable for report-sized batches (< 10k records) |
| SHA-256 file hash for idempotency | Unique batch ID | Works even if the processor resends without changing the batch ID |
| Static FX rates | Live FX API | Eliminates external dependency; ~2% rate drift is acceptable for a prototype |
| Gross vs net for mismatch detection | Net with expected-fee allowance | Gross comparison is unambiguous; net comparison requires knowing each processor's fee schedule in advance |

---

## Production Extension Notes

1. **Live FX rates** — Integrate a rates API (Open Exchange Rates, ECB) and store the rate at transaction time for exact audit trails.

2. **PostgreSQL** — Replace SQLite with PostgreSQL, add connection pooling, proper migration tooling (e.g., `golang-migrate`), and read replicas for reporting queries.

3. **Async ingestion pipeline** — Move report processing to a background worker queue (Kafka, SQS). The ingest endpoint returns a job ID immediately; clients poll for completion. Handles large reports (100k+ records) without HTTP timeout risk.

4. **Alerting rules** — Emit events when: any single discrepancy exceeds $500, daily missing settlement rate exceeds 10% of volume, or any orphaned settlement is detected. Integrate with PagerDuty or Slack.

5. **Per-processor fee schedules** — Store expected fee rates per processor. Use them to validate that `fee_amount` is within the expected range before accepting the settlement.

6. **Partial settlement support** — Handle processors that split a single capture into multiple settlement tranches, which is common with installment products.

7. **Authentication & RBAC** — Finance team read-only access; ops team write access for ingestion; full audit log on all mutations.

8. **Automated report fetching** — Poll processor SFTP/S3 buckets on a schedule instead of requiring manual uploads.
