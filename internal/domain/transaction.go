package domain

import "time"

type TransactionStatus string

const (
	StatusAuthorized TransactionStatus = "authorized"
	StatusCaptured   TransactionStatus = "captured"
	StatusSettled    TransactionStatus = "settled"
	StatusFailed     TransactionStatus = "failed"
)

type Processor string

const (
	ProcessorAfriPay      Processor = "afripay"
	ProcessorNairaGateway Processor = "nairagateway"
	ProcessorCapePay      Processor = "capepay"
)

type Transaction struct {
	ID                 string            `json:"id"`
	ProcessorReference string            `json:"processor_reference"`
	Processor          Processor         `json:"processor"`
	MerchantID         string            `json:"merchant_id"`
	CustomerCountry    string            `json:"customer_country"`
	MerchantCountry    string            `json:"merchant_country"`
	Amount             float64           `json:"amount"`
	Currency           string            `json:"currency"`
	USDAmount          float64           `json:"usd_amount"`
	Status             TransactionStatus `json:"status"`
	CreatedAt          time.Time         `json:"created_at"`
	CapturedAt         *time.Time        `json:"captured_at,omitempty"`
	SettledAt          *time.Time        `json:"settled_at,omitempty"`
}
