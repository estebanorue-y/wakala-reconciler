package currency

import "fmt"

// ratesPerUSD maps currency codes to the number of local currency units per 1 USD.
// These are approximate 2024 rates for African corridors.
var ratesPerUSD = map[string]float64{
	"USD": 1.0,
	"KES": 129.5,  // Kenyan Shilling
	"NGN": 1580.0, // Nigerian Naira
	"ZAR": 18.6,   // South African Rand
}

// ToUSD converts a local currency amount to USD.
func ToUSD(amount float64, currency string) (float64, error) {
	rate, ok := ratesPerUSD[currency]
	if !ok {
		return 0, fmt.Errorf("unsupported currency: %s", currency)
	}
	return amount / rate, nil
}

// FromUSD converts a USD amount to local currency.
func FromUSD(usdAmount float64, currency string) (float64, error) {
	rate, ok := ratesPerUSD[currency]
	if !ok {
		return 0, fmt.Errorf("unsupported currency: %s", currency)
	}
	return usdAmount * rate, nil
}

// Rate returns the exchange rate for a given currency (units per 1 USD).
func Rate(currency string) (float64, error) {
	rate, ok := ratesPerUSD[currency]
	if !ok {
		return 0, fmt.Errorf("unsupported currency: %s", currency)
	}
	return rate, nil
}
