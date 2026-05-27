package converter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const defaultFrankfurterURL = "https://api.frankfurter.app"

// currencyISO maps the human-readable currency labels that appear in the AML
// dataset to ISO-4217 codes that the Frankfurter API understands. Entries
// missing from this map are treated as unsupported and the converter will skip
// transactions denominated in them (logged + counted as received but not sent).
//
// Frankfurter only covers major fiat currencies. Bitcoin and currencies like
// the Ruble or the Saudi Riyal are intentionally absent — they are not
// supported upstream.
var currencyISO = map[string]string{
	"US Dollar":         "USD",
	"Euro":              "EUR",
	"UK Pound":          "GBP",
	"Yuan":              "CNY",
	"Yen":               "JPY",
	"Australian Dollar": "AUD",
	"Canadian Dollar":   "CAD",
	"Mexican Peso":      "MXN",
	"Brazil Real":       "BRL",
	"Rupee":             "INR",
	"Swiss Franc":       "CHF",
	"Shekel":            "ILS",
}

// ErrUnsupportedCurrency is returned when the source currency does not have a
// known ISO mapping or is not quoted by Frankfurter.
var ErrUnsupportedCurrency = fmt.Errorf("unsupported currency")

// fallbackUSDPerUnit holds USD per 1 unit of the currency, used only when
// Frankfurter does not quote the source currency. Values follow the AML
// dataset generator table (Sep 2022 era); they're constant in time, which is
// exact for the pegged Saudi Riyal but an approximation for the more volatile
// Bitcoin and Ruble. Worth revisiting if the dataset window shifts materially.
var fallbackUSDPerUnit = map[string]float64{
	"Saudi Riyal": 1.0 / 3.75, // pegged at 3.75 SAR / USD
	"Ruble":       1.0 / 60.5, // ≈ Sep 2022
	"Bitcoin":     20000.0,    // ≈ Sep 2022
}

type rateKey struct {
	date     string // YYYY-MM-DD
	currency string // ISO-4217 code (e.g. EUR)
}

// rateClient fetches and caches USD conversion rates from the Frankfurter API.
type rateClient struct {
	baseURL    string
	httpClient *http.Client

	mu    sync.RWMutex
	cache map[rateKey]float64
}

func newRateClient() *rateClient {
	base := os.Getenv("FRANKFURTER_URL")
	if base == "" {
		base = defaultFrankfurterURL
	}
	return &rateClient{
		baseURL:    base,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		cache:      make(map[rateKey]float64),
	}
}

// convertToUSD returns the USD-equivalent amount for the given currency on the
// given date. Looks up the cache first; on miss, hits Frankfurter and stores
// the result.
func (rc *rateClient) convertToUSD(date string, currency string, amount float64) (float64, error) {
	iso, ok := currencyISO[currency]
	if !ok {
		if rate, ok := fallbackUSDPerUnit[currency]; ok {
			return amount * rate, nil
		}
		return 0, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, currency)
	}
	if iso == "USD" {
		return amount, nil
	}

	key := rateKey{date: date, currency: iso}

	rc.mu.RLock()
	rate, hit := rc.cache[key]
	rc.mu.RUnlock()
	if hit {
		return amount * rate, nil
	}

	rate, err := rc.fetchRate(date, iso)
	if err != nil {
		return 0, err
	}

	rc.mu.Lock()
	rc.cache[key] = rate
	rc.mu.Unlock()

	return amount * rate, nil
}

// fetchRate calls Frankfurter for one (date, currency) pair and returns the
// rate of 1 unit of `iso` in USD. Frankfurter's response includes the date
// actually used (Frankfurter falls back to the most recent business day for
// weekends/holidays) but we cache against the requested date — this slightly
// over-fetches across weekends but keeps the cache key simple.
func (rc *rateClient) fetchRate(date string, iso string) (float64, error) {
	endpoint := fmt.Sprintf("%s/%s", rc.baseURL, url.PathEscape(date))
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("building frankfurter request: %w", err)
	}
	q := req.URL.Query()
	q.Set("from", iso)
	q.Set("to", "USD")
	req.URL.RawQuery = q.Encode()

	resp, err := rc.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("calling frankfurter: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("frankfurter returned status %d for %s on %s", resp.StatusCode, iso, date)
	}

	var body struct {
		Rates map[string]float64 `json:"rates"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("decoding frankfurter response: %w", err)
	}
	rate, ok := body.Rates["USD"]
	if !ok {
		return 0, fmt.Errorf("frankfurter response missing USD rate for %s", iso)
	}
	return rate, nil
}

// transactionDate normalises a transaction timestamp into Frankfurter's
// YYYY-MM-DD date format. Accepts a handful of layouts the dataset has been
// observed to produce.
func transactionDate(timestamp string) (string, error) {
	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		"2006/01/02 15:04",
		"2006-01-02",
	}
	for _, layout := range layouts {
		t, err := time.Parse(layout, timestamp)
		if err == nil {
			return t.Format("2006-01-02"), nil
		}
	}
	return "", fmt.Errorf("unrecognised timestamp format: %q", timestamp)
}
