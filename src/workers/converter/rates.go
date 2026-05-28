package converter

import (
	"encoding/json"
	"fmt"
	"log/slog"
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
// Frankfurter does not quote the source currency and no date-specific table
// applies (see bitcoinUSDPerUnit). Exact for the pegged Saudi Riyal,
// approximate for Ruble; the Bitcoin entry is a last-resort default for dates
// outside bitcoinUSDPerUnit.
var fallbackUSDPerUnit = map[string]float64{
	"Saudi Riyal": 1.0 / 3.75, // pegged at 3.75 SAR / USD
	"Ruble":       1.0 / 60.5, // ≈ Sep 2022
	"Bitcoin":     20000.0,    // ≈ Sep 2022, used when date not in bitcoinUSDPerUnit
}

// notebookForeignPerUSD mirrors the hardcoded conversion_rates_records table
// in money-laundering-analysis.ipynb. Outer key is date (YYYY-MM-DD); inner
// key is the human-readable currency label used in the dataset. Values are
// **units of the foreign currency per 1 USD** — i.e. apply as
// `usd_amount = paid_amount / notebookForeignPerUSD[date][currency]`,
// the same operation the notebook performs.
//
// When this table has a (date, currency) entry, convertToUSD uses it
// verbatim and bypasses Frankfurter — useful for reproducing the notebook's
// row-by-row results exactly.
var notebookForeignPerUSD = map[string]map[string]float64{
	"2022-09-01": {
		"Australian Dollar": 1.4644,
		"Brazil Real":       5.1805,
		"Canadian Dollar":   1.314,
		"Swiss Franc":       0.97999,
		"Yuan":              6.9,
		"Euro":              1.0002,
		"UK Pound":          0.86272,
		"Shekel":            3.3535,
		"Rupee":             79.543,
		"Yen":               139.34,
		"Mexican Peso":      20.189,
		"Ruble":             60.367,
		"Saudi Riyal":       3.75,
		"US Dollar":         1.0,
		"Bitcoin":           19793.1,
	},
	"2022-09-02": {
		"Australian Dollar": 1.4691,
		"Brazil Real":       5.2035,
		"Canadian Dollar":   1.3141,
		"Swiss Franc":       0.98175,
		"Yuan":              6.9035,
		"Euro":              1.0011,
		"UK Pound":          0.86468,
		"Shekel":            3.3755,
		"Rupee":             79.719,
		"Yen":               140.11,
		"Mexican Peso":      20.085,
		"Ruble":             60.427,
		"Saudi Riyal":       3.75,
		"US Dollar":         1.0,
		"Bitcoin":           199999.0, // notebook value verbatim; suspected typo, see bitcoinUSDPerUnit
	},
	"2022-09-03": {
		"Australian Dollar": 1.4691,
		"Brazil Real":       5.2056,
		"Canadian Dollar":   1.3138,
		"Swiss Franc":       0.98207,
		"Yuan":              6.9046,
		"Euro":              1.0013,
		"UK Pound":          0.86478,
		"Shekel":            3.3791,
		"Rupee":             79.75,
		"Yen":               140.17,
		"Mexican Peso":      20.081,
		"Ruble":             60.471,
		"Saudi Riyal":       3.75,
		"US Dollar":         1.0,
		"Bitcoin":           19831.4,
	},
	"2022-09-04": {
		"Australian Dollar": 1.4695,
		"Brazil Real":       5.2082,
		"Canadian Dollar":   1.3139,
		"Swiss Franc":       0.98219,
		"Yuan":              6.9047,
		"Euro":              1.0013,
		"UK Pound":          0.8649,
		"Shekel":            3.3815,
		"Rupee":             79.754,
		"Yen":               140.22,
		"Mexican Peso":      20.084,
		"Ruble":             60.461,
		"Saudi Riyal":       3.75,
		"US Dollar":         1.0,
		"Bitcoin":           19952.7,
	},
	"2022-09-05": {
		"Australian Dollar": 1.4722,
		"Brazil Real":       5.1786,
		"Canadian Dollar":   1.3142,
		"Swiss Franc":       0.98273,
		"Yuan":              6.9216,
		"Euro":              1.0068,
		"UK Pound":          0.86813,
		"Shekel":            3.4006,
		"Rupee":             79.816,
		"Yen":               140.49,
		"Mexican Peso":      20.018,
		"Ruble":             60.737,
		"Saudi Riyal":       3.75,
		"US Dollar":         1.0,
		"Bitcoin":           20126.1,
	},
}

// bitcoinUSDPerUnit holds the per-day USD-per-1-BTC rate from the dataset
// generator's reference table. Keyed by date in YYYY-MM-DD format (matching
// transactionDate output). For dates outside this map we fall back to
// fallbackUSDPerUnit["Bitcoin"].
//
// NOTE: the source table value for 2022-09-02 was 199999.0, an order of
// magnitude above neighbouring days (~19,800). Preserved verbatim here for
// fidelity, but it almost certainly should be ~19,999.0 — confirm before
// running production datasets that include that date.
var bitcoinUSDPerUnit = map[string]float64{
	"2022-09-01": 19793.1,
	"2022-09-02": 199999.0, // <-- suspected typo; see note above
	"2022-09-03": 19831.4,
	"2022-09-04": 19952.7,
	"2022-09-05": 20126.1,
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
	// Notebook-parity path: if we have the exact notebook rate for this
	// (date, currency) pair, use it verbatim with the notebook's `amount / rate`
	// convention (rate stored as foreign-per-USD).
	if dayRates, ok := notebookForeignPerUSD[date]; ok {
		if rate, ok := dayRates[currency]; ok {
			return amount / rate, nil
		}
	}

	iso, ok := currencyISO[currency]
	if !ok {
		// Frankfurter doesn't quote this currency. Try a date-specific table
		// first (Bitcoin), then the static per-currency fallback.
		if currency == "Bitcoin" {
			if rate, ok := bitcoinUSDPerUnit[date]; ok {
				slog.Debug("Bitcoin currency detected. USD conversion using date-specific fallback rate.", "date", date, "rate", rate, "end_amount", amount*rate)
				return amount * rate, nil
			}
		}
		/* if rate, ok := fallbackUSDPerUnit[currency]; ok {
			slog.Debug("Falling back to static USD conversion rate", "currency", currency, "rate", rate)
			return amount * rate, nil
		} */
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
