package domain

import "github.com/ManusaRivi/money-laundering-analysis/src/common/broker"

type EOFCounts struct {
	Counts map[broker.KeyType]int `json:"counts,omitempty"`
}
