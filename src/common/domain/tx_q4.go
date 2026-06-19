package domain

import "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol"

type TypeTxQ4 string

const (
	TxQ4Scatter TypeTxQ4 = "scatter"
	TxQ4Gather  TypeTxQ4 = "gather"
)

type TxQ4PairKey struct {
	Src, Dst string
}

type TxQ4PairEntry struct {
	Count      int
	SrcAccount Account
	DstAccount Account
}

func (e *TxQ4PairEntry) Merge(other *TxQ4PairEntry) {
	e.Count += other.Count
}

type TxQ4PhaseOne struct {
	Type TypeTxQ4
	// Transaction *Transaction
	Transaction *protocol.Transaction
}

// TxQ4PairCount carries one pair's partial bridge count from the scatter-gather
// aggregator to the accumulator. Accounts are not included: each side of the key
// is an Account.GetID(), so they are reconstructed downstream from the key.
type TxQ4PairCount struct {
	Key   TxQ4PairKey
	Count int
}

type TxQ4PhaseThree struct {
	ScatterGather map[string]*TxQ4PairEntry
}

func (k TxQ4PairKey) Key() string {
	return k.Src + "::" + k.Dst
}

func GetTypeTxQ4ByField(field string) TypeTxQ4 {
	switch TxFieldOptions(field) {
	case TxFieldOrigin:
		return TxQ4Scatter
	case TxFieldDest:
		return TxQ4Gather
	default:
		return ""
	}
}
