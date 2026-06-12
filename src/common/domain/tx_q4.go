package domain

import "github.com/ManusaRivi/money-laundering-analysis/src/common/protocol/external"

type TypeTxQ4 string

const (
	TxQ4Scatter TypeTxQ4 = "scatter"
	TxQ4Gather TypeTxQ4 = "gather"
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
	Type		TypeTxQ4
	// Transaction *Transaction
	Transaction *external.Transaction
}

type TxQ4PhaseTwo struct {
	Key        TxQ4PairKey 
	Count      int         
	SrcAccount *Account    
	DstAccount *Account    
}

type TxQ4PhaseThree struct {
	ScatterGather map[string]*TxQ4PairEntry
}

func (k TxQ4PairKey) Key() string {
	return k.Src + "::" + k.Dst
}


func GetTypeTxQ4ByField(field string) TypeTxQ4 {
	switch (TxFieldOptions(field)) {
	case TxFieldOrigin:
		return TxQ4Scatter
	case TxFieldDest:
		return TxQ4Gather
	default:
		return ""
	}
}
