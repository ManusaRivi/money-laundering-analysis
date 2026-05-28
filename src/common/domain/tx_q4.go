package domain

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
	Type		TypeTxQ4 `json:"type"`
	Transaction *Transaction `json:"transaction"`
}

type TxQ4PhaseTwo struct {
	Key        TxQ4PairKey `json:"key"`
	Count      int         `json:"count"`
	SrcAccount *Account    `json:"src_account"`
	DstAccount *Account    `json:"dst_account"`
	// Entry	  TxQ4PairEntry `json:"entry"`
}

type TxQ4PhaseThree struct {
	ScatterGather map[string]*TxQ4PairEntry `json:"scatter_gather"`
}

func (k TxQ4PairKey) Key() string {
	return k.Src + "::" + k.Dst
}

func MakePhaseThree(gather map[TxQ4PairKey]*TxQ4PairEntry) TxQ4PhaseThree {
	out := make(map[string]*TxQ4PairEntry, len(gather))
	for k, v := range gather {
		out[k.Key()] = v
	}
	return TxQ4PhaseThree{ScatterGather: out}
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
