package domain

type TypeTxQ4 string

const (
	TxQ4Scatter TypeTxQ4 = "scatter"
	TxQ4Gather TypeTxQ4 = "gather"
)

type TxQ4 struct {
	Type		TypeTxQ4 `json:"type,omitempty"`
	Transaction *Transaction `json:"transaction,omitempty"`
}

type TxQ4Phase2 struct {
	ScatterGather map[string]int `json:"scatter_gather,omitempty"`
	Accounts []Account `json:"accounts,omitempty"`
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
