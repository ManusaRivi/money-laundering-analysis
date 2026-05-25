package domain

type Transaction struct {
	Timestamp string   `json:"timestamp,omitempty"`
	Origin    *Account `json:"origin,omitempty"`
	Dest      *Account `json:"dest,omitempty"`
	Paid      *Money   `json:"paid,omitempty"`
	Format    string   `json:"format,omitempty"`
}

func (t *Transaction) IsUSDTransaction() bool {
	return t.Paid.Currency == "US Dollar"
}

func (t *Transaction) CutColumn(column string) {
	switch column {
	case "timestamp":
		t.Timestamp = ""
	case "origin":
		t.Origin = nil
	case "dest":
		t.Dest = nil
	case "paid":
		t.Paid = nil
	case "format":
		t.Format = ""
	}
}
