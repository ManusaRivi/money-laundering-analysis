package domain

type TxFieldOptions string
const (
	TxFieldOrigin TxFieldOptions = "origin"
	TxFieldDest   TxFieldOptions = "dest"
	TxFieldBankID TxFieldOptions = "BankID"
	TxFieldID     TxFieldOptions = "ID"
)
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

func (t *Transaction) GetOriginId() string {
	return t.Origin.GetID()
}

func (t *Transaction) GetDestId() string {
	return t.Dest.GetID()
}

func (t *Transaction) GetTransactionId() string {
	return t.Origin.GetID() + "-" + t.Dest.GetID() + "-" + t.Timestamp
}

func (t *Transaction) GetTransactionField(field string) string {
	switch field {
	case "origin":
		if t.Origin != nil {
			return t.Origin.GetID()
		}
	case "dest":
		if t.Dest != nil {
			return t.Dest.GetID()
		}
	case "BankID":
		if t.Origin != nil {
			return t.Origin.BankID
		}
	case "ID":
		if t.Origin != nil {
			return t.Origin.ID
		}
	}
	return ""
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
