package domain

type Account struct {
	BankID string `json:"bank_id,omitempty"`
	ID     string `json:"id,omitempty"`
}

func (a *Account) GetID() string {
	return a.BankID + "-" + a.ID
}