package domain

type Account struct {
	BankID string `json:"bank_id,omitempty"`
	ID     string `json:"id,omitempty"`
}
