package domain

type Account struct {
	BankID string
	ID     string
}

func (a *Account) GetID() string {
	return a.BankID + "-" + a.ID
}