package domain

type BankInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	AccountNumber string `json:"account_number,omitempty"`
	EntityID      string `json:"entity_id,omitempty"`
	EntityName    string `json:"entity_name,omitempty"`
}
