package domain

type Query1Result struct {
	FromBank    string  `json:"from_bank"`
	FromAccount string  `json:"from_account"`
	ToBank      string  `json:"to_bank"`
	ToAccount   string  `json:"to_account"`
	AmountPaid  float64 `json:"amount_paid"`
}

type Query2Result struct {
	FromBank    string  `json:"from_bank"`
	FromAccount string  `json:"from_account"`
	BankName    string  `json:"bank_name"`
	AmountPaid  float64 `json:"amount_paid"`
}

type Query3Result struct {
	FromBank      string  `json:"from_bank"`
	FromAccount   string  `json:"from_account"`
	PaymentFormat string  `json:"payment_format"`
	AmountPaid    float64 `json:"amount_paid"`
}

type Query4Result struct {
	Bank    string `json:"bank"`
	Account string `json:"account"`
}

type Query5Result struct {
	Count int `json:"count"`
}
