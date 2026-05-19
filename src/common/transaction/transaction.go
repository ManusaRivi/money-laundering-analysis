package transaction

import "time"

type Transaction struct {
    Timestamp		string  `json:"Timestamp"`
	FromBank     	string  `json:"From_Bank"`
	Account       	string  `json:"Account"`
	ToBank        	string  `json:"To_Bank"`
	Account1      	string  `json:"Account1"`
    Amount        	float64 `json:"Amount"`
    PaymentCurrency string  `json:"Payment_Currency"` 
    PaymentFormat 	string  `json:"Payment_Format"`
}

func (tx Transaction) GetStringProperty(fieldName string) (string, bool) {
    switch fieldName {
    case "Timestamp", "timestamp":
        return tx.Timestamp, true
    case "PaymentCurrency", "payment_currency":
        return tx.PaymentCurrency, true
    case "PaymentFormat", "payment_format":
        return tx.PaymentFormat, true
    case "FromBank", "from_bank":
        return tx.FromBank, true
    case "ToBank", "to_bank":
        return tx.ToBank, true
    case "Account", "account":
        return tx.Account, true
    case "Account1", "account1":
        return tx.Account1, true
    default:
        return "", false
    }
}

func (tx Transaction) GetFloatProperty(fieldName string) (float64, bool) {
    switch fieldName {
    case "Amount", "amount":
        return tx.Amount, true
    default:
        return 0, false
    }
}