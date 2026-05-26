package broker

type KeyType string

const (
	KeyDollarTransaction	KeyType = "tx.usd"
	KeyNonDollarTransaction	KeyType = "tx.non-usd"

	KeyControlEOF			KeyType	= "control.eof"
	KeyNil					KeyType	= ""
)

func StringsToKeyType(s []string) []KeyType {
	r := make([]KeyType, len(s))
	for i, v := range s {
		r[i] = KeyType(v)
	}
	return r
}