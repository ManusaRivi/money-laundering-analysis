package domain

type Transaction struct {
	Timestamp string  `json:"timestamp,omitempty"`
	Origin    Account `json:"origin,omitempty"`
	Dest      Account `json:"dest,omitempty"`
	Paid      Money   `json:"paid,omitempty"`
	Received  Money   `json:"received,omitempty"`
	Format    string  `json:"format,omitempty"`
}
