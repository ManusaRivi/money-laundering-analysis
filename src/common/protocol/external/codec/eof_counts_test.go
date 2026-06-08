package codec

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/ManusaRivi/money-laundering-analysis/src/common/broker"
)

func TestEOFCountsRoundTrip(t *testing.T) {
	c := New()

	cases := map[string]map[broker.KeyType]int{
		"nil":   nil,
		"empty": {},
		"single nil key": {
			broker.KeyNil: 42,
		},
		"all partition keys": {
			broker.KeyNil:                  100,
			broker.KeyDollarTransaction:    60,
			broker.KeyNonDollarTransaction: 40,
			broker.KeyAllTransaction:       100,
		},
		"zero value": {
			broker.KeyDollarTransaction: 0,
		},
	}

	for name, counts := range cases {
		t.Run(name, func(t *testing.T) {
			payload, err := c.EncodeEOFCounts(counts)
			if err != nil {
				t.Fatalf("EncodeEOFCounts: %v", err)
			}

			got, err := c.DecodeEOFCounts(payload)
			if err != nil {
				t.Fatalf("DecodeEOFCounts: %v", err)
			}

			// Decode always yields a non-nil map; compare against an empty map
			// when the input was nil so reflect.DeepEqual matches.
			want := counts
			if want == nil {
				want = map[broker.KeyType]int{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip mismatch: got %v, want %v", got, want)
			}
		})
	}
}

// Encoding the same map twice must produce identical bytes: callers rely on
// deterministic output for stable logging and byte-level comparisons.
func TestEOFCountsDeterministic(t *testing.T) {
	c := New()
	counts := map[broker.KeyType]int{
		broker.KeyNil:                  100,
		broker.KeyDollarTransaction:    60,
		broker.KeyNonDollarTransaction: 40,
		broker.KeyAllTransaction:       100,
	}

	first, err := c.EncodeEOFCounts(counts)
	if err != nil {
		t.Fatalf("EncodeEOFCounts (first): %v", err)
	}
	second, err := c.EncodeEOFCounts(counts)
	if err != nil {
		t.Fatalf("EncodeEOFCounts (second): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("non-deterministic encoding: %v != %v", first, second)
	}
}

// A truncated payload (header claims an entry that isn't there) must error
// rather than panic or return a partial map.
func TestDecodeEOFCountsTruncated(t *testing.T) {
	c := New()
	full, err := c.EncodeEOFCounts(map[broker.KeyType]int{broker.KeyDollarTransaction: 7})
	if err != nil {
		t.Fatalf("EncodeEOFCounts: %v", err)
	}

	if _, err := c.DecodeEOFCounts(full[:len(full)-1]); err == nil {
		t.Fatal("expected error decoding truncated payload, got nil")
	}
	if _, err := c.DecodeEOFCounts(full[:2]); err == nil {
		t.Fatal("expected error decoding payload with truncated header, got nil")
	}
}
