// SPDX-License-Identifier: Apache-2.0
package labels

import (
	"reflect"
	"testing"
)

func TestEncodeKey(t *testing.T) {
	if got := EncodeKey("jvm_nonheap_limit", nil); got != "jvm_nonheap_limit" {
		t.Fatalf("nil labels must be base, got %q", got)
	}
	if got := EncodeKey("k", Labels{}); got != "k" {
		t.Fatalf("empty labels must be base, got %q", got)
	}
	if got := EncodeKey("lag", Labels{"topic": "orders", "partition": "3"}); got != "lag{partition=3,topic=orders}" {
		t.Fatalf("labels must be sorted by name: %q", got)
	}
}

func TestDecodeKey(t *testing.T) {
	base, l := DecodeKey("jvm_nonheap_limit")
	if base != "jvm_nonheap_limit" || len(l) != 0 {
		t.Fatalf("no-label decode wrong: %q %v", base, l)
	}
	base, l = DecodeKey("lag{partition=3,topic=orders}")
	if base != "lag" || !reflect.DeepEqual(l, Labels{"topic": "orders", "partition": "3"}) {
		t.Fatalf("decode wrong: %q %v", base, l)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Labels{"pool": "Compressed Class Space"}
	base, out := DecodeKey(EncodeKey("jvm_nonheap_limit", in))
	if base != "jvm_nonheap_limit" || !reflect.DeepEqual(out, in) {
		t.Fatalf("round-trip wrong: %q %v", base, out)
	}
}
