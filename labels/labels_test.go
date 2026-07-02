// SPDX-License-Identifier: Apache-2.0
package labels

import "testing"

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
