// SPDX-License-Identifier: Apache-2.0
package events

import "testing"

func TestKeys(t *testing.T) {
	if got := IDsKey(); got != "g:anom:ids" {
		t.Fatalf("IDsKey()=%q", got)
	}
	id := "container:default/api-1/app"
	if got := StreamKey(id); got != "g:anom:s:"+id {
		t.Fatalf("StreamKey()=%q", got)
	}
}
