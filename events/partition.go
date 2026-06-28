// SPDX-License-Identifier: Apache-2.0
package events

import "hash/fnv"

// Partition maps an entity id to a NATS partition in [0,n). n<=1 → 0. Hashing by
// ENTITY (not series) makes every signal for one container converge on one
// aggregator instance.
func Partition(entityID string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(entityID))
	return int(h.Sum32() % uint32(n))
}
