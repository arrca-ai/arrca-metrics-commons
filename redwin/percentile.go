// SPDX-License-Identifier: Apache-2.0
package redwin

// Percentile computes the q-quantile (0<q<1) from explicit-bucket delta counts.
// counts[i] covers (boundsMs[i-1], boundsMs[i]] (boundsMs[-1]=0); the final
// count is the +Inf overflow bucket. Linear interpolation within the resolved
// bucket; the +Inf bucket yields its lower (last finite) bound. Empty → 0.
func Percentile(counts []uint64, boundsMs []float64, q float64) float64 {
	var total uint64
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := q * float64(total)
	var cum float64
	for i, c := range counts {
		if c == 0 {
			continue
		}
		next := cum + float64(c)
		if rank <= next {
			lb := 0.0
			if i > 0 && i-1 < len(boundsMs) {
				lb = boundsMs[i-1]
			}
			if i >= len(boundsMs) { // +Inf overflow bucket
				return lb
			}
			ub := boundsMs[i]
			return lb + (ub-lb)*((rank-cum)/float64(c))
		}
		cum = next
	}
	if len(boundsMs) > 0 {
		return boundsMs[len(boundsMs)-1]
	}
	return 0
}

// Percentiles returns p50/p90/p99 in one pass-friendly call.
func Percentiles(counts []uint64, boundsMs []float64) (p50, p90, p99 float64) {
	return Percentile(counts, boundsMs, 0.50),
		Percentile(counts, boundsMs, 0.90),
		Percentile(counts, boundsMs, 0.99)
}
