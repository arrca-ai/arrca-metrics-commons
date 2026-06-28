// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"fmt"
	"strconv"
	"strings"
)

// OwnedSubjects returns the partition subjects this shard owns. shardCount<=1
// returns the single wildcard "<prefix>.*" (one ordered subscription owns all
// partitions). Otherwise it returns the contiguous range
// [shardIndex*partitions/shardCount, (shardIndex+1)*partitions/shardCount).
func OwnedSubjects(prefix string, partitions, shardIndex, shardCount int) ([]string, error) {
	if partitions <= 0 {
		return nil, fmt.Errorf("partitions must be > 0, got %d", partitions)
	}
	if shardCount <= 1 {
		if shardIndex != 0 {
			return nil, fmt.Errorf("shardIndex %d out of range [0,1)", shardIndex)
		}
		return []string{prefix + ".*"}, nil
	}
	if shardIndex < 0 || shardIndex >= shardCount {
		return nil, fmt.Errorf("shardIndex %d out of range [0,%d)", shardIndex, shardCount)
	}
	start := shardIndex * partitions / shardCount
	end := (shardIndex + 1) * partitions / shardCount
	subjects := make([]string, 0, end-start)
	for p := start; p < end; p++ {
		subjects = append(subjects, fmt.Sprintf("%s.%d", prefix, p))
	}
	return subjects, nil
}

// ShardIndexFromPodName parses the trailing StatefulSet ordinal
// ("graph-metrics-3" → 3).
func ShardIndexFromPodName(podName string) (int, error) {
	i := strings.LastIndex(podName, "-")
	if i < 0 || i == len(podName)-1 {
		return 0, fmt.Errorf("no ordinal suffix in pod name %q", podName)
	}
	ordinal := podName[i+1:]
	idx, err := strconv.Atoi(ordinal)
	if err != nil {
		return 0, fmt.Errorf("pod name %q: ordinal suffix %q is not numeric: %w", podName, ordinal, err)
	}
	return idx, nil
}
