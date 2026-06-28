// SPDX-License-Identifier: Apache-2.0

package ingest

// Level selects which resource attributes resolve a datapoint's entity id.
type Level int

const (
	LevelContainer Level = iota
	LevelPod
	LevelNode
)
