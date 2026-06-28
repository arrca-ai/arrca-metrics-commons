// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// AttrGetter adapts a pcommon.Map to a string getter (ok=false when absent).
func AttrGetter(m pcommon.Map) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m.Get(k)
		if !ok {
			return "", false
		}
		return v.AsString(), true
	}
}

// NumberValue reads an int/double datapoint as float64 (ok=false otherwise).
func NumberValue(dp pmetric.NumberDataPoint) (float64, bool) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue()), true
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue(), true
	}
	return 0, false
}
