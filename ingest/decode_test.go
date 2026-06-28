// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestNumberValueIntAndDouble(t *testing.T) {
	g := pmetric.NewGauge().DataPoints().AppendEmpty()
	g.SetIntValue(7)
	if v, ok := NumberValue(g); !ok || v != 7 {
		t.Fatalf("int dp = %v,%v want 7,true", v, ok)
	}
	d := pmetric.NewGauge().DataPoints().AppendEmpty()
	d.SetDoubleValue(1.5)
	if v, ok := NumberValue(d); !ok || v != 1.5 {
		t.Fatalf("double dp = %v,%v want 1.5,true", v, ok)
	}
}

func TestAttrGetter(t *testing.T) {
	g := pmetric.NewGauge().DataPoints().AppendEmpty()
	g.Attributes().PutStr("k", "v")
	get := AttrGetter(g.Attributes())
	if v, ok := get("k"); !ok || v != "v" {
		t.Fatalf("get(k) = %v,%v want v,true", v, ok)
	}
	if _, ok := get("missing"); ok {
		t.Fatal("missing attr must be ok=false")
	}
}
