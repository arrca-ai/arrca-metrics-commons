// SPDX-License-Identifier: Apache-2.0

// Package redwin holds RED histogram registry, windowing (histtracker), and percentile math.
package redwin

import "strconv"

// HistSpec declares how one OTLP histogram metric is normalized.
type HistSpec struct {
	Source       string  // g:red meta "source": "app" | "krakend"
	UnitToMs     float64 // multiply duration unit to milliseconds (seconds→1000, ms→1)
	RouteAttr    string  // datapoint attr for the route/endpoint
	RouteAltAttr string  // fallback route attr (empty if none)
	MethodAttr   string  // datapoint attr for the HTTP method
	StatusAttr   string  // datapoint attr for the numeric status code
}

// ClassOrder is the canonical status-class order for stable field/iteration order.
var ClassOrder = []string{"2xx", "3xx", "4xx", "5xx"}

// registry maps OTLP histogram name → spec. A name absent here is dropped.
var registry = map[string]HistSpec{
	"http.server.request.duration": {
		Source: "app", UnitToMs: 1000,
		RouteAttr: "http.route", MethodAttr: "http.request.method", StatusAttr: "http.response.status_code",
	},
	"http.server.duration": {
		Source: "krakend", UnitToMs: 1,
		RouteAttr: "http.route", RouteAltAttr: "http.target", MethodAttr: "http.method", StatusAttr: "http.status_code",
	},
}

// Lookup returns the spec for a histogram name; ok=false → drop.
func Lookup(name string) (HistSpec, bool) { s, ok := registry[name]; return s, ok }

// ResolveEndpoint reads route (with optional fallback) and method. ok=false → drop.
func (s HistSpec) ResolveEndpoint(get func(string) (string, bool)) (route, method string, ok bool) {
	route, ok = nonEmpty(get, s.RouteAttr)
	if !ok && s.RouteAltAttr != "" {
		route, ok = nonEmpty(get, s.RouteAltAttr)
	}
	if !ok {
		return "", "", false
	}
	method, ok = nonEmpty(get, s.MethodAttr)
	if !ok {
		return "", "", false
	}
	return route, method, true
}

// ResolveStatus parses the numeric status code. ok=false → drop.
func (s HistSpec) ResolveStatus(get func(string) (string, bool)) (int, bool) {
	v, ok := nonEmpty(get, s.StatusAttr)
	if !ok {
		return 0, false
	}
	code, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return code, true
}

// StatusClass maps a code to "2xx".."5xx". ok=false for codes outside 200–599.
func StatusClass(code int) (string, bool) {
	switch code / 100 {
	case 2:
		return "2xx", true
	case 3:
		return "3xx", true
	case 4:
		return "4xx", true
	case 5:
		return "5xx", true
	}
	return "", false
}

func nonEmpty(get func(string) (string, bool), key string) (string, bool) {
	if key == "" {
		return "", false
	}
	v, ok := get(key)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}
