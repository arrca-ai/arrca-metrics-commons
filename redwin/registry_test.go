// SPDX-License-Identifier: Apache-2.0
package redwin

import "testing"

func getter(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLookupKnownAndUnknown(t *testing.T) {
	if _, ok := Lookup("http.server.request.duration"); !ok {
		t.Fatal("app server histogram must be claimed")
	}
	if _, ok := Lookup("http.server.duration"); !ok {
		t.Fatal("krakend server histogram must be claimed")
	}
	if _, ok := Lookup("jvm.gc.duration"); ok {
		t.Fatal("unrelated histogram must be dropped")
	}
}

func TestResolveEndpointApp(t *testing.T) {
	s, _ := Lookup("http.server.request.duration")
	route, method, ok := s.ResolveEndpoint(getter(map[string]string{
		"http.route": "/users/{id}", "http.request.method": "GET",
	}))
	if !ok || route != "/users/{id}" || method != "GET" {
		t.Fatalf("got route=%q method=%q ok=%v", route, method, ok)
	}
}

func TestResolveEndpointKrakendFallbackTarget(t *testing.T) {
	s, _ := Lookup("http.server.duration")
	route, method, ok := s.ResolveEndpoint(getter(map[string]string{
		"http.target": "/v1/pay", "http.method": "POST",
	}))
	if !ok || route != "/v1/pay" || method != "POST" {
		t.Fatalf("got route=%q method=%q ok=%v", route, method, ok)
	}
}

func TestResolveStatusAndClass(t *testing.T) {
	s, _ := Lookup("http.server.request.duration")
	code, ok := s.ResolveStatus(getter(map[string]string{"http.response.status_code": "503"}))
	if !ok || code != 503 {
		t.Fatalf("code=%d ok=%v", code, ok)
	}
	if c, ok := StatusClass(503); !ok || c != "5xx" {
		t.Fatalf("class=%q ok=%v", c, ok)
	}
	if _, ok := StatusClass(101); ok {
		t.Fatal("1xx must be dropped")
	}
}
