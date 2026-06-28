package ingest

import "testing"

func attrs(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestResolveID_Levels(t *testing.T) {
	cases := []struct {
		name  string
		level Level
		in    map[string]string
		want  string
		ok    bool
	}{
		{"container", LevelContainer,
			map[string]string{"k8s.namespace.name": "shop", "k8s.pod.name": "payment-x2k", "k8s.container.name": "app"},
			"container:shop/payment-x2k/app", true},
		{"pod", LevelPod,
			map[string]string{"k8s.namespace.name": "shop", "k8s.pod.name": "payment-x2k"},
			"pod:shop/payment-x2k", true},
		{"node", LevelNode,
			map[string]string{"k8s.node.name": "node-a"}, "node:node-a", true},
		{"node-fallback-host", LevelNode,
			map[string]string{"host.name": "node-b"}, "node:node-b", true},
		{"container-missing-attr", LevelContainer,
			map[string]string{"k8s.namespace.name": "shop", "k8s.pod.name": "p"}, "", false},
		{"node-missing", LevelNode, map[string]string{}, "", false},
	}
	for _, c := range cases {
		got, ok := ResolveID(c.level, attrs(c.in))
		if got != c.want || ok != c.ok {
			t.Errorf("%s: got (%q,%v), want (%q,%v)", c.name, got, ok, c.want, c.ok)
		}
	}
}
