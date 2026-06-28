// SPDX-License-Identifier: Apache-2.0

package ingest

// ResolveID builds the graph-read entity id for a datapoint's resource at the
// given level. ok=false when a required attribute is missing or empty → drop.
// Formats are identical to graph-k8s/builder: container:<ns>/<pod>/<name>,
// pod:<ns>/<name>, node:<name>.
func ResolveID(level Level, getAttr func(string) (string, bool)) (string, bool) {
	switch level {
	case LevelContainer:
		ns, ok1 := nonEmpty(getAttr, "k8s.namespace.name")
		pod, ok2 := nonEmpty(getAttr, "k8s.pod.name")
		c, ok3 := nonEmpty(getAttr, "k8s.container.name")
		if !ok1 || !ok2 || !ok3 {
			return "", false
		}
		return "container:" + ns + "/" + pod + "/" + c, true
	case LevelPod:
		ns, ok1 := nonEmpty(getAttr, "k8s.namespace.name")
		pod, ok2 := nonEmpty(getAttr, "k8s.pod.name")
		if !ok1 || !ok2 {
			return "", false
		}
		return "pod:" + ns + "/" + pod, true
	case LevelNode:
		if n, ok := nonEmpty(getAttr, "k8s.node.name"); ok {
			return "node:" + n, true
		}
		if n, ok := nonEmpty(getAttr, "host.name"); ok {
			return "node:" + n, true
		}
		return "", false
	}
	return "", false
}

func nonEmpty(getAttr func(string) (string, bool), key string) (string, bool) {
	v, ok := getAttr(key)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}
