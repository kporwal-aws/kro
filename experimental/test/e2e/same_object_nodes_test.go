package graphcontroller_test

// Tests for two or more nodes of one Graph writing the same object.
//
// Per 003-ownership.md § Field Manager, every template:/patch: node applies
// under its own SSA field manager, so nodes that share a target object each
// own only their own fields. Under a single per-Graph manager, each node's
// apply would be that manager's complete intent for the object and would
// release the sibling's fields, so the object would be rewritten on every
// reconcile and never settle.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	secretGVK    = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
	configMapGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
)

// steadyStateWindow is how long an object must go without a write to count as
// settled. The flap this guards against rewrites the object about once a
// second, so 12s leaves no room for it to hide.
const steadyStateWindow = 12 * time.Second

// legacyGraphFieldManager is the per-Graph field manager kro used before
// managers became per-node.
func legacyGraphFieldManager(graph, ns string) string {
	return graph + "." + ns + ".internal.kro.run"
}

// nodeFieldManager is the SSA field manager a node applies under.
func nodeFieldManager(node, graph, ns string) string {
	return node + "." + legacyGraphFieldManager(graph, ns)
}

func identityLabelKey(node, graph, ns, suffix string) string {
	return node + "." + graph + "." + ns + ".internal.kro.run/" + suffix
}

// graphManagersOn returns the managedFields manager names on obj that belong
// to the given Graph (per-node, per-node status, and the legacy per-Graph
// manager), mapped to their entry count.
func graphManagersOn(obj *unstructured.Unstructured, graph, ns string) map[string]int {
	legacy := legacyGraphFieldManager(graph, ns)
	out := map[string]int{}
	for _, mf := range obj.GetManagedFields() {
		m := strings.TrimSuffix(mf.Manager, ".status")
		if m == legacy || strings.HasSuffix(m, "."+legacy) {
			out[mf.Manager]++
		}
	}
	return out
}

// requireQuiet waits for obj to settle, then asserts its resourceVersion does
// not change over the steady-state window, and returns the object as last
// read. Settling allows for a write or two while the Graph converges (for
// example, while a stale field manager is released); a flapping object never
// settles.
func requireQuiet(t *testing.T, gvk schema.GroupVersionKind, key types.NamespacedName) *unstructured.Unstructured {
	t.Helper()
	get := func() *unstructured.Unstructured {
		cur := &unstructured.Unstructured{}
		cur.SetGroupVersionKind(gvk)
		require.NoError(t, k8sClient.Get(ctx, key, cur))
		return cur
	}

	// Settle: 3s without a write, within 20s.
	last := get()
	stableSince := time.Now()
	settleDeadline := time.Now().Add(20 * time.Second)
	for time.Since(stableSince) < 3*time.Second {
		if time.Now().After(settleDeadline) {
			t.Fatalf("%s %s never settled: still rewritten at resourceVersion %s", gvk.Kind, key, last.GetResourceVersion())
		}
		time.Sleep(250 * time.Millisecond)
		cur := get()
		if cur.GetResourceVersion() != last.GetResourceVersion() {
			stableSince = time.Now()
		}
		last = cur
	}

	// Steady state: no write at all over the window.
	rv := last.GetResourceVersion()
	deadline := time.Now().Add(steadyStateWindow)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		cur := get()
		if cur.GetResourceVersion() != rv {
			before, _ := json.Marshal(last.GetManagedFields())
			after, _ := json.Marshal(cur.GetManagedFields())
			t.Fatalf("%s %s rewritten during steady state: resourceVersion %s -> %s\nmanagedFields before: %s\nmanagedFields after: %s",
				gvk.Kind, key, rv, cur.GetResourceVersion(), before, after)
		}
		last = cur
	}
	return last
}

// TestSameGraph_TemplateAndPatchSameObject_SteadyState is the minimal shape:
// a template: creates a Secret and a patch: in the same Graph adds a field to
// it. Both nodes' fields and identity labels must coexist and the Secret must
// stop being rewritten once the Graph is Ready.
func TestSameGraph_TemplateAndPatchSameObject_SteadyState(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "seed-fill"

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "seed",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]any{
							"name":   "example",
							"labels": map[string]any{"app": "example"},
						},
						"type": "Opaque",
					},
				},
				map[string]any{
					"id": "fill",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "${seed.metadata.name}"},
						"stringData": map[string]any{"key": `${"value"}`},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, types.NamespacedName{Name: graphName, Namespace: ns}))

	key := types.NamespacedName{Name: "example", Namespace: ns}
	secret := requireQuiet(t, secretGVK, key)

	lbls := secret.GetLabels()
	assert.Equal(t, "example", lbls["app"], "template label must persist")
	assert.Equal(t, "template", lbls[identityLabelKey("seed", graphName, ns, "type")])
	assert.Equal(t, "patch", lbls[identityLabelKey("fill", graphName, ns, "type")])
	assert.NotEmpty(t, lbls[identityLabelKey("seed", graphName, ns, "generation")])
	assert.NotEmpty(t, lbls[identityLabelKey("fill", graphName, ns, "generation")])
	assert.Equal(t, graphName, lbls["kro.run/graph-name"])
	data, _, _ := unstructured.NestedStringMap(secret.Object, "data")
	assert.NotEmpty(t, data["key"], "patch field must persist alongside the template's fields")
	assert.Equal(t, "Opaque", secret.Object["type"])

	managers := graphManagersOn(secret, graphName, ns)
	assert.Contains(t, managers, nodeFieldManager("seed", graphName, ns))
	assert.Contains(t, managers, nodeFieldManager("fill", graphName, ns))
	assert.NotContains(t, managers, legacyGraphFieldManager(graphName, ns))
}

// TestSameGraph_TwoPatchesSameObject_SteadyState covers two patch: nodes of
// one Graph layering onto an object the Graph does not own.
func TestSameGraph_TwoPatchesSameObject_SteadyState(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "two-patches"

	target := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "shared", "namespace": ns},
		"data":       map[string]any{"owner": "external"},
	}}
	require.NoError(t, k8sClient.Create(ctx, target))

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "first",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name":   "shared",
							"labels": map[string]any{"first": "yes"},
						},
						"data": map[string]any{"first": "1"},
					},
				},
				map[string]any{
					"id": "second",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name":        "shared",
							"annotations": map[string]any{"second": "yes"},
						},
						"data": map[string]any{"second": "2"},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, types.NamespacedName{Name: graphName, Namespace: ns}))

	cm := requireQuiet(t, configMapGVK, types.NamespacedName{Name: "shared", Namespace: ns})
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	assert.Equal(t, map[string]string{"owner": "external", "first": "1", "second": "2"}, data)
	assert.Equal(t, "yes", cm.GetLabels()["first"])
	assert.Equal(t, "yes", cm.GetAnnotations()["second"])
	assert.Equal(t, "patch", cm.GetLabels()[identityLabelKey("first", graphName, ns, "type")])
	assert.Equal(t, "patch", cm.GetLabels()[identityLabelKey("second", graphName, ns, "type")])

	managers := graphManagersOn(cm, graphName, ns)
	assert.Contains(t, managers, nodeFieldManager("first", graphName, ns))
	assert.Contains(t, managers, nodeFieldManager("second", graphName, ns))
}

// TestSameGraph_SharedObjectTeardown deletes a Graph whose nodes share
// objects: the templated object is deleted, the patched object survives with
// every node's fields released, and no sibling manager blocks the teardown.
func TestSameGraph_SharedObjectTeardown(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "shared-teardown"

	external := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "external", "namespace": ns},
		"data":       map[string]any{"owner": "external"},
	}}
	require.NoError(t, k8sClient.Create(ctx, external))

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "seed",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "owned"},
						"data":       map[string]any{"seed": "1"},
					},
				},
				map[string]any{
					"id": "fill",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "${seed.metadata.name}"},
						"data":       map[string]any{"fill": "1"},
					},
				},
				map[string]any{
					"id": "pa",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "external"},
						"data":       map[string]any{"a": "1"},
					},
				},
				map[string]any{
					"id": "pb",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "external"},
						"data":       map[string]any{"b": "1"},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	graphKey := types.NamespacedName{Name: graphName, Namespace: ns}
	require.NoError(t, waitForGraphReady(ctx, k8sClient, graphKey))

	require.NoError(t, k8sClient.Delete(ctx, graph))
	require.NoError(t, waitForDeletion(ctx, k8sClient, GraphGVK, graphKey))

	require.NoError(t, waitForDeletion(ctx, k8sClient, configMapGVK, types.NamespacedName{Name: "owned", Namespace: ns}),
		"templated object must be deleted on teardown")

	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "external", Namespace: ns}, cm),
		"patched object must survive teardown")
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	assert.Equal(t, map[string]string{"owner": "external"}, data, "both patch nodes' fields must be released")
	assert.Empty(t, graphManagersOn(cm, graphName, ns), "no Graph field manager may remain after teardown")
	for k := range cm.GetLabels() {
		assert.NotContains(t, k, "."+graphName+"."+ns+".internal.kro.run/", "identity label %s must be released", k)
	}
}

// TestSameGraph_LegacyGraphManagerMigration starts from an object last
// written under the legacy per-Graph field manager, the way a controller
// that predates per-node managers leaves it. The legacy entry holds one
// node's fields (including a value the node now declares differently) plus a
// field no node declares. After migration the legacy entry is gone, the
// undeclared field is released, and the object settles.
func TestSameGraph_LegacyGraphManagerMigration(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "legacy-migrate"
	legacy := legacyGraphFieldManager(graphName, ns)

	// Pre-state: the legacy manager owns what "fill" last applied (with an
	// old value), fill's identity labels, and a stale field.
	pre := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "owned",
			"namespace": ns,
			"labels": map[string]any{
				identityLabelKey("fill", graphName, ns, "type"):       "patch",
				identityLabelKey("fill", graphName, ns, "generation"): "1",
			},
		},
		"data": map[string]any{"fill": "old", "stale": "x"},
	}
	raw, err := json.Marshal(pre)
	require.NoError(t, err)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(configMapGVK)
	obj.SetName("owned")
	obj.SetNamespace(ns)
	require.NoError(t, k8sClient.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, raw), client.FieldOwner(legacy)))

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "seed",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "owned"},
						"data":       map[string]any{"seed": "1"},
					},
				},
				map[string]any{
					"id": "fill",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "${seed.metadata.name}"},
						"data":       map[string]any{"fill": "new"},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, types.NamespacedName{Name: graphName, Namespace: ns}))

	key := types.NamespacedName{Name: "owned", Namespace: ns}
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		cur := &unstructured.Unstructured{}
		cur.SetGroupVersionKind(configMapGVK)
		if err := k8sClient.Get(ctx, key, cur); err != nil {
			return false, nil
		}
		_, has := graphManagersOn(cur, graphName, ns)[legacy]
		return !has, nil
	})
	require.NoError(t, err, "legacy per-Graph field manager must be released")

	cm := requireQuiet(t, configMapGVK, key)
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	assert.Equal(t, map[string]string{"seed": "1", "fill": "new"}, data,
		"declared fields must persist with current values; the undeclared field must be released")
	assert.Equal(t, "patch", cm.GetLabels()[identityLabelKey("fill", graphName, ns, "type")])
	assert.Equal(t, "template", cm.GetLabels()[identityLabelKey("seed", graphName, ns, "type")])
	managers := graphManagersOn(cm, graphName, ns)
	assert.NotContains(t, managers, legacy)
	assert.Contains(t, managers, nodeFieldManager("seed", graphName, ns))
	assert.Contains(t, managers, nodeFieldManager("fill", graphName, ns))
}

// TestSameGraph_NodeRenameReleasesOldManager renames a patch node that shares
// its object with a live template node. The object stays in the applied set,
// so prune never sees it; the old node's manager must still be released so
// its fields and identity labels do not linger.
func TestSameGraph_NodeRenameReleasesOldManager(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "rename"
	graphKey := types.NamespacedName{Name: graphName, Namespace: ns}

	nodes := func(patchID, field string) []any {
		return []any{
			map[string]any{
				"id": "seed",
				"template": map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "owned"},
					"data":       map[string]any{"seed": "1"},
				},
			},
			map[string]any{
				"id": patchID,
				"patch": map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "${seed.metadata.name}"},
					"data":       map[string]any{field: "1"},
				},
			},
		}
	}
	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec":       map[string]any{"nodes": nodes("before", "b")},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, graphKey))

	require.NoError(t, updateWithRetry(ctx, k8sClient, GraphGVK, graphKey, func(g *unstructured.Unstructured) {
		_ = unstructured.SetNestedSlice(g.Object, nodes("after", "a"), "spec", "nodes")
	}))
	require.NoError(t, waitForObservedGeneration(ctx, k8sClient, graphKey, 2))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, graphKey))

	key := types.NamespacedName{Name: "owned", Namespace: ns}
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		cur := &unstructured.Unstructured{}
		cur.SetGroupVersionKind(configMapGVK)
		if err := k8sClient.Get(ctx, key, cur); err != nil {
			return false, nil
		}
		_, has := graphManagersOn(cur, graphName, ns)[nodeFieldManager("before", graphName, ns)]
		return !has, nil
	})
	require.NoError(t, err, "renamed node's field manager must be released")

	cm := requireQuiet(t, configMapGVK, key)
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	assert.Equal(t, map[string]string{"seed": "1", "a": "1"}, data)
	for k := range cm.GetLabels() {
		assert.False(t, strings.HasPrefix(k, "before."), "old node's identity label %s must be released", k)
	}
	assert.Equal(t, "patch", cm.GetLabels()[identityLabelKey("after", graphName, ns, "type")])
}

// TestSameGraph_ExternalCoOwnerStillConflicts confirms sibling managers are
// "ours" without hiding a genuinely external Apply manager: an external
// manager co-owning a field a patch node declares (same value) is still a
// Conflict, even while a sibling node of the same Graph writes the object.
func TestSameGraph_ExternalCoOwnerStillConflicts(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "external-coowner"

	// External Apply manager owns data.shared on a ConfigMap.
	ext := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "target", "namespace": ns},
		"data":       map[string]any{"shared": "same"},
	}
	raw, err := json.Marshal(ext)
	require.NoError(t, err)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(configMapGVK)
	obj.SetName("target")
	obj.SetNamespace(ns)
	require.NoError(t, k8sClient.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, raw), client.FieldOwner("external-tool")))

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "benign",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "target"},
						"data":       map[string]any{"mine": "1"},
					},
				},
				map[string]any{
					"id": "contested",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "target"},
						"data":       map[string]any{"shared": "same"},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))

	graphKey := types.NamespacedName{Name: graphName, Namespace: ns}
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		g := &unstructured.Unstructured{}
		g.SetGroupVersionKind(GraphGVK)
		if err := k8sClient.Get(ctx, graphKey, g); err != nil {
			return false, nil
		}
		msg := graphReadyMessage(g)
		return strings.Contains(msg, "contested") && strings.Contains(strings.ToLower(msg), "conflict"), nil
	})
	require.NoError(t, err, "external co-owner must surface as a Conflict on the contested node")

	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "target", Namespace: ns}, cm))
	managers := graphManagersOn(cm, graphName, ns)
	assert.Contains(t, managers, nodeFieldManager("benign", graphName, ns), "the uncontested sibling must still apply")
	assert.NotContains(t, managers, nodeFieldManager("contested", graphName, ns), "the contested node must not apply")
	assertManagerEntry(t, cm, "external-tool", true)
}

// assertManagerEntry asserts whether an Apply manager has an entry on obj.
func assertManagerEntry(t *testing.T, obj *unstructured.Unstructured, manager string, want bool) {
	t.Helper()
	found := false
	for _, mf := range obj.GetManagedFields() {
		if mf.Manager == manager && mf.Operation == metav1.ManagedFieldsOperationApply {
			found = true
		}
	}
	assert.Equal(t, want, found, fmt.Sprintf("manager %q present", manager))
}
