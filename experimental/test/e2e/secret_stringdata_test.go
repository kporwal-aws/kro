package graphcontroller_test

// Tests for Secret stringData written by template:/patch: nodes.
//
// stringData is write-only: the API server merges it into data and never
// stores it. An apply body carrying stringData therefore always differs from
// the stored object, so every apply re-stamps the field manager's time and
// persists a new resourceVersion. kro folds stringData into data before
// applying (stringData wins on key collisions), so a repeat apply is a true
// no-op and nodes sharing a Secret reach a quiet steady state.
//
// Whether a stringData apply persists depends on timing: the API server
// skips the write when the re-stamped managedFields time falls in the same
// second as the stored one. Under load, reconciles land in later seconds and
// two writers keep waking each other up. The tests make that deterministic by
// re-triggering the Graph more than a second apart through a ref: node on a
// ConfigMap that the test keeps updating.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// triggerInterval spaces re-triggered reconciles more than a second apart.
const triggerInterval = 1300 * time.Millisecond

// triggerNode is a ref: node on the trigger ConfigMap. No other node uses it;
// it only makes each update of the ConfigMap re-reconcile the Graph.
func triggerNode() map[string]any {
	return map[string]any{
		"id": "trigger",
		"ref": map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "trigger"},
		},
	}
}

// startTrigger creates the trigger ConfigMap and updates it every
// triggerInterval until the test ends.
func startTrigger(t *testing.T, ns string) {
	t.Helper()
	cm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "trigger", "namespace": ns},
		"data":       map[string]any{"tick": "0"},
	}}
	require.NoError(t, k8sClient.Create(ctx, cm))
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(triggerInterval)
		defer ticker.Stop()
		for i := 1; ; i++ {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			patch := []byte(`{"data":{"tick":"` + strconv.Itoa(i) + `"}}`)
			obj := &unstructured.Unstructured{}
			obj.SetGroupVersionKind(configMapGVK)
			obj.SetName("trigger")
			obj.SetNamespace(ns)
			_ = k8sClient.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
		}
	}()
	t.Cleanup(func() { close(done); <-stopped })
}

// assertNoStringDataClaim asserts that no field manager of the Graph claims
// stringData on obj.
func assertNoStringDataClaim(t *testing.T, obj *unstructured.Unstructured, graph, ns string) {
	t.Helper()
	for _, mf := range obj.GetManagedFields() {
		if _, ok := graphManagersOn(obj, graph, ns)[mf.Manager]; !ok || mf.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		require.NoError(t, json.Unmarshal(mf.FieldsV1.Raw, &fields))
		assert.NotContains(t, fields, "f:stringData", "manager %s must not claim stringData", mf.Manager)
	}
}

// TestSameGraph_TwoStringDataPatchesSameSecret_SteadyState is the shape that
// never settled: two patch: nodes of one Graph each write a stringData key on
// the same Secret, waking each other up on every write.
func TestSameGraph_TwoStringDataPatchesSameSecret_SteadyState(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "stringdata-two-patches"
	startTrigger(t, ns)

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				triggerNode(),
				map[string]any{
					"id": "seed",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "example"},
						"type":       "Opaque",
					},
				},
				map[string]any{
					"id": "keyPatch",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "${seed.metadata.name}"},
						"stringData": map[string]any{"tls.key": `${"key-material"}`},
					},
				},
				map[string]any{
					"id": "caPatch",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "${seed.metadata.name}"},
						"stringData": map[string]any{"ca.crt": `${"ca-material"}`},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, types.NamespacedName{Name: graphName, Namespace: ns}))

	secret := requireQuiet(t, secretGVK, types.NamespacedName{Name: "example", Namespace: ns})
	data, _, _ := unstructured.NestedStringMap(secret.Object, "data")
	assert.Equal(t, map[string]string{"tls.key": b64("key-material"), "ca.crt": b64("ca-material")}, data)
	assertNoStringDataClaim(t, secret, graphName, ns)
	managers := graphManagersOn(secret, graphName, ns)
	assert.Contains(t, managers, nodeFieldManager("keyPatch", graphName, ns))
	assert.Contains(t, managers, nodeFieldManager("caPatch", graphName, ns))
}

// TestSecretStringData_SingleWriter_SteadyState covers one patch: node writing
// stringData onto an external Secret: it writes, then stays quiet. stringData
// overrides a data key the node also declares, matching the API server.
func TestSecretStringData_SingleWriter_SteadyState(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "stringdata-single"
	startTrigger(t, ns)

	target := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": "target", "namespace": ns},
		"data":       map[string]any{"owner": b64("external")},
	}}
	require.NoError(t, k8sClient.Create(ctx, target))

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				triggerNode(),
				map[string]any{
					"id": "fill",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "target"},
						"data":       map[string]any{"both": b64("from-data"), "raw": b64("raw")},
						"stringData": map[string]any{"both": "from-stringData", "key": `${"value"}`},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, types.NamespacedName{Name: graphName, Namespace: ns}))

	secret := requireQuiet(t, secretGVK, types.NamespacedName{Name: "target", Namespace: ns})
	data, _, _ := unstructured.NestedStringMap(secret.Object, "data")
	assert.Equal(t, map[string]string{
		"owner": b64("external"),
		"both":  b64("from-stringData"),
		"raw":   b64("raw"),
		"key":   b64("value"),
	}, data)
	assertNoStringDataClaim(t, secret, graphName, ns)
}

// TestSecretStringData_MigrationFromStringDataClaim starts from a Secret whose
// Graph managers claim stringData, the way a controller that applied
// stringData as written leaves it: the legacy per-Graph manager holds one
// node's key and the node's own manager holds another. After migration no
// Graph manager claims stringData, the data values are current, the Secret
// settles, and teardown releases every key the Graph wrote while leaving the
// external key in place.
func TestSecretStringData_MigrationFromStringDataClaim(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphName := "stringdata-migrate"
	graphKey := types.NamespacedName{Name: graphName, Namespace: ns}
	key := types.NamespacedName{Name: "target", Namespace: ns}
	startTrigger(t, ns)

	target := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": "target", "namespace": ns},
		"data":       map[string]any{"owner": b64("external")},
	}}
	require.NoError(t, k8sClient.Create(ctx, target))

	preApply := func(manager string, body map[string]any) {
		t.Helper()
		body["apiVersion"] = "v1"
		body["kind"] = "Secret"
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(secretGVK)
		obj.SetName("target")
		obj.SetNamespace(ns)
		require.NoError(t, k8sClient.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, raw), client.FieldOwner(manager)))
	}
	// Legacy per-Graph manager: node "pa" with an old value.
	preApply(legacyGraphFieldManager(graphName, ns), map[string]any{
		"metadata": map[string]any{
			"name": "target", "namespace": ns,
			"labels": map[string]any{
				identityLabelKey("pa", graphName, ns, "type"):       "patch",
				identityLabelKey("pa", graphName, ns, "generation"): "1",
			},
		},
		"stringData": map[string]any{"a": "old-a"},
	})
	// Per-node manager of node "pb", with its current value.
	preApply(nodeFieldManager("pb", graphName, ns), map[string]any{
		"metadata": map[string]any{
			"name": "target", "namespace": ns,
			"labels": map[string]any{
				identityLabelKey("pb", graphName, ns, "type"):       "patch",
				identityLabelKey("pb", graphName, ns, "generation"): "1",
			},
		},
		"stringData": map[string]any{"b": "b"},
	})

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphName, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				triggerNode(),
				map[string]any{
					"id": "pa",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "target"},
						"stringData": map[string]any{"a": "new-a"},
					},
				},
				map[string]any{
					"id": "pb",
					"patch": map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata":   map[string]any{"name": "target"},
						"stringData": map[string]any{"b": "b"},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, graphKey))

	legacy := legacyGraphFieldManager(graphName, ns)
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		cur := &unstructured.Unstructured{}
		cur.SetGroupVersionKind(secretGVK)
		if err := k8sClient.Get(ctx, key, cur); err != nil {
			return false, nil
		}
		_, has := graphManagersOn(cur, graphName, ns)[legacy]
		return !has, nil
	})
	require.NoError(t, err, "legacy per-Graph field manager must be released")

	secret := requireQuiet(t, secretGVK, key)
	data, _, _ := unstructured.NestedStringMap(secret.Object, "data")
	assert.Equal(t, map[string]string{"owner": b64("external"), "a": b64("new-a"), "b": b64("b")}, data,
		"releasing the stale stringData claims must not strip keys from data")
	assertNoStringDataClaim(t, secret, graphName, ns)
	managers := graphManagersOn(secret, graphName, ns)
	assert.NotContains(t, managers, legacy)
	assert.Contains(t, managers, nodeFieldManager("pa", graphName, ns))
	assert.Contains(t, managers, nodeFieldManager("pb", graphName, ns))

	require.NoError(t, k8sClient.Delete(ctx, graph))
	require.NoError(t, waitForDeletion(ctx, k8sClient, GraphGVK, graphKey))

	after := &unstructured.Unstructured{}
	after.SetGroupVersionKind(secretGVK)
	require.NoError(t, k8sClient.Get(ctx, key, after), "patched Secret must survive teardown")
	data, _, _ = unstructured.NestedStringMap(after.Object, "data")
	assert.Equal(t, map[string]string{"owner": b64("external")}, data,
		"teardown must release the keys the Graph wrote and keep the external key")
	assert.Empty(t, graphManagersOn(after, graphName, ns), "no Graph field manager may remain after teardown")
}
