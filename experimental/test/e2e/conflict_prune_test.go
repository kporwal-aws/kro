package graphcontroller_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestPrune_ConflictedTemplateIsNotDeleted: a template: node that runs into
// a field conflict stays declared by the active revision. Its object must
// not be pruned — a Conflict is a failure to converge, not a removal. Prune
// once deleted (and the next apply recreated) such objects, so any conflict
// on a template: node destroyed live state.
func TestPrune_ConflictedTemplateIsNotDeleted(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)
	graphKey := types.NamespacedName{Name: "conflicted", Namespace: ns}
	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphKey.Name, "namespace": ns},
		"spec": map[string]any{"nodes": []any{
			map[string]any{"id": "cm", "template": map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "owned"},
				"data":     map[string]any{"k": "kro"},
			}},
			map[string]any{"id": "other", "template": map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "other"},
				"data":     map[string]any{"k": "v"},
			}},
		}},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, graphKey))

	key := types.NamespacedName{Namespace: ns, Name: "owned"}
	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	require.NoError(t, k8sClient.Get(ctx, key, cm))
	uid := cm.GetUID()

	// Another manager takes the field with a different value.
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "owned", "namespace": ns},
		"data":     map[string]any{"k": "external"},
	})
	require.NoError(t, err)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(configMapGVK)
	obj.SetName("owned")
	obj.SetNamespace(ns)
	require.NoError(t, k8sClient.Patch(ctx, obj, client.RawPatch(types.ApplyPatchType, raw),
		client.FieldOwner("external-tool"), client.ForceOwnership))

	require.NoError(t, waitForGraphReadyReason(ctx, k8sClient, graphKey, "Conflict"))

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cur := &unstructured.Unstructured{}
		cur.SetGroupVersionKind(configMapGVK)
		require.NoError(t, k8sClient.Get(ctx, key, cur), "conflicted template object must not be deleted")
		require.Equal(t, uid, cur.GetUID(), "conflicted template object was deleted and recreated")
		require.Nil(t, cur.GetDeletionTimestamp(), "conflicted template object was deleted")
		require.Equal(t, "external", cur.Object["data"].(map[string]any)["k"])
		time.Sleep(250 * time.Millisecond)
	}
	g := &unstructured.Unstructured{}
	g.SetGroupVersionKind(GraphGVK)
	require.NoError(t, k8sClient.Get(ctx, graphKey, g))
	require.True(t, strings.Contains(graphReadyMessage(g), "cm"), graphReadyMessage(g))
}
