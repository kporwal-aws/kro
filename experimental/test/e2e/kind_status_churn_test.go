package graphcontroller_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// TestKindStatusNoChurn verifies that a converged Kind is not rewritten at
// steady state.
//
// Two graphs write the same cluster-scoped Kind's status as different field
// managers: kindConditions (root kind Graph) owns status.conditions and
// kindCount (per-Kind Graph) owns status.items. When the status apply body
// carried "metadata.namespace": "" for a cluster-scoped object, the apiserver
// counted each alternating apply as a change: only managedFields time/order
// moved, but every write bumped resourceVersion and woke the other writer,
// looping forever at roughly one write per second.
//
// Adding an instance after convergence forces kindCount to be the last
// writer, which is what kicks the loop off.
func TestKindStatusNoChurn(t *testing.T) {
	t.Parallel()
	require.NoError(t, waitForCRD(ctx, k8sClient, "kinds.experimental.kro.run", stdlibCRDTimeout))

	kind := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Kind",
		"metadata": map[string]any{
			"name": "churnwidget",
		},
		"spec": map[string]any{
			"schema": map[string]any{
				"apiVersion": "test.stdlib.kro.run/v1alpha1",
				"kind":       "ChurnWidget",
				"spec": map[string]any{
					"value": "string | default=x",
				},
			},
			"nodes": []any{
				map[string]any{
					"id": "cm",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.metadata.name}-churn",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{
							"value": "${schema.spec.value}",
						},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, kind))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), kind) })

	require.NoError(t, waitForCRD(ctx, k8sClient, "churnwidgets.test.stdlib.kro.run", stdlibCRDTimeout))

	kindKey := types.NamespacedName{Name: "churnwidget"}
	require.NoError(t, waitForConditionStatus(ctx, t, k8sClient, kindGVK, kindKey, "Ready", "True", stdlibReconcileTimeout))

	newInstance := func(name string) *unstructured.Unstructured {
		inst := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "test.stdlib.kro.run/v1alpha1",
			"kind":       "ChurnWidget",
			"metadata": map[string]any{
				"name":      name,
				"namespace": "default",
			},
			"spec": map[string]any{"value": name},
		}}
		require.NoError(t, k8sClient.Create(ctx, inst))
		t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), inst) })
		return inst
	}

	newInstance("churn-one")
	require.NoError(t, waitForItems(ctx, k8sClient, kindKey, 1, stdlibReconcileTimeout))

	newInstance("churn-two")
	require.NoError(t, waitForItems(ctx, k8sClient, kindKey, 2, stdlibReconcileTimeout))

	// Let both writers settle.
	time.Sleep(3 * time.Second)

	rv := func() string {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(kindGVK)
		require.NoError(t, k8sClient.Get(ctx, kindKey, obj))
		return obj.GetResourceVersion()
	}
	settled := rv()

	// Make each writer re-apply its unchanged status in turn, more than a
	// second apart so the writes cannot coalesce into one managedFields
	// timestamp. Annotating a Graph requeues it without changing its spec.
	graphGVK := schema.GroupVersionKind{Group: "experimental.kro.run", Version: "v1alpha1", Kind: "Graph"}
	nudge := func(name string, i int) {
		require.NoError(t, updateWithRetry(ctx, k8sClient, graphGVK, types.NamespacedName{Name: name, Namespace: "kro-system"}, func(obj *unstructured.Unstructured) {
			ann := obj.GetAnnotations()
			if ann == nil {
				ann = map[string]string{}
			}
			ann["test.kro.run/nudge"] = fmt.Sprint(i)
			obj.SetAnnotations(ann)
		}))
		time.Sleep(1500 * time.Millisecond)
	}
	for i := range 3 {
		nudge("kind", i)             // kindConditions writes status.conditions
		nudge("kind.churnwidget", i) // kindCount writes status.items
	}

	time.Sleep(3 * time.Second)
	require.Equal(t, settled, rv(), "converged Kind should not be rewritten by unchanged status applies")
}
