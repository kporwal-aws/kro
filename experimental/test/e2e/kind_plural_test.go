package graphcontroller_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// TestStdlibKindIrregularPluralInstanceWatch verifies that a Kind whose CRD
// plural cannot be derived by lowercasing and pluralizing the kind name still
// gets watch events for new instances.
//
// The Kind graph renders the CRD plural as lower(plural("TestKBS")) =
// "testkbses", while pluralizing the lowercased kind gives "testkbs". If the
// controller derives the informer's resource from the kind name instead of
// resolving it through discovery, the informer LIST/WATCH targets a resource
// that does not exist and never delivers events. Reconcile-time reads still
// work (they go through the RESTMapper), so the defect only shows once the
// Kind graph has settled: a new instance is then not stamped until some
// unrelated event reconciles the Kind graph.
func TestStdlibKindIrregularPluralInstanceWatch(t *testing.T) {
	t.Parallel()
	require.NoError(t, waitForCRD(ctx, k8sClient, "kinds.experimental.kro.run", stdlibCRDTimeout))

	const ns = "kro-system"
	kind := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Kind",
		"metadata": map[string]any{
			"name":      "testkbs",
			"namespace": ns,
		},
		"spec": map[string]any{
			"schema": map[string]any{
				"apiVersion": "plural.stdlib.kro.run/v1alpha1",
				"kind":       "TestKBS",
				"spec": map[string]any{
					"message": "string | default=hello",
				},
			},
			"nodes": []any{
				map[string]any{
					"id": "cm",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.metadata.name}-kbs",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{
							"message": "${schema.spec.message}",
						},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, kind))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), kind) })

	require.NoError(t, waitForCRD(ctx, k8sClient, "testkbses.plural.stdlib.kro.run", stdlibCRDTimeout),
		"CRD should use the plural rendered by the Kind graph")

	// Let the Kind graph settle. A graph that applies a CRD template still
	// requeues itself on the controller's exponential backoff, so a new
	// instance can be picked up by one of those retries rather than by its
	// watch event. Idling for a minute pushes the next retry well past the
	// assertion window below, leaving the watch event as the only prompt
	// trigger.
	kindGraphKey := types.NamespacedName{Name: "kind.testkbs", Namespace: ns}
	require.NoError(t, waitForGraphReady(ctx, k8sClient, kindGraphKey, stdlibReconcileTimeout))
	time.Sleep(60 * time.Second)
	require.NoError(t, waitForGraphReady(ctx, k8sClient, kindGraphKey, 5*time.Second))

	inst := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "plural.stdlib.kro.run/v1alpha1",
		"kind":       "TestKBS",
		"metadata": map[string]any{
			"name":      "late",
			"namespace": ns,
		},
		"spec": map[string]any{
			"message": "stamped from a watch event",
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, inst))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), inst) })

	// The only trigger for the Kind graph is the instance watch event.
	instGraph := &unstructured.Unstructured{}
	instGraph.SetGroupVersionKind(GraphGVK)
	require.NoError(t, waitForResource(ctx, k8sClient,
		types.NamespacedName{Name: "kind.testkbs.late", Namespace: ns}, instGraph, 5*time.Second),
		"per-instance Graph should be stamped promptly from the instance watch event")

	cm := &unstructured.Unstructured{}
	cm.SetAPIVersion("v1")
	cm.SetKind("ConfigMap")
	require.NoError(t, waitForResource(ctx, k8sClient,
		types.NamespacedName{Name: "late-kbs", Namespace: ns}, cm, stdlibReconcileTimeout))
}

// TestWatchIrregularPluralCollectionEvents verifies that a watch node over a
// CRD whose plural is unrelated to its kind name (kind Wotsit, plural
// gizmos) receives events. A settled, fully Ready Graph has no periodic
// requeue, so the only way a newly created item reaches the forEach is the
// informer event for the real resource.
func TestWatchIrregularPluralCollectionEvents(t *testing.T) {
	t.Parallel()
	ns := createNamespace(t)

	group := uniqueGroup()
	crdName := "gizmos." + group
	crd := buildCustomCRD(crdName, group, "Wotsit", "gizmos")
	require.NoError(t, k8sClient.Create(ctx, crd))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), crd) })
	require.NoError(t, waitForCRD(ctx, k8sClient, crdName))

	newWotsit := func(name string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": group + "/v1alpha1",
			"kind":       "Wotsit",
			"metadata":   map[string]any{"name": name, "namespace": ns},
		}}
	}
	require.NoError(t, k8sClient.Create(ctx, newWotsit("first")))

	graphKey := types.NamespacedName{Name: "irregular-plural-watch", Namespace: ns}
	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": graphKey.Name, "namespace": ns},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "items",
					"watch": map[string]any{
						"apiVersion": group + "/v1alpha1",
						"kind":       "Wotsit",
						"metadata":   map[string]any{"namespace": ns},
					},
				},
				map[string]any{
					"id":      "copies",
					"forEach": map[string]any{"item": "${items}"},
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "${item.metadata.name}-copy"},
						"data":       map[string]any{"source": "${item.metadata.name}"},
					},
				},
			},
		},
	}}
	require.NoError(t, k8sClient.Create(ctx, graph))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), graph) })

	cmGVK := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(cmGVK)
	require.NoError(t, waitForResource(ctx, k8sClient, types.NamespacedName{Name: "first-copy", Namespace: ns}, cm, 60*time.Second))
	require.NoError(t, waitForGraphReady(ctx, k8sClient, graphKey))
	require.NoError(t, waitForSettle(ctx, k8sClient, GraphGVK, graphKey))

	// The Graph is Ready and idle. Only a watch event can deliver this item.
	require.NoError(t, k8sClient.Create(ctx, newWotsit("second")))
	cm2 := &unstructured.Unstructured{}
	cm2.SetGroupVersionKind(cmGVK)
	require.NoError(t, waitForResource(ctx, k8sClient, types.NamespacedName{Name: "second-copy", Namespace: ns}, cm2, 5*time.Second),
		"forEach output for an item created after settle should appear from the watch event")
}
