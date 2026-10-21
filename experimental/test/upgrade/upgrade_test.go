// Package upgrade tests running a new controller against cluster state that a
// previous controller release wrote.
//
// The previous controller is built from a git ref of this repository
// (KRO_UPGRADE_FROM_REF, default: the last commit that applied every node of
// a Graph under one per-Graph field manager). It converges a set of Graphs,
// is stopped, and the controller built from this tree takes over the same
// API server. Nothing the Graphs declare may be deleted along the way, and
// every Graph must converge again.
package upgrade_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kporwal-aws/kro/experimental/stdlib"
	"github.com/kporwal-aws/kro/experimental/test/testenv"
)

// defaultFromRef is the last commit whose controller applied every node of a
// Graph under the per-Graph field manager <graph>.<namespace>.internal.kro.run.
const defaultFromRef = "11a3f3a0"

var (
	graphGVK     = schema.GroupVersionKind{Group: "experimental.kro.run", Version: "v1alpha1", Kind: "Graph"}
	kindGVK      = schema.GroupVersionKind{Group: "experimental.kro.run", Version: "v1alpha1", Kind: "Kind"}
	widgetGVK    = schema.GroupVersionKind{Group: "upgrade.kro.run", Version: "v1alpha1", Kind: "Widget"}
	gizmoGVK     = schema.GroupVersionKind{Group: "upgrade.kro.run", Version: "v1alpha1", Kind: "Gizmo"}
	configMapGVK = schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
)

const (
	ns               = "upgrade"
	externalManager  = "external-controller"
	externalFinalize = "external.upgrade.kro.run/finalizer"
)

func graph(name string, nodes ...any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       map[string]any{"nodes": nodes},
	}}
}

func node(id, keyword string, body map[string]any, extra ...map[string]any) map[string]any {
	n := map[string]any{"id": id, keyword: body}
	for _, e := range extra {
		for k, v := range e {
			n[k] = v
		}
	}
	return n
}

// watched is an object the Graphs declare, identified for deletion checks.
type watched struct {
	gvk  schema.GroupVersionKind
	key  types.NamespacedName
	uid  types.UID
	desc string
}

func TestUpgradeFromPerGraphFieldManager(t *testing.T) {
	ref := os.Getenv("KRO_UPGRADE_FROM_REF")
	if ref == "" {
		ref = defaultFromRef
	}
	if err := exec.Command("git", "-C", testenv.FindModuleRoot(), "cat-file", "-e", ref+"^{commit}").Run(); err != nil {
		t.Skipf("previous controller ref %q not available in this checkout: %v", ref, err)
	}

	logf.SetLogger(zap.New(zap.UseDevMode(true)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	oldBinary, err := testenv.BuildBinaryAtRef(ref, t.TempDir())
	require.NoError(t, err, "building the previous controller")

	env := &testenv.Environment{BinaryPath: oldBinary}
	cfg, err := env.Start()
	require.NoError(t, err)
	defer env.Stop()
	c, err := env.Client()
	require.NoError(t, err)
	require.NoError(t, apiextensionsv1.AddToScheme(c.Scheme()))

	require.NoError(t, stdlib.Apply(ctx, logf.Log.WithName("stdlib"), cfg))
	require.NoError(t, c.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns},
	}}))
	require.NoError(t, c.Create(ctx, widgetCRD()))
	waitForCRD(ctx, t, c, "widgets.upgrade.kro.run")
	waitForCRD(ctx, t, c, "kinds.experimental.kro.run")

	// (i) A cluster-scoped object one Graph templates and other Graphs patch:
	// a Kind declared by a Graph, whose status the stdlib Kind Graphs write.
	// An instance of the Kind adds a per-instance Graph that patches it.
	require.NoError(t, c.Create(ctx, graph("install.gizmo",
		node("gizmokind", "template", map[string]any{
			"apiVersion": "experimental.kro.run/v1alpha1",
			"kind":       "Kind",
			"metadata":   map[string]any{"name": "gizmo"},
			"spec": map[string]any{
				"schema": map[string]any{
					"apiVersion": "upgrade.kro.run/v1alpha1",
					"kind":       "Gizmo",
					"spec":       map[string]any{"size": "string | default=small"},
				},
				"nodes": []any{map[string]any{
					"id": "cm",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name":      "${${schema.metadata.name}}-gizmo",
							"namespace": "${${schema.metadata.namespace}}",
						},
						"data": map[string]any{"size": "${${schema.spec.size}}"},
					},
				}},
			},
		}, map[string]any{"readyWhen": []any{
			"${has(gizmokind.status) && has(gizmokind.status.conditions) && gizmokind.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')}",
		}}),
	)))

	// (ii) Templated objects an external controller also writes with Update
	// operations (spec defaults, status) and holds with its own finalizer,
	// one applied without force and one with lifecycle.apply: Force.
	widget := func(name string) map[string]any {
		return map[string]any{
			"apiVersion": "upgrade.kro.run/v1alpha1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"document": `{"a": 1}`, "tags": []any{map[string]any{"key": "k", "value": "v"}}},
		}
	}
	require.NoError(t, c.Create(ctx, graph("install.widgets",
		node("plain", "template", widget("widget-plain")),
		node("forced", "template", widget("widget-forced"), map[string]any{"lifecycle": map[string]any{"apply": "Force"}}),
	)))

	// (iii) A template: and a patch: in one Graph writing the same object.
	require.NoError(t, c.Create(ctx, graph("pair",
		node("seed", "template", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "pair", "labels": map[string]any{"app": "pair"}},
			"data":     map[string]any{"seed": "1"},
		}),
		node("fill", "patch", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${seed.metadata.name}"},
			"data":     map[string]any{"fill": "1"},
		}),
	)))

	// (iv) An object one Graph templates and another Graph patches.
	require.NoError(t, c.Create(ctx, graph("owner",
		node("shared", "template", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "shared"},
			"data":     map[string]any{"owner": "1"},
		}),
	)))
	require.NoError(t, c.Create(ctx, graph("contributor",
		node("contrib", "patch", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "shared", "annotations": map[string]any{"contributor": "yes"}},
			"data":     map[string]any{"contrib": "1"},
		}),
	)))

	// Converge on the previous controller.
	waitForCRD(ctx, t, c, "gizmoes.upgrade.kro.run")
	require.NoError(t, c.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "upgrade.kro.run/v1alpha1", "kind": "Gizmo",
		"metadata": map[string]any{"name": "g1", "namespace": ns},
	}}))
	graphs := []types.NamespacedName{
		{Namespace: ns, Name: "install.gizmo"},
		{Namespace: ns, Name: "install.widgets"},
		{Namespace: ns, Name: "pair"},
		{Namespace: ns, Name: "owner"},
		{Namespace: ns, Name: "contributor"},
		{Namespace: "kro-system", Name: "kind.gizmo"},
		{Namespace: ns, Name: "kind.gizmo.g1"},
	}
	// Widgets become Ready only once the external controller has written them.
	for _, name := range []string{"widget-plain", "widget-forced"} {
		externalControllerWrite(ctx, t, c, name)
	}
	for _, g := range graphs {
		if g.Name == "kind.gizmo" {
			// Older controllers wrote cluster-scoped status applies in a way
			// that left the Kind Graph's status patch in Conflict; only
			// require it to exist before the upgrade.
			continue
		}
		waitForGraphReady(ctx, t, c, g, 90*time.Second)
	}

	objs := []watched{
		{gvk: kindGVK, key: types.NamespacedName{Name: "gizmo"}, desc: "Kind templated by install.gizmo, patched by the Kind Graphs"},
		{gvk: widgetGVK, key: types.NamespacedName{Namespace: ns, Name: "widget-plain"}, desc: "template with external Update writer and finalizer"},
		{gvk: widgetGVK, key: types.NamespacedName{Namespace: ns, Name: "widget-forced"}, desc: "Force template with external Update writer and finalizer"},
		{gvk: configMapGVK, key: types.NamespacedName{Namespace: ns, Name: "pair"}, desc: "template+patch in one Graph"},
		{gvk: configMapGVK, key: types.NamespacedName{Namespace: ns, Name: "shared"}, desc: "template patched by another Graph"},
		{gvk: configMapGVK, key: types.NamespacedName{Namespace: ns, Name: "g1-gizmo"}, desc: "Kind instance child"},
		{gvk: gizmoGVK, key: types.NamespacedName{Namespace: ns, Name: "g1"}, desc: "Kind instance"},
	}
	for i := range objs {
		obj := get(ctx, t, c, objs[i].gvk, objs[i].key)
		require.NotNil(t, obj, "%s %s must exist before the upgrade", objs[i].desc, objs[i].key)
		objs[i].uid = obj.GetUID()
	}

	// Take over with the controller built from this tree.
	require.NoError(t, env.SwapController(""))
	for _, name := range []string{"widget-plain", "widget-forced"} {
		externalControllerWrite(ctx, t, c, name)
	}

	// For 30s nothing declared may be deleted, recreated or put into
	// deletion, and every Graph must converge without Conflict.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, o := range objs {
			obj := get(ctx, t, c, o.gvk, o.key)
			require.NotNil(t, obj, "%s %s was deleted after the upgrade", o.desc, o.key)
			require.Equal(t, o.uid, obj.GetUID(), "%s %s was deleted and recreated after the upgrade", o.desc, o.key)
			require.Nil(t, obj.GetDeletionTimestamp(), "%s %s was deleted after the upgrade", o.desc, o.key)
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, g := range graphs {
		waitForGraphReady(ctx, t, c, g, 30*time.Second)
	}
}

// externalControllerWrite plays an external controller that owns part of a
// templated object through Update operations: it adds its finalizer and a
// defaulted spec field, then writes status.
func externalControllerWrite(ctx context.Context, t *testing.T, c client.Client, name string) {
	t.Helper()
	key := types.NamespacedName{Namespace: ns, Name: name}
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		obj := get(ctx, t, c, widgetGVK, key)
		if obj == nil {
			return false, nil
		}
		fins := obj.GetFinalizers()
		if !contains(fins, externalFinalize) {
			obj.SetFinalizers(append(fins, externalFinalize))
		}
		_ = unstructured.SetNestedField(obj.Object, "defaulted", "spec", "defaulted")
		if err := c.Update(ctx, obj, client.FieldOwner(externalManager)); err != nil {
			return false, nil
		}
		_ = unstructured.SetNestedField(obj.Object, true, "status", "synced")
		if err := c.Status().Update(ctx, obj, client.FieldOwner(externalManager)); err != nil {
			return false, nil
		}
		return true, nil
	})
	require.NoError(t, err, "external controller write on %s", name)
}

func widgetCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.upgrade.kro.run"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "upgrade.kro.run",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "widgets", Singular: "widget", Kind: "Widget", ListKind: "WidgetList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object", XPreserveUnknownFields: ptr.To(true),
				}},
				Subresources: &apiextensionsv1.CustomResourceSubresources{Status: &apiextensionsv1.CustomResourceSubresourceStatus{}},
			}},
		},
	}
}

func get(ctx context.Context, t *testing.T, c client.Client, gvk schema.GroupVersionKind, key types.NamespacedName) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := c.Get(ctx, key, obj); err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Logf("get %s %s: %v", gvk.Kind, key, err)
		}
		return nil
	}
	return obj
}

func waitForCRD(ctx context.Context, t *testing.T, c client.Client, name string) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := c.Get(ctx, types.NamespacedName{Name: name}, crd); err != nil {
			return false, nil
		}
		for _, cond := range crd.Status.Conditions {
			if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	require.NoError(t, err, "CRD %s not established", name)
}

func waitForGraphReady(ctx context.Context, t *testing.T, c client.Client, key types.NamespacedName, timeout time.Duration) {
	t.Helper()
	var last string
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		g := get(ctx, t, c, graphGVK, key)
		if g == nil {
			if ctx.Err() == nil {
				last = "not found"
			}
			return false, nil
		}
		conds, _, _ := unstructured.NestedSlice(g.Object, "status", "conditions")
		for _, raw := range conds {
			cond, _ := raw.(map[string]any)
			if cond["type"] == "Ready" {
				b, _ := json.Marshal(cond)
				last = string(b)
				return cond["status"] == "True", nil
			}
		}
		last = "no Ready condition"
		return false, nil
	})
	require.NoError(t, err, "Graph %s not Ready: %s", key, last)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

