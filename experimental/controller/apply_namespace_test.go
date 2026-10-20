package graphcontroller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/kporwal-aws/kro/experimental/test/testenv"
)

var clusterWidgetGVK = schema.GroupVersionKind{Group: "churn.test.kro.run", Version: "v1", Kind: "ClusterWidget"}

func newClusterWidget(name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(clusterWidgetGVK)
	u.SetName(name)
	return u
}

// capturePatches returns a fake client that records the raw body of every
// main-resource and status Patch it receives.
func capturePatches(t *testing.T, objs ...client.Object) (client.Client, *[][]byte, *[][]byte) {
	t.Helper()
	var main, status [][]byte
	c := fake.NewClientBuilder().
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				data, err := patch.Data(obj)
				require.NoError(t, err)
				main = append(main, data)
				return nil
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				data, err := patch.Data(obj)
				require.NoError(t, err)
				status = append(status, data)
				return nil
			},
		}).
		Build()
	return c, &main, &status
}

// assertNoNamespaceKey fails if an apply body sets metadata.namespace at all.
// An empty namespace on a cluster-scoped object must be omitted, not sent as
// "namespace": "" — the apiserver treats the latter as a change whenever two
// field managers alternate, rewriting the object forever.
func assertNoNamespaceKey(t *testing.T, body []byte) {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	md, _ := m["metadata"].(map[string]any)
	require.NotNil(t, md, "body should carry metadata: %s", body)
	_, has := md["namespace"]
	assert.False(t, has, "cluster-scoped apply body must not set metadata.namespace: %s", body)
}

func TestSSAWriteClusterScopedOmitsNamespace(t *testing.T) {
	c, main, status := capturePatches(t)

	obj := newClusterWidget("w")
	obj.Object["status"] = map[string]any{"items": int64(1)}
	_, err := ssaWrite(context.Background(), c, obj, "mgr", false)
	require.NoError(t, err)

	require.Len(t, *main, 1)
	require.Len(t, *status, 1)
	assertNoNamespaceKey(t, (*main)[0])
	assertNoNamespaceKey(t, (*status)[0])
}

func TestSSAWriteDropsEmptyNamespaceFromMainPayload(t *testing.T) {
	c, main, _ := capturePatches(t)

	// Rendered templates can carry an explicit empty namespace, e.g.
	// instance.metadata.?namespace.orValue("") for a cluster-scoped instance.
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "churn.test.kro.run/v1",
		"kind":       "ClusterWidget",
		"metadata":   map[string]any{"name": "w", "namespace": ""},
	}}
	_, err := ssaWrite(context.Background(), c, obj, "mgr", false)
	require.NoError(t, err)

	require.Len(t, *main, 1)
	assertNoNamespaceKey(t, (*main)[0])
	_, has := obj.Object["metadata"].(map[string]any)["namespace"]
	assert.True(t, has, "ssaWrite must not mutate the caller's object")
}

func TestSSAWriteNamespacedKeepsNamespace(t *testing.T) {
	c, main, status := capturePatches(t)

	obj := newClusterWidget("w")
	obj.SetNamespace("ns")
	obj.Object["status"] = map[string]any{"items": int64(1)}
	_, err := ssaWrite(context.Background(), c, obj, "mgr", false)
	require.NoError(t, err)

	for _, body := range append(*main, *status...) {
		var m map[string]any
		require.NoError(t, json.Unmarshal(body, &m))
		ns, _, _ := unstructured.NestedString(m, "metadata", "namespace")
		assert.Equal(t, "ns", ns)
	}
}

func TestBuildSurgicalBodyClusterScopedOmitsNamespace(t *testing.T) {
	current := newClusterWidget("w")
	current.Object["spec"] = map[string]any{"a": "x"}
	body := buildSurgicalBody(current, clusterWidgetGVK, map[string]any{"f:spec": map[string]any{"f:a": map[string]any{}}})

	data, err := json.Marshal(body)
	require.NoError(t, err)
	assertNoNamespaceKey(t, data)
	assert.Equal(t, "churn.test.kro.run/v1", body["apiVersion"])
	assert.Equal(t, "x", body["spec"].(map[string]any)["a"])
}

func TestReleaseApplyClusterScopedOmitsNamespace(t *testing.T) {
	c, main, status := capturePatches(t, newClusterWidget("w"))

	_, err := releaseApply(context.Background(), c, clusterWidgetGVK, "", "w", "mgr", true)
	require.NoError(t, err)

	require.Len(t, *main, 1)
	require.Len(t, *status, 1)
	assertNoNamespaceKey(t, (*main)[0])
	assertNoNamespaceKey(t, (*status)[0])
}

// TestSSAWriteClusterScopedStatusAlternatingManagersNoChurn reproduces the
// Kind status-apply loop against a real apiserver: two field managers own
// disjoint status fields of a cluster-scoped object and re-apply unchanged
// status alternately, more than a second apart (managedFields timestamps
// have one-second granularity, so closer writes can coalesce and hide the
// bug). After the first round every apply must be a no-op.
func TestSSAWriteClusterScopedStatusAlternatingManagersNoChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("starts envtest")
	}

	env := &envtest.Environment{
		BinaryAssetsDirectory: testenv.ResolveEnvtestAssets(),
		CRDs:                  []*apiextensionsv1.CustomResourceDefinition{clusterWidgetCRD()},
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = env.Stop() })

	c, err := client.New(cfg, client.Options{})
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, c.Create(ctx, newClusterWidget("w")))

	applyA := func() {
		obj := newClusterWidget("w")
		obj.Object["status"] = map[string]any{"items": int64(2)}
		_, err := ssaWrite(ctx, c, obj, "manager-a", false)
		require.NoError(t, err)
	}
	applyB := func() {
		obj := newClusterWidget("w")
		obj.Object["status"] = map[string]any{"phase": "Ready"}
		_, err := ssaWrite(ctx, c, obj, "manager-b", false)
		require.NoError(t, err)
	}
	rv := func() string {
		obj := newClusterWidget("w")
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), obj))
		return obj.GetResourceVersion()
	}

	applyA()
	time.Sleep(1100 * time.Millisecond)
	applyB()
	settled := rv()

	time.Sleep(1100 * time.Millisecond)
	applyA()
	assert.Equal(t, settled, rv(), "manager-a re-applying unchanged status rewrote the object")
	time.Sleep(1100 * time.Millisecond)
	applyB()
	assert.Equal(t, settled, rv(), "manager-b re-applying unchanged status rewrote the object")
}

func clusterWidgetCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "clusterwidgets.churn.test.kro.run"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: clusterWidgetGVK.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "clusterwidgets",
				Singular: "clusterwidget",
				Kind:     clusterWidgetGVK.Kind,
				ListKind: clusterWidgetGVK.Kind + "List",
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    clusterWidgetGVK.Version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {Type: "object", XPreserveUnknownFields: ptr.To(true)},
							"status": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"items": {Type: "integer"},
									"phase": {Type: "string"},
								},
							},
						},
					},
				},
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
			}},
		},
	}
}
