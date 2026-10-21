package graphcontroller

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metadatafake "k8s.io/client-go/metadata/fake"

	"github.com/kporwal-aws/kro/experimental/controller/watches"
)

// irregularMapper returns a RESTMapper serving kinds whose resource names
// cannot be derived from the kind name.
func irregularMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	add := func(group, kind, plural string, scope meta.RESTScope) {
		gv := schema.GroupVersion{Group: group, Version: "v1alpha1"}
		m.AddSpecific(gv.WithKind(kind), gv.WithResource(plural), gv.WithResource(plural[:len(plural)-1]), scope)
	}
	// Pluralizing the lowercased kind gives "kbs".
	add("example.kro.run", "KBS", "kbses", meta.RESTScopeNamespace)
	// The resource is unrelated to a prefixed kind name.
	add("net.example.kro.run", "V2NTunnel", "tunnels", meta.RESTScopeNamespace)
	add("net.example.kro.run", "Tunnel", "tunnels", meta.RESTScopeNamespace)
	return m
}

func TestResourceFor_UsesServedPlural(t *testing.T) {
	scope := newScopeResolver(irregularMapper())

	cases := []struct {
		gvk  schema.GroupVersionKind
		want string
	}{
		{schema.GroupVersionKind{Group: "example.kro.run", Version: "v1alpha1", Kind: "KBS"}, "kbses"},
		{schema.GroupVersionKind{Group: "net.example.kro.run", Version: "v1alpha1", Kind: "V2NTunnel"}, "tunnels"},
		{schema.GroupVersionKind{Group: "net.example.kro.run", Version: "v1alpha1", Kind: "Tunnel"}, "tunnels"},
	}
	for _, tc := range cases {
		t.Run(tc.gvk.Kind, func(t *testing.T) {
			gvr, err := scope.resourceFor(tc.gvk)
			require.NoError(t, err)
			assert.Equal(t, tc.gvk.GroupVersion().WithResource(tc.want), gvr)
		})
	}
}

func TestResourceFor_UnknownKindIsError(t *testing.T) {
	scope := newScopeResolver(irregularMapper())
	_, err := scope.resourceFor(schema.GroupVersionKind{Group: "example.kro.run", Version: "v1alpha1", Kind: "Missing"})
	require.Error(t, err)
	assert.True(t, meta.IsNoMatchError(err), "NoMatch should be preserved for callers: %v", err)

	// Without a RESTMapper there is nothing to resolve against — never guess.
	var nilScope *scopeResolver
	_, err = nilScope.resourceFor(schema.GroupVersionKind{Group: "example.kro.run", Version: "v1alpha1", Kind: "KBS"})
	require.Error(t, err)
}

// TestRegisterWatch_IrregularPlural verifies the informer is started for the
// served resource, not a pluralization of the kind name.
func TestRegisterWatch_IrregularPlural(t *testing.T) {
	watchMgr := watches.NewWatchManager(metadatafake.NewSimpleMetadataClient(runtime.NewScheme()), time.Hour, nil, logr.Discard())
	t.Cleanup(watchMgr.Shutdown)
	coord := watches.NewWatchCoordinator(watchMgr, func(watches.GraphKey) {}, logr.Discard())

	graph := &unstructured.Unstructured{}
	graph.SetName("g")
	graph.SetNamespace("ns")
	rs := newReconcileScope(graph, coord.ForGraph(types.NamespacedName{Name: "g", Namespace: "ns"}), nil)
	scope := newScopeResolver(irregularMapper())

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("example.kro.run/v1alpha1")
	obj.SetKind("KBS")
	obj.SetName("a")
	obj.SetNamespace("ns")
	require.NoError(t, registerWatch(rs, scope, "node", obj))

	gv := schema.GroupVersion{Group: "example.kro.run", Version: "v1alpha1"}
	assert.Equal(t, "KBS", watchMgr.KindFor(gv.WithResource("kbses")), "informer should watch the served plural")
	assert.Empty(t, watchMgr.KindFor(gv.WithResource("kbs")), "no informer for a guessed plural")

	missing := obj.DeepCopy()
	missing.SetKind("Missing")
	require.Error(t, registerWatch(rs, scope, "other", missing), "unresolvable kind must not register a watch")
	assert.Empty(t, watchMgr.KindFor(gv.WithResource("missings")))
}
