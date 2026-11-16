package watches

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/tools/cache"
)

// An informer stopped after a cache-sync timeout is restarted the next time a
// graph re-registers the same watch.
func TestEnsureNodeWatch_RestartsStoppedInformer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = metav1.AddMetaToScheme(scheme)
	gvr := schema.GroupVersionResource{Group: "example.kro.run", Version: "v1alpha1", Resource: "tunnels"}
	client := metadatafake.NewSimpleMetadataClient(scheme)

	m := NewWatchManager(client, time.Hour, nil, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.parentCtx = ctx
	created := 0
	base := m.createInformer
	m.createInformer = func(g schema.GroupVersionResource) cache.SharedIndexInformer {
		created++
		return base(g)
	}
	c := NewWatchCoordinator(m, func(GraphKey) {}, logr.Discard())
	graph := types.NamespacedName{Namespace: "shared", Name: "kind.cluster.c1"}

	c.ensureNodeWatch(graph, "tunnel", gvr, "Tunnel", "c1", "tenant-c1", nil)
	m.forceStop(gvr)
	c.ensureNodeWatch(graph, "tunnel", gvr, "Tunnel", "c1", "tenant-c1", nil)

	m.mu.RLock()
	_, running := m.watches[gvr]
	m.mu.RUnlock()
	if !running || created != 2 {
		t.Fatalf("informer running=%v after re-register, created %d times; want running and 2", running, created)
	}
}
