package graphcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestNodeFieldOwner(t *testing.T) {
	assert.Equal(t, "seed.app.team-a.internal.kro.run", string(nodeFieldOwner("app", "team-a", "seed")))
	assert.Equal(t, "seed.rgd.webapps.myapp.ns.internal.kro.run", string(nodeFieldOwner("rgd.webapps.myapp", "ns", "seed")))

	// Over-long names hash the node segment, stay within the API server
	// limit (with room for the status suffix), and remain a sibling.
	long := strings.Repeat("x", 63)
	graph := strings.Repeat("g", 60)
	got := string(nodeFieldOwner(graph, "namespace", long))
	assert.LessOrEqual(t, len(got)+len(statusManagerSuffix), maxFieldManagerLen)
	assert.Equal(t, got, string(nodeFieldOwner(graph, "namespace", long)), "hash must be stable")
	assert.NotEqual(t, got, string(nodeFieldOwner(graph, "namespace", long[:62]+"y")))
	assert.True(t, isGraphFieldManager(got, graph, "namespace"))
}

func TestGraphManagerBase(t *testing.T) {
	const g, ns = "app", "team-a"
	cases := []struct {
		manager string
		base    string
		ours    bool
	}{
		{"seed.app.team-a.internal.kro.run", "seed.app.team-a.internal.kro.run", true},
		{"seed.app.team-a.internal.kro.run.status", "seed.app.team-a.internal.kro.run", true},
		{"status.app.team-a.internal.kro.run", "status.app.team-a.internal.kro.run", true},
		{"app.team-a.internal.kro.run", "app.team-a.internal.kro.run", true},        // legacy
		{"app.team-a.internal.kro.run.status", "app.team-a.internal.kro.run", true}, // legacy status
		{"other.team-a.internal.kro.run", "", false},                                // another Graph
		{"seed.other.team-a.internal.kro.run", "", false},                           // another Graph's node
		{"seed.app.team-b.internal.kro.run", "", false},                             // same name, other namespace
		{"seed.x.app.team-a.internal.kro.run", "", false},                           // node of Graph "x.app"
		{".app.team-a.internal.kro.run", "", false},                                 // empty node segment
		{"kubectl", "", false},
		{"helm", "", false},
	}
	for _, tc := range cases {
		base, ok := graphManagerBase(tc.manager, g, ns)
		assert.Equal(t, tc.ours, ok, tc.manager)
		assert.Equal(t, tc.base, base, tc.manager)
	}
}

func TestThirdPartyFieldManagers_SiblingsAreOurs(t *testing.T) {
	obj := &unstructured.Unstructured{}
	apply := metav1.ManagedFieldsOperationApply
	obj.SetManagedFields([]metav1.ManagedFieldsEntry{
		{Manager: "seed.app.ns.internal.kro.run", Operation: apply},
		{Manager: "fill.app.ns.internal.kro.run", Operation: apply},
		{Manager: "fill.app.ns.internal.kro.run.status", Operation: apply, Subresource: "status"},
		{Manager: "app.ns.internal.kro.run", Operation: apply},
		{Manager: "seed.other.ns.internal.kro.run", Operation: apply},
		{Manager: "external-tool", Operation: apply},
		{Manager: "kubectl-edit", Operation: metav1.ManagedFieldsOperationUpdate},
		{Manager: "kube-apiserver", Operation: apply},
	})
	assert.ElementsMatch(t, []string{"seed.other.ns.internal.kro.run", "external-tool"},
		thirdPartyFieldManagers(obj, "app", "ns"))
	assert.Equal(t, map[string]bool{
		"seed.app.ns.internal.kro.run": false,
		"fill.app.ns.internal.kro.run": true,
		"app.ns.internal.kro.run":      false,
	}, graphManagerEntries(obj, "app", "ns"))
}

func TestOnlyConflictsWith(t *testing.T) {
	legacy := "app.ns.internal.kro.run"
	conflict := func(managers ...string) error {
		var causes []metav1.StatusCause
		for _, m := range managers {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldManagerConflict,
				Message: "conflict with " + m,
				Field:   ".data.k",
			})
		}
		return fmt.Errorf("wrapped: %w", apierrors.NewApplyConflict(causes, "Apply failed"))
	}
	assert.True(t, onlyConflictsWith(conflict(`"app.ns.internal.kro.run"`), legacy))
	assert.True(t, onlyConflictsWith(conflict(`"app.ns.internal.kro.run.status" with subresource "status"`), legacy))
	assert.False(t, onlyConflictsWith(conflict(`"app.ns.internal.kro.run"`, `"external"`), legacy))
	assert.False(t, onlyConflictsWith(conflict(`"seed.app.ns.internal.kro.run"`), legacy), "sibling conflicts are real")
	assert.False(t, onlyConflictsWith(conflict(`"kubectl" using v1 at 2024-01-01T00:00:00Z`), legacy))
	assert.False(t, onlyConflictsWith(apierrors.NewApplyConflict(nil, "no causes"), legacy))
	assert.False(t, onlyConflictsWith(fmt.Errorf("not an API error"), legacy))
}

// TestReleaseApplyStatusClaimsNothing: the status release must be an
// identity-only body. Sending "status": {} claims status itself, leaving an
// entry that owns f:status, which other writers then see as a co-owner of
// every status field.
func TestReleaseApplyStatusClaimsNothing(t *testing.T) {
	c, main, status := capturePatches(t, newClusterWidget("w"))
	_, err := releaseApply(context.Background(), c, clusterWidgetGVK, "", "w", "mgr", true)
	assert.NoError(t, err)
	for _, body := range append(append([][]byte{}, *main...), *status...) {
		var m map[string]any
		assert.NoError(t, json.Unmarshal(body, &m))
		assert.NotContains(t, m, "status", "release body must not claim status: %s", body)
	}
	assert.Len(t, *status, 1)
}

func TestFieldsOverlap_PresenceLeafDoesNotCoverChildren(t *testing.T) {
	presence := map[string]any{"f:status": map[string]any{}}
	items := map[string]any{"f:status": map[string]any{"f:items": map[string]any{}}}
	conditions := map[string]any{"f:status": map[string]any{"f:conditions": map[string]any{}}}
	assert.False(t, fieldsOverlap(presence, items))
	assert.False(t, fieldsOverlap(items, presence))
	assert.False(t, fieldsOverlap(items, conditions))
	assert.True(t, fieldsOverlap(items, items))
	assert.True(t, fieldsOverlap(presence, presence), "two owners of the same leaf overlap")
}
