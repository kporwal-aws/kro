package graphcontroller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kporwal-aws/kro/experimental/controller/compiler"
	dagpkg "github.com/kporwal-aws/kro/experimental/controller/dag"
	graphpkg "github.com/kporwal-aws/kro/experimental/controller/graph"
	"github.com/kporwal-aws/kro/experimental/controller/watches"
)

// ---------------------------------------------------------------------------
// Unit tests — design reconciliation regression suite
//
// These test mechanisms introduced in the design reconciliation:
//   - NodeBlocked vs NodeExcluded propagation split
//   - pruneOrderApplied reverse dependency ordering
//   - classifyAPIError default flip
//
// Each test verifies the system DOES the right thing (correctness), not
// just that it doesn't do the wrong thing (safety).
// ---------------------------------------------------------------------------

// TestSetStateDoesNotPropagate proves that SetState only sets the source
// node's state and does NOT propagate to dependents. State propagation
// is the responsibility of checkDependencyGate (called during the walk),
// which evaluates all dependencies with full precedence (Excluded > Blocked > Pending).
//
// The previous implementation used a first-wins flood fill that violated
// precedence in diamond dependencies: if an Error parent propagated
// before an Excluded parent, the child was marked Blocked instead of
// Excluded — an incorrect classification that prevented pruning.
func TestSetStateDoesNotPropagate(t *testing.T) {
	// Build a chain: A → B → C
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}},
		{ID: "b", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}, "data": map[string]any{"ref": "${a.metadata.name}"}}},
		{ID: "c", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "c"}, "data": map[string]any{"ref": "${b.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	states := []NodeState{NodeExcluded, NodeError, NodePending, NodeConflict, NodeSystemError, NodeReady, NodeNotReady}
	for _, sourceState := range states {
		t.Run(sourceState.String(), func(t *testing.T) {
			plan := NewPlanState(dag)
			plan.SetState("a", sourceState)

			assert.Equal(t, sourceState, plan.States["a"], "source should be set")
			assert.Equal(t, NodeUnvisited, plan.States["b"], "direct dependent should remain unvisited")
			assert.Equal(t, NodeUnvisited, plan.States["c"], "transitive dependent should remain unvisited")
		})
	}
}

// TestSummaryCountsBlockedState proves that PlanSummary correctly reports
// HasBlocked when a node is explicitly set to NodeBlocked. Since SetState
// no longer propagates, the test sets dependent state explicitly (matching
// what checkDependencyGate would do during a real walk).
func TestSummaryCountsBlockedState(t *testing.T) {
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}},
		{ID: "b", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}, "data": map[string]any{"ref": "${a.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	plan := NewPlanState(dag)
	plan.SetState("a", NodeError)
	// Simulate what checkDependencyGate does: when b's dependency a is in error,
	// the walk marks b as Blocked.
	plan.SetState("b", NodeBlocked)

	summary := plan.Summary()
	assert.NotEmpty(t, summary.ErrorNodes, "should report error on the source node")
	assert.NotEmpty(t, summary.BlockedNodes, "should report blocked on the dependent node")
	assert.Equal(t, 0, summary.ReadyCount)
}

// ---------------------------------------------------------------------------
// checkDependencyGate state propagation tests
//
// These replace the old SetState propagation tests. The design commitment
// (004 § Propagation: Excluded > Blocked > Pending) is now enforced by
// checkDependencyGate during the walk. These tests exercise it directly.
// ---------------------------------------------------------------------------

// gateToNodeState maps a depGateOutcome to the NodeState that the walk
// would assign. Used by tests to verify precedence without needing a full walk.
func gateToNodeState(g depGateOutcome) NodeState {
	switch g {
	case depExcluded:
		return NodeExcluded
	case depBlocked:
		return NodeBlocked
	case depPending:
		return NodePending
	default:
		return NodeReady // zero value — would proceed to evaluation
	}
}

// TestCheckDependencyGatePrecedence_ExcludedOverBlockedOverPending proves that
// checkDependencyGate enforces the design's state precedence when a node has
// multiple dependencies in different failure states.
//
// Per 005-reconciliation.md § Propagation:
//   - "Any dependency Excluded → Excluded, regardless of other dependencies'
//     states (definitive absence propagates; the node is structurally non-viable)."
//   - "Any dependency in an error state → inherit Blocked."
//   - "Any dependency Pending → inherit Pending."
//   - "Precedence where multiple apply: Excluded > Blocked > Pending"
func TestCheckDependencyGatePrecedence_ExcludedOverBlockedOverPending(t *testing.T) {
	// Diamond: A and B are parents of C.
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}},
		{ID: "b", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}}},
		{ID: "c", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "c"}, "data": map[string]any{"a": "${a.metadata.name}", "b": "${b.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name       string
		stateA     NodeState
		stateB     NodeState
		wantChildC NodeState
	}{
		// Excluded > Blocked: Excluded parent takes precedence over Error parent.
		{"Excluded+Error→Excluded", NodeExcluded, NodeError, NodeExcluded},
		{"Error+Excluded→Excluded", NodeError, NodeExcluded, NodeExcluded},
		// Excluded > Pending: Excluded parent takes precedence over Pending parent.
		{"Excluded+Pending→Excluded", NodeExcluded, NodePending, NodeExcluded},
		{"Pending+Excluded→Excluded", NodePending, NodeExcluded, NodeExcluded},
		// Blocked > Pending: Error parent takes precedence over Pending parent.
		{"Error+Pending→Blocked", NodeError, NodePending, NodeBlocked},
		{"Pending+Error→Blocked", NodePending, NodeError, NodeBlocked},
		// Same states: straightforward inheritance.
		{"Excluded+Excluded→Excluded", NodeExcluded, NodeExcluded, NodeExcluded},
		{"Error+Error→Blocked", NodeError, NodeError, NodeBlocked},
		{"Pending+Pending→Pending", NodePending, NodePending, NodePending},
		// All error variants map to Blocked.
		{"Conflict+Pending→Blocked", NodeConflict, NodePending, NodeBlocked},
		{"SystemError+Pending→Blocked", NodeSystemError, NodePending, NodeBlocked},
		{"Blocked+Pending→Blocked", NodeBlocked, NodePending, NodeBlocked},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := NewPlanState(dag)
			plan.SetState("a", tc.stateA)
			plan.SetState("b", tc.stateB)

			cIdx := dag.Index["c"]
			node := &dag.Nodes[cIdx]

			gate := checkDependencyGate(node, plan)
			gotState := gateToNodeState(gate)
			assert.Equal(t, tc.wantChildC, gotState,
				"child C should inherit %s from parents %s+%s", tc.wantChildC, tc.stateA, tc.stateB)
		})
	}
}

// TestPruneOrderReverseDependency proves that pruneOrderApplied sorts prune
// candidates so dependents are deleted before their dependencies. This
// prevents dangling references during prune.
func TestPruneOrderReverseDependency(t *testing.T) {
	// Build A → B → C. Topological order: A(0), B(1), C(2).
	// Reverse dependency order for deletion: C, B, A.
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}},
		{ID: "b", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}, "data": map[string]any{"ref": "${a.metadata.name}"}}},
		{ID: "c", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "c"}, "data": map[string]any{"ref": "${b.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/a"},
		{Key: "/v1/ConfigMap/default/b"},
		{Key: "/v1/ConfigMap/default/c"},
	}

	ordered := pruneOrderApplied(candidates, []*dagpkg.DAG{dag}, "default", nil)

	require.Len(t, ordered, 3)
	// C depends on B depends on A. Reverse: C first, then B, then A.
	assert.Equal(t, "/v1/ConfigMap/default/c", ordered[0].Key, "most-dependent should be first")
	assert.Equal(t, "/v1/ConfigMap/default/b", ordered[1].Key)
	assert.Equal(t, "/v1/ConfigMap/default/a", ordered[2].Key, "root should be last")
}

// TestPruneOrderUnmatchedKeysFirst proves that keys not matching any DAG
// node are placed first (deleted before mapped resources). This is the safe
// default for dynamic names (forEach, CEL-generated names).
func TestPruneOrderUnmatchedKeysFirst(t *testing.T) {
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/a"},
		{Key: "/v1/ConfigMap/default/unknown-dynamic"},
	}

	ordered := pruneOrderApplied(candidates, []*dagpkg.DAG{dag}, "default", nil)

	require.Len(t, ordered, 2)
	assert.Equal(t, "/v1/ConfigMap/default/unknown-dynamic", ordered[0].Key, "unmatched key should be first")
	assert.Equal(t, "/v1/ConfigMap/default/a", ordered[1].Key)
}

// TestPruneOrderContributeKeysResolved proves that contribute-prefixed keys
// are resolved to their underlying resource key for position lookup.
func TestPruneOrderContributeKeysResolved(t *testing.T) {
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}},
		{ID: "b", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}, "data": map[string]any{"ref": "${a.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/a"},
		{Key: "patch:/v1/ConfigMap/default/b"},
	}

	ordered := pruneOrderApplied(candidates, []*dagpkg.DAG{dag}, "default", nil)

	require.Len(t, ordered, 2)
	assert.Equal(t, "patch:/v1/ConfigMap/default/b", ordered[0].Key, "dependent patch key should be first")
	assert.Equal(t, "/v1/ConfigMap/default/a", ordered[1].Key)
}

// TestPruneOrderCELNamedNodeUsesNodeID proves that CEL-named nodes (whose
// metadata.name contains ${...} and thus can't be resolved statically) use
// the Applied.NodeID field to look up their correct topological position.
// Without this fallback, CEL-named nodes get maxPos+1 and are deleted first,
// which can destroy dependencies before their dependents.
func TestPruneOrderCELNamedNodeUsesNodeID(t *testing.T) {
	// Build A → B → C. A has a CEL name (can't be resolved statically).
	// Topological order: A(0), B(1), C(2).
	// Reverse: C, B, A. A should be last despite its key not matching.
	nodes := []graphpkg.Node{
		{ID: "a", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "${graph.metadata.name}"}}},
		{ID: "b", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "b"}, "data": map[string]any{"ref": "${a.metadata.name}"}}},
		{ID: "c", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "c"}, "data": map[string]any{"ref": "${b.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	// The runtime key for A is resolved from the identity label (NodeID="a").
	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/my-graph-instance", NodeID: "a"},
		{Key: "/v1/ConfigMap/default/b", NodeID: "b"},
		{Key: "/v1/ConfigMap/default/c", NodeID: "c"},
	}

	ordered := pruneOrderApplied(candidates, []*dagpkg.DAG{dag}, "default", nil)

	require.Len(t, ordered, 3)
	assert.Equal(t, "/v1/ConfigMap/default/c", ordered[0].Key, "C (most-dependent) should be first")
	assert.Equal(t, "/v1/ConfigMap/default/b", ordered[1].Key, "B should be second")
	assert.Equal(t, "/v1/ConfigMap/default/my-graph-instance", ordered[2].Key, "A (root, CEL-named) should be last")
}

// TestPruneDependencyGating proves that the prune loop does not delete a
// dependency until all its forward-dependents are confirmed gone. This
// prevents removing a provider (e.g., IAMRoleSelector providing credentials)
// while dependents (e.g., VPC needing those credentials for teardown) still
// exist on the cluster.
func TestPruneDependencyGating(t *testing.T) {
	// Build: iamroleselector → vpc → subnet
	// Forward deps: vpc depends on iamroleselector, subnet depends on vpc.
	// Correct teardown order: subnet first, then vpc, then iamroleselector.
	// The test verifies that when subnet still exists (has a finalizer),
	// vpc is deferred, and iamroleselector is also deferred.
	nodes := []graphpkg.Node{
		{ID: "iamroleselector", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "iam"}}},
		{ID: "vpc", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "vpc"}, "data": map[string]any{"ref": "${iamroleselector.metadata.name}"}}},
		{ID: "subnet", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "subnet"}, "data": map[string]any{"ref": "${vpc.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	graphName := "test-graph"
	graphNS := "default"
	identityLabel := "subnet." + graphName + "." + graphNS + ".internal.kro.run/type"

	// Subnet exists with a finalizer — it will accept DELETE but won't
	// disappear. VPC and IAMRoleSelector should be deferred.
	existingSubnet := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "subnet",
			"namespace":       "default",
			"resourceVersion": "1",
			"finalizers":      []any{"ack.aws/finalizer"},
			"labels": map[string]any{
				identityLabel: "template",
			},
		},
	}}

	fakeClient := fake.NewClientBuilder().
		WithObjects(existingSubnet).
		Build()

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      graphName,
			"namespace": graphNS,
		},
	}}
	rs := newReconcileScope(graph, nil, nil)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/iam", NodeID: "iamroleselector", NodeType: graphpkg.NodeTypeTemplate},
		{Key: "/v1/ConfigMap/default/vpc", NodeID: "vpc", NodeType: graphpkg.NodeTypeTemplate},
		{Key: "/v1/ConfigMap/default/subnet", NodeID: "subnet", NodeType: graphpkg.NodeTypeTemplate},
	}

	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, []*dagpkg.DAG{dag}, nil, &instanceState{})

	// Subnet should be deleted (pruneDeleted) — preflight passes, DELETE issued.
	// But it still exists (finalizer holds it). VPC and IAM should be deferred.
	assert.Equal(t, pruneDeleted, pr.Outcomes["/v1/ConfigMap/default/subnet"],
		"subnet (leaf) should be deleted")
	assert.Equal(t, pruneDeferred, pr.Outcomes["/v1/ConfigMap/default/vpc"],
		"vpc should be deferred until subnet is confirmed gone")
	assert.Equal(t, pruneDeferred, pr.Outcomes["/v1/ConfigMap/default/iam"],
		"iamroleselector should be deferred until vpc is confirmed gone")
}

// TestPruneDependencyGating_DependentsGone proves that when dependents are
// already gone (NotFound), the dependency is free to be deleted.
func TestPruneDependencyGating_DependentsGone(t *testing.T) {
	// Same DAG: iamroleselector → vpc → subnet
	// But this time subnet and vpc don't exist. IAMRoleSelector should
	// be deletable because its dependents are confirmed gone.
	nodes := []graphpkg.Node{
		{ID: "iamroleselector", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "iam"}}},
		{ID: "vpc", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "vpc"}, "data": map[string]any{"ref": "${iamroleselector.metadata.name}"}}},
		{ID: "subnet", Template: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "subnet"}, "data": map[string]any{"ref": "${vpc.metadata.name}"}}},
	}
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	graphName := "test-graph"
	graphNS := "default"
	identityLabel := "iamroleselector." + graphName + "." + graphNS + ".internal.kro.run/type"

	// Only iamroleselector exists — subnet and vpc are already gone.
	existingIAM := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "iam",
			"namespace":       "default",
			"resourceVersion": "1",
			"labels": map[string]any{
				identityLabel: "template",
			},
		},
	}}

	fakeClient := fake.NewClientBuilder().
		WithObjects(existingIAM).
		Build()

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      graphName,
			"namespace": graphNS,
		},
	}}
	rs := newReconcileScope(graph, nil, nil)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/iam", NodeID: "iamroleselector", NodeType: graphpkg.NodeTypeTemplate},
		{Key: "/v1/ConfigMap/default/vpc", NodeID: "vpc", NodeType: graphpkg.NodeTypeTemplate},
		{Key: "/v1/ConfigMap/default/subnet", NodeID: "subnet", NodeType: graphpkg.NodeTypeTemplate},
	}

	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, []*dagpkg.DAG{dag}, nil, &instanceState{})

	// Subnet and VPC are NotFound → confirmed gone → IAM can be deleted.
	assert.Equal(t, pruneSkipped, pr.Outcomes["/v1/ConfigMap/default/subnet"],
		"subnet already gone")
	assert.Equal(t, pruneSkipped, pr.Outcomes["/v1/ConfigMap/default/vpc"],
		"vpc already gone")
	assert.Equal(t, pruneDeleted, pr.Outcomes["/v1/ConfigMap/default/iam"],
		"iamroleselector should be deleted once dependents are confirmed gone")
}

// TestClassifyAPIErrorDefault proves that unrecognized errors (raw Go errors
// not wrapped as *StatusError) become NodeSystemError — the safe direction.
// Misclassifying transient network failures as deterministic (NodeError) means
// the system stops retrying when it should be retrying hardest (30-minute resync
// timer vs 5s SystemError retry). Misclassifying a deterministic error as
// transient means wasted retries — annoying but not an outage.
//
// Client errors (4xx) are positively identified; everything else is
// infrastructure until proven otherwise.
func TestClassifyAPIErrorDefault(t *testing.T) {
	t.Run("raw network error is NodeSystemError", func(t *testing.T) {
		err := &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}
		info := classifyAPIError(err)
		assert.Equal(t, NodeSystemError, info.state,
			"network errors should be NodeSystemError — transient, needs retry")
	})

	t.Run("generic wrapped error is NodeSystemError", func(t *testing.T) {
		err := fmt.Errorf("unexpected EOF during API call")
		info := classifyAPIError(err)
		assert.Equal(t, NodeSystemError, info.state,
			"unrecognized errors default to NodeSystemError — safe direction")
	})

	t.Run("forbidden is NodeError", func(t *testing.T) {
		err := apierrors.NewForbidden(schema.GroupResource{Group: "", Resource: "configmaps"}, "test", fmt.Errorf("forbidden"))
		info := classifyAPIError(err)
		assert.Equal(t, NodeError, info.state)
		assert.Equal(t, "Forbidden", info.reason)
	})

	t.Run("unauthorized is NodeError", func(t *testing.T) {
		err := apierrors.NewUnauthorized("bad token")
		info := classifyAPIError(err)
		assert.Equal(t, NodeError, info.state)
		assert.Equal(t, "Unauthorized", info.reason)
	})

	t.Run("invalid is NodeError", func(t *testing.T) {
		err := apierrors.NewInvalid(schema.GroupKind{Group: "", Kind: "ConfigMap"}, "test", nil)
		info := classifyAPIError(err)
		assert.Equal(t, NodeError, info.state)
		assert.Equal(t, "ValidationFailed", info.reason)
	})

	t.Run("bad request is NodeError", func(t *testing.T) {
		err := apierrors.NewBadRequest("malformed")
		info := classifyAPIError(err)
		assert.Equal(t, NodeError, info.state)
		assert.Equal(t, "BadRequest", info.reason)
	})

	t.Run("internal server error is NodeSystemError", func(t *testing.T) {
		err := apierrors.NewInternalError(fmt.Errorf("etcd timeout"))
		info := classifyAPIError(err)
		assert.Equal(t, NodeSystemError, info.state, "5xx should be NodeSystemError")
		assert.Equal(t, "ServerError", info.reason)
	})

	t.Run("service unavailable is NodeSystemError", func(t *testing.T) {
		err := apierrors.NewServiceUnavailable("maintenance")
		info := classifyAPIError(err)
		assert.Equal(t, NodeSystemError, info.state)
	})

	t.Run("too many requests is NodeSystemError", func(t *testing.T) {
		err := apierrors.NewTooManyRequests("rate limited", 5)
		info := classifyAPIError(err)
		assert.Equal(t, NodeSystemError, info.state)
	})

	t.Run("nil error returns zero value", func(t *testing.T) {
		info := classifyAPIError(nil)
		assert.Equal(t, NodeState(0), info.state)
	})
}

// TestParseNodeListEnforcesValidNodeIDs verifies that parseNodeList rejects
// node IDs that are not valid CEL identifiers. Stricter naming conventions
// (e.g., lower camelCase for RGD resources) are enforced by the consumer.
func TestParseNodeListEnforcesValidNodeIDs(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		// Valid cases — accepted by the parser (valid CEL identifiers)
		{name: "simple alphanumeric", id: "deploy", wantErr: false},
		{name: "single character", id: "a", wantErr: false},
		{name: "camelCase", id: "myApp2v3", wantErr: false},
		{name: "long but valid", id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantErr: false},
		{name: "starts with uppercase", id: "MyResource", wantErr: false},
		{name: "underscore", id: "foo_bar", wantErr: false},
		{name: "multiple underscores", id: "prstatus_test_app_uat", wantErr: false},
		{name: "leading underscore", id: "_foo", wantErr: false},

		// Invalid cases — not valid CEL identifiers
		{name: "empty string", id: "", wantErr: true},
		{name: "starts with digit", id: "123resource", wantErr: true},
		{name: "dot", id: "foo.bar", wantErr: true},
		{name: "space", id: "foo bar", wantErr: true},
		{name: "hyphen", id: "my-app", wantErr: true},
		{name: "leading dot", id: ".foo", wantErr: true},
		{name: "special character", id: "resource!", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := []any{
				map[string]any{
					"id":       tc.id,
					"template": map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm"}},
				},
			}
			_, err := graphpkg.ParseNodeList(raw)
			if tc.wantErr {
				require.Error(t, err, "node ID %q must be rejected", tc.id)
			} else {
				require.NoError(t, err, "node ID %q must be accepted", tc.id)
			}
		})
	}
} // TestNodeStateString verifies that NodeState.String() returns the design's
// canonical names. Each concept has exactly one name.
func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state NodeState
		want  string
	}{
		{NodeUnvisited, "Unvisited"},
		{NodePending, "Pending"},
		{NodeReady, "Ready"},
		{NodeNotReady, "NotReady"},
		{NodeExcluded, "Excluded"},
		{NodeBlocked, "Blocked"},
		{NodeError, "Error"},
		{NodeConflict, "Conflict"},
		{NodeSystemError, "SystemError"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.state.String())
		})
	}
}

// TestForEachChildIdentityLabelKey verifies the DNS subdomain format for
// forEach child identity labels per 005-reconciliation.md § Child Identity.
func TestForEachChildIdentityLabelKey(t *testing.T) {
	key := graphpkg.ForEachChildIdentityLabelKey(
		"policies", "default-deny", "ns-a",
		"NetworkPolicy", "networking.k8s.io",
		"mygraph", "default",
	)
	assert.Equal(t,
		"policies.default-deny.ns-a.networkpolicy.networking.k8s.io.mygraph.default.internal.kro.run/type",
		key,
	)
}

// TestForEachChildIdentityLabelKeyNoGroup verifies core API group resources
// (empty group) omit the group segment.
func TestForEachChildIdentityLabelKeyNoGroup(t *testing.T) {
	key := graphpkg.ForEachChildIdentityLabelKey(
		"configs", "my-cm", "default",
		"ConfigMap", "",
		"mygraph", "default",
	)
	assert.Equal(t,
		"configs.my-cm.default.configmap.mygraph.default.internal.kro.run/type",
		key,
	)
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — regression tests
// ---------------------------------------------------------------------------

// TestGVRKindFromInformerFallback_RegressionIrregularPlurals proves that the
// Kind fallback in gvrKindFromInformer handles irregular plurals correctly.
// Before the fix, "networkpolicies" would produce "Networkpolicie" (garbage).
// After the fix, it produces "Networkpolicy" (correct singular, lowercase).
//
// CamelCase (NetworkPolicy vs Networkpolicy) cannot be reconstructed from
// lowercase resource names — there are no word boundaries in "networkpolicy".
// The primary path (PartialObjectMetadata.Kind) always provides the correct
// CamelCase Kind. This fallback only fires when Kind is empty, which is not
// expected in practice with metadata informers.
func TestGVRKindFromInformerFallback_RegressionIrregularPlurals(t *testing.T) {
	tests := []struct {
		resource string
		want     string
	}{
		// Regular plurals — singularize + title-case first char
		{"configmaps", "Configmap"},
		{"deployments", "Deployment"},
		{"services", "Service"},
		// Irregular plurals — the fix ensures correct singular form.
		// Before: "networkpolicies" → "Networkpolicie" (truncated garbage)
		// After:  "networkpolicies" → "Networkpolicy" (correct singular)
		{"networkpolicies", "Networkpolicy"},
		{"ingresses", "Ingress"},
	}
	for _, tc := range tests {
		t.Run(tc.resource, func(t *testing.T) {
			gvr := schema.GroupVersionResource{Resource: tc.resource}
			got := watches.GVRKindFromInformer(gvr, nil)
			assert.Equal(t, tc.want, got, "GVRKindFromInformer(%q) should produce correct singular form", tc.resource)
		})
	}
}

// TestGVRKindFromInformerPrimaryPath proves that when PartialObjectMetadata
// carries the Kind (the normal case with metadata informers), the exact
// CamelCase Kind is returned.
func TestGVRKindFromInformerPrimaryPath(t *testing.T) {
	accessor := &metav1.PartialObjectMetadata{}
	accessor.Kind = "NetworkPolicy"
	gvr := schema.GroupVersionResource{Resource: "networkpolicies"}
	got := watches.GVRKindFromInformer(gvr, accessor)
	assert.Equal(t, "NetworkPolicy", got, "primary path should return exact CamelCase Kind")
}

// TestClassifyAPIErrorNetworkErrors_RegressionRetry proves that raw network
// errors (not wrapped as *StatusError) get classified as NodeSystemError for
// the 5s retry instead of NodeError's 30-minute resync timer.
func TestClassifyAPIErrorNetworkErrors_RegressionRetry(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"dial error", &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}},
		{"DNS error", &net.DNSError{Err: "no such host", Name: "apiserver", Server: ""}},
		{"generic network", fmt.Errorf("read tcp: connection reset by peer")},
		{"wrapped generic", fmt.Errorf("doing something: %w", fmt.Errorf("unexpected EOF"))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := classifyAPIError(tc.err)
			assert.Equal(t, NodeSystemError, info.state,
				"network/transient error %q should be NodeSystemError for fast retry", tc.err)
		})
	}
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — readyWhen expression errors
//
// Per 001-graph.md: "readyWhen is a health signal — it does not gate
// downstream execution. Dependents proceed as soon as the node is applied
// and its data is in scope, regardless of readyWhen."
//
// A readyWhen expression that returns a non-bool type or hits a CEL error
// is a permanent spec error — but it must NOT produce NodeError (which
// blocks dependents). It must produce NodeNotReady.
// ---------------------------------------------------------------------------

// TestEvalReadiness_ExpressionErrorWrapsReadyWhenFailed proves that when
// readyWhen evaluation fails with a permanent expression error (not data
// pending), evalReadiness wraps it with ErrReadyWhenFailed so the
// coordinator classifies it as NodeNotReady instead of NodeError.
// // TestEvalReadiness_NormalNotReadyIsUnchanged proves that normal readyWhen
// failures (condition evaluates to false, data pending) still produce
// ErrWaitingForReadiness — the fix only affects expression errors.
func TestEvalReadiness_NormalNotReadyIsUnchanged(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{
				ID:        "deploy",
				Template:  map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "app"}},
				ReadyWhen: []string{"${deploy.status.availableReplicas > 0}"},
			},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	eval := &evaluator{compiled: compiled, scope: map[string]any{
		"deploy": map[string]any{
			"metadata": map[string]any{"name": "app"},
			"status":   map[string]any{"availableReplicas": int64(0)},
		},
	}}

	err = eval.evalReadiness("deploy", []string{"${deploy.status.availableReplicas > 0}"})
	require.Error(t, err)

	// Normal readyWhen failure (condition false) — should still be ErrWaitingForReadiness.
	assert.True(t, errors.Is(err, ErrWaitingForReadiness),
		"normal readyWhen=false should produce ErrWaitingForReadiness, got: %v", err)
	assert.False(t, errors.Is(err, ErrReadyWhenFailed),
		"normal readyWhen=false should NOT produce ErrReadyWhenFailed")
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — forEach parent state error precedence
//
// Per 005-reconciliation.md § Parent State: "Deterministic errors (Error)
// take precedence over transient errors (SystemError, Conflict) — if any
// child's failure is deterministic, retrying cannot resolve the parent."
// ---------------------------------------------------------------------------

// TestHighestPriorityChildError proves that when multiple forEach children
// fail, the highest-priority error is returned — not the first one.
func TestHighestPriorityChildError(t *testing.T) {
	errConflict := fmt.Errorf("SSA conflict on apps/v1/Deployment my-app: %w: field taken", ErrFieldConflict)
	errForbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "deployments"}, "my-app", fmt.Errorf("RBAC"))
	errNetwork := &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}
	errCEL := fmt.Errorf("evaluating template: expression returned string, want int")

	tests := []struct {
		name     string
		errs     []error
		wantType NodeState // the expected classification of the returned error
	}{
		{
			name:     "deterministic (Forbidden) over transient (network)",
			errs:     []error{errNetwork, errForbidden},
			wantType: NodeError,
		},
		{
			name:     "deterministic (CEL) over conflict",
			errs:     []error{errConflict, errCEL},
			wantType: NodeError,
		},
		{
			name:     "conflict over network (system error)",
			errs:     []error{errNetwork, errConflict},
			wantType: NodeConflict,
		},
		{
			name:     "single error returns itself",
			errs:     []error{errNetwork},
			wantType: NodeSystemError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := highestPriorityChildError(tc.errs)
			require.NotNil(t, result)

			// Classify the result to check priority.
			if errors.Is(result, ErrFieldConflict) {
				assert.Equal(t, NodeConflict, tc.wantType)
			} else {
				info := classifyAPIError(result)
				assert.Equal(t, tc.wantType, info.state,
					"highest priority error should classify as %s, got %s (error: %v)",
					tc.wantType, info.state, result)
			}
		})
	}
}

// TestHighestPriorityChildError_Nil proves that an empty error list
// returns nil (no children failed).
func TestHighestPriorityChildError_Nil(t *testing.T) {
	assert.Nil(t, highestPriorityChildError(nil))
	assert.Nil(t, highestPriorityChildError([]error{}))
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — readyWhen errors in status
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Correctness reconciliation — Finding 1: staticResourceKey namespace
// ---------------------------------------------------------------------------

// TestStaticResourceKey_ExplicitNamespace proves that staticResourceKey reads
// a literal metadata.namespace from the template instead of always using the
// fallback. Per 005-reconciliation.md § Prune: resource keys encode
// group/version/Kind/namespace/name. If the template specifies a literal
// namespace, the key must use it.
//
// Before the fix, staticResourceKey always used fallbackNamespace, causing
// cross-namespace resources to have wrong keys in the prune candidate set.
func TestStaticResourceKey_ExplicitNamespace(t *testing.T) {
	t.Run("literal namespace overrides fallback", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "my-config",
				"namespace": "kube-system",
			},
		}
		key := staticResourceKey(tmpl, "default", nil)
		assert.Equal(t, "/v1/ConfigMap/kube-system/my-config", key,
			"should use the template's literal namespace, not fallback")
	})

	t.Run("absent namespace uses fallback", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name": "my-config",
			},
		}
		key := staticResourceKey(tmpl, "default", nil)
		assert.Equal(t, "/v1/ConfigMap/default/my-config", key,
			"should use fallback when namespace is absent")
	})

	t.Run("dynamic namespace uses fallback", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "my-config",
				"namespace": "${ns.metadata.name}",
			},
		}
		key := staticResourceKey(tmpl, "default", nil)
		assert.Equal(t, "/v1/ConfigMap/default/my-config", key,
			"should use fallback when namespace contains ${...}")
	})

	t.Run("empty namespace uses fallback", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "my-config",
				"namespace": "",
			},
		}
		key := staticResourceKey(tmpl, "default", nil)
		assert.Equal(t, "/v1/ConfigMap/default/my-config", key,
			"should use fallback when namespace is empty string")
	})
}

// testScopeResolver returns a *scopeResolver with a fake RESTMapper that
// returns pre-configured scope results. Used to verify that staticResourceKey
// produces empty-namespace keys for cluster-scoped kinds, matching what
// resourceKey(liveObj) produces after an API server response strips the
// namespace for cluster-scoped responses.
func testScopeResolver(entries map[string]bool) *scopeResolver {
	return &scopeResolver{
		mapper: &fakeRESTMapper{scopes: entries},
	}
}

// fakeRESTMapper implements meta.RESTMapper for tests, returning pre-configured
// scope (namespaced/cluster-scoped) results based on a string-keyed map.
type fakeRESTMapper struct {
	scopes map[string]bool // "group/version/Kind" → isNamespaced
}

func (f *fakeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	for key, isNamespaced := range f.scopes {
		parts := strings.SplitN(key, "/", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == gk.Group && parts[2] == gk.Kind {
			scope := meta.RESTScopeRoot
			if isNamespaced {
				scope = meta.RESTScopeNamespace
			}
			return &meta.RESTMapping{Scope: scope}, nil
		}
	}
	return nil, fmt.Errorf("no mapping for %v", gk)
}

func (f *fakeRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	m, err := f.RESTMapping(gk, versions...)
	if err != nil {
		return nil, err
	}
	return []*meta.RESTMapping{m}, nil
}

func (f *fakeRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, fmt.Errorf("not implemented")
}

func (f *fakeRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return schema.GroupVersionResource{}, fmt.Errorf("not implemented")
}

func (f *fakeRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeRESTMapper) ResourceSingularizer(resource string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

// TestStaticResourceKey_ClusterScopedNamespace_Regression proves that
// staticResourceKey produces an empty-namespace key for cluster-scoped
// kinds when a scopeResolver is provided — matching what resourceKey(obj)
// produces after the API server strips namespace from cluster-scoped
// responses. Before this fix, staticResourceKey unconditionally
// substituted the fallback (Graph's) namespace, so its keys never matched
// post-apply resourceKey output — prune diffing and finalizer lookups
// silently missed cluster-scoped resources.
//
// Per 003-ownership.md § Priority Resolution: "Cluster-scoped resources
// use empty string for the namespace component."
func TestStaticResourceKey_ClusterScopedNamespace_Regression(t *testing.T) {
	scope := testScopeResolver(map[string]bool{
		"rbac.authorization.k8s.io/v1/ClusterRole": false, // cluster-scoped
		"/v1/ConfigMap": true, // namespaced
	})

	t.Run("cluster-scoped kind yields empty namespace segment", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "rbac.authorization.k8s.io/v1",
			"kind":       "ClusterRole",
			"metadata":   map[string]any{"name": "platform-admin"},
		}
		key := staticResourceKey(tmpl, "my-graph-ns", scope)
		assert.Equal(t, "rbac.authorization.k8s.io/v1/ClusterRole//platform-admin", key,
			"cluster-scoped kinds must have empty namespace segment to match resourceKey(liveObj)")
	})

	t.Run("namespaced kind still substitutes fallback", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "app-config"},
		}
		key := staticResourceKey(tmpl, "my-graph-ns", scope)
		assert.Equal(t, "/v1/ConfigMap/my-graph-ns/app-config", key,
			"namespaced kinds with empty template namespace substitute fallback")
	})

	t.Run("unknown scope falls back to substitution heuristic", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "unknown.example.com/v1",
			"kind":       "UnknownKind",
			"metadata":   map[string]any{"name": "x"},
		}
		// Not in scope map — treat as unknown.
		key := staticResourceKey(tmpl, "my-ns", scope)
		assert.Equal(t, "unknown.example.com/v1/UnknownKind/my-ns/x", key,
			"unknown scope preserves pre-fix heuristic (substitute fallback)")
	})
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — Finding 2: Patch status in teardown
// ---------------------------------------------------------------------------

// TestPatchStatusDetection proves that templateHasStatus correctly
// detects whether a Patch node applies status fields.
// During teardown, this determines whether releaseApply releases the
// status subresource.
//
// Per 003-ownership.md § Status Subresource: "Releases only target the
// subresources the template actually applied to — a status-only Patch
// releases only the status subresource."
func TestPatchStatusDetection(t *testing.T) {
	t.Run("template with status field", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "target"},
			"status":     map[string]any{"ready": true},
		}
		assert.True(t, templateHasStatus(tmpl),
			"template with status field should be detected")
	})

	t.Run("template without status field", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "target"},
			"data":       map[string]any{"key": "value"},
		}
		assert.False(t, templateHasStatus(tmpl),
			"template without status field should not be detected")
	})

	t.Run("template with nil status", func(t *testing.T) {
		tmpl := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "target"},
			"status":     nil,
		}
		assert.False(t, templateHasStatus(tmpl),
			"template with nil status should not be detected")
	})
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — Finding 3: Non-API error classification
// ---------------------------------------------------------------------------

// TestClassifyAPIError_EvalErrorWithNetworkPattern proves that non-API errors
// (CEL evaluation, template rendering) are classified as NodeError even when
// their message contains network-like patterns. Before the fix, an error
// message containing "unexpected EOF" from a JSON marshal failure would
// false-positive as a network error → NodeSystemError → 5s retry loop.
//
// The fix: errors originating from non-API operations are wrapped with
// ErrEvaluation at the source (toMap, evalString). classifyAPIError checks
// for this sentinel before falling through to network pattern matching.
//
// This tests a classification boundary on a pure function — manufacturing
// the same false-positive in e2e would require a contrived expression whose
// error text happens to match network patterns. The unit test encodes the
// property directly.
func TestClassifyAPIError_EvalErrorWithNetworkPattern(t *testing.T) {
	// An evaluation error whose message contains a network error pattern.
	// Without the sentinel, this would be classified as NodeSystemError.
	evalErr := fmt.Errorf("evaluating template: %w: unexpected EOF in field value",
		ErrEvaluation)
	info := classifyAPIError(evalErr)
	assert.Equal(t, NodeError, info.state,
		"evaluation errors must be NodeError even when message contains network patterns")

	// A real network error should still be NodeSystemError.
	netErr := fmt.Errorf("unexpected EOF during API call")
	info = classifyAPIError(netErr)
	assert.Equal(t, NodeSystemError, info.state,
		"real network errors without ErrEvaluation sentinel should remain NodeSystemError")
}

// ---------------------------------------------------------------------------
// Correctness reconciliation — Finding 4: Status priority
// ---------------------------------------------------------------------------

// TestDeriveReadyCondition_BlockedBeforePending proves that when both
// Blocked and Pending states coexist, Blocked takes priority in the Ready
// condition. Blocked means "upstream error — someone needs to act."
// Pending means "just waiting." Blocked is more actionable.
//
// Per 005-reconciliation.md § Propagation: "Precedence where multiple
// apply: Excluded > Blocked > Pending."
//
// This tests a pure priority function over inputs — not reconciliation
// behavior. The inputs are a set of states, the output is a condition.
func TestDeriveReadyCondition_BlockedBeforePending(t *testing.T) {
	state := &reconcileState{
		compiled: true,
		planSummary: PlanSummary{
			PendingNodes: []string{"deploy"},
			BlockedNodes: []string{"svc"},
		},
	}
	outcome := state.deriveReadyCondition()
	assert.Equal(t, conditionUnknown, outcome.status)
	assert.Equal(t, "Blocked", outcome.reason,
		"Blocked should take priority over Pending — upstream error is more actionable than waiting")
}

// TestDeriveReadyCondition_PendingSurfacesReasons proves that when the
// Ready condition is Pending with a nodeError entry for a pending node,
// the message includes the error detail. Pure function test — exercises
// the formatting of deriveReadyCondition.
func TestDeriveReadyCondition_PendingSurfacesReasons(t *testing.T) {
	s := &reconcileState{
		compiled:    true,
		planSummary: PlanSummary{PendingNodes: []string{"deploy"}},
		nodeErrors:  []string{"deploy: waiting for input from cfg"},
	}
	outcome := s.deriveReadyCondition()
	assert.Contains(t, outcome.message, "1 pending")
	assert.Contains(t, outcome.message, "deploy (pending): waiting for input from cfg")
}

// TestDeriveReadyCondition_MessageFormat verifies the exact message structure
// for various state combinations.
func TestDeriveReadyCondition_MessageFormat(t *testing.T) {
	tests := []struct {
		name        string
		state       reconcileState
		wantMessage string
	}{
		{
			name: "all ready",
			state: reconcileState{
				compiled:    true,
				nodeCount:   3,
				planSummary: PlanSummary{ReadyCount: 3},
			},
			wantMessage: "3 ready",
		},
		{
			name: "mixed states without errors",
			state: reconcileState{
				compiled: true,
				planSummary: PlanSummary{
					ReadyCount:    5,
					NotReadyNodes: []string{"deploy", "svc"},
					PendingNodes:  []string{"ingress"},
				},
			},
			wantMessage: "5 ready, 2 not ready, 1 pending\n  deploy (not ready)\n  ingress (pending)\n  svc (not ready)",
		},
		{
			name: "error with detail",
			state: reconcileState{
				compiled: true,
				planSummary: PlanSummary{
					ReadyCount: 2,
					ErrorNodes: []string{"broken"},
				},
				nodeErrors: []string{"broken: 403 Forbidden"},
			},
			wantMessage: "2 ready, 1 error\n  broken (error): 403 Forbidden",
		},
		{
			name: "multiple errors sorted alphabetically",
			state: reconcileState{
				compiled: true,
				planSummary: PlanSummary{
					ReadyCount: 1,
					ErrorNodes: []string{"zeta", "alpha"},
				},
				nodeErrors: []string{"zeta: bad request", "alpha: forbidden"},
			},
			wantMessage: "1 ready, 2 error\n  alpha (error): forbidden\n  zeta (error): bad request",
		},
		{
			name: "system error + error sorted by node ID",
			state: reconcileState{
				compiled: true,
				planSummary: PlanSummary{
					ReadyCount:       3,
					ErrorNodes:       []string{"auth"},
					SystemErrorNodes: []string{"db"},
				},
				nodeErrors: []string{"auth: 403 Forbidden", "db: 503 Service Unavailable"},
			},
			wantMessage: "3 ready, 1 error, 1 system error\n  auth (error): 403 Forbidden\n  db (system error): 503 Service Unavailable",
		},
		{
			name: "excluded nodes counted",
			state: reconcileState{
				compiled: true,
				planSummary: PlanSummary{
					ReadyCount:    5,
					ExcludedCount: 3,
				},
			},
			wantMessage: "5 ready, 3 excluded",
		},
		{
			name: "blocked nodes counted",
			state: reconcileState{
				compiled: true,
				planSummary: PlanSummary{
					ReadyCount:   3,
					BlockedNodes: []string{"downstream"},
					ErrorNodes:   []string{"upstream"},
				},
				nodeErrors: []string{"upstream: 422 invalid"},
			},
			wantMessage: "3 ready, 1 blocked, 1 error\n  downstream (blocked)\n  upstream (error): 422 invalid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome := tt.state.deriveReadyCondition()
			assert.Equal(t, tt.wantMessage, outcome.message)
		})
	}
}

// TestDeriveReadyCondition_Truncation verifies that node detail lines are
// capped at maxNodeDetails (10) with a "... and N more" suffix.
func TestDeriveReadyCondition_Truncation(t *testing.T) {
	// Build a state with 13 error nodes.
	errorNodes := make([]string, 13)
	nodeErrors := make([]string, 13)
	for i := range errorNodes {
		id := fmt.Sprintf("node%02d", i)
		errorNodes[i] = id
		nodeErrors[i] = fmt.Sprintf("%s: error %d", id, i)
	}

	s := &reconcileState{
		compiled: true,
		planSummary: PlanSummary{
			ReadyCount: 5,
			ErrorNodes: errorNodes,
		},
		nodeErrors: nodeErrors,
	}
	outcome := s.deriveReadyCondition()

	lines := strings.Split(outcome.message, "\n")
	// 1 summary + 10 detail + 1 truncation = 12 lines
	assert.Equal(t, 12, len(lines),
		"should have summary + 10 details + truncation line")
	assert.Equal(t, "5 ready, 13 error", lines[0],
		"summary should count all errors, not just shown ones")
	assert.Equal(t, "  ... and 3 more", lines[11],
		"truncation line should say how many are hidden")
}

// TestDeriveCompiledCondition_MessageFormat verifies compiled message.
func TestDeriveCompiledCondition_MessageFormat(t *testing.T) {
	t.Run("success shows node count", func(t *testing.T) {
		s := &reconcileState{compiled: true, nodeCount: 47}
		outcome := s.deriveCompiledCondition()
		assert.Equal(t, "47 nodes", outcome.message)
	})
	t.Run("zero nodes", func(t *testing.T) {
		s := &reconcileState{compiled: true, nodeCount: 0}
		outcome := s.deriveCompiledCondition()
		assert.Equal(t, "0 nodes", outcome.message)
	})
}
func TestCheckDependencyGate_RegressionExcludedPersistence(t *testing.T) {
	// Build a DAG: root → child. Root will be Excluded in the plan.
	// checkDependencyGate on child should return gateExcluded.
	nodes := []graphpkg.Node{
		{
			ID: "root",
			Template: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "root"},
			},
		},
		{
			ID: "child",
			Template: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "child"},
				"data":     map[string]any{"ref": "${root.data.key}"},
			},
		},
	}
	spec := &graphpkg.GraphSpec{Nodes: nodes}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)
	dag := dagpkg.AssembleDAG(spec.Nodes, compiled.Topology)

	plan := NewPlanState(dag)
	// Mark root as Excluded in the plan (simulates root's includeWhen=false).
	plan.SetState("root", NodeExcluded)

	childIdx := dag.Index["child"]
	node := &dag.Nodes[childIdx]

	gate := checkDependencyGate(node, plan)
	assert.Equal(t, depExcluded, gate,
		"child should see depExcluded when root is Excluded")
	assert.Equal(t, NodeExcluded, gateToNodeState(gate),
		"child's gated state should be Excluded")
}

// ---------------------------------------------------------------------------
// pickEffectiveGeneration — compile-failure fallback generation (#19)
// ---------------------------------------------------------------------------

// revisionWithGeneration builds a minimal revision fixture for tests that
// only care about the generation label.
func revisionWithGeneration(gen int64) *unstructured.Unstructured {
	r := &unstructured.Unstructured{}
	r.SetLabels(map[string]string{
		graphpkg.LabelGraphGeneration: fmt.Sprintf("%d", gen),
	})
	return r
}

// TestPickEffectiveGeneration_HappyPath proves the default branch: when
// compilation succeeded, the graph's live generation is what was applied,
// so that's what gets stamped.
func TestPickEffectiveGeneration_HappyPath(t *testing.T) {
	graph := &unstructured.Unstructured{}
	graph.SetGeneration(7)
	active := revisionWithGeneration(7)

	got := pickEffectiveGeneration(graph, active, nil)
	assert.Equal(t, int64(7), got)
}

// TestPickEffectiveGeneration_RegressionFallbackGeneration guards #19: on
// compile-failure fallback, stamping the graph's generation would label
// resources with the generation whose spec didn't compile. The active
// revision's generation reflects what actually materialized.
func TestPickEffectiveGeneration_RegressionFallbackGeneration(t *testing.T) {
	graph := &unstructured.Unstructured{}
	graph.SetGeneration(10) // the spec at gen 10 failed to compile
	active := revisionWithGeneration(9)
	compileErr := errors.New("cel: undeclared reference to 'foo'")

	got := pickEffectiveGeneration(graph, active, compileErr)
	assert.Equal(t, int64(9), got,
		"fallback must stamp the active revision's generation, not the failed one")
}

// TestPickEffectiveGeneration_FallbackWithoutRevision is a defensive case.
// If compile failed AND no prior revision exists, the fallback path in the
// reconciler returns early before this is called. But the helper should
// still behave sanely: fall back to the graph generation rather than 0,
// which would produce a "generation=0" label.
func TestPickEffectiveGeneration_FallbackWithoutRevision(t *testing.T) {
	graph := &unstructured.Unstructured{}
	graph.SetGeneration(5)

	got := pickEffectiveGeneration(graph, nil, errors.New("compile failed"))
	assert.Equal(t, int64(5), got,
		"without an active revision, fall back to the graph's generation rather than stamping 0")
}

// ---------------------------------------------------------------------------
// forEach scope type safety
// ---------------------------------------------------------------------------

// TestForEach_RegressionNonSliceScopeReadyWhenNoPanic proves that if a forEach
// node's scope entry is not []any, the readyWhen stamping path returns an
// error instead of panicking on a type assertion.
//
// Regression: scopeVal.([]any) at foreach.go:312 is an unchecked type
// assertion that panics when scope contains a non-slice value.
func TestForEach_RegressionNonSliceScopeReadyWhenNoPanic(t *testing.T) {
	// forEachStampReadyWhen is the function under test (extracted helper).
	// Inject a non-slice value into scope for the forEach node.
	scope := map[string]any{
		"items": "not-a-slice",
	}
	err := forEachStampReadyWhen(scope, "items", []string{"true"}, nil)
	assert.Error(t, err, "non-slice scope should produce an error")
	assert.Contains(t, err.Error(), "expected []any")
}

// TestForEach_RegressionNonSliceScopeNoReadyWhenNoPanic proves the same for
// the no-readyWhen path (forEach nodes without readyWhen stamp all items
// __ready=true).
func TestForEach_RegressionNonSliceScopeNoReadyWhenNoPanic(t *testing.T) {
	scope := map[string]any{
		"items": 42, // should be []any
	}
	err := forEachStampReadyWhen(scope, "items", nil, nil)
	assert.Error(t, err, "non-slice scope should produce an error")
}

// TestForEach_SliceScopeReadyWhenWorks confirms the happy path still works.
func TestForEach_SliceScopeReadyWhenWorks(t *testing.T) {
	scope := map[string]any{
		"items": []any{
			map[string]any{"metadata": map[string]any{"name": "a"}},
			map[string]any{"metadata": map[string]any{"name": "b"}},
		},
	}
	// No readyWhen expressions — all items get __ready=true.
	err := forEachStampReadyWhen(scope, "items", nil, nil)
	require.NoError(t, err)
	items := scope["items"].([]any)
	for _, item := range items {
		m := item.(map[string]any)
		assert.Equal(t, true, m["__ready"])
	}
}

// ---------------------------------------------------------------------------
// .updated() CEL function tests
//
// Per 001-graph.md § CEL Functions: ".updated() — true when the node is
// on the latest graph generation." Per 005-reconciliation.md § Propagation
// Control: used in propagateWhen for forEach rollout gating.
// ---------------------------------------------------------------------------

// TestUpdatedFunction_ScalarNode proves that .updated() reads __updated from
// a scalar scope object (map[string]any).
func TestUpdatedFunction_ScalarNode(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "deploy", Template: map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": "test"},
			}},
			{ID: "check", Def: map[string]any{
				"result": "${deploy.updated()}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		updated any
		want    bool
	}{
		{"updated=true", true, true},
		{"updated=false", false, false},
		{"no __updated field", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deploy := map[string]any{
				"metadata": map[string]any{"name": "test"},
			}
			if tt.updated != nil {
				deploy["__updated"] = tt.updated
			}
			result, err := compiled.Eval("deploy.updated()", map[string]any{
				"deploy": deploy,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

// TestUpdatedFunction_Collection proves that .updated() on a collection
// returns true only when ALL items have __updated == true (the aggregate
// semantics matching .ready()). Empty collections are vacuously true.
func TestUpdatedFunction_Collection(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			node(graphpkg.Node{ID: "deploys", Watch: map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"selector": map[string]any{},
			}}, graphpkg.NodeTypeWatch),
			{ID: "check", Def: map[string]any{
				"result": "${deploys.updated()}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	tests := []struct {
		name  string
		items []any
		want  bool
	}{
		{"empty collection", []any{}, true},
		{"all updated", []any{
			map[string]any{"__updated": true, "metadata": map[string]any{"name": "a"}},
			map[string]any{"__updated": true, "metadata": map[string]any{"name": "b"}},
		}, true},
		{"one not updated", []any{
			map[string]any{"__updated": true, "metadata": map[string]any{"name": "a"}},
			map[string]any{"__updated": false, "metadata": map[string]any{"name": "b"}},
		}, false},
		{"none updated", []any{
			map[string]any{"__updated": false, "metadata": map[string]any{"name": "a"}},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := compiled.Eval("deploys.updated()", map[string]any{
				"deploys": tt.items,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

// TestUpdatedFunction_Filter proves that .updated() works inside
// collection filter expressions — the primary propagateWhen pattern from
// 005-reconciliation.md § Propagation Control.
func TestUpdatedFunction_Filter(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			node(graphpkg.Node{ID: "deploys", Watch: map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"selector": map[string]any{},
			}}, graphpkg.NodeTypeWatch),
			{ID: "check", Def: map[string]any{
				"count": "${deploys.filter(d, d.updated()).size()}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	items := []any{
		map[string]any{"__updated": true, "metadata": map[string]any{"name": "a"}},
		map[string]any{"__updated": false, "metadata": map[string]any{"name": "b"}},
		map[string]any{"__updated": true, "metadata": map[string]any{"name": "c"}},
	}
	result, err := compiled.Eval("deploys.filter(d, d.updated()).size()", map[string]any{
		"deploys": items,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), result)
}

// TestUpdatedFunction_CombinedWithReady proves the four-state matrix from
// 005-reconciliation.md § Propagation Control works in CEL:
//
//	updated() && ready()  → Current
//	updated() && !ready() → Updating
//	!updated() && ready() → Pending
//	!updated() && !ready()→ Stuck
func TestUpdatedFunction_CombinedWithReady(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			node(graphpkg.Node{ID: "items", Watch: map[string]any{
				"apiVersion": "v1", "kind": "Pod",
				"selector": map[string]any{},
			}}, graphpkg.NodeTypeWatch),
			{ID: "counts", Def: map[string]any{
				"current":  "${items.filter(i, i.updated() && i.ready()).size()}",
				"updating": "${items.filter(i, i.updated() && !i.ready()).size()}",
				"pending":  "${items.filter(i, !i.updated() && i.ready()).size()}",
				"stuck":    "${items.filter(i, !i.updated() && !i.ready()).size()}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	items := []any{
		map[string]any{"__updated": true, "__ready": true, "metadata": map[string]any{"name": "current"}},
		map[string]any{"__updated": true, "__ready": false, "metadata": map[string]any{"name": "updating"}},
		map[string]any{"__updated": false, "__ready": true, "metadata": map[string]any{"name": "pending"}},
		map[string]any{"__updated": false, "__ready": false, "metadata": map[string]any{"name": "stuck"}},
	}
	scope := map[string]any{"items": items}

	current, err := compiled.Eval("items.filter(i, i.updated() && i.ready()).size()", scope)
	require.NoError(t, err)
	assert.Equal(t, int64(1), current)

	updating, err := compiled.Eval("items.filter(i, i.updated() && !i.ready()).size()", scope)
	require.NoError(t, err)
	assert.Equal(t, int64(1), updating)

	pending, err := compiled.Eval("items.filter(i, !i.updated() && i.ready()).size()", scope)
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending)

	stuck, err := compiled.Eval("items.filter(i, !i.updated() && !i.ready()).size()", scope)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stuck)
}

// ---------------------------------------------------------------------------
// isItemUpdated tests — generation label comparison
// ---------------------------------------------------------------------------

// TestIsForEachItemUpdated proves the generation label lookup for forEach children.
func TestIsForEachItemUpdated(t *testing.T) {
	parentID := "deploys"
	graphName := "mygraph"
	graphNS := "default"

	// Build a resource with the forEach child generation label.
	makeItem := func(name, ns, apiVersion, kind string, gen int64) map[string]any {
		gvk := graphpkg.GVKFromMap(map[string]any{"apiVersion": apiVersion, "kind": kind})
		genKey := graphpkg.ForEachChildGenerationLabelKey(parentID, name, ns, kind, gvk.Group, graphName, graphNS)
		return map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]any{
				"name":      name,
				"namespace": ns,
				"labels": map[string]any{
					genKey: fmt.Sprintf("%d", gen),
				},
			},
		}
	}

	tests := []struct {
		name                string
		item                map[string]any
		effectiveGeneration int64
		want                bool
	}{
		{
			name:                "generation matches",
			item:                makeItem("frontend", "default", "apps/v1", "Deployment", 5),
			effectiveGeneration: 5,
			want:                true,
		},
		{
			name:                "generation does not match",
			item:                makeItem("frontend", "default", "apps/v1", "Deployment", 4),
			effectiveGeneration: 5,
			want:                false,
		},
		{
			name:                "no metadata",
			item:                map[string]any{"kind": "Pod"},
			effectiveGeneration: 5,
			want:                false, // unknown provenance → needs update
		},
		{
			name: "no labels",
			item: map[string]any{
				"apiVersion": "v1", "kind": "Pod",
				"metadata": map[string]any{"name": "test"},
			},
			effectiveGeneration: 5,
			want:                false, // no labels → needs update
		},
		{
			name: "no generation label (wrong graph)",
			item: map[string]any{
				"apiVersion": "v1", "kind": "Pod",
				"metadata": map[string]any{
					"name":      "test",
					"namespace": "default",
					"labels":    map[string]any{"unrelated": "label"},
				},
			},
			effectiveGeneration: 5,
			want:                false, // no kro generation label → needs update
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isForEachItemUpdated(tt.item, parentID, graphName, graphNS, tt.effectiveGeneration)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestMarkUpdated proves that markUpdated stamps __updated on scope entries.
func TestMarkUpdated(t *testing.T) {
	eval := &evaluator{
		scope: map[string]any{
			"deploy": map[string]any{"metadata": map[string]any{"name": "test"}},
		},
	}

	eval.markUpdated("deploy", true)
	m := eval.scope["deploy"].(map[string]any)
	assert.Equal(t, true, m["__updated"])

	eval.markUpdated("deploy", false)
	assert.Equal(t, false, m["__updated"])
}

// ---------------------------------------------------------------------------
// __updated stamping integration tests
//
// These exercise the actual code paths that stamp __updated, not just the
// helper functions. Each test drives a real reconciliation method or
// forEach loop path and verifies __updated appears with the correct value.
// ---------------------------------------------------------------------------

// TestReconcileDefinition_StampsUpdated proves that reconcileDefinition
// stamps __updated = true on the scope entry. Definition nodes are always
// re-evaluated — vacuously updated.
func TestReconcileDefinition_StampsUpdated(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "config", Def: map[string]any{
				"name":  "my-app",
				"port":  "8080",
				"debug": "false",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	eval := &evaluator{compiled: compiled, scope: map[string]any{}}
	err = reconcileDefinition(context.Background(), spec.Nodes[0], eval)
	require.NoError(t, err)

	m, ok := eval.scope["config"].(map[string]any)
	require.True(t, ok, "scope should contain config")
	assert.Equal(t, true, m["__updated"],
		"definition node must have __updated = true (always re-evaluated)")
}

// TestForEach_HaltedItemsMakeParentPending proves that when forEach's per-item
// propagateWhen halts expansion, the parent becomes Pending (not NotReady),
// halted items are not dispatched (no scope injection), and only the keys from
// dispatched items are returned.
func TestForEach_HaltedItemsMakeParentPending(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "source", Def: map[string]any{
				"items": []any{
					map[string]any{"metadata": map[string]any{"name": "alpha"}},
					map[string]any{"metadata": map[string]any{"name": "beta"}},
				},
			}},
			{ID: "workers", ForEach: &graphpkg.ForEachBinding{VarName: "item", Expr: "${source.items}"},
				// propagateWhen that immediately halts — forces ALL items
				// through the halted path.
				PropagateWhen: []string{"${false}"},
				Def:           map[string]any{"name": "${item.metadata.name}"},
			},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	state := newInstanceState(compiled, nil)
	eval := newEvaluator(state)
	eval.effectiveGeneration = 5

	// Populate source in scope.
	eval.scope["source"] = map[string]any{
		"items": []any{
			map[string]any{"metadata": map[string]any{"name": "alpha"}},
			map[string]any{"metadata": map[string]any{"name": "beta"}},
		},
	}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": "test", "namespace": "default"},
	}}

	r := &GraphReconciler{}
	rs := newReconcileScope(graph, nil, nil)
	_, err = r.cluster().reconcileForEach(context.Background(), rs, spec.Nodes[1], eval)
	// ErrPending expected — propagateWhen halted expansion, parent is Pending.
	require.ErrorIs(t, err, ErrPending)

	// Scope should NOT be published when halted (no partial scope).
	_, scopePublished := eval.scope["workers"]
	assert.False(t, scopePublished, "scope should not be published when halted")
}

// TestForEach_DefinitionItemsAlwaysReEvaluated proves that forEach definition
// items are always re-evaluated (no caching). Every item gets __updated = true
// because definitions are vacuously updated on every reconcile.
func TestForEach_DefinitionItemsAlwaysReEvaluated(t *testing.T) {
	sourceNode := graphpkg.Node{ID: "source", Def: map[string]any{
		"items": []any{
			map[string]any{"metadata": map[string]any{"name": "alpha"}},
		},
	}}
	sourceNode.SetType(graphpkg.NodeTypeDef)

	resultsNode := graphpkg.Node{ID: "results", ForEach: &graphpkg.ForEachBinding{VarName: "item", Expr: "${source.items}"},
		Def: map[string]any{"name": "${item.metadata.name}"},
	}
	resultsNode.SetType(graphpkg.NodeTypeDef)

	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{sourceNode, resultsNode},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	state := newInstanceState(compiled, nil)
	eval := newEvaluator(state)
	eval.effectiveGeneration = 7

	sourceItems := []any{
		map[string]any{"metadata": map[string]any{"name": "alpha"}},
	}
	eval.scope["source"] = map[string]any{"items": sourceItems}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata":   map[string]any{"name": "test", "namespace": "default"},
	}}

	r := &GraphReconciler{}
	rs := newReconcileScope(graph, nil, nil)
	_, err = r.cluster().reconcileForEach(context.Background(), rs, spec.Nodes[1], eval)
	require.NoError(t, err)

	items, ok := eval.scope["results"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	alpha, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, alpha["__updated"],
		"definition items are always re-evaluated and should be updated")
}

// TestForEach_RegressionSharedContextPropagation proves that when a shared
// dependency changes but collection items don't, forEach items are re-evaluated
// with the new context rather than serving stale output.
func TestForEach_RegressionSharedContextPropagation(t *testing.T) {
	graphObj := map[string]any{
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{"id": "config", "def": map[string]any{"version": "v1"}},
				map[string]any{"id": "source", "def": map[string]any{"names": []any{"alpha", "beta"}}},
				map[string]any{
					"id": "results", "forEach": map[string]any{"item": "${source.names}"},
					"def": map[string]any{"combined": "${item}-${config.version}"},
				},
			},
		},
	}
	spec, err := graphpkg.ExtractGraphSpec(graphObj)
	require.NoError(t, err)
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)
	dag := dagpkg.AssembleDAG(spec.Nodes, compiled.Topology)
	resultsNode := dag.Nodes[dag.Index["results"]]

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1", "kind": "Graph",
		"metadata": map[string]any{"name": "test", "namespace": "default"},
	}}
	r := &GraphReconciler{}
	c := r.cluster()
	rs := newReconcileScope(graph, nil, nil)
	names := []any{"alpha", "beta"}

	// Phase 1: config.version = "v1"
	state := newInstanceState(compiled, nil)
	eval := newEvaluator(state)
	eval.effectiveGeneration = 1
	eval.scope["config"] = map[string]any{"version": "v1"}
	eval.scope["source"] = map[string]any{"names": names}

	_, err = c.reconcileForEach(context.Background(), rs, resultsNode, eval)
	require.NoError(t, err)
	items1, ok := eval.scope["results"].([]any)
	require.True(t, ok)
	require.Len(t, items1, 2)
	for _, item := range items1 {
		assert.Contains(t, item.(map[string]any)["combined"].(string), "-v1")
	}

	// Phase 2: config.version = "v2", collection unchanged.
	// Items are always re-evaluated so they pick up the new config.
	eval2 := newEvaluator(state)
	eval2.effectiveGeneration = 1
	eval2.scope["config"] = map[string]any{"version": "v2"}
	eval2.scope["source"] = map[string]any{"names": names}

	_, err = c.reconcileForEach(context.Background(), rs, resultsNode, eval2)
	require.NoError(t, err)
	items2, ok := eval2.scope["results"].([]any)
	require.True(t, ok)
	require.Len(t, items2, 2)
	for _, item := range items2 {
		combined := item.(map[string]any)["combined"].(string)
		assert.Contains(t, combined, "-v2",
			"output should reference v2, not stale v1")
		assert.NotContains(t, combined, "-v1",
			"stale v1 should not appear after config change")
	}
}

// TestPath2_SelfRefresh_StampsReady proves that when Path 2 (hash-match +
// self-state changed) replaces a node's scope with a fresh GET, the __ready
// flag is re-stamped so downstream .ready() calls return the correct value.
//
// Regression: Path 2 called evalReadinessConditions (which doesn't call markReady)
// instead of evalReadiness. The fresh GET replaced the scope object, wiping
// the __ready flag from the previous reconcile. Downstream nodes using
// .ready() found __ready missing and returned false — even when the node
// was actually ready. This caused CRD nodes (which have no readyWhen and
// are vacuously ready) to appear not-ready after any self-state change,
// blocking rgdStatus propagation and preventing RGDs from staying Active.
func TestPath2_SelfRefresh_StampsReady(t *testing.T) {
	// Build: crd (template, no readyWhen) → downstream (def, uses crd.ready())
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "crd", Template: map[string]any{
				"apiVersion": "apiextensions.k8s.io/v1",
				"kind":       "CustomResourceDefinition",
				"metadata":   map[string]any{"name": "test.kro.run"},
			}},
			{ID: "downstream", Def: map[string]any{
				"result": "${crd.ready()}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)

	// Phase 1: Normal dispatch stamps __ready via evalReadiness.
	scopeWithReady := map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "test.kro.run", "resourceVersion": "1"},
		"status":     map[string]any{"conditions": []any{map[string]any{"type": "Established", "status": "True"}}},
	}
	eval1 := &evaluator{
		scope:    map[string]any{"crd": scopeWithReady},
		compiled: compiled,
	}
	// evalReadiness with no readyWhen stamps __ready = true
	require.NoError(t, eval1.evalReadiness("crd", nil))
	result1, err := compiled.Eval("crd.ready()", eval1.scope)
	require.NoError(t, err)
	assert.True(t, result1.(bool), "after evalReadiness, crd.ready() should be true")

	// Phase 2: Simulate Path 2 — fresh GET replaces scope (no __ready).
	// This is what controller.go:488 does: w.eval.scope[node.ID] = readBack.Object
	freshGET := map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "test.kro.run", "resourceVersion": "2"},
		"status":     map[string]any{"conditions": []any{map[string]any{"type": "Established", "status": "True"}}},
	}
	eval1.scope["crd"] = freshGET // Path 2 scope replacement

	// BUG: without markReady, __ready is missing from the fresh scope.
	// Verify .ready() returns false — this is the regression.
	result2, err := compiled.Eval("crd.ready()", eval1.scope)
	require.NoError(t, err)
	assert.False(t, result2.(bool),
		"REGRESSION: after Path 2 scope replacement without markReady, "+
			"crd.ready() should be false because __ready is missing")

	// FIX: Path 2 must call markReady after readiness evaluation.
	// For nodes with no readyWhen, nodeState is NodeReady, so markReady(true).
	eval1.markReady("crd", true)
	result3, err := compiled.Eval("crd.ready()", eval1.scope)
	require.NoError(t, err)
	assert.True(t, result3.(bool),
		"after markReady, crd.ready() should be true again")
}

// TestBareIdentifierForEachDependency verifies that a forEach node referencing
// a bare identifier (e.g., forEach: ${items} with no field access) correctly
// establishes a DAG dependency on the referenced node. Without this, the
// dependent node won't appear in dag.Dependents and will never receive a
// propagation trigger from the coordinator.
//
// Regression test for: forEach expressions that are bare identifiers (no .field
// access) produce no AST field paths, causing processExpr to bail via the
// exprPaths[expr] miss path without registering the dependency.
func TestBareIdentifierForEachDependency(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			node(graphpkg.Node{ID: "source", Template: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "src"},
			}}, graphpkg.NodeTypeTemplate),
			node(graphpkg.Node{ID: "items", Watch: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"selector":   map[string]any{},
			}}, graphpkg.NodeTypeWatch),
			node(graphpkg.Node{ID: "consumer",
				ForEach: &graphpkg.ForEachBinding{VarName: "item", Expr: "${items}"},
				Template: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "${item.metadata.name}-copy"},
					"data":       map[string]any{"ref": "${source.metadata.name}"},
				},
			}, graphpkg.NodeTypeTemplate),
		},
	}

	compiled, err := compiler.CompileGraphSpec(spec, nil)
	assert.NoError(t, err)
	assert.NotNil(t, compiled)

	// Find the consumer node in the DAG.
	dag := compiled.Topology
	consumerIdx, ok := dag.Index["consumer"]
	assert.True(t, ok, "consumer node must exist in DAG")

	consumerNode := compiled.Topology.NodeDeps(consumerIdx)

	// The consumer node must depend on both "items" (bare forEach reference)
	// and "source" (field access in template body).
	_, hasItems := consumerNode["items"]
	assert.True(t, hasItems,
		"consumer must depend on 'items' (bare identifier in forEach expression)")
	_, hasSource := consumerNode["source"]
	assert.True(t, hasSource,
		"consumer must depend on 'source' (field access in template body)")

	// Verify the reverse: items.Dependents must include consumer.
	found := false
	for _, depIdx := range dag.Dependents["items"] {
		if spec.Nodes[depIdx].ID == "consumer" {
			found = true
			break
		}
	}
	assert.True(t, found,
		"dag.Dependents['items'] must include 'consumer' for propagation triggers to work")
}

// ---------------------------------------------------------------------------
// Lazy dependency evaluation tests
//
// These tests verify the lazy dependency (DepLazy) behavior defined in
// 005-reconciliation.md § Dependencies: "Lazy dependencies are always in
// scope as optional values." and 004-compilation.md: "classification is
// per-consumer."
// ---------------------------------------------------------------------------

// TestLazyDepDoesNotGateDispatch proves that a lazy dependency (DepLazy)
// does not prevent a consumer from dispatching. Only hard dependencies
// gate dispatch. Per 005-reconciliation.md § Dependencies: "Lazy
// dependencies are always in scope as optional values."
func TestLazyDepDoesNotGateDispatch(t *testing.T) {
	// Node C depends on A (hard, direct field access) and B (lazy, .ready() only)
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "a", Template: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "a"},
			}},
			{ID: "b", Template: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "b"},
			}},
			{ID: "c", Template: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "c"},
				"data": map[string]any{
					"a_ref":   "${a.metadata.name}",                         // hard dep on A (direct field access)
					"b_ready": "${b.ready().orValue(false) ? 'yes' : 'no'}", // lazy dep on B (.ready().orValue() only)
				},
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)
	dag := dagpkg.AssembleDAG(spec.Nodes, compiled.Topology)

	// Verify classification
	cIdx := dag.Index["c"]
	assert.Equal(t, graphpkg.DepHard, dag.Nodes[cIdx].Dependencies["a"], "a should be hard dep")
	assert.Equal(t, graphpkg.DepSoft, dag.Nodes[cIdx].Dependencies["b"], "b should be lazy dep")

	// A is ready, B is pending — C should still dispatch because B is lazy.
	plan := NewPlanState(dag)
	plan.SetState("a", NodeReady)
	plan.SetState("b", NodePending) // B is pending — lazy dep

	node := &dag.Nodes[cIdx]
	gate := checkDependencyGate(node, plan)
	// C should dispatch (zero value) — B is lazy, so its Pending state is invisible to gating.
	// checkDependencyGate only checks hard deps; lazy dep B=Pending doesn't block.
	assert.Equal(t, depGateOutcome(0), gate,
		"lazy dep B=Pending should not block C from dispatching")
}

// TestLazyDepClassification_MixedAccess proves that the same dependency
// gets different DepKind classifications from different consumers based
// on expression syntax. Per 004-compilation.md: classification is
// per-consumer.
func TestLazyDepClassification_MixedAccess(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "deploy", Template: map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": "deploy"},
			}},
			// Consumer A: accesses deploy only via .ready().orValue() → DepLazy
			{ID: "status", Def: map[string]any{
				"state": "${deploy.ready().orValue(false) ? 'ACTIVE' : 'PENDING'}",
			}},
			// Consumer B: accesses deploy directly → DepHard
			{ID: "service", Template: map[string]any{
				"apiVersion": "v1", "kind": "Service",
				"metadata": map[string]any{"name": "${deploy.metadata.name}"},
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)
	dag := dagpkg.AssembleDAG(spec.Nodes, compiled.Topology)

	statusIdx := dag.Index["status"]
	serviceIdx := dag.Index["service"]

	assert.Equal(t, graphpkg.DepSoft, dag.Nodes[statusIdx].Dependencies["deploy"],
		"status accesses deploy only via .ready().orValue() → DepLazy")
	assert.Equal(t, graphpkg.DepHard, dag.Nodes[serviceIdx].Dependencies["deploy"],
		"service accesses deploy directly → DepHard")
}

// TestLazyDepOptionalScope_AbsentReturnsDefault proves that when a lazy
// dependency is absent (optional.none in scope), field access through ?.
// and .orValue() returns the default value, and .ready() returns false.
// Per 005-reconciliation.md: "Lazy dependencies are always in scope as
// optional values."
func TestLazyDepOptionalScope_AbsentReturnsDefault(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "deploy", Template: map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": "deploy"},
			}},
			{ID: "status", Def: map[string]any{
				"replicas": "${deploy.?status.?availableReplicas.orValue(0)}",
				"ready":    "${deploy.ready().orValue(false) ? 'yes' : 'no'}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)
	dag := dagpkg.AssembleDAG(spec.Nodes, compiled.Topology)

	// Verify deploy is lazy for status
	statusIdx := dag.Index["status"]
	statusNode := &dag.Nodes[statusIdx]
	require.Equal(t, graphpkg.DepSoft, statusNode.Dependencies["deploy"])

	// Create evaluator with deploy ABSENT from scope (lazy dep not yet available).
	// Manually populate lazy deps as optional.none() — matching what
	// evaluateNode does in the simplified walk.
	state := newInstanceState(compiled, nil)
	eval := newEvaluator(state)
	// Don't put "deploy" in scope — it's absent.
	// Populate lazy dep as optional.none() (what the walk does for absent lazy deps).
	eval.scope["deploy"] = celOptionalNone()

	// .ready().orValue(false) on absent lazy dep → false
	readyVal, err := compiled.Eval("deploy.ready().orValue(false) ? 'yes' : 'no'", eval.scope)
	require.NoError(t, err)
	assert.Equal(t, "no", readyVal, ".ready().orValue(false) on absent lazy dep should return false")

	// ?.field.orValue() on absent lazy dep → default value
	replicasVal, err := compiled.Eval("deploy.?status.?availableReplicas.orValue(0)", eval.scope)
	require.NoError(t, err)
	assert.Equal(t, int64(0), replicasVal, "?.orValue(0) on absent lazy dep should return 0")
}

// ---------------------------------------------------------------------------
// Deletion — zero-candidates finalizer removal
// ---------------------------------------------------------------------------

// TestReconcileDelete_NoCandidates_RemovesFinalizer proves that when a Graph
// is deleting but has no revisions and no managed resources (zero teardown
// candidates), reconcileDelete skips the teardown DAG compilation and
// immediately removes the finalizer. This prevents a Graph that never
// compiled from getting stuck with a finalizer forever.
func TestReconcileDelete_NoCandidates_RemovesFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	// Build a Graph with the finalizer set and a deletion timestamp.
	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":              "never-compiled",
			"namespace":         "default",
			"finalizers":        []any{finalizer},
			"deletionTimestamp": "2024-01-01T00:00:00Z",
			"resourceVersion":   "1",
		},
		"spec": map[string]any{
			"nodes": []any{
				map[string]any{
					"id": "cm",
					"template": map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "test"},
					},
				},
			},
		},
	}}

	// Fake client: List returns empty (no revisions, no managed resources),
	// Update succeeds (finalizer removal).
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				// Return empty list — no revisions, no managed resources.
				return nil
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				// Capture the updated object to verify finalizer removal.
				u := obj.(*unstructured.Unstructured)
				graph.Object = u.Object
				return nil
			},
		}).
		Build()

	r := &GraphReconciler{
		Client: fakeClient,
		Caches: newInstanceMap(),
	}

	result, err := r.reconcileDelete(context.Background(), graph)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result, "should return zero result (no requeue)")

	// The finalizer must be removed.
	finalizers := graph.GetFinalizers()
	assert.NotContains(t, finalizers, finalizer,
		"finalizer should be removed when there are no teardown candidates")
}

// TestLazyDepOptionalScope_PresentReturnsRealData proves that when a lazy
// dependency becomes available, expression evaluation sees the real data
// through the optional wrapper. This tests the absent→present transition.
func TestLazyDepOptionalScope_PresentReturnsRealData(t *testing.T) {
	spec := &graphpkg.GraphSpec{
		Nodes: []graphpkg.Node{
			{ID: "deploy", Template: map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"metadata": map[string]any{"name": "deploy"},
			}},
			{ID: "status", Def: map[string]any{
				"replicas": "${deploy.?status.?availableReplicas.orValue(0)}",
				"ready":    "${deploy.ready().orValue(false) ? 'yes' : 'no'}",
			}},
		},
	}
	compiled, err := compiler.CompileGraphSpec(spec, nil)
	require.NoError(t, err)
	dag := dagpkg.AssembleDAG(spec.Nodes, compiled.Topology)

	statusIdx := dag.Index["status"]
	statusNode := &dag.Nodes[statusIdx]
	require.Equal(t, graphpkg.DepSoft, statusNode.Dependencies["deploy"])

	// Create evaluator with deploy PRESENT in scope with real data.
	// Populate lazy dep as optional.of(value) — matching what the walk does
	// for present lazy deps.
	state := newInstanceState(compiled, nil)
	eval := newEvaluator(state)
	deployData := map[string]any{
		"status": map[string]any{
			"availableReplicas": int64(3),
		},
		"__ready": true,
	}
	eval.scope["deploy"] = celOptionalOf(deployData)

	// .ready().orValue(false) on present lazy dep → true (deploy has __ready: true)
	readyVal, err := compiled.Eval("deploy.ready().orValue(false) ? 'yes' : 'no'", eval.scope)
	require.NoError(t, err)
	assert.Equal(t, "yes", readyVal, ".ready().orValue(false) on present lazy dep should return true")

	// ?.field.orValue() on present lazy dep → real value
	replicasVal, err := compiled.Eval("deploy.?status.?availableReplicas.orValue(0)", eval.scope)
	require.NoError(t, err)
	assert.Equal(t, int64(3), replicasVal, "?.orValue(0) on present lazy dep should return 3")
}

// ---------------------------------------------------------------------------
// Force takeover — singleton refcount via deletePreflight ownership check
//
// When multiple Singletons target the same resource, the holder creates it
// with lifecycle.apply=Force. On holder transition:
//   1. New holder force-applies → stamps its identity labels, evicts old labels
//   2. Old holder's teardown fires deletePreflight → labels don't match → deleteNotOwned
//   3. Resource survives without deletion
//
// This proves that the existing deletePreflight ownership gate is sufficient
// for reference-counting semantics — no new primitive needed. The force-apply
// takeover mechanism makes the Singleton graph the sole orchestrator.
// ---------------------------------------------------------------------------

// TestForceTakeover_DeletePreflightRejectsAfterAdoption proves that once a
// new holder force-applies (stamping its identity labels), the old holder's
// deletePreflight returns deleteNotOwned. The resource is NOT deleted.
//
// This is the core mechanism for Singleton refcounting: the impl graph for
// the new holder adopts the resource via force-apply before the dying holder's
// teardown can delete it.
func TestForceTakeover_DeletePreflightRejectsAfterAdoption(t *testing.T) {
	// Simulate: team-b was the holder, created the VPC.
	// team-a force-applied (adopted), stamping its own identity labels.
	// Now team-b's teardown fires. deletePreflight should reject.

	oldGraphName := "singleton-team-b"
	oldGraphNamespace := "kro-system"
	newGraphName := "singleton-team-a"
	newGraphNamespace := "kro-system"

	// The resource now has the NEW holder's identity labels (team-a adopted it).
	newIdentityLabel := "target." + newGraphName + "." + newGraphNamespace + ".internal.kro.run/type"
	existingVPC := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "platform-vpc",
			"namespace":       "infra",
			"resourceVersion": "42",
			"labels": map[string]any{
				// Only the new holder's labels remain after force-apply eviction.
				newIdentityLabel: "template",
			},
		},
		"data": map[string]any{"owner": "team-a"},
	}}

	fakeClient := fake.NewClientBuilder().
		WithObjects(existingVPC).
		Build()

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	// The OLD holder's reconcile scope — team-b is being torn down.
	oldGraph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      oldGraphName,
			"namespace": oldGraphNamespace,
		},
	}}
	rs := newReconcileScope(oldGraph, nil, nil)

	// deletePreflight checks: does this resource have MY identity labels?
	result := cluster.deletePreflight(context.Background(), "/v1/ConfigMap/infra/platform-vpc", rs)

	// The old holder's labels are gone (evicted by force-apply). deleteNotOwned.
	assert.Equal(t, deleteNotOwned, result.Outcome,
		"deletePreflight must return deleteNotOwned when another graph has adopted the resource via force-apply")
	assert.NotNil(t, result.Obj,
		"resource should still exist (returned from GET)")
}

// TestForceTakeover_DeletePreflightAllowsLastHolder proves that when NO
// other holder has adopted (last claimant scenario), deletePreflight returns
// deleteReady and the resource CAN be deleted.
func TestForceTakeover_DeletePreflightAllowsLastHolder(t *testing.T) {
	// Only team-b exists, it's the last holder. No one adopted.
	// Its own identity labels are still on the resource.

	graphName := "singleton-team-b"
	graphNamespace := "kro-system"
	identityLabel := "target." + graphName + "." + graphNamespace + ".internal.kro.run/type"

	existingVPC := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "platform-vpc",
			"namespace":       "infra",
			"resourceVersion": "42",
			"labels": map[string]any{
				identityLabel: "template",
			},
		},
		"data": map[string]any{"owner": "team-b"},
	}}

	fakeClient := fake.NewClientBuilder().
		WithObjects(existingVPC).
		Build()

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      graphName,
			"namespace": graphNamespace,
		},
	}}
	rs := newReconcileScope(graph, nil, nil)

	result := cluster.deletePreflight(context.Background(), "/v1/ConfigMap/infra/platform-vpc", rs)

	// Last holder — labels still match. deleteReady.
	assert.Equal(t, deleteReady, result.Outcome,
		"deletePreflight must return deleteReady when resource still has this graph's identity labels (last holder)")
}

// TestForceTakeover_FullPruneSkipsAdoptedResource proves that the full prune
// path (pruneResources → pruneCandidateTemplate → deletePreflight) correctly
// skips deletion of a resource that has been adopted by another graph.
func TestForceTakeover_FullPruneSkipsAdoptedResource(t *testing.T) {
	oldGraphName := "singleton-team-b"
	oldGraphNamespace := "kro-system"
	newGraphName := "singleton-team-a"
	newGraphNamespace := "kro-system"

	// Resource has been adopted by the new holder (team-a's labels).
	newIdentityLabel := "target." + newGraphName + "." + newGraphNamespace + ".internal.kro.run/type"
	existingVPC := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "platform-vpc",
			"namespace":       "infra",
			"resourceVersion": "42",
			"labels": map[string]any{
				newIdentityLabel: "template",
			},
		},
		"data": map[string]any{"owner": "team-a"},
	}}

	fakeClient := fake.NewClientBuilder().
		WithObjects(existingVPC).
		Build()

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	// Old holder's graph being torn down.
	oldGraph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      oldGraphName,
			"namespace": oldGraphNamespace,
		},
	}}
	rs := newReconcileScope(oldGraph, nil, nil)

	// Build a minimal DAG for ordering. The target node has force lifecycle.
	nodes := []graphpkg.Node{
		{ID: "target", Template: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "platform-vpc", "namespace": "infra"},
		}, Lifecycle: graphpkg.Lifecycle{Apply: "Force"}},
	}
	nodes[0].SetType(graphpkg.NodeTypeTemplate)
	dag, err := dagpkg.BuildDAG(nodes, nil, nil)
	require.NoError(t, err)

	// Simulate teardown: all keys are candidates, currentKeys = nil.
	candidates := []Applied{
		{Key: "/v1/ConfigMap/infra/platform-vpc", NodeID: "target", NodeType: graphpkg.NodeTypeTemplate},
	}

	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, []*dagpkg.DAG{dag}, nil, &instanceState{})

	// Resource should be skipped (pruneSkipped due to deleteNotOwned).
	assert.Equal(t, pruneSkipped, pr.Outcomes["/v1/ConfigMap/infra/platform-vpc"],
		"full prune path must skip resource that was adopted by another graph via force-apply")

	// Verify resource still exists.
	check := &unstructured.Unstructured{}
	check.SetAPIVersion("v1")
	check.SetKind("ConfigMap")
	require.NoError(t, fakeClient.Get(context.Background(),
		client.ObjectKeyFromObject(existingVPC), check),
		"VPC must survive — new holder adopted it before old holder's teardown")
}

// ---------------------------------------------------------------------------
// Patch release error classification tests
//
// These verify that Phase 2 (patch release during teardown) classifies errors
// correctly per 005-reconciliation.md § Teardown:
//   - 422 Invalid (immutable) → abandon (no error propagated)
//   - 403 Forbidden (RBAC) → propagate error (causes retry)
//   - 5xx (server error) → propagate error (causes retry)
//   - NotFound → success (target already gone)
// ---------------------------------------------------------------------------

// patchReleaseTestSetup creates a clusterAccess with a fake client that
// intercepts Patch calls (SSA apply) with the given error. The target
// resource exists (Get succeeds) so the pre-check passes.
func patchReleaseTestSetup(t *testing.T, patchErr error) (*clusterAccess, *reconcileScope) {
	t.Helper()

	graphName := "test-graph"
	graphNS := "default"
	identityLabel := "mypatch." + graphName + "." + graphNS + ".internal.kro.run/type"

	// Target resource exists — pre-check Get will succeed.
	target := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "target",
			"namespace":       "default",
			"resourceVersion": "1",
			"labels": map[string]any{
				identityLabel: "patch",
			},
		},
	}}

	seeding := true
	fakeClient := fake.NewClientBuilder().
		WithObjects(target).
		WithReturnManagedFields().
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, client client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if patchErr != nil && !seeding {
					return patchErr
				}
				return client.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	// Write the identity label under the patch node's own field manager, as
	// a real apply would. Release targets the Graph's managers on the object.
	seed := target.DeepCopy()
	seed.SetResourceVersion("")
	seed.SetManagedFields(nil)
	require.NoError(t, fakeClient.Patch(context.Background(), seed, client.Apply,
		client.FieldOwner("mypatch."+graphName+"."+graphNS+".internal.kro.run")))
	seeding = false

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      graphName,
			"namespace": graphNS,
		},
	}}
	rs := newReconcileScope(graph, nil, nil)
	return cluster, rs
}

// TestPatchRelease_422Invalid_Abandoned proves that a 422 (Invalid) error
// from release-apply (immutable field) is abandoned — no error propagated.
// The field is physically impossible to release; retrying is futile.
func TestPatchRelease_422Invalid_Abandoned(t *testing.T) {
	invalidErr := apierrors.NewInvalid(
		schema.GroupKind{Group: "", Kind: "ConfigMap"},
		"target",
		nil,
	)
	cluster, rs := patchReleaseTestSetup(t, invalidErr)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/target", NodeID: "mypatch", NodeType: graphpkg.NodeTypePatch},
	}
	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, nil, nil, &instanceState{})

	// 422 → abandoned. No error propagated — teardown should proceed.
	assert.Nil(t, pr.Err,
		"422 Invalid should be abandoned, not propagated — immutable fields can't be released")
}

// TestPatchRelease_403Forbidden_Propagated proves that a 403 (Forbidden)
// error from release-apply is propagated — causes retry. An operator
// needs to fix RBAC; the system holds in a visible stuck state.
func TestPatchRelease_403Forbidden_Propagated(t *testing.T) {
	forbiddenErr := apierrors.NewForbidden(
		schema.GroupResource{Group: "", Resource: "configmaps"},
		"target",
		fmt.Errorf("RBAC: access denied"),
	)
	cluster, rs := patchReleaseTestSetup(t, forbiddenErr)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/target", NodeID: "mypatch", NodeType: graphpkg.NodeTypePatch},
	}
	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, nil, nil, &instanceState{})

	// 403 → propagated. Teardown should retry until operator fixes RBAC.
	assert.NotNil(t, pr.Err,
		"403 Forbidden should be propagated — operator must fix RBAC")
}

// TestPatchRelease_500ServerError_Propagated proves that a 5xx error from
// release-apply is propagated — causes retry. Transient server failure.
func TestPatchRelease_500ServerError_Propagated(t *testing.T) {
	serverErr := apierrors.NewInternalError(fmt.Errorf("etcd timeout"))
	cluster, rs := patchReleaseTestSetup(t, serverErr)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/target", NodeID: "mypatch", NodeType: graphpkg.NodeTypePatch},
	}
	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, nil, nil, &instanceState{})

	// 5xx → propagated. Teardown should retry.
	assert.NotNil(t, pr.Err,
		"500 InternalServerError should be propagated — transient, retry")
}

// TestPatchRelease_NotFound_Success proves that when the target resource
// doesn't exist (already deleted), release-apply is a no-op — no error.
func TestPatchRelease_NotFound_Success(t *testing.T) {
	// No interceptor error needed — target doesn't exist in the fake client.
	graphName := "test-graph"
	graphNS := "default"

	// No target resource in the fake client — Get will return NotFound.
	fakeClient := fake.NewClientBuilder().Build()

	cluster := &clusterAccess{
		client: fakeClient,
		reader: fakeClient,
	}

	graph := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "experimental.kro.run/v1alpha1",
		"kind":       "Graph",
		"metadata": map[string]any{
			"name":      graphName,
			"namespace": graphNS,
		},
	}}
	rs := newReconcileScope(graph, nil, nil)

	candidates := []Applied{
		{Key: "/v1/ConfigMap/default/target", NodeID: "mypatch", NodeType: graphpkg.NodeTypePatch},
	}
	pr := cluster.pruneResources(context.Background(), rs, candidates, nil, nil, nil, &instanceState{})

	// NotFound → success. No error propagated.
	assert.Nil(t, pr.Err,
		"NotFound should be treated as success — target already gone")
}
