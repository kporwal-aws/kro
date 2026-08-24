// finalization.go implements the finalization state machine described in
// 005-reconciliation.md § Finalization.
//
// Finalization gates target deletion on successful creation and readiness of
// "finalizer" resources. It runs as a separate phase BEFORE the prune walk's
// deletion decisions. The prune walk receives completedTargets (safe to delete)
// and protectedKeys (must not prune) from finalization.
//
// State is derived from cluster state each cycle (stateless):
//   - GET the finalizer resource. If NotFound → create (Creating).
//   - If exists but not ready → WaitingReady.
//   - If exists and ready → Complete.
//
// No cross-cycle state needed — idempotent by design.
package graphcontroller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dagpkg "github.com/kporwal-aws/kro/experimental/controller/dag"
	graphpkg "github.com/kporwal-aws/kro/experimental/controller/graph"
)

// finalizationResult carries the output of advanceFinalization back to the
// coordinator. The prune walk uses these to make deletion decisions without
// any finalization logic of its own.
type finalizationResult struct {
	// CompletedTargets: keys whose finalization is complete — safe to delete.
	CompletedTargets map[string]bool
	// ProtectedKeys: keys that must not be pruned (active children + static deps).
	ProtectedKeys map[string]bool
	// ChildKeysToCleanup: maps target key → finalizer children to clean up
	// AFTER the target is successfully deleted by the prune walk.
	// Template children are deleted; patch children are release-applied
	// (releasing SSA field ownership so their fields persist on the external
	// resource but kro no longer claims them).
	ChildKeysToCleanup map[string][]finalizationChild
	// BlockedReasons: TeardownBlocked messages for in-progress sequences.
	BlockedReasons []string
	// Notes: informational messages (FinalizerSkipped — target absent).
	Notes []string
	// DeferredTargets: target keys that can't be deleted yet.
	DeferredTargets []string
}

// advanceFinalization processes all prune candidates that have finalizer nodes.
// It advances each target's state machine and produces a result the prune walk
// consumes. Called BEFORE the prune walk's deletion loop.
//
// Stateless: derives phase entirely from cluster state (GET each resource).
func (c *clusterAccess) advanceFinalization(
	ctx context.Context,
	rs *reconcileScope,
	pruneCandidates []string,
	keyToNodeID map[string]string,
	nodeIDToKey map[string]string,
	allDAGs []*dagpkg.DAG,
	eval *evaluator,
	state *instanceState,
) (*finalizationResult, error) {
	result := &finalizationResult{
		CompletedTargets:   map[string]bool{},
		ProtectedKeys:      map[string]bool{},
		ChildKeysToCleanup: map[string][]finalizationChild{},
	}

	// findFinalizers looks up finalizer node IDs for a target across all DAGs.
	findFinalizers := func(nodeID string) (*dagpkg.DAG, []string) {
		for _, d := range allDAGs {
			if fins, ok := d.Finalizers[nodeID]; ok && len(fins) > 0 {
				return d, fins
			}
		}
		return nil, nil
	}

	preseedProtectedKeys(result, pruneCandidates, keyToNodeID, nodeIDToKey, findFinalizers)

	// Process each prune candidate that has finalizers.
	for _, key := range pruneCandidates {
		nodeID := keyToNodeID[key]
		if nodeID == "" {
			continue
		}
		finDAG, finalizerNodeIDs := findFinalizers(nodeID)
		if finDAG == nil {
			continue // no finalizers — prune walk handles directly
		}

		if err := c.advanceTarget(ctx, rs, key, nodeID, finalizerNodeIDs, finDAG, eval, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// preseedProtectedKeys seeds the protected keys set from static dependencies
// of finalizer nodes.
func preseedProtectedKeys(
	result *finalizationResult,
	pruneCandidates []string,
	keyToNodeID map[string]string,
	nodeIDToKey map[string]string,
	findFinalizers func(string) (*dagpkg.DAG, []string),
) {
	// Protect static dependencies of finalizer nodes.
	for _, key := range pruneCandidates {
		nodeID := keyToNodeID[key]
		finDAG, finalizerNodeIDs := findFinalizers(nodeID)
		if finDAG == nil {
			continue
		}
		for _, finNodeID := range finalizerNodeIDs {
			finIdx, exists := finDAG.Index[finNodeID]
			if !exists {
				continue
			}
			for depID := range finDAG.Nodes[finIdx].Dependencies {
				if depID == nodeID {
					continue
				}
				if dk, ok := nodeIDToKey[depID]; ok {
					result.ProtectedKeys[dk] = true
				}
			}
			if dk, ok := nodeIDToKey[finNodeID]; ok {
				result.ProtectedKeys[dk] = true
			}
		}
	}
}

// advanceTarget processes a single target's finalization state machine.
// Stateless: GETs the target, handles NotFound vs exists, runs finalization,
// and records the resulting phase on result.
func (c *clusterAccess) advanceTarget(
	ctx context.Context,
	rs *reconcileScope,
	key string,
	nodeID string,
	finalizerNodeIDs []string,
	finDAG *dagpkg.DAG,
	eval *evaluator,
	result *finalizationResult,
) error {
	logger := log.FromContext(ctx)

	// GET the target to verify it exists.
	obj, nn, ok := unstructuredFromKey(key)
	if !ok {
		return nil
	}
	if getErr := c.reader.Get(ctx, nn, obj); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			// Target already gone — finalization complete.
			logger.Info("finalization skipped: target resource does not exist",
				"key", key, "finalizers", finalizerNodeIDs)
			result.Notes = append(result.Notes, fmt.Sprintf("FinalizerSkipped: %s (target absent)", key))
			result.CompletedTargets[key] = true
			return nil
		}
		return fmt.Errorf("checking finalization target %s: %w", key, getErr)
	}

	// Run the finalization sequence.
	ready, children, finErr := c.runFinalization(ctx, rs, obj, nodeID, finalizerNodeIDs, finDAG, eval)

	// Protect all child keys from pruning.
	for _, child := range children {
		result.ProtectedKeys[child.Key] = true
	}

	if finErr != nil {
		logger.Error(finErr, "finalization failed", "key", key)
		result.BlockedReasons = append(result.BlockedReasons, fmt.Sprintf(
			"TeardownBlocked: %s (finalizer creation failed: %s)", key, finErr))
		result.DeferredTargets = append(result.DeferredTargets, key)
		return nil
	}

	if !ready {
		logger.Info("finalization in progress — deletion deferred",
			"key", key, "finalizers", finalizerNodeIDs)
		result.BlockedReasons = append(result.BlockedReasons, fmt.Sprintf(
			"TeardownBlocked: %s (finalizer not ready: %s)",
			key, strings.Join(finalizerNodeIDs, ", ")))
		result.DeferredTargets = append(result.DeferredTargets, key)
		return nil
	}

	// Finalization complete — target can be deleted.
	// All children go into cleanup: template children are deleted, patch
	// children are release-applied (releasing SSA field ownership).
	logger.Info("finalization complete", "key", key)
	result.CompletedTargets[key] = true
	if len(children) > 0 {
		result.ChildKeysToCleanup[key] = children
	}
	return nil
}

// ---------------------------------------------------------------------------
// Finalization execution (stateless — re-derives position from cluster state)
// ---------------------------------------------------------------------------

// ensureFinalizerResource implements the shared GET → create-if-absent →
// evaluate-readyWhen pattern used by both single-resource and forEach
// finalization. Returns (ready, key, error).
//
// When the resource already exists on the cluster, scope is updated to
// the actual cluster object BEFORE evaluating readyWhen. This is
// critical: readyWhen expressions (e.g., `${finalizerJob.status.succeeded
// > 0}`) must evaluate against real status fields, not the template
// output which lacks controller-set fields.
func (c *clusterAccess) ensureFinalizerResource(
	ctx context.Context,
	rs *reconcileScope,
	eval *evaluator,
	node *graphpkg.Node,
	evalMap map[string]any,
) (bool, string, error) {
	logger := log.FromContext(ctx)

	obj := &unstructured.Unstructured{Object: evalMap}
	if obj.GetNamespace() == "" {
		obj.SetNamespace(rs.namespace)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())
	err := c.reader.Get(ctx, client.ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}, existing)

	if err != nil {
		if !apierrors.IsNotFound(err) {
			return false, "", fmt.Errorf("checking finalizer resource %s: %w", node.ID, err)
		}
		// Create finalizer resource.
		logger.Info("creating finalizer resource", "finalizer", node.ID,
			"name", obj.GetName())
		applied, applyErr := c.applySSA(ctx, rs, evalMap, node.ID, node.Type(), eval.effectiveGeneration, false)
		if applyErr != nil {
			return false, "", fmt.Errorf("creating finalizer resource %s: %w", node.ID, applyErr)
		}
		return false, resourceKey(applied), nil
	}

	// Exists — for patch nodes, re-apply SSA every cycle since the patch
	// IS the finalization action (setting fields on an external resource).
	// Force-apply is used because the resource is external and field conflicts
	// with other managers are expected and intentional.
	// For template nodes, the resource was already created above; no re-apply needed.
	if node.Type() == graphpkg.NodeTypePatch {
		logger.Info("applying finalizer patch", "finalizer", node.ID,
			"name", obj.GetName())
		applied, applyErr := c.applySSA(ctx, rs, evalMap, node.ID, node.Type(), eval.effectiveGeneration, true)
		if applyErr != nil {
			return false, "", fmt.Errorf("applying finalizer patch %s: %w", node.ID, applyErr)
		}
		eval.scope[node.ID] = graphpkg.NormalizeTypes(applied.Object)
	} else {
		// Use cluster state so readyWhen evaluates against real status fields,
		// not the template output.
		eval.scope[node.ID] = graphpkg.NormalizeTypes(existing.Object)
	}

	key := resourceKey(existing)
	if len(node.ReadyWhen) > 0 {
		if err := eval.evalReadinessConditions(node.ReadyWhen, node.ID); err != nil {
			logger.V(1).Info("finalizer not ready", "finalizer", node.ID, "name", existing.GetName())
			return false, key, nil
		}
	}
	logger.V(1).Info("finalizer ready", "finalizer", node.ID, "name", existing.GetName())
	return true, key, nil
}

// finalizationChild pairs a resource key with the node type that produced it.
// Used to dispatch cleanup correctly: template → delete, patch → skip (persist).
type finalizationChild struct {
	Key      string
	NodeType graphpkg.NodeType
}

// runFinalization executes the finalization sequence for a single target.
// Returns (true, children, nil) when all finalizer resources are ready.
// Returns (false, children, nil) when finalizers are in progress.
// Returns (false, children, err) when a finalizer can't be created.
func (c *clusterAccess) runFinalization(
	ctx context.Context,
	rs *reconcileScope,
	target *unstructured.Unstructured,
	targetNodeID string,
	finalizerNodeIDs []string,
	dag *dagpkg.DAG,
	eval *evaluator,
) (bool, []finalizationChild, error) {
	var children []finalizationChild

	// Put the target in scope so finalizer templates can reference it.
	if targetNodeID != "" {
		eval.scope[targetNodeID] = graphpkg.NormalizeTypes(target.Object)
	}

	// Sort finalizer nodes by topological position.
	ordered := sortFinalizerNodes(finalizerNodeIDs, dag)

	allReady := true
	for _, finNodeID := range ordered {
		idx, ok := dag.Index[finNodeID]
		if !ok {
			return false, children, fmt.Errorf("finalizer node %q not found in DAG", finNodeID)
		}
		finNode := &dag.Nodes[idx]

		// forEach + finalizes: expand collection.
		if finNode.ForEach != nil {
			ready, fChildren, err := c.runForEachFinalization(ctx, rs, finNode, dag, eval)
			children = append(children, fChildren...)
			if err != nil {
				return false, children, err
			}
			if !ready {
				allReady = false
			}
			continue
		}

		// Single-resource finalizer.
		evalMap, err := eval.toMapNode(*finNode)
		if err != nil {
			return false, children, fmt.Errorf("evaluating finalizer template %s: %w", finNodeID, err)
		}

		// Seed scope with template output so downstream finalizer templates
		// can reference this node. ensureFinalizerResource will overwrite
		// scope with the actual cluster object when the resource exists.
		eval.scope[finNodeID] = graphpkg.NormalizeTypes((&unstructured.Unstructured{Object: evalMap}).Object)
		ready, key, ensureErr := c.ensureFinalizerResource(ctx, rs, eval, finNode, evalMap)
		if ensureErr != nil {
			return false, children, ensureErr
		}
		children = append(children, finalizationChild{Key: key, NodeType: finNode.Type()})
		if !ready {
			allReady = false
		}
	}

	return allReady, children, nil
}

// runForEachFinalization handles the forEach + finalizes case.
func (c *clusterAccess) runForEachFinalization(
	ctx context.Context,
	rs *reconcileScope,
	finNode *graphpkg.Node,
	dag *dagpkg.DAG,
	eval *evaluator,
) (bool, []finalizationChild, error) {
	logger := log.FromContext(ctx)
	var children []finalizationChild
	allReady := true

	varName := finNode.ForEach.VarName
	collectionExpr := finNode.ForEach.Expr

	collection, err := eval.evalString(collectionExpr)
	if err != nil {
		return false, children, fmt.Errorf("forEach finalizer %s: evaluating collection %q: %w", finNode.ID, collectionExpr, err)
	}

	items, ok := collection.([]any)
	if !ok {
		items = []any{collection}
	}
	logger.Info("forEach finalization expanding", "finalizer", finNode.ID, "var", varName, "count", len(items))

	for _, item := range items {
		innerScope := graphpkg.CopyScope(eval.scope)
		innerScope[varName] = item
		innerEval := eval.withScope(innerScope)

		evalMap, err := innerEval.toMapNode(*finNode)
		if err != nil {
			return false, children, fmt.Errorf("forEach finalizer %s item: %w", finNode.ID, err)
		}

		childObj := &unstructured.Unstructured{Object: evalMap}
		if childObj.GetNamespace() == "" {
			childObj.SetNamespace(rs.namespace)
		}
		graphpkg.StampForEachChildLabels(childObj, finNode.ID, rs.name, rs.namespace, eval.effectiveGeneration, finNode.Type())
		evalMap = childObj.Object

		innerEval.scope[finNode.ID] = graphpkg.NormalizeTypes(childObj.Object)
		ready, key, ensureErr := c.ensureFinalizerResource(ctx, rs, innerEval, finNode, evalMap)
		if ensureErr != nil {
			return false, children, ensureErr
		}
		children = append(children, finalizationChild{Key: key, NodeType: finNode.Type()})
		if !ready {
			allReady = false
		}
	}

	return allReady, children, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// sortFinalizerNodes returns finalizer node IDs sorted by topological position.
func sortFinalizerNodes(finalizerNodeIDs []string, dag *dagpkg.DAG) []string {
	ordered := make([]string, len(finalizerNodeIDs))
	copy(ordered, finalizerNodeIDs)
	topoPos := make(map[string]int, len(dag.TopologicalOrder))
	for pos, nodeIdx := range dag.TopologicalOrder {
		topoPos[dag.Nodes[nodeIdx].ID] = pos
	}
	sort.Slice(ordered, func(i, j int) bool {
		return topoPos[ordered[i]] < topoPos[ordered[j]]
	})
	return ordered
}
