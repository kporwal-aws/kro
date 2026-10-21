// prune.go computes and executes the prune walk — removing resources no longer
// in the applied set. Prune candidates are the diff between previous and current
// applied keys. Deletion follows reverse topological order from the DAG.
//
// pruneResources is the single code path for both:
//   - Normal prune: candidates = keys in previous but not current
//   - Teardown: candidates = all managed keys, currentKeys = empty
//
// The prune walk handles: template deletion, patch field release, finalization
// sequencing, and dynamic resource discovery for forEach/CEL-named resources.
package graphcontroller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dagpkg "github.com/kporwal-aws/kro/experimental/controller/dag"
	graphpkg "github.com/kporwal-aws/kro/experimental/controller/graph"
)

// pruneOutcome describes what happened to a single prune candidate.
// Only pruneDeferred is significant — it is consumed by collectDeferredKeys
// to determine retry candidates. pruneDeleted is recorded for logging but
// not consumed by any downstream logic.
type pruneOutcome int

const (
	pruneNone     pruneOutcome = iota // no outcome recorded; used for results already tracked elsewhere (e.g., finalization phase)
	pruneDeleted                      // template resource deleted
	pruneSkipped                      // no action needed (not found, not owned, parse failed)
	pruneDeferred                     // finalization not ready
)

// pruneResult carries the output of pruneResources to the caller.
type pruneResult struct {
	Outcomes       map[string]pruneOutcome // key → what happened
	BlockedReasons []string                // TeardownBlocked messages
	Notes          []string                // informational (FinalizerSkipped)
	Err            error                   // hard API error
}

// pruneResources handles both normal prune (partial candidates) and teardown
// (all keys are candidates). Returns a pruneResult with outcomes, blocked
// reasons, notes, and errors.
func (c *clusterAccess) pruneResources(
	ctx context.Context,
	rs *reconcileScope,
	candidates []Applied,
	currentKeys map[string]Applied,
	dags []*dagpkg.DAG,
	eval *evaluator,
	state *instanceState,
) *pruneResult {
	result := &pruneResult{
		Outcomes: map[string]pruneOutcome{},
	}

	if len(candidates) == 0 {
		return result
	}

	logger := log.FromContext(ctx)

	// Build resource-key-to-node-ID maps from all DAGs.
	keyToNodeID := map[string]string{}
	nodeIDToKey := map[string]string{}
	for _, d := range dags {
		for _, node := range d.Nodes {
			if node.Identity() != nil {
				if rk := staticResourceKey(node.Identity(), rs.namespace, c.scope); rk != "" {
					keyToNodeID[rk] = node.ID
					nodeIDToKey[node.ID] = rk
				}
			}
		}
	}

	// Backfill keyToNodeID from runtime labels for CEL-named nodes that
	// staticResourceKey cannot resolve. The identity label on managed
	// resources encodes the node ID, bridging runtime keys to DAG nodes
	// even when the metadata.name contains CEL expressions.
	for _, candidate := range candidates {
		if candidate.NodeID != "" {
			if _, exists := keyToNodeID[candidate.Key]; !exists {
				keyToNodeID[candidate.Key] = candidate.NodeID
				nodeIDToKey[candidate.NodeID] = candidate.Key
			}
		}
	}

	// Phase 1: Advance finalization state machines for all candidates that
	// have finalizer nodes. This produces completedTargets (safe to delete),
	// protectedKeys (must not prune), and child cleanup info.

	// Build candidate key slice for finalization (plain resource keys).
	candidateKeys := make([]string, len(candidates))
	for i, c := range candidates {
		candidateKeys[i] = c.Key
	}

	finResult, finErr := c.advanceFinalization(ctx, rs, candidateKeys, keyToNodeID, nodeIDToKey, dags, eval, state)
	if finErr != nil {
		result.Err = finErr
		return result
	}
	result.BlockedReasons = append(result.BlockedReasons, finResult.BlockedReasons...)
	result.Notes = append(result.Notes, finResult.Notes...)
	for _, dt := range finResult.DeferredTargets {
		result.Outcomes[dt] = pruneDeferred
	}

	// Phase 2: Order candidates for deletion (reverse topological).
	ordered := pruneOrderApplied(candidates, dags, rs.namespace, c.scope)

	opts := pruneOpts{
		ProtectedKeys:      finResult.ProtectedKeys,
		CompletedTargets:   finResult.CompletedTargets,
		ChildKeysToCleanup: finResult.ChildKeysToCleanup,
		KeyToNodeID:        keyToNodeID,
		DAGs:               dags,
	}

	// Build the set of candidate keys for dependency gating. A node's
	// dependencies (nodes it depends on) should only be deleted after the
	// node itself is confirmed gone. This prevents removing a provider
	// (e.g., IAMRoleSelector) while dependents (e.g., VPC) still need it.
	candidateKeySet := make(map[string]bool, len(ordered))
	for _, a := range ordered {
		if a.Key != "" {
			candidateKeySet[a.Key] = true
		}
	}

	// confirmedGone tracks keys confirmed absent from the API server during
	// this reconcile pass (deletePreflight returned NotFound or NotOwned).
	// A node's dependencies are only eligible for deletion once all the
	// node's dependents in the candidate set are confirmed gone.
	confirmedGone := make(map[string]bool)

	// Build a reverse lookup: for each candidate's node ID, collect the
	// resource keys of its forward-dependents (nodes that depend on it).
	// Before deleting a candidate, all its forward-dependents that are also
	// candidates must be confirmed gone.
	dependentKeys := buildDependentKeysMap(dags, keyToNodeID, nodeIDToKey)

	// deferredDeletes collects finalizer children whose targets were
	// successfully deleted in this walk. Processed after the walk completes.
	var deferredDeletes []finalizationChild

	for _, candidate := range ordered {
		if candidate.Key == "" {
			continue
		}
		// Skip keys that are in the current applied set.
		if _, inCurrent := currentKeys[candidate.Key]; inCurrent {
			continue
		}

		// Dependency gate: don't delete a node until all nodes that depend
		// on it (its forward-dependents) are confirmed gone. This enforces
		// reverse-order teardown across reconcile cycles — a dependency is
		// only removed after its dependents are fully deleted.
		// Protected keys (finalization children) are excluded — finalization
		// handles their lifecycle separately and they should not gate their
		// target's deletion.
		nodeID := keyToNodeID[candidate.Key]
		if nodeID != "" && hasPendingDependents(nodeID, dependentKeys, candidateKeySet, currentKeys, confirmedGone, opts.ProtectedKeys) {
			logger.V(1).Info("prune deferred: waiting for dependents to be deleted",
				"key", candidate.Key, "nodeID", nodeID)
			result.Outcomes[candidate.Key] = pruneDeferred
			continue
		}

		cr := c.pruneCandidate(ctx, rs, candidate, opts)
		if cr.Outcome != pruneNone {
			result.Outcomes[candidate.Key] = cr.Outcome
		}
		// Track confirmed gone from preflight results: pruneSkipped means
		// the resource doesn't exist or isn't ours (safe to ungate deps).
		// Patch nodes are also confirmed gone after release — the node's
		// contribution is removed, so dependencies are no longer coupled.
		if cr.Outcome == pruneSkipped {
			confirmedGone[candidate.Key] = true
		} else if candidate.NodeType == graphpkg.NodeTypePatch && cr.Err == nil && cr.Outcome == pruneNone {
			confirmedGone[candidate.Key] = true
		}
		if len(cr.BlockedReasons) > 0 {
			result.BlockedReasons = append(result.BlockedReasons, cr.BlockedReasons...)
		}
		if len(cr.Notes) > 0 {
			result.Notes = append(result.Notes, cr.Notes...)
		}
		if len(cr.ChildrenToCleanup) > 0 {
			deferredDeletes = append(deferredDeletes, cr.ChildrenToCleanup...)
		}
		if cr.Err != nil {
			result.Err = cr.Err
			return result
		}
	}

	// Clean up finalization children whose targets were successfully deleted.
	// Sort in reverse topological order so dependents are deleted before their
	// dependencies (design: 005-reconciliation.md § Finalization).
	if len(deferredDeletes) > 1 {
		keyPosition, maxPos := buildKeyPositionMap(dags, rs.namespace, c.scope)
		sort.Slice(deferredDeletes, func(i, j int) bool {
			pi, oki := keyPosition[deferredDeletes[i].Key]
			if !oki {
				pi = maxPos + 1
			}
			pj, okj := keyPosition[deferredDeletes[j].Key]
			if !okj {
				pj = maxPos + 1
			}
			return pi > pj
		})
	}
	c.cleanupFinalizationChildren(ctx, deferredDeletes, rs)

	return result
}

// ---------------------------------------------------------------------------
// Per-candidate prune helper
// ---------------------------------------------------------------------------

// pruneOpts carries read-only context from finalization into pruneCandidate.
type pruneOpts struct {
	ProtectedKeys      map[string]bool                // keys to skip (active finalization children)
	CompletedTargets   map[string]bool                // targets whose finalization is done
	ChildKeysToCleanup map[string][]finalizationChild // target key → children to clean up after target
	KeyToNodeID        map[string]string              // resource key → DAG node ID
	DAGs               []*dagpkg.DAG                  // all DAGs for finalizer lookup
}

// pruneCandidateResult carries the outcome of processing a single candidate.
type pruneCandidateResult struct {
	Outcome           pruneOutcome // 0 means no outcome recorded (patch release, skipped internally)
	BlockedReasons    []string
	Notes             []string
	ChildrenToCleanup []finalizationChild // finalization children to clean up
	Err               error               // hard API error — caller should abort
}

// pruneCandidate processes a single prune candidate: dispatches by node type
// (patch release, template delete, finalization-child cleanup).
func (c *clusterAccess) pruneCandidate(
	ctx context.Context,
	rs *reconcileScope,
	candidate Applied,
	opts pruneOpts,
) pruneCandidateResult {
	logger := log.FromContext(ctx)
	key := candidate.Key

	// Defer deletion of resources protected by in-flight finalization.
	if opts.ProtectedKeys[key] {
		logger.Info("prune deferred: resource is protected by in-flight finalization",
			"key", key)
		return pruneCandidateResult{Outcome: pruneDeferred}
	}

	// Dispatch based on NodeType.
	switch candidate.NodeType {
	case graphpkg.NodeTypePatch:
		// Patch keys use release apply (release fields), not delete. No
		// current node writes this object, so every field manager of this
		// Graph on it is released — the pruned node's, any sibling's, and
		// the legacy per-Graph manager's.
		gvk, nn := parseResourceKey(key)
		if gvk.Kind == "" {
			return pruneCandidateResult{}
		}
		if err := c.releaseGraphManagers(ctx, gvk, nn.Namespace, nn.Name, rs.name, rs.namespace); err != nil {
			// Classify release errors per 005-reconciliation.md § Teardown:
			// - 422 Invalid (immutable field) → abandon, will never succeed
			// - Everything else (403, 409, 5xx, network) → propagate for retry
			if apierrors.IsInvalid(err) {
				logger.Info("release abandoned: field is immutable", "key", key, "error", err)
				return pruneCandidateResult{}
			}
			return pruneCandidateResult{Err: fmt.Errorf("releasing patch fields for %s: %w", key, err)}
		}
		logger.Info("released contribution fields", "key", key)
		return pruneCandidateResult{}

	default:
		return c.pruneCandidateTemplate(ctx, rs, key, opts)
	}
}

// pruneCandidateTemplate handles the template-type candidate path: preflight
// check, finalization gating, and deletion.
func (c *clusterAccess) pruneCandidateTemplate(
	ctx context.Context,
	rs *reconcileScope,
	key string,
	opts pruneOpts,
) pruneCandidateResult {
	logger := log.FromContext(ctx)

	pf := c.deletePreflight(ctx, key, rs)
	switch pf.Outcome {
	case deleteSkipParseFailed:
		return pruneCandidateResult{Outcome: pruneSkipped}
	case deleteNotFound:
		return c.pruneCandidateNotFound(ctx, key, opts)
	case deleteNotOwned:
		return pruneCandidateResult{Outcome: pruneSkipped}
	}

	// deleteReady: resource exists and is ours.
	// Finalization check: if this target has finalizers, only delete if
	// advanceFinalization marked it complete.
	nodeID := opts.KeyToNodeID[key]
	if hasFinalizers(nodeID, opts.DAGs) && !opts.CompletedTargets[key] {
		return pruneCandidateResult{} // already tracked as pruneDeferred in Phase 1
	}

	if err := c.client.Delete(ctx, pf.Obj); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return pruneCandidateResult{Err: fmt.Errorf("pruning %s: %w", key, err)}
		}
	} else {
		logger.Info("pruned resource", "key", key)
		var children []finalizationChild
		if ck, ok := opts.ChildKeysToCleanup[key]; ok {
			children = ck
		}
		return pruneCandidateResult{Outcome: pruneDeleted, ChildrenToCleanup: children}
	}
	return pruneCandidateResult{}
}

// pruneCandidateNotFound handles the case where the target resource doesn't
// exist during preflight — cleans up finalization children and emits notes.
func (c *clusterAccess) pruneCandidateNotFound(
	ctx context.Context,
	key string,
	opts pruneOpts,
) pruneCandidateResult {
	logger := log.FromContext(ctx)
	var cr pruneCandidateResult
	cr.Outcome = pruneSkipped

	// Target absent — clean up finalization children if mid-finalization.
	if childKeys, ok := opts.ChildKeysToCleanup[key]; ok {
		cr.ChildrenToCleanup = childKeys
	}

	// Emit FinalizerSkipped note if this target had finalizers.
	nodeID := opts.KeyToNodeID[key]
	if nodeID != "" && hasFinalizers(nodeID, opts.DAGs) {
		logger.Info("finalization skipped: target resource does not exist", "key", key)
		cr.Notes = append(cr.Notes,
			fmt.Sprintf("FinalizerSkipped: %s (target absent)", key))
	}
	return cr
}

// hasFinalizers reports whether any DAG declares finalizer nodes for the given
// node ID.
func hasFinalizers(nodeID string, dags []*dagpkg.DAG) bool {
	for _, d := range dags {
		if fins, ok := d.Finalizers[nodeID]; ok && len(fins) > 0 {
			return true
		}
	}
	return false
}

// collectPruneCandidates returns Applied entries from prev whose Key is not in curr.
func collectPruneCandidates(prev map[string]Applied, curr map[string]Applied) []Applied {
	var candidates []Applied
	for key, a := range prev {
		if _, ok := curr[key]; !ok {
			candidates = append(candidates, a)
		}
	}
	return candidates
}

// collectDeferredKeys extracts Applied entries from allPrevious whose outcome
// is pruneDeferred (need to be retried next reconcile).
func collectDeferredKeys(outcomes map[string]pruneOutcome, allPrevious map[string]Applied) []Applied {
	var deferred []Applied
	for key, outcome := range outcomes {
		if outcome == pruneDeferred {
			if a, ok := allPrevious[key]; ok {
				deferred = append(deferred, a)
			}
		}
	}
	return deferred
}

// findManagedResourceKeys discovers dynamically-named resources (forEach, CEL
// names) by listing resources with our graph-name label. Returns Applied entries.
//
// The GVK list is derived from the revision specs — every resource template
// declares its apiVersion and kind, so we know exactly which types to scan.
// This avoids a hardcoded GVK list that would silently miss new resource types.
func (c *clusterAccess) findManagedResourceKeys(ctx context.Context, rs *reconcileScope) ([]Applied, error) {
	// Collect unique GVKs from all revisions for this Graph.
	revisions, err := listRevisions(ctx, c.client, rs.name, rs.namespace)
	if err != nil {
		return nil, err
	}

	gvkSet := map[schema.GroupVersionKind]bool{}
	for _, rev := range revisions {
		spec, err := extractRevisionSpec(rev)
		if err != nil {
			continue
		}
		for _, node := range spec.Nodes {
			if node.Identity() == nil {
				continue
			}
			nodeType := node.Type()
			if nodeType == graphpkg.NodeTypeRef || nodeType == graphpkg.NodeTypeWatch {
				continue // read-only references don't create resources
			}
			id := node.Identity()
			gvk := graphpkg.GVKFromMap(id)
			if gvk.Kind == "" {
				continue
			}
			gvkSet[gvk] = true
		}
	}

	var keys []Applied
	suffix := graphpkg.GraphLabelSuffix(rs.name, rs.namespace)
	for gvk := range gvkSet {
		list := &unstructured.UnstructuredList{}
		listGVK := gvk
		listGVK.Kind = gvk.Kind + "List"
		list.SetGroupVersionKind(listGVK)

		// Cannot use label selector with DNS subdomain keys — list all and
		// filter client-side. This only runs during teardown (not hot path).
		if err := c.reader.List(ctx, list, &client.ListOptions{
			Namespace: rs.namespace,
		}); err != nil {
			continue // skip GVKs we can't list (e.g., CRD deleted)
		}

		for _, item := range list.Items {
			itemLabels := item.GetLabels()
			var found Applied
			for labelKey, labelValue := range itemLabels {
				if strings.HasSuffix(labelKey, suffix) {
					rk := resourceKey(&item)
					nodeType := graphpkg.NodeTypeTemplate // default
					if nt, ok := graphpkg.NodeTypeFromLabelValue(labelValue); ok {
						nodeType = nt
					}
					nodeID, _ := graphpkg.ParseNodeIDFromLabel(labelKey)
					found = Applied{
						Key:       rk,
						NodeID:    nodeID,
						NodeType:  nodeType,
						HasStatus: false, // unknown from labels
					}
					// A template label on the same object takes precedence
					// over patch labels from sibling nodes: the template is
					// the lifecycle owner.
					if nodeType == graphpkg.NodeTypeTemplate {
						break
					}
				}
			}
			if found.Key != "" {
				keys = append(keys, found)
			}
		}
	}

	return keys, nil
}

// buildKeyPositionMap builds a map from resource key → topological position
// across all DAGs. Uses the highest position found across all DAGs for each
// key. Returns the position map and the maximum position observed.
func buildKeyPositionMap(dags []*dagpkg.DAG, defaultNS string, scope *scopeResolver) (map[string]int, int) {
	keyPosition := map[string]int{}
	maxPosition := 0

	for _, d := range dags {
		topoPosition := make(map[int]int, len(d.TopologicalOrder))
		for pos, nodeIdx := range d.TopologicalOrder {
			topoPosition[nodeIdx] = pos
		}

		for i, node := range d.Nodes {
			if node.Identity() == nil {
				continue
			}
			rk := staticResourceKey(node.Identity(), defaultNS, scope)
			if rk == "" {
				continue
			}
			pos := topoPosition[i]
			if pos > maxPosition {
				maxPosition = pos
			}
			if existing, ok := keyPosition[rk]; !ok || pos > existing {
				keyPosition[rk] = pos
			}
		}
	}

	return keyPosition, maxPosition
}

// pruneOrderApplied sorts Applied entries in reverse dependency order using
// all available DAGs (active + superseded). Dependents are deleted before
// their dependencies. Keys that don't match any DAG node are placed first
// (highest position — deleted first).
func pruneOrderApplied(candidates []Applied, dags []*dagpkg.DAG, defaultNS string, scope *scopeResolver) []Applied {
	keyPosition, maxPosition := buildKeyPositionMap(dags, defaultNS, scope)

	// Build a node-ID-to-position map for fallback resolution of CEL-named
	// nodes whose keys can't be determined statically. The identity label
	// on managed resources carries the node ID, allowing position lookup
	// even when staticResourceKey returns "".
	nodeIDPosition := buildNodeIDPositionMap(dags)

	type scored struct {
		applied  Applied
		position int
	}
	scoredKeys := make([]scored, 0, len(candidates))
	for _, a := range candidates {
		pos, ok := keyPosition[a.Key]
		if !ok && a.NodeID != "" {
			// Fallback: use node ID from identity label to resolve position.
			pos, ok = nodeIDPosition[a.NodeID]
		}
		if !ok {
			pos = maxPosition + 1
		}
		scoredKeys = append(scoredKeys, scored{applied: a, position: pos})
	}

	sort.Slice(scoredKeys, func(i, j int) bool {
		return scoredKeys[i].position > scoredKeys[j].position
	})

	result := make([]Applied, len(scoredKeys))
	for i, s := range scoredKeys {
		result[i] = s.applied
	}
	return result
}

// buildNodeIDPositionMap builds a map from node ID → topological position
// across all DAGs. Used as a fallback for CEL-named nodes whose resource
// keys can't be resolved statically by buildKeyPositionMap.
func buildNodeIDPositionMap(dags []*dagpkg.DAG) map[string]int {
	nodeIDPosition := map[string]int{}
	for _, d := range dags {
		for pos, nodeIdx := range d.TopologicalOrder {
			nodeID := d.Nodes[nodeIdx].ID
			if existing, ok := nodeIDPosition[nodeID]; !ok || pos > existing {
				nodeIDPosition[nodeID] = pos
			}
		}
	}
	return nodeIDPosition
}

// buildDependentKeysMap builds a map from node ID to the set of resource keys
// of nodes that depend on it (forward-dependents). Used by the dependency gate
// to ensure a node is not deleted until all its dependents are confirmed gone.
//
// The map includes both hard and soft dependents — soft dependencies (e.g.,
// propagateWhen references via .ready()) still represent a runtime coupling
// where the dependent needs the dependency to exist during teardown.
func buildDependentKeysMap(dags []*dagpkg.DAG, keyToNodeID map[string]string, nodeIDToKey map[string]string) map[string][]string {
	dependentKeys := map[string][]string{}

	for _, d := range dags {
		for nodeID, depIndices := range d.Dependents {
			for _, depIdx := range depIndices {
				if depIdx < 0 || depIdx >= len(d.Nodes) {
					continue
				}
				depNodeID := d.Nodes[depIdx].ID
				if depKey, ok := nodeIDToKey[depNodeID]; ok {
					dependentKeys[nodeID] = append(dependentKeys[nodeID], depKey)
				}
			}
		}
	}

	return dependentKeys
}

// hasPendingDependents reports whether any forward-dependent of the given node
// is still pending deletion (in the candidate set, not in currentKeys, and not
// yet confirmed gone). This gates deletion: a dependency should not be removed
// until all its dependents have been confirmed absent from the cluster.
// Protected keys (finalization children managed by the finalization state
// machine) are excluded — they should not gate their target's deletion.
func hasPendingDependents(nodeID string, dependentKeys map[string][]string, candidateKeySet map[string]bool, currentKeys map[string]Applied, confirmedGone map[string]bool, protectedKeys map[string]bool) bool {
	depKeys, ok := dependentKeys[nodeID]
	if !ok {
		return false
	}
	for _, dk := range depKeys {
		// Skip dependents managed by finalization (they shouldn't gate).
		if protectedKeys[dk] {
			continue
		}
		// Skip dependents that are in the current applied set (not candidates).
		if currentKeys != nil {
			if _, inCurrent := currentKeys[dk]; inCurrent {
				continue
			}
		}
		// Only check dependents that are actually candidates for deletion.
		if !candidateKeySet[dk] {
			continue
		}
		// If the dependent is a candidate but not confirmed gone, gate.
		if !confirmedGone[dk] {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Delete preflight
// ---------------------------------------------------------------------------

// deletePreflightOutcome describes the result of the preflight check for a
// single resource key.
type deletePreflightOutcome int

const (
	// deleteReady — the resource exists and is owned by this Graph.
	// Caller may proceed with deletion.
	deleteReady deletePreflightOutcome = iota
	// deleteNotFound — the resource does not exist (already gone).
	deleteNotFound
	// deleteNotOwned — the resource exists but is not owned by this Graph
	// (missing identity label).
	deleteNotOwned
	// deleteSkipParseFailed — the resource key could not be parsed.
	deleteSkipParseFailed
)

// deletePreflightResult captures the outcome and metadata from a preflight
// check so the caller can make follow-up decisions (finalization, delete, etc.).
type deletePreflightResult struct {
	Outcome deletePreflightOutcome
	Obj     *unstructured.Unstructured // live object from GET (nil if not found or parse failed)
}

// hasIdentityLabel reports whether any label in labels matches the graph
// identity label for the given graph name and namespace.
func hasIdentityLabel(labels map[string]string, graphName, namespace string) bool {
	for k := range labels {
		if graphpkg.IsGraphIdentityLabel(k, graphName, namespace) {
			return true
		}
	}
	return false
}

// deletePreflight performs the shared ownership and safety checks for a
// resource key:
//
//  1. Parse the resource key into an unstructured stub.
//  2. GET from the API server (authoritative read, not cache).
//  3. Verify ownership via identity labels.
//
// The caller handles all context-specific logic (finalization, patch release,
// deferred key tracking, FinalizerSkipped notes) and the actual delete call
// based on the returned result.
func (c *clusterAccess) deletePreflight(
	ctx context.Context,
	key string,
	rs *reconcileScope,
) deletePreflightResult {
	obj, nn, ok := unstructuredFromKey(key)
	if !ok {
		return deletePreflightResult{Outcome: deleteSkipParseFailed}
	}

	// Direct API server read for authoritative existence/ownership data.
	if err := c.reader.Get(ctx, nn, obj); err != nil {
		return deletePreflightResult{Outcome: deleteNotFound}
	}

	// Ownership gate: identity labels. Always check — a resource without
	// this Graph's identity label was never successfully applied and must
	// not be deleted (e.g., conflicted resources in the spec but never owned).
	if !hasIdentityLabel(obj.GetLabels(), rs.name, rs.namespace) {
		return deletePreflightResult{Outcome: deleteNotOwned, Obj: obj}
	}

	return deletePreflightResult{Outcome: deleteReady, Obj: obj}
}
