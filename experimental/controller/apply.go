// apply.go contains the cluster-mutating primitives for the Graph controller:
// resource apply via SSA (Template and Patch), field release, and SSA
// configuration (field manager identity, third-party manager detection).
//
// The reconcile loop in controller.go dispatches nodes; this file executes
// against the Kubernetes API server. All direct Patch, Delete, and ownership
// verification calls are here — controller.go does not touch the cluster
// directly for resource management.
//
// Prune logic (applied-set diff, deletion ordering) lives in prune.go.
package graphcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	graphpkg "github.com/kporwal-aws/kro/experimental/controller/graph"
)

// clusterAccess bundles the three dependencies needed to read and write
// Kubernetes resources. It is the interface between the orchestration
// layer (controller.go, walk.go) and the execution layer (apply.go,
// node.go, prune.go, finalization.go).
type clusterAccess struct {
	client client.Client  // read-write client (SSA, Delete)
	reader client.Reader  // direct API server reader (bypasses cache)
	scope  *scopeResolver // namespace vs cluster-scope resolution
}

// ---------------------------------------------------------------------------
// SSA configuration
// ---------------------------------------------------------------------------

// Field managers. Per the design (003-ownership § Field Manager), every
// template:/patch: node applies under its own SSA field manager:
//
//	<node>.<graph>.<namespace>.internal.kro.run
//
// SSA treats a manager's apply as its complete intent for the object, so
// nodes that write the same object must not share a manager or each apply
// releases the other's fields and the object is rewritten on every reconcile.
// Status subresource applies use the same name plus ".status".
//
// Before per-node managers, every node of a Graph applied under the per-Graph
// manager <graph>.<namespace>.internal.kro.run. That name is still recognized
// as the Graph's own (never a third party) so objects written by an older
// controller migrate: conflicting claims are taken over on apply, and the
// entry is released once the object's current writers have applied.
const (
	fieldManagerDomain  = ".internal.kro.run"
	statusManagerSuffix = ".status"
	// maxFieldManagerLen is the API server's limit on fieldManager length.
	maxFieldManagerLen = 128
)

// legacyGraphFieldManager returns the per-Graph field manager used before
// field managers became per-node. Every per-node manager of the Graph ends
// with "." + this name.
func legacyGraphFieldManager(graphName, namespace string) string {
	return graphName + "." + namespace + fieldManagerDomain
}

// nodeFieldOwner returns the SSA field manager for one node of a Graph.
// forEach children use their parent node's manager: children of one node
// always target distinct objects, so the (manager, object) pair is already
// unique per child. Node IDs are DNS labels (no dots), which keeps the node
// segment unambiguous. If the name would exceed the API server's limit
// (with room for the status suffix), the node segment is replaced by a
// short, stable, dot-free hash of the node ID.
func nodeFieldOwner(graphName, namespace, nodeID string) client.FieldOwner {
	base := legacyGraphFieldManager(graphName, namespace)
	segment := nodeID
	if len(segment)+1+len(base)+len(statusManagerSuffix) > maxFieldManagerLen {
		sum := sha256.Sum256([]byte(nodeID))
		segment = "n" + hex.EncodeToString(sum[:6])
	}
	return client.FieldOwner(segment + "." + base)
}

// graphManagerBase reports whether manager belongs to the given Graph (any
// node's manager, the legacy per-Graph manager, or a status sub-manager of
// either) and returns the base manager name with any status suffix removed.
func graphManagerBase(manager, graphName, namespace string) (string, bool) {
	legacy := legacyGraphFieldManager(graphName, namespace)
	base := manager
	if strings.HasSuffix(manager, fieldManagerDomain+statusManagerSuffix) {
		base = strings.TrimSuffix(manager, statusManagerSuffix)
	}
	if base == legacy {
		return base, true
	}
	segment, ok := strings.CutSuffix(base, "."+legacy)
	if !ok || segment == "" || strings.Contains(segment, ".") {
		return "", false
	}
	return base, true
}

// isGraphFieldManager reports whether manager belongs to the given Graph.
func isGraphFieldManager(manager, graphName, namespace string) bool {
	_, ok := graphManagerBase(manager, graphName, namespace)
	return ok
}

// graphManagerEntries returns the Graph's base manager names present on obj
// as Apply entries, mapped to whether the manager also has a status
// subresource entry.
func graphManagerEntries(obj *unstructured.Unstructured, graphName, namespace string) map[string]bool {
	var out map[string]bool
	for _, mf := range obj.GetManagedFields() {
		if mf.Operation != metav1.ManagedFieldsOperationApply {
			continue
		}
		base, ok := graphManagerBase(mf.Manager, graphName, namespace)
		if !ok {
			continue
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[base] = out[base] || mf.Subresource == "status"
	}
	return out
}

// thirdPartyFieldManagers returns field manager names on the resource that do
// not belong to this Graph and are not the API server's defaulting manager.
// Managers of sibling nodes of the same Graph, and the legacy per-Graph
// manager, are the Graph's own: two nodes writing one object are not
// co-owners in the conflict sense, and evicting a sibling would delete its
// fields. Only SSA Apply managers are considered — Update managers (from
// kubectl edit, plain client.Update, etc.) don't declare field ownership and
// shouldn't block apply. Per 003-ownership.md: used by the dry-run conflict
// check and force-apply eviction path to detect field co-ownership.
func thirdPartyFieldManagers(obj *unstructured.Unstructured, graphName, graphNamespace string) []string {
	managedFields := obj.GetManagedFields()
	if len(managedFields) == 0 {
		return nil
	}

	var thirdParty []string
	for _, mf := range managedFields {
		manager := mf.Manager
		// Only Apply managers count — they declare field ownership via SSA.
		// Update managers (kubectl edit, plain Update) don't declare ownership.
		if mf.Operation != metav1.ManagedFieldsOperationApply {
			continue
		}
		// Exclude this Graph's managers (every node, legacy, and status sub-managers).
		if isGraphFieldManager(manager, graphName, graphNamespace) {
			continue
		}
		// Exclude the API server's defaulting field managers
		if isAPIServerManager(manager) {
			continue
		}
		thirdParty = append(thirdParty, manager)
	}
	return thirdParty
}

// onlyConflictsWith reports whether err is an SSA apply conflict whose every
// conflicting manager is manager or its status sub-manager.
func onlyConflictsWith(err error, manager string) bool {
	var status apierrors.APIStatus
	if !apierrors.IsConflict(err) || !errors.As(err, &status) {
		return false
	}
	details := status.Status().Details
	if details == nil || len(details.Causes) == 0 {
		return false
	}
	for _, cause := range details.Causes {
		if cause.Type != metav1.CauseTypeFieldManagerConflict {
			return false
		}
		// Message format: conflict with "<manager>"[ with subresource "..."][ using ...]
		rest, ok := strings.CutPrefix(cause.Message, "conflict with ")
		if !ok {
			return false
		}
		quoted, err := strconv.QuotedPrefix(rest)
		if err != nil {
			return false
		}
		name, err := strconv.Unquote(quoted)
		if err != nil || (name != manager && name != manager+statusManagerSuffix) {
			return false
		}
	}
	return true
}

// isAPIServerManager returns true if the field manager name belongs to the
// Kubernetes API server's internal defaulting/admission logic.
func isAPIServerManager(manager string) bool {
	switch manager {
	case "kube-apiserver", "apiserver":
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

// deleteByKeys deletes resources identified by applied-set keys. Logs each
// attempt. Ignores NotFound (resource already gone). Used by both prune and
// teardown to clean up finalizer resources after a target is deleted.
func (c *clusterAccess) deleteByKeys(ctx context.Context, keys []string) {
	logger := log.FromContext(ctx)
	for _, fk := range keys {
		finDel, _, ok := unstructuredFromKey(fk)
		if !ok {
			continue
		}
		if delErr := c.client.Delete(ctx, finDel); delErr != nil {
			if client.IgnoreNotFound(delErr) != nil {
				logger.V(1).Info("finalizer resource cleanup failed", "key", fk, "error", delErr)
			}
		} else {
			logger.V(1).Info("cleaned up finalizer resource", "key", fk)
		}
	}
}

// cleanupFinalizationChildren dispatches cleanup for finalization children by
// node type: template children are deleted, patch children are release-applied
// (releasing SSA field ownership so the fields persist but kro no longer claims them).
func (c *clusterAccess) cleanupFinalizationChildren(ctx context.Context, children []finalizationChild, rs *reconcileScope) {
	logger := log.FromContext(ctx)
	for _, child := range children {
		switch child.NodeType {
		case graphpkg.NodeTypePatch:
			gvk, nn := parseResourceKey(child.Key)
			if gvk.Kind == "" {
				continue
			}
			if err := c.releaseGraphManagers(ctx, gvk, nn.Namespace, nn.Name, rs.name, rs.namespace); err != nil {
				if client.IgnoreNotFound(err) != nil {
					logger.V(1).Info("finalizer patch release failed", "key", child.Key, "error", err)
				}
			} else {
				logger.V(1).Info("released finalizer patch fields", "key", child.Key)
			}
		default:
			finDel, _, ok := unstructuredFromKey(child.Key)
			if !ok {
				continue
			}
			if delErr := c.client.Delete(ctx, finDel); delErr != nil {
				if client.IgnoreNotFound(delErr) != nil {
					logger.V(1).Info("finalizer resource cleanup failed", "key", child.Key, "error", delErr)
				}
			} else {
				logger.V(1).Info("cleaned up finalizer resource", "key", child.Key)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Apply / SSA
// ---------------------------------------------------------------------------

// applySSA applies a template via server-side apply (SSA). Handles both
// Template and Patch node types — the nodeType parameter controls:
//   - Identity labels: Template skips if present (forEach children stamp their own),
//     Patch always stamps.
//   - Pre-apply check: Template does a kro label check (cross-Graph ownership guard),
//     Patch checks target existence (patches apply to existing resources).
//   - Cache miss on NotFound: Template clears cache + returns ErrPending,
//     Patch falls through to apply.
//
// generation is the value to stamp on the identity-generation label. Normally
// matches graph.GetGeneration(); callers pass the reconcile-scoped effective
// generation (the active revision's generation when the current generation
// failed to compile). Explicit parameter rather than reading from graph so
// the choice is visible at the call site.
func (c *clusterAccess) applySSA(ctx context.Context, rs *reconcileScope, evalMap map[string]any, nodeID string, nodeType graphpkg.NodeType, generation int64, forceApply bool) (*unstructured.Unstructured, error) {
	obj, err := prepareObject(evalMap, rs, nodeID, nodeType, generation, c.scope)
	if err != nil {
		return nil, err
	}
	registerWatch(rs, nodeID, obj)
	if err := checkOwnership(ctx, c, obj, rs, nodeType); err != nil {
		return nil, err
	}
	fieldOwner := nodeFieldOwner(rs.name, rs.namespace, nodeID)
	legacy := legacyGraphFieldManager(rs.name, rs.namespace)
	if !forceApply {
		if err := dryRunSSA(ctx, c.client, obj, fieldOwner, nodeType, rs.name, rs.namespace); err != nil {
			return nil, err
		}
	}
	applied, err := ssaWrite(ctx, c.client, obj, fieldOwner, forceApply)
	if err != nil && !forceApply && onlyConflictsWith(err, legacy) {
		// The object was written by a controller that predates per-node
		// field managers and the node now declares a different value for a
		// field the legacy per-Graph manager owns. That claim is this Graph's
		// own, so take it over rather than surfacing a Conflict.
		applied, err = ssaWrite(ctx, c.client, obj, fieldOwner, true)
	}
	if err != nil {
		return nil, err
	}
	if forceApply && nodeType == graphpkg.NodeTypeTemplate {
		applied, err = evictThirdPartyManagers(ctx, c.client, applied, rs, evalMap, nodeID)
		if err != nil {
			return nil, err
		}
	}
	if forceApply && nodeType == graphpkg.NodeTypePatch {
		applied, err = surgicalReleaseThirdPartyManagers(ctx, c.client, applied, rs, nodeID)
		if err != nil {
			return nil, err
		}
	}
	rs.recordApplied(applied)
	return applied, nil
}

// dryRunSSA issues a dry-run SSA apply and inspects the response for conflicts.
// Two checks on the response:
// 1. Lifecycle exclusivity (templates only): another Graph's template identity label → Conflict.
// 2. Field exclusivity (all nodes): field-level co-ownership with another Apply-type manager → Conflict.
func dryRunSSA(ctx context.Context, c client.Client, obj *unstructured.Unstructured, fieldOwner client.FieldOwner, nodeType graphpkg.NodeType, graphName, graphNamespace string) error {
	// Strip .status from payload — matches what ssaWrite sends.
	mainPayload := obj.Object
	if statusData, hasStatus := obj.Object["status"]; hasStatus && statusData != nil {
		mainPayload = make(map[string]any, len(obj.Object))
		for k, v := range obj.Object {
			if k != "status" {
				mainPayload[k] = v
			}
		}
	}

	data, err := json.Marshal(mainPayload)
	if err != nil {
		return fmt.Errorf("marshaling dry-run payload: %w", err)
	}

	// Deep copy so Patch response doesn't mutate the caller's object.
	dryRunCopy := &unstructured.Unstructured{}
	dryRunCopy.SetGroupVersionKind(obj.GroupVersionKind())
	dryRunCopy.SetName(obj.GetName())
	dryRunCopy.SetNamespace(obj.GetNamespace())

	err = c.Patch(ctx, dryRunCopy, client.RawPatch(types.ApplyPatchType, data), fieldOwner, client.DryRunAll)
	if err != nil && onlyConflictsWith(err, legacyGraphFieldManager(graphName, graphNamespace)) {
		// Conflicts only with this Graph's legacy per-Graph manager: the real
		// apply takes those fields over (see applySSA). Re-run the dry-run with
		// force so the checks below see the post-takeover state.
		err = c.Patch(ctx, dryRunCopy, client.RawPatch(types.ApplyPatchType, data), fieldOwner, client.DryRunAll, client.ForceOwnership)
	}
	if err != nil {
		if apierrors.IsConflict(err) {
			return fmt.Errorf("SSA dry-run conflict on %s/%s %s: %w: %w",
				obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), ErrFieldConflict, err)
		}
		// Non-conflict errors from dry-run are propagated as-is.
		return fmt.Errorf("SSA dry-run on %s/%s %s: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), err)
	}

	// Check 1: Lifecycle exclusivity — only one template owner per resource.
	if nodeType == graphpkg.NodeTypeTemplate {
		if otherGraph, conflict := graphpkg.HasOtherGraphIdentityLabel(dryRunCopy.GetLabels(), graphName, graphNamespace); conflict {
			return fmt.Errorf("resource %s/%s %s owned by Graph %q, not %q (use lifecycle.apply: Force to take ownership): %w",
				obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), otherGraph, graphName, ErrFieldConflict)
		}
	}

	// Check 2: Field exclusivity — no field-level co-ownership with other Apply-type managers.
	managers := thirdPartyFieldManagers(dryRunCopy, graphName, graphNamespace)
	if len(managers) > 0 {
		// Check field-level overlap — disjoint managers on the same object is fine.
		kroFields := extractFieldsV1(dryRunCopy, string(fieldOwner))
		kroStatusFields := extractFieldsV1(dryRunCopy, string(fieldOwner)+statusManagerSuffix)
		kroMerged := mergeFieldsTrees(kroFields, kroStatusFields)

		var coOwners []string
		for _, mgr := range managers {
			coOwnerFields := extractFieldsV1(dryRunCopy, mgr)
			if fieldsOverlap(kroMerged, coOwnerFields) {
				coOwners = append(coOwners, mgr)
			}
		}
		if len(coOwners) > 0 {
			return fmt.Errorf("SSA dry-run detected field co-ownership on %s/%s %s by managers [%s]: %w",
				obj.GetAPIVersion(), obj.GetKind(), obj.GetName(),
				strings.Join(coOwners, ", "), ErrFieldConflict)
		}
	}
	return nil
}

// prepareObject creates an Unstructured from the evaluated map, defaults
// namespace for namespace-scoped resources, and stamps identity labels.
func prepareObject(evalMap map[string]any, rs *reconcileScope, nodeID string, nodeType graphpkg.NodeType, generation int64, scope *scopeResolver) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{Object: evalMap}

	// Only default namespace for namespace-scoped resources.
	// Cluster-scoped resources (CRDs, ClusterRoles, etc.) must keep
	// namespace empty — setting it breaks watch event routing because
	// the metadata informer reports events with namespace="" while
	// the scalar index would store namespace="<graph-ns>".
	obj.SetNamespace(defaultNamespace(obj.GroupVersionKind(), obj.GetNamespace(), rs.namespace, scope))

	// Stamp identity labels per 003-ownership.md § Identity Labels.
	generationStr := fmt.Sprintf("%d", generation)
	lbls := obj.GetLabels()
	if lbls == nil {
		lbls = map[string]string{}
	}
	if nodeType == graphpkg.NodeTypeTemplate {
		// Template: skip stamping if identity labels are already present (e.g., forEach
		// children stamp their own child-scoped labels before calling applySSA).
		if !graphpkg.HasGraphIdentityLabels(lbls, rs.name, rs.namespace) {
			lbls = graphpkg.SetIdentityLabels(lbls, nodeID, rs.name, rs.namespace, generationStr, graphpkg.NodeTypeTemplate)
		}
		// Stamp public graph labels on template nodes only. Patches skip these
		// to avoid SSA conflicts when multiple graphs target the same resource
		// (the template graph is the lifecycle owner; patches are contributors).
		lbls = graphpkg.SetGraphLabels(lbls, rs.name, rs.namespace)
	} else {
		// Patch: always stamp identity labels so resources are discoverable via
		// deriveAppliedSet() after controller restart.
		lbls = graphpkg.SetIdentityLabels(lbls, nodeID, rs.name, rs.namespace, generationStr, graphpkg.NodeTypePatch)
	}
	obj.SetLabels(lbls)

	return obj, nil
}

// registerWatch buffers a watch for the resource so that changes trigger
// re-reconciliation. The watch is flushed at done(true).
func registerWatch(rs *reconcileScope, nodeID string, obj *unstructured.Unstructured) {
	gvr := gvkToGVR(obj.GroupVersionKind())
	if rs.watcher != nil {
		rs.watcher.WatchScalar(nodeID, gvr, obj.GroupVersionKind().Kind, obj.GetName(), obj.GetNamespace())
	}
}

// checkOwnership performs the pre-apply check that differs by node type:
//   - Template: checks identity label conflict — if the existing resource has
//     a different Graph's identity label, returns an error unless forceApply.
//     Read labels from the metadata informer first (fast path, no API call).
//     Falls back to direct API read when the informer hasn't observed the
//     resource yet — preserves the cross-Graph safety guarantee during
//     informer startup or watch reconnect.
//   - Patch: checks that the target resource exists — patches apply to
//     existing resources only. Uses direct API server read to avoid stale
//     cache giving false negatives.
func checkOwnership(ctx context.Context, c *clusterAccess, obj *unstructured.Unstructured, rs *reconcileScope, nodeType graphpkg.NodeType) error {
	if nodeType == graphpkg.NodeTypeTemplate {
		return nil // Templates rely on dryRunSSA for conflict detection.
	}

	// Patch: target must exist — patches apply to existing resources.
	targetCheck := &unstructured.Unstructured{}
	targetCheck.SetGroupVersionKind(obj.GroupVersionKind())
	if err := c.reader.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, targetCheck); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("patch target %s/%s %s/%s not found: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetNamespace(), obj.GetName(), ErrPending)
		}
		return fmt.Errorf("checking patch target %s: %w", obj.GetName(), err)
	}
	return nil
}

// ssaWrite performs an SSA patch, optionally with a separate status subresource patch.
// When the object's .status field is non-nil it is split into a separate status
// subresource patch. Returns the most recent server-side state — the response from
// whichever patch runs last.
func ssaWrite(ctx context.Context, c client.Client, obj *unstructured.Unstructured, fieldOwner client.FieldOwner, force bool) (*unstructured.Unstructured, error) {
	var patchOpts []client.PatchOption
	patchOpts = append(patchOpts, fieldOwner)
	if force {
		patchOpts = append(patchOpts, client.ForceOwnership)
	}

	// Split status from main payload — status subresource requires a separate patch.
	statusData, hasStatus := obj.Object["status"]
	mainPayload := obj.Object
	if hasStatus && statusData != nil {
		mainPayload = make(map[string]any, len(obj.Object))
		for k, v := range obj.Object {
			if k != "status" {
				mainPayload[k] = v
			}
		}
	}
	mainPayload = omitEmptyNamespace(mainPayload)

	var readBack *unstructured.Unstructured
	{
		data, err := json.Marshal(mainPayload)
		if err != nil {
			return nil, fmt.Errorf("marshaling: %w", err)
		}

		applied := &unstructured.Unstructured{}
		applied.SetGroupVersionKind(obj.GroupVersionKind())
		applied.SetName(obj.GetName())
		applied.SetNamespace(obj.GetNamespace())

		if err := c.Patch(ctx, applied, client.RawPatch(types.ApplyPatchType, data), patchOpts...); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, err
			}
			if apierrors.IsConflict(err) {
				return nil, fmt.Errorf("SSA conflict on %s/%s %s: %w: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), ErrFieldConflict, err)
			}
			return nil, fmt.Errorf("applying %s/%s %s: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), err)
		}
		readBack = applied
	}
	if hasStatus && statusData != nil {
		statusPayload := identityBody(obj.GroupVersionKind(), obj.GetNamespace(), obj.GetName())
		statusPayload["status"] = statusData
		sData, err := json.Marshal(statusPayload)
		if err != nil {
			return nil, fmt.Errorf("marshaling status: %w", err)
		}
		statusApplied := &unstructured.Unstructured{}
		statusApplied.SetGroupVersionKind(obj.GroupVersionKind())
		statusApplied.SetName(obj.GetName())
		statusApplied.SetNamespace(obj.GetNamespace())
		// Use a distinct sub-field-manager for the status subresource so the
		// status-only body does not cause SSA to release main-resource fields
		// (annotations, labels) owned by the same manager name.
		statusFieldOwner := client.FieldOwner(string(fieldOwner) + statusManagerSuffix)
		var statusOpts []client.SubResourcePatchOption
		statusOpts = append(statusOpts, statusFieldOwner)
		if force {
			statusOpts = append(statusOpts, client.ForceOwnership)
		}
		if err := c.Status().Patch(ctx, statusApplied, client.RawPatch(types.ApplyPatchType, sData), statusOpts...); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, err
			}
			if apierrors.IsConflict(err) {
				return nil, fmt.Errorf("SSA status conflict on %s/%s %s: %w: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), ErrFieldConflict, err)
			}
			return nil, fmt.Errorf("applying status %s/%s %s: %w", obj.GetAPIVersion(), obj.GetKind(), obj.GetName(), err)
		}
		// Status Patch response is strictly newer — includes the status
		// we just wrote plus the current main-resource state.
		readBack = statusApplied
	}

	return readBack, nil
}

// evictThirdPartyManagers removes all third-party Apply-type field managers
// from a template resource after a force-apply. Per 003-ownership.md § Release
// Apply (eviction release): issue a full release impersonating each third-party
// manager so their managedFields entry is garbage-collected and their
// solely-owned fields (e.g., identity labels) are deleted from the object.
func evictThirdPartyManagers(ctx context.Context, c client.Client, readBack *unstructured.Unstructured, rs *reconcileScope, evalMap map[string]any, nodeID string) (*unstructured.Unstructured, error) {
	managers := thirdPartyFieldManagers(readBack, rs.name, rs.namespace)
	if len(managers) == 0 {
		return readBack, nil
	}
	gvk := readBack.GroupVersionKind()
	namespace := readBack.GetNamespace()
	name := readBack.GetName()
	hasStatus := templateHasStatus(evalMap)
	for _, mgr := range managers {
		evicted, err := releaseApply(ctx, c, gvk, namespace, name, client.FieldOwner(mgr), hasStatus)
		if err != nil {
			return nil, fmt.Errorf("eviction release for manager %q on %s: %w", mgr, name, err)
		}
		// Use the last release's Patch response — it reflects
		// the cumulative effect of all evictions.
		if evicted != nil {
			readBack = evicted
		}
		log.FromContext(ctx).Info("evicted field manager", "node", nodeID, "manager", mgr, "name", name)
	}
	return readBack, nil
}

// ---------------------------------------------------------------------------
// Surgical release — release co-ownership on overlapping fields for Patch nodes
// ---------------------------------------------------------------------------

// surgicalReleaseThirdPartyManagers releases co-ownership on overlapping fields
// after a force-apply on a Patch node. Per 003-ownership.md § Surgical Release:
// for each co-owner, apply under their identity with only their non-overlapping
// fields. This releases their claim on overlapping fields while preserving their
// claim on fields kro doesn't manage.
//
// NOTE: This implementation handles struct fields (f: prefix keys in fieldsV1)
// but does not handle list/map items (k: and v: prefix keys). The sole-ownership
// invariant for list items is not enforced by surgical release.
func surgicalReleaseThirdPartyManagers(ctx context.Context, c client.Client, readBack *unstructured.Unstructured, rs *reconcileScope, nodeID string) (*unstructured.Unstructured, error) {
	ownManager := string(nodeFieldOwner(rs.name, rs.namespace, nodeID))
	managers := thirdPartyFieldManagers(readBack, rs.name, rs.namespace)
	if len(managers) == 0 {
		return readBack, nil
	}

	logger := log.FromContext(ctx)
	gvk := readBack.GroupVersionKind()
	namespace := readBack.GetNamespace()
	name := readBack.GetName()

	// Extract this node's fieldsV1 (main + status sub-manager).
	kroFields := extractFieldsV1(readBack, ownManager)
	kroStatusFields := extractFieldsV1(readBack, ownManager+statusManagerSuffix)
	kroMerged := mergeFieldsTrees(kroFields, kroStatusFields)

	for _, mgr := range managers {
		coOwnerFields := extractFieldsV1(readBack, mgr)
		if coOwnerFields == nil {
			continue
		}

		nonOverlapping := fieldsSubtract(coOwnerFields, kroMerged)
		if len(nonOverlapping) == 0 {
			// All co-owner fields overlap with kro — full eviction release.
			hasStatus := managerOwnsStatus(readBack, mgr)
			evicted, err := releaseApply(ctx, c, gvk, namespace, name, client.FieldOwner(mgr), hasStatus)
			if err != nil {
				logger.V(1).Info("surgical release: eviction fallback failed", "node", nodeID, "manager", mgr, "error", err)
				continue
			}
			if evicted != nil {
				readBack = evicted
			}
			logger.Info("surgical release: evicted co-owner (full overlap)", "node", nodeID, "manager", mgr, "name", name)
			continue
		}

		// Build a body with identity fields + values from the current object
		// at non-overlapping paths only.
		body := buildSurgicalBody(readBack, gvk, nonOverlapping)

		obj := &unstructured.Unstructured{Object: body}
		obj.SetGroupVersionKind(gvk)

		released, err := ssaWrite(ctx, c, obj, client.FieldOwner(mgr), false)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			logger.V(1).Info("surgical release failed", "node", nodeID, "manager", mgr, "error", err)
			continue
		}
		if released != nil {
			readBack = released
		}
		logger.Info("surgical release: retained non-overlapping fields", "node", nodeID, "manager", mgr, "name", name)
	}
	return readBack, nil
}

// extractFieldsV1 returns the fieldsV1 tree for the named manager from managedFields.
// Returns nil if the manager is not found or has no fieldsV1 data.
func extractFieldsV1(obj *unstructured.Unstructured, managerName string) map[string]any {
	for _, mf := range obj.GetManagedFields() {
		if mf.Manager == managerName && mf.Operation == metav1.ManagedFieldsOperationApply {
			if mf.FieldsV1 == nil {
				return nil
			}
			var fields map[string]any
			if err := json.Unmarshal(mf.FieldsV1.Raw, &fields); err != nil {
				return nil
			}
			return fields
		}
	}
	return nil
}

// mergeFieldsTrees merges two fieldsV1 trees (union). Used to combine
// main and status sub-manager trees into a single kro ownership view.
func mergeFieldsTrees(a, b map[string]any) map[string]any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	result := make(map[string]any, len(a))
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		if existing, ok := result[k]; ok {
			// Both have this key — recurse if both are maps.
			aMap, aIsMap := existing.(map[string]any)
			bMap, bIsMap := v.(map[string]any)
			if aIsMap && bIsMap {
				result[k] = mergeFieldsTrees(aMap, bMap)
			}
			// If one is a leaf (empty map) and the other is a subtree,
			// the leaf means "owns this field" — keep it as-is (leaf wins).
		} else {
			result[k] = v
		}
	}
	return result
}

// fieldsOverlap returns true if any field path exists in both fieldsV1 tries.
func fieldsOverlap(a, b map[string]any) bool {
	if a == nil || b == nil {
		return false
	}
	for k, v := range a {
		bVal, inB := b[k]
		if !inB {
			continue
		}
		// Both have key k
		aMap, aIsMap := v.(map[string]any)
		bMap, bIsMap := bVal.(map[string]any)
		if !aIsMap || len(aMap) == 0 {
			return true // a is leaf at k, b also has k → overlap
		}
		if !bIsMap || len(bMap) == 0 {
			return true // b is leaf at k, a has subtree at k → b owns all → overlap
		}
		// Both are subtrees → recurse
		if fieldsOverlap(aMap, bMap) {
			return true
		}
	}
	return false
}

// fieldsSubtract returns the subset of `a` that does NOT overlap with `b`.
// A field in `a` overlaps with `b` if `b` contains the same path (at the
// same depth or as a leaf). Returns nil if all fields overlap.
func fieldsSubtract(a, b map[string]any) map[string]any {
	if b == nil {
		return a
	}
	result := make(map[string]any)
	for k, v := range a {
		bVal, inB := b[k]
		if !inB {
			// Not in b — keep entirely.
			result[k] = v
			continue
		}
		// Both have this key. Check if both are subtrees.
		aMap, aIsMap := v.(map[string]any)
		bMap, bIsMap := bVal.(map[string]any)
		if aIsMap && len(aMap) > 0 && bIsMap && len(bMap) > 0 {
			// Recurse — partial overlap possible.
			sub := fieldsSubtract(aMap, bMap)
			if len(sub) > 0 {
				result[k] = sub
			}
		}
		// If a is a leaf ({}) and b contains it, it's overlapping — skip.
		// If a is a subtree but b is a leaf, b owns the whole subtree — skip.
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// identityBody returns an apply body carrying only apiVersion, kind,
// metadata.name and metadata.namespace. The namespace key is omitted when
// empty: sending "namespace": "" for a cluster-scoped object makes the
// apiserver count an otherwise-unchanged apply as a change whenever another
// field manager wrote in between, so two managers sharing an object rewrite
// it (managedFields time/order only) on every apply, forever.
func identityBody(gvk schema.GroupVersionKind, namespace, name string) map[string]any {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetGroupVersionKind(gvk)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u.Object
}

// omitEmptyNamespace returns payload without an empty metadata.namespace key
// (see identityBody). Rendered templates can carry one explicitly, e.g.
// instance.metadata.?namespace.orValue(""). The payload is copied, not
// mutated, when the key is dropped.
func omitEmptyNamespace(payload map[string]any) map[string]any {
	md, ok := payload["metadata"].(map[string]any)
	if !ok {
		return payload
	}
	if ns, has := md["namespace"]; !has || ns != "" {
		return payload
	}
	newMD := make(map[string]any, len(md))
	for k, v := range md {
		if k != "namespace" {
			newMD[k] = v
		}
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		out[k] = v
	}
	out["metadata"] = newMD
	return out
}

// buildSurgicalBody creates an unstructured object body containing identity
// fields (apiVersion, kind, metadata.name, metadata.namespace) plus values
// extracted from the current object at paths specified by the fieldsV1 tree.
func buildSurgicalBody(current *unstructured.Unstructured, gvk schema.GroupVersionKind, fields map[string]any) map[string]any {
	body := identityBody(gvk, current.GetNamespace(), current.GetName())

	// Walk the fieldsV1 tree and extract corresponding values from current object.
	extractFieldValues(current.Object, fields, body)
	return body
}

// extractFieldValues walks a fieldsV1 tree and copies matching values from src
// into dst. Top-level keys "apiVersion", "kind", and "metadata" identity
// sub-fields (name, namespace) are skipped since they're pre-populated.
func extractFieldValues(src map[string]any, fieldsTree map[string]any, dst map[string]any) {
	for key, subtree := range fieldsTree {
		fieldName := fieldsV1KeyToFieldName(key)
		if fieldName == "" {
			continue
		}
		// Skip identity fields already in dst.
		if fieldName == "apiVersion" || fieldName == "kind" {
			continue
		}

		srcVal, exists := src[fieldName]
		if !exists {
			continue
		}

		subMap, isMap := subtree.(map[string]any)
		if !isMap || len(subMap) == 0 {
			// Leaf — copy value directly.
			dst[fieldName] = srcVal
			continue
		}

		// Subtree — recurse into nested struct.
		srcNested, srcIsMap := srcVal.(map[string]any)
		if !srcIsMap {
			// Source doesn't match expected structure — copy as-is.
			dst[fieldName] = srcVal
			continue
		}

		// For metadata, merge into existing dst metadata (to preserve name/namespace).
		if fieldName == "metadata" {
			dstMeta, _ := dst["metadata"].(map[string]any)
			if dstMeta == nil {
				dstMeta = map[string]any{}
				dst["metadata"] = dstMeta
			}
			extractFieldValues(srcNested, subMap, dstMeta)
			continue
		}

		// For other nested fields, create/merge sub-object.
		dstNested, _ := dst[fieldName].(map[string]any)
		if dstNested == nil {
			dstNested = map[string]any{}
		}
		extractFieldValues(srcNested, subMap, dstNested)
		if len(dstNested) > 0 {
			dst[fieldName] = dstNested
		}
	}
}

// fieldsV1KeyToFieldName extracts the field name from a fieldsV1 key.
// "f:fieldName" → "fieldName", "k:{...}" and "v:..." are returned as-is
// (they represent list/set items and are not simple struct fields).
func fieldsV1KeyToFieldName(key string) string {
	if len(key) < 3 {
		return ""
	}
	switch key[0] {
	case 'f':
		if key[1] == ':' {
			return key[2:]
		}
	case '.':
		// "." is used for the root — skip.
		return ""
	}
	// k: and v: entries are list/set items — not simple field extractions.
	// For now, skip them (surgical release focuses on struct fields).
	return ""
}

// managerOwnsStatus returns true if the given manager has a status subresource
// entry in managedFields (i.e., a manager entry with subresource "status").
func managerOwnsStatus(obj *unstructured.Unstructured, managerName string) bool {
	for _, mf := range obj.GetManagedFields() {
		if mf.Manager == managerName && mf.Subresource == "status" {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Release apply — release field ownership without deleting the object
// ---------------------------------------------------------------------------

func releaseApply(ctx context.Context, c client.Client, gvk schema.GroupVersionKind, namespace, name string, fieldOwner client.FieldOwner, hasStatus bool) (*unstructured.Unstructured, error) {
	// Pre-check existence — SSA apply creates resources that don't exist,
	// which would re-create a resource that was already deleted (e.g., when
	// the first release apply removes a finalizer, the resource is GC'd, and
	// a second release apply would create a zombie).
	check := &unstructured.Unstructured{}
	check.SetGroupVersionKind(gvk)
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, check); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checking existence before release apply for %s/%s: %w", namespace, name, err)
	}

	release := identityBody(gvk, namespace, name)
	if hasStatus {
		// Include "status": {} so SSA releases all previously-owned status fields.
		// An identity-only release body (apiVersion/kind/metadata) is ignored by
		// the status endpoint — no fields are claimed, so no ownership is released.
		release["status"] = map[string]any{}
	}

	obj := &unstructured.Unstructured{Object: release}
	obj.SetGroupVersionKind(gvk)

	result, err := ssaWrite(ctx, c, obj, fieldOwner, false)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Resource deleted between our check and the apply — nothing to release.
			return nil, nil
		}
		return nil, fmt.Errorf("release apply for %s/%s: %w", namespace, name, err)
	}
	return result, nil
}

// releaseGraphManagers issues a full release under every field manager of the
// given Graph present on the object: each node's manager, the legacy
// per-Graph manager, and their status sub-managers. Used when no current node
// writes the object (patch prune, teardown, finalizer patch cleanup), so every
// field the Graph claimed on it is released. A missing object is not an error.
func (c *clusterAccess) releaseGraphManagers(ctx context.Context, gvk schema.GroupVersionKind, namespace, name, graphName, graphNamespace string) error {
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvk)
	if err := c.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("reading %s/%s before release: %w", namespace, name, err)
	}
	for manager, hasStatus := range graphManagerEntries(live, graphName, graphNamespace) {
		if _, err := releaseApply(ctx, c.client, gvk, namespace, name, client.FieldOwner(manager), hasStatus); err != nil {
			return err
		}
	}
	return nil
}

// releaseStaleNodeManagers releases this Graph's field managers that remain
// on an object the current walk wrote but that belong to none of the object's
// current writers: a node that was renamed, removed, or excluded while
// another node still writes the object, or the legacy per-Graph manager on an
// object written by an older controller. Prune cannot see these because the
// object's key is still in the applied set. Managers of nodes in Conflict are
// kept (their claim is unresolved, not withdrawn), and while any node is in
// Conflict the legacy manager is kept too, since it may hold that node's
// fields. Callers must only invoke this once every node has settled (no
// Pending, Blocked, or Error nodes), so each current writer has applied under
// its own manager and a release deletes only fields no current node declares.
//
// The check reads the managedFields returned by this walk's applies, so a
// settled Graph makes no API calls here.
func (c *clusterAccess) releaseStaleNodeManagers(ctx context.Context, rs *reconcileScope, appliedKeys []Applied, conflictNodes []string) error {
	if len(rs.applied) == 0 {
		return nil
	}
	writers := map[string]map[string]bool{}
	for _, a := range appliedKeys {
		if a.Key == "" || a.NodeID == "" {
			continue
		}
		if writers[a.Key] == nil {
			writers[a.Key] = map[string]bool{}
		}
		writers[a.Key][string(nodeFieldOwner(rs.name, rs.namespace, a.NodeID))] = true
	}
	keep := map[string]bool{}
	for _, id := range conflictNodes {
		keep[string(nodeFieldOwner(rs.name, rs.namespace, id))] = true
	}
	if len(conflictNodes) > 0 {
		keep[legacyGraphFieldManager(rs.name, rs.namespace)] = true
	}

	logger := log.FromContext(ctx)
	for key, obj := range rs.applied {
		current := writers[key]
		if len(current) == 0 {
			continue
		}
		for manager, hasStatus := range graphManagerEntries(obj, rs.name, rs.namespace) {
			if current[manager] || keep[manager] {
				continue
			}
			if _, err := releaseApply(ctx, c.client, obj.GroupVersionKind(), obj.GetNamespace(), obj.GetName(), client.FieldOwner(manager), hasStatus); err != nil {
				return fmt.Errorf("releasing stale field manager %q on %s: %w", manager, key, err)
			}
			logger.Info("released stale field manager", "manager", manager, "key", key)
		}
	}
	return nil
}
