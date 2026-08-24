package graphcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kporwal-aws/kro/experimental/controller/compiler"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// conditionType identifies a condition on a Graph or GraphRevision.
type conditionType string

// Condition types
const (
	conditionCompiled conditionType = "Compiled"
	conditionReady    conditionType = "Ready"
)

// conditionStatus is the boolean-ish status of a condition (True/False/Unknown).
type conditionStatus string

// Condition statuses
const (
	conditionTrue    conditionStatus = "True"
	conditionFalse   conditionStatus = "False"
	conditionUnknown conditionStatus = "Unknown"
)

// conditionOutcome bundles the three values that describe one Kubernetes
// condition — status, reason, and message — so they travel as a named
// group rather than positional returns.
type conditionOutcome struct {
	status  conditionStatus
	reason  string
	message string
}

// reconcileState captures the outcome of a reconcile cycle for status derivation.
type reconcileState struct {
	// compiled tracks spec validity. Set once when the spec is parsed/compiled.
	// False is terminal — no resources are touched until the spec is fixed.
	compiled    bool
	compiledErr error // non-nil when compiled=false

	// nodeCount is the total number of nodes in the compiled DAG.
	nodeCount int

	planSummary PlanSummary
	// nodeErrors carries detailed error messages ("nodeID: reason") surfaced
	// alongside the node ID lists in PlanSummary. These provide the reason text
	// for NotReady/Error/Blocked conditions — the node lists name *which*
	// resources, nodeErrors explain *why*.
	nodeErrors []string
	// nodeNotes carries informational messages that don't gate Ready — e.g.,
	// FinalizerSkipped emitted during prune when the target resource was
	// already absent. Notes appear in the Ready condition message when the
	// graph is otherwise healthy, so operators see the event without being
	// misled by an Unknown/False status. Per 005-reconciliation.md §
	// Finalization: "FinalizerSkipped is not an error — finalization was
	// bypassed because there was nothing to finalize."
	nodeNotes []string
}

// deriveCompiledCondition computes the Compiled condition.
// Compiled is set once when the spec is parsed/compiled. It's permanent until
// the spec changes. False means the Graph will never converge until the spec is fixed.
func (s *reconcileState) deriveCompiledCondition() conditionOutcome {
	if s.compiled {
		return conditionOutcome{conditionTrue, "Compiled", fmt.Sprintf("%d nodes", s.nodeCount)}
	}
	if s.compiledErr != nil {
		// Classify the error
		if errors.Is(s.compiledErr, compiler.ErrInvalidExpression) {
			return conditionOutcome{conditionFalse, "ExpressionError", s.compiledErr.Error()}
		}
		if errors.Is(s.compiledErr, compiler.ErrDependencyError) {
			return conditionOutcome{conditionFalse, "DependencyError", s.compiledErr.Error()}
		}
		return conditionOutcome{conditionFalse, "DeclarationError", s.compiledErr.Error()}
	}
	return conditionOutcome{conditionFalse, "DeclarationError", "Spec validation failed"}
}

// deriveReadyCondition computes the Ready condition from the reconcile outcome.
//
// The message format is a summary line with state counts followed by up to
// maxNodeDetails indented per-node detail lines for every non-ready node:
//
//	9 ready, 2 not ready, 1 error
//	  authService (error): 403 Forbidden
//	  database (not ready)
//	  cache (not ready)
//
// Nodes with error reasons show them; nodes still converging show only their
// state label. This tells operators exactly which nodes to investigate.
//
// Ready is a rollup of node plan states. Each reason maps to the node state
// blocking convergence. Precedence: SystemError > Error > Conflict > Blocked >
// Pending > NotReady. SystemError surfaces first because it signals degraded
// reconciliation infrastructure — deterministic errors (Error) and conflicts
// may be artifacts of system instability, not real spec problems.
//
//	Ready       → True    — all resources reconciled
//	Pending     → Unknown — waiting for upstream data
//	NotReady    → Unknown — applied but readyWhen conditions not met
//	Blocked     → Unknown — dependency in error state, waiting for resolve
//	NotCompiled → False   — spec invalid; rollup of Compiled=False
//	SystemError → False   — server or infrastructure failure (5xx)
//	Error       → False   — client request failed (4xx)
//	Conflict    → False   — SSA field ownership contested
func (s *reconcileState) deriveReadyCondition() conditionOutcome {
	if !s.compiled {
		return conditionOutcome{conditionFalse, "NotCompiled", "Spec is not valid; resources cannot be reconciled"}
	}

	// Build the summary line: show counts for all non-zero states.
	msg := s.buildReadyMessage()

	// Determine reason and status from precedence.
	if len(s.planSummary.SystemErrorNodes) > 0 {
		return conditionOutcome{conditionFalse, "SystemError", msg}
	}
	if len(s.planSummary.ErrorNodes) > 0 {
		return conditionOutcome{conditionFalse, "Error", msg}
	}
	if len(s.planSummary.ConflictNodes) > 0 {
		return conditionOutcome{conditionFalse, "Conflict", msg}
	}
	if len(s.planSummary.BlockedNodes) > 0 {
		return conditionOutcome{conditionUnknown, "Blocked", msg}
	}
	if len(s.planSummary.PendingNodes) > 0 {
		return conditionOutcome{conditionUnknown, "Pending", msg}
	}
	if len(s.planSummary.NotReadyNodes) > 0 {
		return conditionOutcome{conditionUnknown, "NotReady", msg}
	}
	return conditionOutcome{conditionTrue, "Ready", msg}
}

// maxNodeDetails is the maximum number of per-node detail lines included in
// the Ready condition message. Beyond this, a truncation note is appended.
const maxNodeDetails = 10

// buildReadyMessage constructs the Ready condition message: a summary line
// with state counts, followed by indented per-node detail lines for every
// node in a non-ready state.
func (s *reconcileState) buildReadyMessage() string {
	ps := &s.planSummary

	// Summary line: counts for all non-zero states.
	var parts []string
	if ps.ReadyCount > 0 {
		parts = append(parts, fmt.Sprintf("%d ready", ps.ReadyCount))
	}
	if len(ps.NotReadyNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d not ready", len(ps.NotReadyNodes)))
	}
	if len(ps.PendingNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d pending", len(ps.PendingNodes)))
	}
	if ps.ExcludedCount > 0 {
		parts = append(parts, fmt.Sprintf("%d excluded", ps.ExcludedCount))
	}
	if len(ps.BlockedNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d blocked", len(ps.BlockedNodes)))
	}
	if len(ps.ConflictNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d conflict", len(ps.ConflictNodes)))
	}
	if len(ps.ErrorNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d error", len(ps.ErrorNodes)))
	}
	if len(ps.SystemErrorNodes) > 0 {
		parts = append(parts, fmt.Sprintf("%d system error", len(ps.SystemErrorNodes)))
	}

	// If all states are zero (shouldn't happen), produce a fallback.
	if len(parts) == 0 {
		parts = append(parts, "0 ready")
	}
	summary := strings.Join(parts, ", ")

	// Append node notes when the graph is fully healthy.
	if ps.IsClean() && len(s.nodeNotes) > 0 {
		sorted := make([]string, len(s.nodeNotes))
		copy(sorted, s.nodeNotes)
		sort.Strings(sorted)
		summary += " (" + strings.Join(sorted, "; ") + ")"
	}

	// Collect all non-ready node IDs for detail lines.
	var allNonReady []string
	allNonReady = append(allNonReady, ps.NotReadyNodes...)
	allNonReady = append(allNonReady, ps.PendingNodes...)
	allNonReady = append(allNonReady, ps.BlockedNodes...)
	allNonReady = append(allNonReady, ps.ConflictNodes...)
	allNonReady = append(allNonReady, ps.ErrorNodes...)
	allNonReady = append(allNonReady, ps.SystemErrorNodes...)

	if len(allNonReady) == 0 {
		return summary
	}

	// Build reason index from nodeErrors ("nodeID: reason").
	reasonIndex := make(map[string]string, len(s.nodeErrors))
	for _, e := range s.nodeErrors {
		if i := strings.Index(e, ": "); i > 0 {
			reasonIndex[e[:i]] = e[i+2:]
		}
	}

	// Build detail lines: nodes with reasons get "nodeID (state): reason",
	// others get "nodeID (state)".
	stateIndex := buildStateIndex(ps)
	sort.Strings(allNonReady)

	details := make([]string, 0, len(allNonReady))
	for _, id := range allNonReady {
		state := stateIndex[id]
		if state == "" {
			state = "unknown"
		}
		if reason, ok := reasonIndex[id]; ok {
			details = append(details, fmt.Sprintf("%s (%s): %s", id, state, reason))
		} else {
			details = append(details, fmt.Sprintf("%s (%s)", id, state))
		}
	}

	// Truncate to maxNodeDetails.
	var b strings.Builder
	b.WriteString(summary)
	shown := details
	overflow := 0
	if len(details) > maxNodeDetails {
		shown = details[:maxNodeDetails]
		overflow = len(details) - maxNodeDetails
	}
	for _, d := range shown {
		b.WriteString("\n  ")
		b.WriteString(d)
	}
	if overflow > 0 {
		b.WriteString(fmt.Sprintf("\n  ... and %d more", overflow))
	}
	return b.String()
}

// buildStateIndex creates a map from node ID to its human-readable state label.
func buildStateIndex(ps *PlanSummary) map[string]string {
	idx := make(map[string]string)
	for _, id := range ps.SystemErrorNodes {
		idx[id] = "system error"
	}
	for _, id := range ps.ErrorNodes {
		idx[id] = "error"
	}
	for _, id := range ps.ConflictNodes {
		idx[id] = "conflict"
	}
	for _, id := range ps.BlockedNodes {
		idx[id] = "blocked"
	}
	for _, id := range ps.NotReadyNodes {
		idx[id] = "not ready"
	}
	for _, id := range ps.PendingNodes {
		idx[id] = "pending"
	}
	return idx
}



// updateStatus writes the Graph's status subresource. Reads the latest version
// from the API server to avoid conflicts.
func (r *GraphReconciler) updateStatus(ctx context.Context, graph *unstructured.Unstructured, state *reconcileState) error {
	latest := &unstructured.Unstructured{}
	latest.SetGroupVersionKind(GraphGVK)
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(graph), latest); err != nil {
		return fmt.Errorf("reading latest for status update: %w", err)
	}

	// Build both conditions
	generation := graph.GetGeneration()

	compiled := state.deriveCompiledCondition()
	compiledCondition := buildCondition(string(conditionCompiled), compiled, generation)

	ready := state.deriveReadyCondition()
	readyCondition := buildCondition(string(conditionReady), ready, generation)

	// Preserve lastTransitionTime for conditions whose status hasn't changed
	existingConditions, _, _ := unstructured.NestedSlice(latest.Object, "status", "conditions")
	preserveTransitionTime(existingConditions, compiledCondition, compiled.status)
	preserveTransitionTime(existingConditions, readyCondition, ready.status)

	status := map[string]any{
		"conditions": []any{
			compiledCondition,
			readyCondition,
		},
	}

	// Skip the status write if nothing changed. Compare via JSON to avoid
	// reflect.DeepEqual issues with map[string]any types. This prevents a
	// spurious resourceVersion bump and the resulting watch → reconcile loop.
	existingStatus, _, _ := unstructured.NestedMap(latest.Object, "status")
	if statusEqual(existingStatus, status) {
		return nil
	}

	latest.Object["status"] = status

	if err := r.Client.Status().Update(ctx, latest); err != nil {
		return fmt.Errorf("updating status: %w", err)
	}

	return nil
}

// statusEqual compares two status maps via JSON serialization.
// Returns true if they're semantically identical.
func statusEqual(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	aj, err1 := json.Marshal(a)
	bj, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return string(aj) == string(bj)
}

// buildCondition creates a condition map with the current timestamp.
func buildCondition(condType string, outcome conditionOutcome, observedGeneration int64) map[string]any {
	return map[string]any{
		"type":               condType,
		"status":             string(outcome.status),
		"reason":             outcome.reason,
		"message":            outcome.message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		"observedGeneration": observedGeneration,
	}
}

// preserveTransitionTime scans existing conditions for a match on type+status
// and preserves the lastTransitionTime if the status hasn't changed.
func preserveTransitionTime(existing []any, cond map[string]any, status conditionStatus) {
	for _, ec := range existing {
		ecMap, ok := ec.(map[string]any)
		if !ok {
			continue
		}
		if ecMap["type"] == cond["type"] && ecMap["status"] == string(status) {
			if ltt, ok := ecMap["lastTransitionTime"].(string); ok {
				cond["lastTransitionTime"] = ltt
			}
			return
		}
	}
}
