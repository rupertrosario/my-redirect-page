// Dynatrace JS action: Build Team ServiceNow search query
// Purpose: Runs inside item_iterations. Each run receives one Team candidate in _.loopItemValue.
// Team identity: one active incident per ClusterId using correlation_id = cohesity_ifdown_team_<ClusterId>.

export default async function (_) {
  const item = _ && _.loopItemValue ? _.loopItemValue : null;

  function norm(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }

  if (!item) {
    return {
      ok: false,
      action: "no_write",
      reason: "Missing loopItemValue"
    };
  }

  const clusterId = norm(item.cluster_id || item.ClusterId || item.clusterId);
  const clusterName = norm(item.cluster_name || item.ClusterName || item.clusterName);

  const correlationId = norm(
    item.correlation_id ||
    item.CorrelationId ||
    (clusterId ? "cohesity_ifdown_team_" + clusterId : "")
  );

  const shortDescription = norm(
    item.short_description ||
    item.ShortDescription ||
    (clusterName ? "Cohesity Interface DOWN - Team - " + clusterName : "Cohesity Interface DOWN - Team")
  );

  const fingerprint = norm(item.fingerprint || item.Fingerprint);
  const description = norm(item.description || item.Description);
  const workNotes = norm(item.work_notes || item.WorkNotes || description);

  if (!clusterId) {
    return {
      ok: false,
      action: "no_write",
      reason: "Missing ClusterId",
      candidate: item
    };
  }

  if (!correlationId) {
    return {
      ok: false,
      action: "no_write",
      reason: "Missing correlation_id",
      candidate: item
    };
  }

  return {
    ok: true,
    type: "TEAM",

    // Use this in the ServiceNow lookup step.
    query:
      "correlation_id=" +
      correlationId +
      "^stateNOT IN6,7^ORDERBYDESCsys_updated_on",

    cluster_id: clusterId,
    cluster_name: clusterName,
    short_description: shortDescription,
    fingerprint: fingerprint,
    description: description,
    work_notes: workNotes,
    correlation_id: correlationId,

    // Carry the current loop item forward so downstream normalize/create/update
    // steps do not fall back to teamIncidentCandidates[0].
    candidate: {
      ...item,
      cluster_id: clusterId,
      cluster_name: clusterName,
      short_description: shortDescription,
      fingerprint: fingerprint,
      description: description,
      work_notes: workNotes,
      correlation_id: correlationId
    }
  };
}
