// ------------------------------------------------------------
// Build Team update payload from comparison output
// ------------------------------------------------------------
// Note:
// The current Dynatrace ServiceNow Update Incident action may expose
// only Incident number and Comment fields. In that case, map directly:
//
//   Incident number = {{ _.item.number }}
//   Comment         = {{ _.item.comment }}
//
// This helper is retained only for workflows that use a custom ServiceNow
// table/API update task and need a payload object.
// ------------------------------------------------------------

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const compareRows = await result("compare_team_incident_state");
  const validateResult = await result("validate_interfaces");

  const compare =
    Array.isArray(compareRows)
      ? compareRows[0]
      : compareRows;

  if (!compare || compare.shouldUpdate !== true) {
    throw new Error("Team update payload requires shouldUpdate=true.");
  }

  const teamItems =
    Array.isArray(validateResult?.teamIncidents)
      ? validateResult.teamIncidents
      : Array.isArray(validateResult?.teamincident)
        ? validateResult.teamincident
        : [];

  const candidate =
    teamItems.find(function (x) {
      return String(x?.correlation_id || x?.CorrelationId || "").trim() === String(compare.correlation_id || "").trim();
    }) || {};

  const comment =
    String(candidate.comment || candidate.WorkNotes || candidate.work_notes || candidate.description || candidate.Description || "").trim();

  return {
    sys_id: compare.sys_id || "",
    number: compare.number || "",
    comment: comment,
    payload: {
      comments: comment
    }
  };
}
