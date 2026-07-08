// Dynatrace JS action: Build update target and payload from normalized Team output.
import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  var normalizeExec = await execution("normalize_team_search_result");
  var normalized = normalizeExec && normalizeExec.result ? normalizeExec.result : (normalizeExec || {});
  var candidate = normalized.candidate || {};
  if (normalized.action !== "update" || !normalized.sys_id) {
    throw new Error("Update payload requires action update and a sys_id.");
  }
  return { sys_id: normalized.sys_id, payload: { work_notes: candidate.WorkNotes || "" } };
}
