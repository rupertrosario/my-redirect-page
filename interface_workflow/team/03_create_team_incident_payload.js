// Dynatrace JS action: Build create payload from normalized Team output.
import { execution } from "@dynatrace-sdk/automation-utils";

export default async function () {
  var normalizeExec = await execution("normalize_team_search_result");
  var normalized = normalizeExec && normalizeExec.result ? normalizeExec.result : (normalizeExec || {});
  var candidate = normalized.candidate || {};
  if (normalized.action !== "create") throw new Error("Create payload called when action is not create.");
  return {
    short_description: candidate.ShortDescription || "",
    description: candidate.Description || "",
    work_notes: candidate.WorkNotes || "",
    correlation_id: candidate.CorrelationId || "",
    category: "infrastructure",
    subcategory: "backup",
    impact: "3",
    urgency: "3"
  };
}
