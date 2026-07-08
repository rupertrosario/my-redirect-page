// ------------------------------------------------
// Compare TEAM Incident State
//
// Team incident is cluster-level:
//   correlation_id = cohesity_ifdown_team_<cluster_id>
//
// Change detection is IP/interface-level:
//   Same Cluster + Same IP/Interface = NO UPDATE
//   Same Cluster + NEW IP/Interface  = UPDATE
// ------------------------------------------------

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const validateResult = await result("validate_interfaces");
  const snowSearch = await result("snow_search_team");

  const existing =
    snowSearch?.result?.[0] ||
    snowSearch?.results?.[0] ||
    snowSearch?.records?.[0];

  // No existing incident. Create task should handle create path.
  if (!existing) {
    return [
      {
        shouldUpdate: false,
        reason: "no_existing_incident"
      }
    ];
  }

  const currentItems =
    Array.isArray(validateResult?.teamIncidents)
      ? validateResult.teamIncidents
      : Array.isArray(validateResult?.teamincident)
        ? validateResult.teamincident
        : [];

  const existingCorrelation =
    String(existing.correlation_id || "").trim();

  const current =
    currentItems.find(function (x) {
      return String(x?.correlation_id || x?.CorrelationId || "").trim() === existingCorrelation;
    });

  if (!current) {
    return [
      {
        shouldUpdate: false,
        reason: "current_candidate_not_found",
        sys_id: existing.sys_id || "",
        number: existing.number || ""
      }
    ];
  }

  const currentFingerprint =
    String(current.fingerprint || current.Fingerprint || "").trim();

  const existingDescription =
    String(existing.description || "");

  const match =
    existingDescription.match(/Fingerprint:\s*(.*)/i);

  const oldFingerprint =
    match ? match[1].trim() : "";

  const shouldUpdate =
    currentFingerprint !== oldFingerprint;

  return [
    {
      shouldUpdate: shouldUpdate,
      reason: shouldUpdate ? "fingerprint_changed" : "fingerprint_unchanged",
      sys_id: existing.sys_id || "",
      number: existing.number || "",
      correlation_id: existingCorrelation,
      currentFingerprint: currentFingerprint,
      oldFingerprint: oldFingerprint
    }
  ];
}
