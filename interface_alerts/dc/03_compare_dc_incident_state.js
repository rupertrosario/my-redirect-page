// ------------------------------------------------
// Compare DC Incident State
//
// DC incident is location + cluster level.
// Compare current generated description with existing
// ServiceNow description to avoid repeated updates.
// ------------------------------------------------

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const currentItems = await result("dc_iterations");
  const existingItems = await result("snow_search_dc");

  const current =
    Array.isArray(currentItems)
      ? currentItems[0]
      : currentItems;

  const existing =
    Array.isArray(existingItems)
      ? existingItems[0]
      : existingItems;

  // No existing incident. Create task should handle create path.
  if (!existing) {
    return [
      {
        shouldUpdate: false,
        reason: "no_existing_incident"
      }
    ];
  }

  const currentCorrelation =
    String(current?.correlation_id || current?.CorrelationId || "").trim();

  const existingCorrelation =
    String(existing?.correlation_id || existing?.CorrelationId || "").trim();

  if (
    currentCorrelation &&
    existingCorrelation &&
    currentCorrelation !== existingCorrelation
  ) {
    return [
      {
        shouldUpdate: false,
        reason: "correlation_id_mismatch",
        sys_id: existing.sys_id || "",
        number: existing.number || ""
      }
    ];
  }

  const currentDesc =
    String(current?.description || current?.Description || "").trim();

  const existingDesc =
    String(existing?.description || existing?.Description || "").trim();

  const shouldUpdate =
    currentDesc !== existingDesc;

  return [
    {
      shouldUpdate: shouldUpdate,
      reason: shouldUpdate ? "description_changed" : "description_unchanged",
      sys_id: existing.sys_id || "",
      number: existing.number || "",
      correlation_id: existingCorrelation || currentCorrelation
    }
  ];
}
