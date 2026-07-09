import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {

  const snowResult = await result("dtsk_snow_search");

  function extractRows(payload) {
    if (!payload) return [];

    let body = payload.body;

    if (typeof body === "string") {
      try {
        body = JSON.parse(body);
      } catch {
        body = null;
      }
    }

    if (Array.isArray(payload)) return payload;
    if (Array.isArray(payload.result)) return payload.result;
    if (Array.isArray(payload.results)) return payload.results;
    if (Array.isArray(payload.records)) return payload.records;
    if (Array.isArray(payload.data)) return payload.data;

    if (payload.number) return [payload];
    if (payload.result?.number) return [payload.result];

    if (Array.isArray(body)) return body;
    if (Array.isArray(body?.result)) return body.result;
    if (Array.isArray(body?.results)) return body.results;
    if (Array.isArray(body?.records)) return body.records;
    if (Array.isArray(body?.data)) return body.data;

    if (body?.number) return [body];
    if (body?.result?.number) return [body.result];

    return [];
  }

  function toText(value) {
    if (value === null || value === undefined) return "";

    if (Array.isArray(value)) {
      for (const item of value) {
        const t = toText(item);
        if (t) return t;
      }
      return "";
    }

    if (typeof value === "object") {
      const candidates = [
        value.display_value,
        value.displayName,
        value.name,
        value.value,
        value.id,
        value.sys_id
      ];

      for (const c of candidates) {
        const t = toText(c);
        if (t) return t;
      }

      return "";
    }

    return String(value).trim();
  }

  function getField(row, fieldName) {
    if (!row || !fieldName) return "";

    if (Object.prototype.hasOwnProperty.call(row, fieldName)) {
      return toText(row[fieldName]);
    }

    const parts = fieldName.split(".");
    let current = row;

    for (const part of parts) {
      if (current === null || current === undefined) return "";
      current = current[part];
    }

    return toText(current);
  }

  function firstNonBlank(...values) {
    for (const v of values) {
      const t = toText(v);
      if (t) return t;
    }
    return "";
  }

  function getShortName(value) {
    const v = String(value || "").trim();
    if (!v) return "";

    if (v.includes(".")) {
      return v.split(".")[0].trim();
    }

    return v;
  }

  function normalizeName(value) {
    return String(value || "")
      .trim()
      .replace(/^["']|["']$/g, "")
      .toLowerCase();
  }

  function buildAliases(ciName) {
    const aliases = [];
    const ci = String(ciName || "").trim();
    const shortName = getShortName(ci);

    if (ci) aliases.push(ci);

    if (shortName && normalizeName(shortName) !== normalizeName(ci)) {
      aliases.push(shortName);
    }

    return [...new Set(aliases.filter(Boolean))];
  }

  function isBadCiName(value) {
    const ci = String(value || "").trim();

    if (!ci) return true;
    if (ci.toUpperCase() === "N/A") return true;
    if (/^https?:\/\//i.test(ci)) return true;
    if (/^[0-9a-f]{32}$/i.test(ci)) return true;

    return false;
  }

  function getSlaValue(row) {
    return firstNonBlank(
      getField(row, "u_sla"),
      getField(row, "u_sla_due"),
      getField(row, "sla"),
      getField(row, "sla_due"),
      getField(row, "due_date"),
      getField(row, "made_sla")
    ) || "N/A";
  }

  const dtskRows = extractRows(snowResult);
  const workItems = [];

  for (const row of dtskRows) {

    const sysId = firstNonBlank(
      getField(row, "sys_id"),
      getField(row, "sysId")
    );

    const dtsk = firstNonBlank(
      getField(row, "number")
    ) || "N/A";

    const shortDescription = firstNonBlank(
      getField(row, "short_description")
    ) || "N/A";

    const state = firstNonBlank(
      getField(row, "state")
    ) || "N/A";

    const createdOn = firstNonBlank(
      getField(row, "sys_created_on"),
      getField(row, "opened_at")
    ) || "N/A";

    const assignmentGroup = firstNonBlank(
      getField(row, "assignment_group.name"),
      getField(row, "assignment_group_name")
    ) || "N/A";

    const assignedTo = firstNonBlank(
      getField(row, "assigned_to.name"),
      getField(row, "assigned_to_name")
    ) || "N/A";

    const sla = getSlaValue(row);

    const decomRequest = firstNonBlank(
      getField(row, "decom_request.number"),
      getField(row, "decom_request_number")
    ) || "N/A";

    const ciName = firstNonBlank(
      getField(row, "decom_request.ci_name.name"),
      getField(row, "decom_request_ci_name_name"),
      getField(row, "ci_name"),
      getField(row, "cmdb_ci.name")
    ) || "N/A";

    const aliases = buildAliases(ciName);
    const ciValid = !isBadCiName(ciName);

    workItems.push({
      sysId,
      dtsk,
      decomRequest,
      ciName,
      aliases,
      ciValid,
      assignedTo,
      assignmentGroup,
      assignmentAction: assignedTo === "N/A" ? "Please assign" : "Assigned",
      sla,
      createdOn,
      state,
      shortDescription
    });
  }

  const summary = {
    totalDtsks: workItems.length,
    validCiCount: workItems.filter(x => x.ciValid).length,
    invalidCiCount: workItems.filter(x => !x.ciValid).length,
    assignedCount: workItems.filter(x => x.assignmentAction === "Assigned").length,
    unassignedCount: workItems.filter(x => x.assignmentAction === "Please assign").length,
    slaPopulatedCount: workItems.filter(x => x.sla && x.sla !== "N/A").length,
    slaMissingCount: workItems.filter(x => !x.sla || x.sla === "N/A").length
  };

  console.log("==== DTSK PREPARE WORK ITEMS SUMMARY ====");
  console.log(JSON.stringify(summary, null, 2));

  console.log("==== WORK ITEMS ====");
  console.log(JSON.stringify(workItems, null, 2));

  return {
    workItems,
    summary
  };
}
