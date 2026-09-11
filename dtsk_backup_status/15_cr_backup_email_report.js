// ==========================================================
// Dynatrace JS Task
// Task name: cr_backup_email_report
// Purpose:
// - Runs after cr_backup_validate_one_ci loop
// - Aggregates all validation rows for the current CR
// - Produces ONE email row per CI/server
// - Consolidates multiple Cohesity clusters / protection groups into cells
// - Adds serial number for readability
// - Does not call ServiceNow or Cohesity
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {

  function asArray(value) {
    if (Array.isArray(value)) return value;
    if (value === null || value === undefined) return [];
    return [value];
  }

  function safeText(value, fallback = "N/A") {
    if (value === null || value === undefined) return fallback;
    const text = String(value).trim();
    return text || fallback;
  }

  function markdownEscape(value) {
    return safeText(value).replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
  }

  function uniqueText(values, fallback = "-") {
    const seen = new Set();
    const out = [];

    for (const value of values || []) {
      const text = String(value ?? "").trim();
      if (!text || ["N/A", "-", "NoBackupFound", "NoBackup", "NoBackupTime"].includes(text)) continue;
      const key = text.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(text);
    }

    return out.length ? out.join(", ") : fallback;
  }

  // Dynatrace loop outputs can be wrapped in arrays/objects. Find every
  // validator result containing a rows array without assuming one wrapper shape.
  function extractValidationOutputs(value, out = [], depth = 0, seen = new Set()) {
    if (depth > 12 || value === null || value === undefined) return out;
    if (typeof value !== "object") return out;
    if (seen.has(value)) return out;
    seen.add(value);

    if (Array.isArray(value)) {
      for (const item of value) extractValidationOutputs(item, out, depth + 1, seen);
      return out;
    }

    if (Array.isArray(value.rows)) {
      out.push(value);
      return out;
    }

    for (const key of ["results", "result", "outputs", "output", "values", "items", "executions", "tasks", "data"]) {
      if (value[key] !== undefined) extractValidationOutputs(value[key], out, depth + 1, seen);
    }

    return out;
  }

  function normalizeStatus(row, validationState) {
    const type = safeText(row?.BackupType, "Unknown");

    if (validationState === "ValidationError" || type === "ValidationError") return "Unable to Validate";
    if (type === "NoObject") return "No Backup Found";
    if (type === "NoFSBackupFound") return "DB Only / No Server Backup";
    if (type === "NoDBBackupFound") return "Review Required";
    if (["FS", "VM", "HyperV", "Nutanix", "SQL", "Oracle"].includes(type)) return "Protected";
    return safeText(validationState, "Unknown");
  }

  function backupTypeDisplay(value) {
    const type = safeText(value, "Unknown");
    const map = {
      HyperV: "Hyper-V",
      Nutanix: "Nutanix/AHV",
      NoObject: "-",
      NoFSBackupFound: "DB Only",
      NoDBBackupFound: "Server Backup Only",
      ValidationError: "-"
    };
    return map[type] || type;
  }

  function overallStatus(statuses) {
    const set = new Set(statuses || []);
    if (set.has("Unable to Validate")) return "Unable to Validate";
    if (set.has("Review Required")) return "Review Required";
    if (set.has("DB Only / No Server Backup")) return "DB Only / No Server Backup";
    if (set.has("Protected")) return "Protected";
    if (set.has("No Backup Found")) return "No Backup Found";
    return "Unknown";
  }

  function latestBackupValue(rows) {
    // Keep the newest valid backup timestamp if the CI has multiple rows.
    // Current validator emits MM/DD/YYYY HH:mm:ss ET-style text.
    let bestText = "-";
    let bestTime = -1;

    for (const row of rows || []) {
      const text = String(row?.LastBackupTime || "").trim();
      if (!text || ["N/A", "-", "NoBackupFound", "NoBackup", "NoBackupTime", "ValidationError"].includes(text)) continue;

      const parsed = Date.parse(text);
      if (Number.isFinite(parsed) && parsed > bestTime) {
        bestTime = parsed;
        bestText = text;
      } else if (bestTime < 0 && bestText === "-") {
        bestText = text;
      }
    }

    return bestText;
  }

  const raw = await result("cr_backup_validate_one_ci");
  const validationOutputs = extractValidationOutputs(raw);

  const rawRows = [];
  const warnings = [];

  for (const output of validationOutputs) {
    const validationState = safeText(output?.validationState, "Unknown");
    const summaryCiName = safeText(output?.summary?.ciName, "N/A");

    for (const row of asArray(output?.rows)) {
      rawRows.push({
        ServerName: safeText(row?.ServerName, summaryCiName),
        BackupType: safeText(row?.BackupType, "Unknown"),
        BackupTypeDisplay: backupTypeDisplay(row?.BackupType),
        ObjectName: safeText(row?.ObjectName),
        ClusterName: safeText(row?.ClusterName),
        ProtectionGroup: safeText(row?.ProtectionGroup, "-"),
        LastBackupTime: safeText(row?.LastBackupTime),
        Status: normalizeStatus(row, validationState)
      });
    }

    // Defensive fallback: if a validator result has no rows, still represent the CI.
    if (asArray(output?.rows).length === 0 && summaryCiName !== "N/A") {
      rawRows.push({
        ServerName: summaryCiName,
        BackupType: validationState === "ValidationError" ? "ValidationError" : "Unknown",
        BackupTypeDisplay: "-",
        ObjectName: summaryCiName,
        ClusterName: "N/A",
        ProtectionGroup: "-",
        LastBackupTime: "N/A",
        Status: validationState === "ValidationError" ? "Unable to Validate" : validationState
      });
    }

    for (const warning of asArray(output?.warnings)) {
      const text = safeText(warning, "");
      if (text) warnings.push(text);
    }
  }

  // One row per CI/server. Multiple Cohesity findings are consolidated into
  // comma-separated values rather than producing repeated server rows.
  const byServer = new Map();

  for (const row of rawRows) {
    const server = safeText(row.ServerName, "N/A");
    const key = server.toLowerCase();
    if (!byServer.has(key)) byServer.set(key, { ServerName: server, rows: [] });
    byServer.get(key).rows.push(row);
  }

  const finalRows = [...byServer.values()]
    .map(group => {
      const status = overallStatus(group.rows.map(r => r.Status));
      const noBackup = status === "No Backup Found";
      const unable = status === "Unable to Validate";

      return {
        ServerName: group.ServerName,
        BackupType: (noBackup || unable) ? "-" : uniqueText(group.rows.map(r => r.BackupTypeDisplay), "-"),
        Clusters: (noBackup || unable) ? "-" : uniqueText(group.rows.map(r => r.ClusterName), "-"),
        ProtectionGroups: (noBackup || unable) ? "-" : uniqueText(group.rows.map(r => r.ProtectionGroup), "-"),
        LatestBackup: (noBackup || unable) ? "-" : latestBackupValue(group.rows),
        Status: status
      };
    })
    .sort((a, b) => String(a.ServerName).localeCompare(String(b.ServerName)));

  const summary = {
    ciCount: finalRows.length,
    protectedCiCount: finalRows.filter(r => r.Status === "Protected").length,
    noBackupCiCount: finalRows.filter(r => r.Status === "No Backup Found").length,
    reviewRequiredCiCount: finalRows.filter(r => ["Review Required", "DB Only / No Server Backup"].includes(r.Status)).length,
    unableToValidateCiCount: finalRows.filter(r => r.Status === "Unable to Validate").length,
    warningCount: [...new Set(warnings)].length
  };

  const lines = [];
  lines.push("# Cohesity Backup Validation");
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push("| Metric | Count |");
  lines.push("|---|---:|");
  lines.push(`| CIs reviewed | ${summary.ciCount} |`);
  lines.push(`| Protected | ${summary.protectedCiCount} |`);
  lines.push(`| No Backup Found | ${summary.noBackupCiCount} |`);
  lines.push(`| Review Required | ${summary.reviewRequiredCiCount} |`);
  lines.push(`| Unable to Validate | ${summary.unableToValidateCiCount} |`);
  lines.push("");
  lines.push("## Details");
  lines.push("");
  lines.push("| Sl No | Server | Backup Type | Cluster(s) | Protection Group(s) | Latest Backup | Status |");
  lines.push("|---:|---|---|---|---|---|---|");

  if (finalRows.length === 0) {
    lines.push("| 1 | N/A | - | - | - | - | No validation rows returned |");
  } else {
    finalRows.forEach((row, index) => {
      lines.push(`| ${index + 1} | ${markdownEscape(row.ServerName)} | ${markdownEscape(row.BackupType)} | ${markdownEscape(row.Clusters)} | ${markdownEscape(row.ProtectionGroups)} | ${markdownEscape(row.LatestBackup)} | ${markdownEscape(row.Status)} |`);
    });
  }

  lines.push("");
  lines.push("NOTE:");
  lines.push("- One row is shown per CI/server.");
  lines.push("- If a CI is protected on multiple Cohesity clusters or protection groups, they are consolidated in the same row.");
  lines.push("- NAS backups are excluded from this server validation.");
  lines.push("- No Backup Found is reported only when the Cohesity search completed without a validation-system failure.");
  lines.push("- Unable to Validate indicates a Cohesity/cluster/API validation issue and must not be interpreted as no backup.");

  if (warnings.length > 0) {
    lines.push("");
    lines.push("## Validation Warnings");
    lines.push("");
    for (const warning of [...new Set(warnings)]) lines.push(`- ${markdownEscape(warning)}`);
  }

  return {
    markdown: lines.join("\n"),
    summary,
    rows: finalRows,
    warnings: [...new Set(warnings)]
  };
}
