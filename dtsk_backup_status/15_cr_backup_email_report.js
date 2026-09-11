// ==========================================================
// Dynatrace JS Task
// Task name: cr_backup_email_report
// Purpose:
// - Runs after cr_backup_validate_one_ci loop
// - Aggregates all validation rows for the current CR
// - Preserves multiple rows for the same CI when protection exists on
//   multiple clusters / protection groups
// - Builds manager-friendly Markdown for the email task
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
      if (value[key] !== undefined) {
        extractValidationOutputs(value[key], out, depth + 1, seen);
      }
    }

    return out;
  }

  function normalizeStatus(row, validationState) {
    const type = safeText(row?.BackupType, "Unknown");

    if (validationState === "ValidationError" || type === "ValidationError") {
      return "Unable to Validate";
    }
    if (type === "NoObject") return "No Backup Found";
    if (type === "NoFSBackupFound") return "DB Only / No Server Backup";
    if (type === "NoDBBackupFound") return "Review Required";
    if (["FS", "VM", "HyperV", "Nutanix", "SQL", "Oracle"].includes(type)) {
      return "Protected";
    }
    return safeText(validationState, "Unknown");
  }

  function backupTypeDisplay(value) {
    const type = safeText(value, "Unknown");
    const map = {
      HyperV: "Hyper-V",
      Nutanix: "Nutanix/AHV",
      NoObject: "No Backup Found",
      NoFSBackupFound: "DB Only / No Server Backup",
      NoDBBackupFound: "Server Backup / No DB Backup",
      ValidationError: "Validation Error"
    };
    return map[type] || type;
  }

  function dedupeRows(rows) {
    const seen = new Set();
    const out = [];

    for (const row of rows) {
      // ClusterName + ProtectionGroup are deliberately part of the key.
      // Therefore the same server protected on two clusters remains two rows.
      const key = [
        row.ServerName,
        row.BackupType,
        row.ObjectName,
        row.SourceName,
        row.ClusterName,
        row.ProtectionGroup,
        row.LastBackupTime,
        row.Status
      ].map(v => String(v || "").toLowerCase()).join("|");

      if (seen.has(key)) continue;
      seen.add(key);
      out.push(row);
    }

    return out;
  }

  function sortRows(rows) {
    const rank = {
      FS: 10,
      VM: 20,
      HyperV: 30,
      Nutanix: 40,
      SQL: 50,
      Oracle: 60,
      NoFSBackupFound: 900,
      NoDBBackupFound: 910,
      NoObject: 950,
      ValidationError: 990,
      Unknown: 999
    };

    return [...rows].sort((a, b) => {
      const serverCompare = String(a.ServerName || "").localeCompare(String(b.ServerName || ""));
      if (serverCompare !== 0) return serverCompare;

      const typeCompare = (rank[a.BackupType] ?? 500) - (rank[b.BackupType] ?? 500);
      if (typeCompare !== 0) return typeCompare;

      const clusterCompare = String(a.ClusterName || "").localeCompare(String(b.ClusterName || ""));
      if (clusterCompare !== 0) return clusterCompare;

      return String(a.ProtectionGroup || "").localeCompare(String(b.ProtectionGroup || ""));
    });
  }

  const raw = await result("cr_backup_validate_one_ci");
  const validationOutputs = extractValidationOutputs(raw);

  const rows = [];
  const warnings = [];
  const ciNames = new Set();

  for (const output of validationOutputs) {
    const validationState = safeText(output?.validationState, "Unknown");
    const summaryCiName = safeText(output?.summary?.ciName, "N/A");
    if (summaryCiName !== "N/A") ciNames.add(summaryCiName.toLowerCase());

    for (const row of asArray(output?.rows)) {
      const serverName = safeText(row?.ServerName, summaryCiName);
      if (serverName !== "N/A") ciNames.add(serverName.toLowerCase());

      rows.push({
        ServerName: serverName,
        BackupType: safeText(row?.BackupType, "Unknown"),
        BackupTypeDisplay: backupTypeDisplay(row?.BackupType),
        ObjectName: safeText(row?.ObjectName),
        SourceName: safeText(row?.SourceName),
        ClusterName: safeText(row?.ClusterName),
        ProtectionGroup: safeText(row?.ProtectionGroup, "-"),
        LastBackupTime: safeText(row?.LastBackupTime),
        Status: normalizeStatus(row, validationState)
      });
    }

    for (const warning of asArray(output?.warnings)) {
      const text = safeText(warning, "");
      if (text) warnings.push(text);
    }
  }

  const finalRows = sortRows(dedupeRows(rows));

  const protectedCiNames = new Set(
    finalRows
      .filter(r => r.Status === "Protected")
      .map(r => String(r.ServerName || "").toLowerCase())
      .filter(Boolean)
  );

  const noBackupCiNames = new Set(
    finalRows
      .filter(r => r.Status === "No Backup Found")
      .map(r => String(r.ServerName || "").toLowerCase())
      .filter(Boolean)
  );

  const reviewCiNames = new Set(
    finalRows
      .filter(r => ["Review Required", "DB Only / No Server Backup"].includes(r.Status))
      .map(r => String(r.ServerName || "").toLowerCase())
      .filter(Boolean)
  );

  const unableCiNames = new Set(
    finalRows
      .filter(r => r.Status === "Unable to Validate")
      .map(r => String(r.ServerName || "").toLowerCase())
      .filter(Boolean)
  );

  const summary = {
    ciCount: ciNames.size,
    rowCount: finalRows.length,
    protectedCiCount: protectedCiNames.size,
    noBackupCiCount: noBackupCiNames.size,
    reviewRequiredCiCount: reviewCiNames.size,
    unableToValidateCiCount: unableCiNames.size,
    warningCount: warnings.length
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
  lines.push("| Server | Backup Type | Object | Cluster | Protection Group | Latest Backup | Status |");
  lines.push("|---|---|---|---|---|---|---|");

  if (finalRows.length === 0) {
    lines.push("| N/A | N/A | N/A | N/A | N/A | N/A | No validation rows returned |");
  } else {
    for (const row of finalRows) {
      lines.push(`| ${markdownEscape(row.ServerName)} | ${markdownEscape(row.BackupTypeDisplay)} | ${markdownEscape(row.ObjectName)} | ${markdownEscape(row.ClusterName)} | ${markdownEscape(row.ProtectionGroup)} | ${markdownEscape(row.LastBackupTime)} | ${markdownEscape(row.Status)} |`);
    }
  }

  lines.push("");
  lines.push("NOTE:");
  lines.push("- NAS backups are excluded from this server validation.");
  lines.push("- A CI protected on multiple Cohesity clusters or protection groups is shown on multiple detail rows.");
  lines.push("- No Backup Found is reported only when the Cohesity search completed without a validation-system failure.");
  lines.push("- Unable to Validate indicates a Cohesity/cluster/API validation issue and must not be interpreted as no backup.");

  if (warnings.length > 0) {
    lines.push("");
    lines.push("## Validation Warnings");
    lines.push("");
    for (const warning of [...new Set(warnings)]) {
      lines.push(`- ${markdownEscape(warning)}`);
    }
  }

  return {
    markdown: lines.join("\n"),
    summary,
    rows: finalRows,
    warnings: [...new Set(warnings)]
  };
}
