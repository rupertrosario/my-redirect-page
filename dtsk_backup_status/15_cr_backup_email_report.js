// ==========================================================
// Dynatrace JS Task
// Task name: cr_backup_email_report
// Purpose:
// - Runs after cr_backup_validate_one_ci loop
// - Builds the final Markdown email body for the current CR
// - One row per CI + backup type
// - Same backup type across multiple clusters is consolidated into one row
// - Different backup types remain separate rows
// - Sl No is shown only on the first row for each CI; following type rows are blank
// - No Cohesity or ServiceNow writes
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {
  const PROTECTED_TYPES = ["FS", "VM", "HyperV", "Nutanix", "SQL", "Oracle"];

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
      if (!text || ["N/A", "-", "NoBackupFound", "NoBackup", "NoBackupTime", "ValidationError"].includes(text)) continue;
      const key = text.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(text);
    }

    return out.length ? out.join(", ") : fallback;
  }

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

  function displayType(type) {
    const map = {
      HyperV: "Hyper-V",
      Nutanix: "Nutanix/AHV",
      NoObject: "-",
      NoFSBackupFound: "DB Only",
      NoDBBackupFound: "Server Backup Only",
      ValidationError: "-",
      Unknown: "-"
    };
    return map[type] || type;
  }

  function rowStatus(type, validationState) {
    if (validationState === "ValidationError" || type === "ValidationError") return "Unable to Validate";
    if (type === "NoObject") return "No Backup Found";
    if (type === "NoFSBackupFound") return "DB Only / No Server Backup";
    if (type === "NoDBBackupFound") return "Review Required";
    if (PROTECTED_TYPES.includes(type)) return "Protected";
    return safeText(validationState, "Unknown");
  }

  function statusPriority(status) {
    const rank = {
      "Unable to Validate": 1,
      "Review Required": 2,
      "DB Only / No Server Backup": 3,
      "Protected": 4,
      "No Backup Found": 5,
      "Unknown": 9
    };
    return rank[status] ?? 9;
  }

  function overallCiStatus(rows) {
    const statuses = [...new Set((rows || []).map(r => r.Status))];
    if (statuses.length === 0) return "Unknown";
    statuses.sort((a, b) => statusPriority(a) - statusPriority(b));
    return statuses[0];
  }

  function latestBackupValue(rows) {
    let bestText = "-";
    let bestUsecs = 0;

    for (const row of rows || []) {
      const usecs = Number(row?.LastBackupUsecs || 0);
      const text = String(row?.LastBackupTime || "").trim();
      if (!text || ["N/A", "-", "NoBackupFound", "NoBackup", "NoBackupTime", "ValidationError"].includes(text)) continue;

      if (Number.isFinite(usecs) && usecs > bestUsecs) {
        bestUsecs = usecs;
        bestText = text;
      } else if (bestUsecs === 0 && bestText === "-") {
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
    const outputRows = asArray(output?.rows);

    for (const row of outputRows) {
      const type = safeText(row?.BackupType, "Unknown");
      rawRows.push({
        ServerName: safeText(row?.ServerName, summaryCiName),
        BackupType: type,
        BackupTypeDisplay: displayType(type),
        ClusterName: safeText(row?.ClusterName),
        ProtectionGroup: safeText(row?.ProtectionGroup, "-"),
        LastBackupTime: safeText(row?.LastBackupTime),
        LastBackupUsecs: Number(row?.LastBackupUsecs || 0),
        Status: rowStatus(type, validationState)
      });
    }

    if (outputRows.length === 0 && summaryCiName !== "N/A") {
      const type = validationState === "ValidationError" ? "ValidationError" : "Unknown";
      rawRows.push({
        ServerName: summaryCiName,
        BackupType: type,
        BackupTypeDisplay: "-",
        ClusterName: "N/A",
        ProtectionGroup: "-",
        LastBackupTime: "N/A",
        LastBackupUsecs: 0,
        Status: rowStatus(type, validationState)
      });
    }

    for (const warning of asArray(output?.warnings)) {
      const text = safeText(warning, "");
      if (text) warnings.push(text);
    }
  }

  // Group by CI + backup type.
  // server01 VM on clusterA + clusterB => ONE VM row with both clusters.
  // server01 SQL                       => separate SQL row.
  const groups = new Map();

  for (const row of rawRows) {
    const serverKey = row.ServerName.toLowerCase();
    const typeKey = row.BackupType.toLowerCase();
    const key = `${serverKey}|${typeKey}`;

    if (!groups.has(key)) {
      groups.set(key, {
        ServerName: row.ServerName,
        BackupType: row.BackupType,
        BackupTypeDisplay: row.BackupTypeDisplay,
        rows: []
      });
    }

    groups.get(key).rows.push(row);
  }

  let finalRows = [...groups.values()].map(group => {
    const status = overallCiStatus(group.rows);
    const noBackupOrError = ["No Backup Found", "Unable to Validate"].includes(status);

    return {
      ServerName: group.ServerName,
      BackupTypeRaw: group.BackupType,
      BackupType: noBackupOrError ? "-" : group.BackupTypeDisplay,
      Clusters: noBackupOrError ? "-" : uniqueText(group.rows.map(r => r.ClusterName), "-"),
      ProtectionGroups: noBackupOrError ? "-" : uniqueText(group.rows.map(r => r.ProtectionGroup), "-"),
      LatestBackup: noBackupOrError ? "-" : latestBackupValue(group.rows),
      Status: status
    };
  });

  const typeRank = {
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

  finalRows.sort((a, b) => {
    const serverCompare = a.ServerName.localeCompare(b.ServerName);
    if (serverCompare !== 0) return serverCompare;
    return (typeRank[a.BackupTypeRaw] ?? 500) - (typeRank[b.BackupTypeRaw] ?? 500);
  });

  // Serial number advances once per CI.
  // Additional backup-type rows for the same CI deliberately leave Sl No blank.
  const seenServers = new Set();
  let nextSerial = 1;

  for (const row of finalRows) {
    const key = row.ServerName.toLowerCase();
    if (!seenServers.has(key)) {
      seenServers.add(key);
      row.SlNo = nextSerial++;
    } else {
      row.SlNo = "";
    }
  }

  // Summary counts are CI counts, not row counts.
  const byServer = new Map();
  for (const row of finalRows) {
    const key = row.ServerName.toLowerCase();
    if (!byServer.has(key)) byServer.set(key, []);
    byServer.get(key).push(row);
  }

  const ciStatuses = [...byServer.values()].map(rows => overallCiStatus(rows));

  const summary = {
    ciCount: byServer.size,
    detailRowCount: finalRows.length,
    protectedCiCount: ciStatuses.filter(s => s === "Protected").length,
    noBackupCiCount: ciStatuses.filter(s => s === "No Backup Found").length,
    reviewRequiredCiCount: ciStatuses.filter(s => ["Review Required", "DB Only / No Server Backup"].includes(s)).length,
    unableToValidateCiCount: ciStatuses.filter(s => s === "Unable to Validate").length,
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
    for (const row of finalRows) {
      lines.push(`| ${row.SlNo} | ${markdownEscape(row.ServerName)} | ${markdownEscape(row.BackupType)} | ${markdownEscape(row.Clusters)} | ${markdownEscape(row.ProtectionGroups)} | ${markdownEscape(row.LatestBackup)} | ${markdownEscape(row.Status)} |`);
    }
  }

  lines.push("");
  lines.push("NOTE:");
  lines.push("- Sl No is shown only on the first row for each CI/server; additional backup-type rows for that CI are left blank.");
  lines.push("- Multiple clusters/protection groups for the same backup type are consolidated into that type's row.");
  lines.push("- Different backup types for the same CI are shown on separate rows.");
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
