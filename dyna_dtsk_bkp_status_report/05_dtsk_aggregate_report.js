// ==========================================================
// Dynatrace JS Task
// Task name: dtsk_aggregate_report
// Phase: Aggregate validation rows only - no email, no ServiceNow update
//
// Purpose:
// - Runs after loop task dtsk_validate_one_ci
// - Collects all loop outputs
// - Flattens all rows into one simple report
// - Creates manager-friendly Markdown for email
// - Clearly reports when there are no active decommission DTSKs
//
// Workflow position:
// dtsk_snow_search
//   -> dtsk_prepare_work_items
//   -> dtsk_get_cluster_map
//   -> dtsk_validate_one_ci  (LOOP)
//   -> dtsk_aggregate_report (NO LOOP)
//
// Strictly read/aggregate only. No HTTP calls. No writes.
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";

export default async function () {

  function nowEt() {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    }).format(new Date()).replace(",", "");
  }

  function asArray(value) {
    if (Array.isArray(value)) return value;
    if (value === null || value === undefined) return [];
    return [value];
  }

  function safeText(value) {
    if (value === null || value === undefined) return "N/A";
    const s = String(value).trim();
    return s ? s : "N/A";
  }

  function markdownEscape(value) {
    return safeText(value)
      .replace(/\|/g, "\\|")
      .replace(/\r?\n/g, " ");
  }

  function normalizeRow(row) {
    const backupTypeRaw = safeText(row?.BackupType);
    const displayMap = {
      NoObject: "No Backup Found",
      NoFSBackupFound: "DB Only / No Server Backup",
      HyperV: "Hyper-V",
      Nutanix: "Nutanix/AHV"
    };

    return {
      DTSK: safeText(row?.DTSK),
      DecomRequest: safeText(row?.DecomRequest),
      ServerName: safeText(row?.ServerName),
      BackupType: backupTypeRaw,
      BackupTypeDisplay: displayMap[backupTypeRaw] || backupTypeRaw,
      ObjectName: safeText(row?.ObjectName),
      SourceName: safeText(row?.SourceName),
      ClusterName: safeText(row?.ClusterName),
      ProtectionGroup: safeText(row?.ProtectionGroup),
      LastBackupTime: safeText(row?.LastBackupTime)
    };
  }

  function dedupeRows(rows) {
    const seen = new Set();
    const out = [];

    for (const row of rows || []) {
      const key = [
        row.DTSK,
        row.DecomRequest,
        row.ServerName,
        row.BackupType,
        row.ObjectName,
        row.SourceName,
        row.ClusterName,
        row.ProtectionGroup,
        row.LastBackupTime
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
      NoObject: 950,
      Unknown: 999
    };

    return [...rows].sort((a, b) => {
      const serverCompare = String(a.ServerName || "").localeCompare(String(b.ServerName || ""));
      if (serverCompare !== 0) return serverCompare;

      const ra = rank[a.BackupType] ?? 500;
      const rb = rank[b.BackupType] ?? 500;
      if (ra !== rb) return ra - rb;

      const objectCompare = String(a.ObjectName || "").localeCompare(String(b.ObjectName || ""));
      if (objectCompare !== 0) return objectCompare;

      return String(a.ProtectionGroup || "").localeCompare(String(b.ProtectionGroup || ""));
    });
  }

  function countRows(rows, backupType) {
    return rows.filter(r => r.BackupType === backupType).length;
  }

  function distinctCount(rows, predicate, fieldName) {
    const values = new Set();
    for (const row of rows || []) {
      if (!predicate(row)) continue;
      const v = safeText(row?.[fieldName]);
      if (v !== "N/A") values.add(v.toLowerCase());
    }
    return values.size;
  }

  function extractValidationOutputs(value, out = [], depth = 0, seen = new Set()) {
    if (depth > 10 || value === null || value === undefined) return out;
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

    const preferredKeys = [
      "results", "result", "outputs", "output", "values", "items", "executions", "tasks", "data"
    ];

    for (const key of preferredKeys) {
      if (value[key] !== undefined) extractValidationOutputs(value[key], out, depth + 1, seen);
    }

    return out;
  }

  function getObjectKeys(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return [];
    return Object.keys(value);
  }

  function makeMarkdownTable(rows) {
    const headers = [
      ["DTSK", "DTSK"],
      ["Decom Request", "DecomRequest"],
      ["Server", "ServerName"],
      ["Backup Type", "BackupTypeDisplay"],
      ["Object", "ObjectName"],
      ["Source", "SourceName"],
      ["Cluster", "ClusterName"],
      ["Protection Group", "ProtectionGroup"],
      ["Latest Backup", "LastBackupTime"]
    ];

    const lines = [];
    lines.push(`| ${headers.map(h => h[0]).join(" | ")} |`);
    lines.push(`| ${headers.map(() => "---").join(" | ")} |`);

    for (const row of rows) {
      lines.push(`| ${headers.map(h => markdownEscape(row[h[1]])).join(" | ")} |`);
    }

    return lines.join("\n");
  }

  function makeNoDtskMarkdown(summary) {
    return [
      `# Cohesity Backup Validation - Decommission DTSKs`,
      `Generated: ${summary.generatedAtEt} ET`,
      "",
      "## Run Status",
      "| Status | Active Decommission DTSKs | Backup Validation |",
      "|---|---:|---|",
      "| **NO DTSKs** | **0** | **Not Required** |"
    ].join("\n");
  }

  function makeExecutiveSummaryMarkdown(summary) {
    return [
      "| Metric | Count |",
      "|---|---:|",
      `| **DTSKs reviewed** | **${summary.totalDtsks}** |`,
      `| **Total validation rows** | **${summary.totalRows}** |`,
      `| **Server-level protected CIs** | **${summary.serverLevelProtectedCiCount}** |`,
      `| **DB-protected CIs** | **${summary.dbProtectedCiCount}** |`,
      `| **No backup found** | **${summary.noObjectRowCount}** |`,
      `| **DB backup found but no server-level backup** | **${summary.noFsBackupFoundRowCount}** |`,
      `| **Warnings** | **${summary.warningCount}** |`
    ].join("\n");
  }

  function makeTypeMarkdown(summary) {
    return [
      "| Backup Type | Rows |",
      "|---|---:|",
      `| FS | ${summary.fsRowCount} |`,
      `| VM | ${summary.vmRowCount} |`,
      `| Hyper-V | ${summary.hyperVRowCount} |`,
      `| Nutanix/AHV | ${summary.nutanixRowCount} |`,
      `| SQL | ${summary.sqlRowCount} |`,
      `| Oracle | ${summary.oracleRowCount} |`,
      `| **No Backup Found** | **${summary.noObjectRowCount}** |`,
      `| **DB Only / No Server Backup** | **${summary.noFsBackupFoundRowCount}** |`,
      `| Unknown | ${summary.unknownRowCount} |`
    ].join("\n");
  }

  function makeNoteMarkdown() {
    return [
      "- NAS backups are excluded from this server decommission validation.",
      "- **No Backup Found** means no in-scope Cohesity backup object was found for the CI.",
      "- **DB Only / No Server Backup** means a SQL/Oracle backup was found, but no FS, VM, Hyper-V, or Nutanix/AHV backup was found for the server.",
      "- Servers with naming patterns such as `db` or `cn` may require DB-level backup review if only FS/VM backup is found."
    ].join("\n");
  }

  const prepareResult = await result("dtsk_prepare_work_items");
  const validateRaw = await result("dtsk_validate_one_ci");

  const workItems = asArray(prepareResult?.workItems);
  const validationOutputs = extractValidationOutputs(validateRaw);

  const warnings = [];
  const rows = [];

  let dbNamedServerCount = 0;
  let dbCnFallbackAppliedCount = 0;
  let dbCnFallbackRowsFound = 0;

  for (const output of validationOutputs) {
    for (const row of asArray(output?.rows)) rows.push(normalizeRow(row));

    const s = output?.summary || {};
    if (s.dbNamedServer === true) dbNamedServerCount += 1;
    if (s.dbCnFallbackApplied === true) dbCnFallbackAppliedCount += 1;
    dbCnFallbackRowsFound += Number(s.dbCnFallbackRowsFound || 0);

    for (const warning of asArray(output?.warnings)) {
      const text = safeText(warning);
      if (text !== "N/A") warnings.push(text);
    }
  }

  const finalRows = sortRows(dedupeRows(rows));

  const summary = {
    generatedAtEt: nowEt(),
    totalDtsks: workItems.length,
    validationOutputCount: validationOutputs.length,
    totalRows: finalRows.length,

    fsRowCount: countRows(finalRows, "FS"),
    vmRowCount: countRows(finalRows, "VM"),
    hyperVRowCount: countRows(finalRows, "HyperV"),
    nutanixRowCount: countRows(finalRows, "Nutanix"),
    sqlRowCount: countRows(finalRows, "SQL"),
    oracleRowCount: countRows(finalRows, "Oracle"),
    noFsBackupFoundRowCount: countRows(finalRows, "NoFSBackupFound"),
    noObjectRowCount: countRows(finalRows, "NoObject"),
    unknownRowCount: countRows(finalRows, "Unknown"),

    serverLevelProtectedCiCount: distinctCount(
      finalRows,
      r => ["FS", "VM", "HyperV", "Nutanix"].includes(r.BackupType),
      "ServerName"
    ),
    dbProtectedCiCount: distinctCount(
      finalRows,
      r => ["SQL", "Oracle"].includes(r.BackupType),
      "ServerName"
    ),

    dbNamedServerCount,
    dbCnFallbackAppliedCount,
    dbCnFallbackRowsFound,
    noActiveDecomDtsks: workItems.length === 0,
    warningCount: warnings.length,

    validateRawIsArray: Array.isArray(validateRaw),
    validateRawType: typeof validateRaw,
    validateRawKeys: getObjectKeys(validateRaw)
  };

  const markdown = summary.noActiveDecomDtsks
    ? makeNoDtskMarkdown(summary)
    : [
        `# Cohesity Backup Validation - Decommission DTSKs`,
        `Generated: ${summary.generatedAtEt} ET`,
        "",
        "## Executive Summary",
        makeExecutiveSummaryMarkdown(summary),
        "",
        "## Backup Type Summary",
        makeTypeMarkdown(summary),
        "",
        "## Details",
        finalRows.length > 0 ? makeMarkdownTable(finalRows) : "No rows returned.",
        "",
        "NOTE:",
        makeNoteMarkdown()
      ].join("\n");

  const output = {
    reportTitle: "Cohesity Backup Validation - Decommission DTSKs",
    generatedAtEt: summary.generatedAtEt,
    rows: finalRows,
    summary,
    warnings,
    markdown
  };

  console.log("==== DTSK AGGREGATE REPORT ====");
  console.log(JSON.stringify({ summary, rows: finalRows }, null, 2));
  console.log("==== DTSK AGGREGATE MARKDOWN ====");
  console.log(markdown);

  return output;
}
