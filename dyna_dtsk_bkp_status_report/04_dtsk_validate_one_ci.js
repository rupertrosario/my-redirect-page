// ==========================================================
// Dynatrace JS Task
// Task name: dtsk_validate_one_ci
// Phase: Real Cohesity validation - version 2
//
// Purpose:
// - Runs as LOOP task: one DTSK / one CI per iteration
// - Finds FS, SQL, Oracle, Hyper-V, Nutanix/AHV, and VM backups
// - Produces one output row per protected backup object / DB object
// - If SQL/Oracle DB backups exist but no FS backup exists, adds NoFSBackupFound
//
// Loop task settings:
// - Item variable name: workItem
// - List variable: workItems
// - Concurrency: 1 initially
//
// Strictly GET-only. Do not paste workflow expression braces inside this JS code.
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";
import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function (input = {}) {

  // -----------------------------
  // Config
  // -----------------------------
  const HELIOS_BASE_URL = "https://helios.cohesity.com";
  const COHESITY_API_KEY_CREDENTIAL_ID = "credentials_vault-312312";
  const OUTPUT_TIME_ZONE = "America/New_York";
  const GLOBAL_SEARCH_COUNT = 100;
  const FALLBACK_TO_ALL_CLUSTERS_WHEN_NO_GLOBAL_HIT = true;

  // -----------------------------
  // Basic helpers
  // -----------------------------
  function asArray(value) {
    if (Array.isArray(value)) return value;
    if (value === null || value === undefined) return [];
    return [value];
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
        value.objectName,
        value.entityName,
        value.hostName,
        value.value,
        value.id
      ];

      for (const c of candidates) {
        const t = toText(c);
        if (t) return t;
      }

      return "";
    }

    return String(value).trim();
  }

  function firstNonBlank(...values) {
    for (const v of values) {
      const t = toText(v);
      if (t) return t;
    }
    return "";
  }

  function normalizeName(value) {
    return String(value || "")
      .trim()
      .replace(/^['"]|['"]$/g, "")
      .toLowerCase();
  }

  function getShortName(value) {
    const v = String(value || "").trim();
    if (!v) return "";
    return v.includes(".") ? v.split(".")[0].trim() : v;
  }

  function uniqueStrings(values) {
    const out = [];
    const seen = new Set();

    for (const value of values || []) {
      const text = String(value || "").trim();
      if (!text) continue;

      const key = text.toLowerCase();
      if (seen.has(key)) continue;

      seen.add(key);
      out.push(text);
    }

    return out;
  }

  function buildAliases(workItem) {
    const aliases = [];

    for (const a of asArray(workItem?.aliases)) {
      if (a) aliases.push(a);
    }

    if (workItem?.ciName) aliases.push(workItem.ciName);

    const shortName = getShortName(workItem?.ciName);
    if (shortName) aliases.push(shortName);

    return uniqueStrings(aliases);
  }

  function namesMatchAnyAlias(name, aliases) {
    const n = normalizeName(name);
    if (!n) return false;

    for (const alias of aliases || []) {
      const a = normalizeName(alias);
      const s = normalizeName(getShortName(alias));

      if (!a) continue;
      if (n === a) return true;
      if (s && n === s) return true;
      if (n.includes(a)) return true;
      if (s && n.includes(s)) return true;
    }

    return false;
  }

  function getWorkItem(runtimeInput) {
    if (!runtimeInput) return null;

    return runtimeInput.workItem ||
      runtimeInput.item ||
      runtimeInput.loopItem ||
      runtimeInput.loopItemValue ||
      runtimeInput.value ||
      (runtimeInput.dtsk && runtimeInput.ciName ? runtimeInput : null) ||
      null;
  }

  function isBadCiName(value) {
    const ci = String(value || "").trim();

    if (!ci) return true;
    if (ci.toUpperCase() === "N/A") return true;
    if (/^https?:\/\//i.test(ci)) return true;
    if (/^[0-9a-f]{32}$/i.test(ci)) return true;

    return false;
  }

  // -----------------------------
  // Time helpers
  // -----------------------------
  function usecsToEt(usecs) {
    const n = Number(usecs);
    if (!Number.isFinite(n) || n <= 0) return "";

    const millis = Math.floor(n / 1000);
    const date = new Date(millis);

    if (Number.isNaN(date.getTime())) return "";

    return new Intl.DateTimeFormat("en-US", {
      timeZone: OUTPUT_TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    }).format(date).replace(",", "");
  }

  function findMaxUsecs(value, depth = 0) {
    if (depth > 8 || value === null || value === undefined) return 0;

    let max = 0;

    if (typeof value === "number") {
      if (value > 1000000000000000) return value;
      return 0;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        max = Math.max(max, findMaxUsecs(item, depth + 1));
      }
      return max;
    }

    if (typeof value === "object") {
      for (const [key, val] of Object.entries(value)) {
        const keyLower = key.toLowerCase();

        if (keyLower.includes("usecs") || keyLower.includes("timeusec")) {
          const n = Number(val);
          if (Number.isFinite(n) && n > max) max = n;
        }

        max = Math.max(max, findMaxUsecs(val, depth + 1));
      }
    }

    return max;
  }

  // -----------------------------
  // API helpers
  // -----------------------------
  async function getApiKey() {
    const cred = await credentialVaultClient.getCredentialsDetails({
      id: COHESITY_API_KEY_CREDENTIAL_ID
    });

    const token = cred?.token;
    if (!token) {
      throw new Error("Cohesity API key token was not returned from Dynatrace Credential Vault.");
    }

    return token;
  }

  async function getJson(url, headers) {
    const response = await fetch(url, {
      method: "GET",
      headers
    });

    if (!response.ok) {
      const bodyText = await response.text().catch(() => "");
      throw new Error(
        `GET failed: ${response.status} ${response.statusText} URL=${url} BODY=${bodyText.substring(0, 300)}`
      );
    }

    return await response.json();
  }

  // -----------------------------
  // Cohesity object helpers
  // -----------------------------
  function getClusterName(clusterMap, clusterId) {
    const cid = String(clusterId || "").trim();
    if (!cid) return "Unknown";
    return clusterMap?.[cid] || `Unknown-${cid}`;
  }

  function getClusterIdFromNode(node) {
    return firstNonBlank(
      node?.clusterId,
      node?.clusterID,
      node?.clusterIdentifier?.clusterId,
      node?.entity?.clusterId,
      node?.object?.clusterId,
      node?.protectionSource?.clusterId,
      node?.sourceInfo?.clusterId
    );
  }

  function getObjectName(node) {
    return firstNonBlank(
      node?.name,
      node?.displayName,
      node?.objectName,
      node?.entityName,
      node?.hostName,
      node?.object?.name,
      node?.entity?.name,
      node?.protectionSource?.name,
      node?.sourceInfo?.name
    );
  }

  function getSourceName(node) {
    return firstNonBlank(
      node?.mssqlParams?.hostInfo?.name,
      node?.sqlParams?.hostInfo?.name,
      node?.oracleParams?.hostInfo?.name,
      node?.hostInfo?.name,
      node?.parentSource?.name,
      node?.sourceInfo?.name,
      node?.protectionSource?.name,
      node?.hostName
    ) || "N/A";
  }

  function getProtectionGroupName(node) {
    return firstNonBlank(
      node?.protectionGroupName,
      node?.jobName,
      node?.protectionGroup?.name,
      node?.job?.name,
      node?.protectionJobName,
      node?.groupName
    ) || "N/A";
  }

  function getSignalText(node) {
    const signalValues = [
      node?.environment,
      node?.type,
      node?.objectType,
      node?.entityType,
      node?.sourceType,
      node?.protectionSource?.environment,
      node?.protectionSource?.type,
      node?.sourceInfo?.environment,
      node?.sourceInfo?.type,
      node?.object?.environment,
      node?.object?.type,
      getObjectName(node)
    ];

    return signalValues
      .map(v => String(v || "").toLowerCase())
      .filter(Boolean)
      .join(" ");
  }

  function detectBackupType(node) {
    const signal = getSignalText(node);

    // Strong parameter signals first.
    if (node?.mssqlParams || node?.sqlParams) return "SQL";
    if (node?.oracleParams) return "Oracle";

    // Environment/type/name signals only. Do not inspect JSON key names.
    if (/\bksql\b|\bsql\b|mssql|sqlserver/.test(signal)) return "SQL";
    if (/\bkoracle\b|\boracle\b|racdatabase|oracleinstance/.test(signal)) return "Oracle";
    if (/khyperv|hyperv/.test(signal)) return "HyperV";
    if (/kacropolis|nutanix|\bahv\b/.test(signal)) return "Nutanix";
    if (/kvmware|virtualmachine|\bvm\b|vmware/.test(signal)) return "VM";
    if (/kphysical|physical|file|volume|filesystem|\bfs\b/.test(signal)) return "FS";

    return "Unknown";
  }

  function isOracleContainerRow(node) {
    const name = normalizeName(getObjectName(node));
    const signal = getSignalText(node);

    if (!name) return false;

    if (name === "oracle servers") return true;
    if (signal.includes("kracdatabase") && !node?.oracleParams?.hostInfo?.name) return true;
    if (signal.includes("oracle servers") && !node?.oracleParams?.hostInfo?.name) return true;

    return false;
  }

  function objectBelongsToCi(node, aliases, backupType) {
    const objectName = getObjectName(node);
    const sourceName = getSourceName(node);

    if (["SQL", "Oracle"].includes(backupType)) {
      // DB object name is usually the database name, not the server name.
      // For DB rows, match primarily by host/source name.
      return namesMatchAnyAlias(sourceName, aliases);
    }

    // FS/VM/HyperV/Nutanix rows normally match object or source.
    return namesMatchAnyAlias(objectName, aliases) || namesMatchAnyAlias(sourceName, aliases);
  }

  function collectObjectsDeep(value, out = [], depth = 0) {
    if (depth > 8 || value === null || value === undefined) return out;

    if (Array.isArray(value)) {
      for (const item of value) collectObjectsDeep(item, out, depth + 1);
      return out;
    }

    if (typeof value !== "object") return out;

    const possibleName = getObjectName(value);
    const hasUsefulFields = Boolean(
      possibleName ||
      value.clusterId ||
      value.clusterIdentifier ||
      value.protectionGroupName ||
      value.jobName ||
      value.environment ||
      value.type ||
      value.objectType ||
      value.mssqlParams ||
      value.sqlParams ||
      value.oracleParams ||
      value.localSnapshotInfo ||
      value.snapshotInfo
    );

    if (hasUsefulFields) out.push(value);

    for (const val of Object.values(value)) {
      collectObjectsDeep(val, out, depth + 1);
    }

    return out;
  }

  async function searchGlobalObjects(apiKey, aliases) {
    const all = [];
    const warnings = [];

    for (const term of aliases) {
      const url = `${HELIOS_BASE_URL}/v2/data-protect/search/objects?searchString=${encodeURIComponent(term)}&includeTenants=true&count=${GLOBAL_SEARCH_COUNT}`;

      try {
        const json = await getJson(url, {
          accept: "application/json",
          apiKey
        });

        all.push(...collectObjectsDeep(json));
      } catch (e) {
        warnings.push(`Global search failed for ${term}: ${e.message}`);
      }
    }

    return { objects: all, warnings };
  }

  async function searchProtectedObjects(apiKey, clusterId, aliases) {
    const all = [];
    const warnings = [];

    for (const term of aliases) {
      const url = `${HELIOS_BASE_URL}/v2/data-protect/search/protected-objects?searchString=${encodeURIComponent(term)}`;

      try {
        const json = await getJson(url, {
          accept: "application/json",
          apiKey,
          accessClusterId: String(clusterId)
        });

        all.push(...collectObjectsDeep(json));
      } catch (e) {
        warnings.push(`Protected search failed for ${term} on cluster ${clusterId}: ${e.message}`);
      }
    }

    return { objects: all, warnings };
  }

  function getCandidateClusterIds(globalObjects, aliases, allClusters) {
    const ids = [];

    for (const node of globalObjects || []) {
      const name = getObjectName(node);
      const source = getSourceName(node);

      if (!namesMatchAnyAlias(name, aliases) && !namesMatchAnyAlias(source, aliases)) continue;

      const cid = getClusterIdFromNode(node);
      if (cid) ids.push(String(cid));
    }

    const unique = uniqueStrings(ids);
    if (unique.length > 0) return unique;

    if (FALLBACK_TO_ALL_CLUSTERS_WHEN_NO_GLOBAL_HIT) {
      return uniqueStrings((allClusters || []).map(c => c.clusterId));
    }

    return [];
  }

  function makeBaseRow(workItem, backupType, objectName, sourceName, clusterName, protectionGroup, lastBackupTime) {
    return {
      DTSK: workItem?.dtsk || "N/A",
      DecomRequest: workItem?.decomRequest || "N/A",
      AssignedTo: workItem?.assignedTo || "N/A",
      AssignmentAction: workItem?.assignmentAction || "N/A",
      ServerName: workItem?.ciName || "N/A",
      BackupType: backupType || "Unknown",
      ObjectName: objectName || "N/A",
      SourceName: sourceName || "N/A",
      ClusterName: clusterName || "N/A",
      ProtectionGroup: protectionGroup || "N/A",
      LastBackupTime: lastBackupTime || "N/A"
    };
  }

  function dedupeRows(rows) {
    const seen = new Set();
    const out = [];

    for (const row of rows || []) {
      const key = [
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
      const ra = rank[a.BackupType] ?? 500;
      const rb = rank[b.BackupType] ?? 500;
      if (ra !== rb) return ra - rb;
      return String(a.ObjectName || "").localeCompare(String(b.ObjectName || ""));
    });
  }

  // -----------------------------
  // Main
  // -----------------------------
  const workItem = getWorkItem(input);
  const clusterData = await result("dtsk_get_cluster_map");
  const clusters = Array.isArray(clusterData?.clusters) ? clusterData.clusters : [];
  const clusterMap = clusterData?.clusterMap || {};

  if (!workItem) {
    return {
      validationState: "LoopItemMissing",
      rows: [],
      summary: {
        error: "Loop item was not available to dtsk_validate_one_ci.",
        inputKeys: input && typeof input === "object" ? Object.keys(input) : []
      }
    };
  }

  const aliases = buildAliases(workItem);

  if (isBadCiName(workItem.ciName)) {
    const row = makeBaseRow(workItem, "Unknown", "InvalidCI", "N/A", "N/A", "N/A", "InvalidCI");

    return {
      validationState: "InvalidCI",
      workItem,
      aliases,
      rows: [row],
      summary: {
        dtsk: workItem.dtsk || "N/A",
        ciName: workItem.ciName || "N/A",
        rowCount: 1
      },
      warnings: []
    };
  }

  if (clusters.length === 0) {
    throw new Error("No Cohesity clusters available from dtsk_get_cluster_map.");
  }

  const apiKey = await getApiKey();
  const warnings = [];

  const globalResult = await searchGlobalObjects(apiKey, aliases);
  warnings.push(...globalResult.warnings);

  const candidateClusterIds = getCandidateClusterIds(globalResult.objects, aliases, clusters);
  const protectedObjects = [];

  for (const clusterId of candidateClusterIds) {
    const protectedResult = await searchProtectedObjects(apiKey, clusterId, aliases);
    warnings.push(...protectedResult.warnings);

    for (const node of protectedResult.objects) {
      protectedObjects.push({ clusterId, node });
    }
  }

  let rows = [];
  let skippedOracleContainerRows = 0;

  for (const item of protectedObjects) {
    const node = item.node;
    const backupType = detectBackupType(node);

    if (backupType === "Oracle" && isOracleContainerRow(node)) {
      skippedOracleContainerRows++;
      continue;
    }

    if (!objectBelongsToCi(node, aliases, backupType)) {
      continue;
    }

    const objectName = getObjectName(node);
    const sourceName = getSourceName(node);
    const maxUsecs = findMaxUsecs(node);
    const lastBackupTime = usecsToEt(maxUsecs) || "NoBackupTime";

    rows.push(makeBaseRow(
      workItem,
      backupType,
      objectName || sourceName || workItem.ciName,
      sourceName,
      getClusterName(clusterMap, item.clusterId),
      getProtectionGroupName(node),
      lastBackupTime
    ));
  }

  rows = dedupeRows(rows);

  const hasDbBackup = rows.some(r => ["SQL", "Oracle"].includes(r.BackupType));
  const hasFsBackup = rows.some(r => r.BackupType === "FS");

  if (hasDbBackup && !hasFsBackup) {
    rows.push(makeBaseRow(
      workItem,
      "NoFSBackupFound",
      workItem.ciName,
      workItem.ciName,
      "N/A",
      "N/A",
      "NoFSBackupFound"
    ));
  }

  if (rows.length === 0) {
    rows = [makeBaseRow(
      workItem,
      "NoObject",
      workItem.ciName,
      "N/A",
      candidateClusterIds.length > 0 ? `${candidateClusterIds.length} cluster(s) checked` : "N/A",
      "N/A",
      "NoBackupFound"
    )];
  }

  rows = sortRows(rows);

  const validationState = rows.some(r => !["NoObject", "Unknown"].includes(r.BackupType))
    ? "Validated"
    : "NoBackupFound";

  const output = {
    validationState,
    workItem,
    aliases,
    rows,
    summary: {
      dtsk: workItem.dtsk || "N/A",
      ciName: workItem.ciName || "N/A",
      aliasCount: aliases.length,
      globalObjectCount: globalResult.objects.length,
      candidateClusterCount: candidateClusterIds.length,
      protectedObjectCount: protectedObjects.length,
      rowCount: rows.length,
      fsRowCount: rows.filter(r => r.BackupType === "FS").length,
      sqlRowCount: rows.filter(r => r.BackupType === "SQL").length,
      oracleRowCount: rows.filter(r => r.BackupType === "Oracle").length,
      vmRowCount: rows.filter(r => ["VM", "HyperV", "Nutanix"].includes(r.BackupType)).length,
      noFsBackupFound: rows.some(r => r.BackupType === "NoFSBackupFound"),
      skippedOracleContainerRows,
      clustersChecked: candidateClusterIds.map(cid => ({
        clusterId: cid,
        clusterName: getClusterName(clusterMap, cid)
      }))
    },
    warnings
  };

  console.log("==== DTSK VALIDATE ONE CI RESULT ====");
  console.log(JSON.stringify(output, null, 2));

  return output;
}
