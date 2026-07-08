// ==========================================================
// Dynatrace JS Task
// Task name: dtsk_validate_one_ci
// Phase: Real Cohesity validation - version 7
//
// Corrected to match PowerShell logic:
// - Uses protected-objects search as source of protected object rows
// - Flattens childObjects and DB params
// - Gets ProtectionGroup from latestSnapshotsInfo/protectionGroupName first
// - Adds DB/CN fallback search for SQL/Oracle objects across all clusters
// - Keeps assignment ownership fields from ServiceNow work item
// - Does not place diagnostic cluster-count text in the email Cluster column
// - GET only
// ==========================================================

import { result } from "@dynatrace-sdk/automation-utils";
import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function (input = {}) {
  const HELIOS_BASE_URL = "https://helios.cohesity.com";
  const COHESITY_API_KEY_CREDENTIAL_ID = "credentials_vault-312312";
  const OUTPUT_TIME_ZONE = "America/New_York";
  const GLOBAL_SEARCH_COUNT = 100;
  const FALLBACK_WHEN_NO_GLOBAL_OBJECT = true;

  const DB_NAMED_SERVER_PATTERN = /db|cn/i;
  const SERVER_LEVEL_BACKUP_TYPES = ["FS", "VM", "HyperV", "Nutanix"];
  const DB_BACKUP_TYPES = ["SQL", "Oracle"];
  const IN_SCOPE_BACKUP_TYPES = ["FS", "VM", "HyperV", "Nutanix", "SQL", "Oracle"];

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
      for (const v of [
        value.display_value, value.displayName, value.name, value.value, value.id, value.uid,
        value.clusterId, value.clusterID, value.sourceClusterId, value.sourceClusterID, value.accessClusterId,
        value.protectionGroupId, value.groupId, value.jobId
      ]) {
        const t = toText(v);
        if (t) return t;
      }
      return "";
    }
    return String(value).trim();
  }

  function safeText(value, fallback = "N/A") {
    const t = toText(value);
    return t ? t : fallback;
  }

  function firstNonBlank(...values) {
    for (const v of values) {
      const t = toText(v);
      if (t) return t;
    }
    return "";
  }

  function normalizeName(value) {
    return String(value || "").trim().replace(/^['"]|['"]$/g, "").toLowerCase();
  }

  function shortName(value) {
    const v = String(value || "").trim();
    if (!v) return "";
    return v.includes(".") ? v.split(".")[0].trim() : v;
  }

  function uniqueStrings(values) {
    const out = [];
    const seen = new Set();
    for (const v of values || []) {
      const t = String(v || "").trim();
      if (!t) continue;
      const k = t.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(t);
    }
    return out;
  }

  function textMatchesName(text, name) {
    const t = normalizeName(text);
    const n = normalizeName(name);
    if (!t || !n) return false;
    const ns = normalizeName(shortName(name));
    return t === n || (ns && t === ns) || t.includes(n) || (ns && t.includes(ns));
  }

  function isDbNamedServer(aliases) {
    return asArray(aliases).some(a => DB_NAMED_SERVER_PATTERN.test(String(a || "")));
  }

  function testOracleContainerName(name) {
    const n = String(name || "").trim();
    if (!n) return false;
    if (/^Oracle\s+Servers($|\/)/i.test(n)) return true;
    if (/(^|\/)(kRACDatabase|kNonRACDatabase|kOracleDatabase)$/i.test(n)) return true;
    return false;
  }

  function isBadCiName(value) {
    const ci = String(value || "").trim();
    if (!ci) return true;
    if (ci.toUpperCase() === "N/A") return true;
    if (/^https?:\/\//i.test(ci)) return true;
    if (/^[0-9a-f]{32}$/i.test(ci)) return true;
    return false;
  }

  function getWorkItem(runtimeInput) {
    return runtimeInput?.workItem || runtimeInput?.item || runtimeInput?.loopItem || runtimeInput?.loopItemValue || runtimeInput?.value ||
      (runtimeInput?.dtsk && runtimeInput?.ciName ? runtimeInput : null) || null;
  }

  function buildAliases(workItem) {
    const aliases = [];
    for (const a of asArray(workItem?.aliases)) aliases.push(a);
    aliases.push(workItem?.ciName);
    aliases.push(shortName(workItem?.ciName));
    return uniqueStrings(aliases);
  }

  function buildSearchTerms(workItem) {
    const aliases = buildAliases(workItem);
    const terms = [...aliases];
    for (const a of aliases) terms.push(shortName(a));
    return uniqueStrings(terms);
  }

  function getAssignedTo(workItem) {
    return safeText(workItem?.assignedTo);
  }

  function getAssignmentGroup(workItem) {
    return safeText(workItem?.assignmentGroup);
  }

  function getAssignmentAction(workItem) {
    const assignedTo = getAssignedTo(workItem);
    return assignedTo === "N/A" ? "Please assign" : "Assigned";
  }

  async function getApiKey() {
    const cred = await credentialVaultClient.getCredentialsDetails({ id: COHESITY_API_KEY_CREDENTIAL_ID });
    if (!cred?.token) throw new Error("Cohesity API key token was not returned from Dynatrace Credential Vault.");
    return cred.token;
  }

  async function getJson(url, headers) {
    const response = await fetch(url, { method: "GET", headers });
    if (!response.ok) {
      const bodyText = await response.text().catch(() => "");
      throw new Error(`GET failed: ${response.status} ${response.statusText} URL=${url} BODY=${bodyText.substring(0, 300)}`);
    }
    return await response.json();
  }

  function usecsToEt(usecs) {
    const n = Number(usecs);
    if (!Number.isFinite(n) || n <= 0) return "";
    const date = new Date(Math.floor(n / 1000));
    if (Number.isNaN(date.getTime())) return "";
    return new Intl.DateTimeFormat("en-US", {
      timeZone: OUTPUT_TIME_ZONE,
      year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false
    }).format(date).replace(",", "");
  }

  function getMaxUsecs(values) {
    let max = 0;
    for (const v of values || []) {
      const n = Number(v);
      if (Number.isFinite(n) && n > max) max = n;
    }
    return max;
  }

  function getSnapshotRunType(snapshot) {
    return firstNonBlank(snapshot?.runType, snapshot?.backupRunType);
  }

  function isRegularSnapshot(snapshot) {
    const rt = getSnapshotRunType(snapshot);
    if (!rt) return true;
    return !/log|archive/i.test(rt);
  }

  function getSnapshotUsecs(snapshot) {
    const localInfo = asArray(snapshot?.localSnapshotInfo);
    const archiveInfo = asArray(snapshot?.archivalSnapshotsInfo);
    const values = [snapshot?.protectionRunStartTimeUsecs, snapshot?.runStartTimeUsecs, snapshot?.startTimeUsecs, snapshot?.snapshotTimestampUsecs, snapshot?.endTimeUsecs];
    for (const l of localInfo) values.push(l?.snapshotInfo?.endTimeUsecs, l?.snapshotInfo?.snapshotTimestampUsecs, l?.snapshotInfo?.startTimeUsecs);
    for (const a of archiveInfo) values.push(a?.snapshotInfo?.endTimeUsecs, a?.snapshotInfo?.snapshotTimestampUsecs, a?.snapshotInfo?.startTimeUsecs);
    return getMaxUsecs(values);
  }

  function getBestSnapshot(object) {
    const snapshots = asArray(object?.latestSnapshotsInfo).filter(Boolean);
    if (snapshots.length === 0) return null;
    const regular = snapshots.filter(isRegularSnapshot);
    const candidates = regular.length > 0 ? regular : snapshots;
    candidates.sort((a, b) => getSnapshotUsecs(b) - getSnapshotUsecs(a));
    return candidates[0] || null;
  }

  function getProtectionGroupName(object, snapshot) {
    const pg = firstNonBlank(snapshot?.protectionGroupName, snapshot?.protectionGroup?.name, snapshot?.protectionGroupInfo?.name, object?.protectionGroupName, object?.protectionGroup?.name, object?.protectionGroupInfo?.name);
    return pg || "-";
  }

  function getClusterName(clusterMap, clusterId) {
    const cid = String(clusterId || "").trim();
    if (!cid) return "Unknown";
    return clusterMap?.[cid] || `Unknown-${cid}`;
  }

  function getClusterIdFromSearchNode(node) {
    return firstNonBlank(node?.clusterId, node?.clusterID, node?.accessClusterId, node?.sourceClusterId, node?.sourceClusterID, node?.cluster?.id, node?.cluster?.clusterId, node?.cluster?.clusterID, node?.clusterInfo?.id, node?.clusterInfo?.clusterId, node?.clusterInfo?.clusterID);
  }

  function getGlobalObjects(json) {
    if (!json) return [];
    if (Array.isArray(json)) return json;
    for (const p of ["objects", "searchResults", "results", "entities", "items", "data"]) {
      if (Array.isArray(json?.[p])) return json[p].filter(Boolean);
    }
    return [];
  }

  function getProtectedObjects(json) {
    if (!json) return [];
    if (Array.isArray(json)) return json;
    if (Array.isArray(json?.objects)) return json.objects.filter(Boolean);
    return [];
  }

  function getObjectNameFromNode(object) {
    return firstNonBlank(object?.name, object?.displayName, object?.databaseName, object?.dbName, object?.dbUniqueName);
  }

  function getObjectTypeFromNode(object) {
    return firstNonBlank(object?.objectType, object?.type, object?.entityType);
  }

  function getEnvironmentFromNode(object, parentEnvironment) {
    return firstNonBlank(object?.environment, object?.sourceInfo?.environment, parentEnvironment);
  }

  function getSqlHostNameFromNode(object) {
    return firstNonBlank(object?.mssqlParams?.hostInfo?.name, object?.mssqlParams?.hostInfo?.displayName, object?.mssqlParams?.hostInfo?.entity?.name, object?.mssqlParams?.hostInfo?.entity?.displayName, object?.sqlParams?.hostInfo?.name, object?.sqlParams?.hostInfo?.displayName, object?.sqlParams?.hostInfo?.entity?.name, object?.sqlParams?.hostInfo?.entity?.displayName);
  }

  function getOracleHostNameFromNode(object) {
    return firstNonBlank(object?.oracleParams?.hostInfo?.name, object?.oracleParams?.hostInfo?.displayName, object?.oracleParams?.hostInfo?.entity?.name, object?.oracleParams?.hostInfo?.entity?.displayName);
  }

  function getGenericSourceNameFromNode(object) {
    return firstNonBlank(object?.hostInfo?.name, object?.hostInfo?.displayName, object?.hostInfo?.entity?.name, object?.hostInfo?.entity?.displayName, object?.sourceInfo?.name, object?.sourceInfo?.displayName, object?.sourceInfo?.entity?.name, object?.sourceInfo?.entity?.displayName, object?.sourceName, object?.hostName, object?.serverName);
  }

  function getVmBackupTypeFromText(environment, objectType, objectName, sourceName, sourceInfoName) {
    const text = `${environment || ""} ${objectType || ""} ${objectName || ""} ${sourceName || ""} ${sourceInfoName || ""}`;
    if (/kAcropolis|Acropolis|Nutanix|AHV/i.test(text)) return "Nutanix";
    if (/kHyperV|HyperV|Hyper-V/i.test(text)) return "HyperV";
    if (/kVMware|VMware|kVirtualMachine|VirtualMachine/i.test(text)) return "VM";
    return "";
  }

  function resolveSourceName(backupType, objectName, parentName, sqlHostName, oracleHostName, genericSourceName, parentSourceName) {
    if (backupType === "Oracle") return oracleHostName || "";
    if (backupType === "SQL" && sqlHostName) return sqlHostName;
    if (genericSourceName && !testOracleContainerName(genericSourceName)) return genericSourceName;
    if (parentSourceName && !testOracleContainerName(parentSourceName)) return parentSourceName;
    if (objectName && objectName.includes("/")) {
      const prefix = objectName.split("/", 2)[0].trim();
      if (!testOracleContainerName(prefix)) return prefix;
    }
    if (parentName && !testOracleContainerName(parentName)) return parentName;
    return objectName || "";
  }

  function getPreType(env, objectType, objectName, sqlHostName, oracleHostName, genericSourceName) {
    if (sqlHostName) return "SQL";
    if (oracleHostName) return "Oracle";
    const vmType = getVmBackupTypeFromText(env, objectType, objectName, genericSourceName, "");
    if (vmType) return vmType;
    if (`${env} ${objectType}`.match(/kOracle/i)) return "Oracle";
    if (`${env} ${objectType}`.match(/kSQL/i)) return "SQL";
    return "FS";
  }

  function testValidDbName(name, serverName) {
    const n = String(name || "").trim();
    if (!n) return false;
    if (testOracleContainerName(n)) return false;
    const bad = ["database", "databases", "db", "name", "mssql", "sql", "oracle", "source", "server", "object", "objects", "Oracle Servers", "kRACDatabase", "kNonRACDatabase", "kOracleDatabase"];
    if (bad.map(normalizeName).includes(normalizeName(n))) return false;
    if (textMatchesName(n, serverName)) return false;
    return true;
  }

  function findDatabaseNames(value, serverName, depth = 0) {
    if (depth > 8 || value === null || value === undefined) return [];
    const out = [];
    if (Array.isArray(value)) {
      for (const item of value) out.push(...findDatabaseNames(item, serverName, depth + 1));
      return uniqueStrings(out);
    }
    if (typeof value !== "object") return [];
    const dbName = firstNonBlank(value.databaseName, value.dbName, value.name, value.displayName);
    if (testValidDbName(dbName, serverName)) out.push(dbName);
    for (const [key, val] of Object.entries(value)) {
      if (/mssql|sql|oracle|database|databases|db|params|objects|children|instances|list|info/i.test(key)) out.push(...findDatabaseNames(val, serverName, depth + 1));
    }
    return uniqueStrings(out);
  }

  function getParamDbRows(object, sourceName, parentName, environment, depth) {
    const rows = [];
    if (!sourceName || testOracleContainerName(sourceName)) return rows;
    const sqlHostName = getSqlHostNameFromNode(object);
    const oracleHostName = getOracleHostNameFromNode(object);
    if (!sqlHostName && !oracleHostName) return rows;
    const containers = [object?.mssqlParams, object?.sqlParams, object?.mssql, object?.oracleParams];
    let dbNames = [];
    for (const container of containers) dbNames.push(...findDatabaseNames(container, sourceName));
    dbNames = uniqueStrings(dbNames).filter(db => testValidDbName(db, sourceName));
    for (const db of dbNames) {
      let fullName = String(db || "").trim();
      if (!fullName) continue;
      if (!fullName.includes("/")) fullName = `${sourceName}/${fullName}`;
      if (testOracleContainerName(fullName)) continue;
      rows.push({ Object: object, ObjectName: fullName, ParentName: parentName, ParentEnvironment: environment, Environment: environment, ObjectType: "kDatabase", SqlHostName: sqlHostName, OracleHostName: oracleHostName, GenericSourceName: getGenericSourceNameFromNode(object), SourceName: sourceName, SourceInfoName: sourceName, Depth: depth + 1 });
    }
    return rows;
  }

  function getFlatProtectedObjects(object, parentName = "", parentEnvironment = "", parentSourceName = "", depth = 0) {
    const rows = [];
    if (!object) return rows;
    const objectName = getObjectNameFromNode(object);
    const env = getEnvironmentFromNode(object, parentEnvironment);
    const objectType = getObjectTypeFromNode(object);
    const sqlHostName = getSqlHostNameFromNode(object);
    const oracleHostName = getOracleHostNameFromNode(object);
    const genericSourceName = getGenericSourceNameFromNode(object);
    const preType = getPreType(env, objectType, objectName, sqlHostName, oracleHostName, genericSourceName);
    const sourceName = resolveSourceName(preType, objectName, parentName, sqlHostName, oracleHostName, genericSourceName, parentSourceName);
    rows.push({ Object: object, ObjectName: objectName, ParentName: parentName, ParentEnvironment: parentEnvironment, Environment: env, ObjectType: objectType, SqlHostName: sqlHostName, OracleHostName: oracleHostName, GenericSourceName: genericSourceName, SourceName: sourceName, SourceInfoName: sourceName, Depth: depth });
    rows.push(...getParamDbRows(object, sourceName, objectName, env, depth));
    for (const child of asArray(object?.childObjects)) rows.push(...getFlatProtectedObjects(child, objectName, env, sourceName, depth + 1));
    return rows;
  }

  function testNonDisplayObject(flatObject) {
    return testOracleContainerName(flatObject?.ObjectName) || testOracleContainerName(flatObject?.SourceName);
  }

  function getBackupType(flatObject) {
    if (testNonDisplayObject(flatObject)) return "Container";
    if (flatObject?.SqlHostName) return "SQL";
    if (flatObject?.OracleHostName) return "Oracle";
    const vmType = getVmBackupTypeFromText(flatObject?.Environment, flatObject?.ObjectType, flatObject?.ObjectName, flatObject?.SourceName, flatObject?.SourceInfoName);
    if (vmType) return vmType;
    const envTypeText = `${flatObject?.Environment || ""} ${flatObject?.ObjectType || ""}`;
    if (/kOracle/i.test(envTypeText)) return "Oracle";
    if (/kSQL/i.test(envTypeText)) return "SQL";
    return "FS";
  }

  function objectMatchesCiFlat(flatObject, ciName) {
    for (const v of [flatObject?.SourceName, flatObject?.SourceInfoName, flatObject?.ObjectName, flatObject?.ParentName]) {
      if (v && !testOracleContainerName(v) && textMatchesName(v, ciName)) return true;
    }
    if (flatObject?.ObjectName && String(flatObject.ObjectName).includes("/")) {
      const prefix = String(flatObject.ObjectName).split("/", 2)[0].trim();
      if (prefix && !testOracleContainerName(prefix) && textMatchesName(prefix, ciName)) return true;
    }
    return false;
  }

  function objectMatchesCiAliasesFlat(flatObject, ciAliases) {
    for (const alias of ciAliases || []) if (alias && objectMatchesCiFlat(flatObject, alias)) return true;
    return false;
  }

  function testDbLikeFlat(flatObject) {
    if (testNonDisplayObject(flatObject)) return false;
    return DB_BACKUP_TYPES.includes(getBackupType(flatObject));
  }

  function testVmLikeFlat(flatObject) {
    return ["HyperV", "Nutanix", "VM"].includes(getBackupType(flatObject));
  }

  function flatObjectKey(flatObject) {
    return [flatObject?.ObjectName, flatObject?.SourceName, flatObject?.ObjectType].map(v => String(v || "").toLowerCase()).join("|");
  }

  function dedupeFlatObjects(flatObjects) {
    const seen = new Set();
    const out = [];
    for (const f of flatObjects || []) {
      const key = flatObjectKey(f);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(f);
    }
    return out;
  }

  function getDisplayObjectName(objectName) {
    return objectName || "-";
  }

  function getWorkItemFields(workItem) {
    return { DTSK: safeText(workItem?.dtsk), DecomRequest: safeText(workItem?.decomRequest), AssignedTo: getAssignedTo(workItem), AssignmentGroup: getAssignmentGroup(workItem), AssignmentAction: getAssignmentAction(workItem) };
  }

  function convertFlatObjectToBackupRow(flatObject, ci, candidateCluster) {
    const obj = flatObject.Object;
    const snap = getBestSnapshot(obj);
    const backupType = getBackupType(flatObject);
    const ownership = getWorkItemFields(candidateCluster.workItem);
    if (backupType === "Container") return null;
    if (backupType === "Oracle" && !flatObject.OracleHostName) return null;
    const objectNameOut = getDisplayObjectName(flatObject.ObjectName);
    const sourceNameOut = resolveSourceName(backupType, flatObject.ObjectName, flatObject.ParentName, flatObject.SqlHostName, flatObject.OracleHostName, flatObject.GenericSourceName, flatObject.SourceName);
    if (backupType === "Oracle" && !sourceNameOut) return null;
    let usecs = 0;
    let lastBackupTime = "NoBackup";
    let pg = "-";
    if (snap) {
      usecs = getSnapshotUsecs(snap);
      lastBackupTime = usecs > 0 ? usecsToEt(usecs) : "NoBackupTime";
      pg = getProtectionGroupName(obj, snap);
    } else {
      pg = getProtectionGroupName(obj, null);
    }
    return { ...ownership, ServerName: ci, BackupType: backupType, ObjectName: objectNameOut, SourceName: sourceNameOut || "N/A", ClusterName: String(candidateCluster.clusterName || "N/A"), ProtectionGroup: pg || "-", LastBackupTime: lastBackupTime, LastBackupUsecs: usecs || 0, ClustersChecked: "N/A" };
  }

  async function searchGlobalObjects(apiKey, searchTerms) {
    const all = [];
    const warnings = [];
    for (const term of searchTerms) {
      const url = `${HELIOS_BASE_URL}/v2/data-protect/search/objects?searchString=${encodeURIComponent(term)}&includeTenants=true&count=${GLOBAL_SEARCH_COUNT}`;
      try {
        const json = await getJson(url, { accept: "application/json", apiKey });
        all.push(...getGlobalObjects(json));
      } catch (e) {
        warnings.push(`Global search failed for ${term}: ${e.message}`);
      }
    }
    return { objects: all, warnings };
  }

  function candidateClustersFromGlobal(globalObjects, allClusters, clusterMap) {
    const ids = [];
    function addId(v) {
      const id = firstNonBlank(v);
      if (id) ids.push(id);
    }
    for (const obj of globalObjects || []) {
      addId(getClusterIdFromSearchNode(obj));
      for (const opi of asArray(obj?.objectProtectionInfos)) {
        addId(getClusterIdFromSearchNode(opi));
        for (const pg of asArray(opi?.protectionGroups)) addId(getClusterIdFromSearchNode(pg));
      }
      for (const pg2 of asArray(obj?.protectionGroups)) addId(getClusterIdFromSearchNode(pg2));
    }
    const unique = uniqueStrings(ids);
    if (unique.length > 0) return unique.map(id => ({ clusterId: id, clusterName: getClusterName(clusterMap, id), searchMode: "global" }));
    if (!FALLBACK_WHEN_NO_GLOBAL_OBJECT) return [];
    return asArray(allClusters).map(c => ({ clusterId: String(c.clusterId || ""), clusterName: c.clusterName || getClusterName(clusterMap, c.clusterId), searchMode: "fallback" })).filter(c => c.clusterId);
  }

  function getClusterNamesChecked(candidateClusters) {
    const names = uniqueStrings(asArray(candidateClusters).map(c => c.clusterName || getClusterName({}, c.clusterId)));
    return names.length ? names.join(", ") : "N/A";
  }

  async function searchProtectedObjectsOnClusters(apiKey, ci, ciAliases, searchTerms, candidateClusters, workItem, options = {}) {
    const rows = [];
    const warnings = [];
    const searched = new Set();
    const dbOnly = options.dbOnly === true;
    for (const clu of candidateClusters) {
      if (!clu.clusterId) continue;
      for (const term of searchTerms) {
        if (!term) continue;
        const searchKey = `${dbOnly ? "dbonly" : "normal"}|${clu.clusterId}|${term}`;
        if (searched.has(searchKey)) continue;
        searched.add(searchKey);
        const url = `${HELIOS_BASE_URL}/v2/data-protect/search/protected-objects?searchString=${encodeURIComponent(term)}`;
        let protectedObjects = [];
        try {
          const json = await getJson(url, { accept: "application/json", apiKey, accessClusterId: String(clu.clusterId) });
          protectedObjects = getProtectedObjects(json);
        } catch (e) {
          warnings.push(`Protected search failed for ${term} on cluster ${clu.clusterId}: ${e.message}`);
          continue;
        }
        if (protectedObjects.length === 0) continue;
        let flatObjects = [];
        for (const obj of protectedObjects) flatObjects.push(...getFlatProtectedObjects(obj));
        flatObjects = flatObjects.filter(f => !testNonDisplayObject(f));
        if (flatObjects.length === 0) continue;
        let objectsToCheck = [];
        if (dbOnly) {
          objectsToCheck = flatObjects.filter(f => testDbLikeFlat(f) && objectMatchesCiAliasesFlat(f, ciAliases));
        } else {
          const matchingFlatObjects = flatObjects.filter(f => objectMatchesCiAliasesFlat(f, ciAliases));
          const dbFlatObjects = flatObjects.filter(testDbLikeFlat);
          const vmFlatObjects = flatObjects.filter(testVmLikeFlat);
          objectsToCheck = dedupeFlatObjects([...matchingFlatObjects, ...dbFlatObjects, ...vmFlatObjects]);
          if (objectsToCheck.length === 0) objectsToCheck = flatObjects;
        }
        for (const flat of objectsToCheck) {
          if (testNonDisplayObject(flat)) continue;
          const row = convertFlatObjectToBackupRow(flat, ci, { ...clu, workItem });
          if (!row) continue;
          if (!IN_SCOPE_BACKUP_TYPES.includes(row.BackupType)) continue;
          if (dbOnly && !DB_BACKUP_TYPES.includes(row.BackupType)) continue;
          rows.push(row);
        }
      }
    }
    return { rows, warnings, searchedClusterTermCount: searched.size };
  }

  function dedupeRows(rows) {
    const seen = new Set();
    const out = [];
    for (const row of rows || []) {
      const key = [row.ServerName, row.BackupType, row.ObjectName, row.SourceName, row.ClusterName, row.ProtectionGroup, row.LastBackupTime].map(v => String(v || "").toLowerCase()).join("|");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(row);
    }
    return out;
  }

  function sortRows(rows) {
    const rank = { FS: 10, HyperV: 20, Nutanix: 30, VM: 40, SQL: 50, Oracle: 60, NoFSBackupFound: 900, NoObject: 950, Unknown: 999 };
    return [...rows].sort((a, b) => {
      const ra = rank[a.BackupType] ?? 500;
      const rb = rank[b.BackupType] ?? 500;
      if (ra !== rb) return ra - rb;
      const o = String(a.ObjectName || "").localeCompare(String(b.ObjectName || ""));
      if (o !== 0) return o;
      return String(a.ProtectionGroup || "").localeCompare(String(b.ProtectionGroup || ""));
    });
  }

  function makeSpecialRow(workItem, backupType, objectName, sourceName, clusterName, protectionGroup, lastBackupTime, clustersChecked = "N/A") {
    return { ...getWorkItemFields(workItem), ServerName: workItem?.ciName || "N/A", BackupType: backupType, ObjectName: objectName || "N/A", SourceName: sourceName || "N/A", ClusterName: clusterName || "N/A", ProtectionGroup: protectionGroup || "-", LastBackupTime: lastBackupTime || "N/A", ClustersChecked: clustersChecked || "N/A" };
  }

  const workItem = getWorkItem(input);
  const clusterData = await result("dtsk_get_cluster_map");
  const clusters = asArray(clusterData?.clusters);
  const clusterMap = clusterData?.clusterMap || {};

  if (!workItem) return { validationState: "LoopItemMissing", rows: [], summary: { error: "Loop item was not available.", inputKeys: input && typeof input === "object" ? Object.keys(input) : [] } };

  if (isBadCiName(workItem.ciName)) {
    const row = makeSpecialRow(workItem, "Unknown", "InvalidCI", "N/A", "N/A", "-", "InvalidCI");
    return { validationState: "InvalidCI", rows: [row], summary: { dtsk: workItem.dtsk || "N/A", ciName: workItem.ciName || "N/A", rowCount: 1 }, warnings: [] };
  }

  if (clusters.length === 0) throw new Error("No Cohesity clusters available from dtsk_get_cluster_map.");

  const apiKey = await getApiKey();
  const aliases = buildAliases(workItem);
  const searchTerms = buildSearchTerms(workItem);
  const dbNamedServer = isDbNamedServer(aliases);
  const warnings = [];

  const globalResult = await searchGlobalObjects(apiKey, searchTerms);
  warnings.push(...globalResult.warnings);
  const candidateClusters = candidateClustersFromGlobal(globalResult.objects, clusters, clusterMap);
  const clustersCheckedText = getClusterNamesChecked(candidateClusters);
  const searchResult = await searchProtectedObjectsOnClusters(apiKey, workItem.ciName, aliases, searchTerms, candidateClusters, workItem);
  warnings.push(...searchResult.warnings);

  let workingRows = [...searchResult.rows];
  let dbCnFallbackApplied = false;
  let dbCnFallbackRowsFound = 0;
  let dbCnFallbackSearchedClusterTermCount = 0;
  const dbRowsFoundBeforeFallback = workingRows.filter(r => DB_BACKUP_TYPES.includes(r.BackupType)).length;

  if (dbNamedServer && dbRowsFoundBeforeFallback === 0) {
    dbCnFallbackApplied = true;
    const allClusterCandidates = clusters.map(c => ({ clusterId: String(c.clusterId || ""), clusterName: c.clusterName || getClusterName(clusterMap, c.clusterId), searchMode: "db-cn-fallback" })).filter(c => c.clusterId);
    const fallbackResult = await searchProtectedObjectsOnClusters(apiKey, workItem.ciName, aliases, searchTerms, allClusterCandidates, workItem, { dbOnly: true });
    warnings.push(...fallbackResult.warnings);
    dbCnFallbackRowsFound = fallbackResult.rows.length;
    dbCnFallbackSearchedClusterTermCount = fallbackResult.searchedClusterTermCount;
    workingRows.push(...fallbackResult.rows);
  }

  let rows = sortRows(dedupeRows(workingRows));
  const hasDbBackup = rows.some(r => DB_BACKUP_TYPES.includes(r.BackupType));
  const hasServerLevelBackup = rows.some(r => SERVER_LEVEL_BACKUP_TYPES.includes(r.BackupType));

  if (hasDbBackup && !hasServerLevelBackup) rows.push(makeSpecialRow(workItem, "NoFSBackupFound", workItem.ciName, workItem.ciName, "N/A", "-", "NoFSBackupFound", clustersCheckedText));
  if (rows.length === 0) rows = [makeSpecialRow(workItem, "NoObject", workItem.ciName, "N/A", "N/A", "-", "NoBackupFound", clustersCheckedText)];

  rows = sortRows(rows);
  const pgNamePopulatedCount = rows.filter(r => r.ProtectionGroup && !["-", "N/A"].includes(r.ProtectionGroup)).length;
  const pgNameMissingCount = rows.filter(r => !r.ProtectionGroup || ["-", "N/A"].includes(r.ProtectionGroup)).length;
  const validationState = rows.some(r => IN_SCOPE_BACKUP_TYPES.includes(r.BackupType)) ? "Validated" : "NoBackupFound";

  const output = {
    validationState,
    rows,
    summary: {
      dtsk: workItem.dtsk || "N/A",
      ciName: workItem.ciName || "N/A",
      assignedTo: getAssignedTo(workItem),
      assignmentGroup: getAssignmentGroup(workItem),
      assignmentAction: getAssignmentAction(workItem),
      rowCount: rows.length,
      fsRowCount: rows.filter(r => r.BackupType === "FS").length,
      vmRowCount: rows.filter(r => r.BackupType === "VM").length,
      hyperVRowCount: rows.filter(r => r.BackupType === "HyperV").length,
      nutanixRowCount: rows.filter(r => r.BackupType === "Nutanix").length,
      sqlRowCount: rows.filter(r => r.BackupType === "SQL").length,
      oracleRowCount: rows.filter(r => r.BackupType === "Oracle").length,
      serverLevelBackupFound: rows.some(r => SERVER_LEVEL_BACKUP_TYPES.includes(r.BackupType)),
      dbBackupFound: rows.some(r => DB_BACKUP_TYPES.includes(r.BackupType)),
      noFsBackupFound: rows.some(r => r.BackupType === "NoFSBackupFound"),
      dbNamedServer,
      dbCnFallbackApplied,
      dbCnFallbackRowsFound,
      dbCnFallbackSearchedClusterTermCount,
      pgNamePopulatedCount,
      pgNameMissingCount,
      globalObjectCount: globalResult.objects.length,
      candidateClusterCount: candidateClusters.length,
      searchedClusterTermCount: searchResult.searchedClusterTermCount,
      clustersChecked: candidateClusters.map(c => ({ clusterId: c.clusterId, clusterName: c.clusterName, searchMode: c.searchMode }))
    },
    warnings
  };

  console.log("==== DTSK VALIDATE ONE CI RESULT ====");
  console.log(JSON.stringify(output, null, 2));
  return output;
}
