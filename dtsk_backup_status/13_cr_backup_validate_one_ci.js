// ==========================================================
// Dynatrace JS Task
// Task name: cr_backup_validate_one_ci
// Phase: CR backup validation - GET only
//
// Purpose:
// - Runs as a loop after cr_ci inside the per-CR sub-workflow
// - Accepts the cr_ci loop shape: [ { name: "server01" } ]
// - Reuses the proven Cohesity protected-object search approach from
//   04_dtsk_validate_one_ci.js
// - Handles FS/VM/Hyper-V/Nutanix/SQL/Oracle backup discovery
// - Handles DB/CN fallback across all clusters
// - Distinguishes "No Backup Found" from a Cohesity/cluster/API failure
// - Never writes to Cohesity or ServiceNow
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
  const IN_SCOPE_BACKUP_TYPES = [...SERVER_LEVEL_BACKUP_TYPES, ...DB_BACKUP_TYPES];

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
      for (const v of [value.name, value.display_value, value.displayName, value.value]) {
        const t = toText(v);
        if (t) return t;
      }
      return "";
    }
    return String(value).trim();
  }

  function firstNonBlank(...values) {
    for (const value of values) {
      const text = toText(value);
      if (text) return text;
    }
    return "";
  }

  function normalizeName(value) {
    return String(value || "").trim().replace(/^[\"']|[\"']$/g, "").toLowerCase();
  }

  function shortName(value) {
    const v = String(value || "").trim();
    if (!v) return "";
    return v.includes(".") ? v.split(".")[0].trim() : v;
  }

  function uniqueStrings(values) {
    const seen = new Set();
    const out = [];
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

  function getCiName(runtimeInput) {
    // cr_ci loop output is currently shaped as:
    // [ { "name": "server01" } ]
    // This also tolerates Dynatrace wrapping the loop item in item/loopItem/value.
    const candidate =
      runtimeInput?.serverItem ??
      runtimeInput?.item ??
      runtimeInput?.loopItem ??
      runtimeInput?.loopItemValue ??
      runtimeInput?.value ??
      runtimeInput;

    if (Array.isArray(candidate)) {
      const first = candidate[0];
      if (typeof first === "string") return first.trim();
      return firstNonBlank(first?.name, first?.ciName, first?.display_value, first?.displayName);
    }

    if (typeof candidate === "string") return candidate.trim();
    return firstNonBlank(candidate?.name, candidate?.ciName, candidate?.display_value, candidate?.displayName);
  }

  function isBadCiName(value) {
    const ci = String(value || "").trim();
    if (!ci) return true;
    if (ci.toUpperCase() === "N/A") return true;
    if (/^https?:\/\//i.test(ci)) return true;
    if (/^[0-9a-f]{32}$/i.test(ci)) return true;
    return false;
  }

  function textMatchesName(text, name) {
    const t = normalizeName(text);
    const n = normalizeName(name);
    if (!t || !n) return false;
    const ns = normalizeName(shortName(name));
    return t === n || (ns && t === ns) || t.includes(n) || (ns && t.includes(ns));
  }

  function buildAliases(ciName) {
    return uniqueStrings([ciName, shortName(ciName)]);
  }

  function buildSearchTerms(ciName) {
    const aliases = buildAliases(ciName);
    return uniqueStrings([...aliases, ...aliases.map(shortName)]);
  }

  function isDbNamedServer(aliases) {
    return asArray(aliases).some(a => DB_NAMED_SERVER_PATTERN.test(String(a || "")));
  }

  function testOracleContainerName(name) {
    const n = String(name || "").trim();
    if (!n) return false;
    if (/^Oracle\s+Servers($|\/)/i.test(n)) return true;
    return /(^|\/)(kRACDatabase|kNonRACDatabase|kOracleDatabase)$/i.test(n);
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
      throw new Error(`HTTP ${response.status} ${response.statusText}; ${bodyText.substring(0, 200)}`);
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
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false
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
    const runType = getSnapshotRunType(snapshot);
    return !runType || !/log|archive/i.test(runType);
  }

  function getSnapshotUsecs(snapshot) {
    const values = [
      snapshot?.protectionRunStartTimeUsecs,
      snapshot?.runStartTimeUsecs,
      snapshot?.startTimeUsecs,
      snapshot?.snapshotTimestampUsecs,
      snapshot?.endTimeUsecs
    ];
    for (const x of asArray(snapshot?.localSnapshotInfo)) {
      values.push(x?.snapshotInfo?.endTimeUsecs, x?.snapshotInfo?.snapshotTimestampUsecs, x?.snapshotInfo?.startTimeUsecs);
    }
    for (const x of asArray(snapshot?.archivalSnapshotsInfo)) {
      values.push(x?.snapshotInfo?.endTimeUsecs, x?.snapshotInfo?.snapshotTimestampUsecs, x?.snapshotInfo?.startTimeUsecs);
    }
    return getMaxUsecs(values);
  }

  function getBestSnapshot(object) {
    const snapshots = asArray(object?.latestSnapshotsInfo).filter(Boolean);
    if (snapshots.length === 0) return null;
    const regular = snapshots.filter(isRegularSnapshot);
    const candidates = regular.length ? regular : snapshots;
    candidates.sort((a, b) => getSnapshotUsecs(b) - getSnapshotUsecs(a));
    return candidates[0] || null;
  }

  function getProtectionGroupName(object, snapshot) {
    return firstNonBlank(
      snapshot?.protectionGroupName,
      snapshot?.protectionGroup?.name,
      snapshot?.protectionGroupInfo?.name,
      object?.protectionGroupName,
      object?.protectionGroup?.name,
      object?.protectionGroupInfo?.name
    ) || "-";
  }

  function getClusterName(clusterMap, clusterId) {
    const id = String(clusterId || "").trim();
    if (!id) return "Unknown";
    return clusterMap?.[id] || `Unknown-${id}`;
  }

  function getClusterIdFromSearchNode(node) {
    return firstNonBlank(
      node?.clusterId, node?.clusterID, node?.accessClusterId,
      node?.sourceClusterId, node?.sourceClusterID,
      node?.cluster?.id, node?.cluster?.clusterId, node?.cluster?.clusterID,
      node?.clusterInfo?.id, node?.clusterInfo?.clusterId, node?.clusterInfo?.clusterID
    );
  }

  function getGlobalObjects(json) {
    if (!json) return [];
    if (Array.isArray(json)) return json;
    for (const key of ["objects", "searchResults", "results", "entities", "items", "data"]) {
      if (Array.isArray(json?.[key])) return json[key].filter(Boolean);
    }
    return [];
  }

  function getProtectedObjects(json) {
    if (!json) return [];
    if (Array.isArray(json)) return json;
    return Array.isArray(json?.objects) ? json.objects.filter(Boolean) : [];
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
    return firstNonBlank(
      object?.mssqlParams?.hostInfo?.name, object?.mssqlParams?.hostInfo?.displayName,
      object?.mssqlParams?.hostInfo?.entity?.name, object?.mssqlParams?.hostInfo?.entity?.displayName,
      object?.sqlParams?.hostInfo?.name, object?.sqlParams?.hostInfo?.displayName,
      object?.sqlParams?.hostInfo?.entity?.name, object?.sqlParams?.hostInfo?.entity?.displayName
    );
  }

  function getOracleHostNameFromNode(object) {
    return firstNonBlank(
      object?.oracleParams?.hostInfo?.name, object?.oracleParams?.hostInfo?.displayName,
      object?.oracleParams?.hostInfo?.entity?.name, object?.oracleParams?.hostInfo?.entity?.displayName
    );
  }

  function getGenericSourceNameFromNode(object) {
    return firstNonBlank(
      object?.hostInfo?.name, object?.hostInfo?.displayName,
      object?.hostInfo?.entity?.name, object?.hostInfo?.entity?.displayName,
      object?.sourceInfo?.name, object?.sourceInfo?.displayName,
      object?.sourceInfo?.entity?.name, object?.sourceInfo?.entity?.displayName,
      object?.sourceName, object?.hostName, object?.serverName
    );
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
    if (!n || testOracleContainerName(n)) return false;
    const bad = ["database", "databases", "db", "name", "mssql", "sql", "oracle", "source", "server", "object", "objects", "Oracle Servers", "kRACDatabase", "kNonRACDatabase", "kOracleDatabase"];
    if (bad.map(normalizeName).includes(normalizeName(n))) return false;
    return !textMatchesName(n, serverName);
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
      if (/mssql|sql|oracle|database|databases|db|params|objects|children|instances|list|info/i.test(key)) {
        out.push(...findDatabaseNames(val, serverName, depth + 1));
      }
    }
    return uniqueStrings(out);
  }

  function getParamDbRows(object, sourceName, parentName, environment, depth) {
    const rows = [];
    if (!sourceName || testOracleContainerName(sourceName)) return rows;
    const sqlHostName = getSqlHostNameFromNode(object);
    const oracleHostName = getOracleHostNameFromNode(object);
    if (!sqlHostName && !oracleHostName) return rows;

    let dbNames = [];
    for (const container of [object?.mssqlParams, object?.sqlParams, object?.mssql, object?.oracleParams]) {
      dbNames.push(...findDatabaseNames(container, sourceName));
    }
    dbNames = uniqueStrings(dbNames).filter(db => testValidDbName(db, sourceName));

    for (const db of dbNames) {
      let fullName = String(db || "").trim();
      if (!fullName) continue;
      if (!fullName.includes("/")) fullName = `${sourceName}/${fullName}`;
      if (testOracleContainerName(fullName)) continue;
      rows.push({
        Object: object,
        ObjectName: fullName,
        ParentName: parentName,
        ParentEnvironment: environment,
        Environment: environment,
        ObjectType: "kDatabase",
        SqlHostName: sqlHostName,
        OracleHostName: oracleHostName,
        GenericSourceName: getGenericSourceNameFromNode(object),
        SourceName: sourceName,
        SourceInfoName: sourceName,
        Depth: depth + 1
      });
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

    rows.push({
      Object: object,
      ObjectName: objectName,
      ParentName: parentName,
      ParentEnvironment: parentEnvironment,
      Environment: env,
      ObjectType: objectType,
      SqlHostName: sqlHostName,
      OracleHostName: oracleHostName,
      GenericSourceName: genericSourceName,
      SourceName: sourceName,
      SourceInfoName: sourceName,
      Depth: depth
    });

    rows.push(...getParamDbRows(object, sourceName, objectName, env, depth));
    for (const child of asArray(object?.childObjects)) {
      rows.push(...getFlatProtectedObjects(child, objectName, env, sourceName, depth + 1));
    }
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

  function objectMatchesCiAliasesFlat(flatObject, aliases) {
    return asArray(aliases).some(alias => alias && objectMatchesCiFlat(flatObject, alias));
  }

  function testDbLikeFlat(flatObject) {
    return !testNonDisplayObject(flatObject) && DB_BACKUP_TYPES.includes(getBackupType(flatObject));
  }

  function testVmLikeFlat(flatObject) {
    return ["HyperV", "Nutanix", "VM"].includes(getBackupType(flatObject));
  }

  function flatObjectKey(flatObject) {
    return [flatObject?.ObjectName, flatObject?.SourceName, flatObject?.ObjectType]
      .map(v => String(v || "").toLowerCase()).join("|");
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

  function convertFlatObjectToBackupRow(flatObject, ciName, cluster) {
    const obj = flatObject.Object;
    const snapshot = getBestSnapshot(obj);
    const backupType = getBackupType(flatObject);
    if (backupType === "Container") return null;
    if (backupType === "Oracle" && !flatObject.OracleHostName) return null;

    const objectName = flatObject.ObjectName || "-";
    const sourceName = resolveSourceName(
      backupType,
      flatObject.ObjectName,
      flatObject.ParentName,
      flatObject.SqlHostName,
      flatObject.OracleHostName,
      flatObject.GenericSourceName,
      flatObject.SourceName
    );
    if (backupType === "Oracle" && !sourceName) return null;

    let lastBackupUsecs = 0;
    let lastBackupTime = "NoBackup";
    let protectionGroup = "-";
    if (snapshot) {
      lastBackupUsecs = getSnapshotUsecs(snapshot);
      lastBackupTime = lastBackupUsecs > 0 ? usecsToEt(lastBackupUsecs) : "NoBackupTime";
      protectionGroup = getProtectionGroupName(obj, snapshot);
    } else {
      protectionGroup = getProtectionGroupName(obj, null);
    }

    return {
      ServerName: ciName,
      BackupType: backupType,
      ObjectName: objectName,
      SourceName: sourceName || "N/A",
      ClusterName: String(cluster.clusterName || "N/A"),
      ProtectionGroup: protectionGroup || "-",
      LastBackupTime: lastBackupTime,
      LastBackupUsecs: lastBackupUsecs || 0
    };
  }

  async function searchGlobalObjects(apiKey, searchTerms) {
    const objects = [];
    const warnings = [];
    let successCount = 0;
    let failureCount = 0;

    for (const term of searchTerms) {
      const url = `${HELIOS_BASE_URL}/v2/data-protect/search/objects?searchString=${encodeURIComponent(term)}&includeTenants=true&count=${GLOBAL_SEARCH_COUNT}`;
      try {
        const json = await getJson(url, { accept: "application/json", apiKey });
        objects.push(...getGlobalObjects(json));
        successCount += 1;
      } catch (e) {
        failureCount += 1;
        warnings.push(`Global object search failed for ${term}: ${e.message}`);
      }
    }
    return { objects, warnings, successCount, failureCount };
  }

  function candidateClustersFromGlobal(globalObjects, allClusters, clusterMap) {
    const ids = [];
    const add = value => {
      const id = firstNonBlank(value);
      if (id) ids.push(id);
    };

    for (const obj of globalObjects || []) {
      add(getClusterIdFromSearchNode(obj));
      for (const opi of asArray(obj?.objectProtectionInfos)) {
        add(getClusterIdFromSearchNode(opi));
        for (const pg of asArray(opi?.protectionGroups)) add(getClusterIdFromSearchNode(pg));
      }
      for (const pg of asArray(obj?.protectionGroups)) add(getClusterIdFromSearchNode(pg));
    }

    const unique = uniqueStrings(ids);
    if (unique.length) {
      return unique.map(id => ({ clusterId: id, clusterName: getClusterName(clusterMap, id), searchMode: "global" }));
    }
    if (!FALLBACK_WHEN_NO_GLOBAL_OBJECT) return [];

    return asArray(allClusters)
      .map(c => ({
        clusterId: String(c.clusterId || ""),
        clusterName: c.clusterName || getClusterName(clusterMap, c.clusterId),
        searchMode: "fallback"
      }))
      .filter(c => c.clusterId);
  }

  async function searchProtectedObjectsOnClusters(apiKey, ciName, aliases, searchTerms, candidateClusters, options = {}) {
    const rows = [];
    const warnings = [];
    const searched = new Set();
    const dbOnly = options.dbOnly === true;
    let successCount = 0;
    let failureCount = 0;

    for (const cluster of candidateClusters) {
      if (!cluster.clusterId) continue;
      for (const term of searchTerms) {
        if (!term) continue;
        const key = `${dbOnly ? "db" : "normal"}|${cluster.clusterId}|${term}`;
        if (searched.has(key)) continue;
        searched.add(key);

        const url = `${HELIOS_BASE_URL}/v2/data-protect/search/protected-objects?searchString=${encodeURIComponent(term)}`;
        let protectedObjects = [];
        try {
          const json = await getJson(url, {
            accept: "application/json",
            apiKey,
            accessClusterId: String(cluster.clusterId)
          });
          successCount += 1;
          protectedObjects = getProtectedObjects(json);
        } catch (e) {
          failureCount += 1;
          warnings.push(`Protected-object search failed for ${term} on ${cluster.clusterName} (${cluster.clusterId}): ${e.message}`);
          continue;
        }

        if (!protectedObjects.length) continue;

        let flatObjects = [];
        for (const obj of protectedObjects) flatObjects.push(...getFlatProtectedObjects(obj));
        flatObjects = flatObjects.filter(f => !testNonDisplayObject(f));
        if (!flatObjects.length) continue;

        let objectsToCheck;
        if (dbOnly) {
          objectsToCheck = flatObjects.filter(f => testDbLikeFlat(f) && objectMatchesCiAliasesFlat(f, aliases));
        } else {
          const matching = flatObjects.filter(f => objectMatchesCiAliasesFlat(f, aliases));
          const dbObjects = flatObjects.filter(testDbLikeFlat);
          const vmObjects = flatObjects.filter(testVmLikeFlat);
          objectsToCheck = dedupeFlatObjects([...matching, ...dbObjects, ...vmObjects]);
          if (!objectsToCheck.length) objectsToCheck = flatObjects;
        }

        for (const flat of objectsToCheck) {
          const row = convertFlatObjectToBackupRow(flat, ciName, cluster);
          if (!row) continue;
          if (!IN_SCOPE_BACKUP_TYPES.includes(row.BackupType)) continue;
          if (dbOnly && !DB_BACKUP_TYPES.includes(row.BackupType)) continue;
          rows.push(row);
        }
      }
    }

    return {
      rows,
      warnings,
      successCount,
      failureCount,
      searchedClusterTermCount: searched.size
    };
  }

  function dedupeRows(rows) {
    const seen = new Set();
    const out = [];
    for (const row of rows || []) {
      const key = [
        row.ServerName, row.BackupType, row.ObjectName, row.SourceName,
        row.ClusterName, row.ProtectionGroup, row.LastBackupTime
      ].map(v => String(v || "").toLowerCase()).join("|");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(row);
    }
    return out;
  }

  function sortRows(rows) {
    const rank = {
      FS: 10, VM: 20, HyperV: 30, Nutanix: 40, SQL: 50, Oracle: 60,
      NoFSBackupFound: 900, NoDBBackupFound: 910,
      NoObject: 950, ValidationError: 980, InvalidCI: 990
    };
    return [...rows].sort((a, b) => {
      const ar = rank[a.BackupType] ?? 500;
      const br = rank[b.BackupType] ?? 500;
      if (ar !== br) return ar - br;
      const objectCompare = String(a.ObjectName || "").localeCompare(String(b.ObjectName || ""));
      if (objectCompare !== 0) return objectCompare;
      return String(a.ProtectionGroup || "").localeCompare(String(b.ProtectionGroup || ""));
    });
  }

  function makeSpecialRow(ciName, backupType, message, clusterName = "N/A") {
    return {
      ServerName: ciName || "N/A",
      BackupType: backupType,
      ObjectName: ciName || "N/A",
      SourceName: ciName || "N/A",
      ClusterName: clusterName,
      ProtectionGroup: "-",
      LastBackupTime: message,
      LastBackupUsecs: 0
    };
  }

  const ciName = getCiName(input);

  if (isBadCiName(ciName)) {
    const rows = [makeSpecialRow(ciName || "N/A", "InvalidCI", "Invalid CI/server name")];
    return {
      validationState: "InvalidCI",
      rows,
      summary: { ciName: ciName || "N/A", rowCount: 1, protected: false, reviewRequired: true },
      warnings: ["The current cr_ci loop item did not contain a usable server name."]
    };
  }

  // Reuse the existing cluster-map task. If your copied task has a different name,
  // change only this result() reference.
  let clusterData;
  try {
    clusterData = await result("dtsk_get_cluster_map");
  } catch (e) {
    const message = `Cluster map unavailable: ${e.message}`;
    return {
      validationState: "ValidationError",
      rows: [makeSpecialRow(ciName, "ValidationError", "Unable to validate - cluster map unavailable")],
      summary: { ciName, rowCount: 1, protected: false, reviewRequired: true, errorType: "ClusterMap" },
      warnings: [message]
    };
  }

  const clusters = asArray(clusterData?.clusters).filter(c => c?.clusterId);
  const clusterMap = clusterData?.clusterMap || {};

  if (!clusters.length) {
    return {
      validationState: "ValidationError",
      rows: [makeSpecialRow(ciName, "ValidationError", "Unable to validate - no Cohesity clusters available")],
      summary: { ciName, rowCount: 1, protected: false, reviewRequired: true, errorType: "NoClusters" },
      warnings: ["The cluster-map task returned no usable Cohesity clusters. This is not treated as No Backup Found."]
    };
  }

  let apiKey;
  try {
    apiKey = await getApiKey();
  } catch (e) {
    return {
      validationState: "ValidationError",
      rows: [makeSpecialRow(ciName, "ValidationError", "Unable to validate - Cohesity credential unavailable")],
      summary: { ciName, rowCount: 1, protected: false, reviewRequired: true, errorType: "Credential" },
      warnings: [e.message]
    };
  }

  const aliases = buildAliases(ciName);
  const searchTerms = buildSearchTerms(ciName);
  const dbNamedServer = isDbNamedServer(aliases);
  const warnings = [];

  const globalResult = await searchGlobalObjects(apiKey, searchTerms);
  warnings.push(...globalResult.warnings);

  const candidateClusters = candidateClustersFromGlobal(globalResult.objects, clusters, clusterMap);
  if (!candidateClusters.length) {
    return {
      validationState: "ValidationError",
      rows: [makeSpecialRow(ciName, "ValidationError", "Unable to validate - no candidate clusters")],
      summary: { ciName, rowCount: 1, protected: false, reviewRequired: true, errorType: "NoCandidateClusters" },
      warnings: [...warnings, "No candidate Cohesity clusters could be determined."]
    };
  }

  const searchResult = await searchProtectedObjectsOnClusters(
    apiKey, ciName, aliases, searchTerms, candidateClusters
  );
  warnings.push(...searchResult.warnings);

  let workingRows = [...searchResult.rows];
  let dbCnFallbackApplied = false;
  let dbCnFallbackRowsFound = 0;
  let fallbackSuccessCount = 0;
  let fallbackFailureCount = 0;

  const dbRowsBeforeFallback = workingRows.filter(r => DB_BACKUP_TYPES.includes(r.BackupType)).length;

  if (dbNamedServer && dbRowsBeforeFallback === 0) {
    dbCnFallbackApplied = true;
    const allClusters = clusters.map(c => ({
      clusterId: String(c.clusterId),
      clusterName: c.clusterName || getClusterName(clusterMap, c.clusterId),
      searchMode: "db-cn-fallback"
    }));

    const fallback = await searchProtectedObjectsOnClusters(
      apiKey, ciName, aliases, searchTerms, allClusters, { dbOnly: true }
    );
    warnings.push(...fallback.warnings);
    dbCnFallbackRowsFound = fallback.rows.length;
    fallbackSuccessCount = fallback.successCount;
    fallbackFailureCount = fallback.failureCount;
    workingRows.push(...fallback.rows);
  }

  let rows = sortRows(dedupeRows(workingRows));
  const hasDbBackup = rows.some(r => DB_BACKUP_TYPES.includes(r.BackupType));
  const hasServerBackup = rows.some(r => SERVER_LEVEL_BACKUP_TYPES.includes(r.BackupType));

  const totalProtectedSuccesses = searchResult.successCount + fallbackSuccessCount;
  const totalProtectedFailures = searchResult.failureCount + fallbackFailureCount;

  // Critical distinction: if every protected-object call failed, this is a validation
  // failure, not evidence that the server has no backup.
  if (rows.length === 0 && totalProtectedSuccesses === 0 && totalProtectedFailures > 0) {
    rows = [makeSpecialRow(ciName, "ValidationError", "Unable to validate - Cohesity cluster/API searches failed")];
  } else {
    if (hasDbBackup && !hasServerBackup) {
      rows.push(makeSpecialRow(ciName, "NoFSBackupFound", "DB backup found; no server-level backup found"));
    }

    if (dbNamedServer && hasServerBackup && !hasDbBackup && totalProtectedSuccesses > 0) {
      rows.push(makeSpecialRow(ciName, "NoDBBackupFound", "Server backup found; no SQL/Oracle backup found"));
    }

    if (rows.length === 0 && totalProtectedSuccesses > 0) {
      rows = [makeSpecialRow(ciName, "NoObject", "No Backup Found")];
    }
  }

  rows = sortRows(dedupeRows(rows));

  const hasValidationError = rows.some(r => r.BackupType === "ValidationError");
  const hasProtectedRow = rows.some(r => IN_SCOPE_BACKUP_TYPES.includes(r.BackupType));
  const reviewRequired = rows.some(r => ["NoObject", "NoFSBackupFound", "NoDBBackupFound", "ValidationError", "InvalidCI"].includes(r.BackupType));

  let validationState = "NoBackupFound";
  if (hasValidationError) validationState = "ValidationError";
  else if (hasProtectedRow && totalProtectedFailures > 0) validationState = "ValidatedWithWarnings";
  else if (hasProtectedRow) validationState = "Validated";

  const output = {
    validationState,
    rows,
    summary: {
      ciName,
      rowCount: rows.length,
      protected: hasProtectedRow,
      reviewRequired,
      serverLevelBackupFound: rows.some(r => SERVER_LEVEL_BACKUP_TYPES.includes(r.BackupType)),
      dbBackupFound: rows.some(r => DB_BACKUP_TYPES.includes(r.BackupType)),
      noBackupFound: rows.some(r => r.BackupType === "NoObject"),
      noFsBackupFound: rows.some(r => r.BackupType === "NoFSBackupFound"),
      noDbBackupFound: rows.some(r => r.BackupType === "NoDBBackupFound"),
      dbNamedServer,
      dbCnFallbackApplied,
      dbCnFallbackRowsFound,
      candidateClusterCount: candidateClusters.length,
      globalSearchSuccessCount: globalResult.successCount,
      globalSearchFailureCount: globalResult.failureCount,
      protectedSearchSuccessCount: totalProtectedSuccesses,
      protectedSearchFailureCount: totalProtectedFailures,
      partialValidation: hasProtectedRow && totalProtectedFailures > 0
    },
    warnings: uniqueStrings(warnings)
  };

  console.log("==== CR BACKUP VALIDATE ONE CI RESULT ====");
  console.log(JSON.stringify(output, null, 2));
  return output;
}
