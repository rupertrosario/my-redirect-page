// ================================
// Script 1 (Run JavaScript #1)
// Cohesity Helios – Backup Failures (Object-level, Latest Uncleared Only)
// Partition: 0 (even split by cluster hash)
// GET-only | 502/429 safe | Output includes failures[] only (merged by Script 2)
// ================================

import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function () {
  const baseUrl = "https://helios.cohesity.com";

  // ======================
  // Split control (EVEN)
  // ======================
  const PART_ID = 0; // Script 1 = 0, Script 2 = 1

  // ======================
  // Tunables
  // ======================
  const NUM_RUNS_OBJECT = 7;      // objectDetails scope
  const MAX_ROWS_TOTAL = 6000;    // hard cap
  const CLUSTER_CONCURRENCY = 2;
  const PG_CONCURRENCY = 4;

  // ======================
  // AUTH (READ-ONLY)
  // ======================
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";

  let apiKey = null;
  let authMode = "vault-name";

  async function getKeyByName(name) {
    const all = await credentialVaultClient.getCredentials();
    const found = all.credentials.find((c) => c.name === name);
    if (!found) return null;
    const detail = await credentialVaultClient.getCredentialsDetails({ id: found.id });
    return (detail && (detail.token || detail.password)) || null;
  }

  try {
    apiKey = await getKeyByName(vaultName);
    if (!apiKey) throw new Error("not-found");
  } catch {
    try {
      const detail = await credentialVaultClient.getCredentialsDetails({ id: vaultId });
      apiKey = (detail && (detail.token || detail.password)) || null;
      authMode = "vault-id";
    } catch {
      authMode = "manual";
      apiKey = "PASTE_YOUR_API_KEY_HERE";
    }
  }
  if (!apiKey) throw new Error("No Helios API key available (vault + manual failed).");

  // ======================
  // Helpers
  // ======================
  function buildQuery(params) {
    const usp = new URLSearchParams();
    Object.keys(params || {}).forEach((k) => {
      const v = params[k];
      if (v === undefined || v === null) return;
      if (Array.isArray(v)) v.forEach((x) => usp.append(k, String(x)));
      else usp.append(k, String(v));
    });
    return usp.toString();
  }

  function usecsToET(usecs) {
    if (!usecs) return null;
    const ms = Number(usecs) / 1000;
    if (!Number.isFinite(ms)) return null;
    return new Date(ms).toLocaleString("en-US", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    });
  }

  function cleanMessage(msg) {
    if (msg == null) return "";
    if (Array.isArray(msg)) msg = msg.join(" | ");
    msg = String(msg);
    return msg.replace(/[\r\n]+/g, " ").replace(/,/g, " ").replace(/"/g, "'").trim();
  }

  function combineFailedAttempts(failedAttempts) {
    if (!failedAttempts || !failedAttempts.length) return "";
    const parts = [];
    for (let i = 0; i < failedAttempts.length; i++) {
      const p = cleanMessage(failedAttempts[i] && failedAttempts[i].message);
      if (p) parts.push(p);
    }
    return parts.join(" | ").trim();
  }

  function toInfoArray(localBackupInfo) {
    if (!localBackupInfo) return [];
    return Array.isArray(localBackupInfo) ? localBackupInfo : [localBackupInfo];
  }

  function isSuccessStatus(status) {
    return status === "Succeeded" || status === "SucceededWithWarning";
  }

  function getObjKey(ob) {
    if (!ob || !ob.object) return null;
    if (ob.object.id != null) return String(ob.object.id);
    const sid = (ob.object.sourceId != null) ? String(ob.object.sourceId) : "";
    return `${ob.object.environment || ""}|${ob.object.objectType || ""}|${ob.object.name || ""}|${sid}`;
  }

  function hasFailedAttempts(ob) {
    const lsi = ob && ob.localSnapshotInfo;
    const fa = lsi && lsi.failedAttempts;
    return !!(fa && fa.length);
  }

  function isSuccessForClear(ob) {
    if (!ob || !ob.localSnapshotInfo) return false;
    return !hasFailedAttempts(ob);
  }

  function findHostNameInRun(objects, sourceId) {
    if (!sourceId || !objects || !objects.length) return null;
    const sid = String(sourceId);
    for (let i = 0; i < objects.length; i++) {
      const o = objects[i] && objects[i].object ? objects[i].object : null;
      if (!o || o.id == null) continue;
      if (String(o.id) === sid) return o.name || null;
    }
    return null;
  }

  function part(clusterName) {
    const s = String(clusterName || "");
    let sum = 0;
    for (let i = 0; i < s.length; i++) sum = (sum + s.charCodeAt(i)) % 2;
    return sum; // 0 or 1
  }

  async function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  async function getJson(url, headers, attempt = 0) {
    const resp = await fetch(url, { method: "GET", headers });
    if (resp.ok) return resp.json();

    const status = resp.status;
    let txt = "";
    try { txt = await resp.text(); } catch {}

    if ((status === 429 || status === 502 || status === 503 || status === 504) && attempt < 2) {
      await sleep(250 * (attempt + 1));
      return getJson(url, headers, attempt + 1);
    }
    throw new Error(`GET ${url} -> HTTP ${status} ${txt}`);
  }

  async function mapLimit(items, limit, fn) {
    const out = [];
    let idx = 0;
    const workers = new Array(Math.min(limit, items.length)).fill(0).map(async () => {
      while (idx < items.length) {
        const i = idx++;
        try { out[i] = await fn(items[i], i); } catch { out[i] = null; }
      }
    });
    await Promise.all(workers);
    return out;
  }

  // ======================
  // Environment maps
  // ======================
  const ENV_LABELS = {
    kOracle: "Oracle",
    kSQL: "SQL",
    kPhysical: "Physical",
    kGenericNas: "NAS",
    kIsilon: "NAS",
    kHyperV: "HyperV",
    kAcropolis: "Acropolis",
    kRemoteAdapter: "RemoteAdapter"
  };

  const ENV_OBJECT_TYPES = {
    kOracle: "kDatabase",
    kSQL: "kDatabase",
    kPhysical: "kHost",
    kGenericNas: "kHost",
    kIsilon: "kHost",
    kHyperV: "kVirtualMachine",
    kAcropolis: "kVirtualMachine",
    kRemoteAdapter: "kRemoteAdapter"
  };

  const ENV_FILTERS = {
    kOracle: ["kOracle"],
    kSQL: ["kSQL"],
    kPhysical: ["kPhysical"],
    kGenericNas: ["kGenericNas", "kIsilon"],
    kIsilon: ["kIsilon", "kGenericNas"],
    kHyperV: ["kHyperV"],
    kAcropolis: ["kAcropolis"],
    kRemoteAdapter: ["kRemoteAdapter"]
  };

  function getEnvCode(pg) {
    let envRaw = pg.environment;
    if (!envRaw && Array.isArray(pg.environmentTypes) && pg.environmentTypes.length > 0) {
      envRaw = pg.environmentTypes[0];
    }
    return envRaw || null;
  }

  function mapEnvironmentLabel(envCode) {
    if (!envCode) return "Unknown";
    return ENV_LABELS[envCode] || envCode;
  }

  function extractRemoteAdapterInfo(pg) {
    let raHost = null;
    let raDB = null;

    const ra = pg.remoteAdapterParams || {};
    const hosts = ra.hosts || ra.host || ra.hostList || null;

    let firstHost = null;
    if (Array.isArray(hosts) && hosts.length > 0) firstHost = hosts[0];
    else if (hosts && typeof hosts === "object") firstHost = hosts;

    if (firstHost) {
      raHost = firstHost.hostname || firstHost.hostName || firstHost.name || null;

      const inc =
        firstHost.incrementalBackupScript ||
        firstHost.backupScript ||
        ra.incrementalBackupScript ||
        ra.backupScript ||
        {};

      let args = inc.params || inc.arguments || inc.args || null;
      if (Array.isArray(args)) args = args.join(" ");
      if (typeof args === "string") {
        const m = args.match(/-o\s+(\S+)/i);
        if (m) raDB = m[1];
      }
    }
    return { raHost, raDB };
  }

  async function collectRemoteAdapterPg(clusterName, headers, pg, envLbl) {
    const pgId = pg.id;
    const pgName = pg.name || "Unknown PG";

    const raInfo = extractRemoteAdapterInfo(pg);
    const raHost = raInfo.raHost;
    const raDB = raInfo.raDB;

    const runQuery = buildQuery({
      numRuns: NUM_RUNS_OBJECT,
      excludeNonRestorableRuns: false,
      includeObjectDetails: true
    });

    let runData;
    try {
      runData = await getJson(
        `${baseUrl}/v2/data-protect/protection-groups/${encodeURIComponent(pgId)}/runs?${runQuery}`,
        headers
      );
    } catch {
      return [];
    }

    const runs = runData && runData.runs ? runData.runs : [];
    if (!runs.length) return [];

    const flat = [];
    for (let i = 0; i < runs.length; i++) {
      const infos = toInfoArray(runs[i].localBackupInfo);
      for (let j = 0; j < infos.length; j++) {
        const info = infos[j] || {};
        flat.push({
          RunType: info.runType || "UNKNOWN",
          Status: info.status || "Unknown",
          Message: cleanMessage(info.messages),
          StartUsecs: info.startTimeUsecs || 0,
          EndUsecs: info.endTimeUsecs || 0
        });
      }
    }
    if (!flat.length) return [];

    const byType = {};
    for (let i = 0; i < flat.length; i++) {
      const r = flat[i];
      if (!byType[r.RunType]) byType[r.RunType] = [];
      byType[r.RunType].push(r);
    }

    const out = [];
    for (const rt of Object.keys(byType)) {
      const arr = byType[rt].slice().sort((a, b) => (b.EndUsecs || 0) - (a.EndUsecs || 0));
      const latestFailed = arr.find((x) => x.Status === "Failed");
      if (!latestFailed) continue;

      const hasLaterSuccess = arr.some(
        (x) => isSuccessStatus(x.Status) && (x.StartUsecs || 0) > (latestFailed.EndUsecs || 0)
      );
      if (hasLaterSuccess) continue;

      out.push({
        Environment: envLbl,
        Cluster: clusterName,
        ProtectionGroup: pgName,
        RunType: rt,
        StartTime: usecsToET(latestFailed.StartUsecs),
        EndTime: usecsToET(latestFailed.EndUsecs),
        Status: "Failed",
        Host: raHost || null,
        ObjectName: raDB || raHost || "Unknown RemoteAdapter",
        DatabaseName: null,
        FailedMessage: latestFailed.Message || "",
        StartTimeUsecs: latestFailed.StartUsecs,
        EndTimeUsecs: latestFailed.EndUsecs
      });
    }
    return out;
  }

  async function collectPgLatestUncleared(clusterName, headers, pg) {
    const pgId = pg.id;
    const pgName = pg.name || "Unknown PG";
    const envCode = getEnvCode(pg);
    const envLbl = mapEnvironmentLabel(envCode);

    if (envCode === "kRemoteAdapter") {
      return collectRemoteAdapterPg(clusterName, headers, pg, envLbl);
    }

    const targetType = ENV_OBJECT_TYPES[envCode] || null;
    const envFilter = ENV_FILTERS[envCode] || null;
    const parentHostNeeded = envCode === "kOracle" || envCode === "kSQL";

    const runQuery = buildQuery({
      numRuns: NUM_RUNS_OBJECT,
      excludeNonRestorableRuns: false,
      includeObjectDetails: true
    });

    let runData;
    try {
      runData = await getJson(
        `${baseUrl}/v2/data-protect/protection-groups/${encodeURIComponent(pgId)}/runs?${runQuery}`,
        headers
      );
    } catch {
      return [];
    }

    const runs = runData && runData.runs ? runData.runs : [];
    if (!runs.length) return [];

    const byType = {};
    for (let i = 0; i < runs.length; i++) {
      const infos = toInfoArray(runs[i].localBackupInfo);
      if (!infos.length) continue;
      const rType = infos[0]?.runType ? infos[0].runType : "UNKNOWN";
      if (!byType[rType]) byType[rType] = [];
      byType[rType].push(runs[i]);
    }

    const out = [];

    for (const rType of Object.keys(byType)) {
      const runsForType = byType[rType]
        .slice()
        .sort(
          (a, b) =>
            ((b.localBackupInfo?.[0]?.endTimeUsecs) || 0) -
            ((a.localBackupInfo?.[0]?.endTimeUsecs) || 0)
        );

      const cleared = new Set();
      const latestFailByKey = new Map();

      for (let ri = 0; ri < runsForType.length; ri++) {
        const run = runsForType[ri];
        const info = run.localBackupInfo?.[0] || {};
        const status = info.status || "Unknown";

        const startUsecs = info.startTimeUsecs || 0;
        const endUsecs = info.endTimeUsecs || 0;

        const startTimeStr = usecsToET(startUsecs);
        const endTimeStr = usecsToET(endUsecs);

        const objects = run.objects || [];
        if (!objects.length) continue;

        // clears
        for (let oi = 0; oi < objects.length; oi++) {
          const ob = objects[oi];
          if (!ob?.object || !ob.localSnapshotInfo) continue;
          if (isSuccessForClear(ob)) {
            const k = getObjKey(ob);
            if (k) cleared.add(k);
          }
        }

        // Oracle/SQL host-level failures
        if (parentHostNeeded) {
          for (let oi = 0; oi < objects.length; oi++) {
            const ob = objects[oi];
            const obj = ob?.object || null;
            if (!obj) continue;

            const isHostObj = obj.objectType === "kHost" || obj.environment === "kPhysical";
            if (!isHostObj) continue;

            const fa = ob.localSnapshotInfo?.failedAttempts || [];
            if (!fa.length) continue;

            const k = getObjKey(ob);
            if (!k || cleared.has(k) || latestFailByKey.has(k)) continue;

            const msg = combineFailedAttempts(fa);
            if (!msg) continue;

            latestFailByKey.set(k, {
              Environment: envLbl,
              Cluster: clusterName,
              ProtectionGroup: pgName,
              RunType: rType,
              StartTime: startTimeStr,
              EndTime: endTimeStr,
              Status: status,
              Host: obj.name || null,
              ObjectName: null,
              DatabaseName: "No DBs Found (Host-Level Failure)",
              FailedMessage: msg,
              StartTimeUsecs: startUsecs,
              EndTimeUsecs: endUsecs
            });
          }
        }

        // target object failures
        for (let oi = 0; oi < objects.length; oi++) {
          const ob = objects[oi];
          const obj = ob?.object || null;
          if (!obj) continue;

          if (targetType && obj.objectType !== targetType) continue;
          if (envFilter && obj.environment && envFilter.indexOf(obj.environment) === -1) continue;

          const k = getObjKey(ob);
          if (!k || cleared.has(k) || latestFailByKey.has(k)) continue;

          const fa = ob.localSnapshotInfo?.failedAttempts || [];

          if (!fa.length) {
            if (envCode === "kPhysical" && status === "Failed") {
              latestFailByKey.set(k, {
                Environment: envLbl,
                Cluster: clusterName,
                ProtectionGroup: pgName,
                RunType: rType,
                StartTime: startTimeStr,
                EndTime: endTimeStr,
                Status: status,
                Host: null,
                ObjectName: obj.name || null,
                DatabaseName: null,
                FailedMessage: "No failedAttempts[] details found — Run marked Failed",
                StartTimeUsecs: startUsecs,
                EndTimeUsecs: endUsecs
              });
            }
            continue;
          }

          const msg = combineFailedAttempts(fa);
          if (!msg) continue;

          let hostName = null;
          if (parentHostNeeded) hostName = findHostNameInRun(objects, obj.sourceId);

          if (parentHostNeeded) {
            latestFailByKey.set(k, {
              Environment: envLbl,
              Cluster: clusterName,
              ProtectionGroup: pgName,
              RunType: rType,
              StartTime: startTimeStr,
              EndTime: endTimeStr,
              Status: status,
              Host: hostName,
              ObjectName: null,
              DatabaseName: obj.name || null,
              FailedMessage: msg,
              StartTimeUsecs: startUsecs,
              EndTimeUsecs: endUsecs
            });
          } else {
            latestFailByKey.set(k, {
              Environment: envLbl,
              Cluster: clusterName,
              ProtectionGroup: pgName,
              RunType: rType,
              StartTime: startTimeStr,
              EndTime: endTimeStr,
              Status: status,
              Host: null,
              ObjectName: obj.name || null,
              DatabaseName: null,
              FailedMessage: msg,
              StartTimeUsecs: startUsecs,
              EndTimeUsecs: endUsecs
            });
          }
        }
      }

      for (const v of latestFailByKey.values()) out.push(v);
    }

    return out;
  }

  async function collectClusterFailures(cluster) {
    const clusterId = cluster.clusterId;
    const clusterName =
      cluster.name || cluster.clusterName || cluster.displayName || `Unknown-${clusterId}`;

    if (part(clusterName) !== PART_ID) return [];

    const headers = {
      accept: "application/json",
      apiKey,
      accessClusterId: String(clusterId)
    };

    let pgData;
    try {
      pgData = await getJson(
        `${baseUrl}/v2/data-protect/protection-groups?${buildQuery({
          isDeleted: false,
          isPaused: false,
          isActive: true
        })}`,
        headers
      );
    } catch {
      return [];
    }

    const pgs = pgData && pgData.protectionGroups ? pgData.protectionGroups : [];
    if (!pgs.length) return [];

    const pgResults = await mapLimit(pgs, PG_CONCURRENCY, async (pg) => {
      return collectPgLatestUncleared(clusterName, headers, pg);
    });

    const rows = [];
    for (let i = 0; i < pgResults.length; i++) {
      const r = pgResults[i];
      if (r && r.length) rows.push(...r);
      if (rows.length >= MAX_ROWS_TOTAL) break;
    }
    return rows;
  }

  // clusters
  const clustersResp = await getJson(
    `${baseUrl}/v2/mcm/cluster-mgmt/info`,
    { accept: "application/json", apiKey }
  );
  const clusters = clustersResp && clustersResp.cohesityClusters ? clustersResp.cohesityClusters : [];

  const clusterResults = await mapLimit(clusters, CLUSTER_CONCURRENCY, async (c) => {
    return collectClusterFailures(c);
  });

  const failures = [];
  for (let i = 0; i < clusterResults.length; i++) {
    const r = clusterResults[i];
    if (r && r.length) failures.push(...r);
    if (failures.length >= MAX_ROWS_TOTAL) break;
  }

  failures.sort((a, b) => (b.EndTimeUsecs || 0) - (a.EndTimeUsecs || 0));

  return {
    authMode: authMode,
    count: failures.length,
    failures: failures
  };
}
