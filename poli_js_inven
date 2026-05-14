// ------------------------------------------------------------
// Dynatrace JS | Cohesity Policy Summary
// - Uses Dynatrace classic credential vault
// - Helios GET-only
// - Gets all clusters, policies, and protection groups
// - Excludes default policies: Protect Once, Silver, Gold, Bronze
// - Email output: Summary + compact policy markdown table
// ------------------------------------------------------------

import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

export default async function () {
  const baseUrl = "https://helios.cohesity.com";

  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";
  const manualApiKey = "";

  const defaultPolicies = ["PROTECTONCE", "SILVER", "GOLD", "BRONZE"];

  function norm(v) {
    if (v === null || v === undefined) return "";

    if (Array.isArray(v)) {
      return v
        .filter(x => x !== null && x !== undefined && String(x).trim() !== "")
        .join(", ");
    }

    return String(v);
  }

  function valueOrNA(v) {
    const s = norm(v).trim();
    return s === "" ? "N/A" : s;
  }

  function mdEscape(v) {
    return valueOrNA(v)
      .replace(/\|/g, "\\|")
      .replace(/\r?\n/g, " / ");
  }

  function markdownTable(headers, rows) {
    let out = "";
    out += `| ${headers.join(" | ")} |\n`;
    out += `| ${headers.map(() => "---").join(" | ")} |\n`;

    for (const row of rows) {
      out += `| ${headers.map(h => mdEscape(row[h])).join(" | ")} |\n`;
    }

    return out;
  }

  function nowEtString() {
    return new Date().toLocaleString("en-US", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    }).replace(",", "");
  }

  function normalizeName(name) {
    let n = valueOrNA(name).toUpperCase();
    n = n.replace(/LOG[\s_-]*SHIPPING/g, "LOGSHIPPING");
    n = n.replace(/NON[\s_-]*PROD/g, "NONPROD");
    return n;
  }

  function compactName(name) {
    return normalizeName(name).replace(/[^A-Z0-9]/g, "");
  }

  function isDefaultPolicy(policyName) {
    return defaultPolicies.includes(compactName(policyName));
  }

  function firstValue(obj, names) {
    if (!obj) return "N/A";

    for (const name of names) {
      if (obj[name] !== null && obj[name] !== undefined && String(obj[name]).trim() !== "") {
        return obj[name];
      }
    }

    return "N/A";
  }

  function getPolicyId(policy) {
    return valueOrNA(firstValue(policy, ["id", "policyId", "uid"]));
  }

  function getPgPolicyId(pg) {
    return valueOrNA(firstValue(pg, ["policyId", "protectionPolicyId"]));
  }

  function formatDuration(duration, unit) {
    const dRaw = valueOrNA(duration);
    const uRaw = valueOrNA(unit);

    if (dRaw === "N/A" || uRaw === "N/A" || dRaw === "0") return "N/A";

    const d = Number(dRaw);
    if (!Number.isFinite(d)) return `${dRaw}${uRaw}`;

    const u = uRaw.toLowerCase();

    if (u.includes("minute")) return `${d}Min`;
    if (u.includes("hour")) return `${d}H`;

    if (u.includes("day")) {
      if (d % 7 === 0 && d <= 28) return `${d / 7}W`;
      return `${d}D`;
    }

    if (u.includes("week")) return `${d}W`;
    if (u.includes("month")) return `${d}M`;
    if (u.includes("year")) return `${d}Y`;
    if (u.includes("run")) return d === 1 ? "Run" : `${d}Runs`;

    return `${dRaw}${uRaw}`;
  }

  function formatRetention(retention) {
    if (!retention) return "N/A";
    return formatDuration(retention?.duration, retention?.unit);
  }

  function formatDataLock(retention) {
    const dl = retention?.dataLockConfig;
    if (!dl) return "N/A";

    const dlDuration = formatDuration(dl?.duration, dl?.unit);
    const dlMode = valueOrNA(dl?.mode);
    const worm = valueOrNA(dl?.enableWormOnExternalTarget);

    const parts = [];

    if (dlDuration !== "N/A") {
      parts.push(dlDuration);
    } else if (dlMode !== "N/A") {
      parts.push(dlMode);
    }

    if (worm === "true" || worm === "True") {
      parts.push("WORM");
    }

    return parts.length === 0 ? "N/A" : parts.join("+");
  }

  function formatSchedule(schedule) {
    if (!schedule) return "N/A";

    if (schedule?.minuteSchedule?.frequency && Number(schedule.minuteSchedule.frequency) !== 0) {
      return `Every ${formatDuration(schedule.minuteSchedule.frequency, "Minutes")}`;
    }

    if (schedule?.hourSchedule?.frequency && Number(schedule.hourSchedule.frequency) !== 0) {
      return `Every ${formatDuration(schedule.hourSchedule.frequency, "Hours")}`;
    }

    if (schedule?.daySchedule?.frequency && Number(schedule.daySchedule.frequency) !== 0) {
      const f = Number(schedule.daySchedule.frequency);
      return f === 1 ? "Daily" : `Every ${f}D`;
    }

    if (schedule?.weekSchedule?.dayOfWeek?.length) {
      const days = schedule.weekSchedule.dayOfWeek;
      return days.length === 7 ? "Weekly Sun-Sat" : `Weekly ${days.join(",")}`;
    }

    if (schedule?.monthSchedule) {
      const m = schedule.monthSchedule;

      if (m?.dayOfMonth && Number(m.dayOfMonth) !== 0) {
        return `Monthly day ${m.dayOfMonth}`;
      }

      if (m?.weekOfMonth && m?.dayOfWeek?.length) {
        return `Monthly ${m.weekOfMonth} ${m.dayOfWeek.join(",")}`;
      }
    }

    if (schedule?.yearSchedule) {
      const y = schedule.yearSchedule;

      if (y?.monthDay?.month && y?.monthDay?.dayOfTheMonth) {
        return `Yearly M${y.monthDay.month}/D${y.monthDay.dayOfTheMonth}`;
      }

      if (y?.dayOfYear) {
        return `Yearly ${y.dayOfYear}`;
      }
    }

    if (schedule?.frequency && Number(schedule.frequency) !== 0) {
      const unit = valueOrNA(schedule?.unit);

      if (unit === "Runs") {
        return Number(schedule.frequency) === 1 ? "Every Run" : `Every ${schedule.frequency} Runs`;
      }

      if (unit !== "N/A") {
        return `Every ${schedule.frequency}${unit}`;
      }
    }

    const unitOnly = valueOrNA(schedule?.unit);
    return unitOnly !== "N/A" ? unitOnly : "N/A";
  }

  function getBackupFields(policy) {
    const retention = policy?.backupPolicy?.regular?.retention;

    return {
      run: formatSchedule(policy?.backupPolicy?.regular?.incremental?.schedule),
      retain: formatRetention(retention),
      dataLock: formatDataLock(retention)
    };
  }

  function getLogFields(policy) {
    const retention = policy?.backupPolicy?.log?.retention;

    return {
      run: formatSchedule(policy?.backupPolicy?.log?.schedule),
      retain: formatRetention(retention),
      dataLock: formatDataLock(retention)
    };
  }

  function getPeriodicFullBackup(policy) {
    const items = [];

    const fullSchedule = formatSchedule(policy?.backupPolicy?.regular?.full?.schedule);
    if (fullSchedule !== "N/A") items.push(fullSchedule);

    const fullBackups = policy?.backupPolicy?.regular?.fullBackups || [];

    for (const fb of fullBackups) {
      const schedule = formatSchedule(fb?.schedule);
      const retention = formatRetention(fb?.retention);
      const dataLock = formatDataLock(fb?.retention);

      const parts = [];

      if (schedule !== "N/A") parts.push(schedule);
      if (retention !== "N/A") parts.push(`Ret ${retention}`);
      if (dataLock !== "N/A") parts.push(`DL ${dataLock}`);

      if (parts.length > 0) items.push(parts.join("; "));
    }

    return items.length === 0 ? "N/A" : items.join(" / ");
  }

  function formatQuietTimes(blackoutWindow) {
    if (!Array.isArray(blackoutWindow) || blackoutWindow.length === 0) return "N/A";

    const items = [];

    for (const b of blackoutWindow) {
      const day = valueOrNA(b?.day);
      let start = "N/A";
      let end = "N/A";

      if (b?.startTime?.hour !== undefined && b?.startTime?.minute !== undefined) {
        start = `${Number(b.startTime.hour)}:${String(Number(b.startTime.minute)).padStart(2, "0")}`;
      }

      if (b?.endTime?.hour !== undefined && b?.endTime?.minute !== undefined) {
        end = `${Number(b.endTime.hour)}:${String(Number(b.endTime.minute)).padStart(2, "0")}`;
      }

      const parts = [];
      if (start !== "N/A" && end !== "N/A") parts.push(`${start}-${end}`);
      if (day !== "N/A") parts.push(day);

      if (parts.length > 0) items.push(parts.join(" "));
    }

    return items.length === 0 ? "N/A" : items.join(" / ");
  }

  function formatRetryOptions(retryOptions) {
    if (!retryOptions) return "N/A";

    const retries = valueOrNA(retryOptions?.retries);
    const interval = valueOrNA(retryOptions?.retryIntervalMins);

    if ((retries === "N/A" || retries === "0") && (interval === "N/A" || interval === "0")) {
      return "No Retry";
    }

    const parts = [];

    if (retries !== "N/A" && retries !== "0") {
      parts.push(`${retries}x`);
    }

    if (interval !== "N/A" && interval !== "0") {
      parts.push(`${interval}Min gap`);
    }

    return parts.length === 0 ? "No Retry" : parts.join("; ");
  }

  function getReplicationFields(targets) {
    if (!Array.isArray(targets) || targets.length === 0) {
      return {
        replicatedTo: "N/A",
        retain: "N/A",
        dataLock: "N/A",
        logRetain: "N/A",
        logDataLock: "N/A"
      };
    }

    const replicatedTo = [];
    const retain = [];
    const dataLock = [];
    const logRetain = [];
    const logDataLock = [];

    for (const t of targets) {
      let targetName = valueOrNA(t?.remoteTargetConfig?.clusterName);

      if (targetName === "N/A") targetName = valueOrNA(t?.awsTargetConfig?.name);
      if (targetName === "N/A") targetName = valueOrNA(t?.azureTargetConfig?.name);

      replicatedTo.push(targetName);
      retain.push(formatRetention(t?.retention));
      dataLock.push(formatDataLock(t?.retention));
      logRetain.push(formatRetention(t?.logRetention));
      logDataLock.push(formatDataLock(t?.logRetention));
    }

    return {
      replicatedTo: replicatedTo.join(" / "),
      retain: retain.join(" / "),
      dataLock: dataLock.join(" / "),
      logRetain: logRetain.join(" / "),
      logDataLock: logDataLock.join(" / ")
    };
  }

  async function getApiKey() {
    try {
      const resByName = await credentialVaultClient.getCredentialsDetails({ id: vaultName });

      if (resByName?.token) return resByName.token;
      if (resByName?.password) return resByName.password;
      if (resByName?.value) return resByName.value;
    } catch (e) {}

    try {
      const resById = await credentialVaultClient.getCredentialsDetails({ id: vaultId });

      if (resById?.token) return resById.token;
      if (resById?.password) return resById.password;
      if (resById?.value) return resById.value;
    } catch (e) {}

    if (manualApiKey && manualApiKey.trim() !== "") return manualApiKey.trim();

    throw new Error("Unable to read Cohesity API key from credential vault or manual fallback.");
  }

  async function heliosGetJson(url, headers) {
    const response = await fetch(url, {
      method: "GET",
      headers
    });

    const text = await response.text();

    if (!response.ok) {
      return { __error: true, status: response.status, body: text };
    }

    try {
      return JSON.parse(text);
    } catch (e) {
      return { __error: true, status: response.status, body: text };
    }
  }

  async function collectCluster(cluster, commonHeaders) {
    const clusterName = valueOrNA(firstValue(cluster, ["name", "clusterName", "displayName", "ClusterName", "Name"]));
    const clusterId = valueOrNA(firstValue(cluster, ["clusterId", "id", "ClusterId", "Id"]));

    const rows = [];
    const issues = [];
    let defaultPoliciesExcluded = 0;

    if (clusterId === "N/A") {
      issues.push({ Cluster: clusterName, Issue: "Cluster ID missing" });
      return { rows, issues, defaultPoliciesExcluded };
    }

    const headers = {
      ...commonHeaders,
      "accessClusterId": String(clusterId)
    };

    const [policyResp, pgResp] = await Promise.all([
      heliosGetJson(`${baseUrl}/v2/data-protect/policies`, headers),
      heliosGetJson(`${baseUrl}/v2/data-protect/protection-groups?isDeleted=false&includeLastRunInfo=false`, headers)
    ]);

    if (policyResp.__error) {
      issues.push({ Cluster: clusterName, Issue: `Policy fetch failed HTTP ${policyResp.status}` });
      return { rows, issues, defaultPoliciesExcluded };
    }

    if (pgResp.__error) {
      issues.push({ Cluster: clusterName, Issue: `PG fetch failed HTTP ${pgResp.status}` });
    }

    const policies = policyResp?.policies || [];
    const pgs = (pgResp?.protectionGroups || []).filter(pg => pg?.isDeleted !== true);

    const pgCountByPolicyId = new Map();

    for (const pg of pgs) {
      const pid = getPgPolicyId(pg);

      if (pid !== "N/A") {
        pgCountByPolicyId.set(pid, (pgCountByPolicyId.get(pid) || 0) + 1);
      }
    }

    for (const policy of policies.sort((a, b) => valueOrNA(a?.name).localeCompare(valueOrNA(b?.name)))) {
      const policyName = valueOrNA(policy?.name);

      if (isDefaultPolicy(policyName)) {
        defaultPoliciesExcluded++;
        continue;
      }

      const policyId = getPolicyId(policy);
      const pgCount = policyId !== "N/A" ? (pgCountByPolicyId.get(policyId) || 0) : 0;

      const backup = getBackupFields(policy);
      const log = getLogFields(policy);
      const replication = getReplicationFields(policy?.remoteTargetPolicy?.replicationTargets);

      rows.push({
        Cluster: clusterName,
        Policy: policyName,
        "BKP Run": backup.run,
        "BKP Ret": backup.retain,
        "BKP DL": backup.dataLock,
        "Full BKP": getPeriodicFullBackup(policy),
        Quiet: formatQuietTimes(policy?.blackoutWindow),
        Retry: formatRetryOptions(policy?.retryOptions),
        "Log Run": log.run,
        "Log Ret": log.retain,
        "Log DL": log.dataLock,
        "Repl To": replication.replicatedTo,
        "Repl Ret": replication.retain,
        "Repl DL": replication.dataLock,
        "Repl Log Ret": replication.logRetain,
        "Repl Log DL": replication.logDataLock,
        PGs: pgCount
      });
    }

    return { rows, issues, defaultPoliciesExcluded };
  }

  const apiKey = await getApiKey();

  const commonHeaders = {
    "apiKey": apiKey,
    "Accept": "application/json"
  };

  const clustersResp = await heliosGetJson(`${baseUrl}/v2/mcm/cluster-mgmt/info`, commonHeaders);

  if (clustersResp.__error) {
    throw new Error(`Failed to get clusters. HTTP ${clustersResp.status}. ${clustersResp.body}`);
  }

  const clusters =
    clustersResp?.cohesityClusters ||
    clustersResp?.clusters ||
    clustersResp?.clusterInfos ||
    clustersResp?.mcmInfo?.clusterInfos ||
    [];

  const clusterResults = await Promise.all(
    clusters.map(cluster => collectCluster(cluster, commonHeaders))
  );

  const rows = [];
  const clusterIssues = [];
  let defaultPoliciesExcluded = 0;

  for (const result of clusterResults) {
    rows.push(...result.rows);
    clusterIssues.push(...result.issues);
    defaultPoliciesExcluded += result.defaultPoliciesExcluded;
  }

  rows.sort((a, b) =>
    valueOrNA(a.Cluster).localeCompare(valueOrNA(b.Cluster)) ||
    valueOrNA(a.Policy).localeCompare(valueOrNA(b.Policy))
  );

  const policyHeaders = [
    "Cluster",
    "Policy",
    "BKP Run",
    "BKP Ret",
    "BKP DL",
    "Full BKP",
    "Quiet",
    "Retry",
    "Log Run",
    "Log Ret",
    "Log DL",
    "Repl To",
    "Repl Ret",
    "Repl DL",
    "Repl Log Ret",
    "Repl Log DL",
    "PGs"
  ];

  const summaryRows = [
    { Metric: "Clusters discovered", Count: clusters.length },
    { Metric: "Policies exported", Count: rows.length },
    { Metric: "Default policies excluded", Count: defaultPoliciesExcluded }
  ];

  let markdown = "";
  markdown += `# Cohesity Policy Summary\n\n`;
  markdown += `**Report Date ET:** ${nowEtString()}\n\n`;
  markdown += `**Default policies excluded:** Protect Once, Silver, Gold, Bronze\n\n`;

  markdown += `## Summary\n\n`;
  markdown += markdownTable(["Metric", "Count"], summaryRows);
  markdown += `\n`;

  if (clusterIssues.length > 0) {
    markdown += `## Cluster Fetch Issues\n\n`;
    markdown += markdownTable(["Cluster", "Issue"], clusterIssues);
    markdown += `\n`;
  }

  markdown += `## Policy Summary\n\n`;
  markdown += `**Legend:** BKP = Backup, Repl = Replication, Ret = Retention, DL = DataLock, D = Days, W = Weeks, H = Hours, Min = Minutes.\n\n`;

  if (rows.length === 0) {
    markdown += `No policies found.\n\n`;
  } else {
    markdown += markdownTable(policyHeaders, rows);
    markdown += `\n`;
  }

  return {
    reportDateEt: nowEtString(),
    markdown: markdown,
    rows: rows,
    clusterIssues: clusterIssues,
    policiesExported: rows.length,
    defaultPoliciesExcluded: defaultPoliciesExcluded
  };
}
