import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Cohesity Cluster GFlag Report — split common/specific emails (ET)
 *
 * Outputs:
 * - commonMarkdownEmail: GFlags with the same Service + GFlag + Value on every
 *   successfully queried cluster.
 * - specificMarkdownEmail: GFlags that are missing from one or more clusters,
 *   or have different values between clusters, grouped by cluster.
 *
 * Characteristics:
 * - GET-only
 * - Markdown-only
 * - 502-safe: continues per cluster
 * - Comparison excludes Reason and AppliedAtET
 */
export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const MAX_COMMON_ROWS = 3000;
  const MAX_SPECIFIC_ROWS = 3000;

  // ======================
  // AUTH (READ-ONLY)
  // ======================
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";
  let apiKey = null;

  async function getKeyByName(name) {
    const all = await credentialVaultClient.getCredentials();
    const creds = all?.credentials || [];
    const found = creds.find((c) => c?.name === name);
    if (!found) return null;

    const detail = await credentialVaultClient.getCredentialsDetails({
      id: found.id
    });
    return detail?.token || detail?.password || null;
  }

  try {
    apiKey = await getKeyByName(vaultName);
    if (!apiKey) throw new Error("Vault name lookup returned no key");
  } catch {
    const detail = await credentialVaultClient.getCredentialsDetails({
      id: vaultId
    });
    apiKey = detail?.token || detail?.password || null;
  }

  if (!apiKey) return { error: "No Cohesity API key available" };

  const commonHeaders = {
    accept: "application/json",
    apiKey
  };

  // ======================
  // HELPERS
  // ======================
  const norm = (v) =>
    v === null || v === undefined ? "" : String(v).trim();

  const toArray = (v) =>
    !v ? [] : Array.isArray(v) ? v : [v];

  const safeCell = (v) =>
    v === null || v === undefined
      ? ""
      : String(v).replace(/\|/g, " ").replace(/\r?\n/g, " ");

  function mdTable(headers, rows) {
    const head = "| " + headers.join(" | ") + " |";
    const sep = "| " + headers.map(() => "---").join(" | ") + " |";
    const body = rows.map(
      (row) =>
        "| " + headers.map((h) => safeCell(row[h])).join(" | ") + " |"
    );
    return [head, sep, ...body].join("\n");
  }

  function normalizeReason(reason) {
    const raw = norm(reason);
    if (!raw) return "";

    const lower = raw.toLowerCase();
    if (lower.includes("auto set by cohesity")) return "Auto (Cohesity)";
    if (
      lower.includes("don't update manually") ||
      lower.includes("do not update")
    ) {
      return "System Managed";
    }
    if (lower.includes("default")) return "Default";

    const match = raw.match(/case#\s*\d+/i);
    if (match) return match[0].replace(/\s+/g, "");

    return raw.split(/[.;]/)[0].trim();
  }

  function wrapValueForEmail(value) {
    const text = String(value ?? "");
    if (text.length < 120) return text;

    return text
      .replace(/\s+/g, " ")
      .replace(/,/g, ",\n")
      .replace(/;/g, ";\n")
      .replace(/\|/g, " ")
      .replace(/}/g, "}\n")
      .replace(/]/g, "]\n")
      .replace(/:/g, ":\n")
      .trim();
  }

  function toEtHumanTime(epochSeconds) {
    if (!epochSeconds) return "";
    const value = Number(epochSeconds);
    if (!Number.isFinite(value)) return "";

    return new Date(value * 1000).toLocaleString("en-US", {
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

  function rawValue(value) {
    try {
      return typeof value === "string"
        ? value
        : JSON.stringify(value ?? "");
    } catch {
      return String(value ?? "");
    }
  }

  function keyFor(service, gflag) {
    return `${service}\u0000${gflag}`;
  }

  function sortRows(rows) {
    rows.sort(
      (a, b) =>
        a.Service.localeCompare(b.Service) ||
        a.GFlag.localeCompare(b.GFlag) ||
        String(a.Value).localeCompare(String(b.Value))
    );
    return rows;
  }

  async function getJsonSafe(url, headers) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers
      });

      if (!response.ok) {
        let text = "";
        try {
          text = await response.text();
        } catch {}

        return {
          ok: false,
          status: response.status,
          error: text,
          data: null
        };
      }

      return {
        ok: true,
        status: response.status,
        data: await response.json()
      };
    } catch (error) {
      return {
        ok: false,
        status: 0,
        error: String(error),
        data: null
      };
    }
  }

  // ======================
  // FETCH CLUSTERS
  // ======================
  const clusterResponse = await getJsonSafe(
    `${baseUrl}/v2/mcm/cluster-mgmt/info`,
    commonHeaders
  );

  if (!clusterResponse.ok) {
    return {
      error: "Failed to fetch clusters",
      status: clusterResponse.status,
      details: clusterResponse.error
    };
  }

  const clusters = toArray(clusterResponse.data?.cohesityClusters).sort(
    (a, b) =>
      norm(a?.clusterName).localeCompare(norm(b?.clusterName), undefined, {
        sensitivity: "base"
      })
  );

  // clusterName -> Map(Service+GFlag -> row)
  const clusterMaps = new Map();
  const errors = [];
  let fetchedRowCount = 0;

  for (const cluster of clusters) {
    const clusterId = norm(cluster?.clusterId);
    if (!clusterId) continue;

    const clusterName =
      norm(cluster?.clusterName) || `Cluster-${clusterId}`;

    const headers = {
      ...commonHeaders,
      accessClusterId: clusterId
    };

    const gflagResponse = await getJsonSafe(
      `${baseUrl}/v2/clusters/gflag`,
      headers
    );

    if (!gflagResponse.ok) {
      errors.push({
        scope: "gflags",
        cluster: clusterName,
        status: gflagResponse.status,
        error: gflagResponse.error
      });
      continue;
    }

    const services = Array.isArray(gflagResponse.data)
      ? gflagResponse.data
      : toArray(gflagResponse.data?.items);

    const flagMap = new Map();

    for (const service of services) {
      const serviceName =
        norm(service?.serviceName) || "UNKNOWN_SERVICE";

      for (const flag of toArray(service?.gflags)) {
        const flagName = norm(flag?.name);
        if (!flagName) continue;

        const valueRaw = rawValue(flag?.value);
        const comparisonKey = keyFor(serviceName, flagName);

        flagMap.set(comparisonKey, {
          Service: serviceName,
          GFlag: flagName,
          RawValue: valueRaw,
          Value: wrapValueForEmail(valueRaw),
          Reason: normalizeReason(flag?.reason),
          AppliedAtET: toEtHumanTime(flag?.timestamp)
        });

        fetchedRowCount++;
      }
    }

    clusterMaps.set(clusterName, flagMap);
  }

  const successfulClusters = Array.from(clusterMaps.keys()).sort((a, b) =>
    a.localeCompare(b, undefined, { sensitivity: "base" })
  );

  if (!successfulClusters.length) {
    return {
      clusterCount: clusters.length,
      successfulClusterCount: 0,
      fetchedRowCount,
      commonRowCount: 0,
      specificRowCount: 0,
      errors,
      commonMarkdownEmail: "No clusters returned GFlag data.",
      specificMarkdownEmail: "No clusters returned GFlag data.",
      markdownEmail: "No clusters returned GFlag data."
    };
  }

  // ======================
  // CLASSIFY COMMON/SPECIFIC
  // ======================
  const allKeys = new Set();
  for (const flagMap of clusterMaps.values()) {
    for (const key of flagMap.keys()) allKeys.add(key);
  }

  const commonKeys = new Set();

  for (const key of allKeys) {
    let expectedValue = null;
    let common = true;

    for (const clusterName of successfulClusters) {
      const row = clusterMaps.get(clusterName)?.get(key);
      if (!row) {
        common = false;
        break;
      }

      if (expectedValue === null) expectedValue = row.RawValue;
      else if (row.RawValue !== expectedValue) {
        common = false;
        break;
      }
    }

    if (common) commonKeys.add(key);
  }

  const commonRows = [];
  const firstClusterMap = clusterMaps.get(successfulClusters[0]);

  for (const key of commonKeys) {
    const row = firstClusterMap.get(key);
    commonRows.push({
      Service: row.Service,
      GFlag: row.GFlag,
      Value: row.Value,
      ClusterCount: successfulClusters.length
    });
  }
  sortRows(commonRows);

  const specificByCluster = new Map();
  let specificRowCount = 0;

  for (const clusterName of successfulClusters) {
    const flagMap = clusterMaps.get(clusterName);
    const rows = [];

    for (const [key, row] of flagMap.entries()) {
      if (commonKeys.has(key)) continue;

      rows.push({
        Service: row.Service,
        GFlag: row.GFlag,
        Value: row.Value,
        Reason: row.Reason,
        AppliedAtET: row.AppliedAtET
      });
      specificRowCount++;
    }

    if (rows.length) specificByCluster.set(clusterName, sortRows(rows));
  }

  // ======================
  // MARKDOWN OUTPUTS
  // ======================
  const reportDate = new Date().toLocaleDateString("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "short",
    day: "2-digit"
  });

  const errorNote = errors.length
    ? `_Warning: ${errors.length} cluster query error(s). Common classification uses only ${successfulClusters.length} successfully queried cluster(s)._`
    : "";

  const renderedCommonRows = commonRows.slice(0, MAX_COMMON_ROWS);
  const commonMarkdownEmail = [
    `### Cohesity Common GFlag Report — ${reportDate}`,
    "",
    `Common means the same Service, GFlag and Value exist on all ${successfulClusters.length} successfully queried clusters.`,
    "",
    renderedCommonRows.length
      ? mdTable(
          ["Service", "GFlag", "Value", "ClusterCount"],
          renderedCommonRows
        )
      : "_No common GFlags found._",
    commonRows.length > MAX_COMMON_ROWS
      ? `_Note: Common email capped at ${MAX_COMMON_ROWS} of ${commonRows.length} rows._`
      : "",
    errorNote
  ]
    .filter(Boolean)
    .join("\n");

  const specificSections = [];
  let renderedSpecificCount = 0;
  let clusterIndex = 1;

  for (const clusterName of successfulClusters) {
    const rows = specificByCluster.get(clusterName) || [];
    if (!rows.length || renderedSpecificCount >= MAX_SPECIFIC_ROWS) continue;

    const remaining = MAX_SPECIFIC_ROWS - renderedSpecificCount;
    const renderedRows = rows.slice(0, remaining);

    specificSections.push(`**${clusterIndex}. ${safeCell(clusterName)}**`);
    specificSections.push("");
    specificSections.push(
      mdTable(
        ["Service", "GFlag", "Value", "Reason", "AppliedAtET"],
        renderedRows
      )
    );
    specificSections.push("\n---\n");

    renderedSpecificCount += renderedRows.length;
    clusterIndex++;
  }

  const specificMarkdownEmail = [
    `### Cohesity Cluster-Specific GFlag Report — ${reportDate}`,
    "",
    "This report contains only GFlags that are missing from one or more clusters or have different values between clusters.",
    "",
    specificSections.length
      ? specificSections.join("\n")
      : "_No cluster-specific GFlags found._",
    specificRowCount > MAX_SPECIFIC_ROWS
      ? `_Note: Specific email capped at ${MAX_SPECIFIC_ROWS} of ${specificRowCount} rows._`
      : "",
    errorNote
  ]
    .filter(Boolean)
    .join("\n");

  return {
    clusterCount: clusters.length,
    successfulClusterCount: successfulClusters.length,
    failedClusterCount: errors.length,
    fetchedRowCount,
    commonRowCount: commonRows.length,
    specificRowCount,
    commonMarkdownEmail,
    specificMarkdownEmail,
    // Backward-compatible alias for the existing email task.
    markdownEmail: specificMarkdownEmail,
    errors
  };
}
