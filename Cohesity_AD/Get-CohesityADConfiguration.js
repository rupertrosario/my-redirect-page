import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Cohesity Active Directory Configuration Inventory
 *
 * Purpose:
 * - Discover every Cohesity cluster visible through Helios.
 * - Query each cluster's Active Directory configuration.
 * - Return structured rows using the same ten columns as the PowerShell CSV.
 * - Generate one Markdown table for a Dynatrace email action.
 * - Return per-cluster query failures separately in the errors array.
 *
 * GET-only API flow:
 * 1. GET /v2/mcm/cluster-mgmt/info
 *    Discovers all clusters visible to the Helios API key.
 * 2. GET /v2/active-directories?includeTenants=true
 *    Runs once per cluster with accessClusterId in the request headers.
 *
 * The script does not use POST, PUT, PATCH, or DELETE.
 * The API key is read only from Dynatrace Credential Vault.
 */
export default async function () {
  // ------------------------------------------------------------------
  // Configuration
  // ------------------------------------------------------------------
  const baseUrl = "https://helios.cohesity.com";
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";
  const maxMarkdownRows = 1000;

  // Keep this order identical to the PowerShell CSV output.
  const columns = [
    "Cluster",
    "ADConfigured",
    "DomainName",
    "OrganizationalUnit",
    "WorkGroupName",
    "MachineAccounts",
    "PreferredDomainControllers",
    "DomainControllersDenyList",
    "TrustedDomains",
    "ADConfigurationId"
  ];

  // ------------------------------------------------------------------
  // Generic value and array helpers
  // ------------------------------------------------------------------
  const normalize = (value) =>
    value === null || value === undefined ? "" : String(value).trim();

  const toArray = (value) => {
    if (value === null || value === undefined) return [];
    return Array.isArray(value) ? value : [value];
  };

  // Convert empty or missing API values to N/A for consistent reporting.
  const valueOrNA = (value) => {
    if (value === null || value === undefined) return "N/A";

    if (Array.isArray(value)) {
      const values = value.map(normalize).filter(Boolean);
      return values.length ? values.join(", ") : "N/A";
    }

    return normalize(value) || "N/A";
  };

  // Return the first populated property from a list of possible API names.
  const firstValue = (object, propertyNames) => {
    if (!object) return "N/A";

    for (const propertyName of propertyNames) {
      const value = valueOrNA(object[propertyName]);
      if (value !== "N/A") return value;
    }

    return "N/A";
  };

  // Escape characters that would otherwise break a Markdown table cell.
  const escapeMarkdownCell = (value) =>
    String(value ?? "")
      .replace(/\\/g, "\\\\")
      .replace(/\|/g, "\\|")
      .replace(/\r?\n/g, " ");

  // Build one Markdown table from the structured result rows.
  function markdownTable(headers, rows) {
    const headerRow = `| ${headers.map(escapeMarkdownCell).join(" | ")} |`;
    const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;
    const dataRows = rows.map(
      (row) =>
        `| ${headers
          .map((header) => escapeMarkdownCell(row[header]))
          .join(" | ")} |`
    );

    return [headerRow, separatorRow, ...dataRows].join("\n");
  }

  // ------------------------------------------------------------------
  // Cohesity AD field-formatting helpers
  // ------------------------------------------------------------------

  // Format preferred domain controllers as:
  // controller-name [status]; controller-name [status]
  function formatNameStatusList(items) {
    const values = [];

    for (const item of toArray(items)) {
      if (!item) continue;

      const name = firstValue(item, ["name", "dnsHostName", "hostName"]);
      const status = firstValue(item, ["status", "state"]);

      if (name !== "N/A" && status !== "N/A") {
        values.push(`${name} [${status}]`);
      } else if (name !== "N/A") {
        values.push(name);
      } else if (status !== "N/A") {
        values.push(`Status=${status}`);
      }
    }

    return values.length ? values.join("; ") : "N/A";
  }

  // Format machine accounts as:
  // account-name; DNS=dns-name | second-account
  function formatMachineAccounts(machineAccounts) {
    const values = [];

    for (const account of toArray(machineAccounts)) {
      if (!account) continue;

      const name = firstValue(account, ["name"]);
      const dnsName = firstValue(account, ["dnsHostName"]);
      const parts = [];

      if (name !== "N/A") parts.push(name);
      if (dnsName !== "N/A" && dnsName !== name) {
        parts.push(`DNS=${dnsName}`);
      }

      if (parts.length) values.push(parts.join("; "));
    }

    return values.length ? values.join(" | ") : "N/A";
  }

  // Combine trustedDomains and whitelistedDomains into one report column.
  function formatTrustedDomains(trustedDomainParams) {
    if (!trustedDomainParams) return "N/A";

    const values = [];

    for (const trustedDomain of toArray(trustedDomainParams.trustedDomains)) {
      if (!trustedDomain) continue;

      const domainName = firstValue(trustedDomain, ["domainName"]);
      if (domainName !== "N/A" && !values.includes(domainName)) {
        values.push(domainName);
      }
    }

    for (const domain of toArray(trustedDomainParams.whitelistedDomains)) {
      const domainName = valueOrNA(domain);
      if (domainName !== "N/A" && !values.includes(domainName)) {
        values.push(domainName);
      }
    }

    return values.length ? values.join(", ") : "N/A";
  }

  // Create a complete ten-column row for clusters with no AD configuration
  // or clusters whose AD query could not be completed.
  function emptyRow(clusterName, adConfigured) {
    return {
      Cluster: clusterName,
      ADConfigured: adConfigured,
      DomainName: "N/A",
      OrganizationalUnit: "N/A",
      WorkGroupName: "N/A",
      MachineAccounts: "N/A",
      PreferredDomainControllers: "N/A",
      DomainControllersDenyList: "N/A",
      TrustedDomains: "N/A",
      ADConfigurationId: "N/A"
    };
  }

  // ------------------------------------------------------------------
  // Dynatrace Credential Vault
  // ------------------------------------------------------------------

  // First find the API key credential by name. If the vault list lookup is
  // unavailable or no key is found, use the configured credential ID.
  async function getApiKey() {
    try {
      const credentialList = await credentialVaultClient.getCredentials();
      const credential = toArray(credentialList?.credentials).find(
        (item) => item?.name === vaultName
      );

      if (credential?.id) {
        const detail = await credentialVaultClient.getCredentialsDetails({
          id: credential.id
        });

        const apiKey = detail?.token || detail?.password || null;
        if (apiKey) return apiKey;
      }
    } catch {
      // Continue to the configured vault ID fallback.
    }

    try {
      const detail = await credentialVaultClient.getCredentialsDetails({
        id: vaultId
      });

      return detail?.token || detail?.password || null;
    } catch {
      return null;
    }
  }

  // ------------------------------------------------------------------
  // GET-only HTTP wrapper
  // ------------------------------------------------------------------

  // Every Cohesity API request passes through this function. The HTTP method
  // is fixed to GET so the workflow cannot modify Cohesity configuration.
  async function getJson(url, headers) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers
      });

      if (!response.ok) {
        let responseText = "";

        try {
          responseText = await response.text();
        } catch {
          responseText = "";
        }

        return {
          ok: false,
          status: response.status,
          error:
            responseText ||
            `HTTP ${response.status} ${response.statusText}`,
          data: null
        };
      }

      return {
        ok: true,
        status: response.status,
        error: "",
        data: await response.json()
      };
    } catch (error) {
      return {
        ok: false,
        status: 0,
        error: error instanceof Error ? error.message : String(error),
        data: null
      };
    }
  }

  // ------------------------------------------------------------------
  // Authentication
  // ------------------------------------------------------------------
  const apiKey = await getApiKey();

  if (!apiKey) {
    return {
      error: "No Cohesity API key available in Credential Vault",
      rows: [],
      markdownEmail: "",
      errors: []
    };
  }

  const commonHeaders = {
    accept: "application/json",
    apiKey
  };

  // ------------------------------------------------------------------
  // GET 1: Discover all clusters visible through Cohesity Helios
  // Endpoint: GET /v2/mcm/cluster-mgmt/info
  // ------------------------------------------------------------------
  const clusterResponse = await getJson(
    `${baseUrl}/v2/mcm/cluster-mgmt/info`,
    commonHeaders
  );

  if (!clusterResponse.ok) {
    return {
      error: "Failed to query Cohesity Helios clusters",
      status: clusterResponse.status,
      details: clusterResponse.error,
      rows: [],
      markdownEmail: "",
      errors: []
    };
  }

  const clusterData = clusterResponse.data || {};

  // Support the possible cluster-list response wrappers returned by Helios.
  const clusters = toArray(
    clusterData.cohesityClusters ||
      clusterData.clusters ||
      clusterData.clusterInfos ||
      clusterData?.mcmInfo?.clusterInfos
  ).sort((left, right) => {
    const leftName = firstValue(left, [
      "name",
      "clusterName",
      "displayName",
      "ClusterName",
      "Name"
    ]);

    const rightName = firstValue(right, [
      "name",
      "clusterName",
      "displayName",
      "ClusterName",
      "Name"
    ]);

    return leftName.localeCompare(rightName, undefined, {
      sensitivity: "base"
    });
  });

  if (!clusters.length) {
    return {
      clusterCount: 0,
      configuredClusterCount: 0,
      notConfiguredClusterCount: 0,
      failedClusterCount: 0,
      rowCount: 0,
      columns,
      rows: [],
      markdownEmail:
        "### Cohesity Active Directory Configuration\n\n_No clusters returned from Cohesity Helios._",
      errors: []
    };
  }

  const rows = [];
  const errors = [];
  let configuredClusterCount = 0;
  let notConfiguredClusterCount = 0;

  // ------------------------------------------------------------------
  // GET 2: Query AD configuration separately for every discovered cluster
  // Endpoint: GET /v2/active-directories?includeTenants=true
  // Header: accessClusterId=<current cluster ID>
  // ------------------------------------------------------------------
  for (const cluster of clusters) {
    const clusterNameValue = firstValue(cluster, [
      "name",
      "clusterName",
      "displayName",
      "ClusterName",
      "Name"
    ]);

    const clusterIdValue = firstValue(cluster, [
      "clusterId",
      "id",
      "ClusterId",
      "Id"
    ]);

    const clusterName =
      clusterNameValue === "N/A" ? "Unknown" : clusterNameValue;

    // A missing cluster ID prevents a targeted query. Keep the cluster in the
    // main table as Unknown and record the reason in the separate errors array.
    if (clusterIdValue === "N/A") {
      rows.push(emptyRow(clusterName, "Unknown"));
      errors.push({
        cluster: clusterName,
        status: 0,
        error: "Cluster ID missing"
      });
      continue;
    }

    const clusterHeaders = {
      ...commonHeaders,
      accessClusterId: String(clusterIdValue)
    };

    const adResponse = await getJson(
      `${baseUrl}/v2/active-directories?includeTenants=true`,
      clusterHeaders
    );

    // Keep one Unknown row in the inventory and store the actual HTTP failure
    // separately so a failed query is not treated as AD not configured.
    if (!adResponse.ok) {
      rows.push(emptyRow(clusterName, "Unknown"));
      errors.push({
        cluster: clusterName,
        status: adResponse.status,
        error: adResponse.error
      });
      continue;
    }

    const adData = adResponse.data;
    let activeDirectories = [];

    // Normalize array, wrapped-array, and single-object API responses.
    if (Array.isArray(adData)) {
      activeDirectories = adData.filter(Boolean);
    } else if (Array.isArray(adData?.activeDirectories)) {
      activeDirectories = adData.activeDirectories.filter(Boolean);
    } else if (adData && (adData.domainName || adData.id)) {
      activeDirectories = [adData];
    }

    // An empty successful response means AD is not configured on the cluster.
    if (!activeDirectories.length) {
      rows.push(emptyRow(clusterName, "No"));
      notConfiguredClusterCount++;
      continue;
    }

    configuredClusterCount++;

    // Create one report row per returned AD configuration.
    for (const activeDirectory of activeDirectories) {
      rows.push({
        Cluster: clusterName,
        ADConfigured: "Yes",
        DomainName: firstValue(activeDirectory, ["domainName"]),
        OrganizationalUnit: firstValue(activeDirectory, [
          "organizationalUnitName"
        ]),
        WorkGroupName: firstValue(activeDirectory, ["workGroupName"]),
        MachineAccounts: formatMachineAccounts(
          activeDirectory.machineAccounts
        ),
        PreferredDomainControllers: formatNameStatusList(
          activeDirectory.preferredDomainControllers
        ),
        DomainControllersDenyList: valueOrNA(
          activeDirectory.domainControllersDenyList
        ),
        TrustedDomains: formatTrustedDomains(
          activeDirectory.trustedDomainParams
        ),
        ADConfigurationId: firstValue(activeDirectory, ["id"])
      });
    }
  }

  // Match PowerShell ordering: Cluster first, then DomainName.
  rows.sort(
    (left, right) =>
      left.Cluster.localeCompare(right.Cluster, undefined, {
        sensitivity: "base"
      }) ||
      left.DomainName.localeCompare(right.DomainName, undefined, {
        sensitivity: "base"
      })
  );

  // ------------------------------------------------------------------
  // Build the single Markdown table used by the email action
  // ------------------------------------------------------------------
  const reportDate = new Date().toLocaleDateString("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "short",
    day: "2-digit"
  });

  const markdownRows = rows.slice(0, maxMarkdownRows);
  const markdownEmail = [
    `### Cohesity Active Directory Configuration — ${reportDate}`,
    "",
    markdownTable(columns, markdownRows),
    rows.length > maxMarkdownRows
      ? `_Note: Markdown output limited to ${maxMarkdownRows} of ${rows.length} rows._`
      : null,
    errors.length
      ? `_Warning: ${errors.length} cluster query error(s). See the separate errors output._`
      : null
  ]
    .filter((item) => item !== null && item !== undefined)
    .join("\n");

  // ------------------------------------------------------------------
  // Dynatrace workflow outputs
  // rows          : structured ten-column inventory
  // markdownEmail : one Markdown table for the email body
  // errors        : separate per-cluster query failures
  // ------------------------------------------------------------------
  return {
    clusterCount: clusters.length,
    configuredClusterCount,
    notConfiguredClusterCount,
    failedClusterCount: errors.length,
    rowCount: rows.length,
    columns,
    rows,
    markdownEmail,
    errors
  };
}
