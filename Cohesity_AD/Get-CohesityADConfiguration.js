import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Cohesity Active Directory Configuration Inventory
 *
 * Purpose:
 * - Discover all Cohesity clusters visible through Helios.
 * - Query each cluster's Active Directory configuration.
 * - Return structured rows using the same ten fields as PowerShell.
 * - Generate one compact Markdown table for Dynatrace email.
 * - Return cluster-specific query failures separately in errors.
 *
 * GET-only API flow:
 * 1. GET /v2/mcm/cluster-mgmt/info
 * 2. GET /v2/active-directories?includeTenants=true
 *    The second GET runs once per cluster with accessClusterId.
 *
 * No POST, PUT, PATCH, or DELETE request is used.
 * The Helios API key is read only from Dynatrace Credential Vault.
 */
export default async function () {
  // ------------------------------------------------------------------
  // Configuration
  // ------------------------------------------------------------------
  const baseUrl = "https://helios.cohesity.com";
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";
  const maxMarkdownRows = 1000;

  // Structured rows retain the same ten columns as PowerShell.
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

  // Email omits ADConfigured and uses shorter labels to reduce width.
  const markdownColumns = [
    ["Cluster", "Cluster"],
    ["Domain", "DomainName"],
    ["OU", "OrganizationalUnit"],
    ["Workgroup", "WorkGroupName"],
    ["Machine Accounts", "MachineAccounts"],
    ["Preferred DCs", "PreferredDomainControllers"],
    ["Denied DCs", "DomainControllersDenyList"],
    ["Trusted Domains", "TrustedDomains"],
    ["AD Config ID", "ADConfigurationId"]
  ];

  // ------------------------------------------------------------------
  // Generic normalization helpers
  // ------------------------------------------------------------------
  const normalize = (value) =>
    value === null || value === undefined ? "" : String(value).trim();

  const toArray = (value) => {
    if (value === null || value === undefined) return [];
    return Array.isArray(value) ? value : [value];
  };

  const valueOrNA = (value) => {
    if (value === null || value === undefined) return "N/A";

    if (Array.isArray(value)) {
      const values = value.map(normalize).filter(Boolean);
      return values.length ? values.join(", ") : "N/A";
    }

    return normalize(value) || "N/A";
  };

  const firstValue = (object, propertyNames) => {
    if (!object) return "N/A";

    for (const propertyName of propertyNames) {
      const value = valueOrNA(object[propertyName]);
      if (value !== "N/A") return value;
    }

    return "N/A";
  };

  /**
   * Dynatrace email Markdown treats every pipe as a column separator.
   * Backslash-escaping is not reliable in that renderer. Replace embedded
   * pipes before constructing each row so the final column is not displaced.
   */
  const safeMarkdownCell = (value) =>
    String(value ?? "")
      .replace(/\|/g, " / ")
      .replace(/\r?\n/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  function markdownTable(columnDefinitions, rows) {
    const labels = columnDefinitions.map(([label]) => label);
    const keys = columnDefinitions.map(([, key]) => key);

    const headerRow = `| ${labels.join(" | ")} |`;
    const separatorRow = `| ${labels.map(() => "---").join(" | ")} |`;
    const dataRows = rows.map(
      (row) =>
        `| ${keys
          .map((key) => safeMarkdownCell(row[key]))
          .join(" | ")} |`
    );

    return [headerRow, separatorRow, ...dataRows].join("\n");
  }

  // ------------------------------------------------------------------
  // Cohesity AD field formatting
  // ------------------------------------------------------------------
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

    // Semicolon is deliberate. A pipe inside a Markdown cell breaks the table.
    return values.length ? values.join("; ") : "N/A";
  }

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
      // Continue to the configured credential ID fallback.
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
  // GET 1: Discover clusters through Helios
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
  // GET 2: Query AD configuration once for every cluster
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

    if (Array.isArray(adData)) {
      activeDirectories = adData.filter(Boolean);
    } else if (Array.isArray(adData?.activeDirectories)) {
      activeDirectories = adData.activeDirectories.filter(Boolean);
    } else if (adData && (adData.domainName || adData.id)) {
      activeDirectories = [adData];
    }

    if (!activeDirectories.length) {
      rows.push(emptyRow(clusterName, "No"));
      notConfiguredClusterCount++;
      continue;
    }

    configuredClusterCount++;

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
  // Build one compact Markdown table for the email body
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
    markdownTable(markdownColumns, markdownRows),
    rows.length > maxMarkdownRows
      ? `_Note: Markdown output limited to ${maxMarkdownRows} of ${rows.length} rows._`
      : null,
    errors.length
      ? `_Warning: ${errors.length} cluster query error(s). See the separate errors output._`
      : null
  ]
    .filter((item) => item !== null && item !== undefined)
    .join("\n");

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
