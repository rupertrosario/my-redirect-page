import { credentialVaultClient } from "@dynatrace-sdk/client-classic-environment-v2";

/**
 * Cohesity Password Policy Compliance Report
 *
 * GET-only API flow:
 * 1. GET /v2/mcm/cluster-mgmt/info
 * 2. GET /v2/security-config once per cluster with accessClusterId
 *
 * No POST, PUT, PATCH, or DELETE request is used.
 */
export default async function () {
  const baseUrl = "https://helios.cohesity.com";
  const vaultName = "Cohesity_API_Key";
  const vaultId = "credentials_vault-312312";

  const standard = Object.freeze({
    passwordMinLength: 15,
    complexityRequired: 3,
    disallowedOldPasswords: 6,
    passwordMinLifetimeDays: 2,
    passwordMaxLifetimeDays: 365
  });

  const toArray = (value) => {
    if (value === null || value === undefined) return [];
    return Array.isArray(value) ? value : [value];
  };

  const valueOrNA = (value) => {
    if (value === null || value === undefined) return "N/A";
    const text = String(value).trim();
    return text === "" ? "N/A" : text;
  };

  const firstValue = (object, propertyNames) => {
    if (!object) return "N/A";

    for (const propertyName of propertyNames) {
      const value = valueOrNA(object[propertyName]);
      if (value !== "N/A") return value;
    }

    return "N/A";
  };

  const toFiniteNumber = (value) => {
    if (value === null || value === undefined || value === "N/A") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  };

  const toBoolean = (value) => {
    if (typeof value === "boolean") return value;
    const normalized = String(value).toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
    return null;
  };

  const safeMarkdownCell = (value) =>
    valueOrNA(value)
      .replace(/\|/g, " / ")
      .replace(/\r?\n/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\S{33,}/g, (token) => token.match(/.{1,32}/g).join("\u200B"));

  const markdownTable = (columnDefinitions, rows) => {
    const labels = columnDefinitions.map(([label]) => label);
    const keys = columnDefinitions.map(([, key]) => key);

    const header = `| ${labels.join(" | ")} |`;
    const separator = `| ${labels.map(() => "---").join(" | ")} |`;
    const dataRows = rows.map(
      (row) => `| ${keys.map((key) => safeMarkdownCell(row[key])).join(" | ")} |`
    );

    return [header, separator, ...dataRows].join("\n");
  };

  const evaluatePasswordPolicy = (clusterName, securityConfig) => {
    const passwordMinLength = toFiniteNumber(
      securityConfig?.passwordStrength?.minLength
    );
    const passwordHistory = toFiniteNumber(
      securityConfig?.passwordReuse?.numDisallowedOldPasswords
    );
    const passwordMinAge = toFiniteNumber(
      securityConfig?.passwordLifetime?.minLifetimeDays
    );
    const passwordMaxAge = toFiniteNumber(
      securityConfig?.passwordLifetime?.maxLifetimeDays
    );

    const complexityValues = [
      securityConfig?.passwordStrength?.includeUpperLetter,
      securityConfig?.passwordStrength?.includeLowerLetter,
      securityConfig?.passwordStrength?.includeNumber,
      securityConfig?.passwordStrength?.includeSpecialChar
    ].map(toBoolean);

    const complexityKnown = complexityValues.every((value) => value !== null);
    const complexityCount = complexityKnown
      ? complexityValues.filter(Boolean).length
      : null;

    const checks = [
      passwordMinLength === null
        ? "Not Assessed"
        : passwordMinLength >= standard.passwordMinLength
          ? "Compliant"
          : "Non-Compliant",
      complexityCount === null
        ? "Not Assessed"
        : complexityCount >= standard.complexityRequired
          ? "Compliant"
          : "Non-Compliant",
      passwordHistory === null
        ? "Not Assessed"
        : passwordHistory >= standard.disallowedOldPasswords
          ? "Compliant"
          : "Non-Compliant",
      passwordMinAge === null
        ? "Not Assessed"
        : passwordMinAge >= standard.passwordMinLifetimeDays
          ? "Compliant"
          : "Non-Compliant",
      passwordMaxAge === null
        ? "Not Assessed"
        : passwordMaxAge > 0 &&
            passwordMaxAge <= standard.passwordMaxLifetimeDays
          ? "Compliant"
          : "Non-Compliant"
    ];

    const overallStatus = checks.includes("Not Assessed")
      ? "Not Assessed"
      : checks.every((status) => status === "Compliant")
        ? "Compliant"
        : "Non-Compliant";

    const findings = [];

    if (passwordMinLength === null) {
      findings.push("Password minimum length was not returned");
    } else if (passwordMinLength < standard.passwordMinLength) {
      findings.push(`Length below ${standard.passwordMinLength}`);
    }

    if (complexityCount === null) {
      findings.push("One or more complexity flags were not returned");
    } else if (complexityCount < standard.complexityRequired) {
      findings.push(`Complexity below ${standard.complexityRequired} of 4`);
    }

    if (passwordHistory === null) {
      findings.push("Password history was not returned");
    } else if (passwordHistory < standard.disallowedOldPasswords) {
      findings.push(`History below ${standard.disallowedOldPasswords}`);
    }

    if (passwordMinAge === null) {
      findings.push("Minimum password age was not returned");
    } else if (passwordMinAge < standard.passwordMinLifetimeDays) {
      findings.push(`Minimum age below ${standard.passwordMinLifetimeDays}`);
    }

    if (passwordMaxAge === null) {
      findings.push("Maximum password age was not returned");
    } else if (passwordMaxAge <= 0) {
      findings.push("Maximum age is not enabled");
    } else if (passwordMaxAge > standard.passwordMaxLifetimeDays) {
      findings.push(`Maximum age exceeds ${standard.passwordMaxLifetimeDays}`);
    }

    return {
      Cluster: clusterName,
      PasswordMinLength: passwordMinLength ?? "N/A",
      PasswordComplexityEnabledCount:
        complexityCount === null ? "N/A" : `${complexityCount} of 4`,
      NumDisallowedOldPasswords: passwordHistory ?? "N/A",
      PasswordMinLifetimeDays: passwordMinAge ?? "N/A",
      PasswordMaxLifetimeDays: passwordMaxAge ?? "N/A",
      OverallPasswordPolicyStatus: overallStatus,
      ComplianceFindings: findings.length ? findings.join("; ") : "None"
    };
  };

  const emptyRow = (clusterName, finding) => ({
    Cluster: clusterName,
    PasswordMinLength: "N/A",
    PasswordComplexityEnabledCount: "N/A",
    NumDisallowedOldPasswords: "N/A",
    PasswordMinLifetimeDays: "N/A",
    PasswordMaxLifetimeDays: "N/A",
    OverallPasswordPolicyStatus: "Not Assessed",
    ComplianceFindings: finding
  });

  const getApiKey = async () => {
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
      // Continue to configured credential ID fallback.
    }

    try {
      const detail = await credentialVaultClient.getCredentialsDetails({
        id: vaultId
      });
      return detail?.token || detail?.password || null;
    } catch {
      return null;
    }
  };

  const getJson = async (url, headers) => {
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
            responseText || `HTTP ${response.status} ${response.statusText}`,
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
  };

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
    return leftName.localeCompare(rightName, undefined, { sensitivity: "base" });
  });

  if (!clusters.length) {
    return {
      clusterCount: 0,
      successfulClusterCount: 0,
      failedClusterCount: 0,
      compliantClusterCount: 0,
      nonCompliantClusterCount: 0,
      notAssessedClusterCount: 0,
      rowCount: 0,
      rows: [],
      markdownEmail:
        "### Cohesity Password Policy Compliance\n\n_No clusters returned from Cohesity Helios._",
      errors: []
    };
  }

  const rows = [];
  const errors = [];
  let successfulClusterCount = 0;

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
      rows.push(emptyRow(clusterName, "Cluster ID missing"));
      errors.push({
        cluster: clusterName,
        status: 0,
        error: "Cluster ID missing"
      });
      continue;
    }

    const securityResponse = await getJson(`${baseUrl}/v2/security-config`, {
      ...commonHeaders,
      accessClusterId: String(clusterIdValue)
    });

    if (!securityResponse.ok) {
      rows.push(
        emptyRow(clusterName, "Cluster security configuration was not returned")
      );
      errors.push({
        cluster: clusterName,
        status: securityResponse.status,
        error: securityResponse.error
      });
      continue;
    }

    rows.push(evaluatePasswordPolicy(clusterName, securityResponse.data || {}));
    successfulClusterCount++;
  }

  rows.sort((left, right) =>
    left.Cluster.localeCompare(right.Cluster, undefined, {
      sensitivity: "base"
    })
  );

  const compliantClusterCount = rows.filter(
    (row) => row.OverallPasswordPolicyStatus === "Compliant"
  ).length;
  const nonCompliantClusterCount = rows.filter(
    (row) => row.OverallPasswordPolicyStatus === "Non-Compliant"
  ).length;
  const notAssessedClusterCount = rows.filter(
    (row) => row.OverallPasswordPolicyStatus === "Not Assessed"
  ).length;

  const reportDate = new Date().toLocaleDateString("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "short",
    day: "2-digit"
  });

  const summaryRows = [
    { Metric: "Clusters discovered", Count: clusters.length },
    { Metric: "Clusters successfully read", Count: successfulClusterCount },
    { Metric: "Password policy compliant", Count: compliantClusterCount },
    {
      Metric: "Password policy non-compliant",
      Count: nonCompliantClusterCount
    },
    { Metric: "Password policy not assessed", Count: notAssessedClusterCount },
    { Metric: "Cluster query errors", Count: errors.length }
  ];

  const complianceColumns = [
    ["Cluster", "Cluster"],
    ["Min Length (Expected >= 15)", "PasswordMinLength"],
    ["Complexity (Expected >= 3/4)", "PasswordComplexityEnabledCount"],
    ["History (Expected >= 6)", "NumDisallowedOldPasswords"],
    ["Min Age (Expected >= 2)", "PasswordMinLifetimeDays"],
    ["Max Age (Expected <= 365)", "PasswordMaxLifetimeDays"],
    ["Overall Status", "OverallPasswordPolicyStatus"],
    ["Findings", "ComplianceFindings"]
  ];

  const markdownEmail = [
    `### Cohesity Password Policy Compliance — ${reportDate}`,
    "",
    "#### Summary",
    markdownTable(
      [
        ["Metric", "Metric"],
        ["Count", "Count"]
      ],
      summaryRows
    ),
    "",
    "#### Password Policy Compliance",
    markdownTable(complianceColumns, rows),
    "",
    "_PCI note: The 90-day rule is not assessed because /v2/security-config does not expose PCI scope or MFA usage._"
  ].join("\n");

  return {
    clusterCount: clusters.length,
    successfulClusterCount,
    failedClusterCount: errors.length,
    compliantClusterCount,
    nonCompliantClusterCount,
    notAssessedClusterCount,
    rowCount: rows.length,
    standard,
    rows,
    markdownEmail,
    errors
  };
}
