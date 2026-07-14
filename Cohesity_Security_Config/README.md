# Cohesity Cluster Security Configuration and Password Compliance

## Purpose

Collect cluster security settings from every Cohesity cluster visible through Cohesity Helios and evaluate the returned password policy against the documented enterprise password standard.

Both collectors are strictly read-only and use only HTTP `GET` requests.

## Files

| File | Purpose |
|---|---|
| `Get-CohesitySecurityConfiguration.ps1` | PowerShell 5.1/7 multi-cluster CSV collector and compliance evaluator |
| `Get-CohesitySecurityConfiguration.js` | Dynatrace JavaScript action with structured rows, compliance results, and Markdown email output |
| `Sample_Cohesity_Security_Configuration.csv` | Illustrative output only |

## API Flow

### Cluster discovery

```text
GET /v2/mcm/cluster-mgmt/info
```

### Cluster security configuration

```text
GET /v2/security-config
```

The security configuration request is executed once per cluster with:

```text
accessClusterId: <clusterId>
```

No `POST`, `PUT`, `PATCH`, or `DELETE` request is used.

## Password Standard Evaluation

The scripts evaluate these cluster-level settings:

| Standard requirement | API field | Rule |
|---|---|---|
| Minimum password length | `passwordStrength.minLength` | `>= 15` |
| Password complexity | Four password-strength Boolean fields | At least `3 of 4` enabled |
| Password history | `passwordReuse.numDisallowedOldPasswords` | `>= 6` |
| Minimum password age | `passwordLifetime.minLifetimeDays` | `>= 2` days |
| General maximum password age | `passwordLifetime.maxLifetimeDays` | `1-365` days |
| PCI 90-day numeric value | `passwordLifetime.maxLifetimeDays` | `1-90` days |

### Important scope limitations

- The standard permits an exception to the two-day minimum age for one-time passwords. `/v2/security-config` does not identify OTP usage, so the scripts evaluate the configured cluster-level value directly.
- The PCI rule applies based on PCI scope and whether MFA is used. `/v2/security-config` does not expose those facts.
- `MeetsPCI90DayValue` therefore reports only whether the configured `maxLifetimeDays` value is between 1 and 90. It is not a complete PCI compliance determination.
- Cohesity returns one cluster-level password policy. The endpoint does not prove separate controls for workforce, privileged, functional, or non-human account categories.

## Compliance Output Fields

The scripts add these fields to each cluster row:

| Field | Meaning |
|---|---|
| `PasswordComplexityEnabledCount` | Number of enabled character classes, shown as `x of 4` |
| `PasswordLengthStatus` | `Compliant`, `Non-Compliant`, or `Not Assessed` |
| `PasswordComplexityStatus` | Evaluation against the 3-of-4 requirement |
| `PasswordHistoryStatus` | Evaluation against the six-password history requirement |
| `PasswordMinLifetimeStatus` | Evaluation against the two-day minimum |
| `PasswordMaxLifetime365Status` | Evaluation against the 365-day maximum |
| `MeetsPCI90DayValue` | `Yes`, `No`, or `Not Assessed` for the numeric 90-day threshold |
| `OverallPasswordPolicyStatus` | Overall result for the five general password requirements |
| `ComplianceFindings` | Detailed failed or unavailable checks |

`OverallPasswordPolicyStatus` does not include `MeetsPCI90DayValue`, because PCI applicability cannot be inferred from this API.

## Raw Security Configuration Fields

The original security configuration fields remain in every structured row and CSV:

| Group | Fields |
|---|---|
| Identity | `Cluster` |
| Password strength | `PasswordMinLength`, `PasswordIncludeUpperLetter`, `PasswordIncludeLowerLetter`, `PasswordIncludeNumber`, `PasswordIncludeSpecialChar` |
| Password reuse | `NumDisallowedOldPasswords`, `NumDifferentChars` |
| Password lifetime | `PasswordMinLifetimeDays`, `PasswordMaxLifetimeDays` |
| Account lockout | `MaxFailedLoginAttempts`, `FailedLoginLockTimeDurationMins`, `AccountInactivityTimeDays` |
| General timeouts | `AuthTokenTimeoutMinutes`, `UIInactivityTimeoutMSecs`, `SSHTimeoutInMins` |
| Session management | `SessionManagementEnabled`, `SessionAbsoluteTimeoutSeconds`, `SessionInactivityTimeoutSeconds`, `LimitSessions`, `SessionLimitPerUser`, `SessionLimitSystemWide` |
| Certificate authentication | `CertificateMappingAuthenticationEnabled`, `CertificateMapping`, `CertificateADMapping` |
| Data classification | `IsDataClassified`, `ClassifiedDataMessage`, `UnclassifiedDataMessage` |

`N/A` means the API did not return a value or the cluster-specific GET failed. Query failures are also returned separately.

## PowerShell Configuration

The PowerShell collector uses the shared AES API-key helper pattern:

```powershell
$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\SecurityConfiguration"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
```

Run:

```powershell
.\Get-CohesitySecurityConfiguration.ps1
```

CSV output:

```text
X:\PowerShell\Data\Cohesity\SecurityConfiguration\Cohesity_Security_Configuration_YYYYMMDD_HHmm.csv
```

The console output includes password-standard results, detailed findings, and the original security configuration category tables.

## Dynatrace JavaScript Setup

### Credential Vault

Create or reuse a token/password credential containing the Cohesity Helios API key:

```text
Cohesity_API_Key
```

The JavaScript first searches the vault by name and then uses the configured credential ID fallback:

```javascript
const vaultName = "Cohesity_API_Key";
const vaultId = "credentials_vault-312312";
```

### Workflow action

1. Add a **Run JavaScript** action.
2. Use a task ID such as `cohesity_security_config`.
3. Paste the complete contents of `Get-CohesitySecurityConfiguration.js`.
4. Permit the action to read the Credential Vault entry.
5. Permit outbound HTTPS access to `https://helios.cohesity.com`.

### JavaScript return object

| Property | Purpose |
|---|---|
| `clusterCount` | Number of clusters discovered |
| `successfulClusterCount` | Number of successful security-config GETs |
| `failedClusterCount` | Number of cluster-specific failures |
| `compliantClusterCount` | Clusters meeting all five general password requirements |
| `nonCompliantClusterCount` | Clusters failing one or more assessed general requirements |
| `notAssessedClusterCount` | Clusters with incomplete data or failed collection |
| `pci90ValueMetClusterCount` | Clusters whose configured maximum lifetime is 1-90 days |
| `rowCount` | Number of structured inventory rows |
| `standard` | Numeric thresholds used by the evaluator |
| `columns` | Ordered list of raw and compliance output fields |
| `rows` | Complete flattened security configuration and compliance rows |
| `markdownEmail` | Readable category and compliance tables for email |
| `errors` | Separate per-cluster query failures |

### Email body

```text
{{ result("cohesity_security_config").markdownEmail }}
```

The Markdown output begins with password-standard actual values, compliance statuses, and findings. The original security setting category tables follow.

## Validation

Confirm:

1. `clusterCount` matches the Helios cluster inventory.
2. `rowCount` equals `clusterCount`.
3. Every cluster has one structured row.
4. A cluster with length 15, complexity 3-of-4, history 6, minimum age 2, and maximum age 365 reports `Compliant`.
5. `MeetsPCI90DayValue` is treated separately from the overall general password status.
6. `errors` is empty or contains only known cluster-specific access failures.
7. No API key appears in logs, rows, Markdown, CSV, or errors.
8. Network activity contains only:
   - `GET /v2/mcm/cluster-mgmt/info`
   - `GET /v2/security-config`
