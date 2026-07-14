# Cohesity Cluster Security Configuration

## Purpose

Collect cluster security settings from every Cohesity cluster visible through Cohesity Helios.

Both collectors are strictly read-only and use only HTTP `GET` requests.

## Files

| File | Purpose |
|---|---|
| `Get-CohesitySecurityConfiguration.ps1` | PowerShell 5.1/7 multi-cluster CSV collector |
| `Get-CohesitySecurityConfiguration.js` | Dynatrace JavaScript action with structured rows and Markdown email output |
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

## Output Fields

The scripts return one row per cluster with these fields:

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

The console output is split into readable category tables. The exported CSV contains the complete flattened row.

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
| `rowCount` | Number of structured inventory rows |
| `columns` | Ordered list of output fields |
| `rows` | Complete flattened security configuration rows |
| `markdownEmail` | Readable category tables for email |
| `errors` | Separate per-cluster query failures |

### Email body

```text
{{ result("cohesity_security_config").markdownEmail }}
```

`markdownEmail` uses six compact tables so all security settings remain readable:

1. Password Strength
2. Password Reuse and Lifetime
3. Account Lockout and General Timeouts
4. Session Management
5. Certificate Authentication
6. Data Classification

The structured `rows` output and PowerShell CSV retain all fields in one row per cluster.

## Validation

Confirm:

1. `clusterCount` matches the Helios cluster inventory.
2. `rowCount` equals `clusterCount`.
3. Every cluster has one structured row.
4. `errors` is empty or contains only known cluster-specific access failures.
5. No API key appears in logs, rows, Markdown, CSV, or errors.
6. Network activity contains only:
   - `GET /v2/mcm/cluster-mgmt/info`
   - `GET /v2/security-config`
