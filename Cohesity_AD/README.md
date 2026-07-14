# Cohesity Active Directory Configuration Collector

## Purpose

`Get-CohesityADConfiguration.ps1` retrieves Active Directory integration details from every Cohesity cluster visible through Cohesity Helios and exports the results to CSV.

The collector is strictly read-only. It uses only HTTP `GET` requests and does not change any cluster, Active Directory, LDAP, user, role, or security configuration.

## Files

| File | Purpose |
|---|---|
| `Get-CohesityADConfiguration.ps1` | PowerShell 5.1/7-compatible GET-only collector |
| `README.md` | Setup, execution, validation, fields, and sample output |

## API Flow

1. `GET /v2/mcm/cluster-mgmt/info`
   - Retrieves all Cohesity clusters available to the Helios API key.
2. `GET /v2/active-directories?includeTenants=true`
   - Executed once per cluster.
   - The target cluster is selected through the `accessClusterId` request header.

No `POST`, `PUT`, `PATCH`, or `DELETE` operation is present in the script.

## Requirements

- Windows PowerShell 5.1 or PowerShell 7+
- Outbound HTTPS access to `https://helios.cohesity.com`
- Cohesity Helios API key with:
  - visibility to the required clusters
  - read access to Active Directory/LDAP configuration
- Permission to write the CSV file to the selected output directory

The API key is never written to the console or CSV output.

## Run the Script

### Option 1: Environment Variable

```powershell
$env:COHESITY_API_KEY = '<Helios API key>'

.\Get-CohesityADConfiguration.ps1 `
    -OutputCsvPath .\Cohesity_AD_Configuration.csv
```

### Option 2: Pass the API Key as a Parameter

```powershell
.\Get-CohesityADConfiguration.ps1 `
    -ApiKey '<Helios API key>' `
    -OutputCsvPath .\Cohesity_AD_Configuration.csv
```

### Option 3: Prompt Without Displaying the API Key

```powershell
$secureKey = Read-Host 'Enter Helios API key' -AsSecureString
$pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureKey)

try {
    $plainKey = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)

    .\Get-CohesityADConfiguration.ps1 `
        -ApiKey $plainKey `
        -OutputCsvPath .\Cohesity_AD_Configuration.csv
}
finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    Remove-Variable plainKey -ErrorAction SilentlyContinue
}
```

If `-OutputCsvPath` is omitted, the script creates a timestamped file in the current directory:

```text
Cohesity_AD_Configuration_YYYYMMDD_HHMMSS.csv
```

## Console Output

The console displays a concise table containing:

- cluster name
- whether AD is configured
- domain name
- preferred domain controllers
- ID mapping type
- collection status

## Sample Output

| ClusterName | ADConfigured | DomainName | PreferredDomainControllers | IdMappingType | CollectionStatus |
|---|---:|---|---|---|---|
| CHS-PROD-01 | Yes | `corp.example.com` | `dc01.corp.example.com [Reachable]; dc02.corp.example.com [Reachable]` | `Rfc2307` | `CONFIGURED` |
| CHS-DR-01 | Yes | `corp.example.com` | `dc03.corp.example.com [Unreachable]` | `Rid` | `REVIEW` |
| CHS-TEST-01 | No |  |  |  | `NOT_CONFIGURED` |
| CHS-LAB-01 | Unknown |  |  |  | `COLLECTION_ERROR` |

## Collection Status Values

| Status | Meaning |
|---|---|
| `CONFIGURED` | AD configuration was returned and no controller issue or API-reported error was detected |
| `REVIEW` | AD is configured, but at least one returned domain controller status is not `Reachable` |
| `ERROR` | Cohesity returned an AD configuration error code or error message |
| `NOT_CONFIGURED` | The cluster returned no Active Directory configuration |
| `COLLECTION_ERROR` | The cluster-specific GET request failed; see `CollectionError` |

## CSV Fields

| Field | Description |
|---|---|
| `ClusterName` | Cohesity cluster name |
| `ClusterId` | Helios cluster identifier used in `accessClusterId` |
| `ADConfigured` | `Yes`, `No`, or `Unknown` |
| `DomainName` | Joined Active Directory domain |
| `OrganizationalUnit` | Configured organizational unit |
| `WorkGroupName` | Configured workgroup, when present |
| `ConnectionId` | Cohesity AD connection identifier |
| `MachineAccounts` | Machine-account names and DNS hostnames |
| `PreferredDomainControllers` | Preferred controller names and reachability status |
| `DomainControllers` | Discovered controllers grouped by domain |
| `DomainControllersDenyList` | Configured domain-controller deny list |
| `IdMappingType` | User ID mapping method, such as `Rfc2307` or `Rid` |
| `TrustedDomains` | Discovered/configured trusted domains |
| `WhitelistedDomains` | Trusted-domain allow list |
| `BlacklistedDomains` | Trusted-domain deny list |
| `OnlyUseWhitelistedDomains` | Whether only allow-listed trusted domains are used |
| `TrustDiscoveryStatus` | Trusted-domain discovery status |
| `TrustEnabled` | Trusted-domain processing enabled state |
| `ErrorCode` | Cohesity AD configuration error code |
| `ErrorMessage` | Cohesity AD configuration error message |
| `CollectionStatus` | Normalized result status |
| `CollectionError` | HTTP or processing error for the cluster |

## Quick Validation

```powershell
$data = Import-Csv .\Cohesity_AD_Configuration.csv

# Count each result status
$data | Group-Object CollectionStatus | Select-Object Name, Count

# Find clusters without AD configuration
$data | Where-Object CollectionStatus -eq 'NOT_CONFIGURED'

# Find unreachable controllers or API errors
$data | Where-Object CollectionStatus -in @('REVIEW', 'ERROR', 'COLLECTION_ERROR')
```

## Operational Notes

- One output row is generated for each AD connection returned by a cluster.
- A cluster with no AD configuration still receives one `NOT_CONFIGURED` row.
- A failed cluster request does not stop collection from the remaining clusters.
- `includeTenants=true` is retained so tenant-scoped directory configuration is included when the API key can view it.
- Run the script first against a non-production cluster scope or a limited Helios API key to confirm field availability in the installed Cohesity version.
