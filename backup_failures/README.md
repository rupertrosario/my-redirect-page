# Cohesity Backup Failure Window Consolidator

## Script

```text
backup_failures/Get-CohesityBackupFailureWindowConsolidator.ps1
```

## Test one cluster

Run from your local backup_failures folder:

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Get-CohesityBackupFailureWindowConsolidator.ps1 -ClusterName "YOUR_CLUSTER_NAME"
```

Replace `YOUR_CLUSTER_NAME` with the exact Cohesity/Helios cluster display name.

## Run all clusters

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Get-CohesityBackupFailureWindowConsolidator.ps1
```

## What it does every run

- Scans Cohesity backup failures again.
- Keeps failures that do not have a newer success.
- Compares current failures against the previous `state.json`.
- Reports current still failing items.
- Reports older failures still failing.
- Reports new failures since previous check.
- Reports new recoveries since previous check.
- Reports re-failed items.
- Updates `state.json`.
- Writes paste-ready incident work notes.

## Authentication

Uses the existing encrypted API key method only:

```text
X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1
X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc
```

## Output folder

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INC_NUMBER>\
```

## Output files

```text
current_failures.csv
recovered.csv
new_failures.csv
new_recoveries.csv
worknotes.txt
state.json
```

## Notes

- No Excel output.
- No ServiceNow writes.
- Cohesity API calls are GET-only.
- The incident number is asked once per compute window and reused from the registry after that.
