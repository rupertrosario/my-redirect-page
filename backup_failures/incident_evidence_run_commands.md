# Cohesity Backup Failure Incident Evidence - Run Commands

Branch: `Cohesity_Automations`

## Current files

- Main script: `backup_failures/Get-CohesityBackupFailureIncidentEvidence.ps1`
- One-cluster validation runner: `backup_failures/Run-IncidentEvidence-OneCluster.ps1`

## Stop current long run

Use `Ctrl + C` in the running PowerShell window.

## One-cluster validation run

Use this first while validating the script.

```powershell
cd .\backup_failures

.\Run-IncidentEvidence-OneCluster.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -ResetBaseline
```

Replace `YOUR_CLUSTER_NAME` with the exact Cohesity/Helios cluster display name.

Example:

```powershell
cd .\backup_failures

.\Run-IncidentEvidence-OneCluster.ps1 `
  -ClusterName "my-cohesity-cluster-01" `
  -ResetBaseline
```

## One-cluster validation with incident number supplied

```powershell
cd .\backup_failures

.\Run-IncidentEvidence-OneCluster.ps1 `
  -ClusterName "YOUR_CLUSTER_NAME" `
  -IncidentNumber "INC1234567" `
  -ResetBaseline
```

## Expected output folder

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INC_NUMBER>\
```

Expected files after successful completion:

```text
current_failures.csv
recovered.csv
new_failures.csv
new_recoveries.csv
worknotes.txt
state.json
```

## Normal full run after validation

Do not use this until the one-cluster run is validated.

```powershell
cd .\backup_failures
.\Get-CohesityBackupFailureIncidentEvidence.ps1 -ResetBaseline
```

## Follow-up run in same compute window

After the first successful baseline run, run without `-ResetBaseline`.

```powershell
cd .\backup_failures
.\Get-CohesityBackupFailureIncidentEvidence.ps1
```

Expected mode:

```text
TargetedFollowUp
```

## Notes

- First run in a compute window creates the baseline.
- Follow-up runs in the same compute window check only baseline failures.
- Cohesity API calls are GET-only.
- No Excel output.
- No ServiceNow update.
- The one-cluster runner is temporary validation support.
