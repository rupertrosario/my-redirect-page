# Backup Failure Incident Evidence Commands

Use this file for copy/paste commands.

## One cluster validation

```powershell
cd .\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -ResetBaseline
```

## One cluster validation with incident number

```powershell
cd .\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -IncidentNumber "INC1234567" -ResetBaseline
```

## ResetBaseline spelling

Correct:

```text
-ResetBaseline
```

Wrong:

```text
-ResetBasline
```

## Expected output folder

```text
X:\PowerShell\Data\Cohesity\BackupFailureWindow\<INC_NUMBER>\
```

Expected files after completion:

```text
current_failures.csv
recovered.csv
new_failures.csv
new_recoveries.csv
worknotes.txt
state.json
```

## Normal full run after one-cluster validation

```powershell
cd .\backup_failures
.\Get-CohesityBackupFailureIncidentEvidence.ps1 -ResetBaseline
```

## Follow-up run in same compute window

```powershell
cd .\backup_failures
.\Get-CohesityBackupFailureIncidentEvidence.ps1
```
