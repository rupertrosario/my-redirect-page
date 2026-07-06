# One Cluster Validation

## Local folder rule

The runner and main script must be in the same local folder.

Create/use this local folder:

```text
X:\PowerShell\Cohesity_API_Scripts\backup_failures
```

Put these two files there:

```text
Get-CohesityBackupFailureIncidentEvidence.ps1
Run-IncidentEvidence-OneCluster.ps1
```

## Run one cluster

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -ResetBaseline
```

## Run one cluster with incident number

```powershell
cd X:\PowerShell\Cohesity_API_Scripts\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -IncidentNumber "INC1234567" -ResetBaseline
```

## ResetBaseline spelling

Correct: `-ResetBaseline`

Wrong: `-ResetBasline`
