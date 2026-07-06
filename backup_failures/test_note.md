# One Cluster Validation

## Important spelling

Use exactly:

```text
-ResetBaseline
```

Do not type:

```text
-ResetBasline
```

## Run one cluster

```powershell
cd .\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -ResetBaseline
```

## Run one cluster with incident number

```powershell
cd .\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -IncidentNumber "INC1234567" -ResetBaseline
```

## If you see Missing API key helper

That means the helper file path is not being found, or the command was typed incorrectly.

First run the simple command above without HelperPath.

Only use HelperPath when you have the full helper file path.

```powershell
cd .\backup_failures
.\Run-IncidentEvidence-OneCluster.ps1 -ClusterName "YOUR_CLUSTER_NAME" -IncidentNumber "INC1234567" -HelperPath "FULL_HELPER_FILE_PATH" -EncryptedFile "FULL_KEY_FILE_PATH" -ResetBaseline
```
