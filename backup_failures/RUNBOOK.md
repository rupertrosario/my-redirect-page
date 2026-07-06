# Backup Failures - Current Runbook

## Purpose

This file is the single current run/test instruction file for the backup_failures workstream.

It will be overwritten whenever run/test instructions change, so there is only one current instruction source.

## Current Script Under Test

```text
backup_failures/Test-CohesityHeliosConnection.ps1
```

## Local Copy Target

Copy the script to:

```text
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

## Run Command

```powershell
X:\PowerShell\Cohesity_API_Scripts\Test-CohesityHeliosConnection.ps1
```

## Expected Behavior

The script must:

1. Use `ApiKeyAesHelper.ps1`.
2. Read the encrypted API key from `Common\Secure\cohesity_apikey.enc`.
3. Call only this GET endpoint:

```text
https://helios.cohesity.com/v2/mcm/cluster-mgmt/info
```

4. Print cluster count and cluster names.
5. Save nothing.
6. Create no registry or state files.
7. Make no Cohesity POST, PUT, PATCH, or DELETE calls.

## Output To Share Back

Share only:

```text
Cluster count line
Any red error text
Line/char details, if PowerShell reports them
```

Do not share API key contents.
