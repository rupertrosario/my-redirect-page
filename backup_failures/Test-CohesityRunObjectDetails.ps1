<#
.SYNOPSIS
Probe one Cohesity protection group run response and flatten returned object details.

.DESCRIPTION
GET-only diagnostic script. Use this when the backup failure collector is not showing the object that is visible in Cohesity UI.
It does not update incidents or state.json.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [Parameter(Mandatory=$true)] [string]$ClusterName,
    [string]$ProtectionGroupName = "",
    [string]$ProtectionGroupId = "",
    [string]$Environment = "",
    [int]$NumRuns = 10,
    [int]$RequestTimeoutSec = 90,
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow\Debug",
    [string]$HelperPath = ("X:\PowerShell\Cohesity_API_Scripts\Common\" + "Api" + "KeyAesHelper.ps1"),
    [string]$EncryptedFile = ("X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_" + "api" + "key.enc")
)

$ErrorActionPreference = "Stop"

function Clean($Value) {
    if ($null -eq $Value) { return "" }
    if ($Value -is [array]) { $Value = $Value -join " | " }
    return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim()
}

function As-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function Get-Prop($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { return $p.Value }
    return $Default
}

function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec $RequestTimeoutSec
    } else {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $RequestTimeoutSec
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    $r.Content | ConvertFrom-Json
}

function Get-CohesityApiKey {
    if (!(Test-Path $HelperPath)) { throw "Missing API key helper: $HelperPath" }
    if (!(Test-Path $EncryptedFile)) { throw "Missing encrypted key file: $EncryptedFile" }
    . $HelperPath
    $key = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key is blank from AES helper." }
    $key.Trim()
}

function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Convert-UsecsToEtText($Usecs) {
    if ($null -eq $Usecs) { return "" }
    try {
        $u = [int64]$Usecs
        if ($u -le 0) { return "" }
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
    } catch {
        return ""
    }
}

function Get-ClusterDisplayName($Cluster) {
    $n = Clean (Get-Prop $Cluster "name" "")
    if (!$n) { $n = Clean (Get-Prop $Cluster "clusterName" "") }
    if (!$n) { $n = Clean (Get-Prop $Cluster "displayName" "") }
    if (!$n) { $n = "Unknown-$(Clean (Get-Prop $Cluster 'clusterId' ''))" }
    $n
}

function Get-EnvironmentFilters([string]$Env) {
    $all = @("kOracle","kSQL","kPhysical","kGenericNas","kHyperV","kAcropolis","kRemoteAdapter","kIsilon")
    $map = @{
        "Oracle" = "kOracle"
        "SQL" = "kSQL"
        "Physical" = "kPhysical"
        "GenericNas" = "kGenericNas"
        "NAS" = "kGenericNas"
        "HyperV" = "kHyperV"
        "Acropolis" = "kAcropolis"
        "AHV" = "kAcropolis"
        "RemoteAdapter" = "kRemoteAdapter"
        "Isilon" = "kIsilon"
    }
    $e = Clean $Env
    if (!$e) { return $all }
    if ($e -like "k*") { return @($e) }
    if ($map.ContainsKey($e)) { return @($map[$e]) }
    return @($e)
}

if (!$ProtectionGroupName -and !$ProtectionGroupId) {
    throw "Provide either -ProtectionGroupName or -ProtectionGroupId."
}

if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }

$apiKey = Get-CohesityApiKey
$commonHeaders = @{ accept = "application/json"; apiKey = $apiKey }

$clusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
$clusters = @($clusterJson.cohesityClusters | Sort-Object @{Expression={ Get-ClusterDisplayName $_ }})
$cluster = @($clusters | Where-Object {
    (Get-ClusterDisplayName $_) -eq $ClusterName -or
    (Clean (Get-Prop $_ "name" "")) -eq $ClusterName -or
    (Clean (Get-Prop $_ "clusterName" "")) -eq $ClusterName -or
    (Clean (Get-Prop $_ "displayName" "")) -eq $ClusterName
} | Select-Object -First 1)[0]

if (!$cluster) { throw "Cluster not found: $ClusterName" }
$clusterId = Clean (Get-Prop $cluster "clusterId" "")
$clusterDisplay = Get-ClusterDisplayName $cluster
$headers = @{ accept = "application/json"; apiKey = $apiKey; accessClusterId = $clusterId }

$pg = $null
$pgMatches = @()

if ($ProtectionGroupId) {
    $pg = [pscustomobject]@{ id = $ProtectionGroupId; name = $ProtectionGroupName }
} else {
    foreach ($filter in (Get-EnvironmentFilters $Environment)) {
        try {
            $pgUri = "$BaseUrl/v2/data-protect/protection-groups?environments=$filter&isDeleted=false&isPaused=false&isActive=true"
            $pgJson = Invoke-HeliosGetJson -Uri $pgUri -Headers $headers
            foreach ($candidate in @($pgJson.protectionGroups)) {
                if ((Clean (Get-Prop $candidate "name" "")) -eq $ProtectionGroupName) {
                    $pgMatches += $candidate
                }
            }
        } catch {
            Write-Warning "PG lookup failed for filter $filter : $($_.Exception.Message)"
        }
    }

    $pgMatches = @($pgMatches | Sort-Object id -Unique)
    if ($pgMatches.Count -eq 0) { throw "Protection group not found on $clusterDisplay : $ProtectionGroupName" }
    if ($pgMatches.Count -gt 1) {
        Write-Host "Multiple protection groups matched. Re-run with -ProtectionGroupId using one of these IDs:"
        $pgMatches | Select-Object id,name,environment | Format-Table -AutoSize
        throw "Multiple PG matches found."
    }
    $pg = $pgMatches[0]
}

$pgId = Clean (Get-Prop $pg "id" $ProtectionGroupId)
$pgName = Clean (Get-Prop $pg "name" $ProtectionGroupName)

Write-Host "Cluster          : $clusterDisplay"
Write-Host "ClusterId        : $clusterId"
Write-Host "ProtectionGroup  : $pgName"
Write-Host "ProtectionGroupId: $pgId"
Write-Host "NumRuns          : $NumRuns"

$runsUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgId))/runs?numRuns=$NumRuns&excludeNonRestorableRuns=false&includeObjectDetails=true"
$runsJson = Invoke-HeliosGetJson -Uri $runsUri -Headers $headers
$runs = @($runsJson.runs)

$rows = @()
$runIndex = 0
foreach ($run in $runs) {
    $runIndex++
    $info = @(As-Array (Get-Prop $run "localBackupInfo" @()) | Select-Object -First 1)[0]
    $runType = Clean (Get-Prop $info "runType" "")
    $runStatus = Clean (Get-Prop $info "status" "")
    $startUsecs = Get-Prop $info "startTimeUsecs" ""
    $endUsecs = Get-Prop $info "endTimeUsecs" ""
    $runMessages = Clean (Get-Prop $info "messages" "")
    $runObjects = @(As-Array (Get-Prop $run "objects" @()))

    if ($runObjects.Count -eq 0) {
        $rows += [pscustomobject]@{
            RunIndex = $runIndex
            RunId = Clean (Get-Prop $run "id" "")
            RunType = $runType
            RunStatus = $runStatus
            RunStartET = Convert-UsecsToEtText $startUsecs
            RunEndET = Convert-UsecsToEtText $endUsecs
            RunMessages = $runMessages
            ObjectName = ""
            ObjectType = ""
            ObjectEnvironment = ""
            ObjectId = ""
            SourceId = ""
            ParentId = ""
            LocalSnapshotInfoPresent = "False"
            LocalSnapshotStatus = ""
            SnapshotStatus = ""
            SnapshotError = ""
            FailedAttemptsCount = 0
            FailedMessage = "NO OBJECTS RETURNED IN run.objects"
        }
        continue
    }

    foreach ($ob in $runObjects) {
        $obj = Get-Prop $ob "object" $null
        $lsi = Get-Prop $ob "localSnapshotInfo" $null
        $snapshotInfo = Get-Prop $lsi "snapshotInfo" $null
        $attempts = @(As-Array (Get-Prop $lsi "failedAttempts" @()))
        $attemptMsgs = @()
        foreach ($a in $attempts) {
            $m = Clean (Get-Prop $a "message" "")
            if ($m) { $attemptMsgs += $m }
        }
        $rows += [pscustomobject]@{
            RunIndex = $runIndex
            RunId = Clean (Get-Prop $run "id" "")
            RunType = $runType
            RunStatus = $runStatus
            RunStartET = Convert-UsecsToEtText $startUsecs
            RunEndET = Convert-UsecsToEtText $endUsecs
            RunMessages = $runMessages
            ObjectName = Clean (Get-Prop $obj "name" "")
            ObjectType = Clean (Get-Prop $obj "objectType" "")
            ObjectEnvironment = Clean (Get-Prop $obj "environment" "")
            ObjectId = Clean (Get-Prop $obj "id" "")
            SourceId = Clean (Get-Prop $obj "sourceId" "")
            ParentId = Clean (Get-Prop $obj "parentId" "")
            LocalSnapshotInfoPresent = if ($lsi) { "True" } else { "False" }
            LocalSnapshotStatus = Clean (Get-Prop $lsi "status" "")
            SnapshotStatus = Clean (Get-Prop $snapshotInfo "status" "")
            SnapshotError = Clean (Get-Prop $snapshotInfo "error" "")
            FailedAttemptsCount = $attempts.Count
            FailedMessage = Clean ($attemptMsgs -join " | ")
        }
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$safeCluster = ($clusterDisplay -replace '[^a-zA-Z0-9_.-]', '_')
$safePg = (($pgName, $pgId | Where-Object { $_ } | Select-Object -First 1) -replace '[^a-zA-Z0-9_.-]', '_')
$csvPath = Join-Path $OutputRoot "Cohesity_RunObjectDetails_${safeCluster}_${safePg}_${timestamp}.csv"
$jsonPath = Join-Path $OutputRoot "Cohesity_RunObjectDetails_${safeCluster}_${safePg}_${timestamp}.json"

$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
$runsJson | ConvertTo-Json -Depth 100 | Set-Content -Path $jsonPath -Encoding UTF8

Write-Host ""
Write-Host "Summary:"
Write-Host "Runs returned       : $($runs.Count)"
Write-Host "Flattened rows      : $($rows.Count)"
Write-Host "Rows with objects   : $(@($rows | Where-Object { $_.ObjectName -or $_.ObjectId }).Count)"
Write-Host "Rows with failures  : $(@($rows | Where-Object { [int]$_.FailedAttemptsCount -gt 0 }).Count)"
Write-Host ""
Write-Host "CSV : $csvPath"
Write-Host "JSON: $jsonPath"
Write-Host ""
$rows | Sort-Object RunIndex,ObjectName | Format-Table RunIndex,RunType,RunStatus,RunEndET,ObjectName,ObjectType,ObjectEnvironment,FailedAttemptsCount,LocalSnapshotStatus,SnapshotStatus,FailedMessage -AutoSize -Wrap
