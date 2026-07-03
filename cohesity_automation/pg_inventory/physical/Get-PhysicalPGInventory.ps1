#requires -Version 5.1
<#
.SYNOPSIS
    Cohesity active Physical PG inventory.

.DESCRIPTION
    GET-only Helios report for active kPhysical protection groups.
    Default output is PG-level summary for GridView and CSV.
    Use -Detail to also export object/path-level CSV.
#>

[CmdletBinding()]
param(
    [string]$HeliosUrl = 'https://helios.cohesity.com',
    [string]$ApiKeyPath = (Join-Path 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete' ('api' + 'key.txt')),
    [string]$OutputRoot = 'X:\PowerShell\cohesity_automation\pg_inventory\physical',
    [switch]$Detail,
    [switch]$NoGridView
)

$ErrorActionPreference = 'Stop'

function Get-Val {
    param([object]$Obj, [string[]]$Names)
    if ($null -eq $Obj) { return $null }
    foreach ($n in $Names) {
        $p = $Obj.PSObject.Properties | Where-Object { $_.Name -ieq $n } | Select-Object -First 1
        if ($p -and $null -ne $p.Value -and -not [string]::IsNullOrWhiteSpace([string]$p.Value)) { return $p.Value }
    }
    return $null
}

function Get-PathVal {
    param([object]$Obj, [string[]]$Path)
    $cur = $Obj
    foreach ($p in $Path) {
        $cur = Get-Val $cur @($p)
        if ($null -eq $cur) { return $null }
    }
    return $cur
}

function To-Array {
    param([object]$Val)
    if ($null -eq $Val) { return @() }
    if ($Val -is [string]) { return @($Val) }
    if ($Val -is [System.Collections.IEnumerable]) { return @($Val) }
    return @($Val)
}

function Join-Items {
    param([object]$Val)
    $items = foreach ($i in (To-Array $Val)) {
        if ($null -eq $i) { continue }
        if ($i -is [string]) { $i; continue }
        $simple = Get-Val $i @('includedPath','path','filePath','name','displayName','value','id')
        if ($simple) { [string]$simple } else { $i | ConvertTo-Json -Compress -Depth 8 }
    }
    return (($items | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique) -join '; ')
}

function Convert-UsecsToET {
    param([object]$Usecs)
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return '' }
    try {
        $dto = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($dto, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch { return '' }
}

function Invoke-CGet {
    param([string]$Uri, [hashtable]$Headers)
    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
}

function Get-RespArray {
    param([object]$Resp)
    foreach ($p in @('protectionGroups','data','items')) {
        $v = Get-Val $Resp @($p)
        if ($null -ne $v) { return @(To-Array $v) }
    }
    return @(To-Array $Resp)
}

function Get-ProtectionGroups {
    param([string]$HeliosUrl, [hashtable]$Headers)
    $all = New-Object System.Collections.Generic.List[object]
    $cookie = $null
    do {
        $uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeTenants=true&includeLastRunInfo=true&maxResultCount=1000"
        if ($cookie) { $uri += "&paginationCookie=$([uri]::EscapeDataString([string]$cookie))" }
        $resp = Invoke-CGet -Uri $uri -Headers $Headers
        foreach ($pg in (Get-RespArray $resp)) { [void]$all.Add($pg) }
        $cookie = Get-Val $resp @('paginationCookie','nextPageCookie','nextPaginationCookie')
    } while ($cookie)
    return @($all)
}

function Get-ClusterName {
    param([object]$PG)
    $v = Get-Val $PG @('clusterName','clusterDisplayName','sourceClusterName')
    if ($v) { return [string]$v }
    foreach ($p in @(@('cluster','name'),@('clusterInfo','name'),@('clusterIdentifier','name'),@('clusterIdentifier','clusterName'))) {
        $v = Get-PathVal $PG $p
        if ($v) { return [string]$v }
    }
    return 'Helios'
}

function Get-ClusterId {
    param([object]$PG)
    $v = Get-Val $PG @('clusterId','accessClusterId','sourceClusterId')
    if ($v) { return [string]$v }
    foreach ($p in @(@('cluster','id'),@('clusterInfo','id'),@('clusterIdentifier','id'),@('clusterIdentifier','clusterId'))) {
        $v = Get-PathVal $PG $p
        if ($v) { return [string]$v }
    }
    return ''
}

function Select-PGClusterScope {
    param([object[]]$PGs)
    $clusters = @(
        foreach ($pg in $PGs) {
            $name = Get-ClusterName $pg
            $id = Get-ClusterId $pg
            [pscustomobject]@{ Name = $name; Id = $id; Key = "$name|$id" }
        }
    ) | Sort-Object Name, Id -Unique

    if ($clusters.Count -le 1) { return @($PGs) }

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'
    for ($i = 0; $i -lt $clusters.Count; $i++) { Write-Host ('[{0}] {1}' -f ($i + 1), $clusters[$i].Name) }
    Write-Host 'Examples: 1,3,5 or 2-4' -ForegroundColor DarkGray
    $choice = (Read-Host 'Enter selection').Trim()
    if ($choice -eq '0') { return @($PGs) }

    $idx = New-Object System.Collections.Generic.List[int]
    foreach ($part in ($choice -split ',')) {
        $part = $part.Trim()
        if ($part -match '^(\d+)\s*-\s*(\d+)$') {
            $a = [int]$Matches[1]; $b = [int]$Matches[2]
            if ($a -lt 1 -or $b -gt $clusters.Count -or $a -gt $b) { throw "Invalid range: $part" }
            foreach ($n in $a..$b) { [void]$idx.Add($n) }
        } elseif ($part -match '^\d+$') {
            $n = [int]$part
            if ($n -lt 1 -or $n -gt $clusters.Count) { throw "Invalid cluster selection: $part" }
            [void]$idx.Add($n)
        } else { throw "Invalid cluster selection: $part" }
    }

    $keys = @($idx | Select-Object -Unique | ForEach-Object { $clusters[$_ - 1].Key })
    return @($PGs | Where-Object { $keys -contains "$(Get-ClusterName $_)|$(Get-ClusterId $_)" })
}

function Get-LastRun {
    param([object]$PG)
    $last = Get-Val $PG @('lastRun','lastRunInfo','latestRun','latestRunInfo')
    $local = Get-Val $last @('localBackupInfo','localSnapshotInfo')
    $status = Get-Val $local @('status','runStatus')
    if (-not $status) { $status = Get-Val $last @('status','runStatus','lastRunAnyStatus') }
    $end = Get-Val $local @('endTimeUsecs','runEndTimeUsecs','endTimeInUsecs')
    if (-not $end) { $end = Get-Val $last @('endTimeUsecs','runEndTimeUsecs','endTimeInUsecs') }
    [pscustomobject]@{ Status = [string]$status; EndET = Convert-UsecsToET $end }
}

function Get-PhysicalParts {
    param([object]$PG)
    $physical = Get-Val $PG @('physicalParams')
    $type = [string](Get-Val $physical @('protectionType'))
    $fileParams = Get-Val $physical @('fileProtectionTypeParams')
    $volParams = Get-Val $physical @('volumeProtectionTypeParams')
    $objects = if ($type -eq 'kVolume') { To-Array (Get-Val $volParams @('objects')) } else { To-Array (Get-Val $fileParams @('objects')) }
    [pscustomobject]@{ Physical = $physical; Type = $type; File = $fileParams; Volume = $volParams; Objects = @($objects) }
}

function New-SummaryRow {
    param([object]$PG)
    $parts = Get-PhysicalParts $PG
    $last = Get-LastRun $PG
    [pscustomobject]@{
        Cluster            = Get-ClusterName $PG
        ClusterId          = Get-ClusterId $PG
        PGName             = [string](Get-Val $PG @('name','protectionGroupName'))
        PolicyName         = [string](Get-Val $PG @('policyName'))
        ProtectionType     = $parts.Type
        PGObjectCount      = $parts.Objects.Count
        GlobalExcludePaths = Join-Items (Get-Val $parts.File @('globalExcludePaths'))
        GlobalExcludeFS    = Join-Items (Get-Val $parts.File @('globalExcludeFS'))
        IsActive           = [string](Get-Val $PG @('isActive'))
        IsPaused           = [string](Get-Val $PG @('isPaused'))
        LastRunStatus      = $last.Status
        LastRunEndET       = $last.EndET
    }
}

function New-DetailRows {
    param([object]$PG)
    $parts = Get-PhysicalParts $PG
    $last = Get-LastRun $PG
    foreach ($obj in $parts.Objects) {
        if ($parts.Type -eq 'kVolume') {
            [pscustomobject]@{
                Cluster = Get-ClusterName $PG; PGName = [string](Get-Val $PG @('name','protectionGroupName'))
                ProtectionType = $parts.Type; ObjectName = [string](Get-Val $obj @('name','sourceName','hostName'))
                IncludedPath = Join-Items (Get-Val $obj @('volumeGuids')); ObjectExcludedPaths = ''; SkipNestedVolumes = ''
                GlobalExcludePaths = ''; LastRunStatus = $last.Status; LastRunEndET = $last.EndET
            }
        } else {
            foreach ($fp in (To-Array (Get-Val $obj @('filePaths')))) {
                [pscustomobject]@{
                    Cluster = Get-ClusterName $PG; PGName = [string](Get-Val $PG @('name','protectionGroupName'))
                    ProtectionType = $parts.Type; ObjectName = [string](Get-Val $obj @('name','sourceName','hostName'))
                    IncludedPath = [string](Get-Val $fp @('includedPath'))
                    ObjectExcludedPaths = Join-Items (Get-Val $fp @('excludedPaths'))
                    SkipNestedVolumes = [string](Get-Val $fp @('skipNestedVolumes'))
                    GlobalExcludePaths = Join-Items (Get-Val $parts.File @('globalExcludePaths'))
                    LastRunStatus = $last.Status; LastRunEndET = $last.EndET
                }
            }
        }
    }
}

if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
$key = (Get-Content $ApiKeyPath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($key)) { throw "API key file is empty: $ApiKeyPath" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$headers = @{ accept = 'application/json' }
$headers[('api' + 'Key')] = $key

$pgs = @(Get-ProtectionGroups -HeliosUrl $HeliosUrl -Headers $headers)
if ($pgs.Count -eq 0) { Write-Warning 'No active kPhysical PGs returned.'; return }
$pgs = @(Select-PGClusterScope -PGs $pgs)

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$summary = @($pgs | ForEach-Object { New-SummaryRow $_ } | Sort-Object Cluster, PGName)
$summaryCsv = Join-Path $OutputRoot "physical_pg_summary_$stamp.csv"
$summary | Export-Csv -Path $summaryCsv -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host "PGs        : $($summary.Count)" -ForegroundColor Green
Write-Host "Summary CSV: $summaryCsv" -ForegroundColor Green
$summary | Format-Table Cluster, PGName, ProtectionType, PGObjectCount, GlobalExcludePaths, IsPaused, LastRunStatus, LastRunEndET -AutoSize

if ($Detail) {
    $detailRows = @($pgs | ForEach-Object { New-DetailRows $_ } | Sort-Object Cluster, PGName, ObjectName, IncludedPath)
    $detailCsv = Join-Path $OutputRoot "physical_pg_detail_$stamp.csv"
    $detailRows | Export-Csv -Path $detailCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Detail CSV : $detailCsv" -ForegroundColor Green
}

if (-not $NoGridView) {
    try { $summary | Out-GridView -Title 'Cohesity Physical PG Summary' }
    catch { Write-Warning 'Out-GridView unavailable. CSV was still created.' }
}
