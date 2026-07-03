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
    [string]$ApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt',
    [string]$OutputRoot = 'X:\PowerShell\cohesity_automation\pg_inventory\physical',
    [switch]$Detail,
    [switch]$NoGridView
)

$ErrorActionPreference = 'Stop'

function Get-PropValue {
    param($Object, [string[]]$Names)
    if ($null -eq $Object) { return $null }

    foreach ($name in $Names) {
        foreach ($prop in $Object.PSObject.Properties) {
            if ($prop.Name -ieq $name) {
                if ($null -ne $prop.Value -and -not [string]::IsNullOrWhiteSpace([string]$prop.Value)) {
                    return $prop.Value
                }
            }
        }
    }
    return $null
}

function Convert-ToArray {
    param($Value)
    if ($null -eq $Value) { return @() }
    if ($Value -is [System.Array]) { return $Value }
    return @($Value)
}

function Join-Text {
    param($Value)

    $out = New-Object System.Collections.Generic.List[string]
    foreach ($item in (Convert-ToArray $Value)) {
        if ($null -eq $item) { continue }
        if ($item -is [string]) {
            if (-not [string]::IsNullOrWhiteSpace($item)) { [void]$out.Add($item) }
            continue
        }

        $simple = Get-PropValue $item @('includedPath', 'path', 'filePath', 'name', 'displayName', 'value', 'id')
        if ($null -ne $simple -and -not [string]::IsNullOrWhiteSpace([string]$simple)) {
            [void]$out.Add([string]$simple)
        }
    }

    return (($out | Select-Object -Unique) -join '; ')
}

function Convert-UsecsToET {
    param($Usecs)
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return '' }

    try {
        $dto = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($dto, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch {
        return ''
    }
}

function Invoke-CohesityGet {
    param([string]$Uri, [hashtable]$Headers)
    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
}

function Get-ResponseItems {
    param($Response)

    $items = Get-PropValue $Response @('protectionGroups', 'items', 'data')
    if ($null -ne $items) { return @(Convert-ToArray $items) }
    return @(Convert-ToArray $Response)
}

function Get-ProtectionGroups {
    param([string]$HeliosUrl, [hashtable]$Headers)

    $allGroups = New-Object System.Collections.Generic.List[object]
    $cookie = $null

    do {
        $uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeTenants=true&includeLastRunInfo=true&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace([string]$cookie)) {
            $uri = "$uri&paginationCookie=$([uri]::EscapeDataString([string]$cookie))"
        }

        $response = Invoke-CohesityGet -Uri $uri -Headers $Headers
        foreach ($pg in (Get-ResponseItems $response)) { [void]$allGroups.Add($pg) }
        $cookie = Get-PropValue $response @('paginationCookie', 'nextPageCookie', 'nextPaginationCookie')
    } while (-not [string]::IsNullOrWhiteSpace([string]$cookie))

    return @($allGroups)
}

function Get-ClusterName {
    param($PG)
    $name = Get-PropValue $PG @('clusterName', 'clusterDisplayName', 'sourceClusterName')
    if ($name) { return [string]$name }

    $clusterInfo = Get-PropValue $PG @('clusterInfo', 'cluster', 'clusterIdentifier')
    $name = Get-PropValue $clusterInfo @('name', 'clusterName', 'displayName')
    if ($name) { return [string]$name }

    return 'Helios'
}

function Get-PhysicalParams {
    param($PG)

    $physical = Get-PropValue $PG @('physicalParams')
    $type = [string](Get-PropValue $physical @('protectionType'))

    $fileParams = Get-PropValue $physical @('fileProtectionTypeParams')
    $volumeParams = Get-PropValue $physical @('volumeProtectionTypeParams')

    if ($type -eq 'kVolume') {
        $objects = @(Convert-ToArray (Get-PropValue $volumeParams @('objects')))
    } else {
        $objects = @(Convert-ToArray (Get-PropValue $fileParams @('objects')))
    }

    [pscustomobject]@{
        Physical = $physical
        Type = $type
        FileParams = $fileParams
        VolumeParams = $volumeParams
        Objects = $objects
    }
}

function Get-LastRunSummary {
    param($PG)

    $last = Get-PropValue $PG @('lastRun', 'lastRunInfo', 'latestRun', 'latestRunInfo')
    $local = Get-PropValue $last @('localBackupInfo', 'localSnapshotInfo')

    $status = Get-PropValue $local @('status', 'runStatus')
    if (-not $status) { $status = Get-PropValue $last @('status', 'runStatus', 'lastRunAnyStatus') }

    $endUsecs = Get-PropValue $local @('endTimeUsecs', 'runEndTimeUsecs', 'endTimeInUsecs')
    if (-not $endUsecs) { $endUsecs = Get-PropValue $last @('endTimeUsecs', 'runEndTimeUsecs', 'endTimeInUsecs') }

    [pscustomobject]@{
        Status = [string]$status
        EndET = Convert-UsecsToET $endUsecs
    }
}

function Select-ClusterScope {
    param([object[]]$PGs)

    $clusterNames = @($PGs | ForEach-Object { Get-ClusterName $_ } | Sort-Object -Unique)
    if ($clusterNames.Count -le 1) { return @($PGs) }

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'
    for ($i = 0; $i -lt $clusterNames.Count; $i++) {
        Write-Host ('[{0}] {1}' -f ($i + 1), $clusterNames[$i])
    }
    Write-Host 'Examples: 1,3,5 or 2-4' -ForegroundColor DarkGray

    $choice = (Read-Host 'Enter selection').Trim()
    if ($choice -eq '0') { return @($PGs) }

    $selectedNames = New-Object System.Collections.Generic.List[string]
    foreach ($part in ($choice -split ',')) {
        $part = $part.Trim()
        if ($part -match '^(\d+)\s*-\s*(\d+)$') {
            $start = [int]$Matches[1]
            $end = [int]$Matches[2]
            if ($start -lt 1 -or $end -gt $clusterNames.Count -or $start -gt $end) { throw "Invalid cluster range: $part" }
            foreach ($n in $start..$end) { [void]$selectedNames.Add([string]$clusterNames[$n - 1]) }
        } elseif ($part -match '^\d+$') {
            $n = [int]$part
            if ($n -lt 1 -or $n -gt $clusterNames.Count) { throw "Invalid cluster selection: $part" }
            [void]$selectedNames.Add([string]$clusterNames[$n - 1])
        } else {
            throw "Invalid cluster selection: $part"
        }
    }

    $selected = @($selectedNames | Select-Object -Unique)
    return @($PGs | Where-Object { $selected -contains (Get-ClusterName $_) })
}

function New-SummaryRow {
    param($PG)

    $p = Get-PhysicalParams $PG
    $last = Get-LastRunSummary $PG

    [pscustomobject]@{
        Cluster = Get-ClusterName $PG
        PGName = [string](Get-PropValue $PG @('name', 'protectionGroupName'))
        PolicyName = [string](Get-PropValue $PG @('policyName'))
        ProtectionType = $p.Type
        PGObjectCount = $p.Objects.Count
        GlobalExcludePaths = Join-Text (Get-PropValue $p.FileParams @('globalExcludePaths'))
        GlobalExcludeFS = Join-Text (Get-PropValue $p.FileParams @('globalExcludeFS'))
        IsActive = [string](Get-PropValue $PG @('isActive'))
        IsPaused = [string](Get-PropValue $PG @('isPaused'))
        LastRunStatus = $last.Status
        LastRunEndET = $last.EndET
    }
}

function New-DetailRows {
    param($PG)

    $p = Get-PhysicalParams $PG
    $last = Get-LastRunSummary $PG

    foreach ($obj in $p.Objects) {
        $objectName = [string](Get-PropValue $obj @('name', 'sourceName', 'hostName'))

        if ($p.Type -eq 'kVolume') {
            [pscustomobject]@{
                Cluster = Get-ClusterName $PG
                PGName = [string](Get-PropValue $PG @('name', 'protectionGroupName'))
                ProtectionType = $p.Type
                ObjectName = $objectName
                IncludedPath = Join-Text (Get-PropValue $obj @('volumeGuids'))
                ObjectExcludedPaths = ''
                SkipNestedVolumes = ''
                GlobalExcludePaths = ''
                LastRunStatus = $last.Status
                LastRunEndET = $last.EndET
            }
        } else {
            $filePaths = @(Convert-ToArray (Get-PropValue $obj @('filePaths')))
            foreach ($fp in $filePaths) {
                [pscustomobject]@{
                    Cluster = Get-ClusterName $PG
                    PGName = [string](Get-PropValue $PG @('name', 'protectionGroupName'))
                    ProtectionType = $p.Type
                    ObjectName = $objectName
                    IncludedPath = [string](Get-PropValue $fp @('includedPath'))
                    ObjectExcludedPaths = Join-Text (Get-PropValue $fp @('excludedPaths'))
                    SkipNestedVolumes = [string](Get-PropValue $fp @('skipNestedVolumes'))
                    GlobalExcludePaths = Join-Text (Get-PropValue $p.FileParams @('globalExcludePaths'))
                    LastRunStatus = $last.Status
                    LastRunEndET = $last.EndET
                }
            }
        }
    }
}

if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
$apiKey = (Get-Content $ApiKeyPath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "API key file is empty: $ApiKeyPath" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

$headers = @{
    accept = 'application/json'
    apiKey = $apiKey
}

$pgs = @(Get-ProtectionGroups -HeliosUrl $HeliosUrl -Headers $headers)
if ($pgs.Count -eq 0) {
    Write-Warning 'No active kPhysical Protection Groups returned.'
    return
}

$pgs = @(Select-ClusterScope -PGs $pgs)
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'

$summaryRows = @($pgs | ForEach-Object { New-SummaryRow $_ } | Sort-Object Cluster, PGName)
$summaryCsv = Join-Path $OutputRoot "physical_pg_summary_$timestamp.csv"
$summaryRows | Export-Csv -Path $summaryCsv -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host "PGs        : $($summaryRows.Count)" -ForegroundColor Green
Write-Host "Summary CSV: $summaryCsv" -ForegroundColor Green

$summaryRows | Format-Table Cluster, PGName, ProtectionType, PGObjectCount, GlobalExcludePaths, IsPaused, LastRunStatus, LastRunEndET -AutoSize

if ($Detail) {
    $detailRows = @($pgs | ForEach-Object { New-DetailRows $_ } | Sort-Object Cluster, PGName, ObjectName, IncludedPath)
    $detailCsv = Join-Path $OutputRoot "physical_pg_detail_$timestamp.csv"
    $detailRows | Export-Csv -Path $detailCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Detail CSV : $detailCsv" -ForegroundColor Green
}

if (-not $NoGridView) {
    try {
        $summaryRows | Out-GridView -Title 'Cohesity Physical PG Summary'
    } catch {
        Write-Warning 'Out-GridView unavailable. CSV was still created.'
    }
}
