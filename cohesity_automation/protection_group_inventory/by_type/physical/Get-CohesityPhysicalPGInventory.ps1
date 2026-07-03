#requires -Version 5.1
<#
.SYNOPSIS
    Active Cohesity Physical Protection Group inventory.

.DESCRIPTION
    GET-only Helios report for active kPhysical Protection Groups.
    Uses the documented physical schema directly:
      physicalParams.protectionType
      physicalParams.fileProtectionTypeParams.objects[].filePaths[].includedPath
      physicalParams.fileProtectionTypeParams.objects[].filePaths[].excludedPaths
      physicalParams.fileProtectionTypeParams.globalExcludePaths
      physicalParams.volumeProtectionTypeParams.objects[].volumeGuids

    Output: console preview, Out-GridView, and CSV.
#>

[CmdletBinding()]
param(
    [string]$HeliosUrl  = 'https://helios.cohesity.com',
    [string]$ApiKeyPath = (Join-Path 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete' ('api' + 'key.txt')),
    [string]$OutputRoot = 'X:\PowerShell\cohesity_automation\protection_group_inventory\by_type\physical',
    [switch]$NoGridView
)

$ErrorActionPreference = 'Stop'

function Get-Value {
    param([object]$Object, [string[]]$Names)
    if ($null -eq $Object) { return $null }
    foreach ($name in $Names) {
        $prop = $Object.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
        if ($null -ne $prop -and $null -ne $prop.Value -and -not [string]::IsNullOrWhiteSpace([string]$prop.Value)) {
            return $prop.Value
        }
    }
    return $null
}

function Get-NestedValue {
    param([object]$Object, [string[]]$Path)
    $current = $Object
    foreach ($part in $Path) {
        $current = Get-Value $current @($part)
        if ($null -eq $current) { return $null }
    }
    return $current
}

function To-List {
    param([object]$Value)
    if ($null -eq $Value) { return @() }
    if ($Value -is [string]) { return @($Value) }
    if ($Value -is [System.Collections.IEnumerable]) { return @($Value) }
    return @($Value)
}

function Join-List {
    param([object]$Value)
    $items = foreach ($item in (To-List $Value)) {
        if ($null -eq $item) { continue }
        if ($item -is [string]) { $item; continue }

        $simple = Get-Value $item @('includedPath','path','filePath','name','displayName','value','id')
        if ($null -ne $simple) { [string]$simple }
        else { $item | ConvertTo-Json -Compress -Depth 8 }
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
    } catch {
        return ''
    }
}

function Invoke-CohesityGet {
    param([string]$Uri, [hashtable]$Headers)
    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
}

function Get-ArrayFromResponse {
    param([object]$Response)
    if ($null -eq $Response) { return @() }
    foreach ($name in @('protectionGroups','data','items')) {
        $value = Get-Value $Response @($name)
        if ($null -ne $value) { return @(To-List $value) }
    }
    if ($Response -is [array]) { return @($Response) }
    return @($Response)
}

function Get-ProtectionGroups {
    param([string]$HeliosUrl, [hashtable]$Headers)

    $groups = New-Object System.Collections.Generic.List[object]
    $cookie = $null

    do {
        $uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeTenants=true&includeLastRunInfo=true&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace([string]$cookie)) {
            $uri = "$uri&paginationCookie=$([uri]::EscapeDataString([string]$cookie))"
        }

        $response = Invoke-CohesityGet -Uri $uri -Headers $Headers
        foreach ($group in (Get-ArrayFromResponse $response)) { [void]$groups.Add($group) }
        $cookie = Get-Value $response @('paginationCookie','nextPageCookie','nextPaginationCookie')
    } while (-not [string]::IsNullOrWhiteSpace([string]$cookie))

    return @($groups)
}

function Get-ClusterNameFromPG {
    param([object]$PG)

    $value = Get-Value $PG @('clusterName','clusterDisplayName','sourceClusterName')
    if ($value) { return [string]$value }

    foreach ($path in @(
        @('cluster','name'),
        @('clusterInfo','name'),
        @('clusterIdentifier','name'),
        @('clusterIdentifier','clusterName')
    )) {
        $value = Get-NestedValue $PG $path
        if ($value) { return [string]$value }
    }

    $id = Get-ClusterIdFromPG $PG
    if ($id) { return "ClusterId_$id" }
    return 'Helios'
}

function Get-ClusterIdFromPG {
    param([object]$PG)

    $value = Get-Value $PG @('clusterId','accessClusterId','sourceClusterId')
    if ($value) { return [string]$value }

    foreach ($path in @(
        @('cluster','id'),
        @('clusterInfo','id'),
        @('clusterIdentifier','id'),
        @('clusterIdentifier','clusterId')
    )) {
        $value = Get-NestedValue $PG $path
        if ($value) { return [string]$value }
    }

    return ''
}

function Resolve-SelectionPart {
    param([string]$Part, [int]$Max)

    $Part = $Part.Trim()
    if ([string]::IsNullOrWhiteSpace($Part)) { return @() }

    if ($Part -match '^(\d+)\s*-\s*(\d+)$') {
        $start = [int]$Matches[1]
        $end = [int]$Matches[2]
        if ($start -lt 1 -or $end -gt $Max -or $start -gt $end) { throw "Invalid cluster range: $Part" }
        return @($start..$end)
    }

    $number = 0
    if ([int]::TryParse($Part, [ref]$number)) {
        if ($number -lt 1 -or $number -gt $Max) { throw "Cluster selection out of range: $Part" }
        return @($number)
    }

    throw "Invalid cluster selection: $Part"
}

function Select-ClusterScope {
    param([object[]]$ProtectionGroups)

    $clusters = @(
        foreach ($pg in $ProtectionGroups) {
            $name = Get-ClusterNameFromPG $pg
            $id = Get-ClusterIdFromPG $pg
            [pscustomobject]@{
                ClusterName = $name
                ClusterId = $id
                Key = "$name|$id"
            }
        }
    ) | Sort-Object ClusterName, ClusterId -Unique

    if ($clusters.Count -le 1) { return @($ProtectionGroups) }

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'
    for ($i = 0; $i -lt $clusters.Count; $i++) {
        Write-Host ('[{0}] {1}' -f ($i + 1), $clusters[$i].ClusterName)
    }
    Write-Host 'Examples: 1,3,5 or 2-4' -ForegroundColor DarkGray

    $choice = (Read-Host 'Enter selection').Trim()
    if ($choice -eq '0') { return @($ProtectionGroups) }

    $indexes = foreach ($part in ($choice -split ',')) { Resolve-SelectionPart -Part $part -Max $clusters.Count }
    $selectedKeys = @($indexes | Select-Object -Unique | ForEach-Object { $clusters[$_ - 1].Key })

    return @($ProtectionGroups | Where-Object {
        $key = "$(Get-ClusterNameFromPG $_)|$(Get-ClusterIdFromPG $_)"
        $selectedKeys -contains $key
    })
}

function Get-LastRunSummary {
    param([object]$PG)

    $lastRun = Get-Value $PG @('lastRun','lastRunInfo','latestRun','latestRunInfo')
    $local = Get-Value $lastRun @('localBackupInfo','localSnapshotInfo')

    $status = Get-Value $local @('status','runStatus')
    if (-not $status) { $status = Get-Value $lastRun @('status','runStatus','lastRunAnyStatus') }

    $endUsecs = Get-Value $local @('endTimeUsecs','runEndTimeUsecs','endTimeInUsecs')
    if (-not $endUsecs) { $endUsecs = Get-Value $lastRun @('endTimeUsecs','runEndTimeUsecs','endTimeInUsecs') }

    [pscustomobject]@{
        Status = [string]$status
        EndET = Convert-UsecsToET $endUsecs
    }
}

function New-FileRows {
    param([object]$PG)

    $physical = Get-Value $PG @('physicalParams')
    $fileParams = Get-Value $physical @('fileProtectionTypeParams')
    $objects = @(To-List (Get-Value $fileParams @('objects')))
    $objectCount = $objects.Count
    $globalExcludePaths = Join-List (Get-Value $fileParams @('globalExcludePaths'))
    $globalExcludeFS = Join-List (Get-Value $fileParams @('globalExcludeFS'))
    $jobVss = Join-List (Get-Value $fileParams @('excludedVssWriters'))
    $last = Get-LastRunSummary $PG

    foreach ($object in $objects) {
        $filePaths = @(To-List (Get-Value $object @('filePaths')))
        if ($filePaths.Count -eq 0) { $filePaths = @($null) }

        foreach ($filePath in $filePaths) {
            [pscustomobject]@{
                Cluster                  = Get-ClusterNameFromPG $PG
                ClusterId                = Get-ClusterIdFromPG $PG
                Environment              = 'kPhysical'
                PGName                   = [string](Get-Value $PG @('name','protectionGroupName'))
                PolicyName               = [string](Get-Value $PG @('policyName'))
                ProtectionType           = [string](Get-Value $physical @('protectionType'))
                PGObjectCount            = $objectCount
                ObjectId                 = [string](Get-Value $object @('id','sourceId'))
                ObjectName               = [string](Get-Value $object @('name','sourceName','hostName'))
                IncludedPath             = [string](Get-Value $filePath @('includedPath'))
                ObjectExcludedPaths      = Join-List (Get-Value $filePath @('excludedPaths'))
                SkipNestedVolumes        = [string](Get-Value $filePath @('skipNestedVolumes'))
                ObjectExcludedVssWriters = Join-List (Get-Value $object @('excludedVssWriters'))
                GlobalExcludePaths       = $globalExcludePaths
                GlobalExcludeFS          = $globalExcludeFS
                JobExcludedVssWriters    = $jobVss
                IsActive                 = [string](Get-Value $PG @('isActive'))
                IsPaused                 = [string](Get-Value $PG @('isPaused'))
                LastRunStatus            = $last.Status
                LastRunEndET             = $last.EndET
            }
        }
    }
}

function New-VolumeRows {
    param([object]$PG)

    $physical = Get-Value $PG @('physicalParams')
    $volumeParams = Get-Value $physical @('volumeProtectionTypeParams')
    $objects = @(To-List (Get-Value $volumeParams @('objects')))
    $objectCount = $objects.Count
    $jobVss = Join-List (Get-Value $volumeParams @('excludedVssWriters'))
    $last = Get-LastRunSummary $PG

    foreach ($object in $objects) {
        [pscustomobject]@{
            Cluster                  = Get-ClusterNameFromPG $PG
            ClusterId                = Get-ClusterIdFromPG $PG
            Environment              = 'kPhysical'
            PGName                   = [string](Get-Value $PG @('name','protectionGroupName'))
            PolicyName               = [string](Get-Value $PG @('policyName'))
            ProtectionType           = [string](Get-Value $physical @('protectionType'))
            PGObjectCount            = $objectCount
            ObjectId                 = [string](Get-Value $object @('id','sourceId'))
            ObjectName               = [string](Get-Value $object @('name','sourceName','hostName'))
            IncludedPath             = Join-List (Get-Value $object @('volumeGuids'))
            ObjectExcludedPaths      = ''
            SkipNestedVolumes        = ''
            ObjectExcludedVssWriters = Join-List (Get-Value $object @('excludedVssWriters'))
            GlobalExcludePaths       = ''
            GlobalExcludeFS          = ''
            JobExcludedVssWriters    = $jobVss
            IsActive                 = [string](Get-Value $PG @('isActive'))
            IsPaused                 = [string](Get-Value $PG @('isPaused'))
            LastRunStatus            = $last.Status
            LastRunEndET             = $last.EndET
        }
    }
}

if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
$apiKey = (Get-Content $ApiKeyPath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "API key file is empty: $ApiKeyPath" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

$headers = @{ accept = 'application/json' }
$headers[('api' + 'Key')] = $apiKey

$pgs = @(Get-ProtectionGroups -HeliosUrl $HeliosUrl -Headers $headers)
if ($pgs.Count -eq 0) {
    Write-Warning 'No active kPhysical Protection Groups returned by Helios.'
    return
}

$selectedPgs = @(Select-ClusterScope -ProtectionGroups $pgs)

$rows = foreach ($pg in $selectedPgs) {
    $physical = Get-Value $pg @('physicalParams')
    $type = [string](Get-Value $physical @('protectionType'))

    if ($type -eq 'kVolume') { New-VolumeRows -PG $pg }
    else { New-FileRows -PG $pg }
}

$rows = @($rows | Sort-Object Cluster, PGName, ObjectName, IncludedPath)
$csvPath = Join-Path $OutputRoot ('Physical_PG_Inventory_Active_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host "Protection Groups: $($selectedPgs.Count)" -ForegroundColor Green
Write-Host "Rows             : $($rows.Count)" -ForegroundColor Green
Write-Host "CSV              : $csvPath" -ForegroundColor Green

$rows |
    Select-Object Cluster, PGName, ProtectionType, PGObjectCount, ObjectName, IncludedPath, ObjectExcludedPaths, GlobalExcludePaths |
    Format-Table -AutoSize

if (-not $NoGridView) {
    try {
        $rows | Out-GridView -Title 'Cohesity Active Physical PG Inventory'
    } catch {
        Write-Warning 'Out-GridView is unavailable. CSV was still created.'
    }
}
