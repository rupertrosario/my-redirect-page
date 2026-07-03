#requires -Version 5.1
<#
.SYNOPSIS
    Active Cohesity Physical Protection Group inventory.

.DESCRIPTION
    GET-only Helios report for kPhysical protection groups.
    Supports cluster menu selection: 0 = all, single number, comma list, and numeric ranges.
    Produces Out-GridView and CSV output.

.OUTPUT COLUMNS
    Cluster, Environment, PGName, PolicyName, ProtectionType, PGObjectCount, ServerName,
    ObjectSelection, ObjectExcludePaths, GlobalExcludePaths, DirectiveFile,
    IsActive, IsPaused, LastRunStatus, LastRunEndET
#>

[CmdletBinding()]
param(
    [string]$HeliosUrl  = 'https://helios.cohesity.com',
    [string]$ApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt',
    [string]$OutputRoot = 'X:\PowerShell\cohesity_automation\PG_Inventory\ByType\Physical',
    [switch]$NoGridView
)

$ErrorActionPreference = 'Stop'

function Convert-UsecsToET {
    param([object]$Usecs)
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return '' }
    try {
        $dto = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $tz  = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($dto, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch { return '' }
}

function Get-PropValue {
    param([object]$Object, [string[]]$Names)
    if ($null -eq $Object) { return $null }
    foreach ($name in $Names) {
        $p = $Object.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
        if ($null -ne $p -and $null -ne $p.Value -and [string]$p.Value -ne '') { return $p.Value }
    }
    return $null
}

function Join-Value {
    param([object]$Value)
    if ($null -eq $Value) { return '' }
    if ($Value -is [string]) { return $Value }
    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        $out = foreach ($v in @($Value)) { Join-Value $v }
        return (($out | Where-Object { $_ } | Select-Object -Unique) -join '; ')
    }
    $simple = Get-PropValue $Value @('path','filePath','name','displayName','value','includePath','excludePath')
    if ($simple) { return [string]$simple }
    return ($Value | ConvertTo-Json -Compress -Depth 10)
}

function Find-ValuesByName {
    param([object]$Root, [string[]]$Names, [int]$Depth = 6)
    if ($null -eq $Root -or $Depth -lt 0 -or $Root -is [string]) { return @() }

    $items = New-Object System.Collections.Generic.List[object]

    if ($Root -is [System.Collections.IEnumerable]) {
        foreach ($r in @($Root)) {
            foreach ($v in @(Find-ValuesByName -Root $r -Names $Names -Depth ($Depth - 1))) { [void]$items.Add($v) }
        }
        return @($items)
    }

    foreach ($p in @($Root.PSObject.Properties)) {
        if ($Names | Where-Object { $_ -ieq $p.Name }) {
            if ($null -ne $p.Value) { [void]$items.Add($p.Value) }
        }
        if ($null -ne $p.Value -and -not ($p.Value -is [string])) {
            foreach ($v in @(Find-ValuesByName -Root $p.Value -Names $Names -Depth ($Depth - 1))) { [void]$items.Add($v) }
        }
    }
    return @($items)
}

function Join-FoundValues {
    param([object]$Root, [string[]]$Names, [int]$Depth = 6)
    $flat = foreach ($v in @(Find-ValuesByName -Root $Root -Names $Names -Depth $Depth)) { Join-Value $v }
    return (($flat | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique) -join '; ')
}

function Invoke-CohesityGet {
    param([string]$Uri, [hashtable]$Headers)
    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
}

function As-Array {
    param([object]$Response, [string[]]$CandidateProperties)
    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    foreach ($p in $CandidateProperties) {
        $v = Get-PropValue $Response @($p)
        if ($null -ne $v) { return @($v) }
    }
    return @($Response)
}

function Get-Clusters {
    param([string]$HeliosUrl, [hashtable]$Headers)
    $resp = Invoke-CohesityGet -Uri "$HeliosUrl/v2/mcm/cluster-mgmt/info" -Headers $Headers
    As-Array $resp @('clusterInfos','clusters','data') | ForEach-Object {
        [pscustomobject]@{
            ClusterName = [string](Get-PropValue $_ @('name','clusterName','displayName'))
            ClusterId   = [string](Get-PropValue $_ @('id','clusterId'))
        }
    } | Where-Object { $_.ClusterName -and $_.ClusterId } | Sort-Object ClusterName
}

function Resolve-SelectionPart {
    param([string]$Part, [int]$Max)
    $Part = $Part.Trim()
    if ($Part -match '^(\d+)\s*-\s*(\d+)$') {
        $a = [int]$Matches[1]; $b = [int]$Matches[2]
        if ($a -lt 1 -or $b -gt $Max -or $a -gt $b) { throw "Invalid cluster range: $Part" }
        return @($a..$b)
    }
    $n = 0
    if ([int]::TryParse($Part, [ref]$n)) {
        if ($n -lt 1 -or $n -gt $Max) { throw "Cluster selection out of range: $Part" }
        return @($n)
    }
    throw "Invalid cluster selection: $Part"
}

function Select-Clusters {
    param([object[]]$Clusters)
    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'
    for ($i = 0; $i -lt $Clusters.Count; $i++) { Write-Host ('[{0}] {1}' -f ($i + 1), $Clusters[$i].ClusterName) }
    Write-Host 'Examples: 1,3,5 or 2-4' -ForegroundColor DarkGray
    $choice = (Read-Host 'Enter selection').Trim()
    if ($choice -eq '0') { return @($Clusters) }
    $indexes = foreach ($part in ($choice -split ',')) { Resolve-SelectionPart -Part $part -Max $Clusters.Count }
    @($indexes | Select-Object -Unique | Sort-Object | ForEach-Object { $Clusters[$_ - 1] })
}

function Get-PhysicalObjectsFromPG {
    param([object]$Pg)
    $physical = Get-PropValue $Pg @('physicalParams')
    $lists = @(
        (Get-PropValue $physical @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-PropValue (Get-PropValue $physical @('fileProtectionTypeParams')) @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-PropValue (Get-PropValue $physical @('volumeProtectionTypeParams')) @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-PropValue (Get-PropValue $physical @('systemProtectionTypeParams')) @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-PropValue $Pg @('objects','protectedObjects','entities'))
    )
    foreach ($list in $lists) {
        if ($null -ne $list -and @($list).Count -gt 0) { return @($list) }
    }
    return @()
}

function Get-ServerName {
    param([object]$Object)
    $name = Get-PropValue $Object @('name','serverName','hostName','sourceName','displayName')
    if ($name) { return [string]$name }
    foreach ($n in @('entity','physicalEntity','source','protectionSource','rootEntity','protectedObject')) {
        $child = Get-PropValue $Object @($n)
        $name = Get-PropValue $child @('name','displayName','hostName','sourceName')
        if ($name) { return [string]$name }
    }
    return ''
}

function Get-LastRunSummary {
    param([object]$Pg)
    $last = Get-PropValue $Pg @('lastRun','lastRunInfo','latestRun','latestRunInfo')
    $status = Get-PropValue $last @('status','runStatus')
    $endUsecs = Get-PropValue $last @('endTimeUsecs','runEndTimeUsecs')
    if (-not $endUsecs) { $endUsecs = Get-PropValue (Get-PropValue $last @('localBackupInfo')) @('endTimeUsecs','runEndTimeUsecs') }
    [pscustomobject]@{ Status = [string]$status; EndET = Convert-UsecsToET $endUsecs }
}

function New-Row {
    param([object]$Cluster, [object]$Pg, [object]$Obj, [int]$ObjectCount)
    $physical = Get-PropValue $Pg @('physicalParams')
    $last = Get-LastRunSummary $Pg

    [pscustomobject]@{
        Cluster            = $Cluster.ClusterName
        Environment        = 'kPhysical'
        PGName             = [string](Get-PropValue $Pg @('name','protectionGroupName'))
        PolicyName         = [string](Get-PropValue $Pg @('policyName'))
        ProtectionType     = [string](Get-PropValue $physical @('protectionType'))
        PGObjectCount      = $ObjectCount
        ServerName         = Get-ServerName $Obj
        ObjectSelection    = Join-FoundValues $Obj @('includePaths','includedPaths','selectedPaths','filePaths','paths','volumePaths','selectedVolumes','volumes') 5
        ObjectExcludePaths = Join-FoundValues $Obj @('excludePaths','excludedPaths','exclusionPaths') 5
        GlobalExcludePaths = Join-FoundValues $physical @('globalExcludePaths','globalExcludedPaths','globalExclusionPaths','excludePaths','excludedPaths','exclusionPaths') 6
        DirectiveFile      = (Join-FoundValues $Obj @('directiveFile','directiveFilePath','directivePath','directiveFileName') 5)
        IsActive           = [string](Get-PropValue $Pg @('isActive'))
        IsPaused           = [string](Get-PropValue $Pg @('isPaused'))
        LastRunStatus      = $last.Status
        LastRunEndET       = $last.EndET
    }
}

function Get-InventoryForCluster {
    param([object]$Cluster, [string]$HeliosUrl, [hashtable]$BaseHeaders)
    $headers = @{}
    foreach ($k in $BaseHeaders.Keys) { $headers[$k] = $BaseHeaders[$k] }
    $headers['accessClusterId'] = $Cluster.ClusterId

    $uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true"
    $resp = Invoke-CohesityGet -Uri $uri -Headers $headers
    $pgs = As-Array $resp @('protectionGroups','data','items')

    foreach ($pg in $pgs) {
        if ([string](Get-PropValue $pg @('isActive')) -notmatch '^(True|true|1)$') { continue }
        $objects = @(Get-PhysicalObjectsFromPG $pg)
        $count = if ($objects.Count -gt 0) { $objects.Count } else { [int]([string](Get-PropValue $pg @('objectCount','numObjects','numProtectedObjects','protectedObjectCount') -replace '^$','0')) }
        if ($objects.Count -eq 0) { New-Row -Cluster $Cluster -Pg $pg -Obj $null -ObjectCount $count }
        else { foreach ($obj in $objects) { New-Row -Cluster $Cluster -Pg $pg -Obj $obj -ObjectCount $count } }
    }
}

if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
$apiKey = (Get-Content $ApiKeyPath -Raw).Trim()
if (-not $apiKey) { throw "API key file is empty: $ApiKeyPath" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

$headers = @{ accept = 'application/json'; apiKey = $apiKey }
$clusters = @(Get-Clusters -HeliosUrl $HeliosUrl -Headers $headers)
if ($clusters.Count -eq 0) { throw 'No clusters returned from Helios.' }

$selected = @(Select-Clusters -Clusters $clusters)
$rows = foreach ($cluster in $selected) {
    Write-Host "Collecting ACTIVE Physical PG inventory from $($cluster.ClusterName)..." -ForegroundColor Yellow
    Get-InventoryForCluster -Cluster $cluster -HeliosUrl $HeliosUrl -BaseHeaders $headers
}

$rows = @($rows | Sort-Object Cluster, PGName, ServerName)
$csv = Join-Path $OutputRoot ('Physical_PG_Inventory_Active_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
$rows | Export-Csv -Path $csv -NoTypeInformation -Encoding UTF8

Write-Host "Rows: $($rows.Count)" -ForegroundColor Green
Write-Host "CSV : $csv" -ForegroundColor Green
$rows | Format-Table Cluster, PGName, PGObjectCount, ServerName, ObjectSelection, ObjectExcludePaths, GlobalExcludePaths -AutoSize

if (-not $NoGridView) {
    try { $rows | Out-GridView -Title 'Cohesity ACTIVE Physical PG Inventory' }
    catch { Write-Warning 'Out-GridView unavailable. CSV was still created.' }
}
