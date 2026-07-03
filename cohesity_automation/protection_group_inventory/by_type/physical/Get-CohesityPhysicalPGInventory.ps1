#requires -Version 5.1
<#
Active Cohesity Physical protection group inventory.
GET-only. Output: console, GridView, CSV.
#>

[CmdletBinding()]
param(
    [string]$HeliosUrl = 'https://helios.cohesity.com',
    [string]$KeyFilePath = (Join-Path 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete' ('api' + 'key.txt')),
    [string]$OutputRoot = 'X:\PowerShell\cohesity_automation\protection_group_inventory\by_type\physical',
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

function Is-List {
    param([object]$Val)
    return ($null -ne $Val -and $Val -is [System.Collections.IEnumerable] -and -not ($Val -is [string]) -and -not ($Val -is [hashtable]))
}

function To-ET {
    param([object]$Usecs)
    if ($null -eq $Usecs -or [string]::IsNullOrWhiteSpace([string]$Usecs)) { return '' }
    try {
        $dto = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $tz  = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($dto, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch { return '' }
}

function Join-Any {
    param([object]$Val)
    if ($null -eq $Val) { return '' }
    if ($Val -is [string]) { return $Val }
    if (Is-List $Val) {
        $x = foreach ($v in @($Val)) { Join-Any $v }
        return (($x | Where-Object { $_ } | Select-Object -Unique) -join '; ')
    }
    $simple = Get-Val $Val @('path','filePath','includePath','includedPath','excludePath','excludedPath','name','displayName','value')
    if ($simple) { return [string]$simple }
    return ($Val | ConvertTo-Json -Compress -Depth 8)
}

function Find-ByName {
    param(
        [object]$Root,
        [string[]]$Names,
        [string[]]$Skip = @(),
        [int]$Depth = 6
    )
    if ($null -eq $Root -or $Depth -lt 0 -or $Root -is [string]) { return @() }

    $out = New-Object System.Collections.Generic.List[object]
    if (Is-List $Root) {
        foreach ($i in @($Root)) {
            foreach ($v in @(Find-ByName -Root $i -Names $Names -Skip $Skip -Depth ($Depth - 1))) { [void]$out.Add($v) }
        }
        return @($out)
    }

    foreach ($p in @($Root.PSObject.Properties)) {
        if ($Skip | Where-Object { $_ -ieq $p.Name }) { continue }
        if ($Names | Where-Object { $_ -ieq $p.Name }) { if ($null -ne $p.Value) { [void]$out.Add($p.Value) } }
        if ($null -ne $p.Value -and -not ($p.Value -is [string])) {
            foreach ($v in @(Find-ByName -Root $p.Value -Names $Names -Skip $Skip -Depth ($Depth - 1))) { [void]$out.Add($v) }
        }
    }
    return @($out)
}

function Join-Found {
    param([object]$Root, [string[]]$Names, [string[]]$Skip = @(), [int]$Depth = 6)
    $flat = foreach ($v in @(Find-ByName -Root $Root -Names $Names -Skip $Skip -Depth $Depth)) { Join-Any $v }
    return (($flat | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique) -join '; ')
}

function Get-Array {
    param([object]$Resp, [string[]]$Props)
    if ($null -eq $Resp) { return @() }
    if ($Resp -is [array]) { return @($Resp) }
    foreach ($p in $Props) {
        $v = Get-Val $Resp @($p)
        if ($null -ne $v) { return @($v) }
    }
    return @($Resp)
}

function Invoke-Get {
    param([string]$Uri, [hashtable]$Headers)
    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
}

function Find-ClustersDeep {
    param([object]$Root, [int]$Depth = 8)
    if ($null -eq $Root -or $Depth -lt 0 -or $Root -is [string]) { return @() }

    $out = New-Object System.Collections.Generic.List[object]
    if (Is-List $Root) {
        foreach ($i in @($Root)) {
            foreach ($c in @(Find-ClustersDeep -Root $i -Depth ($Depth - 1))) { [void]$out.Add($c) }
        }
        return @($out)
    }

    $name = Get-Val $Root @('clusterName','name','displayName','clusterDisplayName','fqdn')
    $id   = Get-Val $Root @('clusterId','id','clusterIdentifier','clusterUuid','uuid','clusterIncarnationId')
    if ($name -and $id) {
        [void]$out.Add([pscustomobject]@{ ClusterName = [string]$name; ClusterId = [string]$id })
    }

    foreach ($p in @($Root.PSObject.Properties)) {
        if ($null -ne $p.Value -and -not ($p.Value -is [string])) {
            foreach ($c in @(Find-ClustersDeep -Root $p.Value -Depth ($Depth - 1))) { [void]$out.Add($c) }
        }
    }
    return @($out)
}

function Get-Clusters {
    param([string]$HeliosUrl, [hashtable]$Headers, [string]$OutputRoot)
    $uri = "$HeliosUrl/v2/mcm/cluster-mgmt/info"
    try { $resp = Invoke-Get -Uri $uri -Headers $Headers }
    catch { throw "Cluster discovery failed from $uri. $($_.Exception.Message)" }

    $direct = Get-Array $resp @('clusterInfos','clusterInfoList','clusters','registeredClusters','mcmClusterConnectionDetails','clusterConnectionInfos','data','items')
    $clusters = @(Find-ClustersDeep $direct; Find-ClustersDeep $resp) |
        Where-Object { $_.ClusterName -and $_.ClusterId } |
        Sort-Object ClusterName, ClusterId -Unique

    if ($clusters.Count -eq 0) {
        $debug = Join-Path $OutputRoot ('DEBUG_cluster_discovery_{0}.json' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
        $resp | ConvertTo-Json -Depth 30 | Out-File -FilePath $debug -Encoding UTF8
        throw "No clusters parsed. Debug file written: $debug"
    }
    return @($clusters)
}

function Resolve-Part {
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
    $idx = foreach ($part in ($choice -split ',')) { Resolve-Part -Part $part -Max $Clusters.Count }
    return @($idx | Select-Object -Unique | Sort-Object | ForEach-Object { $Clusters[$_ - 1] })
}

function Get-PhysicalObjects {
    param([object]$Group)
    $p = Get-Val $Group @('physicalParams')
    $lists = @(
        (Get-Val $p @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-Val (Get-Val $p @('fileProtectionTypeParams')) @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-Val (Get-Val $p @('volumeProtectionTypeParams')) @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-Val (Get-Val $p @('systemProtectionTypeParams')) @('objects','sourceParamsList','sources','physicalSources','objectParams','objectParamsList')),
        (Get-Val $Group @('objects','protectedObjects','entities'))
    )
    foreach ($l in $lists) { if ($null -ne $l -and @($l).Count -gt 0) { return @($l) } }
    return @()
}

function Get-ServerName {
    param([object]$Object)
    $n = Get-Val $Object @('name','serverName','hostName','sourceName','displayName')
    if ($n) { return [string]$n }
    foreach ($x in @('entity','physicalEntity','source','protectionSource','rootEntity','protectedObject')) {
        $child = Get-Val $Object @($x)
        $n = Get-Val $child @('name','displayName','hostName','sourceName')
        if ($n) { return [string]$n }
    }
    return ''
}

function Get-LastRun {
    param([object]$Group)
    $last = Get-Val $Group @('lastRun','lastRunInfo','latestRun','latestRunInfo')
    $status = Get-Val $last @('status','runStatus')
    $end = Get-Val $last @('endTimeUsecs','runEndTimeUsecs')
    if (-not $end) { $end = Get-Val (Get-Val $last @('localBackupInfo')) @('endTimeUsecs','runEndTimeUsecs') }
    [pscustomobject]@{ Status = [string]$status; EndET = To-ET $end }
}

function To-Int {
    param([object]$Value)
    $n = 0
    if ($null -ne $Value -and [int]::TryParse([string]$Value, [ref]$n)) { return $n }
    return 0
}

function New-Row {
    param([object]$Cluster, [object]$Group, [object]$Object, [int]$Count)
    $p = Get-Val $Group @('physicalParams')
    $r = Get-LastRun $Group
    $objectListNames = @('objects','sourceParamsList','sources','protectionSources','physicalSources','objectParams','objectParamsList','entities','protectedObjects')

    [pscustomobject]@{
        Cluster            = $Cluster.ClusterName
        Environment        = 'kPhysical'
        PGName             = [string](Get-Val $Group @('name','protectionGroupName'))
        PolicyName         = [string](Get-Val $Group @('policyName'))
        ProtectionType     = [string](Get-Val $p @('protectionType'))
        PGObjectCount      = $Count
        ServerName         = Get-ServerName $Object
        ObjectSelection    = Join-Found -Root $Object -Names @('includePaths','includedPaths','selectedPaths','filePaths','paths','volumePaths','selectedVolumes','volumes') -Skip @('excludePaths','excludedPaths','exclusionPaths') -Depth 5
        ObjectExcludePaths = Join-Found -Root $Object -Names @('excludePaths','excludedPaths','exclusionPaths') -Depth 5
        GlobalExcludePaths = Join-Found -Root $p -Names @('globalExcludePaths','globalExcludedPaths','globalExclusionPaths','excludePaths','excludedPaths','exclusionPaths') -Skip $objectListNames -Depth 6
        DirectiveFile      = Join-Found -Root $Object -Names @('directiveFile','directiveFilePath','directivePath','directiveFileName') -Depth 5
        IsActive           = [string](Get-Val $Group @('isActive'))
        IsPaused           = [string](Get-Val $Group @('isPaused'))
        LastRunStatus      = $r.Status
        LastRunEndET       = $r.EndET
    }
}

function Get-InventoryForCluster {
    param([object]$Cluster, [string]$HeliosUrl, [hashtable]$BaseHeaders)
    $headers = @{}
    foreach ($k in $BaseHeaders.Keys) { $headers[$k] = $BaseHeaders[$k] }
    $headers['accessClusterId'] = $Cluster.ClusterId

    $uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true"
    $resp = Invoke-Get -Uri $uri -Headers $headers
    $groups = Get-Array $resp @('protectionGroups','data','items')

    foreach ($g in $groups) {
        if ([string](Get-Val $g @('isActive')) -notmatch '^(True|true|1)$') { continue }
        $objs = @(Get-PhysicalObjects $g)
        $count = if ($objs.Count -gt 0) { $objs.Count } else { To-Int (Get-Val $g @('objectCount','numObjects','numProtectedObjects','protectedObjectCount')) }
        if ($objs.Count -eq 0) { New-Row -Cluster $Cluster -Group $g -Object $null -Count $count }
        else { foreach ($o in $objs) { New-Row -Cluster $Cluster -Group $g -Object $o -Count $count } }
    }
}

if (-not (Test-Path $KeyFilePath)) { throw "Key file not found: $KeyFilePath" }
$key = (Get-Content $KeyFilePath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($key)) { throw "Key file is empty: $KeyFilePath" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$headers = @{ accept = 'application/json' }
$headers[('api' + 'Key')] = $key

$clusters = @(Get-Clusters -HeliosUrl $HeliosUrl -Headers $headers -OutputRoot $OutputRoot)
$selected = @(Select-Clusters -Clusters $clusters)

$rows = foreach ($c in $selected) {
    Write-Host "Collecting ACTIVE Physical inventory from $($c.ClusterName)..." -ForegroundColor Yellow
    Get-InventoryForCluster -Cluster $c -HeliosUrl $HeliosUrl -BaseHeaders $headers
}

$rows = @($rows | Sort-Object Cluster, PGName, ServerName)
$csv = Join-Path $OutputRoot ('Physical_PG_Inventory_Active_{0}.csv' -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
$rows | Export-Csv -Path $csv -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host "Rows: $($rows.Count)" -ForegroundColor Green
Write-Host "CSV : $csv" -ForegroundColor Green
$rows | Format-Table Cluster, PGName, PGObjectCount, ServerName, ObjectSelection, ObjectExcludePaths, GlobalExcludePaths -AutoSize

if (-not $NoGridView) {
    try { $rows | Out-GridView -Title 'Cohesity ACTIVE Physical Inventory' }
    catch { Write-Warning 'Out-GridView unavailable. CSV was still created.' }
}
