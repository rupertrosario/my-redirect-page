#requires -Version 5.1
<#
.SYNOPSIS
    Cohesity Physical Protection Group Inventory - By Type v3.

.DESCRIPTION
    GET-only Helios report for ACTIVE kPhysical protection groups.
    Cluster scope: [0] ALL, single cluster, or multiple clusters using comma/range selection.
    Output: Out-GridView + CSV.

.NOTES
    Grain: one row per protected Physical object/server inside each active PG.
    Focus fields: object count, PG/global exclude paths, object-level selections, object-level exclude paths.
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
        $epoch = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000))
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([System.TimeZoneInfo]::ConvertTime($epoch, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch {
        return ''
    }
}

function Get-Prop {
    param(
        [object]$InputObject,
        [string[]]$Names
    )
    if ($null -eq $InputObject) { return $null }

    foreach ($name in $Names) {
        $p = $InputObject.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
        if ($null -ne $p -and $null -ne $p.Value -and [string]$p.Value -ne '') { return $p.Value }
    }
    return $null
}

function Test-EnumerableButNotString {
    param([object]$Value)
    return ($null -ne $Value -and
            $Value -is [System.Collections.IEnumerable] -and
            -not ($Value -is [string]) -and
            -not ($Value -is [hashtable]))
}

function Join-FlatValue {
    param([object]$Value)
    if ($null -eq $Value) { return '' }

    if ($Value -is [string]) { return $Value }

    if (Test-EnumerableButNotString $Value) {
        $items = foreach ($v in @($Value)) {
            if ($null -eq $v) { continue }
            if ($v -is [string]) { $v; continue }

            $candidate = Get-Prop $v @(
                'path', 'filePath', 'includePath', 'includedPath',
                'excludePath', 'excludedPath', 'name', 'displayName', 'value'
            )
            if ($null -ne $candidate) { [string]$candidate }
            else { ($v | ConvertTo-Json -Compress -Depth 10) }
        }
        return (($items | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique) -join '; ')
    }

    if ($Value.PSObject.Properties.Count -gt 0) {
        $candidate = Get-Prop $Value @(
            'path', 'filePath', 'includePath', 'includedPath',
            'excludePath', 'excludedPath', 'name', 'displayName', 'value'
        )
        if ($null -ne $candidate) { return [string]$candidate }
        return ($Value | ConvertTo-Json -Compress -Depth 10)
    }

    return [string]$Value
}

function Find-NamedValuesRecursive {
    param(
        [object]$Root,
        [string[]]$Names,
        [string[]]$SkipPropertyNames = @(),
        [int]$MaxDepth = 6
    )

    if ($null -eq $Root -or $MaxDepth -lt 0) { return @() }
    if ($Root -is [string]) { return @() }

    $results = New-Object System.Collections.Generic.List[object]

    if (Test-EnumerableButNotString $Root) {
        foreach ($item in @($Root)) {
            $sub = Find-NamedValuesRecursive -Root $item -Names $Names -SkipPropertyNames $SkipPropertyNames -MaxDepth ($MaxDepth - 1)
            foreach ($s in @($sub)) { [void]$results.Add($s) }
        }
        return @($results)
    }

    foreach ($prop in @($Root.PSObject.Properties)) {
        if ($SkipPropertyNames | Where-Object { $_ -ieq $prop.Name }) { continue }

        if ($Names | Where-Object { $_ -ieq $prop.Name }) {
            if ($null -ne $prop.Value) { [void]$results.Add($prop.Value) }
        }

        if ($null -ne $prop.Value -and -not ($prop.Value -is [string])) {
            $sub = Find-NamedValuesRecursive -Root $prop.Value -Names $Names -SkipPropertyNames $SkipPropertyNames -MaxDepth ($MaxDepth - 1)
            foreach ($s in @($sub)) { [void]$results.Add($s) }
        }
    }

    return @($results)
}

function Join-NamedValues {
    param(
        [object]$Root,
        [string[]]$Names,
        [string[]]$SkipPropertyNames = @(),
        [int]$MaxDepth = 6
    )

    $values = Find-NamedValuesRecursive -Root $Root -Names $Names -SkipPropertyNames $SkipPropertyNames -MaxDepth $MaxDepth
    $flat = foreach ($value in @($values)) { Join-FlatValue $value }
    return (($flat | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique) -join '; ')
}

function Invoke-CohesityGet {
    param(
        [string]$Uri,
        [hashtable]$Headers,
        [int]$TimeoutSec = 120
    )
    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $TimeoutSec
}

function Get-ArrayFromResponse {
    param(
        [object]$Response,
        [string[]]$CandidateProperties
    )

    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }

    foreach ($prop in $CandidateProperties) {
        $value = Get-Prop $Response @($prop)
        if ($null -ne $value) { return @($value) }
    }

    return @($Response)
}

function Get-ClusterInventory {
    param(
        [string]$HeliosUrl,
        [hashtable]$BaseHeaders
    )

    $uri = "$HeliosUrl/v2/mcm/cluster-mgmt/info"
    $resp = Invoke-CohesityGet -Uri $uri -Headers $BaseHeaders
    $clusters = Get-ArrayFromResponse -Response $resp -CandidateProperties @('clusterInfos', 'clusters', 'data')

    $clusters | ForEach-Object {
        [pscustomobject]@{
            ClusterName = [string](Get-Prop $_ @('name', 'clusterName', 'displayName'))
            ClusterId   = [string](Get-Prop $_ @('id', 'clusterId'))
        }
    } | Where-Object { $_.ClusterName -and $_.ClusterId } | Sort-Object ClusterName
}

function Resolve-ClusterSelectionToken {
    param(
        [string]$Token,
        [int]$MaxIndex
    )

    $Token = $Token.Trim()
    if ([string]::IsNullOrWhiteSpace($Token)) { return @() }

    if ($Token -match '^(\d+)\s*-\s*(\d+)$') {
        $start = [int]$Matches[1]
        $end   = [int]$Matches[2]
        if ($start -gt $end) { throw "Invalid cluster range: $Token" }
        if ($start -lt 1 -or $end -gt $MaxIndex) { throw "Cluster range out of range: $Token" }
        return @($start..$end)
    }

    $index = 0
    if ([int]::TryParse($Token, [ref]$index)) {
        if ($index -lt 1 -or $index -gt $MaxIndex) { throw "Cluster selection out of range: $Token" }
        return @($index)
    }

    throw "Invalid cluster selection token: $Token"
}

function Select-Clusters {
    param([object[]]$Clusters)

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'
    for ($i = 0; $i -lt $Clusters.Count; $i++) {
        Write-Host ("[{0}] {1}" -f ($i + 1), $Clusters[$i].ClusterName)
    }
    Write-Host ''
    Write-Host 'Examples: 1 = single cluster, 1,3,5 = multiple clusters, 2-4 = range' -ForegroundColor DarkGray

    $choice = (Read-Host 'Enter selection').Trim()
    if ($choice -eq '0') { return @($Clusters) }
    if ([string]::IsNullOrWhiteSpace($choice)) { throw 'Cluster selection cannot be blank.' }

    $indexes = New-Object System.Collections.Generic.List[int]
    foreach ($token in ($choice -split ',')) {
        $resolved = Resolve-ClusterSelectionToken -Token $token -MaxIndex $Clusters.Count
        foreach ($idx in @($resolved)) { [void]$indexes.Add($idx) }
    }

    $uniqueIndexes = @($indexes | Select-Object -Unique | Sort-Object)
    if ($uniqueIndexes.Count -eq 0) { throw "Invalid cluster selection: $choice" }

    return @($uniqueIndexes | ForEach-Object { $Clusters[$_ - 1] })
}

function Get-PhysicalObjectCandidates {
    param([object]$Pg)

    $physicalParams = Get-Prop $Pg @('physicalParams')

    $candidateLists = @(
        (Get-Prop $physicalParams @('objects', 'sourceParamsList', 'sources', 'protectionSources', 'physicalSources', 'objectParams', 'objectParamsList')),
        (Get-Prop (Get-Prop $physicalParams @('fileProtectionTypeParams')) @('objects', 'sourceParamsList', 'sources', 'physicalSources', 'objectParams', 'objectParamsList')),
        (Get-Prop (Get-Prop $physicalParams @('volumeProtectionTypeParams')) @('objects', 'sourceParamsList', 'sources', 'physicalSources', 'objectParams', 'objectParamsList')),
        (Get-Prop (Get-Prop $physicalParams @('systemProtectionTypeParams')) @('objects', 'sourceParamsList', 'sources', 'physicalSources', 'objectParams', 'objectParamsList')),
        (Get-Prop $Pg @('objects', 'protectedObjects', 'entities'))
    )

    foreach ($list in $candidateLists) {
        if ($null -ne $list) {
            $arr = @($list)
            if ($arr.Count -gt 0) { return $arr }
        }
    }

    return @()
}

function Get-PGLastRunInfo {
    param([object]$Pg)

    $lastRun = Get-Prop $Pg @('lastRun', 'lastRunInfo', 'latestRun', 'latestRunInfo')
    $status = Get-Prop $lastRun @('status', 'runStatus')

    $endUsecs = Get-Prop $lastRun @('endTimeUsecs', 'runEndTimeUsecs')
    if ($null -eq $endUsecs) {
        $localBackupInfo = Get-Prop $lastRun @('localBackupInfo')
        $endUsecs = Get-Prop $localBackupInfo @('endTimeUsecs', 'runEndTimeUsecs')
    }

    [pscustomobject]@{
        Status = [string]$status
        EndET  = Convert-UsecsToET $endUsecs
    }
}

function Get-BooleanText {
    param([object]$Value)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { return '' }
    return [string]([bool]::Parse([string]$Value))
}

function Test-Truthy {
    param([object]$Value)
    if ($null -eq $Value) { return $false }
    $text = ([string]$Value).Trim()
    return ($text -ieq 'true' -or $text -eq '1')
}

function Get-ServerName {
    param([object]$PhysicalObject)

    $serverName = Get-Prop $PhysicalObject @('name', 'serverName', 'hostName', 'sourceName', 'displayName')
    if ($null -ne $serverName) { return [string]$serverName }

    foreach ($propName in @('entity', 'physicalEntity', 'source', 'protectionSource', 'rootEntity', 'protectedObject')) {
        $entity = Get-Prop $PhysicalObject @($propName)
        $serverName = Get-Prop $entity @('name', 'displayName', 'hostName', 'sourceName')
        if ($null -ne $serverName) { return [string]$serverName }
    }

    return ''
}

function New-PhysicalInventoryRow {
    param(
        [object]$Cluster,
        [object]$Pg,
        [object]$PhysicalObject,
        [int]$PGObjectCount
    )

    $physicalParams = Get-Prop $Pg @('physicalParams')
    $lastRun = Get-PGLastRunInfo $Pg

    $globalSkip = @(
        'objects', 'sourceParamsList', 'sources', 'protectionSources', 'physicalSources',
        'objectParams', 'objectParamsList', 'entities', 'protectedObjects'
    )

    $selectionSkip = @(
        'excludePaths', 'excludedPaths', 'exclusionPaths',
        'globalExcludePaths', 'globalExcludedPaths', 'globalExclusionPaths'
    )

    $objectSelection = Join-NamedValues -Root $PhysicalObject -Names @(
        'includePaths', 'includedPaths', 'selectedPaths', 'filePaths', 'paths',
        'volumePaths', 'selectedVolumes', 'volumes', 'includeVolumes', 'includedVolumes'
    ) -SkipPropertyNames $selectionSkip -MaxDepth 5

    $objectExcludePaths = Join-NamedValues -Root $PhysicalObject -Names @(
        'excludePaths', 'excludedPaths', 'exclusionPaths'
    ) -MaxDepth 5

    $globalExcludePaths = Join-NamedValues -Root $physicalParams -Names @(
        'globalExcludePaths', 'globalExcludedPaths', 'globalExclusionPaths',
        'excludePaths', 'excludedPaths', 'exclusionPaths'
    ) -SkipPropertyNames $globalSkip -MaxDepth 6

    $directiveFile = Join-NamedValues -Root $PhysicalObject -Names @(
        'directiveFile', 'directiveFilePath', 'directivePath', 'directiveFileName'
    ) -MaxDepth 5
    if ([string]::IsNullOrWhiteSpace($directiveFile)) {
        $directiveFile = Join-NamedValues -Root $physicalParams -Names @(
            'directiveFile', 'directiveFilePath', 'directivePath', 'directiveFileName'
        ) -SkipPropertyNames $globalSkip -MaxDepth 6
    }

    [pscustomobject]@{
        Cluster             = $Cluster.ClusterName
        Environment         = 'kPhysical'
        PGName              = [string](Get-Prop $Pg @('name', 'protectionGroupName'))
        PolicyName          = [string](Get-Prop $Pg @('policyName'))
        ProtectionType      = [string](Get-Prop $physicalParams @('protectionType'))
        PGObjectCount       = $PGObjectCount
        ServerName          = Get-ServerName $PhysicalObject
        ObjectSelection     = $objectSelection
        ObjectExcludePaths  = $objectExcludePaths
        GlobalExcludePaths  = $globalExcludePaths
        DirectiveFile       = $directiveFile
        IsActive            = Get-BooleanText (Get-Prop $Pg @('isActive'))
        IsPaused            = Get-BooleanText (Get-Prop $Pg @('isPaused'))
        LastRunStatus       = $lastRun.Status
        LastRunEndET        = $lastRun.EndET
    }
}

function Get-PhysicalPGInventoryForCluster {
    param(
        [object]$Cluster,
        [string]$HeliosUrl,
        [hashtable]$BaseHeaders
    )

    $headers = @{}
    foreach ($k in $BaseHeaders.Keys) { $headers[$k] = $BaseHeaders[$k] }
    $headers['accessClusterId'] = $Cluster.ClusterId

    $uri = "$HeliosUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true"
    $resp = Invoke-CohesityGet -Uri $uri -Headers $headers
    $pgs = Get-ArrayFromResponse -Response $resp -CandidateProperties @('protectionGroups', 'data', 'items')

    foreach ($pg in $pgs) {
        if (-not (Test-Truthy (Get-Prop $pg @('isActive')))) { continue }

        $objects = @(Get-PhysicalObjectCandidates $pg)
        $fallbackCount = Get-Prop $pg @('objectCount', 'numObjects', 'numProtectedObjects', 'protectedObjectCount')
        if ($objects.Count -gt 0) { $pgObjectCount = $objects.Count }
        elseif ($null -ne $fallbackCount -and [string]$fallbackCount -match '^\d+$') { $pgObjectCount = [int]$fallbackCount }
        else { $pgObjectCount = 0 }

        if ($objects.Count -eq 0) {
            New-PhysicalInventoryRow -Cluster $Cluster -Pg $pg -PhysicalObject $null -PGObjectCount $pgObjectCount
        } else {
            foreach ($obj in $objects) {
                New-PhysicalInventoryRow -Cluster $Cluster -Pg $pg -PhysicalObject $obj -PGObjectCount $pgObjectCount
            }
        }
    }
}

# Main
if (-not (Test-Path $ApiKeyPath)) { throw "API key file not found: $ApiKeyPath" }
$apiKey = (Get-Content $ApiKeyPath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "API key file is empty: $ApiKeyPath" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

$baseHeaders = @{
    accept = 'application/json'
    apiKey = $apiKey
}

$clusters = @(Get-ClusterInventory -HeliosUrl $HeliosUrl -BaseHeaders $baseHeaders)
if ($clusters.Count -eq 0) { throw 'No clusters returned from Helios.' }

$selectedClusters = @(Select-Clusters -Clusters $clusters)

$rows = foreach ($cluster in $selectedClusters) {
    Write-Host ("Collecting ACTIVE Physical PG inventory from {0}..." -f $cluster.ClusterName) -ForegroundColor Yellow
    Get-PhysicalPGInventoryForCluster -Cluster $cluster -HeliosUrl $HeliosUrl -BaseHeaders $baseHeaders
}

$rows = @($rows | Sort-Object Cluster, PGName, ServerName)
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$csvPath = Join-Path $OutputRoot "Physical_PG_Inventory_Active_$timestamp.csv"

$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host ''
Write-Host "Rows: $($rows.Count)" -ForegroundColor Green
Write-Host "CSV : $csvPath" -ForegroundColor Green

$rows |
    Format-Table Cluster, PGName, PGObjectCount, ServerName, ObjectSelection, ObjectExcludePaths, GlobalExcludePaths -AutoSize

if (-not $NoGridView) {
    try {
        $rows | Out-GridView -Title 'Cohesity ACTIVE Physical PG Inventory'
    } catch {
        Write-Warning 'Out-GridView failed or is unavailable. CSV and console output were still created.'
    }
}
