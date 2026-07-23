# Cohesity workload inventory validation
# GET-only. Windows PowerShell 5.1 compatible.
# Paused PGs are counted; only active PGs contribute protected objects.

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$HelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

if (-not (Test-Path -LiteralPath $HelperPath -PathType Leaf)) {
    throw "API key helper not found: $HelperPath"
}
if (-not (Test-Path -LiteralPath $EncryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key not found: $EncryptedApiKeyPath"
}

. $HelperPath
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'AES API key helper returned an empty API key.'
}

# Required output order.
$Workloads = @(
    [pscustomobject]@{ Name = 'Hyper-V';     Environments = @('kHyperV');               Kind = 'Standard'; Params = @('hypervParams','hyperVParams') },
    [pscustomobject]@{ Name = 'Nutanix AHV'; Environments = @('kAcropolis');            Kind = 'Standard'; Params = @('acropolisParams','nutanixParams','ahvParams') },
    [pscustomobject]@{ Name = 'NAS';         Environments = @('kGenericNas','kIsilon'); Kind = 'Standard'; Params = @('genericNasParams','nasParams','isilonParams') },
    [pscustomobject]@{ Name = 'Physical';    Environments = @('kPhysical');             Kind = 'Physical'; Params = @('physicalParams') },
    [pscustomobject]@{ Name = 'SQL';         Environments = @('kSQL');                  Kind = 'SQL';      Params = @('mssqlParams','sqlParams') },
    [pscustomobject]@{ Name = 'Oracle';      Environments = @('kOracle');               Kind = 'Oracle';   Params = @('oracleParams') }
)

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Get-Val {
    param($Object, [string[]]$Names, $Default = $null)

    if ($null -eq $Object -or $Object -is [string]) { return $Default }
    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                if ($null -ne $property.Value) { return $property.Value }
                return $Default
            }
        }
    }
    return $Default
}

function First-Text {
    param($Values)

    foreach ($value in @($Values)) {
        foreach ($item in @($value)) {
            if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) {
                return ([string]$item).Trim()
            }
        }
    }
    return ''
}

function As-Bool {
    param($Value, [bool]$Default = $false)

    if ($Value -is [bool]) { return $Value }
    if ($null -eq $Value) { return $Default }
    if (([string]$Value).Trim() -match '^(?i:true|1|yes)$') { return $true }
    if (([string]$Value).Trim() -match '^(?i:false|0|no)$') { return $false }
    return $Default
}

function New-Headers {
    param([string]$ClusterId)

    $headers = @{ accept = 'application/json'; apiKey = $ApiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers.accessClusterId = $ClusterId
    }
    return $headers
}

function Get-CohesityJson {
    param([string]$Uri, [hashtable]$Headers)

    $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get `
        -UseBasicParsing -TimeoutSec 90 -ErrorAction Stop
    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }
    return ($response.Content | ConvertFrom-Json)
}

function Get-ProtectionGroups {
    param([string]$Environment, [hashtable]$Headers)

    $groups = @()
    $cookie = ''
    $seenCookies = @{}

    do {
        $uri = "$BaseUrl/v2/data-protect/protection-groups?environments=$([uri]::EscapeDataString($Environment))&isDeleted=false&includeLastRunInfo=false&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-CohesityJson -Uri $uri -Headers $Headers
        foreach ($group in @(As-Array (Get-Val $json @('protectionGroups','items','data')))) {
            if ($null -ne $group) { $groups += $group }
        }

        $cookie = First-Text @((Get-Val $json @('paginationCookie')))
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            if ($seenCookies.ContainsKey($cookie)) {
                throw "Repeated pagination cookie returned for environment $Environment."
            }
            $seenCookies[$cookie] = $true
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($groups)
}

function Get-PgId {
    param($ProtectionGroup)
    return First-Text @(
        (Get-Val $ProtectionGroup @('id','protectionGroupId')),
        (Get-Val $ProtectionGroup @('name','protectionGroupName'))
    )
}

function Get-PgName {
    param($ProtectionGroup)
    return First-Text @(
        (Get-Val $ProtectionGroup @('name','protectionGroupName')),
        (Get-PgId $ProtectionGroup)
    )
}

function Unwrap-Object {
    param($Candidate)
    if ($null -eq $Candidate) { return $null }
    $nested = Get-Val $Candidate @('object')
    if ($null -ne $nested) { return $nested }
    return $Candidate
}

function Get-ObjectKey {
    param($Candidate, [string]$Workload)

    $object = Unwrap-Object $Candidate
    if ($null -eq $object) { return '' }

    $name = First-Text @(
        (Get-Val $object @('databaseUniqueName','databaseName','dbName','name','objectName','displayName','hostName','sourceName'))
    )
    $id = First-Text @(
        (Get-Val $object @('id','objectId','databaseId','databaseUuid','entityId','uuid'))
    )
    $sourceId = First-Text @((Get-Val $object @('sourceId','parentId','rootNodeId')))

    if ([string]::IsNullOrWhiteSpace($name) -and [string]::IsNullOrWhiteSpace($id)) {
        return ''
    }

    return ('{0}|{1}|{2}|{3}' -f $Workload,$sourceId,(First-Text @($id,$name)),$name).ToLowerInvariant()
}

function Get-StandardObjects {
    param($ProtectionGroup, $Workload)

    $objects = @()
    foreach ($parameterName in $Workload.Params) {
        $parameters = Get-Val $ProtectionGroup @($parameterName)
        foreach ($object in @(As-Array (Get-Val $parameters @('objects')))) {
            if ($null -ne $object) { $objects += $object }
        }
    }
    return @($objects)
}

function Get-OracleDatabaseObjects {
    param($ProtectionGroup)

    # Proven Oracle inventory path:
    # oracleParams.objects[].dbParams.dbChannels[].databaseUniqueName
    $databases = @()
    $oracleParams = Get-Val $ProtectionGroup @('oracleParams')

    foreach ($oracleObject in @(As-Array (Get-Val $oracleParams @('objects')))) {
        $dbParams = Get-Val $oracleObject @('dbParams')
        foreach ($channel in @(As-Array (Get-Val $dbParams @('dbChannels')))) {
            $dbName = First-Text @((Get-Val $channel @('databaseUniqueName')))
            if (-not [string]::IsNullOrWhiteSpace($dbName)) {
                $databases += $channel
            }
        }
    }
    return @($databases)
}

function Is-SqlDatabase {
    param($Candidate)

    $object = Unwrap-Object $Candidate
    if ($null -eq $object) { return $false }

    $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
    $dbName = First-Text @((Get-Val $object @('databaseName','dbName')))

    if ($type -match '(?i)host|server|instance|source|cluster') { return $false }
    if ($type -ieq 'kSQL' -or $type -match '(?i)database') { return $true }
    return (-not [string]::IsNullOrWhiteSpace($dbName))
}

function Get-RunObjects {
    param(
        [string]$ProtectionGroupId,
        [string]$Environment,
        [hashtable]$Headers
    )

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) { return @() }

    $uri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?environments=$([uri]::EscapeDataString($Environment))&numRuns=3&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json = Get-CohesityJson -Uri $uri -Headers $Headers
    $runs = @(As-Array (Get-Val $json @('runs')))

    $runs = @($runs | Sort-Object {
        $maxEnd = 0
        foreach ($localInfo in @(As-Array (Get-Val $_ @('localBackupInfo')))) {
            $end = [int64](Get-Val $localInfo @('endTimeUsecs','startTimeUsecs') 0)
            if ($end -gt $maxEnd) { $maxEnd = $end }
        }
        $maxEnd
    } -Descending)

    foreach ($run in $runs) {
        $objects = @(As-Array (Get-Val $run @('objects')))
        if ($objects.Count -gt 0) { return $objects }
    }
    return @()
}

function Get-SqlDatabaseObjects {
    param($ProtectionGroup, [hashtable]$Headers)

    $databases = @()
    foreach ($parameterName in @('mssqlParams','sqlParams')) {
        $parameters = Get-Val $ProtectionGroup @($parameterName)

        foreach ($candidate in @(As-Array (Get-Val $parameters @('objects')))) {
            if (Is-SqlDatabase $candidate) { $databases += $candidate }

            $object = Unwrap-Object $candidate
            foreach ($collectionName in @('databases','databaseList','databaseObjects','dbObjects','dbList')) {
                foreach ($database in @(As-Array (Get-Val $object @($collectionName)))) {
                    if (Is-SqlDatabase $database) { $databases += $database }
                }
            }
        }

        foreach ($collectionName in @('databases','databaseList','databaseObjects','dbObjects','dbList')) {
            foreach ($database in @(As-Array (Get-Val $parameters @($collectionName)))) {
                if (Is-SqlDatabase $database) { $databases += $database }
            }
        }
    }

    # Proven SQL run logic fallback: kSQL rows are DBs; kPhysical rows are hosts.
    if ($databases.Count -eq 0) {
        $pgId = Get-PgId $ProtectionGroup
        try {
            foreach ($candidate in @(Get-RunObjects -ProtectionGroupId $pgId -Environment 'kSQL' -Headers $Headers)) {
                $type = First-Text @((Get-Val (Unwrap-Object $candidate) @('objectType','type','entityType')))
                if ($type -ieq 'kSQL') { $databases += $candidate }
            }
        }
        catch {
            # The caller will report zero DB records if configuration also returned none.
        }
    }

    return @($databases)
}

function Is-PhysicalHost {
    param($Candidate, [bool]$AllowUntyped = $false)

    $object = Unwrap-Object $Candidate
    if ($null -eq $object) { return $false }

    $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
    $environment = First-Text @((Get-Val $object @('environment','environmentType')))

    if ($type -ieq 'kHost' -and ($environment -ieq 'kPhysical' -or [string]::IsNullOrWhiteSpace($environment))) {
        return $true
    }
    if ($environment -ieq 'kPhysical' -and -not ($type -match '(?i)file|folder|volume')) {
        return $true
    }
    if ($AllowUntyped -and [string]::IsNullOrWhiteSpace($type) -and [string]::IsNullOrWhiteSpace($environment)) {
        return $true
    }
    return $false
}

function Get-PhysicalHostObjects {
    param($ProtectionGroup, [hashtable]$Headers)

    $hosts = @()
    $pgId = Get-PgId $ProtectionGroup

    # Proven Physical logic: run.objects where objectType=kHost and environment=kPhysical.
    try {
        foreach ($candidate in @(Get-RunObjects -ProtectionGroupId $pgId -Environment 'kPhysical' -Headers $Headers)) {
            if (Is-PhysicalHost $candidate) { $hosts += $candidate }
        }
    }
    catch {
        # Continue with PG configuration so an unavailable run endpoint does not lose inventory.
    }

    # PG-configuration fallback also covers a new active PG with no completed run.
    $physicalParams = Get-Val $ProtectionGroup @('physicalParams')
    foreach ($parameterName in @('fileProtectionTypeParams','volumeProtectionTypeParams')) {
        $typeParams = Get-Val $physicalParams @($parameterName)
        foreach ($candidate in @(As-Array (Get-Val $typeParams @('objects')))) {
            if (Is-PhysicalHost $candidate $true) { $hosts += $candidate }
        }
    }
    foreach ($candidate in @(As-Array (Get-Val $physicalParams @('objects')))) {
        if (Is-PhysicalHost $candidate $true) { $hosts += $candidate }
    }

    return @($hosts)
}

$clusterJson = Get-CohesityJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$clusters = @()
foreach ($cluster in @(As-Array (Get-Val $clusterJson @('cohesityClusters','clusters','items')))) {
    $clusterId = First-Text @((Get-Val $cluster @('clusterId','id')))
    if ([string]::IsNullOrWhiteSpace($clusterId)) { continue }

    $clusters += [pscustomobject]@{
        ClusterId = $clusterId
        ClusterName = First-Text @(
            (Get-Val $cluster @('clusterName','displayName','name')),
            "Unknown-$clusterId"
        )
    }
}
$clusters = @($clusters | Sort-Object ClusterName)
if ($clusters.Count -eq 0) { throw 'No clusters returned from Helios.' }

$clusterMenu = @()
for ($i = 0; $i -lt $clusters.Count; $i++) {
    $clusterMenu += [pscustomobject]@{
        Index = $i + 1
        ClusterName = $clusters[$i].ClusterName
        ClusterId = $clusters[$i].ClusterId
    }
}

Write-Host ''
Write-Host 'Available Helios clusters:' -ForegroundColor Cyan
$clusterMenu | Format-Table Index,ClusterName -AutoSize
Write-Host '[0] All clusters' -ForegroundColor Yellow
Write-Host '[X] Exit' -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host 'Select cluster'
    if ($selection -match '^(?i:x|q)$') { return }

    $number = -1
    if ([int]::TryParse($selection,[ref]$number) -and $number -ge 0 -and $number -le $clusterMenu.Count) {
        if ($number -eq 0) { $selectedClusters = @($clusterMenu) }
        else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $number }) }
        break
    }
    Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
}

$summary = @()
$warnings = @()

foreach ($workload in $Workloads) {
    $activeCount = 0
    $pausedCount = 0
    $protectedObjects = @{}

    foreach ($cluster in $selectedClusters) {
        $headers = New-Headers -ClusterId $cluster.ClusterId
        $allGroups = @()

        foreach ($environment in $workload.Environments) {
            try {
                $allGroups += @(Get-ProtectionGroups -Environment $environment -Headers $headers)
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    ProtectionGroup = ''
                    Warning = "PG GET failed for $environment`: $($_.Exception.Message)"
                }
            }
        }

        $seenGroups = @{}
        foreach ($group in $allGroups) {
            $pgId = Get-PgId $group
            if ([string]::IsNullOrWhiteSpace($pgId)) { continue }

            $pgKey = ('{0}|{1}' -f $cluster.ClusterId,$pgId).ToLowerInvariant()
            if ($seenGroups.ContainsKey($pgKey)) { continue }
            $seenGroups[$pgKey] = $true

            $isPaused = As-Bool (Get-Val $group @('isPaused','paused')) $false
            $isActive = As-Bool (Get-Val $group @('isActive','active')) $true

            if ($isPaused) {
                $pausedCount++
                continue
            }
            if (-not $isActive) { continue }

            $activeCount++
            $objects = @()

            try {
                switch ($workload.Kind) {
                    'Oracle'  { $objects = @(Get-OracleDatabaseObjects $group) }
                    'SQL'     { $objects = @(Get-SqlDatabaseObjects $group $headers) }
                    'Physical'{ $objects = @(Get-PhysicalHostObjects $group $headers) }
                    default   { $objects = @(Get-StandardObjects $group $workload) }
                }
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    ProtectionGroup = (Get-PgName $group)
                    Warning = "Object GET/processing failed: $($_.Exception.Message)"
                }
                continue
            }

            $foundInPg = 0
            foreach ($candidate in $objects) {
                $objectKey = Get-ObjectKey $candidate $workload.Name
                if ([string]::IsNullOrWhiteSpace($objectKey)) { continue }
                $foundInPg++
                $protectedObjects[("$($cluster.ClusterId)|$objectKey").ToLowerInvariant()] = $true
            }

            if ($foundInPg -eq 0) {
                $warningText = 'No protected object records found in this active PG.'
                if ($workload.Kind -eq 'SQL' -or $workload.Kind -eq 'Oracle') {
                    $warningText = 'No database records found; server, host and instance records were excluded.'
                }
                elseif ($workload.Kind -eq 'Physical') {
                    $warningText = 'No kPhysical/kHost records found in run details or PG configuration.'
                }

                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    ProtectionGroup = (Get-PgName $group)
                    Warning = $warningText
                }
            }
        }
    }

    $summary += [pscustomobject][ordered]@{
        Workload = $workload.Name
        'Active PGs' = $activeCount
        'Paused PGs' = $pausedCount
        'Protected Objects' = $protectedObjects.Count
    }
}

$script:InventorySummary = @($summary)
$script:InventoryWarnings = @($warnings | Sort-Object Cluster,Workload,ProtectionGroup)

Write-Host ''
Write-Host 'WORKLOAD INVENTORY SUMMARY' -ForegroundColor Cyan
$script:InventorySummary | Format-Table 'Workload','Active PGs','Paused PGs','Protected Objects' -AutoSize

if ($script:InventoryWarnings.Count -gt 0) {
    Write-Host ''
    Write-Host 'COLLECTION WARNINGS' -ForegroundColor Yellow
    $script:InventoryWarnings | Format-Table Cluster,Workload,ProtectionGroup,Warning -Wrap -AutoSize
}
