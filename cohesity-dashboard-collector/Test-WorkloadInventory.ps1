# Cohesity workload inventory validation
# GET-only. Windows PowerShell 5.1 compatible.
# Paused PGs are counted separately; only active PGs contribute protected objects.

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

$Workloads = @(
    [pscustomobject]@{ Name = 'Hyper-V';     Environments = @('kHyperV');                Kind = 'HyperV';   Params = @('hypervParams','hyperVParams') },
    [pscustomobject]@{ Name = 'Nutanix AHV'; Environments = @('kAcropolis');             Kind = 'Nutanix';  Params = @('acropolisParams','nutanixParams','ahvParams') },
    [pscustomobject]@{ Name = 'NAS';         Environments = @('kGenericNas','kIsilon');  Kind = 'NAS';      Params = @('genericNasParams','nasParams','isilonParams') },
    [pscustomobject]@{ Name = 'Physical';    Environments = @('kPhysical');              Kind = 'Physical'; Params = @('physicalParams') },
    [pscustomobject]@{ Name = 'SQL';         Environments = @('kSQL');                   Kind = 'SQL';      Params = @('mssqlParams','sqlParams') },
    [pscustomobject]@{ Name = 'Oracle';      Environments = @('kOracle');                Kind = 'Oracle';   Params = @('oracleParams') }
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
        -UseBasicParsing -TimeoutSec 120 -ErrorAction Stop
    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }
    return ($response.Content | ConvertFrom-Json)
}

function Get-ProtectionGroups {
    param(
        [string]$Environment,
        [ValidateSet('Active','Paused')][string]$State,
        [hashtable]$Headers
    )

    $groups = @()
    $cookie = ''
    $seenCookies = @{}

    do {
        $uri = "$BaseUrl/v2/data-protect/protection-groups?environments=$([uri]::EscapeDataString($Environment))&isDeleted=false&maxResultCount=1000"
        if ($State -eq 'Active') {
            $uri += '&isPaused=false&isActive=true&includeLastRunInfo=true'
        }
        else {
            $uri += '&isPaused=true&includeLastRunInfo=false'
        }
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
                throw "Repeated protection-group pagination cookie for $Environment/$State."
            }
            $seenCookies[$cookie] = $true
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($groups)
}

function Get-UnprotectedSearchObjects {
    param([string]$Environment, [hashtable]$Headers)

    $objects = @()
    $cookie = ''
    $seenCookies = @{}

    do {
        $uri = "$BaseUrl/v2/data-protect/search/objects?environments=$([uri]::EscapeDataString($Environment))&isProtected=false&isDeleted=false&count=1000&fetchConsistentSortedOrder=true"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-CohesityJson -Uri $uri -Headers $Headers
        foreach ($object in @(As-Array (Get-Val $json @('objects','items','data')))) {
            if ($null -ne $object) { $objects += $object }
        }

        $cookie = First-Text @((Get-Val $json @('paginationCookie')))
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            if ($seenCookies.ContainsKey($cookie)) {
                throw "Repeated object-search pagination cookie for $Environment."
            }
            $seenCookies[$cookie] = $true
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($objects)
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
        (Get-Val $object @('id','objectId','databaseId','databaseUuid','entityId','uuid','globalId'))
    )
    $sourceId = First-Text @((Get-Val $object @('sourceId','parentId','rootNodeId')))

    if ([string]::IsNullOrWhiteSpace($name) -and [string]::IsNullOrWhiteSpace($id)) {
        return ''
    }

    if ($Workload -eq 'Oracle' -and -not [string]::IsNullOrWhiteSpace($name)) {
        return ("oracle|$name").ToLowerInvariant()
    }

    return ('{0}|{1}|{2}|{3}' -f $Workload,$sourceId,(First-Text @($id,$name)),$name).ToLowerInvariant()
}

function Get-ConfiguredObjects {
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

function Get-RecentRuns {
    param(
        [string]$ProtectionGroupId,
        [string]$Environment,
        [hashtable]$Headers
    )

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) { return @() }
    $uri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?environments=$([uri]::EscapeDataString($Environment))&numRuns=10&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json = Get-CohesityJson -Uri $uri -Headers $Headers
    return @(As-Array (Get-Val $json @('runs')))
}

function Get-LatestRunObjects {
    param(
        [string]$ProtectionGroupId,
        [string]$Environment,
        [hashtable]$Headers
    )

    $runs = @(Get-RecentRuns -ProtectionGroupId $ProtectionGroupId -Environment $Environment -Headers $Headers)
    if ($runs.Count -eq 0) { return @() }

    $latestRun = $runs | Sort-Object {
        $latestEnd = 0
        foreach ($localInfo in @(As-Array (Get-Val $_ @('localBackupInfo')))) {
            $end = [int64](Get-Val $localInfo @('endTimeUsecs','startTimeUsecs') 0)
            if ($end -gt $latestEnd) { $latestEnd = $end }
        }
        $latestEnd
    } -Descending | Select-Object -First 1

    if ($null -eq $latestRun) { return @() }
    return @(As-Array (Get-Val $latestRun @('objects')))
}

function Get-LatestRunObjectsPerType {
    param(
        [string]$ProtectionGroupId,
        [string]$Environment,
        [hashtable]$Headers
    )

    # Matches the proven SQL backup-failure logic: identify every run type,
    # select the latest run for each type, then inspect that run's objects.
    $runs = @(Get-RecentRuns -ProtectionGroupId $ProtectionGroupId -Environment $Environment -Headers $Headers)
    if ($runs.Count -eq 0) { return @() }

    $runRecords = @()
    foreach ($run in $runs) {
        foreach ($localInfo in @(As-Array (Get-Val $run @('localBackupInfo')))) {
            $runType = First-Text @((Get-Val $localInfo @('runType')))
            if ([string]::IsNullOrWhiteSpace($runType)) { continue }
            $runRecords += [pscustomobject]@{
                Run = $run
                RunType = $runType
                EndTimeUsecs = [int64](Get-Val $localInfo @('endTimeUsecs','startTimeUsecs') 0)
            }
        }
    }

    $objects = @()
    foreach ($group in @($runRecords | Group-Object RunType)) {
        $latest = $group.Group | Sort-Object EndTimeUsecs -Descending | Select-Object -First 1
        if ($null -eq $latest) { continue }
        foreach ($candidate in @(As-Array (Get-Val $latest.Run @('objects')))) {
            if ($null -ne $candidate) { $objects += $candidate }
        }
    }
    return @($objects)
}

function Get-HyperVObjects {
    param($ProtectionGroup, $Workload, [hashtable]$Headers)

    $objects = @(Get-ConfiguredObjects -ProtectionGroup $ProtectionGroup -Workload $Workload)
    if ($objects.Count -gt 0) { return $objects }

    $result = @()
    foreach ($candidate in @(Get-LatestRunObjects -ProtectionGroupId (Get-PgId $ProtectionGroup) -Environment 'kHyperV' -Headers $Headers)) {
        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))
        if ($type -ieq 'kVirtualMachine' -and $environment -ieq 'kHyperV') {
            $result += $candidate
        }
    }
    return @($result)
}

function Get-NutanixObjects {
    param($ProtectionGroup, $Workload, [hashtable]$Headers)

    $result = @()
    foreach ($candidate in @(Get-LatestRunObjects -ProtectionGroupId (Get-PgId $ProtectionGroup) -Environment 'kAcropolis' -Headers $Headers)) {
        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))
        if ($type -ieq 'kVirtualMachine' -and $environment -ieq 'kAcropolis') {
            $result += $candidate
        }
    }

    if ($result.Count -eq 0) {
        $result = @(Get-ConfiguredObjects -ProtectionGroup $ProtectionGroup -Workload $Workload)
    }
    return @($result)
}

function Get-NasObjects {
    param($ProtectionGroup, $Workload)
    return @(Get-ConfiguredObjects -ProtectionGroup $ProtectionGroup -Workload $Workload)
}

function Get-SqlDatabaseObjects {
    param($ProtectionGroup, [hashtable]$Headers)

    # SQL is not parsed like Oracle. The proven SQL backup-failure script
    # evaluates the latest run for each run type and counts objectType=kSQL.
    # kPhysical host rows are deliberately excluded.
    $databases = @()
    foreach ($candidate in @(Get-LatestRunObjectsPerType -ProtectionGroupId (Get-PgId $ProtectionGroup) -Environment 'kSQL' -Headers $Headers)) {
        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
        if ($type -ieq 'kSQL') {
            $databases += $candidate
        }
    }
    return @($databases)
}

function Get-OracleDatabaseObjects {
    param($ProtectionGroup)

    # Proven Oracle inventory/configuration path:
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

function Get-PhysicalObjects {
    param($ProtectionGroup, [hashtable]$Headers)

    $hosts = @()
    foreach ($candidate in @(Get-LatestRunObjectsPerType -ProtectionGroupId (Get-PgId $ProtectionGroup) -Environment 'kPhysical' -Headers $Headers)) {
        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))
        if ($type -ieq 'kHost' -and $environment -ieq 'kPhysical') {
            $hosts += $candidate
        }
    }

    if ($hosts.Count -eq 0) {
        $physicalParams = Get-Val $ProtectionGroup @('physicalParams')
        foreach ($parameterName in @('fileProtectionTypeParams','volumeProtectionTypeParams')) {
            $typeParams = Get-Val $physicalParams @($parameterName)
            foreach ($candidate in @(As-Array (Get-Val $typeParams @('objects')))) {
                if ($null -ne $candidate) { $hosts += $candidate }
            }
        }
        foreach ($candidate in @(As-Array (Get-Val $physicalParams @('objects')))) {
            if ($null -ne $candidate) { $hosts += $candidate }
        }
    }
    return @($hosts)
}

function Test-UnprotectedObjectForWorkload {
    param($Candidate, $Workload)

    $object = Unwrap-Object $Candidate
    if ($null -eq $object) { return $false }

    $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
    $environment = First-Text @((Get-Val $object @('environment','environmentType')))

    if (-not [string]::IsNullOrWhiteSpace($environment) -and
        $Workload.Environments -notcontains $environment) {
        return $false
    }

    if ([string]::IsNullOrWhiteSpace($type)) { return $true }

    switch ($Workload.Kind) {
        'HyperV'   { return ($type -ieq 'kVirtualMachine') }
        'Nutanix'  { return ($type -ieq 'kVirtualMachine') }
        'Physical' { return ($type -ieq 'kHost') }
        'SQL'      { return ($type -ieq 'kSQL' -or $type -match '(?i)database') }
        'Oracle'   { return ($type -ieq 'kDatabase' -or $type -ieq 'kOracleDatabase') }
        'NAS'      { return ($type -match '(?i)volume|mountpoint|file.?system|fileset|share|view') }
    }
    return $false
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
    $unprotectedObjects = @{}
    $unprotectedComplete = $true

    foreach ($cluster in $selectedClusters) {
        $headers = New-Headers -ClusterId $cluster.ClusterId
        $activeGroups = @()
        $pausedGroups = @()

        foreach ($environment in $workload.Environments) {
            try {
                $activeGroups += @(Get-ProtectionGroups -Environment $environment -State Active -Headers $headers)
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Active PG GET'
                    Warning = "$environment`: $($_.Exception.Message)"
                }
            }

            try {
                $pausedGroups += @(Get-ProtectionGroups -Environment $environment -State Paused -Headers $headers)
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Paused PG GET'
                    Warning = "$environment`: $($_.Exception.Message)"
                }
            }

            try {
                foreach ($candidate in @(Get-UnprotectedSearchObjects -Environment $environment -Headers $headers)) {
                    if (-not (Test-UnprotectedObjectForWorkload -Candidate $candidate -Workload $workload)) { continue }
                    $key = Get-ObjectKey -Candidate $candidate -Workload $workload.Name
                    if (-not [string]::IsNullOrWhiteSpace($key)) {
                        $unprotectedObjects[("$($cluster.ClusterId)|$key").ToLowerInvariant()] = $true
                    }
                }
            }
            catch {
                $unprotectedComplete = $false
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Unprotected object GET'
                    Warning = "$environment`: $($_.Exception.Message)"
                }
            }
        }

        $seenActiveGroups = @{}
        foreach ($group in $activeGroups) {
            $pgId = Get-PgId $group
            if ([string]::IsNullOrWhiteSpace($pgId)) { continue }
            $pgKey = ("$($cluster.ClusterId)|$pgId").ToLowerInvariant()
            if ($seenActiveGroups.ContainsKey($pgKey)) { continue }
            $seenActiveGroups[$pgKey] = $true
            $activeCount++

            try {
                $objects = @()
                switch ($workload.Kind) {
                    'HyperV'   { $objects = @(Get-HyperVObjects -ProtectionGroup $group -Workload $workload -Headers $headers) }
                    'Nutanix'  { $objects = @(Get-NutanixObjects -ProtectionGroup $group -Workload $workload -Headers $headers) }
                    'NAS'      { $objects = @(Get-NasObjects -ProtectionGroup $group -Workload $workload) }
                    'Physical' { $objects = @(Get-PhysicalObjects -ProtectionGroup $group -Headers $headers) }
                    'SQL'      { $objects = @(Get-SqlDatabaseObjects -ProtectionGroup $group -Headers $headers) }
                    'Oracle'   { $objects = @(Get-OracleDatabaseObjects -ProtectionGroup $group) }
                }

                foreach ($candidate in $objects) {
                    $key = Get-ObjectKey -Candidate $candidate -Workload $workload.Name
                    if (-not [string]::IsNullOrWhiteSpace($key)) {
                        $protectedObjects[("$($cluster.ClusterId)|$key").ToLowerInvariant()] = $true
                    }
                }
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Protected object collection'
                    Warning = "$(Get-PgName $group): $($_.Exception.Message)"
                }
            }
        }

        $seenPausedGroups = @{}
        foreach ($group in $pausedGroups) {
            $pgId = Get-PgId $group
            if ([string]::IsNullOrWhiteSpace($pgId)) { continue }
            $pgKey = ("$($cluster.ClusterId)|$pgId").ToLowerInvariant()
            if ($seenPausedGroups.ContainsKey($pgKey)) { continue }
            $seenPausedGroups[$pgKey] = $true
            $pausedCount++
        }
    }

    $unprotectedDisplay = $unprotectedObjects.Count
    if (-not $unprotectedComplete) { $unprotectedDisplay = 'N/A' }

    $summary += [pscustomobject][ordered]@{
        Workload = $workload.Name
        'Active PGs' = $activeCount
        'Paused PGs' = $pausedCount
        'Protected Objects' = $protectedObjects.Count
        'Unprotected Objects' = $unprotectedDisplay
    }
}

$script:InventorySummary = @($summary)
$script:InventoryWarnings = @($warnings | Sort-Object Cluster,Workload,Operation)

Write-Host ''
Write-Host 'WORKLOAD INVENTORY SUMMARY' -ForegroundColor Cyan
$script:InventorySummary | Format-Table 'Workload','Active PGs','Paused PGs','Protected Objects','Unprotected Objects' -AutoSize

if ($script:InventoryWarnings.Count -gt 0) {
    Write-Host ''
    Write-Host 'COLLECTION WARNINGS (API failures only)' -ForegroundColor Yellow
    $script:InventoryWarnings | Format-Table Cluster,Workload,Operation,Warning -Wrap -AutoSize
}
