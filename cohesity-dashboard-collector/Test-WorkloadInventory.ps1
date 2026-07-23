# Cohesity workload inventory validation
# STRICTLY READ-ONLY / GET-only
# Windows PowerShell 5.1 compatible
#
# Protected Objects are counted at the workload-object level:
#   Hyper-V     : latest run per run type -> kVirtualMachine / kHyperV
#   Nutanix AHV : latest run per run type -> kVirtualMachine / kAcropolis
#   NAS         : latest run per run type -> kGenericNas or kIsilon objects
#   Physical    : active PG configuration -> file/volume protection objects
#   SQL         : latest run per run type -> kDatabase / kSQL
#   Oracle      : latest run per run type -> kDatabase; dbChannels fallback
#
# Paused PGs are counted separately and never contribute protected objects.
# Unprotected-object search is intentionally not used.

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
    [pscustomobject]@{ Name = 'Hyper-V';     Environments = @('kHyperV');               Kind = 'HyperV' },
    [pscustomobject]@{ Name = 'Nutanix AHV'; Environments = @('kAcropolis');            Kind = 'Nutanix' },
    [pscustomobject]@{ Name = 'NAS';         Environments = @('kGenericNas','kIsilon'); Kind = 'NAS' },
    [pscustomobject]@{ Name = 'Physical';    Environments = @('kPhysical');             Kind = 'Physical' },
    [pscustomobject]@{ Name = 'SQL';         Environments = @('kSQL');                  Kind = 'SQL' },
    [pscustomobject]@{ Name = 'Oracle';      Environments = @('kOracle');               Kind = 'Oracle' }
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

    $headers = @{
        accept = 'application/json'
        apiKey = $ApiKey
    }

    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers.accessClusterId = $ClusterId
    }

    return $headers
}

function Get-CohesityJson {
    param(
        [string]$Uri,
        [hashtable]$Headers
    )

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
            if ($null -ne $group) {
                $groups += $group
            }
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
    param(
        $Candidate,
        [string]$Workload
    )

    $object = Unwrap-Object $Candidate
    if ($null -eq $object) { return '' }

    $name = First-Text @(
        (Get-Val $object @(
            'databaseUniqueName','databaseName','dbName','name',
            'objectName','displayName','hostName','sourceName'
        ))
    )

    $id = First-Text @(
        (Get-Val $object @(
            'id','objectId','databaseId','databaseUuid',
            'entityId','uuid','globalId'
        ))
    )

    $sourceId = First-Text @(
        (Get-Val $object @('sourceId','parentId','rootNodeId'))
    )

    if ([string]::IsNullOrWhiteSpace($name) -and [string]::IsNullOrWhiteSpace($id)) {
        return ''
    }

    if ($Workload -eq 'Oracle' -and -not [string]::IsNullOrWhiteSpace($name)) {
        return ("oracle|$name").ToLowerInvariant()
    }

    return ('{0}|{1}|{2}|{3}' -f `
        $Workload,
        $sourceId,
        (First-Text @($id,$name)),
        $name
    ).ToLowerInvariant()
}

function Get-RecentRuns {
    param(
        [string]$ProtectionGroupId,
        [hashtable]$Headers
    )

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) {
        return @()
    }

    # Matches the working failure collector: the PG already identifies the
    # workload, so the runs request is not additionally filtered by environment.
    $uri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json = Get-CohesityJson -Uri $uri -Headers $Headers

    return @(As-Array (Get-Val $json @('runs')))
}

function Get-LatestRunObjectsPerType {
    param(
        [string]$ProtectionGroupId,
        [hashtable]$Headers
    )

    $runs = @(Get-RecentRuns -ProtectionGroupId $ProtectionGroupId -Headers $Headers)
    if ($runs.Count -eq 0) { return @() }

    $runRecords = @()

    foreach ($run in $runs) {
        $localInfoArray = @(As-Array (Get-Val $run @('localBackupInfo')))

        foreach ($localInfo in $localInfoArray) {
            $runType = First-Text @((Get-Val $localInfo @('runType')))
            if ([string]::IsNullOrWhiteSpace($runType)) {
                $runType = 'Unknown'
            }

            $runRecords += [pscustomobject]@{
                Run = $run
                RunType = $runType
                EndTimeUsecs = [int64](Get-Val $localInfo @('endTimeUsecs','startTimeUsecs') 0)
            }
        }

        if ($localInfoArray.Count -eq 0) {
            $runRecords += [pscustomobject]@{
                Run = $run
                RunType = First-Text @((Get-Val $run @('runType')), 'Unknown')
                EndTimeUsecs = [int64](Get-Val $run @('endTimeUsecs','startTimeUsecs') 0)
            }
        }
    }

    $objects = @()

    foreach ($runTypeGroup in @($runRecords | Group-Object RunType)) {
        $latest = $runTypeGroup.Group |
            Sort-Object EndTimeUsecs -Descending |
            Select-Object -First 1

        if ($null -eq $latest) { continue }

        foreach ($candidate in @(As-Array (Get-Val $latest.Run @('objects')))) {
            if ($null -ne $candidate) {
                $objects += $candidate
            }
        }
    }

    return @($objects)
}

function Get-HyperVObjects {
    param(
        $ProtectionGroup,
        [hashtable]$Headers
    )

    $result = @()

    foreach ($candidate in @(Get-LatestRunObjectsPerType `
        -ProtectionGroupId (Get-PgId $ProtectionGroup) `
        -Headers $Headers)) {

        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))

        if ($type -ieq 'kVirtualMachine' -and
            ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kHyperV')) {
            $result += $candidate
        }
    }

    return @($result)
}

function Get-NutanixObjects {
    param(
        $ProtectionGroup,
        [hashtable]$Headers
    )

    $result = @()

    foreach ($candidate in @(Get-LatestRunObjectsPerType `
        -ProtectionGroupId (Get-PgId $ProtectionGroup) `
        -Headers $Headers)) {

        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))

        if ($type -ieq 'kVirtualMachine' -and
            ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kAcropolis')) {
            $result += $candidate
        }
    }

    return @($result)
}

function Get-NasObjects {
    param(
        $ProtectionGroup,
        [hashtable]$Headers
    )

    $result = @()

    foreach ($candidate in @(Get-LatestRunObjectsPerType `
        -ProtectionGroupId (Get-PgId $ProtectionGroup) `
        -Headers $Headers)) {

        $object = Unwrap-Object $candidate
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))

        if ($environment -ieq 'kGenericNas' -or $environment -ieq 'kIsilon') {
            $result += $candidate
        }
    }

    return @($result)
}

function Get-PhysicalObjects {
    param($ProtectionGroup)

    $physical = Get-Val $ProtectionGroup @('physicalParams')
    if ($null -eq $physical) { return @() }

    $protectionType = First-Text @((Get-Val $physical @('protectionType')))

    if ($protectionType -ieq 'kVolume') {
        $volumeParams = Get-Val $physical @('volumeProtectionTypeParams')
        return @(As-Array (Get-Val $volumeParams @('objects')))
    }

    $fileParams = Get-Val $physical @('fileProtectionTypeParams')
    return @(As-Array (Get-Val $fileParams @('objects')))
}

function Get-SqlDatabaseObjects {
    param(
        $ProtectionGroup,
        [hashtable]$Headers
    )

    # The working failure collector defines SQL as:
    # environment=kSQL and objectType=kDatabase.
    $result = @()

    foreach ($candidate in @(Get-LatestRunObjectsPerType `
        -ProtectionGroupId (Get-PgId $ProtectionGroup) `
        -Headers $Headers)) {

        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))

        if ($type -ieq 'kDatabase' -and
            ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kSQL')) {
            $result += $candidate
        }
    }

    return @($result)
}

function Get-OracleDatabaseObjects {
    param(
        $ProtectionGroup,
        [hashtable]$Headers
    )

    $result = @()

    foreach ($candidate in @(Get-LatestRunObjectsPerType `
        -ProtectionGroupId (Get-PgId $ProtectionGroup) `
        -Headers $Headers)) {

        $object = Unwrap-Object $candidate
        $type = First-Text @((Get-Val $object @('objectType','type','entityType')))
        $environment = First-Text @((Get-Val $object @('environment','environmentType')))

        if ($type -ieq 'kDatabase' -and
            ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kOracle')) {
            $result += $candidate
        }
    }

    if ($result.Count -eq 0) {
        $oracleParams = Get-Val $ProtectionGroup @('oracleParams')

        foreach ($oracleObject in @(As-Array (Get-Val $oracleParams @('objects')))) {
            $dbParams = Get-Val $oracleObject @('dbParams')

            foreach ($channel in @(As-Array (Get-Val $dbParams @('dbChannels')))) {
                $databaseUniqueName = First-Text @(
                    (Get-Val $channel @('databaseUniqueName'))
                )

                if (-not [string]::IsNullOrWhiteSpace($databaseUniqueName)) {
                    $result += $channel
                }
            }
        }
    }

    return @($result)
}

$clusterJson = Get-CohesityJson `
    -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers (New-Headers)

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
if ($clusters.Count -eq 0) {
    throw 'No clusters returned from Helios.'
}

$clusterMenu = @()

for ($index = 0; $index -lt $clusters.Count; $index++) {
    $clusterMenu += [pscustomobject]@{
        Index = $index + 1
        ClusterName = $clusters[$index].ClusterName
        ClusterId = $clusters[$index].ClusterId
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

    if ([int]::TryParse($selection,[ref]$number) -and
        $number -ge 0 -and
        $number -le $clusterMenu.Count) {

        if ($number -eq 0) {
            $selectedClusters = @($clusterMenu)
        }
        else {
            $selectedClusters = @(
                $clusterMenu | Where-Object { $_.Index -eq $number }
            )
        }

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
    $protectedComplete = $true

    foreach ($cluster in $selectedClusters) {
        $headers = New-Headers -ClusterId $cluster.ClusterId
        $activeGroups = @()
        $pausedGroups = @()

        foreach ($environment in $workload.Environments) {
            try {
                $activeGroups += @(Get-ProtectionGroups `
                    -Environment $environment `
                    -State Active `
                    -Headers $headers)
            }
            catch {
                $protectedComplete = $false
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Active PG GET'
                    Warning = "$environment`: $($_.Exception.Message)"
                }
            }

            try {
                $pausedGroups += @(Get-ProtectionGroups `
                    -Environment $environment `
                    -State Paused `
                    -Headers $headers)
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Paused PG GET'
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
                    'HyperV' {
                        $objects = @(Get-HyperVObjects `
                            -ProtectionGroup $group `
                            -Headers $headers)
                    }
                    'Nutanix' {
                        $objects = @(Get-NutanixObjects `
                            -ProtectionGroup $group `
                            -Headers $headers)
                    }
                    'NAS' {
                        $objects = @(Get-NasObjects `
                            -ProtectionGroup $group `
                            -Headers $headers)
                    }
                    'Physical' {
                        $objects = @(Get-PhysicalObjects `
                            -ProtectionGroup $group)
                    }
                    'SQL' {
                        $objects = @(Get-SqlDatabaseObjects `
                            -ProtectionGroup $group `
                            -Headers $headers)
                    }
                    'Oracle' {
                        $objects = @(Get-OracleDatabaseObjects `
                            -ProtectionGroup $group `
                            -Headers $headers)
                    }
                }

                foreach ($candidate in $objects) {
                    $key = Get-ObjectKey `
                        -Candidate $candidate `
                        -Workload $workload.Name

                    if (-not [string]::IsNullOrWhiteSpace($key)) {
                        $protectedObjects[
                            ("$($cluster.ClusterId)|$key").ToLowerInvariant()
                        ] = $true
                    }
                }
            }
            catch {
                $protectedComplete = $false
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

    $protectedDisplay = $protectedObjects.Count
    if (-not $protectedComplete) {
        $protectedDisplay = 'N/A'
    }

    $summary += [pscustomobject][ordered]@{
        Workload = $workload.Name
        'Active PGs' = $activeCount
        'Paused PGs' = $pausedCount
        'Protected Objects' = $protectedDisplay
    }
}

$script:InventorySummary = @($summary)
$script:InventoryWarnings = @(
    $warnings | Sort-Object Cluster,Workload,Operation
)

Write-Host ''
Write-Host 'WORKLOAD INVENTORY SUMMARY' -ForegroundColor Cyan

$script:InventorySummary |
    Format-Table 'Workload','Active PGs','Paused PGs','Protected Objects' -AutoSize

if ($script:InventoryWarnings.Count -gt 0) {
    Write-Host ''
    Write-Host 'COLLECTION WARNINGS (API failures only)' -ForegroundColor Yellow

    $script:InventoryWarnings |
        Format-Table Cluster,Workload,Operation,Warning -Wrap -AutoSize
}
