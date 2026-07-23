# Cohesity workload inventory validation
# GET-only, PowerShell 5.1 compatible, console output only.
# Counts objects configured in ACTIVE protection groups.
# Paused protection groups are counted, but their objects are excluded.

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
    [pscustomobject]@{ Name = 'Hyper-V';     Environments = @('kHyperV');              Params = @('hypervParams','hyperVParams'); Kind = 'Standard' },
    [pscustomobject]@{ Name = 'Nutanix AHV'; Environments = @('kAcropolis');           Params = @('acropolisParams','nutanixParams','ahvParams'); Kind = 'Standard' },
    [pscustomobject]@{ Name = 'NAS';         Environments = @('kGenericNas','kIsilon');Params = @('genericNasParams','nasParams','isilonParams'); Kind = 'Standard' },
    [pscustomobject]@{ Name = 'Oracle';      Environments = @('kOracle');              Params = @('oracleParams'); Kind = 'Oracle' },
    [pscustomobject]@{ Name = 'SQL';         Environments = @('kSQL');                 Params = @('mssqlParams','sqlParams'); Kind = 'Database' },
    [pscustomobject]@{ Name = 'Physical';    Environments = @('kPhysical');            Params = @('physicalParams'); Kind = 'Physical' }
)

function Get-PropertyValue {
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

function Get-NestedValue {
    param($Object, [string]$Path)

    $current = $Object
    foreach ($part in ($Path -split '\.')) {
        $current = Get-PropertyValue -Object $current -Names @($part)
        if ($null -eq $current) { return $null }
    }
    return $current
}

function ConvertTo-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Get-FirstText {
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

function ConvertTo-Boolean {
    param($Value, [bool]$Default = $false)

    if ($Value -is [bool]) { return $Value }
    if ($null -eq $Value) { return $Default }
    $text = ([string]$Value).Trim()
    if ($text -match '^(?i:true|1|yes)$') { return $true }
    if ($text -match '^(?i:false|0|no)$') { return $false }
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

function Invoke-CohesityGet {
    param([string]$Uri, [hashtable]$Headers)

    $parameters = @{
        Uri = $Uri
        Headers = $Headers
        Method = 'Get'
        ErrorAction = 'Stop'
        TimeoutSec = 90
    }
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $parameters.UseBasicParsing = $true
    }
    $response = Invoke-WebRequest @parameters
    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }
    return ($response.Content | ConvertFrom-Json)
}

function Get-ProtectionGroups {
    param([string]$Environment, [hashtable]$Headers)

    $groups = @()
    $cookie = ''
    do {
        $uri = "$BaseUrl/v2/data-protect/protection-groups?environments=$([uri]::EscapeDataString($Environment))&isDeleted=false&includeLastRunInfo=false&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Invoke-CohesityGet -Uri $uri -Headers $Headers
        $groups += @(ConvertTo-Array (Get-PropertyValue $json @('protectionGroups','items','data')) |
            Where-Object { $null -ne $_ })

        $cookie = Get-FirstText @((Get-PropertyValue $json @('paginationCookie')))
        $truncated = ConvertTo-Boolean (Get-PropertyValue $json @('isResponseTruncated')) $false
        if (-not $truncated -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    } while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($groups)
}

function Get-ObjectIdentity {
    param($Object, [string]$FallbackType)

    $name = Get-FirstText @(
        (Get-PropertyValue $Object @('databaseUniqueName','databaseName','dbName','name','objectName','sourceName','vmName','hostName','displayName')),
        (Get-PropertyValue $Object @('id','objectId','sourceId','databaseId','entityId'))
    )
    $id = Get-FirstText @(
        (Get-PropertyValue $Object @('id','objectId','databaseId','databaseUuid','sourceId','vmId','entityId')),
        $name
    )
    $type = Get-FirstText @(
        (Get-PropertyValue $Object @('objectType','type','entityType','environmentType')),
        $FallbackType
    )
    return [pscustomobject]@{ Name = $name; Id = $id; Type = $type }
}

function Get-StandardObjects {
    param($ProtectionGroup, $Workload)

    $objects = @()
    foreach ($parameterName in @($Workload.Params)) {
        $parameters = Get-PropertyValue $ProtectionGroup @($parameterName)
        if ($null -eq $parameters) { continue }
        $objects += @(ConvertTo-Array (Get-PropertyValue $parameters @('objects')) |
            Where-Object { $null -ne $_ })
    }
    return @($objects)
}

function Get-PhysicalObjects {
    param($ProtectionGroup)

    $physical = Get-PropertyValue $ProtectionGroup @('physicalParams')
    if ($null -eq $physical) { return @() }

    $objects = @()
    $fileParameters = Get-PropertyValue $physical @('fileProtectionTypeParams')
    $volumeParameters = Get-PropertyValue $physical @('volumeProtectionTypeParams')
    $objects += @(ConvertTo-Array (Get-PropertyValue $fileParameters @('objects')) |
        Where-Object { $null -ne $_ })
    $objects += @(ConvertTo-Array (Get-PropertyValue $volumeParameters @('objects')) |
        Where-Object { $null -ne $_ })
    return @($objects)
}

function Find-DatabaseObjects {
    param(
        $Value,
        [string]$PropertyName = '',
        [int]$Depth = 0
    )

    if ($null -eq $Value -or $Depth -gt 12) { return @() }
    $found = @()

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string] -and
        $Value -isnot [System.Collections.IDictionary] -and
        $Value -isnot [pscustomobject]) {
        foreach ($item in @($Value)) {
            $found += @(Find-DatabaseObjects -Value $item -PropertyName $PropertyName -Depth ($Depth + 1))
        }
        return @($found)
    }

    if ($Value -is [string] -or $Value -is [ValueType]) { return @() }

    $type = Get-FirstText @((Get-PropertyValue $Value @('objectType','type','entityType')))
    $databaseName = Get-FirstText @(
        (Get-PropertyValue $Value @('databaseUniqueName','databaseName','dbName','dbUniqueName'))
    )
    $ordinaryName = Get-FirstText @(
        (Get-PropertyValue $Value @('name','objectName','displayName'))
    )

    $isExcludedType = $type -match '(?i)instance|host|server|source|cluster'
    $isDatabaseType = $type -match '(?i)database|kRACDatabase|kNonRACDatabase'
    $isDatabaseCollection = $PropertyName -match '(?i)databases|databaseList|databaseObjects|dbChannels|dbList'

    if (-not $isExcludedType -and
        ($isDatabaseType -or -not [string]::IsNullOrWhiteSpace($databaseName) -or
        ($isDatabaseCollection -and -not [string]::IsNullOrWhiteSpace($ordinaryName)))) {
        $found += $Value
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        if ($property.Name -match '^(?i)hosts?|servers?|instances?|databaseNodeList$') {
            continue
        }
        if ($null -ne $property.Value -and -not [object]::ReferenceEquals($property.Value,$Value)) {
            $found += @(Find-DatabaseObjects -Value $property.Value -PropertyName $property.Name -Depth ($Depth + 1))
        }
    }
    return @($found)
}

function Get-OracleDatabaseObjects {
    param($ProtectionGroup)

    $oracle = Get-PropertyValue $ProtectionGroup @('oracleParams')
    if ($null -eq $oracle) { return @() }

    $databases = @()

    # Proven path from the existing Oracle inventory:
    # oracleParams.objects[].dbParams.dbChannels[].databaseUniqueName
    foreach ($oracleObject in @(ConvertTo-Array (Get-PropertyValue $oracle @('objects')))) {
        $dbParameters = Get-PropertyValue $oracleObject @('dbParams')
        $databases += @(ConvertTo-Array (Get-PropertyValue $dbParameters @('dbChannels')) |
            Where-Object { $null -ne $_ })
    }

    # Handles versions that return explicit database objects elsewhere in oracleParams.
    $databases += @(Find-DatabaseObjects -Value $oracle -PropertyName 'oracleParams')
    return @($databases)
}

function Get-DatabaseObjects {
    param($ProtectionGroup, $Workload)

    $databases = @()
    foreach ($parameterName in @($Workload.Params)) {
        $parameters = Get-PropertyValue $ProtectionGroup @($parameterName)
        if ($null -ne $parameters) {
            $databases += @(Find-DatabaseObjects -Value $parameters -PropertyName $parameterName)
        }
    }
    return @($databases)
}

function Add-ObjectRows {
    param(
        [System.Collections.Generic.List[object]]$Rows,
        [hashtable]$Seen,
        [string]$Cluster,
        [string]$Workload,
        [string]$ProtectionGroup,
        [string]$ProtectionGroupId,
        [object[]]$Objects,
        [string]$FallbackType
    )

    foreach ($object in @($Objects)) {
        if ($null -eq $object) { continue }
        $identity = Get-ObjectIdentity -Object $object -FallbackType $FallbackType
        if ([string]::IsNullOrWhiteSpace($identity.Name) -and
            [string]::IsNullOrWhiteSpace($identity.Id)) {
            continue
        }
        $key = '{0}|{1}|{2}|{3}' -f $Cluster,$Workload,$ProtectionGroupId,
            (Get-FirstText @($identity.Id,$identity.Name)).ToLowerInvariant()
        if ($Seen.ContainsKey($key)) { continue }
        $Seen[$key] = $true
        $Rows.Add([pscustomobject]@{
            Cluster = $Cluster
            Workload = $Workload
            ProtectionGroup = $ProtectionGroup
            ProtectionGroupId = $ProtectionGroupId
            ObjectName = $identity.Name
            ObjectId = $identity.Id
            ObjectType = $identity.Type
        })
    }
}

$clusterJson = Invoke-CohesityGet -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$clusters = @(ConvertTo-Array (Get-PropertyValue $clusterJson @('cohesityClusters','clusters','items')) |
    ForEach-Object {
        $clusterId = Get-FirstText @((Get-PropertyValue $_ @('clusterId','id')))
        $clusterName = Get-FirstText @(
            (Get-PropertyValue $_ @('clusterName','displayName','name')),
            "Unknown-$clusterId"
        )
        if (-not [string]::IsNullOrWhiteSpace($clusterId)) {
            [pscustomobject]@{ ClusterName = $clusterName; ClusterId = $clusterId }
        }
    } | Sort-Object ClusterName)

if (@($clusters).Count -eq 0) { throw 'No clusters returned from Helios.' }

$clusterMenu = for ($index = 0; $index -lt @($clusters).Count; $index++) {
    [pscustomobject]@{
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
        $number -ge 0 -and $number -le @($clusterMenu).Count) {
        if ($number -eq 0) { $selectedClusters = @($clusterMenu) }
        else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $number }) }
        break
    }
    Write-Host "Enter 0, 1-$(@($clusterMenu).Count), or X." -ForegroundColor Red
}

$summaryRows = New-Object System.Collections.Generic.List[object]
$objectRows = New-Object System.Collections.Generic.List[object]
$warningRows = New-Object System.Collections.Generic.List[object]

foreach ($cluster in @($selectedClusters)) {
    $headers = New-Headers -ClusterId $cluster.ClusterId

    foreach ($workload in @($Workloads)) {
        $allGroups = @()
        foreach ($environment in @($workload.Environments)) {
            try {
                $allGroups += @(Get-ProtectionGroups -Environment $environment -Headers $headers)
            }
            catch {
                $warningRows.Add([pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    ProtectionGroup = ''
                    Warning = "Protection-group GET failed for $environment`: $($_.Exception.Message)"
                })
            }
        }

        $groupSeen = @{}
        $activeGroups = @()
        $activeCount = 0
        $pausedCount = 0

        foreach ($group in @($allGroups)) {
            $groupId = Get-FirstText @(
                (Get-PropertyValue $group @('id','protectionGroupId')),
                (Get-PropertyValue $group @('name','protectionGroupName'))
            )
            if ([string]::IsNullOrWhiteSpace($groupId) -or $groupSeen.ContainsKey($groupId)) {
                continue
            }
            $groupSeen[$groupId] = $true

            $isPaused = ConvertTo-Boolean (Get-PropertyValue $group @('isPaused','paused')) $false
            $isActive = ConvertTo-Boolean (Get-PropertyValue $group @('isActive','active')) $true
            if ($isPaused) {
                $pausedCount++
            }
            elseif ($isActive) {
                $activeCount++
                $activeGroups += $group
            }
        }

        $workloadSeen = @{}
        foreach ($group in @($activeGroups)) {
            $groupId = Get-FirstText @(
                (Get-PropertyValue $group @('id','protectionGroupId')),
                (Get-PropertyValue $group @('name','protectionGroupName'))
            )
            $groupName = Get-FirstText @(
                (Get-PropertyValue $group @('name','protectionGroupName')),
                $groupId
            )

            switch ($workload.Kind) {
                'Physical' {
                    $objects = @(Get-PhysicalObjects -ProtectionGroup $group)
                    $fallbackType = 'PhysicalObject'
                }
                'Oracle' {
                    $objects = @(Get-OracleDatabaseObjects -ProtectionGroup $group)
                    $fallbackType = 'kDatabase'
                }
                'Database' {
                    $objects = @(Get-DatabaseObjects -ProtectionGroup $group -Workload $workload)
                    $fallbackType = 'kDatabase'
                }
                default {
                    $objects = @(Get-StandardObjects -ProtectionGroup $group -Workload $workload)
                    $fallbackType = 'ProtectedObject'
                }
            }

            $before = @($objectRows).Count
            Add-ObjectRows -Rows $objectRows -Seen $workloadSeen `
                -Cluster $cluster.ClusterName -Workload $workload.Name `
                -ProtectionGroup $groupName -ProtectionGroupId $groupId `
                -Objects $objects -FallbackType $fallbackType

            if (@($objectRows).Count -eq $before) {
                $warningRows.Add([pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    ProtectionGroup = $groupName
                    Warning = $(if ($workload.Kind -in @('Oracle','Database')) {
                            'No database-level objects found; host/server/instance rows were deliberately excluded.'
                        }
                        else {
                            'No configured object rows were found in the active PG.'
                        })
                })
            }
        }

        $protectedObjectCount = @($objectRows | Where-Object {
            $_.Cluster -eq $cluster.ClusterName -and $_.Workload -eq $workload.Name
        }).Count

        $summaryRows.Add([pscustomobject]@{
            Cluster = $cluster.ClusterName
            Workload = $workload.Name
            ActivePGs = $activeCount
            PausedPGs = $pausedCount
            ProtectedObjects = $protectedObjectCount
        })
    }
}

$script:InventorySummary = @($summaryRows | Sort-Object Cluster,Workload)
$script:InventoryObjects = @($objectRows | Sort-Object Cluster,Workload,ProtectionGroup,ObjectName)
$script:InventoryWarnings = @($warningRows | Sort-Object Cluster,Workload,ProtectionGroup)

Write-Host ''
Write-Host 'WORKLOAD INVENTORY SUMMARY' -ForegroundColor Cyan
$script:InventorySummary | Format-Table Cluster,Workload,ActivePGs,PausedPGs,ProtectedObjects -AutoSize

Write-Host ''
Write-Host 'SQL AND ORACLE DATABASE OBJECTS (servers and instances excluded)' -ForegroundColor Cyan
$databaseRows = @($script:InventoryObjects | Where-Object { $_.Workload -in @('SQL','Oracle') })
if (@($databaseRows).Count -gt 0) {
    $databaseRows | Format-Table Cluster,Workload,ProtectionGroup,ObjectName,ObjectType -AutoSize
}
else {
    Write-Host 'No SQL or Oracle database objects were returned.' -ForegroundColor Yellow
}

if (@($script:InventoryWarnings).Count -gt 0) {
    Write-Host ''
    Write-Host 'COLLECTION WARNINGS' -ForegroundColor Yellow
    $script:InventoryWarnings | Format-Table Cluster,Workload,ProtectionGroup,Warning -Wrap -AutoSize
}

Write-Host ''
Write-Host 'Full object detail remains available in $InventoryObjects.' -ForegroundColor DarkGray
