# Cohesity SQL Database Inventory - Trial Version
# Multi-cluster | Helios | GET-only | PowerShell 5.1 compatible

[CmdletBinding()]
param(
    [switch]$GridView,
    [switch]$DumpRawJson
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\SQLInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$oneGiB              = 1073741824

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found at $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found at $encryptedApiKeyPath"
}
if ($DumpRawJson -and -not (Test-Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function New-Headers {
    param([string]$ClusterId)

    $headers = @{
        accept = "application/json"
        apiKey = $apiKey
    }

    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers["accessClusterId"] = $ClusterId
    }

    return $headers
}

function Get-Json {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -UseBasicParsing `
            -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function Get-FirstProperty {
    param(
        $Object,
        [string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object) {
        return $Default
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                $value = $property.Value
                if ($null -ne $value) {
                    $text = ([string]$value).Trim()
                    if (-not [string]::IsNullOrWhiteSpace($text)) {
                        return $value
                    }
                }
            }
        }
    }

    return $Default
}

function Get-NestedValue {
    param(
        $Object,
        [string[]]$Paths,
        $Default = $null
    )

    foreach ($path in $Paths) {
        $current = $Object
        $found = $true

        foreach ($segment in ($path -split '\.')) {
            if ($null -eq $current) {
                $found = $false
                break
            }

            if (
                $current -is [System.Collections.IEnumerable] -and
                $current -isnot [string] -and
                $current -isnot [System.Collections.IDictionary]
            ) {
                $currentItems = @($current)
                if ($currentItems.Count -eq 0) {
                    $found = $false
                    break
                }
                $current = $currentItems[0]
            }

            $property = @($current.PSObject.Properties) |
                Where-Object { $_.Name -ieq $segment } |
                Select-Object -First 1

            if ($null -eq $property) {
                $found = $false
                break
            }

            $current = $property.Value
        }

        if ($found -and $null -ne $current) {
            $text = ([string]$current).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                return $current
            }
        }
    }

    return $Default
}

function Convert-ToArray {
    param(
        $Response,
        [string[]]$CandidateProperties
    )

    if ($null -eq $Response) {
        return @()
    }

    if ($Response -is [System.Array]) {
        return @($Response)
    }

    foreach ($candidate in $CandidateProperties) {
        $value = Get-FirstProperty $Response @($candidate)
        if ($null -ne $value) {
            return @($value)
        }
    }

    return @($Response)
}

function Get-ClusterName {
    param($Cluster)

    $name = Get-FirstProperty $Cluster @(
        "name",
        "clusterName",
        "displayName"
    ) "Unknown"

    return [string]$name
}

function Get-ClusterId {
    param($Cluster)

    $id = Get-FirstProperty $Cluster @(
        "clusterId",
        "id"
    ) $null

    if ($null -eq $id) {
        return $null
    }

    return [string]$id
}

function Get-SourceRegistrationInfo {
    param($Registration)

    $source = Get-NestedValue $Registration @(
        "source",
        "registration",
        "protectionSource"
    ) $null

    if ($null -eq $source) {
        $source = $Registration
    }

    return [pscustomobject][ordered]@{
        Id = Get-NestedValue $source @(
            "id",
            "sourceId",
            "entityId.intId"
        ) $null
        Name = Get-NestedValue $source @(
            "name",
            "sourceName"
        ) "Unknown SQL Source"
        Environment = Get-NestedValue $source @(
            "environment"
        ) (Get-NestedValue $Registration @("environment") $null)
    }
}

function Get-EmbeddedChildren {
    param($Node)

    $children = @()
    foreach ($propertyName in @("objects", "childObjects", "children")) {
        $value = Get-FirstProperty $Node @($propertyName) $null
        if ($null -ne $value) {
            $children += @($value)
        }
    }

    return @($children | Where-Object { $null -ne $_ })
}

function Get-SqlSourceObjects {
    param(
        [Parameter(Mandatory = $true)][string]$SourceId,
        [Parameter(Mandatory = $true)][hashtable]$Headers,
        [Parameter(Mandatory = $true)][string]$ClusterName,
        [Parameter(Mandatory = $true)][string]$SourceName
    )

    $objectTypes = @(
        "kRootContainer",
        "kInstance",
        "kDatabase",
        "kAAGRootContainer",
        "kAAG",
        "kAAGDatabase"
    )

    $typeQuery = @(
        $objectTypes | ForEach-Object {
            "mssqlObjectTypes=$([uri]::EscapeDataString($_))"
        }
    ) -join "&"

    $queue = New-Object System.Collections.Queue
    $queue.Enqueue("__ROOT__")

    $requestedParents = @{}
    $recordsById = @{}
    $recordsWithoutId = @()
    $requestCount = 0

    while ($queue.Count -gt 0) {
        $queueValue = $queue.Dequeue()
        $isRootRequest = ([string]$queueValue -eq "__ROOT__")
        $parentId = if ($isRootRequest) { $null } else { $queueValue }
        $parentKey = if ($isRootRequest) { "ROOT" } else { [string]$parentId }

        if ($requestedParents.ContainsKey($parentKey)) {
            continue
        }
        $requestedParents[$parentKey] = $true

        $uri = "$baseUrl/v2/data-protect/sources/$SourceId/objects?$typeQuery&useCachedData=false"
        if ($null -ne $parentId) {
            $uri += "&parentId=$([uri]::EscapeDataString([string]$parentId))"
        }

        $response = Get-Json -Uri $uri -Headers $Headers
        $requestCount++

        if ($DumpRawJson) {
            $safeCluster = $ClusterName -replace '[^A-Za-z0-9_.-]', '_'
            $safeSource = $SourceName -replace '[^A-Za-z0-9_.-]', '_'
            $safeParent = $parentKey -replace '[^A-Za-z0-9_.-]', '_'
            $rawPath = Join-Path $logDirectory "${safeCluster}_${safeSource}_${safeParent}.json"
            $response | ConvertTo-Json -Depth 100 | Set-Content -Path $rawPath -Encoding UTF8
        }

        $items = Convert-ToArray $response @(
            "objects",
            "childObjects",
            "objectSummaries",
            "items"
        )

        foreach ($item in $items) {
            if ($null -eq $item) {
                continue
            }

            $stack = New-Object System.Collections.Stack
            $stack.Push([pscustomobject]@{
                Node = $item
                ParentId = $parentId
            })

            while ($stack.Count -gt 0) {
                $entry = $stack.Pop()
                $node = $entry.Node
                $nodeParentId = $entry.ParentId

                $nodeId = Get-NestedValue $node @(
                    "id",
                    "objectId",
                    "entityId.intId"
                ) $null
                $nodeName = Get-NestedValue $node @(
                    "name"
                ) "N/A"
                $nodeType = Get-NestedValue $node @(
                    "objectType",
                    "type"
                ) "N/A"

                $record = [pscustomobject][ordered]@{
                    Id = $nodeId
                    ParentId = $nodeParentId
                    SourceId = Get-NestedValue $node @("sourceId") $SourceId
                    SourceName = Get-NestedValue $node @("sourceName") $SourceName
                    Name = [string]$nodeName
                    ObjectType = [string]$nodeType
                    LogicalSizeBytes = Get-NestedValue $node @(
                        "logicalSizeBytes",
                        "sizeBytes",
                        "size"
                    ) $null
                    Node = $node
                }

                if ($null -ne $nodeId) {
                    $recordsById[[string]$nodeId] = $record
                }
                else {
                    $recordsWithoutId += $record
                }

                $embeddedChildren = Get-EmbeddedChildren $node
                if ($embeddedChildren.Count -gt 0) {
                    foreach ($child in $embeddedChildren) {
                        $stack.Push([pscustomobject]@{
                            Node = $child
                            ParentId = $nodeId
                        })
                    }
                }
                elseif (
                    $null -ne $nodeId -and
                    $nodeType -notin @("kDatabase", "kAAGDatabase") -and
                    -not $requestedParents.ContainsKey([string]$nodeId)
                ) {
                    $queue.Enqueue($nodeId)
                }
            }
        }

        if ($requestCount -ge 5000) {
            throw "Stopped SQL source traversal after 5000 requests for source '$SourceName' on '$ClusterName'."
        }
    }

    return @($recordsById.Values) + @($recordsWithoutId)
}

function Get-Ancestors {
    param(
        $Record,
        [hashtable]$ById
    )

    $ancestors = @()
    $currentParent = $Record.ParentId
    $visited = @{}

    while ($null -ne $currentParent) {
        $key = [string]$currentParent
        if ($visited.ContainsKey($key)) {
            break
        }
        $visited[$key] = $true

        if (-not $ById.ContainsKey($key)) {
            break
        }

        $parent = $ById[$key]
        $ancestors += $parent
        $currentParent = $parent.ParentId
    }

    return @($ancestors)
}

function Get-IdsFromSqlParams {
    param($Value)

    if ($null -eq $Value) {
        return
    }

    if ($Value -is [string] -or $Value -is [ValueType]) {
        return
    }

    if (
        $Value -is [System.Collections.IEnumerable] -and
        $Value -isnot [string] -and
        $Value -isnot [System.Collections.IDictionary]
    ) {
        foreach ($item in @($Value)) {
            Get-IdsFromSqlParams $item
        }
        return
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        $propertyValue = $property.Value

        if ($property.Name -imatch '^(id|ids|objectId|objectIds|sourceId|sourceIds|databaseId|databaseIds|instanceId|instanceIds|parentId|parentIds)$') {
            foreach ($idValue in @($propertyValue)) {
                if ($null -ne $idValue -and ($idValue -is [string] -or $idValue -is [ValueType])) {
                    [string]$idValue
                }
            }
        }

        if ($null -ne $propertyValue -and $propertyValue -isnot [string] -and $propertyValue -isnot [ValueType]) {
            Get-IdsFromSqlParams $propertyValue
        }
    }
}

function Get-ObjectIdsFromProtectionGroup {
    param($ProtectionGroup)

    $ids = @()

    foreach ($path in @(
        "mssqlParams",
        "sqlParams"
    )) {
        $sqlParams = Get-NestedValue $ProtectionGroup @($path) $null
        if ($null -ne $sqlParams) {
            $ids += @(Get-IdsFromSqlParams $sqlParams)
        }
    }

    foreach ($path in @(
        "sourceIds",
        "objectIds"
    )) {
        $directIds = Get-NestedValue $ProtectionGroup @($path) $null
        foreach ($directId in @($directIds)) {
            if ($null -ne $directId) {
                $ids += [string]$directId
            }
        }
    }

    return @(
        $ids |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )
}

function Get-LastRunStatus {
    param($ProtectionGroup)

    return [string](Get-NestedValue $ProtectionGroup @(
        "lastRun.localBackupInfo.status",
        "lastRunInfo.localBackupInfo.status",
        "lastRunInfo.status",
        "lastRun.status",
        "lastRunAnyStatus"
    ) "N/A")
}

function Get-LogBackupSetting {
    param($ProtectionGroup)

    $value = Get-NestedValue $ProtectionGroup @(
        "mssqlParams.logBackupEnabled",
        "mssqlParams.enableLogBackup",
        "mssqlParams.logBackupParams.enabled",
        "mssqlParams.logBackupPolicy.enabled",
        "sqlParams.logBackupEnabled",
        "sqlParams.enableLogBackup"
    ) $null

    if ($null -eq $value) {
        return "N/A"
    }

    if ($value -is [bool]) {
        if ($value) { return "Enabled" }
        return "Disabled"
    }

    $text = [string]$value
    if ($text -ieq "true") { return "Enabled" }
    if ($text -ieq "false") { return "Disabled" }
    return $text
}

function Convert-UsecsToLocalText {
    param($Value)

    if ($null -eq $Value) {
        return "N/A"
    }

    [int64]$usecs = 0
    if (-not [int64]::TryParse([string]$Value, [ref]$usecs)) {
        return "N/A"
    }

    try {
        $milliseconds = [math]::Floor($usecs / 1000)
        return [DateTimeOffset]::FromUnixTimeMilliseconds($milliseconds).LocalDateTime.ToString("dd-MMM-yyyy HH:mm:ss")
    }
    catch {
        return "N/A"
    }
}

Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "        COHESITY SQL DATABASE INVENTORY" -ForegroundColor White
Write-Host "====================================================" -ForegroundColor Cyan

$clusterResponse = Get-Json `
    -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers (New-Headers)

$clusters = Convert-ToArray $clusterResponse @(
    "cohesityClusters",
    "clusters",
    "clusterInfos"
)

if ($clusters.Count -eq 0) {
    throw "No clusters returned from Cohesity Helios."
}

$inventoryRows = @()
$issues = @()
$successfulClusters = 0
$totalSqlSources = 0
$totalSqlInstances = 0

foreach ($cluster in ($clusters | Sort-Object { Get-ClusterName $_ })) {
    $clusterName = Get-ClusterName $cluster
    $clusterId = Get-ClusterId $cluster

    if ([string]::IsNullOrWhiteSpace($clusterId)) {
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Stage = "Cluster discovery"
            Issue = "Cluster ID missing"
        }
        continue
    }

    Write-Host "Processing cluster: $clusterName" -ForegroundColor Yellow
    $headers = New-Headers $clusterId

    try {
        $registrationResponse = Get-Json `
            -Uri "$baseUrl/v2/data-protect/sources/registrations?includeTenants=true&includeSourceCredentials=false&useCachedData=false" `
            -Headers $headers

        $registrations = Convert-ToArray $registrationResponse @(
            "registrations",
            "sources",
            "sourceRegistrations"
        )

        $sqlSources = @()
        foreach ($registration in $registrations) {
            $sourceInfo = Get-SourceRegistrationInfo $registration
            if ([string]$sourceInfo.Environment -ieq "kSQL") {
                $sqlSources += $sourceInfo
            }
        }

        $totalSqlSources += $sqlSources.Count

        $policyResponse = Get-Json `
            -Uri "$baseUrl/v2/data-protect/policies?includeTenants=true&includeReplicatedPolicies=true" `
            -Headers $headers
        $policies = Convert-ToArray $policyResponse @("policies")
        $policyNameById = @{}
        foreach ($policy in $policies) {
            $policyId = Get-FirstProperty $policy @("id") $null
            $policyName = Get-FirstProperty $policy @("name") "N/A"
            if ($null -ne $policyId) {
                $policyNameById[[string]$policyId] = [string]$policyName
            }
        }

        $pgResponse = Get-Json `
            -Uri "$baseUrl/v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true&includeLastRunInfo=true&includeTenants=true&pruneSourceIds=false&useCachedData=false" `
            -Headers $headers
        $protectionGroups = Convert-ToArray $pgResponse @("protectionGroups", "groups")

        $pgRecords = @()
        foreach ($pg in $protectionGroups) {
            $policyId = Get-FirstProperty $pg @("policyId") $null
            $policyName = "N/A"
            if ($null -ne $policyId -and $policyNameById.ContainsKey([string]$policyId)) {
                $policyName = $policyNameById[[string]$policyId]
            }

            $pgRecords += [pscustomobject][ordered]@{
                Name = [string](Get-FirstProperty $pg @("name") "N/A")
                PolicyId = $policyId
                PolicyName = $policyName
                ObjectIds = @(Get-ObjectIdsFromProtectionGroup $pg)
                LogBackup = Get-LogBackupSetting $pg
                LastRunStatus = Get-LastRunStatus $pg
                LastRunEndTime = Convert-UsecsToLocalText (Get-NestedValue $pg @(
                    "lastRun.localBackupInfo.endTimeUsecs",
                    "lastRunInfo.localBackupInfo.endTimeUsecs",
                    "lastRun.endTimeUsecs",
                    "lastRunInfo.endTimeUsecs"
                ) $null)
            }
        }

        foreach ($sqlSource in $sqlSources) {
            if ($null -eq $sqlSource.Id) {
                $issues += [pscustomobject]@{
                    Cluster = $clusterName
                    Stage = "SQL source discovery"
                    Issue = "Source ID missing for '$($sqlSource.Name)'"
                }
                continue
            }

            try {
                $sourceObjects = Get-SqlSourceObjects `
                    -SourceId ([string]$sqlSource.Id) `
                    -Headers $headers `
                    -ClusterName $clusterName `
                    -SourceName ([string]$sqlSource.Name)
            }
            catch {
                $issues += [pscustomobject]@{
                    Cluster = $clusterName
                    Stage = "SQL object discovery"
                    Issue = "$($sqlSource.Name): $($_.Exception.Message)"
                }
                continue
            }

            $objectById = @{}
            foreach ($record in $sourceObjects) {
                if ($null -ne $record.Id) {
                    $objectById[[string]$record.Id] = $record
                }
            }

            $instanceCount = @(
                $sourceObjects | Where-Object { $_.ObjectType -eq "kInstance" }
            ).Count
            $totalSqlInstances += $instanceCount

            $databaseRecords = @(
                $sourceObjects | Where-Object {
                    $_.ObjectType -in @("kDatabase", "kAAGDatabase")
                }
            )

            foreach ($database in $databaseRecords) {
                $ancestors = Get-Ancestors -Record $database -ById $objectById
                $instance = @($ancestors | Where-Object { $_.ObjectType -eq "kInstance" } | Select-Object -First 1)
                $aag = @($ancestors | Where-Object { $_.ObjectType -eq "kAAG" } | Select-Object -First 1)

                $instanceRecord = if ($instance.Count -gt 0) { $instance[0] } else { $null }
                $aagRecord = if ($aag.Count -gt 0) { $aag[0] } else { $null }

                $candidateIds = @()
                if ($null -ne $database.Id) { $candidateIds += [string]$database.Id }
                if ($null -ne $database.SourceId) { $candidateIds += [string]$database.SourceId }
                foreach ($ancestor in $ancestors) {
                    if ($null -ne $ancestor.Id) { $candidateIds += [string]$ancestor.Id }
                }
                $candidateIds = @($candidateIds | Sort-Object -Unique)

                $matchingPgs = @(
                    $pgRecords | Where-Object {
                        $pgObjectIds = @($_.ObjectIds)
                        @($candidateIds | Where-Object { $pgObjectIds -contains $_ }).Count -gt 0
                    }
                )

                $protectionStatus = if ($matchingPgs.Count -gt 0) { "Protected" } else { "Unprotected" }
                $findings = @()
                if ($matchingPgs.Count -eq 0) {
                    $findings += "No active SQL protection group"
                }
                elseif (@($matchingPgs | Where-Object { $_.LastRunStatus -eq "Failed" }).Count -gt 0) {
                    $findings += "Latest protection-group run failed"
                }

                if ($findings.Count -eq 0) {
                    $findingsText = "None"
                }
                else {
                    $findingsText = $findings -join "; "
                }

                $hostName = Get-NestedValue $database.Node @(
                    "mssqlParams.hostInfo.name",
                    "mssqlParams.hostInfo.hostName",
                    "mssqlParams.hostInfo.fqdn",
                    "hostInfo.name",
                    "hostName"
                ) $null

                if ($null -eq $hostName -and $null -ne $instanceRecord) {
                    $hostName = Get-NestedValue $instanceRecord.Node @(
                        "mssqlParams.hostInfo.name",
                        "mssqlParams.hostInfo.hostName",
                        "mssqlParams.hostInfo.fqdn",
                        "hostInfo.name",
                        "hostName"
                    ) $null
                }
                if ($null -eq $hostName) {
                    $hostName = $database.SourceName
                }

                $instanceName = if ($null -ne $instanceRecord) { $instanceRecord.Name } else { "N/A" }
                $aagName = if ($null -ne $aagRecord) { $aagRecord.Name } else { "N/A" }

                $sqlVersion = Get-NestedValue $database.Node @(
                    "mssqlParams.version",
                    "mssqlParams.sqlVersion",
                    "version",
                    "sqlVersion"
                ) $null
                if ($null -eq $sqlVersion -and $null -ne $instanceRecord) {
                    $sqlVersion = Get-NestedValue $instanceRecord.Node @(
                        "mssqlParams.version",
                        "mssqlParams.sqlVersion",
                        "version",
                        "sqlVersion"
                    ) "N/A"
                }
                if ($null -eq $sqlVersion) { $sqlVersion = "N/A" }

                $logicalSizeBytes = 0
                [double]$sizeNumber = 0
                if (
                    $null -ne $database.LogicalSizeBytes -and
                    [double]::TryParse([string]$database.LogicalSizeBytes, [ref]$sizeNumber)
                ) {
                    $logicalSizeBytes = $sizeNumber
                }

                $protectionGroupNames = "N/A"
                $policyNames = "N/A"
                $logBackupSettings = "N/A"
                $lastRunStatuses = "N/A"
                $lastRunEndTimes = "N/A"

                if ($matchingPgs.Count -gt 0) {
                    $protectionGroupNames = (@($matchingPgs.Name) | Sort-Object -Unique) -join ", "
                    $policyNames = (@($matchingPgs.PolicyName) | Sort-Object -Unique) -join ", "
                    $logBackupSettings = (@($matchingPgs.LogBackup) | Sort-Object -Unique) -join ", "
                    $lastRunStatuses = (@($matchingPgs.LastRunStatus) | Sort-Object -Unique) -join ", "
                    $lastRunEndTimes = (@($matchingPgs.LastRunEndTime) | Sort-Object -Unique) -join ", "
                }

                $databaseObjectId = "N/A"
                if ($null -ne $database.Id) {
                    $databaseObjectId = [string]$database.Id
                }

                $inventoryRows += [pscustomobject][ordered]@{
                    Cluster = $clusterName
                    HostName = [string]$hostName
                    InstanceName = [string]$instanceName
                    DatabaseName = [string]$database.Name
                    ObjectType = [string]$database.ObjectType
                    SQLVersion = [string]$sqlVersion
                    RecoveryModel = [string](Get-NestedValue $database.Node @(
                        "mssqlParams.recoveryModel",
                        "recoveryModel"
                    ) "N/A")
                    DBState = [string](Get-NestedValue $database.Node @(
                        "mssqlParams.databaseState",
                        "mssqlParams.state",
                        "databaseState",
                        "state"
                    ) "N/A")
                    SizeGB = [math]::Round(($logicalSizeBytes / $oneGiB), 2)
                    TDEEnabled = [string](Get-NestedValue $database.Node @(
                        "mssqlParams.isEncrypted",
                        "isEncrypted"
                    ) "N/A")
                    AvailabilityGroup = [string]$aagName
                    ReplicaRole = [string](Get-NestedValue $database.Node @(
                        "mssqlParams.aagInfo.replicaRole",
                        "mssqlParams.aagInfo.role",
                        "aagInfo.replicaRole",
                        "aagInfo.role"
                    ) "N/A")
                    ProtectionGroup = $protectionGroupNames
                    PolicyName = $policyNames
                    LogBackup = $logBackupSettings
                    LastRunStatus = $lastRunStatuses
                    LastRunEndTime = $lastRunEndTimes
                    ProtectionStatus = $protectionStatus
                    Findings = $findingsText
                    ObjectId = $databaseObjectId
                    SourceName = [string]$database.SourceName
                }
            }
        }

        $successfulClusters++
    }
    catch {
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Stage = "Cluster inventory"
            Issue = $_.Exception.Message
        }
    }
}

$inventoryRows = @(
    $inventoryRows |
        Sort-Object Cluster, HostName, InstanceName, DatabaseName, ObjectId -Unique
)

$protectedCount = @(
    $inventoryRows | Where-Object { $_.ProtectionStatus -eq "Protected" }
).Count
$unprotectedCount = @(
    $inventoryRows | Where-Object { $_.ProtectionStatus -eq "Unprotected" }
).Count
$aagDatabaseCount = @(
    $inventoryRows | Where-Object { $_.ObjectType -eq "kAAGDatabase" }
).Count
$standaloneDatabaseCount = @(
    $inventoryRows | Where-Object { $_.ObjectType -eq "kDatabase" }
).Count

$summary = @(
    [pscustomobject]@{ Metric = "Clusters discovered"; Count = $clusters.Count }
    [pscustomobject]@{ Metric = "Clusters successfully queried"; Count = $successfulClusters }
    [pscustomobject]@{ Metric = "SQL sources"; Count = $totalSqlSources }
    [pscustomobject]@{ Metric = "SQL instances"; Count = $totalSqlInstances }
    [pscustomobject]@{ Metric = "Database rows"; Count = $inventoryRows.Count }
    [pscustomobject]@{ Metric = "Protected databases"; Count = $protectedCount }
    [pscustomobject]@{ Metric = "Unprotected databases"; Count = $unprotectedCount }
    [pscustomobject]@{ Metric = "Availability Group databases"; Count = $aagDatabaseCount }
    [pscustomobject]@{ Metric = "Standalone databases"; Count = $standaloneDatabaseCount }
    [pscustomobject]@{ Metric = "Inventory issues"; Count = $issues.Count }
)

Write-Host "`nSUMMARY" -ForegroundColor Cyan
$summary | Format-Table -AutoSize | Out-Host

Write-Host "`nSQL DATABASE INVENTORY" -ForegroundColor Cyan
if ($inventoryRows.Count -gt 0) {
    $inventoryRows |
        Select-Object `
            Cluster,
            HostName,
            InstanceName,
            DatabaseName,
            ObjectType,
            SQLVersion,
            RecoveryModel,
            DBState,
            SizeGB,
            TDEEnabled,
            AvailabilityGroup,
            ReplicaRole,
            ProtectionGroup,
            PolicyName,
            LogBackup,
            LastRunStatus,
            LastRunEndTime,
            ProtectionStatus,
            Findings |
        Format-Table -AutoSize -Wrap |
        Out-Host
}
else {
    Write-Host "No SQL databases were returned." -ForegroundColor Yellow
}

if ($issues.Count -gt 0) {
    Write-Host "`nINVENTORY ISSUES" -ForegroundColor Yellow
    $issues | Sort-Object Cluster, Stage | Format-Table -AutoSize -Wrap | Out-Host
}

if ($GridView) {
    if (Get-Command Out-GridView -ErrorAction SilentlyContinue) {
        $inventoryRows | Out-GridView -Title "Cohesity SQL Database Inventory"
    }
    else {
        Write-Warning "Out-GridView is not available on this system."
    }
}

return [pscustomobject][ordered]@{
    Summary = $summary
    Rows = $inventoryRows
    Issues = $issues
}
