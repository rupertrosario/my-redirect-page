# Cohesity Rack Resiliency Assessment - STRICT READ-ONLY GET COLLECTION
# PowerShell 5.1 compatible
# DATA COLLECTION ONLY. This script contains no Cohesity write operation.

$ErrorActionPreference = 'Stop'
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = 'https://helios.cohesity.com'
$outputRoot          = 'X:\PowerShell\Data\Cohesity\RackResiliencyAssessment'
$helperPath          = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$encryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
$notReturned         = 'NOT RETURNED BY THIS CLUSTER VERSION/API'

$runStamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$runDir = Join-Path $outputRoot ("Run_{0}" -f $runStamp)
$rawRoot = Join-Path $runDir 'Raw'
New-Item -Path $rawRoot -ItemType Directory -Force | Out-Null

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace([string]$apiKey)) {
    throw 'AES API key helper returned an empty API key.'
}

$approvedGetPaths = @(
    '/v2/mcm/cluster-mgmt/info',
    '/v2/clusters',
    '/v2/chassis',
    '/v2/racks',
    '/v2/clusters/nodes',
    '/v2/storage-domains',
    '/v2/storage-domains/fault-tolerance-options'
)

$apiStatusRows       = New-Object System.Collections.Generic.List[object]
$fieldInventoryRows  = New-Object System.Collections.Generic.List[object]
$relationshipRows    = New-Object System.Collections.Generic.List[object]
$nodeMembershipRows  = New-Object System.Collections.Generic.List[object]
$storageDomainRows   = New-Object System.Collections.Generic.List[object]
$clusterSummaryRows  = New-Object System.Collections.Generic.List[object]
$getFailureRows      = New-Object System.Collections.Generic.List[object]

function New-Headers {
    param([string]$ClusterId)

    $headers = @{
        accept = 'application/json'
        apiKey = $apiKey
    }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers['accessClusterId'] = $ClusterId
    }
    return $headers
}

function Get-SafeFileName {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return 'UNKNOWN' }
    return ([regex]::Replace($Value, '[^A-Za-z0-9_.-]', '_'))
}

function Get-ExactMember {
    param($Object, [Parameter(Mandatory=$true)][string]$Name)
    if ($null -eq $Object) { return $null }
    foreach ($property in @($Object.PSObject.Properties)) {
        if ($property.Name -ceq $Name) { return $property }
    }
    return $null
}

function Get-PathValue {
    param($Object, [Parameter(Mandatory=$true)][string]$Path)

    if ($null -eq $Object) { return $null }
    $current = $Object
    foreach ($part in $Path.Split('.')) {
        $member = Get-ExactMember -Object $current -Name $part
        if ($null -eq $member -or $null -eq $member.Value) { return $null }
        $current = $member.Value
    }
    return $current
}

function Get-FirstPathValue {
    param($Object, [Parameter(Mandatory=$true)][string[]]$Paths)

    foreach ($path in $Paths) {
        $value = Get-PathValue -Object $Object -Path $path
        if ($null -ne $value) {
            if ($value -is [string] -and [string]::IsNullOrWhiteSpace([string]$value)) { continue }
            return $value
        }
    }
    return $null
}

function Get-DisplayValue {
    param($Value)

    if ($null -eq $Value) { return $notReturned }
    if ($Value -is [string]) {
        if ([string]::IsNullOrWhiteSpace([string]$Value)) { return $notReturned }
        return [string]$Value
    }
    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        try { return ($Value | ConvertTo-Json -Depth 50 -Compress) } catch { return [string]$Value }
    }
    if ($Value.PSObject -and $Value.PSObject.Properties.Count -gt 0 -and -not $Value.GetType().IsPrimitive) {
        try { return ($Value | ConvertTo-Json -Depth 50 -Compress) } catch { return [string]$Value }
    }
    return [string]$Value
}

function Get-ResponseItems {
    param($Response, [string[]]$ContainerNames)

    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }

    foreach ($name in @($ContainerNames)) {
        $member = Get-ExactMember -Object $Response -Name $name
        if ($null -ne $member -and $null -ne $member.Value) {
            return @($member.Value)
        }
    }

    return @($Response)
}

function Add-PropertyPathsRecursive {
    param(
        $Value,
        [string]$Path,
        [System.Collections.Generic.HashSet[string]]$Set
    )

    if ($null -eq $Value) { return }
    if ($Value -is [string] -or $Value -is [datetime] -or $Value -is [decimal]) { return }
    if ($Value.GetType().IsPrimitive) { return }

    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string]) -and -not ($Value -is [pscustomobject])) {
        foreach ($item in @($Value)) {
            $arrayPath = if ([string]::IsNullOrWhiteSpace($Path)) { '[]' } else { "$Path[]" }
            Add-PropertyPathsRecursive -Value $item -Path $arrayPath -Set $Set
        }
        return
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        $propertyPath = if ([string]::IsNullOrWhiteSpace($Path)) { $property.Name } else { "$Path.$($property.Name)" }
        [void]$Set.Add($propertyPath)
        Add-PropertyPathsRecursive -Value $property.Value -Path $propertyPath -Set $Set
    }
}

function Add-FieldInventory {
    param(
        [string]$Cluster,
        [string]$Version,
        [string]$Endpoint,
        $Json
    )

    if ($null -eq $Json) { return }
    $set = New-Object 'System.Collections.Generic.HashSet[string]'
    Add-PropertyPathsRecursive -Value $Json -Path '' -Set $set
    foreach ($path in @($set | Sort-Object)) {
        $fieldInventoryRows.Add([pscustomobject][ordered]@{
            Cluster      = $Cluster
            Version      = $Version
            Endpoint     = $Endpoint
            PropertyPath = $path
        })
    }
}

function Get-MatchingPropertiesRecursive {
    param(
        $Value,
        [string]$Path,
        [regex]$NamePattern,
        [System.Collections.Generic.List[string]]$Results
    )

    if ($null -eq $Value) { return }
    if ($Value -is [string] -or $Value -is [datetime] -or $Value -is [decimal]) { return }
    if ($Value.GetType().IsPrimitive) { return }

    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string]) -and -not ($Value -is [pscustomobject])) {
        $index = 0
        foreach ($item in @($Value)) {
            Get-MatchingPropertiesRecursive -Value $item -Path ("{0}[{1}]" -f $Path,$index) -NamePattern $NamePattern -Results $Results
            $index++
        }
        return
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        $propertyPath = if ([string]::IsNullOrWhiteSpace($Path)) { $property.Name } else { "$Path.$($property.Name)" }
        if ($NamePattern.IsMatch($property.Name)) {
            $Results.Add(("{0}={1}" -f $propertyPath,(Get-DisplayValue $property.Value)))
        }
        Get-MatchingPropertiesRecursive -Value $property.Value -Path $propertyPath -NamePattern $NamePattern -Results $Results
    }
}

function Get-MatchingPropertyText {
    param($Object, [Parameter(Mandatory=$true)][string]$Pattern)

    if ($null -eq $Object) { return $notReturned }
    $results = New-Object System.Collections.Generic.List[string]
    Get-MatchingPropertiesRecursive -Value $Object -Path '' -NamePattern ([regex]$Pattern) -Results $results
    if ($results.Count -eq 0) { return $notReturned }
    return (($results | Sort-Object -Unique) -join '; ')
}

function Resolve-PhysicalVirtual {
    param($ClusterObject)

    $isVirtual = Get-FirstPathValue -Object $ClusterObject -Paths @('isVirtual','isVirtualEdition')
    if ($null -ne $isVirtual) {
        if ([System.Convert]::ToBoolean($isVirtual)) { return 'Virtual' }
        return 'Physical'
    }

    $isPhysical = Get-FirstPathValue -Object $ClusterObject -Paths @('isPhysical')
    if ($null -ne $isPhysical) {
        if ([System.Convert]::ToBoolean($isPhysical)) { return 'Physical' }
        return 'Virtual'
    }

    $type = Get-FirstPathValue -Object $ClusterObject -Paths @('clusterType','type')
    if ($null -ne $type) {
        $text = [string]$type
        if ($text -match '(?i)virtual') { return 'Virtual' }
        if ($text -match '(?i)physical') { return 'Physical' }
    }

    return $notReturned
}

function Get-ReportedNodeCount {
    param($ClusterObject)

    $count = Get-FirstPathValue -Object $ClusterObject -Paths @('nodeCount','numNodes','nodesCount')
    if ($null -ne $count) { return $count }

    $nodes = Get-PathValue -Object $ClusterObject -Path 'nodes'
    if ($null -ne $nodes) { return @($nodes).Count }

    return $null
}

function Get-CurrentEcText {
    param($StorageDomain)

    $ecObject = Get-FirstPathValue -Object $StorageDomain -Paths @(
        'storagePolicy.erasureCodingParams',
        'storagePolicy.erasureCodingInfo',
        'erasureCodingParams',
        'erasureCodingInfo'
    )

    if ($null -ne $ecObject) {
        $data = Get-FirstPathValue -Object $ecObject -Paths @('numDataStripes','dataStripes')
        $coded = Get-FirstPathValue -Object $ecObject -Paths @('numCodedStripes','codedStripes')
        if ($null -ne $data -and $null -ne $coded) {
            return ('{0}:{1}' -f $data,$coded)
        }
    }

    return (Get-MatchingPropertyText -Object $StorageDomain -Pattern '(?i)(erasure|coding|numDataStripes|numCodedStripes)')
}

function Invoke-ApprovedGet {
    param(
        [Parameter(Mandatory=$true)][string]$Uri,
        [Parameter(Mandatory=$true)][hashtable]$Headers,
        [Parameter(Mandatory=$true)][string]$RawPath
    )

    $parsedUri = [uri]$Uri
    $baseUri = [uri]$baseUrl

    if ($parsedUri.Scheme -cne $baseUri.Scheme -or $parsedUri.Host -cne $baseUri.Host -or $parsedUri.Port -ne $baseUri.Port) {
        throw "Safety block: URI is outside approved Helios base URL: $Uri"
    }
    if ($approvedGetPaths -cnotcontains $parsedUri.AbsolutePath) {
        throw "Safety block: endpoint is not approved for GET collection: $($parsedUri.AbsolutePath)"
    }

    # Mandatory safety verification immediately before every API request.
    $httpMethod = 'GET'
    if ($httpMethod -cne 'GET') {
        throw "SAFETY BLOCK: HTTP method is not GET: $httpMethod"
    }

    try {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method $httpMethod -UseBasicParsing -ErrorAction Stop
        $content = [string]$response.Content
        $rawParent = Split-Path -Parent $RawPath
        if (-not (Test-Path $rawParent -PathType Container)) {
            New-Item -Path $rawParent -ItemType Directory -Force | Out-Null
        }
        $content | Set-Content -Path $RawPath -Encoding UTF8

        $json = $null
        if (-not [string]::IsNullOrWhiteSpace($content)) {
            $json = $content | ConvertFrom-Json
        }

        return [pscustomobject][ordered]@{
            Success    = $true
            StatusCode = [int]$response.StatusCode
            Json       = $json
            RawPath    = $RawPath
            Error      = $null
        }
    }
    catch {
        $statusCode = $null
        try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
        $errorPath = [System.IO.Path]::ChangeExtension($RawPath,'.error.txt')
        ("GET {0}`r`nStatusCode: {1}`r`nError: {2}" -f $Uri,$statusCode,$_.Exception.Message) | Set-Content -Path $errorPath -Encoding UTF8

        return [pscustomobject][ordered]@{
            Success    = $false
            StatusCode = $statusCode
            Json       = $null
            RawPath    = $errorPath
            Error      = $_.Exception.Message
        }
    }
}

function Add-ApiStatus {
    param(
        [string]$Cluster,
        [string]$Version,
        [string]$Endpoint,
        $Result,
        [string]$Notes
    )

    $availability = if ($Result.Success) { 'Available' } else { 'Unavailable' }
    $apiStatusRows.Add([pscustomobject][ordered]@{
        Cluster      = $Cluster
        Version      = $Version
        Endpoint     = $Endpoint
        Availability = $availability
        StatusCode   = $Result.StatusCode
        Notes        = $Notes
        EvidenceFile = $Result.RawPath
    })

    if (-not $Result.Success) {
        $getFailureRows.Add([pscustomobject][ordered]@{
            Cluster    = $Cluster
            Version    = $Version
            Endpoint   = $Endpoint
            StatusCode = $Result.StatusCode
            Error      = $Result.Error
        })
    }
}

# -----------------------------------------------------------------------------
# Helios cluster inventory: GET only, used only to enumerate accessible clusters.
# -----------------------------------------------------------------------------
$mcmRaw = Join-Path $rawRoot '00_Helios_Cluster_Inventory.json'
$mcmResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers '') -RawPath $mcmRaw
Add-ApiStatus -Cluster 'HELIOS' -Version $notReturned -Endpoint 'GET /v2/mcm/cluster-mgmt/info' -Result $mcmResult -Notes 'Cluster enumeration only'
if (-not $mcmResult.Success) {
    throw 'Unable to enumerate clusters from Helios using approved GET endpoint.'
}

$inventoryItems = Get-ResponseItems -Response $mcmResult.Json -ContainerNames @('cohesityClusters','clusters')
$clusterInventory = @()
foreach ($item in $inventoryItems) {
    $clusterId = Get-FirstPathValue -Object $item -Paths @('clusterId','id')
    if ($null -eq $clusterId) { continue }
    $clusterInventory += [pscustomobject][ordered]@{
        Id            = [string]$clusterId
        InventoryName = Get-FirstPathValue -Object $item -Paths @('clusterName','name')
    }
}
$clusterInventory = @($clusterInventory | Sort-Object Id -Unique)
if ($clusterInventory.Count -eq 0) {
    throw 'No clusters returned by Helios cluster inventory GET.'
}

$clusterIndex = 0
foreach ($inventoryCluster in $clusterInventory) {
    $clusterIndex++
    $clusterAlias = 'Cluster-{0:D2}' -f $clusterIndex
    $clusterId = [string]$inventoryCluster.Id
    $headers = New-Headers $clusterId
    $clusterDir = Join-Path $rawRoot ("{0}_{1}" -f $clusterAlias,(Get-SafeFileName $clusterId))
    New-Item -Path $clusterDir -ItemType Directory -Force | Out-Null

    Write-Host ("Collecting {0} ({1})" -f $clusterAlias,$inventoryCluster.InventoryName) -ForegroundColor Yellow

    # 1. VERSION MUST BE COLLECTED FIRST FOR EACH CLUSTER.
    $clusterEndpoint = 'GET /v2/clusters?includeMinimumNodesInfo=true'
    $clusterResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/clusters?includeMinimumNodesInfo=true" -Headers $headers -RawPath (Join-Path $clusterDir '01_clusters_includeMinimumNodesInfo.json')

    $clusterObjects = if ($clusterResult.Success) { Get-ResponseItems -Response $clusterResult.Json -ContainerNames @('clusters') } else { @() }
    $clusterObject = $null
    if ($clusterObjects.Count -gt 0) {
        $matching = @($clusterObjects | Where-Object {
            $candidateId = Get-FirstPathValue -Object $_ -Paths @('id','clusterId')
            $null -ne $candidateId -and [string]$candidateId -eq $clusterId
        })
        if ($matching.Count -gt 0) { $clusterObject = $matching[0] } else { $clusterObject = $clusterObjects[0] }
    }

    $clusterNameValue = Get-FirstPathValue -Object $clusterObject -Paths @('name','clusterName')
    $clusterName = if ($null -ne $clusterNameValue) { [string]$clusterNameValue } elseif ($null -ne $inventoryCluster.InventoryName) { [string]$inventoryCluster.InventoryName } else { $clusterAlias }
    $versionValue = Get-FirstPathValue -Object $clusterObject -Paths @('softwareVersion','clusterSoftwareVersion','version')
    $version = if ($null -ne $versionValue) { [string]$versionValue } else { $notReturned }
    $reportedNodeCountValue = Get-ReportedNodeCount -ClusterObject $clusterObject
    $reportedNodeCount = if ($null -ne $reportedNodeCountValue) { $reportedNodeCountValue } else { $notReturned }
    $physicalVirtual = Resolve-PhysicalVirtual -ClusterObject $clusterObject
    $clusterFailureDomainInfo = Get-MatchingPropertyText -Object $clusterObject -Pattern '(?i)(fault|failure|domain|minimum)'

    Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint $clusterEndpoint -Result $clusterResult -Notes 'Version-detection request; executed first within cluster context'
    if ($clusterResult.Success) { Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/clusters' -Json $clusterResult.Json }

    # 2. Chassis inventory - all chassis.
    $chassisResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/chassis" -Headers $headers -RawPath (Join-Path $clusterDir '02_chassis_all.json')
    Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/chassis' -Result $chassisResult -Notes 'All chassis'
    if ($chassisResult.Success) { Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/chassis' -Json $chassisResult.Json }
    $chassis = if ($chassisResult.Success) { @(Get-ResponseItems -Response $chassisResult.Json -ContainerNames @('chassis','chassisList')) } else { @() }

    # 2b. Chassis explicitly reported as having no rack assignment.
    $noRackResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/chassis?noRackAssigned=true" -Headers $headers -RawPath (Join-Path $clusterDir '03_chassis_noRackAssigned.json')
    Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/chassis?noRackAssigned=true' -Result $noRackResult -Notes 'Explicit no-rack-assigned query'
    if ($noRackResult.Success) { Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/chassis?noRackAssigned=true' -Json $noRackResult.Json }
    $noRackChassis = if ($noRackResult.Success) { @(Get-ResponseItems -Response $noRackResult.Json -ContainerNames @('chassis','chassisList')) } else { @() }

    # 3. Existing Cohesity rack configuration.
    $racksResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/racks" -Headers $headers -RawPath (Join-Path $clusterDir '04_racks.json')
    Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/racks' -Result $racksResult -Notes 'Existing Cohesity rack objects only; no creation or assignment'
    if ($racksResult.Success) { Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/racks' -Json $racksResult.Json }
    $racks = if ($racksResult.Success) { @(Get-ResponseItems -Response $racksResult.Json -ContainerNames @('racks','rackList')) } else { @() }

    # 5. Node membership.
    $nodesResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/clusters/nodes" -Headers $headers -RawPath (Join-Path $clusterDir '05_cluster_nodes.json')
    Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/clusters/nodes' -Result $nodesResult -Notes 'Node inventory for Node -> Chassis -> Cohesity Rack validation'
    if ($nodesResult.Success) { Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/clusters/nodes' -Json $nodesResult.Json }
    $nodes = if ($nodesResult.Success) { @(Get-ResponseItems -Response $nodesResult.Json -ContainerNames @('nodes')) } else { @() }

    # 6. Current Storage Domain configuration.
    $storageResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/storage-domains" -Headers $headers -RawPath (Join-Path $clusterDir '06_storage_domains.json')
    Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/storage-domains' -Result $storageResult -Notes 'Complete Storage Domain response retained'
    if ($storageResult.Success) { Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/storage-domains' -Json $storageResult.Json }
    $storageDomains = if ($storageResult.Success) { @(Get-ResponseItems -Response $storageResult.Json -ContainerNames @('storageDomains','domains')) } else { @() }

    # Build rack lookup using only explicit Cohesity rack objects.
    $rackById = @{}
    foreach ($rack in $racks) {
        $rackIdValue = Get-FirstPathValue -Object $rack -Paths @('id','rackId')
        if ($null -ne $rackIdValue) { $rackById[[string]$rackIdValue] = $rack }
    }

    # Build chassis lookup.
    $chassisById = @{}
    foreach ($ch in $chassis) {
        $chassisIdValue = Get-FirstPathValue -Object $ch -Paths @('id','chassisId')
        if ($null -ne $chassisIdValue) { $chassisById[[string]$chassisIdValue] = $ch }
    }

    # 4. One relationship row per chassis.
    foreach ($ch in $chassis) {
        $chassisIdValue = Get-FirstPathValue -Object $ch -Paths @('id','chassisId')
        $chassisIdText = if ($null -ne $chassisIdValue) { [string]$chassisIdValue } else { $notReturned }
        $nodeIdsValue = Get-FirstPathValue -Object $ch -Paths @('nodeIds')
        $nodeIds = if ($null -ne $nodeIdsValue) { @($nodeIdsValue) } else { @() }
        $rackIdValue = Get-FirstPathValue -Object $ch -Paths @('rackId')
        $rackIdText = if ($null -ne $rackIdValue) { [string]$rackIdValue } else { $notReturned }
        $rackObject = $null
        if ($null -ne $rackIdValue -and $rackById.ContainsKey([string]$rackIdValue)) { $rackObject = $rackById[[string]$rackIdValue] }

        $relationshipRows.Add([pscustomobject][ordered]@{
            Cluster              = $clusterName
            Version              = $version
            ChassisID            = $chassisIdText
            ChassisSerial        = Get-DisplayValue (Get-FirstPathValue -Object $ch -Paths @('serialNumber'))
            Model                = Get-DisplayValue (Get-FirstPathValue -Object $ch -Paths @('hardwareModel'))
            NodeIDs              = if ($nodeIds.Count -gt 0) { ($nodeIds -join ',') } else { $notReturned }
            NodeCount            = if ($null -ne $nodeIdsValue) { $nodeIds.Count } else { $notReturned }
            CohesityRackID       = $rackIdText
            CohesityRackName     = Get-DisplayValue (Get-FirstPathValue -Object $rackObject -Paths @('name','rackName'))
            ChassisLocation      = Get-DisplayValue (Get-FirstPathValue -Object $ch -Paths @('location'))
            RackLocation         = Get-DisplayValue (Get-FirstPathValue -Object $rackObject -Paths @('location'))
            ChassisName          = Get-DisplayValue (Get-FirstPathValue -Object $ch -Paths @('name'))
            ChassisNodeBase      = Get-DisplayValue (Get-FirstPathValue -Object $ch -Paths @('chassisNodeBase'))
            RackMappingValidated = if ($null -ne $rackObject) { 'YES - EXPLICIT COHESITY RACK OBJECT' } else { 'NO' }
        })
    }

    # 5. Validate Node -> Chassis -> Cohesity Rack using explicit node/chassis data only.
    foreach ($node in $nodes) {
        $nodeIdValue = Get-FirstPathValue -Object $node -Paths @('id','nodeId')
        $nodeIdText = if ($null -ne $nodeIdValue) { [string]$nodeIdValue } else { $notReturned }
        $nodeChassisId = Get-FirstPathValue -Object $node -Paths @('chassisId','chassisInfo.chassisId')
        $chassisSource = if ($null -ne $nodeChassisId) { 'Node API chassis field' } else { $notReturned }

        if ($null -eq $nodeChassisId -and $null -ne $nodeIdValue) {
            foreach ($candidateChassis in $chassis) {
                $candidateNodeIdsValue = Get-FirstPathValue -Object $candidateChassis -Paths @('nodeIds')
                if ($null -eq $candidateNodeIdsValue) { continue }
                if (@($candidateNodeIdsValue) -contains $nodeIdValue -or @($candidateNodeIdsValue | ForEach-Object { [string]$_ }) -contains [string]$nodeIdValue) {
                    $nodeChassisId = Get-FirstPathValue -Object $candidateChassis -Paths @('id','chassisId')
                    $chassisSource = 'GET /v2/chassis nodeIds'
                    break
                }
            }
        }

        $nodeChassis = $null
        if ($null -ne $nodeChassisId -and $chassisById.ContainsKey([string]$nodeChassisId)) { $nodeChassis = $chassisById[[string]$nodeChassisId] }
        $nodeRackId = Get-FirstPathValue -Object $nodeChassis -Paths @('rackId')
        $nodeRack = $null
        if ($null -ne $nodeRackId -and $rackById.ContainsKey([string]$nodeRackId)) { $nodeRack = $rackById[[string]$nodeRackId] }

        $nodeMembershipRows.Add([pscustomobject][ordered]@{
            Cluster          = $clusterName
            Version          = $version
            NodeID           = $nodeIdText
            HostName         = Get-DisplayValue (Get-FirstPathValue -Object $node -Paths @('hostName','hostname','name'))
            NodeIP           = Get-DisplayValue (Get-FirstPathValue -Object $node -Paths @('ip','ipAddress'))
            ChassisID        = if ($null -ne $nodeChassisId) { [string]$nodeChassisId } else { $notReturned }
            ChassisIDSource  = $chassisSource
            CohesityRackID   = if ($null -ne $nodeRackId) { [string]$nodeRackId } else { $notReturned }
            CohesityRackName = Get-DisplayValue (Get-FirstPathValue -Object $nodeRack -Paths @('name','rackName'))
            MappingValidated = if ($null -ne $nodeRack) { 'YES - EXPLICIT COHESITY RACK OBJECT' } else { 'NO' }
        })
    }

    # 7. Fault Tolerance options: execute the approved GET for every Storage Domain.
    $sdFtSummaries = New-Object System.Collections.Generic.List[string]
    $sdEcSummaries = New-Object System.Collections.Generic.List[string]
    $rackFtOptionSummaries = New-Object System.Collections.Generic.List[string]
    $minimumDomainSummaries = New-Object System.Collections.Generic.List[string]

    $sdIndex = 0
    foreach ($sd in $storageDomains) {
        $sdIndex++
        $sdIdValue = Get-FirstPathValue -Object $sd -Paths @('id','storageDomainId')
        $sdNameValue = Get-FirstPathValue -Object $sd -Paths @('name','storageDomainName')
        $sdIdText = if ($null -ne $sdIdValue) { [string]$sdIdValue } else { $notReturned }
        $sdNameText = if ($null -ne $sdNameValue) { [string]$sdNameValue } else { "StorageDomain-$sdIndex" }
        $storagePolicy = Get-FirstPathValue -Object $sd -Paths @('storagePolicy')
        $currentEc = Get-CurrentEcText -StorageDomain $sd
        $sdFtInfo = Get-MatchingPropertyText -Object $sd -Pattern '(?i)(fault|toler)'
        $sdFailureDomainInfo = Get-MatchingPropertyText -Object $sd -Pattern '(?i)(failure.*domain|domain.*failure|minimum.*domain)'

        $ftResult = $null
        $ftRelevant = $notReturned
        $ftMinimum = $notReturned
        $ftAvailability = 'Unavailable'

        if ($null -ne $sdIdValue) {
            $encodedSdId = [uri]::EscapeDataString([string]$sdIdValue)
            $safeSd = Get-SafeFileName ([string]$sdIdValue)
            $ftResult = Invoke-ApprovedGet -Uri "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$encodedSdId" -Headers $headers -RawPath (Join-Path $clusterDir ("07_ft_options_SD_{0}.json" -f $safeSd))
            Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/storage-domains/fault-tolerance-options' -Result $ftResult -Notes ("StorageDomainId={0}" -f $sdIdText)
            if ($ftResult.Success) {
                $ftAvailability = 'Available'
                Add-FieldInventory -Cluster $clusterName -Version $version -Endpoint 'GET /v2/storage-domains/fault-tolerance-options' -Json $ftResult.Json
                $ftRelevant = Get-MatchingPropertyText -Object $ftResult.Json -Pattern '(?i)(fault|toler|domain|disabled|warning|supported|default|current)'
                $ftMinimum = Get-MatchingPropertyText -Object $ftResult.Json -Pattern '(?i)(min.*domain|minimum.*domain|failureDomainCount)'
            }
        }
        else {
            $synthetic = [pscustomobject]@{ Success=$false; StatusCode=$null; RawPath=$notReturned; Error='Storage Domain ID not returned; FT options GET cannot be formed without inventing an ID.' }
            Add-ApiStatus -Cluster $clusterName -Version $version -Endpoint 'GET /v2/storage-domains/fault-tolerance-options' -Result $synthetic -Notes 'NOT AVAILABLE THROUGH APPROVED READ-ONLY COLLECTION: Storage Domain ID not returned'
        }

        $storageDomainRows.Add([pscustomobject][ordered]@{
            Cluster                  = $clusterName
            Version                  = $version
            StorageDomainID          = $sdIdText
            StorageDomainName        = $sdNameText
            StoragePolicy            = Get-DisplayValue $storagePolicy
            CurrentEC                = $currentEc
            CurrentStorageDomainFT   = $sdFtInfo
            FailureDomainInformation = $sdFailureDomainInfo
            FTOptionsAPI             = $ftAvailability
            RackFTOptionsReported    = $ftRelevant
            MinimumFailureDomains    = $ftMinimum
        })

        $sdFtSummaries.Add(("{0}={1}" -f $sdNameText,$sdFtInfo))
        $sdEcSummaries.Add(("{0}={1}" -f $sdNameText,$currentEc))
        $rackFtOptionSummaries.Add(("{0}={1}" -f $sdNameText,$ftRelevant))
        $minimumDomainSummaries.Add(("{0}={1}" -f $sdNameText,$ftMinimum))
    }

    $configuredRackNames = @()
    $configuredRackLocations = @()
    foreach ($rack in $racks) {
        $rn = Get-FirstPathValue -Object $rack -Paths @('name','rackName')
        $rl = Get-FirstPathValue -Object $rack -Paths @('location')
        if ($null -ne $rn) { $configuredRackNames += (Get-DisplayValue $rn) }
        if ($null -ne $rl) { $configuredRackLocations += (Get-DisplayValue $rl) }
    }

    $chassisLocations = @()
    $chassisWithRackId = 0
    $chassisWithoutRackId = 0
    $fullyMapped = ($chassis.Count -gt 0)
    foreach ($ch in $chassis) {
        $loc = Get-FirstPathValue -Object $ch -Paths @('location')
        if ($null -ne $loc) { $chassisLocations += (Get-DisplayValue $loc) }

        $rackIdValue = Get-FirstPathValue -Object $ch -Paths @('rackId')
        if ($null -ne $rackIdValue) {
            $chassisWithRackId++
            if (-not $rackById.ContainsKey([string]$rackIdValue)) { $fullyMapped = $false }
        }
        else {
            $chassisWithoutRackId++
            $fullyMapped = $false
        }
    }

    $rackConfigState = if ($racks.Count -eq 0) { 'NO COHESITY RACK CONFIGURATION PRESENT' } else { [string]$racks.Count }
    $physicalMappingKnown = if ($fullyMapped -and $racks.Count -gt 0) { 'YES' } else { 'NO' }
    $collectedNodeCount = $nodes.Count

    $clusterSummaryRows.Add([pscustomobject][ordered]@{
        Cluster                                             = $clusterName
        Version                                             = $version
        PhysicalVirtual                                     = $physicalVirtual
        NodesReportedByClustersAPI                           = $reportedNodeCount
        NodesCollectedFromNodeAPI                            = $collectedNodeCount
        Chassis                                             = $chassis.Count
        CohesityRacksConfigured                              = $rackConfigState
        ChassisWithRackId                                    = $chassisWithRackId
        ChassisWithoutRackId                                 = $chassisWithoutRackId
        NoRackAssignedEndpointCount                          = if ($noRackResult.Success) { $noRackChassis.Count } else { $notReturned }
        RackNames                                            = if ($configuredRackNames.Count -gt 0) { ($configuredRackNames | Sort-Object -Unique) -join '; ' } else { $notReturned }
        RackLocations                                        = if ($configuredRackLocations.Count -gt 0) { ($configuredRackLocations | Sort-Object -Unique) -join '; ' } else { $notReturned }
        ChassisLocations                                     = if ($chassisLocations.Count -gt 0) { ($chassisLocations | Sort-Object -Unique) -join '; ' } else { $notReturned }
        CurrentFailureDomainInformation                      = $clusterFailureDomainInfo
        CurrentStorageDomainFT                               = if ($sdFtSummaries.Count -gt 0) { $sdFtSummaries -join ' || ' } else { $notReturned }
        CurrentEC                                            = if ($sdEcSummaries.Count -gt 0) { $sdEcSummaries -join ' || ' } else { $notReturned }
        RackFTOptionsReportedByAPI                           = if ($rackFtOptionSummaries.Count -gt 0) { $rackFtOptionSummaries -join ' || ' } else { $notReturned }
        MinimumFailureDomainsReportedByAPI                   = if ($minimumDomainSummaries.Count -gt 0) { $minimumDomainSummaries -join ' || ' } else { $notReturned }
        PhysicalChassisToDatacenterRackMappingKnownFromCohesity = $physicalMappingKnown
    })
}

# -----------------------------------------------------------------------------
# VERSION-SPECIFIC FINDINGS based only on responses observed in this run.
# No difference is attributed to software version unless the actual API calls
# demonstrate different endpoint availability. Field presence is reported as
# OBSERVED / NOT OBSERVED, not claimed as a version limitation.
# -----------------------------------------------------------------------------
$versionEndpointRows = New-Object System.Collections.Generic.List[object]
$clusterApiRows = @($apiStatusRows | Where-Object { $_.Cluster -ne 'HELIOS' })
foreach ($group in @($clusterApiRows | Group-Object Version,Endpoint)) {
    $items = @($group.Group)
    $successes = @($items | Where-Object { $_.Availability -eq 'Available' }).Count
    $failures = @($items | Where-Object { $_.Availability -eq 'Unavailable' }).Count
    $status = if ($successes -gt 0 -and $failures -eq 0) {
        'AVAILABLE IN OBSERVED RESPONSES'
    }
    elseif ($successes -eq 0 -and $failures -gt 0) {
        'UNAVAILABLE IN OBSERVED RESPONSES'
    }
    else {
        'MIXED IN OBSERVED RESPONSES'
    }

    $versionEndpointRows.Add([pscustomobject][ordered]@{
        Version   = $items[0].Version
        ItemType  = 'API'
        Item      = $items[0].Endpoint
        Status    = $status
        Evidence  = ("Successful GETs={0}; Failed GETs={1}; observed in this run" -f $successes,$failures)
    })
}

$versionFieldRows = New-Object System.Collections.Generic.List[object]
$versions = @($fieldInventoryRows | Select-Object -ExpandProperty Version -Unique | Sort-Object)
$endpoints = @($fieldInventoryRows | Select-Object -ExpandProperty Endpoint -Unique | Sort-Object)
foreach ($endpoint in $endpoints) {
    $endpointRows = @($fieldInventoryRows | Where-Object { $_.Endpoint -eq $endpoint })
    $allPaths = @($endpointRows | Select-Object -ExpandProperty PropertyPath -Unique | Sort-Object)
    foreach ($path in $allPaths) {
        foreach ($version in $versions) {
            $observed = @($endpointRows | Where-Object { $_.Version -eq $version -and $_.PropertyPath -eq $path }).Count -gt 0
            $versionFieldRows.Add([pscustomobject][ordered]@{
                Version      = $version
                Endpoint     = $endpoint
                PropertyPath = $path
                Presence     = if ($observed) { 'OBSERVED' } else { 'NOT OBSERVED' }
                Evidence     = 'Actual raw GET responses from this run; presence alone is not attributed to version.'
            })
        }
    }
}

# Output files.
$relationshipCsv   = Join-Path $runDir 'Chassis_Rack_Relationship.csv'
$nodeCsv           = Join-Path $runDir 'Node_Chassis_Rack_Membership.csv'
$storageCsv        = Join-Path $runDir 'Storage_Domain_Assessment.csv'
$clusterCsv        = Join-Path $runDir 'Cluster_Rack_Assessment.csv'
$statusCsv         = Join-Path $runDir 'API_Collection_Status.csv'
$fieldCsv          = Join-Path $runDir 'Field_Inventory.csv'
$versionApiCsv     = Join-Path $runDir 'Version_Specific_Endpoint_Findings.csv'
$versionFieldCsv   = Join-Path $runDir 'Version_Specific_Field_Presence.csv'
$failureCsv        = Join-Path $runDir 'GET_Failures.csv'
$summaryTxt        = Join-Path $runDir 'Rack_Resiliency_Summary.txt'

$relationshipRows   | Export-Csv -Path $relationshipCsv -NoTypeInformation -Encoding UTF8
$nodeMembershipRows | Export-Csv -Path $nodeCsv -NoTypeInformation -Encoding UTF8
$storageDomainRows  | Export-Csv -Path $storageCsv -NoTypeInformation -Encoding UTF8
$clusterSummaryRows | Export-Csv -Path $clusterCsv -NoTypeInformation -Encoding UTF8
$apiStatusRows      | Export-Csv -Path $statusCsv -NoTypeInformation -Encoding UTF8
$fieldInventoryRows | Export-Csv -Path $fieldCsv -NoTypeInformation -Encoding UTF8
$versionEndpointRows| Export-Csv -Path $versionApiCsv -NoTypeInformation -Encoding UTF8
$versionFieldRows   | Export-Csv -Path $versionFieldCsv -NoTypeInformation -Encoding UTF8
$getFailureRows     | Export-Csv -Path $failureCsv -NoTypeInformation -Encoding UTF8

$summary = New-Object System.Collections.Generic.List[string]
$summary.Add('COHESITY RACK RESILIENCY - READ-ONLY COLLECTION SUMMARY')
$summary.Add('')
$summary.Add('SAFETY')
$summary.Add('HTTP method permitted by collector: GET only')
$summary.Add('Cohesity write operations executed: 0')
$summary.Add('')
$summary.Add('CLUSTER ASSESSMENT')
$summary.Add(($clusterSummaryRows | Format-List * | Out-String -Width 500).TrimEnd())
$summary.Add('')
$summary.Add('VERSION-SPECIFIC FINDINGS')
$summary.Add('These findings are based only on API responses observed in this run. Field presence is not automatically attributed to version.')
$summary.Add(($versionEndpointRows | Sort-Object Version,Item | Format-Table Version,ItemType,Item,Status,Evidence -AutoSize -Wrap | Out-String -Width 500).TrimEnd())
$summary.Add('')
$summary.Add(("Detailed observed field-presence matrix: {0}" -f $versionFieldCsv))
$summary.Add(("Complete raw JSON responses: {0}" -f $rawRoot))
$summary.Add('')
$summary.Add(("GET failures: {0}" -f $getFailureRows.Count))
$summary.Add(("Output directory: {0}" -f $runDir))
$summary | Set-Content -Path $summaryTxt -Encoding UTF8

Write-Host ''
Write-Host 'CLUSTER RACK ASSESSMENT' -ForegroundColor Cyan
$clusterSummaryRows | Format-Table Cluster,Version,PhysicalVirtual,NodesCollectedFromNodeAPI,Chassis,CohesityRacksConfigured,ChassisWithRackId,ChassisWithoutRackId,PhysicalChassisToDatacenterRackMappingKnownFromCohesity -AutoSize -Wrap | Out-Host
Write-Host ''
Write-Host 'VERSION-SPECIFIC FINDINGS' -ForegroundColor Cyan
$versionEndpointRows | Sort-Object Version,Item | Format-Table Version,Item,Status -AutoSize -Wrap | Out-Host
Write-Host ''
Write-Host ("GET failures: {0}" -f $getFailureRows.Count) -ForegroundColor Green
Write-Host ("Raw JSON: {0}" -f $rawRoot)
Write-Host ("Summary : {0}" -f $summaryTxt)
