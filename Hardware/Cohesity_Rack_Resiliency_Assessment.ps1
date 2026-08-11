# Cohesity Rack Resiliency Assessment - READ ONLY
# PowerShell 5.1 compatible
# ABSOLUTE RULE: HTTP GET ONLY. No Cohesity configuration changes are performed.

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$outputDirectory     = "X:\PowerShell\Data\Cohesity\RackResiliencyAssessment"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

$expectedClusterCount = 22
$expectedNodeCount    = 169
$notAvailable         = "NOT AVAILABLE THROUGH APPROVED READ-ONLY COLLECTION"

if (-not (Test-Path $outputDirectory -PathType Container)) {
    New-Item -Path $outputDirectory -ItemType Directory -Force | Out-Null
}
if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function New-CohesityHeaders {
    param([string]$AccessClusterId)

    $headers = @{
        accept = "application/json"
        apiKey = $apiKey
    }

    if (-not [string]::IsNullOrWhiteSpace($AccessClusterId)) {
        $headers["accessClusterId"] = $AccessClusterId
    }

    return $headers
}

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$ApiLabel
    )

    $method = "GET"
    if ($method -ne "GET") {
        throw "SAFETY BLOCK: Non-GET HTTP method requested for $ApiLabel"
    }

    try {
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
    catch {
        throw "$ApiLabel GET failed: $($_.Exception.Message)"
    }
}

function Get-PropertyValue {
    param(
        $Object,
        [string[]]$Names,
        $Default = $notAvailable
    )

    if ($null -eq $Object) {
        return $Default
    }

    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties |
            Where-Object { $_.Name -ieq $name } |
            Select-Object -First 1

        if ($property -and $null -ne $property.Value) {
            if ($property.Value -is [System.Array]) {
                if (@($property.Value).Count -gt 0) {
                    return $property.Value
                }
            }
            else {
                $text = ([string]$property.Value).Trim()
                if (-not [string]::IsNullOrWhiteSpace($text)) {
                    return $property.Value
                }
            }
        }
    }

    return $Default
}

function Get-ArrayProperty {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return @()
    }

    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties |
            Where-Object { $_.Name -ieq $name } |
            Select-Object -First 1

        if ($property -and $null -ne $property.Value) {
            return @($property.Value)
        }
    }

    return @()
}

function Normalize-Objects {
    param(
        $Response,
        [string[]]$WrapperNames,
        [string[]]$IdentityProperties
    )

    if ($null -eq $Response) {
        return @()
    }

    if ($Response -is [System.Array]) {
        return @($Response)
    }

    foreach ($wrapper in $WrapperNames) {
        $items = Get-ArrayProperty -Object $Response -Names @($wrapper)
        if ($items.Count -gt 0) {
            return @($items)
        }
    }

    foreach ($identity in $IdentityProperties) {
        if ($Response.PSObject.Properties[$identity]) {
            return @($Response)
        }
    }

    return @()
}

function To-Text {
    param($Value)

    if ($null -eq $Value) {
        return $notAvailable
    }

    if ($Value -is [System.Array]) {
        if (@($Value).Count -eq 0) {
            return $notAvailable
        }
        return (@($Value) -join "; ")
    }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $notAvailable
    }

    return $text
}

function Get-NumericValue {
    param($Value)

    if ($null -eq $Value) {
        return $null
    }

    $number = 0.0
    if ([double]::TryParse(([string]$Value), [ref]$number)) {
        return $number
    }

    return $null
}

function Convert-ToCapacityText {
    param($Value)

    $n = Get-NumericValue $Value
    if ($null -eq $n) {
        return (To-Text $Value)
    }

    if ($n -ge 1PB) {
        return ("{0:N2} PB" -f ($n / 1PB))
    }
    if ($n -ge 1TB) {
        return ("{0:N2} TB" -f ($n / 1TB))
    }
    if ($n -ge 1GB) {
        return ("{0:N2} GB" -f ($n / 1GB))
    }

    return ("{0:N0} B" -f $n)
}

function Get-HardwareBucket {
    param([string]$Model)

    switch -Regex ($Model) {
        '^CX8405$' { return 'CX8405' }
        '^C6025$'  { return 'C6025' }
        '^C5066$'  { return 'C5066' }
        '^C5026$'  { return 'C5026' }
        '^C5016$'  { return 'C5016' }
        default    { return 'Other' }
    }
}

function Escape-Markdown {
    param($Value)

    return ((To-Text $Value) -replace '\|','\\|')
}

function Get-BooleanTrue {
    param($Value)

    return ([string]$Value -match '^(?i:true|yes|1)$')
}

function Get-AssessmentFlag {
    param(
        [bool]$Incomplete,
        [bool]$NoRacks,
        [bool]$UnassignedChassis,
        [bool]$WarningOrSuboptimal,
        [bool]$InsufficientDomains,
        [bool]$UnevenDistribution,
        [bool]$MixedArchitecture
    )

    if ($Incomplete) {
        return "UNKNOWN"
    }
    if ($InsufficientDomains -or $WarningOrSuboptimal) {
        return "WARNING"
    }
    if ($NoRacks -or $UnassignedChassis -or $UnevenDistribution -or $MixedArchitecture) {
        return "REVIEW"
    }

    return "NORMAL"
}

$apiCalls = New-Object System.Collections.Generic.List[string]
$apiFailures = New-Object System.Collections.Generic.List[string]
$unavailableFields = New-Object System.Collections.Generic.HashSet[string]

$clusterRows = @()
$nodeRows = @()
$chassisRows = @()
$rackRows = @()
$storageDomainRows = @()
$ftOptionRows = @()
$internalClusterSummary = @()

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host " COHESITY RACK RESILIENCY ASSESSMENT - GET ONLY" -ForegroundColor White
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "Safety mode: HTTP GET only. No configuration changes." -ForegroundColor Green

try {
    $apiCalls.Add("GET /v2/clusters")
    $clusterResponse = Invoke-CohesityGet `
        -Uri "$baseUrl/v2/clusters" `
        -Headers (New-CohesityHeaders) `
        -ApiLabel "/v2/clusters"

    $clusterObjects = @(
        Normalize-Objects `
            -Response $clusterResponse `
            -WrapperNames @("clusters","items") `
            -IdentityProperties @("id","clusterId")
    )
}
catch {
    $apiFailures.Add("GET /v2/clusters : $($_.Exception.Message)")
    throw "Cannot continue without read-only cluster discovery. $($_.Exception.Message)"
}

if ($clusterObjects.Count -eq 0) {
    throw "GET /v2/clusters returned no usable cluster objects."
}

$clusterIndex = 0
foreach ($cluster in $clusterObjects) {
    $clusterId = To-Text (Get-PropertyValue $cluster @("id","clusterId"))

    if ($clusterId -eq $notAvailable) {
        continue
    }

    $clusterIndex++
    $clusterAlias = "Cluster-{0:D2}" -f $clusterIndex
    $clusterName = To-Text (Get-PropertyValue $cluster @("name","clusterName","displayName"))

    $clusterRows += [pscustomobject][ordered]@{
        ClusterAlias    = $clusterAlias
        ClusterId       = $clusterId
        ClusterName     = $clusterName
        SoftwareVersion = To-Text (Get-PropertyValue $cluster @("softwareVersion","version","clusterSoftwareVersion"))
        HardwareModels  = To-Text (Get-PropertyValue $cluster @("hardwareModels","hardwareModel"))
        NodeCount       = To-Text (Get-PropertyValue $cluster @("nodeCount","numNodes"))
        FailureDomain   = To-Text (Get-PropertyValue $cluster @("failureDomain","failureDomainType","faultToleranceLevel"))
    }
}

$clusterRows = @($clusterRows | Sort-Object ClusterAlias)

foreach ($clusterRow in $clusterRows) {
    $clusterId = $clusterRow.ClusterId
    $clusterAlias = $clusterRow.ClusterAlias
    $clusterName = $clusterRow.ClusterName
    $headers = New-CohesityHeaders -AccessClusterId $clusterId
    $clusterIncomplete = $false

    try {
        $apiCalls.Add("GET /v2/clusters/nodes [$clusterAlias]")
        $nodeResponse = Invoke-CohesityGet `
            -Uri "$baseUrl/v2/clusters/nodes" `
            -Headers $headers `
            -ApiLabel "/v2/clusters/nodes [$clusterAlias]"

        $nodes = @(
            Normalize-Objects `
                -Response $nodeResponse `
                -WrapperNames @("nodes","items","nodeList") `
                -IdentityProperties @("nodeId","id")
        )
    }
    catch {
        $apiFailures.Add("GET /v2/clusters/nodes [$clusterAlias] : $($_.Exception.Message)")
        $nodes = @()
        $clusterIncomplete = $true
    }

    foreach ($node in $nodes) {
        $nodeId = To-Text (Get-PropertyValue $node @("nodeId","id"))
        $model = To-Text (Get-PropertyValue $node @("productModel","nodeModel","hardwareModel"))

        $nodeRows += [pscustomobject][ordered]@{
            ClusterAlias     = $clusterAlias
            ClusterId        = $clusterId
            ClusterName      = $clusterName
            NodeId           = $nodeId
            ChassisId        = To-Text (Get-PropertyValue $node @("chassisId"))
            HardwareModel    = $model
            HardwareBucket   = Get-HardwareBucket $model
            NodeModel        = To-Text (Get-PropertyValue $node @("nodeModel"))
            ProductModel     = To-Text (Get-PropertyValue $node @("productModel"))
            ProductModelType = To-Text (Get-PropertyValue $node @("productModelType"))
            SlotNumber       = To-Text (Get-PropertyValue $node @("slotNumber","slot"))
            Status           = To-Text (Get-PropertyValue $node @("status","nodeStatus"))
            Reachability     = To-Text (Get-PropertyValue $node @("isReachable","reachable","reachability"))
            PhysicalCapacity = To-Text (Get-PropertyValue $node @("physicalCapacityBytes","physicalCapacity","capacityBytes"))
        }
    }

    try {
        $apiCalls.Add("GET /v2/node/hardware-info [$clusterAlias]")
        $hardwareResponse = Invoke-CohesityGet `
            -Uri "$baseUrl/v2/node/hardware-info" `
            -Headers $headers `
            -ApiLabel "/v2/node/hardware-info [$clusterAlias]"

        $hardwareObjects = @(
            Normalize-Objects `
                -Response $hardwareResponse `
                -WrapperNames @("hardwareInfo","hardwareInfos","items") `
                -IdentityProperties @("nodeModel","productModel","chassisModel")
        )

        foreach ($hardware in $hardwareObjects) {
            $hardwareNodeId = To-Text (Get-PropertyValue $hardware @("nodeId","id"))

            if ($hardwareNodeId -eq $notAvailable) {
                continue
            }

            $target = $nodeRows |
                Where-Object {
                    $_.ClusterAlias -eq $clusterAlias -and
                    $_.NodeId -eq $hardwareNodeId
                } |
                Select-Object -First 1

            if ($target) {
                $target.NodeModel = To-Text (Get-PropertyValue $hardware @("nodeModel") $target.NodeModel)
                $target.ProductModel = To-Text (Get-PropertyValue $hardware @("productModel") $target.ProductModel)
                $target.ProductModelType = To-Text (Get-PropertyValue $hardware @("productModelType") $target.ProductModelType)
                $target.SlotNumber = To-Text (Get-PropertyValue $hardware @("slotNumber") $target.SlotNumber)
            }
        }
    }
    catch {
        $apiFailures.Add("GET /v2/node/hardware-info [$clusterAlias] : $($_.Exception.Message)")
    }

    try {
        $apiCalls.Add("GET /v2/chassis [$clusterAlias]")
        $chassisResponse = Invoke-CohesityGet `
            -Uri "$baseUrl/v2/chassis" `
            -Headers $headers `
            -ApiLabel "/v2/chassis [$clusterAlias]"

        $chassisList = @(
            Normalize-Objects `
                -Response $chassisResponse `
                -WrapperNames @("chassis","items","chassisList") `
                -IdentityProperties @("id","chassisId","nodeIds")
        )
    }
    catch {
        $apiFailures.Add("GET /v2/chassis [$clusterAlias] : $($_.Exception.Message)")
        $chassisList = @()
        $clusterIncomplete = $true
    }

    foreach ($chassis in $chassisList) {
        $chassisId = To-Text (Get-PropertyValue $chassis @("id","chassisId"))
        $nodeIds = @(Get-ArrayProperty $chassis @("nodeIds")) | ForEach-Object { [string]$_ }
        $hardwareModel = To-Text (Get-PropertyValue $chassis @("hardwareModel","model","chassisModel"))
        $rackId = To-Text (Get-PropertyValue $chassis @("rackId"))

        $chassisRows += [pscustomobject][ordered]@{
            ClusterAlias   = $clusterAlias
            ClusterId      = $clusterId
            ClusterName    = $clusterName
            ChassisId      = $chassisId
            HardwareModel  = $hardwareModel
            HardwareBucket = Get-HardwareBucket $hardwareModel
            NodeCount      = $nodeIds.Count
            NodeIds        = ($nodeIds -join ";")
            RackId         = $rackId
            Location       = To-Text (Get-PropertyValue $chassis @("location"))
        }

        foreach ($nodeIdFromChassis in $nodeIds) {
            $targetNode = $nodeRows |
                Where-Object {
                    $_.ClusterAlias -eq $clusterAlias -and
                    $_.NodeId -eq [string]$nodeIdFromChassis
                } |
                Select-Object -First 1

            if ($targetNode) {
                $targetNode.ChassisId = $chassisId

                if ($targetNode.HardwareModel -eq $notAvailable) {
                    $targetNode.HardwareModel = $hardwareModel
                    $targetNode.HardwareBucket = Get-HardwareBucket $hardwareModel
                }
            }
        }
    }

    try {
        $apiCalls.Add("GET /v2/racks [$clusterAlias]")
        $rackResponse = Invoke-CohesityGet `
            -Uri "$baseUrl/v2/racks" `
            -Headers $headers `
            -ApiLabel "/v2/racks [$clusterAlias]"

        $racks = @(
            Normalize-Objects `
                -Response $rackResponse `
                -WrapperNames @("racks","items","rackList") `
                -IdentityProperties @("id","rackId")
        )
    }
    catch {
        $apiFailures.Add("GET /v2/racks [$clusterAlias] : $($_.Exception.Message)")
        $racks = @()
        $clusterIncomplete = $true
    }

    $rackCounter = 0
    foreach ($rack in $racks) {
        $rackCounter++
        $rackId = To-Text (Get-PropertyValue $rack @("id","rackId"))
        $rackAlias = "Rack-$rackCounter"
        $rackChassisIds = @(Get-ArrayProperty $rack @("chassisIds","chassisIdList")) | ForEach-Object { [string]$_ }

        if ($rackChassisIds.Count -eq 0 -and $rackId -ne $notAvailable) {
            $rackChassisIds = @(
                $chassisRows |
                    Where-Object {
                        $_.ClusterAlias -eq $clusterAlias -and
                        $_.RackId -eq $rackId
                    } |
                    Select-Object -ExpandProperty ChassisId
            )
        }

        $rackNodeIds = @()

        foreach ($cid in $rackChassisIds) {
            $rackNodeIds += @(
                $chassisRows |
                    Where-Object {
                        $_.ClusterAlias -eq $clusterAlias -and
                        $_.ChassisId -eq [string]$cid
                    } |
                    ForEach-Object {
                        if ($_.NodeIds) {
                            $_.NodeIds -split ";"
                        }
                    }
            )
        }

        $rackNodeIds = @(
            $rackNodeIds |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                Select-Object -Unique
        )

        $rackRows += [pscustomobject][ordered]@{
            ClusterAlias = $clusterAlias
            ClusterId    = $clusterId
            ClusterName  = $clusterName
            RackAlias    = $rackAlias
            RackId       = $rackId
            ChassisCount = $rackChassisIds.Count
            ChassisIds   = ($rackChassisIds -join ";")
            NodeCount    = $rackNodeIds.Count
            NodeIds      = ($rackNodeIds -join ";")
        }
    }

    try {
        $apiCalls.Add("GET /v2/storage-domains [$clusterAlias]")
        $sdResponse = Invoke-CohesityGet `
            -Uri "$baseUrl/v2/storage-domains" `
            -Headers $headers `
            -ApiLabel "/v2/storage-domains [$clusterAlias]"

        $storageDomains = @(
            Normalize-Objects `
                -Response $sdResponse `
                -WrapperNames @("storageDomains","items") `
                -IdentityProperties @("id","storageDomainId")
        )
    }
    catch {
        $apiFailures.Add("GET /v2/storage-domains [$clusterAlias] : $($_.Exception.Message)")
        $storageDomains = @()
        $clusterIncomplete = $true
    }

    $sdCounter = 0
    foreach ($sd in $storageDomains) {
        $sdCounter++
        $sdAlias = "SD-$sdCounter"
        $sdId = To-Text (Get-PropertyValue $sd @("id","storageDomainId"))

        $storageDomainRows += [pscustomobject][ordered]@{
            ClusterAlias     = $clusterAlias
            ClusterId        = $clusterId
            ClusterName      = $clusterName
            StorageDomain    = $sdAlias
            StorageDomainId  = $sdId
            CurrentEC        = To-Text (Get-PropertyValue $sd @("ecConfig","erasureCodingConfig","ecConfiguration"))
            CurrentRF        = To-Text (Get-PropertyValue $sd @("replicationFactor","rf"))
            CurrentFT        = To-Text (Get-PropertyValue $sd @("faultTolerance","faultToleranceLevel","failureDomain"))
            PhysicalCapacity = Convert-ToCapacityText (Get-PropertyValue $sd @("physicalCapacityBytes","physicalCapacity"))
            UsedCapacity     = Convert-ToCapacityText (Get-PropertyValue $sd @("usedCapacityBytes","usedCapacity"))
            FreeCapacity     = Convert-ToCapacityText (Get-PropertyValue $sd @("freeCapacityBytes","freeCapacity","availableCapacityBytes"))
            StoragePolicy    = To-Text (Get-PropertyValue $sd @("storagePolicy","resiliencyPolicy","policy"))
        }

        if ($sdId -eq $notAvailable) {
            $unavailableFields.Add("Storage Domain ID") | Out-Null
            continue
        }

        try {
            $encodedSdId = [uri]::EscapeDataString([string]$sdId)
            $apiCalls.Add("GET /v2/storage-domains/fault-tolerance-options?storageDomainId=<ID> [$clusterAlias/$sdAlias]")

            $ftResponse = Invoke-CohesityGet `
                -Uri "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$encodedSdId" `
                -Headers $headers `
                -ApiLabel "/v2/storage-domains/fault-tolerance-options [$clusterAlias/$sdAlias]"

            $enabled = To-Text (Get-PropertyValue $ftResponse @("enabled"))
            $globalTolerance = Get-PropertyValue $ftResponse @("globalTolerance") $null

            if ($null -ne $globalTolerance) {
                $globalLevel = To-Text (Get-PropertyValue $globalTolerance @("faultToleranceLevel","level"))
                $globalCount = To-Text (Get-PropertyValue $globalTolerance @("count"))
            }
            else {
                $globalLevel = $notAvailable
                $globalCount = $notAvailable
            }

            $failureDomainCount = To-Text (Get-PropertyValue $ftResponse @("failureDomainCount","numFailureDomains"))
            $defaultFt = To-Text (Get-PropertyValue $ftResponse @("defaultFaultTolerance"))
            $defaultEc = To-Text (Get-PropertyValue $ftResponse @("defaultEc","defaultEC","currentEc","currentEC"))
            $defaultRf = To-Text (Get-PropertyValue $ftResponse @("defaultRf","defaultRF","currentRf","currentRF"))

            $options = @(
                Normalize-Objects `
                    -Response $ftResponse `
                    -WrapperNames @("options","faultToleranceOptions","availableOptions") `
                    -IdentityProperties @("ecConfig","replicationFactor","rf")
            )

            if ($options.Count -eq 0) {
                $ftOptionRows += [pscustomobject][ordered]@{
                    ClusterAlias                   = $clusterAlias
                    ClusterId                      = $clusterId
                    StorageDomain                  = $sdAlias
                    StorageDomainId                = $sdId
                    Enabled                        = $enabled
                    GlobalFTLevel                  = $globalLevel
                    GlobalFTCount                  = $globalCount
                    FailureDomainCount             = $failureDomainCount
                    DefaultFaultTolerance          = $defaultFt
                    DefaultEC                      = $defaultEc
                    DefaultRF                      = $defaultRf
                    DiskFailuresTolerated          = $notAvailable
                    FailureDomainFailuresTolerated = $notAvailable
                    EC                             = $notAvailable
                    RF                             = $notAvailable
                    Disabled                       = $notAvailable
                    HasWarning                     = $notAvailable
                    IsSuboptimal                   = $notAvailable
                    MinFailureDomainsRequired      = $notAvailable
                    MinFailureDomainsToHeal        = $notAvailable
                    InlineECSupport                = $notAvailable
                }
            }
            else {
                foreach ($option in $options) {
                    $ftOptionRows += [pscustomobject][ordered]@{
                        ClusterAlias                   = $clusterAlias
                        ClusterId                      = $clusterId
                        StorageDomain                  = $sdAlias
                        StorageDomainId                = $sdId
                        Enabled                        = $enabled
                        GlobalFTLevel                  = $globalLevel
                        GlobalFTCount                  = $globalCount
                        FailureDomainCount             = $failureDomainCount
                        DefaultFaultTolerance          = $defaultFt
                        DefaultEC                      = $defaultEc
                        DefaultRF                      = $defaultRf
                        DiskFailuresTolerated          = To-Text (Get-PropertyValue $option @("diskFailuresTolerated","numDiskFailuresTolerated"))
                        FailureDomainFailuresTolerated = To-Text (Get-PropertyValue $option @("failureDomainFailuresTolerated","numFailureDomainFailuresTolerated"))
                        EC                             = To-Text (Get-PropertyValue $option @("ecConfig","ecConfiguration","erasureCodingConfig"))
                        RF                             = To-Text (Get-PropertyValue $option @("replicationFactor","rf"))
                        Disabled                       = To-Text (Get-PropertyValue $option @("disabled","isDisabled"))
                        HasWarning                     = To-Text (Get-PropertyValue $option @("hasWarning","warning"))
                        IsSuboptimal                   = To-Text (Get-PropertyValue $option @("isSuboptimal","suboptimal"))
                        MinFailureDomainsRequired      = To-Text (Get-PropertyValue $option @("minFailureDomainsRequired","minimumFailureDomainsRequired"))
                        MinFailureDomainsToHeal        = To-Text (Get-PropertyValue $option @("minFailureDomainsToHeal","minimumFailureDomainsRequiredToHeal"))
                        InlineECSupport                = To-Text (Get-PropertyValue $option @("inlineEcSupport","inlineECSupport","supportsInlineEC"))
                    }
                }
            }
        }
        catch {
            $apiFailures.Add("GET FT options [$clusterAlias/$sdAlias] : $($_.Exception.Message)")
            $clusterIncomplete = $true
        }
    }

    $clusterNodes = @($nodeRows | Where-Object ClusterAlias -eq $clusterAlias)
    $clusterChassis = @($chassisRows | Where-Object ClusterAlias -eq $clusterAlias)
    $clusterRacks = @($rackRows | Where-Object ClusterAlias -eq $clusterAlias)
    $clusterFt = @($ftOptionRows | Where-Object ClusterAlias -eq $clusterAlias)

    $unassignedChassis = @(
        $clusterChassis |
            Where-Object { $_.RackId -eq $notAvailable }
    ).Count

    $rackNodeCounts = @($clusterRacks | Select-Object -ExpandProperty NodeCount)

    if ($rackNodeCounts.Count -gt 0) {
        $largestRackNodes = ($rackNodeCounts | Measure-Object -Maximum).Maximum
    }
    else {
        $largestRackNodes = 0
    }

    if ($clusterNodes.Count -gt 0 -and $largestRackNodes -gt 0) {
        $largestRackPct = [math]::Round(($largestRackNodes / $clusterNodes.Count) * 100,1)
    }
    else {
        $largestRackPct = 0
    }

    $unevenDistribution = $false
    if ($rackNodeCounts.Count -gt 1) {
        $maxNodes = ($rackNodeCounts | Measure-Object -Maximum).Maximum
        $minNodes = ($rackNodeCounts | Measure-Object -Minimum).Minimum

        if ($maxNodes -ne $minNodes) {
            $unevenDistribution = $true
        }
    }

    $nodesPerChassis = @($clusterChassis | Select-Object -ExpandProperty NodeCount -Unique)
    $hasOneNodeChassis = ($nodesPerChassis -contains 1)
    $hasFourOrMoreNodeChassis = @($nodesPerChassis | Where-Object { $_ -ge 4 }).Count -gt 0
    $mixedArchitecture = ($nodesPerChassis.Count -gt 1 -and $hasOneNodeChassis -and $hasFourOrMoreNodeChassis)

    $warningOrSuboptimal = @(
        $clusterFt |
            Where-Object {
                (Get-BooleanTrue $_.HasWarning) -or
                (Get-BooleanTrue $_.IsSuboptimal) -or
                (Get-BooleanTrue $_.Disabled)
            }
    ).Count -gt 0

    $insufficientDomains = $false
    foreach ($option in $clusterFt) {
        $minReqNumeric = Get-NumericValue $option.MinFailureDomainsRequired

        if ($null -ne $minReqNumeric -and $clusterRacks.Count -lt $minReqNumeric) {
            $insufficientDomains = $true
            break
        }
    }

    $failureDomainLevels = @(
        $clusterFt |
            Select-Object -ExpandProperty GlobalFTLevel -Unique |
            Where-Object { $_ -ne $notAvailable }
    )

    if ($failureDomainLevels.Count -eq 1) {
        $currentFailureDomain = $failureDomainLevels[0]
    }
    elseif ($failureDomainLevels.Count -gt 1) {
        $currentFailureDomain = ($failureDomainLevels -join "; ")
    }
    else {
        $currentFailureDomain = $clusterRow.FailureDomain
    }

    $failuresToleratedValues = @(
        $clusterFt |
            Select-Object -ExpandProperty GlobalFTCount -Unique |
            Where-Object { $_ -ne $notAvailable }
    )

    if ($failuresToleratedValues.Count -gt 0) {
        $failuresTolerated = ($failuresToleratedValues -join "; ")
    }
    else {
        $failuresTolerated = $notAvailable
    }

    $failureDomainCountValues = @(
        $clusterFt |
            Select-Object -ExpandProperty FailureDomainCount -Unique |
            Where-Object { $_ -ne $notAvailable }
    )

    if ($failureDomainCountValues.Count -gt 0) {
        $failureDomainCount = ($failureDomainCountValues -join "; ")
    }
    else {
        $failureDomainCount = $notAvailable
    }

    $hardwareMix = @(
        $clusterNodes |
            Group-Object HardwareBucket |
            Sort-Object Name |
            ForEach-Object { "$($_.Name)=$($_.Count)" }
    ) -join "; "

    if ([string]::IsNullOrWhiteSpace($hardwareMix)) {
        $hardwareMix = $notAvailable
    }

    $flag = Get-AssessmentFlag `
        -Incomplete $clusterIncomplete `
        -NoRacks ($clusterRacks.Count -eq 0) `
        -UnassignedChassis ($unassignedChassis -gt 0) `
        -WarningOrSuboptimal $warningOrSuboptimal `
        -InsufficientDomains $insufficientDomains `
        -UnevenDistribution $unevenDistribution `
        -MixedArchitecture $mixedArchitecture

    $internalClusterSummary += [pscustomobject][ordered]@{
        ClusterAlias           = $clusterAlias
        ClusterId              = $clusterId
        ClusterName            = $clusterName
        Nodes                  = $clusterNodes.Count
        Chassis                = $clusterChassis.Count
        Racks                  = $clusterRacks.Count
        HardwareMix            = $hardwareMix
        CurrentFailureDomain   = $currentFailureDomain
        FailureDomainCount     = $failureDomainCount
        FailuresTolerated      = $failuresTolerated
        UnassignedChassis      = $unassignedChassis
        LargestRackNodePct     = $largestRackPct
        UnevenRackDistribution = $unevenDistribution
        MixedArchitecture      = $mixedArchitecture
        WarningOrSuboptimalFT  = $warningOrSuboptimal
        InsufficientDomains    = $insufficientDomains
        IncompleteData         = $clusterIncomplete
        AssessmentFlag         = $flag
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$internalReportPath  = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Internal_$timestamp.md"
$sanitizedReportPath = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Sanitized_$timestamp.md"
$internalNodeCsv     = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Nodes_Internal_$timestamp.csv"
$internalChassisCsv  = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Chassis_Internal_$timestamp.csv"
$internalRackCsv     = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Racks_Internal_$timestamp.csv"
$internalSdCsv       = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_StorageDomains_Internal_$timestamp.csv"
$internalFtCsv       = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_FTOptions_Internal_$timestamp.csv"

$nodeRows |
    Select-Object ClusterAlias,ClusterId,NodeId,ChassisId,HardwareModel,HardwareBucket,NodeModel,ProductModel,ProductModelType,SlotNumber,Status,Reachability,PhysicalCapacity |
    Export-Csv -Path $internalNodeCsv -NoTypeInformation -Encoding UTF8

$chassisRows |
    Select-Object ClusterAlias,ClusterId,ChassisId,HardwareModel,HardwareBucket,NodeCount,NodeIds,RackId,Location |
    Export-Csv -Path $internalChassisCsv -NoTypeInformation -Encoding UTF8

$rackRows |
    Select-Object ClusterAlias,ClusterId,RackAlias,RackId,ChassisCount,ChassisIds,NodeCount,NodeIds |
    Export-Csv -Path $internalRackCsv -NoTypeInformation -Encoding UTF8

$storageDomainRows | Export-Csv -Path $internalSdCsv -NoTypeInformation -Encoding UTF8
$ftOptionRows | Export-Csv -Path $internalFtCsv -NoTypeInformation -Encoding UTF8

$internal = New-Object System.Collections.Generic.List[string]
$internal.Add("# Cohesity Rack Resiliency Assessment - Internal Detailed Report")
$internal.Add("")
$internal.Add("READ-ONLY DATA COLLECTION ONLY. No Cohesity configuration changes were performed.")
$internal.Add("")
$internal.Add("## Cluster Summary")
$internal.Add("")
$internal.Add("| Cluster | Cluster ID | Nodes | Chassis | Racks | Hardware Mix | Current Failure Domain | Failure Domains | Failures Tolerated | Unassigned Chassis | Largest Rack % | Flag |")
$internal.Add("|---|---|---:|---:|---:|---|---|---|---|---:|---:|---|")

foreach ($row in $internalClusterSummary) {
    $internal.Add("| $($row.ClusterName) | $($row.ClusterId) | $($row.Nodes) | $($row.Chassis) | $($row.Racks) | $(Escape-Markdown $row.HardwareMix) | $(Escape-Markdown $row.CurrentFailureDomain) | $(Escape-Markdown $row.FailureDomainCount) | $(Escape-Markdown $row.FailuresTolerated) | $($row.UnassignedChassis) | $($row.LargestRackNodePct)% | $($row.AssessmentFlag) |")
}

$internal.Add("")
$internal.Add("## Storage Domain Resiliency")
$internal.Add("")
$internal.Add("| Cluster | SD | SD ID | Current EC | Current RF | Current FT | Physical | Used | Free |")
$internal.Add("|---|---|---|---|---|---|---|---|---|")

foreach ($sd in $storageDomainRows) {
    $internal.Add("| $($sd.ClusterName) | $($sd.StorageDomain) | $($sd.StorageDomainId) | $(Escape-Markdown $sd.CurrentEC) | $(Escape-Markdown $sd.CurrentRF) | $(Escape-Markdown $sd.CurrentFT) | $(Escape-Markdown $sd.PhysicalCapacity) | $(Escape-Markdown $sd.UsedCapacity) | $(Escape-Markdown $sd.FreeCapacity) |")
}

$internal.Add("")
$internal.Add("## Supporting CSVs")
$internal.Add("")
$internal.Add("- Nodes: $internalNodeCsv")
$internal.Add("- Chassis: $internalChassisCsv")
$internal.Add("- Racks: $internalRackCsv")
$internal.Add("- Storage Domains: $internalSdCsv")
$internal.Add("- Fault Tolerance Options: $internalFtCsv")
$internal.Add("")
$internal.Add("Number of POST/PUT/PATCH/DELETE operations executed: **0**")
$internal.Add("")
$internal.Add("READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment.")
$internal | Set-Content -Path $internalReportPath -Encoding UTF8

$totalNodes = $nodeRows.Count
$totalChassis = $chassisRows.Count
$totalRacks = $rackRows.Count
$clustersWithRacks = @($internalClusterSummary | Where-Object Racks -gt 0).Count
$clustersWithoutRacks = @($internalClusterSummary | Where-Object Racks -eq 0).Count
$totalUnassignedChassis = ($internalClusterSummary | Measure-Object UnassignedChassis -Sum).Sum

if ($null -eq $totalUnassignedChassis) {
    $totalUnassignedChassis = 0
}

$sanitized = New-Object System.Collections.Generic.List[string]
$sanitized.Add("# Cohesity Rack Resiliency Assessment - Sanitized Summary")
$sanitized.Add("")
$sanitized.Add("### Estate Summary")
$sanitized.Add("")
$sanitized.Add("- Total clusters: $($clusterRows.Count)")
$sanitized.Add("- Total nodes: $totalNodes")
$sanitized.Add("- Total chassis: $totalChassis")
$sanitized.Add("- Total configured racks: $totalRacks")
$sanitized.Add("- Clusters with racks configured: $clustersWithRacks")
$sanitized.Add("- Clusters without racks configured: $clustersWithoutRacks")
$sanitized.Add("- Chassis without rack assignment: $totalUnassignedChassis")
$sanitized.Add("")
$sanitized.Add("### Hardware Distribution")
$sanitized.Add("")
$sanitized.Add("| Model | Nodes | Chassis/Blocks | Clusters Using Model |")
$sanitized.Add("|---|---:|---:|---:|")

foreach ($bucket in @("CX8405","C6025","C5066","C5026","C5016","Other")) {
    $bucketNodes = @($nodeRows | Where-Object HardwareBucket -eq $bucket).Count
    $bucketChassis = @($chassisRows | Where-Object HardwareBucket -eq $bucket).Count
    $bucketClusters = @($nodeRows | Where-Object HardwareBucket -eq $bucket | Select-Object -ExpandProperty ClusterAlias -Unique).Count
    $sanitized.Add("| $bucket | $bucketNodes | $bucketChassis | $bucketClusters |")
}

$sanitized.Add("")
$sanitized.Add("### Cluster Resiliency Summary")
$sanitized.Add("")
$sanitized.Add("| Cluster | Nodes | Chassis | Racks | Hardware Mix | Current Failure Domain | Failures Tolerated | Rack Distribution | Assessment Flag |")
$sanitized.Add("|---|---:|---:|---:|---|---|---|---|---|")

foreach ($row in $internalClusterSummary) {
    $rackDist = @(
        $rackRows |
            Where-Object ClusterAlias -eq $row.ClusterAlias |
            ForEach-Object { "$($_.RackAlias)=$($_.NodeCount) nodes" }
    ) -join "; "

    if ([string]::IsNullOrWhiteSpace($rackDist)) {
        $rackDist = "No configured racks returned"
    }

    $sanitized.Add("| $($row.ClusterAlias) | $($row.Nodes) | $($row.Chassis) | $($row.Racks) | $(Escape-Markdown $row.HardwareMix) | $(Escape-Markdown $row.CurrentFailureDomain) | $(Escape-Markdown $row.FailuresTolerated) | $(Escape-Markdown $rackDist) | $($row.AssessmentFlag) |")
}

$sanitized.Add("")
$sanitized.Add("### Storage Domain Resiliency")
$sanitized.Add("")
$sanitized.Add("| Cluster | SD | Current EC/RF | Current FT | Failure Domains Available | Rack FT Option Available | Min Domains Required | Min Domains to Heal | Cohesity Warning/Suboptimal |")
$sanitized.Add("|---|---|---|---|---|---|---|---|---|")

foreach ($sd in $storageDomainRows) {
    $options = @(
        $ftOptionRows |
            Where-Object {
                $_.ClusterAlias -eq $sd.ClusterAlias -and
                $_.StorageDomain -eq $sd.StorageDomain
            }
    )

    $fdAvailable = @($options | Select-Object -ExpandProperty FailureDomainCount -Unique) -join "; "
    if ([string]::IsNullOrWhiteSpace($fdAvailable)) {
        $fdAvailable = $notAvailable
    }

    $rackOptions = @(
        $options |
            Where-Object {
                ([string]$_.GlobalFTLevel -match '(?i)rack') -or
                ([string]$_.FailureDomainFailuresTolerated -notmatch '^NOT AVAILABLE')
            }
    )

    if ($rackOptions.Count -gt 0) {
        $rackAvailable = "Yes"
    }
    else {
        $rackAvailable = $notAvailable
    }

    $minReq = @(
        $options |
            Select-Object -ExpandProperty MinFailureDomainsRequired -Unique |
            Where-Object { $_ -ne $notAvailable }
    ) -join "; "

    if ([string]::IsNullOrWhiteSpace($minReq)) {
        $minReq = $notAvailable
    }

    $minHeal = @(
        $options |
            Select-Object -ExpandProperty MinFailureDomainsToHeal -Unique |
            Where-Object { $_ -ne $notAvailable }
    ) -join "; "

    if ([string]::IsNullOrWhiteSpace($minHeal)) {
        $minHeal = $notAvailable
    }

    $warn = @(
        $options |
            Where-Object {
                (Get-BooleanTrue $_.Disabled) -or
                (Get-BooleanTrue $_.HasWarning) -or
                (Get-BooleanTrue $_.IsSuboptimal)
            }
    ).Count -gt 0

    $currentEcRf = "EC=$(Escape-Markdown $sd.CurrentEC); RF=$(Escape-Markdown $sd.CurrentRF)"
    $warningText = if ($warn) { "Yes" } else { "No" }

    $sanitized.Add("| $($sd.ClusterAlias) | $($sd.StorageDomain) | $currentEcRf | $(Escape-Markdown $sd.CurrentFT) | $(Escape-Markdown $fdAvailable) | $rackAvailable | $(Escape-Markdown $minReq) | $(Escape-Markdown $minHeal) | $warningText |")
}

$sanitized.Add("")
$sanitized.Add("### Rack Distribution")
$sanitized.Add("")

foreach ($cluster in $internalClusterSummary) {
    $sanitized.Add("**$($cluster.ClusterAlias)**")
    $sanitized.Add("")

    $clusterRackRows = @($rackRows | Where-Object ClusterAlias -eq $cluster.ClusterAlias)
    $sanitized.Add("- Rack count: $($clusterRackRows.Count)")

    foreach ($rack in $clusterRackRows) {
        if ($cluster.Nodes -gt 0) {
            $pct = [math]::Round(($rack.NodeCount / $cluster.Nodes) * 100,1)
        }
        else {
            $pct = 0
        }

        $sanitized.Add("- $($rack.RackAlias): $($rack.ChassisCount) chassis / $($rack.NodeCount) nodes / $pct% of cluster nodes")
    }

    $sanitized.Add("- Unassigned chassis: $($cluster.UnassignedChassis)")
    $sanitized.Add("- Largest single-rack node concentration: $($cluster.LargestRackNodePct)%")
    $sanitized.Add("")
}

$sanitized.Add("### Findings")
$sanitized.Add("")
$sanitized.Add("**Confirmed**")
$sanitized.Add("")

foreach ($cluster in $internalClusterSummary) {
    if ($cluster.Racks -eq 0) {
        $sanitized.Add("- $($cluster.ClusterAlias): no rack configuration was returned by GET /v2/racks.")
    }
    if ($cluster.UnassignedChassis -gt 0) {
        $sanitized.Add("- $($cluster.ClusterAlias): $($cluster.UnassignedChassis) chassis have no rack assignment returned by GET data.")
    }
    if ([string]$cluster.CurrentFailureDomain -match '(?i)node|chassis|rack') {
        $sanitized.Add("- $($cluster.ClusterAlias): current failure-domain value returned: $($cluster.CurrentFailureDomain).")
    }
    if ($cluster.WarningOrSuboptimalFT) {
        $sanitized.Add("- $($cluster.ClusterAlias): at least one Storage Domain FT option is disabled, warning, or suboptimal.")
    }
}

$sanitized.Add("")
$sanitized.Add("**Calculated**")
$sanitized.Add("")

foreach ($cluster in $internalClusterSummary) {
    if ($cluster.UnevenRackDistribution) {
        $sanitized.Add("- $($cluster.ClusterAlias): node distribution across returned racks is uneven; largest rack concentration is $($cluster.LargestRackNodePct)%.")
    }
    if ($cluster.MixedArchitecture) {
        $sanitized.Add("- $($cluster.ClusterAlias): returned chassis data shows mixed 1-node and 4-or-more-node chassis/block architecture.")
    }
    if ($cluster.InsufficientDomains) {
        $sanitized.Add("- $($cluster.ClusterAlias): configured rack count is below at least one returned minFailureDomainsRequired value.")
    }
}

$sanitized.Add("")
$sanitized.Add("**Unknown / Requires Cohesity Confirmation**")
$sanitized.Add("")
$unknownCount = 0

foreach ($cluster in $internalClusterSummary | Where-Object IncompleteData -eq $true) {
    $sanitized.Add("- $($cluster.ClusterAlias): one or more approved GET collections failed or returned incomplete data; Rack FT eligibility is not proven.")
    $unknownCount++
}

if ($unknownCount -eq 0) {
    $sanitized.Add("- No additional unknowns identified from successful GET collection; option availability alone is not treated as proof that a configuration is safe.")
}

$sanitized.Add("")
$sanitized.Add("### Most Important Exceptions")
$sanitized.Add("")

$exceptionRows = @(
    $internalClusterSummary |
        Where-Object {
            $_.Racks -eq 0 -or
            $_.UnassignedChassis -gt 0 -or
            $_.UnevenRackDistribution -or
            $_.WarningOrSuboptimalFT -or
            $_.InsufficientDomains -or
            $_.MixedArchitecture -or
            $_.IncompleteData
        }
)

if ($exceptionRows.Count -eq 0) {
    $sanitized.Add("- None proven by the collected GET results.")
}
else {
    foreach ($cluster in $exceptionRows) {
        $reasons = @()

        if ($cluster.Racks -eq 0) { $reasons += "no rack configuration" }
        if ($cluster.UnassignedChassis -gt 0) { $reasons += "unassigned chassis" }
        if ($cluster.UnevenRackDistribution) { $reasons += "uneven rack distribution" }
        if ($cluster.WarningOrSuboptimalFT) { $reasons += "warning/suboptimal/disabled FT option" }
        if ($cluster.InsufficientDomains) { $reasons += "insufficient failure domains" }
        if ($cluster.MixedArchitecture) { $reasons += "mixed chassis architecture" }
        if ($cluster.IncompleteData) { $reasons += "unknown/ambiguous configuration" }

        $sanitized.Add("- $($cluster.ClusterAlias): $($reasons -join '; ').")
    }
}

$sanitized.Add("")
$sanitized.Add("### Data Quality")
$sanitized.Add("")

$uniqueApiCalls = @(
    $apiCalls |
        ForEach-Object { $_ -replace ' \[.*$','' } |
        Select-Object -Unique
)

$sanitized.Add("- GET APIs queried: $($uniqueApiCalls -join '; ')")

if ($apiFailures.Count -gt 0) {
    $sanitized.Add("- GET APIs that failed: $($apiFailures.Count) call(s). See internal report/log output for cluster-local details.")
}
else {
    $sanitized.Add("- GET APIs that failed: None")
}

if ($unavailableFields.Count -gt 0) {
    $unavailableText = @($unavailableFields) -join "; "
}
else {
    $unavailableText = "None explicitly tracked; individual unavailable values are marked $notAvailable"
}

$sanitized.Add("- Fields unavailable: $unavailableText")
$incompleteClusters = @($internalClusterSummary | Where-Object IncompleteData -eq $true).Count
$sanitized.Add("- Clusters with incomplete data: $incompleteClusters")
$sanitized.Add("- Number of POST/PUT/PATCH/DELETE operations executed: **0**")
$sanitized.Add("")
$sanitized.Add("READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment.")
$sanitized | Set-Content -Path $sanitizedReportPath -Encoding UTF8

Write-Host ""
Write-Host "================ READ-ONLY COLLECTION SUMMARY ================" -ForegroundColor Cyan
Write-Host "Clusters collected        : $($clusterRows.Count)"
Write-Host "Nodes collected           : $($nodeRows.Count)"
Write-Host "Chassis collected         : $($chassisRows.Count)"
Write-Host "Racks collected           : $($rackRows.Count)"
Write-Host "Storage Domains collected : $($storageDomainRows.Count)"
Write-Host "FT option rows collected  : $($ftOptionRows.Count)"
Write-Host "GET failures              : $($apiFailures.Count)"
Write-Host "Non-GET operations        : 0" -ForegroundColor Green
Write-Host "Internal report           : $internalReportPath"
Write-Host "Sanitized report          : $sanitizedReportPath"

if ($expectedClusterCount -gt 0 -and $clusterRows.Count -ne $expectedClusterCount) {
    Write-Host "Expected clusters         : $expectedClusterCount (CHECK REQUIRED)" -ForegroundColor Yellow
}

if ($expectedNodeCount -gt 0 -and $nodeRows.Count -ne $expectedNodeCount) {
    Write-Host "Expected nodes            : $expectedNodeCount (CHECK REQUIRED)" -ForegroundColor Yellow
}

if ($apiFailures.Count -gt 0) {
    Write-Host ""
    Write-Host "GET FAILURES" -ForegroundColor Yellow
    $apiFailures | ForEach-Object { Write-Host "- $_" }
}

Write-Host ""
Write-Host "READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment." -ForegroundColor Green
