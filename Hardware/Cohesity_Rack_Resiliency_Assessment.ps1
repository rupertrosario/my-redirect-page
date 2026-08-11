# Cohesity Rack Readiness / Rack Resiliency Data Collector
# PowerShell 5.1 compatible
# Cohesity API access is strictly HTTP GET only.

$ErrorActionPreference = 'Stop'
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = 'https://helios.cohesity.com'
$outputDirectory     = 'X:\PowerShell\Data\Cohesity\RackResiliencyAssessment'
$helperPath          = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$encryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
$expectedClusters    = 23
$expectedNodes       = 173
$na                  = 'N/A'

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
    throw 'AES API key helper returned an empty API key.'
}

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

function Get-Json {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][hashtable]$Headers
    )

    if (-not $Uri.StartsWith($baseUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Safety block: URI is outside approved Helios base URL: $Uri"
    }

    $parsedUri = [uri]$Uri
    $approvedGetPaths = @(
        '/v2/mcm/cluster-mgmt/info',
        '/v2/clusters/nodes',
        '/v2/chassis',
        '/v2/ipmi/get-lan-info',
        '/v2/storage-domains',
        '/v2/storage-domains/fault-tolerance-options',
        '/irisservices/api/v1/public/cluster'
    )
    if ($approvedGetPaths -cnotcontains $parsedUri.AbsolutePath) {
        throw "Safety block: GET endpoint is not approved: $($parsedUri.AbsolutePath)"
    }

    $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method GET -UseBasicParsing -ErrorAction Stop
    if ($null -eq $response -or [string]::IsNullOrWhiteSpace([string]$response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function Get-ExactMember {
    param(
        $Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }

    foreach ($property in @($Object.PSObject.Properties)) {
        if ($property.Name -ceq $Name) {
            return $property
        }
    }

    return $null
}

function Get-ExactValue {
    param(
        $Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $member = Get-ExactMember -Object $Object -Name $Name
    if ($null -eq $member -or $null -eq $member.Value) {
        return $na
    }

    if ($member.Value -is [string] -and [string]::IsNullOrWhiteSpace([string]$member.Value)) {
        return $na
    }

    return $member.Value
}

function Get-ExactObject {
    param(
        $Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $member = Get-ExactMember -Object $Object -Name $Name
    if ($null -eq $member -or $null -eq $member.Value) {
        return $null
    }

    return $member.Value
}

function Get-ExactArray {
    param(
        $Object,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $member = Get-ExactMember -Object $Object -Name $Name
    if ($null -eq $member -or $null -eq $member.Value) {
        return @()
    }

    return @($member.Value)
}

function Format-FaultTolerance {
    param(
        $DiskFailures,
        $DomainFailures
    )

    if ($null -eq $DiskFailures -or $null -eq $DomainFailures) {
        return $na
    }
    if ([string]$DiskFailures -eq $na -or [string]$DomainFailures -eq $na) {
        return $na
    }

    return ('{0}D:{1}N' -f $DiskFailures, $DomainFailures)
}

function Format-ErasureCoding {
    param($ErasureCodingObject)

    if ($null -eq $ErasureCodingObject) {
        return $na
    }

    $data = Get-ExactValue -Object $ErasureCodingObject -Name 'numDataStripes'
    $coded = Get-ExactValue -Object $ErasureCodingObject -Name 'numCodedStripes'
    if ([string]$data -eq $na -or [string]$coded -eq $na) {
        return $na
    }

    return ('{0}:{1}' -f $data, $coded)
}

function Add-GetFailure {
    param(
        [System.Collections.Generic.List[string]]$Failures,
        [string]$ClusterAlias,
        [string]$Endpoint
    )

    $Failures.Add(('{0} | GET {1} | FAILED' -f $ClusterAlias, $Endpoint))
}

function Get-StorageDomainAssessment {
    param(
        $StorageDomains,
        [hashtable]$Headers,
        [string]$ClusterAlias,
        [System.Collections.Generic.List[string]]$Failures
    )

    $ecSummaries = New-Object System.Collections.Generic.List[string]
    $defaultFtSummaries = New-Object System.Collections.Generic.List[string]
    $globalLevelSummaries = New-Object System.Collections.Generic.List[string]
    $globalCountSummaries = New-Object System.Collections.Generic.List[string]
    $failureDomainSummaries = New-Object System.Collections.Generic.List[string]
    $replicationSummaries = New-Object System.Collections.Generic.List[string]
    $ftOptionSummaries = New-Object System.Collections.Generic.List[string]
    $summaryObjects = @()

    $orderedDomains = @($StorageDomains | Sort-Object { [string](Get-ExactValue -Object $_ -Name 'id') })
    $sdIndex = 0

    foreach ($sd in $orderedDomains) {
        $sdIndex++
        $sdAlias = 'SD-{0}' -f $sdIndex
        $sdId = Get-ExactValue -Object $sd -Name 'id'

        $storagePolicy = Get-ExactObject -Object $sd -Name 'storagePolicy'
        $ecParams = Get-ExactObject -Object $storagePolicy -Name 'erasureCodingParams'
        $ecEnabled = Get-ExactValue -Object $ecParams -Name 'enabled'
        $inlineEnabled = Get-ExactValue -Object $ecParams -Name 'inlineEnabled'
        $ecPair = Format-ErasureCoding -ErasureCodingObject $ecParams
        $ecSummaries.Add(('{0}=EC:{1},Enabled:{2},Inline:{3}' -f $sdAlias, $ecPair, $ecEnabled, $inlineEnabled))

        $ftResponse = $null
        if ([string]$sdId -ne $na) {
            try {
                $encodedId = [uri]::EscapeDataString([string]$sdId)
                $ftResponse = Get-Json -Uri "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$encodedId" -Headers $Headers
            }
            catch {
                Add-GetFailure -Failures $Failures -ClusterAlias $ClusterAlias -Endpoint '/v2/storage-domains/fault-tolerance-options'
            }
        }

        $settingsEnabled = Get-ExactValue -Object $ftResponse -Name 'enabled'
        $failureDomainCount = Get-ExactValue -Object $ftResponse -Name 'failureDomainCount'
        $globalTolerance = Get-ExactObject -Object $ftResponse -Name 'globalTolerance'
        $globalLevel = Get-ExactValue -Object $globalTolerance -Name 'faultToleranceLevel'
        $globalCount = Get-ExactValue -Object $globalTolerance -Name 'count'
        $defaultTolerance = Get-ExactObject -Object $ftResponse -Name 'defaultFaultTolerance'
        $defaultDiskFailures = Get-ExactValue -Object $defaultTolerance -Name 'numDiskFailuresTolerated'
        $defaultDomainFailures = Get-ExactValue -Object $defaultTolerance -Name 'numDomainFailuresTolerated'
        $defaultFt = Format-FaultTolerance -DiskFailures $defaultDiskFailures -DomainFailures $defaultDomainFailures

        $defaultFtSummaries.Add(('{0}={1}' -f $sdAlias, $defaultFt))
        $globalLevelSummaries.Add(('{0}={1}' -f $sdAlias, $globalLevel))
        $globalCountSummaries.Add(('{0}={1}' -f $sdAlias, $globalCount))
        $failureDomainSummaries.Add(('{0}={1}' -f $sdAlias, $failureDomainCount))

        $optionIndex = 0
        $optionTexts = New-Object System.Collections.Generic.List[string]
        $rfTexts = New-Object System.Collections.Generic.List[string]

        foreach ($option in @(Get-ExactArray -Object $ftResponse -Name 'faultToleranceOptions')) {
            $optionIndex++
            $faultTolerance = Get-ExactObject -Object $option -Name 'faultTolerance'
            $diskFailures = Get-ExactValue -Object $faultTolerance -Name 'numDiskFailuresTolerated'
            $domainFailures = Get-ExactValue -Object $faultTolerance -Name 'numDomainFailuresTolerated'
            $ftLabel = Format-FaultTolerance -DiskFailures $diskFailures -DomainFailures $domainFailures
            $disabled = Get-ExactValue -Object $option -Name 'disabled'
            $hasWarning = Get-ExactValue -Object $option -Name 'hasWarning'
            $minDomains = Get-ExactValue -Object $option -Name 'minFailureDomainsRequired'
            $defaultReplicationFactor = Get-ExactValue -Object $option -Name 'defaultReplicationFactor'
            $defaultErasureCoding = Get-ExactObject -Object $option -Name 'defaultErasureCoding'
            $defaultEcPair = Format-ErasureCoding -ErasureCodingObject $defaultErasureCoding

            $ecOptionIndex = 0
            $ecOptionTexts = New-Object System.Collections.Generic.List[string]
            foreach ($ecOption in @(Get-ExactArray -Object $option -Name 'erasureCodingOptions')) {
                $ecOptionIndex++
                $erasureCoding = Get-ExactObject -Object $ecOption -Name 'erasureCoding'
                $ecOptionPair = Format-ErasureCoding -ErasureCodingObject $erasureCoding
                $inlineSupported = Get-ExactValue -Object $ecOption -Name 'inlineSupported'
                $isSuboptimal = Get-ExactValue -Object $ecOption -Name 'isSuboptimal'
                $minDomainsForHeal = Get-ExactValue -Object $ecOption -Name 'minDomainsForHeal'
                $ecOptionTexts.Add(('EC-{0}:{1},InlineSupported:{2},Suboptimal:{3},MinDomainsForHeal:{4}' -f $ecOptionIndex, $ecOptionPair, $inlineSupported, $isSuboptimal, $minDomainsForHeal))
            }

            $ecOptionsText = if ($ecOptionTexts.Count -gt 0) { $ecOptionTexts -join ' | ' } else { $na }
            $optionTexts.Add(('Option-{0}:{1},Disabled:{2},Warning:{3},MinDomains:{4},RF:{5},DefaultEC:{6},ECOptions:[{7}]' -f $optionIndex, $ftLabel, $disabled, $hasWarning, $minDomains, $defaultReplicationFactor, $defaultEcPair, $ecOptionsText))
            $rfTexts.Add(('{0}={1}' -f $ftLabel, $defaultReplicationFactor))
        }

        $optionSummaryText = if ($optionTexts.Count -gt 0) { $optionTexts -join ' || ' } else { $na }
        $rfSummaryText = if ($rfTexts.Count -gt 0) { $rfTexts -join ' | ' } else { $na }
        $replicationSummaries.Add(('{0}={1}' -f $sdAlias, $rfSummaryText))
        $ftOptionSummaries.Add(('{0}=SettingsEnabled:{1}; {2}' -f $sdAlias, $settingsEnabled, $optionSummaryText))

        $summaryObjects += [pscustomobject][ordered]@{
            Alias                    = $sdAlias
            Id                       = $sdId
            ECPair                   = $ecPair
            ECEnabled                = $ecEnabled
            InlineEnabled            = $inlineEnabled
            DefaultFT                = $defaultFt
            GlobalToleranceLevel     = $globalLevel
            GlobalToleranceCount     = $globalCount
            FailureDomainCount       = $failureDomainCount
            DefaultReplicationFactor = $rfSummaryText
            FaultToleranceOptions    = $optionSummaryText
        }
    }

    return [pscustomobject][ordered]@{
        Count                    = @($orderedDomains).Count
        EC                       = if ($ecSummaries.Count -gt 0) { $ecSummaries -join '; ' } else { $na }
        DefaultFT                = if ($defaultFtSummaries.Count -gt 0) { $defaultFtSummaries -join '; ' } else { $na }
        GlobalToleranceLevel     = if ($globalLevelSummaries.Count -gt 0) { $globalLevelSummaries -join '; ' } else { $na }
        GlobalToleranceCount     = if ($globalCountSummaries.Count -gt 0) { $globalCountSummaries -join '; ' } else { $na }
        FailureDomainCount       = if ($failureDomainSummaries.Count -gt 0) { $failureDomainSummaries -join '; ' } else { $na }
        DefaultReplicationFactor = if ($replicationSummaries.Count -gt 0) { $replicationSummaries -join '; ' } else { $na }
        RackFTOptions            = if ($ftOptionSummaries.Count -gt 0) { $ftOptionSummaries -join '; ' } else { $na }
        Domains                  = @($summaryObjects)
    }
}

function Get-HardwareBucket {
    param([string]$Model)

    switch ($Model) {
        'CX8405' { return 'CX8405' }
        'C6025'  { return 'C6025' }
        'C5066'  { return 'C5066' }
        'C5026'  { return 'C5026' }
        'C5016'  { return 'C5016' }
        default  { return 'Other' }
    }
}

function Format-HardwareMix {
    param($NodeRows)

    $counts = @{}
    foreach ($row in @($NodeRows)) {
        $model = [string]$row.ProductModel
        if (-not $counts.ContainsKey($model)) {
            $counts[$model] = 0
        }
        $counts[$model]++
    }

    if ($counts.Count -eq 0) {
        return $na
    }

    $parts = @()
    foreach ($model in @($counts.Keys | Sort-Object)) {
        $parts += ('{0}={1}' -f $model, $counts[$model])
    }

    return ($parts -join '; ')
}

function Format-NodesPerChassis {
    param($ChassisList)

    $counts = @()
    foreach ($chassis in @($ChassisList)) {
        $nodeIds = @(Get-ExactArray -Object $chassis -Name 'nodeIds')
        $counts += $nodeIds.Count
    }

    if ($counts.Count -eq 0) {
        return $na
    }

    return (($counts | Sort-Object) -join ',')
}

Write-Host ''
Write-Host '========================================================' -ForegroundColor Cyan
Write-Host ' COHESITY RACK READINESS COLLECTION - GET ONLY' -ForegroundColor White
Write-Host '========================================================' -ForegroundColor Cyan

$failures = New-Object System.Collections.Generic.List[string]
$validationWarnings = New-Object System.Collections.Generic.List[string]
$detailRows = @()
$summaryRows = @()

$clusterResponse = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$clusterObjects = @(Get-ExactArray -Object $clusterResponse -Name 'cohesityClusters')
if ($clusterObjects.Count -eq 0) {
    throw 'No clusters returned from GET /v2/mcm/cluster-mgmt/info via exact property cohesityClusters.'
}

$clusters = @()
foreach ($cluster in $clusterObjects) {
    $clusterId = Get-ExactValue -Object $cluster -Name 'clusterId'
    $clusterName = Get-ExactValue -Object $cluster -Name 'clusterName'

    if ([string]$clusterId -eq $na) {
        $validationWarnings.Add('Cluster discovery returned an item without exact clusterId; item skipped')
        continue
    }

    $clusters += [pscustomobject][ordered]@{
        ClusterId   = [string]$clusterId
        ClusterName = $clusterName
    }
}

$clusters = @($clusters | Sort-Object ClusterId -Unique)
if ($clusters.Count -eq 0) {
    throw 'Cluster discovery returned no usable clusterId values.'
}

$totalNodes = 0
$totalChassis = 0
$totalStorageDomains = 0
$clusterIndex = 0

foreach ($cluster in $clusters) {
    $clusterIndex++
    $clusterAlias = 'Cluster-{0:D2}' -f $clusterIndex
    $headers = New-Headers -ClusterId $cluster.ClusterId

    Write-Host ("Processing {0}" -f $clusterAlias) -ForegroundColor Yellow

    $nodes = @()
    $chassisList = @()
    $storageDomains = @()
    $clusterFtResponse = $null

    try {
        $nodesResponse = Get-Json -Uri "$baseUrl/v2/clusters/nodes" -Headers $headers
        $nodes = @($nodesResponse)
    }
    catch {
        Add-GetFailure -Failures $failures -ClusterAlias $clusterAlias -Endpoint '/v2/clusters/nodes'
    }

    try {
        $chassisResponse = Get-Json -Uri "$baseUrl/v2/chassis" -Headers $headers
        $chassisList = @(Get-ExactArray -Object $chassisResponse -Name 'chassis')
    }
    catch {
        Add-GetFailure -Failures $failures -ClusterAlias $clusterAlias -Endpoint '/v2/chassis'
    }

    try {
        $storageDomainsResponse = Get-Json -Uri "$baseUrl/v2/storage-domains?matchPartialNames=false&includeTenants=true&includeStats=true" -Headers $headers
        $storageDomains = @(Get-ExactArray -Object $storageDomainsResponse -Name 'storageDomains')
    }
    catch {
        Add-GetFailure -Failures $failures -ClusterAlias $clusterAlias -Endpoint '/v2/storage-domains'
    }

    try {
        $clusterFtResponse = Get-Json -Uri "$baseUrl/irisservices/api/v1/public/cluster?fetchStats=true" -Headers $headers
    }
    catch {
        Add-GetFailure -Failures $failures -ClusterAlias $clusterAlias -Endpoint '/irisservices/api/v1/public/cluster?fetchStats=true'
    }

    $faultToleranceLevel = Get-ExactValue -Object $clusterFtResponse -Name 'faultToleranceLevel'
    $metadataFaultToleranceFactor = Get-ExactValue -Object $clusterFtResponse -Name 'metadataFaultToleranceFactor'
    $minimumFailureDomainsNeeded = Get-ExactValue -Object $clusterFtResponse -Name 'minimumFailureDomainsNeeded'

    $sdAssessment = Get-StorageDomainAssessment -StorageDomains $storageDomains -Headers $headers -ClusterAlias $clusterAlias -Failures $failures

    $chassisById = @{}
    foreach ($chassis in $chassisList) {
        $inventoryChassisId = Get-ExactValue -Object $chassis -Name 'id'
        if ([string]$inventoryChassisId -ne $na) {
            $chassisById[[string]$inventoryChassisId] = $chassis
        }
    }

    $clusterNodeRows = @()
    foreach ($node in $nodes) {
        $nodeId = Get-ExactValue -Object $node -Name 'id'
        $hostName = Get-ExactValue -Object $node -Name 'hostName'
        $nodeIp = Get-ExactValue -Object $node -Name 'ip'
        $cohesityNodeSerial = Get-ExactValue -Object $node -Name 'cohesityNodeSerial'
        $productModel = Get-ExactValue -Object $node -Name 'productModel'
        $slotNumber = Get-ExactValue -Object $node -Name 'slotNumber'
        $chassisInfo = Get-ExactObject -Object $node -Name 'chassisInfo'
        $chassisId = Get-ExactValue -Object $chassisInfo -Name 'chassisId'

        $ipmiIp = $na
        $ipmiSource = $na
        $ipmiSubnetMask = $na
        if ([string]$nodeId -ne $na) {
            try {
                $encodedNodeId = [uri]::EscapeDataString([string]$nodeId)
                $ipmiResponse = Get-Json -Uri "$baseUrl/v2/ipmi/get-lan-info?nodeId=$encodedNodeId" -Headers $headers
                $ipmiIp = Get-ExactValue -Object $ipmiResponse -Name 'lanIp'
                $ipmiSource = Get-ExactValue -Object $ipmiResponse -Name 'ipAddrSource'
                $ipmiSubnetMask = Get-ExactValue -Object $ipmiResponse -Name 'subnetMask'
            }
            catch {
                Add-GetFailure -Failures $failures -ClusterAlias $clusterAlias -Endpoint '/v2/ipmi/get-lan-info'
            }
        }

        $chassis = $null
        if ([string]$chassisId -ne $na -and $chassisById.ContainsKey([string]$chassisId)) {
            $chassis = $chassisById[[string]$chassisId]
        }

        $chassisSerial = Get-ExactValue -Object $chassis -Name 'serialNumber'
        $chassisModel = Get-ExactValue -Object $chassis -Name 'hardwareModel'

        $row = [pscustomobject][ordered]@{
            ClusterName                 = $cluster.ClusterName
            ClusterId                   = $cluster.ClusterId
            FaultToleranceLevel         = $faultToleranceLevel
            MetadataFaultToleranceFactor = $metadataFaultToleranceFactor
            MinimumFailureDomainsNeeded = $minimumFailureDomainsNeeded
            NodeId                      = $nodeId
            Hostname                    = $hostName
            NodeIP                      = $nodeIp
            IPMIIP                      = $ipmiIp
            IPMISource                  = $ipmiSource
            IPMISubnetMask              = $ipmiSubnetMask
            NodeSerial                  = $na
            CohesityNodeSerial          = $cohesityNodeSerial
            NodeModel                   = $na
            ProductModel                = $productModel
            ProductModelType            = $na
            SlotNumber                  = $slotNumber
            NodeStatus                  = $na
            Reachable                   = $na
            ChassisId                   = $chassisId
            ChassisSerial               = $chassisSerial
            CohesityChassisSerial       = $na
            ChassisModel                = $chassisModel
            ChassisType                 = $na
            MaxSlots                    = $na
            StorageDomainCount          = $sdAssessment.Count
            StorageDomainEC             = $sdAssessment.EC
            StorageDomainDefaultFT      = $sdAssessment.DefaultFT
            GlobalToleranceLevel        = $sdAssessment.GlobalToleranceLevel
            GlobalToleranceCount        = $sdAssessment.GlobalToleranceCount
            FailureDomainCount          = $sdAssessment.FailureDomainCount
            DefaultReplicationFactor    = $sdAssessment.DefaultReplicationFactor
            RackFTOptions                = $sdAssessment.RackFTOptions
        }

        $detailRows += $row
        $clusterNodeRows += $row
    }

    $defaultFtForSummary = $sdAssessment.DefaultFT
    $summaryRows += [pscustomobject][ordered]@{
        Cluster       = $clusterAlias
        Nodes         = $nodes.Count
        Chassis       = $chassisList.Count
        HardwareMix   = Format-HardwareMix -NodeRows $clusterNodeRows
        NodesChassis  = Format-NodesPerChassis -ChassisList $chassisList
        EC            = $sdAssessment.EC
        CurrentFT     = $faultToleranceLevel
        DefaultFT     = $defaultFtForSummary
    }

    $totalNodes += $nodes.Count
    $totalChassis += $chassisList.Count
    $totalStorageDomains += $sdAssessment.Count
}

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$csvPath = Join-Path $outputDirectory "Cohesity_Rack_Readiness_Detail_$timestamp.csv"
$txtPath = Join-Path $outputDirectory "Cohesity_Rack_Readiness_Summary_$timestamp.txt"

$detailRows |
    Sort-Object ClusterName, Hostname, NodeId |
    Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

$hardwareCounts = @{
    CX8405 = 0
    C6025  = 0
    C5066  = 0
    C5026  = 0
    C5016  = 0
    Other  = 0
}

foreach ($row in $detailRows) {
    $bucket = Get-HardwareBucket -Model ([string]$row.ProductModel)
    $hardwareCounts[$bucket]++
}

$ftCounts = @{
    kNode    = 0
    kChassis = 0
    kRack    = 0
}
foreach ($row in $summaryRows) {
    $currentFt = [string]$row.CurrentFT
    if ($ftCounts.ContainsKey($currentFt)) {
        $ftCounts[$currentFt]++
    }
}

$estateValidation = if ($clusters.Count -eq $expectedClusters -and $totalNodes -eq $expectedNodes) {
    'PASS'
}
else {
    ('WARNING - expected {0} clusters / {1} nodes; collected {2} clusters / {3} nodes' -f $expectedClusters, $expectedNodes, $clusters.Count, $totalNodes)
}

$txt = New-Object System.Collections.Generic.List[string]
$txt.Add('COHESITY RACK READINESS SUMMARY')
$txt.Add('===============================')
$txt.Add('')
$txt.Add('Cluster Summary')
$txt.Add('---------------')
$txt.Add(($summaryRows | Format-Table Cluster, Nodes, Chassis, HardwareMix, NodesChassis, EC, CurrentFT, DefaultFT -AutoSize -Wrap | Out-String -Width 280).TrimEnd())
$txt.Add('')
$txt.Add('Estate Totals')
$txt.Add('-------------')
$txt.Add(('Clusters        : {0}' -f $clusters.Count))
$txt.Add(('Nodes           : {0}' -f $totalNodes))
$txt.Add(('Chassis         : {0}' -f $totalChassis))
$txt.Add(('Storage Domains : {0}' -f $totalStorageDomains))
$txt.Add(('Estate validation: {0}' -f $estateValidation))
$txt.Add('')
$txt.Add('Hardware')
$txt.Add('--------')
foreach ($name in @('CX8405', 'C6025', 'C5066', 'C5026', 'C5016', 'Other')) {
    $txt.Add(('{0,-7}: {1}' -f $name, $hardwareCounts[$name]))
}
$txt.Add('')
$txt.Add('Current FT')
$txt.Add('----------')
foreach ($name in @('kNode', 'kChassis', 'kRack')) {
    $txt.Add(('{0,-9}: {1}' -f $name, $ftCounts[$name]))
}
$txt.Add('')
$txt.Add(('GET failures    : {0}' -f $failures.Count))
$txt.Add('Non-GET calls   : 0')

if ($failures.Count -gt 0) {
    $txt.Add('')
    $txt.Add('GET Failures')
    $txt.Add('------------')
    foreach ($failure in $failures) {
        $txt.Add($failure)
    }
}

if ($validationWarnings.Count -gt 0) {
    $txt.Add('')
    $txt.Add('Validation Warnings')
    $txt.Add('-------------------')
    foreach ($warning in $validationWarnings) {
        $txt.Add($warning)
    }
}

$txt | Set-Content -Path $txtPath -Encoding UTF8

Write-Host ''
Write-Host 'CLUSTER SUMMARY' -ForegroundColor Cyan
$summaryRows |
    Format-Table Cluster, Nodes, Chassis, HardwareMix, NodesChassis, EC, CurrentFT, DefaultFT -AutoSize -Wrap |
    Out-Host

Write-Host ''
Write-Host '==============================' -ForegroundColor Cyan
Write-Host 'COLLECTION SUMMARY' -ForegroundColor White
Write-Host '==============================' -ForegroundColor Cyan
Write-Host ('Clusters        : {0}' -f $clusters.Count)
Write-Host ('Nodes           : {0}' -f $totalNodes)
Write-Host ('Chassis         : {0}' -f $totalChassis)
Write-Host ('Storage Domains : {0}' -f $totalStorageDomains)
Write-Host ('Estate validation: {0}' -f $estateValidation)
Write-Host ('GET failures    : {0}' -f $failures.Count)
Write-Host 'Non-GET calls   : 0' -ForegroundColor Green
Write-Host ('CSV detail      : {0}' -f $csvPath)
Write-Host ('TXT summary     : {0}' -f $txtPath)
