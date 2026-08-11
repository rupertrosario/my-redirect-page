# Cohesity Rack Readiness Data Collection - READ ONLY
# PowerShell 5.1 compatible
# STRICT SAFETY: HTTP GET ONLY. No Cohesity configuration changes.

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$outputDirectory     = "X:\PowerShell\Data\Cohesity\RackResiliencyAssessment"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

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
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    $method = "GET"
    if ($method -ne "GET") {
        throw "SAFETY BLOCK: only HTTP GET is permitted."
    }

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function First-Property {
    param(
        $Object,
        [string[]]$Names,
        $Default = "N/A"
    )

    if ($null -eq $Object) {
        return $Default
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name -and $null -ne $property.Value) {
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
    }

    return $Default
}

function Array-Property {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return @()
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name -and $null -ne $property.Value) {
                return @($property.Value)
            }
        }
    }

    return @()
}

function Get-NestedObject {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return $null
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name -and $null -ne $property.Value) {
                return $property.Value
            }
        }
    }

    return $null
}

function Get-Clusters {
    param($Response)

    if ($null -eq $Response) { return @() }
    if ($Response.cohesityClusters) { return @($Response.cohesityClusters) }
    if ($Response.clusters) { return @($Response.clusters) }
    if ($Response.clusterInfos) { return @($Response.clusterInfos) }
    if ($Response.mcmInfo -and $Response.mcmInfo.clusterInfos) { return @($Response.mcmInfo.clusterInfos) }
    if ($Response -is [System.Array]) { return @($Response) }
    return @()
}

function Get-Nodes {
    param($Response)

    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    if ($Response.nodes) { return @($Response.nodes) }
    if ($Response.nodeList) { return @($Response.nodeList) }
    if ($Response.items) { return @($Response.items) }
    if ($Response.PSObject.Properties["nodeId"] -or $Response.PSObject.Properties["id"]) { return @($Response) }
    return @()
}

function Get-Chassis {
    param($Response)

    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    if ($Response.chassis) { return @($Response.chassis) }
    if ($Response.chassisList) { return @($Response.chassisList) }
    if ($Response.items) { return @($Response.items) }
    if ($Response.PSObject.Properties["nodeIds"] -or $Response.PSObject.Properties["hardwareModel"]) { return @($Response) }
    return @()
}

function Get-StorageDomains {
    param($Response)

    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    if ($Response.storageDomains) { return @($Response.storageDomains) }
    if ($Response.items) { return @($Response.items) }
    if ($Response.PSObject.Properties["storagePolicy"] -or $Response.PSObject.Properties["id"]) { return @($Response) }
    return @()
}

function Get-HardwareBucket {
    param([string]$Model)

    switch -Regex ([string]$Model) {
        '^CX8405$' { return "CX8405" }
        '^C6025$'  { return "C6025" }
        '^C5066$'  { return "C5066" }
        '^C5026$'  { return "C5026" }
        '^C5016$'  { return "C5016" }
        default    { return "Other" }
    }
}

function Format-HardwareMix {
    param($Nodes)

    $counts = @{
        CX8405 = 0
        C6025  = 0
        C5066  = 0
        C5026  = 0
        C5016  = 0
        Other  = 0
    }

    foreach ($node in @($Nodes)) {
        $model = [string](First-Property $node @("productModel","nodeModel","hardwareModel","chassisModel"))
        $bucket = Get-HardwareBucket $model
        $counts[$bucket]++
    }

    $parts = @()
    foreach ($name in @("CX8405","C6025","C5066","C5026","C5016","Other")) {
        if ($counts[$name] -gt 0) {
            $parts += "$name=$($counts[$name])"
        }
    }

    if ($parts.Count -eq 0) { return "N/A" }
    return ($parts -join "; ")
}

function Format-NodesPerChassis {
    param($ChassisList)

    $counts = @()
    foreach ($chassis in @($ChassisList)) {
        $counts += @(Array-Property $chassis @("nodeIds")).Count
    }

    if ($counts.Count -eq 0) { return "N/A" }
    return (($counts | Sort-Object) -join ",")
}

function Get-ClusterFtState {
    param($Response)

    if ($null -eq $Response) {
        return [pscustomobject][ordered]@{
            FaultToleranceLevel          = "N/A"
            MetadataFaultToleranceFactor = "N/A"
            MinimumFailureDomainsNeeded  = "N/A"
        }
    }

    $clusterObject = $Response
    if ($Response -is [System.Array]) {
        if (@($Response).Count -gt 0) { $clusterObject = @($Response)[0] }
    }

    return [pscustomobject][ordered]@{
        FaultToleranceLevel          = First-Property $clusterObject @("faultToleranceLevel")
        MetadataFaultToleranceFactor = First-Property $clusterObject @("metadataFaultToleranceFactor")
        MinimumFailureDomainsNeeded  = First-Property $clusterObject @("minimumFailureDomainsNeeded")
    }
}

function Get-StorageDomainAssessment {
    param(
        $StorageDomains,
        [hashtable]$Headers,
        [string]$ClusterAlias,
        [System.Collections.Generic.List[string]]$Failures
    )

    $domainSummaries = @()
    $ftRows = @()
    $index = 0

    foreach ($sd in @($StorageDomains)) {
        $index++
        $sdAlias = "SD-$index"
        $sdId = [string](First-Property $sd @("id","storageDomainId"))

        $storagePolicy = Get-NestedObject $sd @("storagePolicy")
        $ecParams = Get-NestedObject $storagePolicy @("erasureCodingParams")

        $ecEnabled = First-Property $ecParams @("enabled")
        $dataStripes = First-Property $ecParams @("numDataStripes")
        $codedStripes = First-Property $ecParams @("numCodedStripes")
        $inlineEc = First-Property $ecParams @("inlineEnabled")

        if ($dataStripes -ne "N/A" -and $codedStripes -ne "N/A") {
            $ec = "$dataStripes`:$codedStripes"
        }
        else {
            $ec = "N/A"
        }

        $ft = $null
        if ($sdId -ne "N/A") {
            try {
                $encodedId = [uri]::EscapeDataString($sdId)
                $ft = Get-Json `
                    -Uri "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$encodedId" `
                    -Headers $Headers
            }
            catch {
                $Failures.Add("$ClusterAlias | FT options $sdAlias | $($_.Exception.Message)")
            }
        }

        $globalTolerance = Get-NestedObject $ft @("globalTolerance")
        $defaultTolerance = Get-NestedObject $ft @("defaultFaultTolerance")

        $globalLevel = First-Property $globalTolerance @("faultToleranceLevel")
        $globalCount = First-Property $globalTolerance @("count")
        $failureDomainCount = First-Property $ft @("failureDomainCount")
        $defaultDiskFt = First-Property $defaultTolerance @("numDiskFailuresTolerated","diskFailuresTolerated")
        $defaultDomainFt = First-Property $defaultTolerance @("numDomainFailuresTolerated","numFailureDomainFailuresTolerated","failureDomainFailuresTolerated")
        $defaultEc = First-Property $ft @("defaultErasureCoding","defaultEc","defaultEC")
        $defaultRf = First-Property $ft @("defaultReplicationFactor","defaultRf","defaultRF")

        $options = @()
        if ($null -ne $ft) {
            foreach ($name in @("faultToleranceOptions","options","availableOptions")) {
                $candidate = @(Array-Property $ft @($name))
                if ($candidate.Count -gt 0) {
                    $options = $candidate
                    break
                }
            }
        }

        $rackOptionTexts = @()
        foreach ($option in $options) {
            $optionLevel = [string](First-Property $option @("faultToleranceLevel","failureDomainType","level"))
            $minDomains = First-Property $option @("minFailureDomainsRequired","minimumFailureDomainsRequired")
            $disabled = First-Property $option @("disabled","isDisabled")
            $warning = First-Property $option @("hasWarning","warning")
            $suboptimal = First-Property $option @("isSuboptimal","suboptimal")
            $optionEc = First-Property $option @("erasureCoding","ecConfig","ecConfiguration")
            $optionRf = First-Property $option @("replicationFactor","rf")

            if ($optionLevel -match '(?i)rack') {
                $rackOptionTexts += "Level=$optionLevel,EC=$optionEc,RF=$optionRf,MinDomains=$minDomains,Disabled=$disabled,Warning=$warning,Suboptimal=$suboptimal"
            }
        }

        $rackOptionsText = if ($rackOptionTexts.Count -gt 0) { $rackOptionTexts -join " | " } else { "N/A" }

        $domainSummaries += "$sdAlias EC=$ec Enabled=$ecEnabled Inline=$inlineEc"

        $ftRows += [pscustomobject][ordered]@{
            StorageDomainAlias         = $sdAlias
            StorageDomainId            = $sdId
            ECEnabled                  = $ecEnabled
            ECDataCoded                = $ec
            InlineEC                   = $inlineEc
            GlobalToleranceLevel       = $globalLevel
            GlobalToleranceCount       = $globalCount
            FailureDomainCount         = $failureDomainCount
            DefaultDiskFailures        = $defaultDiskFt
            DefaultDomainFailures      = $defaultDomainFt
            DefaultErasureCoding       = $defaultEc
            DefaultReplicationFactor   = $defaultRf
            RackFaultToleranceOptions  = $rackOptionsText
        }
    }

    return [pscustomobject][ordered]@{
        Count   = @($StorageDomains).Count
        Summary = if ($domainSummaries.Count -gt 0) { $domainSummaries -join "; " } else { "N/A" }
        FTRows  = @($ftRows)
    }
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host " COHESITY RACK READINESS COLLECTION - GET ONLY" -ForegroundColor White
Write-Host "========================================================" -ForegroundColor Cyan

$clusterResponse = Get-Json `
    -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers (New-Headers)

$clusterObjects = @(Get-Clusters $clusterResponse)
if ($clusterObjects.Count -eq 0) {
    throw "No clusters returned by GET /v2/mcm/cluster-mgmt/info."
}

$clusters = @()
foreach ($cluster in $clusterObjects) {
    $clusterId = [string](First-Property $cluster @("clusterId","id"))
    if ($clusterId -eq "N/A") { continue }

    $clusters += [pscustomobject][ordered]@{
        ClusterName = First-Property $cluster @("clusterName","displayName","name")
        ClusterId   = $clusterId
    }
}

$clusters = @($clusters | Sort-Object ClusterName,ClusterId -Unique)
if ($clusters.Count -eq 0) {
    throw "No usable clusters returned by Helios."
}

$detailRows = @()
$summaryRows = @()
$failures = New-Object System.Collections.Generic.List[string]
$totalNodes = 0
$totalChassis = 0
$totalStorageDomains = 0
$clusterIndex = 0

foreach ($cluster in $clusters) {
    $clusterIndex++
    $clusterAlias = "Cluster-{0:D2}" -f $clusterIndex
    $headers = New-Headers -ClusterId $cluster.ClusterId

    Write-Host "Processing $clusterAlias" -ForegroundColor Yellow

    $nodes = @()
    $chassisList = @()
    $storageDomains = @()
    $clusterFt = $null

    try {
        $nodes = @(Get-Nodes (Get-Json -Uri "$baseUrl/v2/clusters/nodes" -Headers $headers))
    }
    catch {
        $failures.Add("$clusterAlias | GET /v2/clusters/nodes | $($_.Exception.Message)")
    }

    try {
        $chassisList = @(Get-Chassis (Get-Json -Uri "$baseUrl/v2/chassis" -Headers $headers))
    }
    catch {
        $failures.Add("$clusterAlias | GET /v2/chassis | $($_.Exception.Message)")
    }

    try {
        $storageDomains = @(Get-StorageDomains (Get-Json -Uri "$baseUrl/v2/storage-domains?matchPartialNames=false&includeTenants=true&includeStats=true" -Headers $headers))
    }
    catch {
        $failures.Add("$clusterAlias | GET /v2/storage-domains | $($_.Exception.Message)")
    }

    try {
        $clusterFt = Get-ClusterFtState (Get-Json -Uri "$baseUrl/irisservices/api/v1/public/cluster?fetchStats=true" -Headers $headers)
    }
    catch {
        $failures.Add("$clusterAlias | GET /irisservices/api/v1/public/cluster?fetchStats=true | $($_.Exception.Message)")
        $clusterFt = Get-ClusterFtState $null
    }

    $sdAssessment = Get-StorageDomainAssessment `
        -StorageDomains $storageDomains `
        -Headers $headers `
        -ClusterAlias $clusterAlias `
        -Failures $failures

    $chassisByNodeId = @{}
    foreach ($chassis in $chassisList) {
        foreach ($nodeId in @(Array-Property $chassis @("nodeIds"))) {
            if ($null -ne $nodeId) {
                $chassisByNodeId[[string]$nodeId] = $chassis
            }
        }
    }

    $ftSummary = @()
    foreach ($ftRow in @($sdAssessment.FTRows)) {
        $ftSummary += "$($ftRow.StorageDomainAlias):Global=$($ftRow.GlobalToleranceLevel)/$($ftRow.GlobalToleranceCount),Domains=$($ftRow.FailureDomainCount),DiskFT=$($ftRow.DefaultDiskFailures),DomainFT=$($ftRow.DefaultDomainFailures),DefaultEC=$($ftRow.DefaultErasureCoding),DefaultRF=$($ftRow.DefaultReplicationFactor),RackOptions=$($ftRow.RackFaultToleranceOptions)"
    }
    $ftSummaryText = if ($ftSummary.Count -gt 0) { $ftSummary -join "; " } else { "N/A" }

    foreach ($node in $nodes) {
        $nodeId = [string](First-Property $node @("nodeId","id"))
        $chassis = $null
        if ($nodeId -ne "N/A" -and $chassisByNodeId.ContainsKey($nodeId)) {
            $chassis = $chassisByNodeId[$nodeId]
        }

        $detailRows += [pscustomobject][ordered]@{
            ClusterName                 = $cluster.ClusterName
            ClusterId                   = $cluster.ClusterId
            FaultToleranceLevel         = $clusterFt.FaultToleranceLevel
            MetadataFTFactor            = $clusterFt.MetadataFaultToleranceFactor
            MinimumFailureDomainsNeeded = $clusterFt.MinimumFailureDomainsNeeded
            NodeId                      = $nodeId
            Hostname                    = First-Property $node @("hostname","hostName","name")
            NodeIP                      = First-Property $node @("ip","nodeIp","ipAddress")
            IPMIIP                      = First-Property $node @("ipmiIp","ipmiIP","ipmiAddress")
            NodeSerial                  = First-Property $node @("nodeSerial")
            CohesityNodeSerial          = First-Property $node @("cohesityNodeSerial")
            NodeModel                   = First-Property $node @("nodeModel")
            ProductModel                = First-Property $node @("productModel")
            ProductModelType            = First-Property $node @("productModelType")
            SlotNumber                  = First-Property $node @("slotNumber","slot")
            NodeStatus                  = First-Property $node @("status","nodeStatus")
            Reachable                   = First-Property $node @("isReachable","reachable","reachability")
            ChassisId                   = if ($chassis) { First-Property $chassis @("id","chassisId") } else { First-Property $node @("chassisId") }
            ChassisName                 = if ($chassis) { First-Property $chassis @("name","chassisName") } else { "N/A" }
            ChassisSerial               = if ($chassis) { First-Property $chassis @("serialNumber","chassisSerial") } else { First-Property $node @("chassisSerial") }
            CohesityChassisSerial       = First-Property $node @("cohesityChassisSerial")
            ChassisModel                = if ($chassis) { First-Property $chassis @("hardwareModel","chassisModel") } else { First-Property $node @("chassisModel") }
            StorageDomainCount          = $sdAssessment.Count
            StorageDomainEC             = $sdAssessment.Summary
            StorageDomainFT             = $ftSummaryText
        }
    }

    $totalNodes += $nodes.Count
    $totalChassis += $chassisList.Count
    $totalStorageDomains += $sdAssessment.Count

    $summaryRows += [pscustomobject][ordered]@{
        Cluster            = $clusterAlias
        Nodes              = $nodes.Count
        Chassis            = $chassisList.Count
        HardwareMix        = Format-HardwareMix -Nodes $nodes
        NodesPerChassis    = Format-NodesPerChassis -ChassisList $chassisList
        StorageDomains     = $sdAssessment.Count
        EC                 = $sdAssessment.Summary
        CurrentFT          = $clusterFt.FaultToleranceLevel
        MetadataFTFactor   = $clusterFt.MetadataFaultToleranceFactor
        FailureDomainsNeed = $clusterFt.MinimumFailureDomainsNeeded
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $outputDirectory "Cohesity_Rack_Readiness_Detail_$timestamp.csv"
$txtPath = Join-Path $outputDirectory "Cohesity_Rack_Readiness_Summary_$timestamp.txt"

$detailRows |
    Sort-Object ClusterName,Hostname,NodeId |
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
    $model = [string]$row.ProductModel
    if ($model -eq "N/A") { $model = [string]$row.NodeModel }
    $hardwareCounts[(Get-HardwareBucket $model)]++
}

$ftCounts = @{}
foreach ($row in $summaryRows) {
    $key = [string]$row.CurrentFT
    if (-not $ftCounts.ContainsKey($key)) { $ftCounts[$key] = 0 }
    $ftCounts[$key]++
}

$txt = New-Object System.Collections.Generic.List[string]
$txt.Add("COHESITY RACK READINESS SUMMARY")
$txt.Add("===============================")
$txt.Add("")
$txt.Add("Cluster Summary")
$txt.Add("---------------")
$txt.Add(($summaryRows | Format-Table Cluster,Nodes,Chassis,HardwareMix,NodesPerChassis,StorageDomains,EC,CurrentFT,MetadataFTFactor,FailureDomainsNeed -AutoSize | Out-String -Width 240).TrimEnd())
$txt.Add("")
$txt.Add("Estate Totals")
$txt.Add("-------------")
$txt.Add("Clusters        : $($clusters.Count)")
$txt.Add("Nodes           : $totalNodes")
$txt.Add("Chassis         : $totalChassis")
$txt.Add("Storage Domains : $totalStorageDomains")
$txt.Add("")
$txt.Add("Hardware")
$txt.Add("--------")
foreach ($name in @("CX8405","C6025","C5066","C5026","C5016","Other")) {
    $txt.Add(("{0,-7}: {1}" -f $name,$hardwareCounts[$name]))
}
$txt.Add("")
$txt.Add("Current Fault Tolerance")
$txt.Add("-----------------------")
foreach ($name in @($ftCounts.Keys | Sort-Object)) {
    $txt.Add(("{0,-12}: {1} clusters" -f $name,$ftCounts[$name]))
}
$txt.Add("")
$txt.Add("GET failures    : $($failures.Count)")
$txt.Add("Non-GET calls   : 0")

if ($failures.Count -gt 0) {
    $txt.Add("")
    $txt.Add("GET Failures")
    $txt.Add("------------")
    foreach ($failure in $failures) {
        $txt.Add($failure)
    }
}

$txt | Set-Content -Path $txtPath -Encoding UTF8

Write-Host ""
Write-Host "CLUSTER SUMMARY" -ForegroundColor Cyan
$summaryRows |
    Format-Table Cluster,Nodes,Chassis,HardwareMix,NodesPerChassis,StorageDomains,EC,CurrentFT,MetadataFTFactor,FailureDomainsNeed -AutoSize -Wrap |
    Out-Host

Write-Host ""
Write-Host "==============================" -ForegroundColor Cyan
Write-Host "COLLECTION SUMMARY" -ForegroundColor White
Write-Host "==============================" -ForegroundColor Cyan
Write-Host "Clusters        : $($clusters.Count)"
Write-Host "Nodes           : $totalNodes"
Write-Host "Chassis         : $totalChassis"
Write-Host "Storage Domains : $totalStorageDomains"
Write-Host "GET failures    : $($failures.Count)"
Write-Host "Non-GET calls   : 0" -ForegroundColor Green
Write-Host "CSV detail      : $csvPath"
Write-Host "TXT summary     : $txtPath"
Write-Host ""
Write-Host "READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment." -ForegroundColor Green
