# Cohesity Rack Resiliency Assessment - READ ONLY
# PowerShell 5.1 compatible
# STRICT SAFETY: HTTP GET ONLY. No Cohesity configuration changes.

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$outputDirectory     = "X:\PowerShell\Data\Cohesity\RackResiliencyAssessment"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$expectedClusters    = 22
$expectedNodes       = 169
$notAvailable        = "NOT AVAILABLE THROUGH APPROVED READ-ONLY COLLECTION"

if (-not (Test-Path $outputDirectory -PathType Container)) {
    New-Item -Path $outputDirectory -ItemType Directory -Force | Out-Null
}
if (-not (Test-Path $helperPath -PathType Leaf)) { throw "API key helper not found: $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key file not found: $encryptedApiKeyPath" }

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES API key helper returned an empty API key." }

function New-Headers {
    param([string]$ClusterId)
    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers["accessClusterId"] = $ClusterId
    }
    return $headers
}

function Get-Json {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [Parameter(Mandatory)][string]$Label
    )

    # Safety gate before every API request.
    $method = "GET"
    if ($method -ne "GET") { throw "SAFETY BLOCK: non-GET method requested for $Label" }

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function First-Property {
    param($Object,[string[]]$Names,$Default=$notAvailable)
    if ($null -eq $Object) { return $Default }
    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name -and $null -ne $property.Value) {
                if ($property.Value -is [System.Array]) {
                    if (@($property.Value).Count -gt 0) { return $property.Value }
                }
                else {
                    $text = ([string]$property.Value).Trim()
                    if (-not [string]::IsNullOrWhiteSpace($text)) { return $property.Value }
                }
            }
        }
    }
    return $Default
}

function Get-ArrayProperty {
    param($Object,[string[]]$Names)
    if ($null -eq $Object) { return @() }
    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name -and $null -ne $property.Value) { return @($property.Value) }
        }
    }
    return @()
}

function Get-ClusterObjects {
    param($Response)
    if ($null -eq $Response) { return @() }
    if ($Response.cohesityClusters) { return @($Response.cohesityClusters) }
    if ($Response.clusters) { return @($Response.clusters) }
    if ($Response.clusterInfos) { return @($Response.clusterInfos) }
    if ($Response.mcmInfo -and $Response.mcmInfo.clusterInfos) { return @($Response.mcmInfo.clusterInfos) }
    if ($Response -is [System.Array]) { return @($Response) }
    return @()
}

function Get-Objects {
    param($Response,[string[]]$Wrappers,[string[]]$IdentityFields)
    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    foreach ($wrapper in $Wrappers) {
        $items = Get-ArrayProperty $Response @($wrapper)
        if ($items.Count -gt 0) { return @($items) }
    }
    foreach ($field in $IdentityFields) {
        if ($Response.PSObject.Properties[$field]) { return @($Response) }
    }
    return @()
}

function To-Text {
    param($Value)
    if ($null -eq $Value) { return $notAvailable }
    if ($Value -is [System.Array]) {
        if (@($Value).Count -eq 0) { return $notAvailable }
        return (@($Value) -join "; ")
    }
    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) { return $notAvailable }
    return $text
}

function Escape-Markdown {
    param($Value)
    return ((To-Text $Value) -replace '\|','\\|')
}

function Is-True {
    param($Value)
    return ([string]$Value -match '^(?i:true|yes|1)$')
}

function Get-Number {
    param($Value)
    $number = 0.0
    if ($null -ne $Value -and [double]::TryParse(([string]$Value),[ref]$number)) { return $number }
    return $null
}

function Get-HardwareBucket {
    param([string]$Model)
    switch -Regex ($Model) {
        '^CX8405$' { 'CX8405'; break }
        '^C6025$'  { 'C6025'; break }
        '^C5066$'  { 'C5066'; break }
        '^C5026$'  { 'C5026'; break }
        '^C5016$'  { 'C5016'; break }
        default    { 'Other' }
    }
}

$apiCalls = New-Object System.Collections.Generic.List[string]
$apiFailures = New-Object System.Collections.Generic.List[string]
$clusterRows = @()
$nodeRows = @()
$chassisRows = @()
$rackRows = @()
$storageDomainRows = @()
$ftRows = @()
$clusterSummary = @()

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host " COHESITY RACK RESILIENCY ASSESSMENT - GET ONLY" -ForegroundColor White
Write-Host "========================================================" -ForegroundColor Cyan

# IMPORTANT: use the same proven Helios discovery method as the working AD and hardware scripts.
try {
    $apiCalls.Add("GET /v2/mcm/cluster-mgmt/info")
    $clusterResponse = Get-Json "$baseUrl/v2/mcm/cluster-mgmt/info" (New-Headers) "/v2/mcm/cluster-mgmt/info"
}
catch {
    throw "Cluster discovery failed: $($_.Exception.Message)"
}

$clusters = @(Get-ClusterObjects $clusterResponse)
if ($clusters.Count -eq 0) { throw "No clusters returned from Helios cluster discovery." }

$clusters = @($clusters | Sort-Object { To-Text (First-Property $_ @("clusterName","displayName","name")) })
$clusterIndex = 0

foreach ($cluster in $clusters) {
    $clusterId = To-Text (First-Property $cluster @("clusterId","id"))
    if ($clusterId -eq $notAvailable) { continue }

    $clusterIndex++
    $alias = "Cluster-{0:D2}" -f $clusterIndex
    $name = To-Text (First-Property $cluster @("clusterName","displayName","name"))

    $clusterRows += [pscustomobject][ordered]@{
        ClusterAlias    = $alias
        ClusterId       = [string]$clusterId
        ClusterName     = $name
        SoftwareVersion = To-Text (First-Property $cluster @("softwareVersion","version","clusterSoftwareVersion"))
    }
}

foreach ($cluster in $clusterRows) {
    $alias = $cluster.ClusterAlias
    $headers = New-Headers ([string]$cluster.ClusterId)
    $incomplete = $false

    # NODES
    try {
        $apiCalls.Add("GET /v2/clusters/nodes [$alias]")
        $response = Get-Json "$baseUrl/v2/clusters/nodes" $headers "/v2/clusters/nodes [$alias]"
        $nodes = @(Get-Objects $response @("nodes","nodeList","items") @("nodeId","id"))
    }
    catch {
        $apiFailures.Add("GET /v2/clusters/nodes [$alias] : $($_.Exception.Message)")
        $nodes = @(); $incomplete = $true
    }

    foreach ($node in $nodes) {
        $nodeId = To-Text (First-Property $node @("nodeId","id"))
        $model = To-Text (First-Property $node @("productModel","nodeModel","hardwareModel"))
        $nodeRows += [pscustomobject][ordered]@{
            ClusterAlias     = $alias
            ClusterId        = $cluster.ClusterId
            NodeId           = $nodeId
            ChassisId        = To-Text (First-Property $node @("chassisId"))
            HardwareModel    = $model
            HardwareBucket   = Get-HardwareBucket $model
            NodeModel        = To-Text (First-Property $node @("nodeModel"))
            ProductModel     = To-Text (First-Property $node @("productModel"))
            ProductModelType = To-Text (First-Property $node @("productModelType"))
            SlotNumber       = To-Text (First-Property $node @("slotNumber","slot"))
            Status           = To-Text (First-Property $node @("status","nodeStatus"))
            Reachability     = To-Text (First-Property $node @("isReachable","reachable","reachability"))
            PhysicalCapacity = To-Text (First-Property $node @("physicalCapacityBytes","physicalCapacity","capacityBytes"))
        }
    }

    # SUPPLEMENTARY NODE HARDWARE
    try {
        $apiCalls.Add("GET /v2/node/hardware-info [$alias]")
        $response = Get-Json "$baseUrl/v2/node/hardware-info" $headers "/v2/node/hardware-info [$alias]"
        $hardware = @(Get-Objects $response @("hardwareInfo","hardwareInfos","items") @("nodeModel","productModel","chassisModel"))
    }
    catch {
        $apiFailures.Add("GET /v2/node/hardware-info [$alias] : $($_.Exception.Message)")
        $hardware = @()
    }

    # CHASSIS
    try {
        $apiCalls.Add("GET /v2/chassis [$alias]")
        $response = Get-Json "$baseUrl/v2/chassis" $headers "/v2/chassis [$alias]"
        $chassisList = @(Get-Objects $response @("chassis","chassisList","items") @("id","chassisId","nodeIds"))
    }
    catch {
        $apiFailures.Add("GET /v2/chassis [$alias] : $($_.Exception.Message)")
        $chassisList = @(); $incomplete = $true
    }

    foreach ($chassis in $chassisList) {
        $chassisId = To-Text (First-Property $chassis @("id","chassisId"))
        $nodeIds = @(Get-ArrayProperty $chassis @("nodeIds")) | ForEach-Object { [string]$_ }
        $model = To-Text (First-Property $chassis @("hardwareModel","model","chassisModel"))
        $rackId = To-Text (First-Property $chassis @("rackId"))

        $chassisRows += [pscustomobject][ordered]@{
            ClusterAlias   = $alias
            ClusterId      = $cluster.ClusterId
            ChassisId      = $chassisId
            HardwareModel  = $model
            HardwareBucket = Get-HardwareBucket $model
            NodeCount      = $nodeIds.Count
            NodeIds        = ($nodeIds -join ";")
            RackId         = $rackId
            Location       = To-Text (First-Property $chassis @("location"))
        }

        foreach ($nodeId in $nodeIds) {
            $target = $nodeRows | Where-Object { $_.ClusterAlias -eq $alias -and $_.NodeId -eq [string]$nodeId } | Select-Object -First 1
            if ($target) {
                $target.ChassisId = $chassisId
                if ($target.HardwareModel -eq $notAvailable) {
                    $target.HardwareModel = $model
                    $target.HardwareBucket = Get-HardwareBucket $model
                }
            }
        }
    }

    # RACKS
    try {
        $apiCalls.Add("GET /v2/racks [$alias]")
        $response = Get-Json "$baseUrl/v2/racks" $headers "/v2/racks [$alias]"
        $racks = @(Get-Objects $response @("racks","rackList","items") @("id","rackId"))
    }
    catch {
        $apiFailures.Add("GET /v2/racks [$alias] : $($_.Exception.Message)")
        $racks = @(); $incomplete = $true
    }

    $rackCounter = 0
    foreach ($rack in $racks) {
        $rackCounter++
        $rackId = To-Text (First-Property $rack @("id","rackId"))
        $chassisIds = @(Get-ArrayProperty $rack @("chassisIds","chassisIdList")) | ForEach-Object { [string]$_ }
        if ($chassisIds.Count -eq 0 -and $rackId -ne $notAvailable) {
            $chassisIds = @($chassisRows | Where-Object { $_.ClusterAlias -eq $alias -and $_.RackId -eq $rackId } | Select-Object -ExpandProperty ChassisId)
        }
        $nodeIds = @()
        foreach ($cid in $chassisIds) {
            $nodeIds += @($chassisRows | Where-Object { $_.ClusterAlias -eq $alias -and $_.ChassisId -eq $cid } | ForEach-Object { if ($_.NodeIds) { $_.NodeIds -split ';' } })
        }
        $nodeIds = @($nodeIds | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique)

        $rackRows += [pscustomobject][ordered]@{
            ClusterAlias = $alias
            RackAlias    = "Rack-$rackCounter"
            RackId       = $rackId
            ChassisCount = $chassisIds.Count
            ChassisIds   = ($chassisIds -join ";")
            NodeCount    = $nodeIds.Count
            NodeIds      = ($nodeIds -join ";")
        }
    }

    # STORAGE DOMAINS
    try {
        $apiCalls.Add("GET /v2/storage-domains [$alias]")
        $response = Get-Json "$baseUrl/v2/storage-domains" $headers "/v2/storage-domains [$alias]"
        $storageDomains = @(Get-Objects $response @("storageDomains","items") @("id","storageDomainId"))
    }
    catch {
        $apiFailures.Add("GET /v2/storage-domains [$alias] : $($_.Exception.Message)")
        $storageDomains = @(); $incomplete = $true
    }

    $sdCounter = 0
    foreach ($sd in $storageDomains) {
        $sdCounter++
        $sdAlias = "SD-$sdCounter"
        $sdId = To-Text (First-Property $sd @("id","storageDomainId"))
        $storageDomainRows += [pscustomobject][ordered]@{
            ClusterAlias    = $alias
            StorageDomain   = $sdAlias
            StorageDomainId = $sdId
            CurrentEC       = To-Text (First-Property $sd @("ecConfig","erasureCodingConfig","ecConfiguration"))
            CurrentRF       = To-Text (First-Property $sd @("replicationFactor","rf"))
            CurrentFT       = To-Text (First-Property $sd @("faultTolerance","faultToleranceLevel","failureDomain"))
            PhysicalCapacity= To-Text (First-Property $sd @("physicalCapacityBytes","physicalCapacity"))
            UsedCapacity    = To-Text (First-Property $sd @("usedCapacityBytes","usedCapacity"))
            FreeCapacity    = To-Text (First-Property $sd @("freeCapacityBytes","freeCapacity","availableCapacityBytes"))
        }

        if ($sdId -eq $notAvailable) { continue }

        try {
            $encodedSdId = [uri]::EscapeDataString([string]$sdId)
            $apiCalls.Add("GET /v2/storage-domains/fault-tolerance-options [$alias/$sdAlias]")
            $ft = Get-Json "$baseUrl/v2/storage-domains/fault-tolerance-options?storageDomainId=$encodedSdId" $headers "/v2/storage-domains/fault-tolerance-options [$alias/$sdAlias]"
            $global = First-Property $ft @("globalTolerance") $null
            $options = @(Get-Objects $ft @("options","faultToleranceOptions","availableOptions") @("ecConfig","replicationFactor","rf"))
            if ($options.Count -eq 0) { $options = @($null) }

            foreach ($option in $options) {
                $ftRows += [pscustomobject][ordered]@{
                    ClusterAlias                   = $alias
                    StorageDomain                  = $sdAlias
                    Enabled                        = To-Text (First-Property $ft @("enabled"))
                    GlobalFTLevel                  = if ($global) { To-Text (First-Property $global @("faultToleranceLevel","level")) } else { $notAvailable }
                    GlobalFTCount                  = if ($global) { To-Text (First-Property $global @("count")) } else { $notAvailable }
                    FailureDomainCount             = To-Text (First-Property $ft @("failureDomainCount","numFailureDomains"))
                    DefaultFaultTolerance          = To-Text (First-Property $ft @("defaultFaultTolerance"))
                    DefaultEC                      = To-Text (First-Property $ft @("defaultEc","defaultEC","currentEc","currentEC"))
                    DefaultRF                      = To-Text (First-Property $ft @("defaultRf","defaultRF","currentRf","currentRF"))
                    DiskFailuresTolerated          = if ($option) { To-Text (First-Property $option @("diskFailuresTolerated","numDiskFailuresTolerated")) } else { $notAvailable }
                    FailureDomainFailuresTolerated = if ($option) { To-Text (First-Property $option @("failureDomainFailuresTolerated","numFailureDomainFailuresTolerated")) } else { $notAvailable }
                    EC                             = if ($option) { To-Text (First-Property $option @("ecConfig","ecConfiguration","erasureCodingConfig")) } else { $notAvailable }
                    RF                             = if ($option) { To-Text (First-Property $option @("replicationFactor","rf")) } else { $notAvailable }
                    Disabled                       = if ($option) { To-Text (First-Property $option @("disabled","isDisabled")) } else { $notAvailable }
                    HasWarning                     = if ($option) { To-Text (First-Property $option @("hasWarning","warning")) } else { $notAvailable }
                    IsSuboptimal                   = if ($option) { To-Text (First-Property $option @("isSuboptimal","suboptimal")) } else { $notAvailable }
                    MinFailureDomainsRequired      = if ($option) { To-Text (First-Property $option @("minFailureDomainsRequired","minimumFailureDomainsRequired")) } else { $notAvailable }
                    MinFailureDomainsToHeal        = if ($option) { To-Text (First-Property $option @("minFailureDomainsToHeal","minimumFailureDomainsRequiredToHeal")) } else { $notAvailable }
                }
            }
        }
        catch {
            $apiFailures.Add("GET FT options [$alias/$sdAlias] : $($_.Exception.Message)")
            $incomplete = $true
        }
    }

    $cNodes = @($nodeRows | Where-Object ClusterAlias -eq $alias)
    $cChassis = @($chassisRows | Where-Object ClusterAlias -eq $alias)
    $cRacks = @($rackRows | Where-Object ClusterAlias -eq $alias)
    $cFt = @($ftRows | Where-Object ClusterAlias -eq $alias)

    $unassigned = @($cChassis | Where-Object { $_.RackId -eq $notAvailable }).Count
    $rackCounts = @($cRacks | Select-Object -ExpandProperty NodeCount)
    $largestRackPct = 0
    if ($rackCounts.Count -gt 0 -and $cNodes.Count -gt 0) {
        $largest = ($rackCounts | Measure-Object -Maximum).Maximum
        $largestRackPct = [math]::Round(($largest / $cNodes.Count) * 100,1)
    }
    $uneven = $false
    if ($rackCounts.Count -gt 1) {
        $uneven = (($rackCounts | Measure-Object -Maximum).Maximum -ne ($rackCounts | Measure-Object -Minimum).Minimum)
    }
    $nodesPerChassis = @($cChassis | Select-Object -ExpandProperty NodeCount -Unique)
    $mixed = (($nodesPerChassis -contains 1) -and @($nodesPerChassis | Where-Object { $_ -ge 4 }).Count -gt 0)
    $warning = @($cFt | Where-Object { (Is-True $_.Disabled) -or (Is-True $_.HasWarning) -or (Is-True $_.IsSuboptimal) }).Count -gt 0
    $insufficient = $false
    foreach ($option in $cFt) {
        $min = Get-Number $option.MinFailureDomainsRequired
        if ($null -ne $min -and $cRacks.Count -lt $min) { $insufficient = $true; break }
    }
    $levels = @($cFt | Select-Object -ExpandProperty GlobalFTLevel -Unique | Where-Object { $_ -ne $notAvailable })
    $counts = @($cFt | Select-Object -ExpandProperty GlobalFTCount -Unique | Where-Object { $_ -ne $notAvailable })
    $failureDomains = @($cFt | Select-Object -ExpandProperty FailureDomainCount -Unique | Where-Object { $_ -ne $notAvailable })
    $currentFT = if ($levels.Count -gt 0) { $levels -join "; " } else { $notAvailable }
    $failuresTolerated = if ($counts.Count -gt 0) { $counts -join "; " } else { $notAvailable }
    $failureDomainCount = if ($failureDomains.Count -gt 0) { $failureDomains -join "; " } else { $notAvailable }
    $hardwareMix = @($cNodes | Group-Object HardwareBucket | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Count)" }) -join "; "
    if ([string]::IsNullOrWhiteSpace($hardwareMix)) { $hardwareMix = $notAvailable }

    $flag = "NORMAL"
    if ($incomplete) { $flag = "UNKNOWN" }
    elseif ($warning -or $insufficient) { $flag = "WARNING" }
    elseif ($cRacks.Count -eq 0 -or $unassigned -gt 0 -or $uneven -or $mixed) { $flag = "REVIEW" }

    $clusterSummary += [pscustomobject][ordered]@{
        ClusterAlias           = $alias
        ClusterId              = $cluster.ClusterId
        ClusterName            = $cluster.ClusterName
        SoftwareVersion        = $cluster.SoftwareVersion
        Nodes                  = $cNodes.Count
        Chassis                = $cChassis.Count
        Racks                  = $cRacks.Count
        HardwareMix            = $hardwareMix
        CurrentFailureDomain   = $currentFT
        FailureDomainCount     = $failureDomainCount
        FailuresTolerated      = $failuresTolerated
        UnassignedChassis      = $unassigned
        LargestRackNodePct     = $largestRackPct
        UnevenRackDistribution = $uneven
        MixedArchitecture      = $mixed
        WarningOrSuboptimalFT  = $warning
        InsufficientDomains    = $insufficient
        IncompleteData         = $incomplete
        AssessmentFlag         = $flag
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$internalReportPath  = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Internal_$timestamp.md"
$sanitizedReportPath = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Sanitized_$timestamp.md"
$nodeCsv             = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Nodes_Internal_$timestamp.csv"
$chassisCsv          = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Chassis_Internal_$timestamp.csv"
$rackCsv             = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_Racks_Internal_$timestamp.csv"
$sdCsv               = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_StorageDomains_Internal_$timestamp.csv"
$ftCsv               = Join-Path $outputDirectory "Cohesity_Rack_Resiliency_FTOptions_Internal_$timestamp.csv"

$nodeRows | Export-Csv $nodeCsv -NoTypeInformation -Encoding UTF8
$chassisRows | Export-Csv $chassisCsv -NoTypeInformation -Encoding UTF8
$rackRows | Export-Csv $rackCsv -NoTypeInformation -Encoding UTF8
$storageDomainRows | Export-Csv $sdCsv -NoTypeInformation -Encoding UTF8
$ftRows | Export-Csv $ftCsv -NoTypeInformation -Encoding UTF8

$internal = New-Object System.Collections.Generic.List[string]
$internal.Add("# Cohesity Rack Resiliency Assessment - Internal Detailed Report")
$internal.Add("")
$internal.Add("READ-ONLY DATA COLLECTION ONLY. No Cohesity configuration changes were performed.")
$internal.Add("")
$internal.Add("| Cluster | Cluster ID | Version | Nodes | Chassis | Racks | Hardware Mix | Current Failure Domain | Failure Domains | Failures Tolerated | Unassigned Chassis | Largest Rack % | Flag |")
$internal.Add("|---|---|---|---:|---:|---:|---|---|---|---|---:|---:|---|")
foreach ($row in $clusterSummary) {
    $internal.Add("| $($row.ClusterName) | $($row.ClusterId) | $(Escape-Markdown $row.SoftwareVersion) | $($row.Nodes) | $($row.Chassis) | $($row.Racks) | $(Escape-Markdown $row.HardwareMix) | $(Escape-Markdown $row.CurrentFailureDomain) | $(Escape-Markdown $row.FailureDomainCount) | $(Escape-Markdown $row.FailuresTolerated) | $($row.UnassignedChassis) | $($row.LargestRackNodePct)% | $($row.AssessmentFlag) |")
}
$internal.Add("")
$internal.Add("Number of POST/PUT/PATCH/DELETE operations executed: **0**")
$internal.Add("")
$internal.Add("READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment.")
$internal | Set-Content $internalReportPath -Encoding UTF8

$sanitized = New-Object System.Collections.Generic.List[string]
$sanitized.Add("# Cohesity Rack Resiliency Assessment - Sanitized Summary")
$sanitized.Add("")
$sanitized.Add("### Estate Summary")
$sanitized.Add("")
$sanitized.Add("- Total clusters: $($clusterRows.Count)")
$sanitized.Add("- Total nodes: $($nodeRows.Count)")
$sanitized.Add("- Total chassis: $($chassisRows.Count)")
$sanitized.Add("- Total configured racks: $($rackRows.Count)")
$sanitized.Add("- Clusters with racks configured: $(@($clusterSummary | Where-Object Racks -gt 0).Count)")
$sanitized.Add("- Clusters without racks configured: $(@($clusterSummary | Where-Object Racks -eq 0).Count)")
$sanitized.Add("- Chassis without rack assignment: $(($clusterSummary | Measure-Object UnassignedChassis -Sum).Sum)")
$sanitized.Add("")
$sanitized.Add("### Hardware Distribution")
$sanitized.Add("")
$sanitized.Add("| Model | Nodes | Chassis/Blocks | Clusters Using Model |")
$sanitized.Add("|---|---:|---:|---:|")
foreach ($bucket in @("CX8405","C6025","C5066","C5026","C5016","Other")) {
    $bn = @($nodeRows | Where-Object HardwareBucket -eq $bucket).Count
    $bc = @($chassisRows | Where-Object HardwareBucket -eq $bucket).Count
    $bcl = @($nodeRows | Where-Object HardwareBucket -eq $bucket | Select-Object -ExpandProperty ClusterAlias -Unique).Count
    $sanitized.Add("| $bucket | $bn | $bc | $bcl |")
}
$sanitized.Add("")
$sanitized.Add("### Cluster Resiliency Summary")
$sanitized.Add("")
$sanitized.Add("| Cluster | Nodes | Chassis | Racks | Hardware Mix | Current Failure Domain | Failures Tolerated | Rack Distribution | Assessment Flag |")
$sanitized.Add("|---|---:|---:|---:|---|---|---|---|---|")
foreach ($row in $clusterSummary) {
    $dist = @($rackRows | Where-Object ClusterAlias -eq $row.ClusterAlias | ForEach-Object { "$($_.RackAlias)=$($_.NodeCount) nodes" }) -join "; "
    if ([string]::IsNullOrWhiteSpace($dist)) { $dist = "No configured racks returned" }
    $sanitized.Add("| $($row.ClusterAlias) | $($row.Nodes) | $($row.Chassis) | $($row.Racks) | $(Escape-Markdown $row.HardwareMix) | $(Escape-Markdown $row.CurrentFailureDomain) | $(Escape-Markdown $row.FailuresTolerated) | $(Escape-Markdown $dist) | $($row.AssessmentFlag) |")
}
$sanitized.Add("")
$sanitized.Add("### Storage Domain Resiliency")
$sanitized.Add("")
$sanitized.Add("| Cluster | SD | Current EC/RF | Current FT | Failure Domains Available | Rack FT Option Available | Min Domains Required | Min Domains to Heal | Cohesity Warning/Suboptimal |")
$sanitized.Add("|---|---|---|---|---|---|---|---|---|")
foreach ($sd in $storageDomainRows) {
    $opts = @($ftRows | Where-Object { $_.ClusterAlias -eq $sd.ClusterAlias -and $_.StorageDomain -eq $sd.StorageDomain })
    $fd = @($opts | Select-Object -ExpandProperty FailureDomainCount -Unique | Where-Object { $_ -ne $notAvailable }) -join "; "
    if ([string]::IsNullOrWhiteSpace($fd)) { $fd = $notAvailable }
    $rackOption = @($opts | Where-Object { ([string]$_.GlobalFTLevel -match '(?i)rack') -or ([string]$_.FailureDomainFailuresTolerated -ne $notAvailable }).Count -gt 0
    $minReq = @($opts | Select-Object -ExpandProperty MinFailureDomainsRequired -Unique | Where-Object { $_ -ne $notAvailable }) -join "; "
    if ([string]::IsNullOrWhiteSpace($minReq)) { $minReq = $notAvailable }
    $minHeal = @($opts | Select-Object -ExpandProperty MinFailureDomainsToHeal -Unique | Where-Object { $_ -ne $notAvailable }) -join "; "
    if ([string]::IsNullOrWhiteSpace($minHeal)) { $minHeal = $notAvailable }
    $warn = @($opts | Where-Object { (Is-True $_.Disabled) -or (Is-True $_.HasWarning) -or (Is-True $_.IsSuboptimal) }).Count -gt 0
    $sanitized.Add("| $($sd.ClusterAlias) | $($sd.StorageDomain) | EC=$(Escape-Markdown $sd.CurrentEC); RF=$(Escape-Markdown $sd.CurrentRF) | $(Escape-Markdown $sd.CurrentFT) | $(Escape-Markdown $fd) | $(if($rackOption){'Yes'}else{$notAvailable}) | $(Escape-Markdown $minReq) | $(Escape-Markdown $minHeal) | $(if($warn){'Yes'}else{'No'}) |")
}
$sanitized.Add("")
$sanitized.Add("### Rack Distribution")
$sanitized.Add("")
foreach ($row in $clusterSummary) {
    $sanitized.Add("**$($row.ClusterAlias)**")
    $sanitized.Add("")
    $rr = @($rackRows | Where-Object ClusterAlias -eq $row.ClusterAlias)
    $sanitized.Add("- Rack count: $($rr.Count)")
    foreach ($rack in $rr) {
        $pct = if ($row.Nodes -gt 0) { [math]::Round(($rack.NodeCount / $row.Nodes) * 100,1) } else { 0 }
        $sanitized.Add("- $($rack.RackAlias): $($rack.ChassisCount) chassis / $($rack.NodeCount) nodes / $pct% of cluster nodes")
    }
    $sanitized.Add("- Unassigned chassis: $($row.UnassignedChassis)")
    $sanitized.Add("- Largest single-rack node concentration: $($row.LargestRackNodePct)%")
    $sanitized.Add("")
}
$sanitized.Add("### Findings")
$sanitized.Add("")
$sanitized.Add("**Confirmed**")
foreach ($row in $clusterSummary) {
    if ($row.Racks -eq 0) { $sanitized.Add("- $($row.ClusterAlias): no rack configuration was returned by GET /v2/racks.") }
    if ($row.UnassignedChassis -gt 0) { $sanitized.Add("- $($row.ClusterAlias): $($row.UnassignedChassis) chassis have no rack assignment returned by GET data.") }
    if ($row.WarningOrSuboptimalFT) { $sanitized.Add("- $($row.ClusterAlias): at least one returned FT option is disabled, warning, or suboptimal.") }
}
$sanitized.Add("")
$sanitized.Add("**Calculated**")
foreach ($row in $clusterSummary) {
    if ($row.UnevenRackDistribution) { $sanitized.Add("- $($row.ClusterAlias): rack node distribution is uneven; largest rack concentration is $($row.LargestRackNodePct)%.") }
    if ($row.MixedArchitecture) { $sanitized.Add("- $($row.ClusterAlias): mixed 1-node and 4-or-more-node chassis/block architecture was returned.") }
    if ($row.InsufficientDomains) { $sanitized.Add("- $($row.ClusterAlias): configured rack count is below at least one returned minFailureDomainsRequired value.") }
}
$sanitized.Add("")
$sanitized.Add("**Unknown / Requires Cohesity Confirmation**")
$unknown = @($clusterSummary | Where-Object IncompleteData -eq $true)
if ($unknown.Count -eq 0) { $sanitized.Add("- No additional GET collection gaps identified. Option availability is not proof that a configuration is safe.") }
else { foreach ($row in $unknown) { $sanitized.Add("- $($row.ClusterAlias): one or more approved GET collections failed or returned incomplete data.") } }
$sanitized.Add("")
$sanitized.Add("### Most Important Exceptions")
$exceptions = @($clusterSummary | Where-Object { $_.Racks -eq 0 -or $_.UnassignedChassis -gt 0 -or $_.UnevenRackDistribution -or $_.WarningOrSuboptimalFT -or $_.InsufficientDomains -or $_.MixedArchitecture -or $_.IncompleteData })
if ($exceptions.Count -eq 0) { $sanitized.Add("- None proven by the collected GET results.") }
else { foreach ($row in $exceptions) { $sanitized.Add("- $($row.ClusterAlias): $($row.AssessmentFlag)") } }
$sanitized.Add("")
$sanitized.Add("### Data Quality")
$uniqueCalls = @($apiCalls | ForEach-Object { $_ -replace ' \[.*$','' } | Select-Object -Unique)
$sanitized.Add("- GET APIs queried: $($uniqueCalls -join '; ')")
$sanitized.Add("- GET APIs that failed: $($apiFailures.Count)")
$sanitized.Add("- Fields unavailable: individual unavailable values are marked $notAvailable")
$sanitized.Add("- Clusters with incomplete data: $(@($clusterSummary | Where-Object IncompleteData -eq $true).Count)")
$sanitized.Add("- Number of POST/PUT/PATCH/DELETE operations executed: **0**")
$sanitized.Add("")
$sanitized.Add("READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment.")
$sanitized | Set-Content $sanitizedReportPath -Encoding UTF8

Write-Host ""
Write-Host "================ READ-ONLY COLLECTION SUMMARY ================" -ForegroundColor Cyan
Write-Host "Clusters collected        : $($clusterRows.Count)"
Write-Host "Nodes collected           : $($nodeRows.Count)"
Write-Host "Chassis collected         : $($chassisRows.Count)"
Write-Host "Racks collected           : $($rackRows.Count)"
Write-Host "Storage Domains collected : $($storageDomainRows.Count)"
Write-Host "FT option rows collected  : $($ftRows.Count)"
Write-Host "GET failures              : $($apiFailures.Count)"
Write-Host "Non-GET operations        : 0" -ForegroundColor Green
Write-Host "Internal report           : $internalReportPath"
Write-Host "Sanitized report          : $sanitizedReportPath"
if ($clusterRows.Count -ne $expectedClusters) { Write-Host "Expected clusters         : $expectedClusters (CHECK REQUIRED)" -ForegroundColor Yellow }
if ($nodeRows.Count -ne $expectedNodes) { Write-Host "Expected nodes            : $expectedNodes (CHECK REQUIRED)" -ForegroundColor Yellow }
if ($apiFailures.Count -gt 0) {
    Write-Host ""
    Write-Host "GET FAILURES" -ForegroundColor Yellow
    $apiFailures | ForEach-Object { Write-Host "- $_" }
}
Write-Host ""
Write-Host "READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment." -ForegroundColor Green
