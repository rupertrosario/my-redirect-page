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

$expectedClusters = 22
$expectedNodes    = 169

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

function As-Array {
    param($Value)

    if ($null -eq $Value) {
        return @()
    }

    return @($Value)
}

function First-Property {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object -or $Object -is [string]) {
        return "N/A"
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                if ($null -eq $property.Value) {
                    continue
                }

                $text = ([string]$property.Value).Trim()
                if (-not [string]::IsNullOrWhiteSpace($text)) {
                    return $text
                }
            }
        }
    }

    return "N/A"
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

function Get-Clusters {
    param($Response)

    if ($null -eq $Response) {
        return @()
    }

    if ($Response.cohesityClusters) {
        return @($Response.cohesityClusters)
    }
    if ($Response.clusters) {
        return @($Response.clusters)
    }
    if ($Response.clusterInfos) {
        return @($Response.clusterInfos)
    }
    if ($Response.mcmInfo -and $Response.mcmInfo.clusterInfos) {
        return @($Response.mcmInfo.clusterInfos)
    }
    if ($Response -is [System.Array]) {
        return @($Response)
    }

    return @()
}

function Get-Nodes {
    param($Response)

    if ($null -eq $Response) {
        return @()
    }
    if ($Response -is [System.Array]) {
        return @($Response)
    }
    if ($Response.nodes) {
        return @($Response.nodes)
    }
    if ($Response.nodeList) {
        return @($Response.nodeList)
    }
    if ($Response.items) {
        return @($Response.items)
    }
    if ($Response.PSObject.Properties["nodeId"] -or $Response.PSObject.Properties["id"]) {
        return @($Response)
    }

    return @()
}

function Get-Chassis {
    param($Response)

    if ($null -eq $Response) {
        return @()
    }
    if ($Response -is [System.Array]) {
        return @($Response)
    }
    if ($Response.chassis) {
        return @($Response.chassis)
    }
    if ($Response.chassisList) {
        return @($Response.chassisList)
    }
    if ($Response.items) {
        return @($Response.items)
    }
    if ($Response.PSObject.Properties["nodeIds"] -or $Response.PSObject.Properties["hardwareModel"]) {
        return @($Response)
    }

    return @()
}

function Get-StorageDomains {
    param($Response)

    if ($null -eq $Response) {
        return @()
    }
    if ($Response -is [System.Array]) {
        return @($Response)
    }
    if ($Response.storageDomains) {
        return @($Response.storageDomains)
    }
    if ($Response.items) {
        return @($Response.items)
    }
    if ($Response.PSObject.Properties["storagePolicy"] -or $Response.PSObject.Properties["id"]) {
        return @($Response)
    }

    return @()
}

function Get-HardwareBucket {
    param([string]$Model)

    if ([string]::IsNullOrWhiteSpace($Model) -or $Model -eq "N/A") {
        return "Other"
    }

    switch -Regex ($Model.Trim()) {
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

    foreach ($node in @(As-Array $Nodes)) {
        $model = First-Property $node @("productModel","nodeModel","hardwareModel","chassisModel")
        $bucket = Get-HardwareBucket $model
        $counts[$bucket]++
    }

    $parts = @()
    foreach ($name in @("CX8405","C6025","C5066","C5026","C5016","Other")) {
        if ($counts[$name] -gt 0) {
            $parts += "$name=$($counts[$name])"
        }
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return ($parts -join "; ")
}

function Format-NodesPerChassis {
    param($ChassisList)

    $counts = @()

    foreach ($chassis in @(As-Array $ChassisList)) {
        $nodeIds = @(Array-Property $chassis @("nodeIds"))
        $counts += $nodeIds.Count
    }

    if ($counts.Count -eq 0) {
        return "N/A"
    }

    return (($counts | Sort-Object) -join ",")
}

function Get-StoragePolicySummary {
    param($StorageDomains)

    $domainCount = 0
    $ecEnabled = @()
    $ecDataCoded = @()
    $inlineEc = @()

    foreach ($sd in @(As-Array $StorageDomains)) {
        $domainCount++
        $alias = "SD-$domainCount"

        $storagePolicy = $null
        foreach ($property in @($sd.PSObject.Properties)) {
            if ($property.Name -ieq "storagePolicy") {
                $storagePolicy = $property.Value
                break
            }
        }

        if ($null -eq $storagePolicy) {
            $ecEnabled += "$alias=N/A"
            $ecDataCoded += "$alias=N/A"
            $inlineEc += "$alias=N/A"
            continue
        }

        $ecParams = $null
        foreach ($property in @($storagePolicy.PSObject.Properties)) {
            if ($property.Name -ieq "erasureCodingParams") {
                $ecParams = $property.Value
                break
            }
        }

        if ($null -eq $ecParams) {
            $ecEnabled += "$alias=N/A"
            $ecDataCoded += "$alias=N/A"
            $inlineEc += "$alias=N/A"
            continue
        }

        $enabled = First-Property $ecParams @("enabled")
        $dataStripes = First-Property $ecParams @("numDataStripes")
        $codedStripes = First-Property $ecParams @("numCodedStripes")
        $inlineEnabled = First-Property $ecParams @("inlineEnabled")

        $ecEnabled += "$alias=$enabled"

        if ($dataStripes -ne "N/A" -and $codedStripes -ne "N/A") {
            $ecDataCoded += "$alias=$dataStripes`:$codedStripes"
        }
        else {
            $ecDataCoded += "$alias=N/A"
        }

        $inlineEc += "$alias=$inlineEnabled"
    }

    if ($domainCount -eq 0) {
        return [pscustomobject][ordered]@{
            Count       = 0
            ECEnabled   = "N/A"
            ECDataCoded = "N/A"
            InlineEC    = "N/A"
        }
    }

    return [pscustomobject][ordered]@{
        Count       = $domainCount
        ECEnabled   = ($ecEnabled -join "; ")
        ECDataCoded = ($ecDataCoded -join "; ")
        InlineEC    = ($inlineEc -join "; ")
    }
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host " COHESITY RACK READINESS COLLECTION - GET ONLY" -ForegroundColor White
Write-Host "========================================================" -ForegroundColor Cyan

try {
    $clusterResponse = Get-Json `
        -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" `
        -Headers (New-Headers)
}
catch {
    throw "Failed to discover Helios clusters: $($_.Exception.Message)"
}

$clusterObjects = @(Get-Clusters $clusterResponse)
if ($clusterObjects.Count -eq 0) {
    throw "No clusters returned by GET /v2/mcm/cluster-mgmt/info."
}

$clusters = @()
foreach ($cluster in $clusterObjects) {
    $clusterId = First-Property $cluster @("clusterId","id")
    if ($clusterId -eq "N/A") {
        continue
    }

    $clusters += [pscustomobject][ordered]@{
        ClusterName = First-Property $cluster @("clusterName","displayName","name")
        ClusterId   = $clusterId
        Version     = First-Property $cluster @("softwareVersion","version","clusterSoftwareVersion")
    }
}

$clusters = @($clusters | Sort-Object ClusterName,ClusterId -Unique)
if ($clusters.Count -eq 0) {
    throw "No usable clusters returned by Helios."
}

$internalRows = @()
$sanitizedRows = @()
$failures = @()
$totalNodes = 0
$totalChassis = 0
$totalStorageDomains = 0
$clusterNumber = 0

foreach ($cluster in $clusters) {
    $clusterNumber++
    $clusterAlias = "Cluster-{0:D2}" -f $clusterNumber
    $headers = New-Headers -ClusterId $cluster.ClusterId

    $nodes = @()
    $chassisList = @()
    $storageDomains = @()
    $status = "OK"

    try {
        $nodeResponse = Get-Json `
            -Uri "$baseUrl/v2/clusters/nodes" `
            -Headers $headers
        $nodes = @(Get-Nodes $nodeResponse)
    }
    catch {
        $status = "PARTIAL"
        $failures += "$clusterAlias | GET /v2/clusters/nodes | $($_.Exception.Message)"
    }

    try {
        $chassisResponse = Get-Json `
            -Uri "$baseUrl/v2/chassis" `
            -Headers $headers
        $chassisList = @(Get-Chassis $chassisResponse)
    }
    catch {
        $status = "PARTIAL"
        $failures += "$clusterAlias | GET /v2/chassis | $($_.Exception.Message)"
    }

    try {
        $storageDomainResponse = Get-Json `
            -Uri "$baseUrl/v2/storage-domains?matchPartialNames=false&includeTenants=true&includeStats=true" `
            -Headers $headers
        $storageDomains = @(Get-StorageDomains $storageDomainResponse)
    }
    catch {
        $status = "PARTIAL"
        $failures += "$clusterAlias | GET /v2/storage-domains | $($_.Exception.Message)"
    }

    $version = $cluster.Version
    if ($version -eq "N/A" -and $nodes.Count -gt 0) {
        $version = First-Property $nodes[0] @("softwareVersion","version")
    }

    $hardwareMix = Format-HardwareMix -Nodes $nodes
    $nodesPerChassis = Format-NodesPerChassis -ChassisList $chassisList
    $storagePolicy = Get-StoragePolicySummary -StorageDomains $storageDomains

    $totalNodes += $nodes.Count
    $totalChassis += $chassisList.Count
    $totalStorageDomains += $storagePolicy.Count

    $internalRows += [pscustomobject][ordered]@{
        Cluster         = $cluster.ClusterName
        Version         = $version
        Nodes           = $nodes.Count
        Chassis         = $chassisList.Count
        HardwareMix     = $hardwareMix
        NodesPerChassis = $nodesPerChassis
        StorageDomains  = $storagePolicy.Count
        ECEnabled       = $storagePolicy.ECEnabled
        ECDataCoded     = $storagePolicy.ECDataCoded
        InlineEC        = $storagePolicy.InlineEC
        Status          = $status
    }

    $sanitizedRows += [pscustomobject][ordered]@{
        Cluster         = $clusterAlias
        Version         = $version
        Nodes           = $nodes.Count
        Chassis         = $chassisList.Count
        HardwareMix     = $hardwareMix
        NodesPerChassis = $nodesPerChassis
        StorageDomains  = $storagePolicy.Count
        ECEnabled       = $storagePolicy.ECEnabled
        ECDataCoded     = $storagePolicy.ECDataCoded
        InlineEC        = $storagePolicy.InlineEC
        Status          = $status
    }
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $outputDirectory "Cohesity_Rack_Readiness_Detail_$timestamp.csv"
$summaryPath = Join-Path $outputDirectory "Cohesity_Rack_Readiness_Summary_$timestamp.txt"

$internalRows |
    Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

$tableText = $sanitizedRows |
    Format-Table Cluster,Version,Nodes,Chassis,HardwareMix,NodesPerChassis,StorageDomains,ECEnabled,ECDataCoded,InlineEC,Status -AutoSize -Wrap |
    Out-String -Width 4096

$summaryLines = @()
$summaryLines += "COHESITY RACK READINESS COLLECTION - SANITIZED"
$summaryLines += ""
$summaryLines += $tableText.TrimEnd()
$summaryLines += ""
$summaryLines += "SUMMARY"
$summaryLines += "Clusters        : $($clusters.Count)"
$summaryLines += "Nodes           : $totalNodes"
$summaryLines += "Chassis         : $totalChassis"
$summaryLines += "Storage Domains : $totalStorageDomains"
$summaryLines += "GET failures    : $($failures.Count)"
$summaryLines += "Non-GET calls   : 0"

if ($clusters.Count -ne $expectedClusters) {
    $summaryLines += "Expected clusters: $expectedClusters (CHECK)"
}
if ($totalNodes -ne $expectedNodes) {
    $summaryLines += "Expected nodes   : $expectedNodes (CHECK)"
}

if ($failures.Count -gt 0) {
    $summaryLines += ""
    $summaryLines += "GET FAILURES"
    $summaryLines += $failures
}

$summaryLines += ""
$summaryLines += "READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment."

$summaryLines | Set-Content -Path $summaryPath -Encoding UTF8

Write-Host ""
$sanitizedRows |
    Format-Table Cluster,Version,Nodes,Chassis,HardwareMix,NodesPerChassis,StorageDomains,ECEnabled,ECDataCoded,InlineEC,Status -AutoSize -Wrap |
    Out-Host

Write-Host ""
Write-Host "SUMMARY" -ForegroundColor Cyan
Write-Host "Clusters        : $($clusters.Count)"
Write-Host "Nodes           : $totalNodes"
Write-Host "Chassis         : $totalChassis"
Write-Host "Storage Domains : $totalStorageDomains"
Write-Host "GET failures    : $($failures.Count)"
Write-Host "Non-GET calls   : 0" -ForegroundColor Green
Write-Host "CSV             : $csvPath"
Write-Host "TXT summary     : $summaryPath"

if ($failures.Count -gt 0) {
    Write-Host ""
    Write-Host "GET FAILURES" -ForegroundColor Yellow
    $failures | ForEach-Object { Write-Host "- $_" }
}

Write-Host ""
Write-Host "READ-ONLY VALIDATION: No Cohesity configuration was modified during this assessment." -ForegroundColor Green
