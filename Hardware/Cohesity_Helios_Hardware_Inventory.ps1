# Cohesity Helios Node + Chassis Inventory
# GET-only | PowerShell 5.1 compatible
# APIs used:
#   GET /v2/mcm/cluster-mgmt/info
#   GET /v2/clusters/nodes
#   GET /v2/chassis

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\HardwareInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$expectedClusters    = 22
$expectedNodes       = 169

if (-not (Test-Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}
if (-not (Test-Path $helperPath -PathType Leaf)) { throw "API key helper not found: $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key file not found: $encryptedApiKeyPath" }

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES API key helper returned an empty API key." }

function New-CohesityHeaders {
    param([string]$AccessClusterId)
    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($AccessClusterId)) {
        $headers["accessClusterId"] = $AccessClusterId
    }
    return $headers
}

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }
    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function Get-Value {
    param($Object,[string[]]$Names)
    if ($null -eq $Object) { return "N/A" }
    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
        if ($property -and $null -ne $property.Value) {
            $text = ([string]$property.Value).Trim()
            if (-not [string]::IsNullOrWhiteSpace($text)) { return $text }
        }
    }
    return "N/A"
}

function Get-ArrayValue {
    param($Object,[string[]]$Names)
    if ($null -eq $Object) { return @() }
    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
        if ($property -and $null -ne $property.Value) { return @($property.Value) }
    }
    return @()
}

function Get-ClusterObjects {
    param($Response)
    if ($null -eq $Response) { return @() }
    foreach ($name in @("cohesityClusters","clusters","clusterInfos")) {
        $items = Get-ArrayValue $Response @($name)
        if ($items.Count -gt 0) { return $items }
    }
    if ($Response.mcmInfo -and $Response.mcmInfo.clusterInfos) { return @($Response.mcmInfo.clusterInfos) }
    if ($Response -is [System.Array]) { return @($Response) }
    return @()
}

function Get-NodeObjects {
    param($Response)
    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    foreach ($name in @("nodes","nodeList","items")) {
        $items = Get-ArrayValue $Response @($name)
        if ($items.Count -gt 0) { return $items }
    }
    if ($Response.PSObject.Properties["nodeId"] -or $Response.PSObject.Properties["id"]) { return @($Response) }
    return @()
}

function Get-ChassisObjects {
    param($Response)
    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    foreach ($name in @("chassis","chassisList","items")) {
        $items = Get-ArrayValue $Response @($name)
        if ($items.Count -gt 0) { return $items }
    }
    if ($Response.PSObject.Properties["nodeIds"] -or $Response.PSObject.Properties["serialNumber"] -or $Response.PSObject.Properties["hardwareModel"]) { return @($Response) }
    return @()
}

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "   COHESITY NODE + CHASSIS INVENTORY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan

try {
    $clusterResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-CohesityHeaders)
}
catch {
    throw "Unable to retrieve Helios clusters: $($_.Exception.Message)"
}

$clusters = @()
foreach ($cluster in @(Get-ClusterObjects $clusterResponse)) {
    $clusterId = Get-Value $cluster @("clusterId","id")
    if ($clusterId -eq "N/A") { continue }
    $clusters += [pscustomobject][ordered]@{
        ClusterName = Get-Value $cluster @("clusterName","displayName","name")
        ClusterId   = [string]$clusterId
    }
}
$clusters = @($clusters | Sort-Object ClusterName,ClusterId -Unique)
if ($clusters.Count -eq 0) { throw "No usable Cohesity clusters were returned by Helios." }

$inventory = @()
$clusterStatus = @()
$chassisTotal = 0
$mappedNodes = 0

foreach ($cluster in $clusters) {
    try {
        # GET only: all nodes when ids is omitted.
        $nodeResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/clusters/nodes" -Headers (New-CohesityHeaders -AccessClusterId $cluster.ClusterId)
        $nodes = @(Get-NodeObjects $nodeResponse)

        # GET only: all chassis in the cluster.
        $chassisResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/chassis" -Headers (New-CohesityHeaders -AccessClusterId $cluster.ClusterId)
        $chassisList = @(Get-ChassisObjects $chassisResponse)
        $chassisTotal += $chassisList.Count

        # Build nodeId -> chassis lookup from chassis.nodeIds[].
        $chassisByNodeId = @{}
        foreach ($chassis in $chassisList) {
            $nodeIds = @(Get-ArrayValue $chassis @("nodeIds"))
            foreach ($chassisNodeId in $nodeIds) {
                if ($null -ne $chassisNodeId) {
                    $chassisByNodeId[[string]$chassisNodeId] = $chassis
                }
            }
        }

        foreach ($node in $nodes) {
            $nodeId = Get-Value $node @("nodeId","id")
            $chassis = $null
            if ($nodeId -ne "N/A" -and $chassisByNodeId.ContainsKey([string]$nodeId)) {
                $chassis = $chassisByNodeId[[string]$nodeId]
                $mappedNodes++
            }

            $inventory += [pscustomobject][ordered]@{
                ClusterName            = $cluster.ClusterName
                Hostname               = Get-Value $node @("hostname","hostName","name")
                NodeIP                 = Get-Value $node @("nodeIp","ipAddress","ip")
                IPMIIP                 = Get-Value $node @("ipmiIp","ipmiIP")
                NodeSerial             = Get-Value $node @("nodeSerial")
                CohesityNodeSerial     = Get-Value $node @("cohesityNodeSerial")
                NodeModel              = Get-Value $node @("nodeModel")
                ProductModel           = Get-Value $node @("productModel")
                SlotNumber             = Get-Value $node @("slotNumber","slot")
                ChassisSerial          = if ($chassis) { Get-Value $chassis @("serialNumber","chassisSerial") } else { Get-Value $node @("chassisSerial") }
                CohesityChassisSerial  = Get-Value $node @("cohesityChassisSerial")
                ChassisModel           = if ($chassis) { Get-Value $chassis @("hardwareModel","chassisModel") } else { Get-Value $node @("chassisModel") }
                ChassisName            = if ($chassis) { Get-Value $chassis @("name","chassisName") } else { "N/A" }
                RackId                 = if ($chassis) { Get-Value $chassis @("rackId") } else { "N/A" }
            }
        }

        $clusterStatus += [pscustomobject][ordered]@{
            ClusterName = $cluster.ClusterName
            ClusterId   = $cluster.ClusterId
            Nodes       = $nodes.Count
            Chassis     = $chassisList.Count
            Status      = "Success"
            Error       = ""
        }
    }
    catch {
        $clusterStatus += [pscustomobject][ordered]@{
            ClusterName = $cluster.ClusterName
            ClusterId   = $cluster.ClusterId
            Nodes       = 0
            Chassis     = 0
            Status      = "Failed"
            Error       = $_.Exception.Message
        }
    }
}

$inventory = @($inventory | Sort-Object ClusterName,Hostname,NodeSerial)

Write-Host "`nNODE + CHASSIS INVENTORY" -ForegroundColor Cyan
if ($inventory.Count -gt 0) {
    $inventory |
        Select-Object ClusterName,Hostname,NodeIP,IPMIIP,NodeSerial,CohesityNodeSerial,NodeModel,ProductModel,SlotNumber,ChassisSerial,CohesityChassisSerial,ChassisModel,ChassisName,RackId |
        Format-Table -AutoSize -Wrap |
        Out-Host
}
else {
    Write-Host "No node records were returned." -ForegroundColor Yellow
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_Node_Chassis_Inventory_$timestamp.csv"
if ($inventory.Count -gt 0) {
    $inventory | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
}

$successfulClusters = @($clusterStatus | Where-Object Status -eq "Success").Count
$failedClusters = @($clusterStatus | Where-Object Status -eq "Failed").Count
$nodeIpCount = @($inventory | Where-Object { $_.NodeIP -ne "N/A" }).Count
$ipmiIpCount = @($inventory | Where-Object { $_.IPMIIP -ne "N/A" }).Count
$unmappedNodes = $inventory.Count - $mappedNodes

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "              INVENTORY SUMMARY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Clusters discovered     : $($clusters.Count)"
Write-Host "Clusters successful     : $successfulClusters"
Write-Host "Clusters failed         : $failedClusters"
Write-Host "Nodes discovered        : $($inventory.Count)"
Write-Host "Chassis discovered      : $chassisTotal"
Write-Host "Nodes mapped to chassis : $mappedNodes"
Write-Host "Unmapped nodes          : $unmappedNodes"
Write-Host "Nodes with Node IP      : $nodeIpCount"
Write-Host "Nodes with IPMI IP      : $ipmiIpCount"
Write-Host "Expected clusters       : $expectedClusters"
Write-Host "Expected nodes          : $expectedNodes"

if ($clusters.Count -eq $expectedClusters -and $inventory.Count -eq $expectedNodes -and $failedClusters -eq 0 -and $unmappedNodes -eq 0) {
    Write-Host "Validation              : PASS" -ForegroundColor Green
}
else {
    Write-Host "Validation              : CHECK REQUIRED" -ForegroundColor Yellow
}

if ($inventory.Count -gt 0) { Write-Host "CSV output              : $csvPath" }

if ($failedClusters -gt 0) {
    Write-Host "`nFAILED CLUSTERS" -ForegroundColor Yellow
    $clusterStatus | Where-Object Status -eq "Failed" | Select-Object ClusterName,ClusterId,Error | Format-Table -AutoSize -Wrap | Out-Host
}
