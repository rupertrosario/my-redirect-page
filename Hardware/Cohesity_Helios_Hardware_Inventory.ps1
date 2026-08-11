# Cohesity Helios Hardware Inventory
# Expected environment: 22 clusters / 169 nodes
# GET-only | PowerShell 5.1 compatible

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\HardwareInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"
$expectedClusters    = 22
$expectedNodes       = 169

if (-not (Test-Path $logDirectory -PathType Container)) { New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null }
if (-not (Test-Path $helperPath -PathType Leaf)) { throw "API key helper not found: $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key file not found: $encryptedApiKeyPath" }

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES API key helper returned an empty API key." }

function New-CohesityHeaders {
    param([string]$AccessClusterId)
    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($AccessClusterId)) { $headers["accessClusterId"] = $AccessClusterId }
    return $headers
}

function Invoke-CohesityGet {
    param([Parameter(Mandatory)][string]$Uri,[Parameter(Mandatory)][hashtable]$Headers)
    if ($PSVersionTable.PSVersion.Major -lt 6) { $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop }
    else { $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop }
    if ([string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function Get-PropertyValue {
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

function Get-HardwareObjects {
    param($Response)
    if ($null -eq $Response) { return @() }
    if ($Response -is [System.Array]) { return @($Response) }
    foreach ($propertyName in @("hardwareInfo","hardwareInfos","nodeHardwareInfo","nodeHardwareInfos")) {
        $property = $Response.PSObject.Properties | Where-Object { $_.Name -ieq $propertyName } | Select-Object -First 1
        if ($property -and $null -ne $property.Value) { return @($property.Value) }
    }
    if ($Response.PSObject.Properties["nodeSerial"] -or $Response.PSObject.Properties["cohesityNodeSerial"] -or $Response.PSObject.Properties["chassisSerial"]) { return @($Response) }
    return @()
}

Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "      COHESITY HARDWARE INVENTORY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan

try { $clusterResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-CohesityHeaders) }
catch { throw "Unable to retrieve Helios clusters: $($_.Exception.Message)" }

$clusterObjects = @(Get-ClusterObjects -Response $clusterResponse)
$clusters = @()
foreach ($cluster in $clusterObjects) {
    $clusterId = Get-PropertyValue $cluster @("clusterId","id")
    $clusterName = Get-PropertyValue $cluster @("clusterName","displayName","name")
    if ($clusterId -eq "N/A") { continue }
    $clusters += [PSCustomObject][ordered]@{ ClusterName=$clusterName; ClusterId=[string]$clusterId }
}
$clusters = @($clusters | Sort-Object ClusterName,ClusterId -Unique)
if ($clusters.Count -eq 0) { throw "No usable Cohesity clusters were returned by Helios." }

$hardwareInventory = @()
$clusterStatus = @()
foreach ($cluster in $clusters) {
    try {
        $response = Invoke-CohesityGet -Uri "$baseUrl/v2/node/hardware-info" -Headers (New-CohesityHeaders -AccessClusterId $cluster.ClusterId)
        $hardwareObjects = @(Get-HardwareObjects -Response $response)
        foreach ($hardware in $hardwareObjects) {
            $hardwareInventory += [PSCustomObject][ordered]@{
                ClusterName=$cluster.ClusterName
                ChassisModel=Get-PropertyValue $hardware @("chassisModel")
                ChassisSerial=Get-PropertyValue $hardware @("chassisSerial")
                ChassisType=Get-PropertyValue $hardware @("chassisType")
                CohesityChassisSerial=Get-PropertyValue $hardware @("cohesityChassisSerial")
                CohesityNodeSerial=Get-PropertyValue $hardware @("cohesityNodeSerial")
                HbaModel=Get-PropertyValue $hardware @("hbaModel")
                IpmiLanChannel=Get-PropertyValue $hardware @("ipmiLanChannel")
                MaxSlots=Get-PropertyValue $hardware @("maxSlots")
                NodeModel=Get-PropertyValue $hardware @("nodeModel")
                NodeSerial=Get-PropertyValue $hardware @("nodeSerial")
                ProductModel=Get-PropertyValue $hardware @("productModel")
                ProductModelType=Get-PropertyValue $hardware @("productModelType")
                SlotNumber=Get-PropertyValue $hardware @("slotNumber")
            }
        }
        $clusterStatus += [PSCustomObject][ordered]@{ClusterName=$cluster.ClusterName;ClusterId=$cluster.ClusterId;Nodes=$hardwareObjects.Count;Status="Success";Error=""}
    }
    catch { $clusterStatus += [PSCustomObject][ordered]@{ClusterName=$cluster.ClusterName;ClusterId=$cluster.ClusterId;Nodes=0;Status="Failed";Error=$_.Exception.Message} }
}

$hardwareInventory = @($hardwareInventory | Sort-Object ClusterName,CohesityNodeSerial,NodeSerial)
Write-Host "`nHARDWARE INVENTORY" -ForegroundColor Cyan
$hardwareInventory | Format-Table ClusterName,ChassisModel,ChassisSerial,ChassisType,CohesityChassisSerial,CohesityNodeSerial,HbaModel,IpmiLanChannel,MaxSlots,NodeModel,NodeSerial,ProductModel,ProductModelType,SlotNumber -AutoSize | Out-Host

$timestamp=Get-Date -Format "yyyyMMdd_HHmm"
$inventoryCsv=Join-Path $logDirectory "Cohesity_Hardware_Inventory_$timestamp.csv"
$statusCsv=Join-Path $logDirectory "Cohesity_Hardware_Cluster_Status_$timestamp.csv"
$hardwareInventory | Export-Csv -Path $inventoryCsv -NoTypeInformation -Encoding UTF8
$clusterStatus | Export-Csv -Path $statusCsv -NoTypeInformation -Encoding UTF8

$successfulClusters=@($clusterStatus|Where-Object Status -eq "Success").Count
$failedClusters=@($clusterStatus|Where-Object Status -eq "Failed").Count
Write-Host "`n==============================================" -ForegroundColor Cyan
Write-Host "        HARDWARE INVENTORY SUMMARY" -ForegroundColor White
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "Clusters discovered : $($clusters.Count)"
Write-Host "Clusters successful : $successfulClusters"
Write-Host "Clusters failed     : $failedClusters"
Write-Host "Total nodes         : $($hardwareInventory.Count)"
Write-Host "Expected clusters   : $expectedClusters"
Write-Host "Expected nodes      : $expectedNodes"
if ($clusters.Count -eq $expectedClusters -and $hardwareInventory.Count -eq $expectedNodes -and $failedClusters -eq 0) { Write-Host "VALIDATION          : PASS" -ForegroundColor Green }
else { Write-Host "VALIDATION          : CHECK REQUIRED" -ForegroundColor Yellow }
Write-Host "Inventory CSV       : $inventoryCsv"
Write-Host "Status CSV          : $statusCsv"
if ($failedClusters -gt 0) { Write-Host "`nFAILED CLUSTERS" -ForegroundColor Yellow; $clusterStatus | Where-Object Status -eq "Failed" | Format-Table ClusterName,ClusterId,Error -AutoSize -Wrap | Out-Host }
