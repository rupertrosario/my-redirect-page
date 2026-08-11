# Cohesity Helios Hardware Inventory
# Helios-level | GET-only | PowerShell 5.1 compatible
# Discovers Helios-connected clusters and retrieves node hardware information
# from GET /v2/node/hardware-info using the accessClusterId header.

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\HardwareInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $logDirectory -PathType Container)) { New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null }
if (-not (Test-Path $helperPath -PathType Leaf)) { throw "API key helper not found at $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key file not found at $encryptedApiKeyPath" }

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES API key helper returned an empty API key." }

function New-Headers {
    param([string]$AccessClusterId)
    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($AccessClusterId)) { $headers["accessClusterId"] = $AccessClusterId }
    return $headers
}

function Get-Json {
    param([Parameter(Mandatory)][string]$Uri,[Parameter(Mandatory)][hashtable]$Headers)
    if ($PSVersionTable.PSVersion.Major -lt 6) { $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop }
    else { $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop }
    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function As-Array { param($Value) if ($null -eq $Value) { return @() }; return @($Value) }

function Value-OrNA {
    param($Value)
    if ($null -eq $Value) { return "N/A" }
    if ($Value -is [System.Array]) {
        $items = @($Value | ForEach-Object { if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) { ([string]$_).Trim() } })
        if ($items.Count -eq 0) { return "N/A" }
        return (($items | Select-Object -Unique) -join "; ")
    }
    $text = ([string]$Value).Trim(); if ([string]::IsNullOrWhiteSpace($text)) { return "N/A" }; return $text
}

function First-Property {
    param($Object,[string[]]$Names)
    if ($null -eq $Object -or $Object -is [string]) { return "N/A" }
    foreach ($name in $Names) { foreach ($property in @($Object.PSObject.Properties)) { if ($property.Name -ieq $name) { $value = Value-OrNA $property.Value; if ($value -ne "N/A") { return $value } } } }
    return "N/A"
}

function Get-ClusterObjects {
    param($ClusterJson)
    if ($null -eq $ClusterJson) { return @() }
    if ($ClusterJson.cohesityClusters) { return @(As-Array $ClusterJson.cohesityClusters) }
    if ($ClusterJson.clusters) { return @(As-Array $ClusterJson.clusters) }
    if ($ClusterJson.clusterInfos) { return @(As-Array $ClusterJson.clusterInfos) }
    if ($ClusterJson.mcmInfo -and $ClusterJson.mcmInfo.clusterInfos) { return @(As-Array $ClusterJson.mcmInfo.clusterInfos) }
    if ($ClusterJson -is [System.Array]) { return @(As-Array $ClusterJson) }
    return @()
}

function Get-HardwareObjects {
    param($HardwareJson)
    if ($null -eq $HardwareJson) { return @() }
    foreach ($propertyName in @("hardwareInfo","hardwareInfos","nodes","nodeHardwareInfo","nodeHardwareInfos")) {
        $property = @($HardwareJson.PSObject.Properties | Where-Object { $_.Name -ieq $propertyName }) | Select-Object -First 1
        if ($property -and $null -ne $property.Value) { return @(As-Array $property.Value) }
    }
    if ($HardwareJson -is [System.Array]) { return @(As-Array $HardwareJson) }
    if ($HardwareJson.PSObject.Properties["chassisType"] -or $HardwareJson.PSObject.Properties["nodeSerial"] -or $HardwareJson.PSObject.Properties["productModel"]) { return @($HardwareJson) }
    return @()
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "     COHESITY HELIOS HARDWARE INVENTORY" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan

try { $clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers) }
catch { throw "Failed to query Helios cluster inventory: $($_.Exception.Message)" }

$clusterObjects = @(Get-ClusterObjects $clusterJson)
if ($clusterObjects.Count -eq 0) { throw "Helios returned no clusters from /v2/mcm/cluster-mgmt/info." }
$clusters = @()
foreach ($cluster in $clusterObjects) {
    $clusterId = First-Property $cluster @("clusterId","id"); $clusterName = First-Property $cluster @("clusterName","displayName","name")
    if ($clusterId -eq "N/A") { continue }
    $clusters += [pscustomobject][ordered]@{ ClusterName=$clusterName; ClusterId=[string]$clusterId }
}
$clusters = @($clusters | Sort-Object ClusterName,ClusterId -Unique)
if ($clusters.Count -eq 0) { throw "No usable cluster IDs were found in the Helios cluster inventory response." }

$rows=@(); $clusterResults=@()
foreach ($cluster in $clusters) {
    try {
        $hardwareJson = Get-Json -Uri "$baseUrl/v2/node/hardware-info" -Headers (New-Headers -AccessClusterId $cluster.ClusterId)
        $hardwareObjects = @(Get-HardwareObjects $hardwareJson)
        if ($hardwareObjects.Count -eq 0) { $clusterResults += [pscustomobject][ordered]@{ClusterName=$cluster.ClusterName;ClusterId=$cluster.ClusterId;Status="No hardware rows returned";Nodes=0;Error=""}; continue }
        foreach ($hardware in $hardwareObjects) {
            $rows += [pscustomobject][ordered]@{
                ClusterName=$cluster.ClusterName; ClusterId=$cluster.ClusterId; NodeId=First-Property $hardware @("nodeId","id")
                ChassisType=First-Property $hardware @("chassisType"); ChassisModel=First-Property $hardware @("chassisModel"); ChassisSerial=First-Property $hardware @("chassisSerial")
                CohesityChassisSerial=First-Property $hardware @("cohesityChassisSerial"); CohesityNodeSerial=First-Property $hardware @("cohesityNodeSerial"); NodeSerial=First-Property $hardware @("nodeSerial")
                NodeModel=First-Property $hardware @("nodeModel"); ProductModel=First-Property $hardware @("productModel"); ProductModelType=First-Property $hardware @("productModelType")
                SlotNumber=First-Property $hardware @("slotNumber"); MaxSlots=First-Property $hardware @("maxSlots"); IpmiLanChannel=First-Property $hardware @("ipmiLanChannel"); HbaModel=First-Property $hardware @("hbaModel")
            }
        }
        $clusterResults += [pscustomobject][ordered]@{ClusterName=$cluster.ClusterName;ClusterId=$cluster.ClusterId;Status="Success";Nodes=$hardwareObjects.Count;Error=""}
    } catch { $clusterResults += [pscustomobject][ordered]@{ClusterName=$cluster.ClusterName;ClusterId=$cluster.ClusterId;Status="Failed";Nodes=0;Error=$_.Exception.Message} }
}

$rows=@($rows|Sort-Object ClusterName,NodeId,SlotNumber)
Write-Host "`nHARDWARE INVENTORY" -ForegroundColor Cyan
if($rows.Count -gt 0){$rows|Select-Object ClusterName,NodeId,ChassisType,ChassisModel,ChassisSerial,CohesityNodeSerial,NodeSerial,NodeModel,ProductModel,ProductModelType,SlotNumber,MaxSlots,HbaModel|Format-Table -AutoSize -Wrap|Out-Host}else{Write-Host "No hardware rows were returned." -ForegroundColor Yellow}
$timestamp=Get-Date -Format "yyyyMMdd_HHmm";$csvPath=Join-Path $logDirectory "Cohesity_Helios_Hardware_Inventory_$timestamp.csv";$statusCsvPath=Join-Path $logDirectory "Cohesity_Helios_Hardware_Inventory_Status_$timestamp.csv"
if($rows.Count -gt 0){$rows|Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8};$clusterResults|Export-Csv -Path $statusCsvPath -NoTypeInformation -Encoding UTF8
$successfulClusters=@($clusterResults|Where-Object{$_.Status -eq "Success"}).Count;$failedClusters=@($clusterResults|Where-Object{$_.Status -eq "Failed"}).Count;$emptyClusters=@($clusterResults|Where-Object{$_.Status -eq "No hardware rows returned"}).Count;$uniqueChassis=@($rows|Where-Object{$_.ChassisSerial -ne "N/A"}|Select-Object -ExpandProperty ChassisSerial -Unique).Count
Write-Host "`n==============================" -ForegroundColor Cyan;Write-Host "HARDWARE INVENTORY SUMMARY" -ForegroundColor White;Write-Host "==============================" -ForegroundColor Cyan
Write-Host "Clusters discovered : $($clusters.Count)";Write-Host "Clusters successful : $successfulClusters";Write-Host "Clusters empty      : $emptyClusters";Write-Host "Clusters failed     : $failedClusters";Write-Host "Hardware rows       : $($rows.Count)";Write-Host "Unique chassis      : $uniqueChassis";if($rows.Count -gt 0){Write-Host "Inventory CSV       : $csvPath"};Write-Host "Status CSV          : $statusCsvPath"
if($failedClusters -gt 0){Write-Host "`nFAILED CLUSTERS" -ForegroundColor Yellow;$clusterResults|Where-Object{$_.Status -eq "Failed"}|Select-Object ClusterName,ClusterId,Error|Format-Table -AutoSize -Wrap|Out-Host}
Write-Host "Processing complete." -ForegroundColor Green
