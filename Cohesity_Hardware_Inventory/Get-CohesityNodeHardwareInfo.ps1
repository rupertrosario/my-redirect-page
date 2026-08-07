# Cohesity Node Hardware Inventory
# Multi-cluster | Helios proxy | GET-only | PowerShell 5.1 compatible
# Endpoint: GET https://helios.cohesity.com/v2/node/hardware-info

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found at $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found at $encryptedApiKeyPath"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function New-Headers {
    param([string]$ClusterId)

    $headers = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers["accessClusterId"] = $ClusterId
    }
    return $headers
}

function Get-Json {
    param([string]$Uri, [hashtable]$Headers)

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function Get-PropertyValue {
    param(
        $Object,
        [Parameter(Mandatory)][string[]]$Names
    )

    if ($null -eq $Object) { return $null }

    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties |
            Where-Object { $_.Name -ieq $name } |
            Select-Object -First 1

        if ($null -ne $property) {
            return $property.Value
        }
    }

    return $null
}

function To-Text {
    param($Value)

    $items = @(
        @($Value) | ForEach-Object {
            if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                ([string]$_).Trim()
            }
        }
    )

    if (@($items).Count -eq 0) { return "N/A" }
    return (($items | Select-Object -Unique) -join "; ")
}

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "   COHESITY NODE HARDWARE INVENTORY" -ForegroundColor White
Write-Host "================================================" -ForegroundColor Cyan

try {
    $clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

$clusterSource = Get-PropertyValue -Object $clusterJson -Names @("cohesityClusters", "clusters", "clusterInfos")
if ($null -eq $clusterSource) {
    $mcmInfo = Get-PropertyValue -Object $clusterJson -Names @("mcmInfo")
    if ($null -ne $mcmInfo) {
        $clusterSource = Get-PropertyValue -Object $mcmInfo -Names @("clusterInfos")
    }
}

$clusters = @(@($clusterSource) | Where-Object { $null -ne $_ })
if (@($clusters).Count -eq 0) {
    throw "No clusters were returned from Helios."
}

$rows = @()
$issues = @()

foreach ($cluster in $clusters) {
    $clusterName = To-Text (Get-PropertyValue -Object $cluster -Names @("clusterName", "displayName", "name"))
    $clusterId   = To-Text (Get-PropertyValue -Object $cluster -Names @("clusterId", "id"))

    if ($clusterName -eq "N/A") { $clusterName = "Unknown" }

    if ($clusterId -eq "N/A") {
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Cluster ID missing"
        }
        continue
    }

    Write-Host "Querying node hardware for $clusterName..." -ForegroundColor Yellow

    try {
        $hardwareResponse = Get-Json -Uri "$baseUrl/v2/node/hardware-info" -Headers (New-Headers -ClusterId $clusterId)
    }
    catch {
        $message = $_.Exception.Message
        if ($message -match "404") {
            $message = "GET /v2/node/hardware-info is not available on this cluster/API version."
        }

        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = $message
        }
        continue
    }

    if ($null -eq $hardwareResponse) {
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Hardware API returned no data"
        }
        continue
    }

    $hardwareItems = @($hardwareResponse)
    $wrappedHardware = Get-PropertyValue -Object $hardwareResponse -Names @("hardwareInfo", "nodeHardwareInfo")
    if ($null -ne $wrappedHardware) {
        $hardwareItems = @($wrappedHardware)
    }

    foreach ($hardware in @($hardwareItems)) {
        if ($null -eq $hardware) { continue }

        $rows += [pscustomobject][ordered]@{
            Cluster               = $clusterName
            ChassisModel          = To-Text (Get-PropertyValue -Object $hardware -Names @("chassisModel"))
            ChassisSerial         = To-Text (Get-PropertyValue -Object $hardware -Names @("chassisSerial"))
            ChassisType           = To-Text (Get-PropertyValue -Object $hardware -Names @("chassisType"))
            CohesityChassisSerial = To-Text (Get-PropertyValue -Object $hardware -Names @("cohesityChassisSerial"))
            CohesityNodeSerial    = To-Text (Get-PropertyValue -Object $hardware -Names @("cohesityNodeSerial"))
            HbaModel              = To-Text (Get-PropertyValue -Object $hardware -Names @("hbaModel"))
            IpmiLanChannel        = To-Text (Get-PropertyValue -Object $hardware -Names @("ipmiLanChannel"))
            MaxSlots              = To-Text (Get-PropertyValue -Object $hardware -Names @("maxSlots"))
            NodeModel             = To-Text (Get-PropertyValue -Object $hardware -Names @("nodeModel"))
            NodeSerial            = To-Text (Get-PropertyValue -Object $hardware -Names @("nodeSerial"))
            ProductModel          = To-Text (Get-PropertyValue -Object $hardware -Names @("productModel"))
            ProductModelType      = To-Text (Get-PropertyValue -Object $hardware -Names @("productModelType"))
            SlotNumber            = To-Text (Get-PropertyValue -Object $hardware -Names @("slotNumber"))
        }
    }
}

$rows = @($rows | Sort-Object Cluster, ChassisSerial, SlotNumber)

Write-Host ""
if (@($rows).Count -gt 0) {
    $rows |
        Format-Table Cluster, ChassisModel, ChassisSerial, ChassisType, CohesityChassisSerial, CohesityNodeSerial, HbaModel, IpmiLanChannel, MaxSlots, NodeModel, NodeSerial, ProductModel, ProductModelType, SlotNumber -AutoSize -Wrap |
        Out-Host
}
else {
    Write-Host "No node hardware records were returned." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Clusters discovered : $(@($clusters).Count)"
Write-Host "Hardware rows       : $(@($rows).Count)"
Write-Host "Cluster query issues: $(@($issues).Count)"

if (@($issues).Count -gt 0) {
    Write-Host ""
    Write-Warning "One or more clusters could not be queried."
    $issues | Format-Table Cluster, Issue -AutoSize -Wrap | Out-Host
}
