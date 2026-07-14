# Cohesity Active Directory Configuration Inventory
# Multi-cluster | Helios | GET-only | PowerShell 5.1 compatible

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\ADInventory"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}
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
    $h = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $h["accessClusterId"] = $ClusterId
    }
    return $h
}

function Get-Json {
    param([string]$Uri, [hashtable]$Headers)
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Value-OrNA {
    param($Value)
    $items = @(
        As-Array $Value | ForEach-Object {
            if ($null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)) {
                ([string]$_).Trim()
            }
        }
    )
    if ($items.Count -eq 0) { return "N/A" }
    return (($items | Select-Object -Unique) -join "; ")
}

function First-Property {
    param($Object, [string[]]$Names)
    if ($null -eq $Object -or $Object -is [string]) { return "N/A" }
    foreach ($name in $Names) {
        foreach ($p in @($Object.PSObject.Properties)) {
            if ($p.Name -ieq $name) {
                $v = Value-OrNA $p.Value
                if ($v -ne "N/A") { return $v }
            }
        }
    }
    return "N/A"
}

function Format-NameStatus {
    param($Items)
    $out = @()
    foreach ($item in @(As-Array $Items)) {
        if ($null -eq $item) { continue }
        $name = First-Property $item @("name", "dnsHostName", "hostName")
        $status = First-Property $item @("status", "state")
        if ($name -ne "N/A" -and $status -ne "N/A") { $out += "$name [$status]" }
        elseif ($name -ne "N/A") { $out += $name }
        elseif ($status -ne "N/A") { $out += "Status=$status" }
    }
    if ($out.Count -eq 0) { return "N/A" }
    return (($out | Select-Object -Unique) -join "; ")
}

function Format-MachineAccounts {
    param($Accounts)
    $out = @()
    foreach ($a in @(As-Array $Accounts)) {
        if ($null -eq $a) { continue }
        $name = First-Property $a @("name")
        $dns = First-Property $a @("dnsHostName")
        $parts = @()
        if ($name -ne "N/A") { $parts += $name }
        if ($dns -ne "N/A" -and $dns -ne $name) { $parts += "DNS=$dns" }
        if ($parts.Count -gt 0) { $out += ($parts -join "; ") }
    }
    if ($out.Count -eq 0) { return "N/A" }
    return (($out | Select-Object -Unique) -join " | ")
}

function Format-TrustedDomains {
    param($Params)
    $out = @()
    if ($null -ne $Params) {
        foreach ($t in @(As-Array $Params.trustedDomains)) {
            $v = First-Property $t @("domainName")
            if ($v -ne "N/A") { $out += $v }
        }
        foreach ($w in @(As-Array $Params.whitelistedDomains)) {
            $v = Value-OrNA $w
            if ($v -ne "N/A") { $out += $v }
        }
    }
    if ($out.Count -eq 0) { return "N/A" }
    return (($out | Select-Object -Unique) -join "; ")
}

function New-EmptyRow {
    param([string]$Cluster, [string]$ADConfigured)
    [pscustomobject][ordered]@{
        Cluster                    = $Cluster
        ADConfigured               = $ADConfigured
        DomainName                 = "N/A"
        OrganizationalUnit         = "N/A"
        WorkGroupName              = "N/A"
        MachineAccounts            = "N/A"
        PreferredDomainControllers = "N/A"
        DomainControllersDenyList  = "N/A"
        TrustedDomains             = "N/A"
        ADConfigurationId          = "N/A"
    }
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   COHESITY AD CONFIGURATION INVENTORY" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan

try {
    $clusterJson = Get-Json "$baseUrl/v2/mcm/cluster-mgmt/info" (New-Headers)
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

$clusters = @()
if ($clusterJson.cohesityClusters) { $clusters = @($clusterJson.cohesityClusters) }
elseif ($clusterJson.clusters) { $clusters = @($clusterJson.clusters) }
elseif ($clusterJson.clusterInfos) { $clusters = @($clusterJson.clusterInfos) }
elseif ($clusterJson.mcmInfo.clusterInfos) { $clusters = @($clusterJson.mcmInfo.clusterInfos) }
if ($clusters.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = @($clusters | Sort-Object { First-Property $_ @("clusterName", "displayName", "name") })
$rows = @()
$issues = @()
$withAD = 0
$withoutAD = 0

foreach ($cluster in $clusters) {
    $clusterName = First-Property $cluster @("clusterName", "displayName", "name")
    $clusterId = First-Property $cluster @("clusterId", "id")
    if ($clusterName -eq "N/A") { $clusterName = "Unknown" }

    if ($clusterId -eq "N/A") {
        $issues += [pscustomobject]@{ Cluster = $clusterName; Issue = "Cluster ID missing" }
        $rows += New-EmptyRow $clusterName "Unknown"
        continue
    }

    Write-Host "Processing cluster: $clusterName" -ForegroundColor Yellow

    try {
        $adJson = Get-Json "$baseUrl/v2/active-directories?includeTenants=true" (New-Headers ([string]$clusterId))
    }
    catch {
        $issues += [pscustomobject]@{ Cluster = $clusterName; Issue = $_.Exception.Message }
        $rows += New-EmptyRow $clusterName "Unknown"
        continue
    }

    $ads = @()
    if ($adJson.activeDirectories) { $ads = @($adJson.activeDirectories | Where-Object { $_ }) }
    elseif ($adJson -is [array]) { $ads = @($adJson | Where-Object { $_ }) }
    elseif ($adJson.domainName -or $adJson.connectionId -or $adJson.id) { $ads = @($adJson) }

    if ($ads.Count -eq 0) {
        $rows += New-EmptyRow $clusterName "No"
        $withoutAD++
        continue
    }

    $withAD++
    foreach ($ad in $ads) {
        $rows += [pscustomobject][ordered]@{
            Cluster                    = $clusterName
            ADConfigured               = "Yes"
            DomainName                 = First-Property $ad @("domainName")
            OrganizationalUnit         = First-Property $ad @("organizationalUnitName")
            WorkGroupName              = First-Property $ad @("workGroupName")
            MachineAccounts            = Format-MachineAccounts $ad.machineAccounts
            PreferredDomainControllers = Format-NameStatus $ad.preferredDomainControllers
            DomainControllersDenyList  = Value-OrNA $ad.domainControllersDenyList
            TrustedDomains             = Format-TrustedDomains $ad.trustedDomainParams
            ADConfigurationId          = First-Property $ad @("id")
        }
    }
}

$rows = @($rows | Sort-Object Cluster, DomainName)

Write-Host "`nAD CONFIGURATION - IDENTITY" -ForegroundColor Cyan
$rows | Select-Object Cluster, ADConfigured, DomainName, OrganizationalUnit, WorkGroupName, ADConfigurationId |
    Format-Table -AutoSize -Wrap | Out-Host

Write-Host "`nAD CONFIGURATION - CONTROLLERS AND TRUST" -ForegroundColor Cyan
$rows | Select-Object Cluster, MachineAccounts, PreferredDomainControllers, DomainControllersDenyList, TrustedDomains |
    Format-Table -AutoSize -Wrap | Out-Host

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_AD_Configuration_$timestamp.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host "`n==============================" -ForegroundColor Cyan
Write-Host "AD CONFIGURATION SUMMARY" -ForegroundColor White
Write-Host "==============================" -ForegroundColor Cyan
Write-Host "Clusters discovered       : $($clusters.Count)"
Write-Host "Clusters with AD          : $withAD"
Write-Host "Clusters without AD       : $withoutAD"
Write-Host "Rows displayed/exported   : $($rows.Count)"
Write-Host "CSV output                : $csvPath"
if ($issues.Count -gt 0) {
    Write-Host "Cluster fetch issues      : $($issues.Count)" -ForegroundColor Yellow
    $issues | Format-Table -AutoSize -Wrap | Out-Host
}
Write-Host "Processing complete." -ForegroundColor Green
