# Cohesity Interface Diagnosis - SWITCH + INTERFACE VALIDATION
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Reuses the old known-good interface collector shape:
#   Invoke-WebRequest -Method Get -Body $body
#   node -> interfaces -> bondSlavesDetails -> uplinkSwitchInfo

param(
    [string]$TargetsFile = 'X:\PowerShell\Cohesity_API_Scripts\Interface_Diagnosis_Targets.txt'
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$AesHelper = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

if (-not (Test-Path -LiteralPath $AesHelper)) { throw "AES helper not found: $AesHelper" }
if (-not (Test-Path -LiteralPath $EncryptedKeyFile)) { throw "Encrypted API key not found: $EncryptedKeyFile" }

. $AesHelper
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedKeyFile
if ([string]::IsNullOrWhiteSpace([string]$ApiKey)) { throw 'Empty Cohesity API key returned by AES helper.' }

function Normalize-Text {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return '' }
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Normalize-SwitchName {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return '' }
    return ([string]$Value).Trim().TrimEnd('.').ToLowerInvariant()
}

function Get-ShortSwitchName {
    param([AllowNull()][object]$Value)
    $Name = Normalize-SwitchName $Value
    if (-not $Name) { return '' }
    return ($Name -split '\.', 2)[0]
}

function Test-SwitchMatch {
    param([AllowNull()][object]$Requested,[AllowNull()][object]$Actual)
    $RequestedFull = Normalize-SwitchName $Requested
    $ActualFull = Normalize-SwitchName $Actual
    if (-not $RequestedFull -or -not $ActualFull) { return $false }
    if ($RequestedFull -eq $ActualFull) { return $true }
    return (Get-ShortSwitchName $RequestedFull) -eq (Get-ShortSwitchName $ActualFull)
}

function Get-PortName {
    param([AllowNull()][object]$PortValue)
    if ($null -eq $PortValue) { return '' }

    $IfNameProperty = $PortValue.PSObject.Properties['ifname']
    if ($null -ne $IfNameProperty -and $null -ne $IfNameProperty.Value) {
        return ([string]$IfNameProperty.Value).Trim()
    }

    $Text = ([string]$PortValue).Trim()
    if ($Text -match '^ifname\s+(.+)$') { return $Matches[1].Trim() }
    return $Text
}

function Test-PortMatch {
    param([AllowNull()][object]$Requested,[AllowNull()][object]$Actual)
    $RequestedPort = Normalize-Text $Requested
    $ActualPort = Normalize-Text (Get-PortName $Actual)
    return ($RequestedPort -and $ActualPort -and $RequestedPort -eq $ActualPort)
}

function Get-StatValue {
    param([AllowNull()][object]$Stats,[Parameter(Mandatory)][string[]]$Names)
    if ($null -eq $Stats) { return 'NOT RETURNED' }

    foreach ($Name in $Names) {
        $Property = $Stats.PSObject.Properties[$Name]
        if ($null -ne $Property -and $null -ne $Property.Value -and "$($Property.Value)" -ne '') {
            return "$($Property.Value)"
        }
    }

    return 'NOT RETURNED'
}

if (-not (Test-Path -LiteralPath $TargetsFile)) { throw "Targets file not found: $TargetsFile" }

$TargetLines = @(Get-Content -LiteralPath $TargetsFile |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') })

if ($TargetLines.Count -eq 0) { throw "No valid targets found in $TargetsFile" }

$Targets = @()
foreach ($TargetLine in $TargetLines) {
    $Parts = @($TargetLine -split '\s+', 2)
    if ($Parts.Count -lt 2) {
        Write-Host "Skipping invalid target line: $TargetLine" -ForegroundColor Yellow
        continue
    }

    $Targets += [pscustomobject]@{
        Switch    = $Parts[0].Trim()
        Interface = $Parts[1].Trim()
    }
}

if ($Targets.Count -eq 0) { throw 'No valid switch + interface target pairs were found.' }

Write-Host "`nCOHESITY INTERFACE DIAGNOSIS" -ForegroundColor Cyan
Write-Host "Targets loaded : $($Targets.Count)"

$clusterUrl = "$BaseUrl/v2/mcm/cluster-mgmt/info"
$commonHeaders = @{ apiKey = $ApiKey }
$clusterResponse = Invoke-RestMethod -Method Get -Uri $clusterUrl -Headers $commonHeaders
$clusters = @($clusterResponse.cohesityClusters)
if ($clusters.Count -eq 0) { throw 'No clusters were returned by Helios.' }

$url = "$BaseUrl/irisservices/api/v1/public/interface"
$body = @{
    bondInterfaceOnly       = 'true'
    ifaceGroupAssignedOnly  = 'true'
    includeUplinkSwitchInfo = 'true'
    includeBondSlaveDetails = 'true'
    includeStats            = 'true'
}

$AllRows = @()
$Failures = @()

foreach ($cluster in ($clusters | Sort-Object clusterName)) {
    $clusterName = [string]$cluster.clusterName
    $clusterId = $cluster.clusterId
    $headers = @{ apiKey = $ApiKey; accessClusterId = $clusterId }

    try {
        $responseIF = Invoke-WebRequest -Method Get -Uri $url -Headers $headers -Body $body
        $jsonIF = $responseIF.Content | ConvertFrom-Json
    }
    catch {
        $Failures += [pscustomobject]@{ Cluster = $clusterName; Error = $_.Exception.Message }
        continue
    }

    if (-not $jsonIF) { continue }

    foreach ($node in $jsonIF) {
        foreach ($iface in @($node.interfaces)) {
            if ($null -eq $iface) { continue }

            $mtu = if ($null -ne $iface.mtu -and "$($iface.mtu)" -ne '') { "$($iface.mtu)" } else { 'NOT RETURNED' }
            $bondingMode = if ($null -ne $iface.bondingMode -and "$($iface.bondingMode)" -ne '') { "$($iface.bondingMode)" } else { 'NOT RETURNED' }

            $rxErrors  = Get-StatValue -Stats $iface.stats -Names @('rxErr','rxErrs','rxErrors')
            $rxDropped = Get-StatValue -Stats $iface.stats -Names @('rxDropped','rxDrop','rxDrops')
            $txErrors  = Get-StatValue -Stats $iface.stats -Names @('txErr','txErrs','txErrors')
            $txDropped = Get-StatValue -Stats $iface.stats -Names @('txDropped','txDrop','txDrops')

            foreach ($bsd in @($iface.bondSlavesDetails)) {
                if ($null -eq $bsd) { continue }

                $linkState = if ($null -ne $bsd.linkState -and "$($bsd.linkState)" -ne '') { "$($bsd.linkState)" } else { 'NOT RETURNED' }
                $mac = if ($null -ne $bsd.macAddr -and "$($bsd.macAddr)" -ne '') { "$($bsd.macAddr)" } else { 'NOT RETURNED' }
                $speed = if ($null -ne $bsd.speed -and "$($bsd.speed)" -ne '') { "$($bsd.speed)" } else { 'NOT RETURNED' }
                $slaveName = if ($null -ne $bsd.name -and "$($bsd.name)" -ne '') { "$($bsd.name)" } else { 'NOT RETURNED' }
                $slot = if ($null -ne $bsd.slot -and "$($bsd.slot)" -ne '') { "$($bsd.slot)" } else { 'NOT RETURNED' }

                foreach ($usi in @($bsd.uplinkSwitchInfo)) {
                    if ($null -eq $usi) { continue }
                    $switchInfo = if ($null -ne $usi.sysName) { "$($usi.sysName)" } else { '' }

                    foreach ($rawPort in @($usi.portId)) {
                        $portName = Get-PortName $rawPort
                        if (-not $switchInfo -and -not $portName) { continue }

                        $AllRows += [pscustomobject]@{
                            Cluster              = $clusterName
                            NodeID               = "$($node.nodeId)"
                            NodeIP               = $node.nodeIp
                            ChassisSerial        = $node.chassisSerial
                            BondingMode          = $bondingMode
                            MTU                  = $mtu
                            BondSlave            = $slaveName
                            SlaveInterfaceStatus = $linkState
                            MAC                  = $mac
                            SlaveSpeed           = $speed
                            SlotType             = $slot
                            SwitchInfo           = $switchInfo
                            PortId               = $portName
                            RxErrors             = $rxErrors
                            RxDropped            = $rxDropped
                            TxErrors             = $txErrors
                            TxDropped            = $txDropped
                        }
                    }
                }
            }
        }
    }
}

if (-not $AllRows -or $AllRows.Count -eq 0) {
    Write-Host 'No rows collected from interface API.' -ForegroundColor Yellow
    if ($Failures.Count -gt 0) { $Failures | Format-Table Cluster, Error -AutoSize }
    return
}

$Matches = @()
$NotFound = @()

foreach ($Target in $Targets) {
    $TargetMatches = @($AllRows | Where-Object {
        (Test-SwitchMatch -Requested $Target.Switch -Actual $_.SwitchInfo) -and
        (Test-PortMatch -Requested $Target.Interface -Actual $_.PortId)
    })

    if ($TargetMatches.Count -gt 0) {
        $Matches += $TargetMatches
    }
    else {
        $NotFound += [pscustomobject]@{
            Switch   = $Target.Switch
            Ethernet = $Target.Interface
        }
    }
}

Write-Host "`nRESULT" -ForegroundColor Cyan
Write-Host '======' -ForegroundColor Cyan

if ($Matches.Count -gt 0) {
    $DisplayRows = $Matches | Select-Object `
        @{n='Switch';e={$_.SwitchInfo}},
        @{n='Ethernet';e={$_.PortId}},
        Cluster,
        NodeID,
        NodeIP,
        ChassisSerial,
        BondSlave,
        BondingMode,
        SlaveInterfaceStatus,
        MAC,
        SlaveSpeed,
        MTU,
        SlotType,
        RxErrors,
        RxDropped,
        TxErrors,
        TxDropped

    $DisplayRows |
        Format-Table -Property * -AutoSize |
        Out-String -Width 1200 |
        Write-Host

    Write-Host "Matched rows: $($Matches.Count)" -ForegroundColor Green
}
else {
    Write-Host 'No requested switch + Ethernet pairs were matched.' -ForegroundColor Yellow
}

if ($NotFound.Count -gt 0) {
    Write-Host "`nTargets not found:" -ForegroundColor Yellow
    $NotFound | Format-Table Switch, Ethernet -AutoSize
}

if ($Failures.Count -gt 0) {
    Write-Host "`nClusters that could not be queried:" -ForegroundColor Yellow
    $Failures | Format-Table Cluster, Error -AutoSize
}
