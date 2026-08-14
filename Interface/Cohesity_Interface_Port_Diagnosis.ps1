# Cohesity Interface Diagnosis - SINGLE SWITCH VALIDATION
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# IMPORTANT:
# This version intentionally uses the old known-good interface collector shape:
#   Invoke-WebRequest -Method Get -Body $body
#   node -> interfaces -> bondSlavesDetails -> uplinkSwitchInfo
# It reads only the FIRST non-comment target line and matches ONLY SwitchInfo.
# No port matching or synthetic per-port reconstruction is performed here.

param(
    [string]$TargetsFile = 'X:\PowerShell\Cohesity_API_Scripts\Interface_Diagnosis_Targets.txt'
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$AesHelper = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

# -----------------------------------------------------------------------------
# AES API key - existing proven credential flow
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $AesHelper)) {
    throw "AES helper not found: $AesHelper"
}
if (-not (Test-Path -LiteralPath $EncryptedKeyFile)) {
    throw "Encrypted API key not found: $EncryptedKeyFile"
}

. $AesHelper
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedKeyFile
if ([string]::IsNullOrWhiteSpace([string]$ApiKey)) {
    throw 'Empty Cohesity API key returned by AES helper.'
}

# -----------------------------------------------------------------------------
# Helpers copied from the old working interface collector approach
# -----------------------------------------------------------------------------
function To-CsvList {
    param([object]$v)

    if ($null -eq $v) { return '' }

    $arr = @($v | ForEach-Object { "$_" } | Where-Object { $_ -ne '' })

    if ($arr.Count -eq 0) { return '' }
    if ($arr.Count -eq 1) { return $arr[0] }

    return ($arr -join ' ; ')
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
    param(
        [AllowNull()][object]$Requested,
        [AllowNull()][object]$Actual
    )

    $RequestedFull = Normalize-SwitchName $Requested
    $ActualFull    = Normalize-SwitchName $Actual

    if (-not $RequestedFull -or -not $ActualFull) { return $false }
    if ($RequestedFull -eq $ActualFull) { return $true }

    return (Get-ShortSwitchName $RequestedFull) -eq (Get-ShortSwitchName $ActualFull)
}

# -----------------------------------------------------------------------------
# 1) Read only the first valid target line.
#    For this validation build we use ONLY the switch value.
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $TargetsFile)) {
    throw "Targets file not found: $TargetsFile"
}

$TargetLine = Get-Content -LiteralPath $TargetsFile |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') } |
    Select-Object -First 1

if (-not $TargetLine) {
    throw "No valid target found in $TargetsFile"
}

$Parts = @($TargetLine -split '\s+', 2)
$RequestedSwitch = $Parts[0].Trim()
$RequestedInterface = if ($Parts.Count -gt 1) { $Parts[1].Trim() } else { '' }

Write-Host "`nSINGLE SWITCH VALIDATION" -ForegroundColor Cyan
Write-Host "Switch from TXT    : $RequestedSwitch"
if ($RequestedInterface) {
    Write-Host "Interface from TXT : $RequestedInterface (not used for filtering in this validation)"
}

# -----------------------------------------------------------------------------
# 2) Cluster discovery - GET only
# -----------------------------------------------------------------------------
$clusterUrl = "$BaseUrl/v2/mcm/cluster-mgmt/info"
$commonHeaders = @{ apiKey = $ApiKey }

$clusterResponse = Invoke-RestMethod -Method Get -Uri $clusterUrl -Headers $commonHeaders
$clusters = @($clusterResponse.cohesityClusters)

if ($clusters.Count -eq 0) {
    throw 'No clusters were returned by Helios.'
}

# -----------------------------------------------------------------------------
# 3) EXACT old working /public/interface invocation pattern
# -----------------------------------------------------------------------------
$url = "$BaseUrl/irisservices/api/v1/public/interface"
$body = @{
    bondInterfaceOnly       = 'true'
    ifaceGroupAssignedOnly  = 'true'
    includeUplinkSwitchInfo = 'true'
    includeBondSlaveDetails = 'true'
}

$AllRows = @()
$Failures = @()

foreach ($cluster in ($clusters | Sort-Object clusterName)) {
    $clusterName = [string]$cluster.clusterName
    $clusterId   = $cluster.clusterId

    $headers = @{
        apiKey          = $ApiKey
        accessClusterId = $clusterId
    }

    try {
        # STRICTLY GET. This is the same GET + Body pattern as the old known-good script.
        $responseIF = Invoke-WebRequest -Method Get -Uri $url -Headers $headers -Body $body
        $jsonIF = $responseIF.Content | ConvertFrom-Json
    }
    catch {
        $Failures += [pscustomobject]@{
            Cluster = $clusterName
            Error   = $_.Exception.Message
        }
        continue
    }

    if (-not $jsonIF) { continue }

    foreach ($node in $jsonIF) {

        $bondNames   = @()
        $mtus        = @()
        $bondSlaves  = @()
        $linkStates  = @()
        $macs        = @()
        $speeds      = @()
        $slotTypes   = @()
        $switchInfos = @()
        $portIds     = @()

        foreach ($iface in @($node.interfaces)) {
            if ($null -eq $iface) { continue }

            if ($null -ne $iface.name -and "$($iface.name)" -ne '') {
                $bondNames += "$($iface.name)"
            }

            if ($null -ne $iface.mtu -and "$($iface.mtu)" -ne '') {
                $mtus += "$($iface.mtu)"
            }

            foreach ($slave in @($iface.bondSlaves)) {
                if ($null -ne $slave -and "$slave" -ne '') {
                    $bondSlaves += "$slave"
                }
            }

            foreach ($bsd in @($iface.bondSlavesDetails)) {
                if ($null -eq $bsd) { continue }

                if ($null -ne $bsd.linkState -and "$($bsd.linkState)" -ne '') {
                    $linkStates += "$($bsd.linkState)"
                }

                if ($null -ne $bsd.macAddr -and "$($bsd.macAddr)" -ne '') {
                    $macs += "$($bsd.macAddr)"
                }

                if ($null -ne $bsd.speed -and "$($bsd.speed)" -ne '') {
                    $speeds += "$($bsd.speed)"
                }

                if ($bsd.uplinkSwitchInfo) {
                    foreach ($usi in @($bsd.uplinkSwitchInfo)) {
                        if ($null -eq $usi) { continue }

                        if ($null -ne $usi.sysName -and "$($usi.sysName)" -ne '') {
                            $switchInfos += "$($usi.sysName)"
                        }

                        if ($null -ne $usi.portId -and "$($usi.portId)" -ne '') {
                            $portIds += "$($usi.portId)"
                        }
                    }
                }
            }

            foreach ($slot in @($iface.bondSlavesSlotTypes)) {
                if ($null -ne $slot -and "$slot" -ne '') {
                    $slotTypes += "$slot"
                }
            }
        }

        $AllRows += [pscustomobject]@{
            Cluster              = $clusterName
            NodeID               = "$($node.nodeId)"
            NodeIP               = $node.nodeIp
            ChassisSerial        = $node.chassisSerial
            BondName             = $bondNames
            MTU                  = $mtus
            BondSlaves           = $bondSlaves
            SlaveInterfaceStatus = $linkStates
            MAC                  = $macs
            SlaveSpeed           = $speeds
            SlotType             = $slotTypes
            SwitchInfo           = $switchInfos
            PortId               = $portIds
        }
    }
}

if (-not $AllRows -or $AllRows.Count -eq 0) {
    Write-Host 'No rows collected from interface API.' -ForegroundColor Yellow
    if ($Failures.Count -gt 0) {
        $Failures | Format-Table Cluster, Error -AutoSize
    }
    return
}

# -----------------------------------------------------------------------------
# 4) ONLY NEW LOGIC: search the existing SwitchInfo array
# -----------------------------------------------------------------------------
$Matches = @($AllRows | Where-Object {
    $Row = $_
    @($Row.SwitchInfo | Where-Object {
        Test-SwitchMatch -Requested $RequestedSwitch -Actual $_
    }).Count -gt 0
})

Write-Host "`nRESULT" -ForegroundColor Cyan
Write-Host '======' -ForegroundColor Cyan

if ($Matches.Count -eq 0) {
    Write-Host "Switch '$RequestedSwitch' was NOT found in the collected SwitchInfo values." -ForegroundColor Yellow

    # Small diagnostic only: show near evidence without dumping the estate.
    $Available = @($AllRows.SwitchInfo | ForEach-Object { $_ } | Where-Object { $_ } | Sort-Object -Unique)
    Write-Host "Collected SwitchInfo count: $($Available.Count)" -ForegroundColor DarkGray
    return
}

$DisplayRows = $Matches | Select-Object `
    Cluster,
    NodeID,
    NodeIP,
    ChassisSerial,
    @{n='BondName'; e={ To-CsvList $_.BondName }},
    @{n='MTU'; e={ To-CsvList $_.MTU }},
    @{n='BondSlaves'; e={ To-CsvList $_.BondSlaves }},
    @{n='SlaveInterfaceStatus'; e={ To-CsvList $_.SlaveInterfaceStatus }},
    @{n='MAC'; e={ To-CsvList $_.MAC }},
    @{n='SlaveSpeed'; e={ To-CsvList $_.SlaveSpeed }},
    @{n='SlotType'; e={ To-CsvList $_.SlotType }},
    @{n='SwitchInfo'; e={ To-CsvList $_.SwitchInfo }},
    @{n='PortId'; e={ To-CsvList $_.PortId }}

$DisplayRows | Format-List

Write-Host "Matched node rows: $($Matches.Count)" -ForegroundColor Green
Write-Host 'This build intentionally stops here. If this data is correct, port-specific correlation is the next step.' -ForegroundColor DarkGray

if ($Failures.Count -gt 0) {
    Write-Host "`nClusters that could not be queried:" -ForegroundColor Yellow
    $Failures | Format-Table Cluster, Error -AutoSize
}
