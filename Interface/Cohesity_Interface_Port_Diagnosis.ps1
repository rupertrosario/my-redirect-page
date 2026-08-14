# Cohesity Interface Port Diagnosis - SINGLE TARGET VALIDATION
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Purpose of this version:
#   Validate ONE switch/interface from the first non-comment TXT line.
#   Match directly against bondSlavesDetails.uplinkSwitchInfo.sysName.
#   Once the switch matches, report the enclosing node/interface/bond-slave data.
#   PortId is secondary validation only.
#
# BASE: proven /public/interface collector used by the existing interface scripts.

param(
    [string]$TargetsFile = 'X:\PowerShell\Cohesity_API_Scripts\Interface_Diagnosis_Targets.txt',
    [string]$HistoryDir  = 'X:\PowerShell\Data\Cohesity\InterfaceDiagnosis'
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$AesHelper = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

# -----------------------------------------------------------------------------
# Existing encrypted API-key flow
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
# Helpers
# -----------------------------------------------------------------------------
function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    $Method = 'GET'
    if ($Method -cne 'GET') {
        throw 'SAFETY BLOCK: HTTP method is not GET.'
    }
    if (-not $Uri.StartsWith($BaseUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "SAFETY BLOCK: URI outside Helios: $Uri"
    }

    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -ErrorAction Stop
}

function Normalize-Text {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return '' }
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Get-ShortHostName {
    param([AllowNull()][object]$Value)
    $Text = (Normalize-Text $Value).TrimEnd('.')
    if (-not $Text) { return '' }
    return ($Text -split '\.', 2)[0]
}

function Test-SwitchMatch {
    param(
        [AllowNull()][object]$Requested,
        [AllowNull()][object]$Actual
    )

    $RequestedFull = (Normalize-Text $Requested).TrimEnd('.')
    $ActualFull    = (Normalize-Text $Actual).TrimEnd('.')

    if (-not $RequestedFull -or -not $ActualFull) { return $false }
    if ($RequestedFull -eq $ActualFull) { return $true }

    return (Get-ShortHostName $RequestedFull) -eq (Get-ShortHostName $ActualFull)
}

function Get-FirstPropertyValue {
    param(
        [AllowNull()][object]$InputObject,
        [Parameter(Mandatory)][string[]]$PropertyNames,
        [AllowNull()][object]$DefaultValue = 0
    )

    if ($null -eq $InputObject) { return $DefaultValue }

    foreach ($PropertyName in $PropertyNames) {
        $Property = $InputObject.PSObject.Properties[$PropertyName]
        if ($null -ne $Property -and $null -ne $Property.Value) {
            return $Property.Value
        }
    }

    return $DefaultValue
}

function Get-PortNames {
    param([AllowNull()][object]$PortId)

    $Names = @()

    foreach ($Port in @($PortId)) {
        if ($null -eq $Port) { continue }

        if ($Port -is [string]) {
            $Text = $Port.Trim()
            if ($Text -match '^(?i)ifname\s+(.+)$') {
                $Names += $Matches[1].Trim()
            }
            elseif ($Text) {
                $Names += $Text
            }
            continue
        }

        if ($null -ne $Port.PSObject.Properties['ifname']) {
            foreach ($IfName in @($Port.ifname)) {
                if (-not [string]::IsNullOrWhiteSpace([string]$IfName)) {
                    $Names += ([string]$IfName).Trim()
                }
            }
            continue
        }

        if ($null -ne $Port.PSObject.Properties['ifName']) {
            foreach ($IfName in @($Port.ifName)) {
                if (-not [string]::IsNullOrWhiteSpace([string]$IfName)) {
                    $Names += ([string]$IfName).Trim()
                }
            }
        }
    }

    return @($Names | Where-Object { $_ } | Select-Object -Unique)
}

function To-CleanList {
    param([AllowNull()][object]$Value)

    if ($null -eq $Value) { return '' }
    $Items = @($Value | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
    if ($Items.Count -eq 0) { return '' }
    return ($Items -join ', ')
}

# -----------------------------------------------------------------------------
# 1) Read ONLY the first valid target line
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
if ($Parts.Count -ne 2) {
    throw "Target must be: <switch> <interface>. Found: $TargetLine"
}

$RequestedSwitch = $Parts[0].Trim()
$RequestedPort   = $Parts[1].Trim()

Write-Host "`nSINGLE TARGET VALIDATION" -ForegroundColor Cyan
Write-Host "Switch    : $RequestedSwitch"
Write-Host "Interface : $RequestedPort"

# -----------------------------------------------------------------------------
# 2) Cluster discovery - GET only
# -----------------------------------------------------------------------------
$ClusterResponse = Invoke-CohesityGet `
    -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers @{ apiKey = $ApiKey }

$Clusters = @($ClusterResponse.cohesityClusters)
if ($Clusters.Count -eq 0) {
    throw 'No clusters returned by Helios.'
}

# -----------------------------------------------------------------------------
# 3) Same proven /public/interface GET
# -----------------------------------------------------------------------------
$InterfaceQuery = @(
    'bondInterfaceOnly=true'
    'ifaceGroupAssignedOnly=true'
    'includeUplinkSwitchInfo=true'
    'includeBondSlaveDetails=true'
    'includeStats=true'
) -join '&'

$InterfaceUrl = "$BaseUrl/irisservices/api/v1/public/interface?$InterfaceQuery"

$Matches = [System.Collections.Generic.List[object]]::new()
$SwitchSeen = $false
$Failures = [System.Collections.Generic.List[object]]::new()

foreach ($Cluster in ($Clusters | Sort-Object clusterName)) {
    $ClusterName = [string]$Cluster.clusterName
    $ClusterId   = $Cluster.clusterId

    $Headers = @{
        apiKey          = $ApiKey
        accessClusterId = $ClusterId
    }

    try {
        $Nodes = @(Invoke-CohesityGet -Uri $InterfaceUrl -Headers $Headers)
    }
    catch {
        $Failures.Add([pscustomobject]@{
            Cluster = $ClusterName
            Error   = $_.Exception.Message
        })
        continue
    }

    foreach ($Node in $Nodes) {
        foreach ($Iface in @($Node.interfaces)) {
            if ($null -eq $Iface) { continue }

            # Direct fields exactly like the existing collector.
            $Stats = $Iface.stats
            $BondName = $Iface.name
            $MTU = $Iface.mtu
            $BondingMode = $Iface.bondingMode
            $ActiveBondSlave = $Iface.activeBondSlave

            $RxErrors  = Get-FirstPropertyValue $Stats @('rxErr','rxErrs','rxErrors') 0
            $RxDropped = Get-FirstPropertyValue $Stats @('rxDropped','rxDrop','rxDrops') 0
            $TxErrors  = Get-FirstPropertyValue $Stats @('txErr','txErrs','txErrors') 0
            $TxDropped = Get-FirstPropertyValue $Stats @('txDropped','txDrop','txDrops') 0

            foreach ($Detail in @($Iface.bondSlavesDetails)) {
                if ($null -eq $Detail) { continue }

                # Direct bond-slave fields exactly like the existing collector.
                $BondSlave  = $Detail.name
                $LinkState  = $Detail.linkState
                $SlaveSpeed = $Detail.speed
                $MacAddress = $Detail.macAddr
                $SlotType   = Get-FirstPropertyValue $Detail @('slotType','slot') ''

                foreach ($Uplink in @($Detail.uplinkSwitchInfo)) {
                    if ($null -eq $Uplink) { continue }

                    foreach ($ActualSwitch in @($Uplink.sysName)) {
                        if ([string]::IsNullOrWhiteSpace([string]$ActualSwitch)) { continue }
                        if (-not (Test-SwitchMatch -Requested $RequestedSwitch -Actual $ActualSwitch)) { continue }

                        $SwitchSeen = $true
                        $PortNames = @(Get-PortNames -PortId $Uplink.portId)

                        # One row per switch match. The enclosing interface details
                        # come directly from the exact node/iface/detail that owns it.
                        $PortMatch = @($PortNames | Where-Object {
                            (Normalize-Text $_) -eq (Normalize-Text $RequestedPort)
                        }).Count -gt 0

                        $Matches.Add([pscustomobject]@{
                            RequestedSwitch    = $RequestedSwitch
                            RequestedInterface = $RequestedPort
                            SwitchFQDN         = [string]$ActualSwitch
                            ReturnedPorts      = To-CleanList $PortNames
                            PortMatched        = $PortMatch
                            Cluster            = $ClusterName
                            NodeID             = [string]$Node.nodeId
                            NodeIP             = [string]$Node.nodeIp
                            ChassisSerial      = [string]$Node.chassisSerial
                            Bond               = [string]$BondName
                            BondingMode        = [string]$BondingMode
                            ActiveSlave        = [string]$ActiveBondSlave
                            BondSlave          = [string]$BondSlave
                            LinkState          = [string]$LinkState
                            SlaveSpeed         = [string]$SlaveSpeed
                            MTU                = [string]$MTU
                            MacAddress         = [string]$MacAddress
                            SlotType           = [string]$SlotType
                            RxErrors           = [string]$RxErrors
                            RxDropped          = [string]$RxDropped
                            TxErrors           = [string]$TxErrors
                            TxDropped          = [string]$TxDropped
                            Status             = if ($PortMatch) { 'SWITCH + PORT MATCH' } else { 'SWITCH MATCH; PORT DIFFERENT' }
                        })
                    }
                }
            }
        }
    }
}

# -----------------------------------------------------------------------------
# 4) Output only the single target result
# -----------------------------------------------------------------------------
Write-Host "`nRESULT" -ForegroundColor Cyan
Write-Host '======' -ForegroundColor Cyan

if (-not $SwitchSeen) {
    Write-Host "Switch '$RequestedSwitch' was NOT found in uplinkSwitchInfo.sysName." -ForegroundColor Yellow
}
else {
    $Exact = @($Matches | Where-Object { $_.PortMatched })
    $Display = if ($Exact.Count -gt 0) { $Exact } else { @($Matches) }

    $Display |
        Format-Table RequestedSwitch, RequestedInterface, SwitchFQDN, ReturnedPorts,
            Cluster, NodeID, NodeIP, Bond, BondSlave, LinkState, SlaveSpeed, MTU,
            MacAddress, RxErrors, RxDropped, TxErrors, TxDropped, Status -AutoSize

    if ($Exact.Count -eq 0) {
        Write-Host "`nSwitch matched, but requested interface '$RequestedPort' was not one of the returned ports above." -ForegroundColor Yellow
    }
}

# Save only this single-target validation result.
if ($Matches.Count -gt 0) {
    if (-not (Test-Path -LiteralPath $HistoryDir)) {
        New-Item -Path $HistoryDir -ItemType Directory -Force | Out-Null
    }

    $Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    $CsvPath = Join-Path $HistoryDir "Interface_Single_Target_$Timestamp.csv"
    $Matches | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
    Write-Host "`nSaved snapshot: $CsvPath" -ForegroundColor DarkGray
}

if ($Failures.Count -gt 0) {
    Write-Host "`nClusters that could not be queried:" -ForegroundColor Yellow
    $Failures | Format-Table Cluster, Error -AutoSize
}
