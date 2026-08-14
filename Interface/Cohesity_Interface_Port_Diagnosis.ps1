# Cohesity Interface Port Diagnosis
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# BASE: Cohesity_Interface_Health_Stats.ps1
# This script intentionally keeps that working collector's endpoint, hierarchy,
# and direct field access. It only adds:
#   - encrypted API-key loading
#   - uplinkSwitchInfo.sysName + portId.ifname extraction
#   - filtering from Interface_Diagnosis_Targets.txt
#   - timestamped CSV output

param(
    [string]$TargetsFile = 'X:\PowerShell\Cohesity_Automations\Interface\Interface_Diagnosis_Targets.txt',
    [string]$HistoryDir  = 'X:\PowerShell\Data\Cohesity\InterfaceDiagnosis'
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$AesHelper = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

# ============================================================================
# 0) Existing encrypted API-key flow
# ============================================================================
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

# ============================================================================
# Helpers
# ============================================================================
function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    $Method = 'GET'
    if ($Method -cne 'GET') { throw 'SAFETY BLOCK: method is not GET' }
    if (-not $Uri.StartsWith($BaseUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "SAFETY BLOCK: URI outside Helios: $Uri"
    }

    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers -ErrorAction Stop
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

# portId is not always a plain string. In the observed response it contains
# objects whose actual switch-interface name is in .ifname.
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

# ============================================================================
# 1) Load targets: <switch><space/tab><interface>
# ============================================================================
if (-not (Test-Path -LiteralPath $TargetsFile)) {
    throw "Targets file not found: $TargetsFile"
}

$Targets = @()
foreach ($RawLine in @(Get-Content -LiteralPath $TargetsFile)) {
    $Line = $RawLine.Trim()
    if (-not $Line -or $Line.StartsWith('#')) { continue }

    $Parts = @($Line -split '\s+', 2)
    if ($Parts.Count -ne 2) {
        Write-Warning "Skipping invalid target line: $Line"
        continue
    }

    $Targets += [pscustomobject]@{
        Switch    = $Parts[0].Trim()
        Interface = $Parts[1].Trim()
    }
}

if ($Targets.Count -eq 0) {
    throw "No valid targets found in $TargetsFile"
}

# ============================================================================
# 2) SAME cluster discovery as Cohesity_Interface_Health_Stats.ps1
# ============================================================================
$ClusterUrl = "$BaseUrl/v2/mcm/cluster-mgmt/info"
$ClusterResponse = Invoke-CohesityGet -Uri $ClusterUrl -Headers @{ apiKey = $ApiKey }
$Clusters = @($ClusterResponse.cohesityClusters)

if ($Clusters.Count -eq 0) {
    throw 'No clusters were returned by Helios.'
}

# ============================================================================
# 3) SAME /public/interface request as Cohesity_Interface_Health_Stats.ps1
# ============================================================================
$InterfaceQuery = @(
    'bondInterfaceOnly=true'
    'ifaceGroupAssignedOnly=true'
    'includeUplinkSwitchInfo=true'
    'includeBondSlaveDetails=true'
    'includeStats=true'
) -join '&'

$InterfaceUrl = "$BaseUrl/irisservices/api/v1/public/interface?$InterfaceQuery"

$AllRows = [System.Collections.Generic.List[object]]::new()
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
            Cluster   = $ClusterName
            ClusterId = $ClusterId
            Error     = $_.Exception.Message
        })
        continue
    }

    foreach ($Node in $Nodes) {
        foreach ($Iface in @($Node.interfaces)) {
            if ($null -eq $Iface) { continue }

            # EXACTLY the same statistics scope as the working health script.
            $Stats = $Iface.stats

            $RxPkts    = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('rxPkts','rxPackets')
            $RxBytes   = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('rxBytes')
            $RxErrors  = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('rxErr','rxErrs','rxErrors')
            $RxDropped = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('rxDropped','rxDrop','rxDrops')
            $TxPkts    = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('txPkts','txPackets')
            $TxBytes   = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('txBytes')
            $TxErrors  = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('txErr','txErrs','txErrors')
            $TxDropped = Get-FirstPropertyValue -InputObject $Stats -PropertyNames @('txDropped','txDrop','txDrops')

            $BondName        = [string](Get-FirstPropertyValue -InputObject $Iface -PropertyNames @('name','interfaceName') -DefaultValue '')
            $MTU             = Get-FirstPropertyValue -InputObject $Iface -PropertyNames @('mtu') -DefaultValue ''
            $BondingMode     = Get-FirstPropertyValue -InputObject $Iface -PropertyNames @('bondingMode') -DefaultValue ''
            $ActiveBondSlave = [string](Get-FirstPropertyValue -InputObject $Iface -PropertyNames @('activeBondSlave') -DefaultValue '')

            # EXACT same per-slave loop used by the working health script.
            foreach ($Detail in @($Iface.bondSlavesDetails)) {
                if ($null -eq $Detail) { continue }

                $BondSlave  = $Detail.name
                $LinkState  = $Detail.linkState
                $SlaveSpeed = $Detail.speed
                $SlotType   = Get-FirstPropertyValue -InputObject $Detail -PropertyNames @('slotType','slot') -DefaultValue ''
                $MacAddress = $Detail.macAddr

                # ONLY extension: add switch + switch-port fields already returned
                # under this exact bond-slave record.
                foreach ($Uplink in @($Detail.uplinkSwitchInfo)) {
                    if ($null -eq $Uplink) { continue }

                    $SwitchNames = @($Uplink.sysName) | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
                    $PortNames   = @(Get-PortNames -PortId $Uplink.portId)

                    if ($SwitchNames.Count -eq 0 -or $PortNames.Count -eq 0) { continue }

                    if ($SwitchNames.Count -eq 1) {
                        foreach ($PortName in $PortNames) {
                            $AllRows.Add([pscustomobject]@{
                                Cluster              = $ClusterName
                                NodeID               = [string]$Node.nodeId
                                NodeIP               = [string]$Node.nodeIp
                                ChassisSerial        = [string]$Node.chassisSerial
                                Bond                 = $BondName
                                BondingMode          = [string]$BondingMode
                                ActiveSlave          = $ActiveBondSlave
                                BondSlave            = [string]$BondSlave
                                SlaveInterfaceStatus = [string]$LinkState
                                SlaveSpeed           = [string]$SlaveSpeed
                                MTU                  = [string]$MTU
                                MacAddress           = [string]$MacAddress
                                SlotType             = [string]$SlotType
                                SwitchFQDN           = [string]$SwitchNames[0]
                                PortId               = [string]$PortName
                                RxPkts               = [uint64]$RxPkts
                                RxBytes              = [uint64]$RxBytes
                                RxErrors             = [uint64]$RxErrors
                                RxDropped            = [uint64]$RxDropped
                                TxPkts               = [uint64]$TxPkts
                                TxBytes              = [uint64]$TxBytes
                                TxErrors             = [uint64]$TxErrors
                                TxDropped            = [uint64]$TxDropped
                            })
                        }
                    }
                    else {
                        # If both switch names and ports are arrays, preserve their
                        # returned position rather than inventing a cross-product.
                        $Count = [Math]::Min($SwitchNames.Count, $PortNames.Count)
                        for ($i = 0; $i -lt $Count; $i++) {
                            $AllRows.Add([pscustomobject]@{
                                Cluster              = $ClusterName
                                NodeID               = [string]$Node.nodeId
                                NodeIP               = [string]$Node.nodeIp
                                ChassisSerial        = [string]$Node.chassisSerial
                                Bond                 = $BondName
                                BondingMode          = [string]$BondingMode
                                ActiveSlave          = $ActiveBondSlave
                                BondSlave            = [string]$BondSlave
                                SlaveInterfaceStatus = [string]$LinkState
                                SlaveSpeed           = [string]$SlaveSpeed
                                MTU                  = [string]$MTU
                                MacAddress           = [string]$MacAddress
                                SlotType             = [string]$SlotType
                                SwitchFQDN           = [string]$SwitchNames[$i]
                                PortId               = [string]$PortNames[$i]
                                RxPkts               = [uint64]$RxPkts
                                RxBytes              = [uint64]$RxBytes
                                RxErrors             = [uint64]$RxErrors
                                RxDropped            = [uint64]$RxDropped
                                TxPkts               = [uint64]$TxPkts
                                TxBytes              = [uint64]$TxBytes
                                TxErrors             = [uint64]$TxErrors
                                TxDropped            = [uint64]$TxDropped
                            })
                        }
                    }
                }
            }
        }
    }
}

if ($AllRows.Count -eq 0) {
    Write-Host 'No usable uplink switch/interface rows were returned.' -ForegroundColor Yellow
    if ($Failures.Count -gt 0) {
        $Failures | Format-Table Cluster, ClusterId, Error -AutoSize
    }
    return
}

# ============================================================================
# 4) Filter the clean rows using the TXT
# ============================================================================
$Results = [System.Collections.Generic.List[object]]::new()

foreach ($Target in $Targets) {
    $SwitchRows = @($AllRows | Where-Object {
        Test-SwitchMatch -Requested $Target.Switch -Actual $_.SwitchFQDN
    })

    if ($SwitchRows.Count -eq 0) {
        $Results.Add([pscustomobject]@{
            RequestedSwitch    = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN         = ''
            PortId             = ''
            Cluster            = ''
            NodeID             = ''
            NodeIP             = ''
            Bond               = ''
            BondSlave          = ''
            LinkState          = ''
            SlaveSpeed         = ''
            MTU                = ''
            RxErrors           = ''
            RxDropped          = ''
            TxErrors           = ''
            TxDropped          = ''
            Status             = 'SWITCH NOT FOUND'
        })
        continue
    }

    $PortRows = @($SwitchRows | Where-Object {
        (Normalize-Text $_.PortId) -eq (Normalize-Text $Target.Interface)
    })

    if ($PortRows.Count -eq 0) {
        # Keep failure output concise; do not aggregate every returned field into
        # giant cells. Show only the available clean port names for this switch.
        $AvailablePorts = @($SwitchRows.PortId | Where-Object { $_ } | Sort-Object -Unique) -join ', '

        $Results.Add([pscustomobject]@{
            RequestedSwitch    = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN         = [string](@($SwitchRows.SwitchFQDN | Select-Object -First 1)[0])
            PortId             = $AvailablePorts
            Cluster            = ''
            NodeID             = ''
            NodeIP             = ''
            Bond               = ''
            BondSlave          = ''
            LinkState          = ''
            SlaveSpeed         = ''
            MTU                = ''
            RxErrors           = ''
            RxDropped          = ''
            TxErrors           = ''
            TxDropped          = ''
            Status             = 'SWITCH FOUND - INTERFACE NOT FOUND'
        })
        continue
    }

    foreach ($Row in $PortRows) {
        $Results.Add([pscustomobject]@{
            RequestedSwitch    = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN         = $Row.SwitchFQDN
            PortId             = $Row.PortId
            Cluster            = $Row.Cluster
            NodeID             = $Row.NodeID
            NodeIP             = $Row.NodeIP
            Bond               = $Row.Bond
            BondSlave          = $Row.BondSlave
            LinkState          = $Row.SlaveInterfaceStatus
            SlaveSpeed         = $Row.SlaveSpeed
            MTU                = $Row.MTU
            RxErrors           = $Row.RxErrors
            RxDropped          = $Row.RxDropped
            TxErrors           = $Row.TxErrors
            TxDropped          = $Row.TxDropped
            Status             = if ((Normalize-Text $Row.SlaveInterfaceStatus) -match '^(up|active|connected)$') { 'UP' } else { 'CHECK LINK' }
        })
    }
}

# ============================================================================
# 5) Concise output
# ============================================================================
Write-Host "`nCOHESITY INTERFACE DIAGNOSIS" -ForegroundColor Cyan
Write-Host '============================' -ForegroundColor Cyan

$Results |
    Format-Table RequestedSwitch, RequestedInterface, SwitchFQDN, PortId,
        Cluster, NodeID, NodeIP, Bond, BondSlave, LinkState, SlaveSpeed,
        MTU, RxErrors, RxDropped, TxErrors, TxDropped, Status -AutoSize

if (-not (Test-Path -LiteralPath $HistoryDir)) {
    New-Item -Path $HistoryDir -ItemType Directory -Force | Out-Null
}

$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$CsvPath = Join-Path $HistoryDir "Interface_Diagnosis_$Timestamp.csv"
$Results | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
Write-Host "`nSaved snapshot: $CsvPath" -ForegroundColor DarkGray

if ($Failures.Count -gt 0) {
    Write-Host "`nClusters that could not be queried:" -ForegroundColor Yellow
    $Failures | Format-Table Cluster, ClusterId, Error -AutoSize
}
