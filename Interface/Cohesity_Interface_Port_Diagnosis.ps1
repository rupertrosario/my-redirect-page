# Cohesity Interface Port Diagnosis
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Reuses the proven /public/interface collection used by
# Cohesity_Interface_Health_Stats.ps1 and only adds uplinkSwitchInfo filtering.

param(
    [string]$TargetsFile = 'X:\PowerShell\Cohesity_Automations\Interface\Interface_Diagnosis_Targets.txt',
    [string]$HistoryDir  = 'X:\PowerShell\Data\Cohesity\InterfaceDiagnosis'
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$AesHelper = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'
$NotReturned = 'NOT RETURNED'

# ============================================================================
# 0) Existing encrypted API-key flow
# ============================================================================
if (-not (Test-Path -LiteralPath $AesHelper)) { throw "AES helper not found: $AesHelper" }
if (-not (Test-Path -LiteralPath $EncryptedKeyFile)) { throw "Encrypted API key not found: $EncryptedKeyFile" }

. $AesHelper
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedKeyFile
if ([string]::IsNullOrWhiteSpace([string]$ApiKey)) { throw 'Empty Cohesity API key returned by AES helper.' }

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
        [AllowNull()][object]$DefaultValue = $null
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
    $ActualFull = (Normalize-Text $Actual).TrimEnd('.')

    if (-not $RequestedFull -or -not $ActualFull) { return $false }
    if ($RequestedFull -eq $ActualFull) { return $true }

    return (Get-ShortHostName $RequestedFull) -eq (Get-ShortHostName $ActualFull)
}

function Get-Counter {
    param(
        [AllowNull()][object]$Stats,
        [string[]]$Names
    )

    $Value = Get-FirstPropertyValue -InputObject $Stats -PropertyNames $Names -DefaultValue 0
    try { return [uint64]$Value } catch { return [uint64]0 }
}

function To-DisplayValue {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { return $NotReturned }
    return [string]$Value
}

# Cohesity can return uplinkSwitchInfo.portId as an object/array rather than
# a scalar string. In the observed payload the actual switch port is in ifname.
# Flatten every returned port into a clean string before matching.
function Get-UplinkPortNames {
    param([AllowNull()][object]$Uplink)

    if ($null -eq $Uplink) { return @() }

    $RawPortId = Get-FirstPropertyValue -InputObject $Uplink -PropertyNames @('portId') -DefaultValue $null
    if ($null -eq $RawPortId) { return @() }

    $Ports = [System.Collections.Generic.List[string]]::new()

    foreach ($Item in @($RawPortId)) {
        if ($null -eq $Item) { continue }

        if ($Item -is [string]) {
            $Text = $Item.Trim()
            if (-not $Text) { continue }

            # Handles text such as "ifname Ethernet11" if PowerShell/string
            # rendering has already flattened the object.
            if ($Text -match '(?i)^ifname\s+(.+)$') {
                $Ports.Add($Matches[1].Trim())
            }
            else {
                $Ports.Add($Text)
            }
            continue
        }

        $IfName = Get-FirstPropertyValue -InputObject $Item -PropertyNames @('ifname','ifName','name','portName','interfaceName') -DefaultValue $null
        if ($null -ne $IfName -and -not [string]::IsNullOrWhiteSpace([string]$IfName)) {
            $Ports.Add(([string]$IfName).Trim())
            continue
        }

        # Last-resort representation for unexpected releases, without guessing.
        $Text = ([string]$Item).Trim()
        if ($Text -match '(?i)^ifname\s+(.+)$') {
            $Ports.Add($Matches[1].Trim())
        }
    }

    return @($Ports | Where-Object { $_ } | Select-Object -Unique)
}

# ============================================================================
# 1) Load targets: <switch><space/tab><interface>
# ============================================================================
if (-not (Test-Path -LiteralPath $TargetsFile)) { throw "Targets file not found: $TargetsFile" }

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
        Switch = $Parts[0].Trim()
        Interface = $Parts[1].Trim()
    }
}

if ($Targets.Count -eq 0) { throw "No valid targets found in $TargetsFile" }

# ============================================================================
# 2) Same cluster discovery as the working collector
# ============================================================================
$ClusterUrl = "$BaseUrl/v2/mcm/cluster-mgmt/info"
$ClusterResponse = Invoke-CohesityGet -Uri $ClusterUrl -Headers @{ apiKey = $ApiKey }
$Clusters = @($ClusterResponse.cohesityClusters)
if ($Clusters.Count -eq 0) { throw 'No clusters were returned by Helios.' }

# ============================================================================
# 3) Same /public/interface request as Cohesity_Interface_Health_Stats.ps1
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
    $ClusterId = $Cluster.clusterId

    $Headers = @{
        apiKey = $ApiKey
        accessClusterId = $ClusterId
    }

    try {
        $Raw = Invoke-CohesityGet -Uri $InterfaceUrl -Headers $Headers
    }
    catch {
        $Failures.Add([pscustomobject]@{
            Cluster = $ClusterName
            ClusterId = $ClusterId
            Error = $_.Exception.Message
        })
        continue
    }

    $Nodes = if ($Raw.PSObject.Properties['nodes']) { @($Raw.nodes) } else { @($Raw) }

    foreach ($Node in $Nodes) {
        foreach ($Iface in @($Node.interfaces)) {
            if ($null -eq $Iface) { continue }

            # Reuse the existing collector's iface.stats logic exactly.
            $Stats = $Iface.stats
            $RxPkts = Get-Counter $Stats @('rxPkts','rxPackets')
            $RxBytes = Get-Counter $Stats @('rxBytes')
            $RxErrors = Get-Counter $Stats @('rxErr','rxErrs','rxErrors')
            $RxDropped = Get-Counter $Stats @('rxDropped','rxDrop','rxDrops')
            $TxPkts = Get-Counter $Stats @('txPkts','txPackets')
            $TxBytes = Get-Counter $Stats @('txBytes')
            $TxErrors = Get-Counter $Stats @('txErr','txErrs','txErrors')
            $TxDropped = Get-Counter $Stats @('txDropped','txDrop','txDrops')

            $BondName = Get-FirstPropertyValue $Iface @('name','interfaceName') ''
            $MTU = Get-FirstPropertyValue $Iface @('mtu') $null
            $BondingMode = Get-FirstPropertyValue $Iface @('bondingMode') ''
            $ActiveSlave = Get-FirstPropertyValue $Iface @('activeBondSlave') ''

            foreach ($Detail in @($Iface.bondSlavesDetails)) {
                if ($null -eq $Detail) { continue }

                $BondSlave = Get-FirstPropertyValue $Detail @('name','@name','ifaceName','interfaceName','nicName') ''
                $LinkState = Get-FirstPropertyValue $Detail @('linkState','state','status') ''
                $SlaveSpeed = Get-FirstPropertyValue $Detail @('speed') $null
                $MacAddress = Get-FirstPropertyValue $Detail @('macAddr','mac','macAddress','mac_address') ''
                $SlotType = Get-FirstPropertyValue $Detail @('slotType','slot') ''

                foreach ($Uplink in @($Detail.uplinkSwitchInfo)) {
                    if ($null -eq $Uplink) { continue }

                    $SwitchFqdn = Get-FirstPropertyValue $Uplink @('sysName','name') ''
                    $PortNames = @(Get-UplinkPortNames -Uplink $Uplink)

                    if (-not $SwitchFqdn -and $PortNames.Count -eq 0) { continue }

                    # IMPORTANT: one row per actual ifname. Do not stringify the
                    # complete portId object/array into one unusable PortId value.
                    foreach ($PortName in $PortNames) {
                        $AllRows.Add([pscustomobject]@{
                            Cluster = $ClusterName
                            ClusterId = [string]$ClusterId
                            NodeID = [string](Get-FirstPropertyValue $Node @('nodeId','id') '')
                            NodeName = [string](Get-FirstPropertyValue $Node @('nodeName','name','hostname','hostName') '')
                            NodeIP = [string](Get-FirstPropertyValue $Node @('nodeIp','ip') '')
                            ChassisSerial = [string](Get-FirstPropertyValue $Node @('chassisSerial') '')
                            Bond = [string]$BondName
                            BondingMode = [string]$BondingMode
                            ActiveSlave = [string]$ActiveSlave
                            BondSlave = [string]$BondSlave
                            LinkState = [string]$LinkState
                            SlaveSpeed = $SlaveSpeed
                            MTU = $MTU
                            MAC = [string]$MacAddress
                            SlotType = [string]$SlotType
                            SwitchFQDN = [string]$SwitchFqdn
                            SwitchShort = Get-ShortHostName $SwitchFqdn
                            PortId = [string]$PortName
                            RxPkts = $RxPkts
                            RxBytes = $RxBytes
                            RxErrors = $RxErrors
                            RxDropped = $RxDropped
                            TxPkts = $TxPkts
                            TxBytes = $TxBytes
                            TxErrors = $TxErrors
                            TxDropped = $TxDropped
                        })
                    }
                }
            }
        }
    }
}

if ($AllRows.Count -eq 0) {
    Write-Host 'No usable uplinkSwitchInfo/portId records were returned by the interface endpoint.' -ForegroundColor Yellow
    if ($Failures.Count -gt 0) { $Failures | Format-Table Cluster, ClusterId, Error -AutoSize }
    return
}

# ============================================================================
# 4) Search collected data by switch + actual ifname
# ============================================================================
$Results = [System.Collections.Generic.List[object]]::new()

foreach ($Target in $Targets) {
    $SwitchRows = @($AllRows | Where-Object {
        Test-SwitchMatch -Requested $Target.Switch -Actual $_.SwitchFQDN
    })

    if ($SwitchRows.Count -eq 0) {
        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN = ''
            PortId = ''
            Cluster = ''
            NodeName = ''
            NodeID = ''
            NodeIP = ''
            Bond = ''
            BondSlave = ''
            LinkState = ''
            SlaveSpeed = ''
            MTU = ''
            RxErrors = ''
            RxDropped = ''
            TxErrors = ''
            TxDropped = ''
            Status = 'SWITCH NOT FOUND'
        })
        continue
    }

    $PortRows = @($SwitchRows | Where-Object {
        (Normalize-Text $_.PortId) -eq (Normalize-Text $Target.Interface)
    })

    if ($PortRows.Count -eq 0) {
        $ActualSwitch = @($SwitchRows.SwitchFQDN | Sort-Object -Unique) -join '; '
        $ActualPorts = @($SwitchRows.PortId | Where-Object { $_ } | Sort-Object -Unique) -join '; '

        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN = $ActualSwitch
            PortId = $ActualPorts
            Cluster = (@($SwitchRows.Cluster | Sort-Object -Unique) -join '; ')
            NodeName = ''
            NodeID = ''
            NodeIP = ''
            Bond = ''
            BondSlave = ''
            LinkState = ''
            SlaveSpeed = ''
            MTU = ''
            RxErrors = ''
            RxDropped = ''
            TxErrors = ''
            TxDropped = ''
            Status = 'SWITCH FOUND - INTERFACE NOT FOUND; ACTUAL PORTS SHOWN'
        })
        continue
    }

    foreach ($Row in $PortRows) {
        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN = $Row.SwitchFQDN
            PortId = $Row.PortId
            Cluster = $Row.Cluster
            NodeName = (To-DisplayValue $Row.NodeName)
            NodeID = (To-DisplayValue $Row.NodeID)
            NodeIP = (To-DisplayValue $Row.NodeIP)
            Bond = (To-DisplayValue $Row.Bond)
            BondSlave = (To-DisplayValue $Row.BondSlave)
            LinkState = (To-DisplayValue $Row.LinkState)
            SlaveSpeed = (To-DisplayValue $Row.SlaveSpeed)
            MTU = (To-DisplayValue $Row.MTU)
            RxErrors = $Row.RxErrors
            RxDropped = $Row.RxDropped
            TxErrors = $Row.TxErrors
            TxDropped = $Row.TxDropped
            Status = if ((Normalize-Text $Row.LinkState) -match '^(up|active|connected)$') { 'UP' } else { 'CHECK LINK' }
        })
    }
}

# ============================================================================
# 5) NOC-friendly output
# ============================================================================
Write-Host "`nCOHESITY INTERFACE DIAGNOSIS" -ForegroundColor Cyan
Write-Host '============================' -ForegroundColor Cyan

$Results |
    Format-Table RequestedSwitch, RequestedInterface, SwitchFQDN, PortId,
        Cluster, NodeName, NodeID, NodeIP, Bond, BondSlave, LinkState,
        SlaveSpeed, MTU, RxErrors, RxDropped, TxErrors, TxDropped, Status -AutoSize

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
