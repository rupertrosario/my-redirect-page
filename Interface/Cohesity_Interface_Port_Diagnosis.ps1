# Cohesity Interface Port Diagnosis
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Reuses the proven /public/interface collection used by
# Cohesity_Interface_Health_Stats.ps1 and only adds target filtering.
# Nested Cohesity arrays/objects are expanded into logical scalar rows before
# matching or displaying; object renderings are never written to output cells.

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

function Get-PropertyValue {
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

# Recursively return only actual scalar values. Never stringify a PSCustomObject,
# because that is what produced cells such as "ifname Ethernet11 ..." and
# System.Object[]/property dumps.
function Get-ScalarValues {
    param(
        [AllowNull()][object]$Value,
        [string[]]$PreferredProperties = @()
    )

    $Out = [System.Collections.Generic.List[string]]::new()

    function Add-ScalarValue {
        param([AllowNull()][object]$Item)

        if ($null -eq $Item) { return }

        if ($Item -is [string]) {
            $Text = $Item.Trim()
            if ($Text) { $Out.Add($Text) }
            return
        }

        if ($Item -is [bool] -or
            $Item -is [byte] -or $Item -is [sbyte] -or
            $Item -is [int16] -or $Item -is [uint16] -or
            $Item -is [int32] -or $Item -is [uint32] -or
            $Item -is [int64] -or $Item -is [uint64] -or
            $Item -is [single] -or $Item -is [double] -or $Item -is [decimal]) {
            $Out.Add([string]$Item)
            return
        }

        if ($Item -is [System.Collections.IEnumerable] -and $Item -isnot [string] -and $Item -isnot [System.Collections.IDictionary]) {
            foreach ($Child in $Item) { Add-ScalarValue $Child }
            return
        }

        foreach ($Name in $PreferredProperties) {
            $P = $Item.PSObject.Properties[$Name]
            if ($null -ne $P -and $null -ne $P.Value) {
                $Before = $Out.Count
                Add-ScalarValue $P.Value
                if ($Out.Count -gt $Before) { return }
            }
        }

        # Unknown complex objects are deliberately not converted to strings.
        return
    }

    Add-ScalarValue $Value
    return @($Out | Where-Object { $_ } | Select-Object -Unique)
}

function Get-FieldValues {
    param(
        [AllowNull()][object]$Object,
        [Parameter(Mandatory)][string[]]$PropertyNames,
        [string[]]$NestedPreferredProperties = @()
    )

    $Raw = Get-PropertyValue -InputObject $Object -PropertyNames $PropertyNames -DefaultValue $null
    return @(Get-ScalarValues -Value $Raw -PreferredProperties $NestedPreferredProperties)
}

function Get-OneValue {
    param(
        [AllowNull()][object]$Object,
        [Parameter(Mandatory)][string[]]$PropertyNames,
        [string[]]$NestedPreferredProperties = @(),
        [string]$DefaultValue = ''
    )

    $Values = @(Get-FieldValues -Object $Object -PropertyNames $PropertyNames -NestedPreferredProperties $NestedPreferredProperties)
    if ($Values.Count -eq 0) { return $DefaultValue }
    return $Values[0]
}

function Get-IndexedValue {
    param(
        [object[]]$Values,
        [int]$Index,
        [string]$DefaultValue = ''
    )

    $Items = @($Values)
    if ($Items.Count -eq 0) { return $DefaultValue }
    if ($Items.Count -eq 1) { return [string]$Items[0] }
    if ($Index -lt $Items.Count) { return [string]$Items[$Index] }
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

    $Values = @(Get-FieldValues -Object $Stats -PropertyNames $Names)
    if ($Values.Count -eq 0) { return [uint64]0 }
    try { return [uint64]$Values[0] } catch { return [uint64]0 }
}

function To-DisplayValue {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { return $NotReturned }
    return [string]$Value
}

# Expand one uplinkSwitchInfo object into scalar switch/port pairs. This handles:
# - one switch + one port
# - one switch + many ports
# - parallel arrays of switches and ports
function Get-UplinkPairs {
    param([AllowNull()][object]$Uplink)

    if ($null -eq $Uplink) { return @() }

    $SwitchNames = @(Get-FieldValues -Object $Uplink -PropertyNames @('sysName','name'))
    $PortIdsRaw = Get-PropertyValue -InputObject $Uplink -PropertyNames @('portId') -DefaultValue $null
    $PortNames = @(Get-ScalarValues -Value $PortIdsRaw -PreferredProperties @('ifname','ifName','name','portName','interfaceName'))

    if ($SwitchNames.Count -eq 0 -or $PortNames.Count -eq 0) { return @() }

    $Pairs = [System.Collections.Generic.List[object]]::new()

    if ($SwitchNames.Count -eq 1) {
        foreach ($Port in $PortNames) {
            $Pairs.Add([pscustomobject]@{ SwitchFQDN = $SwitchNames[0]; PortId = $Port })
        }
        return @($Pairs)
    }

    if ($PortNames.Count -eq 1) {
        foreach ($Switch in $SwitchNames) {
            $Pairs.Add([pscustomobject]@{ SwitchFQDN = $Switch; PortId = $PortNames[0] })
        }
        return @($Pairs)
    }

    # When both are arrays, preserve positional correlation instead of creating
    # a cross-product that could invent mappings.
    $Count = [Math]::Min($SwitchNames.Count, $PortNames.Count)
    for ($i = 0; $i -lt $Count; $i++) {
        $Pairs.Add([pscustomobject]@{ SwitchFQDN = $SwitchNames[$i]; PortId = $PortNames[$i] })
    }

    return @($Pairs)
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
    $ClusterName = Get-OneValue -Object $Cluster -PropertyNames @('clusterName','name') -DefaultValue ''
    $ClusterId = Get-OneValue -Object $Cluster -PropertyNames @('clusterId','id') -DefaultValue ''

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
        $NodeID = Get-OneValue -Object $Node -PropertyNames @('nodeId','id')
        $NodeName = Get-OneValue -Object $Node -PropertyNames @('nodeName','name','hostname','hostName')
        $NodeIP = Get-OneValue -Object $Node -PropertyNames @('nodeIp','ip')
        $ChassisSerial = Get-OneValue -Object $Node -PropertyNames @('chassisSerial')

        foreach ($Iface in @($Node.interfaces)) {
            if ($null -eq $Iface) { continue }

            $Stats = $Iface.stats
            $RxPkts = Get-Counter $Stats @('rxPkts','rxPackets')
            $RxBytes = Get-Counter $Stats @('rxBytes')
            $RxErrors = Get-Counter $Stats @('rxErr','rxErrs','rxErrors')
            $RxDropped = Get-Counter $Stats @('rxDropped','rxDrop','rxDrops')
            $TxPkts = Get-Counter $Stats @('txPkts','txPackets')
            $TxBytes = Get-Counter $Stats @('txBytes')
            $TxErrors = Get-Counter $Stats @('txErr','txErrs','txErrors')
            $TxDropped = Get-Counter $Stats @('txDropped','txDrop','txDrops')

            $BondNames = @(Get-FieldValues -Object $Iface -PropertyNames @('name','interfaceName'))
            $Mtus = @(Get-FieldValues -Object $Iface -PropertyNames @('mtu'))
            $BondModes = @(Get-FieldValues -Object $Iface -PropertyNames @('bondingMode'))
            $ActiveSlaves = @(Get-FieldValues -Object $Iface -PropertyNames @('activeBondSlave'))

            foreach ($Detail in @($Iface.bondSlavesDetails)) {
                if ($null -eq $Detail) { continue }

                $BondSlaves = @(Get-FieldValues -Object $Detail -PropertyNames @('name','@name','ifaceName','interfaceName','nicName'))
                $LinkStates = @(Get-FieldValues -Object $Detail -PropertyNames @('linkState','state','status'))
                $SlaveSpeeds = @(Get-FieldValues -Object $Detail -PropertyNames @('speed'))
                $MacAddresses = @(Get-FieldValues -Object $Detail -PropertyNames @('macAddr','mac','macAddress','mac_address'))
                $SlotTypes = @(Get-FieldValues -Object $Detail -PropertyNames @('slotType','slot'))
                $Uplinks = @($Detail.uplinkSwitchInfo)

                $DetailCount = @(
                    $BondSlaves.Count,
                    $LinkStates.Count,
                    $SlaveSpeeds.Count,
                    $MacAddresses.Count,
                    $SlotTypes.Count,
                    $Uplinks.Count
                ) | Measure-Object -Maximum | Select-Object -ExpandProperty Maximum

                if (-not $DetailCount -or $DetailCount -lt 1) { $DetailCount = 1 }

                for ($i = 0; $i -lt $DetailCount; $i++) {
                    $BondSlave = Get-IndexedValue -Values $BondSlaves -Index $i
                    $LinkState = Get-IndexedValue -Values $LinkStates -Index $i
                    $SlaveSpeed = Get-IndexedValue -Values $SlaveSpeeds -Index $i
                    $MacAddress = Get-IndexedValue -Values $MacAddresses -Index $i
                    $SlotType = Get-IndexedValue -Values $SlotTypes -Index $i

                    $BondName = Get-IndexedValue -Values $BondNames -Index $i
                    $Mtu = Get-IndexedValue -Values $Mtus -Index $i
                    $BondingMode = Get-IndexedValue -Values $BondModes -Index $i
                    $ActiveSlave = Get-IndexedValue -Values $ActiveSlaves -Index $i

                    $Uplink = $null
                    if ($Uplinks.Count -eq 1) { $Uplink = $Uplinks[0] }
                    elseif ($i -lt $Uplinks.Count) { $Uplink = $Uplinks[$i] }

                    $Pairs = @(Get-UplinkPairs -Uplink $Uplink)
                    foreach ($Pair in $Pairs) {
                        $AllRows.Add([pscustomobject]@{
                            Cluster = $ClusterName
                            ClusterId = $ClusterId
                            NodeID = $NodeID
                            NodeName = $NodeName
                            NodeIP = $NodeIP
                            ChassisSerial = $ChassisSerial
                            Bond = $BondName
                            BondingMode = $BondingMode
                            ActiveSlave = $ActiveSlave
                            BondSlave = $BondSlave
                            LinkState = $LinkState
                            SlaveSpeed = $SlaveSpeed
                            MTU = $Mtu
                            MAC = $MacAddress
                            SlotType = $SlotType
                            SwitchFQDN = $Pair.SwitchFQDN
                            SwitchShort = Get-ShortHostName $Pair.SwitchFQDN
                            PortId = $Pair.PortId
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
    Write-Host 'No usable scalar uplinkSwitchInfo records were returned by the interface endpoint.' -ForegroundColor Yellow
    if ($Failures.Count -gt 0) { $Failures | Format-Table Cluster, ClusterId, Error -AutoSize }
    return
}

# ============================================================================
# 4) Search collected scalar rows by switch + interface
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
        $ActualSwitch = @($SwitchRows.SwitchFQDN | Where-Object { $_ } | Sort-Object -Unique) -join '; '
        $ActualPorts = @($SwitchRows.PortId | Where-Object { $_ } | Sort-Object -Unique) -join '; '

        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            RequestedInterface = $Target.Interface
            SwitchFQDN = $ActualSwitch
            PortId = $ActualPorts
            Cluster = (@($SwitchRows.Cluster | Where-Object { $_ } | Sort-Object -Unique) -join '; ')
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
            Cluster = (To-DisplayValue $Row.Cluster)
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
