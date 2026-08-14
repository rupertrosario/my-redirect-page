# Cohesity Interface Port Diagnosis
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Target file format (space/tab separated):
#   SWITCH-NAME WEC11
#   SWITCH-NAME WEC12
#
# Switch matching is DNS-aware:
#   TXT: switch01
#   API: switch01.example.company.com
# are treated as the same switch. The actual FQDN returned by Cohesity is kept
# in the output.
#
# Design:
#   1. Decrypt Cohesity API key using the existing AES helper.
#   2. GET all accessible clusters.
#   3. GET /irisservices/api/v1/public/interface for each cluster.
#   4. Flatten interfaces[].bondSlavesDetails[].uplinkSwitchInfo[].
#   5. Match target Switch -> sysName (FQDN-aware) and Interface -> portId.
#   6. Report node/bond/NIC, link, speed, MTU and RX/TX counters.
#
# NOTE: Interface counters are cumulative current counters. This API does not
# provide previous-day/week snapshots by itself. Each run is saved to CSV so
# future runs can be compared historically.

param(
    [string]$TargetsFile = "$PSScriptRoot\Interface_Diagnosis_Targets.txt",
    [int]$SampleSeconds = 10,
    [string]$HistoryDir = "X:\PowerShell\Data\Cohesity\InterfaceDiagnosis"
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$Helper  = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$KeyFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

# -----------------------------------------------------------------------------
# 0. Existing encrypted API-key flow
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $Helper)) {
    throw "AES helper not found: $Helper"
}
if (-not (Test-Path -LiteralPath $KeyFile)) {
    throw "Encrypted Cohesity API key not found: $KeyFile"
}

. $Helper
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $KeyFile
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

function Get-FirstValue {
    param(
        [AllowNull()][object]$Object,
        [Parameter(Mandatory)][string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object) { return $Default }

    foreach ($Name in $Names) {
        $Property = $Object.PSObject.Properties[$Name]
        if ($null -ne $Property -and $null -ne $Property.Value) {
            return $Property.Value
        }
    }
    return $Default
}

function Normalize-Text {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return '' }
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Normalize-SwitchFull {
    param([AllowNull()][object]$Value)
    $Text = Normalize-Text $Value
    return $Text.TrimEnd('.')
}

function Normalize-SwitchShort {
    param([AllowNull()][object]$Value)
    $Text = Normalize-SwitchFull $Value
    if ([string]::IsNullOrWhiteSpace($Text)) { return '' }
    return ($Text -split '\.', 2)[0]
}

function Test-SwitchMatch {
    param(
        [AllowNull()][object]$Requested,
        [AllowNull()][object]$Actual
    )

    $RequestedFull = Normalize-SwitchFull $Requested
    $ActualFull    = Normalize-SwitchFull $Actual

    if ([string]::IsNullOrWhiteSpace($RequestedFull) -or
        [string]::IsNullOrWhiteSpace($ActualFull)) {
        return $false
    }

    # Exact FQDN/name match first.
    if ($RequestedFull -eq $ActualFull) { return $true }

    # Then compare short DNS hostname. This intentionally handles:
    # switch01  <-> switch01.company.example.com
    return (Normalize-SwitchShort $RequestedFull) -eq (Normalize-SwitchShort $ActualFull)
}

function Get-CounterValue {
    param(
        [AllowNull()][object]$Stats,
        [string[]]$Names
    )

    $Value = Get-FirstValue -Object $Stats -Names $Names -Default 0
    try { return [uint64]$Value } catch { return [uint64]0 }
}

function Get-SafeDelta {
    param(
        [uint64]$Current,
        [uint64]$Previous
    )

    # Counters can reset after reboot/interface reset. Do not report a huge
    # negative/overflow delta in that case.
    if ($Current -ge $Previous) {
        return [int64]($Current - $Previous)
    }
    return [int64]0
}

# -----------------------------------------------------------------------------
# 1. Load targets: <Switch><space/tab><PortId>
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $TargetsFile)) {
    throw "Targets file not found: $TargetsFile"
}

$TargetLines = @(Get-Content -LiteralPath $TargetsFile |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') })

if ($TargetLines.Count -eq 0) {
    throw "No switch/interface targets found in $TargetsFile"
}

$Targets = @(
    foreach ($Line in $TargetLines) {
        $Parts = @($Line -split '\s+', 2)
        if ($Parts.Count -ne 2) {
            Write-Warning "Skipping invalid target line: $Line"
            continue
        }

        $SwitchName = $Parts[0].Trim()
        $PortId     = $Parts[1].Trim()

        if ([string]::IsNullOrWhiteSpace($SwitchName) -or
            [string]::IsNullOrWhiteSpace($PortId)) {
            Write-Warning "Skipping incomplete target line: $Line"
            continue
        }

        [pscustomobject]@{
            Switch = $SwitchName
            PortId = $PortId
        }
    }
)

if ($Targets.Count -eq 0) {
    throw 'No valid switch/interface targets found.'
}

# -----------------------------------------------------------------------------
# 2. Discover accessible clusters
# -----------------------------------------------------------------------------
$ClusterResponse = Invoke-CohesityGet `
    -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers @{ apiKey = $ApiKey }

$Clusters = @($ClusterResponse.cohesityClusters)
if ($Clusters.Count -eq 0) {
    throw 'No accessible Cohesity clusters returned by Helios.'
}

# Use the payload shape already proven in this repository.
$InterfaceUrl = "$BaseUrl/irisservices/api/v1/public/interface" +
    '?bondInterfaceOnly=true' +
    '&ifaceGroupAssignedOnly=true' +
    '&includeUplinkSwitchInfo=true' +
    '&includeBondSlaveDetails=true' +
    '&includeStats=true'

function Get-DiagnosticSnapshot {
    $Rows = [System.Collections.Generic.List[object]]::new()

    foreach ($Cluster in ($Clusters | Sort-Object clusterName)) {
        $ClusterName = [string]$Cluster.clusterName
        $ClusterId   = $Cluster.clusterId

        $Headers = @{
            apiKey          = $ApiKey
            accessClusterId = [string]$ClusterId
        }

        try {
            $Nodes = @(Invoke-CohesityGet -Uri $InterfaceUrl -Headers $Headers)
        }
        catch {
            Write-Warning "Failed to query '$ClusterName' ($ClusterId): $($_.Exception.Message)"
            continue
        }

        foreach ($Node in $Nodes) {
            $NodeId = Get-FirstValue $Node @('nodeId','id') 'UNKNOWN'
            $NodeIp = Get-FirstValue $Node @('nodeIp','ip') 'UNKNOWN'
            $Serial = Get-FirstValue $Node @('chassisSerial') 'UNKNOWN'

            foreach ($Iface in @($Node.interfaces)) {
                if ($null -eq $Iface) { continue }

                $BondName  = Get-FirstValue $Iface @('name','interfaceName') 'UNKNOWN'
                $Mtu       = Get-FirstValue $Iface @('mtu') 'UNKNOWN'
                $BondStats = $Iface.stats

                foreach ($Member in @($Iface.bondSlavesDetails)) {
                    if ($null -eq $Member) { continue }

                    $MemberName = Get-FirstValue $Member @('name','ifaceName','interfaceName') 'UNKNOWN'
                    $LinkState  = Get-FirstValue $Member @('linkState','state','status') 'UNKNOWN'
                    $Speed      = Get-FirstValue $Member @('speed') 'UNKNOWN'
                    $Mac        = Get-FirstValue $Member @('macAddr','macAddress','mac') 'UNKNOWN'
                    $Slot       = Get-FirstValue $Member @('slot','slotType') 'UNKNOWN'

                    # Prefer physical-member counters when returned. Fall back to
                    # parent bond counters on releases where member stats are absent.
                    $Stats = $Member.stats
                    $StatsScope = 'MEMBER'
                    if ($null -eq $Stats) {
                        $Stats = $BondStats
                        $StatsScope = 'BOND'
                    }

                    foreach ($Uplink in @($Member.uplinkSwitchInfo)) {
                        if ($null -eq $Uplink) { continue }

                        $SwitchName = Get-FirstValue $Uplink @('sysName','name') ''
                        $PortId     = Get-FirstValue $Uplink @('portId') ''

                        if ([string]::IsNullOrWhiteSpace([string]$SwitchName) -or
                            [string]::IsNullOrWhiteSpace([string]$PortId)) {
                            continue
                        }

                        $Rows.Add([pscustomobject]@{
                            Cluster       = $ClusterName
                            ClusterId     = [string]$ClusterId
                            NodeId        = [string]$NodeId
                            NodeIp        = [string]$NodeIp
                            ChassisSerial = [string]$Serial
                            Bond          = [string]$BondName
                            Nic           = [string]$MemberName
                            Link          = [string]$LinkState
                            Speed         = [string]$Speed
                            MTU           = [string]$Mtu
                            MAC           = [string]$Mac
                            Slot          = [string]$Slot
                            Switch        = [string]$SwitchName
                            SwitchShort   = Normalize-SwitchShort $SwitchName
                            PortId        = [string]$PortId
                            StatsScope    = $StatsScope
                            RxErrors      = Get-CounterValue $Stats @('rxErrors','rxErr','rxErrs')
                            RxDropped     = Get-CounterValue $Stats @('rxDropped','rxDrop','rxDrops')
                            TxErrors      = Get-CounterValue $Stats @('txErrors','txErr','txErrs')
                            TxDropped     = Get-CounterValue $Stats @('txDropped','txDrop','txDrops')
                        })
                    }
                }
            }
        }
    }

    return @($Rows)
}

# -----------------------------------------------------------------------------
# 3. Collect complete interface dataset, then search it
# -----------------------------------------------------------------------------
Write-Host "`nCollecting full Cohesity interface dataset..." -ForegroundColor Cyan
$FirstAll = @(Get-DiagnosticSnapshot)

if ($FirstAll.Count -eq 0) {
    throw 'No uplink switch/interface records were returned by Cohesity.'
}

Write-Host "Returned uplink records: $($FirstAll.Count)" -ForegroundColor DarkGray
Write-Host "Waiting $SampleSeconds seconds for RX/TX counter comparison..." -ForegroundColor Cyan
Start-Sleep -Seconds $SampleSeconds

$SecondAll = @(Get-DiagnosticSnapshot)

$Results = [System.Collections.Generic.List[object]]::new()

foreach ($Target in $Targets) {
    # First find the switch. FQDN and short hostname are treated as equivalent.
    $SwitchMatches = @($SecondAll | Where-Object {
        Test-SwitchMatch -Requested $Target.Switch -Actual $_.Switch
    })

    if ($SwitchMatches.Count -eq 0) {
        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            Switch          = 'NOT FOUND'
            PortId          = $Target.PortId
            AvailablePorts  = ''
            Cluster         = ''
            NodeId          = ''
            NodeIp          = ''
            Bond            = ''
            Nic             = ''
            Link            = ''
            Speed           = ''
            MTU             = ''
            RxErrors        = ''
            RxErrDelta      = ''
            RxDropped       = ''
            RxDropDelta     = ''
            TxErrors        = ''
            TxErrDelta      = ''
            TxDropped       = ''
            TxDropDelta     = ''
            StatsScope      = ''
            Status          = 'SWITCH NOT FOUND'
        })
        continue
    }

    # Once the switch is found, match the exact returned portId.
    $Matches = @($SwitchMatches | Where-Object {
        (Normalize-Text $_.PortId) -eq (Normalize-Text $Target.PortId)
    })

    if ($Matches.Count -eq 0) {
        $ActualSwitches = @($SwitchMatches.Switch | Sort-Object -Unique)
        $AvailablePorts = @($SwitchMatches.PortId | Sort-Object -Unique)

        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            Switch          = ($ActualSwitches -join '; ')
            PortId          = $Target.PortId
            AvailablePorts  = ($AvailablePorts -join '; ')
            Cluster         = ''
            NodeId          = ''
            NodeIp          = ''
            Bond            = ''
            Nic             = ''
            Link            = ''
            Speed           = ''
            MTU             = ''
            RxErrors        = ''
            RxErrDelta      = ''
            RxDropped       = ''
            RxDropDelta     = ''
            TxErrors        = ''
            TxErrDelta      = ''
            TxDropped       = ''
            TxDropDelta     = ''
            StatsScope      = ''
            Status          = 'SWITCH FOUND - PORT NOT FOUND'
        })
        continue
    }

    foreach ($Row in $Matches) {
        $Previous = $FirstAll | Where-Object {
            $_.Cluster -eq $Row.Cluster -and
            $_.NodeId -eq $Row.NodeId -and
            $_.Bond -eq $Row.Bond -and
            $_.Nic -eq $Row.Nic -and
            (Normalize-SwitchFull $_.Switch) -eq (Normalize-SwitchFull $Row.Switch) -and
            (Normalize-Text $_.PortId) -eq (Normalize-Text $Row.PortId)
        } | Select-Object -First 1

        $RxErrDelta  = if ($Previous) { Get-SafeDelta $Row.RxErrors  $Previous.RxErrors  } else { 0 }
        $RxDropDelta = if ($Previous) { Get-SafeDelta $Row.RxDropped $Previous.RxDropped } else { 0 }
        $TxErrDelta  = if ($Previous) { Get-SafeDelta $Row.TxErrors  $Previous.TxErrors  } else { 0 }
        $TxDropDelta = if ($Previous) { Get-SafeDelta $Row.TxDropped $Previous.TxDropped } else { 0 }

        $Status = if ((Normalize-Text $Row.Link) -ne 'up') {
            'LINK DOWN'
        }
        elseif ($RxErrDelta -gt 0 -or $RxDropDelta -gt 0 -or
                $TxErrDelta -gt 0 -or $TxDropDelta -gt 0) {
            'COUNTERS INCREASING'
        }
        else {
            'OK'
        }

        $Results.Add([pscustomobject]@{
            RequestedSwitch = $Target.Switch
            Switch          = $Row.Switch
            PortId          = $Row.PortId
            AvailablePorts  = ''
            Cluster         = $Row.Cluster
            NodeId          = $Row.NodeId
            NodeIp          = $Row.NodeIp
            Bond            = $Row.Bond
            Nic             = $Row.Nic
            Link            = $Row.Link
            Speed           = $Row.Speed
            MTU             = $Row.MTU
            RxErrors        = $Row.RxErrors
            RxErrDelta      = $RxErrDelta
            RxDropped       = $Row.RxDropped
            RxDropDelta     = $RxDropDelta
            TxErrors        = $Row.TxErrors
            TxErrDelta      = $TxErrDelta
            TxDropped       = $Row.TxDropped
            TxDropDelta     = $TxDropDelta
            StatsScope      = $Row.StatsScope
            Status          = $Status
        })
    }
}

# -----------------------------------------------------------------------------
# 4. Console output
# -----------------------------------------------------------------------------
Write-Host "`nCOHESITY INTERFACE DIAGNOSIS" -ForegroundColor Cyan
Write-Host "============================" -ForegroundColor Cyan

$Results |
    Format-Table RequestedSwitch, Switch, PortId, Cluster, NodeId, NodeIp, Bond, Nic,
        Link, Speed, MTU, RxErrors, RxErrDelta, RxDropped,
        TxErrors, TxErrDelta, TxDropped, StatsScope, Status -AutoSize

# Show useful lookup diagnostics only when a requested port was not found.
$PortMisses = @($Results | Where-Object { $_.Status -eq 'SWITCH FOUND - PORT NOT FOUND' })
if ($PortMisses.Count -gt 0) {
    Write-Host "`nPORT LOOKUP DETAILS" -ForegroundColor Yellow
    Write-Host "===================" -ForegroundColor Yellow
    $PortMisses | Select-Object RequestedSwitch, Switch, PortId, AvailablePorts | Format-Table -AutoSize
}

# -----------------------------------------------------------------------------
# 5. Save a timestamped snapshot for future day/week comparisons
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $HistoryDir)) {
    New-Item -Path $HistoryDir -ItemType Directory -Force | Out-Null
}

$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$CsvPath = Join-Path $HistoryDir "Interface_Diagnosis_$Timestamp.csv"

$Results | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
Write-Host "`nSaved snapshot: $CsvPath" -ForegroundColor DarkGray

$ProblemRows = @($Results | Where-Object { $_.Status -ne 'OK' })
if ($ProblemRows.Count -eq 0) {
    Write-Host 'VERDICT: PASS - requested switch/port mappings were found, links are UP, and no RX/TX error/drop counters increased during the sample.' -ForegroundColor Green
}
else {
    Write-Host 'VERDICT: ATTENTION REQUIRED - review rows whose Status is not OK.' -ForegroundColor Yellow
}
