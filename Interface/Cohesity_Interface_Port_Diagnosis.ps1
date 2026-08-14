# Cohesity Interface Port Diagnosis
# STRICTLY READ-ONLY / GET ONLY
# PowerShell 5.1 compatible
#
# Target file format (space/tab separated):
#   SWITCH-NAME WEC11
#   SWITCH-NAME WEC12
#
# Design:
#   1. Decrypt Cohesity API key using the existing AES helper.
#   2. GET all accessible clusters.
#   3. GET /irisservices/api/v1/public/interface for each cluster.
#   4. Flatten interfaces[].bondSlavesDetails[].uplinkSwitchInfo[].
#   5. Match target Switch -> sysName and Interface -> portId.
#   6. Report the Cohesity node/bond/NIC details, MTU, speed, link state,
#      RX/TX errors/drops, and a short counter delta.
#
# NOTE: Interface counters are cumulative current counters. This API does not
# provide previous-day/week snapshots by itself. This script also saves each
# run to CSV so future runs can be compared historically.

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

function Get-CounterValue {
    param(
        [AllowNull()][object]$Stats,
        [string[]]$Names
    )

    $Value = Get-FirstValue -Object $Stats -Names $Names -Default 0
    try { return [uint64]$Value } catch { return [uint64]0 }
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

                $BondName = Get-FirstValue $Iface @('name','interfaceName') 'UNKNOWN'
                $Mtu      = Get-FirstValue $Iface @('mtu') 'UNKNOWN'
                $BondStats = $Iface.stats

                foreach ($Member in @($Iface.bondSlavesDetails)) {
                    if ($null -eq $Member) { continue }

                    $MemberName = Get-FirstValue $Member @('name','ifaceName','interfaceName') 'UNKNOWN'
                    $LinkState  = Get-FirstValue $Member @('linkState','state','status') 'UNKNOWN'
                    $Speed      = Get-FirstValue $Member @('speed') 'UNKNOWN'
                    $Mac        = Get-FirstValue $Member @('macAddr','macAddress','mac') 'UNKNOWN'
                    $Slot       = Get-FirstValue $Member @('slot','slotType') 'UNKNOWN'

                    # Some releases expose member stats; older/proven payloads expose
                    # stats on the parent interface. Prefer member stats when present.
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
    $Matches = @($SecondAll | Where-Object {
        (Normalize-Text $_.Switch) -eq (Normalize-Text $Target.Switch) -and
        (Normalize-Text $_.PortId) -eq (Normalize-Text $Target.PortId)
    })

    if ($Matches.Count -eq 0) {
        $Results.Add([pscustomobject]@{
            Switch      = $Target.Switch
            PortId      = $Target.PortId
            Cluster     = 'NOT FOUND'
            NodeId      = 'UNKNOWN'
            NodeIp      = 'UNKNOWN'
            Bond        = 'UNKNOWN'
            Nic         = 'UNKNOWN'
            Link        = 'UNKNOWN'
            Speed       = 'UNKNOWN'
            MTU         = 'UNKNOWN'
            RxErrors    = 'UNKNOWN'
            RxErrDelta  = 'UNKNOWN'
            RxDropped   = 'UNKNOWN'
            RxDropDelta = 'UNKNOWN'
            TxErrors    = 'UNKNOWN'
            TxErrDelta  = 'UNKNOWN'
            TxDropped   = 'UNKNOWN'
            TxDropDelta = 'UNKNOWN'
            StatsScope  = 'UNKNOWN'
            Status      = 'NOT FOUND'
        })
        continue
    }

    foreach ($Row in $Matches) {
        $Previous = $FirstAll | Where-Object {
            $_.Cluster -eq $Row.Cluster -and
            $_.NodeId -eq $Row.NodeId -and
            $_.Bond -eq $Row.Bond -and
            $_.Nic -eq $Row.Nic -and
            (Normalize-Text $_.Switch) -eq (Normalize-Text $Row.Switch) -and
            (Normalize-Text $_.PortId) -eq (Normalize-Text $Row.PortId)
        } | Select-Object -First 1

        $RxErrDelta  = if ($Previous) { [int64]$Row.RxErrors  - [int64]$Previous.RxErrors  } else { 0 }
        $RxDropDelta = if ($Previous) { [int64]$Row.RxDropped - [int64]$Previous.RxDropped } else { 0 }
        $TxErrDelta  = if ($Previous) { [int64]$Row.TxErrors  - [int64]$Previous.TxErrors  } else { 0 }
        $TxDropDelta = if ($Previous) { [int64]$Row.TxDropped - [int64]$Previous.TxDropped } else { 0 }

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
            Switch      = $Row.Switch
            PortId      = $Row.PortId
            Cluster     = $Row.Cluster
            NodeId      = $Row.NodeId
            NodeIp      = $Row.NodeIp
            Bond        = $Row.Bond
            Nic         = $Row.Nic
            Link        = $Row.Link
            Speed       = $Row.Speed
            MTU         = $Row.MTU
            RxErrors    = $Row.RxErrors
            RxErrDelta  = $RxErrDelta
            RxDropped   = $Row.RxDropped
            RxDropDelta = $RxDropDelta
            TxErrors    = $Row.TxErrors
            TxErrDelta  = $TxErrDelta
            TxDropped   = $Row.TxDropped
            TxDropDelta = $TxDropDelta
            StatsScope  = $Row.StatsScope
            Status      = $Status
        })
    }
}

# -----------------------------------------------------------------------------
# 4. Console output
# -----------------------------------------------------------------------------
Write-Host "`nCOHESITY INTERFACE DIAGNOSIS" -ForegroundColor Cyan
Write-Host "============================" -ForegroundColor Cyan

$Results |
    Format-Table Switch, PortId, Cluster, NodeId, NodeIp, Bond, Nic, Link, Speed, MTU,
        RxErrors, RxErrDelta, RxDropped, TxErrors, TxErrDelta, TxDropped,
        StatsScope, Status -AutoSize

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
