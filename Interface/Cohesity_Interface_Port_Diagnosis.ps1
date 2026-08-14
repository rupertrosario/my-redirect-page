# Cohesity Interface Port Diagnosis
# STRICTLY READ-ONLY / GET ONLY
#
# Input file format:
#   SWITCH-NAME-01 Ethernet1/17
#   SWITCH-NAME-02 Ethernet1/18
#
# One switch/interface pair per line. Spaces or tabs are accepted.
# The script scans accessible Cohesity clusters, finds the bond member connected
# to the supplied switch/interface, then reports link, speed, MTU and RX/TX counters.
# A second sample is taken so NOC can see whether error/drop counters are increasing.

param(
    [string]$TargetsFile = "$PSScriptRoot\Interface_Diagnosis_Targets.txt",
    [int]$SampleSeconds = 10,
    [string]$ApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
)

$ErrorActionPreference = "Stop"
$BaseUrl = "https://helios.cohesity.com"

function Invoke-CohesityGet {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers
}

function Get-FirstValue {
    param(
        [AllowNull()][object]$Object,
        [Parameter(Mandatory)][string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object) { return $Default }

    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties[$name]
        if ($null -ne $property -and $null -ne $property.Value) {
            return $property.Value
        }
    }

    return $Default
}

function Normalize-Text {
    param([AllowNull()][object]$Value)
    if ($null -eq $Value) { return "" }
    return ([string]$Value).Trim().ToLowerInvariant()
}

# -----------------------------------------------------------------------------
# 1. Load API key
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $ApiKeyPath)) {
    throw "API key file not found: $ApiKeyPath"
}

$ApiKey = (Get-Content -LiteralPath $ApiKeyPath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "API key file is empty: $ApiKeyPath"
}

# -----------------------------------------------------------------------------
# 2. Load switch/interface targets
# -----------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $TargetsFile)) {
    throw "Targets file not found: $TargetsFile"
}

$targetLines = @(Get-Content -LiteralPath $TargetsFile |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith("#") })

if ($targetLines.Count -eq 0) {
    throw "No switch/interface targets found in $TargetsFile"
}

$Targets = foreach ($line in $targetLines) {
    $parts = @($line -split '\s+', 2)

    if ($parts.Count -ne 2) {
        Write-Warning "Skipping invalid target line: $line"
        continue
    }

    $switchName    = $parts[0].Trim()
    $interfaceName = $parts[1].Trim()

    if ([string]::IsNullOrWhiteSpace($switchName) -or
        [string]::IsNullOrWhiteSpace($interfaceName)) {
        Write-Warning "Skipping incomplete target line: $line"
        continue
    }

    [pscustomobject]@{
        Switch    = $switchName
        Interface = $interfaceName
    }
}

if (@($Targets).Count -eq 0) {
    throw "No valid switch/interface targets found."
}

# -----------------------------------------------------------------------------
# 3. Discover accessible clusters
# -----------------------------------------------------------------------------
$clusterResponse = Invoke-CohesityGet `
    -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers @{ apiKey = $ApiKey }

$Clusters = @($clusterResponse.cohesityClusters)
if ($Clusters.Count -eq 0) {
    throw "No accessible Cohesity clusters returned by Helios."
}

$interfaceQuery = @(
    "cache=false"
    "bondInterfaceOnly=true"
    "ifaceGroupAssignedOnly=false"
    "includeUplinkSwitchInfo=true"
    "includeBondSlaveDetails=true"
    "includeStats=true"
) -join "&"

$InterfaceUrl = "$BaseUrl/v2/network-interfaces?$interfaceQuery"

function Get-DiagnosticSnapshot {
    $rows = [System.Collections.Generic.List[object]]::new()

    foreach ($cluster in ($Clusters | Sort-Object clusterName)) {
        $clusterName = [string]$cluster.clusterName
        $clusterId   = $cluster.clusterId

        $headers = @{
            apiKey          = $ApiKey
            accessClusterId = [string]$clusterId
        }

        try {
            $response = Invoke-CohesityGet -Uri $InterfaceUrl -Headers $headers
        }
        catch {
            Write-Warning "Failed to query '$clusterName' ($clusterId): $($_.Exception.Message)"
            continue
        }

        $nodes = if ($response.nodes) { @($response.nodes) } else { @($response) }

        foreach ($node in $nodes) {
            $nodeId = Get-FirstValue $node @("id", "nodeId") "UNKNOWN"
            $nodeIp = Get-FirstValue $node @("ip", "nodeIp") "UNKNOWN"
            $serial = Get-FirstValue $node @("chassisSerial") "UNKNOWN"

            foreach ($bond in @($node.interfaces)) {
                if ($null -eq $bond) { continue }

                $bondName = Get-FirstValue $bond @("name", "interfaceName") "UNKNOWN"
                $mtu      = Get-FirstValue $bond @("mtu") "UNKNOWN"
                $members  = @($bond.bondSlavesDetails)

                foreach ($member in $members) {
                    if ($null -eq $member) { continue }

                    $uplink         = $member.uplinkSwitch
                    $reportedSwitch = Get-FirstValue $uplink @("name") ""
                    $reportedPort   = Get-FirstValue $uplink @("portId") ""

                    foreach ($target in $Targets) {
                        if ((Normalize-Text $reportedSwitch) -ne (Normalize-Text $target.Switch)) { continue }
                        if ((Normalize-Text $reportedPort) -ne (Normalize-Text $target.Interface)) { continue }

                        $stats = $member.stats

                        $rows.Add([pscustomobject]@{
                            RequestedSwitch    = $target.Switch
                            RequestedInterface = $target.Interface
                            Cluster            = $clusterName
                            ClusterId          = $clusterId
                            NodeId             = $nodeId
                            NodeIp             = $nodeIp
                            ChassisSerial      = $serial
                            Bond               = $bondName
                            Nic                = Get-FirstValue $member @("name") "UNKNOWN"
                            Link               = Get-FirstValue $member @("linkState") "UNKNOWN"
                            Speed              = Get-FirstValue $member @("speed") "UNKNOWN"
                            MTU                = $mtu
                            Switch             = $reportedSwitch
                            SwitchInterface    = $reportedPort
                            RxErrors           = [uint64](Get-FirstValue $stats @("rxErrors", "rxErr", "rxErrs") 0)
                            RxDropped          = [uint64](Get-FirstValue $stats @("rxDropped", "rxDrop", "rxDrops") 0)
                            TxErrors           = [uint64](Get-FirstValue $stats @("txErrors", "txErr", "txErrs") 0)
                            TxDropped          = [uint64](Get-FirstValue $stats @("txDropped", "txDrop", "txDrops") 0)
                        })
                    }
                }
            }
        }
    }

    return @($rows)
}

# -----------------------------------------------------------------------------
# 4. First sample
# -----------------------------------------------------------------------------
Write-Host "`nCollecting Cohesity interface sample 1..." -ForegroundColor Cyan
$First = @(Get-DiagnosticSnapshot)

Write-Host "Waiting $SampleSeconds seconds for counter comparison..." -ForegroundColor Cyan
Start-Sleep -Seconds $SampleSeconds

# -----------------------------------------------------------------------------
# 5. Second sample and deltas
# -----------------------------------------------------------------------------
Write-Host "Collecting Cohesity interface sample 2..." -ForegroundColor Cyan
$Second = @(Get-DiagnosticSnapshot)

$Results = [System.Collections.Generic.List[object]]::new()

foreach ($target in $Targets) {
    $matches = @($Second | Where-Object {
        (Normalize-Text $_.RequestedSwitch) -eq (Normalize-Text $target.Switch) -and
        (Normalize-Text $_.RequestedInterface) -eq (Normalize-Text $target.Interface)
    })

    if ($matches.Count -eq 0) {
        $Results.Add([pscustomobject]@{
            Switch        = $target.Switch
            Interface     = $target.Interface
            Cluster       = "NOT FOUND"
            NodeIp        = "UNKNOWN"
            Bond          = "UNKNOWN"
            Nic           = "UNKNOWN"
            Link          = "UNKNOWN"
            Speed         = "UNKNOWN"
            MTU           = "UNKNOWN"
            RxErrors      = "UNKNOWN"
            RxErrDelta    = "UNKNOWN"
            RxDropped     = "UNKNOWN"
            RxDropDelta   = "UNKNOWN"
            TxErrors      = "UNKNOWN"
            TxErrDelta    = "UNKNOWN"
            TxDropped     = "UNKNOWN"
            TxDropDelta   = "UNKNOWN"
            Status        = "NOT FOUND"
        })
        continue
    }

    foreach ($row in $matches) {
        $previous = $First | Where-Object {
            $_.Cluster -eq $row.Cluster -and
            $_.NodeId -eq $row.NodeId -and
            $_.Bond -eq $row.Bond -and
            $_.Nic -eq $row.Nic -and
            (Normalize-Text $_.Switch) -eq (Normalize-Text $row.Switch) -and
            (Normalize-Text $_.SwitchInterface) -eq (Normalize-Text $row.SwitchInterface)
        } | Select-Object -First 1

        $rxErrDelta  = if ($previous) { [int64]$row.RxErrors  - [int64]$previous.RxErrors  } else { 0 }
        $rxDropDelta = if ($previous) { [int64]$row.RxDropped - [int64]$previous.RxDropped } else { 0 }
        $txErrDelta  = if ($previous) { [int64]$row.TxErrors  - [int64]$previous.TxErrors  } else { 0 }
        $txDropDelta = if ($previous) { [int64]$row.TxDropped - [int64]$previous.TxDropped } else { 0 }

        $status = if ((Normalize-Text $row.Link) -ne "up") {
            "LINK DOWN"
        }
        elseif ($rxErrDelta -gt 0 -or $rxDropDelta -gt 0 -or $txErrDelta -gt 0 -or $txDropDelta -gt 0) {
            "COUNTERS INCREASING"
        }
        else {
            "OK"
        }

        $Results.Add([pscustomobject]@{
            Switch        = $row.Switch
            Interface     = $row.SwitchInterface
            Cluster       = $row.Cluster
            NodeIp        = $row.NodeIp
            Bond          = $row.Bond
            Nic           = $row.Nic
            Link          = $row.Link
            Speed         = $row.Speed
            MTU           = $row.MTU
            RxErrors      = $row.RxErrors
            RxErrDelta    = $rxErrDelta
            RxDropped     = $row.RxDropped
            RxDropDelta   = $rxDropDelta
            TxErrors      = $row.TxErrors
            TxErrDelta    = $txErrDelta
            TxDropped     = $row.TxDropped
            TxDropDelta   = $txDropDelta
            Status        = $status
        })
    }
}

# -----------------------------------------------------------------------------
# 6. NOC-friendly output
# -----------------------------------------------------------------------------
Write-Host "`nCOHESITY INTERFACE DIAGNOSIS" -ForegroundColor Cyan
Write-Host "============================" -ForegroundColor Cyan

$Results |
    Format-Table Switch, Interface, Cluster, NodeIp, Bond, Nic, Link, Speed, MTU,
        RxErrors, RxErrDelta, RxDropped, RxDropDelta,
        TxErrors, TxErrDelta, TxDropped, TxDropDelta, Status -AutoSize

$problemRows = @($Results | Where-Object { $_.Status -ne "OK" })

if ($problemRows.Count -eq 0) {
    Write-Host "`nVERDICT: PASS - all requested switch interfaces were found; links are UP and no RX/TX error or drop counters increased during the sample." -ForegroundColor Green
}
else {
    Write-Host "`nVERDICT: ATTENTION REQUIRED - review rows whose Status is not OK." -ForegroundColor Yellow
}
