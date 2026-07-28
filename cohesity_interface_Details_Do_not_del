# Cohesity Helios - Cluster Interface Health (Full Dump + Summary)
# ----------------------------------------------------------------
# - Loads the Helios API key from the configured file.
# - Retrieves every accessible cluster from /v2/mcm/cluster-mgmt/info.
# - Calls /irisservices/api/v1/public/interface for each cluster.
# - Requests bond slave details, bonding mode, active slave and interface stats.
# - Produces a full per-slave dump, a per-cluster summary and a down-interface view.
#
# NOTE: RX/TX counters are cumulative values returned by Cohesity. They are not
# rates and should not be treated as a current fault without comparing samples.

$ErrorActionPreference = "Stop"

# =========================
# 0) API key configuration
# =========================
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

if (-not (Test-Path -LiteralPath $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$ApiKey = (Get-Content -LiteralPath $apikeypath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "API key file is empty: $apikeypath"
}

$BaseUrl       = "https://helios.cohesity.com"
$commonHeaders = @{ apiKey = $ApiKey }

# ==========================================================
# Helper: return the first matching property value available
# ==========================================================
function Get-FirstPropertyValue {
    param(
        [AllowNull()]
        [object]$InputObject,

        [Parameter(Mandatory)]
        [string[]]$PropertyNames,

        [AllowNull()]
        [object]$DefaultValue = 0
    )

    if ($null -eq $InputObject) {
        return $DefaultValue
    }

    foreach ($propertyName in $PropertyNames) {
        $property = $InputObject.PSObject.Properties[$propertyName]
        if ($null -ne $property -and $null -ne $property.Value) {
            return $property.Value
        }
    }

    return $DefaultValue
}

# ==========================================
# 1) Get clusters (ClusterName + ClusterId)
# ==========================================
$clusterUrl = "$BaseUrl/v2/mcm/cluster-mgmt/info"

try {
    $clusterResponse = Invoke-RestMethod `
        -Method Get `
        -Uri $clusterUrl `
        -Headers $commonHeaders
}
catch {
    throw "Failed to retrieve clusters from Helios: $($_.Exception.Message)"
}

$clusters = @($clusterResponse.cohesityClusters)
if ($clusters.Count -eq 0) {
    throw "No clusters were returned by Helios."
}

$AllRows  = [System.Collections.Generic.List[object]]::new()
$BondRows = [System.Collections.Generic.List[object]]::new()
$Failures = [System.Collections.Generic.List[object]]::new()

# ===========================================================
# 2) Get interface details and statistics from every cluster
# ===========================================================
$interfaceQuery = @(
    "bondInterfaceOnly=true"
    "ifaceGroupAssignedOnly=true"
    "includeUplinkSwitchInfo=true"
    "includeBondSlaveDetails=true"
    "includeStats=true"
) -join "&"

$interfaceUrl = "$BaseUrl/irisservices/api/v1/public/interface?$interfaceQuery"

foreach ($cluster in ($clusters | Sort-Object clusterName)) {
    $clusterName = [string]$cluster.clusterName
    $clusterId   = $cluster.clusterId

    $headers = @{
        apiKey          = $ApiKey
        accessClusterId = $clusterId
    }

    try {
        $nodes = @(Invoke-RestMethod `
            -Method Get `
            -Uri $interfaceUrl `
            -Headers $headers)
    }
    catch {
        $Failures.Add([pscustomobject]@{
            Cluster   = $clusterName
            ClusterId = $clusterId
            Error     = $_.Exception.Message
        })

        Write-Host "Failed to get interface data for '$clusterName' ($clusterId): $($_.Exception.Message)" -ForegroundColor Red
        continue
    }

    foreach ($node in $nodes) {
        foreach ($iface in @($node.interfaces)) {
            if ($null -eq $iface) {
                continue
            }

            $stats = $iface.stats

            # Support the current documented field names and common variants
            # seen across Cohesity releases.
            $rxPkts    = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("rxPkts", "rxPackets")
            $rxBytes   = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("rxBytes")
            $rxErrors  = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("rxErr", "rxErrs", "rxErrors")
            $rxDropped = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("rxDropped", "rxDrop", "rxDrops")
            $txPkts    = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("txPkts", "txPackets")
            $txBytes   = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("txBytes")
            $txErrors  = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("txErr", "txErrs", "txErrors")
            $txDropped = Get-FirstPropertyValue -InputObject $stats -PropertyNames @("txDropped", "txDrop", "txDrops")

            $bondName        = [string](Get-FirstPropertyValue -InputObject $iface -PropertyNames @("name", "interfaceName") -DefaultValue "")
            $bondingMode     = Get-FirstPropertyValue -InputObject $iface -PropertyNames @("bondingMode") -DefaultValue ""
            $activeBondSlave = [string](Get-FirstPropertyValue -InputObject $iface -PropertyNames @("activeBondSlave") -DefaultValue "")

            # One record per bond for accurate cluster-level counter totals.
            $BondRows.Add([pscustomobject]@{
                Cluster     = $clusterName
                ClusterId   = $clusterId
                NodeID      = $node.nodeId
                NodeIP      = $node.nodeIp
                Bond        = $bondName
                BondingMode = $bondingMode
                ActiveSlave = $activeBondSlave
                RxPkts      = [uint64]$rxPkts
                RxBytes     = [uint64]$rxBytes
                RxErrors    = [uint64]$rxErrors
                RxDropped   = [uint64]$rxDropped
                TxPkts      = [uint64]$txPkts
                TxBytes     = [uint64]$txBytes
                TxErrors    = [uint64]$txErrors
                TxDropped   = [uint64]$txDropped
            })

            $slaveDetails = @($iface.bondSlavesDetails)

            if ($slaveDetails.Count -eq 0) {
                # Retain the bond row even when a release does not return
                # bondSlavesDetails, so stats and bonding mode are not lost.
                $AllRows.Add([pscustomobject]@{
                    Cluster              = $clusterName
                    NodeID               = $node.nodeId
                    NodeIP               = $node.nodeIp
                    ChassisSerial        = $node.chassisSerial
                    Bond                 = $bondName
                    BondingMode          = $bondingMode
                    ActiveSlave          = $activeBondSlave
                    BondSlave            = (@($iface.bondSlaves) -join ", ")
                    SlaveInterfaceStatus = "NotReturned"
                    SlaveSpeed           = ""
                    SlotType             = ""
                    MacAddress           = ""
                    RxPkts               = [uint64]$rxPkts
                    RxBytes              = [uint64]$rxBytes
                    RxErrors             = [uint64]$rxErrors
                    RxDropped            = [uint64]$rxDropped
                    TxPkts               = [uint64]$txPkts
                    TxBytes              = [uint64]$txBytes
                    TxErrors             = [uint64]$txErrors
                    TxDropped            = [uint64]$txDropped
                })

                continue
            }

            foreach ($detail in $slaveDetails) {
                $AllRows.Add([pscustomobject]@{
                    Cluster              = $clusterName
                    NodeID               = $node.nodeId
                    NodeIP               = $node.nodeIp
                    ChassisSerial        = $node.chassisSerial
                    Bond                 = $bondName
                    BondingMode          = $bondingMode
                    ActiveSlave          = $activeBondSlave
                    BondSlave            = $detail.name
                    SlaveInterfaceStatus = $detail.linkState
                    SlaveSpeed           = $detail.speed
                    SlotType             = (Get-FirstPropertyValue -InputObject $detail -PropertyNames @("slotType", "slot") -DefaultValue "")
                    MacAddress           = $detail.macAddr
                    RxPkts               = [uint64]$rxPkts
                    RxBytes              = [uint64]$rxBytes
                    RxErrors             = [uint64]$rxErrors
                    RxDropped            = [uint64]$rxDropped
                    TxPkts               = [uint64]$txPkts
                    TxBytes              = [uint64]$txBytes
                    TxErrors             = [uint64]$txErrors
                    TxDropped            = [uint64]$txDropped
                })
            }
        }
    }
}

# ==============================
# 3) Full interface detail dump
# ==============================
if ($AllRows.Count -eq 0) {
    Write-Host "No interface data found." -ForegroundColor Yellow
}
else {
    Write-Host "`nFull Interface Details:" -ForegroundColor Cyan
    Write-Host "=======================" -ForegroundColor Cyan

    $AllRows |
        Sort-Object Cluster, NodeID, Bond, BondSlave |
        Format-Table `
            Cluster, NodeID, NodeIP, Bond, BondingMode, ActiveSlave, BondSlave,
            SlaveInterfaceStatus, SlaveSpeed, RxPkts, RxBytes, RxErrors,
            RxDropped, TxPkts, TxBytes, TxErrors, TxDropped `
            -AutoSize
}

# ==========================================================
# 4) Per-cluster summary using one statistics row per bond
# ==========================================================
if ($AllRows.Count -gt 0) {
    $clusterSummaries = foreach ($group in ($AllRows | Group-Object Cluster | Sort-Object Name)) {
        $clusterName = $group.Name
        $rows        = @($group.Group)
        $bondStats   = @($BondRows | Where-Object Cluster -eq $clusterName)

        $downRows = @($rows | Where-Object {
            $_.SlaveInterfaceStatus -match 'down|error|disabled|unknown|not.?connected'
        })

        $upRows = @($rows | Where-Object {
            $_.SlaveInterfaceStatus -match '^(up|active|connected)$'
        })

        [pscustomobject]@{
            Cluster      = $clusterName
            Nodes        = @($rows.NodeID | Select-Object -Unique).Count
            Bonds        = @($bondStats | ForEach-Object { "$($_.NodeID)|$($_.Bond)" } | Select-Object -Unique).Count
            Interfaces   = @($rows | Where-Object { $_.BondSlave }).Count
            Up           = $upRows.Count
            Down         = $downRows.Count
            RxErrors     = [uint64](($bondStats | Measure-Object RxErrors -Sum).Sum)
            RxDropped    = [uint64](($bondStats | Measure-Object RxDropped -Sum).Sum)
            TxErrors     = [uint64](($bondStats | Measure-Object TxErrors -Sum).Sum)
            TxDropped    = [uint64](($bondStats | Measure-Object TxDropped -Sum).Sum)
            BondingModes = (@($bondStats.BondingMode | Where-Object { "$_" -ne "" } | Select-Object -Unique) -join ", ")
            Status       = if ($downRows.Count -gt 0) { "Down interface detected" } else { "Everything is up" }
        }
    }

    Write-Host "`nPer-Cluster Summary:" -ForegroundColor Green
    Write-Host "====================" -ForegroundColor Green

    $clusterSummaries |
        Format-Table Cluster, Nodes, Bonds, Interfaces, Up, Down, RxErrors,
            RxDropped, TxErrors, TxDropped, BondingModes, Status -AutoSize
}

# ===========================================
# 5) Explicit down-interface detailed view
# ===========================================
$DownRows = @($AllRows | Where-Object {
    $_.SlaveInterfaceStatus -match 'down|error|disabled|unknown|not.?connected'
})

if ($DownRows.Count -gt 0) {
    Write-Host "`nInterfaces Down Detected:" -ForegroundColor Red

    $DownRows |
        Sort-Object Cluster, NodeID, Bond, BondSlave |
        Format-Table Cluster, NodeID, NodeIP, Bond, BondingMode, ActiveSlave,
            BondSlave, SlaveInterfaceStatus, SlaveSpeed -AutoSize
}
elseif ($AllRows.Count -gt 0) {
    Write-Host "`nEverything is UP" -ForegroundColor Green
}

# ==========================================
# 6) Clusters that could not be queried
# ==========================================
if ($Failures.Count -gt 0) {
    Write-Host "`nClusters With API Failures:" -ForegroundColor Yellow
    $Failures | Sort-Object Cluster | Format-Table -AutoSize
}
