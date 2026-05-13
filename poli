# =====================================================================
# Cohesity Policy -> PG Retention Alignment Inventory
# Multi-Cluster | Helios | GET-only | PowerShell 5.1 compatible
#
# Console:
# - Policy Summary:
#   Cluster | Policy Name | Policy Retention | Expected Environment | PG Count
# - Exceptions Only:
#   Cluster | PG Name | Policy Name | Retention | Expected Env | Alignment | Issue
#
# CSV:
# - One full-detail CSV only:
#   Cluster | PGName | PolicyName | PolicyRetention | ExpectedEnvironment |
#   ReplicationTargetCluster | ReplicaRetention | LogRetention | Alignment | Issue
#
# Retention classification:
# - 35D / 35 days = PROD
# - 6M / 6 months = PROD
# - 7Y / 7YR / 7 years = PROD
# - 14D / 14 days = MOD/NONPROD
# - 7D / 7 days = DEV
#
# PG naming validation:
# - PROD policy        -> PG should contain PROD
# - MOD/NONPROD policy -> PG should contain MOD / NONPROD / NON-PROD / CAP
# - DEV policy         -> PG should contain DEV
# - LogShipping policy -> PG should contain LogShipping, case-insensitive
#
# Replication details:
# - CSV only
# - Pulled for ALL policies
# - From policy.remoteTargetPolicy.replicationTargets[]
# - ReplicaRetention = replicationTarget.retention.duration + retention.unit
# - LogRetention     = replicationTarget.logRetention.duration + logRetention.unit
#
# Default policies excluded:
# - Protect Once
# - Silver
# - Gold
# - Bronze
# =====================================================================

$ErrorActionPreference = "Stop"

# -------------------------------
# Folder + API key
# -------------------------------
$logDirectory = "X:\PowerShell\Data\Cohesity\PolicyPGAlignment"

if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory | Out-Null
}

try {
    $files = Get-ChildItem -Path $logDirectory -File -ErrorAction Stop

    if ($files.Count -gt 50) {
        $toDelete = $files | Sort-Object CreationTime | Select-Object -First ($files.Count - 50)
        if ($toDelete) {
            $toDelete | Remove-Item -Force -ErrorAction SilentlyContinue
        }
    }

    $threshold = (Get-Date).AddDays(-30)
    Get-ChildItem -Path $logDirectory -File -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $threshold } |
        Remove-Item -Force -ErrorAction SilentlyContinue
}
catch {}

$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

$baseUrl = "https://helios.cohesity.com"

$commonHeaders = @{
    "apiKey" = $apiKey
    "accept" = "application/json"
}

# -------------------------------
# PS 5.1 safe GET wrapper
# -------------------------------
function Invoke-HeliosGetJson {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $resp -or [string]::IsNullOrWhiteSpace($resp.Content)) {
        return $null
    }

    return ($resp.Content | ConvertFrom-Json)
}

# -------------------------------
# Helpers
# -------------------------------
function Get-FirstValue {
    param(
        [Parameter(Mandatory)]$Object,
        [Parameter(Mandatory)][string[]]$Names
    )

    foreach ($name in $Names) {
        if ($null -ne $Object.PSObject.Properties[$name]) {
            $value = $Object.$name
            if ($null -ne $value -and "$value".Trim() -ne "") {
                return $value
            }
        }
    }

    return $null
}

function ValueOrNA {
    param($Value)

    if ($null -eq $Value) {
        return "N/A"
    }

    $text = [string]$Value

    if ([string]::IsNullOrWhiteSpace($text)) {
        return "N/A"
    }

    return $text.Trim()
}

function Normalize-Name {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return ""
    }

    $n = $Name.Trim().ToUpper()

    # Normalize LogShipping variants.
    $n = $n -replace "LOG[\s_\-]*SHIPPING", "LOGSHIPPING"

    # Normalize NonProd variants.
    $n = $n -replace "NON[\s_\-]*PROD", "NONPROD"

    return $n
}

function Has-Marker {
    param(
        [string]$Name,
        [string]$Marker
    )

    $n = Normalize-Name $Name

    if ([string]::IsNullOrWhiteSpace($n)) {
        return $false
    }

    switch ($Marker) {
        "LOGSHIPPING" {
            return ($n -match 'LOGSHIPPING')
        }

        "NONPROD" {
            return ($n -match 'NONPROD')
        }

        "PROD" {
            # Prevent NONPROD from being treated as PROD.
            if ($n -match 'NONPROD') {
                return $false
            }

            return ($n -match 'PROD')
        }

        "DEV" {
            return ($n -match '(^|[^A-Z0-9])DEV([^A-Z0-9]|$)')
        }

        "MOD" {
            return ($n -match '(^|[^A-Z0-9])MOD([^A-Z0-9]|$)')
        }

        "CAP" {
            return ($n -match '(^|[^A-Z0-9])CAP([^A-Z0-9]|$)')
        }

        default {
            return $false
        }
    }
}

function Is-DefaultPolicy {
    param([string]$PolicyName)

    $n = Normalize-Name $PolicyName
    $compact = $n -replace '[^A-Z0-9]', ''

    return ($compact -in @(
        "PROTECTONCE",
        "SILVER",
        "GOLD",
        "BRONZE"
    ))
}

function Get-RetentionLabelFromName {
    param([string]$PolicyName)

    $n = Normalize-Name $PolicyName

    # Long-term patterns first so 7YR is never mistaken as 7D.
    if ($n -match '(^|[^0-9])7\s*(Y|YR|YRS|YEAR|YEARS)([^A-Z0-9]|$)') {
        return "7YR"
    }

    if ($n -match '(^|[^0-9])6\s*(M|MO|MOS|MONTH|MONTHS)([^A-Z0-9]|$)') {
        return "6M"
    }

    if ($n -match '(^|[^0-9])35\s*(D|DAY|DAYS)([^A-Z0-9]|$)') {
        return "35D"
    }

    if ($n -match '(^|[^0-9])14\s*(D|DAY|DAYS)([^A-Z0-9]|$)') {
        return "14D"
    }

    if ($n -match '(^|[^0-9])7\s*(D|DAY|DAYS)([^A-Z0-9]|$)') {
        return "7D"
    }

    # Fallback for bare retention numbers.
    if ($n -match '(^|[^0-9])35([^0-9]|$)') {
        return "35D"
    }

    if ($n -match '(^|[^0-9])14([^0-9]|$)') {
        return "14D"
    }

    if ($n -match '(^|[^0-9])7([^0-9]|$)') {
        return "7D"
    }

    return $null
}

function Convert-DaysToRetentionLabel {
    param([double]$Days)

    if ($Days -ge 34 -and $Days -le 36) {
        return "35D"
    }

    if ($Days -ge 13 -and $Days -le 15) {
        return "14D"
    }

    if ($Days -ge 6 -and $Days -le 8) {
        return "7D"
    }

    # 6 months commonly appears as 180/181/182/183 days.
    if ($Days -ge 179 -and $Days -le 184) {
        return "6M"
    }

    # 7 years commonly appears as 2555/2556/2557 days.
    if ($Days -ge 2554 -and $Days -le 2557) {
        return "7YR"
    }

    return "$([int][math]::Round($Days))D"
}

function Convert-RetentionToLabel {
    param(
        $Duration,
        $Unit
    )

    if ($null -eq $Duration) {
        return $null
    }

    [double]$d = 0

    if (-not [double]::TryParse([string]$Duration, [ref]$d)) {
        return $null
    }

    if ($d -le 0) {
        return $null
    }

    $u = (ValueOrNA $Unit).ToUpper()

    switch -Regex ($u) {
        "DAY" {
            return (Convert-DaysToRetentionLabel -Days $d)
        }

        "WEEK" {
            return (Convert-DaysToRetentionLabel -Days ($d * 7))
        }

        "MONTH" {
            if ($d -eq 6) {
                return "6M"
            }

            if ($d -eq 84) {
                return "7YR"
            }

            return "$([int][math]::Round($d))M"
        }

        "YEAR" {
            if ($d -eq 7) {
                return "7YR"
            }

            return "$([int][math]::Round($d))YR"
        }

        "HOUR" {
            return (Convert-DaysToRetentionLabel -Days ($d / 24))
        }

        "MINUTE" {
            return (Convert-DaysToRetentionLabel -Days ($d / 1440))
        }

        default {
            if ($d -in @(7, 14, 35)) {
                return "$([int]$d)D"
            }
        }
    }

    return $null
}

function Get-ExpectedEnvironment {
    param([string]$PolicyRetention)

    switch ($PolicyRetention) {
        "35D"  { return "PROD" }
        "6M"   { return "PROD" }
        "7YR"  { return "PROD" }
        "14D"  { return "MOD/NONPROD" }
        "7D"   { return "DEV" }
        default { return "UNKNOWN" }
    }
}

function Get-PolicyRetentionLabel {
    param($Policy)

    # Prefer standard policy name retention if present.
    $fromName = Get-RetentionLabelFromName -PolicyName $Policy.name
    if ($fromName) {
        return $fromName
    }

    # Primary regular retention.
    try {
        $r = $Policy.backupPolicy.regular.retention
        if ($r) {
            $label = Convert-RetentionToLabel -Duration $r.duration -Unit $r.unit
            if ($label) {
                return $label
            }
        }
    }
    catch {}

    # Full backup retention fallback.
    try {
        foreach ($fb in @($Policy.backupPolicy.regular.fullBackups)) {
            if ($fb.retention) {
                $label = Convert-RetentionToLabel -Duration $fb.retention.duration -Unit $fb.retention.unit
                if ($label) {
                    return $label
                }
            }
        }
    }
    catch {}

    # Extended retention fallback.
    try {
        foreach ($er in @($Policy.extendedRetention)) {
            if ($er.retention) {
                $label = Convert-RetentionToLabel -Duration $er.retention.duration -Unit $er.retention.unit
                if ($label) {
                    return $label
                }
            }
        }
    }
    catch {}

    # Common direct fields, if exposed by API version.
    foreach ($field in @("daysToKeep", "retentionDays", "duration")) {
        try {
            if ($null -ne $Policy.PSObject.Properties[$field]) {
                $label = Convert-RetentionToLabel -Duration $Policy.$field -Unit "Days"
                if ($label) {
                    return $label
                }
            }
        }
        catch {}
    }

    return "UNKNOWN"
}

function Format-RetentionText {
    param($Retention)

    if ($null -eq $Retention) {
        return "N/A"
    }

    $duration = ValueOrNA $Retention.duration
    $unit     = ValueOrNA $Retention.unit

    if ($duration -eq "N/A" -or $unit -eq "N/A") {
        return "N/A"
    }

    return "$duration $unit"
}

function Get-ReplicationTargetsForPolicy {
    param($Policy)

    $repTargets = @()

    try {
        if ($Policy.PSObject.Properties["remoteTargetPolicy"] -and
            $Policy.remoteTargetPolicy -and
            $Policy.remoteTargetPolicy.PSObject.Properties["replicationTargets"] -and
            $Policy.remoteTargetPolicy.replicationTargets) {

            $repTargets = @($Policy.remoteTargetPolicy.replicationTargets)
        }
    }
    catch {
        $repTargets = @()
    }

    if (-not $repTargets -or $repTargets.Count -eq 0) {
        return @(
            [pscustomobject]@{
                ReplicationTargetCluster = "N/A"
                ReplicaRetention         = "N/A"
                LogRetention             = "N/A"
            }
        )
    }

    $out = @()

    foreach ($repTarget in $repTargets) {

        $targetCluster    = "N/A"
        $replicaRetention = "N/A"
        $logRetention     = "N/A"

        # Target cluster
        try {
            if ($repTarget.PSObject.Properties["remoteTargetConfig"] -and
                $repTarget.remoteTargetConfig -and
                $repTarget.remoteTargetConfig.PSObject.Properties["clusterName"] -and
                $repTarget.remoteTargetConfig.clusterName) {

                $targetCluster = ValueOrNA $repTarget.remoteTargetConfig.clusterName
            }
        }
        catch {
            $targetCluster = "N/A"
        }

        # Regular replication retention
        try {
            if ($repTarget.PSObject.Properties["retention"] -and $repTarget.retention) {
                $replicaRetention = Format-RetentionText -Retention $repTarget.retention
            }
        }
        catch {
            $replicaRetention = "N/A"
        }

        # Log replication retention
        try {
            if ($repTarget.PSObject.Properties["logRetention"] -and $repTarget.logRetention) {
                $logRetention = Format-RetentionText -Retention $repTarget.logRetention
            }
        }
        catch {
            $logRetention = "N/A"
        }

        $out += [pscustomobject]@{
            ReplicationTargetCluster = $targetCluster
            ReplicaRetention         = $replicaRetention
            LogRetention             = $logRetention
        }
    }

    return @($out)
}

function Test-PGPolicyAlignment {
    param(
        [string]$PolicyName,
        [string]$PolicyRetention,
        [string]$ExpectedEnvironment,
        [string]$PGName
    )

    $policyHasLogShipping = Has-Marker -Name $PolicyName -Marker "LOGSHIPPING"
    $pgHasLogShipping     = Has-Marker -Name $PGName     -Marker "LOGSHIPPING"

    if ($policyHasLogShipping) {
        if ($pgHasLogShipping) {
            return [pscustomobject]@{
                Alignment = "OK"
                Issue     = "N/A"
            }
        }

        return [pscustomobject]@{
            Alignment = "REVIEW"
            Issue     = "PG LogShipping marker missing for LogShipping policy"
        }
    }

    if ($ExpectedEnvironment -eq "UNKNOWN") {
        return [pscustomobject]@{
            Alignment = "REVIEW"
            Issue     = "Policy retention could not be classified"
        }
    }

    $hasProd    = Has-Marker -Name $PGName -Marker "PROD"
    $hasNonProd = Has-Marker -Name $PGName -Marker "NONPROD"
    $hasMod     = Has-Marker -Name $PGName -Marker "MOD"
    $hasCap     = Has-Marker -Name $PGName -Marker "CAP"
    $hasDev     = Has-Marker -Name $PGName -Marker "DEV"

    if (-not $hasProd -and -not $hasNonProd -and -not $hasMod -and -not $hasCap -and -not $hasDev) {
        return [pscustomobject]@{
            Alignment = "REVIEW"
            Issue     = "PG environment marker is missing or unclear"
        }
    }

    switch ($ExpectedEnvironment) {
        "PROD" {
            if ($hasProd -and -not $hasNonProd -and -not $hasMod -and -not $hasCap -and -not $hasDev) {
                return [pscustomobject]@{
                    Alignment = "OK"
                    Issue     = "N/A"
                }
            }

            return [pscustomobject]@{
                Alignment = "NOT MATCHING"
                Issue     = "PG environment conflicts with policy retention"
            }
        }

        "MOD/NONPROD" {
            if (($hasNonProd -or $hasMod -or $hasCap) -and -not $hasProd -and -not $hasDev) {
                return [pscustomobject]@{
                    Alignment = "OK"
                    Issue     = "N/A"
                }
            }

            return [pscustomobject]@{
                Alignment = "NOT MATCHING"
                Issue     = "PG environment conflicts with policy retention"
            }
        }

        "DEV" {
            if ($hasDev -and -not $hasProd -and -not $hasNonProd -and -not $hasMod -and -not $hasCap) {
                return [pscustomobject]@{
                    Alignment = "OK"
                    Issue     = "N/A"
                }
            }

            return [pscustomobject]@{
                Alignment = "NOT MATCHING"
                Issue     = "PG environment conflicts with policy retention"
            }
        }

        default {
            return [pscustomobject]@{
                Alignment = "REVIEW"
                Issue     = "Policy retention could not be classified"
            }
        }
    }
}

function Shorten-Text {
    param(
        [string]$Text,
        [int]$Max
    )

    $v = ValueOrNA $Text

    if ($v.Length -le $Max) {
        return $v
    }

    return $v.Substring(0, $Max - 3) + "..."
}

function Format-FixedLine {
    param(
        [object[]]$Values,
        [int[]]$Widths
    )

    $out = ""

    for ($i = 0; $i -lt $Values.Count; $i++) {
        $w = $Widths[$i]
        $v = Shorten-Text -Text ([string]$Values[$i]) -Max $w
        $out += $v.PadRight($w)
    }

    return $out
}

# -------------------------------
# Get clusters
# -------------------------------
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   COHESITY POLICY -> PG ALIGNMENT INVENTORY" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Default policies excluded: Protect Once, Silver, Gold, Bronze" -ForegroundColor Gray
Write-Host "Replication details are exported to CSV for all policies when available." -ForegroundColor Gray

try {
    $cluJson = Invoke-HeliosGetJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

$json_clu = @()

if ($cluJson.cohesityClusters) {
    $json_clu = @($cluJson.cohesityClusters)
}
elseif ($cluJson.clusters) {
    $json_clu = @($cluJson.clusters)
}
elseif ($cluJson.clusterInfos) {
    $json_clu = @($cluJson.clusterInfos)
}
elseif ($cluJson.mcmInfo.clusterInfos) {
    $json_clu = @($cluJson.mcmInfo.clusterInfos)
}

if (-not $json_clu -or $json_clu.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$json_clu = @(
    $json_clu | Sort-Object {
        $n = Get-FirstValue $_ @("name", "clusterName", "displayName", "ClusterName", "Name")
        ValueOrNA $n
    }
)

# -------------------------------
# Main collection
# -------------------------------
$summaryRows = @()
$detailRows  = @()
$issueRows   = @()
$excludedDefaultRows = @()
$clusterConnectionIssues = @()

foreach ($cluster in $json_clu) {

    $clusterName = Get-FirstValue $cluster @("name", "clusterName", "displayName", "ClusterName", "Name")
    $clusterId   = Get-FirstValue $cluster @("clusterId", "id", "ClusterId", "Id")

    if (-not $clusterName) {
        $clusterName = "Unknown-$clusterId"
    }

    if (-not $clusterId) {
        $clusterConnectionIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Cluster ID missing"
        }
        continue
    }

    Write-Host "`nProcessing cluster: $clusterName" -ForegroundColor Cyan

    $headers = @{
        "apiKey" = $apiKey
        "accessClusterId" = [string]$clusterId
        "accept" = "application/json"
    }

    # Get policies.
    try {
        $policyJson = Invoke-HeliosGetJson -Uri "$baseUrl/v2/data-protect/policies" -Headers $headers

        if ($policyJson.policies) {
            $policies = @($policyJson.policies)
        }
        elseif ($policyJson -is [array]) {
            $policies = @($policyJson)
        }
        else {
            $policies = @()
        }
    }
    catch {
        $clusterConnectionIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Policy fetch failed: $($_.Exception.Message)"
        }

        Write-Host "Policy fetch failed for $clusterName : $($_.Exception.Message)" -ForegroundColor Yellow
        continue
    }

    # Get PGs.
    try {
        $pgUri = "$baseUrl/v2/data-protect/protection-groups?isDeleted=false&includeLastRunInfo=false"
        $pgJson = Invoke-HeliosGetJson -Uri $pgUri -Headers $headers

        if ($pgJson.protectionGroups) {
            $pgs = @($pgJson.protectionGroups)
        }
        elseif ($pgJson -is [array]) {
            $pgs = @($pgJson)
        }
        else {
            $pgs = @()
        }
    }
    catch {
        $clusterConnectionIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "PG fetch failed: $($_.Exception.Message)"
        }

        Write-Host "PG fetch failed for $clusterName : $($_.Exception.Message)" -ForegroundColor Yellow
        continue
    }

    $pgs = @($pgs | Where-Object { $_.isDeleted -ne $true })

    Write-Host "Policies returned: $($policies.Count) | PGs returned: $($pgs.Count)" -ForegroundColor Gray

    foreach ($policy in ($policies | Sort-Object name)) {

        $policyName = ValueOrNA $policy.name
        $policyId   = ValueOrNA $policy.id

        if ($policyId -eq "N/A") {
            continue
        }

        if (Is-DefaultPolicy -PolicyName $policyName) {
            $excludedDefaultRows += [pscustomobject]@{
                Cluster    = $clusterName
                PolicyName = $policyName
            }

            continue
        }

        $policyRetention     = Get-PolicyRetentionLabel -Policy $policy
        $expectedEnvironment = Get-ExpectedEnvironment -PolicyRetention $policyRetention
        $replicationTargets  = @(Get-ReplicationTargetsForPolicy -Policy $policy)

        $matchedPGs = @(
            $pgs |
                Where-Object { (ValueOrNA $_.policyId) -eq $policyId } |
                Sort-Object name
        )

        $summaryRows += [pscustomobject]@{
            Cluster             = $clusterName
            PolicyName          = $policyName
            PolicyRetention     = $policyRetention
            ExpectedEnvironment = $expectedEnvironment
            PGCount             = $matchedPGs.Count
        }

        if ($matchedPGs.Count -eq 0) {
            foreach ($rep in $replicationTargets) {
                $detailRow = [pscustomobject]@{
                    Cluster                  = $clusterName
                    PGName                   = "N/A"
                    PolicyName               = $policyName
                    PolicyRetention          = $policyRetention
                    ExpectedEnvironment      = $expectedEnvironment
                    ReplicationTargetCluster = $rep.ReplicationTargetCluster
                    ReplicaRetention         = $rep.ReplicaRetention
                    LogRetention             = $rep.LogRetention
                    Alignment                = "REVIEW"
                    Issue                    = "No PGs assigned to policy"
                }

                # CSV only. Do not show no-PG policies in console exceptions.
                $detailRows += $detailRow
            }

            continue
        }

        foreach ($pg in $matchedPGs) {

            $pgName = ValueOrNA $pg.name

            $result = Test-PGPolicyAlignment `
                -PolicyName $policyName `
                -PolicyRetention $policyRetention `
                -ExpectedEnvironment $expectedEnvironment `
                -PGName $pgName

            foreach ($rep in $replicationTargets) {
                $detailRow = [pscustomobject]@{
                    Cluster                  = $clusterName
                    PGName                   = $pgName
                    PolicyName               = $policyName
                    PolicyRetention          = $policyRetention
                    ExpectedEnvironment      = $expectedEnvironment
                    ReplicationTargetCluster = $rep.ReplicationTargetCluster
                    ReplicaRetention         = $rep.ReplicaRetention
                    LogRetention             = $rep.LogRetention
                    Alignment                = $result.Alignment
                    Issue                    = $result.Issue
                }

                $detailRows += $detailRow
            }

            if ($result.Alignment -ne "OK") {
                $issueRows += [pscustomobject]@{
                    Cluster             = $clusterName
                    PGName              = $pgName
                    PolicyName          = $policyName
                    PolicyRetention     = $policyRetention
                    ExpectedEnvironment = $expectedEnvironment
                    Alignment           = $result.Alignment
                    Issue               = $result.Issue
                }
            }
        }
    }
}

$summaryRows = @($summaryRows | Sort-Object Cluster, PolicyName)
$detailRows  = @($detailRows  | Sort-Object Cluster, PGName, PolicyName, ReplicationTargetCluster)
$issueRows   = @($issueRows   | Sort-Object Cluster, PGName, PolicyName)
$clusterConnectionIssues = @($clusterConnectionIssues | Sort-Object Cluster)

# -------------------------------
# Console summary
# -------------------------------
Write-Host "`n====================================================================================================" -ForegroundColor Cyan
Write-Host "POLICY SUMMARY" -ForegroundColor White
Write-Host "====================================================================================================" -ForegroundColor Cyan
Write-Host "Default policies excluded from review: Protect Once, Silver, Gold, Bronze" -ForegroundColor Gray
Write-Host "Excluded default policy count: $($excludedDefaultRows.Count)" -ForegroundColor Gray
Write-Host "Replication details are exported to CSV for all policies when available." -ForegroundColor Gray
Write-Host ""

$summaryWidths = @(24, 58, 18, 22, 10)

$summaryHeader = Format-FixedLine `
    -Values @("Cluster", "Policy Name", "Policy Retention", "Expected Environment", "PG Count") `
    -Widths $summaryWidths

Write-Host $summaryHeader -ForegroundColor White
Write-Host ("-" * 132)

foreach ($row in $summaryRows) {
    $line = Format-FixedLine `
        -Values @($row.Cluster, $row.PolicyName, $row.PolicyRetention, $row.ExpectedEnvironment, $row.PGCount) `
        -Widths $summaryWidths

    Write-Host $line
}

# -------------------------------
# Console exceptions only
# -------------------------------
Write-Host "`n====================================================================================================" -ForegroundColor Cyan
Write-Host "EXCEPTIONS ONLY - PGs THAT NEED NAMING OR POLICY REVIEW" -ForegroundColor White
Write-Host "====================================================================================================" -ForegroundColor Cyan

if ($issueRows.Count -eq 0) {
    Write-Host "No PG naming or policy-retention exceptions found." -ForegroundColor Green
}
else {
    $issueWidths = @(20, 48, 42, 16, 20, 16, 48)

    $issueHeader = Format-FixedLine `
        -Values @("Cluster", "PG Name", "Policy Name", "Retention", "Expected Env", "Alignment", "Issue") `
        -Widths $issueWidths

    Write-Host $issueHeader -ForegroundColor White
    Write-Host ("-" * 210)

    foreach ($row in $issueRows) {
        $line = Format-FixedLine `
            -Values @(
                $row.Cluster,
                $row.PGName,
                $row.PolicyName,
                $row.PolicyRetention,
                $row.ExpectedEnvironment,
                $row.Alignment,
                $row.Issue
            ) `
            -Widths $issueWidths

        Write-Host $line -ForegroundColor Yellow
    }
}

# -------------------------------
# Single CSV export
# -------------------------------
$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_Policy_PG_RetentionAlignment_$timestamp.csv"

$detailRows |
    Select-Object `
        Cluster,
        PGName,
        PolicyName,
        PolicyRetention,
        ExpectedEnvironment,
        ReplicationTargetCluster,
        ReplicaRetention,
        LogRetention,
        Alignment,
        Issue |
    Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

# -------------------------------
# Final summary
# -------------------------------
$actualPgCount = @(
    $detailRows |
        Where-Object { $_.PGName -ne "N/A" } |
        Select-Object Cluster,PGName,PolicyName -Unique
).Count

$okPgCount = @(
    $detailRows |
        Where-Object { $_.PGName -ne "N/A" -and $_.Alignment -eq "OK" } |
        Select-Object Cluster,PGName,PolicyName -Unique
).Count

$exceptionPgCount = @(
    $issueRows |
        Where-Object { $_.PGName -ne "N/A" } |
        Select-Object Cluster,PGName,PolicyName -Unique
).Count

Write-Host "`n==============================" -ForegroundColor Cyan
Write-Host "SUMMARY" -ForegroundColor White
Write-Host "==============================" -ForegroundColor Cyan
Write-Host "Policies reviewed          : $($summaryRows.Count)"
Write-Host "Protection Groups reviewed : $actualPgCount"
Write-Host "PGs OK                    : $okPgCount"
Write-Host "PG exceptions found        : $exceptionPgCount"
Write-Host "Default policies excluded  : $($excludedDefaultRows.Count)"

if ($clusterConnectionIssues.Count -gt 0) {
    Write-Host "Clusters with fetch issues : $($clusterConnectionIssues.Count)" -ForegroundColor Yellow
    foreach ($ci in $clusterConnectionIssues) {
        Write-Host (" - {0}: {1}" -f $ci.Cluster, $ci.Issue) -ForegroundColor Yellow
    }
}

Write-Host "CSV Output                 : $csvPath"
Write-Host "=============================="
Write-Host "Processing complete."
