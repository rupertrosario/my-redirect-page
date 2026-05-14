# =====================================================================
# Cohesity Policy Summary CSV
# Multi-Cluster | Helios | GET-only | PowerShell 5.1 compatible
#
# Purpose:
# - Export policy details similar to the Cohesity Policy Details UI
# - One CSV row per non-default policy
# - Excludes default policies: Protect Once, Silver, Gold, Bronze
# - Includes PGCount only, not PG names
# - No JSON columns
# - No POST / PUT / PATCH / DELETE
#
# Uses:
# - GET /v2/mcm/cluster-mgmt/info
# - GET /v2/data-protect/policies
# - GET /v2/data-protect/protection-groups?isDeleted=false&includeLastRunInfo=false
#
# CSV Columns:
# Cluster, PolicyName, Backup, PeriodicFullBackup, QuietTimes,
# RetryOptions, LogBackup, ReplicationTargets, PGCount, OtherPolicyDetails
# =====================================================================

$ErrorActionPreference = "Stop"

# -------------------------------
# Config
# -------------------------------
$baseUrl      = "https://helios.cohesity.com"
$apikeypath   = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$logDirectory = "X:\PowerShell\Data\Cohesity\PolicyInventory"

if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

$commonHeaders = @{
    "apiKey" = $apiKey
    "accept" = "application/json"
}

# -------------------------------
# GET wrapper
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
# Basic helpers
# -------------------------------
function ValueOrNA {
    param($Value)

    if ($null -eq $Value) {
        return "N/A"
    }

    if ($Value -is [array]) {
        $items = @(
            $Value | Where-Object {
                $null -ne $_ -and -not [string]::IsNullOrWhiteSpace([string]$_)
            }
        )

        if ($items.Count -eq 0) {
            return "N/A"
        }

        return ($items -join ", ")
    }

    $text = [string]$Value

    if ([string]::IsNullOrWhiteSpace($text)) {
        return "N/A"
    }

    return $text.Trim()
}

function Get-FirstValue {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return "N/A"
    }

    foreach ($name in $Names) {
        if ($null -ne $Object.PSObject.Properties[$name]) {
            $v = ValueOrNA $Object.$name
            if ($v -ne "N/A") {
                return $v
            }
        }
    }

    return "N/A"
}

function Normalize-Name {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return ""
    }

    $n = $Name.Trim().ToUpper()
    $n = $n -replace "LOG[\s_\-]*SHIPPING", "LOGSHIPPING"
    $n = $n -replace "NON[\s_\-]*PROD", "NONPROD"

    return $n
}

function Is-DefaultPolicy {
    param([string]$PolicyName)

    $compact = (Normalize-Name $PolicyName) -replace '[^A-Z0-9]', ''

    return ($compact -in @(
        "PROTECTONCE",
        "SILVER",
        "GOLD",
        "BRONZE"
    ))
}

# -------------------------------
# Formatting helpers
# -------------------------------
function Format-Duration {
    param(
        $Duration,
        $Unit
    )

    $duration = ValueOrNA $Duration
    $unit     = ValueOrNA $Unit

    if ($duration -eq "N/A" -or $unit -eq "N/A" -or $duration -eq "0") {
        return "N/A"
    }

    [double]$d = 0
    if (-not [double]::TryParse([string]$duration, [ref]$d)) {
        return "$duration $unit"
    }

    $unitText = $unit.ToLower()

    if ($unitText -match "minute") {
        if (($d % 60) -eq 0 -and $d -ge 60) {
            $h = [int]($d / 60)
            if ($h -eq 1) { return "1 hour" }
            return "$h hours"
        }

        if ($d -eq 1) { return "1 minute" }
        return "$([int]$d) minutes"
    }

    if ($unitText -match "hour") {
        if ($d -eq 1) { return "1 hour" }
        return "$([int]$d) hours"
    }

    if ($unitText -match "day") {
        if (($d % 7) -eq 0 -and $d -le 28) {
            $w = [int]($d / 7)
            if ($w -eq 1) { return "1 week" }
            return "$w weeks"
        }

        if ($d -eq 1) { return "1 day" }
        return "$([int]$d) days"
    }

    if ($unitText -match "week") {
        if ($d -eq 1) { return "1 week" }
        return "$([int]$d) weeks"
    }

    if ($unitText -match "month") {
        if ($d -eq 1) { return "1 month" }
        return "$([int]$d) months"
    }

    if ($unitText -match "year") {
        if ($d -eq 1) { return "1 year" }
        return "$([int]$d) years"
    }

    if ($unitText -match "run") {
        if ($d -eq 1) { return "1 run" }
        return "$([int]$d) runs"
    }

    return "$duration $unit"
}

function Format-Retention {
    param($Retention)

    if ($null -eq $Retention) {
        return "N/A"
    }

    $mainRetention = Format-Duration -Duration $Retention.duration -Unit $Retention.unit
    $parts = @()

    if ($mainRetention -ne "N/A") {
        $parts += $mainRetention
    }

    if ($Retention.dataLockConfig) {
        $dl = $Retention.dataLockConfig

        $dlDuration = Format-Duration -Duration $dl.duration -Unit $dl.unit
        $dlMode     = ValueOrNA $dl.mode
        $worm       = ValueOrNA $dl.enableWormOnExternalTarget

        if ($dlDuration -ne "N/A") {
            $parts += "DataLock $dlDuration"
        }
        elseif ($dlMode -ne "N/A") {
            $parts += "DataLock $dlMode"
        }

        if ($worm -eq "True") {
            $parts += "External WORM"
        }
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return ($parts -join "; ")
}

function Format-Schedule {
    param($Schedule)

    if ($null -eq $Schedule) {
        return "N/A"
    }

    if ($Schedule.minuteSchedule -and $Schedule.minuteSchedule.frequency -ne $null -and "$($Schedule.minuteSchedule.frequency)" -ne "0") {
        $v = Format-Duration -Duration $Schedule.minuteSchedule.frequency -Unit "Minutes"
        return "Every $v"
    }

    if ($Schedule.hourSchedule -and $Schedule.hourSchedule.frequency -ne $null -and "$($Schedule.hourSchedule.frequency)" -ne "0") {
        $v = Format-Duration -Duration $Schedule.hourSchedule.frequency -Unit "Hours"
        return "Every $v"
    }

    if ($Schedule.daySchedule -and $Schedule.daySchedule.frequency -ne $null -and "$($Schedule.daySchedule.frequency)" -ne "0") {
        $freq = [int]$Schedule.daySchedule.frequency

        if ($freq -eq 1) {
            return "Every day"
        }

        return "Every $freq days"
    }

    if ($Schedule.weekSchedule -and $Schedule.weekSchedule.dayOfWeek) {
        $days = @($Schedule.weekSchedule.dayOfWeek)

        if ($days.Count -eq 7) {
            return "Every week on Sun-Sat"
        }

        return "Every week on $($days -join ', ')"
    }

    if ($Schedule.monthSchedule) {
        $m = $Schedule.monthSchedule

        if ($m.dayOfMonth -ne $null -and "$($m.dayOfMonth)" -ne "0") {
            return "Every month on day $($m.dayOfMonth)"
        }

        if ($m.weekOfMonth -and $m.dayOfWeek) {
            return "Every month on $($m.weekOfMonth) $((@($m.dayOfWeek)) -join ', ')"
        }
    }

    if ($Schedule.yearSchedule) {
        $y = $Schedule.yearSchedule

        if ($y.monthDay -and
            $y.monthDay.month -ne $null -and "$($y.monthDay.month)" -ne "0" -and
            $y.monthDay.dayOfTheMonth -ne $null -and "$($y.monthDay.dayOfTheMonth)" -ne "0") {
            return "Every year on Month=$($y.monthDay.month), Day=$($y.monthDay.dayOfTheMonth)"
        }

        if ($y.dayOfYear) {
            return "Every year $($y.dayOfYear)"
        }
    }

    if ($Schedule.frequency -ne $null -and "$($Schedule.frequency)" -ne "0") {
        $unit = ValueOrNA $Schedule.unit

        if ($unit -eq "Runs") {
            if ([int]$Schedule.frequency -eq 1) {
                return "Every run"
            }

            return "Every $($Schedule.frequency) runs"
        }

        if ($unit -ne "N/A") {
            return "Every $($Schedule.frequency) $unit"
        }
    }

    $unitOnly = ValueOrNA $Schedule.unit

    if ($unitOnly -ne "N/A") {
        return "Unit=$unitOnly"
    }

    return "N/A"
}

function Format-BackupSummary {
    param($Policy)

    $schedule  = Format-Schedule $Policy.backupPolicy.regular.incremental.schedule
    $retention = Format-Retention $Policy.backupPolicy.regular.retention

    $parts = @()

    if ($schedule -ne "N/A") {
        $parts += $schedule
    }

    if ($retention -ne "N/A") {
        $parts += "Retain $retention"
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return ($parts -join "; ")
}

function Format-PeriodicFullBackup {
    param($Policy)

    $items = @()

    $fullSchedule = Format-Schedule $Policy.backupPolicy.regular.full.schedule
    if ($fullSchedule -ne "N/A") {
        $items += $fullSchedule
    }

    foreach ($fb in @($Policy.backupPolicy.regular.fullBackups)) {
        if ($null -eq $fb) { continue }

        $schedule  = Format-Schedule $fb.schedule
        $retention = Format-Retention $fb.retention

        $parts = @()

        if ($schedule -ne "N/A") {
            $parts += $schedule
        }

        if ($retention -ne "N/A") {
            $parts += "Retain $retention"
        }

        if ($parts.Count -gt 0) {
            $items += ($parts -join "; ")
        }
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return ($items -join " | ")
}

function Format-QuietTimes {
    param($BlackoutWindow)

    if (-not $BlackoutWindow) {
        return "N/A"
    }

    $items = @()

    foreach ($b in @($BlackoutWindow)) {
        $day = ValueOrNA $b.day
        $start = "N/A"
        $end = "N/A"

        if ($b.startTime) {
            $sh = ValueOrNA $b.startTime.hour
            $sm = ValueOrNA $b.startTime.minute

            if ($sh -ne "N/A" -and $sm -ne "N/A") {
                $start = "{0}:{1:00}" -f [int]$sh, [int]$sm
            }
        }

        if ($b.endTime) {
            $eh = ValueOrNA $b.endTime.hour
            $em = ValueOrNA $b.endTime.minute

            if ($eh -ne "N/A" -and $em -ne "N/A") {
                $end = "{0}:{1:00}" -f [int]$eh, [int]$em
            }
        }

        $parts = @()

        if ($start -ne "N/A" -and $end -ne "N/A") {
            $parts += "$start-$end"
        }

        if ($day -ne "N/A") {
            $parts += $day
        }

        if ($parts.Count -gt 0) {
            $items += ($parts -join " ")
        }
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return ($items -join " | ")
}

function Format-RetryOptions {
    param($RetryOptions)

    if ($null -eq $RetryOptions) {
        return "N/A"
    }

    $retries  = ValueOrNA $RetryOptions.retries
    $interval = ValueOrNA $RetryOptions.retryIntervalMins

    if (($retries -eq "N/A" -or $retries -eq "0") -and ($interval -eq "N/A" -or $interval -eq "0")) {
        return "Do not retry on error"
    }

    $parts = @()

    if ($retries -ne "N/A" -and $retries -ne "0") {
        $parts += "Retry $retries times"
    }

    if ($interval -ne "N/A" -and $interval -ne "0") {
        $parts += "$interval minutes apart"
    }

    if ($parts.Count -eq 0) {
        return "Do not retry on error"
    }

    return ($parts -join "; ")
}

function Format-LogBackup {
    param($Policy)

    $schedule  = Format-Schedule $Policy.backupPolicy.log.schedule
    $retention = Format-Retention $Policy.backupPolicy.log.retention

    $parts = @()

    if ($schedule -ne "N/A") {
        $parts += $schedule
    }

    if ($retention -ne "N/A") {
        $parts += "Retain $retention"
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return ($parts -join "; ")
}

function Format-ReplicationTargets {
    param($Targets)

    if (-not $Targets) {
        return "N/A"
    }

    $items = @()

    foreach ($t in @($Targets)) {
        $targetName = ValueOrNA $t.remoteTargetConfig.clusterName

        if ($targetName -eq "N/A") {
            $targetName = ValueOrNA $t.awsTargetConfig.name
        }

        if ($targetName -eq "N/A") {
            $targetName = ValueOrNA $t.azureTargetConfig.name
        }

        $schedule     = Format-Schedule $t.schedule
        $repRetention = Format-Retention $t.retention
        $logRetention = Format-Retention $t.logRetention

        $parts = @()

        if ($targetName -ne "N/A") {
            $parts += "Replicate to $targetName"
        }

        if ($schedule -ne "N/A") {
            $parts += $schedule
        }

        if ($repRetention -ne "N/A") {
            $parts += "Retain $repRetention"
        }

        if ($logRetention -ne "N/A") {
            $parts += "Log Retain $logRetention"
        }

        if ($parts.Count -gt 0) {
            $items += ($parts -join "; ")
        }
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return ($items -join " | ")
}

# -------------------------------
# Other details safety column
# -------------------------------
function Format-Bmr {
    param($Policy)

    $schedule  = Format-Schedule $Policy.backupPolicy.bmr.schedule
    $retention = Format-Retention $Policy.backupPolicy.bmr.retention

    $parts = @()

    if ($schedule -ne "N/A") {
        $parts += $schedule
    }

    if ($retention -ne "N/A") {
        $parts += "Retain $retention"
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return "BMR: $($parts -join '; ')"
}

function Format-Cdp {
    param($Policy)

    $retention = Format-Retention $Policy.backupPolicy.cdp.retention

    if ($retention -eq "N/A") {
        return "N/A"
    }

    return "CDP: Retain $retention"
}

function Format-StorageArraySnapshot {
    param($Policy)

    $schedule  = Format-Schedule $Policy.backupPolicy.storageArraySnapshot.schedule
    $retention = Format-Retention $Policy.backupPolicy.storageArraySnapshot.retention

    $parts = @()

    if ($schedule -ne "N/A") {
        $parts += $schedule
    }

    if ($retention -ne "N/A") {
        $parts += "Retain $retention"
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return "Storage Snapshot: $($parts -join '; ')"
}

function Format-ExtendedRetention {
    param($ExtendedRetention)

    if (-not $ExtendedRetention) {
        return "N/A"
    }

    $items = @()

    foreach ($er in @($ExtendedRetention)) {
        $schedule  = Format-Schedule $er.schedule
        $retention = Format-Retention $er.retention
        $runType   = ValueOrNA $er.runType

        $parts = @()

        if ($runType -ne "N/A") {
            $parts += $runType
        }

        if ($schedule -ne "N/A") {
            $parts += $schedule
        }

        if ($retention -ne "N/A") {
            $parts += "Retain $retention"
        }

        if ($parts.Count -gt 0) {
            $items += ($parts -join "; ")
        }
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return "Extended Retention: $($items -join ' | ')"
}

function Format-PrimaryBackupTarget {
    param($Policy)

    $target = $Policy.backupPolicy.regular.primaryBackupTarget

    if ($null -eq $target) {
        return "N/A"
    }

    $parts = @()

    $targetType = ValueOrNA $target.targetType
    $useDefault = ValueOrNA $target.useDefaultBackupTarget
    $archiveTarget = ValueOrNA $target.archivalTargetSettings.targetName

    if ($targetType -ne "N/A") {
        $parts += "TargetType=$targetType"
    }

    if ($useDefault -ne "N/A") {
        $parts += "UseDefault=$useDefault"
    }

    if ($archiveTarget -ne "N/A") {
        $parts += "ArchivalTarget=$archiveTarget"
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return "Primary Target: $($parts -join '; ')"
}

function Format-RunTimeouts {
    param($Policy)

    $runTimeouts = $Policy.backupPolicy.runTimeouts

    if (-not $runTimeouts) {
        return "N/A"
    }

    $items = @()

    foreach ($rt in @($runTimeouts)) {
        $backupType = ValueOrNA $rt.backupType
        $timeout    = ValueOrNA $rt.timeoutMins

        if ($backupType -eq "N/A" -and ($timeout -eq "N/A" -or $timeout -eq "0")) {
            continue
        }

        $parts = @()

        if ($backupType -ne "N/A") {
            $parts += "BackupType=$backupType"
        }

        if ($timeout -ne "N/A" -and $timeout -ne "0") {
            $parts += "Timeout=$timeout minutes"
        }

        if ($parts.Count -gt 0) {
            $items += ($parts -join "; ")
        }
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return "Run Timeout: $($items -join ' | ')"
}

function Format-IndexingPolicy {
    param($Policy)

    $idx = $Policy.rpoPolicySettings.indexingPolicy

    if ($null -eq $idx) {
        return "N/A"
    }

    $parts = @()

    $enabled = ValueOrNA $idx.enableIndexing
    if ($enabled -ne "N/A") {
        $parts += "Enabled=$enabled"
    }

    if ($idx.includePaths) {
        $parts += "Include=$((@($idx.includePaths)) -join ', ')"
    }

    if ($idx.excludePaths) {
        $parts += "Exclude=$((@($idx.excludePaths)) -join ', ')"
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return "Indexing: $($parts -join '; ')"
}

function Format-AlertingPolicy {
    param($Policy)

    $alert = $Policy.rpoPolicySettings.alertingPolicy

    if ($null -eq $alert) {
        return "N/A"
    }

    $parts = @()

    if ($alert.backupRunStatus) {
        $parts += "RunStatus=$((@($alert.backupRunStatus)) -join ', ')"
    }

    foreach ($field in @(
        "raiseObjectLevelFailureAlert",
        "raiseObjectLevelFailureAlertAfterLastAttempt",
        "raiseObjectLevelFailureAlertAfterEachAttempt"
    )) {
        if ($null -ne $alert.PSObject.Properties[$field]) {
            $parts += "$field=$(ValueOrNA $alert.$field)"
        }
    }

    if ($alert.alertTargets) {
        $targets = @()

        foreach ($t in @($alert.alertTargets)) {
            $email = ValueOrNA $t.emailAddress
            $type  = ValueOrNA $t.recipientType

            $tp = @()

            if ($email -ne "N/A") {
                $tp += "Email=$email"
            }

            if ($type -ne "N/A") {
                $tp += "Type=$type"
            }

            if ($tp.Count -gt 0) {
                $targets += ($tp -join "; ")
            }
        }

        if ($targets.Count -gt 0) {
            $parts += "Targets=$($targets -join ' | ')"
        }
    }

    if ($parts.Count -eq 0) {
        return "N/A"
    }

    return "Alerting: $($parts -join '; ')"
}

function Format-EnvBackupParams {
    param($Policy)

    $env = $Policy.rpoPolicySettings.envBackupParams

    if ($null -eq $env) {
        return "N/A"
    }

    $items = @()

    if ($env.sqlParams) {
        $s = $env.sqlParams
        $parts = @()

        foreach ($field in @(
            "userDbBackupPreferenceType",
            "backupSystemDbs",
            "useAagPreferencesFromServer",
            "aagBackupPreferenceType",
            "fullBackupsCopyOnly",
            "logBackupNumStreams"
        )) {
            if ($null -ne $s.PSObject.Properties[$field]) {
                $parts += "$field=$(ValueOrNA $s.$field)"
            }
        }

        if ($parts.Count -gt 0) {
            $items += "SQL: $($parts -join '; ')"
        }
    }

    if ($env.oracleParams) {
        $o = $env.oracleParams
        $parts = @()

        if ($null -ne $o.PSObject.Properties["persistMountpoints"]) {
            $parts += "PersistMountpoints=$(ValueOrNA $o.persistMountpoints)"
        }

        if ($o.vlanParams) {
            $vlanId = ValueOrNA $o.vlanParams.vlanId
            $iface  = ValueOrNA $o.vlanParams.interfaceName
            $disable = ValueOrNA $o.vlanParams.disableVlan

            if ($vlanId -ne "N/A" -and $vlanId -ne "0") { $parts += "VlanId=$vlanId" }
            if ($iface -ne "N/A") { $parts += "Interface=$iface" }
            if ($disable -ne "N/A") { $parts += "DisableVlan=$disable" }
        }

        if ($parts.Count -gt 0) {
            $items += "Oracle: $($parts -join '; ')"
        }
    }

    if ($env.vmwareParams) {
        $v = $env.vmwareParams
        $parts = @()

        foreach ($field in @("fallbackToCrashConsistent", "skipPhysicalRdmDisks")) {
            if ($null -ne $v.PSObject.Properties[$field]) {
                $parts += "$field=$(ValueOrNA $v.$field)"
            }
        }

        if ($parts.Count -gt 0) {
            $items += "VMware: $($parts -join '; ')"
        }
    }

    if ($env.hypervParams) {
        $h = $env.hypervParams
        $parts = @()

        foreach ($field in @("fallbackToCrashConsistent", "protectionType")) {
            if ($null -ne $h.PSObject.Properties[$field]) {
                $parts += "$field=$(ValueOrNA $h.$field)"
            }
        }

        if ($parts.Count -gt 0) {
            $items += "HyperV: $($parts -join '; ')"
        }
    }

    if ($env.nasParams) {
        $n = $env.nasParams
        $parts = @()

        if ($null -ne $n.PSObject.Properties["includeAllFiles"]) {
            $parts += "includeAllFiles=$(ValueOrNA $n.includeAllFiles)"
        }

        if ($n.target) {
            $view = ValueOrNA $n.target.viewName
            $path = ValueOrNA $n.target.mountPath

            if ($view -ne "N/A") { $parts += "View=$view" }
            if ($path -ne "N/A") { $parts += "MountPath=$path" }
        }

        if ($parts.Count -gt 0) {
            $items += "NAS: $($parts -join '; ')"
        }
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return "Env Params: $($items -join ' | ')"
}

function Format-OtherPolicyDetails {
    param($Policy)

    $items = @()

    foreach ($v in @(
        (Format-Bmr $Policy),
        (Format-Cdp $Policy),
        (Format-StorageArraySnapshot $Policy),
        (Format-ExtendedRetention $Policy.extendedRetention),
        (Format-PrimaryBackupTarget $Policy),
        (Format-RunTimeouts $Policy),
        (Format-IndexingPolicy $Policy),
        (Format-AlertingPolicy $Policy),
        (Format-EnvBackupParams $Policy)
    )) {
        if ($v -ne "N/A") {
            $items += $v
        }
    }

    $qos = ValueOrNA $Policy.rpoPolicySettings.backupQosPrincipal
    if ($qos -ne "N/A") {
        $items += "Backup QoS: $qos"
    }

    $skip = ValueOrNA $Policy.skipIntervalMins
    if ($skip -ne "N/A" -and $skip -ne "0") {
        $items += "Skip Interval: $skip minutes"
    }

    if ($items.Count -eq 0) {
        return "N/A"
    }

    return ($items -join " || ")
}

# -------------------------------
# Get clusters
# -------------------------------
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   COHESITY POLICY SUMMARY CSV" -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Output: CSV only" -ForegroundColor Gray

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
$rows = @()
$clusterIssues = @()
$totalPoliciesExported = 0
$skippedDefaultPolicyCount = 0

foreach ($cluster in $json_clu) {

    $clusterName = Get-FirstValue $cluster @("name", "clusterName", "displayName", "ClusterName", "Name")
    $clusterId   = Get-FirstValue $cluster @("clusterId", "id", "ClusterId", "Id")

    if ($clusterName -eq "N/A") {
        $clusterName = "Unknown"
    }

    if ($clusterId -eq "N/A") {
        $clusterIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Cluster ID missing"
        }
        continue
    }

    Write-Host "Processing cluster: $clusterName" -ForegroundColor Cyan

    $headers = @{
        "apiKey" = $apiKey
        "accessClusterId" = [string]$clusterId
        "accept" = "application/json"
    }

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
        $clusterIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Policy fetch failed: $($_.Exception.Message)"
        }
        continue
    }

    try {
        $pgJson = Invoke-HeliosGetJson -Uri "$baseUrl/v2/data-protect/protection-groups?isDeleted=false&includeLastRunInfo=false" -Headers $headers

        if ($pgJson.protectionGroups) {
            $pgs = @($pgJson.protectionGroups | Where-Object { $_.isDeleted -ne $true })
        }
        elseif ($pgJson -is [array]) {
            $pgs = @($pgJson | Where-Object { $_.isDeleted -ne $true })
        }
        else {
            $pgs = @()
        }
    }
    catch {
        $clusterIssues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Protection Group fetch failed: $($_.Exception.Message)"
        }
        $pgs = @()
    }

    foreach ($policy in ($policies | Sort-Object name)) {

        $policyName = ValueOrNA $policy.name

        if (Is-DefaultPolicy -PolicyName $policyName) {
            $skippedDefaultPolicyCount++
            continue
        }

        $policyId = ValueOrNA $policy.id

        if ($policyId -eq "N/A") {
            $pgCount = 0
        }
        else {
            $pgCount = @(
                $pgs | Where-Object {
                    (ValueOrNA $_.policyId) -eq $policyId
                }
            ).Count
        }

        $row = [pscustomobject][ordered]@{
            Cluster             = $clusterName
            PolicyName          = $policyName
            Backup              = Format-BackupSummary $policy
            PeriodicFullBackup  = Format-PeriodicFullBackup $policy
            QuietTimes          = Format-QuietTimes $policy.blackoutWindow
            RetryOptions        = Format-RetryOptions $policy.retryOptions
            LogBackup           = Format-LogBackup $policy
            ReplicationTargets  = Format-ReplicationTargets $policy.remoteTargetPolicy.replicationTargets
            PGCount             = $pgCount
            OtherPolicyDetails  = Format-OtherPolicyDetails $policy
        }

        $rows += $row
        $totalPoliciesExported++
    }
}

# -------------------------------
# Export CSV
# -------------------------------
$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path $logDirectory "Cohesity_Policy_Summary_$timestamp.csv"

$rows |
    Sort-Object Cluster, PolicyName |
    Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

# -------------------------------
# Console summary
# -------------------------------
Write-Host "`n==============================" -ForegroundColor Cyan
Write-Host "POLICY SUMMARY" -ForegroundColor White
Write-Host "==============================" -ForegroundColor Cyan
Write-Host "Clusters discovered        : $($json_clu.Count)"
Write-Host "Policies exported          : $totalPoliciesExported"
Write-Host "Default policies excluded  : $skippedDefaultPolicyCount"
Write-Host "CSV output                 : $csvPath"

if ($clusterIssues.Count -gt 0) {
    Write-Host "Cluster fetch issues       : $($clusterIssues.Count)" -ForegroundColor Yellow

    foreach ($ci in $clusterIssues) {
        Write-Host (" - {0}: {1}" -f $ci.Cluster, $ci.Issue) -ForegroundColor Yellow
    }
}

Write-Host "=============================="
Write-Host "Processing complete."
