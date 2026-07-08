<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
GET-only Cohesity Helios collector for backup failure incident updates.
The collector works at object level where Cohesity returns object details.
A failed object is suppressed from active failures when the same object has a later successful backup.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$LegacyFailureOutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailures",
    [string]$HelperPath = ("X:\PowerShell\Cohesity_API_Scripts\Common\" + "Api" + "KeyAesHelper.ps1"),
    [string]$EncryptedFile = ("X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_" + "api" + "key.enc"),
    [string]$ClusterName = "",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = "",
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = "",
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35,
    [int]$RequestTimeoutSec = 60
)

$ErrorActionPreference = "Stop"
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:CsvColumns = @("IncidentNumber","WindowKey","Status","Cluster","Environment","ProtectionGroup","Host","ObjectName","ObjectType","RunType","FirstFailedET","LastFailedET","ClearedET","LastSeenET","LatestRunStatus","ConsecutiveFailureCount","Message","ObjectKey","ClusterId","ProtectionGroupId","EnvironmentFilter","FailedRunKeys")
$script:LifecycleColumns = @("Cluster","ProtectionGroup","Environment","Host","ObjectName","ObjectType","RunType","Status","OldestFailedET","NewestFailedET","LatestSuccessET","FailureRuns","Message")
$script:FailureColumns = @("Cluster","ProtectionGroup","Environment","Host","ObjectName","ObjectType","RunType","Status","OldestFailedET","NewestFailedET","LatestSuccessET","FailureRuns","Message")
$script:SuccessColumns = @("Cluster","ProtectionGroup","Environment","RunType","LatestSuccessET")

function Clean($Value) {
    if ($null -eq $Value) { return "" }
    if ($Value -is [array]) { $Value = $Value -join " | " }
    return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim()
}

function As-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function Get-Prop($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
    if ($Object -is [hashtable]) {
        if ($Object.ContainsKey($Name)) { return $Object[$Name] }
        return $Default
    }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { return $p.Value }
    return $Default
}

function Set-ObjProp($Object, [string]$Name, $Value) {
    if ($null -eq $Object) { return }
    $p = $Object.PSObject.Properties[$Name]
    if ($p) { $Object.$Name = $Value }
    else { $Object | Add-Member -MemberType NoteProperty -Name $Name -Value $Value -Force }
}

function Add-RunWarning([string]$Message) {
    $m = Clean $Message
    if ($m) {
        $script:Warnings.Add($m) | Out-Null
        Write-Warning $m
    }
}

function To-Int64($Value) {
    try {
        $s = Clean $Value
        if (!$s) { return [int64]0 }
        return [int64]$s
    } catch {
        return [int64]0
    }
}

function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Get-NowEtDate {
    [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
}

function Get-NowEtText {
    (Get-NowEtDate).ToString("yyyy-MM-dd HH:mm:ss")
}

function Convert-UsecsToEtText($Usecs) {
    $u = To-Int64 $Usecs
    if ($u -le 0) { return "" }
    try {
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
    } catch {
        return ""
    }
}

function Convert-EtToUsecs([datetime]$EtDate) {
    $utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtDate, [DateTimeKind]::Unspecified), $script:EtZone)
    [int64](([DateTimeOffset]::new($utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000)
}

function Parse-EtTextToDate([string]$Text) {
    $t = Clean $Text
    if (!$t) { return $null }
    $formats = @("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd H:mm:ss", "M/d/yyyy h:mm:ss tt", "M/d/yyyy H:mm:ss", "yyyy-MM-ddTHH:mm:ss")
    foreach ($fmt in $formats) {
        try { return [datetime]::ParseExact($t, $fmt, [Globalization.CultureInfo]::InvariantCulture) } catch {}
    }
    try { return [datetime]::Parse($t) } catch { return $null }
}

function Convert-EtTextToUsecs([string]$Text) {
    $dt = Parse-EtTextToDate $Text
    if ($null -eq $dt) { return 0 }
    Convert-EtToUsecs $dt
}

function Date-Sort($Value) {
    $d = Parse-EtTextToDate (Clean $Value)
    if ($d) { return $d.ToString("yyyy-MM-dd HH:mm:ss") }
    $t = Clean $Value
    if ($t) { return $t }
    return "0000-00-00 00:00:00"
}

function Read-Json([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    try { $raw | ConvertFrom-Json } catch { return $null }
}

function Write-Json($Object, [string]$Path) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Write-Csv($Rows, [string]$Path, [string[]]$Columns) {
    $dir = Split-Path $Path -Parent
    if (!(Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
    $list = @($Rows)
    if ($list.Count -eq 0) {
        ($Columns -join ",") | Set-Content -Path $Path -Encoding UTF8
    } else {
        $list | Select-Object -Property $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec $RequestTimeoutSec
    } else {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $RequestTimeoutSec
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    $r.Content | ConvertFrom-Json
}

function Get-CohesityApiKey {
    if (!(Test-Path $HelperPath)) { throw "Missing API key helper: $HelperPath" }
    if (!(Test-Path $EncryptedFile)) { throw "Missing encrypted key file: $EncryptedFile" }
    . $HelperPath
    $key = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key is blank from AES helper." }
    $key.Trim()
}

function Get-ComputeWindow {
    $nowEt = Get-NowEtDate
    if ($nowEt.Hour -lt 18) { $start = $nowEt.Date.AddDays(-1).AddHours(18) } else { $start = $nowEt.Date.AddHours(18) }
    $end = $start.AddDays(1)
    [pscustomobject]@{
        WindowKey = "$($start.ToString('yyyy-MM-dd'))_1800ET"
        WindowLabel = "$($start.ToString('yyyy-MM-dd')) 18:00 ET -> $($end.ToString('yyyy-MM-dd')) 18:00 ET"
        WindowStartET = $start.ToString("yyyy-MM-dd HH:mm:ss")
        WindowEndET = $end.ToString("yyyy-MM-dd HH:mm:ss")
        GeneratedET = Get-NowEtText
    }
}

function Get-RegistryPath {
    if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
    Join-Path $OutputRoot "BackupFailure_WindowRegistry.json"
}

function Get-WindowRegistry {
    $registry = Read-Json (Get-RegistryPath)
    if (!$registry) {
        $registry = [pscustomobject]@{ Windows = [pscustomobject]@{} }
    }
    if (!$registry.PSObject.Properties["Windows"]) {
        $registry | Add-Member -MemberType NoteProperty -Name "Windows" -Value ([pscustomobject]@{}) -Force
    }
    $registry
}

function Save-WindowRegistry($Registry) {
    Write-Json $Registry (Get-RegistryPath)
}

function Resolve-IncidentLock($Window) {
    $registry = Get-WindowRegistry
    $existing = $registry.Windows.PSObject.Properties[$Window.WindowKey]
    if ($existing) {
        $entry = $existing.Value
        Set-ObjProp $entry "LastRunET" $Window.GeneratedET
        Save-WindowRegistry $registry
        return $entry
    }

    $inc = $IncidentNumber
    if (!$inc) { $inc = Read-Host "Enter incident number for this backup-failure window" }
    $inc = $inc.Trim().ToUpper()
    if ($inc -notmatch '^INC[0-9A-Z]+$') { throw "Invalid incident number: $inc" }

    $entry = [pscustomobject]@{
        IncidentNumber = $inc
        WindowKey = $Window.WindowKey
        WindowLabel = $Window.WindowLabel
        WindowStartET = $Window.WindowStartET
        WindowEndET = $Window.WindowEndET
        FirstRunET = $Window.GeneratedET
        LastRunET = $Window.GeneratedET
        OutputFolder = (Join-Path $OutputRoot $inc)
    }
    $registry.Windows | Add-Member -MemberType NoteProperty -Name $Window.WindowKey -Value $entry -Force
    Save-WindowRegistry $registry
    $entry
}

function Get-ClusterName($Cluster) {
    $n = Clean (Get-Prop $Cluster "name" "")
    if (!$n) { $n = Clean (Get-Prop $Cluster "clusterName" "") }
    if (!$n) { $n = Clean (Get-Prop $Cluster "displayName" "") }
    if (!$n) { $n = "Unknown-$([string](Get-Prop $Cluster 'clusterId' ''))" }
    $n
}

function Get-EnvironmentMap {
    @(
        [pscustomobject]@{ Label="Oracle";        Filter="kOracle";        TargetObjectType="kDatabase";       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label="SQL";           Filter="kSQL";           TargetObjectType="kDatabase";       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label="Physical";      Filter="kPhysical";      TargetObjectType="kHost";           ParentHostNeeded=$false },
        [pscustomobject]@{ Label="GenericNas";    Filter="kGenericNas";    TargetObjectType="kHost";           ParentHostNeeded=$false },
        [pscustomobject]@{ Label="HyperV";        Filter="kHyperV";        TargetObjectType="kVirtualMachine"; ParentHostNeeded=$false },
        [pscustomobject]@{ Label="Acropolis";     Filter="kAcropolis";     TargetObjectType="kVirtualMachine"; ParentHostNeeded=$false },
        [pscustomobject]@{ Label="RemoteAdapter"; Filter="kRemoteAdapter"; TargetObjectType="kRemoteAdapter";  ParentHostNeeded=$false },
        [pscustomobject]@{ Label="Isilon";        Filter="kIsilon";        TargetObjectType="kHost";           ParentHostNeeded=$false }
    )
}

function Get-FirstLocalBackupInfo($Run) {
    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    @(($Run.localBackupInfo))[0]
}

function Get-RunEffectiveUsecs($Run) {
    $i = Get-FirstLocalBackupInfo $Run
    if (!$i) { return 0 }
    $end = To-Int64 (Get-Prop $i "endTimeUsecs" 0)
    if ($end -gt 0) { return $end }
    To-Int64 (Get-Prop $i "startTimeUsecs" 0)
}

function Is-FailedStatus([string]$Status) { (Clean $Status) -in @("Failed", "kFailed") }
function Is-SuccessStatus([string]$Status) { (Clean $Status) -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning") }
function Is-RunningStatus([string]$Status) { (Clean $Status) -in @("Running", "kRunning", "Accepted", "kAccepted", "Queued", "kQueued") }
function Is-CancelledStatus([string]$Status) { (Clean $Status) -in @("Canceled", "Cancelled", "kCanceled", "kCancelled", "Canceling", "kCanceling") }
function Is-ActiveLifecycleStatus([string]$Status) { (Clean $Status) -in @("NewlyFailedThisCheck","OlderStillFailing","CurrentStillFailing","CarriedForwardStillFailing","ReFailedAfterClear","RunningAtLatestCheck","CancelledAfterFailure","UnknownNeedsReview") }

function Get-FailedAttempts($RunObject) {
    try { @(Get-Prop (Get-Prop $RunObject "localSnapshotInfo" $null) "failedAttempts" @()) } catch { @() }
}

function Is-SuccessObject($RunObject) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject "localSnapshotInfo" $null)) { return $false }
    (Get-FailedAttempts $RunObject).Count -eq 0
}

function Get-ObjectKey($RunObject, [string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId, [string]$ProtectionGroupName) {
    if ($null -eq $RunObject -or $null -eq (Get-Prop $RunObject "object" $null)) { return "" }
    $obj = Get-Prop $RunObject "object" $null
    $objId = Clean (Get-Prop $obj "id" "")
    if ($objId) { return "$ClusterId|$EnvironmentLabel|$ProtectionGroupId|$objId" }
    $env = Clean (Get-Prop $obj "environment" "")
    $type = Clean (Get-Prop $obj "objectType" "")
    $name = Clean (Get-Prop $obj "name" "")
    $sourceId = Clean (Get-Prop $obj "sourceId" "")
    return "$ClusterId|$EnvironmentLabel|$ProtectionGroupName|$env|$type|$name|$sourceId"
}

function Get-RunLevelKey([string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId, [string]$ProtectionGroupName, [string]$RunType) {
    "$ClusterId|$EnvironmentLabel|$ProtectionGroupId|RUNLEVEL|$RunType|$ProtectionGroupName"
}

function Get-FailureMessage($Attempts) {
    $msgs = @()
    foreach ($a in @($Attempts)) {
        $m = Clean (Get-Prop $a "message" "")
        if ($m) { $msgs += $m }
    }
    ($msgs -join " | ")
}

function Add-SuccessIndex([hashtable]$Index, [string]$Key, [int64]$Usecs, [string]$Status) {
    if (!$Key -or $Usecs -le 0) { return }
    if (!$Index.ContainsKey($Key)) {
        $Index[$Key] = [pscustomobject]@{ Usecs = $Usecs; ET = Convert-UsecsToEtText $Usecs; Status = Clean $Status }
    } elseif ($Usecs -gt [int64]$Index[$Key].Usecs) {
        $Index[$Key] = [pscustomobject]@{ Usecs = $Usecs; ET = Convert-UsecsToEtText $Usecs; Status = Clean $Status }
    }
}

function New-TrackingRow {
    param(
        [string]$IncidentNumber,
        $Window,
        [string]$ClusterName,
        [string]$ClusterId,
        $Env,
        $ProtectionGroup,
        [string]$ObjectKey,
        [string]$HostName,
        [string]$ObjectName,
        [string]$ObjectType,
        [string]$RunType,
        [int64]$StartUsecs,
        [int64]$EndUsecs,
        [string]$Message,
        [string]$Status,
        [string]$LatestRunStatus,
        [int64]$LatestRunUsecs,
        [string[]]$FailedRunKeys
    )

    $effective = if ($EndUsecs -gt 0) { $EndUsecs } else { $StartUsecs }
    if ($LatestRunUsecs -le 0) { $LatestRunUsecs = $effective }

    [pscustomobject]@{
        IncidentNumber = $IncidentNumber
        WindowKey = $Window.WindowKey
        Status = $Status
        Cluster = Clean $ClusterName
        Environment = Clean $Env.Label
        ProtectionGroup = Clean (Get-Prop $ProtectionGroup "name" "")
        Host = Clean $HostName
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = Convert-UsecsToEtText $effective
        LastFailedET = Convert-UsecsToEtText $effective
        LastFailedUsecs = $effective
        ClearedET = ""
        LastSeenET = Convert-UsecsToEtText $LatestRunUsecs
        LatestRunStatus = Clean $LatestRunStatus
        ConsecutiveFailureCount = 1
        Message = Clean $Message
        ObjectKey = Clean $ObjectKey
        ClusterId = Clean $ClusterId
        ProtectionGroupId = [string](Get-Prop $ProtectionGroup "id" "")
        EnvironmentFilter = Clean $Env.Filter
        FailedRunKeys = @($FailedRunKeys)
    }
}

function Get-ProtectionGroups($Cluster, $Env, [string]$ApiKey) {
    $clusterId = [string](Get-Prop $Cluster "clusterId" "")
    $headers = @{ accept = "application/json"; apiKey = $ApiKey; accessClusterId = $clusterId }
    $pgs = @()
    foreach ($filter in ($Env.Filter.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        try {
            $uri = "$BaseUrl/v2/data-protect/protection-groups?environments=$filter&isDeleted=false&isPaused=false&isActive=true"
            $json = Invoke-HeliosGetJson -Uri $uri -Headers $headers
            if ($json -and $json.protectionGroups) { $pgs += @($json.protectionGroups) }
        } catch {
            Add-RunWarning "Protection group lookup failed for $(Get-ClusterName $Cluster) / $filter : $($_.Exception.Message)"
        }
    }
    @($pgs | Sort-Object -Property name,id -Unique)
}

function Get-ProtectionGroupRuns($Cluster, [string]$ProtectionGroupId, [int]$RunLimit, [string]$ApiKey) {
    $clusterId = [string](Get-Prop $Cluster "clusterId" "")
    $headers = @{ accept = "application/json"; apiKey = $ApiKey; accessClusterId = $clusterId }
    $uri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=$RunLimit&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json = Invoke-HeliosGetJson -Uri $uri -Headers $headers
    if ($json -and $json.runs) { return @($json.runs) }
    @()
}

function Get-ObjectNameMap($Runs) {
    $m = @{}
    foreach ($run in $Runs) {
        foreach ($ro in (As-Array (Get-Prop $run "objects" @()))) {
            $obj = Get-Prop $ro "object" $null
            $id = Clean (Get-Prop $obj "id" "")
            $name = Clean (Get-Prop $obj "name" "")
            if ($id -and $name) { $m[$id] = $name }
        }
    }
    $m
}

function Get-RemoteAdapterInfo($ProtectionGroup) {
    $hostName = ""
    $dbName = ""
    try {
        $ra = Get-Prop $ProtectionGroup "remoteAdapterParams" $null
        $hosts = Get-Prop $ra "hosts" $null
        $firstHost = @(As-Array $hosts | Select-Object -First 1)[0]
        if ($firstHost) {
            $hostName = Clean (Get-Prop $firstHost "hostname" "")
            if (!$hostName) { $hostName = Clean (Get-Prop $firstHost "hostName" "") }
            if (!$hostName) { $hostName = Clean (Get-Prop $firstHost "name" "") }
            $scriptBlock = Get-Prop $firstHost "incrementalBackupScript" $null
            if (!$scriptBlock) { $scriptBlock = Get-Prop $firstHost "backupScript" $null }
            $args = Get-Prop $scriptBlock "params" ""
            if ($args -is [array]) { $args = $args -join " " }
            if ((Clean $args) -match "-o\s+(\S+)") { $dbName = $matches[1] }
        }
    } catch {}
    [pscustomobject]@{ Host = $hostName; Object = $(if ($dbName) { $dbName } else { $hostName }) }
}

function Add-FailureRunKey([hashtable]$Map, [string]$Key, [string]$RunKey) {
    if (!$Key -or !$RunKey) { return }
    if (!$Map.ContainsKey($Key)) { $Map[$Key] = New-Object 'System.Collections.Generic.HashSet[string]' }
    [void]$Map[$Key].Add($RunKey)
}

function Get-UsecsFromRunKey([string]$RunKey) {
    if ([string]::IsNullOrWhiteSpace($RunKey)) { return 0 }
    try {
        $parts = $RunKey -split "\|"
        return [int64]$parts[$parts.Count - 1]
    } catch {
        return 0
    }
}

function Update-RowFailureFields($Row, $Keys) {
    $keys = @($Keys | Where-Object { $_ } | Select-Object -Unique)
    Set-ObjProp $Row "FailedRunKeys" @($keys)
    Set-ObjProp $Row "ConsecutiveFailureCount" $keys.Count
    $times = @()
    foreach ($k in $keys) {
        $u = Get-UsecsFromRunKey $k
        if ($u -gt 0) { $times += $u }
    }
    if ($times.Count -gt 0) {
        $min = ($times | Measure-Object -Minimum).Minimum
        $max = ($times | Measure-Object -Maximum).Maximum
        Set-ObjProp $Row "FirstFailedET" (Convert-UsecsToEtText $min)
        Set-ObjProp $Row "LastFailedET" (Convert-UsecsToEtText $max)
        Set-ObjProp $Row "LastFailedUsecs" ([int64]$max)
    }
}

function Merge-UniqueRowsByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = Clean (Get-Prop $r "ObjectKey" "")
        if (!$k) { continue }
        if (!$h.ContainsKey($k)) { $h[$k] = $r }
        else {
            $oldUsecs = To-Int64 (Get-Prop $h[$k] "LastFailedUsecs" 0)
            $newUsecs = To-Int64 (Get-Prop $r "LastFailedUsecs" 0)
            if ($newUsecs -ge $oldUsecs) { $h[$k] = $r }
        }
    }
    @($h.Values)
}

function Collect-CurrentObjectFailures($Incident, $Window, $Clusters, [string]$ApiKey) {
    $rows = @()
    $successIndex = @{}
    $clusterList = @($Clusters | Sort-Object @{Expression={ Get-ClusterName $_ }})
    $clusterTotal = $clusterList.Count
    $clusterIndex = 0

    foreach ($cluster in $clusterList) {
        $clusterIndex++
        $clusterName = Get-ClusterName $cluster
        $clusterId = [string](Get-Prop $cluster "clusterId" "")
        Write-Host ("[{0}/{1}] Cluster: {2}" -f $clusterIndex, $clusterTotal, $clusterName)

        foreach ($env in (Get-EnvironmentMap)) {
            $before = $rows.Count
            $pgsChecked = 0
            $pgs = Get-ProtectionGroups -Cluster $cluster -Env $env -ApiKey $ApiKey
            $filterSet = @($env.Filter.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            $isNas = $env.Label -in @("GenericNas", "Isilon")

            foreach ($pg in $pgs) {
                $pgsChecked++
                $pgId = [string](Get-Prop $pg "id" "")
                $pgName = Clean (Get-Prop $pg "name" "")

                try {
                    $runs = Get-ProtectionGroupRuns -Cluster $cluster -ProtectionGroupId $pgId -RunLimit $NumRuns -ApiKey $ApiKey
                } catch {
                    Add-RunWarning "Runs lookup failed for $clusterName / $pgName : $($_.Exception.Message)"
                    continue
                }
                if ($runs.Count -eq 0) { continue }

                $runTypes = @($runs | ForEach-Object {
                    $i = Get-FirstLocalBackupInfo $_
                    if ($i) { Clean (Get-Prop $i "runType" "") }
                } | Where-Object { $_ } | Select-Object -Unique)

                foreach ($runType in $runTypes) {
                    $runsForType = @($runs | Where-Object {
                        $i = Get-FirstLocalBackupInfo $_
                        $i -and (Clean (Get-Prop $i "runType" "")) -eq $runType
                    } | Sort-Object { Get-RunEffectiveUsecs $_ } -Descending)
                    if ($runsForType.Count -eq 0) { continue }

                    $latestInfo = Get-FirstLocalBackupInfo $runsForType[0]
                    $latestRunStatus = Clean (Get-Prop $latestInfo "status" "")
                    $latestRunUsecs = Get-RunEffectiveUsecs $runsForType[0]
                    $objectNameById = Get-ObjectNameMap $runsForType
                    $cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $latestByKey = @{}
                    $failedKeysByKey = @{}
                    $runLevelKey = Get-RunLevelKey $clusterId $env.Label $pgId $pgName $runType

                    if ($env.Label -eq "RemoteAdapter") {
                        $ra = Get-RemoteAdapterInfo $pg
                        foreach ($run in $runsForType) {
                            $info = Get-FirstLocalBackupInfo $run
                            if (!$info) { continue }
                            $status = Clean (Get-Prop $info "status" "")
                            $effectiveUsecs = Get-RunEffectiveUsecs $run
                            if (Is-SuccessStatus $status) {
                                [void]$cleared.Add($runLevelKey)
                                Add-SuccessIndex $successIndex $runLevelKey $effectiveUsecs $status
                                continue
                            }
                            if (!(Is-FailedStatus $status)) { continue }
                            if ($cleared.Contains($runLevelKey)) { continue }
                            $startUsecs = To-Int64 (Get-Prop $info "startTimeUsecs" 0)
                            $endUsecs = To-Int64 (Get-Prop $info "endTimeUsecs" 0)
                            $msg = Clean (Get-Prop $info "messages" "")
                            if (!$msg) { $msg = "RemoteAdapter run marked Failed" }
                            $rowStatus = "NewlyFailedThisCheck"
                            if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "RunningAtLatestCheck" }
                            elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "CancelledAfterFailure" }
                            $objectName = Clean $ra.Object
                            if (!$objectName) { $objectName = "RemoteAdapter" }
                            $runKey = "$clusterId|$pgId|$runLevelKey|$runType|$effectiveUsecs"
                            Add-FailureRunKey $failedKeysByKey $runLevelKey $runKey
                            if (!$latestByKey.ContainsKey($runLevelKey)) {
                                $latestByKey[$runLevelKey] = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroup $pg -ObjectKey $runLevelKey -HostName (Clean $ra.Host) -ObjectName $objectName -ObjectType "kRemoteAdapter" -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                        }
                        foreach ($k in $latestByKey.Keys) {
                            if ($failedKeysByKey.ContainsKey($k)) { Update-RowFailureFields $latestByKey[$k] @($failedKeysByKey[$k]) }
                            $rows += $latestByKey[$k]
                        }
                        continue
                    }

                    foreach ($run in $runsForType) {
                        $info = Get-FirstLocalBackupInfo $run
                        if (!$info) { continue }
                        $status = Clean (Get-Prop $info "status" "")
                        $startUsecs = To-Int64 (Get-Prop $info "startTimeUsecs" 0)
                        $endUsecs = To-Int64 (Get-Prop $info "endTimeUsecs" 0)
                        $effectiveUsecs = if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs }
                        $objectsAll = @(As-Array (Get-Prop $run "objects" @()) | Where-Object { $_ -and (Get-Prop $_ "object" $null) -and (Get-Prop $_ "localSnapshotInfo" $null) })

                        if (Is-SuccessStatus $status) {
                            if ($objectsAll.Count -eq 0) {
                                [void]$cleared.Add($runLevelKey)
                                Add-SuccessIndex $successIndex $runLevelKey $effectiveUsecs $status
                            }
                            foreach ($ob in $objectsAll) {
                                if (Is-SuccessObject $ob) {
                                    $ck = Get-ObjectKey $ob $clusterId $env.Label $pgId $pgName
                                    if ($ck) {
                                        [void]$cleared.Add($ck)
                                        Add-SuccessIndex $successIndex $ck $effectiveUsecs $status
                                    }
                                }
                            }
                            continue
                        }

                        if (!(Is-FailedStatus $status)) { continue }

                        $candidateObjects = @()
                        if ($isNas) {
                            $candidateObjects = @($objectsAll | Where-Object { (Get-FailedAttempts $_).Count -gt 0 })
                        } else {
                            $candidateObjects = @($objectsAll | Where-Object {
                                $obj = Get-Prop $_ "object" $null
                                $objType = Clean (Get-Prop $obj "objectType" "")
                                $objEnv = Clean (Get-Prop $obj "environment" "")
                                $objType -eq $env.TargetObjectType -and (!$objEnv -or ($filterSet -contains $objEnv))
                            })
                            if ($env.ParentHostNeeded) {
                                $hostObjects = @($objectsAll | Where-Object {
                                    $obj = Get-Prop $_ "object" $null
                                    ((Clean (Get-Prop $obj "objectType" "")) -eq "kHost" -or (Clean (Get-Prop $obj "environment" "")) -eq "kPhysical") -and (Get-FailedAttempts $_).Count -gt 0
                                })
                                $candidateObjects += $hostObjects
                            }
                        }

                        $foundObjectFailure = $false
                        foreach ($ob in $candidateObjects) {
                            $obj = Get-Prop $ob "object" $null
                            $ok = Get-ObjectKey $ob $clusterId $env.Label $pgId $pgName
                            if (!$ok -or $cleared.Contains($ok)) { continue }
                            $attempts = Get-FailedAttempts $ob
                            $hasAttempts = $attempts.Count -gt 0
                            if (!$hasAttempts -and $env.Label -ne "Physical") { continue }
                            $msg = Get-FailureMessage $attempts
                            if (!$msg) { $msg = "Run marked Failed; object returned without failedAttempts details" }

                            $objType = Clean (Get-Prop $obj "objectType" "")
                            $objName = Clean (Get-Prop $obj "name" "")
                            $hostName = ""
                            if ($env.ParentHostNeeded) {
                                $sourceId = Clean (Get-Prop $obj "sourceId" "")
                                if ($sourceId -and $objectNameById.ContainsKey($sourceId)) { $hostName = $objectNameById[$sourceId] }
                                if ($objType -eq "kHost" -or (Clean (Get-Prop $obj "environment" "")) -eq "kPhysical") {
                                    $hostName = $objName
                                    $objName = "No DBs Found (Host-Level Failure)"
                                }
                            }
                            if (!$objName) { continue }

                            $rowStatus = "NewlyFailedThisCheck"
                            if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "RunningAtLatestCheck" }
                            elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "CancelledAfterFailure" }

                            $runKey = "$clusterId|$pgId|$ok|$runType|$effectiveUsecs"
                            Add-FailureRunKey $failedKeysByKey $ok $runKey
                            if (!$latestByKey.ContainsKey($ok)) {
                                $latestByKey[$ok] = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroup $pg -ObjectKey $ok -HostName $hostName -ObjectName $objName -ObjectType $objType -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                            $foundObjectFailure = $true
                        }

                        if (!$foundObjectFailure -and $objectsAll.Count -eq 0 -and !$cleared.Contains($runLevelKey)) {
                            $msg = Clean (Get-Prop $info "messages" "")
                            if (!$msg) { $msg = "Run marked failed; no object-level details returned" }
                            $rowStatus = "UnknownNeedsReview"
                            if ((Is-RunningStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "RunningAtLatestCheck" }
                            elseif ((Is-CancelledStatus $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "CancelledAfterFailure" }
                            $runKey = "$clusterId|$pgId|$runLevelKey|$runType|$effectiveUsecs"
                            Add-FailureRunKey $failedKeysByKey $runLevelKey $runKey
                            if (!$latestByKey.ContainsKey($runLevelKey)) {
                                $latestByKey[$runLevelKey] = New-TrackingRow -IncidentNumber $Incident -Window $Window -ClusterName $clusterName -ClusterId $clusterId -Env $env -ProtectionGroup $pg -ObjectKey $runLevelKey -HostName "" -ObjectName "" -ObjectType "" -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                        }
                    }

                    foreach ($k in $latestByKey.Keys) {
                        if ($failedKeysByKey.ContainsKey($k)) { Update-RowFailureFields $latestByKey[$k] @($failedKeysByKey[$k]) }
                        $rows += $latestByKey[$k]
                    }
                }
            }
            $envFailures = $rows.Count - $before
            Write-Host ("  {0,-13}: PGs checked: {1} | failures: {2}" -f $env.Label, $pgsChecked, $envFailures)
        }
    }

    [pscustomobject]@{
        CurrentFailures = @(Merge-UniqueRowsByKey $rows)
        SuccessIndex = $successIndex
    }
}

function Clone-Row($Row) {
    if ($null -eq $Row) { return $null }
    $Row | Select-Object *
}

function Normalize-ExistingRows($Rows, [string]$Incident, $Window) {
    $out = @()
    foreach ($r in @($Rows)) {
        $n = Clone-Row $r
        if ($null -eq $n) { continue }
        $status = Clean (Get-Prop $n "Status" "")
        if (!$status) { $status = "UnknownNeedsReview" }
        Set-ObjProp $n "Status" $status
        if (!(Get-Prop $n "IncidentNumber" "")) { Set-ObjProp $n "IncidentNumber" $Incident }
        if (!(Get-Prop $n "WindowKey" "")) { Set-ObjProp $n "WindowKey" $Window.WindowKey }
        if (!(Get-Prop $n "LastFailedUsecs" $null)) { Set-ObjProp $n "LastFailedUsecs" (Convert-EtTextToUsecs (Clean (Get-Prop $n "LastFailedET" ""))) }
        if (!(Get-Prop $n "FailedRunKeys" $null)) { Set-ObjProp $n "FailedRunKeys" @() }
        if (!(Get-Prop $n "ConsecutiveFailureCount" $null)) { Set-ObjProp $n "ConsecutiveFailureCount" 1 }
        $out += $n
    }
    @($out)
}

function Index-ByKey($Rows) {
    $h = @{}
    foreach ($r in @($Rows)) {
        $k = Clean (Get-Prop $r "ObjectKey" "")
        if ($k) { $h[$k] = $r }
    }
    $h
}

function Merge-FailedRunKeys($ExistingRow, $NewRow) {
    $keys = @()
    if ($ExistingRow) { $keys += As-Array (Get-Prop $ExistingRow "FailedRunKeys" @()) }
    if ($NewRow) { $keys += As-Array (Get-Prop $NewRow "FailedRunKeys" @()) }
    @($keys | Where-Object { $_ } | Select-Object -Unique)
}

function Merge-Lifecycle($CurrentRows, $PreviousOpenRows, $PreviousClearedRows, [hashtable]$SuccessIndex) {
    $current = @()
    $clearedThisRun = @()
    $currentByKey = Index-ByKey $CurrentRows
    $previousOpenByKey = Index-ByKey $PreviousOpenRows

    foreach ($c in @($CurrentRows)) {
        $key = Clean (Get-Prop $c "ObjectKey" "")
        $n = Clone-Row $c
        if ($previousOpenByKey.ContainsKey($key)) {
            $p = $previousOpenByKey[$key]
            Set-ObjProp $n "FirstFailedET" (Clean (Get-Prop $p "FirstFailedET" (Get-Prop $n "FirstFailedET" "")))
            $status = Clean (Get-Prop $p "Status" "")
            if ($status -eq "ClearedByLaterSuccess" -or $status -eq "NewlyClearedThisCheck") { Set-ObjProp $n "Status" "ReFailedAfterClear" }
            elseif ((Clean (Get-Prop $n "Status" "")) -eq "NewlyFailedThisCheck") { Set-ObjProp $n "Status" "OlderStillFailing" }
            Update-RowFailureFields $n (Merge-FailedRunKeys $p $n)
        }
        $current += $n
    }

    foreach ($p in @($PreviousOpenRows)) {
        $key = Clean (Get-Prop $p "ObjectKey" "")
        if (!$key -or $currentByKey.ContainsKey($key)) { continue }
        $lastFailed = To-Int64 (Get-Prop $p "LastFailedUsecs" 0)
        if ($lastFailed -le 0) { $lastFailed = Convert-EtTextToUsecs (Clean (Get-Prop $p "LastFailedET" "")) }
        if ($SuccessIndex.ContainsKey($key) -and [int64]$SuccessIndex[$key].Usecs -gt $lastFailed) {
            $c = Clone-Row $p
            Set-ObjProp $c "Status" "NewlyClearedThisCheck"
            Set-ObjProp $c "ClearedET" (Clean $SuccessIndex[$key].ET)
            Set-ObjProp $c "LastSeenET" (Clean $SuccessIndex[$key].ET)
            Set-ObjProp $c "LatestRunStatus" (Clean $SuccessIndex[$key].Status)
            $clearedThisRun += $c
        } else {
            $u = Clone-Row $p
            Set-ObjProp $u "Status" "UnknownNeedsReview"
            $current += $u
        }
    }

    $historicalCleared = @()
    foreach ($h in @($PreviousClearedRows)) {
        $x = Clone-Row $h
        if ($x) {
            if ((Clean (Get-Prop $x "Status" "")) -eq "NewlyClearedThisCheck") { Set-ObjProp $x "Status" "ClearedByLaterSuccess" }
            $historicalCleared += $x
        }
    }

    $allCleared = @(Merge-UniqueRowsByKey @($historicalCleared + $clearedThisRun))
    $lifecycle = @(Merge-UniqueRowsByKey @($current + $allCleared))
    [pscustomobject]@{
        Current = @($current)
        ClearedThisRun = @($clearedThisRun)
        AllCleared = @($allCleared)
        Lifecycle = @($lifecycle)
    }
}

function Convert-LifecycleRows($Rows) {
    foreach ($r in @($Rows)) {
        $objectName = Clean (Get-Prop $r "ObjectName" "")
        $objectType = Clean (Get-Prop $r "ObjectType" "")
        if ($objectType -eq "ProtectionGroup") { $objectName = ""; $objectType = "" }
        [pscustomobject]@{
            Cluster = Clean (Get-Prop $r "Cluster" "")
            ProtectionGroup = Clean (Get-Prop $r "ProtectionGroup" "")
            Environment = Clean (Get-Prop $r "Environment" "")
            Host = Clean (Get-Prop $r "Host" "")
            ObjectName = $objectName
            ObjectType = $objectType
            RunType = Clean (Get-Prop $r "RunType" "")
            Status = Clean (Get-Prop $r "Status" "")
            OldestFailedET = Clean (Get-Prop $r "FirstFailedET" "")
            NewestFailedET = Clean (Get-Prop $r "LastFailedET" "")
            LatestSuccessET = Clean (Get-Prop $r "ClearedET" "")
            FailureRuns = Clean (Get-Prop $r "ConsecutiveFailureCount" "")
            Message = Clean (Get-Prop $r "Message" "")
        }
    }
}

function Format-Rows($Rows, [string[]]$Columns) {
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add(($Columns -join " | "))
    $list = @($Rows)
    if ($list.Count -eq 0) {
        $lines.Add("- None")
        return ($lines -join [Environment]::NewLine)
    }
    foreach ($r in $list) {
        $values = @()
        foreach ($c in $Columns) {
            $prop = $r.PSObject.Properties[$c]
            if ($prop) { $values += (Clean $prop.Value) } else { $values += "" }
        }
        $lines.Add(($values -join " | "))
    }
    $lines -join [Environment]::NewLine
}

function Format-Warnings($Warnings) {
    $list = @($Warnings | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    if ($list.Count -eq 0) { return "- None" }
    ($list | ForEach-Object { "- $(Clean $_)" }) -join [Environment]::NewLine
}

function Get-OutputFolder($IncidentEntry) {
    $folder = Clean (Get-Prop $IncidentEntry "OutputFolder" "")
    if (!$folder) { $folder = Join-Path $OutputRoot (Clean (Get-Prop $IncidentEntry "IncidentNumber" "")) }
    if (!(Test-Path $folder)) { New-Item -Path $folder -ItemType Directory -Force | Out-Null }
    $folder
}

function Write-TextOutputs($Folder, $Incident, $Window, $LifecycleExport, $CurrentExport, $NewlyClearedExport, $PreviouslyClearedCount) {
    $apiStatus = if ($script:Warnings.Count -gt 0) { "Incomplete - $($script:Warnings.Count) collection warning(s) recorded. See Incomplete Collection section." } else { "Complete" }
    $activeTally = "Active breakdown tally: $(@($CurrentExport).Count) active/unresolved lifecycle rows."
    $lifecycleTally = "Lifecycle tally: $(@($CurrentExport).Count) active/unresolved + $(@($NewlyClearedExport).Count) newly cleared this check + $PreviouslyClearedCount previously cleared retained = $(@($LifecycleExport).Count) total lifecycle rows."
    $failureText = Format-Rows $CurrentExport $script:FailureColumns
    $successText = Format-Rows (@($NewlyClearedExport | Select-Object -Property $script:SuccessColumns)) $script:SuccessColumns
    $warningsText = Format-Warnings $script:Warnings

    @"
Cohesity Backup Failure Incident Update

Incident: $Incident
Compute Window: $($Window.WindowLabel)
Generated At: $($Window.GeneratedET) ET
Cohesity API Collection Status: $apiStatus
Scope: latest $NumRuns runs per protection group/run type.

Summary Counts:
- Active / unresolved failures: $(@($CurrentExport).Count)
- Newly cleared this check: $(@($NewlyClearedExport).Count)
- Previously cleared rows retained in lifecycle CSV: $PreviouslyClearedCount
- Total lifecycle rows tracked: $(@($LifecycleExport).Count)

Tally Check:
- $activeTally
- $lifecycleTally

Failure Section:
$failureText

Success Section:
$successText

Incomplete Collection:
$warningsText
"@ | Set-Content -Path (Join-Path $Folder "worknotes_summary.txt") -Encoding UTF8

    @"
Backup Failure Incident Closure Summary

Incident: $Incident
Compute Window: $($Window.WindowLabel)
Generated At: $($Window.GeneratedET) ET
Cohesity API Collection Status: $apiStatus
Scope: latest $NumRuns runs per protection group/run type.

Closure Counts:
- Active / unresolved failures: $(@($CurrentExport).Count)
- Newly cleared this check: $(@($NewlyClearedExport).Count)
- Previously cleared rows retained in lifecycle CSV: $PreviouslyClearedCount
- Total lifecycle rows tracked: $(@($LifecycleExport).Count)

Failure Section:
$failureText

Success Section:
$successText

Carry Forward / Handoff:
$(if (@($CurrentExport).Count -eq 0) { "No active backup failures remain based on the latest saved state." } else { "$(@($CurrentExport).Count) active/unresolved rows remain in incident_lifecycle.csv and should be carried forward or separately tracked." })

Incomplete Collection:
$warningsText
"@ | Set-Content -Path (Join-Path $Folder "closing_summary.txt") -Encoding UTF8
}

function Remove-TemporaryCollectorFiles([string]$Folder) {
    foreach ($name in @("worknotes.txt", "summary.txt")) {
        $p = Join-Path $Folder $name
        if (Test-Path $p) { Remove-Item -Path $p -Force -ErrorAction SilentlyContinue }
    }
}

$window = Get-ComputeWindow
$incidentEntry = Resolve-IncidentLock $window
$incident = Clean (Get-Prop $incidentEntry "IncidentNumber" "")
$outputFolder = Get-OutputFolder $incidentEntry
$statePath = Join-Path $outputFolder "state.json"

$apiKey = Get-CohesityApiKey
$headers = @{ accept = "application/json"; apiKey = $apiKey }
try {
    $clusterJson = Invoke-HeliosGetJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers $headers
    $clusters = @($clusterJson.cohesityClusters)
} catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}
if (!$clusters -or $clusters.Count -eq 0) { throw "No clusters returned from Helios." }

if ($ClusterName) {
    $clusters = @($clusters | Where-Object {
        (Get-ClusterName $_) -eq $ClusterName -or
        (Clean (Get-Prop $_ "name" "")) -eq $ClusterName -or
        (Clean (Get-Prop $_ "clusterName" "")) -eq $ClusterName -or
        (Clean (Get-Prop $_ "displayName" "")) -eq $ClusterName
    })
    if ($clusters.Count -eq 0) { throw "Cluster not found: $ClusterName" }
}
$clusters = @($clusters | Sort-Object @{Expression={ Get-ClusterName $_ }})

Write-Host "Processing clusters alphabetically."
Write-Host "Output folder: $outputFolder"

$previousState = Read-Json $statePath
$previousOpen = @()
$previousCleared = @()
if ($previousState) {
    $previousOpen = Normalize-ExistingRows (As-Array (Get-Prop $previousState "CurrentOpenFailures" @())) $incident $window
    $previousCleared = Normalize-ExistingRows (As-Array (Get-Prop $previousState "ClearedBySuccess" @())) $incident $window
}

$collection = Collect-CurrentObjectFailures -Incident $incident -Window $window -Clusters $clusters -ApiKey $apiKey
$merged = Merge-Lifecycle -CurrentRows $collection.CurrentFailures -PreviousOpenRows $previousOpen -PreviousClearedRows $previousCleared -SuccessIndex $collection.SuccessIndex

$currentRows = @($merged.Current | Sort-Object Cluster,ProtectionGroup,Environment,@{Expression={Date-Sort (Get-Prop $_ "LastFailedET" "")};Descending=$true})
$newlyClearedRows = @($merged.ClearedThisRun | Sort-Object @{Expression={Date-Sort (Get-Prop $_ "ClearedET" "")};Descending=$true})
$allClearedRows = @($merged.AllCleared)
$lifecycleRows = @($merged.Lifecycle)

Write-Csv $currentRows (Join-Path $outputFolder "current_failures.csv") $script:CsvColumns
Write-Csv $newlyClearedRows (Join-Path $outputFolder "cleared_by_success.csv") $script:CsvColumns
Write-Csv $lifecycleRows (Join-Path $outputFolder "incident_lifecycle_raw.csv") $script:CsvColumns

$lifecycleExport = @(Convert-LifecycleRows $lifecycleRows | Sort-Object Cluster,ProtectionGroup,Environment,@{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
$currentExport = @($lifecycleExport | Where-Object { Is-ActiveLifecycleStatus $_.Status } | Sort-Object @{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
$newlyClearedExport = @($lifecycleExport | Where-Object { $_.Status -eq "NewlyClearedThisCheck" } | Sort-Object @{Expression={Date-Sort $_.LatestSuccessET};Descending=$true})
$previouslyClearedCount = @($lifecycleExport | Where-Object { $_.Status -eq "ClearedByLaterSuccess" }).Count

Write-Csv $lifecycleExport (Join-Path $outputFolder "incident_lifecycle.csv") $script:LifecycleColumns
Write-TextOutputs -Folder $outputFolder -Incident $incident -Window $window -LifecycleExport $lifecycleExport -CurrentExport $currentExport -NewlyClearedExport $newlyClearedExport -PreviouslyClearedCount $previouslyClearedCount

$state = [pscustomobject]@{
    IncidentNumber = $incident
    WindowKey = $window.WindowKey
    WindowLabel = $window.WindowLabel
    WindowStartET = $window.WindowStartET
    WindowEndET = $window.WindowEndET
    LastRunET = $window.GeneratedET
    NumRuns = $NumRuns
    RequestTimeoutSec = $RequestTimeoutSec
    ClusterName = $ClusterName
    Warnings = @($script:Warnings)
    CurrentOpenFailures = @($currentRows)
    ClearedBySuccess = @($allClearedRows)
    LastRunClearedBySuccess = @($newlyClearedRows)
    LifecycleRows = @($lifecycleRows)
}
Write-Json $state $statePath
Remove-TemporaryCollectorFiles $outputFolder

Write-Host ""
Write-Host "Final Summary:"
Write-Host "Cohesity API Collection Status : $(if ($script:Warnings.Count -gt 0) { 'Incomplete' } else { 'Complete' })"
Write-Host "Active / Unresolved Failures   : $($currentExport.Count)"
Write-Host "Newly Cleared This Check       : $($newlyClearedExport.Count)"
Write-Host "Previously Cleared Retained    : $previouslyClearedCount"
Write-Host "Total Lifecycle Rows           : $($lifecycleExport.Count)"
Write-Host "Incomplete Collection Warnings : $($script:Warnings.Count)"
Write-Host ""
Write-Host "Files Created:"
Write-Host (Join-Path $outputFolder "worknotes_summary.txt")
Write-Host (Join-Path $outputFolder "incident_lifecycle.csv")
Write-Host (Join-Path $outputFolder "closing_summary.txt")
Write-Host (Join-Path $outputFolder "state.json")
