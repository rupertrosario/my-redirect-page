<#
.SYNOPSIS
Entry point for Cohesity Backup Failure INC status updates.
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
    [switch]$ShowGrid
)

$script:FinalColumns = @("Cluster","ProtectionGroup","Environment","Host","ObjectName","ObjectType","RunType","Status","OldestFailedET","NewestFailedET","LatestSuccessET","FailureRuns","Message")
$script:FailureColumns = @("Cluster","ProtectionGroup","Environment","Host","ObjectName","ObjectType","RunType","Status","OldestFailedET","NewestFailedET","LatestSuccessET","FailureRuns","Message")
$script:SuccessColumns = @("Cluster","ProtectionGroup","Environment","RunType","LatestSuccessET")
$script:InternalColumns = @("IncidentNumber","WindowKey","Status","Cluster","Environment","ProtectionGroup","Host","ObjectName","ObjectType","RunType","FirstFailedET","LastFailedET","ClearedET","LastSeenET","LatestRunStatus","ConsecutiveFailureCount","Message","ObjectKey","ClusterId","ProtectionGroupId","EnvironmentFilter","FailedRunKeys")

function Clean($v) {
    if ($null -eq $v) { return "" }
    if ($v -is [array]) { $v = $v -join " | " }
    return (([string]$v -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim()
}

function Read-JsonFile([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    $raw = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    try { $raw | ConvertFrom-Json } catch { $null }
}

function Write-JsonFile($Object, [string]$Path) {
    $Object | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Import-ReportCsv([string]$Path) {
    if (!(Test-Path $Path)) { return @() }
    try { @(Import-Csv -Path $Path) } catch { @() }
}

function Save-Csv([string]$Path, $Rows, [string[]]$Columns) {
    $list = @($Rows)
    if ($list.Count -eq 0) {
        ($Columns -join ",") | Set-Content -Path $Path -Encoding UTF8
    } else {
        $list | Select-Object -Property $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Set-Prop($o, [string]$n, $v) {
    if ($null -eq $o) { return }
    if ($o.PSObject.Properties[$n]) { $o.$n = $v }
    else { $o | Add-Member -MemberType NoteProperty -Name $n -Value $v -Force }
}

function As-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function Parse-DateText([string]$Text) {
    $t = Clean $Text
    if (!$t) { return $null }
    $formats = @("yyyy-MM-dd HH:mm:ss","yyyy-MM-dd H:mm:ss","yyyy-MM-dd HH:mm","yyyy-MM-dd H:mm","M/d/yyyy H:mm:ss","M/d/yyyy HH:mm:ss","M/d/yyyy H:mm","M/d/yyyy HH:mm","M/d/yyyy h:mm:ss tt","M/d/yyyy hh:mm:ss tt","M/d/yyyy h:mm tt","M/d/yyyy hh:mm tt")
    foreach ($f in $formats) { try { return [datetime]::ParseExact($t, $f, [Globalization.CultureInfo]::InvariantCulture) } catch {} }
    try { [datetime]::Parse($t, [Globalization.CultureInfo]::InvariantCulture) } catch { $null }
}

function Date-Sort($v) {
    $d = Parse-DateText (Clean $v)
    if ($d) { return $d.ToString("yyyy-MM-dd HH:mm:ss") }
    $t = Clean $v
    if ($t) { return $t }
    return "0000-00-00 00:00:00"
}

function Convert-UsecsToEtTextLocal($Usecs) {
    if ($null -eq $Usecs) { return "" }
    try {
        $u = [int64]$Usecs
        if ($u -le 0) { return "" }
        $utc = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$u / 1000)).UtcDateTime
        try { $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
        catch { $tz = [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $tz)).ToString("yyyy-MM-dd HH:mm:ss")
    } catch {
        return ""
    }
}

function Get-ReportFolder([string]$Root, [string]$Inc) {
    if ($Inc) {
        $candidate = Join-Path $Root $Inc.Trim().ToUpper()
        if (Test-Path $candidate) { return $candidate }
    }
    $latest = Get-ChildItem -Path $Root -Directory -ErrorAction SilentlyContinue | Where-Object { $_.Name -ne "Archive" } | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($latest) { return $latest.FullName }
    return ""
}

function Row-Key($r) {
    $key = Clean $r.ObjectKey
    if ($key) { return $key }
    @((Clean $r.Cluster),(Clean $r.Environment),(Clean $r.ProtectionGroup),(Clean $r.Host),(Clean $r.ObjectName),(Clean $r.RunType)) -join "|"
}

function Is-Success([string]$s) { (Clean $s) -in @("Succeeded","SucceededWithWarning","kSucceeded","kSucceededWithWarning") }
function Is-Failed([string]$s) { (Clean $s) -in @("Failed","kFailed") }
function Is-Running([string]$s) { (Clean $s) -in @("Running","kRunning","Accepted","kAccepted","Queued","kQueued") }
function Is-Cancelled([string]$s) { (Clean $s) -in @("Canceled","Cancelled","kCanceled","kCancelled","Canceling","kCanceling") }
function Is-Cleared([string]$s) { (Clean $s) -in @("NewlyClearedThisCheck","ClearedByLaterSuccess") }

function Latest-Success-Clears($r) {
    if ($null -eq $r) { return $false }
    if (Is-Cleared $r.Status) { return $false }
    if (!(Is-Success $r.LatestRunStatus)) { return $false }
    $seen = Parse-DateText (Clean $r.LastSeenET)
    $failed = Parse-DateText (Clean $r.LastFailedET)
    if ($null -eq $seen -or $null -eq $failed) { return $false }
    $seen -gt $failed
}

function Merge-Latest($Rows) {
    $map = @{}
    foreach ($r in @($Rows)) {
        if ($null -eq $r) { continue }
        $key = Row-Key $r
        if (!$key) { continue }
        if (!$map.ContainsKey($key)) { $map[$key] = $r; continue }
        $oldValue = if ($map[$key].ClearedET) { $map[$key].ClearedET } elseif ($map[$key].LastFailedET) { $map[$key].LastFailedET } else { $map[$key].FirstFailedET }
        $newValue = if ($r.ClearedET) { $r.ClearedET } elseif ($r.LastFailedET) { $r.LastFailedET } else { $r.FirstFailedET }
        if ((Date-Sort $newValue) -ge (Date-Sort $oldValue)) { $map[$key] = $r }
    }
    @($map.Values)
}

function Reconcile($Current, $Cleared) {
    $active = @()
    $moved = @()
    foreach ($r in @($Current)) {
        if (Latest-Success-Clears $r) {
            $c = $r | Select-Object *
            Set-Prop $c "Status" "NewlyClearedThisCheck"
            Set-Prop $c "ClearedET" (Clean $c.LastSeenET)
            $moved += $c
        } else {
            $active += $r
        }
    }
    $finalCleared = Merge-Latest @($Cleared + $moved)
    [pscustomobject]@{
        Active = @($active)
        Cleared = @($finalCleared)
        Lifecycle = @(Merge-Latest @($active + $finalCleared))
        Moved = @($moved)
    }
}

function Invoke-HeliosGetJsonLocal([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec 120
    } else {
        $r = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec 120
    }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    $r.Content | ConvertFrom-Json
}

function Get-CohesityApiKeyLocal {
    if (!(Test-Path $HelperPath)) { throw "Missing API key helper: $HelperPath" }
    if (!(Test-Path $EncryptedFile)) { throw "Missing encrypted key file: $EncryptedFile" }
    . $HelperPath
    $key = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($key)) { throw "API key is blank from AES helper." }
    $key.Trim()
}

function Get-FirstLocalBackupInfoLocal($Run) {
    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    @(($Run.localBackupInfo))[0]
}

function Get-RunEffectiveUsecsLocal($Run) {
    $i = Get-FirstLocalBackupInfoLocal $Run
    if (!$i) { return 0 }
    $end = [int64](Clean $i.endTimeUsecs)
    if ($end -gt 0) { return $end }
    [int64](Clean $i.startTimeUsecs)
}

function Get-ObjectKeyLocal($RunObject, [string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId, [string]$ProtectionGroupName) {
    if ($null -eq $RunObject -or $null -eq $RunObject.object) { return "" }
    $obj = $RunObject.object
    $objId = Clean $obj.id
    if ($objId) { return "$ClusterId|$EnvironmentLabel|$ProtectionGroupId|$objId" }
    $sourceId = Clean $obj.sourceId
    $name = Clean $obj.name
    $type = Clean $obj.objectType
    $env = Clean $obj.environment
    "$ClusterId|$EnvironmentLabel|$ProtectionGroupName|$env|$type|$name|$sourceId"
}

function Get-FailedAttemptsLocal($RunObject) {
    try { @(($RunObject.localSnapshotInfo.failedAttempts)) } catch { @() }
}

function Is-SuccessObjectLocal($RunObject) {
    if ($null -eq $RunObject -or $null -eq $RunObject.localSnapshotInfo) { return $false }
    (Get-FailedAttemptsLocal $RunObject).Count -eq 0
}

function Get-FailureMessageLocal($Attempts) {
    $msgs = @()
    foreach ($a in @($Attempts)) {
        $m = Clean $a.message
        if ($m) { $msgs += $m }
    }
    ($msgs -join " | ")
}

function Update-RowFailureKeys($Row, $Keys) {
    $keys2 = @($Keys | Where-Object { $_ } | Select-Object -Unique)
    Set-Prop $Row "FailedRunKeys" @($keys2)
    Set-Prop $Row "ConsecutiveFailureCount" $keys2.Count
    $times = @()
    foreach ($k in $keys2) {
        try {
            $parts = $k -split "\|"
            $u = [int64]$parts[$parts.Count - 1]
            if ($u -gt 0) { $times += $u }
        } catch {}
    }
    if ($times.Count -gt 0) {
        $min = ($times | Measure-Object -Minimum).Minimum
        $max = ($times | Measure-Object -Maximum).Maximum
        Set-Prop $Row "FirstFailedET" (Convert-UsecsToEtTextLocal $min)
        Set-Prop $Row "LastFailedET" (Convert-UsecsToEtTextLocal $max)
        Set-Prop $Row "LastFailedUsecs" ([int64]$max)
    }
}

function New-ObjectTrackingRow {
    param(
        [string]$Incident,
        [string]$WindowKey,
        $Cluster,
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
        IncidentNumber = $Incident
        WindowKey = $WindowKey
        Status = $Status
        Cluster = Clean $Cluster.Name
        Environment = Clean $Env.Label
        ProtectionGroup = Clean $ProtectionGroup.name
        Host = Clean $HostName
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = Convert-UsecsToEtTextLocal $effective
        LastFailedET = Convert-UsecsToEtTextLocal $effective
        LastFailedUsecs = $effective
        ClearedET = ""
        LastSeenET = Convert-UsecsToEtTextLocal $LatestRunUsecs
        LatestRunStatus = Clean $LatestRunStatus
        ConsecutiveFailureCount = 1
        Message = Clean $Message
        ObjectKey = $ObjectKey
        ClusterId = [string]$Cluster.Id
        ProtectionGroupId = [string]$ProtectionGroup.id
        EnvironmentFilter = Clean $Env.Filter
        FailedRunKeys = @($FailedRunKeys)
    }
}

function Get-ObjectEnvironmentMapLocal {
    @(
        [pscustomobject]@{ Label="Oracle";        Filter="kOracle";        ObjectType="kDatabase";       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label="SQL";           Filter="kSQL";           ObjectType="kDatabase";       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label="Physical";      Filter="kPhysical";      ObjectType="kHost";           ParentHostNeeded=$false },
        [pscustomobject]@{ Label="GenericNas";    Filter="kGenericNas";    ObjectType="kHost";           ParentHostNeeded=$false },
        [pscustomobject]@{ Label="HyperV";        Filter="kHyperV";        ObjectType="kVirtualMachine"; ParentHostNeeded=$false },
        [pscustomobject]@{ Label="Acropolis";     Filter="kAcropolis";     ObjectType="kVirtualMachine"; ParentHostNeeded=$false },
        [pscustomobject]@{ Label="RemoteAdapter"; Filter="kRemoteAdapter"; ObjectType="kRemoteAdapter";  ParentHostNeeded=$false },
        [pscustomobject]@{ Label="Isilon";        Filter="kIsilon";        ObjectType="kHost";           ParentHostNeeded=$false }
    )
}

function Collect-ObjectLevelCurrentFailures([string]$Incident, [string]$WindowKey, [int]$RunLimit) {
    $rows = @()
    try { $apiKey = Get-CohesityApiKeyLocal } catch { Write-Warning "Object-level refresh skipped: $($_.Exception.Message)"; return @() }
    $commonHeaders = @{ accept = "application/json"; apiKey = $apiKey }
    try {
        $clusterJson = Invoke-HeliosGetJsonLocal -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
        $clustersRaw = @($clusterJson.cohesityClusters)
    } catch {
        Write-Warning "Object-level refresh skipped; cluster lookup failed: $($_.Exception.Message)"
        return @()
    }
    if ($ClusterName) {
        $clustersRaw = @($clustersRaw | Where-Object {
            (Clean $_.name) -eq $ClusterName -or (Clean $_.clusterName) -eq $ClusterName -or (Clean $_.displayName) -eq $ClusterName
        })
    }
    $clusters = @($clustersRaw | ForEach-Object {
        $n = Clean $_.name
        if (!$n) { $n = Clean $_.clusterName }
        if (!$n) { $n = Clean $_.displayName }
        if (!$n) { $n = "Unknown-$($_.clusterId)" }
        [pscustomobject]@{ Id = [string]$_.clusterId; Name = $n; Raw = $_ }
    })
    foreach ($cluster in $clusters) {
        $headers = @{ accept = "application/json"; apiKey = $apiKey; accessClusterId = $cluster.Id }
        foreach ($env in (Get-ObjectEnvironmentMapLocal)) {
            $filterSet = @($env.Filter.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            $isNas = $env.Label -in @("GenericNas","Isilon")
            $pgs = @()
            foreach ($f in $filterSet) {
                try {
                    $pgUri = "$BaseUrl/v2/data-protect/protection-groups?environments=$f&isDeleted=false&isPaused=false&isActive=true"
                    $pgJson = Invoke-HeliosGetJsonLocal -Uri $pgUri -Headers $headers
                    if ($pgJson -and $pgJson.protectionGroups) { $pgs += @($pgJson.protectionGroups) }
                } catch {}
            }
            $pgs = @($pgs | Sort-Object id -Unique)
            foreach ($pg in $pgs) {
                $pgId = [string]$pg.id
                $pgName = Clean $pg.name
                try {
                    $runsUri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgId))/runs?numRuns=$RunLimit&excludeNonRestorableRuns=false&includeObjectDetails=true"
                    $runsJson = Invoke-HeliosGetJsonLocal -Uri $runsUri -Headers $headers
                    $runs = @($runsJson.runs)
                } catch { continue }
                if ($runs.Count -eq 0) { continue }
                $runTypes = @($runs | ForEach-Object { $i = Get-FirstLocalBackupInfoLocal $_; if ($i) { Clean $i.runType } } | Where-Object { $_ } | Select-Object -Unique)
                foreach ($runType in $runTypes) {
                    $runsForType = @($runs | Where-Object { $i = Get-FirstLocalBackupInfoLocal $_; $i -and (Clean $i.runType) -eq $runType } | Sort-Object { Get-RunEffectiveUsecsLocal $_ } -Descending)
                    if ($runsForType.Count -eq 0) { continue }
                    $latestInfo = Get-FirstLocalBackupInfoLocal $runsForType[0]
                    $latestRunStatus = Clean $latestInfo.status
                    $latestRunUsecs = Get-RunEffectiveUsecsLocal $runsForType[0]
                    $idToName = @{}
                    foreach ($rr in $runsForType) {
                        foreach ($ob in (As-Array $rr.objects)) {
                            if ($ob -and $ob.object -and $ob.object.id -and $ob.object.name) { $idToName[[string]$ob.object.id] = Clean $ob.object.name }
                        }
                    }
                    $cleared = New-Object 'System.Collections.Generic.HashSet[string]'
                    $latestByKey = @{}
                    $failedKeysByKey = @{}
                    foreach ($run in $runsForType) {
                        $info = Get-FirstLocalBackupInfoLocal $run
                        if (!$info) { continue }
                        $status = Clean $info.status
                        $startUsecs = [int64](Clean $info.startTimeUsecs)
                        $endUsecs = [int64](Clean $info.endTimeUsecs)
                        $effectiveUsecs = if ($endUsecs -gt 0) { $endUsecs } else { $startUsecs }
                        $objectsAll = @(As-Array $run.objects | Where-Object { $_ -and $_.object -and $_.localSnapshotInfo })
                        if (Is-Success $status) {
                            foreach ($ob in $objectsAll) {
                                if (Is-SuccessObjectLocal $ob) {
                                    $ck = Get-ObjectKeyLocal $ob $cluster.Id $env.Label $pgId $pgName
                                    if ($ck) { [void]$cleared.Add($ck) }
                                }
                            }
                            continue
                        }
                        if (!(Is-Failed $status)) { continue }

                        $candidateObjects = @()
                        if ($isNas) {
                            $candidateObjects = @($objectsAll | Where-Object { (Get-FailedAttemptsLocal $_).Count -gt 0 })
                        } else {
                            $candidateObjects = @($objectsAll | Where-Object {
                                $objType = Clean $_.object.objectType
                                $objEnv = Clean $_.object.environment
                                $objType -eq $env.ObjectType -and (!$objEnv -or ($filterSet -contains $objEnv))
                            })
                            if ($env.ParentHostNeeded) {
                                $hostObjects = @($objectsAll | Where-Object { ((Clean $_.object.objectType) -eq "kHost" -or (Clean $_.object.environment) -eq "kPhysical") -and (Get-FailedAttemptsLocal $_).Count -gt 0 })
                                $candidateObjects += $hostObjects
                            }
                        }

                        foreach ($ob in $candidateObjects) {
                            $obj = $ob.object
                            $ok = Get-ObjectKeyLocal $ob $cluster.Id $env.Label $pgId $pgName
                            if (!$ok -or $cleared.Contains($ok)) { continue }
                            $attempts = Get-FailedAttemptsLocal $ob
                            $hasAttempts = $attempts.Count -gt 0
                            if (!$hasAttempts -and $env.Label -ne "Physical") { continue }
                            $msg = Get-FailureMessageLocal $attempts
                            if (!$msg) { $msg = "Run marked Failed; object returned without failedAttempts details" }
                            $objType = Clean $obj.objectType
                            $objName = Clean $obj.name
                            $hostName = ""
                            if ($env.ParentHostNeeded) {
                                $sourceId = Clean $obj.sourceId
                                if ($sourceId -and $idToName.ContainsKey($sourceId)) { $hostName = $idToName[$sourceId] }
                                if ($objType -eq "kHost" -or (Clean $obj.environment) -eq "kPhysical") {
                                    $hostName = $objName
                                    $objName = "No DBs Found (Host-Level Failure)"
                                }
                            }
                            if (!$objName) { continue }
                            $rowStatus = "NewlyFailedThisCheck"
                            if ((Is-Running $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "RunningAtLatestCheck" }
                            elseif ((Is-Cancelled $latestRunStatus) -and $latestRunUsecs -gt $effectiveUsecs) { $rowStatus = "CancelledAfterFailure" }
                            $runKey = "$($cluster.Id)|$pgId|$ok|$runType|$effectiveUsecs"
                            if (!$failedKeysByKey.ContainsKey($ok)) { $failedKeysByKey[$ok] = New-Object 'System.Collections.Generic.HashSet[string]' }
                            [void]$failedKeysByKey[$ok].Add($runKey)
                            if (!$latestByKey.ContainsKey($ok)) {
                                $latestByKey[$ok] = New-ObjectTrackingRow -Incident $Incident -WindowKey $WindowKey -Cluster $cluster -Env $env -ProtectionGroup $pg -ObjectKey $ok -HostName $hostName -ObjectName $objName -ObjectType $objType -RunType $runType -StartUsecs $startUsecs -EndUsecs $endUsecs -Message $msg -Status $rowStatus -LatestRunStatus $latestRunStatus -LatestRunUsecs $latestRunUsecs -FailedRunKeys @($runKey)
                            }
                        }
                    }
                    foreach ($k in $latestByKey.Keys) {
                        if ($failedKeysByKey.ContainsKey($k)) { Update-RowFailureKeys $latestByKey[$k] @($failedKeysByKey[$k]) }
                        $rows += $latestByKey[$k]
                    }
                }
            }
        }
    }
    @(Merge-Latest $rows)
}

function Display-ObjectName($r) {
    $name = Clean $r.ObjectName
    $type = Clean $r.ObjectType
    $pg = Clean $r.ProtectionGroup
    if (!$name) { return "" }
    if ($type -eq "ProtectionGroup") { return "" }
    if (($name -eq $pg) -and (!$type -or $type -eq "ProtectionGroup")) { return "" }
    return $name
}

function Display-ObjectType($r, [string]$DisplayName) {
    $type = Clean $r.ObjectType
    if (!$DisplayName) { return "" }
    if ($type -eq "ProtectionGroup") { return "" }
    return $type
}

function Convert-LifecycleRows($Rows) {
    foreach ($r in @($Rows)) {
        $dn = Display-ObjectName $r
        $dt = Display-ObjectType $r $dn
        [pscustomobject]@{
            Cluster = Clean $r.Cluster
            ProtectionGroup = Clean $r.ProtectionGroup
            Environment = Clean $r.Environment
            Host = Clean $r.Host
            ObjectName = $dn
            ObjectType = $dt
            RunType = Clean $r.RunType
            Status = Clean $r.Status
            OldestFailedET = Clean $r.FirstFailedET
            NewestFailedET = Clean $r.LastFailedET
            LatestSuccessET = Clean $r.ClearedET
            FailureRuns = Clean $r.ConsecutiveFailureCount
            Message = Clean $r.Message
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

function Rerun-Command([string]$Incident, [string]$Cluster, [int]$RunLimit) {
    $line = '.\Cohesity_Backup_Failure_INC_Status_Update.ps1'
    if ($Incident) { $line += ' -IncidentNumber "' + $Incident + '"' }
    if ($Cluster) { $line += ' -ClusterName "' + $Cluster + '"' }
    if ($RunLimit -ne 30) { $line += ' -NumRuns ' + $RunLimit }
    "cd X:\PowerShell\Cohesity_API_Scripts\backup_failures`n$line"
}

function Open-Grid([string]$Folder) {
    if (!(Get-Command Out-GridView -ErrorAction SilentlyContinue)) { Write-Warning "Out-GridView is not available. CSV files were still generated."; return }
    $p = Join-Path $Folder "incident_lifecycle.csv"
    if (!(Test-Path $p)) { Write-Warning "incident_lifecycle.csv was not found."; return }
    $rows = Import-ReportCsv $p
    if ($rows.Count -eq 0) { Write-Warning "incident_lifecycle.csv has no rows."; return }
    $rows | Out-GridView -Title "Cohesity - Incident Lifecycle"
}

function Remove-TemporaryOutputs([string]$Folder) {
    foreach ($name in @("current_failures.csv","cleared_by_success.csv","worknotes.txt","summary.txt")) {
        $p = Join-Path $Folder $name
        if (Test-Path $p) { Remove-Item -Path $p -Force -ErrorAction SilentlyContinue }
    }
}

function Write-FinalOutputs([string]$Folder, [int]$RunLimit) {
    $statePath = Join-Path $Folder "state.json"
    $state = Read-JsonFile $statePath
    $currentPath = Join-Path $Folder "current_failures.csv"
    $clearedPath = Join-Path $Folder "cleared_by_success.csv"
    $lifecyclePath = Join-Path $Folder "incident_lifecycle.csv"

    $incident = if ($state -and $state.IncidentNumber) { Clean $state.IncidentNumber } else { Split-Path $Folder -Leaf }
    $windowKey = if ($state -and $state.WindowKey) { Clean $state.WindowKey } else { "" }

    $objectCurrent = @(Collect-ObjectLevelCurrentFailures -Incident $incident -WindowKey $windowKey -RunLimit $RunLimit)
    if ($objectCurrent.Count -gt 0) {
        Save-Csv $currentPath $objectCurrent $script:InternalColumns
    }

    $reconciled = Reconcile (Import-ReportCsv $currentPath) (Import-ReportCsv $clearedPath)
    $active = @($reconciled.Active)
    $cleared = @($reconciled.Cleared)
    $lifecycle = @($reconciled.Lifecycle)
    $moved = @($reconciled.Moved)

    if ($state) {
        Set-Prop $state "CurrentOpenFailures" $active
        Set-Prop $state "ClearedBySuccess" $cleared
        Set-Prop $state "LifecycleRows" $lifecycle
        Set-Prop $state "LatestSuccessReconciledClearCount" $moved.Count
        $lastCleared = @()
        if ($state.PSObject.Properties["LastRunClearedBySuccess"]) { $lastCleared += @($state.LastRunClearedBySuccess) }
        $lastCleared += $moved
        Set-Prop $state "LastRunClearedBySuccess" @(Merge-Latest $lastCleared)
        Write-JsonFile $state $statePath
    }

    $lifecycleExport = @(Convert-LifecycleRows $lifecycle | Sort-Object Cluster,ProtectionGroup,Environment,@{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
    Save-Csv $lifecyclePath $lifecycleExport $script:FinalColumns

    $windowLabel = if ($state -and $state.WindowLabel) { Clean $state.WindowLabel } else { "" }
    $generated = if ($state -and $state.LastRunET) { Clean $state.LastRunET } else { (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }
    $warnings = if ($state -and $state.Warnings) { @($state.Warnings) } else { @() }

    $activeStatuses = @("NewlyFailedThisCheck","OlderStillFailing","CurrentStillFailing","CarriedForwardStillFailing","ReFailedAfterClear","RunningAtLatestCheck","CancelledAfterFailure","UnknownNeedsReview")
    $activeExport = @($lifecycleExport | Where-Object { $activeStatuses -contains $_.Status } | Sort-Object @{Expression={Date-Sort $_.NewestFailedET};Descending=$true})
    $newlyCleared = @($lifecycleExport | Where-Object { $_.Status -eq "NewlyClearedThisCheck" } | Sort-Object @{Expression={Date-Sort $_.LatestSuccessET};Descending=$true})
    $previouslyCleared = @($lifecycleExport | Where-Object { $_.Status -eq "ClearedByLaterSuccess" })
    $expectedLifecycle = $activeExport.Count + $newlyCleared.Count + $previouslyCleared.Count

    $activeTally = "Active breakdown tally: $($activeExport.Count) active/unresolved lifecycle rows."
    $lifecycleTally = if ($expectedLifecycle -eq $lifecycleExport.Count) { "Lifecycle tally: $($activeExport.Count) active/unresolved + $($newlyCleared.Count) newly cleared this check + $($previouslyCleared.Count) previously cleared retained = $($lifecycleExport.Count) total lifecycle rows." } else { "Lifecycle tally requires review: expected $expectedLifecycle rows, but incident_lifecycle.csv has $($lifecycleExport.Count) rows." }
    $successRecon = if ($moved.Count -gt 0) { "Latest-success reconciliation: $($moved.Count) active row(s) moved to NewlyClearedThisCheck because a later successful backup was found." } else { "Latest-success reconciliation: no active rows had a later successful backup." }

    $apiStatus = if ($warnings.Count -gt 0) { "Incomplete - $($warnings.Count) collection warning(s) recorded. See Incomplete Collection section." } else { "Complete - all collected scopes returned without recorded lookup warnings." }
    $followUp = if ($warnings.Count -gt 0) { "Retry Failed Collection Scope:`nRun the command below to refresh the incident output after the timed-out Cohesity API scope is available.`n`n$(Rerun-Command $incident $ClusterName $RunLimit)`n`nAfter the rerun completes, use the refreshed worknotes_summary.txt and incident_lifecycle.csv for the incident update." } else { "Retry Failed Collection Scope:`n- Not required for this run." }

    $failureText = Format-Rows $activeExport $script:FailureColumns
    $successText = Format-Rows (@($newlyCleared | Select-Object -Property $script:SuccessColumns)) $script:SuccessColumns

    @"
Cohesity Backup Failure Incident Update

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Cohesity API Collection Status: $apiStatus
Scope: latest $RunLimit runs per protection group/run type.

Do Not Edit Generated Files:
- Do not manually edit incident_lifecycle.csv, worknotes_summary.txt, closing_summary.txt, or state.json.
- If the output looks incorrect, stale, or incomplete, rerun the script and use the refreshed files.

Summary Counts:
- Active / unresolved failures: $($activeExport.Count)
- Newly cleared this check: $($newlyCleared.Count)
- Previously cleared rows retained in lifecycle CSV: $($previouslyCleared.Count)
- Total lifecycle rows tracked: $($lifecycleExport.Count)

Tally Check:
- $activeTally
- $lifecycleTally
- $successRecon

Team Focus:
- Focus on object-level rows in the Failure Section.
- Success section only lists rows newly cleared in this check.

Failure Section:
$failureText

Success Section:
$successText

Incomplete Collection:
$(Format-Warnings $warnings)

$followUp

Files to Attach / Update:
- worknotes_summary.txt
- incident_lifecycle.csv
- closing_summary.txt, for closure or handoff

Script Memory:
- state.json is required by the script for lifecycle tracking. Do not manually edit or attach it.
"@ | Set-Content -Path (Join-Path $Folder "worknotes_summary.txt") -Encoding UTF8

    @"
Backup Failure Incident Closure Summary

Incident: $incident
Compute Window: $windowLabel
Generated At: $generated ET
Cohesity API Collection Status: $apiStatus
Scope: latest $RunLimit runs per protection group/run type.

Closure Counts:
- Active / unresolved failures: $($activeExport.Count)
- Newly cleared this check: $($newlyCleared.Count)
- Previously cleared rows retained in lifecycle CSV: $($previouslyCleared.Count)
- Total lifecycle rows tracked: $($lifecycleExport.Count)

Tally Check:
- $activeTally
- $lifecycleTally
- $successRecon

Failure Section:
$failureText

Success Section:
$successText

Carry Forward / Handoff:
$(if ($activeExport.Count -eq 0) { "No active backup failures remain based on the latest saved state." } else { "$($activeExport.Count) active/unresolved rows remain in incident_lifecycle.csv and should be carried forward or separately tracked." })

Incomplete Collection:
$(Format-Warnings $warnings)

Evidence Files:
- incident_lifecycle.csv
- worknotes_summary.txt
- closing_summary.txt

Script Memory:
- state.json is required by the script for lifecycle tracking. Do not manually edit or attach it.
"@ | Set-Content -Path (Join-Path $Folder "closing_summary.txt") -Encoding UTF8

    Remove-TemporaryOutputs $Folder

    Write-Host ""
    Write-Host "Final Normalized Summary (matches worknotes_summary.txt):"
    Write-Host "Cohesity API Collection Status : $apiStatus"
    Write-Host "Active / Unresolved Failures   : $($activeExport.Count)"
    Write-Host "Newly Cleared This Check       : $($newlyCleared.Count)"
    Write-Host "Previously Cleared Retained    : $($previouslyCleared.Count)"
    Write-Host "Total Lifecycle Rows           : $($lifecycleExport.Count)"
    Write-Host "Incomplete Collection Warnings : $($warnings.Count)"
    Write-Host "Tally Check:"
    Write-Host "  $activeTally"
    Write-Host "  $lifecycleTally"
    Write-Host "  $successRecon"
}

$target = Join-Path $PSScriptRoot "Get-CohesityBackupFailureWindowConsolidator.ps1"
if (!(Test-Path $target)) { throw "Main implementation script not found: $target" }

$targetParams = @{}
foreach ($k in $PSBoundParameters.Keys) {
    if ($k -ne "ShowGrid") { $targetParams[$k] = $PSBoundParameters[$k] }
}

Write-Host ""
Write-Host "Collection stage will run first. The final normalized summary printed after post-processing is the source of truth and matches worknotes_summary.txt."
Write-Host ""

& $target @targetParams
$mainExitCode = $LASTEXITCODE

try {
    $folder = Get-ReportFolder -Root $OutputRoot -Inc $IncidentNumber
    if ($folder) {
        Write-FinalOutputs -Folder $folder -RunLimit $NumRuns
        Write-Host ""
        Write-Host "Final operator-facing files:"
        Write-Host (Join-Path $folder "worknotes_summary.txt")
        Write-Host (Join-Path $folder "incident_lifecycle.csv")
        Write-Host (Join-Path $folder "closing_summary.txt")
        Write-Host "Script memory retained, do not edit:"
        Write-Host (Join-Path $folder "state.json")
        if ($ShowGrid) { Open-Grid $folder }
    } else {
        Write-Warning "Unable to locate incident output folder for text normalization."
    }
} catch {
    Write-Warning "Text output normalization failed: $($_.Exception.Message)"
}

exit $mainExitCode
