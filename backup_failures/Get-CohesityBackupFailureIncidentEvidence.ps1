<#
Standalone incident evidence script for Cohesity backup failures.
First run in a window creates the failed-object baseline.
Later runs in the same window check only that baseline, not every PG again.
GET-only. No Excel. No ServiceNow update.
#>
[CmdletBinding()]
param(
  [string]$BaseUrl = "https://helios.cohesity.com",
  [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
  [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
  [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
  [int]$NumRuns = 30,
  [string]$ClusterName = "",
  [string]$IncidentNumber = ""
)

$ErrorActionPreference = "Stop"

function Get-EtZone { try { [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") } catch { [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") } }
$script:EtZone = Get-EtZone

function Get-Value($Object, [string]$Name, $Default = $null) {
  if ($null -eq $Object) { return $Default }
  $Property = $Object.PSObject.Properties[$Name]
  if ($Property) { return $Property.Value }
  return $Default
}
function As-List($Value) { if ($null -eq $Value) { @() } elseif ($Value -is [array]) { @($Value) } else { @($Value) } }
function Clean-Value($Value) { if ($null -eq $Value) { return "" }; if ($Value -is [array]) { $Value = $Value -join " | " }; return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim() }
function Usecs-ToEt($Usecs) { if (-not $Usecs) { return "" }; $Milliseconds = [int64]([double]$Usecs / 1000); $Utc = [DateTimeOffset]::FromUnixTimeMilliseconds($Milliseconds).UtcDateTime; ([TimeZoneInfo]::ConvertTimeFromUtc($Utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss") }
function Et-ToUsecs([datetime]$EtTime) { $Utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtTime, [DateTimeKind]::Unspecified), $script:EtZone); [int64](([DateTimeOffset]::new($Utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000) }
function Read-JsonFile([string]$Path) { if (Test-Path $Path) { $Raw = Get-Content $Path -Raw; if ($Raw) { return $Raw | ConvertFrom-Json } }; return $null }
function Write-JsonFile($Object, [string]$Path) { $Folder = Split-Path $Path -Parent; if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }; $Object | ConvertTo-Json -Depth 80 | Set-Content $Path -Encoding UTF8 }
function Write-CsvFile($Rows, [string]$Path, [string[]]$Columns) { $Folder = Split-Path $Path -Parent; if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }; $List = @($Rows); if ($List.Count -eq 0) { ($Columns -join ",") | Set-Content $Path -Encoding UTF8 } else { $List | Select-Object $Columns | Export-Csv $Path -NoTypeInformation -Encoding UTF8 } }
function Query-String([hashtable]$Items) { (($Items.GetEnumerator() | ForEach-Object { [uri]::EscapeDataString([string]$_.Key) + "=" + [uri]::EscapeDataString([string]$_.Value) }) -join "&") }
function Invoke-GetJson([string]$Uri, [hashtable]$Headers) { $Response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing; if (!$Response.Content) { return $null }; $Response.Content | ConvertFrom-Json }

function Get-WindowNow {
  $NowEt = [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)
  if ($NowEt.Hour -lt 18) { $Start = $NowEt.Date.AddDays(-1).AddHours(18) } else { $Start = $NowEt.Date.AddHours(18) }
  $End = $Start.AddDays(1)
  $StartDate = $Start.ToString("yyyy-MM-dd")
  $EndDate = $End.ToString("yyyy-MM-dd")
  [pscustomobject]@{
    Key = "${StartDate}_1800ET"
    Label = "$StartDate 18:00 ET -> $EndDate 18:00 ET"
    StartUsecs = Et-ToUsecs $Start
    EndUsecs = Et-ToUsecs $End
    GeneratedET = ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
  }
}

function Resolve-Incident($Window) {
  if (!(Test-Path $OutputRoot)) { New-Item $OutputRoot -ItemType Directory -Force | Out-Null }
  $RegistryPath = Join-Path $OutputRoot "BackupFailure_IncidentRegistry.json"
  $Registry = Read-JsonFile $RegistryPath
  if (!$Registry) { $Registry = [pscustomobject]@{ WindowSource = "backup_failures/compute_window.js"; Windows = [pscustomobject]@{} } }
  $Existing = $Registry.Windows.PSObject.Properties[$Window.Key]
  if ($Existing) { return $Existing.Value }
  $Incident = $IncidentNumber
  if (!$Incident) { $Incident = Read-Host "Enter incident number for this backup-failure window" }
  $Incident = $Incident.Trim().ToUpper()
  if ($Incident -notmatch '^INC[0-9A-Z]+$') { throw "Invalid incident number: $Incident" }
  $Entry = [pscustomobject]@{ IncidentNumber = $Incident; WindowKey = $Window.Key; WindowLabel = $Window.Label; FirstRunET = $Window.GeneratedET; LastRunET = $Window.GeneratedET; OutputFolder = (Join-Path $OutputRoot $Incident) }
  $Registry.Windows | Add-Member NoteProperty $Window.Key $Entry -Force
  Write-JsonFile $Registry $RegistryPath
  return $Entry
}

function Get-EnvironmentCode($ProtectionGroup) {
  $Code = [string](Get-Value $ProtectionGroup "environment" "")
  if (!$Code) { $Types = As-List (Get-Value $ProtectionGroup "environmentTypes" @()); if ($Types.Count -gt 0) { $Code = [string]$Types[0] } }
  return $Code
}
function Get-EnvironmentLabel([string]$Code) { switch ($Code) { "kOracle" { "Oracle" } "kSQL" { "SQL" } "kPhysical" { "Physical" } "kGenericNas" { "NAS" } "kIsilon" { "Isilon" } "kHyperV" { "HyperV" } "kAcropolis" { "Acropolis" } default { if ($Code) { $Code } else { "Unknown" } } } }
function Is-Fail([string]$Status) { $Status -in @("Failed", "kFailed") }
function Is-Ok([string]$Status) { $Status -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning") }
function Is-RelevantObject($EnvironmentCode, $Object) {
  $Type = [string](Get-Value $Object "objectType" "")
  switch ($EnvironmentCode) {
    "kOracle" { $Type -in @("kDatabase", "kHost") }
    "kSQL" { $Type -in @("kDatabase", "kHost") }
    "kPhysical" { $Type -eq "kHost" }
    "kGenericNas" { $Type -eq "kHost" }
    "kIsilon" { $Type -eq "kHost" }
    "kHyperV" { $Type -eq "kVirtualMachine" }
    "kAcropolis" { $Type -eq "kVirtualMachine" }
    default { $true }
  }
}
function Get-FailedAttempts($RunObject) { try { @(Get-Value $RunObject.localSnapshotInfo "failedAttempts" @()) } catch { @() } }
function Get-FailureMessage($RunObject) { ((Get-FailedAttempts $RunObject | ForEach-Object { Clean-Value (Get-Value $_ "message" "") } | Where-Object { $_ }) -join " | ") }
function New-ObjectKey($ClusterId, $EnvironmentCode, $ProtectionGroupId, $ObjectId, $ClusterDisplayName, $ProtectionGroupName, $SourceHostName, $ObjectName) { if ($ObjectId) { "$ClusterId|$EnvironmentCode|$ProtectionGroupId|$ObjectId" } else { "$ClusterDisplayName|$EnvironmentCode|$ProtectionGroupName|$SourceHostName|$ObjectName" } }

function New-EventRow($Incident, $Window, $ClusterId, $ClusterDisplayName, $EnvironmentCode, $ProtectionGroupId, $ProtectionGroupName, $SourceHostName, $ObjectId, $ObjectName, $ObjectType, $RunType, $Kind, $Usecs, $Message) {
  $Key = New-ObjectKey $ClusterId $EnvironmentCode $ProtectionGroupId $ObjectId $ClusterDisplayName $ProtectionGroupName $SourceHostName $ObjectName
  [pscustomobject]@{ IncidentNumber = $Incident; WindowKey = $Window.Key; ClusterId = $ClusterId; ProtectionGroupId = $ProtectionGroupId; Cluster = $ClusterDisplayName; Environment = (Get-EnvironmentLabel $EnvironmentCode); ProtectionGroup = $ProtectionGroupName; SourceHostName = $SourceHostName; ObjectId = $ObjectId; ObjectName = $ObjectName; ObjectType = $ObjectType; RunType = $RunType; EventKind = $Kind; EventTimeET = (Usecs-ToEt $Usecs); EventTimeUsecs = $Usecs; Message = $Message; ObjectKey = $Key }
}

function Expand-RunEvents($Incident, $Window, $Cluster, $ProtectionGroup, $Run, $Info) {
  $Status = [string](Get-Value $Info "status" "")
  if (Is-Fail $Status) { $Kind = "Failed" } elseif (Is-Ok $Status) { $Kind = "Success" } else { return @() }
  $StartUsecs = [int64](Get-Value $Info "startTimeUsecs" 0)
  $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
  $Usecs = if ($EndUsecs -gt 0) { $EndUsecs } else { $StartUsecs }
  if ($Usecs -lt $Window.StartUsecs -or $Usecs -ge $Window.EndUsecs) { return @() }
  $ClusterId = [string](Get-Value $Cluster "clusterId" "")
  $ClusterDisplayName = [string](Get-Value $Cluster "name" "")
  if (!$ClusterDisplayName) { $ClusterDisplayName = [string](Get-Value $Cluster "clusterName" "Unknown-$ClusterId") }
  $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
  $ProtectionGroupName = [string](Get-Value $ProtectionGroup "name" "Unknown PG")
  $EnvironmentCode = Get-EnvironmentCode $ProtectionGroup
  $RunType = [string](Get-Value $Info "runType" "")
  $Message = Clean-Value (Get-Value $Info "messages" "")
  $RunObjects = As-List (Get-Value $Run "objects" @())
  if ($RunObjects.Count -eq 0) { return @(New-EventRow $Incident $Window $ClusterId $ClusterDisplayName $EnvironmentCode $ProtectionGroupId $ProtectionGroupName "" "" $ProtectionGroupName "ProtectionGroup" $RunType $Kind $Usecs $Message) }
  $IdToName = @{}
  foreach ($RunObject in $RunObjects) { $Object = Get-Value $RunObject "object" $null; $ObjectId = [string](Get-Value $Object "id" ""); $Name = [string](Get-Value $Object "name" ""); if ($ObjectId -and $Name) { $IdToName[$ObjectId] = $Name } }
  $Rows = @()
  foreach ($RunObject in $RunObjects) {
    $Object = Get-Value $RunObject "object" $null
    if (!$Object -or !(Is-RelevantObject $EnvironmentCode $Object)) { continue }
    if ($Kind -eq "Failed" -and (Get-FailedAttempts $RunObject).Count -eq 0) { continue }
    $ObjectType = [string](Get-Value $Object "objectType" "")
    $ObjectId = [string](Get-Value $Object "id" "")
    $ObjectName = [string](Get-Value $Object "name" "")
    $SourceHostName = ""
    if ($EnvironmentCode -in @("kOracle", "kSQL") -and $ObjectType -eq "kHost") { $SourceHostName = $ObjectName; if ($Kind -eq "Failed") { $ObjectName = "No DBs Found (Host-Level Failure)" } }
    if ($EnvironmentCode -in @("kOracle", "kSQL") -and $ObjectType -eq "kDatabase") { $SourceId = [string](Get-Value $Object "sourceId" ""); if ($SourceId -and $IdToName.ContainsKey($SourceId)) { $SourceHostName = $IdToName[$SourceId] } }
    $FinalMessage = $Message
    if ($Kind -eq "Failed") { $FailureMessage = Get-FailureMessage $RunObject; if ($FailureMessage) { $FinalMessage = $FailureMessage } }
    $Rows += New-EventRow $Incident $Window $ClusterId $ClusterDisplayName $EnvironmentCode $ProtectionGroupId $ProtectionGroupName $SourceHostName $ObjectId $ObjectName $ObjectType $RunType $Kind $Usecs $FinalMessage
  }
  return @($Rows)
}

function Get-PgRuns($ProtectionGroupId, $Headers) {
  $RunQuery = Query-String @{ numRuns = $NumRuns; excludeNonRestorableRuns = "false"; includeObjectDetails = "true" }
  Invoke-GetJson "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?$RunQuery" $Headers
}

function New-OutputRow($Event, [string]$Status, [string]$FirstFailedET, [string]$LastFailedET, [string]$RecoveredET, [int]$Count) {
  [pscustomobject]@{ IncidentNumber = $Event.IncidentNumber; WindowKey = $Event.WindowKey; Status = $Status; Cluster = $Event.Cluster; Environment = $Event.Environment; ProtectionGroup = $Event.ProtectionGroup; SourceHostName = $Event.SourceHostName; ObjectName = $Event.ObjectName; ObjectType = $Event.ObjectType; RunType = $Event.RunType; FirstFailedET = $FirstFailedET; LastFailedET = $LastFailedET; RecoveredET = $RecoveredET; ConsecutiveFailureCount = $Count; Message = $Event.Message; ObjectKey = $Event.ObjectKey; ClusterId = $Event.ClusterId; ProtectionGroupId = $Event.ProtectionGroupId; ObjectId = $Event.ObjectId }
}

if (!(Test-Path $HelperPath)) { throw "Missing API key helper: $HelperPath" }
if (!(Test-Path $EncryptedFile)) { throw "Missing encrypted key file: $EncryptedFile" }
. $HelperPath
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
if (!$ApiKey) { throw "API key is blank" }

$Window = Get-WindowNow
$IncidentLock = Resolve-Incident $Window
$Incident = $IncidentLock.IncidentNumber
$OutputFolder = $IncidentLock.OutputFolder
if (!(Test-Path $OutputFolder)) { New-Item $OutputFolder -ItemType Directory -Force | Out-Null }
$StatePath = Join-Path $OutputFolder "state.json"
$PreviousState = Read-JsonFile $StatePath
$IsBaselineRun = (!$PreviousState -or !(Get-Value $PreviousState "BaselineFailures" $null) -or (As-List (Get-Value $PreviousState "BaselineFailures" @())).Count -eq 0)

$CommonHeaders = @{ apiKey = $ApiKey; accept = "application/json" }
$Events = @()
$Warnings = @()
$Clusters = @((Invoke-GetJson "$BaseUrl/v2/mcm/cluster-mgmt/info" $CommonHeaders).cohesityClusters)
if ($ClusterName) { $Clusters = @($Clusters | Where-Object { (Get-Value $_ "name" "") -eq $ClusterName -or (Get-Value $_ "clusterName" "") -eq $ClusterName -or (Get-Value $_ "displayName" "") -eq $ClusterName }) }

if ($IsBaselineRun) {
  $Mode = "FullBaseline"
  foreach ($Cluster in $Clusters) {
    $ClusterId = [string](Get-Value $Cluster "clusterId" "")
    $Headers = @{ apiKey = $ApiKey; accept = "application/json"; accessClusterId = $ClusterId }
    try { $ProtectionGroups = @((Invoke-GetJson "$BaseUrl/v2/data-protect/protection-groups?$(Query-String @{isDeleted='false';isPaused='false';isActive='true'})" $Headers).protectionGroups) } catch { $Warnings += $_.Exception.Message; continue }
    foreach ($ProtectionGroup in $ProtectionGroups) {
      $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
      try { $Runs = @((Get-PgRuns $ProtectionGroupId $Headers).runs) } catch { $Warnings += $_.Exception.Message; continue }
      foreach ($Run in $Runs) { foreach ($Info in (As-List (Get-Value $Run "localBackupInfo" @()))) { $Events += Expand-RunEvents $Incident $Window $Cluster $ProtectionGroup $Run $Info } }
    }
  }
} else {
  $Mode = "TargetedFollowUp"
  $Baseline = As-List (Get-Value $PreviousState "BaselineFailures" @())
  $Targets = @($Baseline | Group-Object ClusterId, ProtectionGroupId)
  foreach ($Target in $Targets) {
    $Sample = $Target.Group[0]
    $ClusterId = [string](Get-Value $Sample "ClusterId" "")
    $ProtectionGroupId = [string](Get-Value $Sample "ProtectionGroupId" "")
    $Cluster = $Clusters | Where-Object { [string](Get-Value $_ "clusterId" "") -eq $ClusterId } | Select-Object -First 1
    if (!$Cluster) { continue }
    $Headers = @{ apiKey = $ApiKey; accept = "application/json"; accessClusterId = $ClusterId }
    try { $ProtectionGroup = (Invoke-GetJson "$BaseUrl/v2/data-protect/protection-groups?$(Query-String @{isDeleted='false';isPaused='false';isActive='true'})" $Headers).protectionGroups | Where-Object { [string](Get-Value $_ "id" "") -eq $ProtectionGroupId } | Select-Object -First 1 } catch { $Warnings += $_.Exception.Message; continue }
    if (!$ProtectionGroup) { continue }
    try { $Runs = @((Get-PgRuns $ProtectionGroupId $Headers).runs) } catch { $Warnings += $_.Exception.Message; continue }
    foreach ($Run in $Runs) { foreach ($Info in (As-List (Get-Value $Run "localBackupInfo" @()))) { $Events += Expand-RunEvents $Incident $Window $Cluster $ProtectionGroup $Run $Info } }
  }
}

$Current = @()
$Recovered = @()
$BaselineFailures = if ($IsBaselineRun) { @() } else { As-List (Get-Value $PreviousState "BaselineFailures" @()) }

if ($IsBaselineRun) {
  foreach ($Group in ($Events | Sort-Object ObjectKey, EventTimeUsecs | Group-Object ObjectKey)) {
    $ObjectEvents = @($Group.Group | Sort-Object EventTimeUsecs)
    $Failures = @($ObjectEvents | Where-Object { $_.EventKind -eq "Failed" })
    if ($Failures.Count -eq 0) { continue }
    $First = $Failures[0]
    $Last = $Failures[-1]
    $LaterSuccess = @($ObjectEvents | Where-Object { $_.EventKind -eq "Success" -and $_.EventTimeUsecs -gt $Last.EventTimeUsecs } | Sort-Object EventTimeUsecs | Select-Object -First 1)
    if ($LaterSuccess.Count -eq 0) { $Row = New-OutputRow $Last "StillFailing" $First.EventTimeET $Last.EventTimeET "" 1; $Current += $Row; $BaselineFailures += $Row } else { $Recovered += New-OutputRow $Last "Recovered" $First.EventTimeET $Last.EventTimeET $LaterSuccess[0].EventTimeET 0 }
  }
  $NewFailures = @($Current)
  $NewRecoveries = @()
} else {
  $PreviouslyRecovered = @{}
  foreach ($Item in (As-List (Get-Value $PreviousState "Recovered" @()))) { $Key = [string](Get-Value $Item "ObjectKey" ""); if ($Key) { $PreviouslyRecovered[$Key] = $true } }
  foreach ($BaseFailure in $BaselineFailures) {
    $Key = [string](Get-Value $BaseFailure "ObjectKey" "")
    $LastFailedET = [string](Get-Value $BaseFailure "LastFailedET" "")
    $LastFailedUsecs = 0
    if ($LastFailedET) { $LastFailedUsecs = Et-ToUsecs ([datetime]$LastFailedET) }
    $ObjectEvents = @($Events | Where-Object { $_.ObjectKey -eq $Key } | Sort-Object EventTimeUsecs)
    $Success = @($ObjectEvents | Where-Object { $_.EventKind -eq "Success" -and $_.EventTimeUsecs -gt $LastFailedUsecs } | Select-Object -First 1)
    if ($Success.Count -gt 0) { $Recovered += New-OutputRow $Success[0] "Recovered" ([string](Get-Value $BaseFailure "FirstFailedET" "")) ([string](Get-Value $BaseFailure "LastFailedET" "")) $Success[0].EventTimeET 0 } else { $Current += $BaseFailure }
  }
  $NewFailures = @()
  $NewRecoveries = @($Recovered | Where-Object { !$PreviouslyRecovered.ContainsKey($_.ObjectKey) })
}

$Columns = "IncidentNumber", "WindowKey", "Status", "Cluster", "Environment", "ProtectionGroup", "SourceHostName", "ObjectName", "ObjectType", "RunType", "FirstFailedET", "LastFailedET", "RecoveredET", "ConsecutiveFailureCount", "Message", "ObjectKey"
Write-CsvFile $Current (Join-Path $OutputFolder "current_failures.csv") $Columns
Write-CsvFile $Recovered (Join-Path $OutputFolder "recovered.csv") $Columns
Write-CsvFile $NewFailures (Join-Path $OutputFolder "new_failures.csv") $Columns
Write-CsvFile $NewRecoveries (Join-Path $OutputFolder "new_recoveries.csv") $Columns

$Worknotes = @"
Backup Failure Evidence

Incident: $Incident
Window: $($Window.Label)
Mode: $Mode
Generated At: $($Window.GeneratedET) ET

Summary:
- Current baseline failures still failing: $(@($Current).Count)
- Baseline failures recovered: $(@($Recovered).Count)
- New failures: $(@($NewFailures).Count)
- New recoveries since last run: $(@($NewRecoveries).Count)

Important:
- First run creates the baseline by scanning all active protection groups.
- Later runs in the same window check only the baseline failed objects.
- new_failures.csv is populated only on the baseline run. In targeted follow-up mode it is intentionally header-only.
- Cohesity calls are GET-only.
- No Excel output is generated.
"@
if ($Warnings.Count -gt 0) { $Worknotes += "`nWarnings:`n" + (($Warnings | ForEach-Object { "- $_" }) -join "`n") }
$Worknotes | Set-Content (Join-Path $OutputFolder "worknotes.txt") -Encoding UTF8

$State = [pscustomobject]@{ IncidentNumber = $Incident; WindowKey = $Window.Key; WindowLabel = $Window.Label; Mode = $Mode; LastRunET = $Window.GeneratedET; BaselineFailures = @($BaselineFailures); CurrentFailures = @($Current); Recovered = @($Recovered) }
Write-JsonFile $State $StatePath

Write-Host "`nIncident: $Incident`nWindow  : $($Window.Label)`nMode    : $Mode`nOutput  : $OutputFolder`nCreated : current_failures.csv, recovered.csv, new_failures.csv, new_recoveries.csv, worknotes.txt, state.json"
