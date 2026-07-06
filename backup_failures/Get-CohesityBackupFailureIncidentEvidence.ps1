<#
Standalone incident evidence script for Cohesity backup failures.
Baseline source is the latest CSV created by backup_failures/Cohesity_Backup_Failures:
X:\PowerShell\Data\Cohesity\BackupFailures\BackupFailures_AllEnvironments_*.csv

This script does not create the baseline by scanning every PG.
It reads the baseline CSV, then checks only those baseline cluster/PG rows for recovery.
GET-only. No Excel. No ServiceNow update.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "https://helios.cohesity.com",
    [string]$BaselineDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures",
    [string]$OutputRoot = "X:\PowerShell\Data\Cohesity\BackupFailureWindow",
    [string]$HelperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1",
    [string]$EncryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc",
    [int]$NumRuns = 30,
    [string]$IncidentNumber = "",
    [string]$BaselineCsv = ""
)

$ErrorActionPreference = "Stop"

function Get-EtZone {
    try { [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time") }
    catch { [TimeZoneInfo]::FindSystemTimeZoneById("America/New_York") }
}
$script:EtZone = Get-EtZone

function Get-Value($Object, [string]$Name, $Default = $null) {
    if ($null -eq $Object) { return $Default }
    $Property = $Object.PSObject.Properties[$Name]
    if ($Property) { return $Property.Value }
    return $Default
}

function As-List($Value) {
    if ($null -eq $Value) { @() }
    elseif ($Value -is [array]) { @($Value) }
    else { @($Value) }
}

function Clean-Value($Value) {
    if ($null -eq $Value) { return "" }
    if ($Value -is [array]) { $Value = $Value -join " | " }
    return (([string]$Value -replace "[\r\n]+", " ") -replace "\s+", " ").Replace('"', "'").Trim()
}

function Usecs-ToEt($Usecs) {
    if (-not $Usecs) { return "" }
    $Milliseconds = [int64]([double]$Usecs / 1000)
    $Utc = [DateTimeOffset]::FromUnixTimeMilliseconds($Milliseconds).UtcDateTime
    ([TimeZoneInfo]::ConvertTimeFromUtc($Utc, $script:EtZone)).ToString("yyyy-MM-dd HH:mm:ss")
}

function Et-ToUsecs([datetime]$EtTime) {
    $Utc = [TimeZoneInfo]::ConvertTimeToUtc([datetime]::SpecifyKind($EtTime, [DateTimeKind]::Unspecified), $script:EtZone)
    [int64](([DateTimeOffset]::new($Utc, [TimeSpan]::Zero)).ToUnixTimeMilliseconds() * 1000)
}

function Read-JsonFile([string]$Path) {
    if (Test-Path $Path) {
        $Raw = Get-Content $Path -Raw
        if ($Raw) { return $Raw | ConvertFrom-Json }
    }
    return $null
}

function Write-JsonFile($Object, [string]$Path) {
    $Folder = Split-Path $Path -Parent
    if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }
    $Object | ConvertTo-Json -Depth 80 | Set-Content $Path -Encoding UTF8
}

function Write-CsvFile($Rows, [string]$Path, [string[]]$Columns) {
    $Folder = Split-Path $Path -Parent
    if (!(Test-Path $Folder)) { New-Item $Folder -ItemType Directory -Force | Out-Null }
    $List = @($Rows)
    if ($List.Count -eq 0) {
        ($Columns -join ",") | Set-Content $Path -Encoding UTF8
    } else {
        $List | Select-Object $Columns | Export-Csv $Path -NoTypeInformation -Encoding UTF8
    }
}

function Query-String([hashtable]$Items) {
    (($Items.GetEnumerator() | ForEach-Object {
        [uri]::EscapeDataString([string]$_.Key) + "=" + [uri]::EscapeDataString([string]$_.Value)
    }) -join "&")
}

function Invoke-GetJson([string]$Uri, [hashtable]$Headers) {
    $Response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
    if (!$Response.Content) { return $null }
    $Response.Content | ConvertFrom-Json
}

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
    $Entry = [pscustomobject]@{
        IncidentNumber = $Incident
        WindowKey = $Window.Key
        WindowLabel = $Window.Label
        FirstRunET = $Window.GeneratedET
        LastRunET = $Window.GeneratedET
        OutputFolder = (Join-Path $OutputRoot $Incident)
    }
    $Registry.Windows | Add-Member NoteProperty $Window.Key $Entry -Force
    Write-JsonFile $Registry $RegistryPath
    return $Entry
}

function Find-BaselineCsv {
    if ($BaselineCsv -and (Test-Path $BaselineCsv)) { return (Resolve-Path $BaselineCsv).Path }
    if (!(Test-Path $BaselineDirectory)) { throw "Baseline directory not found: $BaselineDirectory" }
    $Latest = Get-ChildItem -Path $BaselineDirectory -Filter "BackupFailures_AllEnvironments_*.csv" -File |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (!$Latest) {
        throw "No baseline CSV found. Run backup_failures\Cohesity_Backup_Failures first using All environments or Consolidated silent. Expected: $BaselineDirectory\BackupFailures_AllEnvironments_*.csv"
    }
    return $Latest.FullName
}

function New-BaselineKey($Row) {
    $HostValue = Clean-Value (Get-Value $Row "Host" "")
    $ObjectName = Clean-Value (Get-Value $Row "ObjectName" "")
    "$((Clean-Value $Row.Environment))|$((Clean-Value $Row.Cluster))|$((Clean-Value $Row.ProtectionGroup))|$HostValue|$ObjectName|$((Clean-Value $Row.RunType))"
}

function Import-Baseline($CsvPath, $Window, $Incident) {
    $Rows = @(Import-Csv -Path $CsvPath)
    if ($Rows.Count -eq 0) { throw "Baseline CSV has no rows: $CsvPath" }
    foreach ($Row in $Rows) {
        $HostValue = Clean-Value (Get-Value $Row "Host" "")
        $ObjectName = Clean-Value (Get-Value $Row "ObjectName" "")
        [pscustomobject]@{
            IncidentNumber = $Incident
            WindowKey = $Window.Key
            Status = "BaselineFailure"
            Cluster = Clean-Value $Row.Cluster
            Environment = Clean-Value $Row.Environment
            ProtectionGroup = Clean-Value $Row.ProtectionGroup
            SourceHostName = $HostValue
            ObjectName = $ObjectName
            ObjectType = ""
            RunType = Clean-Value $Row.RunType
            FirstFailedET = Clean-Value $Row.EndTime
            LastFailedET = Clean-Value $Row.EndTime
            RecoveredET = ""
            ConsecutiveFailureCount = 1
            Message = Clean-Value $Row.FailedMessage
            BaselineKey = (New-BaselineKey $Row)
        }
    }
}

function Environment-ToFilter($Environment) {
    switch ($Environment) {
        "Oracle" { "kOracle" }
        "SQL" { "kSQL" }
        "Physical" { "kPhysical" }
        "GenericNas" { "kGenericNas" }
        "NAS" { "kGenericNas" }
        "Isilon" { "kIsilon" }
        "HyperV" { "kHyperV" }
        "Acropolis" { "kAcropolis" }
        "RemoteAdapter" { "kRemoteAdapter" }
        default { "" }
    }
}

function Is-Ok([string]$Status) { $Status -in @("Succeeded", "SucceededWithWarning", "kSucceeded", "kSucceededWithWarning") }

function Get-RunObjectName($RunObject, $Environment, $IdToName) {
    $Object = Get-Value $RunObject "object" $null
    if (!$Object) { return [pscustomobject]@{ SourceHostName = ""; ObjectName = "" } }
    $Name = Clean-Value (Get-Value $Object "name" "")
    $Type = [string](Get-Value $Object "objectType" "")
    $SourceHostName = ""
    $ObjectName = $Name
    if ($Environment -in @("Oracle", "SQL") -and $Type -eq "kHost") {
        $SourceHostName = $Name
        $ObjectName = "No DBs Found (Host-Level Failure)"
    }
    if ($Environment -in @("Oracle", "SQL") -and $Type -eq "kDatabase") {
        $SourceId = [string](Get-Value $Object "sourceId" "")
        if ($SourceId -and $IdToName.ContainsKey($SourceId)) { $SourceHostName = $IdToName[$SourceId] }
    }
    [pscustomobject]@{ SourceHostName = $SourceHostName; ObjectName = $ObjectName }
}

function Test-BaselineRecovered($BaselineRow, $Cluster, $ProtectionGroup, $Runs) {
    $BaselineUsecs = 0
    if ($BaselineRow.LastFailedET) { try { $BaselineUsecs = Et-ToUsecs ([datetime]$BaselineRow.LastFailedET) } catch {} }
    $IdToName = @{}
    foreach ($Run in $Runs) {
        foreach ($RunObject in (As-List (Get-Value $Run "objects" @()))) {
            $Object = Get-Value $RunObject "object" $null
            $ObjectId = [string](Get-Value $Object "id" "")
            $ObjectName = Clean-Value (Get-Value $Object "name" "")
            if ($ObjectId -and $ObjectName) { $IdToName[$ObjectId] = $ObjectName }
        }
    }
    foreach ($Run in $Runs) {
        foreach ($Info in (As-List (Get-Value $Run "localBackupInfo" @()))) {
            if (!(Is-Ok ([string](Get-Value $Info "status" "")))) { continue }
            $EndUsecs = [int64](Get-Value $Info "endTimeUsecs" 0)
            if ($EndUsecs -le $BaselineUsecs) { continue }
            if ((Clean-Value (Get-Value $Info "runType" "")) -ne $BaselineRow.RunType) { continue }
            $Objects = As-List (Get-Value $Run "objects" @())
            if ($Objects.Count -eq 0) {
                return Usecs-ToEt $EndUsecs
            }
            foreach ($RunObject in $Objects) {
                $Resolved = Get-RunObjectName $RunObject $BaselineRow.Environment $IdToName
                $CandidateKey = "$($BaselineRow.Environment)|$($BaselineRow.Cluster)|$($BaselineRow.ProtectionGroup)|$($Resolved.SourceHostName)|$($Resolved.ObjectName)|$($BaselineRow.RunType)"
                if ($CandidateKey -eq $BaselineRow.BaselineKey) { return Usecs-ToEt $EndUsecs }
            }
        }
    }
    return ""
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
$SelectedBaselineCsv = if ($PreviousState -and (Get-Value $PreviousState "BaselineCsv" "")) { Get-Value $PreviousState "BaselineCsv" "" } else { Find-BaselineCsv }
$BaselineRows = @(Import-Baseline $SelectedBaselineCsv $Window $Incident)

$PreviouslyRecovered = @{}
if ($PreviousState) {
    foreach ($RecoveredItem in (As-List (Get-Value $PreviousState "Recovered" @()))) {
        $Key = [string](Get-Value $RecoveredItem "BaselineKey" "")
        if ($Key) { $PreviouslyRecovered[$Key] = $true }
    }
}

$CommonHeaders = @{ apiKey = $ApiKey; accept = "application/json" }
$Clusters = @((Invoke-GetJson "$BaseUrl/v2/mcm/cluster-mgmt/info" $CommonHeaders).cohesityClusters)
$ClusterByName = @{}
foreach ($Cluster in $Clusters) {
    $ClusterDisplayName = Clean-Value (@(Get-Value $Cluster "name" "", Get-Value $Cluster "clusterName" "", Get-Value $Cluster "displayName" "") | Where-Object { $_ } | Select-Object -First 1)
    if ($ClusterDisplayName) { $ClusterByName[$ClusterDisplayName] = $Cluster }
}

$Current = @()
$Recovered = @()
$Warnings = @()
$TargetGroups = @($BaselineRows | Group-Object Cluster, Environment, ProtectionGroup)

foreach ($Group in $TargetGroups) {
    $Sample = $Group.Group[0]
    $Cluster = $ClusterByName[$Sample.Cluster]
    if (!$Cluster) { $Warnings += "Cluster not found from baseline: $($Sample.Cluster)"; foreach ($r in $Group.Group) { $Current += $r }; continue }
    $ClusterId = [string](Get-Value $Cluster "clusterId" "")
    $Headers = @{ apiKey = $ApiKey; accept = "application/json"; accessClusterId = $ClusterId }
    $Filter = Environment-ToFilter $Sample.Environment
    if (!$Filter) { $Warnings += "Unknown environment filter for: $($Sample.Environment)"; foreach ($r in $Group.Group) { $Current += $r }; continue }
    try {
        $PgJson = Invoke-GetJson "$BaseUrl/v2/data-protect/protection-groups?$(Query-String @{ environments=$Filter; isDeleted='false'; isPaused='false'; isActive='true' })" $Headers
        $ProtectionGroup = @($PgJson.protectionGroups | Where-Object { (Clean-Value (Get-Value $_ "name" "")) -eq $Sample.ProtectionGroup } | Select-Object -First 1)
    } catch {
        $Warnings += "PG lookup failed for $($Sample.Cluster)/$($Sample.ProtectionGroup): $($_.Exception.Message)"
        foreach ($r in $Group.Group) { $Current += $r }
        continue
    }
    if (!$ProtectionGroup) { $Warnings += "Protection group not found from baseline: $($Sample.Cluster)/$($Sample.ProtectionGroup)"; foreach ($r in $Group.Group) { $Current += $r }; continue }
    $ProtectionGroupId = [string](Get-Value $ProtectionGroup "id" "")
    try {
        $RunQuery = Query-String @{ numRuns=$NumRuns; excludeNonRestorableRuns='false'; includeObjectDetails='true' }
        $Runs = @((Invoke-GetJson "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?$RunQuery" $Headers).runs)
    } catch {
        $Warnings += "Runs lookup failed for $($Sample.Cluster)/$($Sample.ProtectionGroup): $($_.Exception.Message)"
        foreach ($r in $Group.Group) { $Current += $r }
        continue
    }
    foreach ($BaselineRow in $Group.Group) {
        $RecoveredET = Test-BaselineRecovered $BaselineRow $Cluster $ProtectionGroup $Runs
        if ($RecoveredET) {
            $Recovered += [pscustomobject]@{
                IncidentNumber = $Incident
                WindowKey = $Window.Key
                Status = "Recovered"
                Cluster = $BaselineRow.Cluster
                Environment = $BaselineRow.Environment
                ProtectionGroup = $BaselineRow.ProtectionGroup
                SourceHostName = $BaselineRow.SourceHostName
                ObjectName = $BaselineRow.ObjectName
                ObjectType = $BaselineRow.ObjectType
                RunType = $BaselineRow.RunType
                FirstFailedET = $BaselineRow.FirstFailedET
                LastFailedET = $BaselineRow.LastFailedET
                RecoveredET = $RecoveredET
                ConsecutiveFailureCount = 0
                Message = $BaselineRow.Message
                BaselineKey = $BaselineRow.BaselineKey
            }
        } else {
            $Current += $BaselineRow
        }
    }
}

$NewFailures = @()
$NewRecoveries = @($Recovered | Where-Object { !$PreviouslyRecovered.ContainsKey($_.BaselineKey) })
$Columns = "IncidentNumber", "WindowKey", "Status", "Cluster", "Environment", "ProtectionGroup", "SourceHostName", "ObjectName", "ObjectType", "RunType", "FirstFailedET", "LastFailedET", "RecoveredET", "ConsecutiveFailureCount", "Message", "BaselineKey"
Write-CsvFile $Current (Join-Path $OutputFolder "current_failures.csv") $Columns
Write-CsvFile $Recovered (Join-Path $OutputFolder "recovered.csv") $Columns
Write-CsvFile $NewFailures (Join-Path $OutputFolder "new_failures.csv") $Columns
Write-CsvFile $NewRecoveries (Join-Path $OutputFolder "new_recoveries.csv") $Columns

$Worknotes = @"
Backup Failure Evidence

Incident: $Incident
Window: $($Window.Label)
Mode: BaselineCsvTargetedFollowUp
Baseline CSV: $SelectedBaselineCsv
Generated At: $($Window.GeneratedET) ET

Summary:
- Baseline failures still failing: $(@($Current).Count)
- Baseline failures recovered: $(@($Recovered).Count)
- New failures: 0
- New recoveries since last run: $(@($NewRecoveries).Count)

Important:
- Baseline is the latest BackupFailures_AllEnvironments_*.csv generated by Cohesity_Backup_Failures.
- This script checks only baseline cluster/protection-group/object rows for recovery.
- It does not scan every PG again.
- new_failures.csv is intentionally header-only because new failures require a new full baseline run.
- Cohesity calls are GET-only.
- No Excel output is generated.
"@
if ($Warnings.Count -gt 0) { $Worknotes += "`nWarnings:`n" + (($Warnings | ForEach-Object { "- $_" }) -join "`n") }
$Worknotes | Set-Content (Join-Path $OutputFolder "worknotes.txt") -Encoding UTF8

$State = [pscustomobject]@{
    IncidentNumber = $Incident
    WindowKey = $Window.Key
    WindowLabel = $Window.Label
    Mode = "BaselineCsvTargetedFollowUp"
    BaselineCsv = $SelectedBaselineCsv
    LastRunET = $Window.GeneratedET
    BaselineFailures = @($BaselineRows)
    CurrentFailures = @($Current)
    Recovered = @($Recovered)
}
Write-JsonFile $State $StatePath

Write-Host "`nIncident: $Incident`nWindow  : $($Window.Label)`nMode    : BaselineCsvTargetedFollowUp`nBaseline: $SelectedBaselineCsv`nOutput  : $OutputFolder`nCreated : current_failures.csv, recovered.csv, new_failures.csv, new_recoveries.csv, worknotes.txt, state.json"
