<#
.SYNOPSIS
Formats Cohesity backup failure collector outputs for operator-facing review.

.DESCRIPTION
Post-processing only. Does not call Cohesity and does not change state.json.
Keeps raw collector CSVs as *_raw.csv, then rewrites operator-facing CSVs with cleaner columns.
Rewrites worknotes_summary.txt so ServiceNow work notes show failures, recovered objects, and concise running/cancelled counts only.
Attempt/run-count details such as FailedRunCount stay in *_raw.csv/state.json and are intentionally not shown in operator-facing CSVs or work notes.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)] [string]$ReportFolder
)

$ErrorActionPreference = 'Stop'

function Clean($Value) {
    if ($null -eq $Value) { return '' }
    if ($Value -is [array]) { $Value = @($Value) -join ' | ' }
    $TextValue = [string]$Value
    $TextValue = $TextValue.Replace([char]13, ' ').Replace([char]10, ' ')
    $TextValue = [regex]::Replace($TextValue, '\s+', ' ')
    return $TextValue.Replace([char]34, [char]39).Trim()
}

function Get-Prop($ObjectValue, [string]$Name, $DefaultValue = '') {
    if ($null -eq $ObjectValue) { return $DefaultValue }
    if ($ObjectValue -is [hashtable]) {
        if ($ObjectValue.ContainsKey($Name)) { return $ObjectValue[$Name] }
        return $DefaultValue
    }
    $Property = $ObjectValue.PSObject.Properties[$Name]
    if ($Property) { return $Property.Value }
    return $DefaultValue
}

function Import-CsvSafe([string]$Path) {
    if (!(Test-Path $Path)) { return @() }
    try { return @(Import-Csv -Path $Path) }
    catch { return @() }
}

function Read-StateSafe([string]$Folder) {
    $Path = Join-Path $Folder 'state.json'
    if (!(Test-Path $Path)) { return $null }
    try {
        $Raw = Get-Content -Path $Path -Raw
        if ([string]::IsNullOrWhiteSpace($Raw)) { return $null }
        return ($Raw | ConvertFrom-Json)
    } catch {
        return $null
    }
}

function Backup-RawCsv([string]$Path) {
    if (!(Test-Path $Path)) { return }
    $RawPath = [System.IO.Path]::Combine(
        [System.IO.Path]::GetDirectoryName($Path),
        ('{0}_raw{1}' -f [System.IO.Path]::GetFileNameWithoutExtension($Path), [System.IO.Path]::GetExtension($Path))
    )
    if (!(Test-Path $RawPath)) {
        Copy-Item -Path $Path -Destination $RawPath -Force
    }
}

function New-CleanRow($Row) {
    [pscustomobject][ordered]@{
        IncidentNumber = Clean (Get-Prop $Row 'IncidentNumber' '')
        Status = Clean (Get-Prop $Row 'Status' '')
        StatusChange = Clean (Get-Prop $Row 'StatusChange' (Get-Prop $Row 'Change' ''))
        Cluster = Clean (Get-Prop $Row 'Cluster' '')
        ProtectionGroup = Clean (Get-Prop $Row 'ProtectionGroup' '')
        Environment = Clean (Get-Prop $Row 'Environment' '')
        Host = Clean (Get-Prop $Row 'Host' '')
        ObjectName = Clean (Get-Prop $Row 'ObjectName' '')
        ObjectType = Clean (Get-Prop $Row 'ObjectType' '')
        RunType = Clean (Get-Prop $Row 'RunType' '')
        FirstFailedET = Clean (Get-Prop $Row 'FirstFailedET' '')
        LastFailedET = Clean (Get-Prop $Row 'LastFailedET' (Get-Prop $Row 'NewestFailedET' ''))
        LatestSuccessET = Clean (Get-Prop $Row 'LatestSuccessET' '')
        LastSeenET = Clean (Get-Prop $Row 'LastSeenET' '')
        FailureDates = Clean (Get-Prop $Row 'FailureDates' '')
        ConsecutiveFailureDays = Clean (Get-Prop $Row 'ConsecutiveFailureDays' (Get-Prop $Row 'FailureRuns' ''))
        Message = Clean (Get-Prop $Row 'Message' '')
    }
}

function Write-CleanCsv([string]$Path) {
    if (!(Test-Path $Path)) { return @() }
    Backup-RawCsv -Path $Path
    $Rows = Import-CsvSafe -Path $Path
    $CleanRows = @($Rows | ForEach-Object { New-CleanRow $_ })
    $Columns = @('IncidentNumber','Status','StatusChange','Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','LatestSuccessET','LastSeenET','FailureDates','ConsecutiveFailureDays','Message')
    if ($CleanRows.Count -eq 0) {
        ($Columns -join ',') | Set-Content -Path $Path -Encoding UTF8
    } else {
        $CleanRows | Select-Object -Property $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
    return $CleanRows
}

function Get-UniqueProtectionGroupCount($Rows) {
    $Keys = @{}
    foreach ($Row in @($Rows)) {
        $Key = '{0}|{1}|{2}' -f (Clean (Get-Prop $Row 'Cluster' '')), (Clean (Get-Prop $Row 'Environment' '')), (Clean (Get-Prop $Row 'ProtectionGroup' ''))
        if ($Key.Trim('|') -and !$Keys.ContainsKey($Key)) { $Keys[$Key] = $true }
    }
    return $Keys.Count
}

function Test-StatusIn([object]$Row, [string[]]$StatusSet) {
    $StatusValue = Clean (Get-Prop $Row 'Status' '')
    return ($StatusSet -contains $StatusValue)
}

function Get-TableText($Rows, [string[]]$Columns, [string]$EmptyText) {
    $List = @($Rows)
    if ($List.Count -eq 0) { return $EmptyText }
    return (($List | Select-Object -Property $Columns | Format-Table -AutoSize | Out-String).Trim())
}

if (!(Test-Path $ReportFolder)) { throw "Report folder not found: $ReportFolder" }

$CurrentPath = Join-Path $ReportFolder 'current_failures.csv'
$ClearedPath = Join-Path $ReportFolder 'cleared_by_success.csv'
$LifecyclePath = Join-Path $ReportFolder 'incident_lifecycle.csv'

$CurrentRows = Write-CleanCsv -Path $CurrentPath
$ClearedRows = Write-CleanCsv -Path $ClearedPath
$LifecycleRows = Write-CleanCsv -Path $LifecyclePath

$State = Read-StateSafe -Folder $ReportFolder
$IncidentNumber = Clean (Get-Prop $State 'IncidentNumber' '')
$WindowLabel = Clean (Get-Prop $State 'WindowLabel' '')
$RunMode = Clean (Get-Prop $State 'RunMode' '')
$ScanRunCount = Clean (Get-Prop $State 'ScanRunCount' '')
$CollectionStatus = Clean (Get-Prop $State 'CollectionStatus' '')
if (!$CollectionStatus) { $CollectionStatus = 'Unknown' }

$FailureStatusSet = @('Failure','NewlyFailedThisCheck','OlderStillFailing','CurrentStillFailing','CarriedForwardStillFailing','ReFailedAfterClear','UnknownNeedsReview')
$SuccessStatusSet = @('Success','NewlyClearedThisCheck','ClearedByLaterSuccess')
$RunningStatusSet = @('Running','RunningAtLatestCheck')
$CancelledStatusSet = @('Cancelled','CancelledAfterFailure')

$FailureRows = @($CurrentRows | Where-Object { Test-StatusIn $_ $FailureStatusSet })
$SuccessRows = @($ClearedRows | Where-Object { Test-StatusIn $_ $SuccessStatusSet })
$RunningRows = @($LifecycleRows | Where-Object { Test-StatusIn $_ $RunningStatusSet })
$CancelledRows = @($LifecycleRows | Where-Object { Test-StatusIn $_ $CancelledStatusSet })

$RunningProtectionGroups = Get-UniqueProtectionGroupCount $RunningRows
$CancelledProtectionGroups = Get-UniqueProtectionGroupCount $CancelledRows

$WorknoteColumns = @('StatusChange','Status','Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','LastFailedET','FailureDates','Message')
$SuccessColumns = @('StatusChange','Status','Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','LatestSuccessET','Message')

$Lines = New-Object System.Collections.Generic.List[string]
$Lines.Add('Cohesity Backup Failure Daily Update')
if ($IncidentNumber) { $Lines.Add(('Incident: {0}' -f $IncidentNumber)) }
if ($WindowLabel) { $Lines.Add(('Window: {0}' -f $WindowLabel)) }
if ($RunMode) { $Lines.Add(('Run Mode: {0}' -f $RunMode)) }
if ($ScanRunCount) { $Lines.Add(('Scan NumRuns: {0}' -f $ScanRunCount)) }
$Lines.Add(('Collection Status: {0}' -f $CollectionStatus))
$Lines.Add('')
$Lines.Add('Summary:')
$Lines.Add(('Active Failures: {0}' -f $FailureRows.Count))
$Lines.Add(('Recovered Today: {0}' -f $SuccessRows.Count))
$Lines.Add(('Running / In-progress PGs: {0}' -f $RunningProtectionGroups))
$Lines.Add(('Cancelled Backup PGs: {0}' -f $CancelledProtectionGroups))
if ($RunningProtectionGroups -gt 0 -or $CancelledProtectionGroups -gt 0) {
    $Lines.Add('')
    $Lines.Add('Note:')
    $Lines.Add('Please check incident_lifecycle.csv and continue monitoring running/cancelled backups.')
}
$Lines.Add('')
$Lines.Add('Failure Section:')
$Lines.Add((Get-TableText -Rows $FailureRows -Columns $WorknoteColumns -EmptyText 'No active object-level backup failures found.'))
$Lines.Add('')
$Lines.Add('Success Section:')
$Lines.Add((Get-TableText -Rows $SuccessRows -Columns $SuccessColumns -EmptyText 'No previously failed objects recovered in this run.'))

($Lines -join [Environment]::NewLine) | Set-Content -Path (Join-Path $ReportFolder 'worknotes_summary.txt') -Encoding UTF8

Write-Host ('Formatted operator-facing backup failure report: {0}' -f $ReportFolder)
