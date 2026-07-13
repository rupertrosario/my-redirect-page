<#
.SYNOPSIS
Cohesity Backup Failure Window Consolidator.

.DESCRIPTION
GET-only Cohesity Helios collector for daily backup failure incident updates.

Architectural rules:
- New 18:00 ET window / new INC uses a full object-detail baseline scan with 30 runs.
- Existing window / same INC uses incremental scan with a smaller run count.
- Previous active object failures are always rechecked, even during incremental runs.
- New failures are discovered from run-level Failed / Warning / Cancelled / Running / message signals, then confirmed at object level.
- V6: Running must not override a same-object successful completed backup unless a later terminal Failure/Cancelled exists.
- Final reporting is object-level only. Protection Group is context.
- Clear when the same object identity has a newer successful object backup, even if run type changed.
- If a known failed object is not visible in the current lookback, carry it forward.
- Same-day repeated object failures are consolidated to the latest failure row.
- Consecutive failures are tracked date-wise for clarity.
- RemoteAdapter is intentionally excluded.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow',
    [string]$LegacyFailureOutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailures',
    [string]$HelperPath = ('X:\PowerShell\Cohesity_API_Scripts\Common\' + 'Api' + 'KeyAesHelper.ps1'),
    [string]$EncryptedFile = ('X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_' + 'api' + 'key.enc'),
    [string]$ClusterName = '',
    [int]$NumRuns = 15,
    [int]$BaselineNumRuns = 30,
    [string]$IncidentNumber = '',
    [switch]$UseLatestFailureCsv,
    [string]$LegacyFailureCsvPath = '',
    [int]$KeepFoldersDays = 14,
    [int]$ArchiveFoldersUntilDays = 35,
    [int]$RequestTimeoutSec = 120
)

$ErrorActionPreference = 'Stop'
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:CollectionIncomplete = $false
$script:CsvColumns = @('IncidentNumber','WindowKey','Status','Change','Cluster','Environment','ProtectionGroup','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','LatestSuccessET','LastSeenET','FailureDates','ConsecutiveFailureDays','FailedRunCount','Message','ObjectKey','ClusterId','ProtectionGroupId','EnvironmentFilter','FailedRunKeys')
$script:WorknoteColumns = @('Status','Change','Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','LastFailedET','LatestSuccessET','FailureDates','ConsecutiveFailureDays','FailedRunCount','Message')
$script:ReviewColumns = @('Cluster','Environment','ProtectionGroup','ProtectionGroupId','Reason')

function Clean($Value) {
    if ($null -eq $Value) { return '' }
    if ($Value -is [array]) { $Value = @($Value) -join ' | ' }
    $TextValue = [string]$Value
    $TextValue = $TextValue.Replace([char]13, ' ').Replace([char]10, ' ')
    $TextValue = [regex]::Replace($TextValue, '\s+', ' ')
    return $TextValue.Replace([char]34, [char]39).Trim()
}

function As-Array($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [array]) { return @($Value) }
    return @($Value)
}

function Get-Prop($ObjectValue, [string]$Name, $DefaultValue = $null) {
    if ($null -eq $ObjectValue) { return $DefaultValue }
    if ($ObjectValue -is [hashtable]) {
        if ($ObjectValue.ContainsKey($Name)) { return $ObjectValue[$Name] }
        return $DefaultValue
    }
    $Property = $ObjectValue.PSObject.Properties[$Name]
    if ($Property) { return $Property.Value }
    return $DefaultValue
}

function Set-ObjProp($ObjectValue, [string]$Name, $NewValue) {
    if ($null -eq $ObjectValue) { return }
    $Property = $ObjectValue.PSObject.Properties[$Name]
    if ($Property) { $ObjectValue.$Name = $NewValue }
    else { $ObjectValue | Add-Member -MemberType NoteProperty -Name $Name -Value $NewValue -Force }
}

function Add-RunWarning([string]$Message) {
    $CleanMessage = Clean $Message
    if ($CleanMessage) {
        $script:CollectionIncomplete = $true
        $script:Warnings.Add($CleanMessage) | Out-Null
        Write-Warning $CleanMessage
    }
}

function To-Int64($Value) {
    try {
        $CleanValue = Clean $Value
        if (!$CleanValue) { return [int64]0 }
        return [int64]$CleanValue
    } catch {
        return [int64]0
    }
}

function Get-EtZone {
    try { return [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
    catch { return [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
}
$script:EtZone = Get-EtZone

function Get-NowEtDate { [TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone) }
function Get-NowEtText { (Get-NowEtDate).ToString('yyyy-MM-dd HH:mm:ss') }

function Convert-UsecsToEtText($Usecs) {
    $UsecsInt = To-Int64 $Usecs
    if ($UsecsInt -le 0) { return '' }
    try {
        $UtcDate = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$UsecsInt / 1000)).UtcDateTime
        return ([TimeZoneInfo]::ConvertTimeFromUtc($UtcDate, $script:EtZone)).ToString('yyyy-MM-dd HH:mm:ss')
    } catch {
        return ''
    }
}

function Convert-UsecsToEtDateKey($Usecs) {
    $TextValue = Convert-UsecsToEtText $Usecs
    if (!$TextValue) { return '' }
    return $TextValue.Substring(0,10)
}

function Parse-EtTextToDate([string]$TextValue) {
    $CleanText = Clean $TextValue
    if (!$CleanText) { return $null }
    foreach ($FormatValue in @('yyyy-MM-dd HH:mm:ss','yyyy-MM-dd H:mm:ss','M/d/yyyy h:mm:ss tt','M/d/yyyy H:mm:ss','yyyy-MM-ddTHH:mm:ss')) {
        try { return [datetime]::ParseExact($CleanText, $FormatValue, [Globalization.CultureInfo]::InvariantCulture) } catch {}
    }
    try { return [datetime]::Parse($CleanText) } catch { return $null }
}

function Get-DateSortValue([string]$TextValue) {
    $ParsedDate = Parse-EtTextToDate $TextValue
    if ($ParsedDate) { return $ParsedDate.ToString('yyyy-MM-dd HH:mm:ss') }
    return '0000-00-00 00:00:00'
}

function Read-JsonSafe([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    try {
        $RawText = Get-Content -Path $Path -Raw
        if ([string]::IsNullOrWhiteSpace($RawText)) { return $null }
        return ($RawText | ConvertFrom-Json)
    } catch {
        return $null
    }
}

function Write-JsonAtomic($ObjectValue, [string]$Path) {
    $DirectoryPath = Split-Path $Path -Parent
    if (!(Test-Path $DirectoryPath)) { New-Item -Path $DirectoryPath -ItemType Directory -Force | Out-Null }
    $PreviousPath = Join-Path $DirectoryPath 'state.previous.json'
    $TempPath = ('{0}.tmp' -f $Path)
    if (Test-Path $Path) { Copy-Item -Path $Path -Destination $PreviousPath -Force -ErrorAction SilentlyContinue }
    $ObjectValue | ConvertTo-Json -Depth 100 | Set-Content -Path $TempPath -Encoding UTF8
    Move-Item -Path $TempPath -Destination $Path -Force
}

function Write-CsvRows($Rows, [string]$Path, [string[]]$Columns) {
    $DirectoryPath = Split-Path $Path -Parent
    if (!(Test-Path $DirectoryPath)) { New-Item -Path $DirectoryPath -ItemType Directory -Force | Out-Null }
    $List = @($Rows)
    if ($List.Count -eq 0) {
        ($Columns -join ',') | Set-Content -Path $Path -Encoding UTF8
    } else {
        $List | Select-Object -Property $Columns | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
}

function Invoke-HeliosGetJson([string]$Uri, [hashtable]$Headers) {
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $Response = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -UseBasicParsing -TimeoutSec $RequestTimeoutSec
    } else {
        $Response = Invoke-WebRequest -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $RequestTimeoutSec
    }
    if (-not $Response -or [string]::IsNullOrWhiteSpace($Response.Content)) { return $null }
    return ($Response.Content | ConvertFrom-Json)
}

function Get-CohesityApiKeySafe {
    if (!(Test-Path $HelperPath)) { throw ('Missing API key helper: {0}' -f $HelperPath) }
    if (!(Test-Path $EncryptedFile)) { throw ('Missing encrypted key file: {0}' -f $EncryptedFile) }
    . $HelperPath
    $ApiKeyValue = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($ApiKeyValue)) { throw 'API key helper returned a blank value.' }
    return $ApiKeyValue.Trim()
}

function Get-ComputeWindow {
    $NowEt = Get-NowEtDate
    if ($NowEt.Hour -lt 18) { $StartEt = $NowEt.Date.AddDays(-1).AddHours(18) } else { $StartEt = $NowEt.Date.AddHours(18) }
    $EndEt = $StartEt.AddDays(1)
    [pscustomobject]@{
        WindowKey = ('{0}_1800ET' -f $StartEt.ToString('yyyy-MM-dd'))
        WindowLabel = ('{0} 18:00 ET -> {1} 18:00 ET' -f $StartEt.ToString('yyyy-MM-dd'), $EndEt.ToString('yyyy-MM-dd'))
        WindowStartET = $StartEt.ToString('yyyy-MM-dd HH:mm:ss')
        WindowEndET = $EndEt.ToString('yyyy-MM-dd HH:mm:ss')
        GeneratedET = Get-NowEtText
    }
}

function Get-RegistryPath {
    if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
    return (Join-Path $OutputRoot 'BackupFailure_WindowRegistry.json')
}

function Get-WindowRegistry {
    $Registry = Read-JsonSafe (Get-RegistryPath)
    if (!$Registry) { $Registry = [pscustomobject]@{ Windows = [pscustomobject]@{} } }
    if (!$Registry.PSObject.Properties['Windows']) { $Registry | Add-Member -MemberType NoteProperty -Name 'Windows' -Value ([pscustomobject]@{}) -Force }
    return $Registry
}

function Save-WindowRegistry($Registry) { Write-JsonAtomic $Registry (Get-RegistryPath) }

function Resolve-IncidentLock($Window) {
    $Registry = Get-WindowRegistry
    $ExistingProperty = $Registry.Windows.PSObject.Properties[$Window.WindowKey]
    if ($ExistingProperty) {
        $Entry = $ExistingProperty.Value
        Set-ObjProp $Entry 'LastRunET' $Window.GeneratedET
        Save-WindowRegistry $Registry
        return [pscustomobject]@{ Entry=$Entry; IsNewWindow=$false }
    }

    $IncidentValue = $IncidentNumber
    if (!$IncidentValue) { $IncidentValue = Read-Host 'Enter incident number for this backup-failure window' }
    $IncidentValue = $IncidentValue.Trim().ToUpper()
    if ($IncidentValue -notmatch '^INC[0-9A-Z]+$') { throw ('Invalid incident number: {0}' -f $IncidentValue) }

    $Entry = [pscustomobject]@{
        IncidentNumber = $IncidentValue
        WindowKey = $Window.WindowKey
        WindowLabel = $Window.WindowLabel
        WindowStartET = $Window.WindowStartET
        WindowEndET = $Window.WindowEndET
        FirstRunET = $Window.GeneratedET
        LastRunET = $Window.GeneratedET
        OutputFolder = (Join-Path $OutputRoot $IncidentValue)
    }
    $Registry.Windows | Add-Member -MemberType NoteProperty -Name $Window.WindowKey -Value $Entry -Force
    Save-WindowRegistry $Registry
    return [pscustomobject]@{ Entry=$Entry; IsNewWindow=$true }
}

function Enter-CollectorLock([string]$FolderPath) {
    if (!(Test-Path $FolderPath)) { New-Item -Path $FolderPath -ItemType Directory -Force | Out-Null }
    $LockPath = Join-Path $FolderPath 'collector.lock'
    if (Test-Path $LockPath) {
        throw ('Collector lock exists. Another run may be active. Remove only after confirming no run is active: {0}' -f $LockPath)
    }
    ('PID={0}; StartedET={1}' -f $PID,(Get-NowEtText)) | Set-Content -Path $LockPath -Encoding UTF8
    return $LockPath
}

function Get-LatestPreviousState([string]$CurrentFolder) {
    if (!(Test-Path $OutputRoot)) { return $null }
    $Folders = @(Get-ChildItem -Path $OutputRoot -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -ne 'Archive' -and $_.FullName -ne $CurrentFolder } |
        Sort-Object LastWriteTime -Descending)
    foreach ($Folder in $Folders) {
        $StatePathCandidate = Join-Path $Folder.FullName 'state.json'
        $StateValue = Read-JsonSafe $StatePathCandidate
        if ($StateValue -and $StateValue.CurrentOpenFailures) { return $StateValue }
    }
    return $null
}

function Read-StateWithBackup([string]$FolderPath) {
    $StatePathValue = Join-Path $FolderPath 'state.json'
    $StateValue = Read-JsonSafe $StatePathValue
    if ($StateValue) { return $StateValue }
    $BackupPath = Join-Path $FolderPath 'state.previous.json'
    $BackupValue = Read-JsonSafe $BackupPath
    if ($BackupValue) {
        Add-RunWarning ('state.json could not be read; using state.previous.json from {0}' -f $FolderPath)
        return $BackupValue
    }
    return $null
}

function Get-ClusterDisplayName($ClusterObject) {
    $NameValue = Clean (Get-Prop $ClusterObject 'name' '')
    if (!$NameValue) { $NameValue = Clean (Get-Prop $ClusterObject 'clusterName' '') }
    if (!$NameValue) { $NameValue = Clean (Get-Prop $ClusterObject 'displayName' '') }
    if (!$NameValue) { $NameValue = ('Unknown-{0}' -f (Clean (Get-Prop $ClusterObject 'clusterId' ''))) }
    return $NameValue
}

function Get-EnvironmentMap {
    @(
        [pscustomobject]@{ Label='Oracle';     Filter='kOracle';     TargetType='kDatabase';       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label='SQL';        Filter='kSQL';        TargetType='kDatabase';       ParentHostNeeded=$true  },
        [pscustomobject]@{ Label='Physical';   Filter='kPhysical';   TargetType='kHost';           ParentHostNeeded=$false },
        [pscustomobject]@{ Label='GenericNas'; Filter='kGenericNas'; TargetType='kHost';           ParentHostNeeded=$false },
        [pscustomobject]@{ Label='HyperV';     Filter='kHyperV';     TargetType='kVirtualMachine'; ParentHostNeeded=$false },
        [pscustomobject]@{ Label='Acropolis';  Filter='kAcropolis';  TargetType='kVirtualMachine'; ParentHostNeeded=$false },
        [pscustomobject]@{ Label='Isilon';     Filter='kIsilon';     TargetType='kHost';           ParentHostNeeded=$false }
    )
}

function Test-FailedStatus([string]$Status) { (Clean $Status) -in @('Failed','kFailed','Failure','kFailure','Error','kError') }
function Test-SuccessStatus([string]$Status) { (Clean $Status) -in @('Succeeded','kSucceeded','SucceededWithWarning','kSucceededWithWarning','Success','kSuccess','Successful','Completed','kCompleted') }
function Test-WarningStatus([string]$Status) { (Clean $Status) -in @('SucceededWithWarning','kSucceededWithWarning','Warning','kWarning') }
function Test-RunningStatus([string]$Status) { (Clean $Status) -in @('Running','kRunning','Accepted','kAccepted','Queued','kQueued') }
function Test-CancelledStatus([string]$Status) { (Clean $Status) -in @('Canceled','Cancelled','kCanceled','kCancelled','Canceling','kCanceling') }

function Get-FirstRunInfo($RunObject) {
    $InfoList = @(As-Array (Get-Prop $RunObject 'localBackupInfo' @()))
    if ($InfoList.Count -gt 0) { return $InfoList[0] }
    return $null
}

function Get-RunUsecs($RunObject) {
    $RunInfo = Get-FirstRunInfo $RunObject
    if (!$RunInfo) { return 0 }
    $EndUsecs = To-Int64 (Get-Prop $RunInfo 'endTimeUsecs' 0)
    if ($EndUsecs -gt 0) { return $EndUsecs }
    return (To-Int64 (Get-Prop $RunInfo 'startTimeUsecs' 0))
}

function Get-RunStatus($RunObject) {
    $RunInfo = Get-FirstRunInfo $RunObject
    if (!$RunInfo) { return '' }
    return Clean (Get-Prop $RunInfo 'status' '')
}

function Get-RunType($RunObject) {
    $RunInfo = Get-FirstRunInfo $RunObject
    if (!$RunInfo) { return 'Unknown' }
    $RunTypeValue = Clean (Get-Prop $RunInfo 'runType' '')
    if (!$RunTypeValue) { return 'Unknown' }
    return $RunTypeValue
}

function Test-RunHasMessage($RunObject) {
    $RunInfo = Get-FirstRunInfo $RunObject
    if (!$RunInfo) { return $false }
    foreach ($FieldName in @('messages','message','error','errorMessage','failureMessage','reason','lastError')) {
        $MessageValue = Clean (Get-Prop $RunInfo $FieldName '')
        if ($MessageValue) { return $true }
    }
    return $false
}

function Test-SuspiciousRun($RunObject) {
    $RunStatusValue = Get-RunStatus $RunObject
    if (Test-FailedStatus $RunStatusValue) { return $true }
    if (Test-WarningStatus $RunStatusValue) { return $true }
    if (Test-RunningStatus $RunStatusValue) { return $true }
    if (Test-CancelledStatus $RunStatusValue) { return $true }
    if (Test-RunHasMessage $RunObject) { return $true }
    return $false
}

function Get-ProtectionGroupId($ProtectionGroupObject) {
    $ProtectionGroupId = Clean (Get-Prop $ProtectionGroupObject 'id' '')
    if (!$ProtectionGroupId) { $ProtectionGroupId = Clean (Get-Prop $ProtectionGroupObject 'protectionGroupId' '') }
    return $ProtectionGroupId
}

function Get-ProtectionGroupName($ProtectionGroupObject) {
    $ProtectionGroupName = Clean (Get-Prop $ProtectionGroupObject 'name' '')
    if (!$ProtectionGroupName) { $ProtectionGroupName = Clean (Get-Prop $ProtectionGroupObject 'protectionGroupName' '') }
    if (!$ProtectionGroupName) { $ProtectionGroupName = Clean (Get-Prop $ProtectionGroupObject 'displayName' '') }
    return $ProtectionGroupName
}

function Get-ProtectionGroupKey([string]$ClusterId, [string]$EnvironmentLabel, [string]$ProtectionGroupId) {
    return ('{0}|{1}|{2}' -f $ClusterId,$EnvironmentLabel,$ProtectionGroupId)
}

function Get-ObjectIdentityKey($RunObject, [string]$ClusterId, [string]$ProtectionGroupId, [string]$EnvironmentLabel, [string]$RunType) {
    $ObjectMeta = Get-Prop $RunObject 'object' $null
    if (!$ObjectMeta) { return '' }
    $ObjectEnvironment = Clean (Get-Prop $ObjectMeta 'environment' '')
    $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
    $ObjectName = Clean (Get-Prop $ObjectMeta 'name' '')
    $ObjectId = Clean (Get-Prop $ObjectMeta 'id' '')
    $SourceId = Clean (Get-Prop $ObjectMeta 'sourceId' '')
    $IdentityPart = $ObjectId
    if (!$IdentityPart) { $IdentityPart = ('{0}|{1}|{2}|{3}' -f $ObjectEnvironment,$ObjectType,$ObjectName,$SourceId) }

    # Recovery identity rule:
    # ObjectKey must not include RunType. A cancelled/failed incremental run can be
    # cleared by a later successful full/synthetic/incremental run for the same object.
    # RunType is still retained in the report row and FailedRunKeys for audit context.
    return ('{0}|{1}|{2}|{3}' -f $ClusterId,$ProtectionGroupId,$EnvironmentLabel,$IdentityPart)
}

function Get-FailedAttempts($RunObject) {
    $FailedAttemptList = @()
    foreach ($LocalSnapshotInfo in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        $FailedAttemptList += @(As-Array (Get-Prop $LocalSnapshotInfo 'failedAttempts' @()))
        foreach ($SnapshotInfo in As-Array (Get-Prop $LocalSnapshotInfo 'snapshotInfo' @())) {
            $FailedAttemptList += @(As-Array (Get-Prop $SnapshotInfo 'failedAttempts' @()))
        }
    }
    foreach ($SnapshotInfo in As-Array (Get-Prop $RunObject 'snapshotInfo' @())) {
        $FailedAttemptList += @(As-Array (Get-Prop $SnapshotInfo 'failedAttempts' @()))
    }
    return @($FailedAttemptList | Where-Object { $_ })
}

function Get-ObjectStatusValues($RunObject) {
    $StatusList = @()
    $StatusList += Clean (Get-Prop $RunObject 'status' '')
    $ObjectMeta = Get-Prop $RunObject 'object' $null
    if ($ObjectMeta) { $StatusList += Clean (Get-Prop $ObjectMeta 'status' '') }
    foreach ($LocalSnapshotInfo in As-Array (Get-Prop $RunObject 'localSnapshotInfo' @())) {
        $StatusList += Clean (Get-Prop $LocalSnapshotInfo 'status' '')
        foreach ($SnapshotInfo in As-Array (Get-Prop $LocalSnapshotInfo 'snapshotInfo' @())) {
            $StatusList += Clean (Get-Prop $SnapshotInfo 'status' '')
        }
    }
    foreach ($SnapshotInfo in As-Array (Get-Prop $RunObject 'snapshotInfo' @())) {
        $StatusList += Clean (Get-Prop $SnapshotInfo 'status' '')
    }
    return @($StatusList | Where-Object { $_ })
}

function Get-ObjectMessage($RunObject, $RunInfo) {
    $MessageList = @()
    foreach ($FailedAttempt in Get-FailedAttempts $RunObject) {
        foreach ($FieldName in @('message','error','reason','errorMessage','failureMessage')) {
            $MessageValue = Clean (Get-Prop $FailedAttempt $FieldName '')
            if ($MessageValue) { $MessageList += $MessageValue }
        }
    }
    foreach ($ContainerObject in @($RunObject, (Get-Prop $RunObject 'object' $null), $RunInfo)) {
        if (!$ContainerObject) { continue }
        foreach ($FieldName in @('message','messages','error','reason','errorMessage','failureMessage','lastError')) {
            $MessageValue = Clean (Get-Prop $ContainerObject $FieldName '')
            if ($MessageValue) { $MessageList += $MessageValue }
        }
    }
    return Clean (($MessageList | Where-Object { $_ } | Select-Object -Unique) -join ' | ')
}

function Get-ObjectState($RunObject, [string]$RunStatus) {
    $ObjectStatuses = @(Get-ObjectStatusValues $RunObject)

    # Retry-aware rule:
    # Cohesity can record failedAttempts inside the same run/object even when
    # the final object/snapshot status is success, running, or cancelled.
    # Therefore final object/snapshot status must be evaluated before failedAttempts.
    # A cancelled/run-level state must not override an explicit object/snapshot success.
    if (@($ObjectStatuses | Where-Object { Test-SuccessStatus $_ }).Count -gt 0) {
        return 'Success'
    }

    if (@($ObjectStatuses | Where-Object { Test-FailedStatus $_ }).Count -gt 0) {
        return 'Failure'
    }

    if (@($ObjectStatuses | Where-Object { Test-CancelledStatus $_ }).Count -gt 0) {
        return 'Cancelled'
    }

    if (@($ObjectStatuses | Where-Object { Test-RunningStatus $_ }).Count -gt 0) {
        return 'Running'
    }

    # If there is no object/snapshot-level status, fall back to run-level status.
    if (Test-SuccessStatus $RunStatus) {
        return 'Success'
    }

    if (Test-CancelledStatus $RunStatus) {
        return 'Cancelled'
    }

    if (Test-RunningStatus $RunStatus) {
        return 'Running'
    }

    if (Test-FailedStatus $RunStatus) {
        return 'Failure'
    }

    # failedAttempts is fallback failure evidence only.
    # It must not override an explicit final Success/Running/Cancelled object status.
    if (@(Get-FailedAttempts $RunObject).Count -gt 0) {
        return 'Failure'
    }

    return 'Success'
}

function Test-TargetObject($RunObject, $EnvironmentSpec, [string[]]$EnvironmentFilterSet) {
    $ObjectMeta = Get-Prop $RunObject 'object' $null
    if (!$ObjectMeta) { return $false }
    $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
    $ObjectEnvironment = Clean (Get-Prop $ObjectMeta 'environment' '')
    if ($EnvironmentSpec.Label -in @('GenericNas','Isilon')) { return $true }
    if ($EnvironmentSpec.ParentHostNeeded -and ($ObjectType -eq 'kHost' -or $ObjectEnvironment -eq 'kPhysical')) { return $true }
    if ($ObjectType -ne $EnvironmentSpec.TargetType) { return $false }
    if (!$ObjectEnvironment) { return $true }
    return ($EnvironmentFilterSet -contains $ObjectEnvironment)
}

function Get-ObjectParentHostName($RunObject, [hashtable]$ObjectIdToName, [hashtable]$PhysicalHostById) {
    $ObjectMeta = Get-Prop $RunObject 'object' $null
    if (!$ObjectMeta) { return '' }
    $SourceId = Clean (Get-Prop $ObjectMeta 'sourceId' '')
    if ($SourceId -and $ObjectIdToName.ContainsKey($SourceId)) { return Clean $ObjectIdToName[$SourceId] }
    if ($SourceId -and $PhysicalHostById.ContainsKey($SourceId)) { return Clean $PhysicalHostById[$SourceId] }
    $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
    $ObjectEnvironment = Clean (Get-Prop $ObjectMeta 'environment' '')
    if ($ObjectType -eq 'kHost' -or $ObjectEnvironment -eq 'kPhysical') { return Clean (Get-Prop $ObjectMeta 'name' '') }
    return ''
}

function Add-DateToSet([hashtable]$DateSet, [string]$DateKey) {
    if ($DateKey) { $DateSet[$DateKey] = $true }
}

function Merge-DateStrings([string]$OldDates, [hashtable]$NewDateSet) {
    $DateSet = @{}
    foreach ($DateValue in @((Clean $OldDates).Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })) { $DateSet[$DateValue] = $true }
    foreach ($DateValue in $NewDateSet.Keys) { if ($DateValue) { $DateSet[$DateValue] = $true } }
    return Clean (($DateSet.Keys | Sort-Object) -join ', ')
}

function New-StatusRow(
    [string]$Incident,
    [string]$WindowKey,
    [string]$Status,
    [string]$Change,
    [string]$ClusterDisplayName,
    [string]$ClusterId,
    $EnvironmentSpec,
    [string]$ProtectionGroupName,
    [string]$ProtectionGroupId,
    [string]$ParentHostName,
    [string]$ObjectName,
    [string]$ObjectType,
    [string]$RunType,
    [int64]$FirstFailedUsecs,
    [int64]$LastFailedUsecs,
    [int64]$LatestSuccessUsecs,
    [int64]$LastSeenUsecs,
    [string]$FailureDates,
    [int]$FailedRunCount,
    [string]$Message,
    [string]$ObjectKey,
    [string]$EnvironmentFilter,
    [string]$FailedRunKeys
) {
    $DateList = @((Clean $FailureDates).Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Select-Object -Unique)
    [pscustomobject]@{
        IncidentNumber = Clean $Incident
        WindowKey = Clean $WindowKey
        Status = Clean $Status
        Change = Clean $Change
        Cluster = Clean $ClusterDisplayName
        Environment = Clean $EnvironmentSpec.Label
        ProtectionGroup = Clean $ProtectionGroupName
        Host = Clean $ParentHostName
        ObjectName = Clean $ObjectName
        ObjectType = Clean $ObjectType
        RunType = Clean $RunType
        FirstFailedET = Convert-UsecsToEtText $FirstFailedUsecs
        LastFailedET = Convert-UsecsToEtText $LastFailedUsecs
        LatestSuccessET = Convert-UsecsToEtText $LatestSuccessUsecs
        LastSeenET = Convert-UsecsToEtText $LastSeenUsecs
        FailureDates = Clean (($DateList | Sort-Object) -join ', ')
        ConsecutiveFailureDays = @($DateList).Count
        FailedRunCount = $FailedRunCount
        Message = Clean $Message
        ObjectKey = Clean $ObjectKey
        ClusterId = Clean $ClusterId
        ProtectionGroupId = Clean $ProtectionGroupId
        EnvironmentFilter = Clean $EnvironmentFilter
        FailedRunKeys = Clean $FailedRunKeys
    }
}

function Get-CohesityProtectionGroups($ClusterObject, $EnvironmentSpec, [string]$ApiKey) {
    $ClusterId = Clean (Get-Prop $ClusterObject 'clusterId' '')
    $Headers = @{ accept='application/json'; apiKey=$ApiKey; accessClusterId=$ClusterId }
    $ProtectionGroups = @()
    foreach ($EnvironmentFilter in @($EnvironmentSpec.Filter.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        try {
            $Uri = ('{0}/v2/data-protect/protection-groups?environments={1}&isDeleted=false&isPaused=false&isActive=true' -f $BaseUrl,$EnvironmentFilter)
            $Json = Invoke-HeliosGetJson -Uri $Uri -Headers $Headers
            if ($Json -and $Json.protectionGroups) { $ProtectionGroups += @($Json.protectionGroups) }
        } catch {
            Add-RunWarning ('Protection group lookup failed: {0} / {1} / {2}' -f (Get-ClusterDisplayName $ClusterObject),$EnvironmentFilter,$_.Exception.Message)
        }
    }
    $UniqueIndex = @{}
    foreach ($ProtectionGroup in $ProtectionGroups) {
        $ProtectionGroupId = Get-ProtectionGroupId $ProtectionGroup
        if ($ProtectionGroupId -and !$UniqueIndex.ContainsKey($ProtectionGroupId)) { $UniqueIndex[$ProtectionGroupId] = $ProtectionGroup }
    }
    return @($UniqueIndex.Values | Sort-Object { Get-ProtectionGroupName $_ })
}

function Get-CohesityRuns($ClusterObject, [string]$ProtectionGroupId, [bool]$IncludeObjectDetails, [int]$RunCount, [string]$ApiKey) {
    $ClusterId = Clean (Get-Prop $ClusterObject 'clusterId' '')
    $Headers = @{ accept='application/json'; apiKey=$ApiKey; accessClusterId=$ClusterId }
    $EscapedProtectionGroupId = [uri]::EscapeDataString($ProtectionGroupId)
    if ($IncludeObjectDetails) {
        $Uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false&includeObjectDetails=true' -f $BaseUrl,$EscapedProtectionGroupId,$RunCount)
    } else {
        $Uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false' -f $BaseUrl,$EscapedProtectionGroupId,$RunCount)
    }
    $Json = Invoke-HeliosGetJson -Uri $Uri -Headers $Headers
    if ($Json -and $Json.runs) { return @($Json.runs) }
    return @()
}

function Index-RowsByProtectionGroup($Rows) {
    $Index = @{}
    foreach ($Row in @($Rows)) {
        $ProtectionGroupKey = Get-ProtectionGroupKey (Clean (Get-Prop $Row 'ClusterId' '')) (Clean (Get-Prop $Row 'Environment' '')) (Clean (Get-Prop $Row 'ProtectionGroupId' ''))
        if (!$ProtectionGroupKey.Trim('|')) { continue }
        if (!$Index.ContainsKey($ProtectionGroupKey)) { $Index[$ProtectionGroupKey] = @() }
        $Index[$ProtectionGroupKey] = @($Index[$ProtectionGroupKey] + $Row)
    }
    return $Index
}

function Index-RowsByObjectKey($Rows) {
    $Index = @{}
    foreach ($Row in @($Rows)) {
        $ObjectKey = Clean (Get-Prop $Row 'ObjectKey' '')
        if ($ObjectKey -and !$Index.ContainsKey($ObjectKey)) { $Index[$ObjectKey] = $Row }
    }
    return $Index
}

function Merge-RowsByObjectKey($Rows) {
    $Index = @{}
    foreach ($Row in @($Rows)) {
        $ObjectKey = Clean (Get-Prop $Row 'ObjectKey' '')
        if (!$ObjectKey) { continue }
        if (!$Index.ContainsKey($ObjectKey)) { $Index[$ObjectKey] = $Row; continue }
        $OldSortValue = Get-RowSortValue $Index[$ObjectKey]
        $NewSortValue = Get-RowSortValue $Row
        if ($NewSortValue -ge $OldSortValue) { $Index[$ObjectKey] = $Row }
    }
    return @($Index.Values)
}

function Get-RowSortValue($Row) {
    $LastSeenValue = Get-DateSortValue (Get-Prop $Row 'LastSeenET' '')
    $LatestSuccessValue = Get-DateSortValue (Get-Prop $Row 'LatestSuccessET' '')
    if ($LatestSuccessValue -gt $LastSeenValue) { return $LatestSuccessValue }
    return $LastSeenValue
}

function Get-RecoveryIdentityKeyForRow($Row) {
    $ClusterId = Clean (Get-Prop $Row 'ClusterId' '')
    $ProtectionGroupId = Clean (Get-Prop $Row 'ProtectionGroupId' '')
    $EnvironmentLabel = Clean (Get-Prop $Row 'Environment' '')
    $ObjectType = Clean (Get-Prop $Row 'ObjectType' '')
    $HostName = Clean (Get-Prop $Row 'Host' '')
    $ObjectName = Clean (Get-Prop $Row 'ObjectName' '')

    if ($ClusterId -and $ProtectionGroupId -and $EnvironmentLabel -and $ObjectName) {
        # V5 state-reconciliation identity rule:
        # Previous state may contain old ObjectKey values that included RunType.
        # Active/carry-forward suppression must compare the protected object itself,
        # not the saved ObjectKey format. This prevents an old CancelledAfterFailure
        # row from being carried forward after the same object has newer success.
        return (('{0}|{1}|{2}|{3}|{4}|{5}' -f $ClusterId,$ProtectionGroupId,$EnvironmentLabel,$ObjectType,$HostName,$ObjectName).ToLowerInvariant())
    }

    $ObjectKey = Clean (Get-Prop $Row 'ObjectKey' '')
    if ($ObjectKey) { return $ObjectKey.ToLowerInvariant() }
    return ''
}

function Index-RowsByRecoveryIdentity($Rows) {
    $Index = @{}
    foreach ($Row in @($Rows)) {
        $RecoveryKey = Get-RecoveryIdentityKeyForRow $Row
        if (!$RecoveryKey) { continue }
        if (!$Index.ContainsKey($RecoveryKey)) {
            $Index[$RecoveryKey] = $Row
            continue
        }
        $OldSortValue = Get-RowSortValue $Index[$RecoveryKey]
        $NewSortValue = Get-RowSortValue $Row
        if ($NewSortValue -ge $OldSortValue) { $Index[$RecoveryKey] = $Row }
    }
    return $Index
}

function Merge-RowsByRecoveryIdentity($Rows) {
    $Index = Index-RowsByRecoveryIdentity $Rows
    return @($Index.Values)
}

function Process-DetailedRuns(
    $Runs,
    [string]$Incident,
    [string]$WindowKey,
    [string]$ClusterDisplayName,
    [string]$ClusterId,
    $EnvironmentSpec,
    [string]$ProtectionGroupName,
    [string]$ProtectionGroupId,
    $PreviousRowsForProtectionGroup
) {
    $PreviousByKey = Index-RowsByObjectKey $PreviousRowsForProtectionGroup
    $PreviousByRecoveryKey = Index-RowsByRecoveryIdentity $PreviousRowsForProtectionGroup
    $EvidenceByKey = @{}
    $ObjectIdToName = @{}
    $PhysicalHostById = @{}
    $EnvironmentFilterSet = @($EnvironmentSpec.Filter.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })

    foreach ($Run in @($Runs)) {
        foreach ($RunObject in As-Array (Get-Prop $Run 'objects' @())) {
            $ObjectMeta = Get-Prop $RunObject 'object' $null
            if (!$ObjectMeta) { continue }
            $ObjectId = Clean (Get-Prop $ObjectMeta 'id' '')
            $ObjectName = Clean (Get-Prop $ObjectMeta 'name' '')
            $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
            $ObjectEnvironment = Clean (Get-Prop $ObjectMeta 'environment' '')
            if ($ObjectId -and $ObjectName -and !$ObjectIdToName.ContainsKey($ObjectId)) { $ObjectIdToName[$ObjectId] = $ObjectName }
            if ($ObjectId -and $ObjectName -and ($ObjectType -eq 'kHost' -or $ObjectEnvironment -eq 'kPhysical')) { $PhysicalHostById[$ObjectId] = $ObjectName }
        }
    }

    $RunsSorted = @($Runs | Sort-Object { Get-RunUsecs $_ } -Descending)
    foreach ($Run in $RunsSorted) {
        $RunInfo = Get-FirstRunInfo $Run
        if (!$RunInfo) { continue }
        $RunStatus = Get-RunStatus $Run
        $RunType = Get-RunType $Run
        $RunUsecs = Get-RunUsecs $Run
        $RunDateKey = Convert-UsecsToEtDateKey $RunUsecs

        foreach ($RunObject in As-Array (Get-Prop $Run 'objects' @())) {
            if (!(Test-TargetObject -RunObject $RunObject -EnvironmentSpec $EnvironmentSpec -EnvironmentFilterSet $EnvironmentFilterSet)) { continue }
            $ObjectMeta = Get-Prop $RunObject 'object' $null
            if (!$ObjectMeta) { continue }
            $ObjectKey = Get-ObjectIdentityKey -RunObject $RunObject -ClusterId $ClusterId -ProtectionGroupId $ProtectionGroupId -EnvironmentLabel $EnvironmentSpec.Label -RunType $RunType
            if (!$ObjectKey) { continue }

            if (!$EvidenceByKey.ContainsKey($ObjectKey)) {
                $EvidenceByKey[$ObjectKey] = @{
                    LatestState = ''
                    LatestUsecs = [int64]0
                    LatestSuccessUsecs = [int64]0
                    LatestSuccessRunType = ''
                    LatestRunningUsecs = [int64]0
                    LastTerminalProblemUsecs = [int64]0
                    LatestTerminalProblemState = ''
                    FirstFailedUsecs = [int64]0
                    LastFailedUsecs = [int64]0
                    ObjectName = ''
                    ObjectType = ''
                    ParentHostName = ''
                    RunType = $RunType
                    Message = ''
                    FailureDates = @{}
                    FailedRunCount = 0
                    FailedRunKeys = New-Object System.Collections.Generic.List[string]
                    RecoveryIdentityKey = ''
                }
            }
            $Entry = $EvidenceByKey[$ObjectKey]
            $ObjectState = Get-ObjectState -RunObject $RunObject -RunStatus $RunStatus
            $ObjectName = Clean (Get-Prop $ObjectMeta 'name' '')
            $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
            $ParentHostName = ''
            if ($EnvironmentSpec.ParentHostNeeded) { $ParentHostName = Get-ObjectParentHostName -RunObject $RunObject -ObjectIdToName $ObjectIdToName -PhysicalHostById $PhysicalHostById }
            if (!$Entry.RecoveryIdentityKey) {
                $Entry.RecoveryIdentityKey = (('{0}|{1}|{2}|{3}|{4}|{5}' -f $ClusterId,$ProtectionGroupId,$EnvironmentSpec.Label,$ObjectType,$ParentHostName,$ObjectName).ToLowerInvariant())
            }
            $ObjectMessage = Get-ObjectMessage -RunObject $RunObject -RunInfo $RunInfo
            if (!$ObjectMessage -and $ObjectState -eq 'Failure') { $ObjectMessage = 'Object-level failure detected.' }
            if (!$ObjectMessage -and $ObjectState -eq 'Running') { $ObjectMessage = 'Latest object or run state is running.' }
            if (!$ObjectMessage -and $ObjectState -eq 'Cancelled') { $ObjectMessage = 'Latest object or run state is cancelled.' }

            if (!$Entry.LatestState -or $RunUsecs -gt [int64]$Entry.LatestUsecs) {
                $Entry.LatestState = $ObjectState
                $Entry.LatestUsecs = $RunUsecs
                $Entry.ObjectName = $ObjectName
                $Entry.ObjectType = $ObjectType
                $Entry.ParentHostName = $ParentHostName
                $Entry.RunType = $RunType
                $Entry.Message = $ObjectMessage
            }

            if ($ObjectState -eq 'Success' -and $RunUsecs -gt [int64]$Entry.LatestSuccessUsecs) {
                # V6 latest-success-over-running rule:
                # A newer/current Running row must not reopen or keep an incident active
                # when the same protected object already has a successful backup and
                # no later terminal Failure/Cancelled exists.
                $Entry.LatestSuccessUsecs = $RunUsecs
                $Entry.LatestSuccessRunType = $RunType
            }

            if ($ObjectState -eq 'Running' -and $RunUsecs -gt [int64]$Entry.LatestRunningUsecs) {
                $Entry.LatestRunningUsecs = $RunUsecs
            }

            if ($ObjectState -in @('Failure','Cancelled') -and $RunUsecs -gt [int64]$Entry.LastTerminalProblemUsecs) {
                $Entry.LastTerminalProblemUsecs = $RunUsecs
                $Entry.LatestTerminalProblemState = $ObjectState
            }

            if ($ObjectState -in @('Failure','Running','Cancelled')) {
                Add-DateToSet -DateSet $Entry.FailureDates -DateKey $RunDateKey
                $Entry.FailedRunCount = [int]$Entry.FailedRunCount + 1
                $Entry.FailedRunKeys.Add(('{0}|{1}|{2}' -f $RunType,$ObjectState,$RunUsecs)) | Out-Null
                if ($Entry.FirstFailedUsecs -eq 0 -or $RunUsecs -lt $Entry.FirstFailedUsecs) { $Entry.FirstFailedUsecs = $RunUsecs }
                if ($RunUsecs -gt $Entry.LastFailedUsecs) { $Entry.LastFailedUsecs = $RunUsecs }
                if (!$Entry.Message -and $ObjectMessage) { $Entry.Message = $ObjectMessage }
            }
            $EvidenceByKey[$ObjectKey] = $Entry
        }
    }

    $ActiveRows = @()
    $ClearedRows = @()
    foreach ($ObjectKey in $EvidenceByKey.Keys) {
        $Entry = $EvidenceByKey[$ObjectKey]
        $PreviousRow = $null
        if ($PreviousByKey.ContainsKey($ObjectKey)) { $PreviousRow = $PreviousByKey[$ObjectKey] }
        elseif ($Entry.RecoveryIdentityKey -and $PreviousByRecoveryKey.ContainsKey($Entry.RecoveryIdentityKey)) { $PreviousRow = $PreviousByRecoveryKey[$Entry.RecoveryIdentityKey] }
        $OldFailureDates = ''
        if ($PreviousRow) { $OldFailureDates = Clean (Get-Prop $PreviousRow 'FailureDates' '') }
        $MergedFailureDates = Merge-DateStrings -OldDates $OldFailureDates -NewDateSet $Entry.FailureDates

        $HasSuccessForObject = ([int64]$Entry.LatestSuccessUsecs -gt 0)
        $HasTerminalProblemAfterSuccess = ([int64]$Entry.LastTerminalProblemUsecs -gt [int64]$Entry.LatestSuccessUsecs)

        if ($HasSuccessForObject -and !$HasTerminalProblemAfterSuccess) {
            # V6 success-suppresses-running rule:
            # Running is not a terminal failure. If the same protected object has a success
            # and no later terminal Failure/Cancelled exists, do not report Running as active.
            # This also clears older Failure/Cancelled/Running evidence for the same object.
            if ($PreviousRow -or [int64]$Entry.LastTerminalProblemUsecs -gt 0) {
                $ClearMessage = 'Previously failed/running/cancelled object has newer successful backup.'
                if (!$PreviousRow -and [int64]$Entry.LastTerminalProblemUsecs -gt 0) {
                    $ClearMessage = 'Earlier failed/cancelled object state in this scan has newer successful backup.'
                }
                if ([int64]$Entry.LatestRunningUsecs -gt [int64]$Entry.LatestSuccessUsecs) {
                    $ClearMessage = ($ClearMessage + ' Newer running attempt is ignored until it finishes because the last completed backup is successful.')
                }
                $ClearRunType = Clean $Entry.LatestSuccessRunType
                if (!$ClearRunType) { $ClearRunType = $Entry.RunType }
                $ClearedRows += New-StatusRow -Incident $Incident -WindowKey $WindowKey -Status 'Success' -Change 'Cleared' -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $Entry.ParentHostName -ObjectName $Entry.ObjectName -ObjectType $Entry.ObjectType -RunType $ClearRunType -FirstFailedUsecs 0 -LastFailedUsecs 0 -LatestSuccessUsecs $Entry.LatestSuccessUsecs -LastSeenUsecs $Entry.LatestSuccessUsecs -FailureDates $MergedFailureDates -FailedRunCount $Entry.FailedRunCount -Message $ClearMessage -ObjectKey $ObjectKey -EnvironmentFilter $EnvironmentSpec.Filter -FailedRunKeys (($Entry.FailedRunKeys | Select-Object -Unique) -join ' | ')
            }
            continue
        }

        if ($Entry.LatestState -eq 'Success') {
            # Recovery-aware rule:
            # If the latest object/snapshot state is Success, any older Failure/Running/Cancelled
            # evidence in the same scan is cleared by that newer success. This covers cases such as
            # a cancelled backup followed by a later successful backup before the next saved state exists.
            if ($PreviousRow -or [int]$Entry.FailedRunCount -gt 0) {
                $ClearMessage = 'Previously failed/running/cancelled object has newer successful backup.'
                if (!$PreviousRow -and [int]$Entry.FailedRunCount -gt 0) {
                    $ClearMessage = 'Earlier failed/running/cancelled object state in this scan has newer successful backup.'
                }
                $ClearRunType = Clean $Entry.LatestSuccessRunType
                if (!$ClearRunType) { $ClearRunType = $Entry.RunType }
                $ClearedRows += New-StatusRow -Incident $Incident -WindowKey $WindowKey -Status 'Success' -Change 'Cleared' -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $Entry.ParentHostName -ObjectName $Entry.ObjectName -ObjectType $Entry.ObjectType -RunType $ClearRunType -FirstFailedUsecs 0 -LastFailedUsecs 0 -LatestSuccessUsecs $Entry.LatestSuccessUsecs -LastSeenUsecs $Entry.LatestSuccessUsecs -FailureDates $MergedFailureDates -FailedRunCount $Entry.FailedRunCount -Message $ClearMessage -ObjectKey $ObjectKey -EnvironmentFilter $EnvironmentSpec.Filter -FailedRunKeys (($Entry.FailedRunKeys | Select-Object -Unique) -join ' | ')
            }
            continue
        }

        if ($Entry.LatestState -in @('Failure','Running','Cancelled')) {
            $ChangeValue = 'New'
            if ($PreviousRow) { $ChangeValue = 'Existing' }
            $FirstFailedUsecs = $Entry.FirstFailedUsecs
            if ($PreviousRow -and (Clean (Get-Prop $PreviousRow 'FirstFailedET' ''))) { $FirstFailedUsecs = 0 }
            $ActiveRows += New-StatusRow -Incident $Incident -WindowKey $WindowKey -Status $Entry.LatestState -Change $ChangeValue -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $Entry.ParentHostName -ObjectName $Entry.ObjectName -ObjectType $Entry.ObjectType -RunType $Entry.RunType -FirstFailedUsecs $FirstFailedUsecs -LastFailedUsecs $Entry.LastFailedUsecs -LatestSuccessUsecs 0 -LastSeenUsecs $Entry.LatestUsecs -FailureDates $MergedFailureDates -FailedRunCount $Entry.FailedRunCount -Message $Entry.Message -ObjectKey $ObjectKey -EnvironmentFilter $EnvironmentSpec.Filter -FailedRunKeys (($Entry.FailedRunKeys | Select-Object -Unique) -join ' | ')
        }
    }
    return [pscustomobject]@{ Active=@($ActiveRows); Cleared=@($ClearedRows) }
}

function New-CarryForwardRow($PreviousRow, [string]$Reason) {
    $Copy = $PreviousRow | Select-Object *
    Set-ObjProp $Copy 'Change' 'CarriedForward'
    $PreviousMessage = Clean (Get-Prop $Copy 'Message' '')
    if ($Reason) {
        if ($PreviousMessage) { Set-ObjProp $Copy 'Message' (Clean ('{0} | {1}' -f $PreviousMessage,$Reason)) }
        else { Set-ObjProp $Copy 'Message' (Clean $Reason) }
    }
    return $Copy
}

function Get-TableText($Rows, [string[]]$Columns, [string]$EmptyText) {
    $List = @($Rows)
    if ($List.Count -eq 0) { return $EmptyText }
    return (($List | Select-Object -Property $Columns | Format-Table -AutoSize | Out-String).Trim())
}

$Window = Get-ComputeWindow
$IncidentResolution = Resolve-IncidentLock $Window
$IncidentEntry = $IncidentResolution.Entry
$ResolvedIncident = Clean (Get-Prop $IncidentEntry 'IncidentNumber' '')
$OutputFolder = Clean (Get-Prop $IncidentEntry 'OutputFolder' '')
if (!(Test-Path $OutputFolder)) { New-Item -Path $OutputFolder -ItemType Directory -Force | Out-Null }
$LockPath = Enter-CollectorLock $OutputFolder

try {
    $CurrentState = Read-StateWithBackup $OutputFolder
    $RunMode = 'Incremental'
    $SeedState = $CurrentState
    if ($IncidentResolution.IsNewWindow) {
        $RunMode = 'Baseline'
        $SeedState = Get-LatestPreviousState -CurrentFolder $OutputFolder
    } elseif (!$CurrentState) {
        Add-RunWarning 'Current window state is missing or unreadable. Forcing full 30-run baseline scan.'
        $RunMode = 'Baseline'
        $SeedState = Get-LatestPreviousState -CurrentFolder $OutputFolder
    }

    $ScanRunCount = $NumRuns
    if ($RunMode -eq 'Baseline') { $ScanRunCount = $BaselineNumRuns }
    if ($ScanRunCount -lt 1) { throw 'NumRuns must be greater than zero.' }

    $PreviousOpenRows = @()
    $PreviousClearedRows = @()
    if ($SeedState) {
        $PreviousOpenRows = @(As-Array (Get-Prop $SeedState 'CurrentOpenFailures' @()))
        $PreviousClearedRows = @(As-Array (Get-Prop $SeedState 'ClearedBySuccess' @()))
    }
    $PreviousOpenByProtectionGroup = Index-RowsByProtectionGroup $PreviousOpenRows
    $PreviousOpenByObjectKey = Index-RowsByObjectKey $PreviousOpenRows

    $ApiKey = Get-CohesityApiKeySafe
    $BaseHeaders = @{ accept='application/json'; apiKey=$ApiKey }
    $ClusterJson = Invoke-HeliosGetJson -Uri ('{0}/v2/mcm/cluster-mgmt/info' -f $BaseUrl) -Headers $BaseHeaders
    $Clusters = @($ClusterJson.cohesityClusters)
    if ($ClusterName) {
        $Clusters = @($Clusters | Where-Object { (Get-ClusterDisplayName $_) -eq $ClusterName })
        if ($Clusters.Count -eq 0) { throw ('Cluster not found: {0}' -f $ClusterName) }
    }
    $Clusters = @($Clusters | Sort-Object { Get-ClusterDisplayName $_ })

    $ActiveRows = @()
    $ClearedRows = @()
    $NoObjectEvidenceRows = @()
    $CheckedProtectionGroups = 0
    $DetailApiCalls = 0
    $LightApiCalls = 0
    $SkippedProtectionGroups = 0

    Write-Host ''
    Write-Host 'Running Cohesity backup failure collector.'
    Write-Host ('Incident        : {0}' -f $ResolvedIncident)
    Write-Host ('Window          : {0}' -f $Window.WindowLabel)
    Write-Host ('RunMode         : {0}' -f $RunMode)
    Write-Host ('Scan NumRuns    : {0}' -f $ScanRunCount)
    Write-Host 'RemoteAdapter   : excluded'
    Write-Host 'Decision model  : object-level only; recovery identity success clears stale state'
    Write-Host ''

    foreach ($EnvironmentSpec in Get-EnvironmentMap) {
        Write-Host ('Environment: {0}' -f $EnvironmentSpec.Label)
        foreach ($ClusterObject in $Clusters) {
            $ClusterId = Clean (Get-Prop $ClusterObject 'clusterId' '')
            $ClusterDisplayName = Get-ClusterDisplayName $ClusterObject
            $ProtectionGroups = Get-CohesityProtectionGroups -ClusterObject $ClusterObject -EnvironmentSpec $EnvironmentSpec -ApiKey $ApiKey
            if (@($ProtectionGroups).Count -eq 0) { continue }
            foreach ($ProtectionGroup in $ProtectionGroups) {
                $CheckedProtectionGroups++
                $ProtectionGroupId = Get-ProtectionGroupId $ProtectionGroup
                $ProtectionGroupName = Get-ProtectionGroupName $ProtectionGroup
                if (!$ProtectionGroupId) { continue }
                $ProtectionGroupKey = Get-ProtectionGroupKey -ClusterId $ClusterId -EnvironmentLabel $EnvironmentSpec.Label -ProtectionGroupId $ProtectionGroupId
                $PreviousRowsForProtectionGroup = @()
                if ($PreviousOpenByProtectionGroup.ContainsKey($ProtectionGroupKey)) { $PreviousRowsForProtectionGroup = @($PreviousOpenByProtectionGroup[$ProtectionGroupKey]) }

                $NeedsDetailFetch = $false
                $HasSuspiciousRun = $false
                if ($RunMode -eq 'Baseline') {
                    $NeedsDetailFetch = $true
                } elseif (@($PreviousRowsForProtectionGroup).Count -gt 0) {
                    $NeedsDetailFetch = $true
                } else {
                    try {
                        $LightRuns = Get-CohesityRuns -ClusterObject $ClusterObject -ProtectionGroupId $ProtectionGroupId -IncludeObjectDetails $false -RunCount $ScanRunCount -ApiKey $ApiKey
                        $LightApiCalls++
                        $HasSuspiciousRun = @($LightRuns | Where-Object { Test-SuspiciousRun $_ }).Count -gt 0
                        if ($HasSuspiciousRun) { $NeedsDetailFetch = $true }
                    } catch {
                        Add-RunWarning ('Light run lookup failed: {0} / {1} / {2}' -f $ClusterDisplayName,$ProtectionGroupName,$_.Exception.Message)
                    }
                }

                if (!$NeedsDetailFetch) {
                    $SkippedProtectionGroups++
                    continue
                }

                try {
                    $DetailedRuns = Get-CohesityRuns -ClusterObject $ClusterObject -ProtectionGroupId $ProtectionGroupId -IncludeObjectDetails $true -RunCount $ScanRunCount -ApiKey $ApiKey
                    $DetailApiCalls++
                } catch {
                    Add-RunWarning ('Object-detail run lookup failed: {0} / {1} / {2}' -f $ClusterDisplayName,$ProtectionGroupName,$_.Exception.Message)
                    continue
                }

                $ObjectEvidenceCount = 0
                foreach ($DetailedRun in @($DetailedRuns)) { $ObjectEvidenceCount += @(As-Array (Get-Prop $DetailedRun 'objects' @())).Count }
                if ($ObjectEvidenceCount -eq 0 -and ($HasSuspiciousRun -or $RunMode -eq 'Baseline')) {
                    $NoObjectEvidenceRows += [pscustomobject]@{
                        Cluster = $ClusterDisplayName
                        Environment = $EnvironmentSpec.Label
                        ProtectionGroup = $ProtectionGroupName
                        ProtectionGroupId = $ProtectionGroupId
                        Reason = 'Cohesity returned no run.objects. No PG-level object row was created.'
                    }
                }

                $Processed = Process-DetailedRuns -Runs $DetailedRuns -Incident $ResolvedIncident -WindowKey $Window.WindowKey -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -PreviousRowsForProtectionGroup $PreviousRowsForProtectionGroup
                $ActiveRows += @($Processed.Active)
                $ClearedRows += @($Processed.Cleared)
            }
        }
    }

    $ActiveRows = @(Merge-RowsByRecoveryIdentity $ActiveRows)
    $ClearedRows = @(Merge-RowsByRecoveryIdentity $ClearedRows)
    $ActiveByRecoveryKey = Index-RowsByRecoveryIdentity $ActiveRows
    $ClearedByRecoveryKey = Index-RowsByRecoveryIdentity $ClearedRows

    foreach ($PreviousRow in @($PreviousOpenRows)) {
        $PreviousRecoveryKey = Get-RecoveryIdentityKeyForRow $PreviousRow
        if (!$PreviousRecoveryKey) { continue }
        if ($ActiveByRecoveryKey.ContainsKey($PreviousRecoveryKey)) { continue }
        if ($ClearedByRecoveryKey.ContainsKey($PreviousRecoveryKey)) { continue }
        $ActiveRows += New-CarryForwardRow -PreviousRow $PreviousRow -Reason 'Known failed object not seen in current lookback; carried forward until same-object success is observed.'
    }

    # V5 final reconciliation rule:
    # Do not allow stale active rows from old state/ObjectKey formats to coexist with
    # newer same-object success. Recovery matching is done by protected-object identity,
    # not by the saved ObjectKey string.
    $ClearedRows = @(Merge-RowsByRecoveryIdentity $ClearedRows | Sort-Object Cluster,Environment,ProtectionGroup,ObjectName)
    $AllClearedRows = @(Merge-RowsByRecoveryIdentity @($PreviousClearedRows + $ClearedRows) | Sort-Object Cluster,Environment,ProtectionGroup,ObjectName)
    $AllClearedByRecoveryKey = Index-RowsByRecoveryIdentity $AllClearedRows

    # V6 final active suppression rule:
    # A Running row must not stay active when the same protected object has a successful
    # backup and no later terminal Failure/Cancelled. Running is pending evidence only;
    # it should not override the last completed successful backup.
    $ActiveRows = @(Merge-RowsByRecoveryIdentity $ActiveRows | Where-Object {
        $ActiveRecoveryKey = Get-RecoveryIdentityKeyForRow $_
        if (!$ActiveRecoveryKey -or !$AllClearedByRecoveryKey.ContainsKey($ActiveRecoveryKey)) { return $true }

        $ActiveStatus = Clean (Get-Prop $_ 'Status' '')
        $ActiveSortValue = Get-RowSortValue $_
        $ClearedSortValue = Get-RowSortValue $AllClearedByRecoveryKey[$ActiveRecoveryKey]

        if ($ActiveStatus -eq 'Running') { return $false }
        return ($ActiveSortValue -gt $ClearedSortValue)
    } | Sort-Object Cluster,Environment,ProtectionGroup,ObjectName)

    $LifecycleRows = @(Merge-RowsByRecoveryIdentity @($ActiveRows + $AllClearedRows) | Sort-Object Cluster,Environment,ProtectionGroup,ObjectName)

    Write-CsvRows -Rows $ActiveRows -Path (Join-Path $OutputFolder 'current_failures.csv') -Columns $script:CsvColumns
    Write-CsvRows -Rows $ClearedRows -Path (Join-Path $OutputFolder 'cleared_by_success.csv') -Columns $script:CsvColumns
    Write-CsvRows -Rows $LifecycleRows -Path (Join-Path $OutputFolder 'incident_lifecycle.csv') -Columns $script:CsvColumns
    Write-CsvRows -Rows $NoObjectEvidenceRows -Path (Join-Path $OutputFolder 'no_object_evidence_review.csv') -Columns $script:ReviewColumns

    $CollectionStatus = 'Complete'
    if ($script:CollectionIncomplete) { $CollectionStatus = 'Incomplete - RERUN REQUIRED' }

    $WorknoteLines = New-Object System.Collections.Generic.List[string]
    $WorknoteLines.Add('Cohesity Backup Failure Daily Update')
    $WorknoteLines.Add(('Incident: {0}' -f $ResolvedIncident))
    $WorknoteLines.Add(('Window: {0}' -f $Window.WindowLabel))
    $WorknoteLines.Add(('Run Mode: {0}' -f $RunMode))
    $WorknoteLines.Add(('Scan NumRuns: {0}' -f $ScanRunCount))
    $WorknoteLines.Add(('Collection Status: {0}' -f $CollectionStatus))
    $WorknoteLines.Add('')
    $WorknoteLines.Add('Failure Section:')
    $WorknoteLines.Add((Get-TableText -Rows $ActiveRows -Columns $script:WorknoteColumns -EmptyText 'No active object-level backup failures/running/cancelled states found.'))
    $WorknoteLines.Add('')
    $WorknoteLines.Add('Success Section:')
    $WorknoteLines.Add((Get-TableText -Rows $ClearedRows -Columns $script:WorknoteColumns -EmptyText 'No previously failed objects cleared by newer same-object success in this run.'))
    ($WorknoteLines -join [Environment]::NewLine) | Set-Content -Path (Join-Path $OutputFolder 'worknotes_summary.txt') -Encoding UTF8

    $WarningTextPath = Join-Path $OutputFolder 'collection_warnings.txt'
    if ($script:Warnings.Count -gt 0) { ($script:Warnings -join [Environment]::NewLine) | Set-Content -Path $WarningTextPath -Encoding UTF8 }
    else { 'None' | Set-Content -Path $WarningTextPath -Encoding UTF8 }

    $ClosingLines = New-Object System.Collections.Generic.List[string]
    $ClosingLines.Add(('Incident: {0}' -f $ResolvedIncident))
    $ClosingLines.Add(('Window: {0}' -f $Window.WindowLabel))
    $ClosingLines.Add(('Collection Status: {0}' -f $CollectionStatus))
    if ($script:CollectionIncomplete) {
        $ClosingLines.Add('Closure Recommendation: Do not close. Collection incomplete; rerun required.')
    } elseif ($ActiveRows.Count -gt 0) {
        $ClosingLines.Add(('Closure Recommendation: Do not close. Active object-level failures remain: {0}' -f $ActiveRows.Count))
    } else {
        $ClosingLines.Add('Closure Recommendation: Candidate for closure. No active object-level failures remain.')
    }
    $ClosingLines.Add(('Cleared This Run: {0}' -f $ClearedRows.Count))
    ($ClosingLines -join [Environment]::NewLine) | Set-Content -Path (Join-Path $OutputFolder 'closing_summary.txt') -Encoding UTF8

    $StateObject = [pscustomobject]@{
        IncidentNumber = $ResolvedIncident
        WindowKey = $Window.WindowKey
        WindowLabel = $Window.WindowLabel
        GeneratedET = Get-NowEtText
        RunMode = $RunMode
        BaselineNumRuns = $BaselineNumRuns
        IncrementalNumRuns = $NumRuns
        ScanRunCount = $ScanRunCount
        RemoteAdapterExcluded = $true
        CollectionStatus = $CollectionStatus
        CheckedProtectionGroups = $CheckedProtectionGroups
        DetailApiCalls = $DetailApiCalls
        LightApiCalls = $LightApiCalls
        SkippedProtectionGroups = $SkippedProtectionGroups
        Warnings = @($script:Warnings)
        CurrentOpenFailures = @($ActiveRows)
        ClearedBySuccess = @($AllClearedRows)
        LastRunClearedBySuccess = @($ClearedRows)
        NoObjectEvidenceReview = @($NoObjectEvidenceRows)
    }
    Write-JsonAtomic -ObjectValue $StateObject -Path (Join-Path $OutputFolder 'state.json')

    Write-Host ''
    Write-Host 'Cohesity backup failure collector completed.'
    Write-Host ('Collection Status       : {0}' -f $CollectionStatus)
    Write-Host ('Run Mode                : {0}' -f $RunMode)
    Write-Host ('ProtectionGroups checked: {0}' -f $CheckedProtectionGroups)
    Write-Host ('Detail API calls        : {0}' -f $DetailApiCalls)
    Write-Host ('Light API calls         : {0}' -f $LightApiCalls)
    Write-Host ('Skipped PGs             : {0}' -f $SkippedProtectionGroups)
    Write-Host ('Active object rows      : {0}' -f $ActiveRows.Count)
    Write-Host ('Cleared object rows     : {0}' -f $ClearedRows.Count)
    Write-Host ('Output folder           : {0}' -f $OutputFolder)
}
finally {
    if ($LockPath -and (Test-Path $LockPath)) { Remove-Item -Path $LockPath -Force -ErrorAction SilentlyContinue }
}
