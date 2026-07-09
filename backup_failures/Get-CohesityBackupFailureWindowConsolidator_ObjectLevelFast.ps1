<#
.SYNOPSIS
Experimental fast object-level Cohesity backup failure collector.

.DESCRIPTION
Separate test script. Does not replace the production wrapper or collector.

Rules:
- Protection Group is context only.
- Object is the decision point.
- PG/run status is used only as a lightweight filter before fetching object details.
- RemoteAdapter is excluded.
- No PG-level failure rows are written to current_failures.csv.
- If a suspicious PG has no run.objects evidence, it is written to no_object_evidence_review.csv only.

Operator-facing Status values:
- Failure
- Success
- Cancelled
- Running

Internal comparison Change values:
- New
- Existing
- CarriedForward
- Cleared
- PreviouslyCleared

Important PowerShell safety:
- Does not use reserved/automatic variable names such as $Host for local variables or parameters.
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'https://helios.cohesity.com',
    [string]$OutputRoot = 'X:\PowerShell\Data\Cohesity\BackupFailureWindow_ObjectLevelFast',
    [string]$HelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1',
    [string]$EncryptedFile = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc',
    [string]$ClusterName = '',
    [string]$IncidentNumber = '',
    [int]$NumRuns = 20,
    [int]$RequestTimeoutSec = 60,
    [switch]$ResetState
)

$ErrorActionPreference = 'Stop'
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:Columns = @('IncidentNumber','Status','Change','Cluster','Environment','ProtectionGroup','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','LatestSuccessET','LastSeenET','Message','ObjectKey','ClusterId','ProtectionGroupId','EnvironmentFilter','FailureRuns')
$script:ReviewColumns = @('Cluster','Environment','ProtectionGroup','ProtectionGroupId','Reason')
$script:WorknoteColumns = @('Status','Change','Cluster','ProtectionGroup','Environment','Host','ObjectName','ObjectType','RunType','FirstFailedET','LastFailedET','LatestSuccessET','Message')

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

function Get-NowEtText {
    ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $script:EtZone)).ToString('yyyy-MM-dd HH:mm:ss')
}

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

function Parse-DateText([string]$TextValue) {
    $CleanText = Clean $TextValue
    if (!$CleanText) { return $null }
    try { return [datetime]::Parse($CleanText) } catch { return $null }
}

function Get-DateSortValue([string]$TextValue) {
    $ParsedDate = Parse-DateText $TextValue
    if ($ParsedDate) { return $ParsedDate.ToString('yyyy-MM-dd HH:mm:ss') }
    return '0000-00-00 00:00:00'
}

function Read-Json([string]$Path) {
    if (!(Test-Path $Path)) { return $null }
    try {
        $RawText = Get-Content -Path $Path -Raw
        if ([string]::IsNullOrWhiteSpace($RawText)) { return $null }
        return ($RawText | ConvertFrom-Json)
    } catch {
        return $null
    }
}

function Write-Json($ObjectValue, [string]$Path) {
    $DirectoryPath = Split-Path $Path -Parent
    if (!(Test-Path $DirectoryPath)) { New-Item -Path $DirectoryPath -ItemType Directory -Force | Out-Null }
    $ObjectValue | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Write-CsvFile($Rows, [string]$Path, [string[]]$Columns) {
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
    if (!(Test-Path $EncryptedFile)) { throw ('Missing encrypted API key file: {0}' -f $EncryptedFile) }
    . $HelperPath
    $ApiKeyValue = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedFile
    if ([string]::IsNullOrWhiteSpace($ApiKeyValue)) { throw 'API key helper returned a blank value.' }
    return $ApiKeyValue.Trim()
}

function Get-ClusterDisplayName($ClusterObject) {
    $NameValue = Clean (Get-Prop $ClusterObject 'name' '')
    if (!$NameValue) { $NameValue = Clean (Get-Prop $ClusterObject 'clusterName' '') }
    if (!$NameValue) { $NameValue = Clean (Get-Prop $ClusterObject 'displayName' '') }
    if (!$NameValue) { $NameValue = ('Unknown-{0}' -f (Clean (Get-Prop $ClusterObject 'clusterId' ''))) }
    return $NameValue
}

function Get-EnvironmentSpecs {
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
function Test-SuccessStatus([string]$Status) { (Clean $Status) -in @('Succeeded','kSucceeded') }
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
    if (Test-RunningStatus $RunStatusValue) { return $true }
    if (Test-CancelledStatus $RunStatusValue) { return $true }
    if (Test-WarningStatus $RunStatusValue) { return $true }
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
    return ('{0}|{1}|{2}|{3}|{4}' -f $ClusterId,$ProtectionGroupId,$EnvironmentLabel,$RunType,$IdentityPart)
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
    if (@(Get-FailedAttempts $RunObject).Count -gt 0) { return 'Failure' }
    $ObjectStatuses = @(Get-ObjectStatusValues $RunObject)
    if (@($ObjectStatuses | Where-Object { Test-FailedStatus $_ }).Count -gt 0) { return 'Failure' }
    if (@($ObjectStatuses | Where-Object { Test-CancelledStatus $_ }).Count -gt 0) { return 'Cancelled' }
    if (@($ObjectStatuses | Where-Object { Test-RunningStatus $_ }).Count -gt 0) { return 'Running' }
    if (Test-CancelledStatus $RunStatus) { return 'Cancelled' }
    if (Test-RunningStatus $RunStatus) { return 'Running' }
    return 'Success'
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

function Test-TargetObject($RunObject, $EnvironmentSpec, [string[]]$EnvironmentFilterSet) {
    $ObjectMeta = Get-Prop $RunObject 'object' $null
    if (!$ObjectMeta) { return $false }
    $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
    $ObjectEnvironment = Clean (Get-Prop $ObjectMeta 'environment' '')
    if ($EnvironmentSpec.Label -in @('GenericNas','Isilon')) { return $true }
    if ($ObjectType -ne $EnvironmentSpec.TargetType) { return $false }
    if (!$ObjectEnvironment) { return $true }
    return ($EnvironmentFilterSet -contains $ObjectEnvironment)
}

function New-ObjectStatusRow(
    [string]$Incident,
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
    [string]$Message,
    [string]$ObjectKey,
    $FailureRuns
) {
    [pscustomobject]@{
        IncidentNumber = Clean $Incident
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
        Message = Clean $Message
        ObjectKey = Clean $ObjectKey
        ClusterId = Clean $ClusterId
        ProtectionGroupId = Clean $ProtectionGroupId
        EnvironmentFilter = Clean $EnvironmentSpec.Filter
        FailureRuns = Clean $FailureRuns
    }
}

function Merge-RowsByObjectKey($Rows) {
    $Index = @{}
    foreach ($Row in @($Rows)) {
        $ObjectKey = Clean (Get-Prop $Row 'ObjectKey' '')
        if (!$ObjectKey) { continue }
        if (!$Index.ContainsKey($ObjectKey)) {
            $Index[$ObjectKey] = $Row
        } else {
            $OldSortValue = Get-DateSortValue (Get-Prop $Index[$ObjectKey] 'LastSeenET' '')
            $NewSortValue = Get-DateSortValue (Get-Prop $Row 'LastSeenET' '')
            if ($NewSortValue -ge $OldSortValue) { $Index[$ObjectKey] = $Row }
        }
    }
    return @($Index.Values)
}

function Index-PreviousOpenByProtectionGroup($Rows) {
    $Index = @{}
    foreach ($Row in @($Rows)) {
        $ProtectionGroupKey = Get-ProtectionGroupKey (Clean (Get-Prop $Row 'ClusterId' '')) (Clean (Get-Prop $Row 'Environment' '')) (Clean (Get-Prop $Row 'ProtectionGroupId' ''))
        if (!$ProtectionGroupKey.Trim('|')) { continue }
        if (!$Index.ContainsKey($ProtectionGroupKey)) { $Index[$ProtectionGroupKey] = @() }
        $Index[$ProtectionGroupKey] = @($Index[$ProtectionGroupKey] + $Row)
    }
    return $Index
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

function Get-CohesityRuns($ClusterObject, [string]$ProtectionGroupId, [bool]$IncludeObjectDetails, [string]$ApiKey) {
    $ClusterId = Clean (Get-Prop $ClusterObject 'clusterId' '')
    $Headers = @{ accept='application/json'; apiKey=$ApiKey; accessClusterId=$ClusterId }
    $EscapedProtectionGroupId = [uri]::EscapeDataString($ProtectionGroupId)
    if ($IncludeObjectDetails) {
        $Uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false&includeObjectDetails=true' -f $BaseUrl,$EscapedProtectionGroupId,$NumRuns)
    } else {
        $Uri = ('{0}/v2/data-protect/protection-groups/{1}/runs?numRuns={2}&excludeNonRestorableRuns=false' -f $BaseUrl,$EscapedProtectionGroupId,$NumRuns)
    }
    $Json = Invoke-HeliosGetJson -Uri $Uri -Headers $Headers
    if ($Json -and $Json.runs) { return @($Json.runs) }
    return @()
}

function Process-DetailedObjectRuns($Runs, [string]$Incident, [string]$ClusterDisplayName, [string]$ClusterId, $EnvironmentSpec, [string]$ProtectionGroupName, [string]$ProtectionGroupId, $PreviousRowsForProtectionGroup) {
    $ActiveIndex = @{}
    $ClearedIndex = @{}
    $LatestStateByObject = @{}
    $LatestSuccessByObject = @{}
    $FailureCountByObject = @{}
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

        foreach ($RunObject in As-Array (Get-Prop $Run 'objects' @())) {
            if (!(Test-TargetObject -RunObject $RunObject -EnvironmentSpec $EnvironmentSpec -EnvironmentFilterSet $EnvironmentFilterSet)) { continue }
            $ObjectMeta = Get-Prop $RunObject 'object' $null
            if (!$ObjectMeta) { continue }
            $ObjectKey = Get-ObjectIdentityKey -RunObject $RunObject -ClusterId $ClusterId -ProtectionGroupId $ProtectionGroupId -EnvironmentLabel $EnvironmentSpec.Label -RunType $RunType
            if (!$ObjectKey) { continue }

            $ObjectState = Get-ObjectState -RunObject $RunObject -RunStatus $RunStatus
            $ObjectName = Clean (Get-Prop $ObjectMeta 'name' '')
            $ObjectType = Clean (Get-Prop $ObjectMeta 'objectType' '')
            $ParentHostName = ''
            if ($EnvironmentSpec.ParentHostNeeded) { $ParentHostName = Get-ObjectParentHostName -RunObject $RunObject -ObjectIdToName $ObjectIdToName -PhysicalHostById $PhysicalHostById }
            $ObjectMessage = Get-ObjectMessage -RunObject $RunObject -RunInfo $RunInfo
            if (!$ObjectMessage -and $ObjectState -eq 'Failure') { $ObjectMessage = 'Object-level failure detected.' }
            if (!$ObjectMessage -and $ObjectState -eq 'Running') { $ObjectMessage = 'Latest object or run state is running.' }
            if (!$ObjectMessage -and $ObjectState -eq 'Cancelled') { $ObjectMessage = 'Latest object or run state is cancelled.' }

            if ($ObjectState -eq 'Success') {
                if (!$LatestStateByObject.ContainsKey($ObjectKey)) {
                    $LatestStateByObject[$ObjectKey] = 'Success'
                    $LatestSuccessByObject[$ObjectKey] = [pscustomobject]@{
                        Usecs=$RunUsecs; ObjectName=$ObjectName; ObjectType=$ObjectType; ParentHostName=$ParentHostName; RunType=$RunType; Message=$ObjectMessage
                    }
                }
                continue
            }

            if ($ObjectState -in @('Failure','Running','Cancelled')) {
                if (!$FailureCountByObject.ContainsKey($ObjectKey)) { $FailureCountByObject[$ObjectKey] = 0 }
                $FailureCountByObject[$ObjectKey] = [int]$FailureCountByObject[$ObjectKey] + 1

                if (!$LatestStateByObject.ContainsKey($ObjectKey)) {
                    $LatestStateByObject[$ObjectKey] = $ObjectState
                    $ChangeValue = 'New'
                    if (@($PreviousRowsForProtectionGroup | Where-Object { (Clean (Get-Prop $_ 'ObjectKey' '')) -eq $ObjectKey }).Count -gt 0) { $ChangeValue = 'Existing' }
                    $ActiveIndex[$ObjectKey] = New-ObjectStatusRow -Incident $Incident -Status $ObjectState -Change $ChangeValue -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $ParentHostName -ObjectName $ObjectName -ObjectType $ObjectType -RunType $RunType -FirstFailedUsecs $RunUsecs -LastFailedUsecs $RunUsecs -LatestSuccessUsecs 0 -LastSeenUsecs $RunUsecs -Message $ObjectMessage -ObjectKey $ObjectKey -FailureRuns $FailureCountByObject[$ObjectKey]
                    continue
                }

                if ($LatestStateByObject[$ObjectKey] -eq 'Success') {
                    if (!$ClearedIndex.ContainsKey($ObjectKey)) {
                        $SuccessData = $LatestSuccessByObject[$ObjectKey]
                        $ClearedIndex[$ObjectKey] = New-ObjectStatusRow -Incident $Incident -Status 'Success' -Change 'Cleared' -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $SuccessData.ParentHostName -ObjectName $SuccessData.ObjectName -ObjectType $SuccessData.ObjectType -RunType $SuccessData.RunType -FirstFailedUsecs $RunUsecs -LastFailedUsecs $RunUsecs -LatestSuccessUsecs $SuccessData.Usecs -LastSeenUsecs $SuccessData.Usecs -Message 'Previously failed object has newer successful backup.' -ObjectKey $ObjectKey -FailureRuns $FailureCountByObject[$ObjectKey]
                    }
                    continue
                }

                if ($ActiveIndex.ContainsKey($ObjectKey)) {
                    $ExistingRow = $ActiveIndex[$ObjectKey]
                    $ExistingFirstFailed = Get-DateSortValue (Get-Prop $ExistingRow 'FirstFailedET' '')
                    $ThisFailed = Convert-UsecsToEtText $RunUsecs
                    if ((Get-DateSortValue $ThisFailed) -lt $ExistingFirstFailed) { Set-ObjProp $ExistingRow 'FirstFailedET' $ThisFailed }
                    Set-ObjProp $ExistingRow 'FailureRuns' $FailureCountByObject[$ObjectKey]
                    if (!$ExistingRow.Message -and $ObjectMessage) { Set-ObjProp $ExistingRow 'Message' $ObjectMessage }
                    $ActiveIndex[$ObjectKey] = $ExistingRow
                }
            }
        }
    }

    foreach ($PreviousRow in @($PreviousRowsForProtectionGroup)) {
        $PreviousObjectKey = Clean (Get-Prop $PreviousRow 'ObjectKey' '')
        if (!$PreviousObjectKey) { continue }
        if ($ActiveIndex.ContainsKey($PreviousObjectKey)) { continue }
        if ($ClearedIndex.ContainsKey($PreviousObjectKey)) { continue }

        if ($LatestStateByObject.ContainsKey($PreviousObjectKey) -and $LatestStateByObject[$PreviousObjectKey] -eq 'Success') {
            $SuccessData = $LatestSuccessByObject[$PreviousObjectKey]
            $ClearedIndex[$PreviousObjectKey] = New-ObjectStatusRow -Incident $Incident -Status 'Success' -Change 'Cleared' -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName $SuccessData.ParentHostName -ObjectName $SuccessData.ObjectName -ObjectType $SuccessData.ObjectType -RunType $SuccessData.RunType -FirstFailedUsecs 0 -LastFailedUsecs 0 -LatestSuccessUsecs $SuccessData.Usecs -LastSeenUsecs $SuccessData.Usecs -Message 'Previously failed object has newer successful backup.' -ObjectKey $PreviousObjectKey -FailureRuns 0
        } else {
            $ActiveIndex[$PreviousObjectKey] = New-ObjectStatusRow -Incident $Incident -Status (Clean (Get-Prop $PreviousRow 'Status' 'Failure')) -Change 'CarriedForward' -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -ParentHostName (Get-Prop $PreviousRow 'Host' '') -ObjectName (Get-Prop $PreviousRow 'ObjectName' '') -ObjectType (Get-Prop $PreviousRow 'ObjectType' '') -RunType (Get-Prop $PreviousRow 'RunType' '') -FirstFailedUsecs 0 -LastFailedUsecs 0 -LatestSuccessUsecs 0 -LastSeenUsecs 0 -Message (Get-Prop $PreviousRow 'Message' '') -ObjectKey $PreviousObjectKey -FailureRuns (Get-Prop $PreviousRow 'FailureRuns' '')
        }
    }

    return [pscustomobject]@{ Active=@($ActiveIndex.Values); Cleared=@($ClearedIndex.Values) }
}

if (!(Test-Path $OutputRoot)) { New-Item -Path $OutputRoot -ItemType Directory -Force | Out-Null }
if (!$IncidentNumber) { $IncidentNumber = Read-Host 'Enter incident number for fast object-level test' }
$IncidentNumber = $IncidentNumber.Trim().ToUpper()
if (!$IncidentNumber) { throw 'IncidentNumber is required.' }
$OutputFolder = Join-Path $OutputRoot $IncidentNumber
if (!(Test-Path $OutputFolder)) { New-Item -Path $OutputFolder -ItemType Directory -Force | Out-Null }
$StatePath = Join-Path $OutputFolder 'state.json'
if ($ResetState -and (Test-Path $StatePath)) { Remove-Item -Path $StatePath -Force }

$OldState = Read-Json $StatePath
$PreviousOpenRows = @()
$PreviousClearedRows = @()
if ($OldState) {
    $PreviousOpenRows = @(As-Array (Get-Prop $OldState 'CurrentOpenFailures' @()))
    $PreviousClearedRows = @(As-Array (Get-Prop $OldState 'ClearedBySuccess' @()))
}
$PreviousOpenByProtectionGroup = Index-PreviousOpenByProtectionGroup $PreviousOpenRows

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
$LightApiCalls = 0
$DetailApiCalls = 0
$SkippedCleanProtectionGroups = 0
$CheckedProtectionGroups = 0

Write-Host ''
Write-Host 'Running experimental fast object-level collector.'
Write-Host ('OutputRoot : {0}' -f $OutputRoot)
Write-Host ('Incident   : {0}' -f $IncidentNumber)
Write-Host ('NumRuns    : {0}' -f $NumRuns)
Write-Host ('Timeout    : {0}' -f $RequestTimeoutSec)
Write-Host 'RemoteAdapter is excluded.'
Write-Host 'Protection Group is context only. Final decisions are object-level.'
Write-Host ''

$ClusterCounter = 0
foreach ($ClusterObject in $Clusters) {
    $ClusterCounter++
    $ClusterId = Clean (Get-Prop $ClusterObject 'clusterId' '')
    $ClusterDisplayName = Get-ClusterDisplayName $ClusterObject
    Write-Host ('[{0}/{1}] Cluster: {2}' -f $ClusterCounter,$Clusters.Count,$ClusterDisplayName)

    foreach ($EnvironmentSpec in Get-EnvironmentSpecs) {
        $EnvironmentProtectionGroups = 0
        $EnvironmentDetailCalls = 0
        $EnvironmentSkipped = 0
        $ProtectionGroups = Get-CohesityProtectionGroups -ClusterObject $ClusterObject -EnvironmentSpec $EnvironmentSpec -ApiKey $ApiKey

        foreach ($ProtectionGroup in $ProtectionGroups) {
            $EnvironmentProtectionGroups++
            $CheckedProtectionGroups++
            $ProtectionGroupId = Get-ProtectionGroupId $ProtectionGroup
            $ProtectionGroupName = Get-ProtectionGroupName $ProtectionGroup
            if (!$ProtectionGroupId) { continue }
            $ProtectionGroupKey = Get-ProtectionGroupKey -ClusterId $ClusterId -EnvironmentLabel $EnvironmentSpec.Label -ProtectionGroupId $ProtectionGroupId
            $PreviousRowsForProtectionGroup = @()
            if ($PreviousOpenByProtectionGroup.ContainsKey($ProtectionGroupKey)) { $PreviousRowsForProtectionGroup = @($PreviousOpenByProtectionGroup[$ProtectionGroupKey]) }

            try {
                $LightRuns = Get-CohesityRuns -ClusterObject $ClusterObject -ProtectionGroupId $ProtectionGroupId -IncludeObjectDetails $false -ApiKey $ApiKey
                $LightApiCalls++
            } catch {
                Add-RunWarning ('Light runs lookup failed: {0} / {1} / {2}' -f $ClusterDisplayName,$ProtectionGroupName,$_.Exception.Message)
                continue
            }

            if (@($LightRuns).Count -eq 0) { continue }
            $HasSuspiciousRun = @($LightRuns | Where-Object { Test-SuspiciousRun $_ }).Count -gt 0
            $NeedsDetailFetch = ($HasSuspiciousRun -or @($PreviousRowsForProtectionGroup).Count -gt 0)
            if (!$NeedsDetailFetch) {
                $SkippedCleanProtectionGroups++
                $EnvironmentSkipped++
                continue
            }

            try {
                $DetailedRuns = Get-CohesityRuns -ClusterObject $ClusterObject -ProtectionGroupId $ProtectionGroupId -IncludeObjectDetails $true -ApiKey $ApiKey
                $DetailApiCalls++
                $EnvironmentDetailCalls++
            } catch {
                Add-RunWarning ('Detail runs lookup failed: {0} / {1} / {2}' -f $ClusterDisplayName,$ProtectionGroupName,$_.Exception.Message)
                continue
            }

            $ObjectEvidenceCount = 0
            foreach ($DetailedRun in @($DetailedRuns)) { $ObjectEvidenceCount += @(As-Array (Get-Prop $DetailedRun 'objects' @())).Count }
            if ($ObjectEvidenceCount -eq 0 -and $HasSuspiciousRun) {
                $NoObjectEvidenceRows += [pscustomobject]@{
                    Cluster = $ClusterDisplayName
                    Environment = $EnvironmentSpec.Label
                    ProtectionGroup = $ProtectionGroupName
                    ProtectionGroupId = $ProtectionGroupId
                    Reason = 'Suspicious/failed PG run but Cohesity returned no run.objects. Not included in object-level failure output.'
                }
                continue
            }

            $Processed = Process-DetailedObjectRuns -Runs $DetailedRuns -Incident $IncidentNumber -ClusterDisplayName $ClusterDisplayName -ClusterId $ClusterId -EnvironmentSpec $EnvironmentSpec -ProtectionGroupName $ProtectionGroupName -ProtectionGroupId $ProtectionGroupId -PreviousRowsForProtectionGroup $PreviousRowsForProtectionGroup
            $ActiveRows += @($Processed.Active)
            $ClearedRows += @($Processed.Cleared)
        }
        Write-Host ('  {0,-11}: PGs {1} | detail {2} | skipped clean {3}' -f $EnvironmentSpec.Label,$EnvironmentProtectionGroups,$EnvironmentDetailCalls,$EnvironmentSkipped)
    }
}

$ActiveRows = @(Merge-RowsByObjectKey $ActiveRows | Sort-Object Cluster,ProtectionGroup,ObjectName)
$ClearedRows = @(Merge-RowsByObjectKey $ClearedRows | Sort-Object Cluster,ProtectionGroup,ObjectName)
$PreviousClearedRows = @($PreviousClearedRows | ForEach-Object {
    $ClearedRow = $_ | Select-Object *
    if ((Clean (Get-Prop $ClearedRow 'Status' '')) -eq 'Success') { Set-ObjProp $ClearedRow 'Change' 'PreviouslyCleared' }
    $ClearedRow
})
$AllClearedRows = @(Merge-RowsByObjectKey @($PreviousClearedRows + $ClearedRows))
$LifecycleRows = @(Merge-RowsByObjectKey @($ActiveRows + $AllClearedRows))

Write-CsvFile -Rows $ActiveRows -Path (Join-Path $OutputFolder 'current_failures.csv') -Columns $script:Columns
Write-CsvFile -Rows $ClearedRows -Path (Join-Path $OutputFolder 'cleared_by_success.csv') -Columns $script:Columns
Write-CsvFile -Rows $LifecycleRows -Path (Join-Path $OutputFolder 'incident_lifecycle.csv') -Columns $script:Columns
Write-CsvFile -Rows $NoObjectEvidenceRows -Path (Join-Path $OutputFolder 'no_object_evidence_review.csv') -Columns $script:ReviewColumns

$FailureTextRows = @($ActiveRows | Select-Object -Property $script:WorknoteColumns)
$SuccessTextRows = @($ClearedRows | Select-Object -Property $script:WorknoteColumns)
$WorknoteLines = New-Object System.Collections.Generic.List[string]
$WorknoteLines.Add('Cohesity Backup Failure Object-Level Fast Test')
$WorknoteLines.Add('')
$WorknoteLines.Add(('Incident: {0}' -f $IncidentNumber))
$WorknoteLines.Add(('Generated At: {0} ET' -f (Get-NowEtText)))
$WorknoteLines.Add(('Collection Status: {0}' -f $(if ($script:Warnings.Count -gt 0) { 'Incomplete' } else { 'Complete' })))
$WorknoteLines.Add(('Scope: latest {0} runs; RemoteAdapter excluded.' -f $NumRuns))
$WorknoteLines.Add(('API calls: light={0}, detail={1}, skippedCleanPGs={2}' -f $LightApiCalls,$DetailApiCalls,$SkippedCleanProtectionGroups))
$WorknoteLines.Add('')
$WorknoteLines.Add('Failure Section:')
$FailureTableText = ($FailureTextRows | Format-Table -AutoSize | Out-String).Trim()
if (!$FailureTableText) { $FailureTableText = 'No active object-level failures/running/cancelled states found.' }
$WorknoteLines.Add($FailureTableText)
$WorknoteLines.Add('')
$WorknoteLines.Add('Success Section:')
$SuccessTableText = ($SuccessTextRows | Format-Table -AutoSize | Out-String).Trim()
if (!$SuccessTableText) { $SuccessTableText = 'No newly cleared object-level failures found in this run.' }
$WorknoteLines.Add($SuccessTableText)
$WorknoteLines.Add('')
$WorknoteLines.Add('Warnings:')
if ($script:Warnings.Count -eq 0) { $WorknoteLines.Add('- None') } else { foreach ($WarningText in $script:Warnings) { $WorknoteLines.Add(('- {0}' -f $WarningText)) } }
($WorknoteLines -join [Environment]::NewLine) | Set-Content -Path (Join-Path $OutputFolder 'worknotes_summary.txt') -Encoding UTF8

$StateObject = [pscustomobject]@{
    IncidentNumber = $IncidentNumber
    GeneratedET = Get-NowEtText
    NumRuns = $NumRuns
    RequestTimeoutSec = $RequestTimeoutSec
    RemoteAdapterExcluded = $true
    LightApiCalls = $LightApiCalls
    DetailApiCalls = $DetailApiCalls
    SkippedCleanProtectionGroups = $SkippedCleanProtectionGroups
    ProtectionGroupsChecked = $CheckedProtectionGroups
    Warnings = @($script:Warnings)
    CurrentOpenFailures = @($ActiveRows)
    ClearedBySuccess = @($AllClearedRows)
    LastRunClearedBySuccess = @($ClearedRows)
    NoObjectEvidenceReview = @($NoObjectEvidenceRows)
}
Write-Json -ObjectValue $StateObject -Path $StatePath

Write-Host ''
Write-Host 'Fast object-level run completed.'
Write-Host ('ProtectionGroups checked : {0}' -f $CheckedProtectionGroups)
Write-Host ('Light API calls          : {0}' -f $LightApiCalls)
Write-Host ('Detail API calls         : {0}' -f $DetailApiCalls)
Write-Host ('Skipped clean PGs        : {0}' -f $SkippedCleanProtectionGroups)
Write-Host ('Active object rows       : {0}' -f $ActiveRows.Count)
Write-Host ('Cleared object rows      : {0}' -f $ClearedRows.Count)
Write-Host ('No-object review rows    : {0}' -f @($NoObjectEvidenceRows).Count)
Write-Host ('Output folder            : {0}' -f $OutputFolder)
