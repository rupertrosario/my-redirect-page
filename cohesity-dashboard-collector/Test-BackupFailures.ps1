# Cohesity backup failure validation
# STRICTLY READ-ONLY / GET-only against Helios
# Windows PowerShell 5.1 compatible
#
# Checks active protection groups for:
#   Hyper-V, Nutanix AHV, NAS, Physical, SQL and Oracle
#
# Rules:
# - Last 30 runs are checked independently by run type.
# - The newest state for an object/run type decides whether it is unresolved.
# - A newer success clears an older failure or cancellation.
# - SQL and Oracle database objects are reported separately from host-discovery failures.
# - One timestamped CSV is exported beside this script.

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$HelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

if (-not (Test-Path -LiteralPath $HelperPath -PathType Leaf)) {
    throw "API key helper not found: $HelperPath"
}
if (-not (Test-Path -LiteralPath $EncryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key not found: $EncryptedApiKeyPath"
}

. $HelperPath
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'AES API key helper returned an empty API key.'
}

$Workloads = @(
    [pscustomobject]@{
        Name='Hyper-V'; Environments=@('kHyperV'); ObjectType='kVirtualMachine'; Kind='HyperV'
    },
    [pscustomobject]@{
        Name='Nutanix AHV'; Environments=@('kAcropolis'); ObjectType='kVirtualMachine'; Kind='Nutanix'
    },
    [pscustomobject]@{
        Name='NAS'; Environments=@('kGenericNas','kIsilon'); ObjectType=''; Kind='NAS'
    },
    [pscustomobject]@{
        Name='Physical'; Environments=@('kPhysical'); ObjectType='kHost'; Kind='Physical'
    },
    [pscustomobject]@{
        Name='SQL'; Environments=@('kSQL'); ObjectType='kDatabase'; Kind='SQL'
    },
    [pscustomobject]@{
        Name='Oracle'; Environments=@('kOracle'); ObjectType='kDatabase'; Kind='Oracle'
    }
)

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Get-Val {
    param(
        $Object,
        [string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object -or $Object -is [string]) { return $Default }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                if ($null -ne $property.Value) { return $property.Value }
                return $Default
            }
        }
    }

    return $Default
}

function First-Text {
    param($Values)

    foreach ($value in @($Values)) {
        foreach ($item in @($value)) {
            if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) {
                return ([string]$item).Trim()
            }
        }
    }

    return ''
}

function New-Headers {
    param([string]$ClusterId)

    $headers = @{
        accept = 'application/json'
        apiKey = $ApiKey
    }

    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers.accessClusterId = $ClusterId
    }

    return $headers
}

function Get-CohesityJson {
    param(
        [string]$Uri,
        [hashtable]$Headers
    )

    $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get `
        -UseBasicParsing -TimeoutSec 120 -ErrorAction Stop

    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function Convert-UsecsToUtc {
    param($Usecs)

    try {
        $value = [int64]$Usecs
        if ($value -le 0) { return '' }
        return [DateTimeOffset]::FromUnixTimeMilliseconds(
            [int64]($value / 1000)
        ).UtcDateTime.ToString('o')
    }
    catch {
        return ''
    }
}

function Normalize-RunType {
    param([string]$RunType)

    if ($RunType -match '(?i)log') { return 'Log' }
    if ($RunType -match '(?i)full') { return 'Full' }
    if ($RunType -match '(?i)increment|regular') { return 'Incremental' }
    if ([string]::IsNullOrWhiteSpace($RunType)) { return 'Unknown' }
    return ($RunType -replace '^k','')
}

function Clean-Message {
    param($Value)

    $messages = @()
    foreach ($item in @(As-Array $Value)) {
        if ($null -eq $item) { continue }
        $text = ([string]$item -replace '[\r\n]+',' ' -replace '\s+',' ').Trim()
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            $messages += $text
        }
    }

    return (@($messages | Select-Object -Unique) -join ' | ')
}

function Get-ProtectionGroups {
    param(
        [string]$Environment,
        [hashtable]$Headers
    )

    $groups = @()
    $cookie = ''
    $seenCookies = @{}

    do {
        $uri = "$BaseUrl/v2/data-protect/protection-groups?environments=$([uri]::EscapeDataString($Environment))&isDeleted=false&isPaused=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"

        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-CohesityJson -Uri $uri -Headers $Headers

        foreach ($group in @(As-Array (Get-Val $json @('protectionGroups','items','data')))) {
            if ($null -ne $group) { $groups += $group }
        }

        $cookie = First-Text @((Get-Val $json @('paginationCookie')))
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            if ($seenCookies.ContainsKey($cookie)) {
                throw "Repeated protection-group pagination cookie for $Environment."
            }
            $seenCookies[$cookie] = $true
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($groups)
}

function Get-PgId {
    param($ProtectionGroup)
    return First-Text @(
        (Get-Val $ProtectionGroup @('id','protectionGroupId')),
        (Get-Val $ProtectionGroup @('name','protectionGroupName'))
    )
}

function Get-PgName {
    param($ProtectionGroup)
    return First-Text @(
        (Get-Val $ProtectionGroup @('name','protectionGroupName')),
        (Get-PgId $ProtectionGroup)
    )
}

function Get-RunRecords {
    param(
        [string]$ProtectionGroupId,
        [hashtable]$Headers
    )

    $uri = "$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json = Get-CohesityJson -Uri $uri -Headers $Headers
    $records = @()

    foreach ($run in @(As-Array (Get-Val $json @('runs','items','data')))) {
        $localInfos = @(As-Array (Get-Val $run @('localBackupInfo','localSnapshotInfo')))

        foreach ($info in $localInfos) {
            $records += [pscustomobject]@{
                Run = $run
                RunType = Normalize-RunType (First-Text @((Get-Val $info @('runType'))))
                Status = First-Text @((Get-Val $info @('status')))
                StartTimeUsecs = [int64](Get-Val $info @('startTimeUsecs') 0)
                EndTimeUsecs = [int64](Get-Val $info @('endTimeUsecs','startTimeUsecs') 0)
                Messages = Get-Val $info @('messages','message')
            }
        }

        if ($localInfos.Count -eq 0) {
            $records += [pscustomobject]@{
                Run = $run
                RunType = Normalize-RunType (First-Text @((Get-Val $run @('runType'))))
                Status = First-Text @((Get-Val $run @('status')))
                StartTimeUsecs = [int64](Get-Val $run @('startTimeUsecs') 0)
                EndTimeUsecs = [int64](Get-Val $run @('endTimeUsecs','startTimeUsecs') 0)
                Messages = Get-Val $run @('messages','message')
            }
        }
    }

    return @($records | Sort-Object EndTimeUsecs -Descending)
}

function Get-ObjectCore {
    param($RunObject)
    if ($null -eq $RunObject) { return $null }
    return (Get-Val $RunObject @('object') $RunObject)
}

function Get-ObjectName {
    param($RunObject)
    $object = Get-ObjectCore $RunObject
    return First-Text @(
        (Get-Val $object @(
            'databaseUniqueName','databaseName','dbName','name',
            'objectName','displayName','hostName','sourceName','vmName'
        ))
    )
}

function Get-ObjectId {
    param($RunObject)
    $object = Get-ObjectCore $RunObject
    return First-Text @(
        (Get-Val $object @(
            'id','objectId','databaseId','databaseUuid',
            'entityId','uuid','globalId','vmId'
        ))
    )
}

function Get-SourceId {
    param($RunObject)
    $object = Get-ObjectCore $RunObject
    return First-Text @((Get-Val $object @('sourceId','parentId','rootNodeId')))
}

function Get-ObjectType {
    param($RunObject)
    $object = Get-ObjectCore $RunObject
    return First-Text @((Get-Val $object @('objectType','type','entityType')))
}

function Get-ObjectEnvironment {
    param($RunObject)
    $object = Get-ObjectCore $RunObject
    return First-Text @((Get-Val $object @('environment','environmentType')))
}

function Get-ObjectKey {
    param($RunObject)

    $name = Get-ObjectName $RunObject
    $id = Get-ObjectId $RunObject
    $sourceId = Get-SourceId $RunObject
    $type = Get-ObjectType $RunObject
    $environment = Get-ObjectEnvironment $RunObject

    if ([string]::IsNullOrWhiteSpace($name) -and [string]::IsNullOrWhiteSpace($id)) {
        return ''
    }

    return ('{0}|{1}|{2}|{3}|{4}' -f `
        $environment,$type,$sourceId,(First-Text @($id,$name)),$name
    ).ToLowerInvariant()
}

function Get-FailedAttempts {
    param($RunObject)

    $attempts = @()
    foreach ($localInfo in @(As-Array (Get-Val $RunObject @('localSnapshotInfo','localBackupInfo')))) {
        $attempts += @(As-Array (Get-Val $localInfo @('failedAttempts')))
        foreach ($snapshotInfo in @(As-Array (Get-Val $localInfo @('snapshotInfo')))) {
            $attempts += @(As-Array (Get-Val $snapshotInfo @('failedAttempts')))
        }
    }

    foreach ($snapshotInfo in @(As-Array (Get-Val $RunObject @('snapshotInfo')))) {
        $attempts += @(As-Array (Get-Val $snapshotInfo @('failedAttempts')))
    }

    return @($attempts | Where-Object { $null -ne $_ })
}

function Get-ObjectStatusValues {
    param($RunObject)

    $statuses = @()
    $statuses += First-Text @((Get-Val $RunObject @('status')))

    $object = Get-ObjectCore $RunObject
    $statuses += First-Text @((Get-Val $object @('status')))

    foreach ($localInfo in @(As-Array (Get-Val $RunObject @('localSnapshotInfo','localBackupInfo')))) {
        $statuses += First-Text @((Get-Val $localInfo @('status')))
        foreach ($snapshotInfo in @(As-Array (Get-Val $localInfo @('snapshotInfo')))) {
            $statuses += First-Text @((Get-Val $snapshotInfo @('status')))
        }
    }

    return @($statuses | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Resolve-ObjectState {
    param(
        $RunObject,
        [string]$RunStatus,
        [bool]$AllowRunFallback
    )

    $attempts = @(Get-FailedAttempts $RunObject)
    $statuses = @(Get-ObjectStatusValues $RunObject)

    if ($attempts.Count -eq 0 -and @($statuses | Where-Object {
        $_ -match '^(k)?(Success|Succeeded|Successful|Completed|SucceededWithWarning)$'
    }).Count -gt 0) {
        return 'Success'
    }

    if ($attempts.Count -gt 0 -or @($statuses | Where-Object {
        $_ -match '^(k)?(Failed|Failure|Error)$'
    }).Count -gt 0) {
        return 'Failed'
    }

    if (@($statuses | Where-Object {
        $_ -match '^(k)?(Canceled|Cancelled|Canceling)$'
    }).Count -gt 0) {
        return 'Cancelled'
    }

    if ($AllowRunFallback) {
        if ($RunStatus -match '^(k)?(Failed|Failure|Error)$') { return 'Failed' }
        if ($RunStatus -match '^(k)?(Canceled|Cancelled|Canceling)$') { return 'Cancelled' }
        if ($RunStatus -match '^(k)?(Success|Succeeded|Successful|Completed|SucceededWithWarning)$') {
            return 'Success'
        }
    }

    return 'Success'
}

function Get-FailureMessage {
    param(
        $RunObject,
        $RunRecord
    )

    $messages = @()
    foreach ($attempt in @(Get-FailedAttempts $RunObject)) {
        $messages += Get-Val $attempt @(
            'message','error','reason','errorMessage','failureMessage'
        )
    }

    foreach ($container in @(
        $RunObject,
        (Get-ObjectCore $RunObject)
    )) {
        $messages += Get-Val $container @(
            'message','messages','error','reason','errorMessage','failureMessage','lastError'
        )
    }

    $messages += $RunRecord.Messages
    $text = Clean-Message $messages

    if ([string]::IsNullOrWhiteSpace($text)) {
        return "No detailed message returned for $($RunRecord.Status) state."
    }

    return $text
}

function Test-TargetObject {
    param(
        $RunObject,
        $Workload
    )

    $type = Get-ObjectType $RunObject
    $environment = Get-ObjectEnvironment $RunObject

    if ($Workload.Kind -eq 'NAS') {
        return ($environment -in $Workload.Environments)
    }

    if ($type -ine $Workload.ObjectType) { return $false }
    if (-not [string]::IsNullOrWhiteSpace($environment) -and
        $environment -notin $Workload.Environments) {
        return $false
    }

    return $true
}

function Add-CurrentState {
    param(
        [hashtable]$Seen,
        [hashtable]$Unresolved,
        [string]$StateKey,
        [string]$State,
        $Row,
        [string]$ResultTimeUtc
    )

    if (-not $Seen.ContainsKey($StateKey)) {
        $Seen[$StateKey] = $State
        if ($State -in @('Failed','Cancelled')) {
            $Unresolved[$StateKey] = $Row
        }
        return
    }

    if ($Unresolved.ContainsKey($StateKey) -and $State -eq 'Success') {
        $existing = $Unresolved[$StateKey]
        if ([string]::IsNullOrWhiteSpace([string]$existing.LatestSuccessUtc)) {
            $existing.LatestSuccessUtc = $ResultTimeUtc
        }
    }
}

$clusterJson = Get-CohesityJson `
    -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" `
    -Headers (New-Headers)

$clusters = @()
foreach ($cluster in @(As-Array (Get-Val $clusterJson @(
    'cohesityClusters','clusters','clusterInfos','items','data'
)))) {
    $clusterId = First-Text @((Get-Val $cluster @('clusterId','id')))
    if ([string]::IsNullOrWhiteSpace($clusterId)) { continue }

    $clusters += [pscustomobject]@{
        ClusterId = $clusterId
        ClusterName = First-Text @(
            (Get-Val $cluster @('clusterName','name','displayName')),
            "Unknown-$clusterId"
        )
    }
}

$clusters = @($clusters | Sort-Object ClusterName)
if ($clusters.Count -eq 0) { throw 'No clusters returned from Helios.' }

$menu = @()
for ($index = 0; $index -lt $clusters.Count; $index++) {
    $menu += [pscustomobject]@{
        Index = $index + 1
        ClusterName = $clusters[$index].ClusterName
        ClusterId = $clusters[$index].ClusterId
    }
}

Write-Host ''
Write-Host 'Available Helios clusters:' -ForegroundColor Cyan
$menu | Format-Table Index,ClusterName -AutoSize
Write-Host '[0] All clusters' -ForegroundColor Yellow
Write-Host '[X] Exit' -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host 'Select cluster'
    if ($selection -match '^(?i:x|q)$') { return }

    $number = -1
    if ([int]::TryParse($selection,[ref]$number) -and
        $number -ge 0 -and $number -le $menu.Count) {
        if ($number -eq 0) {
            $selectedClusters = @($menu)
        }
        else {
            $selectedClusters = @($menu | Where-Object { $_.Index -eq $number })
        }
        break
    }

    Write-Host "Enter 0, 1-$($menu.Count), or X." -ForegroundColor Red
}

$allResults = @()
$warnings = @()

foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId

    foreach ($workload in $Workloads) {
        $groups = @()

        foreach ($environment in $workload.Environments) {
            try {
                $groups += @(Get-ProtectionGroups -Environment $environment -Headers $headers)
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Protection-group GET'
                    Warning = "$environment`: $($_.Exception.Message)"
                }
            }
        }

        $seenGroups = @{}
        foreach ($pg in $groups) {
            $pgId = Get-PgId $pg
            if ([string]::IsNullOrWhiteSpace($pgId)) { continue }

            $pgKey = $pgId.ToLowerInvariant()
            if ($seenGroups.ContainsKey($pgKey)) { continue }
            $seenGroups[$pgKey] = $true

            $pgName = Get-PgName $pg

            try {
                $runRecords = @(Get-RunRecords -ProtectionGroupId $pgId -Headers $headers)
            }
            catch {
                $warnings += [pscustomobject]@{
                    Cluster = $cluster.ClusterName
                    Workload = $workload.Name
                    Operation = 'Run details GET'
                    Warning = "$pgName`: $($_.Exception.Message)"
                }
                continue
            }

            foreach ($runTypeGroup in @($runRecords | Group-Object RunType)) {
                $records = @($runTypeGroup.Group | Sort-Object EndTimeUsecs -Descending)
                $seen = @{}
                $unresolved = @{}
                $runFallbackSeen = $false
                $hostNamesById = @{}

                foreach ($record in $records) {
                    foreach ($runObject in @(As-Array (Get-Val $record.Run @('objects','objectDetails')))) {
                        $objectId = Get-ObjectId $runObject
                        $objectName = Get-ObjectName $runObject
                        if (-not [string]::IsNullOrWhiteSpace($objectId) -and
                            -not [string]::IsNullOrWhiteSpace($objectName)) {
                            $hostNamesById[$objectId] = $objectName
                        }
                    }
                }

                foreach ($record in $records) {
                    $resultTimeUtc = Convert-UsecsToUtc $record.EndTimeUsecs
                    $runObjects = @(As-Array (Get-Val $record.Run @('objects','objectDetails')))
                    $targetObjects = @($runObjects | Where-Object {
                        Test-TargetObject -RunObject $_ -Workload $workload
                    })

                    foreach ($runObject in $targetObjects) {
                        $objectKey = Get-ObjectKey $runObject
                        if ([string]::IsNullOrWhiteSpace($objectKey)) { continue }

                        $stateKey = "Object|$objectKey"
                        $allowRunFallback = ($workload.Kind -eq 'Physical')
                        $state = Resolve-ObjectState -RunObject $runObject `
                            -RunStatus $record.Status -AllowRunFallback:$allowRunFallback

                        $sourceId = Get-SourceId $runObject
                        $hostName = ''
                        if (($workload.Kind -eq 'SQL' -or $workload.Kind -eq 'Oracle') -and
                            -not [string]::IsNullOrWhiteSpace($sourceId) -and
                            $hostNamesById.ContainsKey($sourceId)) {
                            $hostName = $hostNamesById[$sourceId]
                        }

                        $row = [pscustomobject][ordered]@{
                            Cluster = $cluster.ClusterName
                            Workload = $workload.Name
                            Scope = 'Object'
                            ProtectionGroup = $pgName
                            Host = $hostName
                            ObjectName = Get-ObjectName $runObject
                            ObjectType = Get-ObjectType $runObject
                            Environment = Get-ObjectEnvironment $runObject
                            RunType = $record.RunType
                            Status = $state
                            FailureTimeUtc = $resultTimeUtc
                            LatestSuccessUtc = ''
                            Message = Get-FailureMessage -RunObject $runObject -RunRecord $record
                        }

                        Add-CurrentState -Seen $seen -Unresolved $unresolved `
                            -StateKey $stateKey -State $state -Row $row `
                            -ResultTimeUtc $resultTimeUtc
                    }

                    if ($workload.Kind -eq 'SQL' -or $workload.Kind -eq 'Oracle') {
                        $hostObjects = @($runObjects | Where-Object {
                            (Get-ObjectType $_) -ieq 'kHost' -or
                            (Get-ObjectEnvironment $_) -ieq 'kPhysical'
                        })

                        foreach ($runObject in $hostObjects) {
                            $objectKey = Get-ObjectKey $runObject
                            if ([string]::IsNullOrWhiteSpace($objectKey)) { continue }

                            $stateKey = "HostDiscovery|$objectKey"
                            $state = Resolve-ObjectState -RunObject $runObject `
                                -RunStatus $record.Status -AllowRunFallback:$false

                            $row = [pscustomobject][ordered]@{
                                Cluster = $cluster.ClusterName
                                Workload = $workload.Name
                                Scope = 'Host discovery'
                                ProtectionGroup = $pgName
                                Host = Get-ObjectName $runObject
                                ObjectName = 'No database object returned'
                                ObjectType = Get-ObjectType $runObject
                                Environment = Get-ObjectEnvironment $runObject
                                RunType = $record.RunType
                                Status = $state
                                FailureTimeUtc = $resultTimeUtc
                                LatestSuccessUtc = ''
                                Message = Get-FailureMessage -RunObject $runObject -RunRecord $record
                            }

                            Add-CurrentState -Seen $seen -Unresolved $unresolved `
                                -StateKey $stateKey -State $state -Row $row `
                                -ResultTimeUtc $resultTimeUtc
                        }
                    }

                    if ($workload.Kind -eq 'NAS' -and $targetObjects.Count -eq 0 -and
                        -not $runFallbackSeen) {

                        $runFallbackSeen = $true
                        $runState = 'Success'
                        if ($record.Status -match '^(k)?(Failed|Failure|Error)$') {
                            $runState = 'Failed'
                        }
                        elseif ($record.Status -match '^(k)?(Canceled|Cancelled|Canceling)$') {
                            $runState = 'Cancelled'
                        }

                        if ($runState -in @('Failed','Cancelled')) {
                            $fallbackKey = "PgFallback|$($record.RunType)"
                            $message = Clean-Message $record.Messages
                            if ([string]::IsNullOrWhiteSpace($message)) {
                                $message = 'Run failed or was cancelled, but Cohesity returned no object-level details.'
                            }

                            $unresolved[$fallbackKey] = [pscustomobject][ordered]@{
                                Cluster = $cluster.ClusterName
                                Workload = $workload.Name
                                Scope = 'PG fallback'
                                ProtectionGroup = $pgName
                                Host = ''
                                ObjectName = $pgName
                                ObjectType = ''
                                Environment = First-Text @($workload.Environments)
                                RunType = $record.RunType
                                Status = $runState
                                FailureTimeUtc = $resultTimeUtc
                                LatestSuccessUtc = ''
                                Message = $message
                            }
                        }
                    }
                }

                $allResults += @($unresolved.Values)
            }
        }
    }
}

$script:FailureResults = @(
    $allResults |
        Group-Object {
            "$($_.Cluster)|$($_.Workload)|$($_.Scope)|$($_.ProtectionGroup)|$($_.Host)|$($_.ObjectName)|$($_.RunType)"
        } |
        ForEach-Object {
            $_.Group | Sort-Object FailureTimeUtc -Descending | Select-Object -First 1
        } |
        Sort-Object Cluster,Workload,ProtectionGroup,ObjectName,RunType
)

$script:FailureWarnings = @(
    $warnings | Sort-Object Cluster,Workload,Operation
)

$summary = foreach ($workload in $Workloads) {
    $workloadRows = @($script:FailureResults | Where-Object {
        $_.Workload -eq $workload.Name
    })

    [pscustomobject][ordered]@{
        Workload = $workload.Name
        Failed = @($workloadRows | Where-Object { $_.Status -eq 'Failed' }).Count
        Cancelled = @($workloadRows | Where-Object { $_.Status -eq 'Cancelled' }).Count
        Total = $workloadRows.Count
    }
}

Write-Host ''
Write-Host 'UNRESOLVED BACKUP FAILURE SUMMARY' -ForegroundColor Cyan
$summary | Format-Table Workload,Failed,Cancelled,Total -AutoSize

$outputDirectory = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($outputDirectory)) {
    $outputDirectory = (Get-Location).Path
}

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$script:FailureCsvPath = Join-Path $outputDirectory `
    "Cohesity_BackupFailures_$timestamp.csv"

if ($script:FailureResults.Count -gt 0) {
    $script:FailureResults |
        Export-Csv -Path $script:FailureCsvPath -NoTypeInformation -Encoding UTF8

    Write-Host ''
    Write-Host 'Failure verification CSV:' -ForegroundColor Cyan
    Write-Host $script:FailureCsvPath
    Write-Host "CSV rows: $($script:FailureResults.Count)"

    Write-Host ''
    $script:FailureResults |
        Format-Table Cluster,Workload,Scope,ProtectionGroup,Host,ObjectName,RunType,Status,FailureTimeUtc -Wrap -AutoSize
}
else {
    $script:FailureCsvPath = $null
    Write-Host ''
    Write-Host 'No unresolved failures or cancellations were found.' -ForegroundColor Green
}

if ($script:FailureWarnings.Count -gt 0) {
    Write-Host ''
    Write-Host 'COLLECTION WARNINGS' -ForegroundColor Yellow
    $script:FailureWarnings |
        Format-Table Cluster,Workload,Operation,Warning -Wrap -AutoSize
}
