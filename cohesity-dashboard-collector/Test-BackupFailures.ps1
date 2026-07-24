# Cohesity backup result validation
# STRICTLY READ-ONLY / GET-only against Helios
# Windows PowerShell 5.1 compatible
#
# Reports the latest state found in the last 30 runs for every object/run type:
# Success, Failed, or Cancelled. SQL/Oracle host-discovery exceptions and
# objectless NAS failures are kept separate from object-result counts.

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$BaseUrl = 'https://helios.cohesity.com'
$HelperPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1'
$EncryptedApiKeyPath = 'X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc'

if (-not (Test-Path -LiteralPath $HelperPath -PathType Leaf)) { throw "API key helper not found: $HelperPath" }
if (-not (Test-Path -LiteralPath $EncryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key not found: $EncryptedApiKeyPath" }

. $HelperPath
$ApiKey = Get-CohesityApiKeyFromAes -EncryptedFile $EncryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($ApiKey)) { throw 'AES API key helper returned an empty API key.' }

$Workloads = @(
    [pscustomobject]@{Name='Hyper-V';     Environments=@('kHyperV');               ObjectType='kVirtualMachine'; Kind='HyperV'},
    [pscustomobject]@{Name='Nutanix AHV'; Environments=@('kAcropolis');            ObjectType='kVirtualMachine'; Kind='Nutanix'},
    [pscustomobject]@{Name='NAS';         Environments=@('kGenericNas','kIsilon'); ObjectType='';                Kind='NAS'},
    [pscustomobject]@{Name='Physical';    Environments=@('kPhysical');            ObjectType='kHost';           Kind='Physical'},
    [pscustomobject]@{Name='SQL';         Environments=@('kSQL');                 ObjectType='kDatabase';       Kind='SQL'},
    [pscustomobject]@{Name='Oracle';      Environments=@('kOracle');              ObjectType='kDatabase';       Kind='Oracle'}
)

function As-Array($Value) { if ($null -eq $Value) { return @() }; return @($Value) }

function Get-Val {
    param($Object,[string[]]$Names,$Default=$null)
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

function First-Text($Values) {
    foreach ($value in @($Values)) {
        foreach ($item in @($value)) {
            if ($null -ne $item -and -not [string]::IsNullOrWhiteSpace([string]$item)) { return ([string]$item).Trim() }
        }
    }
    return ''
}

function New-Headers([string]$ClusterId) {
    $headers = @{accept='application/json';apiKey=$ApiKey}
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) { $headers.accessClusterId=$ClusterId }
    return $headers
}

function Get-CohesityJson([string]$Uri,[hashtable]$Headers) {
    $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -TimeoutSec 120 -ErrorAction Stop
    if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function Convert-UsecsToUtc($Usecs) {
    try {
        $value=[int64]$Usecs
        if ($value -le 0) { return '' }
        return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]($value/1000)).UtcDateTime.ToString('o')
    } catch { return '' }
}

function Normalize-RunType([string]$RunType) {
    if ($RunType -match '(?i)log') { return 'Log' }
    if ($RunType -match '(?i)full') { return 'Full' }
    if ($RunType -match '(?i)increment|regular') { return 'Incremental' }
    if ([string]::IsNullOrWhiteSpace($RunType)) { return 'Unknown' }
    return ($RunType -replace '^k','')
}

function Clean-Message($Value) {
    $messages=@()
    foreach ($item in @(As-Array $Value)) {
        if ($null -eq $item) { continue }
        $text=([string]$item -replace '[\r\n]+',' ' -replace '\s+',' ').Trim()
        if ($text) { $messages += $text }
    }
    return (@($messages | Select-Object -Unique) -join ' | ')
}

function Get-ProtectionGroups([string]$Environment,[hashtable]$Headers) {
    $groups=@();$cookie='';$seenCookies=@{}
    do {
        $uri="$BaseUrl/v2/data-protect/protection-groups?environments=$([uri]::EscapeDataString($Environment))&isDeleted=false&isPaused=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
        if ($cookie) { $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))" }
        $json=Get-CohesityJson -Uri $uri -Headers $Headers
        foreach ($group in @(As-Array (Get-Val $json @('protectionGroups','items','data')))) { if ($null -ne $group) { $groups += $group } }
        $cookie=First-Text @((Get-Val $json @('paginationCookie')))
        if ($cookie) {
            if ($seenCookies.ContainsKey($cookie)) { throw "Repeated protection-group pagination cookie for $Environment." }
            $seenCookies[$cookie]=$true
        }
    } while ($cookie)
    return @($groups)
}

function Get-PgId($ProtectionGroup) { return First-Text @((Get-Val $ProtectionGroup @('id','protectionGroupId')),(Get-Val $ProtectionGroup @('name','protectionGroupName'))) }
function Get-PgName($ProtectionGroup) { return First-Text @((Get-Val $ProtectionGroup @('name','protectionGroupName')),(Get-PgId $ProtectionGroup)) }

function Get-RunRecords([string]$ProtectionGroupId,[hashtable]$Headers) {
    $uri="$BaseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($ProtectionGroupId))/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true"
    $json=Get-CohesityJson -Uri $uri -Headers $Headers
    $records=@()
    foreach ($run in @(As-Array (Get-Val $json @('runs','items','data')))) {
        $infos=@(As-Array (Get-Val $run @('localBackupInfo','localSnapshotInfo')))
        foreach ($info in $infos) {
            $records += [pscustomobject]@{
                Run=$run
                RunType=Normalize-RunType (First-Text @((Get-Val $info @('runType'))))
                Status=First-Text @((Get-Val $info @('status')))
                EndTimeUsecs=[int64](Get-Val $info @('endTimeUsecs','startTimeUsecs') 0)
                Messages=Get-Val $info @('messages','message')
            }
        }
        if ($infos.Count -eq 0) {
            $records += [pscustomobject]@{
                Run=$run
                RunType=Normalize-RunType (First-Text @((Get-Val $run @('runType'))))
                Status=First-Text @((Get-Val $run @('status')))
                EndTimeUsecs=[int64](Get-Val $run @('endTimeUsecs','startTimeUsecs') 0)
                Messages=Get-Val $run @('messages','message')
            }
        }
    }
    return @($records | Sort-Object EndTimeUsecs -Descending)
}

function Get-ObjectCore($RunObject) { if ($null -eq $RunObject) { return $null }; return (Get-Val $RunObject @('object') $RunObject) }
function Get-ObjectName($RunObject) { $o=Get-ObjectCore $RunObject; return First-Text @((Get-Val $o @('databaseUniqueName','databaseName','dbName','name','objectName','displayName','hostName','sourceName','vmName'))) }
function Get-ObjectId($RunObject) { $o=Get-ObjectCore $RunObject; return First-Text @((Get-Val $o @('id','objectId','databaseId','databaseUuid','entityId','uuid','globalId','vmId'))) }
function Get-SourceId($RunObject) { $o=Get-ObjectCore $RunObject; return First-Text @((Get-Val $o @('sourceId','parentId','rootNodeId'))) }
function Get-ObjectType($RunObject) { $o=Get-ObjectCore $RunObject; return First-Text @((Get-Val $o @('objectType','type','entityType'))) }
function Get-ObjectEnvironment($RunObject) { $o=Get-ObjectCore $RunObject; return First-Text @((Get-Val $o @('environment','environmentType'))) }

function Get-ObjectKey($RunObject) {
    $name=Get-ObjectName $RunObject;$id=Get-ObjectId $RunObject;$sourceId=Get-SourceId $RunObject
    $type=Get-ObjectType $RunObject;$environment=Get-ObjectEnvironment $RunObject
    if (-not $name -and -not $id) { return '' }
    return ('{0}|{1}|{2}|{3}|{4}' -f $environment,$type,$sourceId,(First-Text @($id,$name)),$name).ToLowerInvariant()
}

function Get-FailedAttempts($RunObject) {
    $attempts=@()
    foreach ($localInfo in @(As-Array (Get-Val $RunObject @('localSnapshotInfo','localBackupInfo')))) {
        $attempts += @(As-Array (Get-Val $localInfo @('failedAttempts')))
        foreach ($snapshotInfo in @(As-Array (Get-Val $localInfo @('snapshotInfo')))) { $attempts += @(As-Array (Get-Val $snapshotInfo @('failedAttempts'))) }
    }
    foreach ($snapshotInfo in @(As-Array (Get-Val $RunObject @('snapshotInfo')))) { $attempts += @(As-Array (Get-Val $snapshotInfo @('failedAttempts'))) }
    return @($attempts | Where-Object { $null -ne $_ })
}

function Get-ObjectStatuses($RunObject) {
    $statuses=@();$statuses += First-Text @((Get-Val $RunObject @('status')))
    $object=Get-ObjectCore $RunObject;$statuses += First-Text @((Get-Val $object @('status')))
    foreach ($localInfo in @(As-Array (Get-Val $RunObject @('localSnapshotInfo','localBackupInfo')))) {
        $statuses += First-Text @((Get-Val $localInfo @('status')))
        foreach ($snapshotInfo in @(As-Array (Get-Val $localInfo @('snapshotInfo')))) { $statuses += First-Text @((Get-Val $snapshotInfo @('status'))) }
    }
    foreach ($snapshotInfo in @(As-Array (Get-Val $RunObject @('snapshotInfo')))) { $statuses += First-Text @((Get-Val $snapshotInfo @('status'))) }
    return @($statuses | Where-Object { $_ })
}

function Resolve-ObjectState($RunObject,[string]$RunStatus,[bool]$AllowRunFallback) {
    $statuses=@(Get-ObjectStatuses $RunObject)
    if (@($statuses | Where-Object { $_ -match '^(k)?(Success|Succeeded|Successful|Completed|SucceededWithWarning)$' }).Count -gt 0) { return 'Success' }
    if (@($statuses | Where-Object { $_ -match '^(k)?(Failed|Failure|Error)$' }).Count -gt 0) { return 'Failed' }
    if (@($statuses | Where-Object { $_ -match '^(k)?(Canceled|Cancelled|Canceling)$' }).Count -gt 0) { return 'Cancelled' }
    if (@(Get-FailedAttempts $RunObject).Count -gt 0) { return 'Failed' }
    if ($AllowRunFallback) {
        if ($RunStatus -match '^(k)?(Failed|Failure|Error)$') { return 'Failed' }
        if ($RunStatus -match '^(k)?(Canceled|Cancelled|Canceling)$') { return 'Cancelled' }
    }
    return 'Success'
}

function Get-ResultMessage($RunObject,$RunRecord,[string]$State) {
    if ($State -eq 'Success') { return '' }
    $messages=@()
    foreach ($attempt in @(Get-FailedAttempts $RunObject)) { $messages += Get-Val $attempt @('message','error','reason','errorMessage','failureMessage') }
    foreach ($container in @($RunObject,(Get-ObjectCore $RunObject))) { $messages += Get-Val $container @('message','messages','error','reason','errorMessage','failureMessage','lastError') }
    $messages += $RunRecord.Messages
    $text=Clean-Message $messages
    if (-not $text) { return "No detailed message returned for $State state." }
    return $text
}

function Test-TargetObject($RunObject,$Workload) {
    $type=Get-ObjectType $RunObject;$environment=Get-ObjectEnvironment $RunObject
    if ($Workload.Kind -eq 'NAS') { return ($environment -in $Workload.Environments) }
    if ($type -ine $Workload.ObjectType) { return $false }
    if ($environment -and $environment -notin $Workload.Environments) { return $false }
    return $true
}

function Add-LatestState([hashtable]$Current,[string]$Key,[string]$State,$Row,[string]$TimeUtc) {
    if (-not $Current.ContainsKey($Key)) {
        if ($State -eq 'Success') { $Row.LatestSuccessUtc=$TimeUtc }
        $Current[$Key]=$Row
        return
    }
    $existing=$Current[$Key]
    if ($existing.Status -in @('Failed','Cancelled') -and $State -eq 'Success' -and -not $existing.LatestSuccessUtc) { $existing.LatestSuccessUtc=$TimeUtc }
}

$clusterJson=Get-CohesityJson -Uri "$BaseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$clusters=@()
foreach ($cluster in @(As-Array (Get-Val $clusterJson @('cohesityClusters','clusters','clusterInfos','items','data')))) {
    $clusterId=First-Text @((Get-Val $cluster @('clusterId','id')))
    if (-not $clusterId) { continue }
    $clusters += [pscustomobject]@{ClusterId=$clusterId;ClusterName=First-Text @((Get-Val $cluster @('clusterName','name','displayName')),"Unknown-$clusterId")}
}
$clusters=@($clusters | Sort-Object ClusterName)
if ($clusters.Count -eq 0) { throw 'No clusters returned from Helios.' }

$menu=@()
for ($i=0;$i -lt $clusters.Count;$i++) { $menu += [pscustomobject]@{Index=$i+1;ClusterName=$clusters[$i].ClusterName;ClusterId=$clusters[$i].ClusterId} }
Write-Host '';Write-Host 'Available Helios clusters:' -ForegroundColor Cyan
$menu | Format-Table Index,ClusterName -AutoSize
Write-Host '[0] All clusters' -ForegroundColor Yellow;Write-Host '[X] Exit' -ForegroundColor Yellow
while ($true) {
    $selection=Read-Host 'Select cluster'
    if ($selection -match '^(?i:x|q)$') { return }
    $number=-1
    if ([int]::TryParse($selection,[ref]$number) -and $number -ge 0 -and $number -le $menu.Count) {
        if ($number -eq 0) { $selectedClusters=@($menu) } else { $selectedClusters=@($menu | Where-Object {$_.Index -eq $number}) }
        break
    }
    Write-Host "Enter 0, 1-$($menu.Count), or X." -ForegroundColor Red
}

$objectResults=@();$exceptions=@();$warnings=@()
foreach ($cluster in $selectedClusters) {
    $headers=New-Headers -ClusterId $cluster.ClusterId
    foreach ($workload in $Workloads) {
        $groups=@()
        foreach ($environment in $workload.Environments) {
            try { $groups += @(Get-ProtectionGroups -Environment $environment -Headers $headers) }
            catch { $warnings += [pscustomobject]@{Cluster=$cluster.ClusterName;Workload=$workload.Name;Operation='Protection-group GET';Warning="$environment`: $($_.Exception.Message)"} }
        }
        $seenGroups=@{}
        foreach ($pg in $groups) {
            $pgId=Get-PgId $pg;if (-not $pgId) { continue }
            if ($seenGroups.ContainsKey($pgId.ToLowerInvariant())) { continue };$seenGroups[$pgId.ToLowerInvariant()]=$true
            $pgName=Get-PgName $pg
            try { $runRecords=@(Get-RunRecords -ProtectionGroupId $pgId -Headers $headers) }
            catch { $warnings += [pscustomobject]@{Cluster=$cluster.ClusterName;Workload=$workload.Name;Operation='Run details GET';Warning="$pgName`: $($_.Exception.Message)"};continue }

            foreach ($runTypeGroup in @($runRecords | Group-Object RunType)) {
                $records=@($runTypeGroup.Group | Sort-Object EndTimeUsecs -Descending)
                $currentObjects=@{};$currentHosts=@{};$nasHandled=$false;$hostNames=@{}
                foreach ($record in $records) {
                    foreach ($ro in @(As-Array (Get-Val $record.Run @('objects','objectDetails')))) {
                        $oid=Get-ObjectId $ro;$oname=Get-ObjectName $ro
                        if (((Get-ObjectType $ro) -ieq 'kHost' -or (Get-ObjectEnvironment $ro) -ieq 'kPhysical') -and $oid -and $oname) { $hostNames[$oid]=$oname }
                    }
                }
                foreach ($record in $records) {
                    $time=Convert-UsecsToUtc $record.EndTimeUsecs
                    $runObjects=@(As-Array (Get-Val $record.Run @('objects','objectDetails')))
                    $targets=@($runObjects | Where-Object { Test-TargetObject $_ $workload })
                    foreach ($ro in $targets) {
                        $key=Get-ObjectKey $ro;if (-not $key) { continue }
                        $state=Resolve-ObjectState $ro $record.Status ($workload.Kind -eq 'Physical')
                        $sourceId=Get-SourceId $ro;$host=''
                        if (($workload.Kind -eq 'SQL' -or $workload.Kind -eq 'Oracle') -and $sourceId -and $hostNames.ContainsKey($sourceId)) { $host=$hostNames[$sourceId] }
                        $row=[pscustomobject][ordered]@{
                            Cluster=$cluster.ClusterName;Workload=$workload.Name;Scope='Object';ProtectionGroup=$pgName;Host=$host
                            ObjectName=Get-ObjectName $ro;ObjectId=Get-ObjectId $ro;SourceId=$sourceId;ObjectType=Get-ObjectType $ro
                            Environment=Get-ObjectEnvironment $ro;RunType=$record.RunType;Status=$state;ResultTimeUtc=$time
                            LatestSuccessUtc='';Message=Get-ResultMessage $ro $record $state
                        }
                        Add-LatestState $currentObjects "Object|$key" $state $row $time
                    }

                    if ($workload.Kind -eq 'SQL' -or $workload.Kind -eq 'Oracle') {
                        foreach ($ro in @($runObjects | Where-Object {(Get-ObjectType $_) -ieq 'kHost' -or (Get-ObjectEnvironment $_) -ieq 'kPhysical'})) {
                            $key=Get-ObjectKey $ro;if (-not $key) { continue }
                            $state=Resolve-ObjectState $ro $record.Status $false
                            $row=[pscustomobject][ordered]@{
                                Cluster=$cluster.ClusterName;Workload=$workload.Name;Scope='Host discovery';ProtectionGroup=$pgName;Host=Get-ObjectName $ro
                                ObjectName='No database object returned';ObjectId=Get-ObjectId $ro;SourceId=Get-SourceId $ro;ObjectType=Get-ObjectType $ro
                                Environment=Get-ObjectEnvironment $ro;RunType=$record.RunType;Status=$state;ResultTimeUtc=$time
                                LatestSuccessUtc='';Message=Get-ResultMessage $ro $record $state
                            }
                            Add-LatestState $currentHosts "Host|$key" $state $row $time
                        }
                    }

                    if ($workload.Kind -eq 'NAS' -and -not $nasHandled) {
                        if ($targets.Count -gt 0 -or $record.Status -match '^(k)?(Success|Succeeded|Successful|Completed|SucceededWithWarning)$') { $nasHandled=$true }
                        elseif ($record.Status -match '^(k)?(Failed|Failure|Error|Canceled|Cancelled|Canceling)$') {
                            $nasHandled=$true;$state = if ($record.Status -match 'Cancel') { 'Cancelled' } else { 'Failed' }
                            $message=Clean-Message $record.Messages;if (-not $message) {$message='Run failed or was cancelled, but Cohesity returned no object-level details.'}
                            $exceptions += [pscustomobject][ordered]@{
                                Cluster=$cluster.ClusterName;Workload=$workload.Name;Scope='PG fallback';ProtectionGroup=$pgName;Host='';ObjectName=$pgName
                                ObjectId='';SourceId='';ObjectType='';Environment=First-Text @($workload.Environments);RunType=$record.RunType
                                Status=$state;ResultTimeUtc=$time;LatestSuccessUtc='';Message=$message
                            }
                        }
                    }
                }
                $objectResults += @($currentObjects.Values)
                $exceptions += @($currentHosts.Values | Where-Object {$_.Status -in @('Failed','Cancelled')})
            }
        }
    }
}

$script:BackupObjectResults=@(
    $objectResults | Group-Object {"$($_.Cluster)|$($_.Workload)|$($_.ProtectionGroup)|$($_.ObjectId)|$($_.SourceId)|$($_.ObjectName)|$($_.RunType)"} |
    ForEach-Object {$_.Group | Sort-Object ResultTimeUtc -Descending | Select-Object -First 1} |
    Sort-Object Cluster,Workload,ProtectionGroup,ObjectName,RunType
)
$script:BackupExceptions=@(
    $exceptions | Group-Object {"$($_.Cluster)|$($_.Workload)|$($_.Scope)|$($_.ProtectionGroup)|$($_.ObjectId)|$($_.ObjectName)|$($_.RunType)"} |
    ForEach-Object {$_.Group | Sort-Object ResultTimeUtc -Descending | Select-Object -First 1} |
    Sort-Object Cluster,Workload,ProtectionGroup,ObjectName,RunType
)
$script:BackupResults=@((@($script:BackupObjectResults)+@($script:BackupExceptions)) | Sort-Object Cluster,Workload,Scope,ProtectionGroup,ObjectName,RunType)
$script:FailureResults=@($script:BackupResults | Where-Object {$_.Status -in @('Failed','Cancelled')})
$script:BackupWarnings=@($warnings | Sort-Object Cluster,Workload,Operation)

$summary=foreach ($workload in $Workloads) {
    $rows=@($script:BackupObjectResults | Where-Object {$_.Workload -eq $workload.Name})
    [pscustomobject][ordered]@{
        Workload=$workload.Name
        Successful=@($rows | Where-Object {$_.Status -eq 'Success'}).Count
        Failed=@($rows | Where-Object {$_.Status -eq 'Failed'}).Count
        Cancelled=@($rows | Where-Object {$_.Status -eq 'Cancelled'}).Count
        'Total Results'=$rows.Count
    }
}

Write-Host '';Write-Host 'LATEST BACKUP OBJECT RESULT SUMMARY' -ForegroundColor Cyan
$summary | Format-Table Workload,Successful,Failed,Cancelled,'Total Results' -AutoSize
Write-Host 'Each object/run type is counted once using its newest state in the last 30 runs.' -ForegroundColor DarkGray

if ($script:BackupExceptions.Count -gt 0) {
    Write-Host '';Write-Host 'ADDITIONAL NON-OBJECT EXCEPTIONS' -ForegroundColor Yellow
    $script:BackupExceptions | Format-Table Cluster,Workload,Scope,ProtectionGroup,Host,RunType,Status,ResultTimeUtc -Wrap -AutoSize
}

$outputDirectory=$PSScriptRoot;if (-not $outputDirectory) {$outputDirectory=(Get-Location).Path}
$timestamp=Get-Date -Format 'yyyyMMdd_HHmmss'
$script:BackupResultsCsvPath=Join-Path $outputDirectory "Cohesity_BackupResults_$timestamp.csv"
if ($script:BackupResults.Count -gt 0) {
    $script:BackupResults | Export-Csv -Path $script:BackupResultsCsvPath -NoTypeInformation -Encoding UTF8
    Write-Host '';Write-Host 'Complete backup-result CSV:' -ForegroundColor Cyan
    Write-Host $script:BackupResultsCsvPath;Write-Host "CSV rows: $($script:BackupResults.Count)"
} else { $script:BackupResultsCsvPath=$null;Write-Host '';Write-Host 'No object results or exceptions were returned.' -ForegroundColor Yellow }

Write-Host '';Write-Host 'UNRESOLVED FAILURES AND CANCELLATIONS' -ForegroundColor Cyan
if ($script:FailureResults.Count -gt 0) {
    $script:FailureResults | Format-Table Cluster,Workload,Scope,ProtectionGroup,Host,ObjectName,RunType,Status,ResultTimeUtc -Wrap -AutoSize
} else { Write-Host 'None found.' -ForegroundColor Green }

if ($script:BackupWarnings.Count -gt 0) {
    Write-Host '';Write-Host 'COLLECTION WARNINGS' -ForegroundColor Yellow
    $script:BackupWarnings | Format-Table Cluster,Workload,Operation,Warning -Wrap -AutoSize
}
