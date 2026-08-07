### Replication Queue + Summary
### Based on bseltz-cohesity/scripts powershell/replicationQueue
### Existing Cohesity API method is retained; reporting is enhanced only.

[CmdletBinding()]
param (
    [Parameter()][string]$vip='helios.cohesity.com',
    [Parameter()][string]$username = 'helios',
    [Parameter()][string]$domain = 'local',
    [Parameter()][string]$tenant,
    [Parameter()][switch]$useApiKey,
    [Parameter()][string]$password,
    [Parameter()][switch]$noPrompt,
    [Parameter()][switch]$mcm,
    [Parameter()][string]$mfaCode,
    [Parameter()][switch]$emailMfaCode,
    [Parameter()][string]$clusterName,
    [Parameter()][array]$jobName,
    [Parameter()][string]$joblist = '',
    [Parameter()][int]$numRuns = 999,
    [Parameter()][datetime]$before,
    [Parameter()][datetime]$after,
    [Parameter()][int]$olderThan,
    [Parameter()][int]$newerThan,
    [Parameter()][int]$daysToKeep = 0,
    [Parameter()][switch]$cancelAll,
    [Parameter()][switch]$cancelOutdated,
    [Parameter()][switch]$showFinished,
    [Parameter()][switch]$commit
)

function gatherList($Param=$null, $FilePath=$null, $Required=$True, $Name='items'){
    $items = @()
    if($Param){
        $Param | ForEach-Object {$items += $_}
    }
    if($FilePath){
        if(Test-Path -Path $FilePath -PathType Leaf){
            Get-Content $FilePath | ForEach-Object {$items += [string]$_}
        }else{
            Write-Host "Text file $FilePath not found!" -ForegroundColor Yellow
            exit
        }
    }
    if($Required -eq $True -and $items.Count -eq 0){
        Write-Host "No $Name specified" -ForegroundColor Yellow
        exit
    }
    return ($items | Sort-Object -Unique)
}

$jobNames = @(gatherList -Param $jobName -FilePath $jobList -Name 'jobs' -Required $false)

# Cohesity REST API helper must be in the same directory
. $(Join-Path -Path $PSScriptRoot -ChildPath cohesity-api.ps1)

# Authenticate
apiauth -vip $vip `
        -username $username `
        -domain $domain `
        -passwd $password `
        -apiKeyAuthentication $useApiKey `
        -mfaCode $mfaCode `
        -sendMfaCode $emailMfaCode `
        -heliosAuthentication $mcm `
        -regionid $region `
        -tenant $tenant `
        -noPromptForPassword $noPrompt

# Select Helios/MCM managed cluster
if($USING_HELIOS -and !$region){
    if($clusterName){
        $thisCluster = heliosCluster $clusterName
    }else{
        Write-Host "Please provide -clusterName when connecting through helios" -ForegroundColor Yellow
        exit 1
    }
}

if(!$cohesity_api.authorized){
    Write-Host "Not authenticated" -ForegroundColor Yellow
    exit 1
}

$cluster = api get cluster

# Preserve existing CSV outputs
$outfileName = "replicationQueue-$($cluster.name).csv"
'"Start Time","Job Name","Status"' | Out-File -FilePath $outfileName

$outfileName2 = "replicationQueue-$($cluster.name)-activeObjects.csv"
'"Start Time","Job Name","Status","Object","Percent Complete"' | Out-File -FilePath $outfileName2

# New reporting outputs
$summaryFile = "replicationQueue-$($cluster.name)-summary.csv"
$taskDetailFile = "replicationQueue-$($cluster.name)-taskDetails.csv"

if(!$commit -and ($cancelAll -or $cancelOutdated)){
    Write-Host "Running in test mode. Use the -commit switch to actually perform cancelations" -ForegroundColor Yellow
}

$now = Get-Date

if($before){
    $olderThanUsecs = dateToUsecs $before
}
if($after){
    $newerThanUsecs = dateToUsecs $after
}
if($olderThan){
    $olderThanUsecs = dateToUsecs ($now.AddDays(-$olderThan))
}
if($newerThan){
    $newerThanUsecs = dateToUsecs ($now.AddDays(-$newerThan))
}

$jobs = api get protectionJobs

# Catch invalid job names
if($jobNames.Count -gt 0){
    $notfoundJobs = $jobNames | Where-Object {$_ -notin $jobs.name}
    if($notfoundJobs){
        Write-Host "Jobs not found $($notfoundJobs -join ', ')" -ForegroundColor Yellow
    }
}

# Same finished-state definition as the original replicationQueue script
$finishedStates = @('kCanceled', 'kSuccess', 'kFailure', 'kWarning')
$nowUsecs = dateToUsecs (Get-Date)

# Reporting collections
$replicationRecords = @()
$taskRows = @()
$itemRows = @()

Write-Host ""
Write-Host "Scanning replication queue on $($cluster.name)..." -ForegroundColor Cyan
Write-Host ""

$selectedJobs = @(
    $jobs |
        Sort-Object -Property name |
        Where-Object {$jobNames.Count -eq 0 -or $_.name -in $jobNames}
)

$totalJobsToScan = $selectedJobs.Count
$jobNumber = 0

foreach($job in $selectedJobs){
    $jobNumber++
    $jobId = $job.id
    $thisJobName = $job.name

    Write-Host ("[{0}/{1}] Getting tasks for {2} - querying up to {3} runs..." -f `
        $jobNumber, $totalJobsToScan, $thisJobName, $numRuns) -ForegroundColor DarkGray

    $runs = api get "protectionRuns?jobId=$jobId&numRuns=$numRuns&excludeTasks=true"
    $returnedRuns = @($runs).Count
    $remoteBefore = $replicationRecords.Count

    foreach($run in $runs){
        $runStartTimeUsecs = $run.backupRun.stats.startTimeUsecs

        foreach($copyRun in $run.copyRun){
            if($copyRun.target.type -eq 'kRemote'){
                $replicationRecords += [PSCustomObject]@{
                    JobName        = $thisJobName
                    JobId          = $jobId
                    StartTimeUsecs = [Int64]$runStartTimeUsecs
                    Status         = $copyRun.status
                }
            }
        }
    }

    $remoteFound = $replicationRecords.Count - $remoteBefore
    Write-Host ("      Returned {0} backup runs; found {1} remote replication record(s)." -f `
        $returnedRuns, $remoteFound) -ForegroundColor DarkGray
}

# Apply the same time filters to both summary and detail output
$filteredRecords = @(
    $replicationRecords | Where-Object {
        $startTimeUsecs = $_.StartTimeUsecs

        $matchesOlder = (
            !($olderThan -or $before) -or
            $startTimeUsecs -le $olderThanUsecs
        )

        $matchesNewer = (
            !($newerThan -or $after) -or
            $startTimeUsecs -ge $newerThanUsecs
        )

        $matchesOlder -and $matchesNewer
    }
)

# ---------------------------------------------------------------------------
# SUMMARY COUNTERS
# ---------------------------------------------------------------------------

$totalCount      = @($filteredRecords).Count
$successCount    = @($filteredRecords | Where-Object {$_.Status -eq 'kSuccess'}).Count
$runningCount    = @($filteredRecords | Where-Object {$_.Status -eq 'kRunning'}).Count
$acceptedCount   = @($filteredRecords | Where-Object {$_.Status -eq 'kAccepted'}).Count
$queuedCount     = @($filteredRecords | Where-Object {$_.Status -eq 'kQueued'}).Count
$failedCount     = @($filteredRecords | Where-Object {$_.Status -eq 'kFailure'}).Count
$warningCount    = @($filteredRecords | Where-Object {$_.Status -eq 'kWarning'}).Count
$canceledCount   = @($filteredRecords | Where-Object {$_.Status -eq 'kCanceled'}).Count

$remainingRecords = @(
    $filteredRecords | Where-Object {$_.Status -notin $finishedStates}
)
$remainingCount = $remainingRecords.Count

$otherActiveCount = @(
    $remainingRecords | Where-Object {
        $_.Status -notin @('kRunning', 'kAccepted', 'kQueued')
    }
).Count

# ---------------------------------------------------------------------------
# CONSOLE SUMMARY - display immediately after queue scan
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "================ REPLICATION SUMMARY ================" -ForegroundColor Cyan

$summaryRows = @(
    [PSCustomObject]@{ Metric = 'Total Replication Tasks'; Count = $totalCount }
    [PSCustomObject]@{ Metric = 'Successful';              Count = $successCount }
    [PSCustomObject]@{ Metric = 'Running';                 Count = $runningCount }
    [PSCustomObject]@{ Metric = 'Accepted';                Count = $acceptedCount }
    [PSCustomObject]@{ Metric = 'Queued';                  Count = $queuedCount }
    [PSCustomObject]@{ Metric = 'Failed';                  Count = $failedCount }
    [PSCustomObject]@{ Metric = 'Warning';                 Count = $warningCount }
    [PSCustomObject]@{ Metric = 'Canceled';                Count = $canceledCount }
    [PSCustomObject]@{ Metric = 'Other Active';            Count = $otherActiveCount }
)

$summaryRows | Format-Table -AutoSize
$summaryRows | Export-Csv -Path $summaryFile -NoTypeInformation

$remainingParts = @()
if($runningCount -gt 0){ $remainingParts += "$runningCount Running" }
if($acceptedCount -gt 0){ $remainingParts += "$acceptedCount Accepted" }
if($queuedCount -gt 0){ $remainingParts += "$queuedCount Queued" }
if($otherActiveCount -gt 0){ $remainingParts += "$otherActiveCount Other Active" }

if($remainingParts.Count -gt 0){
    Write-Host ("Remaining to go: {0} task(s) - {1}." -f `
        $remainingCount, ($remainingParts -join ', ')) -ForegroundColor Yellow
}else{
    Write-Host "Remaining to go: 0 tasks." -ForegroundColor Green
}

Write-Host ""

# ---------------------------------------------------------------------------
# TASK / OBJECT DETAIL
# ---------------------------------------------------------------------------

$sortedRecords = @($filteredRecords | Sort-Object StartTimeUsecs, JobName)
$detailNumber = 0
$activeDetailTotal = @($sortedRecords | Where-Object {$_.Status -notin $finishedStates}).Count

foreach($t in $sortedRecords){

    $backupDate = usecsToDate $t.StartTimeUsecs

    if($t.Status -notin $finishedStates){
        $detailNumber++
        Write-Host ("[Detail {0}/{1}] {2} - {3}" -f `
            $detailNumber, $activeDetailTotal, $t.JobName, $backupDate) -ForegroundColor DarkGray
    }

    # Preserve original basic queue CSV
    '"{0}","{1}","{2}"' -f $backupDate, $t.JobName, $t.Status |
        Out-File -FilePath $outfileName -Append

    if($t.Status -notin $finishedStates){

        $run = api get "/backupjobruns?allUnderHierarchy=true&exactMatchStartTimeUsecs=$($t.StartTimeUsecs)&id=$($t.JobId)"

        if($run -and $run.backupJobRuns.protectionRuns.Count -gt 0){

            $protectionRun = $run.backupJobRuns.protectionRuns[0]
            $runStartTimeUsecs = $protectionRun.backupRun.base.startTimeUsecs

            $remoteTasks = @(
                $protectionRun.copyRun.activeTasks |
                    Where-Object {$_.snapshotTarget.type -eq 2}
            )

            if($remoteTasks.Count -eq 0){
                # Queue status exists but no active task object was returned.
                $taskRows += [PSCustomObject]@{
                    BackupDate       = $backupDate
                    ProtectionGroup  = $t.JobName
                    ReplicationTaskId= '-'
                    Status           = $t.Status
                    Items            = 0
                    RunningItems     = 0
                    AcceptedItems    = 0
                    QueuedItems      = 0
                    Progress         = '-'
                }
            }

            foreach($task in $remoteTasks){

                $noLongerNeeded = ''
                $usecsToKeep = $daysToKeep * 1000000 * 86400
                $timePassed = $nowUsecs - $runStartTimeUsecs

                if($daysToKeep -gt 0 -and $timePassed -gt $usecsToKeep){
                    $noLongerNeeded = "NO LONGER NEEDED"
                }

                $taskId = $task.taskUid.objectId

                $remoteSubTasks = @(
                    $task.activeCopySubTasks |
                        Where-Object {$_.snapshotTarget.type -eq 2}
                )

                $itemCount = $remoteSubTasks.Count
                $runningItems = @(
                    $remoteSubTasks | Where-Object {$_.publicStatus -eq 'kRunning'}
                ).Count
                $acceptedItems = @(
                    $remoteSubTasks | Where-Object {$_.publicStatus -eq 'kAccepted'}
                ).Count
                $queuedItems = @(
                    $remoteSubTasks | Where-Object {$_.publicStatus -eq 'kQueued'}
                ).Count

                # Keep progress calculation close to original script behavior:
                # only kRunning subtasks expose pctCompleted for reporting here.
                $progressValues = @(
                    $remoteSubTasks |
                    Where-Object {$_.publicStatus -eq 'kRunning'} |
                    ForEach-Object {
                        if($_.replicationInfo -and
                           $_.replicationInfo.PSObject.Properties['pctCompleted']){
                            [double]$_.replicationInfo.pctCompleted
                        }
                    }
                )

                if($progressValues.Count -gt 0){
                    $avgProgress = [math]::Round(
                        (($progressValues | Measure-Object -Average).Average),
                        1
                    )
                    $progressDisplay = "$avgProgress%"
                }else{
                    $progressDisplay = "0%"
                }

                $displayStatus = $t.Status
                if($noLongerNeeded){
                    $displayStatus = "$($t.Status) / $noLongerNeeded"
                }

                $taskRows += [PSCustomObject]@{
                    BackupDate        = $backupDate
                    ProtectionGroup   = $t.JobName
                    ReplicationTaskId = $taskId
                    Status            = $displayStatus
                    Items             = $itemCount
                    RunningItems      = $runningItems
                    AcceptedItems     = $acceptedItems
                    QueuedItems       = $queuedItems
                    Progress          = $progressDisplay
                }

                foreach($subTask in ($remoteSubTasks | Sort-Object publicStatus -Descending)){

                    if($subTask.publicStatus -eq 'kRunning' -and
                       $subTask.replicationInfo -and
                       $subTask.replicationInfo.PSObject.Properties['pctCompleted']){
                        $pct = $subTask.replicationInfo.pctCompleted
                    }else{
                        $pct = 0
                    }

                    $itemRows += [PSCustomObject]@{
                        BackupDate        = $backupDate
                        ProtectionGroup   = $t.JobName
                        ReplicationTaskId = $taskId
                        Status            = $subTask.publicStatus
                        Object            = $subTask.entity.displayName
                        PercentComplete   = $pct
                    }

                    # Preserve original activeObjects CSV format
                    '"{0}","{1}","{2}","{3}","{4}"' -f `
                        $backupDate,
                        $t.JobName,
                        $subTask.publicStatus,
                        $subTask.entity.displayName,
                        $pct |
                        Out-File -FilePath $outfileName2 -Append
                }

                # Preserve cancellation logic from the original script
                if($cancelAll -or ($cancelOutdated -and ($noLongerNeeded -eq "NO LONGER NEEDED"))){

                    $cancelTaskParams = @{
                        "jobId"       = $t.JobId
                        "copyTaskUid" = @{
                            "id"                   = $task.taskUid.objectId
                            "clusterId"            = $task.taskUid.clusterId
                            "clusterIncarnationId" = $task.taskUid.clusterIncarnationId
                        }
                    }

                    if($commit){
                        $null = api post "protectionRuns/cancel/$($t.JobId)" $cancelTaskParams
                    }
                }
            }
        }

    }elseif($showFinished){

        # The original script exposes finished copy-run status, but not an
        # active replication task ID once the active task is gone.
        $taskRows += [PSCustomObject]@{
            BackupDate        = $backupDate
            ProtectionGroup   = $t.JobName
            ReplicationTaskId = '-'
            Status            = $t.Status
            Items             = '-'
            RunningItems      = '-'
            AcceptedItems     = '-'
            QueuedItems       = '-'
            Progress          = '-'
        }
    }
}

# ---------------------------------------------------------------------------
# TASK DETAIL
# ---------------------------------------------------------------------------

if($taskRows.Count -gt 0){

    Write-Host ""
    Write-Host "================ TASK DETAIL ========================" -ForegroundColor Cyan

    $taskRows |
        Sort-Object BackupDate, ProtectionGroup |
        Format-Table BackupDate,
                     ProtectionGroup,
                     ReplicationTaskId,
                     Status,
                     Items,
                     RunningItems,
                     AcceptedItems,
                     QueuedItems,
                     Progress -AutoSize

    $taskRows |
        Sort-Object BackupDate, ProtectionGroup |
        Export-Csv -Path $taskDetailFile -NoTypeInformation

}else{
    Write-Host ""
    Write-Host "No replication task details found"
    @() | Export-Csv -Path $taskDetailFile -NoTypeInformation
}

# ---------------------------------------------------------------------------
# ACTIVE ITEM DETAIL
# ---------------------------------------------------------------------------

if($itemRows.Count -gt 0){

    Write-Host ""
    Write-Host "================ ACTIVE ITEM DETAIL =================" -ForegroundColor Cyan

    $itemRows |
        Sort-Object BackupDate, ProtectionGroup, Status, Object |
        Format-Table BackupDate,
                     ProtectionGroup,
                     ReplicationTaskId,
                     Status,
                     Object,
                     PercentComplete -AutoSize
}

Write-Host ""
Write-Host "Output saved to:" -ForegroundColor Green
Write-Host "  $outfileName"
Write-Host "  $outfileName2"
Write-Host "  $summaryFile"
Write-Host "  $taskDetailFile"
Write-Host ""
