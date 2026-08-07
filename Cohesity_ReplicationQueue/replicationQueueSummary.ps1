### Cohesity Replication Queue Status Summary
### Minimal status checker based on bseltz-cohesity/scripts powershell/replicationQueue
### Authentication uses the normal Cohesity username/password login POST.
### After authentication, all replication/status operations are GET only.
### No cancellation, no PUT/PATCH/DELETE, and no CSV output.

[CmdletBinding()]
param (
    [Parameter(Mandatory=$True)][string]$vip,
    [Parameter(Mandatory=$True)][string]$username,
    [Parameter()][string]$domain = 'local',
    [Parameter()][string]$password,
    [Parameter()][array]$jobName,
    [Parameter()][int]$numRuns = 999
)

# Cohesity REST API helper must be in the same directory.
. $(Join-Path -Path $PSScriptRoot -ChildPath cohesity-api.ps1)

# Direct-cluster username/password authentication.
# If -password is omitted, cohesity-api.ps1 uses its normal cached-password /
# interactive prompt behavior. This authentication path performs POST /login.
if($vip -in @('helios.cohesity.com', 'helios.gov-cohesity.com')){
    Write-Host 'Use the Cohesity cluster VIP/name for this script, not Helios.' -ForegroundColor Yellow
    exit 1
}

apiauth -vip $vip `
        -username $username `
        -domain $domain `
        -passwd $password

if(!$cohesity_api.authorized){
    Write-Host 'Not authenticated.' -ForegroundColor Yellow
    exit 1
}

# Everything below this point is GET only.
$cluster = api get cluster
$jobs = @(api get protectionJobs)

if($jobName){
    $missingJobs = @($jobName | Where-Object {$_ -notin $jobs.name})
    if($missingJobs.Count -gt 0){
        Write-Host ("Protection group(s) not found: {0}" -f ($missingJobs -join ', ')) -ForegroundColor Yellow
        exit 1
    }
}

$selectedJobs = @(
    $jobs |
        Sort-Object -Property name |
        Where-Object {!$jobName -or $_.name -in $jobName}
)

$finishedStates = @('kCanceled', 'kSuccess', 'kFailure', 'kWarning')
$records = @()

Write-Host ''
Write-Host ("Scanning replication queue on {0}..." -f $cluster.name) -ForegroundColor Cyan
Write-Host ''

$jobIndex = 0
foreach($job in $selectedJobs){
    $jobIndex++
    Write-Host ("[{0}/{1}] {2}" -f $jobIndex, $selectedJobs.Count, $job.name) -ForegroundColor DarkGray

    # Same queue-level GET used by the original replicationQueue script.
    $runs = @(api get "protectionRuns?jobId=$($job.id)&numRuns=$numRuns&excludeTasks=true")

    foreach($run in $runs){
        $runStartTimeUsecs = $run.backupRun.stats.startTimeUsecs

        foreach($copyRun in @($run.copyRun)){
            if($copyRun.target.type -eq 'kRemote'){
                $records += [PSCustomObject]@{
                    JobName        = $job.name
                    JobId          = $job.id
                    StartTimeUsecs = [Int64]$runStartTimeUsecs
                    OriginalStatus = [string]$copyRun.status
                    Status         = [string]$copyRun.status
                    Progress       = $null
                }
            }
        }
    }
}

# IMPORTANT:
# The top-level remote copy status can still be kAccepted while one or more
# active replication subtasks are already kRunning. The original script does
# not rely on copyRun.status alone: for every non-finished copy it GETs the
# detailed backup run and reads activeCopySubTasks.publicStatus.
#
# Do the same here so the summary reflects what is actually running.
$activeRecords = @($records | Where-Object {$_.OriginalStatus -notin $finishedStates})
$allRunningPctValues = @()

if($activeRecords.Count -gt 0){
    Write-Host ''
    Write-Host ("Checking {0} active replication record(s)..." -f $activeRecords.Count) -ForegroundColor DarkGray

    $activeIndex = 0
    foreach($record in $activeRecords){
        $activeIndex++
        Write-Host ("  [{0}/{1}] {2}" -f $activeIndex, $activeRecords.Count, $record.JobName) -ForegroundColor DarkGray

        # Same detailed GET path used by the original script.
        $run = api get "/backupjobruns?allUnderHierarchy=true&exactMatchStartTimeUsecs=$($record.StartTimeUsecs)&id=$($record.JobId)"

        if(!$run -or $run.backupJobRuns.protectionRuns.Count -eq 0){
            continue
        }

        $remoteSubTasks = @()

        foreach($task in @($run.backupJobRuns.protectionRuns[0].copyRun.activeTasks)){
            if($task.snapshotTarget.type -eq 2){
                foreach($subTask in @($task.activeCopySubTasks)){
                    if($subTask.snapshotTarget.type -eq 2){
                        $remoteSubTasks += $subTask
                    }
                }
            }
        }

        if($remoteSubTasks.Count -eq 0){
            continue
        }

        $subStatuses = @($remoteSubTasks.publicStatus | Where-Object {$_})

        # One replication can contain both running and waiting objects.
        # Classify the replication as running whenever at least one object is running.
        if('kRunning' -in $subStatuses){
            $record.Status = 'kRunning'

            $pctValues = @(
                $remoteSubTasks |
                    Where-Object {$_.publicStatus -eq 'kRunning'} |
                    ForEach-Object {
                        if($_.replicationInfo -and $_.replicationInfo.PSObject.Properties['pctCompleted']){
                            [double]$_.replicationInfo.pctCompleted
                        }
                    }
            )

            if($pctValues.Count -gt 0){
                $record.Progress = [math]::Round((($pctValues | Measure-Object -Average).Average), 1)
                $allRunningPctValues += $pctValues
            }
        }
        elseif('kAccepted' -in $subStatuses){
            $record.Status = 'kAccepted'
        }
        elseif('kSkipped' -in $subStatuses){
            $record.Status = 'kSkipped'
        }
        else{
            # Preserve any status Cohesity actually returns rather than inventing one.
            $firstStatus = @($subStatuses | Sort-Object -Unique | Select-Object -First 1)
            if($firstStatus.Count -gt 0){
                $record.Status = [string]$firstStatus[0]
            }
        }
    }
}

if($allRunningPctValues.Count -gt 0){
    $runningAverage = [math]::Round((($allRunningPctValues | Measure-Object -Average).Average), 1)
    $runningProgress = "$runningAverage% avg"
}else{
    $runningProgress = '-'
}

$statusOrder = @(
    'kSuccess',
    'kRunning',
    'kAccepted',
    'kSkipped',
    'kFailure',
    'kWarning',
    'kCanceled'
)

$summary = @()
foreach($status in $statusOrder){
    $count = @($records | Where-Object {$_.Status -eq $status}).Count

    $detail = '-'
    if($status -eq 'kRunning'){
        $detail = $runningProgress
    }
    elseif($status -eq 'kAccepted'){
        $detail = 'Waiting'
    }

    $summary += [PSCustomObject]@{
        Status = $status
        Count  = $count
        Detail = $detail
    }
}

# Surface any additional status returned by Cohesity instead of hiding it.
$otherStatuses = @(
    $records.Status |
        Where-Object {$_ -and $_ -notin $statusOrder} |
        Sort-Object -Unique
)

foreach($status in $otherStatuses){
    $summary += [PSCustomObject]@{
        Status = $status
        Count  = @($records | Where-Object {$_.Status -eq $status}).Count
        Detail = 'Returned by API'
    }
}

$runningCount = @($records | Where-Object {$_.Status -eq 'kRunning'}).Count
$acceptedCount = @($records | Where-Object {$_.Status -eq 'kAccepted'}).Count
$remainingCount = $runningCount + $acceptedCount

Write-Host ''
Write-Host '================ REPLICATION SUMMARY ================' -ForegroundColor Cyan
Write-Host ''
$summary | Format-Table -AutoSize

Write-Host ("Remaining to go: {0} - {1} kRunning, {2} kAccepted (waiting)." -f `
    $remainingCount, $runningCount, $acceptedCount) -ForegroundColor Yellow
Write-Host ''
