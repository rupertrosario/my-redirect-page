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

# This version intentionally uses direct-cluster username/password authentication.
# Helios authentication uses an API key in the Cohesity helper and is not required here.
if($vip -in @('helios.cohesity.com', 'helios.gov-cohesity.com')){
    Write-Host 'Use the Cohesity cluster VIP/name for this script, not Helios.' -ForegroundColor Yellow
    exit 1
}

# Normal Cohesity authentication. If -password is omitted, cohesity-api.ps1
# uses its normal cached-password / interactive prompt behavior.
# This authentication path performs POST /login.
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

$records = @()

Write-Host ''
Write-Host ("Scanning replication queue on {0}..." -f $cluster.name) -ForegroundColor Cyan
Write-Host ''

$jobIndex = 0
foreach($job in $selectedJobs){
    $jobIndex++
    Write-Host ("[{0}/{1}] {2}" -f $jobIndex, $selectedJobs.Count, $job.name) -ForegroundColor DarkGray

    # GET recent protection runs for this protection group.
    $runs = @(api get "protectionRuns?jobId=$($job.id)&numRuns=$numRuns&excludeTasks=true")

    foreach($run in $runs){
        $runStartTimeUsecs = $run.backupRun.stats.startTimeUsecs

        foreach($copyRun in @($run.copyRun)){
            if($copyRun.target.type -eq 'kRemote'){
                $records += [PSCustomObject]@{
                    JobName        = $job.name
                    JobId          = $job.id
                    StartTimeUsecs = [Int64]$runStartTimeUsecs
                    Status         = [string]$copyRun.status
                }
            }
        }
    }
}

# Calculate average percentage for running replication subtasks only.
# Uses the same GET detail path as the original replicationQueue script.
$runningPctValues = @()
$runningRecords = @($records | Where-Object {$_.Status -eq 'kRunning'})

if($runningRecords.Count -gt 0){
    Write-Host ''
    Write-Host ("Checking progress for {0} running replication(s)..." -f $runningRecords.Count) -ForegroundColor DarkGray

    $runningIndex = 0
    foreach($record in $runningRecords){
        $runningIndex++
        Write-Host ("  [{0}/{1}] {2}" -f $runningIndex, $runningRecords.Count, $record.JobName) -ForegroundColor DarkGray

        $run = api get "/backupjobruns?allUnderHierarchy=true&exactMatchStartTimeUsecs=$($record.StartTimeUsecs)&id=$($record.JobId)"

        if($run -and $run.backupJobRuns.protectionRuns.Count -gt 0){
            foreach($task in @($run.backupJobRuns.protectionRuns[0].copyRun.activeTasks)){
                if($task.snapshotTarget.type -eq 2){
                    foreach($subTask in @($task.activeCopySubTasks)){
                        if($subTask.snapshotTarget.type -eq 2 -and $subTask.publicStatus -eq 'kRunning'){
                            if($subTask.replicationInfo -and $subTask.replicationInfo.PSObject.Properties['pctCompleted']){
                                $runningPctValues += [double]$subTask.replicationInfo.pctCompleted
                            }
                        }
                    }
                }
            }
        }
    }
}

if($runningPctValues.Count -gt 0){
    $runningAverage = [math]::Round((($runningPctValues | Measure-Object -Average).Average), 1)
    $runningProgress = "$runningAverage% avg"
}else{
    $runningProgress = '-'
}

# Operational status order requested for the summary.
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
    }elseif($status -eq 'kAccepted'){
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
