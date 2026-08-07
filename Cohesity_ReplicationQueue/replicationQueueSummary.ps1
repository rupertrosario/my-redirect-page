### Cohesity Replication Queue Status Summary
### Read-only checker based on bseltz-cohesity/scripts powershell/replicationQueue
### Cohesity data operations are GET only. No POST/PUT/PATCH/DELETE, no cancellation, no CSV output.

[CmdletBinding()]
param (
    [Parameter()][string]$vip = 'helios.cohesity.com',
    [Parameter()][string]$username = 'helios',
    [Parameter()][string]$domain = 'local',
    [Parameter()][string]$password,
    [Parameter()][string]$clusterName,
    [Parameter()][array]$jobName,
    [Parameter()][int]$numRuns = 999
)

# Cohesity REST API helper must be in the same directory.
. $(Join-Path -Path $PSScriptRoot -ChildPath cohesity-api.ps1)

# API-key authentication is required so authentication validation is GET-only.
apiauth -vip $vip `
        -username $username `
        -domain $domain `
        -passwd $password `
        -apiKeyAuthentication $true

if(!$cohesity_api.authorized){
    Write-Host 'Not authenticated. This checker requires Cohesity API-key authentication.' -ForegroundColor Yellow
    exit 1
}

# Select a Helios-managed cluster when connecting through Helios.
if($USING_HELIOS){
    if($clusterName){
        $null = heliosCluster $clusterName
    }else{
        Write-Host 'Please provide -clusterName when connecting through Helios.' -ForegroundColor Yellow
        exit 1
    }
}

# GET cluster identity and protection groups.
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

    # GET only: retrieve recent protection runs for this protection group.
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

# Calculate average percentage only for currently running replication subtasks.
# This reuses the same GET detail call used by the original replicationQueue script.
$runningPctValues = @()
$runningRecords = @($records | Where-Object {$_.Status -eq 'kRunning'})

if($runningRecords.Count -gt 0){
    Write-Host ''
    Write-Host ("Checking progress for {0} running replication(s)..." -f $runningRecords.Count) -ForegroundColor DarkGray

    foreach($record in $runningRecords){
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

# Statuses requested for the operational summary.
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

    $note = '-'
    if($status -eq 'kRunning'){
        $note = $runningProgress
    }elseif($status -eq 'kAccepted'){
        $note = 'Waiting'
    }

    $summary += [PSCustomObject]@{
        Status = $status
        Count  = $count
        Detail = $note
    }
}

# Do not hide a status returned by Cohesity that is not in the expected list.
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
