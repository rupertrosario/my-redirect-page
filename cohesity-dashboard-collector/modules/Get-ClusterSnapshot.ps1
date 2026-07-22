function Get-ClusterSnapshot {
    [CmdletBinding()]
    param([object]$Cluster, [hashtable]$Config, [hashtable]$BaseHeaders)

    $id = [string](Get-PropertyValue $Cluster @('clusterId','id') '')
    $name = [string](Get-PropertyValue $Cluster @('clusterName','name','displayName') ("Unknown-$id"))
    $headers = @{}
    foreach ($key in $BaseHeaders.Keys) { $headers[$key] = $BaseHeaders[$key] }
    $headers.accessClusterId = $id
    $errors = New-Object System.Collections.Generic.List[string]
    $allFailures = @()
    $pgSeen = @{}
    $activePg = 0
    $pausedPg = 0

    $inventory = [ordered]@{}
    foreach ($environment in Get-EnvironmentDefinitions) {
        $inventory[$environment.Key] = [ordered]@{ label=$environment.Label; total=0; successful=0; failed=0; cancelled=0 }
    }

    $capacity = [ordered]@{ usedBytes=$null; totalBytes=$null; availableBytes=$null; usedPercent=$null }
    try {
        $storage = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints.Capacity -Headers $headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
        $used = [double](Get-PropertyValue $storage @('localUsageBytes','usedCapacityBytes','usedBytes') 0)
        $total = [double](Get-PropertyValue $storage @('totalCapacityBytes','capacityBytes') 0)
        $available = [double](Get-PropertyValue $storage @('localAvailableBytes','availableBytes') ([math]::Max(0,$total-$used)))
        $capacity = [ordered]@{ usedBytes=$used; totalBytes=$total; availableBytes=$available; usedPercent=if($total){[math]::Round(100*$used/$total,1)}else{$null} }
    } catch { $errors.Add("Capacity: $($_.Exception.Message)") | Out-Null }

    $garbageBytes = $null
    try {
        $variants = @("$name (ID $id)", "$name(ID $id)", "$name+(ID+$id)")
        foreach ($entity in $variants) {
            $query = '?schemaName=ApolloV2ClusterStats&metricName=EstimatedGarbageBytes&startTimeMsecs=2&entityId={0}&rollupFunction=latest&rollupIntervalSecs=30&metricUnitType=0&range=day' -f [uri]::EscapeDataString($entity)
            try {
                $gc = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path ($Config.Endpoints.Garbage+$query) -Headers $headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
                $points = Get-Collection $gc @('dataPointVec')
                if ($points.Count) {
                    $last = $points | Select-Object -Last 1
                    $garbageBytes = [double](Get-NestedValue $last 'data.int64Value' 0)
                    break
                }
            } catch { }
        }
        if ($null -eq $garbageBytes) { throw 'No EstimatedGarbageBytes datapoint returned.' }
    } catch { $errors.Add("GC reclaimable: $($_.Exception.Message)") | Out-Null }

    foreach ($environment in Get-EnvironmentDefinitions) {
        $objects = @()
        $unresolved = @{}
        try {
            $query = '?environments={0}&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000' -f [uri]::EscapeDataString($environment.Api)
            $pgResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path ($Config.Endpoints.ProtectionGroups+$query) -Headers $headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
            $groups = Get-Collection $pgResponse @('protectionGroups','items','data')

            foreach ($pg in $groups) {
                $pgId = [string](Get-PropertyValue $pg @('id','protectionGroupId') '')
                $pgName = [string](Get-PropertyValue $pg @('name','protectionGroupName') $pgId)
                if ($pgId -and -not $pgSeen.ContainsKey($pgId)) {
                    $pgSeen[$pgId] = $true
                    if ((Get-PropertyValue $pg @('isPaused','paused') $false) -eq $true) { $pausedPg++ } else { $activePg++ }
                }
                $pgObjects = @(Get-PgObjects -ProtectionGroup $pg -Environment $environment)
                $objects += $pgObjects

                $lastRun = Get-PropertyValue $pg @('lastRun') $null
                $lastInfo = @(Get-Collection (Get-PropertyValue $lastRun @('localBackupInfo','localSnapshotInfo') @())) | Select-Object -First 1
                $lastStatus = [string](Get-PropertyValue $lastInfo @('status') (Get-PropertyValue $lastRun @('status') ''))
                $inspectRuns = ($environment.Key -in @('sql','oracle')) -or (-not (Test-SuccessStatus $lastStatus)) -or ($lastStatus -match 'warning')
                if (-not $inspectRuns -or -not $pgId) { continue }

                try {
                    $runPath = $Config.Endpoints.PgRunsTemplate -f [uri]::EscapeDataString($pgId)
                    $runPath += '?numRuns={0}&excludeNonRestorableRuns=false&includeObjectDetails=true' -f $Config.FailureRunsPerPG
                    $runResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $runPath -Headers $headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
                    $runs = @(Get-Collection $runResponse @('runs','items','data') | Sort-Object {
                        $info = @(Get-Collection (Get-PropertyValue $_ @('localBackupInfo','localSnapshotInfo') @())) | Select-Object -First 1
                        [int64](Get-PropertyValue $info @('endTimeUsecs','startTimeUsecs') 0)
                    } -Descending)

                    foreach ($run in $runs) {
                        $runInfo = @(Get-Collection (Get-PropertyValue $run @('localBackupInfo','localSnapshotInfo') @())) | Select-Object -First 1
                        $topStatus = [string](Get-PropertyValue $runInfo @('status') (Get-PropertyValue $run @('status') ''))
                        $topType = Convert-RunType ([string](Get-PropertyValue $runInfo @('runType') (Get-PropertyValue $run @('runType') 'Unknown')))
                        $topTime = Get-PropertyValue $runInfo @('endTimeUsecs','startTimeUsecs') (Get-PropertyValue $run @('endTimeUsecs','startTimeUsecs') 0)
                        $runObjects = Get-Collection (Get-PropertyValue $run @('objects','objectDetails') @())

                        if (-not $runObjects.Count -and ((Test-FailedStatus $topStatus) -or (Test-CancelledStatus $topStatus))) {
                            $key = "PG:$pgId|$topType"
                            if (-not $unresolved.ContainsKey($key)) {
                                $unresolved[$key] = [ordered]@{ cluster=$name; workload=$environment.Label; protectionGroup=$pgName; objectName=$pgName; objectId=$pgId; scope='ProtectionGroupFallback'; runType=$topType; status=if(Test-CancelledStatus $topStatus){'Cancelled'}else{'Failed'}; failureTimeUtc=Convert-UsecsToUtc $topTime; latestSuccessUtc=''; error='Object details were not returned by the API.' }
                            }
                            continue
                        }

                        foreach ($runObject in $runObjects) {
                            $object = Get-PropertyValue $runObject @('object') $runObject
                            $objectName = [string](Get-PropertyValue $object @('name','objectName','sourceName','databaseUniqueName') '')
                            $objectId = [string](Get-PropertyValue $object @('id','objectId','sourceId') $objectName)
                            if (-not $objectId) { continue }
                            $objectInfos = Get-Collection (Get-PropertyValue $runObject @('localSnapshotInfo','localBackupInfo') @())
                            if (-not $objectInfos.Count) { $objectInfos = @($runInfo) }
                            foreach ($objectInfo in $objectInfos) {
                                $status = [string](Get-PropertyValue $objectInfo @('status') $topStatus)
                                $runType = Convert-RunType ([string](Get-PropertyValue $objectInfo @('runType') $topType))
                                $time = Get-PropertyValue $objectInfo @('endTimeUsecs','startTimeUsecs') $topTime
                                $key = "$objectId|$runType"
                                if ($unresolved.ContainsKey($key)) {
                                    $existing = $unresolved[$key]
                                    if ($null -ne $existing -and -not $existing.latestSuccessUtc -and (Test-SuccessStatus $status)) { $existing.latestSuccessUtc = Convert-UsecsToUtc $time }
                                    continue
                                }
                                if (Test-SuccessStatus $status) { $unresolved[$key] = $null; continue }
                                if ((Test-FailedStatus $status) -or (Test-CancelledStatus $status)) {
                                    $attempt = @(Get-Collection (Get-PropertyValue $objectInfo @('failedAttempts') @())) | Select-Object -First 1
                                    $message = [string](Get-PropertyValue $attempt @('message','errorMessage') (Get-PropertyValue $objectInfo @('message','errorMessage','reason') ''))
                                    $unresolved[$key] = [ordered]@{ cluster=$name; workload=$environment.Label; protectionGroup=$pgName; objectName=if($objectName){$objectName}else{$objectId}; objectId=$objectId; scope='Object'; runType=$runType; status=if(Test-CancelledStatus $status){'Cancelled'}else{'Failed'}; failureTimeUtc=Convert-UsecsToUtc $time; latestSuccessUtc=''; error=$message }
                                }
                            }
                        }
                    }
                } catch { $errors.Add("$($environment.Label) run details $pgName : $($_.Exception.Message)") | Out-Null }
            }
        } catch { $errors.Add("$($environment.Label) inventory: $($_.Exception.Message)") | Out-Null }

        $uniqueObjects = @($objects | Group-Object { if($_.id){$_.id}else{$_.name} } | ForEach-Object { $_.Group[0] })
        $failures = @($unresolved.Values | Where-Object { $null -ne $_ })
        $failedIds = @($failures | Where-Object status -eq 'Failed' | Select-Object -ExpandProperty objectId -Unique)
        $cancelledIds = @($failures | Where-Object status -eq 'Cancelled' | Select-Object -ExpandProperty objectId -Unique | Where-Object { $_ -notin $failedIds })
        $inventory[$environment.Key].total = $uniqueObjects.Count
        $inventory[$environment.Key].failed = $failedIds.Count
        $inventory[$environment.Key].cancelled = $cancelledIds.Count
        $inventory[$environment.Key].successful = [math]::Max(0, $uniqueObjects.Count-$failedIds.Count-$cancelledIds.Count)
        $allFailures += $failures
    }

    $version = [string](Get-PropertyValue $Cluster @('currentVersion','version','clusterSoftwareVersion','softwareVersion') 'Unknown')
    $reportedHealth = [string](Get-PropertyValue $Cluster @('healthStatus','health','status') 'Healthy')
    $health = if ($errors.Count -eq 0) { $reportedHealth } elseif ($capacity.totalBytes -or ($inventory.Values | Measure-Object total -Sum).Sum) { 'Warning' } else { 'Unavailable' }
    [ordered]@{
        id=$id; name=$name
        location=Get-LocationText (Get-PropertyValue $Cluster @('location','clusterLocation','siteName','regionName') 'Unknown')
        version=$version; versionStatus=Get-VersionStatus $version $Config.TargetVersion
        health=$health; availability=if($health -eq 'Unavailable'){'Unavailable'}else{'Available'}
        lastSuccessfulCollectionUtc=if($health -eq 'Unavailable'){''}else{[datetime]::UtcNow.ToString('o')}
        stale=$false; missedRuns=0
        capacity=$capacity; gcReclaimableBytes=$garbageBytes
        protectionGroups=[ordered]@{ active=$activePg; paused=$pausedPg; total=$activePg+$pausedPg }
        inventory=$inventory; failures=@($allFailures)
        collectionErrors=@($errors)
    }
}
