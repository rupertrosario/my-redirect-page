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
    $openAlerts = 0
    $hardwareAlerts = @()

    $inventory = [ordered]@{}
    foreach ($environment in @(Get-EnvironmentDefinitions)) {
        $inventory[$environment.Key] = [ordered]@{
            label=$environment.Label; total=0; successful=0; failed=0; cancelled=0
        }
    }

    $capacity = [ordered]@{
        usedBytes=$null; totalBytes=$null; availableBytes=$null; usedPercent=$null
    }
    try {
        $storage = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $Config.Endpoints.Capacity `
            -Headers $headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls

        $storageCandidates = @($storage)
        foreach ($containerName in @('stats','storageStats','data','result')) {
            $container = Get-PropertyValue $storage @($containerName) $null
            if ($null -ne $container) { $storageCandidates += @($container) }
        }

        $used = $null
        $total = $null
        $available = $null
        foreach ($candidate in @($storageCandidates)) {
            if ($null -eq $used) {
                $used = Get-NumericPropertyValue $candidate @(
                    'localUsageBytes','usedCapacityBytes','usedBytes','usageBytes'
                )
            }
            if ($null -eq $total) {
                $total = Get-NumericPropertyValue $candidate @(
                    'totalCapacityBytes','capacityBytes','totalBytes'
                )
            }
            if ($null -eq $available) {
                $available = Get-NumericPropertyValue $candidate @(
                    'localAvailableBytes','availableCapacityBytes','availableBytes'
                )
            }
        }
        if ($null -eq $used -or $null -eq $total) {
            throw 'Storage response did not contain numeric used and total capacity byte values.'
        }
        if ($null -eq $available) { $available = [math]::Max(0,[double]$total-[double]$used) }
        $capacity = [ordered]@{
            usedBytes=[double]$used
            totalBytes=[double]$total
            availableBytes=[double]$available
            usedPercent=if([double]$total -gt 0){[math]::Round(100*[double]$used/[double]$total,1)}else{$null}
        }
    } catch {
        $errors.Add("Capacity: $($_.Exception.Message)") | Out-Null
    }

    $garbageBytes = $null
    try {
        $safeName = $name -replace '\s+',''
        $entityVariants = @(
            "$safeName+(ID+$id)",
            [uri]::EscapeDataString("$name (ID $id)"),
            [uri]::EscapeDataString("$name(ID $id)")
        )
        foreach ($entity in $entityVariants) {
            $query = '?schemaName=ApolloV2ClusterStats&metricName=EstimatedGarbageBytes' +
                '&startTimeMsecs=2&entityId={0}&rollupFunction=latest' +
                '&rollupIntervalSecs=30&metricUnitType=0&range=day' -f $entity
            try {
                $gc = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl `
                    -Path ($Config.Endpoints.Garbage+$query) -Headers $headers `
                    -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
                $points = @(Get-Collection $gc @('dataPointVec','dataPoints','items'))
                if (@($points).Count -gt 0) {
                    $last = @($points) | Select-Object -Last 1
                    $garbageBytes = ConvertTo-NullableDouble (
                        Get-NestedValue $last 'data.int64Value' (
                            Get-PropertyValue $last @('int64Value','value') $null
                        )
                    )
                    if ($null -ne $garbageBytes) { break }
                }
            } catch { }
        }
        if ($null -eq $garbageBytes) { throw 'No numeric EstimatedGarbageBytes datapoint returned.' }
    } catch {
        $errors.Add("GC reclaimable: $($_.Exception.Message)") | Out-Null
    }

    foreach ($environment in @(Get-EnvironmentDefinitions)) {
        $objectIndex = @{}
        $streamState = @{}
        $missingObjectDetailPgs = @{}
        $protectionGroupCount = 0

        try {
            $query = '?environments={0}&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000' -f `
                [uri]::EscapeDataString($environment.Api)
            $pgResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl `
                -Path ($Config.Endpoints.ProtectionGroups+$query) -Headers $headers `
                -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
            $groups = @(Get-Collection $pgResponse @('protectionGroups','items','data'))
            $protectionGroupCount = @($groups).Count

            foreach ($pg in @($groups)) {
                $pgId = [string](Get-PropertyValue $pg @('id','protectionGroupId') '')
                $pgName = [string](Get-PropertyValue $pg @('name','protectionGroupName') $pgId)
                if ($pgId -and -not $pgSeen.ContainsKey($pgId)) {
                    $pgSeen[$pgId] = $true
                    if ((Get-PropertyValue $pg @('isPaused','paused') $false) -eq $true) {
                        $pausedPg++
                    } else {
                        $activePg++
                    }
                }
                if (-not $pgId) { continue }

                try {
                    $runPath = $Config.Endpoints.PgRunsTemplate -f [uri]::EscapeDataString($pgId)
                    $runPath += '?numRuns={0}&excludeNonRestorableRuns=false&includeObjectDetails=true' -f `
                        [int]$Config.FailureRunsPerPG
                    $runResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl `
                        -Path $runPath -Headers $headers -TimeoutSec $Config.RequestTimeoutSec `
                        -VerifyTls $Config.VerifyTls
                    $runs = @(Get-Collection $runResponse @('runs','items','data') | Sort-Object {
                        $infos = @(Get-Collection (Get-PropertyValue $_ @('localBackupInfo','localSnapshotInfo') @()))
                        $info = @($infos) | Select-Object -First 1
                        [int64](Get-PropertyValue $info @('endTimeUsecs','startTimeUsecs') 0)
                    } -Descending)

                    foreach ($run in @($runs)) {
                        $runInfos = @(Get-Collection (Get-PropertyValue $run @('localBackupInfo','localSnapshotInfo') @()))
                        $runInfo = @($runInfos) | Select-Object -First 1
                        $topStatus = [string](Get-PropertyValue $runInfo @('status') (
                            Get-PropertyValue $run @('status') ''
                        ))
                        $topType = Convert-RunType ([string](Get-PropertyValue $runInfo @('runType') (
                            Get-PropertyValue $run @('runType') 'Unknown'
                        )))
                        $topTime = Get-PropertyValue $runInfo @('endTimeUsecs','startTimeUsecs') (
                            Get-PropertyValue $run @('endTimeUsecs','startTimeUsecs') 0
                        )
                        $runObjects = @(Get-Collection (
                            Get-PropertyValue $run @('objects','objectDetails') @()
                        ))
                        $matchedObjects = @($runObjects | Where-Object {
                            Test-RunObjectForEnvironment $_ $environment
                        })

                        if (@($matchedObjects).Count -eq 0 -and
                            ((Test-FailedStatus $topStatus) -or (Test-CancelledStatus $topStatus))) {
                            $missingObjectDetailPgs[$pgId] = $pgName
                            continue
                        }

                        foreach ($runObject in @($matchedObjects)) {
                            $identity = Get-RunObjectIdentity $runObject
                            if ([string]::IsNullOrWhiteSpace([string]$identity.id)) { continue }
                            $objectIndex[[string]$identity.id] = $identity

                            $objectInfos = @(Get-Collection (
                                Get-PropertyValue $runObject @('localSnapshotInfo','localBackupInfo') @()
                            ))
                            $objectInfo = @($objectInfos) | Select-Object -First 1
                            $runType = Convert-RunType ([string](Get-PropertyValue $objectInfo @('runType') $topType))
                            $time = Get-PropertyValue $objectInfo @(
                                'endTimeUsecs','startTimeUsecs'
                            ) $topTime
                            $statusValues = @(Get-RunObjectStatusValues $runObject)
                            $state = Get-RunObjectState -RunObject $runObject -RunStatus $topStatus
                            $key = '{0}|{1}' -f [string]$identity.id,$runType

                            if (@($statusValues).Count -eq 0 -and
                                $state -in @('Failed','Cancelled')) {
                                $missingObjectDetailPgs[$pgId] = $pgName
                                continue
                            }

                            if ($streamState.ContainsKey($key)) {
                                $existing = $streamState[$key]
                                if ($null -ne $existing -and $state -eq 'Success' -and
                                    [string]::IsNullOrWhiteSpace([string]$existing.latestSuccessUtc)) {
                                    $existing.latestSuccessUtc = Convert-UsecsToUtc $time
                                }
                                continue
                            }

                            if ($state -eq 'Success') {
                                $streamState[$key] = $null
                                continue
                            }
                            if ($state -in @('Failed','Cancelled')) {
                                $message = Get-RunObjectMessage -RunObject $runObject -RunInfo $runInfo
                                if ([string]::IsNullOrWhiteSpace($message)) {
                                    $message = 'Object-level failure state returned without an error message.'
                                }
                                $streamState[$key] = [ordered]@{
                                    cluster=$name
                                    workload=$environment.Label
                                    protectionGroup=$pgName
                                    objectName=[string]$identity.name
                                    objectId=[string]$identity.id
                                    scope='Object'
                                    runType=$runType
                                    status=$state
                                    failureTimeUtc=Convert-UsecsToUtc $time
                                    latestSuccessUtc=''
                                    error=$message
                                }
                            }
                        }
                    }
                } catch {
                    $errors.Add("$($environment.Label) run details $pgName : $($_.Exception.Message)") | Out-Null
                }
            }
        } catch {
            $errors.Add("$($environment.Label) protection groups: $($_.Exception.Message)") | Out-Null
        }

        foreach ($missingPg in $missingObjectDetailPgs.Values) {
            $errors.Add("$($environment.Label) object details were not returned for failed PG: $missingPg") | Out-Null
        }

        $failures = @($streamState.Values | Where-Object { $null -ne $_ })
        $failedIds = @($failures | Where-Object { $_.status -eq 'Failed' } |
            ForEach-Object { [string]$_.objectId } | Sort-Object -Unique)
        $cancelledIds = @($failures | Where-Object { $_.status -eq 'Cancelled' } |
            ForEach-Object { [string]$_.objectId } | Where-Object { $_ -notin $failedIds } |
            Sort-Object -Unique)
        $total = @($objectIndex.Keys).Count
        if ($protectionGroupCount -gt 0 -and $total -eq 0) {
            $errors.Add(
                "$($environment.Label) has $protectionGroupCount protection group(s), but no matching object-level run details were returned."
            ) | Out-Null
        }
        $inventory[$environment.Key].total = $total
        $inventory[$environment.Key].failed = @($failedIds).Count
        $inventory[$environment.Key].cancelled = @($cancelledIds).Count
        $inventory[$environment.Key].successful = [math]::Max(
            0,$total-@($failedIds).Count-@($cancelledIds).Count
        )
        $allFailures += @($failures)
    }

    try {
        $clusterAlertsPath = Get-PropertyValue $Config.Endpoints @('ClusterAlerts') `
            '/v2/alerts?maxAlerts=10000&alertStates=kOpen,kNote'
        $alertResponse = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl `
            -Path $clusterAlertsPath -Headers $headers `
            -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
        $clusterAlerts = @(Get-Collection $alertResponse @('alerts','alertsList','items','data'))
        $openAlerts = @($clusterAlerts).Count

        $hardwareAlerts = @($clusterAlerts | Where-Object {
            $document = Get-PropertyValue $_ @('alertDocument') $null
            $classification = @(
                Get-PropertyValue $_ @('alertTypeBucket','alertCategory','category','type','alertTypeName') ''
                Get-PropertyValue $document @('alertName','alertTitle','alertCause','alertDescription') ''
            ) -join ' '
            $classification -match '(?i)hardware|node.?health|disk|ssd|hdd|chassis|fan|power|temperature|memory|cpu|nic'
        } | ForEach-Object {
            $alert = $_
            $document = Get-PropertyValue $alert @('alertDocument') $null
            $occurrences = ConvertTo-NullableDouble (
                Get-PropertyValue $alert @('dedupCount','occurrenceCount') 1
            )
            [ordered]@{
                cluster=$name
                severity=([string](Get-PropertyValue $alert @('severity') 'Unknown') -replace '^k','')
                component=([string](Get-PropertyValue $alert @(
                    'alertTypeBucket','alertCategory','category','type'
                ) 'Hardware') -replace '^k','')
                alertCode=[string](Get-PropertyValue $alert @('alertCode','code') '')
                node=[string](Get-PropertyValue $alert @('nodeName','entityName','affectedEntityName') (
                    Get-AlertPropertyValue $alert @('node_name','node_ip','nodeId','node_id','disk_id')
                ))
                message=[string](Get-PropertyValue $document @(
                    'alertDescription','alertCause','alertName'
                ) (Get-PropertyValue $alert @('description','message') ''))
                firstSeenUtc=Convert-UsecsToUtc (
                    Get-PropertyValue $alert @('firstTimestampUsecs','timestampUsecs') 0
                )
                lastSeenUtc=Convert-UsecsToUtc (
                    Get-PropertyValue $alert @('latestTimestampUsecs','timestampUsecs') 0
                )
                occurrences=if($null -eq $occurrences){1}else{[int64]$occurrences}
                status=([string](Get-PropertyValue $alert @('alertState','state') 'Open') -replace '^k','')
            }
        })
    } catch {
        $errors.Add("Cluster alerts: $($_.Exception.Message)") | Out-Null
    }

    $version = [string](Get-PropertyValue $Cluster @(
        'currentVersion','version','clusterSoftwareVersion','softwareVersion'
    ) 'Unknown')
    $reportedHealth = [string](Get-PropertyValue $Cluster @('healthStatus','health','status') 'Healthy')
    $inventoryTotal = 0
    foreach ($item in $inventory.Values) { $inventoryTotal += [int]$item.total }
    $health = if (@($errors).Count -eq 0) {
        $reportedHealth
    } elseif ($null -ne $capacity.totalBytes -or $inventoryTotal -gt 0) {
        'Warning'
    } else {
        'Unavailable'
    }

    return [ordered]@{
        id=$id
        name=$name
        location=Get-LocationText (Get-PropertyValue $Cluster @(
            'location','clusterLocation','siteName','regionName'
        ) 'Unknown')
        version=$version
        versionStatus=Get-VersionStatus $version $Config.TargetVersion
        health=$health
        availability=if($health -eq 'Unavailable'){'Unavailable'}else{'Available'}
        lastSuccessfulCollectionUtc=if($health -eq 'Unavailable'){''}else{[datetime]::UtcNow.ToString('o')}
        stale=$false
        missedRuns=0
        capacity=$capacity
        gcReclaimableBytes=$garbageBytes
        protectionGroups=[ordered]@{
            active=$activePg; paused=$pausedPg; total=$activePg+$pausedPg
        }
        inventory=$inventory
        openAlerts=$openAlerts
        hardwareAlerts=@($hardwareAlerts)
        failures=@($allFailures)
        collectionErrors=@($errors)
    }
}
