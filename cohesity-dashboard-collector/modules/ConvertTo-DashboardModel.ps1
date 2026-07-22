function ConvertTo-DashboardModel {
    [CmdletBinding()]
    param([object]$Raw, [hashtable]$Config, [object]$PreviousModel)

    function Set-Value($Object,[string]$Name,$Value) {
        if ($Object.PSObject.Properties[$Name]) { $Object.$Name = $Value }
        else { $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force }
    }
    function Clone-Value($Value) { if($null -eq $Value){return $null}; $Value | ConvertTo-Json -Depth 100 | ConvertFrom-Json }

    $previousById = @{}
    foreach ($old in @(Get-PropertyValue $PreviousModel @('clusters') @())) { if($old.id){$previousById[[string]$old.id]=$old} }
    $currentIds = @{}
    $rows = @()

    foreach ($snapshot in @(Get-PropertyValue $Raw @('snapshots') @())) {
        $id = [string](Get-PropertyValue $snapshot @('id') '')
        $currentIds[$id] = $true
        $isUnavailable = [string](Get-PropertyValue $snapshot @('availability') '') -eq 'Unavailable'
        if ($isUnavailable -and $previousById.ContainsKey($id)) {
            $row = Clone-Value $previousById[$id]
            Set-Value $row 'availability' 'Unavailable'
            Set-Value $row 'health' 'Unavailable'
            Set-Value $row 'stale' $true
            Set-Value $row 'missedRuns' ([int](Get-PropertyValue $row @('missedRuns') 0)+1)
            Set-Value $row 'collectionErrors' @(Get-PropertyValue $snapshot @('collectionErrors') @())
        } else { $row = $snapshot }
        $rows += $row
    }

    foreach ($old in $previousById.Values) {
        if ($currentIds.ContainsKey([string]$old.id)) { continue }
        $row = Clone-Value $old
        Set-Value $row 'availability' 'Cluster Gone'
        Set-Value $row 'health' 'Unavailable'
        Set-Value $row 'stale' $true
        Set-Value $row 'missedRuns' ([int](Get-PropertyValue $row @('missedRuns') 0)+1)
        Set-Value $row 'collectionErrors' @('Cluster is no longer returned by the Helios cluster-list GET. Values are from the last successful refresh.')
        $rows += $row
    }

    $alerts = @(Get-PropertyValue $Raw @('alerts') @())
    foreach ($row in $rows) {
        $clusterAlerts = @($alerts | Where-Object {
            ([string](Get-PropertyValue $_ @('clusterId','cluster_id') '') -eq [string]$row.id) -or
            ([string](Get-PropertyValue $_ @('clusterName') '') -eq [string]$row.name)
        })
        $hardware = foreach ($alert in $clusterAlerts | Where-Object { [string](Get-PropertyValue $_ @('alertTypeBucket','type') '') -eq 'kHardware' }) {
            $document = Get-PropertyValue $alert @('alertDocument') $null
            [ordered]@{
                cluster=$row.name
                severity=[string](Get-PropertyValue $alert @('severity') 'Unknown') -replace '^k',''
                component=([string](Get-PropertyValue $alert @('alertCategory','category') 'Unknown') -replace '^k','')
                alertCode=[string](Get-PropertyValue $alert @('alertCode','code') '')
                node=[string](Get-PropertyValue $alert @('nodeName','entityName','affectedEntityName') '')
                message=[string](Get-PropertyValue $document @('alertDescription') (Get-PropertyValue $alert @('description','message') ''))
                occurrenceTimeUtc=Convert-UsecsToUtc (Get-PropertyValue $alert @('latestTimestampUsecs','timestampUsecs','firstTimestampUsecs') 0)
                occurrences=[int](Get-PropertyValue $alert @('dedupCount','occurrenceCount') 1)
                status=([string](Get-PropertyValue $alert @('alertState','state') 'Open') -replace '^k','')
            }
        }
        Set-Value $row 'openAlerts' $clusterAlerts.Count
        Set-Value $row 'hardwareAlerts' @($hardware)
    }

    $availableRows = @($rows | Where-Object { $_.availability -eq 'Available' })
    $totalInventory = 0
    $gcTotal = 0.0
    $used = 0.0; $capacity = 0.0
    foreach ($row in $availableRows) {
        foreach ($key in @('hyperV','nutanix','nas','oracle','sql','physical')) { $totalInventory += [int](Get-NestedValue $row "inventory.$key.total" 0) }
        $gcTotal += [double](Get-PropertyValue $row @('gcReclaimableBytes') 0)
        $used += [double](Get-NestedValue $row 'capacity.usedBytes' 0)
        $capacity += [double](Get-NestedValue $row 'capacity.totalBytes' 0)
    }
    $warningCount = @($rows | Where-Object { $_.health -eq 'Warning' }).Count
    $unavailableCount = @($rows | Where-Object { $_.availability -ne 'Available' }).Count
    $allFailures = @($rows | ForEach-Object { @(Get-PropertyValue $_ @('failures') @()) })
    $allHardware = @($rows | ForEach-Object { @(Get-PropertyValue $_ @('hardwareAlerts') @()) })
    [ordered]@{
        schemaVersion='2.0'; generatedAtUtc=[datetime]::UtcNow.ToString('o')
        collectionStatus=if($unavailableCount -or $warningCount -or (Get-PropertyValue $Raw @('alertError') '')){'CompletedWithWarnings'}else{'Completed'}
        summary=[ordered]@{
            totalClusters=$rows.Count; availableClusters=$availableRows.Count; unavailableClusters=$unavailableCount
            totalProtectedInventory=$totalInventory; openAlerts=($rows | Measure-Object openAlerts -Sum).Sum
            unresolvedFailures=$allFailures.Count; hardwareAlerts=$allHardware.Count
            capacityUsedPercent=if($capacity){[math]::Round(100*$used/$capacity,1)}else{$null}
            gcReclaimableBytes=$gcTotal
        }
        clusters=@($rows | Sort-Object name)
        failures=$allFailures
        hardwareAlerts=$allHardware
        warnings=@(@(Get-PropertyValue $Raw @('alertError') '') | Where-Object { $_ })
    }
}
