function ConvertTo-DashboardModel {
    [CmdletBinding()]
    param([object]$Raw, [hashtable]$Config, [object]$PreviousModel)

    function Set-Value {
        param($Object,[string]$Name,$Value)
        if ($Object -is [System.Collections.IDictionary]) {
            $Object[$Name] = $Value
        } elseif ($Object.PSObject.Properties[$Name]) {
            $Object.$Name = $Value
        } else {
            $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force
        }
    }
    function Clone-Value {
        param($Value)
        if ($null -eq $Value) { return $null }
        return ($Value | ConvertTo-Json -Depth 100 | ConvertFrom-Json)
    }

    $previousById = @{}
    foreach ($old in @(Get-PropertyValue $PreviousModel @('clusters') @())) {
        $oldId = [string](Get-PropertyValue $old @('id') '')
        if ($oldId) { $previousById[$oldId] = $old }
    }

    $currentIds = @{}
    $rows = @()
    foreach ($snapshot in @(Get-PropertyValue $Raw @('snapshots') @())) {
        $id = [string](Get-PropertyValue $snapshot @('id') '')
        if ($id) { $currentIds[$id] = $true }
        $isUnavailable = (
            [string](Get-PropertyValue $snapshot @('availability') '')
        ) -eq 'Unavailable'

        if ($isUnavailable -and $id -and $previousById.ContainsKey($id)) {
            $row = Clone-Value $previousById[$id]
            Set-Value $row 'availability' 'Unavailable'
            Set-Value $row 'health' 'Unavailable'
            Set-Value $row 'stale' $true
            Set-Value $row 'missedRuns' (
                [int](Get-PropertyValue $row @('missedRuns') 0)+1
            )
            Set-Value $row 'collectionErrors' @(
                Get-PropertyValue $snapshot @('collectionErrors') @()
            )
        } else {
            $row = $snapshot
        }
        $rows += $row
    }

    foreach ($old in $previousById.Values) {
        $oldId = [string](Get-PropertyValue $old @('id') '')
        if ($currentIds.ContainsKey($oldId)) { continue }
        $row = Clone-Value $old
        Set-Value $row 'availability' 'Cluster Gone'
        Set-Value $row 'health' 'Unavailable'
        Set-Value $row 'stale' $true
        Set-Value $row 'missedRuns' (
            [int](Get-PropertyValue $row @('missedRuns') 0)+1
        )
        Set-Value $row 'collectionErrors' @(
            'Cluster is no longer returned by the Helios cluster-list GET. Values are from the last successful refresh.'
        )
        $rows += $row
    }

    $availableRows = @($rows | Where-Object {
        [string](Get-PropertyValue $_ @('availability') '') -eq 'Available'
    })
    $totalInventory = 0
    $gcTotal = 0.0
    $used = 0.0
    $capacity = 0.0
    foreach ($row in @($availableRows)) {
        foreach ($key in @('hyperV','nutanix','nas','oracle','sql','physical')) {
            $value = ConvertTo-NullableDouble (Get-NestedValue $row "inventory.$key.total" 0)
            if ($null -ne $value) { $totalInventory += [int64]$value }
        }
        $gcValue = ConvertTo-NullableDouble (Get-PropertyValue $row @('gcReclaimableBytes') 0)
        if ($null -ne $gcValue) { $gcTotal += [double]$gcValue }
        $usedValue = ConvertTo-NullableDouble (Get-NestedValue $row 'capacity.usedBytes' 0)
        $capacityValue = ConvertTo-NullableDouble (Get-NestedValue $row 'capacity.totalBytes' 0)
        if ($null -ne $usedValue) { $used += [double]$usedValue }
        if ($null -ne $capacityValue) { $capacity += [double]$capacityValue }
    }

    $warningCount = @($rows | Where-Object {
        [string](Get-PropertyValue $_ @('health') '') -eq 'Warning'
    }).Count
    $unavailableCount = @($rows | Where-Object {
        [string](Get-PropertyValue $_ @('availability') '') -ne 'Available'
    }).Count
    $allFailures = @($rows | ForEach-Object {
        @(Get-PropertyValue $_ @('failures') @())
    })
    $allHardware = @($rows | ForEach-Object {
        @(Get-PropertyValue $_ @('hardwareAlerts') @())
    })
    $warnings = @($rows | ForEach-Object {
        $rowName = [string](Get-PropertyValue $_ @('name') 'Unknown cluster')
        foreach ($message in @(Get-PropertyValue $_ @('collectionErrors') @())) {
            if (-not [string]::IsNullOrWhiteSpace([string]$message)) {
                "$rowName : $message"
            }
        }
    })
    $openAlertTotal = 0
    foreach ($row in @($rows)) {
        $alertCount = ConvertTo-NullableDouble (Get-PropertyValue $row @('openAlerts') 0)
        if ($null -ne $alertCount) { $openAlertTotal += [int64]$alertCount }
    }

    return [ordered]@{
        schemaVersion='2.1'
        generatedAtUtc=[datetime]::UtcNow.ToString('o')
        collectionStatus=if($unavailableCount -or $warningCount -or @($warnings).Count){
            'CompletedWithWarnings'
        }else{
            'Completed'
        }
        summary=[ordered]@{
            totalClusters=@($rows).Count
            availableClusters=@($availableRows).Count
            unavailableClusters=$unavailableCount
            totalProtectedInventory=$totalInventory
            openAlerts=$openAlertTotal
            unresolvedFailures=@($allFailures).Count
            hardwareAlerts=@($allHardware).Count
            capacityUsedPercent=if($capacity -gt 0){
                [math]::Round(100*$used/$capacity,1)
            }else{
                $null
            }
            gcReclaimableBytes=$gcTotal
        }
        clusters=@($rows | Sort-Object {
            [string](Get-PropertyValue $_ @('name') '')
        })
        failures=@($allFailures)
        hardwareAlerts=@($allHardware)
        warnings=@($warnings)
    }
}
