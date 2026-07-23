# Validated protected-object inventory for the dashboard collector.
# GET-only and Windows PowerShell 5.1 compatible.

function Get-VwiDefinitions {
    return @(
        [pscustomobject]@{ Key='hyperV';   Label='Hyper-V';     Environments=@('kHyperV');               Kind='HyperV' },
        [pscustomobject]@{ Key='nutanix';  Label='Nutanix AHV'; Environments=@('kAcropolis');            Kind='Nutanix' },
        [pscustomobject]@{ Key='nas';      Label='NAS';         Environments=@('kGenericNas','kIsilon'); Kind='NAS' },
        [pscustomobject]@{ Key='physical'; Label='Physical';    Environments=@('kPhysical');             Kind='Physical' },
        [pscustomobject]@{ Key='sql';      Label='SQL';         Environments=@('kSQL');                  Kind='SQL' },
        [pscustomobject]@{ Key='oracle';   Label='Oracle';      Environments=@('kOracle');               Kind='Oracle' }
    )
}

function Get-VwiText {
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

function Join-VwiText {
    param([string]$Current,[string]$Additional)

    $values = @()
    foreach ($value in @($Current,$Additional)) {
        if ([string]::IsNullOrWhiteSpace($value)) { continue }
        foreach ($part in @($value -split ';')) {
            $trimmed = $part.Trim()
            if (-not [string]::IsNullOrWhiteSpace($trimmed)) { $values += $trimmed }
        }
    }

    return (@($values | Select-Object -Unique) -join '; ')
}

function Set-VwiProperty {
    param($Object,[string]$Name,$Value)

    if ($null -eq $Object) { return }
    if ($Object -is [System.Collections.IDictionary]) {
        $Object[$Name] = $Value
    }
    elseif ($null -ne $Object.PSObject.Properties[$Name]) {
        $Object.$Name = $Value
    }
    else {
        $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value -Force
    }
}

function Get-VwiPgId {
    param($ProtectionGroup)
    return Get-VwiText @(
        (Get-PropertyValue $ProtectionGroup @('id','protectionGroupId') ''),
        (Get-PropertyValue $ProtectionGroup @('name','protectionGroupName') '')
    )
}

function Get-VwiPgName {
    param($ProtectionGroup)
    return Get-VwiText @(
        (Get-PropertyValue $ProtectionGroup @('name','protectionGroupName') ''),
        (Get-VwiPgId $ProtectionGroup)
    )
}

function Get-VwiObjectCore {
    param($Candidate)
    if ($null -eq $Candidate) { return $null }
    return (Get-PropertyValue $Candidate @('object') $Candidate)
}

function Get-VwiObjectName {
    param($Candidate)
    $object = Get-VwiObjectCore $Candidate
    if ($null -eq $object) { return '' }
    return Get-VwiText @(
        (Get-PropertyValue $object @(
            'databaseUniqueName','databaseName','dbName','name','objectName',
            'displayName','hostName','sourceName','vmName'
        ) '')
    )
}

function Get-VwiObjectId {
    param($Candidate)
    $object = Get-VwiObjectCore $Candidate
    if ($null -eq $object) { return '' }
    return Get-VwiText @(
        (Get-PropertyValue $object @(
            'id','objectId','databaseId','databaseUuid','entityId','uuid','globalId','vmId'
        ) '')
    )
}

function Get-VwiSourceId {
    param($Candidate)
    $object = Get-VwiObjectCore $Candidate
    if ($null -eq $object) { return '' }
    return Get-VwiText @(
        (Get-PropertyValue $object @('sourceId','parentId','rootNodeId') '')
    )
}

function Get-VwiObjectType {
    param($Candidate)
    $object = Get-VwiObjectCore $Candidate
    if ($null -eq $object) { return '' }
    return Get-VwiText @(
        (Get-PropertyValue $object @('objectType','type','entityType') '')
    )
}

function Get-VwiEnvironment {
    param($Candidate)
    $object = Get-VwiObjectCore $Candidate
    if ($null -eq $object) { return '' }
    return Get-VwiText @(
        (Get-PropertyValue $object @('environment','environmentType') '')
    )
}

function Get-VwiObjectKey {
    param($Candidate,[string]$Workload,[string]$ObjectNameOverride)

    $name = Get-VwiText @($ObjectNameOverride,(Get-VwiObjectName $Candidate))
    $objectId = Get-VwiObjectId $Candidate
    $sourceId = Get-VwiSourceId $Candidate

    if ([string]::IsNullOrWhiteSpace($name) -and [string]::IsNullOrWhiteSpace($objectId)) {
        return ''
    }

    if ($Workload -eq 'Oracle' -and -not [string]::IsNullOrWhiteSpace($name)) {
        return ("oracle|$name").ToLowerInvariant()
    }

    return ('{0}|{1}|{2}|{3}' -f `
        $Workload,$sourceId,(Get-VwiText @($objectId,$name)),$name
    ).ToLowerInvariant()
}

function New-VwiFoundObject {
    param(
        $Candidate,
        [string]$RunType,
        [int64]$RunEndTimeUsecs,
        [string]$DiscoverySource,
        [string]$ObjectTypeOverride,
        [string]$EnvironmentOverride,
        [string]$ObjectNameOverride
    )

    return [pscustomobject]@{
        Candidate=$Candidate
        RunType=$RunType
        RunEndTimeUsecs=$RunEndTimeUsecs
        DiscoverySource=$DiscoverySource
        ObjectTypeOverride=$ObjectTypeOverride
        EnvironmentOverride=$EnvironmentOverride
        ObjectNameOverride=$ObjectNameOverride
    }
}

function Get-VwiProtectionGroups {
    param(
        [string]$Environment,
        [ValidateSet('Active','Paused')][string]$State,
        [hashtable]$Config,
        [hashtable]$Headers
    )

    $groups = @()
    $cookie = ''
    $seenCookies = @{}
    $basePath = [string](Get-PropertyValue $Config.Endpoints @('ProtectionGroups') '/v2/data-protect/protection-groups')

    do {
        $query = '?environments={0}&isDeleted=false&maxResultCount=1000' -f `
            [uri]::EscapeDataString($Environment)
        if ($State -eq 'Active') {
            $query += '&isPaused=false&isActive=true&includeLastRunInfo=true'
        }
        else {
            $query += '&isPaused=true&includeLastRunInfo=false'
        }
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $query += '&paginationCookie={0}' -f [uri]::EscapeDataString($cookie)
        }

        $response = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl `
            -Path ($basePath+$query) -Headers $Headers `
            -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls

        foreach ($group in @(Get-Collection $response @('protectionGroups','items','data'))) {
            if ($null -ne $group) { $groups += $group }
        }

        $cookie = Get-VwiText @(
            (Get-PropertyValue $response @('paginationCookie') '')
        )
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            if ($seenCookies.ContainsKey($cookie)) {
                throw "Repeated protection-group pagination cookie for $Environment/$State."
            }
            $seenCookies[$cookie] = $true
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($groups)
}

function Get-VwiLatestRunObjectsPerType {
    param([string]$ProtectionGroupId,[hashtable]$Config,[hashtable]$Headers)

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) { return @() }

    $template = [string](Get-PropertyValue $Config.Endpoints @('PgRunsTemplate') `
        '/v2/data-protect/protection-groups/{0}/runs')
    $path = $template -f [uri]::EscapeDataString($ProtectionGroupId)
    $path += '?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true'

    $response = Invoke-HeliosGet -BaseUrl $Config.HeliosBaseUrl -Path $path `
        -Headers $Headers -TimeoutSec $Config.RequestTimeoutSec -VerifyTls $Config.VerifyTls
    $runs = @(Get-Collection $response @('runs','items','data'))
    if ($runs.Count -eq 0) { return @() }

    $records = @()
    foreach ($run in $runs) {
        $localInfos = @(Get-Collection (
            Get-PropertyValue $run @('localBackupInfo','localSnapshotInfo') @()
        ))

        foreach ($localInfo in $localInfos) {
            $runType = Get-VwiText @(
                (Get-PropertyValue $localInfo @('runType') '')
            )
            if ([string]::IsNullOrWhiteSpace($runType)) { $runType = 'Unknown' }
            $records += [pscustomobject]@{
                Run=$run
                RunType=$runType
                EndTimeUsecs=[int64](Get-PropertyValue $localInfo @('endTimeUsecs','startTimeUsecs') 0)
            }
        }

        if ($localInfos.Count -eq 0) {
            $runType = Get-VwiText @(
                (Get-PropertyValue $run @('runType') ''),'Unknown'
            )
            $records += [pscustomobject]@{
                Run=$run
                RunType=$runType
                EndTimeUsecs=[int64](Get-PropertyValue $run @('endTimeUsecs','startTimeUsecs') 0)
            }
        }
    }

    $objects = @()
    foreach ($group in @($records | Group-Object RunType)) {
        $latest = $group.Group | Sort-Object EndTimeUsecs -Descending | Select-Object -First 1
        if ($null -eq $latest) { continue }
        foreach ($candidate in @(Get-Collection (
            Get-PropertyValue $latest.Run @('objects','objectDetails') @()
        ))) {
            if ($null -eq $candidate) { continue }
            $objects += New-VwiFoundObject -Candidate $candidate -RunType $latest.RunType `
                -RunEndTimeUsecs $latest.EndTimeUsecs `
                -DiscoverySource 'Latest backup run per run type'
        }
    }

    return @($objects)
}

function Get-VwiObjectsForWorkload {
    param($Definition,$ProtectionGroup,[hashtable]$Config,[hashtable]$Headers)

    if ($Definition.Kind -eq 'Physical') {
        $physical = Get-PropertyValue $ProtectionGroup @('physicalParams') $null
        if ($null -eq $physical) { return @() }
        $protectionType = Get-VwiText @(
            (Get-PropertyValue $physical @('protectionType') '')
        )
        if ($protectionType -ieq 'kVolume') {
            $parameters = Get-PropertyValue $physical @('volumeProtectionTypeParams') $null
        }
        else {
            $parameters = Get-PropertyValue $physical @('fileProtectionTypeParams') $null
        }

        $result = @()
        foreach ($candidate in @(Get-Collection (
            Get-PropertyValue $parameters @('objects') @()
        ))) {
            if ($null -eq $candidate) { continue }
            $result += New-VwiFoundObject -Candidate $candidate -RunType 'PG configuration' `
                -RunEndTimeUsecs 0 -DiscoverySource "Physical $protectionType configuration" `
                -ObjectTypeOverride 'kHost' -EnvironmentOverride 'kPhysical'
        }
        return @($result)
    }

    $result = @()
    foreach ($found in @(Get-VwiLatestRunObjectsPerType `
        -ProtectionGroupId (Get-VwiPgId $ProtectionGroup) -Config $Config -Headers $Headers)) {

        $type = Get-VwiObjectType $found.Candidate
        $environment = Get-VwiEnvironment $found.Candidate
        $include = $false

        switch ($Definition.Kind) {
            'HyperV' {
                $include = ($type -ieq 'kVirtualMachine' -and `
                    ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kHyperV'))
            }
            'Nutanix' {
                $include = ($type -ieq 'kVirtualMachine' -and `
                    ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kAcropolis'))
            }
            'NAS' {
                $include = ($environment -ieq 'kGenericNas' -or $environment -ieq 'kIsilon')
            }
            'SQL' {
                # SQL hosts are deliberately excluded. Only database objects are counted.
                $include = ($type -ieq 'kDatabase' -and `
                    ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kSQL'))
            }
            'Oracle' {
                $include = ($type -ieq 'kDatabase' -and `
                    ([string]::IsNullOrWhiteSpace($environment) -or $environment -ieq 'kOracle'))
            }
        }

        if ($include) { $result += $found }
    }

    if ($Definition.Kind -eq 'Oracle' -and $result.Count -eq 0) {
        $oracleParams = Get-PropertyValue $ProtectionGroup @('oracleParams') $null
        foreach ($oracleObject in @(Get-Collection (
            Get-PropertyValue $oracleParams @('objects') @()
        ))) {
            $dbParams = Get-PropertyValue $oracleObject @('dbParams') $null
            foreach ($channel in @(Get-Collection (
                Get-PropertyValue $dbParams @('dbChannels') @()
            ))) {
                $databaseUniqueName = Get-VwiText @(
                    (Get-PropertyValue $channel @('databaseUniqueName') '')
                )
                if ([string]::IsNullOrWhiteSpace($databaseUniqueName)) { continue }
                $result += New-VwiFoundObject -Candidate $channel `
                    -RunType 'PG configuration fallback' -RunEndTimeUsecs 0 `
                    -DiscoverySource 'Oracle dbChannels configuration fallback' `
                    -ObjectTypeOverride 'kDatabase' -EnvironmentOverride 'kOracle' `
                    -ObjectNameOverride $databaseUniqueName
            }
        }
    }

    return @($result)
}

function New-VwiDetailRow {
    param($Found,$Cluster,$Definition,$ProtectionGroup,[string]$CountKey)

    $candidate = $Found.Candidate
    return [pscustomobject][ordered]@{
        cluster=[string](Get-PropertyValue $Cluster @('clusterName','name','displayName') '')
        clusterId=[string](Get-PropertyValue $Cluster @('clusterId','id') '')
        workload=$Definition.Label
        protectionGroup=Get-VwiPgName $ProtectionGroup
        protectionGroupId=Get-VwiPgId $ProtectionGroup
        objectName=Get-VwiText @($Found.ObjectNameOverride,(Get-VwiObjectName $candidate))
        objectType=Get-VwiText @($Found.ObjectTypeOverride,(Get-VwiObjectType $candidate))
        environment=Get-VwiText @($Found.EnvironmentOverride,(Get-VwiEnvironment $candidate))
        objectId=Get-VwiObjectId $candidate
        sourceId=Get-VwiSourceId $candidate
        runType=$Found.RunType
        runEndTimeUtc=Convert-UsecsToUtc $Found.RunEndTimeUsecs
        discoverySource=$Found.DiscoverySource
        countKey=$CountKey
        occurrencesCollapsed=1
    }
}

function Get-ValidatedWorkloadInventory {
    [CmdletBinding()]
    param($Cluster,[hashtable]$Config,[hashtable]$Headers)

    $clusterId = [string](Get-PropertyValue $Cluster @('clusterId','id') '')
    $inventory = [ordered]@{}
    $errors = New-Object System.Collections.Generic.List[string]
    $allActiveIds = @{}
    $allPausedIds = @{}
    $allActiveComplete = $true
    $allPausedComplete = $true

    foreach ($definition in @(Get-VwiDefinitions)) {
        $activeGroups = @()
        $pausedGroups = @()
        $activeComplete = $true
        $pausedComplete = $true
        $protectedComplete = $true

        foreach ($environment in $definition.Environments) {
            try {
                $activeGroups += @(Get-VwiProtectionGroups -Environment $environment `
                    -State Active -Config $Config -Headers $Headers)
            }
            catch {
                $activeComplete = $false
                $protectedComplete = $false
                $allActiveComplete = $false
                $errors.Add("$($definition.Label) active PG GET ($environment): $($_.Exception.Message)") | Out-Null
            }

            try {
                $pausedGroups += @(Get-VwiProtectionGroups -Environment $environment `
                    -State Paused -Config $Config -Headers $Headers)
            }
            catch {
                $pausedComplete = $false
                $allPausedComplete = $false
                $errors.Add("$($definition.Label) paused PG GET ($environment): $($_.Exception.Message)") | Out-Null
            }
        }

        $seenActive = @{}
        $seenPaused = @{}
        $protectedIndex = @{}
        $detailIndex = @{}

        foreach ($group in $activeGroups) {
            $pgId = Get-VwiPgId $group
            if ([string]::IsNullOrWhiteSpace($pgId)) { continue }
            $pgKey = $pgId.ToLowerInvariant()
            if ($seenActive.ContainsKey($pgKey)) { continue }
            $seenActive[$pgKey] = $true
            $allActiveIds[$pgKey] = $true

            try {
                foreach ($found in @(Get-VwiObjectsForWorkload -Definition $definition `
                    -ProtectionGroup $group -Config $Config -Headers $Headers)) {

                    $objectKey = Get-VwiObjectKey -Candidate $found.Candidate `
                        -Workload $definition.Label -ObjectNameOverride $found.ObjectNameOverride
                    if ([string]::IsNullOrWhiteSpace($objectKey)) { continue }
                    $countKey = ("$clusterId|$objectKey").ToLowerInvariant()
                    $protectedIndex[$countKey] = $true
                    $row = New-VwiDetailRow -Found $found -Cluster $Cluster `
                        -Definition $definition -ProtectionGroup $group -CountKey $countKey

                    if (-not $detailIndex.ContainsKey($countKey)) {
                        $detailIndex[$countKey] = $row
                    }
                    else {
                        $existing = $detailIndex[$countKey]
                        $existing.protectionGroup = Join-VwiText $existing.protectionGroup $row.protectionGroup
                        $existing.protectionGroupId = Join-VwiText $existing.protectionGroupId $row.protectionGroupId
                        $existing.runType = Join-VwiText $existing.runType $row.runType
                        $existing.discoverySource = Join-VwiText $existing.discoverySource $row.discoverySource
                        $existing.occurrencesCollapsed = [int]$existing.occurrencesCollapsed + 1
                        if ($row.runEndTimeUtc -and `
                            ([string]::IsNullOrWhiteSpace($existing.runEndTimeUtc) -or `
                            [datetime]$row.runEndTimeUtc -gt [datetime]$existing.runEndTimeUtc)) {
                            $existing.runEndTimeUtc = $row.runEndTimeUtc
                        }
                    }
                }
            }
            catch {
                $protectedComplete = $false
                $errors.Add("$($definition.Label) protected objects $(Get-VwiPgName $group): $($_.Exception.Message)") | Out-Null
            }
        }

        foreach ($group in $pausedGroups) {
            $pgId = Get-VwiPgId $group
            if ([string]::IsNullOrWhiteSpace($pgId)) { continue }
            $pgKey = $pgId.ToLowerInvariant()
            if ($seenPaused.ContainsKey($pgKey)) { continue }
            $seenPaused[$pgKey] = $true
            $allPausedIds[$pgKey] = $true
        }

        $inventory[$definition.Key] = [ordered]@{
            label=$definition.Label
            activeProtectionGroups=if($activeComplete){$seenActive.Count}else{$null}
            pausedProtectionGroups=if($pausedComplete){$seenPaused.Count}else{$null}
            protectedObjectCount=if($protectedComplete){$protectedIndex.Count}else{$null}
            protectedObjects=@($detailIndex.Values | Sort-Object protectionGroup,objectName)
            activeComplete=$activeComplete
            pausedComplete=$pausedComplete
            protectedComplete=$protectedComplete
        }
    }

    return [ordered]@{
        inventory=$inventory
        activeProtectionGroups=if($allActiveComplete){$allActiveIds.Count}else{$null}
        pausedProtectionGroups=if($allPausedComplete){$allPausedIds.Count}else{$null}
        errors=@($errors)
    }
}

function Add-ValidatedInventoryToSnapshot {
    [CmdletBinding()]
    param($Snapshot,$Cluster,[hashtable]$Config,[hashtable]$Headers)

    try {
        $validated = Get-ValidatedWorkloadInventory -Cluster $Cluster -Config $Config -Headers $Headers
        $snapshotInventory = Get-PropertyValue $Snapshot @('inventory') $null

        foreach ($definition in @(Get-VwiDefinitions)) {
            $target = Get-PropertyValue $snapshotInventory @($definition.Key) $null
            $source = Get-PropertyValue $validated.inventory @($definition.Key) $null
            if ($null -eq $target -or $null -eq $source) { continue }

            Set-VwiProperty $target 'label' $definition.Label
            Set-VwiProperty $target 'activeProtectionGroups' (
                Get-PropertyValue $source @('activeProtectionGroups') $null
            )
            Set-VwiProperty $target 'pausedProtectionGroups' (
                Get-PropertyValue $source @('pausedProtectionGroups') $null
            )
            Set-VwiProperty $target 'protectedObjects' @(
                Get-PropertyValue $source @('protectedObjects') @()
            )
            Set-VwiProperty $target 'inventorySource' 'Validated workload-object inventory'
            Set-VwiProperty $target 'inventoryComplete' (
                [bool](Get-PropertyValue $source @('protectedComplete') $false)
            )

            $protectedCount = Get-PropertyValue $source @('protectedObjectCount') $null
            if ($null -ne $protectedCount) {
                Set-VwiProperty $target 'total' ([int]$protectedCount)
                $failed = [int](Get-PropertyValue $target @('failed') 0)
                $cancelled = [int](Get-PropertyValue $target @('cancelled') 0)
                Set-VwiProperty $target 'successful' ([math]::Max(0,[int]$protectedCount-$failed-$cancelled))
            }
        }

        $protectionGroups = Get-PropertyValue $Snapshot @('protectionGroups') $null
        $activePg = Get-PropertyValue $validated @('activeProtectionGroups') $null
        $pausedPg = Get-PropertyValue $validated @('pausedProtectionGroups') $null
        if ($null -ne $protectionGroups) {
            if ($null -ne $activePg) { Set-VwiProperty $protectionGroups 'active' ([int]$activePg) }
            if ($null -ne $pausedPg) { Set-VwiProperty $protectionGroups 'paused' ([int]$pausedPg) }
            if ($null -ne $activePg -and $null -ne $pausedPg) {
                Set-VwiProperty $protectionGroups 'total' ([int]$activePg+[int]$pausedPg)
            }
        }

        $validationErrors = @(Get-PropertyValue $validated @('errors') @())
        if ($validationErrors.Count -gt 0) {
            $existingErrors = @(Get-PropertyValue $Snapshot @('collectionErrors') @())
            Set-VwiProperty $Snapshot 'collectionErrors' @($existingErrors+$validationErrors)
            $availability = [string](Get-PropertyValue $Snapshot @('availability') '')
            if ($availability -ne 'Unavailable') { Set-VwiProperty $Snapshot 'health' 'Warning' }
        }
    }
    catch {
        $existingErrors = @(Get-PropertyValue $Snapshot @('collectionErrors') @())
        Set-VwiProperty $Snapshot 'collectionErrors' @(
            $existingErrors+@("Validated inventory: $($_.Exception.Message)")
        )
        $availability = [string](Get-PropertyValue $Snapshot @('availability') '')
        if ($availability -ne 'Unavailable') { Set-VwiProperty $Snapshot 'health' 'Warning' }
    }

    return $Snapshot
}
