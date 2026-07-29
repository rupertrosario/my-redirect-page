# Cohesity SQL Inventory - Test 02
# One row per SQL database. GET-only.

[CmdletBinding()]
param(
    [string]$ClusterName,
    [string]$ProtectionGroupName,
    [int]$MaxProtectionGroups = 1,
    [int]$RunHistoryCount = 20,
    [switch]$ScanAllClusters,
    [switch]$IncludeHistoricalObjects,
    [switch]$SaveRawJson,
    [string]$OutputDirectory = "X:\PowerShell\Data\Cohesity\SQLInventory\Tests"
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if ($MaxProtectionGroups -lt 1) { throw "MaxProtectionGroups must be at least 1." }
if ($RunHistoryCount -lt 1) { throw "RunHistoryCount must be at least 1." }
if (-not (Get-Command Out-GridView -ErrorAction SilentlyContinue)) {
    throw "Out-GridView is required but is not available."
}
if (-not (Test-Path $helperPath -PathType Leaf)) { throw "API key helper not found: $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) { throw "Encrypted API key not found: $encryptedApiKeyPath" }
if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES helper returned an empty API key." }

. "$PSScriptRoot\SQLInventory.Helpers.ps1"

$commonHeaders = @{ accept = "application/json"; apiKey = $apiKey }
$clusterResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
$clusters = @($clusterResponse.cohesityClusters | Sort-Object { Get-ClusterNameValue $_ })
if ($clusters.Count -eq 0) { throw "No clusters were returned by Cohesity Helios." }

if (-not [string]::IsNullOrWhiteSpace($ClusterName)) {
    $clusters = @($clusters | Where-Object { (Get-ClusterNameValue $_) -like $ClusterName })
    if ($clusters.Count -eq 0) { throw "No cluster matched '$ClusterName'." }
}

$rows = New-Object System.Collections.ArrayList
$issues = New-Object System.Collections.ArrayList
$clustersChecked = 0
$pgsChecked = 0
$processedAnyPg = $false

foreach ($cluster in $clusters) {
    $clusterNameResolved = Get-ClusterNameValue $cluster
    $clusterId = Get-ClusterIdValue $cluster
    if ([string]::IsNullOrWhiteSpace($clusterId)) {
        [void]$issues.Add([pscustomobject]@{ Cluster=$clusterNameResolved; ProtectionGroup=""; Stage="Cluster discovery"; Issue="Cluster ID was not returned" })
        continue
    }

    $clustersChecked++
    $headers = @{ accept="application/json"; apiKey=$apiKey; accessClusterId=$clusterId }
    $objectCache = @{}
    $policyCache = @{}

    try {
        $pgResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/data-protect/protection-groups?environments=kSQL&isDeleted=false&isActive=true" -Headers $headers
        $pgs = @($pgResponse.protectionGroups)
        if (-not [string]::IsNullOrWhiteSpace($ProtectionGroupName)) {
            $pgs = @($pgs | Where-Object { [string](Get-PropertyValue $_ @("name") "") -like $ProtectionGroupName })
        }
        else { $pgs = @($pgs | Select-Object -First $MaxProtectionGroups) }
        if ($pgs.Count -eq 0) { continue }

        foreach ($pg in $pgs) {
            $processedAnyPg = $true
            $pgsChecked++
            $pgName = [string](Get-PropertyValue $pg @("name") "N/A")
            $pgId = [string](Get-PropertyValue $pg @("id") "")
            $policyId = [string](Get-PropertyValue $pg @("policyId") "")
            $mssql = Get-PropertyValue $pg @("mssqlParams") $null
            $active = Get-ActiveSqlParameters $mssql
            $advanced = Get-PropertyValue $active @("advancedSettings") $null

            $protectionType = [string](Get-PropertyValue $mssql @("protectionType") "")
            $aagPreference = [string](Get-PropertyValue $active @("aagBackupPreferenceType") "")
            $backupSystemDbs = Convert-ToDisplayValue (Get-PropertyValue $active @("backupSystemDbs") $null)
            $userDbPreference = [string](Get-PropertyValue $active @("userDbBackupPreferenceType") "")
            $logChainBreak = Convert-ToDisplayValue (Get-PropertyValue $advanced @("logChainBreakAutoTriggerOobIncrBackup") $null)

            $alertPolicy = Get-PropertyValue $pg @("alertPolicy") $null
            $alertEmails = @(@(Get-PropertyValue $alertPolicy @("alertTargets") @()) | ForEach-Object { [string](Get-PropertyValue $_ @("emailAddress") "") } | Where-Object { $_ } | Select-Object -Unique) -join "; "
            $alertStatuses = @(@(Get-PropertyValue $alertPolicy @("backupRunStatus") @()) | ForEach-Object { [string]$_ } | Where-Object { $_ } | Select-Object -Unique) -join "; "
            $exclusionRules = @(Get-ExclusionRules $mssql)
            $matchedExclusions = New-Object System.Collections.ArrayList

            $policy = Get-CohesityPolicyById -PolicyId $policyId -Headers $headers -Cache $policyCache -Issues $issues -Cluster $clusterNameResolved -ProtectionGroup $pgName
            $policyName = [string](Get-PropertyValue $policy @("name") $policyId)
            $policySettings = Convert-PolicyToText $policy

            if ([string]::IsNullOrWhiteSpace($pgId)) {
                [void]$issues.Add([pscustomobject]@{ Cluster=$clusterNameResolved; ProtectionGroup=$pgName; Stage="Runs"; Issue="Protection group ID was not returned" })
                continue
            }

            try {
                $runsResponse = Invoke-CohesityGet -Uri "$baseUrl/v2/data-protect/protection-groups/$([uri]::EscapeDataString($pgId))/runs?numRuns=$RunHistoryCount&includeObjectDetails=true" -Headers $headers
            }
            catch {
                [void]$issues.Add([pscustomobject]@{ Cluster=$clusterNameResolved; ProtectionGroup=$pgName; Stage="Runs"; Issue=$_.Exception.Message })
                continue
            }

            if ($SaveRawJson) {
                $prefix = "$(Get-SafeFileName $clusterNameResolved)-$(Get-SafeFileName $pgName)"
                $pg | ConvertTo-Json -Depth 100 | Set-Content (Join-Path $OutputDirectory "$prefix-PG.json") -Encoding UTF8
                $runsResponse | ConvertTo-Json -Depth 100 | Set-Content (Join-Path $OutputDirectory "$prefix-Runs.json") -Encoding UTF8
                if ($null -ne $policy) { $policy | ConvertTo-Json -Depth 100 | Set-Content (Join-Path $OutputDirectory "$prefix-Policy.json") -Encoding UTF8 }
            }

            $runs = @(@($runsResponse.runs) | Sort-Object { $ri = Get-RunInformation $_; if ($null -ne $ri.EndUsecs) { [decimal]$ri.EndUsecs } else { 0 } } -Descending)
            if ($runs.Count -eq 0) {
                [void]$issues.Add([pscustomobject]@{ Cluster=$clusterNameResolved; ProtectionGroup=$pgName; Stage="Runs"; Issue="No runs were returned" })
                continue
            }

            $history = @{}
            for ($runIndex = 0; $runIndex -lt $runs.Count; $runIndex++) {
                $run = $runs[$runIndex]
                $runInfo = Get-RunInformation $run
                $category = Get-BackupCategory $runInfo.RunType
                foreach ($runObject in @(Get-PropertyValue $run @("objects") @())) {
                    $summary = Get-PropertyValue $runObject @("object") $null
                    if (-not (Test-IsSqlDatabaseObject $summary)) { continue }
                    $objectName = [string](Get-PropertyValue $summary @("name") "")
                    $sourceName = [string](Get-PropertyValue $summary @("sourceName") "")
                    $objectId = [string](Get-PropertyValue $summary @("id") "")
                    $key = ("$sourceName|$objectName").ToLowerInvariant()
                    if ([string]::IsNullOrWhiteSpace($objectName)) { $key = $objectId.ToLowerInvariant() }

                    if (-not $history.ContainsKey($key)) {
                        $history[$key] = [pscustomobject][ordered]@{
                            Summary=$summary; LatestRunObject=$runObject; SeenInLatestRun=($runIndex -eq 0)
                            LatestBackupType=$runInfo.RunType; LatestBackupStatus=(Get-ObjectBackupStatus $runObject $runInfo.Status)
                            LatestBackupStart=$runInfo.StartText; LatestBackupEnd=$runInfo.EndText
                            LatestFailureMessage=(Get-ObjectFailureMessage $runObject)
                            LastFullStatus=""; LastFullTime=""; LastIncrementalStatus=""; LastIncrementalTime=""; LastLogStatus=""; LastLogTime=""
                        }
                    }
                    elseif ($runIndex -eq 0) { $history[$key].SeenInLatestRun = $true }

                    $entry = $history[$key]
                    $objectStatus = Get-ObjectBackupStatus $runObject $runInfo.Status
                    if ($category -eq "Full" -and -not $entry.LastFullStatus) { $entry.LastFullStatus=$objectStatus; $entry.LastFullTime=$runInfo.EndText }
                    elseif ($category -eq "Incremental" -and -not $entry.LastIncrementalStatus) { $entry.LastIncrementalStatus=$objectStatus; $entry.LastIncrementalTime=$runInfo.EndText }
                    elseif ($category -eq "Log" -and -not $entry.LastLogStatus) { $entry.LastLogStatus=$objectStatus; $entry.LastLogTime=$runInfo.EndText }
                }
            }

            $pgRows = New-Object System.Collections.ArrayList
            foreach ($entry in @($history.Values)) {
                if (-not $IncludeHistoricalObjects -and -not $entry.SeenInLatestRun) { continue }
                $summary = $entry.Summary
                $objectId = [string](Get-PropertyValue $summary @("id") "")
                $detail = Get-CohesityObjectById -ObjectId $objectId -Headers $headers -Cache $objectCache -Issues $issues -Cluster $clusterNameResolved -ProtectionGroup $pgName

                $rawName = [string](Get-PropertyValue $detail @("name") (Get-PropertyValue $summary @("name") ""))
                $parts = @([regex]::Split($rawName, '[\\/]') | Where-Object { $_ })
                $database = if ($parts.Count -gt 0) { [string]$parts[-1] } else { $rawName }
                $instanceFromName = if ($parts.Count -gt 1) { [string]$parts[-2] } else { "" }

                $parentId = [string](Get-PropertyValue $detail @("sourceId") (Get-PropertyValue $summary @("sourceId") ""))
                $parent = Get-CohesityObjectById -ObjectId $parentId -Headers $headers -Cache $objectCache -Issues $issues -Cluster $clusterNameResolved -ProtectionGroup $pgName
                $parentName = [string](Get-PropertyValue $parent @("name") "")
                $parentType = [string](Get-PropertyValue $parent @("objectType") "")
                $grandParentId = [string](Get-PropertyValue $parent @("sourceId") "")
                $grandParent = Get-CohesityObjectById -ObjectId $grandParentId -Headers $headers -Cache $objectCache -Issues $issues -Cluster $clusterNameResolved -ProtectionGroup $pgName
                $grandParentName = [string](Get-PropertyValue $grandParent @("name") "")
                $runSourceName = [string](Get-PropertyValue $summary @("sourceName") "")

                $instanceName = $instanceFromName
                $serverName = ""
                if ($parentType -match '(?i)(Instance|AAG)') {
                    if ($parentName) { $instanceName = $parentName }
                    $serverName = $grandParentName
                }
                else { $serverName = $parentName }
                if (-not $serverName) { $serverName = $grandParentName }
                if (-not $serverName) { $serverName = $runSourceName }
                if (-not $instanceName) { $instanceName = $parentName }

                $serverInstance = if ($serverName -and $instanceName -and $serverName -ine $instanceName) { "$serverName\$instanceName" }
                                  elseif ($instanceName) { $instanceName } else { $serverName }

                $aagName = [string](Get-NestedValue $detail @("aagInfo.name", "mssqlParams.aagInfo.name", "sqlParams.aagInfo.name", "databaseParams.aagInfo.name") "")
                if (-not $aagName -and $parentType -match '(?i)AAG') { $aagName = $parentName }
                $replicaRole = [string](Get-NestedValue $detail @("aagInfo.replicaRole", "aagInfo.role", "mssqlParams.aagInfo.replicaRole", "sqlParams.aagInfo.replicaRole") "")

                $objectParameters = @(
                    Convert-SectionsToText $detail @("mssqlParams", "sqlParams", "mssqlObjectParams", "databaseParams", "aagInfo", "hostInfo", "sourceParams")
                    Convert-SectionsToText $entry.LatestRunObject @("mssqlParams", "sqlParams", "databaseParams", "aagInfo") "LatestRun."
                ) | Where-Object { $_ }
                $sourceSettings = @(
                    Convert-SectionsToText $parent @("mssqlParams", "sqlParams", "mssqlSourceParams", "hostInfo", "aagInfo", "sourceParams", "connectionParams") "Instance."
                    Convert-SectionsToText $grandParent @("mssqlParams", "sqlParams", "mssqlSourceParams", "hostInfo", "aagInfo", "sourceParams", "connectionParams") "Server."
                ) | Where-Object { $_ }

                $objectExclusions = New-Object System.Collections.ArrayList
                $candidates = @($database, $rawName, "$instanceName/$database", "$instanceName\$database", "$serverInstance/$database", "$serverInstance\$database")
                foreach ($rule in $exclusionRules) {
                    if (Test-ExclusionMatches $rule $candidates) {
                        [void]$objectExclusions.Add($rule.FilterString)
                        if ($matchedExclusions -notcontains $rule.FilterString) { [void]$matchedExclusions.Add($rule.FilterString) }
                    }
                }

                [void]$pgRows.Add([pscustomobject][ordered]@{
                    Cluster=$clusterNameResolved; ServerInstance=$serverInstance; Database=$database
                    AvailabilityGroup=$aagName; ReplicaRole=$replicaRole; ProtectionGroup=$pgName; PolicyName=$policyName
                    ProtectionType=$protectionType; AAGBackupPreference=$aagPreference; BackupSystemDBs=$backupSystemDbs
                    UserDBPreference=$userDbPreference; LogChainBreakOOBIncremental=$logChainBreak
                    AlertEmails=$alertEmails; AlertOnStatuses=$alertStatuses
                    ObjectExclusions=(@($objectExclusions | Select-Object -Unique) -join "; "); OtherObjectExclusions=""
                    LatestBackupType=$entry.LatestBackupType; LatestBackupStatus=$entry.LatestBackupStatus
                    LatestBackupStart=$entry.LatestBackupStart; LatestBackupEnd=$entry.LatestBackupEnd
                    LatestFailureMessage=$entry.LatestFailureMessage
                    LastFullStatus=$entry.LastFullStatus; LastFullTime=$entry.LastFullTime
                    LastIncrementalStatus=$entry.LastIncrementalStatus; LastIncrementalTime=$entry.LastIncrementalTime
                    LastLogStatus=$entry.LastLogStatus; LastLogTime=$entry.LastLogTime
                    SeenInLatestRun=$(if ($entry.SeenInLatestRun) { "Yes" } else { "No" })
                    SourceSettings=(@($sourceSettings | Select-Object -Unique) -join "; ")
                    ObjectParameters=(@($objectParameters | Select-Object -Unique) -join "; ")
                    PolicySettings=$policySettings; RawObjectName=$rawName
                })
            }

            $unmatched = @($exclusionRules | Where-Object { $matchedExclusions -notcontains $_.FilterString } | ForEach-Object { $_.FilterString } | Select-Object -Unique)
            if ($pgRows.Count -gt 0) {
                $sortedPgRows = @($pgRows | Sort-Object ServerInstance, Database)
                if ($unmatched.Count -gt 0) { $sortedPgRows[0].OtherObjectExclusions = $unmatched -join "; " }
                foreach ($row in $sortedPgRows) { [void]$rows.Add($row) }
            }
            else {
                $extra = if ($unmatched.Count -gt 0) { " Unmatched exclusions: $($unmatched -join '; ')" } else { "" }
                [void]$issues.Add([pscustomobject]@{ Cluster=$clusterNameResolved; ProtectionGroup=$pgName; Stage="Database inventory"; Issue="No SQL database objects were returned.$extra" })
            }
        }
    }
    catch {
        [void]$issues.Add([pscustomobject]@{ Cluster=$clusterNameResolved; ProtectionGroup=""; Stage="SQL protection groups"; Issue=$_.Exception.Message })
    }

    if ($processedAnyPg -and -not $ScanAllClusters) { break }
}

$rows = @($rows | Sort-Object Cluster, ServerInstance, Database, ProtectionGroup)
if ($rows.Count -eq 0) {
    if ($issues.Count -gt 0) { $issues | Format-Table -AutoSize -Wrap | Out-Host }
    throw "No SQL database inventory rows were collected."
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$csvPath = Join-Path $OutputDirectory "Test-02-SQLDatabaseInventory_$timestamp.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host "`nCohesity SQL database inventory" -ForegroundColor Cyan
Write-Host "Clusters checked: $clustersChecked"
Write-Host "Protection groups inspected: $pgsChecked"
Write-Host "Database rows collected: $($rows.Count)"
Write-Host "Run history checked per PG: $RunHistoryCount"
Write-Host "CSV: $csvPath" -ForegroundColor Green

if ($issues.Count -gt 0) {
    $issuesPath = Join-Path $OutputDirectory "Test-02-SQLDatabaseInventoryIssues_$timestamp.csv"
    $issues | Export-Csv -Path $issuesPath -NoTypeInformation -Encoding UTF8
    Write-Warning "$($issues.Count) issue(s) were recorded. Issues CSV: $issuesPath"
}

$rows | Out-GridView -Title "Cohesity SQL Database Inventory"
