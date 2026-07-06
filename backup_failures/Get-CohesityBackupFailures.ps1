# =====================================================================
# Cohesity Backup Failures - Multi-Cluster via Helios
# READ-ONLY / GET-only
#
# One PowerShell script.
# Uses encrypted API key file through ApiKeyAesHelper.ps1.
# Does not read apikey.txt.
# =====================================================================

$ErrorActionPreference = "Stop"

# -----------------------------
# Paths
# -----------------------------
$logDirectory = "X:\PowerShell\Data\Cohesity\BackupFailures"

if (-not (Test-Path -Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory | Out-Null
}

try {
    $files = Get-ChildItem -Path $logDirectory -File -ErrorAction Stop
    if ($files.Count -gt 50) {
        $toDelete = $files | Sort-Object CreationTime | Select-Object -First ($files.Count - 50)
        $toDelete | Remove-Item -Force -ErrorAction SilentlyContinue
    }

    $threshold = (Get-Date).AddDays(-30)
    Get-ChildItem -Path $logDirectory -File -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $threshold } |
        Remove-Item -Force -ErrorAction SilentlyContinue
} catch {}

# -----------------------------
# API key - AES encrypted file
# -----------------------------
$helperPath    = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $helperPath)) {
    throw "API key helper file not found at $helperPath"
}

if (-not (Test-Path $encryptedFile)) {
    throw "Encrypted API key file not found at $encryptedFile"
}

. $helperPath

$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedFile

if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "Encrypted API key read returned blank value."
}

$baseUrl = "https://helios.cohesity.com"

$commonHeaders = @{
    apiKey = $apiKey
    accept = "application/json"
}

# -----------------------------
# GET wrapper
# -----------------------------
function Invoke-HeliosGetJson {
    param(
        [Parameter(Mandatory)] [string] $Uri,
        [Parameter(Mandatory)] [hashtable] $Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
    } else {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get
    }

    if (-not $resp -or [string]::IsNullOrWhiteSpace($resp.Content)) {
        return $null
    }

    return ($resp.Content | ConvertFrom-Json)
}

# -----------------------------
# Helpers
# -----------------------------
function Write-Log {
    param(
        [string] $Message,
        [string] $Color = "Gray"
    )

    if ($script:VerboseMode) {
        Write-Host $Message -ForegroundColor $Color
    }
}

function Clean-Text {
    param($Value)

    if ($null -eq $Value) { return $null }

    if ($Value -is [System.Array]) {
        $Value = ($Value | ForEach-Object { [string]$_ }) -join " | "
    }

    $s = [string]$Value
    if ([string]::IsNullOrWhiteSpace($s)) { return $null }

    return (($s -replace "[\r\n]+", " ") -replace "\s+", " ").Trim()
}

function Get-FirstLocalBackupInfo {
    param($Run)

    if ($null -eq $Run -or $null -eq $Run.localBackupInfo) { return $null }
    return @($Run.localBackupInfo)[0]
}

function Convert-UsecsToUtc {
    param($Usecs)

    if ($null -eq $Usecs -or $Usecs -eq 0) { return $null }

    try {
        return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$Usecs / 1000)).UtcDateTime
    } catch {
        try { return [DateTimeOffset]::FromUnixTimeMilliseconds([int64]$Usecs).UtcDateTime }
        catch { return $null }
    }
}

function Convert-UsecsToEtText {
    param($Usecs)

    $utc = Convert-UsecsToUtc -Usecs $Usecs
    if ($null -eq $utc) { return $null }

    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        $et = [System.TimeZoneInfo]::ConvertTimeFromUtc($utc, $tz)
        return $et.ToString("yyyy-MM-dd HH:mm:ss")
    } catch {
        return $utc.ToString("yyyy-MM-dd HH:mm:ss")
    }
}

function Test-FailedStatus {
    param([string] $Status)

    if ([string]::IsNullOrWhiteSpace($Status)) { return $false }
    return ($Status -match "Failed|Failure|Error|Canceled")
}

function Test-SuccessStatus {
    param([string] $Status)

    if ([string]::IsNullOrWhiteSpace($Status)) { return $false }
    return ($Status -match "Succeeded|Success")
}

function Get-ObjectKey {
    param($ObjectRow)

    if ($null -eq $ObjectRow -or $null -eq $ObjectRow.object) { return $null }

    $obj = $ObjectRow.object

    if ($obj.id) { return [string]$obj.id }

    $sourceId = $null
    if ($obj.PSObject.Properties["sourceId"]) { $sourceId = [string]$obj.sourceId }

    return "$($obj.environment)|$($obj.objectType)|$($obj.name)|$sourceId"
}

function Test-ObjectHasFailedAttempts {
    param($ObjectRow)

    try {
        $fa = $ObjectRow.localSnapshotInfo.failedAttempts
        return ($fa -and @($fa).Count -gt 0)
    } catch {
        return $false
    }
}

function Test-ObjectSuccessForClear {
    param($ObjectRow)

    if ($null -eq $ObjectRow -or $null -eq $ObjectRow.localSnapshotInfo) { return $false }
    return (-not (Test-ObjectHasFailedAttempts -ObjectRow $ObjectRow))
}

function Get-FailedMessage {
    param(
        $ObjectRow,
        $Info
    )

    $messages = @()

    try {
        foreach ($attempt in @($ObjectRow.localSnapshotInfo.failedAttempts)) {
            $m = Clean-Text $attempt.message
            if ($m) { $messages += $m }

            $m = Clean-Text $attempt.errorMessage
            if ($m) { $messages += $m }
        }
    } catch {}

    if ($messages.Count -gt 0) {
        return (($messages | Select-Object -Unique) -join " | ")
    }

    $runMsg = Clean-Text $Info.messages
    if ($runMsg) { return $runMsg }

    return "Run/object marked failed but no detailed failedAttempts message was returned."
}

function Get-ClusterName {
    param($Cluster)

    $name = @($Cluster.name, $Cluster.clusterName, $Cluster.displayName) |
        Where-Object { $_ -and $_.Trim() } |
        Select-Object -First 1

    if ($name) { return $name }
    return "Unknown-$($Cluster.clusterId)"
}

function Find-HostNameInRunObjects {
    param(
        $Objects,
        $ObjectRow
    )

    if ($null -eq $ObjectRow -or $null -eq $ObjectRow.object) { return $null }

    $obj = $ObjectRow.object

    if ($obj.objectType -eq "kHost" -or $obj.environment -eq "kPhysical") {
        return $obj.name
    }

    $hostNames = @()

    foreach ($row in @($Objects)) {
        if ($null -eq $row.object) { continue }
        if ($row.object.objectType -eq "kHost" -or $row.object.environment -eq "kPhysical") {
            if ($row.object.name) { $hostNames += $row.object.name }
        }
    }

    if ($hostNames.Count -eq 1) { return $hostNames[0] }

    return $null
}

function Get-PgEnvironmentCode {
    param($ProtectionGroup)

    if ($ProtectionGroup.environment) { return $ProtectionGroup.environment }

    if ($ProtectionGroup.environmentTypes -and @($ProtectionGroup.environmentTypes).Count -gt 0) {
        return @($ProtectionGroup.environmentTypes)[0]
    }

    return $null
}

function Get-RemoteAdapterInfo {
    param($ProtectionGroup)

    $raHost = $null
    $raObject = $null

    try {
        $ra = $ProtectionGroup.remoteAdapterParams
        $hosts = @($ra.hosts)

        if ($hosts.Count -gt 0) {
            $firstHost = $hosts[0]
            $raHost = @($firstHost.hostname, $firstHost.hostName, $firstHost.name) |
                Where-Object { $_ } |
                Select-Object -First 1

            $scriptBlock = @($firstHost.incrementalBackupScript, $firstHost.backupScript, $ra.incrementalBackupScript, $ra.backupScript) |
                Where-Object { $_ } |
                Select-Object -First 1

            $args = @($scriptBlock.params, $scriptBlock.arguments, $scriptBlock.args) |
                Where-Object { $_ } |
                Select-Object -First 1

            if ($args -is [System.Array]) { $args = $args -join " " }

            if ($args -and ([string]$args) -match "-o\s+(\S+)") {
                $raObject = $Matches[1]
            }
        }
    } catch {}

    return [pscustomobject]@{
        Host = $raHost
        ObjectName = $raObject
    }
}

function New-FailureRow {
    param(
        [string] $Environment,
        [string] $Cluster,
        [string] $ProtectionGroup,
        [string] $RunType,
        [string] $Status,
        $StartUsecs,
        $EndUsecs,
        [string] $HostName,
        [string] $ObjectType,
        [string] $ObjectName,
        [string] $FailureType,
        [string] $FailedMessage
    )

    return [pscustomobject]@{
        Environment     = $Environment
        Cluster         = $Cluster
        ProtectionGroup = $ProtectionGroup
        RunType         = $RunType
        Status          = $Status
        StartTimeET     = Convert-UsecsToEtText -Usecs $StartUsecs
        EndTimeET       = Convert-UsecsToEtText -Usecs $EndUsecs
        Host            = $HostName
        ObjectType      = $ObjectType
        ObjectName      = $ObjectName
        FailureType     = $FailureType
        FailedMessage   = $FailedMessage
    }
}

# -----------------------------
# Environment map
# -----------------------------
$envMap = @(
    [pscustomobject]@{ Key = 2;  Label = "Oracle";        Filters = @("kOracle")        }
    [pscustomobject]@{ Key = 3;  Label = "SQL";           Filters = @("kSQL")           }
    [pscustomobject]@{ Key = 4;  Label = "Physical";      Filters = @("kPhysical")      }
    [pscustomobject]@{ Key = 5;  Label = "GenericNas";    Filters = @("kGenericNas")    }
    [pscustomobject]@{ Key = 6;  Label = "HyperV";        Filters = @("kHyperV")        }
    [pscustomobject]@{ Key = 7;  Label = "Acropolis";     Filters = @("kAcropolis")     }
    [pscustomobject]@{ Key = 8;  Label = "RemoteAdapter"; Filters = @("kRemoteAdapter") }
    [pscustomobject]@{ Key = 9;  Label = "Isilon";        Filters = @("kIsilon")        }
)

# -----------------------------
# Collector
# -----------------------------
function Collect-FailuresForScope {
    param(
        [Parameter(Mandatory)] $Scope,
        [Parameter(Mandatory)] $Clusters
    )

    $rows = @()

    foreach ($cluster in @($Clusters)) {
        $clusterName = Get-ClusterName -Cluster $cluster

        $headers = @{
            apiKey          = $apiKey
            accessClusterId = $cluster.clusterId
            accept          = "application/json"
        }

        Write-Log "`nProcessing cluster: $clusterName" "Cyan"

        $pgs = @()

        foreach ($filter in @($Scope.Filters)) {
            try {
                $pgUri = "$baseUrl/v2/data-protect/protection-groups?environments=$filter&isDeleted=false&isPaused=false&isActive=true"
                Write-Log "  PG query: $pgUri" "DarkGray"

                $pgJson = Invoke-HeliosGetJson -Uri $pgUri -Headers $headers

                if ($pgJson -and $pgJson.protectionGroups) {
                    $pgs += @($pgJson.protectionGroups)
                }
            } catch {
                Write-Log "  Failed to get PGs for $filter on $clusterName : $($_.Exception.Message)" "Yellow"
            }
        }

        $pgs = @($pgs | Sort-Object id -Unique)

        if (-not $pgs -or $pgs.Count -eq 0) {
            Write-Log "  No protection groups found." "Yellow"
            continue
        }

        Write-Log "  Protection groups found: $($pgs.Count)" "Green"

        foreach ($pg in @($pgs)) {
            $pgId = $pg.id
            $pgName = $pg.name
            $envCode = Get-PgEnvironmentCode -ProtectionGroup $pg
            $envLabel = $Scope.Label

            Write-Log "  Checking PG: $pgName" "Gray"

            try {
                $runsUri = "$baseUrl/v2/data-protect/protection-groups/$pgId/runs?numRuns=30&excludeNonRestorableRuns=false&includeObjectDetails=true"
                $jsonRuns = Invoke-HeliosGetJson -Uri $runsUri -Headers $headers
            } catch {
                Write-Log "    Failed to get runs for PG $pgName : $($_.Exception.Message)" "Yellow"
                continue
            }

            if (-not $jsonRuns -or -not $jsonRuns.runs) { continue }

            $runs = @($jsonRuns.runs)

            $runTypes = @(
                $runs |
                    ForEach-Object {
                        $info = Get-FirstLocalBackupInfo -Run $_
                        if ($info -and $info.runType) { $info.runType }
                    } |
                    Sort-Object -Unique
            )

            foreach ($runType in @($runTypes)) {
                $runsForType = @(
                    $runs |
                        Where-Object {
                            $info = Get-FirstLocalBackupInfo -Run $_
                            $info -and $info.runType -eq $runType
                        } |
                        Sort-Object {
                            $info = Get-FirstLocalBackupInfo -Run $_
                            if ($info) { [int64]$info.endTimeUsecs } else { 0 }
                        } -Descending
                )

                $clearedObjectKeys = New-Object 'System.Collections.Generic.HashSet[string]'
                $runLevelCleared = $false
                $runLevelFallbackAdded = $false

                foreach ($run in @($runsForType)) {
                    $info = Get-FirstLocalBackupInfo -Run $run
                    if (-not $info) { continue }

                    $status = [string]$info.status
                    $objects = @($run.objects)

                    if (Test-SuccessStatus -Status $status) {
                        if (-not $objects -or $objects.Count -eq 0) {
                            $runLevelCleared = $true
                        }

                        foreach ($ob in @($objects)) {
                            if (Test-ObjectSuccessForClear -ObjectRow $ob) {
                                $key = Get-ObjectKey -ObjectRow $ob
                                if ($key) { [void]$clearedObjectKeys.Add($key) }
                            }
                        }

                        continue
                    }

                    if (-not (Test-FailedStatus -Status $status)) {
                        continue
                    }

                    $capturedInThisRun = 0

                    # RemoteAdapter often needs PG-level row.
                    if ($envCode -eq "kRemoteAdapter") {
                        if (-not $runLevelCleared -and -not $runLevelFallbackAdded) {
                            $ra = Get-RemoteAdapterInfo -ProtectionGroup $pg
                            $objectName = $ra.ObjectName
                            if (-not $objectName) { $objectName = $ra.Host }
                            if (-not $objectName) { $objectName = $pgName }

                            $rows += New-FailureRow `
                                -Environment $envLabel `
                                -Cluster $clusterName `
                                -ProtectionGroup $pgName `
                                -RunType $runType `
                                -Status $status `
                                -StartUsecs $info.startTimeUsecs `
                                -EndUsecs $info.endTimeUsecs `
                                -HostName $ra.Host `
                                -ObjectType "RemoteAdapter" `
                                -ObjectName $objectName `
                                -FailureType "RunLevelRemoteAdapter" `
                                -FailedMessage (Clean-Text $info.messages)

                            $runLevelFallbackAdded = $true
                        }

                        continue
                    }

                    foreach ($ob in @($objects)) {
                        if (-not (Test-ObjectHasFailedAttempts -ObjectRow $ob)) { continue }

                        $key = Get-ObjectKey -ObjectRow $ob
                        if (-not $key) { continue }

                        if ($clearedObjectKeys.Contains($key)) { continue }

                        $obj = $ob.object
                        $hostName = Find-HostNameInRunObjects -Objects $objects -ObjectRow $ob
                        $failedMessage = Get-FailedMessage -ObjectRow $ob -Info $info

                        $rows += New-FailureRow `
                            -Environment $envLabel `
                            -Cluster $clusterName `
                            -ProtectionGroup $pgName `
                            -RunType $runType `
                            -Status $status `
                            -StartUsecs $info.startTimeUsecs `
                            -EndUsecs $info.endTimeUsecs `
                            -HostName $hostName `
                            -ObjectType $obj.objectType `
                            -ObjectName $obj.name `
                            -FailureType "ObjectFailedAttempt" `
                            -FailedMessage $failedMessage

                        [void]$clearedObjectKeys.Add($key)
                        $capturedInThisRun++
                    }

                    if ($capturedInThisRun -eq 0 -and -not $runLevelCleared -and -not $runLevelFallbackAdded) {
                        $rows += New-FailureRow `
                            -Environment $envLabel `
                            -Cluster $clusterName `
                            -ProtectionGroup $pgName `
                            -RunType $runType `
                            -Status $status `
                            -StartUsecs $info.startTimeUsecs `
                            -EndUsecs $info.endTimeUsecs `
                            -HostName $null `
                            -ObjectType "ProtectionGroup" `
                            -ObjectName $pgName `
                            -FailureType "RunLevelFailedNoObjectFailureCaptured" `
                            -FailedMessage (Clean-Text $info.messages)

                        $runLevelFallbackAdded = $true
                    }
                }
            }
        }
    }

    return $rows
}

# -----------------------------
# Get clusters
# -----------------------------
try {
    $clusterJson = Invoke-HeliosGetJson -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers $commonHeaders
    $clusters = @($clusterJson.cohesityClusters)
} catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

if (-not $clusters -or $clusters.Count -eq 0) {
    throw "No clusters returned from Helios."
}

# -----------------------------
# Menu
# -----------------------------
Write-Host "`n====================================================" -ForegroundColor Cyan
Write-Host "       COHESITY BACKUP FAILURES - MAIN MENU" -ForegroundColor White
Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "1.  All Environments"
Write-Host "2.  Oracle"
Write-Host "3.  SQL"
Write-Host "4.  Physical (File System)"
Write-Host "5.  NAS / GenericNas"
Write-Host "6.  Hyper-V"
Write-Host "7.  Acropolis (AHV)"
Write-Host "8.  Remote Adapter"
Write-Host "9.  Isilon"
Write-Host "10. Consolidated (All Environments) - Silent"
Write-Host "11. Exit"
Write-Host "----------------------------------------------------"

$choice = Read-Host "Enter your choice(s) e.g. 2,3,4,9"

if ($choice.Trim() -eq "11") {
    Write-Host "Exiting..."
    return
}

$script:VerboseMode = $false
$selectedScopes = @()

if ($choice.Trim() -eq "1" -or $choice.Trim() -eq "10") {
    $selectedScopes = @($envMap)
    $script:VerboseMode = $false
} else {
    Write-Host "`nSelect Mode:" -ForegroundColor Yellow
    Write-Host "1. Verbose"
    Write-Host "2. Silent"
    $modeChoice = Read-Host "Enter mode [1 or 2]"
    $script:VerboseMode = ($modeChoice -eq "1")

    $selectedKeys = @(
        $choice.Split(',') |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ -match '^\d+$' } |
            ForEach-Object { [int]$_ }
    )

    $selectedScopes = @($envMap | Where-Object { $selectedKeys -contains $_.Key })
}

if (-not $selectedScopes -or $selectedScopes.Count -eq 0) {
    throw "No valid menu option selected."
}

# -----------------------------
# Run collection
# -----------------------------
$allRows = @()

foreach ($scope in @($selectedScopes)) {
    Write-Host "`nCollecting $($scope.Label) failures..." -ForegroundColor Cyan
    $allRows += @(Collect-FailuresForScope -Scope $scope -Clusters $clusters)
}

$allRows = @(
    $allRows |
        Sort-Object Cluster, Environment, ProtectionGroup, ObjectName, EndTimeET -Descending
)

# -----------------------------
# Output
# -----------------------------
Write-Host "`n====================================================" -ForegroundColor Cyan
Write-Host "       COHESITY BACKUP FAILURES - RESULT" -ForegroundColor White
Write-Host "====================================================" -ForegroundColor Cyan
Write-Host ("Failures found: {0}" -f $allRows.Count) -ForegroundColor Yellow

if ($allRows.Count -gt 0) {
    $allRows |
        Select-Object Environment, Cluster, ProtectionGroup, RunType, Status, EndTimeET, Host, ObjectType, ObjectName, FailureType, FailedMessage |
        Format-Table -AutoSize

    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $csvPath = Join-Path $logDirectory "Cohesity_BackupFailures_$timestamp.csv"

    $allRows |
        Select-Object Environment, Cluster, ProtectionGroup, RunType, Status, StartTimeET, EndTimeET, Host, ObjectType, ObjectName, FailureType, FailedMessage |
        Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

    Write-Host "`nCSV created: $csvPath" -ForegroundColor Green
} else {
    Write-Host "No latest uncleared backup failures found in the last 30 runs per protection group." -ForegroundColor Green
}

Write-Host "`nDone." -ForegroundColor Green
