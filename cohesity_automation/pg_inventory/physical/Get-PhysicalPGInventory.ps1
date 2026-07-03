# === COHESITY ACTIVE PHYSICAL PG INVENTORY ===
# Style: simple ora_details / ora_sccuss pattern
# Scope: GET-only

$ErrorActionPreference = 'Stop'

# ---------- CONFIG ----------
$helios = 'https://helios.cohesity.com'
$apiKeyFile = 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt'
$outputDir = 'X:\PowerShell\cohesity_automation\pg_inventory\physical'

# Put your cluster list here in this exact format: "clusterId clusterName"
# Keep the clusterId first, then one space, then the display name.
$clustersAvailable = @(
    # "1234567890123456789 ClusterName01"
    # "9876543210987654321 ClusterName02"
)

# ---------- BASIC HELPERS ----------
function Usecs-ToET($usecs) {
    if ($null -eq $usecs -or [string]::IsNullOrWhiteSpace([string]$usecs)) { return '' }
    try {
        $dto = [DateTimeOffset]::FromUnixTimeMilliseconds([int64]([double]$usecs / 1000))
        $tz  = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        return ([TimeZoneInfo]::ConvertTime($dto, $tz)).ToString('yyyy-MM-dd HH:mm:ss')
    }
    catch { return '' }
}

function Join-Strings($items) {
    if ($null -eq $items) { return '' }
    return (@($items) | Where-Object { $_ -and -not [string]::IsNullOrWhiteSpace([string]$_) } | Select-Object -Unique) -join '; '
}

function Get-PolicyName($policyId, $headers, $policyCache) {
    if ([string]::IsNullOrWhiteSpace([string]$policyId)) { return '' }
    if ($policyCache.ContainsKey($policyId)) { return $policyCache[$policyId] }

    try {
        $policyUri = "$helios/v2/data-protect/policies?ids=$policyId"
        $policyJson = (Invoke-WebRequest -Method Get -Uri $policyUri -Headers $headers).Content | ConvertFrom-Json
        $policyName = ($policyJson.policies | Where-Object { $_.id -eq $policyId } | Select-Object -ExpandProperty name -First 1)
        if ([string]::IsNullOrWhiteSpace([string]$policyName)) { $policyName = $policyId }
        $policyCache[$policyId] = $policyName
        return $policyName
    }
    catch {
        $policyCache[$policyId] = $policyId
        return $policyId
    }
}

function Select-Clusters($clustersAvailable) {
    if (-not $clustersAvailable -or $clustersAvailable.Count -eq 0) {
        throw 'No clusters configured. Add cluster entries to $clustersAvailable at the top of the script.'
    }

    Write-Host ''
    Write-Host 'Select cluster scope:' -ForegroundColor Cyan
    Write-Host '[0] ALL'
    for ($i = 0; $i -lt $clustersAvailable.Count; $i++) {
        $cid, $cname = $clustersAvailable[$i] -split ' ', 2
        Write-Host ('[{0}] {1}' -f ($i + 1), $cname)
    }
    Write-Host 'Examples: 1 or 1,3,5 or 2-4' -ForegroundColor DarkGray

    $choice = (Read-Host 'Enter selection').Trim()
    if ($choice -eq '0') { return @($clustersAvailable) }

    $selectedIndexes = @()
    foreach ($part in ($choice -split ',')) {
        $part = $part.Trim()
        if ($part -match '^(\d+)\s*-\s*(\d+)$') {
            $start = [int]$Matches[1]
            $end   = [int]$Matches[2]
            if ($start -lt 1 -or $end -gt $clustersAvailable.Count -or $start -gt $end) { throw "Invalid range: $part" }
            $selectedIndexes += $start..$end
        }
        elseif ($part -match '^\d+$') {
            $n = [int]$part
            if ($n -lt 1 -or $n -gt $clustersAvailable.Count) { throw "Invalid cluster number: $part" }
            $selectedIndexes += $n
        }
        else {
            throw "Invalid selection: $part"
        }
    }

    $selectedIndexes = $selectedIndexes | Select-Object -Unique | Sort-Object
    return @($selectedIndexes | ForEach-Object { $clustersAvailable[$_ - 1] })
}

# ---------- MAIN ----------
if (-not (Test-Path $apiKeyFile)) { throw "API key file not found: $apiKeyFile" }
$apikey = (Get-Content $apiKeyFile -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($apikey)) { throw "API key file is empty: $apiKeyFile" }

New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$selectedClusters = Select-Clusters $clustersAvailable
$summaryRows = @()
$detailRows = @()
$policyCache = @{}

foreach ($cluster in $selectedClusters) {
    $cluster_id, $cluster_name = $cluster -split ' ', 2

    $headers = @{
        apiKey          = $apikey
        accessClusterId = $cluster_id
        accept          = 'application/json'
    }

    Write-Host "Collecting active Physical PGs from $cluster_name ..." -ForegroundColor Yellow

    $uri = "$helios/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
    $json = (Invoke-WebRequest -Method Get -Uri $uri -Headers $headers).Content | ConvertFrom-Json

    foreach ($pg in ($json.protectionGroups | Where-Object { $_ })) {
        $physical = $pg.physicalParams
        $ptype = $physical.protectionType

        $fileParams = $physical.fileProtectionTypeParams
        $volParams  = $physical.volumeProtectionTypeParams

        if ($ptype -eq 'kVolume') {
            $objects = @($volParams.objects | Where-Object { $_ })
            $globalExcludes = ''
        }
        else {
            $objects = @($fileParams.objects | Where-Object { $_ })
            $globalExcludes = Join-Strings $fileParams.globalExcludePaths
        }

        $last = $pg.lastRun
        $local = $last.localBackupInfo
        if (-not $local) { $local = $last.localSnapshotInfo }

        $lastStatus = $local.status
        if ([string]::IsNullOrWhiteSpace([string]$lastStatus)) { $lastStatus = $last.status }

        $endUsecs = $local.endTimeUsecs
        if (-not $endUsecs) { $endUsecs = $last.endTimeUsecs }

        $policyName = $pg.policyName
        if ([string]::IsNullOrWhiteSpace([string]$policyName)) {
            $policyName = Get-PolicyName $pg.policyId $headers $policyCache
        }

        $summaryRows += [PSCustomObject]@{
            Cluster            = $cluster_name
            PGName             = $pg.name
            PolicyName         = $policyName
            ProtectionType     = $ptype
            PGObjectCount      = $objects.Count
            GlobalExcludePaths = $globalExcludes
            IsActive           = $pg.isActive
            IsPaused           = $pg.isPaused
            LastRunStatus      = $lastStatus
            LastRunEndET       = Usecs-ToET $endUsecs
        }

        # Detail rows are collected but exported only if you enable below.
        foreach ($obj in $objects) {
            if ($ptype -eq 'kVolume') {
                $detailRows += [PSCustomObject]@{
                    Cluster             = $cluster_name
                    PGName              = $pg.name
                    ProtectionType      = $ptype
                    ObjectName          = $obj.name
                    IncludedPath        = Join-Strings $obj.volumeGuids
                    ObjectExcludedPaths = ''
                    SkipNestedVolumes   = ''
                    GlobalExcludePaths  = ''
                }
            }
            else {
                foreach ($fp in ($obj.filePaths | Where-Object { $_ })) {
                    $detailRows += [PSCustomObject]@{
                        Cluster             = $cluster_name
                        PGName              = $pg.name
                        ProtectionType      = $ptype
                        ObjectName          = $obj.name
                        IncludedPath        = $fp.includedPath
                        ObjectExcludedPaths = Join-Strings $fp.excludedPaths
                        SkipNestedVolumes   = $fp.skipNestedVolumes
                        GlobalExcludePaths  = $globalExcludes
                    }
                }
            }
        }
    }
}

$summaryRows = $summaryRows | Sort-Object Cluster, PGName
$stamp = Get-Date -Format 'yyyy-MM-dd_HHmm'
$summaryCsv = Join-Path $outputDir "Physical_PG_Summary_$stamp.csv"
$summaryRows | Export-Csv $summaryCsv -NoTypeInformation

Write-Host ''
Write-Host "Summary rows: $($summaryRows.Count)" -ForegroundColor Green
Write-Host "Summary CSV : $summaryCsv" -ForegroundColor Green

$summaryRows | Format-Table Cluster, PGName, PolicyName, ProtectionType, PGObjectCount, GlobalExcludePaths, IsPaused, LastRunStatus, LastRunEndET -AutoSize

# GridView summary only. This avoids loading thousands of object/path rows in GridView.
$summaryRows | Out-GridView -Title 'Cohesity Physical PG Summary'

# Uncomment only when object/path export is needed.
# $detailCsv = Join-Path $outputDir "Physical_PG_Detail_$stamp.csv"
# $detailRows | Sort-Object Cluster, PGName, ObjectName, IncludedPath | Export-Csv $detailCsv -NoTypeInformation
# Write-Host "Detail CSV  : $detailCsv" -ForegroundColor Green
