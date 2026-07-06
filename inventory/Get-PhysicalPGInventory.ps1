# Cohesity Helios - Physical PG Inventory
# STRICTLY READ-ONLY / GET-only
# Output only:
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Summary_Latest.csv
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Object_Detail_Latest.csv

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$outDir     = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$baseUrl    = "https://helios.cohesity.com"

if (-not (Test-Path -Path $outDir -PathType Container)) {
    New-Item -Path $outDir -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

function New-Headers {
    param([string]$ClusterId)

    $h = @{
        accept = "application/json"
    }

    $h["apiKey"] = $apiKey

    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $h["accessClusterId"] = $ClusterId
    }

    return $h
}

function Get-Json {
    param(
        [string]$Uri,
        [hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing
    }
    else {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get
    }

    if (-not $resp -or [string]::IsNullOrWhiteSpace($resp.Content)) {
        return $null
    }

    return ($resp.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Flat {
    param($Value)

    if ($null -eq $Value) { return "" }

    $items = @()
    foreach ($v in @($Value)) {
        if ($null -ne $v -and "$v".Trim() -ne "") { $items += "$v" }
    }

    if ($items.Count -eq 0) { return "" }
    return (($items | Select-Object -Unique) -join ";")
}

function FirstValue {
    param($Values)

    foreach ($v in @($Values)) {
        foreach ($vv in @($v)) {
            if ($null -ne $vv -and "$vv".Trim() -ne "") { return $vv }
        }
    }

    return ""
}

function UsecsToET {
    param($Usecs)

    if ($null -eq $Usecs -or "$Usecs".Trim() -eq "" -or "$Usecs" -eq "0") { return "" }

    try {
        $epochUtc = [DateTime]::SpecifyKind([datetime]"1970-01-01 00:00:00", [DateTimeKind]::Utc)
        $dtUtc = $epochUtc.AddSeconds(([double]$Usecs / 1000000))
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        return ([TimeZoneInfo]::ConvertTimeFromUtc($dtUtc, $tz)).ToString("yyyy-MM-dd HH:mm:ss")
    }
    catch {
        return ""
    }
}

function Get-PGKey {
    param([string]$Cluster, [string]$PGName)
    return (("{0}|{1}" -f $Cluster, $PGName).Trim())
}

function Test-LooksLikeId {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) { return $false }

    $v = $Value.Trim()

    if ($v -match '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') { return $true }
    if ($v -match '^[a-fA-F0-9]{24,}$') { return $true }
    if ($v -match '^[0-9]+:[0-9]+:[0-9]+$') { return $true }

    return $false
}

function Get-PhysicalPGs {
    param([hashtable]$Headers)

    $all = @()
    $cookie = ""

    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=kPhysical&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri = "$uri&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-Json -Uri $uri -Headers $Headers

        if ($json.protectionGroups) {
            $all += @($json.protectionGroups | Where-Object { $_ })
        }

        $cookie = FirstValue @($json.paginationCookie)

        if ($json.isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) {
            break
        }
    } while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-PolicyMap {
    param([hashtable]$Headers)

    $map = @{}
    $uris = @(
        "$baseUrl/v2/data-protect/policies?maxResultCount=1000",
        "$baseUrl/v2/data-protect/policies"
    )

    foreach ($uri in $uris) {
        try {
            $json = Get-Json -Uri $uri -Headers $Headers
            $policies = @()

            if ($json.policies) { $policies = @($json.policies) }
            elseif ($json.policyList) { $policies = @($json.policyList) }
            elseif ($json.items) { $policies = @($json.items) }
            elseif ($json -is [array]) { $policies = @($json) }
            elseif ($json) { $policies = @($json) }

            foreach ($p in @($policies | Where-Object { $_ })) {
                $id = FirstValue @($p.id, $p.policyId)
                $name = FirstValue @($p.name, $p.policyName, $p.displayName)

                if (-not [string]::IsNullOrWhiteSpace($id) -and -not [string]::IsNullOrWhiteSpace($name)) {
                    $map[$id] = $name
                }
            }

            if ($map.Count -gt 0) { break }
        }
        catch {
            # Policy lookup is best effort. Inventory collection continues.
        }
    }

    return $map
}

function Resolve-PolicyName {
    param(
        $ProtectionGroup,
        [hashtable]$PolicyMap
    )

    $policyName = FirstValue @(
        $ProtectionGroup.policyInfo.name,
        $ProtectionGroup.policy.name,
        $ProtectionGroup.policyConfig.name,
        $ProtectionGroup.policyName
    )

    $policyId = FirstValue @(
        $ProtectionGroup.policyId,
        $ProtectionGroup.policyInfo.id,
        $ProtectionGroup.policy.id
    )

    if (-not [string]::IsNullOrWhiteSpace($policyId) -and $PolicyMap.ContainsKey($policyId)) {
        return $PolicyMap[$policyId]
    }

    if (-not [string]::IsNullOrWhiteSpace($policyName)) {
        if (-not (Test-LooksLikeId -Value $policyName)) {
            return $policyName
        }

        if ($PolicyMap.ContainsKey($policyName)) {
            return $PolicyMap[$policyName]
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($policyId) -and $PolicyMap.ContainsKey($policyId)) {
        return $PolicyMap[$policyId]
    }

    return "UNRESOLVED_POLICY_NAME"
}

# -------------------------------
# Cluster menu
# -------------------------------
$cluJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$json_clu = @($cluJson.cohesityClusters)

if (-not $json_clu -or $json_clu.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$clusters = $json_clu | ForEach-Object {
    $name = FirstValue @($_.name, $_.clusterName, $_.displayName)
    $cid  = FirstValue @($_.clusterId, $_.id)

    if ([string]::IsNullOrWhiteSpace($name)) { $name = "Unknown-$cid" }

    [PSCustomObject]@{
        ClusterName = $name
        ClusterId   = $cid
    }
} | Sort-Object ClusterName

$clusterMenu = for ($i = 0; $i -lt $clusters.Count; $i++) {
    [PSCustomObject]@{
        Index       = $i + 1
        ClusterName = $clusters[$i].ClusterName
        ClusterId   = $clusters[$i].ClusterId
    }
}

Write-Host ""
Write-Host "Available Helios Clusters (sorted):" -ForegroundColor Cyan
$clusterMenu | Format-Table -AutoSize
Write-Host ""
Write-Host "[0] All clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host "Select cluster: 0 for ALL, 1-$($clusterMenu.Count) for single, or X"

    if ($selection -match '^(x|X|q|Q)$') { return }

    $n = 0
    if (-not [int]::TryParse($selection, [ref]$n)) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($n -lt 0 -or $n -gt $clusterMenu.Count) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($n -eq 0) { $selectedClusters = @($clusterMenu) }
    else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $n }) }

    break
}

# -------------------------------
# Collect data
# -------------------------------
$summaryRows = @()
$detailRows = @()
$pgIndex = 0

foreach ($c in $selectedClusters) {
    $clusterName = $c.ClusterName
    $headers = New-Headers -ClusterId $c.ClusterId
    $policyMap = Get-PolicyMap -Headers $headers

    Write-Host "Collecting active Physical PGs from $clusterName ..." -ForegroundColor Yellow

    $pgs = Get-PhysicalPGs -Headers $headers

    foreach ($pg in @($pgs | Where-Object { $_ })) {
        $physical = $pg.physicalParams
        if ($null -eq $physical) { continue }

        $pgIndex++
        $pgKey = Get-PGKey -Cluster $clusterName -PGName $pg.name
        $protectionType = FirstValue @($physical.protectionType)

        $fileParams = $physical.fileProtectionTypeParams
        $volumeParams = $physical.volumeProtectionTypeParams

        if ($protectionType -eq "kVolume") {
            $objects = @(As-Array $volumeParams.objects | Where-Object { $_ })
            $globalExcludePaths = ""
            $jobExcludedVssWriters = Flat $volumeParams.excludedVssWriters
        }
        else {
            $objects = @(As-Array $fileParams.objects | Where-Object { $_ })
            $globalExcludePaths = Flat $fileParams.globalExcludePaths
            $jobExcludedVssWriters = Flat $fileParams.excludedVssWriters
        }

        $lastRun = $pg.lastRun
        $localInfo = $lastRun.localBackupInfo
        if ($null -eq $localInfo) { $localInfo = $lastRun.localSnapshotInfo }

        $summaryRows += [PSCustomObject]@{
            PGKey                 = $pgKey
            PGIndex               = $pgIndex
            Cluster               = $clusterName
            PGName                = $pg.name
            PolicyName            = Resolve-PolicyName -ProtectionGroup $pg -PolicyMap $policyMap
            ProtectionType        = $protectionType
            PGObjectCount         = @($objects).Count
            GlobalExcludePaths    = $globalExcludePaths
            JobExcludedVssWriters = $jobExcludedVssWriters
            IsActive              = $pg.isActive
            IsPaused              = $pg.isPaused
            LastRunStatus         = FirstValue @($localInfo.status, $lastRun.status)
            LastRunStartET        = UsecsToET (FirstValue @($localInfo.startTimeUsecs, $lastRun.startTimeUsecs))
            LastRunEndET          = UsecsToET (FirstValue @($localInfo.endTimeUsecs, $lastRun.endTimeUsecs))
        }

        foreach ($obj in $objects) {
            $objectName = FirstValue @($obj.name, $obj.sourceName, $obj.hostName, $obj.displayName, $obj.id)
            $objectExcludedVssWriters = Flat $obj.excludedVssWriters
            $filePaths = @(As-Array $obj.filePaths | Where-Object { $_ })

            if ($protectionType -eq "kVolume") {
                $detailRows += [PSCustomObject]@{
                    PGKey                          = $pgKey
                    Cluster                        = $clusterName
                    PGName                         = $pg.name
                    ObjectName                     = $objectName
                    ObjectIncludedPaths            = Flat $obj.volumeGuids
                    ObjectExcludedPathsAll         = ""
                    IncludedPath                   = Flat $obj.volumeGuids
                    ExcludedPathsUnderIncludedPath = ""
                    SkipNestedVolumes              = ""
                    GlobalExcludePaths             = $globalExcludePaths
                    ObjectExcludedVssWriters       = $objectExcludedVssWriters
                    JobExcludedVssWriters          = $jobExcludedVssWriters
                }
                continue
            }

            $objectIncludedPaths = Flat @($filePaths | ForEach-Object { $_.includedPath })
            $allExcludedPaths = @()
            foreach ($fp in $filePaths) { $allExcludedPaths += @(As-Array $fp.excludedPaths) }
            $objectExcludedPathsAll = Flat $allExcludedPaths

            if ($filePaths.Count -eq 0) {
                $detailRows += [PSCustomObject]@{
                    PGKey                          = $pgKey
                    Cluster                        = $clusterName
                    PGName                         = $pg.name
                    ObjectName                     = $objectName
                    ObjectIncludedPaths            = ""
                    ObjectExcludedPathsAll         = ""
                    IncludedPath                   = ""
                    ExcludedPathsUnderIncludedPath = ""
                    SkipNestedVolumes              = ""
                    GlobalExcludePaths             = $globalExcludePaths
                    ObjectExcludedVssWriters       = $objectExcludedVssWriters
                    JobExcludedVssWriters          = $jobExcludedVssWriters
                }
            }
            else {
                foreach ($fp in $filePaths) {
                    $detailRows += [PSCustomObject]@{
                        PGKey                          = $pgKey
                        Cluster                        = $clusterName
                        PGName                         = $pg.name
                        ObjectName                     = $objectName
                        ObjectIncludedPaths            = $objectIncludedPaths
                        ObjectExcludedPathsAll         = $objectExcludedPathsAll
                        IncludedPath                   = $fp.includedPath
                        ExcludedPathsUnderIncludedPath = Flat $fp.excludedPaths
                        SkipNestedVolumes              = $fp.skipNestedVolumes
                        GlobalExcludePaths             = $globalExcludePaths
                        ObjectExcludedVssWriters       = $objectExcludedVssWriters
                        JobExcludedVssWriters          = $jobExcludedVssWriters
                    }
                }
            }
        }
    }
}

# -------------------------------
# Export two CSV files only
# -------------------------------
$summaryRows = $summaryRows | Sort-Object Cluster, PGName
$detailRows  = $detailRows  | Sort-Object Cluster, PGName, ObjectName, IncludedPath

$summaryCsv = Join-Path $outDir "Physical_PG_Summary_Latest.csv"
$detailCsv  = Join-Path $outDir "Physical_PG_Object_Detail_Latest.csv"

$summaryRows | Export-Csv -Path $summaryCsv -NoTypeInformation -Encoding utf8
$detailRows  | Export-Csv -Path $detailCsv -NoTypeInformation -Encoding utf8

Write-Host ""
Write-Host "CSV export complete." -ForegroundColor Green
Write-Host "Summary rows : $(@($summaryRows).Count)" -ForegroundColor Green
Write-Host "Detail rows  : $(@($detailRows).Count)" -ForegroundColor Green
Write-Host "Summary CSV  : $summaryCsv" -ForegroundColor Green
Write-Host "Detail CSV   : $detailCsv" -ForegroundColor Green
