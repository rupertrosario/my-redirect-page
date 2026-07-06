# Cohesity Helios - Physical PG Inventory
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
# Output only:
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Summary_Latest.csv
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Physical_PG_Object_Detail_Latest.csv

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$outDir     = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$baseUrl    = "https://helios.cohesity.com"
$helperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path -Path $outDir -PathType Container)) {
    New-Item -Path $outDir -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path $helperPath)) {
    throw "API key helper not found at $helperPath"
}

if (-not (Test-Path $encryptedApiKeyPath)) {
    throw "Encrypted API key file not found at $encryptedApiKeyPath"
}

. $helperPath

$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath

if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function New-Headers {
    param([string]$ClusterId)

    $h = @{
        accept = "application/json"
        apiKey = $apiKey
    }

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
            if ($null -ne $vv -and "$vv".Trim() -ne "") { return "$vv" }
        }
    }

    return ""
}

function Get-PropValue {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) { return $null }
    if ($Object -is [string]) { return $null }

    foreach ($name in @($Names)) {
        foreach ($prop in @($Object.PSObject.Properties)) {
            if ($prop.Name -ieq $name) { return $prop.Value }
        }
    }

    return $null
}

function Get-NestedPropValue {
    param(
        $Object,
        [string]$Path
    )

    if ($null -eq $Object -or [string]::IsNullOrWhiteSpace($Path)) { return $null }

    $current = $Object
    foreach ($part in ($Path -split "\.")) {
        if ($null -eq $current) { return $null }
        if ($current -is [string]) { return $null }
        $current = Get-PropValue -Object $current -Names @($part)
    }

    return $current
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
        $groups = Get-PropValue -Object $json -Names @("protectionGroups")

        if ($groups) {
            $all += @(As-Array $groups | Where-Object { $_ })
        }

        $cookie = FirstValue @((Get-PropValue -Object $json -Names @("paginationCookie")))
        $isResponseTruncated = Get-PropValue -Object $json -Names @("isResponseTruncated")

        if ($isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) {
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
            $policyArray = Get-PropValue -Object $json -Names @("policies", "policyList", "items")

            if ($policyArray) { $policies = @(As-Array $policyArray) }
            elseif ($json -is [array]) { $policies = @($json) }
            elseif ($json) { $policies = @($json) }

            foreach ($p in @($policies | Where-Object { $_ })) {
                if ($p -is [string]) { continue }

                $id = FirstValue @((Get-PropValue -Object $p -Names @("id", "policyId")))
                $name = FirstValue @((Get-PropValue -Object $p -Names @("name", "policyName", "displayName")))

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
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policyInfo.name"),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policy.name"),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policyConfig.name"),
        (Get-PropValue -Object $ProtectionGroup -Names @("policyName"))
    )

    $policyId = FirstValue @(
        (Get-PropValue -Object $ProtectionGroup -Names @("policyId")),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policyInfo.id"),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policy.id")
    )

    if (-not [string]::IsNullOrWhiteSpace($policyId) -and $PolicyMap.ContainsKey($policyId)) {
        return $PolicyMap[$policyId]
    }

    if (-not [string]::IsNullOrWhiteSpace($policyName)) {
        if (-not (Test-LooksLikeId -Value $policyName)) { return $policyName }
        if ($PolicyMap.ContainsKey($policyName)) { return $PolicyMap[$policyName] }
    }

    return "UNRESOLVED_POLICY_NAME"
}

# -------------------------------
# Cluster menu
# -------------------------------
$cluJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$json_clu = @(As-Array (Get-PropValue -Object $cluJson -Names @("cohesityClusters")))

if (-not $json_clu -or $json_clu.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$clusters = $json_clu | ForEach-Object {
    $name = FirstValue @(
        (Get-PropValue -Object $_ -Names @("clusterName")),
        (Get-PropValue -Object $_ -Names @("displayName")),
        (Get-PropValue -Object $_ -Names @("name"))
    )
    $cid  = FirstValue @(
        (Get-PropValue -Object $_ -Names @("clusterId")),
        (Get-PropValue -Object $_ -Names @("id"))
    )

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
        $physical = Get-PropValue -Object $pg -Names @("physicalParams")
        if ($null -eq $physical) { continue }

        $pgName = FirstValue @((Get-PropValue -Object $pg -Names @("name", "protectionGroupName")))
        $pgIndex++
        $pgKey = Get-PGKey -Cluster $clusterName -PGName $pgName
        $protectionType = FirstValue @((Get-PropValue -Object $physical -Names @("protectionType")))

        $fileParams = Get-PropValue -Object $physical -Names @("fileProtectionTypeParams")
        $volumeParams = Get-PropValue -Object $physical -Names @("volumeProtectionTypeParams")

        if ($protectionType -eq "kVolume") {
            $objects = @(As-Array (Get-PropValue -Object $volumeParams -Names @("objects")) | Where-Object { $_ })
            $globalExcludePaths = ""
            $jobExcludedVssWriters = Flat (Get-PropValue -Object $volumeParams -Names @("excludedVssWriters"))
        }
        else {
            $objects = @(As-Array (Get-PropValue -Object $fileParams -Names @("objects")) | Where-Object { $_ })
            $globalExcludePaths = Flat (Get-PropValue -Object $fileParams -Names @("globalExcludePaths"))
            $jobExcludedVssWriters = Flat (Get-PropValue -Object $fileParams -Names @("excludedVssWriters"))
        }

        $lastRun = Get-PropValue -Object $pg -Names @("lastRun")
        $localInfo = Get-PropValue -Object $lastRun -Names @("localBackupInfo", "localSnapshotInfo")

        $summaryRows += [PSCustomObject]@{
            PGKey                 = $pgKey
            PGIndex               = $pgIndex
            Cluster               = $clusterName
            PGName                = $pgName
            PolicyName            = Resolve-PolicyName -ProtectionGroup $pg -PolicyMap $policyMap
            ProtectionType        = $protectionType
            PGObjectCount         = @($objects).Count
            GlobalExcludePaths    = $globalExcludePaths
            JobExcludedVssWriters = $jobExcludedVssWriters
            IsActive              = Get-PropValue -Object $pg -Names @("isActive")
            IsPaused              = Get-PropValue -Object $pg -Names @("isPaused")
            LastRunStatus         = FirstValue @((Get-PropValue -Object $localInfo -Names @("status")), (Get-PropValue -Object $lastRun -Names @("status")))
            LastRunStartET        = UsecsToET (FirstValue @((Get-PropValue -Object $localInfo -Names @("startTimeUsecs")), (Get-PropValue -Object $lastRun -Names @("startTimeUsecs"))))
            LastRunEndET          = UsecsToET (FirstValue @((Get-PropValue -Object $localInfo -Names @("endTimeUsecs")), (Get-PropValue -Object $lastRun -Names @("endTimeUsecs"))))
        }

        foreach ($obj in $objects) {
            $objectName = FirstValue @(
                (Get-PropValue -Object $obj -Names @("name")),
                (Get-PropValue -Object $obj -Names @("sourceName")),
                (Get-PropValue -Object $obj -Names @("hostName")),
                (Get-PropValue -Object $obj -Names @("displayName")),
                (Get-PropValue -Object $obj -Names @("id"))
            )
            $objectExcludedVssWriters = Flat (Get-PropValue -Object $obj -Names @("excludedVssWriters"))
            $filePaths = @(As-Array (Get-PropValue -Object $obj -Names @("filePaths")) | Where-Object { $_ })

            if ($protectionType -eq "kVolume") {
                $volumeGuids = Flat (Get-PropValue -Object $obj -Names @("volumeGuids"))
                $detailRows += [PSCustomObject]@{
                    PGKey                          = $pgKey
                    Cluster                        = $clusterName
                    PGName                         = $pgName
                    ObjectName                     = $objectName
                    ObjectIncludedPaths            = $volumeGuids
                    ObjectExcludedPathsAll         = ""
                    IncludedPath                   = $volumeGuids
                    ExcludedPathsUnderIncludedPath = ""
                    SkipNestedVolumes              = ""
                    GlobalExcludePaths             = $globalExcludePaths
                    ObjectExcludedVssWriters       = $objectExcludedVssWriters
                    JobExcludedVssWriters          = $jobExcludedVssWriters
                }
                continue
            }

            $objectIncludedPaths = Flat @($filePaths | ForEach-Object { Get-PropValue -Object $_ -Names @("includedPath") })
            $allExcludedPaths = @()
            foreach ($fp in $filePaths) { $allExcludedPaths += @(As-Array (Get-PropValue -Object $fp -Names @("excludedPaths"))) }
            $objectExcludedPathsAll = Flat $allExcludedPaths

            if ($filePaths.Count -eq 0) {
                $detailRows += [PSCustomObject]@{
                    PGKey                          = $pgKey
                    Cluster                        = $clusterName
                    PGName                         = $pgName
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
                        PGName                         = $pgName
                        ObjectName                     = $objectName
                        ObjectIncludedPaths            = $objectIncludedPaths
                        ObjectExcludedPathsAll         = $objectExcludedPathsAll
                        IncludedPath                   = Get-PropValue -Object $fp -Names @("includedPath")
                        ExcludedPathsUnderIncludedPath = Flat (Get-PropValue -Object $fp -Names @("excludedPaths"))
                        SkipNestedVolumes              = Get-PropValue -Object $fp -Names @("skipNestedVolumes")
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
