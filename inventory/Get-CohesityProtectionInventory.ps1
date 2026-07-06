# Cohesity Helios - Protection Inventory Framework
# STRICTLY READ-ONLY / GET-only
# Baseline environments: Physical, Hyper-V, Nutanix AHV
# Output:
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Cohesity_Protection_PG_Summary_Latest.csv
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Cohesity_Protection_Object_Detail_Latest.csv
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Cohesity_Protection_Path_Detail_Latest.csv
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Cohesity_Protection_Exceptions_Latest.csv
#   X:\PowerShell\Cohesity_API_Scripts\inventory\Cohesity_Protection_Run_Metadata.json

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$outDir     = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$baseUrl    = "https://helios.cohesity.com"

$InventoryDateET = $null
try {
    $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    $InventoryDateET = ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)).ToString("yyyy-MM-dd HH:mm:ss")
}
catch {
    $InventoryDateET = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
}

$EnvironmentMap = @(
    [PSCustomObject]@{ ApiName = "kPhysical";  DisplayName = "Physical";    ParamName = "physicalParams"  },
    [PSCustomObject]@{ ApiName = "kHyperV";    DisplayName = "Hyper-V";     ParamName = "hypervParams"    },
    [PSCustomObject]@{ ApiName = "kAcropolis"; DisplayName = "Nutanix AHV"; ParamName = "acropolisParams" }
)

if (-not (Test-Path -Path $outDir -PathType Container)) {
    New-Item -Path $outDir -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

function New-Headers {
    param([string]$ClusterId)

    $h = @{ accept = "application/json" }
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

    if (-not $resp -or [string]::IsNullOrWhiteSpace($resp.Content)) { return $null }
    return ($resp.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
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

function Flat {
    param($Value)

    if ($null -eq $Value) { return "" }

    $items = @()
    foreach ($v in @($Value)) {
        foreach ($vv in @($v)) {
            if ($null -ne $vv -and "$vv".Trim() -ne "") { $items += "$vv" }
        }
    }

    if ($items.Count -eq 0) { return "" }
    return (($items | Select-Object -Unique) -join ";")
}

function Count-FlatItems {
    param($Value)

    $flatValue = Flat $Value
    if ([string]::IsNullOrWhiteSpace($flatValue)) { return 0 }
    return @($flatValue -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
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
    catch { return "" }
}

function Get-AgeHoursFromET {
    param([string]$DateET)

    if ([string]::IsNullOrWhiteSpace($DateET)) { return "" }

    try {
        $dt = [datetime]::ParseExact($DateET, "yyyy-MM-dd HH:mm:ss", $null)
        $now = [datetime]::ParseExact($InventoryDateET, "yyyy-MM-dd HH:mm:ss", $null)
        return [math]::Round(($now - $dt).TotalHours, 2)
    }
    catch { return "" }
}

function Is-SuccessStatus {
    param([string]$Status)

    if ([string]::IsNullOrWhiteSpace($Status)) { return $false }
    $s = $Status.Trim().ToLower()
    return @("ksuccess", "success", "succeeded") -contains $s
}

function Get-PGKey {
    param(
        [string]$ClusterId,
        [string]$ProtectionGroupId,
        [string]$ClusterName,
        [string]$ProtectionGroupName
    )

    if (-not [string]::IsNullOrWhiteSpace($ClusterId) -and -not [string]::IsNullOrWhiteSpace($ProtectionGroupId)) {
        return "$ClusterId|$ProtectionGroupId"
    }

    return (("{0}|{1}" -f $ClusterName, $ProtectionGroupName).Trim())
}

function Get-PolicyMap {
    param([hashtable]$Headers)

    $map = @{}
    $uris = @(
        "$baseUrl/v2/data-protect/policies?isDeleted=false&maxResultCount=1000",
        "$baseUrl/v2/data-protect/policies?maxResultCount=1000",
        "$baseUrl/v2/data-protect/policies"
    )

    foreach ($uri in $uris) {
        try {
            $json = Get-Json -Uri $uri -Headers $Headers
            $policies = @()
            if ($json.policies) { $policies = @($json.policies) }
            elseif ($json -is [array]) { $policies = @($json) }
            elseif ($json) { $policies = @($json) }

            foreach ($p in @($policies | Where-Object { $_ })) {
                $id = FirstValue @($p.id, $p.policyId)
                $name = FirstValue @($p.name, $p.policyName)
                if (-not [string]::IsNullOrWhiteSpace($id) -and -not [string]::IsNullOrWhiteSpace($name)) {
                    $map[$id] = $name
                }
            }

            if ($map.Count -gt 0) { break }
        }
        catch {
            # Policy resolution is best-effort. Inventory collection should continue.
        }
    }

    return $map
}

function Resolve-PolicyName {
    param(
        $ProtectionGroup,
        [hashtable]$PolicyMap
    )

    $policyId = FirstValue @(
        $ProtectionGroup.policyId,
        $ProtectionGroup.policyInfo.id,
        $ProtectionGroup.policy.id,
        $ProtectionGroup.policyName
    )

    if (-not [string]::IsNullOrWhiteSpace($policyId) -and $PolicyMap.ContainsKey($policyId)) {
        return $PolicyMap[$policyId]
    }

    return FirstValue @(
        $ProtectionGroup.policyInfo.name,
        $ProtectionGroup.policy.name,
        $ProtectionGroup.policyConfig.name,
        $ProtectionGroup.policyName,
        $ProtectionGroup.policyId
    )
}

function Get-ProtectionGroups {
    param(
        [string]$EnvironmentApiName,
        [hashtable]$Headers
    )

    $all = @()
    $cookie = ""

    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=$EnvironmentApiName&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri = "$uri&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-Json -Uri $uri -Headers $Headers

        if ($json.protectionGroups) {
            $all += @($json.protectionGroups | Where-Object { $_ })
        }

        $cookie = FirstValue @($json.paginationCookie)
        if ($json.isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    } while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-EnvironmentParams {
    param(
        $ProtectionGroup,
        [string]$ParamName
    )

    if ($null -eq $ProtectionGroup) { return $null }
    return $ProtectionGroup.$ParamName
}

function Get-ObjectsFromParams {
    param(
        $Params,
        [string]$EnvironmentApiName
    )

    if ($null -eq $Params) { return @() }

    if ($EnvironmentApiName -eq "kPhysical") {
        $protectionType = FirstValue @($Params.protectionType)
        if ($protectionType -eq "kVolume") {
            return @(As-Array $Params.volumeProtectionTypeParams.objects | Where-Object { $_ })
        }
        return @(As-Array $Params.fileProtectionTypeParams.objects | Where-Object { $_ })
    }

    $candidateLists = @(
        $Params.objects,
        $Params.sourceObjects,
        $Params.virtualMachines,
        $Params.vms,
        $Params.protectedObjects,
        $Params.vmObjects
    )

    foreach ($candidate in $candidateLists) {
        $items = @(As-Array $candidate | Where-Object { $_ })
        if ($items.Count -gt 0) { return $items }
    }

    return @()
}

function Get-ObjectName {
    param($Object)

    return FirstValue @(
        $Object.name,
        $Object.objectName,
        $Object.sourceName,
        $Object.vmName,
        $Object.hostName,
        $Object.displayName,
        $Object.id
    )
}

function Get-ObjectId {
    param($Object)

    return FirstValue @(
        $Object.id,
        $Object.objectId,
        $Object.sourceId,
        $Object.vmId
    )
}

function New-ExceptionRow {
    param(
        [string]$Cluster,
        [string]$Environment,
        [string]$ProtectionGroup,
        [string]$HostName,
        [string]$ObjectName,
        [string]$ExceptionType,
        [string]$Severity,
        [string]$ExceptionReason,
        [string]$RecommendedAction
    )

    return [PSCustomObject]@{
        InventoryDateET   = $InventoryDateET
        Cluster           = $Cluster
        Environment       = $Environment
        ProtectionGroup   = $ProtectionGroup
        HostName          = $HostName
        ObjectName        = $ObjectName
        ExceptionType     = $ExceptionType
        Severity          = $Severity
        ExceptionReason   = $ExceptionReason
        RecommendedAction = $RecommendedAction
    }
}

# -------------------------------
# Cluster menu
# -------------------------------
$cluJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$json_clu = @($cluJson.cohesityClusters)

if (-not $json_clu -or $json_clu.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = $json_clu | ForEach-Object {
    $name = FirstValue @($_.name, $_.clusterName, $_.displayName)
    $cid  = FirstValue @($_.clusterId, $_.id)
    if ([string]::IsNullOrWhiteSpace($name)) { $name = "Unknown-$cid" }

    [PSCustomObject]@{ ClusterName = $name; ClusterId = $cid }
} | Sort-Object ClusterName

$clusterMenu = for ($i = 0; $i -lt $clusters.Count; $i++) {
    [PSCustomObject]@{ Index = $i + 1; ClusterName = $clusters[$i].ClusterName; ClusterId = $clusters[$i].ClusterId }
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
    if (-not [int]::TryParse($selection, [ref]$n)) { Write-Host "Invalid selection." -ForegroundColor Red; continue }
    if ($n -lt 0 -or $n -gt $clusterMenu.Count) { Write-Host "Invalid selection." -ForegroundColor Red; continue }

    if ($n -eq 0) { $selectedClusters = @($clusterMenu) }
    else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $n }) }
    break
}

Write-Host ""
Write-Host "Environment scope:" -ForegroundColor Cyan
Write-Host "[0] Baseline: Physical + Hyper-V + Nutanix AHV" -ForegroundColor Yellow
Write-Host "[1] Physical only" -ForegroundColor Yellow
Write-Host "[2] Hyper-V only" -ForegroundColor Yellow
Write-Host "[3] Nutanix AHV only" -ForegroundColor Yellow

while ($true) {
    $envSelection = Read-Host "Select environment scope"
    if ($envSelection -eq "0") { $selectedEnvironments = @($EnvironmentMap); break }
    if ($envSelection -eq "1") { $selectedEnvironments = @($EnvironmentMap | Where-Object { $_.ApiName -eq "kPhysical" }); break }
    if ($envSelection -eq "2") { $selectedEnvironments = @($EnvironmentMap | Where-Object { $_.ApiName -eq "kHyperV" }); break }
    if ($envSelection -eq "3") { $selectedEnvironments = @($EnvironmentMap | Where-Object { $_.ApiName -eq "kAcropolis" }); break }
    Write-Host "Enter 0, 1, 2, or 3." -ForegroundColor Red
}

# -------------------------------
# Collect inventory
# -------------------------------
$pgSummaryRows = @()
$objectDetailRows = @()
$pathDetailRows = @()
$exceptionRows = @()

foreach ($c in $selectedClusters) {
    $clusterName = $c.ClusterName
    $clusterId = $c.ClusterId
    $headers = New-Headers -ClusterId $clusterId
    $policyMap = Get-PolicyMap -Headers $headers

    foreach ($env in $selectedEnvironments) {
        Write-Host "Collecting $($env.DisplayName) PGs from $clusterName ..." -ForegroundColor Yellow

        $pgs = Get-ProtectionGroups -EnvironmentApiName $env.ApiName -Headers $headers

        foreach ($pg in @($pgs | Where-Object { $_ })) {
            $params = Get-EnvironmentParams -ProtectionGroup $pg -ParamName $env.ParamName
            if ($null -eq $params) { continue }

            $pgId = FirstValue @($pg.id, $pg.protectionGroupId)
            $pgName = FirstValue @($pg.name, $pg.protectionGroupName)
            $pgKey = Get-PGKey -ClusterId $clusterId -ProtectionGroupId $pgId -ClusterName $clusterName -ProtectionGroupName $pgName
            $policyId = FirstValue @($pg.policyId, $pg.policyInfo.id, $pg.policy.id, $pg.policyName)
            $policyName = Resolve-PolicyName -ProtectionGroup $pg -PolicyMap $policyMap
            $objects = @(Get-ObjectsFromParams -Params $params -EnvironmentApiName $env.ApiName)

            $lastRun = $pg.lastRun
            $localInfo = $lastRun.localBackupInfo
            if ($null -eq $localInfo) { $localInfo = $lastRun.localSnapshotInfo }
            $lastRunStatus = FirstValue @($localInfo.status, $lastRun.status)
            $lastRunEndET = UsecsToET (FirstValue @($localInfo.endTimeUsecs, $lastRun.endTimeUsecs))
            $lastRunType = FirstValue @($localInfo.runType, $lastRun.runType)

            $lastSuccessET = ""
            $lastSuccessStatus = ""
            if (Is-SuccessStatus $lastRunStatus) {
                $lastSuccessET = $lastRunEndET
                $lastSuccessStatus = $lastRunStatus
            }
            $lastSuccessAgeHours = Get-AgeHoursFromET $lastSuccessET

            $globalExcludePaths = ""
            $globalExcludeCount = 0
            $objectExcludePathCount = 0
            $hasGlobalExclusions = $false
            $hasObjectExclusions = $false

            if ($env.ApiName -eq "kPhysical") {
                $protectionType = FirstValue @($params.protectionType)
                if ($protectionType -ne "kVolume") {
                    $globalExcludePaths = Flat $params.fileProtectionTypeParams.globalExcludePaths
                    $globalExcludeCount = Count-FlatItems $params.fileProtectionTypeParams.globalExcludePaths
                    if ($globalExcludeCount -gt 0) { $hasGlobalExclusions = $true }

                    foreach ($obj in $objects) {
                        foreach ($fp in @(As-Array $obj.filePaths | Where-Object { $_ })) {
                            $objectExcludePathCount += Count-FlatItems $fp.excludedPaths
                        }
                    }
                    if ($objectExcludePathCount -gt 0) { $hasObjectExclusions = $true }
                }
            }

            $pgSummaryRows += [PSCustomObject]@{
                PGKey                         = $pgKey
                InventoryDateET               = $InventoryDateET
                Cluster                       = $clusterName
                ClusterId                     = $clusterId
                Environment                   = $env.DisplayName
                ProtectionGroup               = $pgName
                ProtectionGroupId             = $pgId
                PolicyName                    = $policyName
                PolicyId                      = $policyId
                IsActive                      = $pg.isActive
                IsDeleted                     = $pg.isDeleted
                ObjectCount                   = @($objects).Count
                GlobalExcludePathCount        = $globalExcludeCount
                ObjectExcludePathCount        = $objectExcludePathCount
                HasGlobalExclusions           = $hasGlobalExclusions
                HasObjectExclusions           = $hasObjectExclusions
                LastSuccessfulBackupET        = $lastSuccessET
                LastSuccessfulBackupStatus    = $lastSuccessStatus
                LastSuccessfulBackupAgeHours  = $lastSuccessAgeHours
                LastRunStatus                 = $lastRunStatus
                LastRunType                   = $lastRunType
                IsPaused                      = $pg.isPaused
                StorageDomain                 = FirstValue @($pg.storageDomainName, $pg.storageDomain.name, $pg.storageDomain.id)
                SourceName                    = FirstValue @($pg.sourceName, $pg.source.name, $params.sourceName, $params.source.name)
            }

            if (@($objects).Count -eq 0) {
                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "PG_ZERO_OBJECTS" -Severity "High" -ExceptionReason "Protection group has zero objects." -RecommendedAction "Confirm whether the protection group is intentionally empty or needs object membership fixed."
            }

            if ([string]::IsNullOrWhiteSpace($policyName)) {
                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "MISSING_POLICY" -Severity "Critical" -ExceptionReason "Protection group policy name could not be resolved." -RecommendedAction "Check policy assignment in Cohesity and policy API visibility."
            }

            if ([string]::IsNullOrWhiteSpace($lastSuccessET)) {
                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "MISSING_LAST_SUCCESS" -Severity "Critical" -ExceptionReason "No successful backup timestamp was found from the latest run information." -RecommendedAction "Check recent runs and job failures in Cohesity."
            }
            elseif ($lastSuccessAgeHours -ne "") {
                if ([double]$lastSuccessAgeHours -gt 48) {
                    $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "LAST_SUCCESS_GT_48H" -Severity "Critical" -ExceptionReason "Last successful backup is older than 48 hours." -RecommendedAction "Investigate job schedule, failures, pause state, and object availability."
                }
                elseif ([double]$lastSuccessAgeHours -gt 24) {
                    $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "LAST_SUCCESS_GT_24H" -Severity "High" -ExceptionReason "Last successful backup is older than 24 hours." -RecommendedAction "Review backup freshness and recent run status."
                }
            }

            if ($hasGlobalExclusions) {
                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "PG_GLOBAL_EXCLUSIONS" -Severity "Medium" -ExceptionReason "Protection group has global exclude paths." -RecommendedAction "Review whether global exclusions are approved and documented."
            }

            if ($objectExcludePathCount -gt 20) {
                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "HIGH_EXCLUSION_COUNT" -Severity "Medium" -ExceptionReason "Protection group has a high number of object-level exclusions." -RecommendedAction "Review exclusions for audit and operational correctness."
            }

            if (@($objects).Count -gt 250) {
                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "VERY_LARGE_PG" -Severity "Medium" -ExceptionReason "Protection group has more than 250 objects." -RecommendedAction "Review whether the PG should be split for operations, run duration, and blast-radius control."
            }

            foreach ($obj in $objects) {
                $objectName = Get-ObjectName $obj
                $objectId = Get-ObjectId $obj
                $hostName = FirstValue @($obj.hostName, $obj.sourceName, $obj.parentSourceName, $objectName)
                $objectType = FirstValue @($obj.objectType, $obj.type, $obj.entityType)
                if ([string]::IsNullOrWhiteSpace($objectType)) {
                    if ($env.ApiName -eq "kPhysical") { $objectType = "PhysicalObject" }
                    else { $objectType = "VirtualMachine" }
                }

                $includedPathCount = 0
                $objExcludeCount = 0
                $objHasExclusions = $false
                $objHasIncludedPath = $true

                if ($env.ApiName -eq "kPhysical") {
                    $protectionType = FirstValue @($params.protectionType)
                    if ($protectionType -eq "kVolume") {
                        $includedPathCount = Count-FlatItems $obj.volumeGuids
                    }
                    else {
                        $filePaths = @(As-Array $obj.filePaths | Where-Object { $_ })
                        $includedPathCount = @($filePaths | Where-Object { -not [string]::IsNullOrWhiteSpace($_.includedPath) }).Count
                        foreach ($fp in $filePaths) { $objExcludeCount += Count-FlatItems $fp.excludedPaths }
                    }

                    if ($includedPathCount -eq 0) { $objHasIncludedPath = $false }
                    if ($objExcludeCount -gt 0) { $objHasExclusions = $true }
                }

                $objectDetailRows += [PSCustomObject]@{
                    ObjectKey                    = "$pgKey|$objectId|$objectName"
                    PGKey                        = $pgKey
                    InventoryDateET              = $InventoryDateET
                    Cluster                      = $clusterName
                    ClusterId                    = $clusterId
                    Environment                  = $env.DisplayName
                    ProtectionGroup              = $pgName
                    ProtectionGroupId            = $pgId
                    PolicyName                   = $policyName
                    HostName                     = $hostName
                    ObjectName                   = $objectName
                    ObjectType                   = $objectType
                    ObjectId                     = $objectId
                    ParentSource                 = FirstValue @($obj.parentSourceName, $obj.sourceName, $obj.parentSource.id, $obj.sourceId)
                    IncludedPathCount            = $includedPathCount
                    ObjectExcludePathCount       = $objExcludeCount
                    HasGlobalExclusions          = $hasGlobalExclusions
                    HasObjectExclusions          = $objHasExclusions
                    LastSuccessfulBackupET       = $lastSuccessET
                    LastSuccessfulBackupStatus   = $lastSuccessStatus
                }

                if ($env.ApiName -eq "kPhysical") {
                    $protectionType = FirstValue @($params.protectionType)
                    if ($protectionType -eq "kVolume") {
                        $pathDetailRows += [PSCustomObject]@{
                            PathKey            = "$pgKey|$objectId|volume"
                            PGKey              = $pgKey
                            ObjectKey          = "$pgKey|$objectId|$objectName"
                            InventoryDateET    = $InventoryDateET
                            Cluster            = $clusterName
                            Environment        = $env.DisplayName
                            ProtectionGroup    = $pgName
                            HostName           = $hostName
                            ObjectName         = $objectName
                            IncludedPath       = Flat $obj.volumeGuids
                            ExcludedPath       = ""
                            ExclusionLevel     = "None"
                            SkipNestedVolumes  = ""
                            GlobalExcludePaths = $globalExcludePaths
                        }
                    }
                    else {
                        $filePaths = @(As-Array $obj.filePaths | Where-Object { $_ })
                        foreach ($fp in $filePaths) {
                            $excluded = @(As-Array $fp.excludedPaths | Where-Object { $_ })
                            if ($excluded.Count -eq 0) {
                                $pathDetailRows += [PSCustomObject]@{
                                    PathKey            = "$pgKey|$objectId|$($fp.includedPath)|none"
                                    PGKey              = $pgKey
                                    ObjectKey          = "$pgKey|$objectId|$objectName"
                                    InventoryDateET    = $InventoryDateET
                                    Cluster            = $clusterName
                                    Environment        = $env.DisplayName
                                    ProtectionGroup    = $pgName
                                    HostName           = $hostName
                                    ObjectName         = $objectName
                                    IncludedPath       = $fp.includedPath
                                    ExcludedPath       = ""
                                    ExclusionLevel     = "None"
                                    SkipNestedVolumes  = $fp.skipNestedVolumes
                                    GlobalExcludePaths = $globalExcludePaths
                                }
                            }
                            else {
                                foreach ($excludedPath in $excluded) {
                                    $pathDetailRows += [PSCustomObject]@{
                                        PathKey            = "$pgKey|$objectId|$($fp.includedPath)|$excludedPath"
                                        PGKey              = $pgKey
                                        ObjectKey          = "$pgKey|$objectId|$objectName"
                                        InventoryDateET    = $InventoryDateET
                                        Cluster            = $clusterName
                                        Environment        = $env.DisplayName
                                        ProtectionGroup    = $pgName
                                        HostName           = $hostName
                                        ObjectName         = $objectName
                                        IncludedPath       = $fp.includedPath
                                        ExcludedPath       = $excludedPath
                                        ExclusionLevel     = "Object"
                                        SkipNestedVolumes  = $fp.skipNestedVolumes
                                        GlobalExcludePaths = $globalExcludePaths
                                    }
                                }
                            }

                            if ($fp.skipNestedVolumes -eq $true) {
                                $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName $hostName -ObjectName $objectName -ExceptionType "SKIP_NESTED_VOLUMES" -Severity "Medium" -ExceptionReason "SkipNestedVolumes is enabled for an included path." -RecommendedAction "Validate whether nested volume exclusion is expected and approved."
                            }
                        }
                    }

                    if (-not $objHasIncludedPath) {
                        $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName $hostName -ObjectName $objectName -ExceptionType "OBJECT_NO_INCLUDED_PATH" -Severity "High" -ExceptionReason "Physical object has no included path." -RecommendedAction "Review object include path configuration."
                    }

                    if ($objHasExclusions) {
                        $exceptionRows += New-ExceptionRow -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName $hostName -ObjectName $objectName -ExceptionType "OBJECT_LEVEL_EXCLUSIONS" -Severity "Medium" -ExceptionReason "Object has object-level exclude paths." -RecommendedAction "Review object-level exclusions for audit and operational correctness."
                    }
                }
            }
        }
    }
}

# -------------------------------
# Export stable data contract files
# -------------------------------
$pgSummaryRows    = $pgSummaryRows    | Sort-Object Cluster, Environment, ProtectionGroup
$objectDetailRows = $objectDetailRows | Sort-Object Cluster, Environment, ProtectionGroup, ObjectName
$pathDetailRows   = $pathDetailRows   | Sort-Object Cluster, Environment, ProtectionGroup, ObjectName, IncludedPath, ExcludedPath
$exceptionRows    = $exceptionRows    | Sort-Object Severity, Cluster, Environment, ProtectionGroup, ObjectName, ExceptionType

$pgSummaryCsv    = Join-Path $outDir "Cohesity_Protection_PG_Summary_Latest.csv"
$objectDetailCsv = Join-Path $outDir "Cohesity_Protection_Object_Detail_Latest.csv"
$pathDetailCsv   = Join-Path $outDir "Cohesity_Protection_Path_Detail_Latest.csv"
$exceptionsCsv   = Join-Path $outDir "Cohesity_Protection_Exceptions_Latest.csv"
$metadataJson    = Join-Path $outDir "Cohesity_Protection_Run_Metadata.json"

$pgSummaryRows    | Export-Csv -Path $pgSummaryCsv -NoTypeInformation -Encoding utf8
$objectDetailRows | Export-Csv -Path $objectDetailCsv -NoTypeInformation -Encoding utf8
$pathDetailRows   | Export-Csv -Path $pathDetailCsv -NoTypeInformation -Encoding utf8
$exceptionRows    | Export-Csv -Path $exceptionsCsv -NoTypeInformation -Encoding utf8

$metadata = [PSCustomObject]@{
    InventoryDateET     = $InventoryDateET
    ScriptName          = "Get-CohesityProtectionInventory.ps1"
    HeliosBaseUrl       = $baseUrl
    SelectedClusters    = @($selectedClusters | Select-Object ClusterName, ClusterId)
    SelectedEnvironments = @($selectedEnvironments | Select-Object ApiName, DisplayName)
    OutputFiles         = [PSCustomObject]@{
        PGSummary     = $pgSummaryCsv
        ObjectDetail  = $objectDetailCsv
        PathDetail    = $pathDetailCsv
        Exceptions    = $exceptionsCsv
    }
    Counts              = [PSCustomObject]@{
        PGSummaryRows     = @($pgSummaryRows).Count
        ObjectDetailRows  = @($objectDetailRows).Count
        PathDetailRows    = @($pathDetailRows).Count
        ExceptionRows     = @($exceptionRows).Count
    }
    Notes               = @(
        "GET-only baseline collector.",
        "Physical path detail is populated.",
        "Hyper-V and Nutanix AHV use object-level VM inventory; path detail is not forced.",
        "LastSuccessfulBackupET currently uses latest run information when latest run status is success. Older-success run scan is a future enhancement."
    )
}

$metadata | ConvertTo-Json -Depth 8 | Out-File -FilePath $metadataJson -Encoding utf8

Write-Host ""
Write-Host "Cohesity Protection Inventory export complete." -ForegroundColor Green
Write-Host "PG Summary rows    : $(@($pgSummaryRows).Count)" -ForegroundColor Green
Write-Host "Object Detail rows : $(@($objectDetailRows).Count)" -ForegroundColor Green
Write-Host "Path Detail rows   : $(@($pathDetailRows).Count)" -ForegroundColor Green
Write-Host "Exception rows     : $(@($exceptionRows).Count)" -ForegroundColor Green
Write-Host "PG Summary CSV     : $pgSummaryCsv" -ForegroundColor Green
Write-Host "Object Detail CSV  : $objectDetailCsv" -ForegroundColor Green
Write-Host "Path Detail CSV    : $pathDetailCsv" -ForegroundColor Green
Write-Host "Exceptions CSV     : $exceptionsCsv" -ForegroundColor Green
Write-Host "Metadata JSON      : $metadataJson" -ForegroundColor Green
