# Cohesity Helios - Protection Inventory Framework
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
# Baseline environments: Physical, Hyper-V, Nutanix AHV
#
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

$EnvironmentMap = @(
    [PSCustomObject]@{ ApiName = "kPhysical";  DisplayName = "Physical";    ParamNames = @("physicalParams") },
    [PSCustomObject]@{ ApiName = "kHyperV";    DisplayName = "Hyper-V";     ParamNames = @("hypervParams", "hyperVParams") },
    [PSCustomObject]@{ ApiName = "kAcropolis"; DisplayName = "Nutanix AHV"; ParamNames = @("acropolisParams", "nutanixParams", "ahvParams") }
)

if (-not (Test-Path -Path $outDir -PathType Container)) {
    New-Item -Path $outDir -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKey = (Get-Content -Path $apikeypath -Raw).Trim()

function Get-InventoryDateET {
    try {
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        return ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)).ToString("yyyy-MM-dd HH:mm:ss")
    }
    catch {
        return (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    }
}

$InventoryDateET = Get-InventoryDateET

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

function FirstValue {
    param($Values)

    foreach ($v in @($Values)) {
        foreach ($vv in @($v)) {
            if ($null -ne $vv -and "$vv".Trim() -ne "") { return "$vv" }
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
    return (@("ksuccess", "success", "succeeded") -contains $s)
}

function Get-FreshnessBucket {
    param($AgeHours)

    if ($AgeHours -eq "" -or $null -eq $AgeHours) { return "No Success Found" }

    try {
        $age = [double]$AgeHours
        if ($age -le 24) { return "<=24h" }
        if ($age -le 48) { return "24-48h" }
        return ">48h"
    }
    catch { return "Unknown" }
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

function Get-ObjectKey {
    param(
        [string]$PGKey,
        [string]$ObjectId,
        [string]$ObjectName
    )

    if (-not [string]::IsNullOrWhiteSpace($ObjectId)) { return "$PGKey|$ObjectId" }
    return "$PGKey|$ObjectName"
}

function Test-LooksLikeInventoryObject {
    param($Object)

    if ($null -eq $Object) { return $false }
    if ($Object -is [string]) { return $false }

    $props = @($Object.PSObject.Properties.Name)
    $interesting = @("id", "objectId", "sourceId", "vmId", "entityId", "name", "objectName", "sourceName", "vmName", "hostName", "displayName")

    foreach ($p in $props) {
        foreach ($i in $interesting) {
            if ($p -ieq $i) { return $true }
        }
    }

    return $false
}

function Find-InventoryObjectsRecursive {
    param(
        $Node,
        [int]$Depth
    )

    $results = @()
    if ($null -eq $Node) { return @() }
    if ($Depth -gt 4) { return @() }
    if ($Node -is [string]) { return @() }

    foreach ($item in @(As-Array $Node)) {
        if ($null -eq $item -or $item -is [string]) { continue }

        if (Test-LooksLikeInventoryObject -Object $item) { $results += $item }

        foreach ($prop in @($item.PSObject.Properties)) {
            $val = $prop.Value
            if ($null -eq $val -or $val -is [string]) { continue }

            $arrayVal = @(As-Array $val | Where-Object { $_ -and ($_ -isnot [string]) })
            if ($arrayVal.Count -gt 0) {
                $looksUseful = $false
                foreach ($av in $arrayVal) {
                    if (Test-LooksLikeInventoryObject -Object $av) { $looksUseful = $true; break }
                }

                if ($looksUseful) { $results += $arrayVal }
                elseif ($Depth -lt 4) { $results += @(Find-InventoryObjectsRecursive -Node $arrayVal -Depth ($Depth + 1)) }
            }
        }
    }

    return @($results)
}

function Get-ObjectName {
    param($Object)

    return FirstValue @(
        (Get-PropValue -Object $Object -Names @("name")),
        (Get-PropValue -Object $Object -Names @("objectName")),
        (Get-PropValue -Object $Object -Names @("sourceName")),
        (Get-PropValue -Object $Object -Names @("vmName")),
        (Get-PropValue -Object $Object -Names @("hostName")),
        (Get-PropValue -Object $Object -Names @("displayName")),
        (Get-PropValue -Object $Object -Names @("id"))
    )
}

function Get-ObjectId {
    param($Object)

    return FirstValue @(
        (Get-PropValue -Object $Object -Names @("id")),
        (Get-PropValue -Object $Object -Names @("objectId")),
        (Get-PropValue -Object $Object -Names @("sourceId")),
        (Get-PropValue -Object $Object -Names @("vmId")),
        (Get-PropValue -Object $Object -Names @("entityId"))
    )
}

function Get-DedupedObjects {
    param($Objects)

    $seen = @{}
    $deduped = @()

    foreach ($obj in @(As-Array $Objects | Where-Object { $_ })) {
        if ($obj -is [string]) { continue }
        $id = Get-ObjectId -Object $obj
        $name = Get-ObjectName -Object $obj
        $key = FirstValue @($id, $name)

        if ([string]::IsNullOrWhiteSpace($key)) { continue }

        if (-not $seen.ContainsKey($key)) {
            $seen[$key] = $true
            $deduped += $obj
        }
    }

    return @($deduped)
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
            # Policy resolution is best-effort. Inventory collection continues.
        }
    }

    return $map
}

function Resolve-PolicyId {
    param($ProtectionGroup)

    return FirstValue @(
        (Get-PropValue -Object $ProtectionGroup -Names @("policyId")),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policyInfo.id"),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policy.id")
    )
}

function Resolve-PolicyName {
    param(
        $ProtectionGroup,
        [hashtable]$PolicyMap
    )

    $policyId = Resolve-PolicyId -ProtectionGroup $ProtectionGroup

    if (-not [string]::IsNullOrWhiteSpace($policyId) -and $PolicyMap.ContainsKey($policyId)) {
        return $PolicyMap[$policyId]
    }

    $policyName = FirstValue @(
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policyInfo.name"),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policy.name"),
        (Get-NestedPropValue -Object $ProtectionGroup -Path "policyConfig.name"),
        (Get-PropValue -Object $ProtectionGroup -Names @("policyName"))
    )

    if (-not [string]::IsNullOrWhiteSpace($policyName)) {
        if (-not (Test-LooksLikeId -Value $policyName)) { return $policyName }
        if ($PolicyMap.ContainsKey($policyName)) { return $PolicyMap[$policyName] }
    }

    return "UNRESOLVED_POLICY_NAME"
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
        $groups = Get-PropValue -Object $json -Names @("protectionGroups")

        if ($groups) { $all += @(As-Array $groups | Where-Object { $_ }) }

        $cookie = FirstValue @((Get-PropValue -Object $json -Names @("paginationCookie")))
        $isResponseTruncated = Get-PropValue -Object $json -Names @("isResponseTruncated")

        if ($isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    } while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-EnvironmentParams {
    param(
        $ProtectionGroup,
        [string[]]$ParamNames
    )

    foreach ($name in @($ParamNames)) {
        $value = Get-PropValue -Object $ProtectionGroup -Names @($name)
        if ($null -ne $value) { return $value }
    }

    return $null
}

function Get-ObjectsFromParams {
    param(
        $Params,
        [string]$EnvironmentApiName
    )

    if ($null -eq $Params) { return @() }

    if ($EnvironmentApiName -eq "kPhysical") {
        $protectionType = FirstValue @((Get-PropValue -Object $Params -Names @("protectionType")))
        if ($protectionType -eq "kVolume") {
            return @(As-Array (Get-NestedPropValue -Object $Params -Path "volumeProtectionTypeParams.objects") | Where-Object { $_ })
        }

        return @(As-Array (Get-NestedPropValue -Object $Params -Path "fileProtectionTypeParams.objects") | Where-Object { $_ })
    }

    $directCandidates = @(
        (Get-PropValue -Object $Params -Names @("objects")),
        (Get-PropValue -Object $Params -Names @("sourceObjects")),
        (Get-PropValue -Object $Params -Names @("virtualMachines")),
        (Get-PropValue -Object $Params -Names @("vms")),
        (Get-PropValue -Object $Params -Names @("vmObjects")),
        (Get-PropValue -Object $Params -Names @("protectedObjects")),
        (Get-PropValue -Object $Params -Names @("selectedObjects")),
        (Get-PropValue -Object $Params -Names @("entities"))
    )

    foreach ($candidate in $directCandidates) {
        $items = @(As-Array $candidate | Where-Object { $_ })
        if ($items.Count -gt 0) { return @(Get-DedupedObjects -Objects $items) }
    }

    $recursive = @(Find-InventoryObjectsRecursive -Node $Params -Depth 0)
    return @(Get-DedupedObjects -Objects $recursive)
}

function Get-RunInfo {
    param($ProtectionGroup)

    $lastRun = Get-PropValue -Object $ProtectionGroup -Names @("lastRun")
    $localInfo = Get-PropValue -Object $lastRun -Names @("localBackupInfo", "localSnapshotInfo")

    $status = FirstValue @(
        (Get-PropValue -Object $localInfo -Names @("status")),
        (Get-PropValue -Object $lastRun -Names @("status"))
    )

    $runType = FirstValue @(
        (Get-PropValue -Object $localInfo -Names @("runType")),
        (Get-PropValue -Object $lastRun -Names @("runType"))
    )

    $startUsecs = FirstValue @(
        (Get-PropValue -Object $localInfo -Names @("startTimeUsecs")),
        (Get-PropValue -Object $lastRun -Names @("startTimeUsecs"))
    )

    $endUsecs = FirstValue @(
        (Get-PropValue -Object $localInfo -Names @("endTimeUsecs")),
        (Get-PropValue -Object $lastRun -Names @("endTimeUsecs"))
    )

    $startET = UsecsToET $startUsecs
    $endET = UsecsToET $endUsecs

    $lastSuccessET = ""
    $lastSuccessStatus = ""

    if (Is-SuccessStatus $status) {
        $lastSuccessET = $endET
        $lastSuccessStatus = $status
    }

    $ageHours = Get-AgeHoursFromET $lastSuccessET

    return [PSCustomObject]@{
        LastRunStatus                = $status
        LastRunType                  = $runType
        LastRunStartET               = $startET
        LastRunEndET                 = $endET
        LastSuccessfulBackupET       = $lastSuccessET
        LastSuccessfulBackupStatus   = $lastSuccessStatus
        LastSuccessfulBackupAgeHours = $ageHours
        BackupFreshnessBucket        = Get-FreshnessBucket $ageHours
        IsSuccessLast24h             = (($ageHours -ne "") -and ([double]$ageHours -le 24))
        IsSuccessLast48h             = (($ageHours -ne "") -and ([double]$ageHours -le 48))
    }
}

function New-ExceptionRow {
    param(
        [string]$PGKey,
        [string]$ObjectKey,
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
        PGKey             = $PGKey
        ObjectKey         = $ObjectKey
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
$json_clu = @(As-Array (Get-PropValue -Object $cluJson -Names @("cohesityClusters")))

if (-not $json_clu -or $json_clu.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = $json_clu | ForEach-Object {
    $name = FirstValue @(
        (Get-PropValue -Object $_ -Names @("clusterName")),
        (Get-PropValue -Object $_ -Names @("displayName")),
        (Get-PropValue -Object $_ -Names @("name"))
    )
    $cid = FirstValue @(
        (Get-PropValue -Object $_ -Names @("clusterId")),
        (Get-PropValue -Object $_ -Names @("id"))
    )

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
    if (-not [int]::TryParse($selection, [ref]$n)) { Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red; continue }
    if ($n -lt 0 -or $n -gt $clusterMenu.Count) { Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red; continue }

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
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $envSelection = Read-Host "Select environment scope"
    if ($envSelection -match '^(x|X|q|Q)$') { return }
    if ($envSelection -eq "0") { $selectedEnvironments = @($EnvironmentMap); break }
    if ($envSelection -eq "1") { $selectedEnvironments = @($EnvironmentMap | Where-Object { $_.ApiName -eq "kPhysical" }); break }
    if ($envSelection -eq "2") { $selectedEnvironments = @($EnvironmentMap | Where-Object { $_.ApiName -eq "kHyperV" }); break }
    if ($envSelection -eq "3") { $selectedEnvironments = @($EnvironmentMap | Where-Object { $_.ApiName -eq "kAcropolis" }); break }
    Write-Host "Enter 0, 1, 2, 3, or X." -ForegroundColor Red
}

# -------------------------------
# Collect inventory
# -------------------------------
$pgSummaryRows = @()
$objectDetailRows = @()
$pathDetailRows = @()
$exceptionRows = @()
$collectionErrors = @()

foreach ($c in $selectedClusters) {
    $clusterName = $c.ClusterName
    $clusterId = $c.ClusterId
    $headers = New-Headers -ClusterId $clusterId
    $policyMap = Get-PolicyMap -Headers $headers

    foreach ($env in $selectedEnvironments) {
        Write-Host "Collecting $($env.DisplayName) PGs from $clusterName ..." -ForegroundColor Yellow

        try {
            $pgs = @(Get-ProtectionGroups -EnvironmentApiName $env.ApiName -Headers $headers)
        }
        catch {
            $collectionErrors += [PSCustomObject]@{ Cluster = $clusterName; Environment = $env.DisplayName; Stage = "Get-ProtectionGroups"; Error = $_.Exception.Message }
            Write-Host "Failed to collect $($env.DisplayName) from $clusterName : $($_.Exception.Message)" -ForegroundColor Red
            continue
        }

        foreach ($pg in @($pgs | Where-Object { $_ })) {
            try {
                $params = Get-EnvironmentParams -ProtectionGroup $pg -ParamNames @($env.ParamNames)
                if ($null -eq $params) {
                    $collectionErrors += [PSCustomObject]@{ Cluster = $clusterName; Environment = $env.DisplayName; Stage = "EnvironmentParams"; Error = "No matching environment params found on protection group." }
                    continue
                }

                $pgId = FirstValue @((Get-PropValue -Object $pg -Names @("id")), (Get-PropValue -Object $pg -Names @("protectionGroupId")))
                $pgName = FirstValue @((Get-PropValue -Object $pg -Names @("name")), (Get-PropValue -Object $pg -Names @("protectionGroupName")), $pgId)
                $pgKey = Get-PGKey -ClusterId $clusterId -ProtectionGroupId $pgId -ClusterName $clusterName -ProtectionGroupName $pgName
                $policyId = Resolve-PolicyId -ProtectionGroup $pg
                $policyName = Resolve-PolicyName -ProtectionGroup $pg -PolicyMap $policyMap
                $runInfo = Get-RunInfo -ProtectionGroup $pg
                $objects = @(Get-ObjectsFromParams -Params $params -EnvironmentApiName $env.ApiName)

                $globalExcludePaths = ""
                $globalExcludeCount = 0
                $objectExcludePathCount = 0
                $hasGlobalExclusions = $false
                $hasObjectExclusions = $false
                $protectionType = ""

                if ($env.ApiName -eq "kPhysical") {
                    $protectionType = FirstValue @((Get-PropValue -Object $params -Names @("protectionType")))
                    if ($protectionType -ne "kVolume") {
                        $globalExcludePaths = Flat (Get-NestedPropValue -Object $params -Path "fileProtectionTypeParams.globalExcludePaths")
                        $globalExcludeCount = Count-FlatItems (Get-NestedPropValue -Object $params -Path "fileProtectionTypeParams.globalExcludePaths")
                        if ($globalExcludeCount -gt 0) { $hasGlobalExclusions = $true }

                        foreach ($obj in $objects) {
                            foreach ($fp in @(As-Array (Get-PropValue -Object $obj -Names @("filePaths")) | Where-Object { $_ })) {
                                $objectExcludePathCount += Count-FlatItems (Get-PropValue -Object $fp -Names @("excludedPaths"))
                            }
                        }
                        if ($objectExcludePathCount -gt 0) { $hasObjectExclusions = $true }
                    }
                }
                else {
                    $protectionType = "VirtualMachine"
                }

                $pgSummaryRows += [PSCustomObject]@{
                    PGKey                        = $pgKey
                    InventoryDateET              = $InventoryDateET
                    Cluster                      = $clusterName
                    ClusterId                    = $clusterId
                    Environment                  = $env.DisplayName
                    ProtectionGroup              = $pgName
                    ProtectionGroupId            = $pgId
                    PolicyName                   = $policyName
                    PolicyId                     = $policyId
                    IsActive                     = Get-PropValue -Object $pg -Names @("isActive")
                    IsDeleted                    = Get-PropValue -Object $pg -Names @("isDeleted")
                    ObjectCount                  = @($objects).Count
                    GlobalExcludePathCount       = $globalExcludeCount
                    ObjectExcludePathCount       = $objectExcludePathCount
                    HasGlobalExclusions          = $hasGlobalExclusions
                    HasObjectExclusions          = $hasObjectExclusions
                    LastSuccessfulBackupET       = $runInfo.LastSuccessfulBackupET
                    LastSuccessfulBackupStatus   = $runInfo.LastSuccessfulBackupStatus
                    LastSuccessfulBackupAgeHours = $runInfo.LastSuccessfulBackupAgeHours
                    BackupFreshnessBucket        = $runInfo.BackupFreshnessBucket
                    IsSuccessLast24h             = $runInfo.IsSuccessLast24h
                    IsSuccessLast48h             = $runInfo.IsSuccessLast48h
                    LastRunStatus                = $runInfo.LastRunStatus
                    LastRunType                  = $runInfo.LastRunType
                    LastRunStartET               = $runInfo.LastRunStartET
                    LastRunEndET                 = $runInfo.LastRunEndET
                    IsPaused                     = Get-PropValue -Object $pg -Names @("isPaused")
                    ProtectionType               = $protectionType
                    StorageDomain                = FirstValue @((Get-PropValue -Object $pg -Names @("storageDomainName")), (Get-NestedPropValue -Object $pg -Path "storageDomain.name"), (Get-NestedPropValue -Object $pg -Path "storageDomain.id"))
                    SourceName                   = FirstValue @((Get-PropValue -Object $pg -Names @("sourceName")), (Get-NestedPropValue -Object $pg -Path "source.name"), (Get-PropValue -Object $params -Names @("sourceName")), (Get-NestedPropValue -Object $params -Path "source.name"))
                }

                if (@($objects).Count -eq 0) {
                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "PG_ZERO_OBJECTS" -Severity "High" -ExceptionReason "Protection group has zero objects." -RecommendedAction "Confirm whether the protection group is intentionally empty or object discovery failed."
                }

                if ([string]::IsNullOrWhiteSpace($policyName) -or $policyName -eq "UNRESOLVED_POLICY_NAME") {
                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "MISSING_POLICY" -Severity "Critical" -ExceptionReason "Protection group policy name could not be resolved." -RecommendedAction "Check policy assignment and policy API visibility."
                }

                if ([string]::IsNullOrWhiteSpace($runInfo.LastSuccessfulBackupET)) {
                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "MISSING_LAST_SUCCESS" -Severity "Critical" -ExceptionReason "No successful backup timestamp was found from latest run information." -RecommendedAction "Check recent runs. Future enhancement should scan prior runs for latest success."
                }
                elseif ($runInfo.LastSuccessfulBackupAgeHours -ne "") {
                    if ([double]$runInfo.LastSuccessfulBackupAgeHours -gt 48) {
                        $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "LAST_SUCCESS_GT_48H" -Severity "Critical" -ExceptionReason "Last successful backup is older than 48 hours." -RecommendedAction "Investigate job schedule, failures, pause state, and object availability."
                    }
                    elseif ([double]$runInfo.LastSuccessfulBackupAgeHours -gt 24) {
                        $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "LAST_SUCCESS_GT_24H" -Severity "High" -ExceptionReason "Last successful backup is older than 24 hours." -RecommendedAction "Review backup freshness and recent run status."
                    }
                }

                if ($hasGlobalExclusions) {
                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "PG_GLOBAL_EXCLUSIONS" -Severity "Medium" -ExceptionReason "Protection group has global exclude paths." -RecommendedAction "Review whether global exclusions are approved and documented."
                }

                if ($objectExcludePathCount -gt 20) {
                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "HIGH_EXCLUSION_COUNT" -Severity "Medium" -ExceptionReason "Protection group has a high number of object-level exclusions." -RecommendedAction "Review exclusions for audit and operational correctness."
                }

                if (@($objects).Count -gt 250) {
                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey "" -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName "" -ObjectName "" -ExceptionType "VERY_LARGE_PG" -Severity "Medium" -ExceptionReason "Protection group has more than 250 objects." -RecommendedAction "Review whether the PG should be split for operations, run duration, and blast-radius control."
                }

                foreach ($obj in $objects) {
                    $objectName = Get-ObjectName -Object $obj
                    $objectId = Get-ObjectId -Object $obj
                    $objectKey = Get-ObjectKey -PGKey $pgKey -ObjectId $objectId -ObjectName $objectName
                    $hostName = FirstValue @((Get-PropValue -Object $obj -Names @("hostName")), (Get-PropValue -Object $obj -Names @("sourceName")), (Get-PropValue -Object $obj -Names @("parentSourceName")), $objectName)
                    $objectType = FirstValue @((Get-PropValue -Object $obj -Names @("objectType")), (Get-PropValue -Object $obj -Names @("type")), (Get-PropValue -Object $obj -Names @("entityType")))
                    if ([string]::IsNullOrWhiteSpace($objectType)) {
                        if ($env.ApiName -eq "kPhysical") { $objectType = "PhysicalObject" }
                        else { $objectType = "VirtualMachine" }
                    }

                    $includedPathCount = 0
                    $objExcludeCount = 0
                    $objHasExclusions = $false
                    $objHasIncludedPath = $true

                    if ($env.ApiName -eq "kPhysical") {
                        if ($protectionType -eq "kVolume") {
                            $includedPathCount = Count-FlatItems (Get-PropValue -Object $obj -Names @("volumeGuids"))
                        }
                        else {
                            $filePaths = @(As-Array (Get-PropValue -Object $obj -Names @("filePaths")) | Where-Object { $_ })
                            $includedPathCount = @($filePaths | Where-Object { -not [string]::IsNullOrWhiteSpace((Get-PropValue -Object $_ -Names @("includedPath"))) }).Count
                            foreach ($fp in $filePaths) { $objExcludeCount += Count-FlatItems (Get-PropValue -Object $fp -Names @("excludedPaths")) }
                        }

                        if ($includedPathCount -eq 0) { $objHasIncludedPath = $false }
                        if ($objExcludeCount -gt 0) { $objHasExclusions = $true }
                    }

                    $objectDetailRows += [PSCustomObject]@{
                        ObjectKey                  = $objectKey
                        PGKey                      = $pgKey
                        InventoryDateET            = $InventoryDateET
                        Cluster                    = $clusterName
                        ClusterId                  = $clusterId
                        Environment                = $env.DisplayName
                        ProtectionGroup            = $pgName
                        ProtectionGroupId          = $pgId
                        PolicyName                 = $policyName
                        HostName                   = $hostName
                        ObjectName                 = $objectName
                        ObjectType                 = $objectType
                        ObjectId                   = $objectId
                        ParentSource               = FirstValue @((Get-PropValue -Object $obj -Names @("parentSourceName")), (Get-PropValue -Object $obj -Names @("sourceName")), (Get-NestedPropValue -Object $obj -Path "parentSource.id"), (Get-PropValue -Object $obj -Names @("sourceId")))
                        IncludedPathCount          = $includedPathCount
                        ObjectExcludePathCount     = $objExcludeCount
                        HasGlobalExclusions        = $hasGlobalExclusions
                        HasObjectExclusions        = $objHasExclusions
                        LastSuccessfulBackupET     = $runInfo.LastSuccessfulBackupET
                        LastSuccessfulBackupStatus = $runInfo.LastSuccessfulBackupStatus
                    }

                    if ($env.ApiName -eq "kPhysical") {
                        if ($protectionType -eq "kVolume") {
                            $volumePaths = Flat (Get-PropValue -Object $obj -Names @("volumeGuids"))
                            $pathDetailRows += [PSCustomObject]@{
                                PathKey            = "$objectKey|volume"
                                PGKey              = $pgKey
                                ObjectKey          = $objectKey
                                InventoryDateET    = $InventoryDateET
                                Cluster            = $clusterName
                                Environment        = $env.DisplayName
                                ProtectionGroup    = $pgName
                                HostName           = $hostName
                                ObjectName         = $objectName
                                IncludedPath       = $volumePaths
                                ExcludedPath       = ""
                                ExclusionLevel     = "None"
                                SkipNestedVolumes  = ""
                                GlobalExcludePaths = $globalExcludePaths
                            }
                        }
                        else {
                            $filePaths = @(As-Array (Get-PropValue -Object $obj -Names @("filePaths")) | Where-Object { $_ })
                            foreach ($fp in $filePaths) {
                                $includedPath = FirstValue @((Get-PropValue -Object $fp -Names @("includedPath")))
                                $skipNested = Get-PropValue -Object $fp -Names @("skipNestedVolumes")
                                $excludedPaths = @(As-Array (Get-PropValue -Object $fp -Names @("excludedPaths")) | Where-Object { $_ })

                                if ($excludedPaths.Count -eq 0) {
                                    $pathDetailRows += [PSCustomObject]@{
                                        PathKey            = "$objectKey|$includedPath|none"
                                        PGKey              = $pgKey
                                        ObjectKey          = $objectKey
                                        InventoryDateET    = $InventoryDateET
                                        Cluster            = $clusterName
                                        Environment        = $env.DisplayName
                                        ProtectionGroup    = $pgName
                                        HostName           = $hostName
                                        ObjectName         = $objectName
                                        IncludedPath       = $includedPath
                                        ExcludedPath       = ""
                                        ExclusionLevel     = "None"
                                        SkipNestedVolumes  = $skipNested
                                        GlobalExcludePaths = $globalExcludePaths
                                    }
                                }
                                else {
                                    foreach ($excludedPath in $excludedPaths) {
                                        $pathDetailRows += [PSCustomObject]@{
                                            PathKey            = "$objectKey|$includedPath|$excludedPath"
                                            PGKey              = $pgKey
                                            ObjectKey          = $objectKey
                                            InventoryDateET    = $InventoryDateET
                                            Cluster            = $clusterName
                                            Environment        = $env.DisplayName
                                            ProtectionGroup    = $pgName
                                            HostName           = $hostName
                                            ObjectName         = $objectName
                                            IncludedPath       = $includedPath
                                            ExcludedPath       = $excludedPath
                                            ExclusionLevel     = "Object"
                                            SkipNestedVolumes  = $skipNested
                                            GlobalExcludePaths = $globalExcludePaths
                                        }
                                    }
                                }

                                if ($skipNested -eq $true) {
                                    $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey $objectKey -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName $hostName -ObjectName $objectName -ExceptionType "SKIP_NESTED_VOLUMES" -Severity "Medium" -ExceptionReason "SkipNestedVolumes is enabled for an included path." -RecommendedAction "Validate whether nested volume exclusion is expected and approved."
                                }
                            }
                        }

                        if (-not $objHasIncludedPath) {
                            $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey $objectKey -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName $hostName -ObjectName $objectName -ExceptionType "OBJECT_NO_INCLUDED_PATH" -Severity "High" -ExceptionReason "Physical object has no included path." -RecommendedAction "Review object include path configuration."
                        }

                        if ($objHasExclusions) {
                            $exceptionRows += New-ExceptionRow -PGKey $pgKey -ObjectKey $objectKey -Cluster $clusterName -Environment $env.DisplayName -ProtectionGroup $pgName -HostName $hostName -ObjectName $objectName -ExceptionType "OBJECT_LEVEL_EXCLUSIONS" -Severity "Medium" -ExceptionReason "Object has object-level exclude paths." -RecommendedAction "Review object-level exclusions for audit and operational correctness."
                        }
                    }
                }
            }
            catch {
                $collectionErrors += [PSCustomObject]@{ Cluster = $clusterName; Environment = $env.DisplayName; Stage = "ProcessProtectionGroup"; Error = $_.Exception.Message }
                Write-Host "Failed processing a $($env.DisplayName) PG on $clusterName : $($_.Exception.Message)" -ForegroundColor Red
                continue
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
    InventoryDateET      = $InventoryDateET
    ScriptName           = "Get-CohesityProtectionInventory.ps1"
    HeliosBaseUrl        = $baseUrl
    SelectedClusters     = @($selectedClusters | Select-Object ClusterName, ClusterId)
    SelectedEnvironments = @($selectedEnvironments | Select-Object ApiName, DisplayName)
    OutputFiles          = [PSCustomObject]@{
        PGSummary    = $pgSummaryCsv
        ObjectDetail = $objectDetailCsv
        PathDetail   = $pathDetailCsv
        Exceptions   = $exceptionsCsv
    }
    Counts               = [PSCustomObject]@{
        PGSummaryRows    = @($pgSummaryRows).Count
        ObjectDetailRows = @($objectDetailRows).Count
        PathDetailRows   = @($pathDetailRows).Count
        ExceptionRows    = @($exceptionRows).Count
        CollectionErrors = @($collectionErrors).Count
    }
    EnvironmentCounts    = @($pgSummaryRows | Group-Object Environment | Select-Object Name, Count)
    CollectionErrors     = @($collectionErrors)
    Notes                = @(
        "GET-only baseline collector.",
        "Physical path detail is populated.",
        "Hyper-V and Nutanix AHV use object-level VM inventory; path detail is not forced.",
        "Cluster parsing uses documented clusterName and clusterId fields from /v2/mcm/cluster-mgmt/info.",
        "Pagination fields are read safely because some API responses omit paginationCookie or isResponseTruncated.",
        "LastSuccessfulBackupET currently uses latest run information when the latest run status is success. Recent-run success scanning is a future enhancement."
    )
}

$metadata | ConvertTo-Json -Depth 10 | Out-File -FilePath $metadataJson -Encoding utf8

Write-Host ""
Write-Host "Cohesity Protection Inventory export complete." -ForegroundColor Green
Write-Host "PG Summary rows    : $(@($pgSummaryRows).Count)" -ForegroundColor Green
Write-Host "Object Detail rows : $(@($objectDetailRows).Count)" -ForegroundColor Green
Write-Host "Path Detail rows   : $(@($pathDetailRows).Count)" -ForegroundColor Green
Write-Host "Exception rows     : $(@($exceptionRows).Count)" -ForegroundColor Green
Write-Host "Collection errors  : $(@($collectionErrors).Count)" -ForegroundColor Green
Write-Host "PG Summary CSV     : $pgSummaryCsv" -ForegroundColor Green
Write-Host "Object Detail CSV  : $objectDetailCsv" -ForegroundColor Green
Write-Host "Path Detail CSV    : $pathDetailCsv" -ForegroundColor Green
Write-Host "Exceptions CSV     : $exceptionsCsv" -ForegroundColor Green
Write-Host "Metadata JSON      : $metadataJson" -ForegroundColor Green
