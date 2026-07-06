# Cohesity Helios - Protection Inventory Framework
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
# Environments: Physical, Hyper-V, Nutanix AHV

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$outDir = "X:\PowerShell\Cohesity_API_Scripts\inventory"
$baseUrl = "https://helios.cohesity.com"
$helperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

$EnvironmentMap = @(
    [PSCustomObject]@{ ApiName = "kPhysical";  DisplayName = "Physical";    ParamNames = @("physicalParams") },
    [PSCustomObject]@{ ApiName = "kHyperV";    DisplayName = "Hyper-V";     ParamNames = @("hypervParams", "hyperVParams") },
    [PSCustomObject]@{ ApiName = "kAcropolis"; DisplayName = "Nutanix AHV"; ParamNames = @("acropolisParams", "nutanixParams", "ahvParams") }
)

if (-not (Test-Path -Path $outDir -PathType Container)) { New-Item -Path $outDir -ItemType Directory -Force | Out-Null }
if (-not (Test-Path $helperPath)) { throw "API key helper not found at $helperPath" }
if (-not (Test-Path $encryptedApiKeyPath)) { throw "Encrypted API key file not found at $encryptedApiKeyPath" }

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "AES API key helper returned an empty API key." }

function Get-InventoryDateET {
    try {
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        return ([TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)).ToString("yyyy-MM-dd HH:mm:ss")
    } catch { return (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }
}
$InventoryDateET = Get-InventoryDateET

function New-Headers {
    param([string]$ClusterId)
    $h = @{ accept = "application/json"; apiKey = $apiKey }
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) { $h["accessClusterId"] = $ClusterId }
    return $h
}

function Get-Json {
    param([string]$Uri, [hashtable]$Headers)
    if ($PSVersionTable.PSVersion.Major -lt 6) { $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing }
    else { $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get }
    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function As-Array { param($Value) if ($null -eq $Value) { return @() } return @($Value) }

function Get-PropValue {
    param($Object, [string[]]$Names)
    if ($null -eq $Object -or $Object -is [string]) { return $null }
    foreach ($n in @($Names)) {
        foreach ($p in @($Object.PSObject.Properties)) {
            if ($p.Name -ieq $n) { return $p.Value }
        }
    }
    return $null
}

function Get-NestedPropValue {
    param($Object, [string]$Path)
    if ($null -eq $Object -or [string]::IsNullOrWhiteSpace($Path)) { return $null }
    $cur = $Object
    foreach ($part in ($Path -split "\.")) {
        if ($null -eq $cur -or $cur -is [string]) { return $null }
        $cur = Get-PropValue -Object $cur -Names @($part)
    }
    return $cur
}

function FirstValue {
    param($Values)
    foreach ($v in @($Values)) { foreach ($vv in @($v)) { if ($null -ne $vv -and "$vv".Trim() -ne "") { return "$vv" } } }
    return ""
}

function Flat {
    param($Value)
    if ($null -eq $Value) { return "" }
    $items = @()
    foreach ($v in @($Value)) { foreach ($vv in @($v)) { if ($null -ne $vv -and "$vv".Trim() -ne "") { $items += "$vv" } } }
    if ($items.Count -eq 0) { return "" }
    return (($items | Select-Object -Unique) -join ";")
}

function Count-FlatItems { param($Value) $f = Flat $Value; if ([string]::IsNullOrWhiteSpace($f)) { return 0 }; return @($f -split ";" | Where-Object { $_ }).Count }

function UsecsToET {
    param($Usecs)
    if ($null -eq $Usecs -or "$Usecs".Trim() -eq "" -or "$Usecs" -eq "0") { return "" }
    try {
        $epoch = [DateTime]::SpecifyKind([datetime]"1970-01-01 00:00:00", [DateTimeKind]::Utc)
        $utc = $epoch.AddSeconds(([double]$Usecs / 1000000))
        $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        return ([TimeZoneInfo]::ConvertTimeFromUtc($utc, $tz)).ToString("yyyy-MM-dd HH:mm:ss")
    } catch { return "" }
}

function Get-AgeHoursFromET {
    param([string]$DateET)
    if ([string]::IsNullOrWhiteSpace($DateET)) { return "" }
    try {
        $dt = [datetime]::ParseExact($DateET, "yyyy-MM-dd HH:mm:ss", $null)
        $now = [datetime]::ParseExact($InventoryDateET, "yyyy-MM-dd HH:mm:ss", $null)
        return [math]::Round(($now - $dt).TotalHours, 2)
    } catch { return "" }
}

function Is-SuccessStatus { param([string]$Status) if ([string]::IsNullOrWhiteSpace($Status)) { return $false }; return (@("ksuccess", "success", "succeeded") -contains $Status.Trim().ToLower()) }
function LooksLikeId { param([string]$Value) if ([string]::IsNullOrWhiteSpace($Value)) { return $false }; return ($Value -match '^[a-fA-F0-9]{24,}$' -or $Value -match '^[0-9]+:[0-9]+:[0-9]+$') }

function Get-PolicyMap {
    param([hashtable]$Headers)
    $map = @{}
    foreach ($uri in @("$baseUrl/v2/data-protect/policies?maxResultCount=1000", "$baseUrl/v2/data-protect/policies")) {
        try {
            $j = Get-Json -Uri $uri -Headers $Headers
            $arr = Get-PropValue -Object $j -Names @("policies", "policyList", "items")
            if (-not $arr) { $arr = $j }
            foreach ($p in @(As-Array $arr | Where-Object { $_ -and $_ -isnot [string] })) {
                $id = FirstValue @((Get-PropValue -Object $p -Names @("id", "policyId")))
                $name = FirstValue @((Get-PropValue -Object $p -Names @("name", "policyName", "displayName")))
                if ($id -and $name) { $map[$id] = $name }
            }
            if ($map.Count -gt 0) { break }
        } catch { }
    }
    return $map
}

function Resolve-PolicyId { param($PG) return FirstValue @((Get-PropValue -Object $PG -Names @("policyId")), (Get-NestedPropValue -Object $PG -Path "policyInfo.id"), (Get-NestedPropValue -Object $PG -Path "policy.id")) }

function Resolve-PolicyName {
    param($PG, [hashtable]$PolicyMap)
    $pid = Resolve-PolicyId $PG
    if ($pid -and $PolicyMap.ContainsKey($pid)) { return $PolicyMap[$pid] }
    $name = FirstValue @((Get-NestedPropValue -Object $PG -Path "policyInfo.name"), (Get-NestedPropValue -Object $PG -Path "policy.name"), (Get-NestedPropValue -Object $PG -Path "policyConfig.name"), (Get-PropValue -Object $PG -Names @("policyName")))
    if ($name -and -not (LooksLikeId $name)) { return $name }
    if ($name -and $PolicyMap.ContainsKey($name)) { return $PolicyMap[$name] }
    return "UNRESOLVED_POLICY_NAME"
}

function Get-ProtectionGroups {
    param([string]$EnvironmentApiName, [hashtable]$Headers)
    $all = @(); $cookie = ""
    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=$EnvironmentApiName&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
        if ($cookie) { $uri = "$uri&paginationCookie=$([uri]::EscapeDataString($cookie))" }
        $j = Get-Json -Uri $uri -Headers $Headers
        $groups = Get-PropValue -Object $j -Names @("protectionGroups")
        if ($groups) { $all += @(As-Array $groups | Where-Object { $_ }) }
        $cookie = FirstValue @((Get-PropValue -Object $j -Names @("paginationCookie")))
        $truncated = Get-PropValue -Object $j -Names @("isResponseTruncated")
        if ($truncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    } while ($cookie)
    return @($all)
}

function Get-EnvParams {
    param($PG, [string[]]$ParamNames)
    foreach ($n in $ParamNames) { $v = Get-PropValue -Object $PG -Names @($n); if ($null -ne $v) { return $v } }
    return $null
}

function ObjectName { param($Obj) return FirstValue @((Get-PropValue -Object $Obj -Names @("name", "objectName", "sourceName", "vmName", "hostName", "displayName", "id"))) }
function ObjectId { param($Obj) return FirstValue @((Get-PropValue -Object $Obj -Names @("id", "objectId", "sourceId", "vmId", "entityId"))) }

function LooksLikeObject { param($Obj) if ($null -eq $Obj -or $Obj -is [string]) { return $false }; return [bool](ObjectName $Obj) }

function Find-Objects {
    param($Node, [int]$Depth)
    $out = @()
    if ($null -eq $Node -or $Node -is [string] -or $Depth -gt 3) { return @() }
    foreach ($i in @(As-Array $Node)) {
        if ($null -eq $i -or $i -is [string]) { continue }
        if (LooksLikeObject $i) { $out += $i }
        foreach ($p in @($i.PSObject.Properties)) {
            if ($p.Value -and $p.Value -isnot [string]) { $out += @(Find-Objects -Node $p.Value -Depth ($Depth + 1)) }
        }
    }
    return @($out)
}

function DedupeObjects {
    param($Objects)
    $seen = @{}; $out = @()
    foreach ($o in @(As-Array $Objects | Where-Object { $_ -and $_ -isnot [string] })) {
        $key = FirstValue @((ObjectId $o), (ObjectName $o))
        if ($key -and -not $seen.ContainsKey($key)) { $seen[$key] = $true; $out += $o }
    }
    return @($out)
}

function Get-ObjectsFromParams {
    param($Params, [string]$EnvironmentApiName)
    if ($null -eq $Params) { return @() }
    if ($EnvironmentApiName -eq "kPhysical") {
        $pt = FirstValue @((Get-PropValue -Object $Params -Names @("protectionType")))
        if ($pt -eq "kVolume") { return @(As-Array (Get-NestedPropValue -Object $Params -Path "volumeProtectionTypeParams.objects") | Where-Object { $_ }) }
        return @(As-Array (Get-NestedPropValue -Object $Params -Path "fileProtectionTypeParams.objects") | Where-Object { $_ })
    }
    foreach ($name in @("objects", "sourceObjects", "virtualMachines", "vms", "vmObjects", "protectedObjects", "selectedObjects", "entities")) {
        $items = @(As-Array (Get-PropValue -Object $Params -Names @($name)) | Where-Object { $_ })
        if ($items.Count -gt 0) { return @(DedupeObjects $items) }
    }
    return @(DedupeObjects (Find-Objects -Node $Params -Depth 0))
}

function Get-RunInfo {
    param($PG)
    $lr = Get-PropValue -Object $PG -Names @("lastRun")
    $li = Get-PropValue -Object $lr -Names @("localBackupInfo", "localSnapshotInfo")
    $status = FirstValue @((Get-PropValue -Object $li -Names @("status")), (Get-PropValue -Object $lr -Names @("status")))
    $rtype = FirstValue @((Get-PropValue -Object $li -Names @("runType")), (Get-PropValue -Object $lr -Names @("runType")))
    $start = UsecsToET (FirstValue @((Get-PropValue -Object $li -Names @("startTimeUsecs")), (Get-PropValue -Object $lr -Names @("startTimeUsecs"))))
    $end = UsecsToET (FirstValue @((Get-PropValue -Object $li -Names @("endTimeUsecs")), (Get-PropValue -Object $lr -Names @("endTimeUsecs"))))
    $successET = ""; $successStatus = ""
    if (Is-SuccessStatus $status) { $successET = $end; $successStatus = $status }
    $age = Get-AgeHoursFromET $successET
    $bucket = "No Success Found"
    if ($age -ne "") { if ([double]$age -le 24) { $bucket = "<=24h" } elseif ([double]$age -le 48) { $bucket = "24-48h" } else { $bucket = ">48h" } }
    return [PSCustomObject]@{ LastRunStatus=$status; LastRunType=$rtype; LastRunStartET=$start; LastRunEndET=$end; LastSuccessfulBackupET=$successET; LastSuccessfulBackupStatus=$successStatus; LastSuccessfulBackupAgeHours=$age; BackupFreshnessBucket=$bucket; IsSuccessLast24h=(($age -ne "") -and ([double]$age -le 24)); IsSuccessLast48h=(($age -ne "") -and ([double]$age -le 48)) }
}

function New-ExceptionRow {
    param($PGKey, $ObjectKey, $Cluster, $Environment, $ProtectionGroup, $HostName, $ObjectName, $ExceptionType, $Severity, $ExceptionReason, $RecommendedAction)
    return [PSCustomObject]@{ InventoryDateET=$InventoryDateET; PGKey=$PGKey; ObjectKey=$ObjectKey; Cluster=$Cluster; Environment=$Environment; ProtectionGroup=$ProtectionGroup; HostName=$HostName; ObjectName=$ObjectName; ExceptionType=$ExceptionType; Severity=$Severity; ExceptionReason=$ExceptionReason; RecommendedAction=$RecommendedAction }
}

# Cluster menu
$clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$clustersRaw = @(As-Array (Get-PropValue -Object $clusterJson -Names @("cohesityClusters")))
if (-not $clustersRaw -or $clustersRaw.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = $clustersRaw | ForEach-Object {
    $cn = FirstValue @((Get-PropValue -Object $_ -Names @("clusterName")), (Get-PropValue -Object $_ -Names @("displayName")), (Get-PropValue -Object $_ -Names @("name")))
    $cid = FirstValue @((Get-PropValue -Object $_ -Names @("clusterId")), (Get-PropValue -Object $_ -Names @("id")))
    [PSCustomObject]@{ ClusterName=$cn; ClusterId=$cid }
} | Sort-Object ClusterName

$clusterMenu = for ($i=0; $i -lt $clusters.Count; $i++) { [PSCustomObject]@{ Index=$i+1; ClusterName=$clusters[$i].ClusterName; ClusterId=$clusters[$i].ClusterId } }
Write-Host "`nAvailable Helios Clusters (sorted):" -ForegroundColor Cyan
$clusterMenu | Format-Table -AutoSize
Write-Host "`n[0] All clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host "Select cluster: 0 for ALL, 1-$($clusterMenu.Count) for single, or X"
    if ($selection -match '^(x|X|q|Q)$') { return }
    $n = 0
    if (-not [int]::TryParse($selection, [ref]$n) -or $n -lt 0 -or $n -gt $clusterMenu.Count) { Write-Host "Invalid selection." -ForegroundColor Red; continue }
    if ($n -eq 0) { $selectedClusters = @($clusterMenu) } else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $n }) }
    break
}

Write-Host "`nEnvironment scope:" -ForegroundColor Cyan
Write-Host "[0] Physical + Hyper-V + Nutanix AHV" -ForegroundColor Yellow
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
    Write-Host "Invalid selection." -ForegroundColor Red
}

$pgRows = @(); $objRows = @(); $pathRows = @(); $exRows = @(); $errors = @()

foreach ($c in $selectedClusters) {
    $headers = New-Headers -ClusterId $c.ClusterId
    $policyMap = Get-PolicyMap -Headers $headers
    foreach ($env in $selectedEnvironments) {
        Write-Host "Collecting $($env.DisplayName) PGs from $($c.ClusterName) ..." -ForegroundColor Yellow
        try { $pgs = @(Get-ProtectionGroups -EnvironmentApiName $env.ApiName -Headers $headers) }
        catch { $errors += [PSCustomObject]@{Cluster=$c.ClusterName;Environment=$env.DisplayName;Stage="Get-ProtectionGroups";Error=$_.Exception.Message}; continue }

        foreach ($pg in $pgs) {
            try {
                $params = Get-EnvParams -PG $pg -ParamNames $env.ParamNames
                if ($null -eq $params) { $errors += [PSCustomObject]@{Cluster=$c.ClusterName;Environment=$env.DisplayName;Stage="EnvironmentParams";Error="No matching params"}; continue }
                $pgId = FirstValue @((Get-PropValue -Object $pg -Names @("id")), (Get-PropValue -Object $pg -Names @("protectionGroupId")))
                $pgName = FirstValue @((Get-PropValue -Object $pg -Names @("name")), (Get-PropValue -Object $pg -Names @("protectionGroupName")), $pgId)
                $pgKey = Get-PGKey -ClusterId $c.ClusterId -ProtectionGroupId $pgId -ClusterName $c.ClusterName -ProtectionGroupName $pgName
                $policyId = Resolve-PolicyId $pg
                $policyName = Resolve-PolicyName -PG $pg -PolicyMap $policyMap
                $run = Get-RunInfo $pg
                $objects = @(Get-ObjectsFromParams -Params $params -EnvironmentApiName $env.ApiName)
                $protectionType = if ($env.ApiName -eq "kPhysical") { FirstValue @((Get-PropValue -Object $params -Names @("protectionType"))) } else { "VirtualMachine" }
                $globalEx = ""; $globalExCount = 0; $objExCount = 0; $hasGlobal = $false; $hasObjEx = $false
                if ($env.ApiName -eq "kPhysical" -and $protectionType -ne "kVolume") {
                    $globalEx = Flat (Get-NestedPropValue -Object $params -Path "fileProtectionTypeParams.globalExcludePaths")
                    $globalExCount = Count-FlatItems $globalEx
                    $hasGlobal = ($globalExCount -gt 0)
                }

                $pgRows += [PSCustomObject]@{ PGKey=$pgKey; InventoryDateET=$InventoryDateET; Cluster=$c.ClusterName; ClusterId=$c.ClusterId; Environment=$env.DisplayName; ProtectionGroup=$pgName; ProtectionGroupId=$pgId; PolicyName=$policyName; PolicyId=$policyId; IsActive=(Get-PropValue -Object $pg -Names @("isActive")); IsDeleted=(Get-PropValue -Object $pg -Names @("isDeleted")); ObjectCount=@($objects).Count; GlobalExcludePathCount=$globalExCount; ObjectExcludePathCount=$objExCount; HasGlobalExclusions=$hasGlobal; HasObjectExclusions=$hasObjEx; LastSuccessfulBackupET=$run.LastSuccessfulBackupET; LastSuccessfulBackupStatus=$run.LastSuccessfulBackupStatus; LastSuccessfulBackupAgeHours=$run.LastSuccessfulBackupAgeHours; BackupFreshnessBucket=$run.BackupFreshnessBucket; IsSuccessLast24h=$run.IsSuccessLast24h; IsSuccessLast48h=$run.IsSuccessLast48h; LastRunStatus=$run.LastRunStatus; LastRunType=$run.LastRunType; LastRunStartET=$run.LastRunStartET; LastRunEndET=$run.LastRunEndET; IsPaused=(Get-PropValue -Object $pg -Names @("isPaused")); ProtectionType=$protectionType; StorageDomain=FirstValue @((Get-PropValue -Object $pg -Names @("storageDomainName")), (Get-NestedPropValue -Object $pg -Path "storageDomain.name")); SourceName=FirstValue @((Get-PropValue -Object $pg -Names @("sourceName")), (Get-NestedPropValue -Object $pg -Path "source.name")) }

                if (@($objects).Count -eq 0) { $exRows += New-ExceptionRow $pgKey "" $c.ClusterName $env.DisplayName $pgName "" "" "PG_ZERO_OBJECTS" "High" "Protection group has zero objects." "Confirm whether the protection group is intentionally empty or object discovery failed." }
                if ([string]::IsNullOrWhiteSpace($policyName) -or $policyName -eq "UNRESOLVED_POLICY_NAME") { $exRows += New-ExceptionRow $pgKey "" $c.ClusterName $env.DisplayName $pgName "" "" "MISSING_POLICY" "Critical" "Policy name could not be resolved." "Check policy assignment and policy API visibility." }
                if ([string]::IsNullOrWhiteSpace($run.LastSuccessfulBackupET)) { $exRows += New-ExceptionRow $pgKey "" $c.ClusterName $env.DisplayName $pgName "" "" "MISSING_LAST_SUCCESS" "Critical" "No successful backup timestamp found from latest run." "Check recent runs." }

                foreach ($obj in $objects) {
                    $on = ObjectName $obj; $oid = ObjectId $obj; $okey = Get-ObjectKey -PGKey $pgKey -ObjectId $oid -ObjectName $on
                    $host = FirstValue @((Get-PropValue -Object $obj -Names @("hostName")), (Get-PropValue -Object $obj -Names @("sourceName")), $on)
                    $otype = FirstValue @((Get-PropValue -Object $obj -Names @("objectType", "type", "entityType")))
                    if (-not $otype) { if ($env.ApiName -eq "kPhysical") { $otype = "PhysicalObject" } else { $otype = "VirtualMachine" } }
                    $incCount = 0; $exCount = 0; $objHasEx = $false
                    if ($env.ApiName -eq "kPhysical") {
                        if ($protectionType -eq "kVolume") { $incCount = Count-FlatItems (Get-PropValue -Object $obj -Names @("volumeGuids")) }
                        else {
                            $fps = @(As-Array (Get-PropValue -Object $obj -Names @("filePaths")) | Where-Object { $_ })
                            $incCount = @($fps | Where-Object { -not [string]::IsNullOrWhiteSpace((Get-PropValue -Object $_ -Names @("includedPath"))) }).Count
                            foreach ($fp in $fps) { $exCount += Count-FlatItems (Get-PropValue -Object $fp -Names @("excludedPaths")) }
                        }
                        $objHasEx = ($exCount -gt 0)
                    }
                    $objRows += [PSCustomObject]@{ ObjectKey=$okey; PGKey=$pgKey; InventoryDateET=$InventoryDateET; Cluster=$c.ClusterName; ClusterId=$c.ClusterId; Environment=$env.DisplayName; ProtectionGroup=$pgName; ProtectionGroupId=$pgId; PolicyName=$policyName; HostName=$host; ObjectName=$on; ObjectType=$otype; ObjectId=$oid; ParentSource=FirstValue @((Get-PropValue -Object $obj -Names @("parentSourceName")), (Get-PropValue -Object $obj -Names @("sourceName")), (Get-PropValue -Object $obj -Names @("sourceId"))); IncludedPathCount=$incCount; ObjectExcludePathCount=$exCount; HasGlobalExclusions=$hasGlobal; HasObjectExclusions=$objHasEx; LastSuccessfulBackupET=$run.LastSuccessfulBackupET; LastSuccessfulBackupStatus=$run.LastSuccessfulBackupStatus }
                    if ($env.ApiName -eq "kPhysical") {
                        $fps = @(As-Array (Get-PropValue -Object $obj -Names @("filePaths")) | Where-Object { $_ })
                        if ($protectionType -eq "kVolume") {
                            $pathRows += [PSCustomObject]@{ PathKey="$okey|volume"; PGKey=$pgKey; ObjectKey=$okey; InventoryDateET=$InventoryDateET; Cluster=$c.ClusterName; Environment=$env.DisplayName; ProtectionGroup=$pgName; HostName=$host; ObjectName=$on; IncludedPath=Flat (Get-PropValue -Object $obj -Names @("volumeGuids")); ExcludedPath=""; ExclusionLevel="None"; SkipNestedVolumes=""; GlobalExcludePaths=$globalEx }
                        } else {
                            foreach ($fp in $fps) {
                                $inc = FirstValue @((Get-PropValue -Object $fp -Names @("includedPath")))
                                $skip = Get-PropValue -Object $fp -Names @("skipNestedVolumes")
                                $exs = @(As-Array (Get-PropValue -Object $fp -Names @("excludedPaths")) | Where-Object { $_ })
                                if ($exs.Count -eq 0) { $pathRows += [PSCustomObject]@{ PathKey="$okey|$inc|none"; PGKey=$pgKey; ObjectKey=$okey; InventoryDateET=$InventoryDateET; Cluster=$c.ClusterName; Environment=$env.DisplayName; ProtectionGroup=$pgName; HostName=$host; ObjectName=$on; IncludedPath=$inc; ExcludedPath=""; ExclusionLevel="None"; SkipNestedVolumes=$skip; GlobalExcludePaths=$globalEx } }
                                else { foreach ($ex in $exs) { $pathRows += [PSCustomObject]@{ PathKey="$okey|$inc|$ex"; PGKey=$pgKey; ObjectKey=$okey; InventoryDateET=$InventoryDateET; Cluster=$c.ClusterName; Environment=$env.DisplayName; ProtectionGroup=$pgName; HostName=$host; ObjectName=$on; IncludedPath=$inc; ExcludedPath=$ex; ExclusionLevel="Object"; SkipNestedVolumes=$skip; GlobalExcludePaths=$globalEx } } }
                            }
                        }
                    }
                }
            } catch { $errors += [PSCustomObject]@{Cluster=$c.ClusterName;Environment=$env.DisplayName;Stage="ProcessProtectionGroup";Error=$_.Exception.Message}; continue }
        }
    }
}

$pgCsv = Join-Path $outDir "Cohesity_Protection_PG_Summary_Latest.csv"
$objCsv = Join-Path $outDir "Cohesity_Protection_Object_Detail_Latest.csv"
$pathCsv = Join-Path $outDir "Cohesity_Protection_Path_Detail_Latest.csv"
$exCsv = Join-Path $outDir "Cohesity_Protection_Exceptions_Latest.csv"
$metaJson = Join-Path $outDir "Cohesity_Protection_Run_Metadata.json"

$pgRows | Sort-Object Cluster, Environment, ProtectionGroup | Export-Csv -Path $pgCsv -NoTypeInformation -Encoding utf8
$objRows | Sort-Object Cluster, Environment, ProtectionGroup, ObjectName | Export-Csv -Path $objCsv -NoTypeInformation -Encoding utf8
$pathRows | Sort-Object Cluster, Environment, ProtectionGroup, ObjectName, IncludedPath, ExcludedPath | Export-Csv -Path $pathCsv -NoTypeInformation -Encoding utf8
$exRows | Sort-Object Severity, Cluster, Environment, ProtectionGroup, ObjectName, ExceptionType | Export-Csv -Path $exCsv -NoTypeInformation -Encoding utf8

$meta = [PSCustomObject]@{ InventoryDateET=$InventoryDateET; ScriptName="Get-CohesityProtectionInventory.ps1"; HeliosBaseUrl=$baseUrl; SelectedClusters=@($selectedClusters | Select-Object ClusterName, ClusterId); SelectedEnvironments=@($selectedEnvironments | Select-Object ApiName, DisplayName); OutputFiles=[PSCustomObject]@{PGSummary=$pgCsv;ObjectDetail=$objCsv;PathDetail=$pathCsv;Exceptions=$exCsv}; Counts=[PSCustomObject]@{PGSummaryRows=@($pgRows).Count;ObjectDetailRows=@($objRows).Count;PathDetailRows=@($pathRows).Count;ExceptionRows=@($exRows).Count;CollectionErrors=@($errors).Count}; EnvironmentCounts=@($pgRows | Group-Object Environment | Select-Object Name, Count); CollectionErrors=@($errors); Notes=@("GET-only collector.", "API key is loaded using AES helper.") }
$meta | ConvertTo-Json -Depth 10 | Out-File -FilePath $metaJson -Encoding utf8

Write-Host ""
Write-Host "Cohesity Protection Inventory export complete." -ForegroundColor Green
Write-Host "PG Summary rows    : $(@($pgRows).Count)" -ForegroundColor Green
Write-Host "Object Detail rows : $(@($objRows).Count)" -ForegroundColor Green
Write-Host "Path Detail rows   : $(@($pathRows).Count)" -ForegroundColor Green
Write-Host "Exception rows     : $(@($exRows).Count)" -ForegroundColor Green
Write-Host "Collection errors  : $(@($errors).Count)" -ForegroundColor Green
Write-Host "PG Summary CSV     : $pgCsv" -ForegroundColor Green
Write-Host "Object Detail CSV  : $objCsv" -ForegroundColor Green
Write-Host "Path Detail CSV    : $pathCsv" -ForegroundColor Green
Write-Host "Exceptions CSV     : $exCsv" -ForegroundColor Green
Write-Host "Metadata JSON      : $metaJson" -ForegroundColor Green
