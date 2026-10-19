# Cohesity Helios - DR Protection Group Reverse Engineering Export V2
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Scope:
# - Full raw V2 Protection Group JSON
# - Full raw V1 Protection Job JSON for complementary legacy settings
# - Active objects only: isDeleted=false, isActive=true; paused objects excluded
# - Workloads: NAS (Generic NAS + Isilon), SQL, Hyper-V, Nutanix AHV, Oracle, Physical
# - Cluster selection and environment selection
# - No source registration, policy expansion, storage-domain expansion, or credential collection
# - Original Get-CohesityDRReadyPGConfig.ps1 remains unchanged

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready"
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl = "https://helios.cohesity.com"
$root = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath = Join-Path $root ("Common\" + "ApiKeyAesHelper.ps1")
$keyFile = Join-Path $root ("Common\Secure\cohesity_" + "apikey.enc")

$EnvironmentMap = @(
    [pscustomobject]@{ ApiNames=@("kGenericNas","kIsilon"); DisplayName="NAS";         FileName="NAS.json" },
    [pscustomobject]@{ ApiNames=@("kSQL");                  DisplayName="SQL";         FileName="SQL.json" },
    [pscustomobject]@{ ApiNames=@("kHyperV");               DisplayName="Hyper-V";     FileName="HyperV.json" },
    [pscustomobject]@{ ApiNames=@("kAcropolis");            DisplayName="Nutanix AHV"; FileName="AHV.json" },
    [pscustomobject]@{ ApiNames=@("kOracle");               DisplayName="Oracle";      FileName="Oracle.json" },
    [pscustomobject]@{ ApiNames=@("kPhysical");             DisplayName="Physical";    FileName="Physical.json" }
)

if (-not (Test-Path $helperPath -PathType Leaf)) { throw "Missing API key helper: $helperPath" }
if (-not (Test-Path $keyFile -PathType Leaf)) { throw "Missing encrypted API key file: $keyFile" }
if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

. $helperPath
$keyLoader = "Get-Cohesity" + "ApiKeyFromAes"
$apiKey = & $keyLoader -EncryptedFile $keyFile
if ([string]::IsNullOrWhiteSpace([string]$apiKey)) { throw "API key helper returned an empty value." }

function New-Headers {
    param([string]$ClusterId)
    $headers = @{ accept="application/json" }
    $headers.Add(("api" + "Key"),$apiKey)
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) { $headers["accessClusterId"]=$ClusterId }
    return $headers
}

function Get-Json {
    param(
        [Parameter(Mandatory=$true)][string]$Uri,
        [Parameter(Mandatory=$true)][hashtable]$Headers
    )

    if (-not $Uri.StartsWith($baseUrl,[System.StringComparison]::OrdinalIgnoreCase)) {
        throw "SAFETY BLOCK: URI outside Helios: $Uri"
    }

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    } else {
        $r = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $r -or [string]::IsNullOrWhiteSpace($r.Content)) { return $null }
    return ($r.Content | ConvertFrom-Json)
}

function Write-RawJson {
    param([AllowNull()]$Value,[Parameter(Mandatory=$true)][string]$Path)
    ConvertTo-Json -InputObject $Value -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Get-PropValue {
    param($Object,[string[]]$Names,$Default=$null)
    if ($null -eq $Object -or $Object -is [string]) { return $Default }
    foreach ($name in $Names) {
        foreach ($p in @($Object.PSObject.Properties)) {
            if ($p.Name -ieq $name) { return $p.Value }
        }
    }
    return $Default
}

function First-Value {
    param($Values)
    foreach ($value in @($Values)) {
        foreach ($item in @($value)) {
            if ($null -ne $item -and "$item".Trim() -ne "") { return "$item" }
        }
    }
    return ""
}

function Safe-Name {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return "UNNAMED" }
    $safe = $Value -replace '[:\\/\*\?"<>\|]+','_'
    if ($safe.Length -gt 120) { $safe=$safe.Substring(0,120) }
    return $safe.Trim()
}

function Url-Encode {
    param($Value)
    return [uri]::EscapeDataString([string]$Value)
}

function Get-CollectionItems {
    param($Json,[string[]]$PropertyNames)
    if ($null -eq $Json) { return @() }

    foreach ($name in $PropertyNames) {
        $value = Get-PropValue -Object $Json -Names @($name) -Default $null
        if ($null -ne $value) { return @(As-Array $value | Where-Object { $null -ne $_ }) }
    }

    if ($Json -is [System.Collections.IEnumerable] -and $Json -isnot [string]) {
        return @(As-Array $Json | Where-Object { $null -ne $_ })
    }

    return @($Json)
}

function Get-V1ProtectionJobs {
    param([string[]]$Environments,[hashtable]$Headers)

    $all=@()

    foreach ($environment in $Environments) {
        $uri = "$baseUrl/irisservices/api/v1/public/protectionJobs" +
               "?environments=$(Url-Encode $environment)" +
               "&isDeleted=false" +
               "&isActive=true" +
               "&onlyReturnBasicSummary=false" +
               "&includeLastRunAndStats=false" +
               "&pruneExcludedSourceIds=false" +
               "&includeSourceNames=true"

        $json = Get-Json -Uri $uri -Headers $Headers
        if ($null -eq $json) { throw "V1 Protection Jobs GET returned no JSON for $environment." }

        $jobs = @(As-Array $json | Where-Object {
            $null -ne $_ -and
            (Get-PropValue -Object $_ -Names @("isDeleted") -Default $false) -ne $true -and
            (Get-PropValue -Object $_ -Names @("isActive") -Default $false) -eq $true -and
            (Get-PropValue -Object $_ -Names @("isPaused") -Default $false) -ne $true
        })

        if ($jobs.Count -gt 0) { $all += $jobs }
    }

    return @($all)
}

function Get-ActiveProtectionGroups {
    param([string[]]$Environments,[hashtable]$Headers)

    $all=@()

    foreach ($environment in $Environments) {
        $cookie=""

        do {
            $uri = "$baseUrl/v2/data-protect/protection-groups" +
                   "?environments=$(Url-Encode $environment)" +
                   "&isDeleted=false" +
                   "&isPaused=false" +
                   "&isActive=true" +
                   "&includeTenants=true" +
                   "&includeLastRunInfo=true" +
                   "&pruneSourceIds=false" +
                   "&pruneExcludedSourceIds=false" +
                   "&useCachedData=false" +
                   "&maxResultCount=1000"

            if (-not [string]::IsNullOrWhiteSpace($cookie)) {
                $uri += "&paginationCookie=$(Url-Encode $cookie)"
            }

            $json = Get-Json -Uri $uri -Headers $Headers
            if ($null -eq $json) { throw "Protection Group GET returned no JSON for $environment." }

            $groups = Get-CollectionItems -Json $json -PropertyNames @("protectionGroups")
            if ($groups.Count -gt 0) { $all += $groups }

            $cookie = First-Value @(
                (Get-PropValue -Object $json -Names @("paginationCookie") -Default "")
            )
            $truncated = Get-PropValue -Object $json -Names @("isResponseTruncated") -Default $false

            if ($truncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
        } while (-not [string]::IsNullOrWhiteSpace($cookie))
    }

    return @($all)
}

$clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$rawClusters = @(As-Array (Get-PropValue -Object $clusterJson -Names @("cohesityClusters")))
if ($rawClusters.Count -eq 0) { throw "No clusters returned from Helios." }

$clusters = @(
    $rawClusters | ForEach-Object {
        $name = First-Value @(
            (Get-PropValue -Object $_ -Names @("clusterName")),
            (Get-PropValue -Object $_ -Names @("displayName")),
            (Get-PropValue -Object $_ -Names @("name"))
        )
        $id = First-Value @(
            (Get-PropValue -Object $_ -Names @("clusterId")),
            (Get-PropValue -Object $_ -Names @("id"))
        )
        if ([string]::IsNullOrWhiteSpace($name)) { $name="Unknown-$id" }
        [pscustomobject]@{ ClusterName=$name; ClusterId=$id }
    } | Where-Object { -not [string]::IsNullOrWhiteSpace($_.ClusterId) } | Sort-Object ClusterName
)

$clusterMenu = for ($i=0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{ Index=$i+1; ClusterName=$clusters[$i].ClusterName; ClusterId=$clusters[$i].ClusterId }
}

Write-Host ""
Write-Host "Available Helios Clusters (sorted):" -ForegroundColor Cyan
$clusterMenu | Format-Table -AutoSize
Write-Host "[0] All clusters" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host "Select cluster: 0 for ALL, 1-$($clusterMenu.Count) for single, or X"
    if ($selection -match '^(x|X|q|Q)$') { return }

    $number=0
    if ([int]::TryParse($selection,[ref]$number) -and $number -ge 0 -and $number -le $clusterMenu.Count) {
        if ($number -eq 0) { $selectedClusters=@($clusterMenu) }
        else { $selectedClusters=@($clusterMenu | Where-Object { $_.Index -eq $number }) }
        break
    }

    Write-Host "Invalid cluster selection." -ForegroundColor Red
}

Write-Host ""
Write-Host "Available Workload Environments:" -ForegroundColor Cyan
for ($i=0; $i -lt $EnvironmentMap.Count; $i++) {
    Write-Host ("[{0}] {1}" -f ($i+1),$EnvironmentMap[$i].DisplayName) -ForegroundColor Yellow
}
Write-Host "[0] All six environments" -ForegroundColor Yellow
Write-Host "[X] Exit" -ForegroundColor Yellow

while ($true) {
    $selection = Read-Host "Select environment: 0 for ALL, 1-$($EnvironmentMap.Count) for single, or X"
    if ($selection -match '^(x|X|q|Q)$') { return }

    $number=0
    if ([int]::TryParse($selection,[ref]$number) -and $number -ge 0 -and $number -le $EnvironmentMap.Count) {
        if ($number -eq 0) { $selectedEnvironments=@($EnvironmentMap) }
        else { $selectedEnvironments=@($EnvironmentMap[$number-1]) }
        break
    }

    Write-Host "Invalid environment selection." -ForegroundColor Red
}

foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $clusterDir = Join-Path $OutputDirectory ("{0}_{1}_PG_ReverseEngineering" -f (Safe-Name $cluster.ClusterName),$timestamp)
    $v2Dir = Join-Path $clusterDir "V2_ProtectionGroups"
    $v1Dir = Join-Path $clusterDir "V1_ProtectionJobs"

    foreach ($directory in @($clusterDir,$v2Dir,$v1Dir)) {
        New-Item -Path $directory -ItemType Directory -Force | Out-Null
    }

    $v2Counts=[ordered]@{}
    $v1Counts=[ordered]@{}
    $errors=New-Object System.Collections.ArrayList

    Write-Host ""
    Write-Host "Collecting raw PG configuration from $($cluster.ClusterName) ..." -ForegroundColor Cyan

    foreach ($environment in $selectedEnvironments) {
        try {
            $groups = @(Get-ActiveProtectionGroups -Environments $environment.ApiNames -Headers $headers)
            Write-RawJson -Value $groups -Path (Join-Path $v2Dir $environment.FileName)
            $v2Counts[$environment.DisplayName]=$groups.Count
            Write-Host ("  V2 {0}: {1} active PGs" -f $environment.DisplayName,$groups.Count) -ForegroundColor Yellow
        }
        catch {
            $v2Counts[$environment.DisplayName]=0
            [void]$errors.Add([pscustomobject][ordered]@{
                ApiVersion="V2"
                Cluster=$cluster.ClusterName
                Environment=$environment.DisplayName
                Error=$_.Exception.Message
            })
            Write-Host ("  V2 {0}: GET failed" -f $environment.DisplayName) -ForegroundColor Red
        }

        try {
            $jobs = @(Get-V1ProtectionJobs -Environments $environment.ApiNames -Headers $headers)
            Write-RawJson -Value $jobs -Path (Join-Path $v1Dir $environment.FileName)
            $v1Counts[$environment.DisplayName]=$jobs.Count
            Write-Host ("  V1 {0}: {1} active jobs" -f $environment.DisplayName,$jobs.Count) -ForegroundColor DarkYellow
        }
        catch {
            $v1Counts[$environment.DisplayName]=0
            [void]$errors.Add([pscustomobject][ordered]@{
                ApiVersion="V1"
                Cluster=$cluster.ClusterName
                Environment=$environment.DisplayName
                Error=$_.Exception.Message
            })
            Write-Host ("  V1 {0}: GET failed" -f $environment.DisplayName) -ForegroundColor Red
        }
    }

    $manifest=[pscustomobject][ordered]@{
        SchemaVersion="2"
        Collector="Get-CohesityDRReadyPGConfig_v2.ps1"
        Purpose="Raw Protection Group reverse-engineering evidence for DR reconstruction"
        GeneratedAt=(Get-Date).ToString("yyyy-MM-ddTHH:mm:ssK")
        ClusterName=$cluster.ClusterName
        ClusterId=$cluster.ClusterId
        ActiveDefinition="V2: isDeleted=false; isPaused=false; isActive=true. V1: isDeleted=false; isActive=true and paused jobs filtered from raw results."
        SelectedWorkloads=@($selectedEnvironments | ForEach-Object { $_.DisplayName })
        V2ProtectionGroupCounts=[pscustomobject]$v2Counts
        V1ProtectionJobCounts=[pscustomobject]$v1Counts
        V2Directory="V2_ProtectionGroups"
        V1Directory="V1_ProtectionJobs"
        RawV2ProtectionGroupJsonPreserved=$true
        RawV1ProtectionJobJsonPreserved=$true
        PropertyProjectionApplied=$false
        ExternalDependenciesCollected=$false
        ErrorCount=$errors.Count
    }

    Write-RawJson -Value $manifest -Path (Join-Path $clusterDir "DR_Manifest_v2.json")
    Write-RawJson -Value @($errors) -Path (Join-Path $clusterDir "Errors.json")

    Write-Host ""
    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green
    Write-Host "Errors recorded: $($errors.Count)" -ForegroundColor $(if ($errors.Count -eq 0) { "Green" } else { "Yellow" })
}
