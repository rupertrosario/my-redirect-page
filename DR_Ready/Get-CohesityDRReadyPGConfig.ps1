# Cohesity Helios - DR Configuration Export
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Collects complete GET responses for:
#   - Active Protection Groups: NAS, SQL, Hyper-V, Nutanix AHV, Oracle, Physical
#   - Protection Sources, registrations, source hierarchies, and app trees
#   - Protection Policies
#   - Storage Domains
#
# Source credentials are NEVER requested.
# Username/user fields returned by Cohesity are retained.
# Secret/password fields are removed before JSON is written.
#
# No Cohesity POST, PUT, PATCH, or DELETE operation is implemented.

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready"
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl = "https://helios.cohesity.com"
$v1BaseUrl = "$baseUrl/irisservices/api/v1/public"
$root = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath = Join-Path $root ("Common\" + "ApiKeyAesHelper.ps1")
$keyFile = Join-Path $root ("Common\Secure\cohesity_" + "apikey.enc")

$EnvironmentMap = @(
    [pscustomobject]@{ ApiName="kGenericNas"; DisplayName="NAS";         FileName="NAS.json" },
    [pscustomobject]@{ ApiName="kSQL";        DisplayName="SQL";         FileName="SQL.json" },
    [pscustomobject]@{ ApiName="kHyperV";     DisplayName="Hyper-V";     FileName="HyperV.json" },
    [pscustomobject]@{ ApiName="kAcropolis";  DisplayName="Nutanix AHV"; FileName="AHV.json" },
    [pscustomobject]@{ ApiName="kOracle";     DisplayName="Oracle";      FileName="Oracle.json" },
    [pscustomobject]@{ ApiName="kPhysical";   DisplayName="Physical";    FileName="Physical.json" }
)

$SensitiveFieldNames = @(
    "password",
    "encryptedPassword",
    "passwd",
    "passphrase",
    "secret",
    "secretKey",
    "clientSecret",
    "apiKey",
    "privateKey",
    "sshPrivateKey",
    "accessKey",
    "accessToken",
    "refreshToken",
    "sessionToken",
    "authToken",
    "bearerToken",
    "token",
    "encryptedCredential",
    "encryptedCredentials",
    "communityString"
)

if (-not (Test-Path $helperPath -PathType Leaf)) { throw "Missing API key helper: $helperPath" }
if (-not (Test-Path $keyFile -PathType Leaf)) { throw "Missing encrypted API key file: $keyFile" }
if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

. $helperPath
$keyLoader = "Get-Cohesity" + "ApiKeyFromAes"
$apiKey = & $keyLoader -EncryptedFile $keyFile
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "API key helper returned an empty value." }

function New-Headers {
    param([string]$ClusterId)

    $headers = @{ accept = "application/json" }
    $headers.Add(("api" + "Key"), $apiKey)
    if (-not [string]::IsNullOrWhiteSpace($ClusterId)) {
        $headers["accessClusterId"] = $ClusterId
    }
    return $headers
}

function Get-Json {
    param(
        [Parameter(Mandatory=$true)][string]$Uri,
        [Parameter(Mandatory=$true)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return ($response.Content | ConvertFrom-Json)
}

function Test-SensitiveFieldName {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) { return $false }
    return ($SensitiveFieldNames -contains $Name)
}

function Remove-SensitiveValues {
    param($Value)

    if ($null -eq $Value) { return $null }

    if ($Value -is [string] -or $Value -is [char] -or $Value.GetType().IsValueType) {
        return $Value
    }

    if ($Value -is [System.Collections.IDictionary]) {
        $clean = [ordered]@{}
        foreach ($key in @($Value.Keys)) {
            $name = [string]$key
            if (Test-SensitiveFieldName -Name $name) { continue }
            $clean[$name] = Remove-SensitiveValues -Value $Value[$key]
        }
        return [pscustomobject]$clean
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @()
        foreach ($item in @($Value)) {
            $items += ,(Remove-SensitiveValues -Value $item)
        }
        return @($items)
    }

    $cleanObject = [ordered]@{}
    foreach ($property in @($Value.PSObject.Properties)) {
        if (Test-SensitiveFieldName -Name $property.Name) { continue }
        $cleanObject[$property.Name] = Remove-SensitiveValues -Value $property.Value
    }
    return [pscustomobject]$cleanObject
}

function Write-Json {
    param([AllowNull()]$Value,[Parameter(Mandatory=$true)][string]$Path)

    $safeValue = Remove-SensitiveValues -Value $Value
    ConvertTo-Json -InputObject $safeValue -Depth 100 | Set-Content -Path $Path -Encoding UTF8
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
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) { return $property.Value }
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
    if ($safe.Length -gt 120) { $safe = $safe.Substring(0,120) }
    return $safe.Trim()
}

function Url-Encode {
    param($Value)
    return [uri]::EscapeDataString([string]$Value)
}

function Add-CollectionError {
    param(
        [System.Collections.ArrayList]$Errors,
        [string]$Stage,
        [string]$Environment,
        [string]$ObjectId,
        [string]$Message
    )

    [void]$Errors.Add([pscustomobject][ordered]@{
        Stage=$Stage
        Environment=$Environment
        ObjectId=$ObjectId
        Error=$Message
    })
}

function Get-CollectionItems {
    param($Json,[string[]]$PropertyNames)

    if ($null -eq $Json) { return @() }

    foreach ($name in $PropertyNames) {
        $value = Get-PropValue -Object $Json -Names @($name) -Default $null
        if ($null -ne $value) {
            return @(As-Array $value | Where-Object { $null -ne $_ })
        }
    }

    if ($Json -is [System.Collections.IEnumerable] -and $Json -isnot [string]) {
        return @(As-Array $Json | Where-Object { $null -ne $_ })
    }

    return @($Json)
}

function Get-ActiveProtectionGroups {
    param([string]$Environment,[hashtable]$Headers)

    $all = @()
    $cookie = ""

    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups" +
               "?environments=$(Url-Encode $Environment)" +
               "&isDeleted=false" +
               "&isActive=true" +
               "&includeLastRunInfo=true" +
               "&pruneSourceIds=false" +
               "&pruneExcludedSourceIds=false" +
               "&useCachedData=false" +
               "&maxResultCount=1000"

        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$(Url-Encode $cookie)"
        }

        $json = Get-Json -Uri $uri -Headers $Headers
        if ($null -eq $json) { throw "Protection Group GET returned no JSON content." }

        $groups = Get-CollectionItems -Json $json -PropertyNames @("protectionGroups")
        if ($groups.Count -gt 0) { $all += $groups }

        $cookie = First-Value @((Get-PropValue -Object $json -Names @("paginationCookie") -Default ""))
        $truncated = Get-PropValue -Object $json -Names @("isResponseTruncated") -Default $false

        if ($truncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-SourceId {
    param($Source)

    $protectionSource = Get-PropValue -Object $Source -Names @("protectionSource") -Default $null
    return First-Value @(
        (Get-PropValue -Object $Source -Names @("id")),
        (Get-PropValue -Object $Source -Names @("sourceId")),
        (Get-PropValue -Object $protectionSource -Names @("id"))
    )
}

function Get-RegistrationId {
    param($Registration)

    return First-Value @(
        (Get-PropValue -Object $Registration -Names @("id")),
        (Get-PropValue -Object $Registration -Names @("registrationId")),
        (Get-PropValue -Object $Registration -Names @("sourceId"))
    )
}

function Get-RootNodeIds {
    param($Json)

    $ids = @{}
    $roots = Get-CollectionItems -Json $Json -PropertyNames @("rootNodes")

    foreach ($rootNode in $roots) {
        $id = Get-SourceId -Source $rootNode
        if (-not [string]::IsNullOrWhiteSpace($id)) { $ids[$id] = $true }
    }

    return @($ids.Keys)
}

function Add-NoCredentialQuery {
    param([string]$Uri)

    if ($Uri.Contains("?")) { return ($Uri + "&includeSourceCredentials=false") }
    return ($Uri + "?includeSourceCredentials=false")
}

# Cluster selection
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
        if ([string]::IsNullOrWhiteSpace($name)) { $name = "Unknown-$id" }
        [pscustomobject]@{ ClusterName=$name; ClusterId=$id }
    } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ClusterId) } |
    Sort-Object ClusterName
)

$clusterMenu = for ($index=0; $index -lt $clusters.Count; $index++) {
    [pscustomobject]@{
        Index=$index + 1
        ClusterName=$clusters[$index].ClusterName
        ClusterId=$clusters[$index].ClusterId
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

    $number = 0
    if ([int]::TryParse($selection,[ref]$number) -and $number -ge 0 -and $number -le $clusterMenu.Count) {
        if ($number -eq 0) { $selectedClusters = @($clusterMenu) }
        else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $number }) }
        break
    }

    Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
}

foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

    $clusterDir = Join-Path $OutputDirectory ("{0}_{1}" -f (Safe-Name $cluster.ClusterName),$timestamp)
    $rawDir = Join-Path $clusterDir "Raw"
    $pgDir = Join-Path $rawDir "ProtectionGroups"
    $sourceDir = Join-Path $rawDir "Sources"
    $sourceDetailsDir = Join-Path $sourceDir "V2_SourceDetails"
    $sourceObjectsDir = Join-Path $sourceDir "V2_SourceObjects"
    $registrationDetailsDir = Join-Path $sourceDir "V2_RegistrationDetails"
    $applicationServersDir = Join-Path $sourceDir "V2_ApplicationServers"
    $v1SourceDir = Join-Path $sourceDir "V1"
    $policyDir = Join-Path $rawDir "Policies"

    foreach ($directory in @(
        $clusterDir,$rawDir,$pgDir,$sourceDir,$sourceDetailsDir,$sourceObjectsDir,
        $registrationDetailsDir,$applicationServersDir,$v1SourceDir,$policyDir
    )) {
        New-Item -Path $directory -ItemType Directory -Force | Out-Null
    }

    $errors = New-Object System.Collections.ArrayList
    $allSourceIds = @{}
    $applicationRootIds = @{ "kSQL"=@{}; "kOracle"=@{} }

    Write-Host ""
    Write-Host "Collecting DR configuration from $($cluster.ClusterName) ..." -ForegroundColor Cyan
    Write-Host "Source credentials are not requested. Username/user fields returned by the API are retained." -ForegroundColor DarkYellow

    # 1. Complete Protection Group objects for all six environments.
    Write-Host ""
    Write-Host "Protection Groups" -ForegroundColor Cyan

    foreach ($environment in $EnvironmentMap) {
        try {
            $groups = @(Get-ActiveProtectionGroups -Environment $environment.ApiName -Headers $headers)
            Write-Json -Value $groups -Path (Join-Path $pgDir $environment.FileName)

            foreach ($group in $groups) {
                $paramsName = switch ($environment.ApiName) {
                    "kGenericNas" { "genericNasParams" }
                    "kSQL"        { "mssqlParams" }
                    "kHyperV"     { "hypervParams" }
                    "kAcropolis"  { "acropolisParams" }
                    "kOracle"     { "oracleParams" }
                    "kPhysical"   { "physicalParams" }
                    default       { "" }
                }

                if (-not [string]::IsNullOrWhiteSpace($paramsName)) {
                    $params = Get-PropValue -Object $group -Names @($paramsName) -Default $null
                    if ($null -ne $params) {
                        $sourceId = First-Value @((Get-PropValue -Object $params -Names @("sourceId")))
                        if (-not [string]::IsNullOrWhiteSpace($sourceId)) { $allSourceIds[$sourceId] = $true }

                        foreach ($obj in @(As-Array (Get-PropValue -Object $params -Names @("objects") -Default @()))) {
                            $objectSourceId = First-Value @((Get-PropValue -Object $obj -Names @("sourceId")))
                            if (-not [string]::IsNullOrWhiteSpace($objectSourceId)) { $allSourceIds[$objectSourceId] = $true }
                        }
                    }
                }
            }

            Write-Host ("  {0}: {1} active PGs" -f $environment.DisplayName,$groups.Count) -ForegroundColor Yellow
        }
        catch {
            Add-CollectionError -Errors $errors -Stage "ProtectionGroups" -Environment $environment.DisplayName -ObjectId "" -Message $_.Exception.Message
            Write-Json -Value @() -Path (Join-Path $pgDir $environment.FileName)
            Write-Host "  $($environment.DisplayName): GET failed" -ForegroundColor Red
        }
    }

    # 2. Complete Protection Policy responses from V2 and V1.
    Write-Host ""
    Write-Host "Protection Policies" -ForegroundColor Cyan

    try {
        $policyV2Uri = "$baseUrl/v2/data-protect/policies?includeReplicatedPolicies=true&includeStats=true"
        $policyV2 = Get-Json -Uri $policyV2Uri -Headers $headers
        Write-Json -Value $policyV2 -Path (Join-Path $policyDir "V2_Policies.json")
        Write-Host "  V2 policies collected" -ForegroundColor Yellow
    }
    catch {
        Add-CollectionError -Errors $errors -Stage "V2Policies" -Environment "" -ObjectId "" -Message $_.Exception.Message
        Write-Host "  V2 policies GET failed" -ForegroundColor Red
    }

    try {
        $policyV1 = Get-Json -Uri "$v1BaseUrl/protectionPolicies" -Headers $headers
        Write-Json -Value $policyV1 -Path (Join-Path $policyDir "V1_Policies.json")
        Write-Host "  V1 policies collected" -ForegroundColor Yellow
    }
    catch {
        Add-CollectionError -Errors $errors -Stage "V1Policies" -Environment "" -ObjectId "" -Message $_.Exception.Message
        Write-Host "  V1 policies GET failed" -ForegroundColor Red
    }

    # 3. V2 Protection Source list. Source credentials explicitly disabled.
    Write-Host ""
    Write-Host "Protection Sources" -ForegroundColor Cyan

    try {
        $sourceListUri = Add-NoCredentialQuery -Uri "$baseUrl/v2/data-protect/sources?includeTenants=true"
        $v2Sources = Get-Json -Uri $sourceListUri -Headers $headers
        Write-Json -Value $v2Sources -Path (Join-Path $sourceDir "V2_Sources.json")

        $sourceItems = @(Get-CollectionItems -Json $v2Sources -PropertyNames @("sources"))
        foreach ($source in $sourceItems) {
            $sourceId = Get-SourceId -Source $source
            if (-not [string]::IsNullOrWhiteSpace($sourceId)) { $allSourceIds[$sourceId] = $true }
        }

        Write-Host ("  V2 source list collected: {0} entries" -f $sourceItems.Count) -ForegroundColor Yellow
    }
    catch {
        Add-CollectionError -Errors $errors -Stage "V2Sources" -Environment "" -ObjectId "" -Message $_.Exception.Message
        Write-Host "  V2 source list GET failed" -ForegroundColor Red
    }

    # 4. V2 source registrations: full list plus each registration detail.
    #    Credentials are not requested. includeHosts/external metadata remain enabled.
    try {
        $registrationsUri = "$baseUrl/v2/data-protect/sources/registrations" +
                            "?includeTenants=true" +
                            "&includeExternalMetadata=true" +
                            "&includeHosts=true" +
                            "&useCachedData=false"
        $registrationsUri = Add-NoCredentialQuery -Uri $registrationsUri
        $registrations = Get-Json -Uri $registrationsUri -Headers $headers
        Write-Json -Value $registrations -Path (Join-Path $sourceDir "V2_Registrations.json")

        $registrationItems = @(Get-CollectionItems -Json $registrations -PropertyNames @("registrations"))
        foreach ($registration in $registrationItems) {
            $registrationId = Get-RegistrationId -Registration $registration
            $sourceId = Get-SourceId -Source $registration
            if (-not [string]::IsNullOrWhiteSpace($sourceId)) { $allSourceIds[$sourceId] = $true }
            if ([string]::IsNullOrWhiteSpace($registrationId)) { continue }

            try {
                $registrationDetail = Get-Json -Uri "$baseUrl/v2/data-protect/sources/registrations/$(Url-Encode $registrationId)" -Headers $headers
                Write-Json -Value $registrationDetail -Path (Join-Path $registrationDetailsDir ("{0}.json" -f (Safe-Name $registrationId)))
            }
            catch {
                Add-CollectionError -Errors $errors -Stage "V2RegistrationDetail" -Environment "" -ObjectId $registrationId -Message $_.Exception.Message
            }
        }

        Write-Host ("  V2 registrations collected: {0} entries" -f $registrationItems.Count) -ForegroundColor Yellow
    }
    catch {
        Add-CollectionError -Errors $errors -Stage "V2Registrations" -Environment "" -ObjectId "" -Message $_.Exception.Message
        Write-Host "  V2 registrations GET failed" -ForegroundColor Red
    }

    # 5. V1 source trees, root nodes, and registration/application information.
    #    These are retained because they expose workload-specific source information
    #    that is not always present in the V2 source list.
    foreach ($environment in $EnvironmentMap) {
        $environmentName = $environment.ApiName
        $safeEnvironment = Safe-Name $environment.DisplayName

        try {
            $v1SourcesUri = "$v1BaseUrl/protectionSources" +
                            "?environments=$(Url-Encode $environmentName)" +
                            "&includeObjectProtectionInfo=true" +
                            "&includeExternalMetadata=true" +
                            "&pruneNonCriticalInfo=false" +
                            "&pruneAggregationInfo=false" +
                            "&useCachedData=false"
            $v1SourcesUri = Add-NoCredentialQuery -Uri $v1SourcesUri
            $v1Sources = Get-Json -Uri $v1SourcesUri -Headers $headers
            Write-Json -Value $v1Sources -Path (Join-Path $v1SourceDir ("ProtectionSources_{0}.json" -f $safeEnvironment))
        }
        catch {
            Add-CollectionError -Errors $errors -Stage "V1ProtectionSources" -Environment $environment.DisplayName -ObjectId "" -Message $_.Exception.Message
        }

        try {
            $rootNodes = Get-Json -Uri "$v1BaseUrl/protectionSources/rootNodes?environments=$(Url-Encode $environmentName)" -Headers $headers
            Write-Json -Value $rootNodes -Path (Join-Path $v1SourceDir ("RootNodes_{0}.json" -f $safeEnvironment))

            foreach ($rootId in @(Get-RootNodeIds -Json $rootNodes)) {
                $allSourceIds[$rootId] = $true
                if ($environmentName -eq "kSQL" -or $environmentName -eq "kOracle") {
                    $applicationRootIds[$environmentName][$rootId] = $true
                }
            }
        }
        catch {
            Add-CollectionError -Errors $errors -Stage "V1RootNodes" -Environment $environment.DisplayName -ObjectId "" -Message $_.Exception.Message
        }

        try {
            $registrationInfoUri = "$v1BaseUrl/protectionSources/registrationInfo" +
                                   "?environments=$(Url-Encode $environmentName)" +
                                   "&includeEntityPermissionInfo=true" +
                                   "&includeApplicationsTreeInfo=true" +
                                   "&pruneNonCriticalInfo=false" +
                                   "&useCachedData=false"

            if ($environmentName -eq "kSQL" -or $environmentName -eq "kOracle") {
                $registrationInfoUri += "&includeDBApplicationInfo=true&allUnderHierarchy=true&includeData=true"
            }

            $registrationInfoUri = Add-NoCredentialQuery -Uri $registrationInfoUri
            $registrationInfo = Get-Json -Uri $registrationInfoUri -Headers $headers
            Write-Json -Value $registrationInfo -Path (Join-Path $v1SourceDir ("RegistrationInfo_{0}.json" -f $safeEnvironment))

            foreach ($rootId in @(Get-RootNodeIds -Json $registrationInfo)) {
                $allSourceIds[$rootId] = $true
                if ($environmentName -eq "kSQL" -or $environmentName -eq "kOracle") {
                    $applicationRootIds[$environmentName][$rootId] = $true
                }
            }
        }
        catch {
            Add-CollectionError -Errors $errors -Stage "V1RegistrationInfo" -Environment $environment.DisplayName -ObjectId "" -Message $_.Exception.Message
        }
    }

    # 6. Per-source V2 detail and source object hierarchy.
    foreach ($sourceId in @($allSourceIds.Keys | Sort-Object)) {
        if ([string]::IsNullOrWhiteSpace($sourceId)) { continue }

        try {
            $sourceDetail = Get-Json -Uri "$baseUrl/v2/data-protect/sources/$(Url-Encode $sourceId)" -Headers $headers
            Write-Json -Value $sourceDetail -Path (Join-Path $sourceDetailsDir ("{0}.json" -f (Safe-Name $sourceId)))
        }
        catch {
            Add-CollectionError -Errors $errors -Stage "V2SourceDetail" -Environment "" -ObjectId $sourceId -Message $_.Exception.Message
        }

        try {
            $sourceObjects = Get-Json -Uri "$baseUrl/v2/data-protect/sources/$(Url-Encode $sourceId)/objects?includeTenants=true" -Headers $headers
            Write-Json -Value $sourceObjects -Path (Join-Path $sourceObjectsDir ("{0}.json" -f (Safe-Name $sourceId)))
        }
        catch {
            Add-CollectionError -Errors $errors -Stage "V2SourceObjects" -Environment "" -ObjectId $sourceId -Message $_.Exception.Message
        }
    }

    Write-Host ("  Per-source detail attempted for {0} source IDs" -f $allSourceIds.Count) -ForegroundColor Yellow

    # 7. SQL / Oracle application server trees.
    foreach ($applicationEnvironment in @("kSQL","kOracle")) {
        foreach ($rootId in @($applicationRootIds[$applicationEnvironment].Keys | Sort-Object)) {
            try {
                $appServerUri = "$baseUrl/v2/data-protect/sources/application-servers" +
                                "?rootNodeId=$(Url-Encode $rootId)" +
                                "&environment=$(Url-Encode $applicationEnvironment)" +
                                "&applicationEnvironment=$(Url-Encode $applicationEnvironment)" +
                                "&pageSize=1000"
                $appServers = Get-Json -Uri $appServerUri -Headers $headers
                $fileName = "{0}_{1}.json" -f (Safe-Name $applicationEnvironment),(Safe-Name $rootId)
                Write-Json -Value $appServers -Path (Join-Path $applicationServersDir $fileName)
            }
            catch {
                Add-CollectionError -Errors $errors -Stage "V2ApplicationServers" -Environment $applicationEnvironment -ObjectId $rootId -Message $_.Exception.Message
            }
        }
    }

    # 8. Storage Domains with optional detail flags enabled.
    Write-Host ""
    Write-Host "Storage Domains" -ForegroundColor Cyan

    try {
        $storageUri = "$baseUrl/v2/storage-domains" +
                      "?includeTenants=true" +
                      "&includeStats=true" +
                      "&includeTimeSeriesSchema=true" +
                      "&includeFileCountBySize=true"
        $storageDomains = Get-Json -Uri $storageUri -Headers $headers
        Write-Json -Value $storageDomains -Path (Join-Path $rawDir "StorageDomains.json")
        Write-Host "  Storage domains collected" -ForegroundColor Yellow
    }
    catch {
        Add-CollectionError -Errors $errors -Stage "StorageDomains" -Environment "" -ObjectId "" -Message $_.Exception.Message
        Write-Host "  Storage domains GET failed" -ForegroundColor Red
    }

    Write-Json -Value @($errors) -Path (Join-Path $clusterDir "Errors.json")

    Write-Host ""
    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green
    Write-Host "Errors recorded: $($errors.Count)" -ForegroundColor $(if ($errors.Count -eq 0) { "Green" } else { "Yellow" })
}
