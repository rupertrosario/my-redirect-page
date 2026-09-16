# Cohesity Helios - Active Protection Group Configuration Export
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Purpose:
#   Export complete configuration data for ACTIVE Cohesity Protection Groups.
#
# Environments:
#   NAS, SQL, Hyper-V, Nutanix AHV, Oracle, Physical
#
# Safety:
#   Every Cohesity API request in this script uses HTTP GET only.

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready"
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

$EnvironmentMap = @(
    [pscustomobject]@{ ApiName="kGenericNas"; DisplayName="NAS";         ParamNames=@("genericNasParams") },
    [pscustomobject]@{ ApiName="kSQL";        DisplayName="SQL";         ParamNames=@("mssqlParams") },
    [pscustomobject]@{ ApiName="kHyperV";     DisplayName="Hyper-V";     ParamNames=@("hypervParams","hyperVParams") },
    [pscustomobject]@{ ApiName="kAcropolis";  DisplayName="Nutanix AHV"; ParamNames=@("acropolisParams","nutanixParams","ahvParams") },
    [pscustomobject]@{ ApiName="kOracle";     DisplayName="Oracle";      ParamNames=@("oracleParams") },
    [pscustomobject]@{ ApiName="kPhysical";   DisplayName="Physical";    ParamNames=@("physicalParams") }
)

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found: $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found: $encryptedApiKeyPath"
}
if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedApiKeyPath
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "AES API key helper returned an empty API key."
}

function New-Headers {
    param([string]$ClusterId)

    $headers = @{
        accept = "application/json"
        apiKey = $apiKey
    }

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

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function As-Array {
    param($Value)
    if ($null -eq $Value) { return @() }
    return @($Value)
}

function Get-PropValue {
    param(
        $Object,
        [string[]]$Names,
        $Default = $null
    )

    if ($null -eq $Object -or $Object -is [string]) {
        return $Default
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                if ($null -ne $property.Value) {
                    return $property.Value
                }
            }
        }
    }

    return $Default
}

function Get-NestedValue {
    param(
        $Object,
        [string]$Path
    )

    if ($null -eq $Object -or [string]::IsNullOrWhiteSpace($Path)) {
        return $null
    }

    $current = $Object
    foreach ($segment in ($Path -split "\.")) {
        if ($null -eq $current -or $current -is [string]) {
            return $null
        }
        $current = Get-PropValue -Object $current -Names @($segment)
    }

    return $current
}

function First-Value {
    param($Values)

    foreach ($value in @($Values)) {
        foreach ($item in @($value)) {
            if ($null -ne $item -and "$item".Trim() -ne "") {
                return "$item"
            }
        }
    }

    return ""
}

function Safe-Name {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "UNNAMED"
    }

    $safe = $Value -replace '[:\\/\*\?"<>\|]+','_'
    if ($safe.Length -gt 120) {
        $safe = $safe.Substring(0,120)
    }

    return $safe.Trim()
}

function Write-Json {
    param(
        [AllowNull()]$Value,
        [Parameter(Mandatory=$true)][string]$Path
    )

    $Value | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Expand-LeafValue {
    param(
        $Value,
        [string]$Path = ""
    )

    if ($null -eq $Value) {
        [pscustomobject]@{ Field=$Path; Value="<null>" }
        return
    }

    if (
        $Value -is [string] -or
        $Value -is [char] -or
        $Value -is [bool] -or
        $Value -is [byte] -or
        $Value -is [sbyte] -or
        $Value -is [int16] -or
        $Value -is [uint16] -or
        $Value -is [int32] -or
        $Value -is [uint32] -or
        $Value -is [int64] -or
        $Value -is [uint64] -or
        $Value -is [single] -or
        $Value -is [double] -or
        $Value -is [decimal] -or
        $Value -is [datetime] -or
        $Value -is [guid]
    ) {
        [pscustomobject]@{ Field=$Path; Value=[string]$Value }
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        $keys = @($Value.Keys)
        if ($keys.Count -eq 0) {
            [pscustomobject]@{ Field=$Path; Value="{}" }
            return
        }

        foreach ($key in $keys) {
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) {
                [string]$key
            }
            else {
                "$Path.$key"
            }
            Expand-LeafValue -Value $Value[$key] -Path $childPath
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @($Value)
        if ($items.Count -eq 0) {
            [pscustomobject]@{ Field=$Path; Value="[]" }
            return
        }

        for ($index=0; $index -lt $items.Count; $index++) {
            Expand-LeafValue -Value $items[$index] -Path "$Path[$index]"
        }
        return
    }

    $properties = @($Value.PSObject.Properties)
    if ($properties.Count -eq 0) {
        [pscustomobject]@{ Field=$Path; Value=[string]$Value }
        return
    }

    foreach ($property in $properties) {
        $childPath = if ([string]::IsNullOrWhiteSpace($Path)) {
            $property.Name
        }
        else {
            "$Path.$($property.Name)"
        }
        Expand-LeafValue -Value $property.Value -Path $childPath
    }
}

function Get-EnvironmentBlock {
    param(
        $ProtectionGroup,
        [string[]]$Names
    )

    foreach ($name in $Names) {
        $value = Get-PropValue -Object $ProtectionGroup -Names @($name)
        if ($null -ne $value) {
            return [pscustomobject]@{
                Name  = $name
                Value = $value
            }
        }
    }

    return $null
}

function Get-ActiveProtectionGroups {
    param(
        [string]$Environment,
        [hashtable]$Headers
    )

    $all = @()
    $cookie = ""

    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=$Environment&isDeleted=false&isActive=true&includeLastRunInfo=false&maxResultCount=1000"

        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri = "$uri&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-Json -Uri $uri -Headers $Headers
        $groups = Get-PropValue -Object $json -Names @("protectionGroups") -Default @()

        if ($groups) {
            $all += @(As-Array $groups | Where-Object { $_ })
        }

        $cookie = First-Value @((Get-PropValue -Object $json -Names @("paginationCookie") -Default ""))
        $truncated = Get-PropValue -Object $json -Names @("isResponseTruncated") -Default $false

        if ($truncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) {
            break
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-ProtectionGroupDetail {
    param(
        [string]$ProtectionGroupId,
        [hashtable]$Headers
    )

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) {
        return $null
    }

    $encodedId = [uri]::EscapeDataString($ProtectionGroupId)
    $uri = "$baseUrl/v2/data-protect/protection-groups/$encodedId?includeLastRunInfo=false&pruneSourceIds=false"
    return Get-Json -Uri $uri -Headers $Headers
}

function Get-AllPolicies {
    param([hashtable]$Headers)

    foreach ($uri in @(
        "$baseUrl/v2/data-protect/policies?maxResultCount=1000",
        "$baseUrl/v2/data-protect/policies"
    )) {
        try {
            $json = Get-Json -Uri $uri -Headers $Headers
            if ($null -eq $json) { continue }

            $items = Get-PropValue -Object $json -Names @("policies","policyList","items") -Default $null
            if ($null -ne $items) { return @(As-Array $items) }
            if ($json -is [array]) { return @($json) }
            return @($json)
        }
        catch {
            continue
        }
    }

    return @()
}

function Get-AllStorageDomains {
    param([hashtable]$Headers)

    $json = Get-Json -Uri "$baseUrl/v2/storage-domains?includeStats=false" -Headers $Headers
    $items = Get-PropValue -Object $json -Names @("storageDomains","items") -Default $null

    if ($null -ne $items) { return @(As-Array $items) }
    if ($json -is [array]) { return @($json) }
    if ($json) { return @($json) }

    return @()
}

function Get-AllSourceRegistrations {
    param([hashtable]$Headers)

    $uri = "$baseUrl/v2/data-protect/sources/registrations?includeSourceCredentials=false"
    $json = Get-Json -Uri $uri -Headers $Headers
    $items = Get-PropValue -Object $json -Names @("registrations","items") -Default $null

    if ($null -ne $items) { return @(As-Array $items) }
    if ($json -is [array]) { return @($json) }
    if ($json) { return @($json) }

    return @()
}

function Build-IdMap {
    param(
        $Items,
        [string[]]$IdNames,
        [string[]]$NameNames
    )

    $map = @{}

    foreach ($item in @(As-Array $Items | Where-Object { $_ })) {
        $id = First-Value @((Get-PropValue -Object $item -Names $IdNames))
        $name = First-Value @((Get-PropValue -Object $item -Names $NameNames))

        if (-not [string]::IsNullOrWhiteSpace($id)) {
            $map[[string]$id] = [pscustomobject]@{
                Id   = [string]$id
                Name = $name
                Raw  = $item
            }
        }
    }

    return $map
}

function Build-SourceRegistrationIndex {
    param($Registrations)

    $map = @{}

    foreach ($registration in @(As-Array $Registrations | Where-Object { $_ })) {
        $sourceInfo = Get-PropValue -Object $registration -Names @("sourceInfo") -Default $null

        $ids = @(
            (First-Value @((Get-PropValue -Object $registration -Names @("id","sourceId")))),
            (First-Value @((Get-PropValue -Object $sourceInfo -Names @("id","sourceId","entityId"))))
        ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique

        $name = First-Value @(
            (Get-PropValue -Object $registration -Names @("name","sourceName","displayName")),
            (Get-PropValue -Object $sourceInfo -Names @("name","sourceName","displayName","hostName"))
        )

        foreach ($id in $ids) {
            $map[[string]$id] = [pscustomobject]@{
                Id   = [string]$id
                Name = $name
                Raw  = $registration
            }
        }
    }

    return $map
}

function Get-DependencyReferences {
    param($EnvironmentParams)

    $references = @()
    if ($null -eq $EnvironmentParams) {
        return @()
    }

    foreach ($leaf in @(Expand-LeafValue -Value $EnvironmentParams -Path "")) {
        $field = [string]$leaf.Field
        $value = [string]$leaf.Value

        if ([string]::IsNullOrWhiteSpace($value) -or $value -eq "<null>") {
            continue
        }

        $referenceType = ""

        if ($field -match '(?i)(^|\.)(sourceId|sourceIds|includedSourceIds|excludedSourceIds)(\[\d+\])?$') {
            $referenceType = "Source"
        }
        elseif (
            $field -match '(?i)(^|\.)(objectId|objectIds)(\[\d+\])?$' -or
            $field -match '(?i)(^|\.)objects\[\d+\]\.id$'
        ) {
            $referenceType = "Object"
        }

        if (-not [string]::IsNullOrWhiteSpace($referenceType)) {
            $references += [pscustomobject]@{
                ReferenceType = $referenceType
                FieldPath     = $field
                OriginalId    = $value
            }
        }
    }

    return @($references | Sort-Object ReferenceType,FieldPath,OriginalId -Unique)
}

function Resolve-DependencyReference {
    param(
        [string]$ReferenceType,
        [string]$Id,
        [hashtable]$Headers
    )

    if ([string]::IsNullOrWhiteSpace($Id)) {
        return $null
    }

    $encodedId = [uri]::EscapeDataString($Id)

    if ($ReferenceType -eq "Object") {
        $uris = @(
            "$baseUrl/v2/data-protect/objects/$encodedId",
            "$baseUrl/v2/data-protect/sources/$encodedId"
        )
    }
    else {
        $uris = @(
            "$baseUrl/v2/data-protect/sources/$encodedId",
            "$baseUrl/v2/data-protect/objects/$encodedId"
        )
    }

    foreach ($uri in $uris) {
        try {
            $raw = Get-Json -Uri $uri -Headers $Headers
            if ($null -eq $raw) { continue }

            if ($raw -is [array]) {
                $first = @($raw | Where-Object { $_ } | Select-Object -First 1)
                if ($first.Count -eq 0) { continue }
                $raw = $first[0]
            }

            return [pscustomobject]@{
                Endpoint    = $uri
                Name        = First-Value @((Get-PropValue -Object $raw -Names @("name","objectName","sourceName","displayName","hostName")))
                Environment = First-Value @((Get-PropValue -Object $raw -Names @("environment")))
                SourceId    = First-Value @((Get-PropValue -Object $raw -Names @("sourceId")))
                Raw         = $raw
            }
        }
        catch {
            continue
        }
    }

    return $null
}

# ---------------------------------------------------------------------------
# Cluster selection
# ---------------------------------------------------------------------------
$clusterResponse = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$rawClusters = @(As-Array (Get-PropValue -Object $clusterResponse -Names @("cohesityClusters")))

if ($rawClusters.Count -eq 0) {
    throw "No clusters returned from Helios."
}

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

        if ([string]::IsNullOrWhiteSpace($name)) {
            $name = "Unknown-$id"
        }

        [pscustomobject]@{
            ClusterName = $name
            ClusterId   = $id
        }
    } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ClusterId) } |
    Sort-Object ClusterName
)

$clusterMenu = for ($index=0; $index -lt $clusters.Count; $index++) {
    [pscustomobject]@{
        Index       = $index + 1
        ClusterName = $clusters[$index].ClusterName
        ClusterId   = $clusters[$index].ClusterId
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

    if ($selection -match '^(x|X|q|Q)$') {
        return
    }

    $number = 0
    if (-not [int]::TryParse($selection,[ref]$number)) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($number -lt 0 -or $number -gt $clusterMenu.Count) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($number -eq 0) {
        $selectedClusters = @($clusterMenu)
    }
    else {
        $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $number })
    }

    break
}

# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------
foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $clusterDir = Join-Path $OutputDirectory ("{0}_{1}" -f (Safe-Name $cluster.ClusterName),$timestamp)
    $pgRoot = Join-Path $clusterDir "PGs"

    New-Item -Path $pgRoot -ItemType Directory -Force | Out-Null

    Write-Host ""
    Write-Host "Collecting active Protection Group configuration from $($cluster.ClusterName) ..." -ForegroundColor Cyan

    $errors = @()
    $summaryRows = @()
    $allParameterRows = @()
    $allDependencyRows = @()

    try {
        $policies = @(Get-AllPolicies -Headers $headers)
    }
    catch {
        $policies = @()
        $errors += [pscustomobject]@{
            Environment="ALL"
            ProtectionGroup=""
            Stage="Policies"
            Error=$_.Exception.Message
        }
    }

    try {
        $storageDomains = @(Get-AllStorageDomains -Headers $headers)
    }
    catch {
        $storageDomains = @()
        $errors += [pscustomobject]@{
            Environment="ALL"
            ProtectionGroup=""
            Stage="StorageDomains"
            Error=$_.Exception.Message
        }
    }

    try {
        $sourceRegistrations = @(Get-AllSourceRegistrations -Headers $headers)
    }
    catch {
        $sourceRegistrations = @()
        $errors += [pscustomobject]@{
            Environment="ALL"
            ProtectionGroup=""
            Stage="SourceRegistrations"
            Error=$_.Exception.Message
        }
    }

    Write-Json -Value $policies -Path (Join-Path $clusterDir "Policies_All.json")
    Write-Json -Value $storageDomains -Path (Join-Path $clusterDir "StorageDomains_All.json")
    Write-Json -Value $sourceRegistrations -Path (Join-Path $clusterDir "SourceRegistrations_All.json")

    $policyMap = Build-IdMap -Items $policies -IdNames @("id","policyId") -NameNames @("name","policyName","displayName")
    $storageMap = Build-IdMap -Items $storageDomains -IdNames @("id","storageDomainId") -NameNames @("name","storageDomainName","displayName")
    $sourceRegistrationMap = Build-SourceRegistrationIndex -Registrations $sourceRegistrations

    foreach ($environment in $EnvironmentMap) {
        try {
            $pgList = @(Get-ActiveProtectionGroups -Environment $environment.ApiName -Headers $headers)
        }
        catch {
            $errors += [pscustomobject]@{
                Environment=$environment.DisplayName
                ProtectionGroup=""
                Stage="ProtectionGroupList"
                Error=$_.Exception.Message
            }
            Write-Host "  $($environment.DisplayName): GET failed" -ForegroundColor Red
            continue
        }

        Write-Host "  $($environment.DisplayName): $($pgList.Count) active PGs" -ForegroundColor Yellow

        foreach ($pgStub in $pgList) {
            $pgId = First-Value @((Get-PropValue -Object $pgStub -Names @("id","protectionGroupId")))
            $pgName = First-Value @(
                (Get-PropValue -Object $pgStub -Names @("name","protectionGroupName")),
                $pgId
            )

            if ([string]::IsNullOrWhiteSpace($pgId)) {
                $errors += [pscustomobject]@{
                    Environment=$environment.DisplayName
                    ProtectionGroup=$pgName
                    Stage="ProtectionGroupId"
                    Error="Protection Group did not return an id."
                }
                continue
            }

            $pg = $null
            $detailSource = "DetailedGET"

            try {
                $pg = Get-ProtectionGroupDetail -ProtectionGroupId $pgId -Headers $headers
            }
            catch {
                $pg = $pgStub
                $detailSource = "ListGETFallback"
                $errors += [pscustomobject]@{
                    Environment=$environment.DisplayName
                    ProtectionGroup=$pgName
                    Stage="ProtectionGroupDetail"
                    Error=$_.Exception.Message
                }
            }

            if ($null -eq $pg) {
                $pg = $pgStub
                $detailSource = "ListGETFallback"
            }

            $isActive = Get-PropValue -Object $pg -Names @("isActive") -Default $true
            $isDeleted = Get-PropValue -Object $pg -Names @("isDeleted") -Default $false

            if ($isActive -eq $false -or $isDeleted -eq $true) {
                continue
            }

            $environmentBlock = Get-EnvironmentBlock -ProtectionGroup $pg -Names $environment.ParamNames
            $parameterBlockName = if ($environmentBlock) { $environmentBlock.Name } else { "NOT_FOUND" }
            $parameterBlockValue = if ($environmentBlock) { $environmentBlock.Value } else { $null }

            if ($parameterBlockName -eq "NOT_FOUND") {
                $errors += [pscustomobject]@{
                    Environment=$environment.DisplayName
                    ProtectionGroup=$pgName
                    Stage="EnvironmentParams"
                    Error="Expected environment parameter block was not returned."
                }
            }

            $policyId = First-Value @(
                (Get-PropValue -Object $pg -Names @("policyId")),
                (Get-NestedValue -Object $pg -Path "policyInfo.id"),
                (Get-NestedValue -Object $pg -Path "policy.id")
            )

            $policyName = ""
            $policyRaw = $null
            if ($policyId -and $policyMap.ContainsKey([string]$policyId)) {
                $policyName = $policyMap[[string]$policyId].Name
                $policyRaw = $policyMap[[string]$policyId].Raw
            }

            $storageDomainId = First-Value @(
                (Get-PropValue -Object $pg -Names @("storageDomainId")),
                (Get-NestedValue -Object $pg -Path "storageDomain.id")
            )

            $storageDomainName = First-Value @(
                (Get-PropValue -Object $pg -Names @("storageDomainName")),
                (Get-NestedValue -Object $pg -Path "storageDomain.name")
            )
            $storageDomainRaw = $null

            if ($storageDomainId -and $storageMap.ContainsKey([string]$storageDomainId)) {
                if ([string]::IsNullOrWhiteSpace($storageDomainName)) {
                    $storageDomainName = $storageMap[[string]$storageDomainId].Name
                }
                $storageDomainRaw = $storageMap[[string]$storageDomainId].Raw
            }

            $dependencyReferences = @(Get-DependencyReferences -EnvironmentParams $parameterBlockValue)
            $pgDependencyRows = @()
            $resolvedDetails = @()
            $relevantRegistrations = @{}
            $resolvedCount = 0

            foreach ($reference in $dependencyReferences) {
                $resolvedName = ""
                $resolvedAs = ""
                $resolvedEnvironment = ""
                $registeredSourceId = ""
                $registeredSourceName = ""

                if ($sourceRegistrationMap.ContainsKey([string]$reference.OriginalId)) {
                    $registration = $sourceRegistrationMap[[string]$reference.OriginalId]
                    $resolvedName = $registration.Name
                    $resolvedAs = "SourceRegistration"
                    $registeredSourceId = $registration.Id
                    $registeredSourceName = $registration.Name
                    $relevantRegistrations[[string]$registration.Id] = $registration.Raw
                }

                $resolved = Resolve-DependencyReference -ReferenceType $reference.ReferenceType -Id ([string]$reference.OriginalId) -Headers $headers

                if ($resolved) {
                    if (-not [string]::IsNullOrWhiteSpace($resolved.Name)) {
                        $resolvedName = $resolved.Name
                    }
                    $resolvedEnvironment = $resolved.Environment
                    $resolvedAs = if ($resolved.Endpoint -match '/objects/') { "Object" } else { "Source" }

                    if (-not [string]::IsNullOrWhiteSpace($resolved.SourceId)) {
                        $registeredSourceId = $resolved.SourceId

                        if ($sourceRegistrationMap.ContainsKey([string]$resolved.SourceId)) {
                            $registration = $sourceRegistrationMap[[string]$resolved.SourceId]
                            $registeredSourceName = $registration.Name
                            $relevantRegistrations[[string]$registration.Id] = $registration.Raw
                        }
                    }

                    $resolvedDetails += [pscustomobject]@{
                        ReferenceType        = $reference.ReferenceType
                        FieldPath            = $reference.FieldPath
                        OriginalId           = $reference.OriginalId
                        ResolvedAs           = $resolvedAs
                        ResolvedName         = $resolvedName
                        ResolvedEnvironment  = $resolvedEnvironment
                        RegisteredSourceId   = $registeredSourceId
                        RegisteredSourceName = $registeredSourceName
                        Raw                  = $resolved.Raw
                    }
                }

                $isResolved = -not [string]::IsNullOrWhiteSpace($resolvedName)
                if ($isResolved) {
                    $resolvedCount++
                }

                $dependencyRow = [pscustomobject]@{
                    Cluster              = $cluster.ClusterName
                    Environment          = $environment.DisplayName
                    ProtectionGroup      = $pgName
                    ProtectionGroupId    = $pgId
                    ReferenceType        = $reference.ReferenceType
                    FieldPath            = $reference.FieldPath
                    OriginalId           = $reference.OriginalId
                    Resolved             = $isResolved
                    ResolvedName         = $resolvedName
                    ResolvedAs           = $resolvedAs
                    ResolvedEnvironment  = $resolvedEnvironment
                    RegisteredSourceId   = $registeredSourceId
                    RegisteredSourceName = $registeredSourceName
                }

                $pgDependencyRows += $dependencyRow
                $allDependencyRows += $dependencyRow
            }

            $folderName = "{0}__{1}" -f (Safe-Name $pgName),(Safe-Name $pgId)
            $pgDir = Join-Path $pgRoot $folderName
            New-Item -Path $pgDir -ItemType Directory -Force | Out-Null

            Write-Json -Value $pg -Path (Join-Path $pgDir "ProtectionGroup.json")
            Write-Json -Value $parameterBlockValue -Path (Join-Path $pgDir "EnvironmentParams.json")
            Write-Json -Value $policyRaw -Path (Join-Path $pgDir "Policy.json")
            Write-Json -Value $storageDomainRaw -Path (Join-Path $pgDir "StorageDomain.json")
            Write-Json -Value $pgDependencyRows -Path (Join-Path $pgDir "DependencyReferences.json")
            Write-Json -Value $resolvedDetails -Path (Join-Path $pgDir "ResolvedSourceObjectDetails.json")
            Write-Json -Value @($relevantRegistrations.Values) -Path (Join-Path $pgDir "RelevantSourceRegistrations.json")

            $pgParameterRows = @()
            foreach ($leaf in @(Expand-LeafValue -Value $pg -Path "")) {
                $parameterRow = [pscustomobject]@{
                    Cluster           = $cluster.ClusterName
                    Environment       = $environment.DisplayName
                    ProtectionGroup   = $pgName
                    ProtectionGroupId = $pgId
                    Field             = $leaf.Field
                    Value             = $leaf.Value
                }
                $pgParameterRows += $parameterRow
                $allParameterRows += $parameterRow
            }

            $pgParameterRows |
                Sort-Object Field |
                Export-Csv (Join-Path $pgDir "ConfiguredParameters.csv") -NoTypeInformation -Encoding UTF8

            Write-Json -Value ([ordered]@{
                ExportedAt                    = (Get-Date).ToString("o")
                ReadOnly                      = $true
                RequestMethod                 = "GET"
                Cluster                       = $cluster.ClusterName
                ClusterId                     = $cluster.ClusterId
                Environment                   = $environment.DisplayName
                EnvironmentApiName            = $environment.ApiName
                ProtectionGroup               = $pgName
                ProtectionGroupId             = $pgId
                DetailSource                  = $detailSource
                ParameterBlock                = $parameterBlockName
                PolicyId                      = $policyId
                PolicyName                    = $policyName
                StorageDomainId               = $storageDomainId
                StorageDomainName             = $storageDomainName
                DependencyReferenceCount      = $dependencyReferences.Count
                ResolvedReferenceCount        = $resolvedCount
                SourceRegistrationCount       = $relevantRegistrations.Count
                ActiveOnly                    = $true
                Files                         = @(
                    "ProtectionGroup.json",
                    "EnvironmentParams.json",
                    "Policy.json",
                    "StorageDomain.json",
                    "DependencyReferences.json",
                    "ResolvedSourceObjectDetails.json",
                    "RelevantSourceRegistrations.json",
                    "ConfiguredParameters.csv"
                )
            }) -Path (Join-Path $pgDir "Manifest.json")

            $summaryRows += [pscustomobject]@{
                Cluster                 = $cluster.ClusterName
                ClusterId               = $cluster.ClusterId
                Environment             = $environment.DisplayName
                EnvironmentApiName      = $environment.ApiName
                ProtectionGroup         = $pgName
                ProtectionGroupId       = $pgId
                DetailSource            = $detailSource
                ParameterBlock          = $parameterBlockName
                PolicyId                = $policyId
                PolicyName              = $policyName
                StorageDomainId         = $storageDomainId
                StorageDomainName       = $storageDomainName
                IsActive                = $isActive
                IsDeleted               = $isDeleted
                IsPaused                = (Get-PropValue -Object $pg -Names @("isPaused"))
                ParameterFieldCount     = $pgParameterRows.Count
                DependencyReferenceCount= $dependencyReferences.Count
                ResolvedReferenceCount  = $resolvedCount
                SourceRegistrationCount = $relevantRegistrations.Count
                OutputFolder            = $folderName
            }
        }
    }

    $summaryRows |
        Sort-Object Environment,ProtectionGroup |
        Export-Csv (Join-Path $clusterDir "Active_ProtectionGroups.csv") -NoTypeInformation -Encoding UTF8

    $allParameterRows |
        Sort-Object Environment,ProtectionGroup,Field |
        Export-Csv (Join-Path $clusterDir "All_Configured_Parameters.csv") -NoTypeInformation -Encoding UTF8

    $allDependencyRows |
        Sort-Object Environment,ProtectionGroup,ReferenceType,FieldPath |
        Export-Csv (Join-Path $clusterDir "Dependency_References.csv") -NoTypeInformation -Encoding UTF8

    $errors |
        Export-Csv (Join-Path $clusterDir "Collection_Errors.csv") -NoTypeInformation -Encoding UTF8

    Write-Json -Value ([ordered]@{
        ExportedAt           = (Get-Date).ToString("o")
        Script               = "Get-CohesityDRReadyPGConfig.ps1"
        ReadOnly             = $true
        RequestMethod        = "GET"
        HeliosBaseUrl        = $baseUrl
        Cluster              = $cluster.ClusterName
        ClusterId            = $cluster.ClusterId
        ActiveOnly           = $true
        DeletedIncluded      = $false
        Environments         = @($EnvironmentMap | ForEach-Object { $_.DisplayName })
        ProtectionGroupCount = $summaryRows.Count
        CollectionErrorCount = $errors.Count
        Safety               = "GET-only. No write operations are performed."
    }) -Path (Join-Path $clusterDir "Run_Metadata.json")

    Write-Host ""
    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Active PGs exported: $($summaryRows.Count)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green

    if ($errors.Count -gt 0) {
        Write-Host "Warnings/errors: $($errors.Count) - see Collection_Errors.csv" -ForegroundColor Yellow
    }
}
