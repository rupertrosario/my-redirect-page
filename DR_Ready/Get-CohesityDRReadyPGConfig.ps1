# Cohesity Helios - DR Protection Group Reverse Engineering Export
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Purpose:
#   Reverse-engineer ACTIVE Cohesity Protection Groups into data that can be used
#   to construct a valid POST /v2/data-protect/protection-groups request later.
#
# Environments:
#   NAS, SQL, Hyper-V, Nutanix AHV, Oracle, Physical
#
# Safety:
#   This script performs GET requests only. It does NOT create, update, pause,
#   resume, activate, deactivate, or delete anything.

[CmdletBinding()]
param(
    [string]$OutputDirectory = "X:\PowerShell\Cohesity_API_Scripts\DR_Ready"
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$root                = "X:\PowerShell\Cohesity_API_Scripts"
$helperPath          = Join-Path $root "Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = Join-Path $root "Common\Secure\cohesity_apikey.enc"

$EnvironmentMap = @(
    [pscustomobject]@{ ApiName="kGenericNas"; DisplayName="NAS";         ParamNames=@("genericNasParams") },
    [pscustomobject]@{ ApiName="kSQL";        DisplayName="SQL";         ParamNames=@("mssqlParams") },
    [pscustomobject]@{ ApiName="kHyperV";     DisplayName="Hyper-V";     ParamNames=@("hypervParams","hyperVParams") },
    [pscustomobject]@{ ApiName="kAcropolis";  DisplayName="Nutanix AHV"; ParamNames=@("acropolisParams") },
    [pscustomobject]@{ ApiName="kOracle";     DisplayName="Oracle";      ParamNames=@("oracleParams") },
    [pscustomobject]@{ ApiName="kPhysical";   DisplayName="Physical";    ParamNames=@("physicalParams") }
)

# Top-level fields documented for Create Protection Group.
# lastModifiedTimestampUsecs is intentionally excluded because it is a PUT stale-write guard,
# not something needed when creating a new Protection Group.
$CreateTopLevelFields = @(
    "abortInBlackouts",
    "advancedConfigs",
    "alertPolicy",
    "description",
    "endTimeUsecs",
    "environment",
    "isPaused",
    "name",
    "pauseInBlackouts",
    "pausedNote",
    "policyId",
    "priority",
    "qosPolicy",
    "sla",
    "startTime",
    "storageDomainId"
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
        [Parameter(Mandatory=$true)][string]$Uri,
        [Parameter(Mandatory=$true)][hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
    }
    else {
        $resp = Invoke-WebRequest -Uri $Uri -Headers $Headers -Method Get -ErrorAction Stop
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
        foreach ($prop in @($Object.PSObject.Properties)) {
            if ($prop.Name -ieq $name) {
                if ($null -ne $prop.Value) { return $prop.Value }
                return $Default
            }
        }
    }

    return $Default
}

function Test-PropExists {
    param($Object,[string]$Name)

    if ($null -eq $Object -or $Object -is [string]) { return $false }

    foreach ($prop in @($Object.PSObject.Properties)) {
        if ($prop.Name -ieq $Name) { return $true }
    }

    return $false
}

function First-Value {
    param($Values)

    foreach ($v in @($Values)) {
        foreach ($item in @($v)) {
            if ($null -ne $item -and "$item".Trim() -ne "") {
                return "$item"
            }
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

        for ($i=0; $i -lt $items.Count; $i++) {
            Expand-LeafValue -Value $items[$i] -Path "$Path[$i]"
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
        if (Test-PropExists -Object $ProtectionGroup -Name $name) {
            return [pscustomobject]@{
                Name  = $name
                Value = Get-PropValue -Object $ProtectionGroup -Names @($name)
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
        # Kept intentionally close to the working inventory scripts.
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
        $isResponseTruncated = Get-PropValue -Object $json -Names @("isResponseTruncated") -Default $false

        if ($isResponseTruncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) {
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

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) { return $null }

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

    try {
        $json = Get-Json -Uri "$baseUrl/v2/storage-domains?includeStats=false" -Headers $Headers
        $items = Get-PropValue -Object $json -Names @("storageDomains","items") -Default $null
        if ($null -ne $items) { return @(As-Array $items) }
        if ($json -is [array]) { return @($json) }
        if ($json) { return @($json) }
    }
    catch {
        return @()
    }

    return @()
}

function Get-AllSourceRegistrations {
    param([hashtable]$Headers)

    # No optional query parameters here. This mirrors the simplest documented GET.
    $json = Get-Json -Uri "$baseUrl/v2/data-protect/sources/registrations" -Headers $Headers
    $items = Get-PropValue -Object $json -Names @("registrations","items") -Default $null

    if ($null -ne $items) { return @(As-Array $items) }
    if ($json -is [array]) { return @($json) }
    if ($json) { return @($json) }

    return @()
}

function Build-IdNameMap {
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
            $map[$id] = [pscustomobject]@{
                Id   = $id
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

    foreach ($reg in @(As-Array $Registrations | Where-Object { $_ })) {
        $sourceInfo = Get-PropValue -Object $reg -Names @("sourceInfo") -Default $null

        $ids = @(
            First-Value @((Get-PropValue -Object $reg -Names @("id","sourceId"))),
            First-Value @((Get-PropValue -Object $sourceInfo -Names @("id","sourceId","entityId")))
        ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique

        $name = First-Value @(
            (Get-PropValue -Object $reg -Names @("name","sourceName","displayName")),
            (Get-PropValue -Object $sourceInfo -Names @("name","sourceName","displayName","hostName"))
        )

        foreach ($id in $ids) {
            $map[[string]$id] = [pscustomobject]@{
                Id   = [string]$id
                Name = $name
                Raw  = $reg
            }
        }
    }

    return $map
}

function Get-DependencyReferences {
    param($EnvironmentParams)

    $refs = @()

    if ($null -eq $EnvironmentParams) { return @() }

    foreach ($leaf in @(Expand-LeafValue -Value $EnvironmentParams -Path "")) {
        $field = [string]$leaf.Field
        $value = [string]$leaf.Value

        if ([string]::IsNullOrWhiteSpace($value) -or $value -eq "<null>") { continue }

        $type = $null

        if ($field -match '(?i)(^|\.)(sourceId|sourceIds)(\[\d+\])?$') {
            $type = "Source"
        }
        elseif (
            $field -match '(?i)(^|\.)(objectId|objectIds)(\[\d+\])?$' -or
            $field -match '(?i)(^|\.)objects\[\d+\]\.id$'
        ) {
            $type = "Object"
        }
        elseif ($field -match '(?i)(^|\.)(excludedSourceIds|includedSourceIds)(\[\d+\])?$') {
            $type = "Source"
        }

        if ($type) {
            $refs += [pscustomobject]@{
                ReferenceType = $type
                FieldPath     = $field
                OriginalId    = $value
            }
        }
    }

    return @($refs | Sort-Object ReferenceType,FieldPath,OriginalId -Unique)
}

function Resolve-DependencyReference {
    param(
        [string]$ReferenceType,
        [string]$Id,
        [hashtable]$Headers
    )

    if ([string]::IsNullOrWhiteSpace($Id)) { return $null }
    if ($Id -notmatch '^\d+$') { return $null }

    $paths = if ($ReferenceType -eq "Object") {
        @(
            "$baseUrl/v2/data-protect/objects/$Id",
            "$baseUrl/v2/data-protect/sources/$Id"
        )
    }
    else {
        @(
            "$baseUrl/v2/data-protect/sources/$Id",
            "$baseUrl/v2/data-protect/objects/$Id"
        )
    }

    foreach ($uri in $paths) {
        try {
            $raw = Get-Json -Uri $uri -Headers $Headers
            if ($null -eq $raw) { continue }

            if ($raw -is [array]) {
                $raw = @($raw | Where-Object { $_ } | Select-Object -First 1)
                if ($raw.Count -eq 0) { continue }
                $raw = $raw[0]
            }

            $name = First-Value @(
                (Get-PropValue -Object $raw -Names @("name","objectName","sourceName","displayName","hostName"))
            )
            $environment = First-Value @(
                (Get-PropValue -Object $raw -Names @("environment"))
            )
            $sourceId = First-Value @(
                (Get-PropValue -Object $raw -Names @("sourceId"))
            )

            return [pscustomobject]@{
                Endpoint    = $uri
                Name        = $name
                Environment = $environment
                SourceId    = $sourceId
                Raw         = $raw
            }
        }
        catch {
            continue
        }
    }

    return $null
}

function New-RecreatePayload {
    param(
        $ProtectionGroup,
        [string]$EnvironmentBlockName,
        $EnvironmentBlockValue
    )

    $payload = [ordered]@{}

    foreach ($field in $CreateTopLevelFields) {
        if (Test-PropExists -Object $ProtectionGroup -Name $field) {
            $payload[$field] = Get-PropValue -Object $ProtectionGroup -Names @($field)
        }
    }

    if (-not $payload.Contains("environment")) {
        $payload["environment"] = Get-PropValue -Object $ProtectionGroup -Names @("environment")
    }

    if (-not [string]::IsNullOrWhiteSpace($EnvironmentBlockName) -and $EnvironmentBlockName -ne "NOT_FOUND") {
        $payload[$EnvironmentBlockName] = $EnvironmentBlockValue
    }

    return [pscustomobject]$payload
}

# ---------------------------------------------------------------------------
# Helios cluster selection
# Copied structurally from the known-working Physical inventory pattern.
# ---------------------------------------------------------------------------

$clusterResponse = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$rawClusters = @(As-Array (Get-PropValue -Object $clusterResponse -Names @("cohesityClusters")))

if (-not $rawClusters -or $rawClusters.Count -eq 0) {
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

        if ([string]::IsNullOrWhiteSpace($name)) { $name = "Unknown-$id" }

        [pscustomobject]@{
            ClusterName = $name
            ClusterId   = $id
            Raw          = $_
        }
    } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ClusterId) } |
    Sort-Object ClusterName
)

$clusterMenu = for ($i=0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{
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
    if (-not [int]::TryParse($selection,[ref]$n)) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($n -lt 0 -or $n -gt $clusterMenu.Count) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($n -eq 0) {
        $selectedClusters = @($clusterMenu)
    }
    else {
        $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $n })
    }

    break
}

# ---------------------------------------------------------------------------
# Reverse-engineering collection
# ---------------------------------------------------------------------------

foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $clusterDir = Join-Path $OutputDirectory ("{0}_{1}" -f (Safe-Name $cluster.ClusterName),$timestamp)
    $pgRoot = Join-Path $clusterDir "PGs"

    New-Item -Path $pgRoot -ItemType Directory -Force | Out-Null

    Write-Host ""
    Write-Host "Reverse-engineering active PGs from $($cluster.ClusterName) ..." -ForegroundColor Cyan

    $errors = @()
    $summary = @()
    $allPgFields = @()
    $recreateFields = @()
    $dependencyRows = @()

    # Dependencies needed to recreate a PG on another cluster.
    try {
        $policies = @(Get-AllPolicies -Headers $headers)
    }
    catch {
        $policies = @()
        $errors += [pscustomobject]@{
            Environment="ALL"; ProtectionGroup=""; Stage="Policies"; Error=$_.Exception.Message
        }
    }

    try {
        $storageDomains = @(Get-AllStorageDomains -Headers $headers)
    }
    catch {
        $storageDomains = @()
        $errors += [pscustomobject]@{
            Environment="ALL"; ProtectionGroup=""; Stage="StorageDomains"; Error=$_.Exception.Message
        }
    }

    try {
        $sourceRegistrations = @(Get-AllSourceRegistrations -Headers $headers)
    }
    catch {
        $sourceRegistrations = @()
        $errors += [pscustomobject]@{
            Environment="ALL"; ProtectionGroup=""; Stage="SourceRegistrations"; Error=$_.Exception.Message
        }
    }

    Write-Json -Value $policies -Path (Join-Path $clusterDir "Policies_All.json")
    Write-Json -Value $storageDomains -Path (Join-Path $clusterDir "StorageDomains_All.json")
    Write-Json -Value $sourceRegistrations -Path (Join-Path $clusterDir "SourceRegistrations_All.json")

    $policyMap = Build-IdNameMap -Items $policies -IdNames @("id","policyId") -NameNames @("name","policyName","displayName")
    $storageMap = Build-IdNameMap -Items $storageDomains -IdNames @("id","storageDomainId") -NameNames @("name","storageDomainName","displayName")
    $sourceRegistrationMap = Build-SourceRegistrationIndex -Registrations $sourceRegistrations

    foreach ($env in $EnvironmentMap) {
        Write-Host "  $($env.DisplayName) ..." -ForegroundColor Yellow

        try {
            $pgList = @(Get-ActiveProtectionGroups -Environment $env.ApiName -Headers $headers)
        }
        catch {
            $errors += [pscustomobject]@{
                Environment=$env.DisplayName
                ProtectionGroup=""
                Stage="ListActiveProtectionGroups"
                Error=$_.Exception.Message
            }
            Write-Host "    GET failed - recorded in Collection_Errors.csv" -ForegroundColor Red
            continue
        }

        Write-Host "    Active PGs: $($pgList.Count)" -ForegroundColor DarkGray

        foreach ($pgStub in $pgList) {
            $pgId = First-Value @(
                (Get-PropValue -Object $pgStub -Names @("id","protectionGroupId"))
            )
            $pgName = First-Value @(
                (Get-PropValue -Object $pgStub -Names @("name","protectionGroupName")),
                $pgId
            )

            if ([string]::IsNullOrWhiteSpace($pgId)) {
                $errors += [pscustomobject]@{
                    Environment=$env.DisplayName
                    ProtectionGroup=$pgName
                    Stage="ProtectionGroupId"
                    Error="PG did not return an id."
                }
                continue
            }

            # Prefer the documented single-PG GET because it can explicitly retain source IDs.
            # If that endpoint fails through Helios for a particular cluster/version, fall back
            # to the list response rather than stopping the export.
            $pg = $null
            $detailSource = "SinglePG_GET"

            try {
                $pg = Get-ProtectionGroupDetail -ProtectionGroupId $pgId -Headers $headers
            }
            catch {
                $pg = $pgStub
                $detailSource = "List_GET_Fallback"
                $errors += [pscustomobject]@{
                    Environment=$env.DisplayName
                    ProtectionGroup=$pgName
                    Stage="ProtectionGroupDetailFallback"
                    Error=$_.Exception.Message
                }
            }

            if ($null -eq $pg) {
                $pg = $pgStub
                $detailSource = "List_GET_Fallback"
            }

            $environmentBlock = Get-EnvironmentBlock -ProtectionGroup $pg -Names $env.ParamNames
            $blockName = if ($environmentBlock) { $environmentBlock.Name } else { "NOT_FOUND" }
            $blockValue = if ($environmentBlock) { $environmentBlock.Value } else { $null }

            $policyId = First-Value @((Get-PropValue -Object $pg -Names @("policyId")))
            $storageDomainId = First-Value @((Get-PropValue -Object $pg -Names @("storageDomainId")))

            $policyName = ""
            $policyRaw = $null
            if ($policyId -and $policyMap.ContainsKey($policyId)) {
                $policyName = $policyMap[$policyId].Name
                $policyRaw = $policyMap[$policyId].Raw
            }

            $storageDomainName = ""
            $storageDomainRaw = $null
            if ($storageDomainId -and $storageMap.ContainsKey($storageDomainId)) {
                $storageDomainName = $storageMap[$storageDomainId].Name
                $storageDomainRaw = $storageMap[$storageDomainId].Raw
            }

            $recreatePayload = New-RecreatePayload `
                -ProtectionGroup $pg `
                -EnvironmentBlockName $blockName `
                -EnvironmentBlockValue $blockValue

            $refs = @(Get-DependencyReferences -EnvironmentParams $blockValue)
            $resolvedRefCount = 0
            $pgDependencyRows = @()
            $pgResolvedReferences = @()
            $relevantRegistrationMap = @{}

            foreach ($ref in $refs) {
                $resolvedName = ""
                $resolvedAs = ""
                $resolvedEnvironment = ""
                $registeredSourceId = ""
                $registeredSourceName = ""

                # First resolve direct registration ids from the source-registration inventory.
                if ($sourceRegistrationMap.ContainsKey([string]$ref.OriginalId)) {
                    $regMatch = $sourceRegistrationMap[[string]$ref.OriginalId]
                    $resolvedName = $regMatch.Name
                    $resolvedAs = "SourceRegistration"
                    $registeredSourceId = [string]$regMatch.Id
                    $registeredSourceName = $regMatch.Name
                    $relevantRegistrationMap[$registeredSourceId] = $regMatch.Raw
                }

                # Then resolve the referenced object/source itself so we have a stable
                # human identity for cross-cluster remapping instead of only an integer id.
                $resolved = Resolve-DependencyReference `
                    -ReferenceType $ref.ReferenceType `
                    -Id ([string]$ref.OriginalId) `
                    -Headers $headers

                if ($resolved) {
                    if (-not [string]::IsNullOrWhiteSpace($resolved.Name)) {
                        $resolvedName = $resolved.Name
                    }
                    $resolvedEnvironment = $resolved.Environment
                    $resolvedAs = if ($resolved.Endpoint -match '/objects/') { "Object" } else { "Source" }

                    if (-not [string]::IsNullOrWhiteSpace($resolved.SourceId)) {
                        $registeredSourceId = $resolved.SourceId

                        if ($sourceRegistrationMap.ContainsKey([string]$resolved.SourceId)) {
                            $regMatch = $sourceRegistrationMap[[string]$resolved.SourceId]
                            $registeredSourceName = $regMatch.Name
                            $relevantRegistrationMap[[string]$resolved.SourceId] = $regMatch.Raw
                        }
                    }

                    $pgResolvedReferences += [pscustomobject]@{
                        ReferenceType       = $ref.ReferenceType
                        FieldPath           = $ref.FieldPath
                        OriginalId          = $ref.OriginalId
                        ResolvedAs          = $resolvedAs
                        ResolvedName        = $resolvedName
                        ResolvedEnvironment = $resolvedEnvironment
                        RegisteredSourceId  = $registeredSourceId
                        RegisteredSourceName= $registeredSourceName
                        Raw                 = $resolved.Raw
                    }
                }

                $isResolved = -not [string]::IsNullOrWhiteSpace($resolvedName)
                if ($isResolved) { $resolvedRefCount++ }

                $row = [pscustomobject]@{
                    Cluster               = $cluster.ClusterName
                    Environment           = $env.DisplayName
                    ProtectionGroup       = $pgName
                    ProtectionGroupId     = $pgId
                    ReferenceType         = $ref.ReferenceType
                    FieldPath             = $ref.FieldPath
                    OriginalId            = $ref.OriginalId
                    Resolved              = $isResolved
                    ResolvedName          = $resolvedName
                    ResolvedAs            = $resolvedAs
                    ResolvedEnvironment   = $resolvedEnvironment
                    RegisteredSourceId    = $registeredSourceId
                    RegisteredSourceName  = $registeredSourceName
                    RequiresDRIdRemap     = $true
                }

                $pgDependencyRows += $row
                $dependencyRows += $row
            }

            $folderName = "{0}__{1}" -f (Safe-Name $pgName),(Safe-Name $pgId)
            $pgDir = Join-Path $pgRoot $folderName
            New-Item -Path $pgDir -ItemType Directory -Force | Out-Null

            Write-Json -Value $pg -Path (Join-Path $pgDir "01_OriginalPG.json")
            Write-Json -Value $recreatePayload -Path (Join-Path $pgDir "02_RecreatePayload_Candidate.json")
            Write-Json -Value $blockValue -Path (Join-Path $pgDir "03_EnvironmentParams.json")
            Write-Json -Value $policyRaw -Path (Join-Path $pgDir "04_ReferencedPolicy.json")
            Write-Json -Value $storageDomainRaw -Path (Join-Path $pgDir "05_ReferencedStorageDomain.json")
            Write-Json -Value $pgDependencyRows -Path (Join-Path $pgDir "06_DependencyReferences.json")
            Write-Json -Value $pgResolvedReferences -Path (Join-Path $pgDir "07_ResolvedSourceObjectDetails.json")
            Write-Json -Value @($relevantRegistrationMap.Values) -Path (Join-Path $pgDir "08_RelevantSourceRegistrations.json")

            foreach ($leaf in @(Expand-LeafValue -Value $pg -Path "")) {
                $allPgFields += [pscustomobject]@{
                    Cluster=$cluster.ClusterName
                    Environment=$env.DisplayName
                    ProtectionGroup=$pgName
                    ProtectionGroupId=$pgId
                    Field=$leaf.Field
                    Value=$leaf.Value
                }
            }

            foreach ($leaf in @(Expand-LeafValue -Value $recreatePayload -Path "")) {
                $recreateFields += [pscustomobject]@{
                    Cluster=$cluster.ClusterName
                    Environment=$env.DisplayName
                    ProtectionGroup=$pgName
                    ProtectionGroupId=$pgId
                    Field=$leaf.Field
                    Value=$leaf.Value
                }
            }

            $missingRequired = @()
            if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -Object $recreatePayload -Names @("name")))) {
                $missingRequired += "name"
            }
            if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -Object $recreatePayload -Names @("environment")))) {
                $missingRequired += "environment"
            }
            if ([string]::IsNullOrWhiteSpace([string](Get-PropValue -Object $recreatePayload -Names @("policyId")))) {
                $missingRequired += "policyId"
            }
            if ($blockName -eq "NOT_FOUND") {
                $missingRequired += "environmentParams"
            }

            $candidateReady = ($missingRequired.Count -eq 0)

            $summary += [pscustomobject]@{
                Cluster                    = $cluster.ClusterName
                ClusterId                  = $cluster.ClusterId
                Environment                = $env.DisplayName
                EnvironmentApiName         = $env.ApiName
                ProtectionGroup            = $pgName
                ProtectionGroupId          = $pgId
                DetailSource               = $detailSource
                ParameterBlock             = $blockName
                PolicyId                   = $policyId
                PolicyName                 = $policyName
                StorageDomainId            = $storageDomainId
                StorageDomainName          = $storageDomainName
                DependencyReferenceCount   = $refs.Count
                ResolvedReferenceCount     = $resolvedRefCount
                RelevantSourceRegistrationCount = $relevantRegistrationMap.Count
                RecreateCandidateReady     = $candidateReady
                MissingRequiredFields      = ($missingRequired -join ";")
                CrossClusterRemapRequired  = $true
                OutputFolder               = $folderName
            }

            Write-Json -Value ([ordered]@{
                ExportVersion = "2.0"
                Purpose = "Reverse-engineer this active PG into a future create request"
                ReadOnly = $true
                Cluster = $cluster.ClusterName
                ClusterId = $cluster.ClusterId
                Environment = $env.DisplayName
                EnvironmentApiName = $env.ApiName
                ProtectionGroup = $pgName
                ProtectionGroupId = $pgId
                DetailSource = $detailSource
                ActiveOnly = $true
                CandidateCreateEndpoint = "POST /v2/data-protect/protection-groups"
                CandidatePayloadFile = "02_RecreatePayload_Candidate.json"
                ResolvedSourceObjectFile = "07_ResolvedSourceObjectDetails.json"
                RelevantSourceRegistrationsFile = "08_RelevantSourceRegistrations.json"
                CandidateReady = $candidateReady
                MissingRequiredFields = @($missingRequired)
                RequiredCrossClusterRemaps = @(
                    "policyId -> target cluster policy id",
                    "storageDomainId -> target cluster storage domain id when present",
                    "source/object ids inside $blockName -> target cluster ids"
                )
                CredentialNote = "Protection source credentials are not exported. If a source must be re-registered on the DR cluster, credentials must come from an approved external secret source."
                Safety = "GET-only. Candidate JSON is written to disk only; it is never POSTed."
            }) -Path (Join-Path $pgDir "00_Manifest.json")
        }
    }

    $summary |
        Sort-Object Environment,ProtectionGroup |
        Export-Csv (Join-Path $clusterDir "Recreate_Readiness.csv") -NoTypeInformation -Encoding UTF8

    $allPgFields |
        Sort-Object Environment,ProtectionGroup,Field |
        Export-Csv (Join-Path $clusterDir "OriginalPG_All_Fields.csv") -NoTypeInformation -Encoding UTF8

    $recreateFields |
        Sort-Object Environment,ProtectionGroup,Field |
        Export-Csv (Join-Path $clusterDir "RecreatePayload_All_Fields.csv") -NoTypeInformation -Encoding UTF8

    $dependencyRows |
        Sort-Object Environment,ProtectionGroup,ReferenceType,FieldPath |
        Export-Csv (Join-Path $clusterDir "Dependency_References.csv") -NoTypeInformation -Encoding UTF8

    $errors |
        Export-Csv (Join-Path $clusterDir "Collection_Errors.csv") -NoTypeInformation -Encoding UTF8

    Write-Json -Value ([ordered]@{
        ExportVersion = "2.0"
        Script = "Get-CohesityDRReadyPGConfig.ps1"
        ExportedAt = (Get-Date).ToString("o")
        HeliosBaseUrl = $baseUrl
        Cluster = $cluster.ClusterName
        ClusterId = $cluster.ClusterId
        ActiveOnly = $true
        DeletedIncluded = $false
        Environments = @($EnvironmentMap | ForEach-Object { $_.DisplayName })
        ProtectionGroupCount = @($summary).Count
        RecreateCandidateReadyCount = @($summary | Where-Object { $_.RecreateCandidateReady }).Count
        CollectionErrorCount = @($errors).Count
        Safety = "GET-only. No POST, PUT, PATCH, DELETE, state change, pause, resume, activate, or deactivate calls."
    }) -Path (Join-Path $clusterDir "Run_Metadata.json")

    Write-Host ""
    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green
    Write-Host "Active PGs reverse-engineered: $(@($summary).Count)" -ForegroundColor Green
    Write-Host "Recreate candidates with required fields: $(@($summary | Where-Object { $_.RecreateCandidateReady }).Count)" -ForegroundColor Green

    if (@($errors).Count -gt 0) {
        Write-Host "Warnings/errors: $(@($errors).Count) - see Collection_Errors.csv" -ForegroundColor Yellow
    }
}
