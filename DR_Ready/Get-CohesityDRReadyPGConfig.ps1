# Cohesity Helios - Active Protection Group Evidence Collector
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Purpose:
#   Collect authoritative configuration evidence for ACTIVE Protection Groups.
#   No Cohesity write operation is implemented by this script.
#
# Environments:
#   NAS, SQL, Hyper-V, Nutanix AHV, Oracle, Physical

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
    [pscustomobject]@{ ApiName="kGenericNas"; DisplayName="NAS";         ParamNames=@("genericNasParams") },
    [pscustomobject]@{ ApiName="kSQL";        DisplayName="SQL";         ParamNames=@("mssqlParams") },
    [pscustomobject]@{ ApiName="kHyperV";     DisplayName="Hyper-V";     ParamNames=@("hypervParams","hyperVParams") },
    [pscustomobject]@{ ApiName="kAcropolis";  DisplayName="Nutanix AHV"; ParamNames=@("acropolisParams","nutanixParams","ahvParams") },
    [pscustomobject]@{ ApiName="kOracle";      DisplayName="Oracle";      ParamNames=@("oracleParams") },
    [pscustomobject]@{ ApiName="kPhysical";    DisplayName="Physical";    ParamNames=@("physicalParams") }
)

# Safety invariant: fail if a future edit introduces an HTTP method other than GET.
if ($PSCommandPath -and (Test-Path $PSCommandPath -PathType Leaf)) {
    $selfText = Get-Content -Path $PSCommandPath -Raw
    $writeMethodNames = @("Po" + "st", "P" + "ut", "Pa" + "tch", "Del" + "ete")
    $writeMethodPattern = '(?im)-Method\s+(' + (($writeMethodNames | ForEach-Object { [regex]::Escape($_) }) -join '|') + ')\b'
    if ($selfText -match $writeMethodPattern) {
        throw "Safety validation failed: a non-GET HTTP method exists in this script."
    }
}

if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "Missing API key helper: $helperPath"
}
if (-not (Test-Path $keyFile -PathType Leaf)) {
    throw "Missing encrypted API key file: $keyFile"
}
if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

# Match the key-loading pattern used by the known-working inventory script.
. $helperPath
$keyLoader = "Get-Cohesity" + "ApiKeyFromAes"
try {
    $apiKey = & $keyLoader -EncryptedFile $keyFile
}
catch {
    throw "API key decryption failed in '$helperPath' while reading '$keyFile'. Original error: $($_.Exception.Message)"
}
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "API key helper returned an empty value."
}

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

    if ($null -eq $Object -or $Object -is [string]) { return $Default }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                if ($null -ne $property.Value) { return $property.Value }
                return $Default
            }
        }
    }

    return $Default
}

function Get-NestedValue {
    param($Object,[string]$Path)

    if ($null -eq $Object -or [string]::IsNullOrWhiteSpace($Path)) { return $null }

    $current = $Object
    foreach ($segment in ($Path -split "\.")) {
        if ($null -eq $current -or $current -is [string]) { return $null }
        $current = Get-PropValue -Object $current -Names @($segment)
    }

    return $current
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

function Write-Json {
    param(
        [AllowNull()]$Value,
        [Parameter(Mandatory=$true)][string]$Path
    )

    ConvertTo-Json -InputObject $Value -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Add-ErrorRecord {
    param(
        [System.Collections.ArrayList]$ErrorList,
        [string]$Cluster,
        [string]$Environment,
        [string]$ProtectionGroup,
        [string]$Stage,
        [string]$Message
    )

    [void]$ErrorList.Add([pscustomobject][ordered]@{
        Cluster=$Cluster
        Environment=$Environment
        ProtectionGroup=$ProtectionGroup
        Stage=$Stage
        Error=$Message
    })
}

function Get-EnvironmentBlock {
    param($ProtectionGroup,[string[]]$Names)

    foreach ($name in $Names) {
        $value = Get-PropValue -Object $ProtectionGroup -Names @($name)
        if ($null -ne $value) {
            return [pscustomobject]@{ Name=$name; Value=$value }
        }
    }

    return $null
}

function Get-ActiveProtectionGroups {
    param([string]$Environment,[hashtable]$Headers)

    $all = @()
    $cookie = ""

    do {
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=$Environment&isDeleted=false&isActive=true&includeLastRunInfo=false&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-Json -Uri $uri -Headers $Headers
        $groups = Get-PropValue -Object $json -Names @("protectionGroups") -Default @()
        if ($groups) { $all += @(As-Array $groups | Where-Object { $_ }) }

        $cookie = First-Value @((Get-PropValue -Object $json -Names @("paginationCookie") -Default ""))
        $truncated = Get-PropValue -Object $json -Names @("isResponseTruncated") -Default $false

        if ($truncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-ProtectionGroupDetail {
    param([string]$ProtectionGroupId,[hashtable]$Headers)

    if ([string]::IsNullOrWhiteSpace($ProtectionGroupId)) { return $null }

    $encodedId = [uri]::EscapeDataString($ProtectionGroupId)
    $uri = "$baseUrl/v2/data-protect/protection-groups/$encodedId?includeLastRunInfo=false&pruneSourceIds=false"
    return Get-Json -Uri $uri -Headers $Headers
}

function Invoke-FirstSuccessfulGet {
    param(
        [string[]]$Uris,
        [hashtable]$Headers
    )

    $attemptErrors = @()

    foreach ($uri in $Uris) {
        try {
            $data = Get-Json -Uri $uri -Headers $Headers
            return [pscustomobject][ordered]@{
                Status="SUCCESS"
                Uri=$uri
                Data=$data
                Error=""
                Attempts=@($attemptErrors)
            }
        }
        catch {
            $attemptErrors += [pscustomobject]@{
                Uri=$uri
                Error=$_.Exception.Message
            }
        }
    }

    return [pscustomobject][ordered]@{
        Status="FAILED"
        Uri=""
        Data=$null
        Error=(($attemptErrors | ForEach-Object { $_.Error }) -join " | ")
        Attempts=@($attemptErrors)
    }
}

function Convert-ToItemArray {
    param($Data,[string[]]$ContainerNames)

    if ($null -eq $Data) { return @() }

    $items = Get-PropValue -Object $Data -Names $ContainerNames -Default $null
    if ($null -ne $items) { return @(As-Array $items | Where-Object { $_ }) }
    if ($Data -is [array]) { return @($Data | Where-Object { $_ }) }
    return @($Data)
}

function Get-PolicyEvidence {
    param([hashtable]$Headers)

    $result = Invoke-FirstSuccessfulGet -Uris @(
        "$baseUrl/v2/data-protect/policies?maxResultCount=1000",
        "$baseUrl/v2/data-protect/policies"
    ) -Headers $Headers

    $items = if ($result.Status -eq "SUCCESS") {
        @(Convert-ToItemArray -Data $result.Data -ContainerNames @("policies","policyList","items"))
    }
    else { @() }

    return [pscustomobject][ordered]@{
        Status=$result.Status
        Uri=$result.Uri
        Items=$items
        Error=$result.Error
        Attempts=$result.Attempts
    }
}

function Get-StorageDomainEvidence {
    param([hashtable]$Headers)

    $result = Invoke-FirstSuccessfulGet -Uris @(
        "$baseUrl/v2/storage-domains?includeStats=false",
        "$baseUrl/v2/storage-domains"
    ) -Headers $Headers

    $items = if ($result.Status -eq "SUCCESS") {
        @(Convert-ToItemArray -Data $result.Data -ContainerNames @("storageDomains","items"))
    }
    else { @() }

    return [pscustomobject][ordered]@{
        Status=$result.Status
        Uri=$result.Uri
        Items=$items
        Error=$result.Error
        Attempts=$result.Attempts
    }
}

function Test-ContainsSensitiveValue {
    param($Value)

    if ($null -eq $Value) { return $false }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $child = $Value[$key]
            if ([string]$key -match '(?i)(password|credential|secret|privateKey|accessKey|apiKey|token)') {
                if ($null -ne $child -and "$child".Trim() -ne "") { return $true }
            }
            if (Test-ContainsSensitiveValue -Value $child) { return $true }
        }
        return $false
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        foreach ($item in @($Value)) {
            if (Test-ContainsSensitiveValue -Value $item) { return $true }
        }
        return $false
    }

    if ($Value -isnot [string]) {
        foreach ($property in @($Value.PSObject.Properties)) {
            if ($property.Name -match '(?i)(password|credential|secret|privateKey|accessKey|apiKey|token)') {
                if ($null -ne $property.Value -and "$($property.Value)".Trim() -ne "") { return $true }
            }
            if (Test-ContainsSensitiveValue -Value $property.Value) { return $true }
        }
    }

    return $false
}

function Get-SourceRegistrationEvidence {
    param([hashtable]$Headers)

    # Never fall back to a request that could include credentials.
    $uri = "$baseUrl/v2/data-protect/sources/registrations?includeSourceCredentials=false"

    try {
        $data = Get-Json -Uri $uri -Headers $Headers
        $items = @(Convert-ToItemArray -Data $data -ContainerNames @("registrations","sourceRegistrations","items"))

        if (Test-ContainsSensitiveValue -Value $items) {
            return [pscustomobject][ordered]@{
                Status="BLOCKED_SENSITIVE_FIELDS"
                Uri=$uri
                Items=@()
                Error="Source-registration response contained a non-empty credential/secret-like field. Raw registration data was not written."
            }
        }

        return [pscustomobject][ordered]@{
            Status="SUCCESS"
            Uri=$uri
            Items=$items
            Error=""
        }
    }
    catch {
        return [pscustomobject][ordered]@{
            Status="FAILED"
            Uri=$uri
            Items=@()
            Error=$_.Exception.Message
        }
    }
}

function Build-IdMap {
    param($Items,[string[]]$IdNames,[string[]]$NameNames)

    $map = @{}
    foreach ($item in @(As-Array $Items | Where-Object { $_ })) {
        $id = First-Value @((Get-PropValue -Object $item -Names $IdNames))
        $name = First-Value @((Get-PropValue -Object $item -Names $NameNames))
        if (-not [string]::IsNullOrWhiteSpace($id)) {
            $map[[string]$id] = [pscustomobject]@{
                Id=[string]$id
                Name=$name
                Raw=$item
            }
        }
    }

    return $map
}

function Add-SourceNodeToIndex {
    param(
        $Node,
        [hashtable]$Map,
        $Registration
    )

    if ($null -eq $Node -or $Node -is [string]) { return }

    $id = First-Value @((Get-PropValue -Object $Node -Names @("id","sourceId","entityId","objectId")))
    $name = First-Value @((Get-PropValue -Object $Node -Names @("name","sourceName","displayName","hostName","objectName")))

    if (-not [string]::IsNullOrWhiteSpace($id)) {
        $Map[[string]$id] = [pscustomobject]@{
            Id=[string]$id
            Name=$name
            Raw=$Node
            Registration=$Registration
        }
    }

    foreach ($childName in @("childObjects","children","objects")) {
        $children = Get-PropValue -Object $Node -Names @($childName) -Default @()
        foreach ($child in @(As-Array $children | Where-Object { $_ })) {
            Add-SourceNodeToIndex -Node $child -Map $Map -Registration $Registration
        }
    }
}

function Build-SourceRegistrationIndex {
    param($Registrations)

    $map = @{}
    foreach ($registration in @(As-Array $Registrations | Where-Object { $_ })) {
        Add-SourceNodeToIndex -Node $registration -Map $map -Registration $registration
        $sourceInfo = Get-PropValue -Object $registration -Names @("sourceInfo") -Default $null
        if ($sourceInfo) {
            Add-SourceNodeToIndex -Node $sourceInfo -Map $map -Registration $registration
        }
    }

    return $map
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
        $Value -is [string] -or $Value -is [char] -or $Value -is [bool] -or
        $Value -is [byte] -or $Value -is [sbyte] -or $Value -is [int16] -or
        $Value -is [uint16] -or $Value -is [int32] -or $Value -is [uint32] -or
        $Value -is [int64] -or $Value -is [uint64] -or $Value -is [single] -or
        $Value -is [double] -or $Value -is [decimal] -or $Value -is [datetime] -or
        $Value -is [guid]
    ) {
        [pscustomobject]@{ Field=$Path; Value=[string]$Value }
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { [string]$key } else { "$Path.$key" }
            Expand-LeafValue -Value $Value[$key] -Path $childPath
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @($Value)
        for ($index=0; $index -lt $items.Count; $index++) {
            Expand-LeafValue -Value $items[$index] -Path "$Path[$index]"
        }
        return
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { $property.Name } else { "$Path.$($property.Name)" }
        Expand-LeafValue -Value $property.Value -Path $childPath
    }
}

function Get-DependencyReferences {
    param($EnvironmentParams)

    $references = @()
    if ($null -eq $EnvironmentParams) { return @() }

    foreach ($leaf in @(Expand-LeafValue -Value $EnvironmentParams -Path "")) {
        $field = [string]$leaf.Field
        $value = [string]$leaf.Value

        if ([string]::IsNullOrWhiteSpace($value) -or $value -eq "<null>") { continue }

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

        if ($referenceType) {
            $references += [pscustomobject][ordered]@{
                ReferenceType=$referenceType
                FieldPath=$field
                OriginalId=$value
            }
        }
    }

    return @($references | Sort-Object ReferenceType,FieldPath,OriginalId -Unique)
}

function Resolve-DependencyReference {
    param(
        [string]$ReferenceType,
        [string]$Id,
        [hashtable]$Headers,
        [hashtable]$SourceIndex
    )

    if ([string]::IsNullOrWhiteSpace($Id)) {
        return [pscustomobject][ordered]@{
            Status="UNRESOLVED"
            ResolutionSource=""
            Name=""
            Environment=""
            SourceId=""
            Raw=$null
            Registration=$null
            Error="Empty reference id."
        }
    }

    if ($SourceIndex.ContainsKey($Id)) {
        $match = $SourceIndex[$Id]
        return [pscustomobject][ordered]@{
            Status="RESOLVED"
            ResolutionSource="SourceRegistration"
            Name=$match.Name
            Environment=(First-Value @((Get-PropValue -Object $match.Raw -Names @("environment"))))
            SourceId=(First-Value @((Get-PropValue -Object $match.Raw -Names @("sourceId"))))
            Raw=$match.Raw
            Registration=$match.Registration
            Error=""
        }
    }

    if ($Id -notmatch '^\d+$') {
        return [pscustomobject][ordered]@{
            Status="UNRESOLVED"
            ResolutionSource=""
            Name=""
            Environment=""
            SourceId=""
            Raw=$null
            Registration=$null
            Error="Reference was not present in source registration data and is not a numeric Cohesity object/source id."
        }
    }

    $uris = if ($ReferenceType -eq "Object") {
        @("$baseUrl/v2/data-protect/objects/$Id", "$baseUrl/v2/data-protect/sources/$Id")
    }
    else {
        @("$baseUrl/v2/data-protect/sources/$Id", "$baseUrl/v2/data-protect/objects/$Id")
    }

    $attemptErrors = @()

    foreach ($uri in $uris) {
        try {
            $raw = Get-Json -Uri $uri -Headers $Headers
            if ($null -eq $raw) { continue }

            if ($raw -is [array]) {
                $candidate = @($raw | Where-Object { $_ } | Select-Object -First 1)
                if ($candidate.Count -eq 0) { continue }
                $raw = $candidate[0]
            }

            return [pscustomobject][ordered]@{
                Status="RESOLVED"
                ResolutionSource=$uri
                Name=(First-Value @((Get-PropValue -Object $raw -Names @("name","objectName","sourceName","displayName","hostName"))))
                Environment=(First-Value @((Get-PropValue -Object $raw -Names @("environment"))))
                SourceId=(First-Value @((Get-PropValue -Object $raw -Names @("sourceId"))))
                Raw=$raw
                Registration=$null
                Error=""
            }
        }
        catch {
            $attemptErrors += $_.Exception.Message
        }
    }

    return [pscustomobject][ordered]@{
        Status="UNRESOLVED"
        ResolutionSource=""
        Name=""
        Environment=""
        SourceId=""
        Raw=$null
        Registration=$null
        Error=(($attemptErrors | Select-Object -Unique) -join " | ")
    }
}

function Write-Sha256File {
    param(
        [Parameter(Mandatory=$true)][string]$Directory,
        [switch]$Recurse
    )

    $checksumPath = Join-Path $Directory "SHA256SUMS.txt"

    if ($Recurse) {
        $files = @(Get-ChildItem -Path $Directory -File -Recurse | Where-Object { $_.FullName -ne $checksumPath } | Sort-Object FullName)
    }
    else {
        $files = @(Get-ChildItem -Path $Directory -File | Where-Object { $_.FullName -ne $checksumPath } | Sort-Object Name)
    }

    $lines = @()
    foreach ($file in $files) {
        $hash = (Get-FileHash -Path $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        $relative = $file.FullName.Substring($Directory.Length).TrimStart([char]'\')
        $lines += "$hash  $relative"
    }

    Set-Content -Path $checksumPath -Value $lines -Encoding UTF8
}

# -------------------------------
# Cluster selection
# -------------------------------
$clusterJson = Get-Json -Uri "$baseUrl/v2/mcm/cluster-mgmt/info" -Headers (New-Headers)
$rawClusters = @(As-Array (Get-PropValue -Object $clusterJson -Names @("cohesityClusters")))

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

        if ([string]::IsNullOrWhiteSpace($name)) { $name = "Unknown-$id" }

        [pscustomobject]@{
            ClusterName=$name
            ClusterId=$id
        }
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
    if (-not [int]::TryParse($selection,[ref]$number)) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($number -lt 0 -or $number -gt $clusterMenu.Count) {
        Write-Host "Enter 0, 1-$($clusterMenu.Count), or X." -ForegroundColor Red
        continue
    }

    if ($number -eq 0) { $selectedClusters = @($clusterMenu) }
    else { $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $number }) }

    break
}

# -------------------------------
# Collection
# -------------------------------
foreach ($cluster in $selectedClusters) {
    $headers = New-Headers -ClusterId $cluster.ClusterId
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $clusterDir = Join-Path $OutputDirectory ("{0}_{1}" -f (Safe-Name $cluster.ClusterName),$timestamp)
    $pgRoot = Join-Path $clusterDir "PGs"

    New-Item -Path $pgRoot -ItemType Directory -Force | Out-Null

    Write-Host ""
    Write-Host "Collecting active Protection Group evidence from $($cluster.ClusterName) ..." -ForegroundColor Cyan

    $errorList = New-Object System.Collections.ArrayList
    $pgIndex = New-Object System.Collections.ArrayList
    $environmentValidation = New-Object System.Collections.ArrayList

    $policyEvidence = Get-PolicyEvidence -Headers $headers
    $storageEvidence = Get-StorageDomainEvidence -Headers $headers
    $sourceEvidence = Get-SourceRegistrationEvidence -Headers $headers

    Write-Json -Value $policyEvidence.Items -Path (Join-Path $clusterDir "Policies_All.json")
    Write-Json -Value $storageEvidence.Items -Path (Join-Path $clusterDir "StorageDomains_All.json")
    Write-Json -Value $sourceEvidence.Items -Path (Join-Path $clusterDir "SourceRegistrations_All.json")

    if ($policyEvidence.Status -ne "SUCCESS") {
        Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment "ALL" -ProtectionGroup "" -Stage "Policies" -Message $policyEvidence.Error
    }
    if ($storageEvidence.Status -ne "SUCCESS") {
        Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment "ALL" -ProtectionGroup "" -Stage "StorageDomains" -Message $storageEvidence.Error
    }
    if ($sourceEvidence.Status -ne "SUCCESS") {
        Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment "ALL" -ProtectionGroup "" -Stage "SourceRegistrations" -Message $sourceEvidence.Error
    }

    $policyMap = Build-IdMap -Items $policyEvidence.Items -IdNames @("id","policyId") -NameNames @("name","policyName","displayName")
    $storageMap = Build-IdMap -Items $storageEvidence.Items -IdNames @("id","storageDomainId") -NameNames @("name","storageDomainName","displayName")
    $sourceIndex = Build-SourceRegistrationIndex -Registrations $sourceEvidence.Items

    foreach ($environment in $EnvironmentMap) {
        $pgList = @()
        $listStatus = "SUCCESS"
        $listError = ""

        try {
            $pgList = @(Get-ActiveProtectionGroups -Environment $environment.ApiName -Headers $headers)
        }
        catch {
            $listStatus = "FAILED"
            $listError = $_.Exception.Message
            Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment $environment.DisplayName -ProtectionGroup "" -Stage "ProtectionGroupList" -Message $listError
        }

        [void]$environmentValidation.Add([pscustomobject][ordered]@{
            Environment=$environment.DisplayName
            EnvironmentApiName=$environment.ApiName
            ListStatus=$listStatus
            ProtectionGroupCount=$pgList.Count
            Error=$listError
        })

        if ($listStatus -ne "SUCCESS") {
            Write-Host "  $($environment.DisplayName): GET failed" -ForegroundColor Red
            continue
        }

        Write-Host "  $($environment.DisplayName): $($pgList.Count) active PGs" -ForegroundColor Yellow

        foreach ($pgStub in $pgList) {
            $pgId = First-Value @((Get-PropValue -Object $pgStub -Names @("id","protectionGroupId")))
            $pgName = First-Value @(
                (Get-PropValue -Object $pgStub -Names @("name","protectionGroupName")),
                $pgId,
                "UNNAMED"
            )

            $folderId = if ([string]::IsNullOrWhiteSpace($pgId)) { "NO_ID" } else { $pgId }
            $folderName = "{0}__{1}" -f (Safe-Name $pgName),(Safe-Name $folderId)
            $pgDir = Join-Path $pgRoot $folderName
            New-Item -Path $pgDir -ItemType Directory -Force | Out-Null

            Write-Json -Value $pgStub -Path (Join-Path $pgDir "01_PG_ListRecord.json")

            $issues = New-Object System.Collections.ArrayList
            $detail = $null
            $detailStatus = "NOT_ATTEMPTED"
            $detailError = ""

            if ([string]::IsNullOrWhiteSpace($pgId)) {
                $detailStatus = "FAILED"
                $detailError = "Protection Group list record did not contain an id."
                [void]$issues.Add($detailError)
                Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment $environment.DisplayName -ProtectionGroup $pgName -Stage "ProtectionGroupId" -Message $detailError
            }
            else {
                try {
                    $detail = Get-ProtectionGroupDetail -ProtectionGroupId $pgId -Headers $headers
                    if ($null -eq $detail) {
                        $detailStatus = "FAILED"
                        $detailError = "Detailed Protection Group GET returned no content."
                    }
                    else {
                        $detailStatus = "SUCCESS"
                    }
                }
                catch {
                    $detailStatus = "FAILED"
                    $detailError = $_.Exception.Message
                }

                if ($detailStatus -ne "SUCCESS") {
                    [void]$issues.Add("Detailed Protection Group GET failed.")
                    Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment $environment.DisplayName -ProtectionGroup $pgName -Stage "ProtectionGroupDetail" -Message $detailError
                }
            }

            Write-Json -Value $detail -Path (Join-Path $pgDir "02_PG_Detail.json")

            if ($detailStatus -eq "SUCCESS") {
                $detailActive = Get-PropValue -Object $detail -Names @("isActive") -Default $true
                $detailDeleted = Get-PropValue -Object $detail -Names @("isDeleted") -Default $false
                $detailEnvironment = First-Value @((Get-PropValue -Object $detail -Names @("environment")))

                if ($detailActive -eq $false -or $detailDeleted -eq $true) {
                    [void]$issues.Add("Detailed Protection Group state did not match the active/non-deleted collection scope.")
                }
                if ($detailEnvironment -and $detailEnvironment -ne $environment.ApiName) {
                    [void]$issues.Add("Detailed Protection Group environment did not match the environment being collected.")
                }
            }

            # Detail is authoritative. If unavailable, list data may be used only as supplemental evidence.
            $supplementalRecord = if ($detailStatus -eq "SUCCESS") { $detail } else { $pgStub }

            $environmentBlock = Get-EnvironmentBlock -ProtectionGroup $supplementalRecord -Names $environment.ParamNames
            $parameterBlockName = if ($environmentBlock) { $environmentBlock.Name } else { "NOT_FOUND" }
            $parameterBlockValue = if ($environmentBlock) { $environmentBlock.Value } else { $null }
            $parameterSource = if ($detailStatus -eq "SUCCESS") { "DetailedGET" } else { "ListRecordSupplemental" }

            if ($parameterBlockName -eq "NOT_FOUND") {
                [void]$issues.Add("Environment parameter block was not returned.")
                Add-ErrorRecord -ErrorList $errorList -Cluster $cluster.ClusterName -Environment $environment.DisplayName -ProtectionGroup $pgName -Stage "EnvironmentParameters" -Message "No matching environment parameter block was returned."
            }

            Write-Json -Value $parameterBlockValue -Path (Join-Path $pgDir "03_EnvironmentParams.json")

            $policyId = First-Value @(
                (Get-PropValue -Object $supplementalRecord -Names @("policyId")),
                (Get-NestedValue -Object $supplementalRecord -Path "policyInfo.id"),
                (Get-NestedValue -Object $supplementalRecord -Path "policy.id")
            )

            $policyRaw = $null
            $policyResolutionStatus = "NOT_REFERENCED"
            if ($policyId) {
                if ($policyEvidence.Status -ne "SUCCESS") {
                    $policyResolutionStatus = "COLLECTION_FAILED"
                    [void]$issues.Add("Policy collection failed; referenced policy could not be verified.")
                }
                elseif ($policyMap.ContainsKey($policyId)) {
                    $policyRaw = $policyMap[$policyId].Raw
                    $policyResolutionStatus = "RESOLVED"
                }
                else {
                    $policyResolutionStatus = "UNRESOLVED"
                    [void]$issues.Add("Referenced policy id was not found in collected policy data.")
                }
            }

            Write-Json -Value $policyRaw -Path (Join-Path $pgDir "04_Policy.json")

            $storageDomainId = First-Value @(
                (Get-PropValue -Object $supplementalRecord -Names @("storageDomainId")),
                (Get-NestedValue -Object $supplementalRecord -Path "storageDomain.id")
            )

            $storageDomainRaw = $null
            $storageResolutionStatus = "NOT_REFERENCED"
            if ($storageDomainId) {
                if ($storageEvidence.Status -ne "SUCCESS") {
                    $storageResolutionStatus = "COLLECTION_FAILED"
                    [void]$issues.Add("Storage-domain collection failed; referenced storage domain could not be verified.")
                }
                elseif ($storageMap.ContainsKey($storageDomainId)) {
                    $storageDomainRaw = $storageMap[$storageDomainId].Raw
                    $storageResolutionStatus = "RESOLVED"
                }
                else {
                    $storageResolutionStatus = "UNRESOLVED"
                    [void]$issues.Add("Referenced storage-domain id was not found in collected storage-domain data.")
                }
            }

            Write-Json -Value $storageDomainRaw -Path (Join-Path $pgDir "05_StorageDomain.json")

            $references = @(Get-DependencyReferences -EnvironmentParams $parameterBlockValue)
            Write-Json -Value $references -Path (Join-Path $pgDir "06_DependencyReferences.json")

            $resolvedRows = @()
            $relevantRegistrationMap = @{}
            $unresolvedReferenceCount = 0

            foreach ($reference in $references) {
                $resolved = Resolve-DependencyReference `
                    -ReferenceType $reference.ReferenceType `
                    -Id ([string]$reference.OriginalId) `
                    -Headers $headers `
                    -SourceIndex $sourceIndex

                if ($resolved.Status -ne "RESOLVED") {
                    $unresolvedReferenceCount++
                }

                if ($resolved.Registration) {
                    $registrationKey = First-Value @(
                        (Get-PropValue -Object $resolved.Registration -Names @("id","sourceId")),
                        (Get-NestedValue -Object $resolved.Registration -Path "sourceInfo.id"),
                        $reference.OriginalId
                    )
                    if ($registrationKey) {
                        $relevantRegistrationMap[[string]$registrationKey] = $resolved.Registration
                    }
                }

                $resolvedRows += [pscustomobject][ordered]@{
                    ReferenceType=$reference.ReferenceType
                    FieldPath=$reference.FieldPath
                    OriginalId=$reference.OriginalId
                    Status=$resolved.Status
                    ResolutionSource=$resolved.ResolutionSource
                    Name=$resolved.Name
                    Environment=$resolved.Environment
                    SourceId=$resolved.SourceId
                    Error=$resolved.Error
                    Raw=$resolved.Raw
                }
            }

            if ($policyEvidence.Status -ne "SUCCESS") {
                [void]$issues.Add("Cluster policy evidence collection was not successful.")
            }
            if ($storageEvidence.Status -ne "SUCCESS") {
                [void]$issues.Add("Cluster storage-domain evidence collection was not successful.")
            }
            if ($sourceEvidence.Status -ne "SUCCESS") {
                [void]$issues.Add("Cluster source-registration evidence collection was not successful.")
            }
            if ($unresolvedReferenceCount -gt 0) {
                [void]$issues.Add("$unresolvedReferenceCount source/object reference(s) remain unresolved.")
            }

            Write-Json -Value $resolvedRows -Path (Join-Path $pgDir "07_ResolvedReferences.json")
            Write-Json -Value @($relevantRegistrationMap.Values) -Path (Join-Path $pgDir "08_SourceRegistrations.json")

            $status = if ([string]::IsNullOrWhiteSpace($pgId)) {
                "FAILED"
            }
            elseif ($issues.Count -gt 0) {
                "PARTIAL"
            }
            else {
                "COMPLETE"
            }

            $validation = [ordered]@{
                Status=$status
                DetailGetStatus=$detailStatus
                DetailGetError=$detailError
                DetailIsAuthoritative=($detailStatus -eq "SUCCESS")
                ParameterBlock=$parameterBlockName
                ParameterSource=$parameterSource
                PolicyCollectionStatus=$policyEvidence.Status
                PolicyResolutionStatus=$policyResolutionStatus
                StorageDomainCollectionStatus=$storageEvidence.Status
                StorageDomainResolutionStatus=$storageResolutionStatus
                SourceRegistrationCollectionStatus=$sourceEvidence.Status
                DependencyReferenceCount=$references.Count
                UnresolvedReferenceCount=$unresolvedReferenceCount
                IssueCount=$issues.Count
                Issues=@($issues)
            }

            Write-Json -Value $validation -Path (Join-Path $pgDir "09_Validation.json")

            $manifest = [ordered]@{
                ExportedAt=(Get-Date).ToString("o")
                ReadOnly=$true
                RequestMethod="GET"
                Cluster=$cluster.ClusterName
                ClusterId=$cluster.ClusterId
                Environment=$environment.DisplayName
                EnvironmentApiName=$environment.ApiName
                ProtectionGroup=$pgName
                ProtectionGroupId=$pgId
                Status=$status
                ActiveOnly=$true
                DetailIsAuthoritative=($detailStatus -eq "SUCCESS")
                Files=@(
                    "01_PG_ListRecord.json",
                    "02_PG_Detail.json",
                    "03_EnvironmentParams.json",
                    "04_Policy.json",
                    "05_StorageDomain.json",
                    "06_DependencyReferences.json",
                    "07_ResolvedReferences.json",
                    "08_SourceRegistrations.json",
                    "09_Validation.json",
                    "10_Manifest.json",
                    "SHA256SUMS.txt"
                )
            }

            Write-Json -Value $manifest -Path (Join-Path $pgDir "10_Manifest.json")
            Write-Sha256File -Directory $pgDir

            [void]$pgIndex.Add([pscustomobject][ordered]@{
                Environment=$environment.DisplayName
                EnvironmentApiName=$environment.ApiName
                ProtectionGroup=$pgName
                ProtectionGroupId=$pgId
                Status=$status
                DetailGetStatus=$detailStatus
                ParameterBlock=$parameterBlockName
                ParameterSource=$parameterSource
                PolicyResolutionStatus=$policyResolutionStatus
                StorageDomainResolutionStatus=$storageResolutionStatus
                DependencyReferenceCount=$references.Count
                UnresolvedReferenceCount=$unresolvedReferenceCount
                OutputFolder=$folderName
            })
        }
    }

    $completeCount = @($pgIndex | Where-Object { $_.Status -eq "COMPLETE" }).Count
    $partialCount = @($pgIndex | Where-Object { $_.Status -eq "PARTIAL" }).Count
    $failedCount = @($pgIndex | Where-Object { $_.Status -eq "FAILED" }).Count
    $failedEnvironmentCount = @($environmentValidation | Where-Object { $_.ListStatus -eq "FAILED" }).Count

    $overallStatus = if ($failedEnvironmentCount -gt 0 -or $failedCount -gt 0) {
        "FAILED"
    }
    elseif ($partialCount -gt 0 -or $policyEvidence.Status -ne "SUCCESS" -or $storageEvidence.Status -ne "SUCCESS" -or $sourceEvidence.Status -ne "SUCCESS") {
        "PARTIAL"
    }
    else {
        "COMPLETE"
    }

    Write-Json -Value @($pgIndex) -Path (Join-Path $clusterDir "PG_Index.json")
    Write-Json -Value @($errorList) -Path (Join-Path $clusterDir "Collection_Errors.json")

    Write-Json -Value ([ordered]@{
        OverallStatus=$overallStatus
        PolicyCollection=[ordered]@{
            Status=$policyEvidence.Status
            Uri=$policyEvidence.Uri
            ItemCount=$policyEvidence.Items.Count
            Error=$policyEvidence.Error
            Attempts=$policyEvidence.Attempts
        }
        StorageDomainCollection=[ordered]@{
            Status=$storageEvidence.Status
            Uri=$storageEvidence.Uri
            ItemCount=$storageEvidence.Items.Count
            Error=$storageEvidence.Error
            Attempts=$storageEvidence.Attempts
        }
        SourceRegistrationCollection=[ordered]@{
            Status=$sourceEvidence.Status
            Uri=$sourceEvidence.Uri
            ItemCount=$sourceEvidence.Items.Count
            Error=$sourceEvidence.Error
        }
        Environments=@($environmentValidation)
        ProtectionGroups=[ordered]@{
            Total=$pgIndex.Count
            Complete=$completeCount
            Partial=$partialCount
            Failed=$failedCount
        }
        ErrorCount=$errorList.Count
    }) -Path (Join-Path $clusterDir "Collection_Validation.json")

    Write-Json -Value ([ordered]@{
        ExportedAt=(Get-Date).ToString("o")
        Script="Get-CohesityDRReadyPGConfig.ps1"
        ReadOnly=$true
        RequestMethod="GET"
        HeliosBaseUrl=$baseUrl
        Cluster=$cluster.ClusterName
        ClusterId=$cluster.ClusterId
        ActiveOnly=$true
        DeletedIncluded=$false
        Environments=@($EnvironmentMap | ForEach-Object { $_.DisplayName })
        OverallStatus=$overallStatus
        ProtectionGroupCount=$pgIndex.Count
        CollectionErrorCount=$errorList.Count
        Safety="GET-only. No Cohesity write operations are implemented."
    }) -Path (Join-Path $clusterDir "Run_Metadata.json")

    Write-Sha256File -Directory $clusterDir -Recurse

    Write-Host ""
    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Evidence status: $overallStatus" -ForegroundColor $(if ($overallStatus -eq "COMPLETE") { "Green" } elseif ($overallStatus -eq "PARTIAL") { "Yellow" } else { "Red" })
    Write-Host "PGs: $($pgIndex.Count)  Complete: $completeCount  Partial: $partialCount  Failed: $failedCount" -ForegroundColor Cyan
    Write-Host "Output: $clusterDir" -ForegroundColor Green
}
