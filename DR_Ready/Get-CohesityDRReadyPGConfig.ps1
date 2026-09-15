# Cohesity Helios - Active Protection Group Configuration Export
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Purpose:
#   Export active Protection Groups and their configured parameters for:
#   NAS, SQL, Hyper-V, Nutanix AHV, Oracle, and Physical.
#
# Safety:
#   GET-only. No write operations are performed.

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
    param([AllowNull()]$Value,[string]$Path)
    $Value | ConvertTo-Json -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Expand-LeafValue {
    param($Value,[string]$Path="")

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
        $keys = @($Value.Keys)
        if ($keys.Count -eq 0) {
            [pscustomobject]@{ Field=$Path; Value="{}" }
            return
        }

        foreach ($key in $keys) {
            $childPath = if ($Path) { "$Path.$key" } else { [string]$key }
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

    foreach ($property in @($Value.PSObject.Properties)) {
        $childPath = if ($Path) { "$Path.$($property.Name)" } else { $property.Name }
        Expand-LeafValue -Value $property.Value -Path $childPath
    }
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
        $uri = "$baseUrl/v2/data-protect/protection-groups?environments=$Environment&isDeleted=false&isActive=true&includeLastRunInfo=true&maxResultCount=1000"
        if (-not [string]::IsNullOrWhiteSpace($cookie)) {
            $uri += "&paginationCookie=$([uri]::EscapeDataString($cookie))"
        }

        $json = Get-Json -Uri $uri -Headers $Headers
        $groups = Get-PropValue -Object $json -Names @("protectionGroups") -Default @()

        if ($groups) {
            $all += @(As-Array $groups | Where-Object { $_ })
        }

        $cookie = First-Value @((Get-PropValue -Object $json -Names @("paginationCookie") -Default ""))
        $truncated = Get-PropValue -Object $json -Names @("isResponseTruncated") -Default $false

        if ($truncated -ne $true -and [string]::IsNullOrWhiteSpace($cookie)) { break }
    }
    while (-not [string]::IsNullOrWhiteSpace($cookie))

    return @($all)
}

function Get-PolicyData {
    param([hashtable]$Headers)

    $nameMap = @{}
    $rawMap = @{}

    foreach ($uri in @(
        "$baseUrl/v2/data-protect/policies?maxResultCount=1000",
        "$baseUrl/v2/data-protect/policies"
    )) {
        try {
            $json = Get-Json -Uri $uri -Headers $Headers
            $items = Get-PropValue -Object $json -Names @("policies","policyList","items") -Default $null
            if ($null -eq $items) { $items = $json }

            foreach ($policy in @(As-Array $items | Where-Object { $_ -and $_ -isnot [string] })) {
                $id = First-Value @((Get-PropValue -Object $policy -Names @("id","policyId")))
                $name = First-Value @((Get-PropValue -Object $policy -Names @("name","policyName","displayName")))
                if ($id) {
                    $nameMap[$id] = $name
                    $rawMap[$id] = $policy
                }
            }

            if ($nameMap.Count -gt 0) { break }
        }
        catch {
            continue
        }
    }

    return [pscustomobject]@{ NameMap=$nameMap; RawMap=$rawMap }
}

function Get-PolicyId {
    param($ProtectionGroup)

    return First-Value @(
        (Get-PropValue -Object $ProtectionGroup -Names @("policyId")),
        (Get-NestedValue -Object $ProtectionGroup -Path "policyInfo.id"),
        (Get-NestedValue -Object $ProtectionGroup -Path "policy.id")
    )
}

function Get-SourceObjectRows {
    param(
        $EnvironmentParams,
        [string]$Cluster,
        [string]$Environment,
        [string]$ProtectionGroup,
        [string]$ProtectionGroupId
    )

    $rows = @()
    if ($null -eq $EnvironmentParams) { return @() }

    foreach ($leaf in @(Expand-LeafValue -Value $EnvironmentParams -Path "")) {
        $field = [string]$leaf.Field
        if ($field -match '(?i)(source|object|host|server|instance|database|vm|share|path|volume|file)') {
            $rows += [pscustomobject]@{
                Cluster=$Cluster
                Environment=$Environment
                ProtectionGroup=$ProtectionGroup
                ProtectionGroupId=$ProtectionGroupId
                Field=$field
                Value=$leaf.Value
            }
        }
    }

    return @($rows)
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

        [pscustomobject]@{
            ClusterName = if ($name) { $name } else { "Unknown-$id" }
            ClusterId = $id
        }
    } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_.ClusterId) } |
    Sort-Object ClusterName
)

$clusterMenu = for ($i=0; $i -lt $clusters.Count; $i++) {
    [pscustomobject]@{
        Index=$i+1
        ClusterName=$clusters[$i].ClusterName
        ClusterId=$clusters[$i].ClusterId
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

    if ($number -eq 0) {
        $selectedClusters = @($clusterMenu)
    }
    else {
        $selectedClusters = @($clusterMenu | Where-Object { $_.Index -eq $number })
    }

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
    Write-Host "Collecting active Protection Groups from $($cluster.ClusterName) ..." -ForegroundColor Cyan

    $summaryRows = @()
    $allParameterRows = @()
    $allSourceObjectRows = @()
    $errors = @()

    $policyData = Get-PolicyData -Headers $headers

    foreach ($environment in $EnvironmentMap) {
        try {
            $protectionGroups = @(Get-ActiveProtectionGroups -Environment $environment.ApiName -Headers $headers)
        }
        catch {
            $errors += [pscustomobject]@{
                Cluster=$cluster.ClusterName
                Environment=$environment.DisplayName
                Stage="ProtectionGroupGET"
                Error=$_.Exception.Message
            }
            Write-Host "  $($environment.DisplayName): GET failed" -ForegroundColor Red
            continue
        }

        Write-Host "  $($environment.DisplayName): $($protectionGroups.Count) active PGs" -ForegroundColor Yellow

        foreach ($pg in $protectionGroups) {
            $pgName = First-Value @(
                (Get-PropValue -Object $pg -Names @("name","protectionGroupName")),
                "UNNAMED"
            )
            $pgId = First-Value @(
                (Get-PropValue -Object $pg -Names @("id","protectionGroupId"))
            )

            $environmentBlock = Get-EnvironmentBlock -ProtectionGroup $pg -Names $environment.ParamNames
            $parameterBlockName = if ($environmentBlock) { $environmentBlock.Name } else { "NOT_FOUND" }
            $parameterBlockValue = if ($environmentBlock) { $environmentBlock.Value } else { $null }

            $policyId = Get-PolicyId -ProtectionGroup $pg
            $policyName = ""
            $policyRaw = $null
            if ($policyId -and $policyData.NameMap.ContainsKey($policyId)) {
                $policyName = $policyData.NameMap[$policyId]
                $policyRaw = $policyData.RawMap[$policyId]
            }

            $pgDir = Join-Path $pgRoot ("{0}__{1}" -f (Safe-Name $pgName),(Safe-Name $pgId))
            New-Item -Path $pgDir -ItemType Directory -Force | Out-Null

            Write-Json -Value $pg -Path (Join-Path $pgDir "ProtectionGroup.json")
            Write-Json -Value $parameterBlockValue -Path (Join-Path $pgDir "EnvironmentParams.json")
            Write-Json -Value $policyRaw -Path (Join-Path $pgDir "Policy.json")

            $pgParameterRows = @()
            foreach ($leaf in @(Expand-LeafValue -Value $pg -Path "")) {
                $row = [pscustomobject]@{
                    Cluster=$cluster.ClusterName
                    Environment=$environment.DisplayName
                    ProtectionGroup=$pgName
                    ProtectionGroupId=$pgId
                    Field=$leaf.Field
                    Value=$leaf.Value
                }
                $pgParameterRows += $row
                $allParameterRows += $row
            }

            $pgSourceObjectRows = @(Get-SourceObjectRows `
                -EnvironmentParams $parameterBlockValue `
                -Cluster $cluster.ClusterName `
                -Environment $environment.DisplayName `
                -ProtectionGroup $pgName `
                -ProtectionGroupId $pgId)

            $allSourceObjectRows += $pgSourceObjectRows

            $pgParameterRows | Export-Csv (Join-Path $pgDir "ConfiguredParameters.csv") -NoTypeInformation -Encoding UTF8
            $pgSourceObjectRows | Export-Csv (Join-Path $pgDir "SourceObjectConfiguration.csv") -NoTypeInformation -Encoding UTF8

            Write-Json -Value ([ordered]@{
                ExportedAt=(Get-Date).ToString("o")
                ReadOnly=$true
                RequestMethod="GET"
                Cluster=$cluster.ClusterName
                ClusterId=$cluster.ClusterId
                Environment=$environment.DisplayName
                EnvironmentApiName=$environment.ApiName
                ProtectionGroup=$pgName
                ProtectionGroupId=$pgId
                ParameterBlock=$parameterBlockName
                ActiveOnly=$true
                Files=@(
                    "ProtectionGroup.json",
                    "EnvironmentParams.json",
                    "Policy.json",
                    "ConfiguredParameters.csv",
                    "SourceObjectConfiguration.csv"
                )
            }) -Path (Join-Path $pgDir "Manifest.json")

            $summaryRows += [pscustomobject]@{
                Cluster=$cluster.ClusterName
                ClusterId=$cluster.ClusterId
                Environment=$environment.DisplayName
                EnvironmentApiName=$environment.ApiName
                ProtectionGroup=$pgName
                ProtectionGroupId=$pgId
                PolicyId=$policyId
                PolicyName=$policyName
                StorageDomainId=First-Value @((Get-PropValue -Object $pg -Names @("storageDomainId")))
                StorageDomainName=First-Value @(
                    (Get-PropValue -Object $pg -Names @("storageDomainName")),
                    (Get-NestedValue -Object $pg -Path "storageDomain.name")
                )
                IsActive=Get-PropValue -Object $pg -Names @("isActive")
                IsDeleted=Get-PropValue -Object $pg -Names @("isDeleted")
                IsPaused=Get-PropValue -Object $pg -Names @("isPaused")
                ParameterBlock=$parameterBlockName
                ParameterFieldCount=$pgParameterRows.Count
                SourceObjectFieldCount=$pgSourceObjectRows.Count
                OutputFolder=(Split-Path $pgDir -Leaf)
            }
        }
    }

    $summaryRows |
        Sort-Object Environment,ProtectionGroup |
        Export-Csv (Join-Path $clusterDir "Active_ProtectionGroups.csv") -NoTypeInformation -Encoding UTF8

    $allParameterRows |
        Sort-Object Environment,ProtectionGroup,Field |
        Export-Csv (Join-Path $clusterDir "All_Configured_Parameters.csv") -NoTypeInformation -Encoding UTF8

    $allSourceObjectRows |
        Sort-Object Environment,ProtectionGroup,Field |
        Export-Csv (Join-Path $clusterDir "All_Source_Object_Configuration.csv") -NoTypeInformation -Encoding UTF8

    $errors |
        Export-Csv (Join-Path $clusterDir "Collection_Errors.csv") -NoTypeInformation -Encoding UTF8

    Write-Json -Value ([ordered]@{
        ExportedAt=(Get-Date).ToString("o")
        Script="Get-CohesityDRReadyPGConfig.ps1"
        ReadOnly=$true
        RequestMethod="GET"
        HeliosBaseUrl=$baseUrl
        Cluster=$cluster.ClusterName
        ClusterId=$cluster.ClusterId
        ActiveOnly=$true
        Environments=@($EnvironmentMap | ForEach-Object { $_.DisplayName })
        ProtectionGroupCount=$summaryRows.Count
        CollectionErrorCount=$errors.Count
        Safety="GET-only. No write operations are performed."
    }) -Path (Join-Path $clusterDir "Run_Metadata.json")

    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Active PGs exported: $($summaryRows.Count)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green

    if ($errors.Count -gt 0) {
        Write-Host "Warnings/errors: $($errors.Count) - see Collection_Errors.csv" -ForegroundColor Yellow
    }
}
