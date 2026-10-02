# Cohesity Helios - Active Protection Group Configuration Export
# STRICTLY READ-ONLY / GET-only
# PowerShell 5.1 compatible
#
# Purpose:
#   Collect active Protection Group configuration as raw JSON and generate
#   a keys/types-only structure view. No Cohesity write operation is implemented.

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
    [pscustomobject]@{ ApiName="kGenericNas"; DisplayName="NAS";         FileName="NAS.json" },
    [pscustomobject]@{ ApiName="kSQL";        DisplayName="SQL";         FileName="SQL.json" },
    [pscustomobject]@{ ApiName="kHyperV";     DisplayName="Hyper-V";     FileName="HyperV.json" },
    [pscustomobject]@{ ApiName="kAcropolis";  DisplayName="Nutanix AHV"; FileName="AHV.json" },
    [pscustomobject]@{ ApiName="kOracle";     DisplayName="Oracle";      FileName="Oracle.json" },
    [pscustomobject]@{ ApiName="kPhysical";   DisplayName="Physical";    FileName="Physical.json" }
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

function Write-Json {
    param([AllowNull()]$Value,[string]$Path)
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
        if ($null -eq $json) { throw "Protection Group list GET returned no JSON content." }

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
    $uri = "$baseUrl/v2/data-protect/protection-groups/${encodedId}?includeLastRunInfo=false&pruneSourceIds=false"
    return Get-Json -Uri $uri -Headers $Headers
}

function Get-SafeFieldName {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) { return "{empty-key}" }
    if ($Name -match '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') { return "{dynamic-key}" }
    if ($Name -match '^\d{6,}$') { return "{dynamic-key}" }
    if ($Name -match '^\d{1,3}(\.\d{1,3}){3}$') { return "{dynamic-key}" }
    if ($Name -match '[\\/@]') { return "{dynamic-key}" }
    return $Name
}

function Get-ValueType {
    param($Value)

    if ($null -eq $Value) { return "Null" }
    if ($Value -is [string] -or $Value -is [char]) { return "String" }
    if ($Value -is [bool]) { return "Boolean" }
    if ($Value -is [datetime]) { return "DateTime" }
    if ($Value -is [guid]) { return "Guid" }
    if ($Value -is [byte] -or $Value -is [sbyte] -or $Value -is [int16] -or $Value -is [uint16] -or $Value -is [int32] -or $Value -is [uint32] -or $Value -is [int64] -or $Value -is [uint64]) { return "Integer" }
    if ($Value -is [single] -or $Value -is [double] -or $Value -is [decimal]) { return "Number" }
    if ($Value -is [System.Collections.IDictionary]) { return "Object" }
    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) { return "Array" }
    if (@($Value.PSObject.Properties).Count -gt 0) { return "Object" }
    return $Value.GetType().Name
}

function Add-StructureRows {
    param(
        $Value,
        [string]$Path,
        [string]$Environment,
        [System.Collections.ArrayList]$Rows
    )

    $type = Get-ValueType -Value $Value

    if (-not [string]::IsNullOrWhiteSpace($Path)) {
        [void]$Rows.Add([pscustomobject][ordered]@{
            Environment=$Environment
            FieldPath=$Path
            Type=$type
        })
    }

    if ($null -eq $Value) { return }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $safeKey = Get-SafeFieldName -Name ([string]$key)
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { $safeKey } else { "$Path.$safeKey" }
            Add-StructureRows -Value $Value[$key] -Path $childPath -Environment $Environment -Rows $Rows
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        foreach ($item in @($Value)) {
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { "[]" } else { "$Path[]" }
            Add-StructureRows -Value $item -Path $childPath -Environment $Environment -Rows $Rows
        }
        return
    }

    if ($type -eq "Object") {
        foreach ($property in @($Value.PSObject.Properties)) {
            $safeName = Get-SafeFieldName -Name $property.Name
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { $safeName } else { "$Path.$safeName" }
            Add-StructureRows -Value $property.Value -Path $childPath -Environment $Environment -Rows $Rows
        }
    }
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
    New-Item -Path $rawDir -ItemType Directory -Force | Out-Null

    $errors = New-Object System.Collections.ArrayList
    $structureRows = New-Object System.Collections.ArrayList

    Write-Host ""
    Write-Host "Collecting active Protection Group configuration from $($cluster.ClusterName) ..." -ForegroundColor Cyan

    foreach ($environment in $EnvironmentMap) {
        $collected = @()
        $pgList = @()

        try {
            $pgList = @(Get-ActiveProtectionGroups -Environment $environment.ApiName -Headers $headers)
        }
        catch {
            [void]$errors.Add([pscustomobject][ordered]@{
                Environment=$environment.DisplayName
                ProtectionGroup=""
                Stage="ProtectionGroupList"
                Error=$_.Exception.Message
            })
            Write-Host "  $($environment.DisplayName): list GET failed" -ForegroundColor Red
            Write-Json -Value @() -Path (Join-Path $rawDir $environment.FileName)
            continue
        }

        foreach ($pgStub in $pgList) {
            $pgId = First-Value @((Get-PropValue -Object $pgStub -Names @("id","protectionGroupId")))
            $pgName = First-Value @((Get-PropValue -Object $pgStub -Names @("name","protectionGroupName")),$pgId,"UNNAMED")
            $pgData = $pgStub

            if ([string]::IsNullOrWhiteSpace($pgId)) {
                [void]$errors.Add([pscustomobject][ordered]@{
                    Environment=$environment.DisplayName
                    ProtectionGroup=$pgName
                    Stage="ProtectionGroupDetail"
                    Error="Protection Group list record did not contain an id; list record was retained."
                })
            }
            else {
                try {
                    $detail = Get-ProtectionGroupDetail -ProtectionGroupId $pgId -Headers $headers
                    if ($null -ne $detail) {
                        $pgData = $detail
                    }
                    else {
                        [void]$errors.Add([pscustomobject][ordered]@{
                            Environment=$environment.DisplayName
                            ProtectionGroup=$pgName
                            Stage="ProtectionGroupDetail"
                            Error="Detailed GET returned no content; list record was retained."
                        })
                    }
                }
                catch {
                    [void]$errors.Add([pscustomobject][ordered]@{
                        Environment=$environment.DisplayName
                        ProtectionGroup=$pgName
                        Stage="ProtectionGroupDetail"
                        Error=$_.Exception.Message
                    })
                }
            }

            $collected += $pgData
            Add-StructureRows -Value $pgData -Path "" -Environment $environment.DisplayName -Rows $structureRows
        }

        Write-Json -Value @($collected) -Path (Join-Path $rawDir $environment.FileName)
        Write-Host "  $($environment.DisplayName): $($pgList.Count) active PGs" -ForegroundColor Yellow
    }

    $distinctStructure = @($structureRows | Sort-Object Environment,FieldPath,Type -Unique)
    Write-Json -Value $distinctStructure -Path (Join-Path $clusterDir "FieldStructure.json")

    $txt = New-Object System.Collections.ArrayList
    foreach ($environment in $EnvironmentMap) {
        $rows = @($distinctStructure | Where-Object { $_.Environment -eq $environment.DisplayName })
        if ($rows.Count -eq 0) { continue }

        [void]$txt.Add("[$($environment.DisplayName)]")
        foreach ($row in $rows) {
            [void]$txt.Add(("{0} | {1}" -f $row.FieldPath,$row.Type))
        }
        [void]$txt.Add("")
    }
    Set-Content -Path (Join-Path $clusterDir "FieldStructure.txt") -Value $txt -Encoding UTF8

    Write-Json -Value @($errors) -Path (Join-Path $clusterDir "Errors.json")

    Write-Host ""
    Write-Host "Completed: $($cluster.ClusterName)" -ForegroundColor Green
    Write-Host "Output: $clusterDir" -ForegroundColor Green
    Write-Host "Errors recorded: $($errors.Count)" -ForegroundColor $(if ($errors.Count -eq 0) { "Green" } else { "Yellow" })
}
