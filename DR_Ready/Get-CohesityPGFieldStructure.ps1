# Cohesity Protection Group Field Structure Extractor
# OFFLINE ONLY - NO API CALLS
# PowerShell 5.1 compatible
#
# Purpose:
#   Read a previously collected PG evidence directory and produce shareable
#   field-path/type structure without exporting production values.
#
# Safety:
#   - No network calls.
#   - No Cohesity authentication.
#   - No source values are intentionally written to shareable output.
#   - Array indexes are normalized to [].
#   - Obvious value-shaped/dynamic property keys are replaced with {dynamic-key}.
#   - Output is blocked if privacy validation detects a source scalar in output.

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$EvidenceDirectory,
    [string]$OutputDirectory
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1

if (-not (Test-Path $EvidenceDirectory -PathType Container)) {
    throw "Evidence directory not found: $EvidenceDirectory"
}

$EvidenceDirectory = (Resolve-Path $EvidenceDirectory).Path
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path $EvidenceDirectory "Shareable_Structure"
}

$EnvironmentFileNames = [ordered]@{
    "kGenericNas" = "NAS"
    "kSQL"        = "SQL"
    "kHyperV"     = "HyperV"
    "kAcropolis"  = "AHV"
    "kOracle"     = "Oracle"
    "kPhysical"   = "Physical"
}

$EvidenceFiles = @(
    "01_PG_ListRecord.json",
    "02_PG_Detail.json",
    "03_EnvironmentParams.json",
    "04_Policy.json",
    "05_StorageDomain.json",
    "06_DependencyReferences.json",
    "07_ResolvedReferences.json",
    "08_SourceRegistrations.json"
)

function Read-JsonFile {
    param([Parameter(Mandatory=$true)][string]$Path)
    if (-not (Test-Path $Path -PathType Leaf)) { return $null }
    $text = Get-Content -Path $Path -Raw
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    return ($text | ConvertFrom-Json)
}

function Get-ValueTypeName {
    param($Value)

    if ($null -eq $Value) { return "Null" }
    if ($Value -is [bool]) { return "Boolean" }
    if ($Value -is [string] -or $Value -is [char] -or $Value -is [guid] -or $Value -is [datetime]) { return "String" }
    if ($Value -is [byte] -or $Value -is [sbyte] -or $Value -is [int16] -or $Value -is [uint16] -or
        $Value -is [int32] -or $Value -is [uint32] -or $Value -is [int64] -or $Value -is [uint64]) { return "Integer" }
    if ($Value -is [single] -or $Value -is [double] -or $Value -is [decimal]) { return "Number" }
    if ($Value -is [System.Collections.IDictionary]) { return "Object" }
    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) { return "Array" }
    if (@($Value.PSObject.Properties).Count -gt 0) { return "Object" }
    return "Scalar"
}

function Add-ScalarValues {
    param(
        $Value,
        [System.Collections.Generic.HashSet[string]]$Set
    )

    if ($null -eq $Value) { return }

    $type = Get-ValueTypeName -Value $Value
    if ($type -eq "String" -or $type -eq "Integer" -or $type -eq "Number") {
        $text = [string]$Value
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            [void]$Set.Add($text)
        }
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            Add-ScalarValues -Value $Value[$key] -Set $Set
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        foreach ($item in @($Value)) {
            Add-ScalarValues -Value $item -Set $Set
        }
        return
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        Add-ScalarValues -Value $property.Value -Set $Set
    }
}

function Get-SafePropertyName {
    param(
        [string]$Name,
        [System.Collections.Generic.HashSet[string]]$ScalarValues
    )

    if ([string]::IsNullOrWhiteSpace($Name)) { return "{dynamic-key}" }

    $n = $Name.Trim()

    # Property names that visibly look like production identifiers are not emitted.
    if ($n -match '^\d+$') { return "{dynamic-key}" }
    if ($n -match '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') { return "{dynamic-key}" }
    if ($n -match '^[0-9a-fA-F]{20,}$') { return "{dynamic-key}" }
    if ($n -match '^\d{1,3}(\.\d{1,3}){3}$') { return "{dynamic-key}" }
    if ($n -match '[\\/@:]') { return "{dynamic-key}" }
    if ($n.Length -gt 100) { return "{dynamic-key}" }

    # If a property name is also present as a source scalar value, treat it as dynamic.
    if ($ScalarValues.Contains($n)) { return "{dynamic-key}" }

    return $n
}

function Add-StructureRows {
    param(
        $Value,
        [string]$Path,
        [string]$Source,
        [System.Collections.Generic.HashSet[string]]$ScalarValues,
        [System.Collections.ArrayList]$Rows
    )

    $type = Get-ValueTypeName -Value $Value

    if (-not [string]::IsNullOrWhiteSpace($Path)) {
        [void]$Rows.Add([pscustomobject][ordered]@{
            Source=$Source
            FieldPath=$Path
            Type=$type
        })
    }

    if ($null -eq $Value) { return }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $safeKey = Get-SafePropertyName -Name ([string]$key) -ScalarValues $ScalarValues
            $childPath = if ($Path) { "$Path.$safeKey" } else { $safeKey }
            Add-StructureRows -Value $Value[$key] -Path $childPath -Source $Source -ScalarValues $ScalarValues -Rows $Rows
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @($Value)
        if ($items.Count -eq 0) {
            $arrayPath = if ($Path -match '\[\]$') { $Path } else { "$Path[]" }
            [void]$Rows.Add([pscustomobject][ordered]@{ Source=$Source; FieldPath=$arrayPath; Type="Unknown" })
            return
        }

        $arrayPath = if ($Path -match '\[\]$') { $Path } else { "$Path[]" }
        foreach ($item in $items) {
            Add-StructureRows -Value $item -Path $arrayPath -Source $Source -ScalarValues $ScalarValues -Rows $Rows
        }
        return
    }

    if ($type -eq "Object") {
        foreach ($property in @($Value.PSObject.Properties)) {
            $safeName = Get-SafePropertyName -Name $property.Name -ScalarValues $ScalarValues
            $childPath = if ($Path) { "$Path.$safeName" } else { $safeName }
            Add-StructureRows -Value $property.Value -Path $childPath -Source $Source -ScalarValues $ScalarValues -Rows $Rows
        }
    }
}

function Test-OutputForScalarLeak {
    param(
        [string]$Text,
        [System.Collections.Generic.HashSet[string]]$ScalarValues
    )

    $blockedCount = 0

    foreach ($value in $ScalarValues) {
        if ([string]::IsNullOrWhiteSpace($value)) { continue }

        # Short/common values create false positives and cannot be emitted by this structure format
        # except as property names, which are independently sanitized above.
        if ($value.Length -lt 6) { continue }
        if ($value -match '^(true|false|null|string|integer|number|object|array|scalar|unknown)$') { continue }

        if ($Text.IndexOf($value,[StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $blockedCount++
        }
    }

    return $blockedCount
}

function Convert-RowsToText {
    param($Rows)

    $lines = New-Object System.Collections.ArrayList
    foreach ($row in @($Rows | Sort-Object Source,FieldPath,Type -Unique)) {
        [void]$lines.Add(("{0} | {1} | {2}" -f $row.Source,$row.FieldPath,$row.Type))
    }
    return ($lines -join [Environment]::NewLine)
}

$pgRoot = Join-Path $EvidenceDirectory "PGs"
if (-not (Test-Path $pgRoot -PathType Container)) {
    throw "PGs directory not found under evidence directory: $pgRoot"
}

$allScalarValues = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
$records = New-Object System.Collections.ArrayList

foreach ($pgDir in @(Get-ChildItem -Path $pgRoot -Directory | Sort-Object Name)) {
    $manifestPath = Join-Path $pgDir.FullName "10_Manifest.json"
    $manifest = Read-JsonFile -Path $manifestPath
    if ($null -eq $manifest) { continue }

    $environmentApiName = [string]$manifest.EnvironmentApiName
    if (-not $EnvironmentFileNames.Contains($environmentApiName)) { continue }

    foreach ($fileName in $EvidenceFiles) {
        $path = Join-Path $pgDir.FullName $fileName
        if (-not (Test-Path $path -PathType Leaf)) { continue }

        $data = Read-JsonFile -Path $path
        Add-ScalarValues -Value $data -Set $allScalarValues

        [void]$records.Add([pscustomobject]@{
            EnvironmentApiName=$environmentApiName
            Source=([IO.Path]::GetFileNameWithoutExtension($fileName))
            Data=$data
        })
    }
}

if ($records.Count -eq 0) {
    throw "No usable PG evidence records were found."
}

$pendingOutputs = New-Object System.Collections.ArrayList
$privacyFailures = New-Object System.Collections.ArrayList

foreach ($environmentApiName in $EnvironmentFileNames.Keys) {
    $environmentRecords = @($records | Where-Object { $_.EnvironmentApiName -eq $environmentApiName })
    if ($environmentRecords.Count -eq 0) { continue }

    $rows = New-Object System.Collections.ArrayList
    foreach ($record in $environmentRecords) {
        Add-StructureRows -Value $record.Data -Path "" -Source $record.Source -ScalarValues $allScalarValues -Rows $rows
    }

    $distinctRows = @($rows | Sort-Object Source,FieldPath,Type -Unique)
    $jsonText = ConvertTo-Json -InputObject $distinctRows -Depth 10
    $textOutput = Convert-RowsToText -Rows $distinctRows

    $jsonLeakCount = Test-OutputForScalarLeak -Text $jsonText -ScalarValues $allScalarValues
    $textLeakCount = Test-OutputForScalarLeak -Text $textOutput -ScalarValues $allScalarValues

    if (($jsonLeakCount + $textLeakCount) -gt 0) {
        [void]$privacyFailures.Add([pscustomobject][ordered]@{
            Environment=$environmentApiName
            Status="BLOCKED"
            PotentialLeakMatchCount=($jsonLeakCount + $textLeakCount)
        })
        continue
    }

    [void]$pendingOutputs.Add([pscustomobject]@{
        Environment=$environmentApiName
        SafeName=$EnvironmentFileNames[$environmentApiName]
        Json=$jsonText
        Text=$textOutput
        FieldCount=$distinctRows.Count
    })
}

if ($privacyFailures.Count -gt 0) {
    if (-not (Test-Path $OutputDirectory -PathType Container)) {
        New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
    }

    # Fail closed: do not create any shareable field-structure files when validation fails.
    [ordered]@{
        Status="BLOCKED"
        ShareableOutputCreated=$false
        Reason="Potential source scalar value detected in generated structural output."
        FailedEnvironmentCount=$privacyFailures.Count
        Environments=@($privacyFailures)
        GeneratedAt=(Get-Date).ToString("o")
    } | ConvertTo-Json -Depth 10 | Set-Content -Path (Join-Path $OutputDirectory "PrivacyValidation.json") -Encoding UTF8

    throw "Privacy validation failed. Shareable structure files were not created. See PrivacyValidation.json."
}

if (-not (Test-Path $OutputDirectory -PathType Container)) {
    New-Item -Path $OutputDirectory -ItemType Directory -Force | Out-Null
}

foreach ($output in $pendingOutputs) {
    $jsonPath = Join-Path $OutputDirectory ("{0}_FieldStructure.json" -f $output.SafeName)
    $textPath = Join-Path $OutputDirectory ("{0}_FieldStructure.txt" -f $output.SafeName)
    Set-Content -Path $jsonPath -Value $output.Json -Encoding UTF8
    Set-Content -Path $textPath -Value $output.Text -Encoding UTF8
}

[ordered]@{
    Status="PASSED"
    ShareableOutputCreated=$true
    GeneratedAt=(Get-Date).ToString("o")
    EvidenceDirectoryHash=(Get-FileHash -Path (Join-Path $EvidenceDirectory "Run_Metadata.json") -Algorithm SHA256).Hash
    Environments=@($pendingOutputs | ForEach-Object {
        [pscustomobject]@{
            Environment=$_.Environment
            OutputName=$_.SafeName
            FieldCount=$_.FieldCount
        }
    })
    Safety="Offline structure-only output. No API calls. No production values intentionally emitted."
} | ConvertTo-Json -Depth 10 | Set-Content -Path (Join-Path $OutputDirectory "PrivacyValidation.json") -Encoding UTF8

$hashLines = @()
foreach ($file in @(Get-ChildItem -Path $OutputDirectory -File | Where-Object { $_.Name -ne "SHA256SUMS.txt" } | Sort-Object Name)) {
    $hash = Get-FileHash -Path $file.FullName -Algorithm SHA256
    $hashLines += ("{0}  {1}" -f $hash.Hash,$file.Name)
}
$hashLines | Set-Content -Path (Join-Path $OutputDirectory "SHA256SUMS.txt") -Encoding UTF8

Write-Host ""
Write-Host "Structure extraction complete." -ForegroundColor Green
Write-Host "Privacy validation: PASSED" -ForegroundColor Green
Write-Host "Output: $OutputDirectory" -ForegroundColor Green
