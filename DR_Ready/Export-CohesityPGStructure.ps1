# Cohesity Protection Group Evidence - Offline Structure Extractor
# PowerShell 5.1 compatible
#
# Purpose:
#   Read a LOCAL evidence directory produced by Get-CohesityDRReadyPGConfig.ps1
#   and create a value-free structural view containing field paths and data types.
#
# Safety:
#   This script performs no network or Cohesity API operations.

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$EvidenceRoot
)

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1

# Safety invariant: this script must remain offline.
if ($PSCommandPath -and (Test-Path $PSCommandPath -PathType Leaf)) {
    $selfText = Get-Content -Path $PSCommandPath -Raw
    $networkTerms = @(
        ("Invoke-" + "WebRequest"),
        ("Invoke-" + "RestMethod"),
        ("System.Net." + "WebClient"),
        ("Http" + "Client")
    )
    $networkPattern = '(?i)\b(' + (($networkTerms | ForEach-Object { [regex]::Escape($_) }) -join '|') + ')\b'
    if ($selfText -match $networkPattern) {
        throw "Safety validation failed: network-capable code exists in the offline structure extractor."
    }
}

if (-not (Test-Path $EvidenceRoot -PathType Container)) {
    throw "EvidenceRoot not found: $EvidenceRoot"
}

$pgRoot = Join-Path $EvidenceRoot "PGs"
if (-not (Test-Path $pgRoot -PathType Container)) {
    throw "EvidenceRoot must be one collected cluster directory containing a PGs folder."
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputDirectory = Join-Path $EvidenceRoot ("Shareable_Structure_{0}" -f $timestamp)
New-Item -Path $outputDirectory -ItemType Directory -Force | Out-Null

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

$KnownEnvironments = @(
    "NAS",
    "SQL",
    "Hyper-V",
    "Nutanix AHV",
    "Oracle",
    "Physical"
)

$SafeLowercaseKeys = @(
    "id","name","type","status","value","key","path","paths","items","objects","children",
    "environment","description","priority","enabled","disabled","state","mode","protocol",
    "port","ports","username","domain","host","hosts","cluster","clusters","policy","policies",
    "source","sources","object","storage","retention","frequency","schedule","timezone",
    "version","provider","region","tags","tag","params","config","configs","settings",
    "writers","disks","volumes","files","shares","instances","databases","database",
    "vm","vms","vss","sla","qos","mssql","oracle","nas","hyperv","acropolis","physical"
)

$SafeScalarValues = @(
    "NAS","SQL","Hyper-V","Nutanix AHV","Oracle","Physical",
    "GET","SUCCESS","FAILED","COMPLETE","PARTIAL","RESOLVED","UNRESOLVED",
    "NOT_REFERENCED","COLLECTION_FAILED","DetailedGET","ListRecordSupplemental",
    "String","Boolean","Byte","SByte","Int16","UInt16","Int32","UInt32","Int64","UInt64",
    "Single","Double","Decimal","DateTime","Guid","Object","Array","Null"
)

function Write-JsonText {
    param(
        [Parameter(Mandatory=$true)][string]$Text,
        [Parameter(Mandatory=$true)][string]$Path
    )

    Set-Content -Path $Path -Value $Text -Encoding UTF8
}

function Write-Json {
    param(
        [AllowNull()]$Value,
        [Parameter(Mandatory=$true)][string]$Path
    )

    ConvertTo-Json -InputObject $Value -Depth 100 | Set-Content -Path $Path -Encoding UTF8
}

function Get-JsonFile {
    param([Parameter(Mandatory=$true)][string]$Path)

    $raw = Get-Content -Path $Path -Raw -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    return ($raw | ConvertFrom-Json)
}

function Test-SuspiciousKey {
    param([string]$Key)

    if ([string]::IsNullOrWhiteSpace($Key)) { return $true }

    $k = $Key.Trim()

    if ($k -match '^\d+$') { return $true }
    if ($k -match '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') { return $true }
    if ($k -match '^[0-9a-fA-F]{16,}$') { return $true }
    if ($k -match '^(?:\d{1,3}\.){3}\d{1,3}$') { return $true }
    if ($k -match '[\\/@:=,\s]') { return $true }
    if ($k.Length -gt 80) { return $true }

    # Schema property names are normally lower-camel-case. Preserve known simple lowercase keys.
    if ($k -cmatch '^[a-z][A-Za-z0-9]*$') {
        if ($SafeLowercaseKeys -contains $k) { return $false }
        if ($k -cmatch '[A-Z]') { return $false }
        if ($k -match '(?i)(Id|Ids|Params|Config|Info|Policy|Source|Object|Storage|Domain|Alert|Backup|Snapshot|Retention|Schedule|Time|Count|Path|Disk|Writer|Include|Exclude|Enabled|Active|Deleted|Paused|Name|Type|Status|Mode|Settings)$') {
            return $false
        }

        return $true
    }

    return $true
}

function Convert-ToSafeKeyName {
    param([string]$Key)

    if (Test-SuspiciousKey -Key $Key) {
        $script:UnclassifiedKeyCount++
        return "{unclassified-key}"
    }

    return $Key
}

function Get-ValueTypeName {
    param($Value)

    if ($null -eq $Value) { return "Null" }

    if ($Value -is [string]) { return "String" }
    if ($Value -is [char]) { return "String" }
    if ($Value -is [bool]) { return "Boolean" }
    if ($Value -is [byte]) { return "Byte" }
    if ($Value -is [sbyte]) { return "SByte" }
    if ($Value -is [int16]) { return "Int16" }
    if ($Value -is [uint16]) { return "UInt16" }
    if ($Value -is [int32]) { return "Int32" }
    if ($Value -is [uint32]) { return "UInt32" }
    if ($Value -is [int64]) { return "Int64" }
    if ($Value -is [uint64]) { return "UInt64" }
    if ($Value -is [single]) { return "Single" }
    if ($Value -is [double]) { return "Double" }
    if ($Value -is [decimal]) { return "Decimal" }
    if ($Value -is [datetime]) { return "DateTime" }
    if ($Value -is [guid]) { return "Guid" }
    if ($Value -is [System.Collections.IDictionary]) { return "Object" }
    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) { return "Array" }

    return "Object"
}

function Get-StructureRows {
    param(
        $Value,
        [string]$Path,
        [string]$SourceFile
    )

    $effectivePath = if ([string]::IsNullOrWhiteSpace($Path)) { '$' } else { $Path }
    $typeName = Get-ValueTypeName -Value $Value

    [pscustomobject][ordered]@{
        SourceFile=$SourceFile
        FieldPath=$effectivePath
        Type=$typeName
    }

    if ($null -eq $Value) { return }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            $safeKey = Convert-ToSafeKeyName -Key ([string]$key)
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { $safeKey } else { "$Path.$safeKey" }
            Get-StructureRows -Value $Value[$key] -Path $childPath -SourceFile $SourceFile
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        foreach ($item in @($Value)) {
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { '[]' } else { "$Path[]" }
            Get-StructureRows -Value $item -Path $childPath -SourceFile $SourceFile
        }
        return
    }

    if ($Value -isnot [string] -and
        $Value -isnot [char] -and
        $Value -isnot [bool] -and
        $Value -isnot [byte] -and
        $Value -isnot [sbyte] -and
        $Value -isnot [int16] -and
        $Value -isnot [uint16] -and
        $Value -isnot [int32] -and
        $Value -isnot [uint32] -and
        $Value -isnot [int64] -and
        $Value -isnot [uint64] -and
        $Value -isnot [single] -and
        $Value -isnot [double] -and
        $Value -isnot [decimal] -and
        $Value -isnot [datetime] -and
        $Value -isnot [guid]) {

        foreach ($property in @($Value.PSObject.Properties)) {
            $safeKey = Convert-ToSafeKeyName -Key $property.Name
            $childPath = if ([string]::IsNullOrWhiteSpace($Path)) { $safeKey } else { "$Path.$safeKey" }
            Get-StructureRows -Value $property.Value -Path $childPath -SourceFile $SourceFile
        }
    }
}

function Add-ScalarCandidates {
    param(
        $Value,
        [hashtable]$CandidateSet
    )

    if ($null -eq $Value) { return }

    if ($Value -is [bool]) { return }

    if (
        $Value -is [byte] -or $Value -is [sbyte] -or
        $Value -is [int16] -or $Value -is [uint16] -or
        $Value -is [int32] -or $Value -is [uint32] -or
        $Value -is [int64] -or $Value -is [uint64] -or
        $Value -is [single] -or $Value -is [double] -or
        $Value -is [decimal]
    ) {
        $numberText = [string]$Value
        if ($numberText.Length -ge 5) { $CandidateSet[$numberText] = $true }
        return
    }

    if ($Value -is [string] -or $Value -is [char] -or $Value -is [datetime] -or $Value -is [guid]) {
        $text = ([string]$Value).Trim()
        if ($text.Length -ge 6 -and -not ($SafeScalarValues -contains $text)) {
            $CandidateSet[$text] = $true
        }
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in @($Value.Keys)) {
            Add-ScalarCandidates -Value $Value[$key] -CandidateSet $CandidateSet
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        foreach ($item in @($Value)) {
            Add-ScalarCandidates -Value $item -CandidateSet $CandidateSet
        }
        return
    }

    foreach ($property in @($Value.PSObject.Properties)) {
        Add-ScalarCandidates -Value $property.Value -CandidateSet $CandidateSet
    }
}

function Test-ContentForSourceValues {
    param(
        [string]$Content,
        [hashtable]$CandidateSet
    )

    $matchCount = 0

    foreach ($candidate in @($CandidateSet.Keys)) {
        if ([string]::IsNullOrWhiteSpace($candidate)) { continue }

        if ($Content.IndexOf($candidate,[System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $matchCount++
        }
    }

    return $matchCount
}

function Get-SafeEnvironmentFileName {
    param([string]$Environment)

    switch ($Environment) {
        "NAS" { return "NAS" }
        "SQL" { return "SQL" }
        "Hyper-V" { return "HyperV" }
        "Nutanix AHV" { return "AHV" }
        "Oracle" { return "Oracle" }
        "Physical" { return "Physical" }
        default { return "Unknown" }
    }
}

function Write-Sha256File {
    param([string]$Directory)

    $checksumPath = Join-Path $Directory "SHA256SUMS.txt"
    $files = @(Get-ChildItem -Path $Directory -File | Where-Object { $_.FullName -ne $checksumPath } | Sort-Object Name)

    $lines = @()
    foreach ($file in $files) {
        $hash = (Get-FileHash -Path $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        $lines += "$hash  $($file.Name)"
    }

    Set-Content -Path $checksumPath -Value $lines -Encoding UTF8
}

$privacyResults = @()
$diagnosticResults = @()

foreach ($environment in $KnownEnvironments) {
    $script:UnclassifiedKeyCount = 0

    $rows = @()
    $candidateSet = @{}
    $pgsInspected = 0
    $filesInspected = 0
    $readErrors = 0

    foreach ($pgDir in @(Get-ChildItem -Path $pgRoot -Directory | Sort-Object Name)) {
        $manifestPath = Join-Path $pgDir.FullName "10_Manifest.json"
        if (-not (Test-Path $manifestPath -PathType Leaf)) {
            $readErrors++
            continue
        }

        try {
            $manifest = Get-JsonFile -Path $manifestPath
        }
        catch {
            $readErrors++
            continue
        }

        if ([string]$manifest.Environment -ne $environment) { continue }

        $pgsInspected++

        foreach ($fileName in $EvidenceFiles) {
            $filePath = Join-Path $pgDir.FullName $fileName
            if (-not (Test-Path $filePath -PathType Leaf)) {
                $readErrors++
                continue
            }

            try {
                $data = Get-JsonFile -Path $filePath
                $filesInspected++

                foreach ($row in @(Get-StructureRows -Value $data -Path "" -SourceFile $fileName)) {
                    $rows += $row
                }

                Add-ScalarCandidates -Value $data -CandidateSet $candidateSet
            }
            catch {
                $readErrors++
            }
        }
    }

    $dedupedRows = @(
        $rows |
        Sort-Object SourceFile,FieldPath,Type -Unique |
        Select-Object SourceFile,FieldPath,Type
    )

    $jsonText = ConvertTo-Json -InputObject @($dedupedRows) -Depth 10

    $txtLines = @(
        "SourceFile`tFieldPath`tType"
        foreach ($row in $dedupedRows) {
            "$($row.SourceFile)`t$($row.FieldPath)`t$($row.Type)"
        }
    )
    $txtText = $txtLines -join [Environment]::NewLine

    $combinedShareableContent = $jsonText + [Environment]::NewLine + $txtText
    $potentialMatchCount = Test-ContentForSourceValues -Content $combinedShareableContent -CandidateSet $candidateSet

    $privacyStatus = if ($potentialMatchCount -eq 0) { "PASSED" } else { "BLOCKED" }
    $safeEnvironmentName = Get-SafeEnvironmentFileName -Environment $environment

    if ($privacyStatus -eq "PASSED") {
        Write-JsonText -Text $jsonText -Path (Join-Path $outputDirectory ("{0}_Structure.json" -f $safeEnvironmentName))
        Set-Content -Path (Join-Path $outputDirectory ("{0}_Structure.txt" -f $safeEnvironmentName)) -Value $txtText -Encoding UTF8
    }

    $privacyResults += [pscustomobject][ordered]@{
        Environment=$environment
        Status=$privacyStatus
        ProtectionGroupsInspected=$pgsInspected
        EvidenceFilesInspected=$filesInspected
        StructureRowCount=$dedupedRows.Count
        UnclassifiedKeyCount=$script:UnclassifiedKeyCount
        ScalarCandidatesTested=@($candidateSet.Keys).Count
        PotentialSourceValueMatches=$potentialMatchCount
        ShareableFilesCreated=($privacyStatus -eq "PASSED")
    }

    $diagnosticResults += [pscustomobject][ordered]@{
        Environment=$environment
        ProtectionGroupsInspected=$pgsInspected
        EvidenceFilesInspected=$filesInspected
        ReadErrorCount=$readErrors
        StructureRowCount=$dedupedRows.Count
        UnclassifiedKeyCount=$script:UnclassifiedKeyCount
    }
}

Write-Json -Value $privacyResults -Path (Join-Path $outputDirectory "PrivacyValidation.json")
Write-Json -Value $diagnosticResults -Path (Join-Path $outputDirectory "StructureDiagnostics.json")

$blockedCount = @($privacyResults | Where-Object { $_.Status -eq "BLOCKED" }).Count
$passedCount = @($privacyResults | Where-Object { $_.Status -eq "PASSED" }).Count

Write-Json -Value ([ordered]@{
    GeneratedAt=(Get-Date).ToString("o")
    Offline=$true
    EnvironmentsEvaluated=$KnownEnvironments.Count
    Passed=$passedCount
    Blocked=$blockedCount
    Rule="Only field paths and data types are written when privacy validation passes."
}) -Path (Join-Path $outputDirectory "Run_Metadata.json")

Write-Sha256File -Directory $outputDirectory

Write-Host ""
Write-Host "Offline structure extraction completed." -ForegroundColor Green
Write-Host "Passed: $passedCount  Blocked: $blockedCount" -ForegroundColor Cyan
Write-Host "Output: $outputDirectory" -ForegroundColor Green

if ($blockedCount -gt 0) {
    Write-Host "One or more environment outputs were blocked. See PrivacyValidation.json." -ForegroundColor Yellow
}
