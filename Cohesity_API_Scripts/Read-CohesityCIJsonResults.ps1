param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

# Read Cohesity Siren / CI diagnostic JSON and export only operationally useful fields.
$json = Get-Content -Path $Path -Raw | ConvertFrom-Json

if (-not $json.results) {
    Write-Warning "No 'results' property was found in the JSON file."
    return
}

# Convert nested strings/arrays/objects into readable CSV text instead of JSON such as ["..."] or {"..."}.
function ConvertTo-ReadableText {
    param($Value)

    if ($null -eq $Value) {
        return ''
    }

    if ($Value -is [string]) {
        return $Value.Trim()
    }

    if ($Value -is [System.Collections.IDictionary]) {
        $parts = foreach ($key in $Value.Keys) {
            $text = ConvertTo-ReadableText $Value[$key]
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                "$key: $text"
            }
        }
        return ($parts -join '; ')
    }

    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $parts = foreach ($property in $Value.PSObject.Properties) {
            $text = ConvertTo-ReadableText $property.Value
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                "$($property.Name): $text"
            }
        }
        return ($parts -join '; ')
    }

    if ($Value -is [System.Collections.IEnumerable]) {
        $parts = foreach ($item in $Value) {
            $text = ConvertTo-ReadableText $item
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                $text
            }
        }
        return (($parts | Select-Object -Unique) -join '; ')
    }

    return [string]$Value
}

# Use the source JSON file timestamp for the report date/time.
$sourceFile = Get-Item -Path $Path
$reportDate = $sourceFile.LastWriteTime.ToString('yyyy-MM-dd')
$reportTime = $sourceFile.LastWriteTime.ToString('HH:mm:ss')

$rawResults = $json.results |
    Select-Object `
        @{Name='Test #'; Expression={$_.testNumber}},
        @{Name='Test Name'; Expression={$_.testName}},
        @{Name='State'; Expression={$_.state}},
        @{Name='Severity'; Expression={$_.severity}},
        @{Name='Module'; Expression={$_.module}},
        @{Name='Code'; Expression={$_.code}},
        @{Name='Result'; Expression={
            if ($null -ne $_.out -and "$($_.out)".Trim()) {
                if ($_.out -is [string]) { $_.out }
                else { $_.out | ConvertTo-Json -Depth 10 -Compress }
            }
            elseif ($null -ne $_.data) {
                if ($_.data -is [string]) { $_.data }
                else { $_.data | ConvertTo-Json -Depth 10 -Compress }
            }
            else {
                ''
            }
        }},
        @{Name='Remediation'; Expression={ ConvertTo-ReadableText $_.remediation }}

# Consolidate duplicate tests so the same issue is not repeated for multiple nodes/instances.
# Unique Result/Remediation values are retained and combined when they differ.
$results = $rawResults |
    Group-Object -Property 'Test #','Test Name','State','Severity','Module','Code' |
    ForEach-Object {
        $first = $_.Group[0]

        $uniqueResults = $_.Group.Result |
            Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
            Select-Object -Unique

        $uniqueRemediation = $_.Group.Remediation |
            Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
            Select-Object -Unique

        [PSCustomObject]@{
            'Date'        = $reportDate
            'Time'        = $reportTime
            'Test #'      = $first.'Test #'
            'Test Name'   = $first.'Test Name'
            'State'       = $first.State
            'Severity'    = $first.Severity
            'Module'      = $first.Module
            'Code'        = $first.Code
            'Occurrences' = $_.Count
            'Result'      = ($uniqueResults -join ' | ')
            'Remediation' = ($uniqueRemediation -join ' | ')
        }
    } |
    Sort-Object @{Expression={
        $number = 0
        if ([int]::TryParse([string]$_.'Test #', [ref]$number)) { $number } else { [int]::MaxValue }
    }}, 'Test #'

# Export to a timestamped CSV so existing results are never overwritten.
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$sourceName = [System.IO.Path]::GetFileNameWithoutExtension($Path)
$outputDir = Split-Path -Parent (Resolve-Path $Path)
$csvPath = Join-Path $outputDir ("{0}_CI_Results_{1}.csv" -f $sourceName, $timestamp)

$results | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
Write-Host "CSV exported to: $csvPath"
