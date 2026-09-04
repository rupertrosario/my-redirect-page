param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

# Read Cohesity Siren / CI diagnostic JSON.
# Keep the raw JSON untouched so it can also be consumed later by Claude or other analysis tooling.
$json = Get-Content -Path $Path -Raw | ConvertFrom-Json

if (-not $json.results) {
    Write-Warning "No 'results' property was found in the JSON file."
    return
}

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
                "$($key): $text"
            }
        }
        return ($parts -join "`n")
    }

    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $parts = foreach ($property in $Value.PSObject.Properties) {
            $text = ConvertTo-ReadableText $property.Value
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                "$($property.Name): $text"
            }
        }
        return ($parts -join "`n")
    }

    if ($Value -is [System.Collections.IEnumerable]) {
        $parts = foreach ($item in $Value) {
            $text = ConvertTo-ReadableText $item
            if (-not [string]::IsNullOrWhiteSpace($text)) {
                $text
            }
        }
        return (($parts | Select-Object -Unique) -join "`n")
    }

    return [string]$Value
}

# Build only non-Pass checks.
$rawResults = $json.results |
    Where-Object { $_.state -ne 'Pass' } |
    Select-Object `
        @{Name='Test #'; Expression={$_.testNumber}},
        @{Name='Test Name'; Expression={$_.testName}},
        @{Name='State'; Expression={$_.state}},
        @{Name='Severity'; Expression={$_.severity}},
        @{Name='Module'; Expression={$_.module}},
        @{Name='Code'; Expression={$_.code}},
        @{Name='Result'; Expression={
            if ($null -ne $_.data) {
                ConvertTo-ReadableText $_.data
            }
            elseif ($null -ne $_.out) {
                ConvertTo-ReadableText $_.out
            }
            else {
                ''
            }
        }},
        @{Name='Remediation'; Expression={
            ConvertTo-ReadableText $_.remediation
        }}

if (-not $rawResults) {
    Write-Host 'No non-Pass CI checks found.'
    return
}

# Consolidate repeated checks so the same issue is not written again and again.
# Preserve unique Result and Remediation text when duplicate rows contain different details.
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
            'Test #'      = $first.'Test #'
            'Test Name'   = $first.'Test Name'
            'State'       = $first.State
            'Severity'    = $first.Severity
            'Module'      = $first.Module
            'Code'        = $first.Code
            'Occurrences' = $_.Count
            'Result'      = ($uniqueResults -join "`n")
            'Remediation' = ($uniqueRemediation -join "`n")
        }
    } |
    Sort-Object 'Test #','Test Name'

# Export only to CSV; do not print the result table in the PowerShell window.
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$sourceName = [System.IO.Path]::GetFileNameWithoutExtension($Path)
$outputDir = Split-Path -Parent (Resolve-Path $Path)
$csvPath = Join-Path $outputDir ("{0}_CI_Results_{1}.csv" -f $sourceName, $timestamp)

$results | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
Write-Host "CSV exported to: $csvPath"
