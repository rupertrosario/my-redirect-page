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

$results = $json.results |
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
        @{Name='Remediation'; Expression={
            if ($null -eq $_.remediation) { '' }
            elseif ($_.remediation -is [string]) { $_.remediation }
            else { $_.remediation | ConvertTo-Json -Depth 10 -Compress }
        }}

# Export to a timestamped CSV so existing results are never overwritten.
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$sourceName = [System.IO.Path]::GetFileNameWithoutExtension($Path)
$outputDir = Split-Path -Parent (Resolve-Path $Path)
$csvPath = Join-Path $outputDir ("{0}_CI_Results_{1}.csv" -f $sourceName, $timestamp)

$results | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
Write-Host "CSV exported to: $csvPath"
