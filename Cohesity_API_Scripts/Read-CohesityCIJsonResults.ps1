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
                "$key: $text"
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

# Only show checks requiring attention. Passed checks are intentionally excluded.
$results = $json.results |
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

if (-not $results) {
    Write-Host 'No non-Pass CI checks found.'
    return
}

# Human-readable console table. Remediation entries remain on separate lines.
$results | Format-Table -AutoSize -Wrap
