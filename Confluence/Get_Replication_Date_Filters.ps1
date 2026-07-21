# Generates -newerThan and -olderThan values for replicateOldSnapshots.ps1.
# This helper does not start replication.
# Run it on the actual day the replication command will be executed.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$runDate = (Get-Date).Date
$culture = [System.Globalization.CultureInfo]::InvariantCulture

function Convert-ToBackupDate {
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    try {
        return [datetime]::ParseExact($Value.Trim(), 'yyyy-MM-dd', $culture).Date
    }
    catch {
        throw "Invalid date '$Value'. Use yyyy-MM-dd."
    }
}

function Show-DateFilter {
    param(
        [Parameter(Mandatory)]
        [datetime]$StartDate,

        [Parameter(Mandatory)]
        [datetime]$EndDate,

        [string]$Label = 'Backup date selection'
    )

    $StartDate = $StartDate.Date
    $EndDate = $EndDate.Date

    if ($StartDate -gt $EndDate) {
        throw 'Start date cannot be later than end date.'
    }

    if ($EndDate -ge $runDate) {
        throw 'Backup dates must be earlier than the run date.'
    }

    $newerThan = ($runDate - $StartDate).Days + 1
    $olderThan = ($runDate - $EndDate).Days
    $numberOfDays = ($EndDate - $StartDate).Days + 1

    Write-Host ''
    Write-Host '============================================================'
    Write-Host $Label
    Write-Host "Run date:          $($runDate.ToString('dd MMMM yyyy'))"
    Write-Host "Backup date range: $($StartDate.ToString('dd MMMM yyyy')) to $($EndDate.ToString('dd MMMM yyyy'))"
    Write-Host "Number of days:    $numberOfDays"
    Write-Host "Use: -newerThan $newerThan -olderThan $olderThan"
}

Write-Host 'Select the required backup-date option:'
Write-Host '1. Single backup date'
Write-Host '2. Continuous date range'
Write-Host '3. Random dates - maximum 5'
Write-Host '4. Dates from a text file - use for more than 5'

$option = Read-Host 'Select option 1-4'

try {
    switch ($option) {
        '1' {
            $date = Convert-ToBackupDate -Value (Read-Host 'Enter backup date (yyyy-MM-dd)')
            Show-DateFilter -StartDate $date -EndDate $date -Label 'Single backup date'
        }

        '2' {
            $startDate = Convert-ToBackupDate -Value (Read-Host 'Enter first backup date (yyyy-MM-dd)')
            $endDate = Convert-ToBackupDate -Value (Read-Host 'Enter last backup date (yyyy-MM-dd)')
            Show-DateFilter -StartDate $startDate -EndDate $endDate -Label 'Continuous backup date range'
        }

        '3' {
            $entries = @(
                (Read-Host 'Enter up to 5 dates separated by commas (yyyy-MM-dd)').Split(',') |
                    ForEach-Object { $_.Trim() } |
                    Where-Object { $_ }
            )

            if ($entries.Count -eq 0) {
                throw 'No dates were entered.'
            }

            if ($entries.Count -gt 5) {
                throw 'More than 5 dates entered. Use option 4 with a text file.'
            }

            foreach ($entry in $entries) {
                $date = Convert-ToBackupDate -Value $entry
                Show-DateFilter -StartDate $date -EndDate $date -Label 'Random backup date'
            }
        }

        '4' {
            $filePath = Read-Host 'Enter text-file path'

            if (-not (Test-Path -LiteralPath $filePath -PathType Leaf)) {
                throw "File not found: $filePath"
            }

            $entries = @(
                Get-Content -LiteralPath $filePath |
                    ForEach-Object { $_.Trim() } |
                    Where-Object { $_ -and -not $_.StartsWith('#') } |
                    Sort-Object -Unique
            )

            if ($entries.Count -eq 0) {
                throw 'The file does not contain any dates.'
            }

            foreach ($entry in $entries) {
                $date = Convert-ToBackupDate -Value $entry
                Show-DateFilter -StartDate $date -EndDate $date -Label 'Backup date from file'
            }
        }

        default {
            throw 'Invalid option. Select a value from 1 to 4.'
        }
    }

    Write-Host ''
    Write-Host "Run each generated filter pair separately in preview mode. Confirm the required snapshot is shown as 'Would replicate' before adding -commit."
}
catch {
    Write-Host ''
    Write-Host "Error: $($_.Exception.Message)"
    exit 1
}
