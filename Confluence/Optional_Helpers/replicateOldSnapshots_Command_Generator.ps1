# Cohesity replicateOldSnapshots command generator
# Generates preview and commit commands only.
# It does not execute replication.

$sourceCluster = Read-Host "Enter source cluster"
$targetCluster = Read-Host "Enter replication target"
$domain = Read-Host "Enter domain"
$protectionGroup = Read-Host "Enter exact protection group name"
$crNumber = Read-Host "Enter CR number"

Write-Host ""
Write-Host "Select snapshot scope:"
Write-Host "1. All eligible snapshots"
Write-Host "2. One backup date"
Write-Host "3. Continuous date range"
Write-Host "4. Random dates (maximum 5)"
Write-Host "5. Dates from a text file"
$scope = Read-Host "Enter option 1-5"

$keepFor = Read-Host "Enter keepFor days, or press Enter to retain the local expiration"
$resyncAnswer = Read-Host "Resync previously replicated snapshots? Enter Y only if deleted or extending retention"

$optionalParameters = @()

if ($keepFor -match '^\d+$' -and [int]$keepFor -gt 0) {
    $optionalParameters += "-keepFor $keepFor"
}

if ($resyncAnswer -eq "Y") {
    $optionalParameters += "-resync_WARNING_READ_THE_README_YOU_PROBABLY_DONT_WANT_TO_DO_THIS"
}

$runDate = (Get-Date).Date

function Convert-ToFilter {
    param (
        [datetime]$StartDate,
        [datetime]$EndDate
    )

    if ($StartDate -gt $EndDate) {
        throw "Start date cannot be later than end date."
    }

    if ($EndDate -ge $runDate) {
        throw "Backup dates must be earlier than the run date."
    }

    $newerThan = [math]::Floor(($runDate - $StartDate.Date).TotalDays) + 1
    $olderThan = [math]::Floor(($runDate - $EndDate.Date).TotalDays)

    return @{
        NewerThan = $newerThan
        OlderThan = $olderThan
    }
}

function New-ReplicationCommand {
    param (
        [string]$Description,
        [Nullable[int]]$NewerThan,
        [Nullable[int]]$OlderThan
    )

    $parameters = @(
        '.\replicateOldSnapshots.ps1'
        "    -vip `"$sourceCluster`""
        '    -username "x_"'
        "    -domain `"$domain`""
        "    -replicateTo `"$targetCluster`""
        "    -jobName `"$protectionGroup`""
    )

    if ($null -ne $NewerThan) {
        $parameters += "    -newerThan $NewerThan"
    }

    if ($null -ne $OlderThan) {
        $parameters += "    -olderThan $OlderThan"
    }

    foreach ($parameter in $optionalParameters) {
        $parameters += "    $parameter"
    }

    $previewCommand = $parameters -join " ``$([Environment]::NewLine)"
    $commitCommand = ($parameters + "    -commit") -join " ``$([Environment]::NewLine)"

    Write-Host ""
    Write-Host "============================================================"
    Write-Host $Description
    Write-Host "CR: $crNumber"
    Write-Host "Run date: $($runDate.ToString('dd MMMM yyyy'))"

    Write-Host ""
    Write-Host "PREVIEW COMMAND"
    Write-Host $previewCommand

    Write-Host ""
    Write-Host "COMMIT COMMAND - use only after reviewing the preview"
    Write-Host $commitCommand
}

try {
    switch ($scope) {
        "1" {
            New-ReplicationCommand `
                -Description "All eligible snapshots for $protectionGroup" `
                -NewerThan $null `
                -OlderThan $null
        }

        "2" {
            $dateInput = Read-Host "Enter backup date (yyyy-MM-dd)"
            $backupDate = [datetime]::ParseExact(
                $dateInput,
                "yyyy-MM-dd",
                [Globalization.CultureInfo]::InvariantCulture
            )

            $filter = Convert-ToFilter -StartDate $backupDate -EndDate $backupDate

            New-ReplicationCommand `
                -Description "Backup date: $($backupDate.ToString('dd MMMM yyyy'))" `
                -NewerThan $filter.NewerThan `
                -OlderThan $filter.OlderThan
        }

        "3" {
            $startInput = Read-Host "Enter first backup date (yyyy-MM-dd)"
            $endInput = Read-Host "Enter last backup date (yyyy-MM-dd)"

            $startDate = [datetime]::ParseExact(
                $startInput,
                "yyyy-MM-dd",
                [Globalization.CultureInfo]::InvariantCulture
            )

            $endDate = [datetime]::ParseExact(
                $endInput,
                "yyyy-MM-dd",
                [Globalization.CultureInfo]::InvariantCulture
            )

            $filter = Convert-ToFilter -StartDate $startDate -EndDate $endDate
            $numberOfDays = ($endDate.Date - $startDate.Date).Days + 1

            New-ReplicationCommand `
                -Description "Getting backup runs for $numberOfDays days: $($startDate.ToString('dd MMMM yyyy')) to $($endDate.ToString('dd MMMM yyyy'))" `
                -NewerThan $filter.NewerThan `
                -OlderThan $filter.OlderThan
        }

        "4" {
            $dateInput = Read-Host "Enter up to 5 random dates separated by commas (yyyy-MM-dd)"
            $dates = @(
                $dateInput.Split(",") |
                ForEach-Object { $_.Trim() } |
                Where-Object { $_ }
            )

            if ($dates.Count -gt 5) {
                throw "More than 5 dates entered. Use option 5 with a text file."
            }

            foreach ($date in $dates) {
                $backupDate = [datetime]::ParseExact(
                    $date,
                    "yyyy-MM-dd",
                    [Globalization.CultureInfo]::InvariantCulture
                )

                $filter = Convert-ToFilter -StartDate $backupDate -EndDate $backupDate

                New-ReplicationCommand `
                    -Description "Random backup date: $($backupDate.ToString('dd MMMM yyyy'))" `
                    -NewerThan $filter.NewerThan `
                    -OlderThan $filter.OlderThan
            }
        }

        "5" {
            $filePath = Read-Host "Enter text-file path"

            if (-not (Test-Path -LiteralPath $filePath -PathType Leaf)) {
                throw "File not found: $filePath"
            }

            $dates = @(
                Get-Content -LiteralPath $filePath |
                ForEach-Object { $_.Trim() } |
                Where-Object { $_ -and -not $_.StartsWith("#") } |
                Sort-Object -Unique
            )

            if ($dates.Count -eq 0) {
                throw "The file does not contain any dates."
            }

            foreach ($date in $dates) {
                $backupDate = [datetime]::ParseExact(
                    $date,
                    "yyyy-MM-dd",
                    [Globalization.CultureInfo]::InvariantCulture
                )

                $filter = Convert-ToFilter -StartDate $backupDate -EndDate $backupDate

                New-ReplicationCommand `
                    -Description "File backup date: $($backupDate.ToString('dd MMMM yyyy'))" `
                    -NewerThan $filter.NewerThan `
                    -OlderThan $filter.OlderThan
            }
        }

        default {
            throw "Invalid option. Select a value from 1 to 5."
        }
    }

    Write-Host ""
    Write-Host "Verify each required snapshot is displayed as 'Would replicate' before using -commit."
}
catch {
    Write-Host ""
    Write-Host "Error: $($_.Exception.Message)"
    exit 1
}
