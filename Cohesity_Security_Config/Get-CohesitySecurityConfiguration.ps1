# Cohesity Cluster Security Configuration and Password Compliance Inventory
# Multi-cluster | Helios | GET-only | PowerShell 5.1 compatible

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\SecurityConfiguration"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

$script:PasswordStandard = [ordered]@{
    PasswordMinLength           = 15
    ComplexityRequired         = 3
    DisallowedOldPasswords     = 6
    PasswordMinLifetimeDays    = 2
    PasswordMaxLifetimeDays    = 365
    PciPasswordMaxLifetimeDays = 90
}

if (-not (Test-Path $logDirectory -PathType Container)) {
    New-Item -Path $logDirectory -ItemType Directory -Force | Out-Null
}
if (-not (Test-Path $helperPath -PathType Leaf)) {
    throw "API key helper not found at $helperPath"
}
if (-not (Test-Path $encryptedApiKeyPath -PathType Leaf)) {
    throw "Encrypted API key file not found at $encryptedApiKeyPath"
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
        [string]$Uri,
        [hashtable]$Headers
    )

    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -UseBasicParsing `
            -ErrorAction Stop
    }
    else {
        $response = Invoke-WebRequest `
            -Uri $Uri `
            -Headers $Headers `
            -Method Get `
            -ErrorAction Stop
    }

    if (-not $response -or [string]::IsNullOrWhiteSpace($response.Content)) {
        return $null
    }

    return ($response.Content | ConvertFrom-Json)
}

function Value-OrNA {
    param($Value)

    if ($null -eq $Value) {
        return "N/A"
    }

    if ($Value -is [bool]) {
        if ($Value) { return "True" }
        return "False"
    }

    $text = ([string]$Value).Trim()
    if ([string]::IsNullOrWhiteSpace($text)) {
        return "N/A"
    }

    return $text
}

function First-Property {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object -or $Object -is [string]) {
        return "N/A"
    }

    foreach ($name in $Names) {
        foreach ($property in @($Object.PSObject.Properties)) {
            if ($property.Name -ieq $name) {
                $value = Value-OrNA $property.Value
                if ($value -ne "N/A") {
                    return $value
                }
            }
        }
    }

    return "N/A"
}

function Convert-ToNullableNumber {
    param($Value)

    if ($null -eq $Value -or ([string]$Value).Trim() -eq "N/A") {
        return $null
    }

    [double]$number = 0
    if ([double]::TryParse(
        ([string]$Value),
        [Globalization.NumberStyles]::Any,
        [Globalization.CultureInfo]::InvariantCulture,
        [ref]$number
    )) {
        return $number
    }

    return $null
}

function Convert-ToNullableBoolean {
    param($Value)

    if ($Value -is [bool]) {
        return [bool]$Value
    }

    $text = ([string]$Value).Trim()
    if ($text -ieq "True") { return $true }
    if ($text -ieq "False") { return $false }

    return $null
}

function Get-MinimumStatus {
    param(
        $Value,
        [double]$Minimum
    )

    $number = Convert-ToNullableNumber $Value
    if ($null -eq $number) { return "Not Assessed" }
    if ($number -ge $Minimum) { return "Compliant" }
    return "Non-Compliant"
}

function Get-MaximumStatus {
    param(
        $Value,
        [double]$Maximum
    )

    $number = Convert-ToNullableNumber $Value
    if ($null -eq $number) { return "Not Assessed" }
    if ($number -gt 0 -and $number -le $Maximum) { return "Compliant" }
    return "Non-Compliant"
}

function Get-PciValueStatus {
    param($Value)

    $number = Convert-ToNullableNumber $Value
    if ($null -eq $number) { return "Not Assessed" }

    if (
        $number -gt 0 -and
        $number -le $script:PasswordStandard.PciPasswordMaxLifetimeDays
    ) {
        return "Yes"
    }

    return "No"
}

function Get-PasswordPolicyAssessment {
    param($Row)

    $complexityValues = @(
        Convert-ToNullableBoolean $Row.PasswordIncludeUpperLetter
        Convert-ToNullableBoolean $Row.PasswordIncludeLowerLetter
        Convert-ToNullableBoolean $Row.PasswordIncludeNumber
        Convert-ToNullableBoolean $Row.PasswordIncludeSpecialChar
    )

    $complexityKnown = @(
        $complexityValues | Where-Object { $null -ne $_ }
    ).Count -eq 4

    $complexityEnabledCount = $null
    if ($complexityKnown) {
        $complexityEnabledCount = @(
            $complexityValues | Where-Object { $_ -eq $true }
        ).Count
    }

    $passwordLengthStatus = Get-MinimumStatus `
        $Row.PasswordMinLength `
        $script:PasswordStandard.PasswordMinLength

    if (-not $complexityKnown) {
        $passwordComplexityStatus = "Not Assessed"
    }
    elseif (
        $complexityEnabledCount -ge
        $script:PasswordStandard.ComplexityRequired
    ) {
        $passwordComplexityStatus = "Compliant"
    }
    else {
        $passwordComplexityStatus = "Non-Compliant"
    }

    $passwordHistoryStatus = Get-MinimumStatus `
        $Row.NumDisallowedOldPasswords `
        $script:PasswordStandard.DisallowedOldPasswords

    $passwordMinLifetimeStatus = Get-MinimumStatus `
        $Row.PasswordMinLifetimeDays `
        $script:PasswordStandard.PasswordMinLifetimeDays

    $passwordMaxLifetime365Status = Get-MaximumStatus `
        $Row.PasswordMaxLifetimeDays `
        $script:PasswordStandard.PasswordMaxLifetimeDays

    $requiredStatuses = @(
        $passwordLengthStatus
        $passwordComplexityStatus
        $passwordHistoryStatus
        $passwordMinLifetimeStatus
        $passwordMaxLifetime365Status
    )

    if ($requiredStatuses -contains "Not Assessed") {
        $overallStatus = "Not Assessed"
    }
    elseif (@(
        $requiredStatuses | Where-Object { $_ -ne "Compliant" }
    ).Count -eq 0) {
        $overallStatus = "Compliant"
    }
    else {
        $overallStatus = "Non-Compliant"
    }

    $findings = @()

    $minLength = Convert-ToNullableNumber $Row.PasswordMinLength
    if ($passwordLengthStatus -eq "Not Assessed") {
        $findings += "Password minimum length was not returned"
    }
    elseif ($passwordLengthStatus -eq "Non-Compliant") {
        $findings += "Minimum length $minLength is below $($script:PasswordStandard.PasswordMinLength)"
    }

    if ($passwordComplexityStatus -eq "Not Assessed") {
        $findings += "One or more complexity flags were not returned"
    }
    elseif ($passwordComplexityStatus -eq "Non-Compliant") {
        $findings += "Complexity $complexityEnabledCount of 4 is below $($script:PasswordStandard.ComplexityRequired) of 4"
    }

    $history = Convert-ToNullableNumber $Row.NumDisallowedOldPasswords
    if ($passwordHistoryStatus -eq "Not Assessed") {
        $findings += "Password history was not returned"
    }
    elseif ($passwordHistoryStatus -eq "Non-Compliant") {
        $findings += "Password history $history is below $($script:PasswordStandard.DisallowedOldPasswords)"
    }

    $minAge = Convert-ToNullableNumber $Row.PasswordMinLifetimeDays
    if ($passwordMinLifetimeStatus -eq "Not Assessed") {
        $findings += "Minimum password age was not returned"
    }
    elseif ($passwordMinLifetimeStatus -eq "Non-Compliant") {
        $findings += "Minimum password age $minAge days is below $($script:PasswordStandard.PasswordMinLifetimeDays)"
    }

    $maxAge = Convert-ToNullableNumber $Row.PasswordMaxLifetimeDays
    if ($passwordMaxLifetime365Status -eq "Not Assessed") {
        $findings += "Maximum password age was not returned"
    }
    elseif ($passwordMaxLifetime365Status -eq "Non-Compliant") {
        if ($maxAge -le 0) {
            $findings += "Maximum password age is not enabled"
        }
        else {
            $findings += "Maximum password age $maxAge days exceeds $($script:PasswordStandard.PasswordMaxLifetimeDays)"
        }
    }

    if ($null -eq $complexityEnabledCount) {
        $complexityDisplay = "N/A"
    }
    else {
        $complexityDisplay = "$complexityEnabledCount of 4"
    }

    if ($findings.Count -gt 0) {
        $findingText = $findings -join "; "
    }
    else {
        $findingText = "None"
    }

    return [pscustomobject][ordered]@{
        PasswordComplexityEnabledCount = $complexityDisplay
        PasswordLengthStatus            = $passwordLengthStatus
        PasswordComplexityStatus        = $passwordComplexityStatus
        PasswordHistoryStatus           = $passwordHistoryStatus
        PasswordMinLifetimeStatus       = $passwordMinLifetimeStatus
        PasswordMaxLifetime365Status    = $passwordMaxLifetime365Status
        MeetsPCI90DayValue              = Get-PciValueStatus $Row.PasswordMaxLifetimeDays
        OverallPasswordPolicyStatus     = $overallStatus
        ComplianceFindings              = $findingText
    }
}

function New-EmptyRow {
    param([string]$Cluster)

    return [pscustomobject][ordered]@{
        Cluster                                 = $Cluster
        PasswordMinLength                       = "N/A"
        PasswordIncludeUpperLetter              = "N/A"
        PasswordIncludeLowerLetter              = "N/A"
        PasswordIncludeNumber                   = "N/A"
        PasswordIncludeSpecialChar              = "N/A"
        NumDisallowedOldPasswords               = "N/A"
        NumDifferentChars                       = "N/A"
        PasswordMinLifetimeDays                 = "N/A"
        PasswordMaxLifetimeDays                 = "N/A"
        MaxFailedLoginAttempts                  = "N/A"
        FailedLoginLockTimeDurationMins         = "N/A"
        AccountInactivityTimeDays               = "N/A"
        AuthTokenTimeoutMinutes                 = "N/A"
        UIInactivityTimeoutMSecs                = "N/A"
        SessionManagementEnabled                = "N/A"
        SessionAbsoluteTimeoutSeconds           = "N/A"
        SessionInactivityTimeoutSeconds         = "N/A"
        LimitSessions                           = "N/A"
        SessionLimitPerUser                     = "N/A"
        SessionLimitSystemWide                  = "N/A"
        CertificateMappingAuthenticationEnabled = "N/A"
        CertificateMapping                     = "N/A"
        CertificateADMapping                   = "N/A"
        IsDataClassified                        = "N/A"
        ClassifiedDataMessage                  = "N/A"
        UnclassifiedDataMessage                = "N/A"
        SSHTimeoutInMins                        = "N/A"
        PasswordComplexityEnabledCount          = "N/A"
        PasswordLengthStatus                    = "Not Assessed"
        PasswordComplexityStatus                = "Not Assessed"
        PasswordHistoryStatus                   = "Not Assessed"
        PasswordMinLifetimeStatus               = "Not Assessed"
        PasswordMaxLifetime365Status            = "Not Assessed"
        MeetsPCI90DayValue                      = "Not Assessed"
        OverallPasswordPolicyStatus             = "Not Assessed"
        ComplianceFindings                      = "Cluster security configuration was not returned"
    }
}

function New-SecurityRow {
    param(
        [string]$Cluster,
        $SecurityConfig
    )

    $rowData = [ordered]@{
        Cluster                                 = $Cluster
        PasswordMinLength                       = Value-OrNA $SecurityConfig.passwordStrength.minLength
        PasswordIncludeUpperLetter              = Value-OrNA $SecurityConfig.passwordStrength.includeUpperLetter
        PasswordIncludeLowerLetter              = Value-OrNA $SecurityConfig.passwordStrength.includeLowerLetter
        PasswordIncludeNumber                   = Value-OrNA $SecurityConfig.passwordStrength.includeNumber
        PasswordIncludeSpecialChar              = Value-OrNA $SecurityConfig.passwordStrength.includeSpecialChar
        NumDisallowedOldPasswords               = Value-OrNA $SecurityConfig.passwordReuse.numDisallowedOldPasswords
        NumDifferentChars                       = Value-OrNA $SecurityConfig.passwordReuse.numDifferentChars
        PasswordMinLifetimeDays                 = Value-OrNA $SecurityConfig.passwordLifetime.minLifetimeDays
        PasswordMaxLifetimeDays                 = Value-OrNA $SecurityConfig.passwordLifetime.maxLifetimeDays
        MaxFailedLoginAttempts                  = Value-OrNA $SecurityConfig.accountLockout.maxFailedLoginAttempts
        FailedLoginLockTimeDurationMins         = Value-OrNA $SecurityConfig.accountLockout.failedLoginLockTimeDurationMins
        AccountInactivityTimeDays               = Value-OrNA $SecurityConfig.accountLockout.inactivityTimeDays
        AuthTokenTimeoutMinutes                 = Value-OrNA $SecurityConfig.authTokenTimeoutMinutes
        UIInactivityTimeoutMSecs                = Value-OrNA $SecurityConfig.inactivityTimeoutMSecs
        SessionManagementEnabled                = Value-OrNA $SecurityConfig.sessionManagementEnabled
        SessionAbsoluteTimeoutSeconds           = Value-OrNA $SecurityConfig.sessionConfiguration.absoluteTimeout
        SessionInactivityTimeoutSeconds         = Value-OrNA $SecurityConfig.sessionConfiguration.inactivityTimeout
        LimitSessions                           = Value-OrNA $SecurityConfig.sessionConfiguration.limitSessions
        SessionLimitPerUser                     = Value-OrNA $SecurityConfig.sessionConfiguration.sessionLimitPerUser
        SessionLimitSystemWide                  = Value-OrNA $SecurityConfig.sessionConfiguration.sessionLimitSystemWide
        CertificateMappingAuthenticationEnabled = Value-OrNA $SecurityConfig.certificateBasedAuth.enableMappingBasedAuthentication
        CertificateMapping                     = Value-OrNA $SecurityConfig.certificateBasedAuth.certificateMapping
        CertificateADMapping                   = Value-OrNA $SecurityConfig.certificateBasedAuth.adMapping
        IsDataClassified                        = Value-OrNA $SecurityConfig.dataClassification.isDataClassified
        ClassifiedDataMessage                  = Value-OrNA $SecurityConfig.dataClassification.classifiedDataMessage
        UnclassifiedDataMessage                = Value-OrNA $SecurityConfig.dataClassification.unclassifiedDataMessage
        SSHTimeoutInMins                        = Value-OrNA $SecurityConfig.sshConfiguration.sshTimeoutInMins
    }

    $assessment = Get-PasswordPolicyAssessment ([pscustomobject]$rowData)

    foreach ($property in @($assessment.PSObject.Properties)) {
        $rowData[$property.Name] = $property.Value
    }

    # $rowData is already an ordered dictionary. Cast it directly.
    return [pscustomobject]$rowData
}

Write-Host "====================================================" -ForegroundColor Cyan
Write-Host "   COHESITY CLUSTER SECURITY CONFIGURATION INVENTORY" -ForegroundColor White
Write-Host "====================================================" -ForegroundColor Cyan

try {
    $clusterJson = Get-Json `
        "$baseUrl/v2/mcm/cluster-mgmt/info" `
        (New-Headers)
}
catch {
    throw "Failed to query Helios clusters: $($_.Exception.Message)"
}

$clusters = @()
if ($clusterJson.cohesityClusters) {
    $clusters = @($clusterJson.cohesityClusters)
}
elseif ($clusterJson.clusters) {
    $clusters = @($clusterJson.clusters)
}
elseif ($clusterJson.clusterInfos) {
    $clusters = @($clusterJson.clusterInfos)
}
elseif ($clusterJson.mcmInfo.clusterInfos) {
    $clusters = @($clusterJson.mcmInfo.clusterInfos)
}

if ($clusters.Count -eq 0) {
    throw "No clusters returned from Helios."
}

$clusters = @(
    $clusters |
        Sort-Object {
            First-Property $_ @("clusterName", "displayName", "name")
        }
)

$rows = @()
$issues = @()
$successfulClusters = 0

foreach ($cluster in $clusters) {
    $clusterName = First-Property $cluster @(
        "clusterName",
        "displayName",
        "name"
    )
    $clusterId = First-Property $cluster @("clusterId", "id")

    if ($clusterName -eq "N/A") {
        $clusterName = "Unknown"
    }

    if ($clusterId -eq "N/A") {
        $rows += New-EmptyRow $clusterName
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = "Cluster ID missing"
        }
        continue
    }

    Write-Host "Processing cluster: $clusterName" -ForegroundColor Yellow

    try {
        $securityConfig = Get-Json `
            "$baseUrl/v2/security-config" `
            (New-Headers ([string]$clusterId))
    }
    catch {
        $rows += New-EmptyRow $clusterName
        $issues += [pscustomobject]@{
            Cluster = $clusterName
            Issue   = $_.Exception.Message
        }
        continue
    }

    $rows += New-SecurityRow $clusterName $securityConfig
    $successfulClusters++
}

$rows = @($rows | Sort-Object Cluster)

Write-Host "`nPASSWORD STANDARD - ACTUAL VALUES" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        PasswordMinLength,
        PasswordComplexityEnabledCount,
        NumDisallowedOldPasswords,
        PasswordMinLifetimeDays,
        PasswordMaxLifetimeDays,
        OverallPasswordPolicyStatus |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nPASSWORD STANDARD - COMPLIANCE" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        PasswordLengthStatus,
        PasswordComplexityStatus,
        PasswordHistoryStatus,
        PasswordMinLifetimeStatus,
        PasswordMaxLifetime365Status,
        MeetsPCI90DayValue |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nPASSWORD COMPLIANCE FINDINGS" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        OverallPasswordPolicyStatus,
        ComplianceFindings |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nPASSWORD STRENGTH" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        PasswordMinLength,
        PasswordIncludeUpperLetter,
        PasswordIncludeLowerLetter,
        PasswordIncludeNumber,
        PasswordIncludeSpecialChar |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nPASSWORD REUSE AND LIFETIME" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        NumDisallowedOldPasswords,
        NumDifferentChars,
        PasswordMinLifetimeDays,
        PasswordMaxLifetimeDays |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nACCOUNT LOCKOUT AND GENERAL TIMEOUTS" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        MaxFailedLoginAttempts,
        FailedLoginLockTimeDurationMins,
        AccountInactivityTimeDays,
        AuthTokenTimeoutMinutes,
        UIInactivityTimeoutMSecs,
        SSHTimeoutInMins |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nSESSION MANAGEMENT" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        SessionManagementEnabled,
        SessionAbsoluteTimeoutSeconds,
        SessionInactivityTimeoutSeconds,
        LimitSessions,
        SessionLimitPerUser,
        SessionLimitSystemWide |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nCERTIFICATE AUTHENTICATION" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        CertificateMappingAuthenticationEnabled,
        CertificateMapping,
        CertificateADMapping |
    Format-Table -AutoSize -Wrap |
    Out-Host

Write-Host "`nDATA CLASSIFICATION" -ForegroundColor Cyan
$rows |
    Select-Object `
        Cluster,
        IsDataClassified,
        ClassifiedDataMessage,
        UnclassifiedDataMessage |
    Format-Table -AutoSize -Wrap |
    Out-Host

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$csvPath = Join-Path `
    $logDirectory `
    "Cohesity_Security_Configuration_$timestamp.csv"

$rows | Export-Csv `
    -Path $csvPath `
    -NoTypeInformation `
    -Encoding UTF8

$compliantClusters = @(
    $rows | Where-Object {
        $_.OverallPasswordPolicyStatus -eq "Compliant"
    }
).Count
$nonCompliantClusters = @(
    $rows | Where-Object {
        $_.OverallPasswordPolicyStatus -eq "Non-Compliant"
    }
).Count
$notAssessedClusters = @(
    $rows | Where-Object {
        $_.OverallPasswordPolicyStatus -eq "Not Assessed"
    }
).Count
$pci90ValueMetClusters = @(
    $rows | Where-Object {
        $_.MeetsPCI90DayValue -eq "Yes"
    }
).Count

Write-Host "`n====================================" -ForegroundColor Cyan
Write-Host "SECURITY CONFIGURATION SUMMARY" -ForegroundColor White
Write-Host "====================================" -ForegroundColor Cyan
Write-Host "Clusters discovered          : $($clusters.Count)"
Write-Host "Clusters successfully read   : $successfulClusters"
Write-Host "Password policy compliant    : $compliantClusters"
Write-Host "Password policy non-compliant: $nonCompliantClusters"
Write-Host "Password policy not assessed : $notAssessedClusters"
Write-Host "Meets PCI 90-day value       : $pci90ValueMetClusters"
Write-Host "Cluster fetch issues         : $($issues.Count)"
Write-Host "Rows displayed/exported      : $($rows.Count)"
Write-Host "CSV output                   : $csvPath"

Write-Host `
    "PCI note: MeetsPCI90DayValue checks only whether maxLifetimeDays is 1-90. PCI scope and MFA usage are not exposed by /v2/security-config." `
    -ForegroundColor Yellow

if ($issues.Count -gt 0) {
    $issues | Format-Table -AutoSize -Wrap | Out-Host
}

Write-Host "Processing complete." -ForegroundColor Green
