# Cohesity Cluster Security Configuration Inventory
# Multi-cluster | Helios | GET-only | PowerShell 5.1 compatible

$ErrorActionPreference = "Stop"
$FormatEnumerationLimit = -1
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$baseUrl             = "https://helios.cohesity.com"
$logDirectory        = "X:\PowerShell\Data\Cohesity\SecurityConfiguration"
$helperPath          = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedApiKeyPath = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

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

function As-Array {
    param($Value)

    if ($null -eq $Value) {
        return @()
    }

    return @($Value)
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

function New-EmptyRow {
    param([string]$Cluster)

    [pscustomobject][ordered]@{
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
    }
}

function New-SecurityRow {
    param(
        [string]$Cluster,
        $SecurityConfig
    )

    [pscustomobject][ordered]@{
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

Write-Host "`n====================================" -ForegroundColor Cyan
Write-Host "SECURITY CONFIGURATION SUMMARY" -ForegroundColor White
Write-Host "====================================" -ForegroundColor Cyan
Write-Host "Clusters discovered       : $($clusters.Count)"
Write-Host "Clusters successfully read: $successfulClusters"
Write-Host "Cluster fetch issues      : $($issues.Count)"
Write-Host "Rows displayed/exported   : $($rows.Count)"
Write-Host "CSV output                : $csvPath"

if ($issues.Count -gt 0) {
    $issues | Format-Table -AutoSize -Wrap | Out-Host
}

Write-Host "Processing complete." -ForegroundColor Green
