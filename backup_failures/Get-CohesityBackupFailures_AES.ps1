# =====================================================================
# Cohesity Backup Failures - AES API Key Runner
# READ-ONLY / GET-only
#
# Purpose:
# - Runs Get-CohesityBackupFailures.ps1 with the AES .enc API-key block.
# - Does not change the backup-failure logic.
# - Does not write the API key to disk.
# =====================================================================

$ErrorActionPreference = "Stop"

$realScript = Join-Path $PSScriptRoot "Get-CohesityBackupFailures.ps1"

if (-not (Test-Path $realScript)) {
    throw "Real backup failure script not found at $realScript"
}

$oldApiBlock = @'
$apikeypath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"

if (-not (Test-Path $apikeypath)) {
    throw "API key file not found at $apikeypath"
}

$apiKeyFileText = (Get-Content -Path $apikeypath -Raw).Trim()

try {
    $secureApiKey = $apiKeyFileText | ConvertTo-SecureString -ErrorAction Stop
    $apiKey = [System.Net.NetworkCredential]::new("", $secureApiKey).Password
} catch {
    $apiKey = $apiKeyFileText
}
'@

$newApiBlock = @'
$keyCheckPath = "X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete\apikey.txt"
$helperPath = "X:\PowerShell\Cohesity_API_Scripts\Common\ApiKeyAesHelper.ps1"
$encryptedFile = "X:\PowerShell\Cohesity_API_Scripts\Common\Secure\cohesity_apikey.enc"

if (-not (Test-Path $keyCheckPath)) {
    throw "API key check file not found at $keyCheckPath"
}

if (-not (Test-Path $helperPath)) {
    throw "API key helper file not found at $helperPath"
}

if (-not (Test-Path $encryptedFile)) {
    throw "Encrypted API key file not found at $encryptedFile"
}

. $helperPath
$apiKey = Get-CohesityApiKeyFromAes -EncryptedFile $encryptedFile
'@

$scriptText = Get-Content -Path $realScript -Raw

if (-not $scriptText.Contains($oldApiBlock)) {
    throw "Expected plain-text API-key block was not found in $realScript. No logic was executed."
}

$patchedScriptText = $scriptText.Replace($oldApiBlock, $newApiBlock)

Write-Host "Running real backup-failure script with AES API-key block..." -ForegroundColor Cyan
Write-Host "Source script: $realScript" -ForegroundColor Gray

$scriptBlock = [ScriptBlock]::Create($patchedScriptText)
& $scriptBlock
