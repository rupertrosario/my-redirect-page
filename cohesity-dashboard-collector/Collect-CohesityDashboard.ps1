[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config.psd1'),
    [string]$OutputPath = (Join-Path $PSScriptRoot 'output/dashboard.json'),
    [string]$FixtureDirectory
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
. (Join-Path $PSScriptRoot 'modules/Common.ps1')
. (Join-Path $PSScriptRoot 'modules/Get-HeliosSession.ps1')
. (Join-Path $PSScriptRoot 'modules/Get-HeliosData.ps1')
. (Join-Path $PSScriptRoot 'modules/ConvertTo-DashboardModel.ps1')

if (-not (Test-Path $ConfigPath)) { throw "Config not found: $ConfigPath. Copy config.example.psd1 to config.psd1 first." }
$config = Import-PowerShellDataFile $ConfigPath
$headers = if ($FixtureDirectory) { @{} } else { Get-HeliosSession -Config $config }
$raw = Get-HeliosData -Config $config -Headers $headers -FixtureDirectory $FixtureDirectory
$model = ConvertTo-DashboardModel -Raw $raw -Config $config

$directory = Split-Path $OutputPath -Parent
if ($directory -and -not (Test-Path $directory)) { New-Item -ItemType Directory -Path $directory -Force | Out-Null }
$model | ConvertTo-Json -Depth 10 | Set-Content -Path $OutputPath -Encoding utf8
Write-Host "Dashboard data written to $OutputPath"
