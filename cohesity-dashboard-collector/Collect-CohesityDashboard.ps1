[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$OutputPath,
    [string]$StatusPath,
    [string]$FixtureDirectory
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$scriptRoot = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
    $scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
    throw 'Unable to resolve the cohesity-dashboard-collector folder.'
}
$scriptRoot = [IO.Path]::GetFullPath($scriptRoot)
if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $scriptRoot 'config.psd1'
}
if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $scriptRoot 'output/dashboard.json'
}
if ([string]::IsNullOrWhiteSpace($StatusPath)) {
    $StatusPath = Join-Path $scriptRoot 'output/refresh-status.json'
}

. (Join-Path $scriptRoot 'modules/Common.ps1')
. (Join-Path $scriptRoot 'modules/Get-HeliosSession.ps1')
. (Join-Path $scriptRoot 'modules/Get-ClusterSnapshot.ps1')
. (Join-Path $scriptRoot 'modules/Get-HeliosData.ps1')
. (Join-Path $scriptRoot 'modules/ConvertTo-DashboardModel.ps1')

function Write-JsonAtomic($Value,[string]$Path,[int]$Depth=100) {
    $directory = Split-Path $Path -Parent
    if ($directory -and -not (Test-Path $directory)) { New-Item -ItemType Directory -Path $directory -Force | Out-Null }
    $temp = "$Path.tmp"
    $Value | ConvertTo-Json -Depth $Depth | Set-Content -Path $temp -Encoding UTF8
    Move-Item -Path $temp -Destination $Path -Force
}

if (-not (Test-Path $ConfigPath)) { throw "Config not found: $ConfigPath. Copy config.example.psd1 to config.psd1 first." }
$started = [datetime]::UtcNow
Write-JsonAtomic ([ordered]@{state='Running';startedAtUtc=$started.ToString('o');completedAtUtc=$null;message='Collecting cluster data';pid=$PID}) $StatusPath

try {
    $config = Import-PowerShellDataFile $ConfigPath
    $previous = if(Test-Path $OutputPath){ try{Get-Content $OutputPath -Raw | ConvertFrom-Json}catch{$null} }else{$null}
    $headers = if ($FixtureDirectory) { @{} } else { Get-HeliosSession -Config $config }
    $raw = Get-HeliosData -Config $config -Headers $headers -FixtureDirectory $FixtureDirectory
    $model = ConvertTo-DashboardModel -Raw $raw -Config $config -PreviousModel $previous
    Write-JsonAtomic $model $OutputPath
    $claudePath = Join-Path (Split-Path $OutputPath -Parent) 'claude-context.json'
    $claudeContext = [ordered]@{
        schemaVersion=$model.schemaVersion; generatedAtUtc=$model.generatedAtUtc; summary=$model.summary
        clusters=@($model.clusters | ForEach-Object {
            [ordered]@{name=$_.name;availability=$_.availability;health=$_.health;capacityUsedPercent=$_.capacity.usedPercent;gcReclaimableBytes=$_.gcReclaimableBytes;protectionGroups=$_.protectionGroups;inventory=$_.inventory;openAlerts=$_.openAlerts;failureCount=@($_.failures).Count;hardwareAlertCount=@($_.hardwareAlerts).Count}
        })
        failures=$model.failures; hardwareAlerts=$model.hardwareAlerts
    }
    Write-JsonAtomic $claudeContext $claudePath
    $elapsed = [math]::Round(([datetime]::UtcNow-$started).TotalSeconds,1)
    Write-JsonAtomic ([ordered]@{state=$model.collectionStatus;startedAtUtc=$started.ToString('o');completedAtUtc=[datetime]::UtcNow.ToString('o');durationSeconds=$elapsed;message="Refresh completed in $elapsed seconds";pid=$PID}) $StatusPath
    Write-Host "Dashboard refresh complete in $elapsed seconds: $OutputPath" -ForegroundColor Green
} catch {
    Write-JsonAtomic ([ordered]@{state='Failed';startedAtUtc=$started.ToString('o');completedAtUtc=[datetime]::UtcNow.ToString('o');message=$_.Exception.Message;pid=$PID}) $StatusPath
    throw
}
