[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$OutputPath,
    [string]$StatusPath,
    [string]$FixtureDirectory
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# Resolve defaults only after parameter binding so Join-Path never receives an
# empty $PSScriptRoot when the script is launched from Explorer/Windows PowerShell.
$scriptRoot = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($scriptRoot) -and $MyInvocation.MyCommand.Path) {
    $scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($scriptRoot)) {
    throw 'Unable to determine the collector folder.'
}
$scriptRoot = [IO.Path]::GetFullPath($scriptRoot)
if ([string]::IsNullOrWhiteSpace($ConfigPath)) { $ConfigPath = Join-Path $scriptRoot 'config.psd1' }
elseif (-not [IO.Path]::IsPathRooted($ConfigPath)) { $ConfigPath = Join-Path $scriptRoot $ConfigPath }
if ([string]::IsNullOrWhiteSpace($OutputPath)) { $OutputPath = Join-Path $scriptRoot 'output\dashboard.json' }
elseif (-not [IO.Path]::IsPathRooted($OutputPath)) { $OutputPath = Join-Path $scriptRoot $OutputPath }
if ([string]::IsNullOrWhiteSpace($StatusPath)) { $StatusPath = Join-Path $scriptRoot 'output\refresh-status.json' }
elseif (-not [IO.Path]::IsPathRooted($StatusPath)) { $StatusPath = Join-Path $scriptRoot $StatusPath }

$ConfigPath = [IO.Path]::GetFullPath($ConfigPath)
$OutputPath = [IO.Path]::GetFullPath($OutputPath)
$StatusPath = [IO.Path]::GetFullPath($StatusPath)

$moduleFiles = @(
    'Common.ps1',
    'Get-HeliosSession.ps1',
    'Get-ClusterSnapshot.ps1',
    'Get-HeliosData.ps1',
    'ConvertTo-DashboardModel.ps1'
)
foreach ($moduleFile in $moduleFiles) {
    $modulePath = Join-Path (Join-Path $scriptRoot 'modules') $moduleFile
    if (-not (Test-Path -LiteralPath $modulePath -PathType Leaf)) {
        throw "Required module not found: $modulePath. Copy the complete cohesity-dashboard-collector folder."
    }
    . $modulePath
}

function Write-JsonAtomic($Value,[string]$Path,[int]$Depth=100) {
    if ([string]::IsNullOrWhiteSpace($Path)) { throw 'JSON output path is empty.' }
    $directory = Split-Path $Path -Parent
    if ([string]::IsNullOrWhiteSpace($directory)) { throw "JSON output folder is empty for path: $Path" }
    if (-not (Test-Path -LiteralPath $directory -PathType Container)) { New-Item -ItemType Directory -Path $directory -Force | Out-Null }
    $temp = "$Path.tmp"
    $Value | ConvertTo-Json -Depth $Depth | Set-Content -LiteralPath $temp -Encoding UTF8
    Move-Item -LiteralPath $temp -Destination $Path -Force
}

if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw "Config not found: $ConfigPath. Run Run-CohesityDashboard.ps1; it creates config.psd1 automatically."
}
$started = [datetime]::UtcNow
Write-JsonAtomic ([ordered]@{state='Running';startedAtUtc=$started.ToString('o');completedAtUtc=$null;message='Collecting cluster data';pid=$PID}) $StatusPath

try {
    $config = Import-PowerShellDataFile -LiteralPath $ConfigPath
    foreach ($requiredSetting in @('HeliosBaseUrl','ApiKeyHelperPath','EncryptedApiKeyPath')) {
        if ([string]::IsNullOrWhiteSpace([string]$config[$requiredSetting])) {
            throw "Required config setting is empty: $requiredSetting"
        }
    }
    $previous = if(Test-Path -LiteralPath $OutputPath -PathType Leaf){ try{Get-Content -LiteralPath $OutputPath -Raw | ConvertFrom-Json}catch{$null} }else{$null}
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
