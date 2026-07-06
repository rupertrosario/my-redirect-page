<#
Temporary one-cluster validation runner for Get-CohesityBackupFailureIncidentEvidence.ps1.
This does not change the source script. It creates a temporary filtered copy and runs only the requested cluster.
Use this only to validate baseline runtime and output.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$ClusterName,
    [switch]$ResetBaseline,
    [string]$IncidentNumber = ""
)

$ErrorActionPreference = "Stop"

$SourceScript = Join-Path $PSScriptRoot "Get-CohesityBackupFailureIncidentEvidence.ps1"
if (!(Test-Path $SourceScript)) { throw "Source script not found: $SourceScript" }

$Text = Get-Content $SourceScript -Raw

if ($Text -notmatch '\[string\]\$ClusterName') {
    $Text = $Text -replace '\[string\]\$IncidentNumber = "",\s*\r?\n\s*\[switch\]\$ResetBaseline', "[string]`$IncidentNumber = `"`",`r`n    [string]`$ClusterName = `"`",`r`n    [switch]`$ResetBaseline"
}

$Needle = '$Clusters = @($ClusterJson.cohesityClusters)'
$Replacement = @'
$Clusters = @($ClusterJson.cohesityClusters)
if ($ClusterName) {
    $Clusters = @($Clusters | Where-Object { (Get-ClusterDisplayName $_) -eq $ClusterName -or [string](Get-Value $_ "clusterId" "") -eq $ClusterName })
    if ($Clusters.Count -eq 0) { throw "Cluster not found: $ClusterName" }
    Write-Host ("One-cluster validation mode: {0}" -f (Get-ClusterDisplayName $Clusters[0])) -ForegroundColor Cyan
}
'@

if ($Text -notmatch 'One-cluster validation mode') {
    if ($Text -notlike "*$Needle*") { throw "Could not find cluster assignment line in source script." }
    $Text = $Text.Replace($Needle, $Replacement)
}

$TempScript = Join-Path $env:TEMP "Get-CohesityBackupFailureIncidentEvidence_OneCluster.ps1"
Set-Content -Path $TempScript -Value $Text -Encoding UTF8

$InvokeArgs = @("-ClusterName", $ClusterName)
if ($ResetBaseline) { $InvokeArgs += "-ResetBaseline" }
if ($IncidentNumber) { $InvokeArgs += @("-IncidentNumber", $IncidentNumber) }

& $TempScript @InvokeArgs
