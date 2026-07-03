# Cohesity physical inventory
param(
  [string]$HeliosUrl='https://helios.cohesity.com',
  [string]$KeyFilePath=(Join-Path 'X:\PowerShell\Cohesity_API_Scripts\DO_NOT_Delete' ('api' + 'key.txt'))
)
$k=(Get-Content $KeyFilePath -Raw).Trim()
$h=@{accept='application/json'}
$h[('api'+'Key')]=$k
Write-Host $HeliosUrl
