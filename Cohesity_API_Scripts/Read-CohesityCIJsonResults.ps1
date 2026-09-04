param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

# Read a Cohesity Siren / CI diagnostic JSON file and present .results in a human-readable table.
$json = Get-Content -Path $Path -Raw | ConvertFrom-Json

if (-not $json.results) {
    Write-Warning "No 'results' property was found in the JSON file."
    return
}

$json.results |
    Select-Object testNumber,
                  testName,
                  state,
                  severity,
                  module,
                  code,
                  @{Name='Details'; Expression={
                      if ($null -eq $_.data) {
                          ''
                      }
                      elseif ($_.data -is [string]) {
                          $_.data
                      }
                      else {
                          $_.data | ConvertTo-Json -Depth 10 -Compress
                      }
                  }} |
    Format-Table -AutoSize -Wrap
