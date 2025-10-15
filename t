$policyName = (
    Invoke-RestMethod -Uri "https://helios.cohesity.com/v2/data-protect/policies?ids=$($dbd.policyId)" `
                      -Headers $headers -Method Get -ErrorAction Stop
).policies | Where-Object { $_.id -eq $dbd.policyId } | Select-Object -ExpandProperty name -First 1


$username = (whoami -split '\\')[-1].ToLower()
if ($username -notmatch '^x_') { $username = "x_$username" }
$username
