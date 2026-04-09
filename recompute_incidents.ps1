# recompute_incidents.ps1
# Rewrites incident_id for ALL rows using region-aware thresholds.
# No Hebrew literals in source — constructed via Unicode code points to avoid encoding issues.
#
# tier n = confrontation-line north (15s shelter / UAV go-immediately)
# tier o = rest of Israel (90s shelter)

$CF_ACCOUNT_ID  = if ($env:CF_ACCOUNT_ID)  { $env:CF_ACCOUNT_ID }  else { "913dd7d67a19b98eb74cab6d8e8e0b4a" }
$CF_API_TOKEN   = if ($env:CF_API_TOKEN)   { $env:CF_API_TOKEN }   else { "cfat_vec9LvsRcJtkxrx5p15DkZbWiMdW53U9jp2bj9b8e52ce9e2" }
$D1_DATABASE_ID = if ($env:D1_DATABASE_ID) { $env:D1_DATABASE_ID } else { "ac645c9a-e7cc-4eb1-a0b1-17fe4cc437e5" }
$D1_URL = "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT_ID/d1/database/$D1_DATABASE_ID"
$CF_HEADERS = @{ Authorization = "Bearer $CF_API_TOKEN"; "Content-Type" = "application/json" }

function Invoke-D1Query([string]$sql) {
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes((@{ sql = $sql } | ConvertTo-Json -Depth 3))
    $resp = Invoke-RestMethod -Uri "$D1_URL/query" -Method Post -Headers $CF_HEADERS -Body $bodyBytes -ContentType "application/json; charset=utf-8"
    if (-not $resp.success) { throw ("D1 error: " + ($resp.errors | ConvertTo-Json)) }
    return $resp.result
}

# "kav ha-imut" = region string for the confrontation line, built from Unicode codepoints
$KAV = -join [char[]](0x05E7,0x05D5,0x20,0x05D4,0x05E2,0x05D9,0x05DE,0x05D5,0x05EA)

# Thresholds: n = north (15s), o = other (90s)
$GAP = @{
    rockets      = @{ n=15;  o=90  }
    pre_alert    = @{ n=15;  o=120 }
    ballistic    = @{ n=15;  o=90  }
    uav          = @{ n=5;   o=60  }
    earthquake   = @{ n=600; o=600 }
    infiltration = @{ n=15;  o=300 }
    hazmat       = @{ n=600; o=600 }
    wildfire     = @{ n=600; o=600 }
    tsunami      = @{ n=600; o=600 }
}
$GAP_DEFAULT = @{ n=15; o=90 }

$MAX = @{
    rockets      = @{ n=25;  o=180 }
    pre_alert    = @{ n=25;  o=180 }
    ballistic    = @{ n=25;  o=300 }
    uav          = @{ n=10;  o=240 }
    earthquake   = @{ n=900; o=900 }
    infiltration = @{ n=30;  o=600 }
    hazmat       = @{ n=900; o=900 }
    wildfire     = @{ n=1800;o=1800}
    tsunami      = @{ n=900; o=900 }
}
$MAX_DEFAULT = @{ n=25; o=180 }

# Step 1a: find north msg_ids via a simple LIKE query
$likePattern = "%" + $KAV + "%"
$northSql = "SELECT DISTINCT msg_id FROM tg_alerts WHERE region LIKE '" + $likePattern + "'"
$northRes = Invoke-D1Query $northSql
$northSet = [System.Collections.Generic.HashSet[long]]::new()
@($northRes[0].results) | ForEach-Object { [void]$northSet.Add([long]$_.msg_id) }
Write-Host ("North msg_ids: " + $northSet.Count)

# Step 1b: fetch all siren events
$allRes  = Invoke-D1Query "SELECT msg_id, MIN(alert_ts) AS first_ts, alert_type FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown') GROUP BY msg_id ORDER BY alert_type, first_ts"
$rows    = @($allRes[0].results) | Sort-Object alert_type, first_ts
Write-Host ("Events to cluster: " + $rows.Count)

# Step 2: cluster
$asgn    = @{}
$lastTs  = @{}
$incSt   = @{}
$incStDt = @{}

foreach ($row in $rows) {
    $at   = $row.alert_type
    $ts   = [datetime]$row.first_ts
    $tier = if ($northSet.Contains([long]$row.msg_id)) { "n" } else { "o" }
    $key  = $at + "|" + $tier

    $g = if ($GAP.ContainsKey($at)) { $GAP[$at][$tier] } else { $GAP_DEFAULT[$tier] }
    $m = if ($MAX.ContainsKey($at)) { $MAX[$at][$tier] } else { $MAX_DEFAULT[$tier] }

    $newInc = (
        -not $lastTs.ContainsKey($key) -or
        ($ts - $lastTs[$key]).TotalSeconds -gt $g -or
        ($incStDt.ContainsKey($key) -and ($ts - $incStDt[$key]).TotalSeconds -gt $m)
    )

    if ($newInc) { $incSt[$key] = $row.first_ts; $incStDt[$key] = $ts }
    $lastTs[$key] = $ts
    $asgn[$row.msg_id] = $at + "|" + $tier + "|" + $incSt[$key]
}

$unique = ($asgn.Values | Sort-Object -Unique).Count
Write-Host ("Unique incidents: " + $unique)

# Step 3: batch UPDATE
$allIds = @($asgn.Keys)
[int]$total = $allIds.Count
[int]$chunk = 200
[int]$i = 0
[int]$done = 0

Write-Host ("Writing to D1 (" + $total + " rows in batches of " + $chunk + ")...")
while ($i -lt $total) {
    [int]$end  = [Math]::Min($i + $chunk - 1, $total - 1)
    $slice = $allIds[$i..$end]
    $cases = ($slice | ForEach-Object {
        $safeId = $asgn[$_] -replace "'", "''"
        "WHEN " + $_ + " THEN '" + $safeId + "'"
    }) -join " "
    $inList = $slice -join ","
    $sql = "UPDATE tg_alerts SET incident_id = CASE msg_id " + $cases + " ELSE incident_id END WHERE msg_id IN (" + $inList + ")"
    try   { Invoke-D1Query $sql | Out-Null }
    catch { Write-Warning ("Batch " + $i + " failed: " + $_) }
    $done += $slice.Count
    if ($done % 1000 -eq 0 -or $done -eq $total) { Write-Host ("  " + $done + " / " + $total) }
    $i += $chunk
    Start-Sleep -Milliseconds 120
}

Write-Host "Done."

# Summary
$sum = (Invoke-D1Query "SELECT alert_type, COUNT(DISTINCT incident_id) AS inc, COUNT(DISTINCT msg_id) AS ev FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown') GROUP BY alert_type ORDER BY inc DESC")[0].results
Write-Host "Result:"
$sum | ForEach-Object { Write-Host ("  " + $_.alert_type + ": " + $_.inc + " incidents (" + $_.ev + " events)") }

# Spot-check: last 5 north rocket msg_ids -- each should have a different incident_id
$spotSql = "SELECT msg_id, alert_ts, incident_id FROM tg_alerts WHERE region LIKE '" + $likePattern + "' AND alert_type='rockets' GROUP BY msg_id ORDER BY alert_ts DESC LIMIT 5"
$spot = (Invoke-D1Query $spotSql)[0].results
Write-Host "Spot-check last 5 north rocket msgs:"
$spot | ForEach-Object { Write-Host ("  msg=" + $_.msg_id + "  ts=" + $_.alert_ts + "  inc=" + $_.incident_id) }