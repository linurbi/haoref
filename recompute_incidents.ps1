# recompute_incidents.ps1
# Recomputes incident_id for ALL rows in tg_alerts using region-aware thresholds.
# Safe to run any time — only writes new incident_id values, does not insert/delete rows.
#
# tier 'n' = אזור קו העימות  (15-second shelter time)
# tier 'o' = rest of Israel  (90-second shelter time)
# UAV       = "go immediately" — no fixed time, tightest window

$CF_ACCOUNT_ID  = if ($env:CF_ACCOUNT_ID)  { $env:CF_ACCOUNT_ID }  else { "913dd7d67a19b98eb74cab6d8e8e0b4a" }
$CF_API_TOKEN   = if ($env:CF_API_TOKEN)   { $env:CF_API_TOKEN }   else { "cfat_vec9LvsRcJtkxrx5p15DkZbWiMdW53U9jp2bj9b8e52ce9e2" }
$D1_DATABASE_ID = if ($env:D1_DATABASE_ID) { $env:D1_DATABASE_ID } else { "ac645c9a-e7cc-4eb1-a0b1-17fe4cc437e5" }
$D1_URL = "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT_ID/d1/database/$D1_DATABASE_ID"
$CF_HEADERS = @{ Authorization = "Bearer $CF_API_TOKEN"; "Content-Type" = "application/json" }

function Invoke-D1Query([string]$sql) {
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes((@{ sql = $sql } | ConvertTo-Json -Depth 3))
    $resp = Invoke-RestMethod -Uri "$D1_URL/query" -Method Post `
        -Headers $CF_HEADERS -Body $bodyBytes -ContentType "application/json; charset=utf-8"
    if (-not $resp.success) { throw "D1 error: $($resp.errors | ConvertTo-Json)" }
    return $resp.result
}

# ── Thresholds ────────────────────────────────────────────────────────────────
$INCIDENT_GAP = @{
    rockets      = @{ n=20;  o=90  }
    pre_alert    = @{ n=20;  o=120 }
    ballistic    = @{ n=20;  o=300 }
    uav          = @{ n=15;  o=60  }
    earthquake   = @{ n=600; o=600 }
    infiltration = @{ n=30;  o=300 }
    hazmat       = @{ n=600; o=600 }
    wildfire     = @{ n=600; o=600 }
    tsunami      = @{ n=600; o=600 }
}
$INCIDENT_GAP_DEFAULT = @{ n=20; o=90 }

$INCIDENT_MAX_DURATION = @{
    rockets      = @{ n=45;  o=180 }
    pre_alert    = @{ n=60;  o=180 }
    ballistic    = @{ n=60;  o=300 }
    uav          = @{ n=90;  o=240 }
    earthquake   = @{ n=900; o=900 }
    infiltration = @{ n=120; o=600 }
    hazmat       = @{ n=900; o=900 }
    wildfire     = @{ n=1800;o=1800}
    tsunami      = @{ n=900; o=900 }
}
$INCIDENT_MAX_DURATION_DEFAULT = @{ n=45; o=180 }

# ── Fetch all msg_id rows with north flag ─────────────────────────────────────
Write-Host "Fetching all siren events from D1..."
$sql = @"
SELECT msg_id, MIN(alert_ts) AS first_ts, alert_type,
       MAX(CASE WHEN region = 'אזור קו העימות' THEN 1 ELSE 0 END) AS is_north
FROM tg_alerts
WHERE alert_type NOT IN ('all_clear','unknown')
GROUP BY msg_id
ORDER BY alert_type, first_ts
"@
$res     = Invoke-D1Query $sql
$msgRows = @($res[0].results) | Sort-Object alert_type, first_ts
Write-Host "  $($msgRows.Count) siren events to re-cluster..."

# ── Cluster ───────────────────────────────────────────────────────────────────
$assignments = @{}
$lastTs      = @{}
$incStart    = @{}
$incStartDt  = @{}

foreach ($row in $msgRows) {
    $at   = $row.alert_type
    $ts   = [datetime]$row.first_ts
    $tier = if ($row.is_north -eq 1) { 'n' } else { 'o' }
    $key  = "$at|$tier"

    $gapMap = if ($INCIDENT_GAP.ContainsKey($at))          { $INCIDENT_GAP[$at] }          else { $INCIDENT_GAP_DEFAULT }
    $durMap = if ($INCIDENT_MAX_DURATION.ContainsKey($at)) { $INCIDENT_MAX_DURATION[$at] } else { $INCIDENT_MAX_DURATION_DEFAULT }
    $gap    = $gapMap[$tier]
    $maxDur = $durMap[$tier]

    $newIncident = (
        -not $lastTs.ContainsKey($key) -or
        ($ts - $lastTs[$key]).TotalSeconds       -gt $gap -or
        ($incStartDt.ContainsKey($key) -and ($ts - $incStartDt[$key]).TotalSeconds -gt $maxDur)
    )

    if ($newIncident) {
        $incStart[$key]   = $row.first_ts
        $incStartDt[$key] = $ts
    }
    $lastTs[$key] = $ts
    $assignments[$row.msg_id] = "$at|$tier|$($incStart[$key])"
}

$uniqueIncidents = ($assignments.Values | Sort-Object -Unique).Count
Write-Host "  → $uniqueIncidents unique incidents"

# ── Batch UPDATE ──────────────────────────────────────────────────────────────
$allMsgIds = @($assignments.Keys)
[int]$total = $allMsgIds.Count
[int]$chunk = 200
[int]$i     = 0
[int]$done  = 0

Write-Host "Writing incident IDs to D1 ($total msg_ids in batches of $chunk)..."
while ($i -lt $total) {
    [int]$end  = [Math]::Min($i + $chunk - 1, $total - 1)
    $slice  = $allMsgIds[$i..$end]
    $cases  = ($slice | ForEach-Object { "WHEN $_ THEN '$(($assignments[$_] -replace "'","''"))'" }) -join " "
    $inList = $slice -join ","
    $sql    = "UPDATE tg_alerts SET incident_id = CASE msg_id $cases ELSE incident_id END WHERE msg_id IN ($inList)"
    try   { Invoke-D1Query $sql | Out-Null }
    catch { Write-Warning "  Update failed at batch $i`: $_" }
    $done += $slice.Count
    if ($done % 1000 -eq 0 -or $done -eq $total) { Write-Host "  ...processed $done / $total" }
    $i += $chunk
    Start-Sleep -Milliseconds 120
}

Write-Host ""
Write-Host "Done. Incident IDs recomputed."
Write-Host ""

# ── Summary ───────────────────────────────────────────────────────────────────
$summary = (Invoke-D1Query "SELECT alert_type, COUNT(DISTINCT incident_id) AS incidents, COUNT(DISTINCT msg_id) AS events FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown') GROUP BY alert_type ORDER BY incidents DESC")[0].results
Write-Host "Incidents per type (region-aware clustering):"
$summary | ForEach-Object { Write-Host ("  {0,-14} {1,5} incidents  ({2} events)" -f $_.alert_type, $_.incidents, $_.events) }
