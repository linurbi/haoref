# recompute_incidents.ps1
# Recomputes incident_id for ALL rows in tg_alerts using the updated gap + max-duration thresholds.
# Safe to run any time — only writes new incident_id values, does not insert/delete rows.

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
# Gap: max seconds between consecutive events of same type to stay in one incident
$INCIDENT_GAP = @{
    rockets      = 90    # 90 sec  — salvo window
    pre_alert    = 120   # 2 min
    ballistic    = 300   # 5 min
    uav          = 120   # 2 min   — consecutive cities along a drone path
    earthquake   = 600
    infiltration = 300
    hazmat       = 600
    wildfire     = 600
    tsunami      = 600
}
$INCIDENT_GAP_DEFAULT = 90

# MaxDuration: hard cap on total incident length.
# Critical for קו העימות: ~15s inter-message gaps can otherwise chain alerts 8+ minutes apart.
$INCIDENT_MAX_DURATION = @{
    rockets      = 180   # 3 min
    pre_alert    = 180
    ballistic    = 300   # 5 min
    uav          = 240   # 4 min  — a drone pass through the confrontation line
    earthquake   = 900
    infiltration = 600
    hazmat       = 900
    wildfire     = 1800
    tsunami      = 900
}
$INCIDENT_MAX_DURATION_DEFAULT = 180

# ── Fetch all msg_id rows ─────────────────────────────────────────────────────
Write-Host "Fetching all siren events from D1..."
$res     = Invoke-D1Query "SELECT msg_id, MIN(alert_ts) AS first_ts, alert_type FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown') GROUP BY msg_id ORDER BY alert_type, first_ts"
$msgRows = @($res[0].results) | Sort-Object alert_type, first_ts
Write-Host "  $($msgRows.Count) siren events to re-cluster..."

# ── Cluster ───────────────────────────────────────────────────────────────────
$assignments = @{}
$lastTs      = @{}
$incStart    = @{}
$incStartDt  = @{}

foreach ($row in $msgRows) {
    $at     = $row.alert_type
    $ts     = [datetime]$row.first_ts
    $gap    = if ($INCIDENT_GAP.ContainsKey($at))          { $INCIDENT_GAP[$at] }          else { $INCIDENT_GAP_DEFAULT }
    $maxDur = if ($INCIDENT_MAX_DURATION.ContainsKey($at)) { $INCIDENT_MAX_DURATION[$at] } else { $INCIDENT_MAX_DURATION_DEFAULT }

    $newIncident = (
        -not $lastTs.ContainsKey($at) -or
        ($ts - $lastTs[$at]).TotalSeconds       -gt $gap -or
        ($incStartDt.ContainsKey($at) -and ($ts - $incStartDt[$at]).TotalSeconds -gt $maxDur)
    )

    if ($newIncident) {
        $incStart[$at]   = $row.first_ts
        $incStartDt[$at] = $ts
    }
    $lastTs[$at] = $ts
    $assignments[$row.msg_id] = "$at|$($incStart[$at])"
}

$uniqueIncidents = ($assignments.Values | Sort-Object -Unique).Count
Write-Host "  → $uniqueIncidents unique incidents (was $(($msgRows | Group-Object { $_.alert_type } | Measure-Object).Count) groups)"

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
Write-Host "Incidents per type (new clustering):"
$summary | ForEach-Object { Write-Host ("  {0,-14} {1,4} incidents  ({2} events)" -f $_.alert_type, $_.incidents, $_.events) }
