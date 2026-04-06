# load_history.ps1 — Bulk-load RedAlert history into Cloudflare D1
# Fill in the four CONFIG values below, then run:
#   .\load_history.ps1

# ─── CONFIG ───────────────────────────────────────────────────────────────────
$REDALERT_KEY   = "YOUR_REDALERT_PRIVATE_KEY"
$CF_ACCOUNT_ID  = "YOUR_CLOUDFLARE_ACCOUNT_ID"
$CF_API_TOKEN   = "YOUR_CLOUDFLARE_API_TOKEN"   # needs D1 write permission
$D1_DATABASE_ID = "YOUR_D1_DATABASE_ID"
$OP_START       = "2026-02-28T00:00:00Z"
# ──────────────────────────────────────────────────────────────────────────────

$REDALERT_BASE = "https://redalert.orielhaim.com"
$D1_URL        = "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT_ID/d1/database/$D1_DATABASE_ID"

$RA_HEADERS = @{ Authorization = "Bearer $REDALERT_KEY"; Accept = "application/json" }
$CF_HEADERS = @{ Authorization = "Bearer $CF_API_TOKEN"; "Content-Type" = "application/json" }

# ── D1 helpers ────────────────────────────────────────────────────────────────

function Invoke-D1Query($sql, $params = $null) {
    $body = @{ sql = $sql }
    if ($params) { $body.params = $params }
    $resp = Invoke-RestMethod -Uri "$D1_URL/query" -Method Post `
        -Headers $CF_HEADERS -Body ($body | ConvertTo-Json -Depth 5) -ContentType "application/json"
    if (-not $resp.success) { throw "D1 error: $($resp.errors | ConvertTo-Json)" }
    return $resp.result
}

function Invoke-D1Batch($statements) {
    $body = @{ requests = $statements }
    $resp = Invoke-RestMethod -Uri "$D1_URL/batch" -Method Post `
        -Headers $CF_HEADERS -Body ($body | ConvertTo-Json -Depth 10) -ContentType "application/json"
    if (-not $resp.success) { throw "D1 batch error: $($resp.errors | ConvertTo-Json)" }
}

function Get-LastTimestamp {
    try {
        $res = Invoke-D1Query "SELECT MAX(ts) AS last_ts FROM alerts"
        $val = $res[0].results[0].last_ts
        if ($val) { return $val } else { return $OP_START }
    } catch { return $OP_START }
}

# ── Main load ─────────────────────────────────────────────────────────────────

$startDate = Get-LastTimestamp
Write-Host "Last record in D1: $startDate"
Write-Host "Starting load from $startDate ..."

$offset     = 0
$totalNew   = 0
$pageSize   = 1000
$batchSize  = 80    # D1 batch limit per request

do {
    Write-Host "  Fetching offset=$offset ..." -NoNewline

    $params = "startDate=$([Uri]::EscapeDataString($startDate))&limit=$pageSize&offset=$offset&order=asc"
    $page   = Invoke-RestMethod -Uri "$REDALERT_BASE/api/stats/history?$params" `
                -Headers $RA_HEADERS -Method Get

    $alerts     = $page.data
    $pagination = $page.pagination
    Write-Host " $($alerts.Count) alerts  (total=$($pagination.total))"

    if ($alerts.Count -eq 0) { break }

    # Build batch statements
    $stmts = @()
    foreach ($a in $alerts) {
        $stmts += @{
            sql    = "INSERT OR IGNORE INTO alerts (id, ts, type, origin) VALUES (?,?,?,?)"
            params = @($a.id, $a.timestamp, $a.type, $a.origin)
        }
        foreach ($c in $a.cities) {
            $stmts += @{
                sql    = "INSERT OR IGNORE INTO alert_cities (alert_id, city_id, city_name) VALUES (?,?,?)"
                params = @($a.id, $c.id, $c.name)
            }
        }
    }

    # Send in chunks of $batchSize
    for ($i = 0; $i -lt $stmts.Count; $i += $batchSize) {
        $chunk = $stmts[$i..([Math]::Min($i + $batchSize - 1, $stmts.Count - 1))]
        Invoke-D1Batch $chunk
        Start-Sleep -Milliseconds 150
    }

    $totalNew += $alerts.Count
    $offset   += $alerts.Count
    Start-Sleep -Milliseconds 500

} while ($pagination.hasMore)

Write-Host "`nInserted/skipped $totalNew alert records."

# ── Sync city→zone from summary ───────────────────────────────────────────────

Write-Host "`nSyncing city→zone mappings ..."
$summaryParams = "startDate=$([Uri]::EscapeDataString($OP_START))&include=topCities&topLimit=200"
$summary       = Invoke-RestMethod -Uri "$REDALERT_BASE/api/stats/summary?$summaryParams" `
                    -Headers $RA_HEADERS -Method Get

$synced = 0
foreach ($city in $summary.topCities) {
    if ($city.zone) {
        Invoke-D1Query `
            "INSERT OR REPLACE INTO city_zones (city_id, city_name, zone)
             SELECT ac.city_id, ?, ? FROM alert_cities ac WHERE ac.city_name = ? LIMIT 1" `
            @($city.city, $city.zone, $city.city) | Out-Null
        $synced++
    }
}
Write-Host "  Synced $synced city→zone mappings."

Write-Host "`nAll done!"
