# load_history.ps1 — Bulk-load Siren (api.siren.co.il) history into Cloudflare D1
# Stores (alert_id, zone) pairs — deduped via city→zone lookup from topCities.
# Resulting tables:
#   alerts       — one row per alarm event  (id, ts, type, origin)
#   alert_zones  — one row per (event, zone) pair  (~3 rows per event)
#
# Run:  powershell -ExecutionPolicy Bypass -File .\load_history.ps1

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# Load local secrets if present (never committed — see secrets.local.ps1.example)
$_localSecrets = Join-Path $PSScriptRoot "secrets.local.ps1"
if (Test-Path $_localSecrets) { . $_localSecrets }

$CF_ACCOUNT_ID  = if ($env:CF_ACCOUNT_ID)  { $env:CF_ACCOUNT_ID }  else { "913dd7d67a19b98eb74cab6d8e8e0b4a" }
$D1_DATABASE_ID = if ($env:D1_DATABASE_ID) { $env:D1_DATABASE_ID } else { "ac645c9a-e7cc-4eb1-a0b1-17fe4cc437e5" }
$CF_API_TOKEN   = $env:CF_API_TOKEN
if (-not $CF_API_TOKEN)  { throw "CF_API_TOKEN env var not set. See secrets.local.ps1.example." }
$REDALERT_KEY   = $env:REDALERT_KEY
if (-not $REDALERT_KEY)  { throw "REDALERT_KEY env var not set. See secrets.local.ps1.example." }
$OP_START       = "2026-02-28T00:00:00Z"
# ──────────────────────────────────────────────────────────────────────────────

$REDALERT_BASE = "https://api.siren.co.il"
$D1_URL        = "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT_ID/d1/database/$D1_DATABASE_ID"

$RA_HEADERS = @{ Authorization = "Bearer $REDALERT_KEY"; Accept = "application/json" }
$CF_HEADERS = @{ Authorization = "Bearer $CF_API_TOKEN"; "Content-Type" = "application/json" }

# ── D1 helpers ────────────────────────────────────────────────────────────────

function Invoke-D1Query([string]$sql) {
    $body = @{ sql = $sql } | ConvertTo-Json -Depth 3
    $resp = Invoke-RestMethod -Uri "$D1_URL/query" -Method Post `
        -Headers $CF_HEADERS -Body $body -ContentType "application/json"
    if (-not $resp.success) { throw "D1 error: $($resp.errors | ConvertTo-Json)" }
    return $resp.result
}

function EscSql($v) {
    if ($null -eq $v)                                          { return "NULL" }
    if ($v -is [int] -or $v -is [long] -or $v -is [double])   { return "$v"   }
    return "'" + ($v.ToString() -replace "'", "''") + "'"
}

function Invoke-D1BulkInsert([string]$table, [string[]]$columns, $rows) {
    $rowArray = @($rows)
    [int]$total = $rowArray.Length
    if ($total -eq 0) { return }
    [int]$chunk = 200
    [int]$i = 0
    while ($i -lt $total) {
        [int]$end   = [Math]::Min($i + $chunk - 1, $total - 1)
        $slice = $rowArray[$i..$end]
        $vals  = ($slice | ForEach-Object {
            "(" + (($_ | ForEach-Object { EscSql $_ }) -join ",") + ")"
        }) -join ","
        $sql = "INSERT OR IGNORE INTO $table ($($columns -join ',')) VALUES $vals"
        try { Invoke-D1Query $sql | Out-Null }
        catch { Write-Warning "Insert failed [$table row $i]: $_" }
        $i += $chunk
        Start-Sleep -Milliseconds 120
    }
}

function Get-LastTimestamp {
    try {
        # If alert_zones is empty, we need a full pass regardless of alerts table
        $zoneCount = (Invoke-D1Query "SELECT COUNT(*) AS n FROM alert_zones")[0].results[0].n
        if ([int]$zoneCount -eq 0) { return $OP_START }

        $res = Invoke-D1Query "SELECT MAX(ts) AS last_ts FROM alerts"
        $val = $res[0].results[0].last_ts
        if ($val) { return $val } else { return $OP_START }
    } catch { return $OP_START }
}

# ── Build city→zone lookup — sliding weekly windows (API max = 50 per call) ──

Write-Host "Building city->zone lookup (weekly windows) ..."
$cityZone = @{}

# Generate weekly windows from OP_START to today
$winStart = [DateTime]::Parse($OP_START)
$winEnd   = [DateTime]::UtcNow

while ($winStart -lt $winEnd) {
    $winStop = $winStart.AddDays(7)
    if ($winStop -gt $winEnd) { $winStop = $winEnd }

    $s = $winStart.ToString("yyyy-MM-ddTHH:mm:ssZ")
    $e = $winStop.ToString("yyyy-MM-ddTHH:mm:ssZ")
    $uri = "$REDALERT_BASE/stats/summary?startDate=$([Uri]::EscapeDataString($s))&endDate=$([Uri]::EscapeDataString($e))&include=topCities&topLimit=50"

    try {
        $data = Invoke-RestMethod -Uri $uri -Headers $RA_HEADERS -Method Get
        $added = 0
        foreach ($c in $data.topCities) {
            if ($c.zone -and -not $cityZone.ContainsKey($c.city)) {
                $cityZone[$c.city] = $c.zone
                $added++
            }
        }
        Write-Host "  $s -> $e : +$added new mappings (total $($cityZone.Count))"
    } catch {
        Write-Warning "  Window $s failed: $_"
    }

    $winStart = $winStop
    Start-Sleep -Milliseconds 300
}

Write-Host "  Final lookup: $($cityZone.Count) city->zone mappings."

# ── Ensure schema exists ──────────────────────────────────────────────────────

Invoke-D1Query "CREATE TABLE IF NOT EXISTS alerts (id TEXT PRIMARY KEY, ts TEXT NOT NULL, type TEXT, origin TEXT)" | Out-Null
Invoke-D1Query "CREATE TABLE IF NOT EXISTS alert_zones (alert_id TEXT NOT NULL, zone TEXT NOT NULL, PRIMARY KEY (alert_id, zone))" | Out-Null

# ── Main load ─────────────────────────────────────────────────────────────────

$startDate = Get-LastTimestamp
Write-Host "Last record in D1: $startDate"
Write-Host "Starting load from $startDate ..."

$offset   = 0
$totalNew = 0
$pageSize = 1000

do {
    Write-Host "  Fetching offset=$offset ..." -NoNewline

    $params = "startDate=$([Uri]::EscapeDataString($startDate))&limit=$pageSize&offset=$offset&order=asc"
    $page   = Invoke-RestMethod -Uri "$REDALERT_BASE/stats/history?$params" `
                -Headers $RA_HEADERS -Method Get

    $alerts  = $page.data
    [int]$pgTotal = [int]$page.pagination.total
    Write-Host " $($alerts.Count) alerts  (total=$pgTotal)"

    if ($alerts.Count -eq 0) { break }

    # For each alert deduplicate zones via city lookup, then build row lists
    $alertRows = [System.Collections.ArrayList]::new()
    $zoneRows  = [System.Collections.ArrayList]::new()

    foreach ($a in $alerts) {
        [void]$alertRows.Add(@($a.id, $a.timestamp, $a.type, $a.origin))

        $seen = [System.Collections.Generic.HashSet[string]]::new()
        foreach ($c in $a.cities) {
            $z = $cityZone[$c.name]
            if ($z -and $seen.Add($z)) {
                [void]$zoneRows.Add(@($a.id, $z))
            }
        }
    }

    Write-Host "    Inserting $($alertRows.Count) alert rows, $($zoneRows.Count) zone rows ..."
    Invoke-D1BulkInsert "alerts"       @("id","ts","type","origin")   $alertRows
    Invoke-D1BulkInsert "alert_zones"  @("alert_id","zone")           $zoneRows
    Write-Host "    Done (page)."

    $totalNew += $alerts.Count
    $offset   += $alerts.Count
    Start-Sleep -Milliseconds 400

} while ($offset -lt $pgTotal)

Write-Host "`nInserted/skipped $totalNew alert records."
Write-Host "`nAll done!"
