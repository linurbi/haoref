# ============================================================
#  OREF Alert History Accumulator — PowerShell edition
#  Fetches fresh OREF alerts via the Cloudflare Worker,
#  merges them into the GitHub Gist, and logs progress.
#
#  HOW TO RUN ONCE MANUALLY:
#    Right-click this file → "Run with PowerShell"
#    or in a terminal:  .\accumulate.ps1
#
#  HOW TO SCHEDULE (runs every hour automatically):
#    Open Task Scheduler → Create Basic Task
#      Name:    OREF Accumulator
#      Trigger: Daily, repeat every 1 hour indefinitely
#      Action:  Start a program
#               Program:  powershell.exe
#               Args:     -ExecutionPolicy Bypass -File "C:\Linur\Projects\customers\Haoref\accumulate.ps1"
# ============================================================

$CF_WORKER_URL = "https://misty-pond-cb9d.linurbi.workers.dev"
$GIST_ID       = if ($env:GIST_ID)    { $env:GIST_ID }    else { "972798220ff080e050a3a4a0d386b3e0" }
$GITHUB_TOKEN  = if ($env:GIST_TOKEN) { $env:GIST_TOKEN } else { throw "Set GIST_TOKEN env var or env:GIST_TOKEN" }
$LOG_FILE      = if ($env:GITHUB_ACTIONS) { "./accumulate.log" } else { "$PSScriptRoot\accumulate.log" }

function Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Write-Host $line
    Add-Content -Path $LOG_FILE -Value $line -Encoding UTF8
}

Log "--- OREF accumulator starting ---"

# ── Step 1: fetch fresh alerts from the Cloudflare Worker ──────────────────
try {
    $fresh = Invoke-RestMethod -Uri $CF_WORKER_URL -UseBasicParsing -TimeoutSec 20
    Log "Cloudflare Worker returned $($fresh.Count) records"
} catch {
    Log "ERROR fetching from Cloudflare Worker: $_"
    exit 1
}

# ── Step 2: load existing Gist content ─────────────────────────────────────
$headers = @{
    Authorization = "token $GITHUB_TOKEN"
    Accept        = "application/vnd.github+json"
}

try {
    $gistResp = Invoke-RestMethod -Uri "https://api.github.com/gists/$GIST_ID" -Headers $headers -UseBasicParsing
    $existing = $gistResp.files."oref_history.json".content | ConvertFrom-Json
    Log "Gist currently has $($existing.Count) stored records"
} catch {
    Log "WARNING: could not load Gist, starting fresh: $_"
    $existing = @()
}

# ── Step 3: merge (deduplicate by alertDate + location) ────────────────────
$seen = @{}
foreach ($a in $existing) { $seen["$($a.alertDate)||$($a.data)"] = $true }

$added = 0
foreach ($a in $fresh) {
    $key = "$($a.alertDate)||$($a.data)"
    if (-not $seen.ContainsKey($key)) {
        $existing += $a
        $seen[$key] = $true
        $added++
    }
}

if ($added -eq 0) {
    Log "No new records. Nothing to upload. Total: $($existing.Count)"
    exit 0
}

Log "+$added new records  →  $($existing.Count) total"

# ── Step 4: sort newest first and upload to Gist ───────────────────────────
$sorted  = $existing | Sort-Object alertDate -Descending
$newJson = $sorted | ConvertTo-Json -Compress

$body = @{
    files = @{
        "oref_history.json" = @{ content = $newJson }
    }
} | ConvertTo-Json -Depth 5

try {
    $result = Invoke-RestMethod -Uri "https://api.github.com/gists/$GIST_ID" `
        -Method Patch -Headers $headers -Body $body -ContentType "application/json"
    Log "Gist updated successfully  (updated_at: $($result.updated_at))"
} catch {
    Log "ERROR uploading to Gist: $_"
    exit 1
}

Log "--- Done ---"
