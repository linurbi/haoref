# ============================================================
#  OREF Alert History Accumulator — PowerShell edition
#  Fetches fresh OREF alerts via the Cloudflare Worker,
#  merges them into the GitHub Gist, and logs progress.
# ============================================================

$CF_WORKER_URL = "https://misty-pond-cb9d.linurbi.workers.dev"
$GIST_ID       = if ($env:GIST_ID)    { $env:GIST_ID }    else { "972798220ff080e050a3a4a0d386b3e0" }
$GITHUB_TOKEN  = if ($env:GIST_TOKEN) { $env:GIST_TOKEN } else { throw "Set GIST_TOKEN env var" }
$LOG_FILE      = if ($env:GITHUB_ACTIONS) { "./accumulate.log" } else { "$PSScriptRoot\accumulate.log" }

function Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Write-Host $line
    Add-Content -Path $LOG_FILE -Value $line -Encoding UTF8
}

function Escape-Str([string]$s) { $s.Replace('\','\\').Replace('"','\"') }

function Upload-GistFile([string]$filename, [string]$content) {
    $escaped   = $content.Replace('\','\\').Replace('"','\"').Replace("`r`n",'\n').Replace("`n",'\n').Replace("`r",'\r')
    $bodyStr   = '{"files":{"' + $filename + '":{"content":"' + $escaped + '"}}}'
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($bodyStr)

    $req = [System.Net.HttpWebRequest]::Create("https://api.github.com/gists/$GIST_ID")
    $req.Method = "PATCH"
    $req.ContentType = "application/json; charset=utf-8"
    $req.Headers["Authorization"] = "token $GITHUB_TOKEN"
    $req.Headers["Accept"] = "application/vnd.github+json"
    $req.UserAgent = "PowerShell"
    $req.ContentLength = $bodyBytes.Length

    $stream = $req.GetRequestStream()
    $stream.Write($bodyBytes, 0, $bodyBytes.Length)
    $stream.Close()

    try {
        $resp   = $req.GetResponse()
        $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
        $text   = $reader.ReadToEnd()
        $reader.Close(); $resp.Close()
        if ($text -match '"updated_at":"([^"]+)"') { return $Matches[1] }
        return "ok"
    } catch [System.Net.WebException] {
        $errReader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $errText   = $errReader.ReadToEnd(); $errReader.Close()
        return "ERROR: $errText"
    }
}

Log "--- OREF accumulator starting ---"

# ── Step 1: fetch only recent alerts from the Cloudflare Worker ───────────────
$fromDate = (Get-Date).AddDays(-2).ToString("dd.MM.yyyy")
try {
    $fresh = Invoke-RestMethod -Uri "$CF_WORKER_URL`?fromDate=$fromDate" -UseBasicParsing -TimeoutSec 60
    if (-not $fresh -or $fresh.Count -eq 0) { Log "CF Worker returned 0 records — nothing to do."; exit 0 }
    Log "Cloudflare Worker returned $($fresh.Count) records (since $fromDate)"
} catch {
    Log "WARNING: CF Worker blocked or unavailable ($fromDate) — skipping this run: $_"
    exit 0
}

# ── Step 2: load existing Gist content (two-part files) ───────────────────────
$headers = @{ Authorization = "token $GITHUB_TOKEN"; Accept = "application/vnd.github+json" }
try {
    $gistResp  = Invoke-RestMethod -Uri "https://api.github.com/gists/$GIST_ID" -Headers $headers -UseBasicParsing
    $existing1 = $gistResp.files."oref_history_1.json".content | ConvertFrom-Json
    $existing2 = $gistResp.files."oref_history_2.json".content | ConvertFrom-Json
    $existing  = @($existing1) + @($existing2) | Where-Object { $_ -ne $null }
    Log "Gist currently has $($existing.Count) stored records"
} catch {
    Log "WARNING: could not load Gist, starting fresh: $_"
    $existing = @()
}

# ── Step 3: merge (deduplicate by alertDate + city) ───────────────────────────
$seen = [System.Collections.Generic.HashSet[string]]::new()
foreach ($a in $existing) { [void]$seen.Add("$($a.alertDate)||$($a.data)") }

$added = 0
foreach ($a in $fresh) {
    $key = "$($a.alertDate)||$($a.data)"
    if ($seen.Add($key)) { $existing += $a; $added++ }
}

if ($added -eq 0) {
    Log "No new records. Nothing to upload. Total: $($existing.Count)"
    exit 0
}
Log "+$added new records  →  $($existing.Count) total"

# ── Step 4: build compact JSON with actual UTF-8 ──────────────────────────────
$sorted    = @($existing | Sort-Object alertDate -Descending)
$jsonParts = $sorted | ForEach-Object {
    '{"alertDate":"' + (Escape-Str $_.alertDate) + '","title":"' + (Escape-Str $_.title) + '","data":"' + (Escape-Str $_.data) + '","category":' + [string]$_.category + '}'
}
$json      = '[' + ($jsonParts -join ',') + ']'
$jsonBytes = [System.Text.Encoding]::UTF8.GetByteCount($json)
Log "JSON size: $([Math]::Round($jsonBytes/1MB,1)) MB"

# ── Step 5: split at ~22 MB ───────────────────────────────────────────────────
$LIMIT = 22 * 1024 * 1024
if ($jsonBytes -le $LIMIT) {
    $part1 = $json; $part2 = "[]"
} else {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    $pos   = $LIMIT
    while ($pos -gt 0 -and ($bytes[$pos] -ne [byte]0x7D -or $bytes[$pos+1] -ne [byte]0x2C)) { $pos-- }
    $pos++
    $part1 = [System.Text.Encoding]::UTF8.GetString($bytes[0..($pos-1)]) + ']'
    $part2 = '[' + ([System.Text.Encoding]::UTF8.GetString($bytes[$pos..($bytes.Length-1)])).TrimStart(',')
}

$s1 = [Math]::Round([System.Text.Encoding]::UTF8.GetByteCount($part1)/1MB,1)
$s2 = [Math]::Round([System.Text.Encoding]::UTF8.GetByteCount($part2)/1MB,1)
Log "part1=$s1 MB  part2=$s2 MB"

# ── Step 6: upload both parts to Gist ────────────────────────────────────────
Log "Uploading oref_history_1.json ($s1 MB)..."
$r1 = Upload-GistFile "oref_history_1.json" $part1
Log "  -> $r1"

Log "Uploading oref_history_2.json ($s2 MB)..."
$r2 = Upload-GistFile "oref_history_2.json" $part2
Log "  -> $r2"

if ($r1 -like "ERROR*" -or $r2 -like "ERROR*") { exit 1 }

Log "--- Done ---"
