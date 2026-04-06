# Fetch Telegram channel via t.me/s/ public web view -> parse alerts -> save to GitHub repo data branch
# No HTML export files, no bot admin rights needed.

$CHANNEL    = "PikudHaOref_all"
$outputFile = if ($env:GITHUB_ACTIONS) { "./oref_history.json" } else { "c:\Linur\Projects\customers\Haoref\oref_history.json" }
$START_DATE = [datetime]"2026-03-31"   # TEST: change to 2026-02-28 for full backfill
# Data is read from / written to the repo's 'data' branch via raw.githubusercontent.com
$DATA_BASE  = "https://raw.githubusercontent.com/linurbi/haoref/data"

# ── JSON helpers ──────────────────────────────────────────────────────────────
function Escape-Str([string]$s) {
    return $s.Replace('\','\\').Replace('"','\"')
}
function Rec([PSCustomObject]$r) {
    return '{"alertDate":"' + (Escape-Str $r.alertDate) + '","title":"' + (Escape-Str $r.title) + '","data":"' + (Escape-Str $r.data) + '","region":"' + (Escape-Str $r.region) + '","category":' + [string]$r.category + '}'
}
function Strip-Html($s) { [regex]::Replace($s,'<[^>]+>',' ').Trim() -replace '\s+',' ' }

# ── Load existing data from repo data branch so we only fetch what's new ──────
$results  = [System.Collections.ArrayList]::new()
$seen     = [System.Collections.Generic.HashSet[string]]::new()

function Load-DataFile([string]$url) {
    try {
        $r = Invoke-WebRequest "$url`?t=$(Get-Date -UFormat %s)" -UseBasicParsing -TimeoutSec 60 -ErrorAction Stop
        $parsed = $r.Content | ConvertFrom-Json
        return if ($parsed) { @($parsed) } else { @() }
    } catch {
        return @()
    }
}

try {
    $existing1 = Load-DataFile "$DATA_BASE/oref_history_1.json"
    $existing2 = Load-DataFile "$DATA_BASE/oref_history_2.json"
    $existing  = @($existing1) + @($existing2) | Where-Object { $_ -ne $null }
    foreach ($a in $existing) {
        [void]$results.Add($a)
        [void]$seen.Add("$($a.alertDate)||$($a.data)")
    }
    $latestDate = ($existing | Sort-Object alertDate -Descending | Select-Object -First 1).alertDate
    if ($existing.Count -lt 500 -or -not $latestDate) {
        $SINCE = $START_DATE
        Write-Host "Loaded $($existing.Count) existing records — too few, backfilling from $START_DATE"
    } else {
        $SINCE = [datetime]$latestDate
        Write-Host "Loaded $($existing.Count) existing records. Latest: $latestDate"
    }
} catch {
    Write-Host "Could not load existing data (starting fresh): $_"
    $SINCE = $START_DATE
}

$beforeId = $null
$done     = $false
$pageNum  = 0

Write-Host "Fetching https://t.me/s/$CHANNEL (since $($SINCE.ToString('yyyy-MM-dd HH:mm:ss'))) ..."

while (-not $done) {
    $url = "https://t.me/s/$CHANNEL"
    if ($beforeId) { $url += "?before=$beforeId" }
    $pageNum++
    Write-Host "  Page $pageNum : $url"

    try {
        $resp = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 30 `
                    -Headers @{ 'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
        # Use Content (PS Core on Linux already decodes as UTF-8 from charset header)
        $html = $resp.Content
    } catch {
        Write-Host "ERROR fetching: $_"; break
    }

    Write-Host "    HTML length: $($html.Length) chars"

    # Split page into per-message blocks
    $blocks = ($html -split '(?=<div class="tgme_widget_message_wrap)') |
              Where-Object { $_ -match 'data-post=' }

    Write-Host "    Blocks found: $($blocks.Count)"
    if ($blocks.Count -eq 0) { Write-Host "  No message blocks found — stopping."; break }

    # Debug: show first block's datetime and msgType
    if ($blocks.Count -gt 0) {
        $sample = $blocks[0]
        $sdt = [regex]::Match($sample, 'datetime="([^"]+)"').Groups[1].Value
        $stxt = [regex]::Match($sample, '<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]{0,100})').Groups[1].Value
        Write-Host "    Sample datetime: $sdt | text preview: $($stxt -replace '<[^>]+>','' | Select-Object -First 1)"
    }

    $minId = [int]::MaxValue
    $pageNew = 0; $pageSkipped = 0

    foreach ($block in $blocks) {
        # Message ID
        $idM = [regex]::Match($block, 'data-post="[^/]+/(\d+)"')
        if (-not $idM.Success) { continue }
        $msgId = [int]$idM.Groups[1].Value
        if ($msgId -lt $minId) { $minId = $msgId }

        # Message datetime (UTC)
        $dtM = [regex]::Match($block, 'datetime="([^"]+)"')
        if (-not $dtM.Success) { continue }
        try { $msgDate = [datetime]::Parse($dtM.Groups[1].Value).ToUniversalTime() }
        catch { continue }

        if ($msgDate -le $SINCE) { $done = $true; continue }

        # Message text HTML
        $textM = [regex]::Match($block, '<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)</div>')
        if (-not $textM.Success) { continue }
        $rawHtml = $textM.Groups[1].Value.Trim()
        if ($rawHtml.Length -lt 10) { continue }

        # Normalise tags to match the section parser
        $rawHtml = $rawHtml -replace '<br\s*/?>', '<br>'
        $rawHtml = $rawHtml -replace '<b>',        '<strong>'
        $rawHtml = $rawHtml -replace '</b>',       '</strong>'

        $alertDate = $msgDate.ToString("yyyy-MM-dd HH:mm:ss")

        # ── Detect message type from header (e.g. "ירי רקטות וטילים (8/3/2026) 6:22") ──
        $typeM = [regex]::Match($rawHtml, '<strong>([^\d<(]+?)\s*\(\d+/\d+/\d+\)')
        $msgType = if ($typeM.Success) { $typeM.Groups[1].Value.Trim() } else { "" }

        # Skip only all-clears (עדכון) — pre-alerts (מבזק) are stored with category 4
        if ($msgType -match "\u05E2\u05D3\u05DB\u05D5\u05DF") { $pageSkipped++; continue }

        # Skip if we can't identify the type at all
        if ($msgType.Length -eq 0) { $pageSkipped++; continue }

        $advisoryRx = "\u05D9\u05E9 \u05DC\u05E4\u05E2\u05D5\u05DC|\u05D4\u05D9\u05DB\u05E0\u05E1\u05D5|\u05DE\u05E8\u05D7\u05D1 \u05DE\u05D5\u05D2\u05DF|\u05E4\u05D9\u05E7\u05D5\u05D3 \u05D4\u05E2\u05D5\u05E8\u05E3|\u05D4\u05E9\u05D5\u05D4\u05D9\u05DD|\u05D1\u05D4\u05EA\u05D0\u05DD \u05DC\u05D4\u05E0\u05D7\u05D9\u05D5\u05EA"

        # ── Parse sections: find every <strong>TITLE</strong><br>CITIES block ─
        # Global scan — no split needed. Content ends at next <br><br> or end.
        $sectionRx = [System.Text.RegularExpressions.Regex]::new(
            '<strong>([^<]+)</strong><br>((?:(?!<br><br>).)*)',
            [System.Text.RegularExpressions.RegexOptions]::Singleline
        )
        foreach ($m in $sectionRx.Matches($rawHtml)) {
            $sectionTitle = Strip-Html $m.Groups[1].Value.Trim()

            # Skip alarm-type headers (they contain a date like "(5/4/2026)")
            if ($sectionTitle -match '\d+/\d+/\d+') { continue }

            # Skip advisory/instructional text sections
            if ($sectionTitle -match $advisoryRx) { continue }

            # Individual alarm → section title is a region ("אזור X"), use $msgType as alarm title
            # Summary message  → section title is the alarm type, leave region blank
            $isRegion  = $sectionTitle -match '^\u05D0\u05D6\u05D5\u05E8'
            $recTitle  = if ($isRegion) { $msgType } else { $sectionTitle }
            $recRegion = if ($isRegion) { $sectionTitle } else { "" }
            # Categories: 1=rockets, 2=UAV (כלי טיס עוין), 3=ballistic (כטב"מ), 4=pre-alert (מבזק)
            $recCat = 1
            if     ($recTitle -match "\u05DE\u05D1\u05D6\u05E7")                           { $recCat = 4 }
            elseif ($recTitle -match "\u05DB\u05D8\u05D1")                                  { $recCat = 3 }
            elseif ($recTitle -match "\u05DB\u05DC\u05D9 \u05D8\u05D9\u05E1 \u05E2\u05D5\u05D9\u05DF") { $recCat = 2 }

            $citiesHtml = $m.Groups[2].Value
            # Treat <br> between time-groups as a city separator
            $citiesHtml = $citiesHtml -replace '<br>', ','
            # Strip time annotations like (<strong>30 שניות</strong>)
            $citiesHtml = [regex]::Replace($citiesHtml, '\(<strong>[^<]*</strong>\)', '')
            # Strip remaining HTML tags
            $citiesHtml = [regex]::Replace($citiesHtml, '<[^>]+>', ' ')
            $citiesHtml = ($citiesHtml -replace '[\r\n\t]+',' ').Trim()

            $cities = $citiesHtml -split '[,،]' |
                ForEach-Object { $_.Trim() -replace '\s{2,}',' ' } |
                Where-Object { $_.Length -ge 2 -and $_.Length -le 50 -and $_ -notmatch $advisoryRx }

            if ($cities.Count -eq 0) { continue }

            foreach ($city in $cities) {
                $key = "$alertDate||$city"
                if ($seen.Add($key)) {
                    $pageNew++
                    [void]$results.Add([PSCustomObject]@{
                        alertDate = $alertDate
                        title     = $recTitle
                        data      = $city
                        region    = $recRegion
                        category  = $recCat
                    })
                }
            }
        }
    }

    Write-Host "    -> $($results.Count) total records | +$pageNew new | $pageSkipped skipped | min ID: $minId"

    if ($done -or $minId -eq [int]::MaxValue) { break }
    $beforeId = $minId
    Start-Sleep -Milliseconds 500
}

$sorted = @($results | Sort-Object alertDate -Descending)
Write-Host "`nTotal: $($sorted.Count) records"
if ($sorted.Count -gt 0) {
    Write-Host "Newest: $($sorted[0].alertDate)"
    Write-Host "Oldest: $($sorted[-1].alertDate)"
}

if ($sorted.Count -eq 0) {
    Write-Host "No records — aborting to avoid overwriting data with empty file."
    exit 0
}

# ── Build JSON with actual UTF-8 Hebrew ──────────────────────────────────────
Write-Host "Building JSON (UTF-8)..."
$jsonParts = $sorted | ForEach-Object { Rec $_ }
$json = '[' + ($jsonParts -join ',') + ']'
$jsonBytes = [System.Text.Encoding]::UTF8.GetByteCount($json)
Write-Host "JSON size: $([Math]::Round($jsonBytes/1MB,1)) MB"

$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($outputFile, $json, $utf8NoBom)
Write-Host "Saved: $outputFile"

# ── Split at ~45 MB (GitHub file limit is 100 MB, well within limits) ────────
$LIMIT = 45 * 1024 * 1024

if ($jsonBytes -le $LIMIT) {
    $part1 = $json; $part2 = "[]"
} else {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    $pos = $LIMIT
    while ($pos -gt 0 -and ($bytes[$pos] -ne [byte]0x7D -or $bytes[$pos+1] -ne [byte]0x2C)) { $pos-- }
    $pos++
    $p1bytes = $bytes[0..($pos-1)]
    $p2bytes = $bytes[$pos..($bytes.Length-1)]
    $part1 = [System.Text.Encoding]::UTF8.GetString($p1bytes) + ']'
    $part2 = '[' + ([System.Text.Encoding]::UTF8.GetString($p2bytes)).TrimStart(',')
    $cnt1 = ($part1 -split '},\{' | Measure-Object).Count
    $cnt2 = ($part2 -split '},\{' | Measure-Object).Count - 1
    Write-Host "Split: ~$cnt1 records in part1, ~$cnt2 in part2"
}

$s1 = [Math]::Round([System.Text.Encoding]::UTF8.GetByteCount($part1)/1MB,1)
$s2 = [Math]::Round([System.Text.Encoding]::UTF8.GetByteCount($part2)/1MB,1)
Write-Host "part1=$s1 MB  part2=$s2 MB"

# ── Save files locally — the workflow's git step will push them to the data branch ──
$utf8NoBom2 = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText("./oref_history_1.json", $part1, $utf8NoBom2)
[System.IO.File]::WriteAllText("./oref_history_2.json", $part2, $utf8NoBom2)
Write-Host "Saved oref_history_1.json ($s1 MB) and oref_history_2.json ($s2 MB)"
Write-Host "Done! Workflow git step will push these to the data branch."
