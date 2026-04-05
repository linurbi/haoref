# Fetch Telegram channel via t.me/s/ public web view -> parse alerts -> upload to GitHub Gist
# No HTML export files, no bot admin rights needed.

$CHANNEL      = "PikudHaOref_all"
$outputFile   = if ($env:GITHUB_ACTIONS) { "./oref_history.json" } else { "c:\Linur\Projects\customers\Haoref\oref_history.json" }
$GIST_ID      = if ($env:GIST_ID)    { $env:GIST_ID }    else { "972798220ff080e050a3a4a0d386b3e0" }
$GITHUB_TOKEN = if ($env:GIST_TOKEN) { $env:GIST_TOKEN } else { throw "Set GIST_TOKEN env var or env:GIST_TOKEN" }
$START_DATE   = [datetime]"2026-02-28"

# ── JSON helpers ──────────────────────────────────────────────────────────────
function Escape-Str([string]$s) {
    return $s.Replace('\','\\').Replace('"','\"')
}
function Rec([PSCustomObject]$r) {
    return '{"alertDate":"' + (Escape-Str $r.alertDate) + '","title":"' + (Escape-Str $r.title) + '","data":"' + (Escape-Str $r.data) + '","region":"' + (Escape-Str $r.region) + '","category":' + [string]$r.category + '}'
}
function Strip-Html($s) { [regex]::Replace($s,'<[^>]+>',' ').Trim() -replace '\s+',' ' }

# ── Load existing Gist data so we only fetch what's new ──────────────────────
$results  = [System.Collections.ArrayList]::new()
$seen     = [System.Collections.Generic.HashSet[string]]::new()

$headers = @{ Authorization = "token $GITHUB_TOKEN"; Accept = "application/vnd.github+json" }
try {
    $gistResp = Invoke-RestMethod "https://api.github.com/gists/$GIST_ID" -Headers $headers -UseBasicParsing
    $existing1 = $gistResp.files."oref_history_1.json".content | ConvertFrom-Json
    $existing2 = $gistResp.files."oref_history_2.json".content | ConvertFrom-Json
    $existing  = @($existing1) + @($existing2) | Where-Object { $_ -ne $null }
    foreach ($a in $existing) {
        [void]$results.Add($a)
        [void]$seen.Add("$($a.alertDate)||$($a.data)")
    }
    $latestDate = ($existing | Sort-Object alertDate -Descending | Select-Object -First 1).alertDate
    $SINCE = if ($latestDate) { [datetime]$latestDate } else { $START_DATE }
    Write-Host "Loaded $($existing.Count) existing records from Gist. Latest: $latestDate"
} catch {
    Write-Host "Could not load Gist (starting fresh): $_"
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
        # Force UTF-8 decoding to avoid ??? for Hebrew characters
        $html = [System.Text.Encoding]::UTF8.GetString($resp.RawContentBytes)
    } catch {
        Write-Host "ERROR fetching: $_"; break
    }

    # Split page into per-message blocks
    $blocks = ($html -split '(?=<div class="tgme_widget_message_wrap)') |
              Where-Object { $_ -match 'data-post=' }

    if ($blocks.Count -eq 0) { Write-Host "  No message blocks found — stopping."; break }

    $minId = [int]::MaxValue

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

        # Skip pre-alerts (מבזק) and all-clears (עדכון) — not actual alarms
        if ($msgType -match "\u05DE\u05D1\u05D6\u05E7|\u05E2\u05D3\u05DB\u05D5\u05DF") { continue }

        # Skip if we can't identify the type at all
        if ($msgType.Length -eq 0) { continue }

        # Determine category from the alarm type
        $category = 1
        if ($msgType -match "\u05DB\u05D8\u05D1|\u05DB\u05D8\u05B7\u05D1\u05B4") { $category = 3 }

        # ── Parse per-region sections ─────────────────────────────────────────
        $sections = $rawHtml -split '<br><br>'
        foreach ($sec in $sections) {
            $sec = $sec.Trim()
            if ($sec -match '<strong>(.*?)</strong><br>(.+)') {
                $sectionTitle = Strip-Html $Matches[1]

                # Skip the alarm header section (contains date like "(8/3/2026)")
                if ($sectionTitle -match '\d+/\d+/\d+') { continue }
                # Skip advisory/instructional sections — only accept region names (start with אזור)
                if ($sectionTitle -notmatch '^\u05D0\u05D6\u05D5\u05E8') { continue }

                $citiesHtml = $Matches[2]
                $citiesHtml = [regex]::Replace($citiesHtml, '\(<strong>[^<]*</strong>\)', '')
                $citiesHtml = [regex]::Replace($citiesHtml, '<[^>]+>', ' ')
                $citiesHtml = $citiesHtml.Trim()

                $advisoryRx = "\u05D9\u05E9 \u05DC\u05E4\u05E2\u05D5\u05DC|\u05D4\u05D9\u05DB\u05E0\u05E1\u05D5|\u05DE\u05E8\u05D7\u05D1 \u05DE\u05D5\u05D2\u05DF|\u05E4\u05D9\u05E7\u05D5\u05D3 \u05D4\u05E2\u05D5\u05E8\u05E3|\u05D4\u05E9\u05D5\u05D4\u05D9\u05DD|\u05D1\u05D4\u05EA\u05D0\u05DD \u05DC\u05D4\u05E0\u05D7\u05D9\u05D5\u05EA"
                $cities = $citiesHtml -split '[,،]' |
                    ForEach-Object { $_.Trim() -replace '[\r\n]+',' ' -replace '\s{2,}',' ' } |
                    Where-Object { $_.Length -ge 2 -and $_.Length -le 50 -and $_ -notmatch $advisoryRx }

                if ($cities.Count -eq 0) { continue }

                foreach ($city in $cities) {
                    $key = "$alertDate||$city"
                    if ($seen.Add($key)) {
                        [void]$results.Add([PSCustomObject]@{
                            alertDate = $alertDate
                            title     = $msgType
                            data      = $city
                            region    = $sectionTitle
                            category  = $category
                        })
                    }
                }
            }
        }
    }

    Write-Host "    -> $($results.Count) records so far (min msg ID on page: $minId)"

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

# ── Build JSON with actual UTF-8 Hebrew ──────────────────────────────────────
Write-Host "Building JSON (UTF-8)..."
$jsonParts = $sorted | ForEach-Object { Rec $_ }
$json = '[' + ($jsonParts -join ',') + ']'
$jsonBytes = [System.Text.Encoding]::UTF8.GetByteCount($json)
Write-Host "JSON size: $([Math]::Round($jsonBytes/1MB,1)) MB"

$utf8NoBom = New-Object System.Text.UTF8Encoding $false
[System.IO.File]::WriteAllText($outputFile, $json, $utf8NoBom)
Write-Host "Saved: $outputFile"

# ── Split at ~22 MB ───────────────────────────────────────────────────────────
$LIMIT = 22 * 1024 * 1024

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

# ── Upload each file to Gist ──────────────────────────────────────────────────
function Upload-GistFile([string]$filename, [string]$content) {
    $escaped = $content.Replace('\','\\').Replace('"','\"').Replace("`r`n",'\n').Replace("`n",'\n').Replace("`r",'\r')
    $bodyStr = '{"files":{"' + $filename + '":{"content":"' + $escaped + '"}}}'
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
        $resp = $req.GetResponse()
        $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
        $respText = $reader.ReadToEnd()
        $reader.Close(); $resp.Close()
        if ($respText -match '"updated_at":"([^"]+)"') { return $Matches[1] }
        return "ok"
    } catch [System.Net.WebException] {
        $errStream = $_.Exception.Response.GetResponseStream()
        $errReader = New-Object System.IO.StreamReader($errStream)
        $errText = $errReader.ReadToEnd()
        $errReader.Close()
        return "ERROR: $errText"
    }
}

Write-Host "Uploading oref_history_1.json ($s1 MB)..."
$r1 = Upload-GistFile "oref_history_1.json" $part1
Write-Host "  -> $r1"

Write-Host "Uploading oref_history_2.json ($s2 MB)..."
$r2 = Upload-GistFile "oref_history_2.json" $part2
Write-Host "  -> $r2"

Write-Host "Done!"
