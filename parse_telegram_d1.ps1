# parse_telegram_d1.ps1
# Scrapes https://t.me/s/PikudHaOref_all (public web view, no credentials needed)
# Parses every alert message and bulk-inserts into Cloudflare D1.
#
# Schema:
#   id TEXT PK          -- msg_id|city  (or just msg_id for all-clears)
#   msg_id INTEGER      -- Telegram message ID (unique per post = unique siren event)
#   alert_ts TEXT       -- ISO datetime
#   alert_type TEXT     -- rockets | uav | ballistic | pre_alert | all_clear |
#                          earthquake | infiltration | hazmat | wildfire | tsunami | unknown
#   title TEXT          -- section header from the message
#   city TEXT           -- city name (empty for all-clears)
#   region TEXT         -- zone/area name
#   category INTEGER    -- official Pikud HaOref category ID (1=missilealert 2=uav 7=earthquake 10=terrorattack 11=tsunami 12=hazmat 13=update/all_clear 14=flash/pre_alert)
#
# COUNT(DISTINCT msg_id) = unique siren activations (not city-hits, not rockets)
# Use alert_type filter to separate rockets / UAV / ballistic etc.
#
# Run:  powershell -ExecutionPolicy Bypass -File .\parse_telegram_d1.ps1

# ---- CONFIG ------------------------------------------------------------------
# Credentials: prefer environment variables (GitHub Actions secrets),
# fall back to hardcoded values for local runs.
$CF_ACCOUNT_ID  = if ($env:CF_ACCOUNT_ID)  { $env:CF_ACCOUNT_ID }  else { "913dd7d67a19b98eb74cab6d8e8e0b4a" }
$CF_API_TOKEN   = if ($env:CF_API_TOKEN)   { $env:CF_API_TOKEN }   else { "cfat_vec9LvsRcJtkxrx5p15DkZbWiMdW53U9jp2bj9b8e52ce9e2" }
$D1_DATABASE_ID = if ($env:D1_DATABASE_ID) { $env:D1_DATABASE_ID } else { "ac645c9a-e7cc-4eb1-a0b1-17fe4cc437e5" }
$CHANNEL        = "PikudHaOref_all"
$OP_START       = [datetime]"2026-02-28"
# ------------------------------------------------------------------------------

$D1_URL = "https://api.cloudflare.com/client/v4/accounts/$CF_ACCOUNT_ID/d1/database/$D1_DATABASE_ID"

# ---- Hebrew patterns ---------------------------------------------------------
# Use \uXXXX inside regex strings — interpreted by the .NET regex engine,
# NOT by PowerShell string parsing, so they work correctly in PowerShell 5.1.
#
# צבע אדום | ירי רקט  →  rocket alert
$PAT_ROCKETS      = "\u05E6\u05D1\u05E2|\u05D9\u05E8\u05D9 \u05E8\u05E7\u05D8"
# מבזק  →  pre-alert
$PAT_PREALERT     = "\u05DE\u05D1\u05D6\u05E7"
# כטב  →  ballistic (כטב"מ)
$PAT_BALLISTIC    = "\u05DB\u05D8\u05D1"
# כלי טיס  →  hostile UAV / drone
$PAT_UAV          = "\u05DB\u05DC\u05D9 \u05D8\u05D9\u05E1"
# עדכון | ביטול | גמר התרעה  →  all-clear / end of event
$PAT_ALLCLEAR     = "\u05E2\u05D3\u05DB\u05D5\u05DF|\u05D1\u05D9\u05D8\u05D5\u05DC|\u05D2\u05DE\u05E8 \u05D4\u05EA\u05E8\u05E2\u05D4"
# רעידת  →  earthquake
$PAT_EARTHQUAKE   = "\u05E8\u05E2\u05D9\u05D3\u05EA"
# חדירת  →  terrorist infiltration
$PAT_INFILTRATION = "\u05D7\u05D3\u05D9\u05E8\u05EA"
# חומרים  →  hazmat
$PAT_HAZMAT       = "\u05D7\u05D5\u05DE\u05E8\u05D9\u05DD"
# שריפה  →  wildfire
$PAT_WILDFIRE     = "\u05E9\u05E8\u05D9\u05E4\u05D4"
# צונאמי  →  tsunami
$PAT_TSUNAMI      = "\u05E6\u05D5\u05E0\u05D0\u05DE\u05D9"
# advisory phrases: יש לפעול | היכנסו | להיכנס | מרחב מוגן | מרחב המוגן | לשהות | פיקוד העורף | השוהים | בהתאם | בדקות הקרובות
$PAT_ADVISORY     = "\u05D9\u05E9 \u05DC\u05E4\u05E2\u05D5\u05DC|\u05D4\u05D9\u05DB\u05E0\u05E1\u05D5|\u05DC\u05D4\u05D9\u05DB\u05E0\u05E1|\u05DE\u05E8\u05D7\u05D1 \u05DE\u05D5\u05D2\u05DF|\u05DE\u05E8\u05D7\u05D1 \u05D4\u05DE\u05D5\u05D2\u05DF|\u05DC\u05E9\u05D4\u05D5\u05EA|\u05E4\u05D9\u05E7\u05D5\u05D3 \u05D4\u05E2\u05D5\u05E8\u05E3|\u05D4\u05E9\u05D5\u05D4\u05D9\u05DD|\u05D1\u05D4\u05EA\u05D0\u05DD|\u05D1\u05D3\u05E7\u05D5\u05EA"
# אזור  →  region prefix  (^ = line start)
$PAT_REGION_PFX   = "^\u05D0\u05D6\u05D5\u05E8"

function Get-AlertType([string]$t) {
    if ($t -match $PAT_ALLCLEAR)     { return "all_clear"    }
    if ($t -match $PAT_PREALERT)     { return "pre_alert"    }
    if ($t -match $PAT_BALLISTIC)    { return "ballistic"    }
    if ($t -match $PAT_UAV)          { return "uav"          }
    if ($t -match $PAT_EARTHQUAKE)   { return "earthquake"   }
    if ($t -match $PAT_INFILTRATION) { return "infiltration" }
    if ($t -match $PAT_HAZMAT)       { return "hazmat"       }
    if ($t -match $PAT_WILDFIRE)     { return "wildfire"     }
    if ($t -match $PAT_TSUNAMI)      { return "tsunami"      }
    if ($t.Length -gt 0)             { return "rockets"      }
    return "unknown"
}

function Get-Category([string]$alertType) {
    # Official Pikud HaOref category IDs
    switch ($alertType) {
        "rockets"      { return 1  }   # missilealert
        "ballistic"    { return 1  }   # missilealert (ballistic = high-trajectory missile)
        "uav"          { return 2  }   # uav
        "earthquake"   { return 7  }   # earthquakealert1
        "tsunami"      { return 11 }   # tsunami
        "hazmat"       { return 12 }   # hazmat
        "all_clear"    { return 13 }   # update
        "pre_alert"    { return 14 }   # flash
        "infiltration" { return 10 }   # terrorattack
        "wildfire"     { return 4  }   # warning
        default        { return 0  }
    }
}

# ---- D1 helpers --------------------------------------------------------------

function Invoke-D1Query([string]$sql) {
    $body      = @{ sql = $sql } | ConvertTo-Json -Depth 3
    $bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($body)
    $resp = Invoke-RestMethod -Uri "$D1_URL/query" -Method Post `
        -Headers @{ Authorization = "Bearer $CF_API_TOKEN" } `
        -Body $bodyBytes -ContentType "application/json; charset=utf-8"
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
    [int]$chunk = 150
    [int]$i = 0
    while ($i -lt $total) {
        [int]$end  = [Math]::Min($i + $chunk - 1, $total - 1)
        $slice = $rowArray[$i..$end]
        $vals  = ($slice | ForEach-Object {
            "(" + (($_ | ForEach-Object { EscSql $_ }) -join ",") + ")"
        }) -join ","
        $sql = "INSERT OR IGNORE INTO $table ($($columns -join ',')) VALUES $vals"
        try { Invoke-D1Query $sql | Out-Null }
        catch { Write-Warning "Insert failed at row $i`: $_" }
        $i += $chunk
        Start-Sleep -Milliseconds 120
    }
}

function Get-LastTimestamp {
    try {
        $res = Invoke-D1Query "SELECT MAX(alert_ts) AS last_ts FROM tg_alerts"
        $val = $res[0].results[0].last_ts
        if ($val) { return [datetime]$val } else { return $OP_START }
    } catch { return $OP_START }
}

function Strip-Html([string]$s) {
    [regex]::Replace($s, '<[^>]+>', ' ').Trim() -replace '\s+', ' '
}

# Incident clustering: region-aware (north=15s shelter, other=90s, UAV=go-immediately)
# Hebrew built from code points to avoid PS1 encoding issues
$KAV_IMUT = -join [char[]](0x05E7,0x05D5,0x20,0x05D4,0x05E2,0x05D9,0x05DE,0x05D5,0x05EA)

$INCIDENT_GAP = @{
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
$INCIDENT_GAP_DEFAULT = @{ n=15; o=90 }

$INCIDENT_MAX_DURATION = @{
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
$INCIDENT_MAX_DURATION_DEFAULT = @{ n=25; o=180 }

function Set-IncidentIds {
    Write-Host ""
    Write-Host "=== Computing incident IDs (region-aware) ==="

    # Step 1a: north msg_ids — LIKE query, Hebrew built at runtime to avoid encoding issues
    $likeP = "%" + $KAV_IMUT + "%"
    $northRes = Invoke-D1Query ("SELECT DISTINCT msg_id FROM tg_alerts WHERE region LIKE '" + $likeP + "'")
    $northSet = [System.Collections.Generic.HashSet[long]]::new()
    @($northRes[0].results) | ForEach-Object { [void]$northSet.Add([long]$_.msg_id) }
    Write-Host ("  North msg_ids: " + $northSet.Count)

    # Step 1b: all siren events
    $res     = Invoke-D1Query "SELECT msg_id, MIN(alert_ts) AS first_ts, alert_type FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown') GROUP BY msg_id ORDER BY alert_type, first_ts"
    $msgRows = @($res[0].results) | Sort-Object alert_type, first_ts
    Write-Host ("  Events: " + $msgRows.Count)

    # Step 2: cluster
    $assignments = @{}
    $lastTs      = @{}
    $incStart    = @{}
    $incStartDt  = @{}

    foreach ($row in $msgRows) {
        $at   = $row.alert_type
        $ts   = [datetime]$row.first_ts
        $tier = if ($northSet.Contains([long]$row.msg_id)) { "n" } else { "o" }
        $key  = $at + "|" + $tier

        $g = if ($INCIDENT_GAP.ContainsKey($at)) { $INCIDENT_GAP[$at][$tier] } else { $INCIDENT_GAP_DEFAULT[$tier] }
        $m = if ($INCIDENT_MAX_DURATION.ContainsKey($at)) { $INCIDENT_MAX_DURATION[$at][$tier] } else { $INCIDENT_MAX_DURATION_DEFAULT[$tier] }

        $newInc = (
            -not $lastTs.ContainsKey($key) -or
            ($ts - $lastTs[$key]).TotalSeconds -gt $g -or
            ($incStartDt.ContainsKey($key) -and ($ts - $incStartDt[$key]).TotalSeconds -gt $m)
        )

        if ($newInc) { $incStart[$key] = $row.first_ts; $incStartDt[$key] = $ts }
        $lastTs[$key] = $ts
        $assignments[$row.msg_id] = $at + "|" + $tier + "|" + $incStart[$key]
    }

    $unique = ($assignments.Values | Sort-Object -Unique).Count
    Write-Host ("  -> " + $unique + " unique incidents across " + $assignments.Count + " events")

    # Step 3: batch UPDATE
    $allMsgIds = @($assignments.Keys)
    [int]$total = $allMsgIds.Count
    [int]$chunk = 200
    [int]$i = 0; [int]$done = 0

    while ($i -lt $total) {
        [int]$end = [Math]::Min($i + $chunk - 1, $total - 1)
        $slice = $allMsgIds[$i..$end]
        $cases = ($slice | ForEach-Object {
            $safeId = $assignments[$_] -replace "'", "''"
            "WHEN " + $_ + " THEN '" + $safeId + "'"
        }) -join " "
        $inList = $slice -join ","
        $sql = "UPDATE tg_alerts SET incident_id = CASE msg_id " + $cases + " ELSE incident_id END WHERE msg_id IN (" + $inList + ")"
        try { Invoke-D1Query $sql | Out-Null }
        catch { Write-Warning ("Batch " + $i + " failed: " + $_) }
        $done += $slice.Count
        if ($done % 1000 -eq 0 -or $done -eq $total) { Write-Host ("  ..." + $done + "/" + $total) }
        $i += $chunk
        Start-Sleep -Milliseconds 150
    }

    Write-Host "  incident_id populated."
    Write-Host ""
}

# ---- Main --------------------------------------------------------------------

$since = Get-LastTimestamp
Write-Host "Last record in D1 : $since"
Write-Host "Scraping t.me/s/$CHANNEL back to $($since.ToString('yyyy-MM-dd')) ..."

$beforeId = $null
$done     = $false
$pageNum  = 0
$totalNew = 0
$seen     = [System.Collections.Generic.HashSet[string]]::new()

$sectionRx = [System.Text.RegularExpressions.Regex]::new(
    '<strong>([^<]+)</strong><br>((?:(?!<br><br>).)*)',
    [System.Text.RegularExpressions.RegexOptions]::Singleline
)

$COLS = @("id","msg_id","alert_ts","alert_type","title","city","region","category")

while (-not $done) {
    $url = "https://t.me/s/$CHANNEL"
    if ($beforeId) { $url += "?before=$beforeId" }
    $pageNum++
    Write-Host "  Page $pageNum (before=$beforeId)" -NoNewline

    try {
        $wc = New-Object System.Net.WebClient
        $wc.Headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        $wc.Encoding = [System.Text.Encoding]::UTF8
        $html = $wc.DownloadString($url)
    } catch {
        Write-Host " FETCH ERROR: $_"
        break
    }

    $blocks = ($html -split '(?=<div class="tgme_widget_message_wrap)') |
              Where-Object { $_ -match 'data-post=' }

    Write-Host " -- $($blocks.Count) blocks" -NoNewline

    if ($blocks.Count -eq 0) { Write-Host ""; break }

    $minId = [int]::MaxValue
    $rows  = [System.Collections.ArrayList]::new()

    foreach ($block in $blocks) {
        $idM = [regex]::Match($block, 'data-post="[^/]+/(\d+)"')
        if (-not $idM.Success) { continue }
        $msgId = [int]$idM.Groups[1].Value
        if ($msgId -lt $minId) { $minId = $msgId }

        $dtM = [regex]::Match($block, 'datetime="([^"]+)"')
        if (-not $dtM.Success) { continue }
        try   { $msgDate = [datetime]::Parse($dtM.Groups[1].Value).ToUniversalTime().AddHours(3) }
        catch { continue }

        if ($msgDate -le $since)    { $done = $true; continue }
        if ($msgDate -lt $OP_START) { $done = $true; continue }

        $textM = [regex]::Match($block, '<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)</div>')
        if (-not $textM.Success) { continue }
        $rawHtml = $textM.Groups[1].Value.Trim()
        if ($rawHtml.Length -lt 10) { continue }

        $rawHtml = $rawHtml -replace '<br\s*/?>', '<br>'
        $rawHtml = $rawHtml -replace '<b>',  '<strong>'
        $rawHtml = $rawHtml -replace '</b>', '</strong>'

        $alertTs = $msgDate.ToString("yyyy-MM-dd HH:mm:ss")

        # Main header (e.g. "צבע אדום" or "מבזק" or "עדכון")
        $typeM   = [regex]::Match($rawHtml, '<strong>([^\d<(]+?)\s*\(\d+/\d+/\d+\)')
        $msgType = if ($typeM.Success) { $typeM.Groups[1].Value.Trim() } else { "" }

        $alertType = Get-AlertType $msgType
        $catNum    = Get-Category $alertType

        # ── All-clear: store one row per message, no city breakdown ──
        if ($alertType -eq "all_clear") {
            $rowId = "$msgId"
            if ($seen.Add($rowId)) {
                [void]$rows.Add(@($rowId, $msgId, $alertTs, $alertType, $msgType, "", "", $catNum))
            }
            continue
        }

        # ── Unknown / empty header: skip ──
        if ($alertType -eq "unknown") { continue }

        # ── Regular alert: parse sections → city rows ──
        # Strip the message header (everything up to the first blank line <br><br>)
        # so the header section doesn't absorb the first region's cities as its content.
        $bodyHtml = [regex]::Replace($rawHtml, '^[\s\S]*?<br><br>', '', 'Singleline')
        foreach ($m in $sectionRx.Matches($bodyHtml)) {
            $sectionTitle = [System.Net.WebUtility]::HtmlDecode($(Strip-Html $m.Groups[1].Value.Trim()))

            if ($sectionTitle -match '\d+/\d+/\d+') { continue }   # date header
            if ($sectionTitle -match $PAT_ADVISORY)  { continue }   # instructions

            $isRegion  = $sectionTitle -match $PAT_REGION_PFX
            $recTitle  = if ($isRegion) { $msgType }      else { $sectionTitle }
            $recRegion = if ($isRegion) { $sectionTitle } else { "" }

            $citiesHtml = $m.Groups[2].Value
            $citiesHtml = $citiesHtml -replace '<br>', ','
            $citiesHtml = [regex]::Replace($citiesHtml, '\(<strong>[^<]*</strong>\)', '')
            $citiesHtml = [regex]::Replace($citiesHtml, '<[^>]+>', ' ')
            $citiesHtml = ($citiesHtml -replace '[\r\n\t]+', ' ').Trim()
            # Decode HTML entities so city names like ע&#39;ג&#39;ר are stored correctly
            $citiesHtml = [System.Net.WebUtility]::HtmlDecode($citiesHtml)

            $cities = $citiesHtml -split '[,،]' |
                ForEach-Object { $_.Trim() -replace '\s{2,}', ' ' } |
                Where-Object   { $_.Length -ge 2 -and $_.Length -le 50 -and $_ -notmatch $PAT_ADVISORY }

            foreach ($city in $cities) {
                $rowId = "$msgId|$city"
                if ($seen.Add($rowId)) {
                    [void]$rows.Add(@($rowId, $msgId, $alertTs, $alertType, $recTitle, $city, $recRegion, $catNum))
                }
            }
        }
    }

    Write-Host " -- $($rows.Count) new rows"

    if ($rows.Count -gt 0) {
        Invoke-D1BulkInsert "tg_alerts" $COLS $rows
        $totalNew += $rows.Count
    }

    if ($done -or $minId -eq [int]::MaxValue) { break }
    $beforeId = $minId
    Start-Sleep -Milliseconds 600
}

Write-Host ""
Write-Host "Done. Inserted $totalNew new rows."

$count     = (Invoke-D1Query "SELECT COUNT(*)           AS n  FROM tg_alerts")[0].results[0].n
$events    = (Invoke-D1Query "SELECT COUNT(DISTINCT msg_id) AS n  FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown')")[0].results[0].n
$allClears = (Invoke-D1Query "SELECT COUNT(*)           AS n  FROM tg_alerts WHERE alert_type = 'all_clear'")[0].results[0].n
$latest    = (Invoke-D1Query "SELECT MAX(alert_ts)      AS ts FROM tg_alerts")[0].results[0].ts
$oldest    = (Invoke-D1Query "SELECT MIN(alert_ts)      AS ts FROM tg_alerts")[0].results[0].ts
Write-Host "D1 tg_alerts : $count rows | $events unique siren events | $allClears all-clears"
Write-Host "Date range   : $oldest -> $latest"

# Alert type breakdown
Write-Host "`nBreakdown by alert_type:"
$breakdown = (Invoke-D1Query "SELECT alert_type, COUNT(DISTINCT msg_id) AS n FROM tg_alerts GROUP BY alert_type ORDER BY n DESC")[0].results
$breakdown | ForEach-Object { Write-Host "  $($_.alert_type): $($_.n)" }

# Assign incident IDs (clusters nearby same-type events into one incident)
Set-IncidentIds

# Incident summary
Write-Host "Incident summary:"
$incSummary = (Invoke-D1Query "SELECT alert_type, COUNT(DISTINCT incident_id) AS incidents FROM tg_alerts WHERE alert_type NOT IN ('all_clear','unknown') GROUP BY alert_type ORDER BY incidents DESC")[0].results
$incSummary | ForEach-Object { Write-Host "  $($_.alert_type): $($_.incidents) incidents" }
