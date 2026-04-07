/**
 * Cloudflare Worker — RedAlert Statistics API proxy
 * Requires REDALERT_KEY secret binding set in Worker Settings → Variables and Secrets
 */

const REDALERT_BASE  = "https://redalert.orielhaim.com";
const MOH_CASUALTIES       = "https://datadashboard.health.gov.il/api/war-casualties/totalCasualtiesByStatus";
const MOH_DAILY_CASUALTIES = "https://datadashboard.health.gov.il/api/war-casualties/dailyCasualties";
const OP_START       = "2026-02-28T00:00:00Z";

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Max-Age":       "86400",
};

addEventListener("fetch", function(event) {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: CORS });
  }

  var url      = new URL(request.url);
  var pathname = url.pathname;

  // ── Route: /casualties — MOH aggregate totals ──
  if (pathname.endsWith("/casualties")) {
    return handleCasualties();
  }

  // ── Route: /daily-casualties — MOH daily time-series ──
  if (pathname.endsWith("/daily-casualties")) {
    return handleMohEndpoint(MOH_DAILY_CASUALTIES, 1800);
  }

  // ── Route: /tg-stats — stats computed from Telegram data in D1 ──
  if (pathname.endsWith("/tg-stats")) {
    return handleTgStats(request, url);
  }

  // ── Route: default — proxy RedAlert stats ──
  var startDate = url.searchParams.get("startDate") || OP_START;
  var endDate   = url.searchParams.get("endDate")   || "";

  var apiUrl = REDALERT_BASE + "/api/stats/summary"
    + "?startDate=" + encodeURIComponent(startDate)
    + (endDate ? "&endDate=" + encodeURIComponent(endDate) : "")
    + "&include=topCities,topZones,topOrigins,timeline,peak"
    + "&timelineGroup=day"
    + "&topLimit=15";

  try {
    var resp = await fetch(apiUrl, {
      headers: { "Authorization": "Bearer " + REDALERT_KEY, "Accept": "application/json" },
      cf: { cacheEverything: false },
    });
    if (!resp.ok) {
      var errBody = await resp.text().catch(() => "");
      throw new Error("RedAlert API " + resp.status + ": " + errBody.slice(0, 300));
    }
    var body = await resp.text();
    return new Response(body, {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "max-age=60, s-maxage=60" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 503,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }
}

async function handleTgStats(request, url) {
  // Requires D1 binding named DB (set in Worker Settings → Bindings → D1)
  if (typeof DB === "undefined") {
    return new Response(JSON.stringify({ error: "D1 binding 'DB' not configured" }), {
      status: 503, headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  var startDate = url.searchParams.get("startDate") || OP_START;
  var endDate   = url.searchParams.get("endDate")   || "";
  var where     = "WHERE alert_ts >= '" + startDate.replace("T", " ").slice(0, 19) + "'";
  if (endDate) where += " AND alert_ts <= '" + endDate.replace("T", " ").slice(0, 19) + "'";

  try {
    // All queries in parallel
    var [totRow, timelineRows, zoneRows, originRows, peakRow, cityRows] = await Promise.all([
      // totals
      DB.prepare("SELECT COUNT(DISTINCT alert_ts||'|'||title) AS range_events, COUNT(*) AS city_activations FROM tg_alerts " + where).first(),
      // daily timeline — distinct events per day
      DB.prepare("SELECT DATE(alert_ts) AS period, COUNT(DISTINCT alert_ts||'|'||title) AS count FROM tg_alerts " + where + " GROUP BY DATE(alert_ts) ORDER BY period").all(),
      // top zones — distinct events per region (only rows with region filled)
      DB.prepare("SELECT region AS zone, COUNT(DISTINCT alert_ts||'|'||title) AS count FROM tg_alerts " + where + " AND region != '' GROUP BY region ORDER BY count DESC LIMIT 20").all(),
      // top origins (categories) — category label + event count
      DB.prepare("SELECT category, COUNT(DISTINCT alert_ts||'|'||title) AS count FROM tg_alerts " + where + " GROUP BY category ORDER BY count DESC").all(),
      // peak hour
      DB.prepare("SELECT SUBSTR(alert_ts,1,13)||':00:00Z' AS period, COUNT(DISTINCT alert_ts||'|'||title) AS count FROM tg_alerts " + where + " GROUP BY SUBSTR(alert_ts,1,13) ORDER BY count DESC LIMIT 1").first(),
      // top 15 cities
      DB.prepare("SELECT city, region AS zone, COUNT(DISTINCT alert_ts||'|'||title) AS count FROM tg_alerts " + where + " GROUP BY city ORDER BY count DESC LIMIT 15").all(),
    ]);

    var catLabel = { 1: "rockets", 2: "uav", 3: "ballistic", 4: "pre_alert" };
    var result = {
      source:      "d1-telegram",
      totals:      { range: totRow.range_events, cityActivations: totRow.city_activations },
      timeline:    timelineRows.results,
      topZones:    zoneRows.results,
      topOrigins:  originRows.results.map(function(r) { return { origin: catLabel[r.category] || String(r.category), count: r.count }; }),
      topCities:   cityRows.results,
      peak:        peakRow || {},
    };

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "max-age=120, s-maxage=120" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 503, headers: { ...CORS, "Content-Type": "application/json" },
    });
  }
}

async function handleCasualties() {
  return handleMohEndpoint(MOH_CASUALTIES, 1800);
}

async function handleMohEndpoint(url, cacheSec) {
  try {
    var resp = await fetch(url, {
      headers: { "Accept": "application/json", "Referer": "https://datadashboard.health.gov.il/" },
    });
    if (!resp.ok) throw new Error("MOH API " + resp.status);
    var body = await resp.text();
    return new Response(body, {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8",
                 "Cache-Control": "max-age=" + cacheSec + ", s-maxage=" + cacheSec },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 503,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }
}
