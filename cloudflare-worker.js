/**
 * Cloudflare Worker — RedAlert Statistics API proxy
 * Requires REDALERT_KEY secret binding set in Worker Settings → Variables and Secrets
 */

const REDALERT_BASE  = "https://redalert.orielhaim.com";
const MOH_CASUALTIES = "https://datadashboard.health.gov.il/api/war-casualties/totalCasualtiesByStatus";
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

  // ── Route: /casualties — proxy MOH Health Ministry data ──
  if (pathname.endsWith("/casualties")) {
    return handleCasualties();
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

async function handleCasualties() {
  try {
    var resp = await fetch(MOH_CASUALTIES, {
      headers: { "Accept": "application/json", "Referer": "https://datadashboard.health.gov.il/" },
    });
    if (!resp.ok) throw new Error("MOH API " + resp.status);
    var body = await resp.text();
    return new Response(body, {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "max-age=1800, s-maxage=1800" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 503,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }
}
