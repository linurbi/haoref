/**
 * Cloudflare Worker — RedAlert Statistics API proxy
 * Requires REDALERT_KEY secret binding set in Worker Settings → Variables and Secrets
 */

const REDALERT_BASE = "https://redalert.orielhaim.com";
const OP_START      = "2026-02-28T00:00:00Z";  // Operation start date

addEventListener("fetch", function(event) {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  if (request.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Max-Age":       "86400",
      },
    });
  }

  var url       = new URL(request.url);
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
      headers: {
        "Authorization": "Bearer " + REDALERT_KEY,
        "Accept":        "application/json",
      },
      cf: { cacheEverything: false },
    });

    if (!resp.ok) throw new Error("RedAlert API returned " + resp.status);
    var body = await resp.text();

    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type":                "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control":               "max-age=60, s-maxage=60",
        "X-Oref-Source":               "redalert",
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 503,
      headers: {
        "Content-Type":                "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }
}
