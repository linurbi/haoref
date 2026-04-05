/**
 * Cloudflare Worker — OREF alert history proxy
 * Uses legacy "Service Worker" syntax (works with the default Cloudflare editor)
 *
 * Tries multiple URL strategies to get the most complete history possible.
 */

// ── Set this to your Gist ID once accumulate.py has uploaded data ──
// Example: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
var GIST_ID      = "972798220ff080e050a3a4a0d386b3e0";
var GIST_OWNER   = "linurbi";

addEventListener("fetch", function (event) {
  event.respondWith(handleRequest(event.request));
});

var HEADERS = {
  "Referer":           "https://www.oref.org.il/",
  "X-Requested-With":  "XMLHttpRequest",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Accept":            "application/json, text/javascript, */*; q=0.01",
  "Accept-Language":   "he-IL,he;q=0.9,en-US;q=0.8",
};

async function orefFetch(url) {
  var resp = await fetch(url, { headers: HEADERS });
  var body = await resp.text();
  // Reject Akamai "Access Denied" HTML
  if (body.includes("<HTML>") || body.includes("Access Denied")) {
    throw new Error("blocked: " + resp.status);
  }
  return { status: resp.status, body: body };
}

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

  var url          = new URL(request.url);
  var fromDateParam = url.searchParams.get("fromDate"); // e.g. "02.04.2026" (DD.MM.YYYY)

  var errors = [];

  // ── Strategy 0: GitHub Gist — only used when no fromDate filter is requested ─
  if (GIST_ID && GIST_OWNER && !fromDateParam) {
    try {
      // Use the Gist API to get the current raw_url (includes revision hash → no CDN staleness)
      var metaResp = await fetch("https://api.github.com/gists/" + GIST_ID, {
        headers: { Accept: "application/vnd.github+json" },
        cf: { cacheEverything: false },
      });
      var meta = await metaResp.json();
      var files = meta.files || {};
      async function fetchGistFile(name) {
        var f = files[name];
        if (!f || !f.raw_url) return [];
        var r = await fetch(f.raw_url, { cf: { cacheEverything: false } });
        var j = JSON.parse(await r.text());
        return Array.isArray(j) ? j : [];
      }
      var parts = await Promise.all([
        fetchGistFile("oref_history_1.json"),
        fetchGistFile("oref_history_2.json"),
      ]);
      var combined = parts[0].concat(parts[1]);
      if (combined.length > 0) {
        return new Response(JSON.stringify(combined), {
          status: 200,
          headers: corsHeaders("gist:" + combined.length),
        });
      }
    } catch (e) { errors.push("gist: " + e.message); }
  }

  // ── Strategy 1: AlertsHistory with date range ─────────────────────────────
  try {
    var today   = new Date();
    var dd      = String(today.getDate()).padStart(2, "0");
    var mm      = String(today.getMonth() + 1).padStart(2, "0");
    var yyyy    = today.getFullYear();
    var toDate  = dd + "." + mm + "." + yyyy;
    var fromDate = fromDateParam || "28.02.2026"; // caller can override start date

    var rangeUrl = "https://www.oref.org.il/warningMessages/alert/History/AlertsHistory.json" +
                   "?fromDate=" + fromDate + "&toDate=" + toDate;

    var r1 = await orefFetch(rangeUrl);
    var parsed1 = JSON.parse(r1.body);
    if (Array.isArray(parsed1) && parsed1.length > 0) {
      return new Response(r1.body, {
        status: 200,
        headers: corsHeaders("range:" + parsed1.length),
      });
    }
  } catch (e) { errors.push("range: " + e.message); }

  // ── Strategy 2: Plain AlertsHistory (rolling recent window) ─────────────────
  try {
    var r2 = await orefFetch(
      "https://www.oref.org.il/warningMessages/alert/History/AlertsHistory.json"
    );
    if (r2.body && r2.body.trim().startsWith("[")) {
      return new Response(r2.body, {
        status: 200,
        headers: corsHeaders("plain"),
      });
    }
  } catch (e) { errors.push("plain: " + e.message); }

  // ── Strategy 3: Alternate capitalisation path ────────────────────────────────
  try {
    var r3 = await orefFetch(
      "https://www.oref.org.il/WarningMessages/History/AlertsHistory.json"
    );
    if (r3.body && r3.body.trim().startsWith("[")) {
      return new Response(r3.body, {
        status: 200,
        headers: corsHeaders("alt"),
      });
    }
  } catch (e) { errors.push("alt: " + e.message); }

  // All failed
  return new Response(JSON.stringify({ error: "all strategies failed", details: errors }), {
    status: 503,
    headers: {
      "Content-Type":                "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  });
}

function corsHeaders(source) {
  return {
    "Content-Type":                "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Cache-Control":               "no-cache, no-store",
    "X-Oref-Source":               source,
  };
}
