/**
 * Netlify Function — OREF proxy
 * Accessible at: /.netlify/functions/oref
 *
 * Mirrors the exact headers used by the working Python/urllib3 implementation.
 * Tries the history endpoint first, falls back to the real-time endpoint.
 */
const https = require("https");
const zlib  = require("zlib");

// Exact headers from the working Python project (nothing extra that could trigger bot-detection)
const HEADERS = {
  "Referer":           "https://www.oref.org.il/",
  "User-Agent":        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
  "X-Requested-With":  "XMLHttpRequest",
};

function httpsGet(path) {
  return new Promise((resolve) => {
    const req = https.get(
      { hostname: "www.oref.org.il", path, headers: HEADERS },
      (res) => {
        const enc = res.headers["content-encoding"];
        let stream = res;
        if      (enc === "gzip")    stream = res.pipe(zlib.createGunzip());
        else if (enc === "deflate") stream = res.pipe(zlib.createInflate());
        else if (enc === "br")      stream = res.pipe(zlib.createBrotliDecompress());

        const chunks = [];
        stream.on("data",  c   => chunks.push(c));
        stream.on("end",   ()  => resolve({
          status:  res.statusCode,
          headers: res.headers,
          body:    Buffer.concat(chunks).toString("utf-8"),
        }));
        stream.on("error", err => resolve({ status: 0, body: "", error: err.message }));
      }
    );
    req.on("error", err => resolve({ status: 0, body: "", error: err.message }));
    req.setTimeout(12000, () => { req.destroy(); resolve({ status: 0, body: "", error: "timeout" }); });
  });
}

exports.handler = async function () {
  // ── Attempt 1: full alert history ─────────────────────────────────────────
  const history = await httpsGet(
    "/warningMessages/alert/History/AlertsHistory.json"
  );

  if (history.status === 200 && history.body && !history.body.includes("<HTML>")) {
    return {
      statusCode: 200,
      headers: {
        "Content-Type":                "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control":               "no-cache, no-store",
        "X-Oref-Source":               "history",
      },
      body: history.body,
    };
  }

  console.warn("[oref fn] history endpoint blocked:", history.status, history.body.slice(0, 120));

  // ── Attempt 2: real-time alerts.json (same endpoint the Python project uses)
  //    Returns only currently ACTIVE alerts — still useful as a connectivity check
  const realtime = await httpsGet("/WarningMessages/alert/alerts.json");

  if (realtime.status === 200 && realtime.body) {
    return {
      statusCode: 200,
      headers: {
        "Content-Type":                "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control":               "no-cache, no-store",
        "X-Oref-Source":               "realtime",
      },
      body: realtime.body,
    };
  }

  // ── Both blocked — return the raw error so the client can display it ───────
  return {
    statusCode: history.status || realtime.status || 500,
    headers: {
      "Content-Type":                "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify({
      error: "OREF blocked both endpoints from Netlify IPs",
      historyStatus:   history.status,
      realtimeStatus:  realtime.status,
      historySnippet:  history.body.slice(0, 200),
    }),
  };
};
