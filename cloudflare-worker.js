/**
 * Cloudflare Worker — Alert Statistics API
 * ES Module format (required for D1 bindings)
 *
 * Bindings required (Worker Settings → Bindings):
 *   DB            — D1 Database (tg_alerts table)
 *   REDALERT_KEY  — Secret (Bearer token for redalert.orielhaim.com)
 */

const REDALERT_BASE        = "https://redalert.orielhaim.com";
const MOH_CASUALTIES       = "https://datadashboard.health.gov.il/api/war-casualties/totalCasualtiesByStatus";
const MOH_DAILY_CASUALTIES = "https://datadashboard.health.gov.il/api/war-casualties/dailyCasualties";
const OP_START             = "2026-02-28T00:00:00Z";

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Max-Age":       "86400",
};

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    const url      = new URL(request.url);
    const pathname = url.pathname;

    if (pathname.endsWith("/casualties"))       return handleMohEndpoint(MOH_CASUALTIES, 1800);
    if (pathname.endsWith("/daily-casualties")) return handleMohEndpoint(MOH_DAILY_CASUALTIES, 1800);
    if (pathname.endsWith("/tg-stats"))         return handleTgStats(url, env);
    if (pathname.endsWith("/incidents"))        return handleIncidents(url, env);
    if (pathname.endsWith("/cities"))           return handleCities(url, env);

    // Default: proxy RedAlert
    return handleRedAlert(url, env);
  }
};

// ── /tg-stats — query D1 Telegram data ───────────────────────────────────────
async function handleTgStats(url, env) {
  if (!env.DB) {
    return json({ error: "D1 binding 'DB' not configured" }, 503);
  }

  const startDate = url.searchParams.get("startDate") || OP_START;
  const endDate   = url.searchParams.get("endDate")   || "";

  const where = buildWhere(startDate, endDate);
  const cityFilter   = url.searchParams.get("city")   || "";
  const regionFilter = url.searchParams.get("region") || "";

  const cf  = cityFilter   ? ` AND city   = '${cityFilter.replace(/'/g,"''")}'`   : "";
  const rf  = regionFilter ? ` AND region = '${regionFilter.replace(/'/g,"''")}'` : "";
  const loc = cf + rf;   // combined location filter

  // af  = real operational alerts (excludes pre_alert/all_clear/unknown) — used for totals & charts
  // afA = all user-visible types (includes pre_alert) — used only for origin-type breakdown card
  const af  = " AND alert_type NOT IN ('all_clear','unknown','pre_alert')";
  const afA = " AND alert_type NOT IN ('all_clear','unknown')";

  try {
    // Strict real-alert filter for city ranking (rockets/UAV/ballistic/infiltration only)
    const realAf = " AND alert_type IN ('rockets','uav','ballistic','infiltration')";

    const [totRow, timelineRows, zoneRows, originRows, peakRow, cityRows, hourlyRows] = await Promise.all([
      // Totals counted by incident (one launch = one incident, regardless of cities hit)
      env.DB.prepare(`SELECT
          COUNT(DISTINCT incident_id) AS range_events,
          COUNT(*)                    AS city_activations,
          COUNT(DISTINCT city)        AS unique_cities,
          COUNT(DISTINCT region)      AS unique_zones
        FROM tg_alerts ${where}${af}${loc}`).first(),

      env.DB.prepare(`SELECT DATE(alert_ts) AS period,
          COUNT(DISTINCT incident_id) AS count
        FROM tg_alerts ${where}${af}${loc}
        GROUP BY DATE(alert_ts) ORDER BY period`).all(),

      env.DB.prepare(`SELECT region AS zone,
          COUNT(DISTINCT incident_id) AS count
        FROM tg_alerts ${where}${af}${loc} AND region != ''
        GROUP BY region ORDER BY count DESC LIMIT 20`).all(),

      // Origin breakdown by incident count
      env.DB.prepare(`SELECT alert_type,
          COUNT(DISTINCT incident_id) AS count
        FROM tg_alerts ${where}${afA}${loc}
        GROUP BY alert_type ORDER BY count DESC`).all(),

      env.DB.prepare(`SELECT SUBSTR(alert_ts,1,13)||':00:00Z' AS period,
          COUNT(DISTINCT incident_id) AS count
        FROM tg_alerts ${where}${af}${loc}
        GROUP BY SUBSTR(alert_ts,1,13) ORDER BY count DESC LIMIT 1`).first(),

      // Per (city, zone): rank by total incidents, then unique days
      env.DB.prepare(`SELECT city, region AS zone,
          COUNT(DISTINCT DATE(alert_ts))                                                       AS count,
          COUNT(DISTINCT incident_id)                                                          AS incidents,
          COUNT(DISTINCT CASE WHEN alert_type='rockets'      THEN incident_id END)             AS rockets,
          COUNT(DISTINCT CASE WHEN alert_type='uav'          THEN incident_id END)             AS uav,
          COUNT(DISTINCT CASE WHEN alert_type='ballistic'    THEN incident_id END)             AS ballistic,
          COUNT(DISTINCT CASE WHEN alert_type='infiltration' THEN incident_id END)             AS infiltration
        FROM tg_alerts ${where}${realAf}${loc} AND city != ''
        GROUP BY city, region ORDER BY incidents DESC, count DESC LIMIT 15`).all(),

      env.DB.prepare(`SELECT CAST(strftime('%H', alert_ts) AS INTEGER) AS hour,
          COUNT(DISTINCT incident_id) AS count
        FROM tg_alerts ${where}${af}${loc}
        GROUP BY hour ORDER BY hour`).all(),
    ]);

    const result = {
      source:       "d1-telegram",
      filterCity:   cityFilter   || null,
      filterRegion: regionFilter || null,
      uniqueCities: totRow.unique_cities,
      uniqueZones:  totRow.unique_zones,
      totals:       { range: totRow.range_events, cityActivations: totRow.city_activations },
      timeline:     timelineRows.results,
      topZones:     zoneRows.results,
      topOrigins:   originRows.results.map(r => ({ origin: r.alert_type, count: r.count })),
      topCities: cityRows.results,   // rockets+uav combined, per (city,zone)
      peak:      peakRow || {},
      hourly:    hourlyRows.results,
    };

    return json(result, 200, "max-age=120, s-maxage=120");
  } catch (e) {
    return json({ error: e.message }, 503);
  }
}

// ── /cities — all distinct (city, region) combos for autocomplete ─────────────
async function handleCities(url, env) {
  if (!env.DB) return json({ error: "D1 binding 'DB' not configured" }, 503);
  const where = buildWhere(OP_START, "");
  try {
    const rows = await env.DB.prepare(
      `SELECT DISTINCT city, region FROM tg_alerts ${where}
         AND city != ''
         AND alert_type IN ('rockets','uav','ballistic','infiltration')
       ORDER BY city`
    ).all();
    return json({ cities: rows.results }, 200, "max-age=3600, s-maxage=3600");
  } catch (e) {
    return json({ error: e.message }, 503);
  }
}

// ── /incidents — proxy RedAlert incident analysis ────────────────────────────
async function handleIncidents(url, env) {
  const params = new URLSearchParams(url.search);
  const apiUrl = REDALERT_BASE + "/api/stats/incidents?" + params.toString();
  try {
    const resp = await fetch(apiUrl, {
      headers: { "Authorization": "Bearer " + env.REDALERT_KEY, "Accept": "application/json" },
    });
    if (!resp.ok) throw new Error("RedAlert incidents API " + resp.status);
    const body = await resp.text();
    return new Response(body, {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "max-age=300, s-maxage=300" },
    });
  } catch (e) {
    return json({ error: e.message }, 503);
  }
}

// ── Default: proxy RedAlert summary ──────────────────────────────────────────
async function handleRedAlert(url, env) {
  const startDate = url.searchParams.get("startDate") || OP_START;
  const endDate   = url.searchParams.get("endDate")   || "";

  const apiUrl = REDALERT_BASE + "/api/stats/summary"
    + "?startDate=" + encodeURIComponent(startDate)
    + (endDate ? "&endDate=" + encodeURIComponent(endDate) : "")
    + "&include=topCities,topZones,topOrigins,timeline,peak"
    + "&timelineGroup=day&topLimit=15";

  try {
    const resp = await fetch(apiUrl, {
      headers: { "Authorization": "Bearer " + env.REDALERT_KEY, "Accept": "application/json" },
    });
    if (!resp.ok) {
      const errBody = await resp.text().catch(() => "");
      throw new Error("RedAlert API " + resp.status + ": " + errBody.slice(0, 200));
    }
    const body = await resp.text();
    return new Response(body, {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "max-age=60, s-maxage=60" },
    });
  } catch (e) {
    return json({ error: e.message }, 503);
  }
}

// ── MOH endpoints ─────────────────────────────────────────────────────────────
async function handleMohEndpoint(apiUrl, cacheSec) {
  try {
    const resp = await fetch(apiUrl, {
      headers: { "Accept": "application/json", "Referer": "https://datadashboard.health.gov.il/" },
    });
    if (!resp.ok) throw new Error("MOH API " + resp.status);
    const body = await resp.text();
    return new Response(body, {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json; charset=utf-8",
                 "Cache-Control": `max-age=${cacheSec}, s-maxage=${cacheSec}` },
    });
  } catch (e) {
    return json({ error: e.message }, 503);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function buildWhere(startDate, endDate) {
  let w = "WHERE alert_ts >= '" + startDate.replace("T", " ").slice(0, 19) + "'";
  if (endDate) w += " AND alert_ts <= '" + endDate.replace("T", " ").slice(0, 19) + "'";
  return w;
}

function json(data, status, cacheControl) {
  const headers = { ...CORS, "Content-Type": "application/json; charset=utf-8" };
  if (cacheControl) headers["Cache-Control"] = cacheControl;
  return new Response(JSON.stringify(data), { status, headers });
}
