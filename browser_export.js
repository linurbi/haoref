/**
 * OREF History Browser Export — v2 (with diagnostics)
 * Run this in the DevTools Console while on:
 *   https://alerts-history.oref.org.il
 */
(async function () {
  // First, check we are on the right page
  console.log("Current page:", window.location.href);

  // Test one request and show raw result before processing
  console.log("Testing API with a single request...");
  try {
    const testResp = await fetch(
      "/Shared/Ajax/GetAlarmsHistory.aspx?lang=he&fromDate=01.04.2026&toDate=02.04.2026",
      { headers: { "X-Requested-With": "XMLHttpRequest" } }
    );
    console.log("Status:", testResp.status);
    const testText = await testResp.text();
    console.log("Response (first 300 chars):", testText.substring(0, 300));
    const testData = JSON.parse(testText);
    console.log("Records in test:", testData.length);
    if (testData.length > 0) {
      console.log("Sample record:", JSON.stringify(testData[0]));
    }
  } catch (e) {
    console.error("Test request FAILED:", e.message);
    return;
  }

  const results = [];
  const seen    = new Set();

  const fmt = d =>
    `${String(d.getDate()).padStart(2,"0")}.${String(d.getMonth()+1).padStart(2,"0")}.${d.getFullYear()}`;

  // Generate weekly chunks Feb 28 → today
  const start = new Date("2026-02-28");
  const end   = new Date();
  const ranges = [];
  let cur = new Date(start);
  while (cur <= end) {
    const from = new Date(cur);
    const to   = new Date(cur);
    to.setDate(to.getDate() + 6);
    if (to > end) to.setTime(end.getTime());
    ranges.push({ from: fmt(from), to: fmt(to) });
    cur.setDate(cur.getDate() + 7);
  }

  console.log(`Fetching ${ranges.length} weekly chunks...`);

  for (const range of ranges) {
    try {
      const resp = await fetch(
        `/Shared/Ajax/GetAlarmsHistory.aspx?lang=he&fromDate=${range.from}&toDate=${range.to}`,
        { headers: { "X-Requested-With": "XMLHttpRequest" } }
      );
      const text = await resp.text();
      const data = JSON.parse(text);
      let added = 0;
      for (const item of data) {
        const key = `${item.alertDate}||${item.data}`;
        if (!seen.has(key)) {
          results.push({
            alertDate: (item.alertDate || "").replace("T", " ").substring(0, 19),
            title:     item.category_desc || item.title || "",
            data:      item.data || "",
            category:  item.category || 0,
          });
          seen.add(key);
          added++;
        }
      }
      console.log(`${range.from}→${range.to}: ${data.length} returned, ${added} new (total: ${results.length})`);
    } catch (e) {
      console.warn(`${range.from}→${range.to}: ERROR - ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 400));
  }

  results.sort((a, b) => b.alertDate.localeCompare(a.alertDate));
  console.log("FINAL TOTAL:", results.length, "records");

  const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement("a"), { href: url, download: "oref_history.json" });
  document.body.appendChild(a);
  a.click();
  a.remove();
  console.log("File downloaded!");
})();
