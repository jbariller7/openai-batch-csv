exports.config = { /* path: "/api/direct-cancel" */ };

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") return { statusCode: 204, headers: { "Access-Control-Allow-Origin": "*" }, body: "" };

  const urlStr = typeof event?.rawUrl === "string" ? event.rawUrl : "";
  const url = urlStr ? new URL(urlStr) : null;
  const id = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";

  if (!id) return { statusCode: 400, body: JSON.stringify({ error: "Missing id" }) };

  const { getStore } = await import("@netlify/blobs");
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
  const store  = (siteID && token) ? getStore({ name: "openai-batch-csv", siteID, token }) : getStore("openai-batch-csv");

  try {
    let statusJson = null;
    try { statusJson = await store.get(`jobs/${id}.status.json`, { type: "json" }); } catch {}
    
    if (statusJson) {
      statusJson.status = "cancelled";
      statusJson.events = statusJson.events || [];
      statusJson.events.push({ ts: new Date().toISOString(), msg: "Job aborted by user." });
      await store.set(`jobs/${id}.status.json`, JSON.stringify(statusJson), { contentType: "application/json" });
    }
    return { statusCode: 200, headers: { "Access-Control-Allow-Origin": "*" }, body: JSON.stringify({ ok: true, id, status: "cancelled" }) };
  } catch (e) {
    return { statusCode: 500, body: JSON.stringify({ error: e?.message || String(e) }) };
  }
};
