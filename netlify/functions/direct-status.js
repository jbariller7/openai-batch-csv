// netlify/functions/direct-status.js (CommonJS + Lambda-style)

exports.config = { /* path: "/api/direct-status" */ };

function res(statusCode, bodyObj) {
  return { statusCode, headers: { "Content-Type": "application/json" }, body: JSON.stringify(bodyObj ?? {}) };
}

exports.handler = async function (event) {
  const urlStr = typeof event?.rawUrl === "string" ? event.rawUrl : "";
  const url = urlStr ? new URL(urlStr) : null;
  const id = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";

  if (!id) return res(400, { error: "Missing id" });

  const { getStore } = await import("@netlify/blobs");
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
const store  = (siteID && token)
  ? getStore({ name: "openai-batch-csv", siteID, token })
  : getStore("openai-batch-csv");

  try {
    let statusJson = null;
    try { statusJson = await store.get(`jobs/${id}.status.json`, { type: "json" }); } catch {}

    let csvExists = false;
    if (!statusJson || statusJson.status !== "ready") {
      try { csvExists = typeof (await store.get(`results/${id}.csv`, { type: "text" })) === "string"; } catch {}
    }

    const ready = statusJson?.status === "ready" || csvExists;
    const status = ready ? "ready" : (statusJson?.status || "running");
    const error = statusJson?.error || null;

    return res(200, { id, ready, status, error });
  } catch (e) {
    return res(500, { error: e?.message || String(e) });
  }
};
