// netlify/functions/direct-status.js (CommonJS + Lambda-style, returns progress + events)

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

    // Fallback: ready if CSV already exists
    let csvExists = false;
    if (!statusJson || statusJson.status !== "ready") {
      try { csvExists = typeof (await store.get(`results/${id}.csv`, { type: "text" })) === "string"; } catch {}
    }

    const ready = (statusJson?.status === "ready") || csvExists;
    const status = ready ? "ready" : (statusJson?.status || "running");

    return res(200, {
      id,
      ready,
      status,
      error: statusJson?.error || null,
      model: statusJson?.model || null,
      rowCount: statusJson?.rowCount || null,
      completedChunks: statusJson?.completedChunks || 0,
      totalChunks: statusJson?.totalChunks || null,
      processedRows: statusJson?.processedRows || null,
      updatedAt: statusJson?.updatedAt || null,
      // last few worker messages
      events: Array.isArray(statusJson?.events) ? statusJson.events.slice(-10) : [],
    });
  } catch (e) {
    return res(500, { error: e?.message || String(e) });
  }
};
