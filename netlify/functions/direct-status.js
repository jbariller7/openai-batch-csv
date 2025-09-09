// netlify/functions/direct-status.js  (CommonJS + Lambda-style returns)

exports.config = { /* path: "/api/direct-status" */ };

function res(statusCode, bodyObj, extraHeaders) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json", ...(extraHeaders || {}) },
    body: JSON.stringify(bodyObj ?? {}),
  };
}

exports.handler = async function (event) {
  const urlStr = typeof event?.url === "string" ? event.url : (event?.rawUrl || "");
  const url = urlStr ? new URL(urlStr) : null;
  const id = url?.searchParams.get("id") || (event?.queryStringParameters?.id || "");

  if (!id) return res(400, { error: "Missing id" });

  const { getStore } = await import("@netlify/blobs");
  const store = getStore("openai-batch-csv");

  try {
    let statusJson = null;
    try { statusJson = await store.get(`jobs/${id}.status.json`, { type: "json" }); } catch {}

    // If no explicit status yet, check for CSV presence
    let csvExists = false;
    if (!statusJson || statusJson.status !== "ready") {
      try {
        const txt = await store.get(`results/${id}.csv`, { type: "text" });
        csvExists = typeof txt === "string";
      } catch {}
    }

    const ready = statusJson?.status === "ready" || csvExists;
    const status = ready ? "ready" : (statusJson?.status || "running");
    const error = statusJson?.error || null;

    return res(200, { id, ready, status, error });
  } catch (e) {
    return res(500, { error: e?.message || String(e) });
  }
};
