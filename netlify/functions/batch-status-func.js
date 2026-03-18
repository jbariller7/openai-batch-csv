exports.config = { /* path: "/api/batch-status" */ };

const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD", "Access-Control-Allow-Headers": "Content-Type" };

function res(statusCode, body, headers) { return { statusCode, headers: { ...(headers || {}), ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body ?? {}) }; }

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") return { statusCode: 204, headers: CORS, body: "" };

  try {
    const rawUrl = typeof event?.rawUrl === "string" ? event.rawUrl : "";
    const idParam = (rawUrl ? new URL(rawUrl) : null)?.searchParams.get("id") || event?.queryStringParameters?.id || "";
    if (!idParam) return res(400, { error: "Missing id" });

    const { getStore } = await import("@netlify/blobs");
    const { default: OpenAI } = await import("openai");
    const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
    const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
    const store  = (siteID && token) ? getStore({ name: "openai-batch-csv", siteID, token }) : getStore("openai-batch-csv");

    let batchId = idParam;
    if (!String(batchId).startsWith("batch_")) {
      const meta = await store.get(`jobs/${idParam}.json`, { type: "json" }).catch(() => null);
      if (!meta?.batchId) return res(400, { error: `Invalid id '${idParam}'.` });
      batchId = meta.batchId;
    }

    const b = await client.batches.retrieve(batchId);
    return res(200, {
      id: b.id,
      status: b.status,
      request_counts: b.request_counts, // Contains { completed, failed, total }
      created_at: b.created_at,
      completed_at: b.completed_at,
      output_file_id: b.output_file_id || null,
      error_file_id: b.error_file_id || null,
      errors: b.errors || null // <--- We are now passing OpenAI's internal errors to your UI!
    });
  } catch (e) {
    return res(500, { error: e?.message || String(e) });
  }
};
