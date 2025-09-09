// netlify/functions/batch-status.js (CommonJS + Lambda-style)

exports.config = { /* path: "/api/batch-status" */ };

function res(statusCode, bodyObj) {
  return { statusCode, headers: { "Content-Type": "application/json" }, body: JSON.stringify(bodyObj ?? {}) };
}

exports.handler = async function (event) {
  const urlStr = typeof event?.rawUrl === "string" ? event.rawUrl : "";
  const url = urlStr ? new URL(urlStr) : null;
  const id = url?.searchParams.get("id") || event?.queryStringParameters?.id || "";
  if (!id) return res(400, { error: "Missing id" });

  const { default: OpenAI } = await import("openai");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  try {
    const b = await client.batches.retrieve(id);
    return res(200, b);
  } catch (e) {
    return res(500, { error: e?.message || String(e) });
  }
};
