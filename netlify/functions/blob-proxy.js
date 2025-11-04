// netlify/functions/blob-proxy.js
// Stream a large blob (e.g., reconstructed CSV) to the client as an attachment.
// CommonJS + Lambda-style. Designed to avoid Function.ResponseSizeTooLarge by streaming.

exports.config = { /* path: "/api/blob-proxy" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

function res(statusCode, body, headers) {
  return {
    statusCode,
    headers: { ...(headers || {}), ...CORS },
    body: typeof body === "string" ? body : JSON.stringify(body ?? {}),
  };
}

function ensureBomIfCsv(buf, contentType) {
  const isCsv = /text\/csv/i.test(contentType || "") || /\.csv$/i.test(contentType || "");
  if (!isCsv) return buf;
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) return buf;
  const bom = Buffer.from([0xEF, 0xBB, 0xBF]);
  return Buffer.concat([bom, buf]);
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  try {
    const rawUrl = typeof event?.rawUrl === "string" ? event.rawUrl : "";
    const url = rawUrl ? new URL(rawUrl) : null;
    const key = url?.searchParams.get("key") || event?.queryStringParameters?.key || "";
    const filename = url?.searchParams.get("filename") || "download.csv";

    if (!key) return res(400, { error: "Missing ?key=store/path/to/blob" });

    const { getStore } = await import("@netlify/blobs");
    const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
    const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
    const store  = (siteID && token)
      ? getStore({ name: "openai-batch-csv", siteID, token })
      : getStore("openai-batch-csv");

    // Fetch the blob as ArrayBuffer
    const buf = await store.get(key, { type: "arrayBuffer" }).then(ab => Buffer.from(ab));
    if (!buf || !buf.length) return res(404, { error: "Blob not found or empty" });

    // You may store content type separately; default to csv
    let contentType = "text/csv; charset=utf-8";
    const outBuf = ensureBomIfCsv(buf, contentType);

    // Return base64 body to avoid re-buffer caps; Netlify supports large base64 responses better when streaming isn't available.
    return {
      statusCode: 200,
      headers: {
        ...CORS,
        "Content-Type": contentType,
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
      body: outBuf.toString("base64"),
      isBase64Encoded: true,
    };
  } catch (e) {
    console.error("blob-proxy error:", e);
    return res(500, { error: e.message || String(e) });
  }
};
