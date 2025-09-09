// netlify/functions/batch-create.js (CommonJS + Lambda-style returns)

const Busboy = require("busboy");
const { parse: csvParse } = require("csv-parse");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

exports.config = { /* path: "/api/batch-create" */ };

const supportsTemperature = (name) => !/^gpt-5(\b|[-_])/.test(name);

function res(statusCode, bodyObj, headers) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json", ...(headers || {}), ...CORS },
    body: JSON.stringify(bodyObj ?? {}),
  };
}

function getQuery(event) {
  if (event && event.queryStringParameters) return event.queryStringParameters || {};
  try {
    const url = typeof event?.rawUrl === "string" ? new URL(event.rawUrl) : null;
    return url ? Object.fromEntries(url.searchParams.entries()) : {};
  } catch { return {}; }
}

// Parse multipart with Busboy (Lambda event)
function parseMultipartEvent(event) {
  return new Promise((resolve, reject) => {
    const headers = event.headers || {};
    const bb = Busboy({
      headers: {
        "content-type": headers["content-type"] || headers["Content-Type"] || ""
      }
    });

    const fields = {};
    let fileBuffers = [];
    let fileInfo = null;

    bb.on("file", (_name, file, info) => {
      fileInfo = info; // { filename, mimeType, encoding }
      file.on("data", (d) => fileBuffers.push(d));
    });
    bb.on("field", (name, val) => (fields[name] = val));
    bb.on("error", reject);
    bb.on("finish", () => {
      resolve({
        fields,
        fileBuffer: fileBuffers.length ? Buffer.concat(fileBuffers) : null,
        fileInfo,
      });
    });

    const body = event.isBase64Encoded
      ? Buffer.from(event.body || "", "base64")
      : Buffer.from(event.body || "");
    bb.end(body);
  });
}

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS" || event.httpMethod === "HEAD") {
    return { statusCode: 204, headers: CORS, body: "" };
  }
  if (event.httpMethod !== "POST") {
    return res(405, { error: "POST only" });
  }

  // ESM deps (loaded at runtime)
  const { getStore } = await import("@netlify/blobs");
  const openaiMod = await import("openai");
  const OpenAI = openaiMod.default;
  const { toFile } = openaiMod;
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  try {
    // Parse multipart
    const { fields, fileBuffer } = await parseMultipartEvent(event);

    const inputCol = fields.inputCol || "text";
    const model = fields.model || "gpt-4.1-mini";
    const prompt = fields.prompt || "Translate to English.";
    const completionWindow = fields.completionWindow || "24h";
    const chunkSize = Math.max(1, Math.min(1000, Number(fields.chunkSize || 200)));
    const reasoningEffort = (fields.reasoning_effort || "").trim();

    // testing flags
    const query = getQuery(event);
    const maxRows = Number(fields.maxRows || query.maxRows || 0) || 0;
    const dryRun = String(fields.dryRun || query.dryRun || "") === "1";
    const direct = String(fields.direct || query.direct || "") === "1";
    const MAX_DIRECT_CONCURRENCY = Number(process.env.MAX_DIRECT_CONCURRENCY || 8);
    const concurrency = Math.max(
      1,
      Math.min(MAX_DIRECT_CONCURRENCY, Number(fields.concurrency || query.concurrency || 4))
    );

    if (!fileBuffer) {
      return res(400, { error: "CSV file is required" });
    }

    // Store original CSV
    const jobId = crypto.randomUUID();
    const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
const store  = (siteID && token)
  ? getStore({ name: "openai-batch-csv", siteID, token })
  : getStore("openai-batch-csv");
    await store.set(`csv/${jobId}.csv`, fileBuffer, { contentType: "text/csv; charset=utf-8" });

    // Parse CSV
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, { columns: true, relax_quotes: true, skip_empty_lines: true, bom: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });
    if (!rows.length) return res(400, { error: "CSV has no rows" });

    const effectiveRows = maxRows > 0 ? rows.slice(0, maxRows) : rows;
    const supportsReasoning =
      /^o\d/i.test(model) || model.startsWith("o") || model.startsWith("gpt-5");

    const suffix =
      ' You will receive a json object {"rows":[{"id":number,"text":string},...]}.'
      + ' For each item, produce {"id": same id, "result": <string>} following the user instructions above.'
      + ' The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"result":string},...]}'
      + ' in the SAME ORDER as input. Do not include any commentary.';

    // DRY RUN
    if (dryRun) {
      const firstChunk = effectiveRows.slice(0, chunkSize).map((r, j) => ({
        id: j,
        text: String(r?.[inputCol] ?? ""),
      }));
      const body = {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: "Return only a json object as specified. The output must be valid json." },
          { role: "user", content: JSON.stringify({ rows: firstChunk }) }
        ],
        text: { format: { type: "json_object" } },
        ...(supportsTemperature(model) ? { temperature: 0 } : {}),
        ...(supportsReasoning && reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
      };
      const resp = await client.responses.create(body);
      let parsed = null; try { parsed = JSON.parse(resp.output_text || ""); } catch {}
      return res(200, { mode: "dryRun", jobId, usedRows: firstChunk.length, model, response: resp, parsed });
    }

 // ---- DIRECT: kick off the background worker and return 202 (or 500 on invoke failure)
if (direct) {
  // Persist metadata for the worker
  await store.set(
    `jobs/${jobId}.json`,
    JSON.stringify({
      jobId,
      model,
      prompt,
      inputCol,
      chunkSize,
      reasoningEffort,
      concurrency,
      createdAt: new Date().toISOString(),
    }),
    { contentType: "application/json" }
  );

  // Seed an initial status so the UI shows progress immediately
  const now = new Date().toISOString();
  await store.set(
    `jobs/${jobId}.status.json`,
    JSON.stringify({
      jobId,
      status: "queued",
      updatedAt: now,
      events: [{ ts: now, msg: "queued" }],
    }),
    { contentType: "application/json" }
  );

  // ---- BUILD ORIGIN FROM INCOMING REQUEST HEADERS (works in prod & netlify dev)
  const hdrs = reqOrEvent.headers || {};
  const host =
    hdrs["x-forwarded-host"] ||
    hdrs["host"] ||
    hdrs["Host"] ||
    "";
  const proto =
    hdrs["x-forwarded-proto"] ||
    (host.startsWith("localhost") || host.startsWith("127.0.0.1") ? "http" : "https");
  const origin =
    host
      ? `${proto}://${host}`
      : (process.env.NETLIFY_SITE_URL || process.env.URL || process.env.DEPLOY_URL || "http://localhost:8888");

  // Try to invoke the background worker; if it fails, mark job failed and bubble up
  try {
    const wr = await fetch(`${origin}/.netlify/functions/direct-worker-background`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jobId }),
    });

    if (!wr.ok) {
      const msg = `worker invoke failed: HTTP ${wr.status}`;
      await store.set(
        `jobs/${jobId}.status.json`,
        JSON.stringify({
          jobId,
          status: "failed",
          updatedAt: new Date().toISOString(),
          events: [{ ts: new Date().toISOString(), msg: msg }],
          error: msg,
        }),
        { contentType: "application/json" }
      );
      return {
        statusCode: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: msg }),
      };
    }
  } catch (e) {
    const msg = `worker invoke error: ${e?.message || String(e)}`;
    await store.set(
      `jobs/${jobId}.status.json`,
      JSON.stringify({
        jobId,
        status: "failed",
        updatedAt: new Date().toISOString(),
        events: [{ ts: new Date().toISOString(), msg: msg }],
        error: msg,
      }),
      { contentType: "application/json" }
    );
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: msg }),
    };
  }

  // Tell the UI how to poll + where to download once ready
  return {
    statusCode: 202,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      mode: "direct",
      jobId,
      model,
      rowCount: effectiveRows.length,
      download: `/.netlify/functions/batch-download?id=${jobId}`,
    }),
  };
}


    // BATCH
    const lines = [];
    for (let start = 0; start < effectiveRows.length; start += chunkSize) {
      const chunk = effectiveRows.slice(start, start + chunkSize).map((r, j) => ({
        id: start + j,
        text: String(r?.[inputCol] ?? ""),
      }));
      const body = {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: "Return only a json object as specified. The output must be valid json." },
          { role: "user", content: JSON.stringify({ rows: chunk }) }
        ],
        text: { format: { type: "json_object" } },
        ...(supportsTemperature(model) ? { temperature: 0 } : {}),
        ...(supportsReasoning && reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
      };
      lines.push(JSON.stringify({ custom_id: String(start), method: "POST", url: "/v1/responses", body }));
    }

    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");
    const jsonlFile = await client.files.create({
      file: await toFile(jsonlBuffer, `${jobId}.jsonl`, { type: "application/jsonl" }),
      purpose: "batch",
    });
    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow,
    });

    await store.set(
      `jobs/${batch.id}.json`,
      JSON.stringify({
        jobId, batchId: batch.id, inputCol, model, prompt, chunkSize, reasoningEffort,
        createdAt: new Date().toISOString(), rowCount: effectiveRows.length,
      }),
      { contentType: "application/json" }
    );

    return res(200, { mode: "batch", batchId: batch.id, jobId });
  } catch (err) {
    console.error("batch-create error:", err);
    return res(500, { error: err?.message || String(err) });
  }
};
