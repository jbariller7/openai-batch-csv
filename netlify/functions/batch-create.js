// netlify/functions/batch-create.js  (ESM)

import OpenAI, { toFile } from "openai";
import { getStore } from "@netlify/blobs";
import Busboy from "busboy";
import { parse as csvParse } from "csv-parse";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Optional pretty path when invoking as /.netlify/functions/*
export const config = { /* path: "/api/batch-create" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

function getMethod(reqOrEvent) {
  if (reqOrEvent && typeof reqOrEvent.method === "string") return reqOrEvent.method;
  if (reqOrEvent && typeof reqOrEvent.httpMethod === "string") return reqOrEvent.httpMethod;
  return "GET";
}
function hasFormData(reqOrEvent) {
  return reqOrEvent && typeof reqOrEvent.formData === "function";
}
function getQuery(reqOrEvent) {
  if (reqOrEvent && typeof reqOrEvent.queryStringParameters === "object") {
    return reqOrEvent.queryStringParameters || {};
  }
  try {
    const url = typeof reqOrEvent?.url === "string" ? new URL(reqOrEvent.url) : null;
    return url ? Object.fromEntries(url.searchParams.entries()) : {};
  } catch {
    return {};
  }
}

// Parse multipart for Node/event using Busboy
function parseMultipartEvent(event) {
  return new Promise((resolve, reject) => {
    const bb = Busboy({
      headers: {
        "content-type":
          event.headers?.["content-type"] ||
          event.headers?.["Content-Type"] ||
          "",
      },
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

export default async function handler(reqOrEvent) {
  const method = getMethod(reqOrEvent);
  const query = getQuery(reqOrEvent);

  if (method === "OPTIONS" || method === "HEAD") {
    return new Response("", { status: 204, headers: CORS });
  }
  if (method !== "POST") {
    return new Response(JSON.stringify({ error: "POST only" }), {
      status: 405,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  try {
    // 1) Parse multipart form
    let fields = {};
    let fileBuffer = null;

    if (hasFormData(reqOrEvent)) {
      // Web API (Deno-like Request)
      const form = await reqOrEvent.formData();
      for (const [k, v] of form.entries()) {
        if (v && typeof v.arrayBuffer === "function") {
          const ab = await v.arrayBuffer();
          fileBuffer = Buffer.from(ab);
        } else {
          fields[k] = String(v);
        }
      }
    } else {
      // Node/event
      const out = await parseMultipartEvent(reqOrEvent);
      fields = out.fields || {};
      fileBuffer = out.fileBuffer || null;
    }

    const inputCol = fields.inputCol || "text";
    const model = fields.model || "gpt-4.1-mini";
    const prompt = fields.prompt || "Translate to English.";
    const completionWindow = fields.completionWindow || "24h";
    const chunkSize = Math.max(1, Math.min(1000, Number(fields.chunkSize || 200)));
    const reasoningEffort = (fields.reasoning_effort || "").trim(); // "minimal" | "low" | "medium" | "high"

    // Testing flags (can be set by form fields or query params)
    const maxRows = Number(fields.maxRows || query.maxRows || 0) || 0;
    const dryRun = String(fields.dryRun || query.dryRun || "") === "1";
    const direct = String(fields.direct || query.direct || "") === "1";
    const concurrency = Math.max(1, Math.min(8, Number(fields.concurrency || query.concurrency || 4)));

    if (!fileBuffer) {
      return new Response(JSON.stringify({ error: "CSV file is required" }), {
        status: 400,
        headers: { ...CORS, "Content-Type": "application/json" },
      });
    }

    // 2) Store original CSV in Netlify Blobs
    const jobId = crypto.randomUUID();
    const store = getStore("openai-batch-csv");
    await store.set(`csv/${jobId}.csv`, fileBuffer);

    // 3) Parse CSV
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, {
        columns: true,
        relax_quotes: true,
        skip_empty_lines: true,
        bom: true,
      })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });

    if (!rows.length) {
      return new Response(JSON.stringify({ error: "CSV has no rows" }), {
        status: 400,
        headers: { ...CORS, "Content-Type": "application/json" },
      });
    }

    const effectiveRows = maxRows > 0 ? rows.slice(0, maxRows) : rows;
    const supportsReasoning =
      /^o\d/i.test(model) || model.startsWith("o") || model.startsWith("gpt-5");

    const suffix =
  ' You will receive a json object {"rows":[{"id":number,"text":string},...]}.'
  + ' For each item, produce {"id": same id, "result": <string>} following the user instructions above.'
  + ' The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"result":string},...]}'
  + ' in the SAME ORDER as input. Do not include any commentary.';


    // 4) DRY RUN: run first chunk immediately with /v1/responses
    if (dryRun) {
      const firstChunk = effectiveRows.slice(0, chunkSize).map((r, j) => ({
        id: j,
        text: String(r?.[inputCol] ?? ""),
      }));

      const body = {
        model,
        instructions: `${prompt}${suffix}`,
        input: JSON.stringify({ rows: firstChunk }),
        // UPDATED: JSON mode moved under text.format
        text: { format: { type: "json_object" } },
        temperature: 0,
        ...(supportsReasoning && reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
      };

      const resp = await client.responses.create(body);

      let parsed = null;
      try { parsed = JSON.parse(resp.output_text || ""); } catch {}

      return new Response(
        JSON.stringify({
          mode: "dryRun",
          jobId,
          usedRows: firstChunk.length,
          model,
          response: resp,
          parsed,
        }),
        { status: 200, headers: { ...CORS, "Content-Type": "application/json" } }
      );
    }

    // 5) DIRECT: process the whole CSV now with parallel /v1/responses
    if (direct) {
      const items = effectiveRows.map((r, idx) => ({
        id: idx,
        text: String(r?.[inputCol] ?? ""),
      }));

      // chunk
      const chunks = [];
      for (let i = 0; i < items.length; i += chunkSize) {
        chunks.push(items.slice(i, i + chunkSize));
      }

      function buildBody(rowsChunk) {
        return {
          model,
          instructions: `${prompt}${suffix}`,
          input: JSON.stringify({ rows: rowsChunk }),
          // UPDATED: JSON mode moved under text.format
          text: { format: { type: "json_object" } },
          temperature: 0,
          ...(supportsReasoning && reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
        };
      }

      // simple concurrency pool
      let i = 0;
      const results = new Array(chunks.length);
      async function worker() {
        while (i < chunks.length) {
          const my = i++;
          const resp = await client.responses.create(buildBody(chunks[my]));
          let parsed = null;
          try { parsed = JSON.parse(resp.output_text || ""); } catch {}
          results[my] = parsed;
        }
      }
      await Promise.all(Array.from({ length: concurrency }, worker));

      // flatten {"results":[...]} blocks
      const merged = [];
      for (const part of results) if (part?.results) merged.push(...part.results);

      // store merged for inspection if you want
      await store.setJSON(`results/${jobId}.json`, {
        jobId, model, rowCount: effectiveRows.length, results: merged,
      });

      return new Response(
        JSON.stringify({ mode: "direct", jobId, model, rowCount: effectiveRows.length, results: merged }),
        { status: 200, headers: { ...CORS, "Content-Type": "application/json" } }
      );
    }

    // 6) BATCH: build JSONL lines and enqueue
    const lines = [];
    for (let start = 0; start < effectiveRows.length; start += chunkSize) {
      const chunk = effectiveRows.slice(start, start + chunkSize).map((r, j) => ({
        id: start + j,
        text: String(r?.[inputCol] ?? ""),
      }));

      const body = {
        model,
        instructions: `${prompt}${suffix}`,
        input: JSON.stringify({ rows: chunk }),
        // UPDATED: JSON mode moved under text.format
        text: { format: { type: "json_object" } },
        temperature: 0,
        ...(supportsReasoning && reasoningEffort ? { reasoning: { effort: reasoningEffort } } : {}),
      };

      lines.push(
        JSON.stringify({
          custom_id: String(start),
          method: "POST",
          url: "/v1/responses",
          body,
        })
      );
    }

    const jsonlBuffer = Buffer.from(lines.join("\n"), "utf8");

    // Upload JSONL using SDK helper for cross-runtime compatibility
    const jsonlFile = await client.files.create({
      file: await toFile(jsonlBuffer, `${jobId}.jsonl`, { type: "application/jsonl" }),
      purpose: "batch",
    });

    // Create Batch
    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow, // "24h" or "4h"
    });

    // Persist job metadata
    await store.setJSON(`jobs/${batch.id}.json`, {
      jobId,
      batchId: batch.id,
      inputCol,
      model,
      prompt,
      chunkSize,
      reasoningEffort,
      createdAt: new Date().toISOString(),
      rowCount: effectiveRows.length,
    });

    return new Response(
      JSON.stringify({ mode: "batch", batchId: batch.id, jobId }),
      { status: 200, headers: { ...CORS, "Content-Type": "application/json" } }
    );
  } catch (err) {
    console.error("batch-create error:", err);
    return new Response(
      JSON.stringify({ error: err?.message || String(err) }),
      { status: 500, headers: { ...CORS, "Content-Type": "application/json" } }
    );
  }
}
