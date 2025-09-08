// netlify/functions/batch-create.js (ESM)

import OpenAI, { toFile } from "openai";
import { getStore } from "@netlify/blobs";
import Busboy from "busboy";
import { parse as csvParse } from "csv-parse";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Optional path config when invoking as /.netlify/functions/*
export const config = { /* path: "/api/batch-create" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

// Helpers to support both Netlify Node (event) and Web Request shapes
function getMethod(reqOrEvent) {
  if (reqOrEvent && typeof reqOrEvent.method === "string") return reqOrEvent.method;         // Web/Deno
  if (reqOrEvent && typeof reqOrEvent.httpMethod === "string") return reqOrEvent.httpMethod; // Node/event
  return "GET";
}
function hasFormData(reqOrEvent) {
  return reqOrEvent && typeof reqOrEvent.formData === "function"; // Web/Deno
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
    const reasoningEffort = (fields.reasoning_effort || "").trim(); // "low" | "medium" | "high"
    // Note: verbosity is no longer used
    const verbosity = (fields.verbosity || "").trim();

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

    // 4) Build JSONL lines in chunks
    const supportsReasoning =
      /^o\d/i.test(model) || model.startsWith("o") || model.startsWith("gpt-5");

    const suffix =
      ' You will receive a JSON object {"rows":[{"id":number,"text":string},...]}.'
      + ' For each item, produce {"id": same id, "result": <string>} following the user instructions above.'
      + ' Return ONLY a JSON object: {"results":[{"id":number,"result":string},...]} in the SAME ORDER as input. Do not include any commentary.';

    const lines = [];

    for (let start = 0; start < rows.length; start += chunkSize) {
      const chunk = rows.slice(start, start + chunkSize).map((r, j) => ({
        id: start + j,
        text: String(r?.[inputCol] ?? ""),
      }));

      const body = {
        model,
        // Use Responses API idioms: general instructions + one user input blob
        instructions: `${prompt}${suffix}`,
        input: JSON.stringify({ rows: chunk }),
        response_format: { type: "json_object" },
        temperature: 0,
        ...(supportsReasoning && reasoningEffort
          ? { reasoning: { effort: reasoningEffort } }
          : {}),
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

    // 5) Upload JSONL using SDK helper for cross-runtime compatibility
    const jsonlFile = await client.files.create({
      file: await toFile(jsonlBuffer, `${jobId}.jsonl`, {
        type: "application/jsonl",
      }),
      purpose: "batch",
    });

    // 6) Create Batch
    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow, // eg "24h"
    });

    // 7) Persist job metadata
    await store.setJSON(`jobs/${batch.id}.json`, {
      jobId,
      batchId: batch.id,
      inputCol,
      model,
      prompt,
      chunkSize,
      reasoningEffort,
      verbosity, // kept only for your logs
      createdAt: new Date().toISOString(),
    });

    return new Response(
      JSON.stringify({ batchId: batch.id, jobId }),
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
