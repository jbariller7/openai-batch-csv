import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import Busboy from "busboy";
import { parse as csvParse } from "csv-parse";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Optional path note only. Redirect in netlify.toml already maps /api/* to functions.
export const config = { /* path: "/api/batch-create" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

function getMethod(req) {
  if (req && typeof req.method === "string") return req.method;
  if (req && typeof req.httpMethod === "string") return req.httpMethod;
  return "GET";
}

function hasFormData(req) {
  return req && typeof req.formData === "function";
}

function parseMultipartEvent(event) {
  return new Promise((resolve, reject) => {
    const bb = Busboy({
      headers: {
        "content-type":
          event.headers?.["content-type"] || event.headers?.["Content-Type"] || "",
      },
    });

    const fields = {};
    let fileBuffers = [];
    let fileInfo = null;

    bb.on("file", (_name, file, info) => {
      fileInfo = info;
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

export default async (reqOrEvent) => {
  const method = getMethod(reqOrEvent);
  if (method === "OPTIONS" || method === "HEAD")
    return new Response("", { status: 204, headers: CORS });
  if (method !== "POST")
    return new Response(JSON.stringify({ error: "POST only" }), {
      status: 405,
      headers: CORS,
    });

  try {
    // Parse form (Deno/Web API first, Node fallback)
    let fields = {};
    let fileBuffer = null;

    if (hasFormData(reqOrEvent)) {
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
      const out = await parseMultipartEvent(reqOrEvent);
      fields = out.fields || {};
      fileBuffer = out.fileBuffer || null;
    }

    const inputCol = fields.inputCol || "text";
    const model = fields.model || "gpt-4.1-mini";
    const prompt = fields.prompt || "Translate to English.";
    const completionWindow = fields.completionWindow || "24h";
    const chunkSize = Math.max(1, Math.min(1000, Number(fields.chunkSize || 200)));
    const reasoningEffort = (fields.reasoning_effort || "").trim(); // minimal|low|medium|high
    const verbosity = (fields.verbosity || "").trim(); // "", low|medium|high (GPT-5)

    if (!fileBuffer) {
      return new Response(JSON.stringify({ error: "CSV file is required" }), {
        status: 400,
        headers: CORS,
      });
    }

    // Store original CSV
    const jobId = crypto.randomUUID();
    const store = getStore("openai-batch-csv");
    await store.set(`csv/${jobId}.csv`, fileBuffer);

    // Parse CSV
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(fileBuffer, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });
    if (!rows.length)
      return new Response(JSON.stringify({ error: "CSV has no rows" }), {
        status: 400,
        headers: CORS,
      });

    // Build JSONL (micro-batches)
    const isGpt5 = model.startsWith("gpt-5");
    const isOseries = /^o\d/i.test(model) || model.startsWith("o");

    const suffix =
      ' You will receive a JSON object {"rows":[{"id":number,"text":string},...]}.' +
      ' For each item, produce {"id": same id, "result": } following the user instructions above.' +
      ' Return ONLY a JSON object: {"results":[{"id":number,"result":string},...]} in the SAME ORDER as input.' +
      " Do not include any commentary.";

    const lines = [];
    for (let start = 0; start < rows.length; start += chunkSize) {
      const chunk = rows.slice(start, start + chunkSize).map((r, j) => ({
        id: start + j,
        text: String(r?.[inputCol] ?? ""),
      }));

      const body = {
        model,
        input: [
          { role: "system", content: `${prompt}${suffix}` },
          { role: "user", content: JSON.stringify({ rows: chunk }) },
        ],
        text_format: { type: "json_object" },
        temperature: 0,
        ...(((isGpt5 || isOseries) && reasoningEffort)
          ? { reasoning_effort: reasoningEffort }
          : {}),
        ...(isGpt5 && verbosity ? { text: { verbosity } } : {}),
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

    // Upload JSONL
    const jsonlFile = await client.files.create({
      file: new File([jsonlBuffer], `${jobId}.jsonl`, { type: "application/jsonl" }),
      purpose: "batch",
    });

    // Create Batch
    const batch = await client.batches.create({
      input_file_id: jsonlFile.id,
      endpoint: "/v1/responses",
      completion_window: completionWindow,
    });

    // Save metadata (portable JSON)
    const meta = {
      jobId,
      batchId
