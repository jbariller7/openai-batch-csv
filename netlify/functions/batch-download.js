import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import { parse as csvParse } from "csv-parse";
import { stringify as csvStringify } from "csv-stringify";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export const config = { /* path: "/api/batch-download" */ };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS,HEAD",
  "Access-Control-Allow-Headers": "Content-Type",
};

function getMethod(req) {
  if (req && typeof req.method === "string") return req.method;
  if (req && typeof req.httpMethod === "string") return req.httpMethod;
  return "GET";
}
function getUrl(req) {
  return typeof req.url === "string" ? req.url : (req.rawUrl || "");
}

export default async (reqOrEvent) => {
  const method = getMethod(reqOrEvent);
  if (method === "OPTIONS" || method === "HEAD") return new Response("", { status: 204, headers: CORS });

  const url = new URL(getUrl(reqOrEvent));
  const id = url.searchParams.get("id");
  if (!id) return new Response(JSON.stringify({ error: "Missing id" }), { status: 400, headers: CORS });

  try {
    const b = await client.batches.retrieve(id);
    if (!["completed", "failed", "cancelled", "expired"].includes(b.status)) {
      return new Response(JSON.stringify({ error: `Batch not completed. Status: ${b.status}` }), { status: 400, headers: CORS });
    }

    const store = getStore("openai-batch-csv");

    // Read metadata (portable JSON)
    const meta = await store.get(`jobs/${id}.json`, { type: "json" });
    if (!meta) return new Response(JSON.stringify({ error: "Job metadata not found" }), { status: 404, headers: CORS });

    // Read original CSV rows
    const csvBuf = await store.get(`csv/${meta.jobId}.csv`, { type: "buffer" });
    if (!csvBuf) return new Response(JSON.stringify({ error: "Original CSV not found" }), { status: 404, headers: CORS });

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(csvBuf, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });

    // Prepare merged rows
    const merged = rows.map((r) => ({ ...r, result: "" }));

    // Helper to assign array results
    const assignByArray = (arr, baseIndex = 0) => {
      arr.forEach((item, j) => {
        const idx = Number.isFinite(Number(item?.id)) ? Number(item.id) : (baseIndex + j);
        if (idx >= 0 && idx < merged.length) {
          merged[idx].result = typeof item?.result === "string" ? item.result : (item?.toString?.() ?? "");
        }
      });
    };

    // Prefer output file; else fall back to error file
    const fileId = b.output_file_id || b.error_file_id;
    if (!fileId) {
      return new Response(JSON.stringify({ error: "No output or error file id" }), { status: 400, headers: CORS });
    }

    const fileResp = await client.files.content(fileId);
    const buf = Buffer.from(await fileResp.arrayBuffer());
    const lines = buf.toString("utf8").trim().split("\n");

    const isErrorOnly = !b.output_file_id && !!b.error_file_id;

    for (const line of lines) {
      if (!line.trim()) continue;

      let obj;
      try { obj = JSON.parse(line); } catch { continue; }

      const base = Number(obj?.custom_id) || 0;

      // Successful case payload path
      const body = obj?.response?.body || {};
      const successText =
        typeof body?.output_text === "string"
          ? body.output_text
          : typeof body?.content === "string"
          ? body.content
          : "";

      // Error payload path
      const errObj = obj?.error || null;
      const errMsg = errObj ? (errObj?.message || errObj?.code || JSON.stringify(errObj)) : "";

      if (!isErrorOnly && successText) {
        let parsed = null;
        try { parsed = JSON.parse(successText); } catch { parsed = null; }

        if (parsed && Array.isArray(parsed.results)) {
          assignByArray(parsed.results, base);
        } else if (Array.isArray(parsed)) {
          assignByArray(parsed, base);
        } else if (parsed && typeof parsed.result === "string") {
          if (base >= 0 && base < merged.length) merged[base].result = parsed.result;
        } else if (typeof successText === "string" && successText) {
          if (base >= 0 && base < merged.length) merged[base].result = successText;
        }
      } else {
        // Entire chunk failed: mark each row in the chunk with the error
        const K = Number(me
