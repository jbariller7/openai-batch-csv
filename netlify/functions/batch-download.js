import OpenAI from "openai";
import { getStore } from "@netlify/blobs";
import { parse as csvParse } from "csv-parse";
import { stringify as csvStringify } from "csv-stringify";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
export const config = { path: "/api/batch-download" };

export default async (event) => {
  const url = new URL(event.rawUrl);
  const id = url.searchParams.get("id");
  if (!id) return new Response(JSON.stringify({ error: "Missing id" }), { status: 400 });

  try {
    const b = await client.batches.retrieve(id);
    if (b.status !== "completed") {
      return new Response(JSON.stringify({ error: `Batch not completed. Status: ${b.status}` }), { status: 400 });
    }
    if (!b.output_file_id) {
      return new Response(JSON.stringify({ error: "No output file id" }), { status: 400 });
    }

    const store = getStore("openai-batch-csv");
    const meta = await store.getJSON(`jobs/${id}.json`);
    if (!meta) return new Response(JSON.stringify({ error: "Job metadata not found" }), { status: 404 });

    // Read original CSV rows
    const csvBuf = await store.get(`csv/${meta.jobId}.csv`, { type: "buffer" });
    if (!csvBuf) return new Response(JSON.stringify({ error: "Original CSV not found" }), { status: 404 });

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(csvBuf, { columns: true, relax_quotes: true })
        .on("data", r => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });

    // Download batch output JSONL
    const out = await client.files.content(b.output_file_id);
    const outputBuf = Buffer.from(await out.arrayBuffer());
    const lines = outputBuf.toString("utf8").trim().split("\n");

    // Prepare result rows (copy original + new columns)
    const merged = rows.map(r => ({ ...r, result: "" }));

    // For each line (each chunk response), parse and write results
    for (const line of lines) {
      if (!line.trim()) continue;

      let obj;
      try { obj = JSON.parse(line); } catch { continue; }

      const base = Number(obj?.custom_id) || 0;
      const text = obj?.response?.body?.output_text ?? "";
      let parsed;
      try { parsed = JSON.parse(text); } catch { parsed = null; }

      const assignByArray = (arr, baseIndex = 0) => {
        arr.forEach((item, j) => {
          const idx = Number.isFinite(Number(item?.id)) ? Number(item.id) : (baseIndex + j);
          if (idx >= 0 && idx < merged.length) {
            merged[idx].result = typeof item?.result === "string" ? item.result : (item?.toString?.() ?? "");
          }
        });
      };

      if (parsed && Array.isArray(parsed.results)) {
        assignByArray(parsed.results, base);
      } else if (Array.isArray(parsed)) {
        assignByArray(parsed, base);
      } else if (parsed && typeof parsed.result === "string") {
        if (base >= 0 && base < merged.length) merged[base].result = parsed.result;
      } else if (typeof text === "string" && text) {
        // Fallback: put raw text into the first row of this chunk
        if (base >= 0 && base < merged.length) merged[base].result = text;
      }
    }

    // Write merged CSV to response
    const headers = Object.keys(merged[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(merged, { header: true, columns: headers }, (err, outStr) => {
        if (err) reject(err);
        else resolve(outStr);
      });
    });

    return new Response(csvStr, {
      status: 200,
      headers: {
        "Content-Type": "text/csv",
        "Content-Disposition": `attachment; filename="${id}.csv"`
      }
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};
