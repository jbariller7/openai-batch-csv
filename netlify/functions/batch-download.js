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
  if (method === "OPTIONS" || method === "HEAD") {
    return new Response("", { status: 204, headers: CORS });
  }

  const url = new URL(getUrl(reqOrEvent));
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "Missing id" }), {
      status: 400,
      headers: CORS,
    });
  }

  try {
    const store = getStore("openai-batch-csv");

    // ----- NEW: direct-mode download path -----
    const directCsvBuf = await store.get(`results/${id}.csv`, { type: "buffer" });
    if (directCsvBuf) {
      return new Response(directCsvBuf, {
        status: 200,
        headers: {
          ...CORS,
          "Content-Type": "text/csv",
          "Content-Disposition": `attachment; filename="${id}.csv"`,
        },
      });
    }

    // ----- Existing batch download path -----
    const b = await client.batches.retrieve(id);
    if (b.status !== "completed") {
      return new Response(
        JSON.stringify({ error: `Batch not completed. Status: ${b.status}` }),
        { status: 400, headers: CORS }
      );
    }
    if (!b.output_file_id) {
      return new Response(JSON.stringify({ error: "No output file id" }), {
        status: 400,
        headers: CORS,
      });
    }

    const meta = await store.getJSON(`jobs/${id}.json`);
    if (!meta) {
      return new Response(JSON.stringify({ error: "Job metadata not found" }), {
        status: 404,
        headers: CORS,
      });
    }

    // Read original CSV rows
    const csvBuf = await store.get(`csv/${meta.jobId}.csv`, { type: "buffer" });
    if (!csvBuf) {
      return new Response(JSON.stringify({ error: "Original CSV not found" }), {
        status: 404,
        headers: CORS,
      });
    }

    const rows = await new Promise((resolve, reject) => {
      const out = [];
      csvParse(csvBuf, { columns: true, relax_quotes: true })
        .on("data", (r) => out.push(r))
        .on("end", () => resolve(out))
        .on("error", reject);
    });

    // Download batch output JSONL
    const fileResp = await client.files.content(b.output_file_id);
    const outputBuf = Buffer.from(await fileResp.arrayBuffer());
    const jsonlLines = outputBuf.toString("utf8").trim().split("\n");

    // Prepare merged rows (copy original + result column)
    const merged = rows.map((r) => ({ ...r, result: "" }));

    const assignByArray = (arr, baseIndex = 0) => {
      arr.forEach((item, j) => {
        const idx = Number.isFinite(Number(item?.id))
          ? Number(item.id)
          : baseIndex + j;
        if (idx >= 0 && idx < merged.length) {
          merged[idx].result =
            typeof item?.result === "string"
              ? item.result
              : item?.toString?.() ?? "";
        }
      });
    };

    // Each JSONL line = one micro-batch response
    for (const line of jsonlLines) {
      if (!line.trim()) continue;

      let obj;
      try { obj = JSON.parse(line); } catch { continue; }

      const base = Number(obj?.custom_id) || 0;
      const body = obj?.response?.body || {};
      const text =
        typeof body?.output_text === "string"
          ? body.output_text
          : typeof body?.content === "string"
          ? body.content
          : "";

      let parsed = null;
      if (text) {
        try { parsed = JSON.parse(text); } catch { parsed = null; }
      }

      if (parsed && Array.isArray(parsed.results)) {
        assignByArray(parsed.results, base);
      } else if (Array.isArray(parsed)) {
        assignByArray(parsed, base);
      } else if (parsed && typeof parsed.result === "string") {
        if (base >= 0 && base < merged.length) merged[base].result = parsed.result;
      } else if (typeof text === "string" && text) {
        if (base >= 0 && base < merged.length) merged[base].result = text;
      }
    }

    // Serialize merged CSV
    const headers = Object.keys(merged[0] || {});
    const csvStr = await new Promise((resolve, reject) => {
      csvStringify(merged, { header: true, columns: headers }, (err, out) => {
        if (err) reject(err);
        else resolve(out);
      });
    });

    return new Response(csvStr, {
      status: 200,
      headers: {
        ...CORS,
        "Content-Type": "text/csv",
        "Content-Disposition": `attachment; filename="${id}.csv"`,
      },
    });
  } catch (e) {
    console.error("batch-download error:", e);
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: CORS,
    });
  }
};
