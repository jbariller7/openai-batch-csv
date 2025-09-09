// netlify/functions/direct-status.js
import { getStore } from "@netlify/blobs";

export const config = { /* path: "/api/direct-status" */ };

export default async function handler(req) {
  const url = new URL(req.url);
  const id = url.searchParams.get("id");
  if (!id) return new Response(JSON.stringify({ error: "Missing id" }), { status: 400, headers: { "Content-Type": "application/json" } });

  try {
    const store = getStore("openai-batch-csv");
    // Ready if the CSV exists
    let csvExists = false;
    try {
      const txt = await store.get(`results/${id}.csv`, { type: "text" });
      csvExists = typeof txt === "string";
    } catch (_) {}
    // Optional: read status JSON
    const meta = await store.getJSON(`jobs/${id}.status.json`).catch(() => null);

    return new Response(JSON.stringify({ id, ready: csvExists, status: meta?.status || (csvExists ? "ready" : "running") }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e?.message || String(e) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}
