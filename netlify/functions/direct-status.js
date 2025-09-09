// netlify/functions/direct-status.js
import { getStore } from "@netlify/blobs";

export const config = { /* path: "/api/direct-status" */ };

export default async function handler(req) {
  const url = new URL(req.url);
  const id = url.searchParams.get("id");
  if (!id) {
    return new Response(JSON.stringify({ error: "Missing id" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const store = getStore("openai-batch-csv");

    // Prefer explicit status if present
    const statusJson = await store.getJSON(`jobs/${id}.status.json`).catch(() => null);

    // If no status JSON yet, consider CSV existence as readiness
    let csvExists = false;
    if (!statusJson || statusJson.status !== "ready") {
      try {
        const txt = await store.get(`results/${id}.csv`, { type: "text" });
        csvExists = typeof txt === "string";
      } catch {}
    }

    const ready = statusJson?.status === "ready" || csvExists;
    const status = ready ? "ready" : (statusJson?.status || "running");
    const error = statusJson?.error || null;

    return new Response(JSON.stringify({ id, ready, status, error }), {
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
