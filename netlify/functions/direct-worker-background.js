const { parse: csvParse } = require("csv-parse");
const { stringify: csvStringify } = require("csv-stringify");

exports.config = { /* path: "/api/direct-worker-background" */ };
const MAX_DIRECT_CONCURRENCY = Number(process.env.MAX_DIRECT_CONCURRENCY || 8);
const LOCK_TTL_MS = 15 * 60 * 1000; const LOCK_HEARTBEAT_MS = 60 * 1000;
function res(statusCode, bodyObj) { return { statusCode, headers: { "Content-Type": "application/json" }, body: JSON.stringify(bodyObj ?? {}) }; }
function ensureUtf8Bom(str) { return str && !str.startsWith("\uFEFF") ? "\uFEFF" + str : str; }

exports.handler = async function (event) {
  const { getStore } = await import("@netlify/blobs");
  const { default: OpenAI } = await import("openai");
  const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const siteID = process.env.NETLIFY_SITE_ID || process.env.SITE_ID;
  const token  = process.env.NETLIFY_BLOBS_TOKEN || process.env.NETLIFY_AUTH_TOKEN;
  const store  = (siteID && token) ? getStore({ name: "openai-batch-csv", siteID, token }) : getStore("openai-batch-csv");

  let jobId = ""; let lockTimer = null;

  async function writeStatus(status, extra = {}, message) {
    if (!jobId) return; const now = new Date().toISOString(); let prev = null;
    try { prev = await store.get(`jobs/${jobId}.status.json`, { type: "json" }); } catch {}
    if (prev?.status === "cancelled") return; 
    const payload = {
      jobId, status: status || prev?.status || "running", updatedAt: now,
      totalChunks: Number(extra?.totalChunks ?? prev?.totalChunks ?? 0),
      concurrency: Number(extra?.concurrency ?? prev?.concurrency ?? 0),
      completedChunks: Math.max(Number(prev?.completedChunks || 0), Number(extra?.completedChunks ?? 0)),
      partial: Boolean(extra?.partial || prev?.partial || false),
      events: prev ? prev.events.slice(-49) : []
    };
    if (message) payload.events.push({ ts: now, msg: message });
    try { await store.set(`jobs/${jobId}.status.json`, JSON.stringify(payload), { contentType: "application/json" }); } catch {}
  }

  async function checkCancelled() {
    try { const st = await store.get(`jobs/${jobId}.status.json`, { type: "json" }); return st?.status === "cancelled"; } catch { return false; }
  }

  async function acquireLock() {
    let lock = null; try { lock = await store.get(`jobs/${jobId}.lock.json`, { type: "json" }); } catch {}
    if (lock?.ts && (Date.now() - new Date(lock.ts).getTime()) < LOCK_TTL_MS) return false;
    await store.set(`jobs/${jobId}.lock.json`, JSON.stringify({ ts: new Date().toISOString() }), { contentType: "application/json" });
    lockTimer = setInterval(async () => { try { await store.set(`jobs/${jobId}.lock.json`, JSON.stringify({ ts: new Date().toISOString() }), { contentType: "application/json" }); } catch {} }, LOCK_HEARTBEAT_MS);
    return true;
  }
  async function releaseLock() { if (lockTimer) clearInterval(lockTimer); lockTimer = null; try { await store.delete?.(`jobs/${jobId}.lock.json`); } catch {} }

  try {
    const raw = event?.body || ""; const body = JSON.parse(event?.isBase64Encoded ? Buffer.from(raw, "base64").toString("utf8") : raw || "{}");
    jobId = body?.jobId || ""; if (!jobId) return res(400, { error: "Missing jobId" });
    if (!(await acquireLock())) return res(202, { ok: true, ignored: true });

    const meta = await store.get(`jobs/${jobId}.json`, { type: "json" }).catch(() => null);
    if (!meta) { await releaseLock(); return res(404, { error: "Job meta not found" }); }
    
    const { model, prompt, contextDoc = "", inputCol = "text", skipCol = "", targetCols = [], chunkSize = 500, concurrency: desiredConcurrency = 4 } = meta;

    const csvTxt = await store.get(`csv/${jobId}.csv`, { type: "text" }).catch(() => null);
    if (!csvTxt) { await writeStatus("failed", {}, "csv missing"); await releaseLock(); return res(404, { error: "CSV missing" }); }

    const rows = await new Promise((resolve, reject) => {
      const out = []; csvParse(csvTxt, { columns: true, relax_quotes: true, bom: true, skip_empty_lines: true }).on("data", (r) => out.push(r)).on("end", () => resolve(out)).on("error", reject);
    });

    const items = [];
    rows.forEach((r, idx) => {
      const text = String(r?.[inputCol] ?? "").trim();
      const skipText = skipCol ? String(r?.[skipCol] ?? "").trim() : "";
      if (text && !skipText) items.push({ id: idx, text });
    });

    const chunks = []; 
    if (targetCols && targetCols.length > 0) {
      targetCols.forEach(colName => {
        for (let i = 0; i < items.length; i += chunkSize) chunks.push({ rows: items.slice(i, i + chunkSize), targetCol: colName });
      });
    } else {
      for (let i = 0; i < items.length; i += chunkSize) chunks.push({ rows: items.slice(i, i + chunkSize), targetCol: null });
    }
    const totalChunks = chunks.length;
    
    const concurrency = Math.max(1, Math.min(MAX_DIRECT_CONCURRENCY, Number(desiredConcurrency || 4)));
    await writeStatus("running", { totalChunks, concurrency, completedChunks: 0, partial: false }, `plan: ${totalChunks} chunks`);

    const parts = new Array(totalChunks); let nextIdx = 0; let completedChunks = 0;
    function pickNext() { return nextIdx >= totalChunks ? -1 : nextIdx++; }

    async function worker() {
      for (;;) {
        if (await checkCancelled()) break; 
        const idx = pickNext(); if (idx === -1) break;
        try {
            const chunkObj = chunks[idx];
            let currentPrompt = prompt;
            let suffix = ' You will receive a json object {"rows":[{"id":number,"text":string},...]}. For each item, produce {"id": same id, "result": <string>} following the user instructions above. The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"result":string},...]} in the SAME ORDER as input.';
            
            if (chunkObj.targetCol) {
              currentPrompt = prompt.replace(/\$\{columnName(s)?\}/gi, chunkObj.targetCol);
              suffix = ` You will receive a json object {"rows":[{"id":number,"text":string},...]}. For each item, produce {"id": same id, "cols": {"${chunkObj.targetCol}": <string>}} following the user instructions above. The output must be valid json. Return ONLY a json object exactly like: {"results":[{"id":number,"cols":{"${chunkObj.targetCol}": "..."}},...]} in the SAME ORDER as input.`;
            }
            
            const systemPromptContent = contextDoc ? `[REFERENCE CONTEXT]\n${contextDoc}\n\n[INSTRUCTIONS]\n${currentPrompt}` : currentPrompt;

            const resp = await client.responses.create({
              model, input: [{ role: "system", content: `${systemPromptContent}${suffix}` }, { role: "user", content: "Return only a json object as specified. The output must be valid json." }, { role: "user", content: JSON.stringify({ rows: chunkObj.rows }) }], text: { format: { type: "json_object" } }
            });
            let parsed = null; try { parsed = JSON.parse(resp.output_text || ""); } catch {}
            parts[idx] = parsed; completedChunks++;
            await store.set(`partials/${jobId}/${idx}.json`, JSON.stringify(parsed || {}), { contentType: "application/json" });
            await writeStatus("running", { completedChunks, totalChunks, partial: true });
        } catch (e) { await writeStatus("running", {}, `chunk#${idx} err: ${e.message}`); }
      }
    }

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    
    if (await checkCancelled()) { await releaseLock(); return res(200, { aborted: true }); }

    const merged = rows.map((r) => ({ ...r })); const colSet = new Set();
    for (let idx = 0; idx < totalChunks; idx++) {
      const part = parts[idx]; if (!part) continue;
      const arr = Array.isArray(part.results) ? part.results : (Array.isArray(part) ? part : null);
      if (!arr) continue;
      for (const item of arr) {
        const outIdx = Number(item?.id); if (!Number.isFinite(outIdx) || outIdx < 0 || outIdx >= merged.length) continue;
        if (item && typeof item.cols === "object" && item.cols !== null) {
          for (const [k, v] of Object.entries(item.cols)) { merged[outIdx][k] = (v == null) ? "" : String(v); colSet.add(k); }
        } else if (typeof item?.result === "string") { merged[outIdx].result = item.result; colSet.add("result"); }
      }
    }

    // CRITICAL FIX: Prevent duplicate headers
    const originalHeaders = Object.keys(rows[0] || {});
    const dynamicHeaders = Array.from(colSet).filter(h => !originalHeaders.includes(h));
    const headers = [...originalHeaders, ...dynamicHeaders];

    const csvStr = await new Promise((res, rej) => { csvStringify(merged, { header: true, columns: headers }, (err, out) => err ? rej(err) : res(out)); });
    await store.set(`results/${jobId}.csv`, ensureUtf8Bom(csvStr), { contentType: "text/csv; charset=utf-8" });
    await writeStatus("ready", { completedChunks: totalChunks }, "csv written: ready");

    await releaseLock(); return res(202, { ok: true, jobId });
  } catch (err) {
    try { await writeStatus("failed", {}, `fatal: ${err?.message || String(err)}`); } catch {}
    try { await releaseLock(); } catch {} return res(500, { error: err?.message || String(err) });
  }
};
