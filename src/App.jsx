// src/App.jsx
import React, { useEffect, useRef, useState } from "react";

const API_BASE = "/.netlify/functions"; // call functions directly to avoid rewrite issues

export default function App() {
  // ========== TAB ==========
  const [tab, setTab] = useState("run"); // "run" | "reconstruct"

  // ========== RUN (existing) ==========
  const [file, setFile] = useState(null);
  const [inputCol, setInputCol] = useState("text");
  const [prompt, setPrompt] = useState("Translate the user input into English.");
  const [model, setModel] = useState("gpt-4.1-mini");
  const [chunkSize, setChunkSize] = useState(200); // rows per request (K)
  const [reasoningEffort, setReasoningEffort] = useState("medium"); // minimal|low|medium|high

  const [mode, setMode] = useState("batch"); // "batch" | "dry" | "direct"
  const [maxRows, setMaxRows] = useState(""); // limit rows during testing
  const [concurrency, setConcurrency] = useState(4); // direct mode only
  const [lastRunMode, setLastRunMode] = useState("batch");
  const [preview, setPreview] = useState(null);

  const [batchId, setBatchId] = useState("");
  const [status, setStatus] = useState("");
  const [outputReady, setOutputReady] = useState(false);
  const [downloadUrl, setDownloadUrl] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Debug console
  const [logs, setLogs] = useState([]);
  const logRef = useRef(null);
  function log(msg) {
    const line = `[${new Date().toLocaleTimeString()}] ${msg}`;
    setLogs((prev) => [...prev, line]);
  }
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs]);

  const pollRef = useRef(null);
  function stopPolling() {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = null;
  }
  function startPolling() {
    stopPolling();
    pollRef.current = setInterval(checkStatus, 3000);
  }
  function startPollingDirect(id) {
    stopPolling();
    pollRef.current = setInterval(() => checkDirectStatus(id), 3000);
  }
  function downloadPartial() {
    if (!batchId) return;
    log("Downloading partial CSV…");
    const url = `${API_BASE}/batch-download?id=${encodeURIComponent(batchId)}&partial=1`;
    window.location.href = url;
  }
  useEffect(() => {
    const terminal = ["completed", "failed", "cancelled", "expired", "ready"];
    if (terminal.includes(status)) stopPolling();
  }, [status]);

  const isGpt5 = model.startsWith("gpt-5");
  const isOseries = /^o\d/i.test(model) || model.startsWith("o");

  async function submitBatch(e) {
    e.preventDefault();
    setError("");
    setOutputReady(false);
    setStatus("");
    setPreview(null);
    setDownloadUrl("");
    setLastRunMode(mode);

    if (!file) {
      setError("Please choose a CSV file.");
      log("No file selected.");
      return;
    }

    try {
      setIsSubmitting(true);
      stopPolling();
      setBatchId("");

      log(`Preparing form…`);
      log(`File: ${file.name} (${file.size.toLocaleString()} bytes)`);

      const fd = new FormData();
      fd.append("file", file);
      fd.append("inputCol", inputCol);
      fd.append("prompt", prompt);
      fd.append("model", model);
      fd.append("chunkSize", String(Math.max(1, Math.min(1000, Number(chunkSize) || 1))));
      fd.append("reasoning_effort", reasoningEffort);

      if (maxRows) fd.append("maxRows", String(maxRows));
      if (mode === "dry") fd.append("dryRun", "1");
      if (mode === "direct") {
        fd.append("direct", "1");
        fd.append("concurrency", String(concurrency || 4));
      }

      log(
        `Submit → mode=${mode}, model=${model}, inputCol=${inputCol}, K=${chunkSize}, reasoning=${reasoningEffort}` +
          (maxRows ? `, maxRows=${maxRows}` : "") +
          (mode === "direct" ? `, concurrency=${concurrency}` : "")
      );
      const t0 = performance.now();

      const r = await fetch(`${API_BASE}/batch-create`, { method: "POST", body: fd });
      const bodyText = await r.text().catch(() => "");

      const dt = ((performance.now() - t0) / 1000).toFixed(2);
      log(`Response ${r.status} in ${dt}s`);

      if (!r.ok) {
        let msg = bodyText;
        try { msg = JSON.parse(bodyText).error || msg; } catch {}
        setError(msg || `Request failed (HTTP ${r.status})`);
        log(`Error: ${msg || `HTTP ${r.status}`}`);
        return;
      }

      let j = {};
      try { j = JSON.parse(bodyText || "{}"); } catch {}
      const returnedMode = j.mode || mode;

      if (returnedMode === "dryRun") {
        setBatchId("");
        setStatus("done");
        setPreview(j.parsed || j.results || j);
        log(`Dry run OK. Used rows: ${j.usedRows ?? "?"}. Preview ready below.`);
        return;
      }

      if (returnedMode === "direct") {
        const id = j.jobId || "";
        setBatchId(id);
        setStatus("running");
        setOutputReady(false);
        setPreview(null);
        setDownloadUrl(j.download || `${API_BASE}/batch-download?id=${encodeURIComponent(id)}`);
        log(`Direct job started. Processing in the background… Polling for readiness.`);
        startPollingDirect(id);
        return;
      }

      if (!j.batchId) {
        setError("No batchId returned from server.");
        log("Error: No batchId in response.");
        return;
      }
      setBatchId(j.batchId);
      setStatus("submitted");
      log(`Batch created: ${j.batchId}`);
      log("Starting status polling every 3s…");
      startPolling();
    } catch (err) {
      setError(err?.message || String(err));
      log(`Network/Unhandled error: ${err?.message || String(err)}`);
    } finally {
      setIsSubmitting(false);
    }
  }

  async function checkStatus() {
    if (!batchId) return;
    try {
      const r = await fetch(`${API_BASE}/batch-status?id=${encodeURIComponent(batchId)}`);
      const t = await r.text();
      if (!r.ok) {
        let msg = t; try { msg = JSON.parse(t).error || msg; } catch {}
        setError(msg || `Status failed (HTTP ${r.status})`);
        log(`Status error: ${msg || `HTTP ${r.status}`}`);
        return;
      }
      let j = {}; try { j = JSON.parse(t); } catch {}
      const s = j.status || "(unknown)";
      setStatus(s);
      log(`Batch ${j.id || batchId} → ${s}`);
      if (s === "completed") { setOutputReady(true); log("Batch completed. You can download the merged CSV."); }
    } catch (err) {
      setError(err?.message || String(err));
      log(`Status exception: ${err?.message || String(err)}`);
    }
  }

  async function checkDirectStatus(idParam) {
    const id = idParam || batchId;
    if (!id) return;
    try {
      const r = await fetch(`${API_BASE}/direct-status?id=${encodeURIComponent(id)}`);
      const t = await r.text();
      let j = {}; try { j = JSON.parse(t); } catch {}

      if (j.error) {
        setStatus("failed");
        setOutputReady(false);
        setError(j.error);
        log(`Direct job failed: ${j.error}`);
        stopPolling();
        return;
      }

      if (j.totalChunks) {
        log(`Progress: ${j.completedChunks}/${j.totalChunks} chunks, ${j.processedRows ?? "?"} rows processed`);
      }
      if (Array.isArray(j.events) && j.events.length) {
        const last = j.events[j.events.length - 1];
        if (last?.ts && last?.msg) log(`bg: ${last.msg}`);
      }

      if (j.ready) {
        setStatus("ready");
        setOutputReady(true);
        log("Direct job finished. CSV is ready to download.");
        stopPolling();
      } else if (j.status === "failed") {
        setStatus("failed");
        setOutputReady(false);
        setError(j.error || "Direct job failed.");
        log(`Direct job failed: ${j.error || "(unknown error)"}`);
        stopPolling();
      } else {
        setStatus(j.status || "running");
      }
    } catch (err) {
      log(`Direct status error: ${err?.message || String(err)}`);
    }
  }

  function downloadOutput() {
    if (!batchId) return;
    log("Downloading merged CSV…");
    const url = downloadUrl || `${API_BASE}/batch-download?id=${encodeURIComponent(batchId)}`;
    window.location.href = url;
  }

  function clearLogs() { setLogs([]); }
  async function copyLogs() {
    try { await navigator.clipboard.writeText(logs.join("\n")); log("Logs copied to clipboard."); }
    catch { log("Copy failed (clipboard permissions)."); }
  }

  // ========== RECONSTRUCT UI ==========
  const [reBatchId, setReBatchId] = useState("");
  const [reBusy, setReBusy] = useState(false);
  const [reErr, setReErr] = useState("");
  const [reCsvPreview, setReCsvPreview] = useState("");
  const [reDownloadHref, setReDownloadHref] = useState("");

  async function reconstructNow(e) {
    e.preventDefault();
    setReErr("");
    setReCsvPreview("");
    setReDownloadHref("");

    const id = reBatchId.trim();
    if (!id || !id.startsWith("batch_")) {
      setReErr("Enter a valid id that starts with batch_…");
      return;
    }

    try {
      setReBusy(true);
      const url = `${API_BASE}/batch-reconstruct?id=${encodeURIComponent(id)}`;
      setReDownloadHref(url);

      // Fetch a text copy to preview a few lines
      const r = await fetch(url, { method: "GET" });
      const txt = await r.text();
      if (!r.ok) {
        let msg = txt; try { msg = JSON.parse(txt).error || msg; } catch {}
        setReErr(msg || `Failed (HTTP ${r.status})`);
        return;
      }

      // Keep preview short
      const lines = txt.split(/\r?\n/).slice(0, 15).join("\n");
      setReCsvPreview(lines);
    } catch (err) {
      setReErr(err?.message || String(err));
    } finally {
      setReBusy(false);
    }
  }

  return (
    <div className="container" style={{ maxWidth: 960, margin: "40px auto", fontFamily: "system-ui, sans-serif" }}>
      <h1>OpenAI Batch CSV</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 10, marginBottom: 16 }}>
        <button
          onClick={() => setTab("run")}
          style={{
            padding: "8px 12px",
            borderRadius: 8,
            border: "1px solid #ddd",
            background: tab === "run" ? "#eef3ff" : "white",
            cursor: "pointer"
          }}
        >
          Run Jobs
        </button>
        <button
          onClick={() => setTab("reconstruct")}
          style={{
            padding: "8px 12px",
            borderRadius: 8,
            border: "1px solid #ddd",
            background: tab === "reconstruct" ? "#eef3ff" : "white",
            cursor: "pointer"
          }}
        >
          Reconstruct by batch id
        </button>
      </div>

      {tab === "run" && (
        <>
          <p>
            Upload a CSV, choose the input column, write your instruction, pick a model, set <em>Rows per request (K)</em>,
            then choose a <strong>Mode</strong>. <strong>Batch</strong> queues on OpenAI (slow). <strong>Dry run</strong> tests first chunk instantly.
            <strong> Direct</strong> processes everything now with parallel requests.
          </p>

          <form onSubmit={submitBatch} style={{ display: "grid", gap: 12 }}>
            <label>CSV File
              <input type="file" accept=".csv" onChange={(e) => {
                const f = e.target.files?.[0] || null;
                setFile(f);
                if (f) log(`Selected file: ${f.name} (${f.size.toLocaleString()} bytes)`);
              }}/>
            </label>

            <label>Input column name
              <input value={inputCol} onChange={(e) => setInputCol(e.target.value)} placeholder="text" />
            </label>

            <label>Instruction prompt
              <textarea rows={6} value={prompt} onChange={(e) => setPrompt(e.target.value)}
                        placeholder="Describe what to do with each row..." />
            </label>

            <label>Model
              <select value={model} onChange={(e) => setModel(e.target.value)}>
                {/* Add GPT-5 options if your org has access */}
                <option>gpt-5</option>
                <option>gpt-5-mini</option>
                <option>gpt-5-nano</option>
                <option>gpt-4.1-mini</option>
                <option>gpt-4o-mini</option>
                <option>gpt-4.1</option>
              </select>
            </label>

            {(isGpt5 || isOseries) && (
              <label>Reasoning effort
                <select value={reasoningEffort} onChange={e => setReasoningEffort(e.target.value)}>
                  <option value="minimal">minimal</option>
                  <option value="low">low</option>
                  <option value="medium">medium</option>
                  <option value="high">high</option>
                </select>
              </label>
            )}

            <label>Rows per request (K)
              <input
                type="number"
                min={1}
                max={1000}
                value={chunkSize}
                onChange={(e) => setChunkSize(Number(e.target.value || 1))}
              />
            </label>

            <div style={{ display: "grid", gap: 8 }}>
              <label>Mode
                <select value={mode} onChange={(e) => setMode(e.target.value)}>
                  <option value="batch">Batch (slow, cheapest at scale)</option>
                  <option value="dry">Dry run (first chunk now)</option>
                  <option value="direct">Direct (process all now)</option>
                </select>
              </label>

              <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                <label>Max rows (test)
                  <input
                    type="number"
                    min={0}
                    placeholder="0 = all"
                    value={maxRows}
                    onChange={(e) => setMaxRows(e.target.value)}
                    style={{ width: 120, marginLeft: 8 }}
                  />
                </label>

                <label>Concurrency
                  <input
                    type="number"
                    min={1}
                    value={concurrency}
                    onChange={(e) => setConcurrency(Number(e.target.value || 1))}
                    style={{ width: 90, marginLeft: 8 }}
                    disabled={mode !== "direct"}
                    title="Parallel /v1/responses calls (Direct mode only)"
                  />
                </label>
              </div>
            </div>

            <button type="submit" disabled={isSubmitting} style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              {isSubmitting ? (mode === "batch" ? "Creating Batch…" : "Processing…") : (mode === "batch" ? "Create Batch" : "Run Now")}
              {isSubmitting && <span aria-hidden>⏳</span>}
            </button>
          </form>

          {/* Job panel (Batch and Direct) */}
          {batchId && (
            <div style={{ marginTop: 16, padding: 12, border: "1px solid #ddd", borderRadius: 8 }}>
              <div><strong>Job ID:</strong> {batchId}</div>
              <div><strong>Status:</strong> {status || (lastRunMode === "direct" ? "running" : "(unknown)")} </div>

              {lastRunMode === "direct" && !outputReady && (
                <div style={{ marginTop: 6, fontSize: 13, opacity: 0.8 }}>
                  Building CSV in the background… you can refresh the status or leave this tab.
                </div>
              )}

              <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
                {lastRunMode === "batch"  && <button onClick={checkStatus}>Refresh Status</button>}
                {lastRunMode === "direct" && <button onClick={() => checkDirectStatus()}>Refresh Status</button>}

                <button onClick={downloadOutput} disabled={!outputReady}>Download merged CSV</button>

                {lastRunMode === "direct" && !outputReady && (
                  <button onClick={downloadPartial}>Download partial CSV</button>
                )}

                {lastRunMode === "direct" && downloadUrl && (
                  <a href={downloadUrl} style={{ alignSelf: "center" }}>Open link</a>
                )}
              </div>

              {error && <p style={{ color: "crimson", marginTop: 8 }}>{error}</p>}
            </div>
          )}

          {/* Dry preview panel */}
          {!batchId && preview && lastRunMode === "dry" && (
            <div style={{ marginTop: 16, padding: 12, border: "1px solid #ddd", borderRadius: 8 }}>
              <div style={{ marginBottom: 6 }}>
                <strong>Preview</strong> (Dry run: first chunk only)
              </div>
              <pre style={{
                background: "#0b1020", color: "#d7e3ff", padding: 12, borderRadius: 8,
                maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.45
              }}>
{JSON.stringify(preview, null, 2)}
              </pre>
              {error && <p style={{ color: "crimson", marginTop: 8 }}>{error}</p>}
            </div>
          )}

          {/* Console */}
          <div style={{ marginTop: 24 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
              <h2 style={{ margin: 0, fontSize: 18 }}>Console</h2>
              <div style={{ display: "flex", gap: 8 }}>
                <button type="button" onClick={clearLogs}>Clear</button>
                <button type="button" onClick={copyLogs}>Copy</button>
              </div>
            </div>
            <pre ref={logRef} style={{
              background: "#0b1020", color: "#d7e3ff", padding: 12, borderRadius: 8,
              maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.45
            }}>
{logs.length ? logs.join("\n") : "Console will show progress, HTTP codes, and errors…"}
            </pre>
          </div>
        </>
      )}

      {tab === "reconstruct" && (
        <div style={{ padding: 12, border: "1px solid #ddd", borderRadius: 8 }}>
          <p>
            Paste an OpenAI <code>batch_…</code> id to rebuild the CSV directly from OpenAI’s stored JSONL.
            This works even if your original UI was closed and your blobs are gone.
          </p>

          <form onSubmit={reconstructNow} style={{ display: "grid", gap: 10 }}>
            <label>Batch ID
              <input
                placeholder="batch_XXXXXXXXXXXX"
                value={reBatchId}
                onChange={(e) => setReBatchId(e.target.value)}
              />
            </label>

            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <button type="submit" disabled={reBusy}>
                {reBusy ? "Rebuilding…" : "Rebuild CSV"}
              </button>
              {reDownloadHref && (
                <a href={reDownloadHref} style={{ alignSelf: "center" }}>
                  Download link
                </a>
              )}
            </div>
          </form>

          {reErr && <p style={{ color: "crimson", marginTop: 8 }}>{reErr}</p>}

          {reCsvPreview && (
            <div style={{ marginTop: 12 }}>
              <div style={{ marginBottom: 6 }}>
                <strong>CSV preview</strong> (first ~15 lines)
              </div>
              <pre style={{
                background: "#0b1020", color: "#d7e3ff", padding: 12, borderRadius: 8,
                maxHeight: 260, overflow: "auto", whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.45
              }}>
{reCsvPreview}
              </pre>
              <p style={{ fontSize: 12, opacity: 0.7 }}>
                The full CSV is saved at <code>results/&lt;batch_id&gt;.reconstructed.csv</code> in Netlify Blobs and downloadable via the link above.
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
