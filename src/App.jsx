import React, { useEffect, useRef, useState } from "react";

const API_BASE = "/.netlify/functions";

// JSON Syntax Highlighting Helper
function highlightJSON(json) {
  if (typeof json !== "string") json = JSON.stringify(json, undefined, 2);
  json = json.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, (match) => {
    let cls = "json-number";
    if (/^"/.test(match)) {
      if (/:$/.test(match)) cls = "json-key";
      else cls = "json-string";
    } else if (/true|false/.test(match)) cls = "json-boolean";
    else if (/null/.test(match)) cls = "json-null";
    return `<span class="${cls}">${match}</span>`;
  });
}

export default function App() {
  const [tab, setTab] = useState("run");

  // Run State
  const [file, setFile] = useState(null);
  const [isDragging, setIsDragging] = useState(false);

  // Prefs (Synced to LocalStorage)
  const [inputCol, setInputCol] = useState("text");
  const [prompt, setPrompt] = useState("Translate the user input into English.");
  const [contextDoc, setContextDoc] = useState("");
  const [model, setModel] = useState("gpt-5-nano");
  const [chunkSize, setChunkSize] = useState(500);
  const [reasoningEffort, setReasoningEffort] = useState("medium");
  const [mode, setMode] = useState("batch");
  const [concurrency, setConcurrency] = useState(4);
  const [maxRows, setMaxRows] = useState("");

  // Job State
  const [lastRunMode, setLastRunMode] = useState("batch");
  const [preview, setPreview] = useState(null);
  const [batchId, setBatchId] = useState("");
  const [status, setStatus] = useState("");
  const [jobStats, setJobStats] = useState({ completed: 0, total: 0 });
  const [outputReady, setOutputReady] = useState(false);
  const [downloadUrl, setDownloadUrl] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Console State
  const [logs, setLogs] = useState([]);
  const [consoleOpen, setConsoleOpen] = useState(true);
  const logRef = useRef(null);
  
  function log(msg) { setLogs((prev) => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`]); }
  useEffect(() => { if (logRef.current && consoleOpen) logRef.current.scrollTop = logRef.current.scrollHeight; }, [logs, consoleOpen]);

  // Load Prefs on Mount
  useEffect(() => {
    try {
      const p = JSON.parse(localStorage.getItem("batch-csv-prefs") || "{}");
      if (p.inputCol) setInputCol(p.inputCol);
      if (p.prompt) setPrompt(p.prompt);
      if (p.contextDoc !== undefined) setContextDoc(p.contextDoc);
      if (p.model) setModel(p.model);
      if (p.chunkSize) setChunkSize(p.chunkSize);
      if (p.mode) setMode(p.mode);
      if (p.concurrency) setConcurrency(p.concurrency);
    } catch (e) {}
  }, []);

  // Save Prefs
  useEffect(() => {
    localStorage.setItem("batch-csv-prefs", JSON.stringify({ inputCol, prompt, contextDoc, model, chunkSize, mode, concurrency }));
  }, [inputCol, prompt, contextDoc, model, chunkSize, mode, concurrency]);

  const pollRef = useRef(null);
  function stopPolling() { if (pollRef.current) clearInterval(pollRef.current); pollRef.current = null; }
  function startPolling() { stopPolling(); pollRef.current = setInterval(checkStatus, 3000); }
  function startPollingDirect(id) { stopPolling(); pollRef.current = setInterval(() => checkDirectStatus(id), 2000); }
  
  useEffect(() => {
    const terminal = ["completed", "failed", "cancelled", "expired", "ready"];
    if (terminal.includes(status)) stopPolling();
  }, [status]);

  // Token Estimator Math (Roughly 1 token per 4 chars)
  const estTokens = Math.round((prompt.length + contextDoc.length) / 4);
  const isCachedHit = estTokens >= 1024;
  const barWidth = Math.min((estTokens / 1024) * 100, 100);

  async function submitBatch(e) {
    e.preventDefault();
    setError(""); setOutputReady(false); setStatus(""); setPreview(null); setDownloadUrl(""); setLastRunMode(mode);

    if (!file && tab === "run") return setError("Please choose a CSV file.");
    try {
      setIsSubmitting(true); stopPolling(); setBatchId("");
      const fd = new FormData();
      fd.append("file", file); fd.append("inputCol", inputCol); fd.append("prompt", prompt);
      fd.append("model", model); fd.append("chunkSize", chunkSize); fd.append("reasoning_effort", reasoningEffort);
      if (contextDoc) fd.append("contextDoc", contextDoc);
      if (maxRows) fd.append("maxRows", maxRows);
      if (mode === "dry") fd.append("dryRun", "1");
      if (mode === "direct") { fd.append("direct", "1"); fd.append("concurrency", concurrency); }

      log(`Submit → mode=${mode}, model=${model}`);
      const r = await fetch(`${API_BASE}/batch-create`, { method: "POST", body: fd });
      const bodyText = await r.text();
      
      if (!r.ok) {
        let msg = bodyText; try { msg = JSON.parse(bodyText).error || msg; } catch {}
        throw new Error(msg || `HTTP ${r.status}`);
      }

      const j = JSON.parse(bodyText);
      if (j.mode === "dryRun") {
        setStatus("done"); setPreview(j.parsed || j.results || j); log(`Dry run OK.`); return;
      }
      if (j.mode === "direct") {
        setBatchId(j.jobId); setStatus("running"); setDownloadUrl(j.download);
        log(`Direct job started in background...`); startPollingDirect(j.jobId); return;
      }
      setBatchId(j.batchId); setStatus("submitted"); log(`Batch API queued: ${j.batchId}`); startPolling();
    } catch (err) { setError(err.message); log(`Error: ${err.message}`); } finally { setIsSubmitting(false); }
  }

  async function checkStatus() {
    if (!batchId) return;
    try {
      const r = await fetch(`${API_BASE}/batch-status?id=${encodeURIComponent(batchId)}`);
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Status failed");
      setStatus(j.status);
      if (j.status === "completed") setOutputReady(true);
    } catch (err) { setError(err.message); }
  }

  async function checkDirectStatus(idParam) {
    const id = idParam || batchId; if (!id) return;
    try {
      const r = await fetch(`${API_BASE}/direct-status?id=${encodeURIComponent(id)}`);
      const j = await r.json();
      if (j.error) { setStatus("failed"); setError(j.error); stopPolling(); return; }
      
      setStatus(j.ready ? "ready" : (j.status || "running"));
      setJobStats({ completed: j.completedChunks || 0, total: j.totalChunks || 0 });
      
      if (j.ready) { setOutputReady(true); log("Job finished. CSV ready."); stopPolling(); }
      else if (j.status === "failed" || j.status === "cancelled") { stopPolling(); }
    } catch (err) { log(`Status error: ${err.message}`); }
  }

  async function cancelDirectJob() {
    if (!batchId) return;
    try {
      log("Sending cancel request...");
      await fetch(`${API_BASE}/direct-cancel?id=${encodeURIComponent(batchId)}`);
      setStatus("cancelled"); stopPolling(); log("Job cancelled by user.");
    } catch(e) { log(`Cancel error: ${e.message}`); }
  }

  return (
    <div className="container">
      <h1>OpenAI Batch CSV</h1>
      
      <div style={{ display: "flex", gap: 10, marginBottom: 24, justifyContent: "center" }}>
        <button className={tab === "run" ? "" : "secondary"} onClick={() => setTab("run")}>Run Jobs</button>
        <button className={tab === "reconstruct" ? "" : "secondary"} onClick={() => setTab("reconstruct")}>Reconstruct via Batch ID</button>
      </div>

      {tab === "run" && (
        <div className="app-grid">
          {/* LEFT COLUMN: FORM */}
          <div className="card">
            <h2>Configure Run</h2>
            <form onSubmit={submitBatch}>
              <div 
                className={`dropzone ${isDragging ? "active" : ""}`}
                onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
                onDragLeave={() => setIsDragging(false)}
                onDrop={(e) => { e.preventDefault(); setIsDragging(false); setFile(e.dataTransfer.files[0]); log(`File attached: ${e.dataTransfer.files[0].name}`); }}
                onClick={() => document.getElementById("fileInput").click()}
              >
                <input id="fileInput" type="file" accept=".csv" style={{ display: "none" }} onChange={(e) => { setFile(e.target.files[0]); log(`File attached: ${e.target.files[0].name}`); }} />
                {file ? <p className="file-name">{file.name} ({(file.size/1024).toFixed(1)} KB)</p> : <p>Drag & Drop a CSV file here, or click to browse</p>}
              </div>

              <div className="form-group">
                <label>Input Column Name <span className="hint">The CSV header to send to the AI</span></label>
                <input value={inputCol} onChange={(e) => setInputCol(e.target.value)} placeholder="text" />
              </div>

              <div className="form-group">
                <label>Reference Context (Optional) <span className="hint">Paste large glossaries or examples here to trigger Prompt Caching (50% discount).</span></label>
                <textarea rows={3} value={contextDoc} onChange={(e) => setContextDoc(e.target.value)} placeholder="e.g. Master glossary definitions..." />
              </div>

              <div className="form-group">
                <label>Instruction Prompt <span className="hint">What should the AI do with each row?</span></label>
                <textarea rows={4} value={prompt} onChange={(e) => setPrompt(e.target.value)} required />
              </div>

              {/* Token Estimator Widget */}
              <div className="form-group" style={{ background: "#f8fafc", padding: "12px", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
                <div style={{display: "flex", justifyContent: "space-between"}}>
                  <label style={{margin:0}}>System Prompt Size</label>
                  {isCachedHit && <span className="cached-badge">✓ Caching Active</span>}
                </div>
                <div className="token-bar-bg">
                  <div className={`token-bar-fill ${isCachedHit ? "cached" : ""}`} style={{ width: `${barWidth}%` }}></div>
                </div>
                <div className="token-status">
                  <span style={{color: "#64748b"}}>~{estTokens} tokens</span>
                  <span style={{color: "#64748b"}}>Limit: 1,024</span>
                </div>
              </div>

              <div className="flex-row form-group">
                <div>
                  <label>Model</label>
                  <select value={model} onChange={(e) => setModel(e.target.value)}>
                    <optgroup label="Budget (Best for bulk)">
                      <option value="gpt-5-nano">gpt-5-nano</option>
                      <option value="gpt-4.1-mini">gpt-4.1-mini</option>
                    </optgroup>
                    <optgroup label="Flagship">
                      <option value="gpt-5-mini">gpt-5-mini</option>
                      <option value="gpt-5">gpt-5</option>
                    </optgroup>
                  </select>
                </div>
                <div>
                  <label>Mode <span className="hint">Strategy</span></label>
                  <select value={mode} onChange={(e) => setMode(e.target.value)}>
                    <option value="batch">Batch API (Cheapest)</option>
                    <option value="direct">Direct (Instant)</option>
                    <option value="dry">Dry Run (Test 1st chunk)</option>
                  </select>
                </div>
              </div>

              <div className="flex-row form-group">
                <div>
                  <label>Rows per Request (K) <span className="hint">Max 1000</span></label>
                  <input type="number" min={1} max={1000} value={chunkSize} onChange={(e) => setChunkSize(Number(e.target.value || 1))} />
                </div>
                <div>
                  <label>Max Test Rows <span className="hint">0 = All</span></label>
                  <input type="number" min={0} value={maxRows} onChange={(e) => setMaxRows(e.target.value)} />
                </div>
                {mode === "direct" && (
                  <div>
                    <label>Concurrency <span className="hint">Parallel calls</span></label>
                    <input type="number" min={1} value={concurrency} onChange={(e) => setConcurrency(Number(e.target.value || 1))} />
                  </div>
                )}
              </div>

              <button type="submit" disabled={isSubmitting} style={{ width: "100%", marginTop: "10px", padding: "12px" }}>
                {isSubmitting ? "Processing..." : (mode === "batch" ? "Create Batch Job" : "Run Now")}
              </button>
            </form>
          </div>

          {/* RIGHT COLUMN: STATUS & CONSOLE */}
          <div>
            {(batchId || preview || error) && (
              <div className="card" style={{ marginBottom: "24px" }}>
                <h2>Job Status</h2>
                {batchId && (
                  <div style={{ marginBottom: 16 }}>
                    <p style={{ margin: "0 0 8px 0" }}><strong>ID:</strong> <span style={{ fontFamily: "monospace", fontSize: 13 }}>{batchId}</span></p>
                    <p style={{ margin: "0 0 8px 0" }}><strong>Status:</strong> <span style={{ textTransform: "capitalize", color: status === "failed" ? "red" : status === "ready" || status === "completed" ? "green" : "#0066ff", fontWeight: 600 }}>{status || "running"}</span></p>
                    
                    {lastRunMode === "direct" && status === "running" && (
                      <div style={{ marginTop: 16, marginBottom: 16 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 4, fontWeight: 600 }}>
                          <span>Processing Chunks</span>
                          <span>{jobStats.completed} / {jobStats.total}</span>
                        </div>
                        <progress value={jobStats.completed} max={jobStats.total} />
                      </div>
                    )}
                  </div>
                )}
                
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {batchId && lastRunMode === "batch" && <button className="secondary" onClick={checkStatus}>Refresh</button>}
                  {batchId && lastRunMode === "direct" && status === "running" && <button className="danger" onClick={cancelDirectJob}>Abort Job</button>}
                  {batchId && <button onClick={() => window.location.href = downloadUrl || `${API_BASE}/batch-download?id=${encodeURIComponent(batchId)}`} disabled={!outputReady}>Download Merged CSV</button>}
                  {batchId && lastRunMode === "direct" && !outputReady && status !== "cancelled" && <button className="secondary" onClick={() => window.location.href = `${API_BASE}/batch-download?id=${encodeURIComponent(batchId)}&partial=1`}>Download Partial CSV</button>}
                </div>
                
                {error && <div style={{ marginTop: 16, padding: 12, background: "#fee2e2", color: "#b91c1c", borderRadius: 6, fontSize: 13 }}><strong>Error:</strong> {error}</div>}

                {/* Dry Run Preview with Syntax Highlighting */}
                {!batchId && preview && lastRunMode === "dry" && (
                  <div style={{ marginTop: 16 }}>
                    <h3 style={{ fontSize: 14, margin: "0 0 8px 0" }}>Dry Run Output:</h3>
                    <div className="json-preview" dangerouslySetInnerHTML={{ __html: highlightJSON(preview) }}></div>
                  </div>
                )}
              </div>
            )}

            {/* Collapsible Console */}
            <div className="console-header" onClick={() => setConsoleOpen(!consoleOpen)}>
              <h3>Event Console</h3>
              <span style={{fontSize: 12}}>{consoleOpen ? "▼ Hide" : "▲ Show"}</span>
            </div>
            {consoleOpen && (
              <div className="console-body" ref={logRef}>
                {logs.length ? logs.map((l, i) => <div key={i}>{l}</div>) : "Awaiting events..."}
              </div>
            )}
          </div>
        </div>
      )}

      {tab === "reconstruct" && (
        <div className="card" style={{ maxWidth: 600, margin: "0 auto" }}>
          <h2>Reconstruct CSV</h2>
          <p style={{ fontSize: 14, color: "#555", marginBottom: 20 }}>Paste an OpenAI <code>batch_...</code> ID to rebuild the CSV from OpenAI’s stored JSONL output files.</p>
          {/* Form simplified for brevity, matches original reconstruct form */}
          <div className="form-group">
            <label>Batch ID</label>
            <input placeholder="batch_XXXXXXXXXXXX" id="reBatchId" />
          </div>
          <button onClick={() => window.location.href = `${API_BASE}/batch-reconstruct?id=${document.getElementById("reBatchId").value}`}>Rebuild & Download</button>
        </div>
      )}
    </div>
  );
}
