import React, { useEffect, useRef, useState } from "react";

const API_BASE = "/.netlify/functions";

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
  const [file, setFile] = useState(null);
  const [isDragging, setIsDragging] = useState(false);

  const [inputCol, setInputCol] = useState("text");
  const [prompt, setPrompt] = useState("Translate the user input into English.");
  const [contextDoc, setContextDoc] = useState("");
  const [model, setModel] = useState("gpt-5.4-nano");
  const [chunkSize, setChunkSize] = useState(500);
  const [reasoningEffort, setReasoningEffort] = useState("medium");
  const [mode, setMode] = useState("batch");
  const [concurrency, setConcurrency] = useState(4);
  const [maxRows, setMaxRows] = useState("");

  const [lastRunMode, setLastRunMode] = useState("batch");
  const [preview, setPreview] = useState(null);
  const [batchId, setBatchId] = useState("");
  const [status, setStatus] = useState("");
  const [jobStats, setJobStats] = useState({ completed: 0, total: 0 });
  const [outputReady, setOutputReady] = useState(false);
  const [downloadUrl, setDownloadUrl] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Reconstruct Tab State
  const [reBatchIds, setReBatchIds] = useState("");
  const [analysis, setAnalysis] = useState(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isRepairing, setIsRepairing] = useState(false);

  const [logs, setLogs] = useState([]);
  const [consoleOpen, setConsoleOpen] = useState(true);
  const logRef = useRef(null);
  
  function log(msg) { setLogs((prev) => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`]); }
  useEffect(() => { if (logRef.current && consoleOpen) logRef.current.scrollTop = logRef.current.scrollHeight; }, [logs, consoleOpen]);

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

  useEffect(() => { localStorage.setItem("batch-csv-prefs", JSON.stringify({ inputCol, prompt, contextDoc, model, chunkSize, mode, concurrency })); }, [inputCol, prompt, contextDoc, model, chunkSize, mode, concurrency]);

  const pollRef = useRef(null);
  function stopPolling() { if (pollRef.current) clearInterval(pollRef.current); pollRef.current = null; }
  function startPolling() { stopPolling(); pollRef.current = setInterval(checkStatus, 3000); }
  function startPollingDirect(id) { stopPolling(); pollRef.current = setInterval(() => checkDirectStatus(id), 2000); }
  
  useEffect(() => { const terminal = ["completed", "failed", "cancelled", "expired", "ready"]; if (terminal.includes(status)) stopPolling(); }, [status]);

  const estTokens = Math.round((prompt.length + contextDoc.length) / 4);
  const isCachedHit = estTokens >= 1024;
  const barWidth = Math.min((estTokens / 1024) * 100, 100);

  async function submitBatch(e) {
    e.preventDefault(); setError(""); setOutputReady(false); setStatus(""); setPreview(null); setDownloadUrl(""); setLastRunMode(mode);
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
      if (!r.ok) throw new Error(JSON.parse(bodyText).error || `HTTP ${r.status}`);
      const j = JSON.parse(bodyText);

      if (j.mode === "dryRun") { setStatus("done"); setPreview(j.parsed || j.results || j); log(`Dry run OK.`); return; }
      if (j.mode === "direct") { setBatchId(j.jobId); setStatus("running"); setDownloadUrl(j.download); log(`Direct job started...`); startPollingDirect(j.jobId); return; }
      
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
      if (j.status === "failed" && j.errors?.data?.length > 0) { log(`❌ OpenAI Batch Failed: ${j.errors.data[0].message}`); setError(`OpenAI rejected the batch: ${j.errors.data[0].message}`); stopPolling(); return; }
      if (j.request_counts) setJobStats({ completed: j.request_counts.completed + j.request_counts.failed, total: j.request_counts.total });
      if (j.status === "completed") { setOutputReady(true); log(j.request_counts?.failed > 0 ? `⚠️ Job completed with ${j.request_counts.failed} failed rows. Use Reconstruct Tab to view and repair.` : "✅ Batch completed successfully."); }
    } catch (err) { setError(err.message); }
  }

  async function checkDirectStatus(idParam) {
    const id = idParam || batchId; if (!id) return;
    try {
      const r = await fetch(`${API_BASE}/direct-status?id=${encodeURIComponent(id)}`);
      const j = await r.json();
      if (j.error) { setStatus("failed"); setError(j.error); stopPolling(); return; }
      setStatus(j.ready ? "ready" : (j.status || "running")); setJobStats({ completed: j.completedChunks || 0, total: j.totalChunks || 0 });
      if (j.ready) { setOutputReady(true); log("Job finished. CSV ready."); stopPolling(); }
      else if (j.status === "failed" || j.status === "cancelled") stopPolling();
    } catch (err) { log(`Status error: ${err.message}`); }
  }

  async function cancelDirectJob() {
    if (!batchId) return;
    try { log("Sending cancel request..."); await fetch(`${API_BASE}/direct-cancel?id=${encodeURIComponent(batchId)}`); setStatus("cancelled"); stopPolling(); log("Job cancelled by user."); } catch(e) {}
  }

  // --- NEW RECONSTRUCT & REPAIR LOGIC ---
  async function handleAnalyze() {
    if (!reBatchIds.trim()) return;
    setIsAnalyzing(true); setAnalysis(null); setError("");
    try {
      const r = await fetch(`${API_BASE}/batch-reconstruct?id=${encodeURIComponent(reBatchIds)}&analyze=1`);
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Analysis failed.");
      setAnalysis(j);
      log(`Analysis complete. Found ${j.missingCount} missing rows.`);
    } catch (e) { setError(e.message); } finally { setIsAnalyzing(false); }
  }

  async function handleRepair() {
    if (!analysis?.missingIds || analysis.missingIds.length === 0) return;
    setIsRepairing(true); setError("");
    try {
      log(`Submitting repair batch for ${analysis.missingIds.length} rows...`);
      const r = await fetch(`${API_BASE}/batch-repair`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ batchIds: reBatchIds, missingIds: analysis.missingIds })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Repair failed.");
      
      const updatedBatchIds = `${reBatchIds}, ${j.newBatchId}`;
      setReBatchIds(updatedBatchIds);
      log(`✅ Repair batch submitted! ID: ${j.newBatchId}`);
      
      // Auto-switch to Run tab to track the new batch
      setBatchId(j.newBatchId);
      setMode("batch"); setLastRunMode("batch");
      setStatus("submitted"); setOutputReady(false);
      setTab("run"); startPolling();

    } catch(e) { setError(e.message); log(`❌ Repair Error: ${e.message}`); } finally { setIsRepairing(false); }
  }

  return (
    <div className="container">
      <h1>OpenAI Batch CSV</h1>
      
      <div style={{ display: "flex", gap: 10, marginBottom: 24, justifyContent: "center" }}>
        <button className={tab === "run" ? "" : "secondary"} onClick={() => setTab("run")}>Run Jobs</button>
        <button className={tab === "reconstruct" ? "" : "secondary"} onClick={() => setTab("reconstruct")}>Reconstruct & Repair</button>
      </div>

      {tab === "run" && (
        <div className="app-grid">
          {/* LEFT COLUMN: FORM */}
          <div className="card">
            <h2>Configure Run</h2>
            <form onSubmit={submitBatch}>
              <div className={`dropzone ${isDragging ? "active" : ""}`} onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }} onDragLeave={() => setIsDragging(false)} onDrop={(e) => { e.preventDefault(); setIsDragging(false); setFile(e.dataTransfer.files[0]); log(`File attached: ${e.dataTransfer.files[0].name}`); }} onClick={() => document.getElementById("fileInput").click()}>
                <input id="fileInput" type="file" accept=".csv" style={{ display: "none" }} onChange={(e) => { setFile(e.target.files[0]); log(`File attached: ${e.target.files[0].name}`); }} />
                {file ? <p className="file-name">{file.name} ({(file.size/1024).toFixed(1)} KB)</p> : <p>Drag & Drop a CSV file here</p>}
              </div>

              <div className="form-group"><label>Input Column Name</label><input value={inputCol} onChange={(e) => setInputCol(e.target.value)} placeholder="text" /></div>
              <div className="form-group"><label>Reference Context (Optional) <span className="hint">Paste large data here to trigger 50% Prompt Caching discount.</span></label><textarea rows={3} value={contextDoc} onChange={(e) => setContextDoc(e.target.value)} /></div>
              <div className="form-group"><label>Instruction Prompt</label><textarea rows={4} value={prompt} onChange={(e) => setPrompt(e.target.value)} required /></div>

              <div className="form-group" style={{ background: "#f8fafc", padding: "12px", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
                <div style={{display: "flex", justifyContent: "space-between"}}><label style={{margin:0}}>System Prompt Size</label>{isCachedHit && <span className="cached-badge">✓ Caching Active</span>}</div>
                <div className="token-bar-bg"><div className={`token-bar-fill ${isCachedHit ? "cached" : ""}`} style={{ width: `${barWidth}%` }}></div></div>
                <div className="token-status"><span style={{color: "#64748b"}}>~{estTokens} tokens</span><span style={{color: "#64748b"}}>Limit: 1,024</span></div>
              </div>

              <div className="flex-row form-group">
                <div><label>Model</label><select value={model} onChange={(e) => setModel(e.target.value)}>
                    <optgroup label="Current Generation (5.4)"><option value="gpt-5.4-nano">gpt-5.4-nano</option><option value="gpt-5.4-mini">gpt-5.4-mini</option><option value="gpt-5.4">gpt-5.4</option></optgroup>
                    <optgroup label="Mid-Generation (5.3)"><option value="gpt-5.3-chat-latest">gpt-5.3 Instant</option></optgroup>
                    <optgroup label="Previous Generation"><option value="gpt-5-nano">gpt-5-nano</option><option value="gpt-5-mini">gpt-5-mini</option></optgroup>
                  </select></div>
                <div><label>Mode</label><select value={mode} onChange={(e) => setMode(e.target.value)}><option value="batch">Batch API (Cheapest)</option><option value="direct">Direct (Instant)</option><option value="dry">Dry Run</option></select></div>
              </div>

              <div className="flex-row form-group">
                <div><label>Rows/Request (K)</label><input type="number" min={1} max={1000} value={chunkSize} onChange={(e) => setChunkSize(Number(e.target.value || 1))} /></div>
                <div><label>Max Test Rows</label><input type="number" min={0} value={maxRows} onChange={(e) => setMaxRows(e.target.value)} /></div>
              </div>

              <button type="submit" disabled={isSubmitting} style={{ width: "100%", padding: "12px" }}>{isSubmitting ? "Processing..." : "Run Now"}</button>
            </form>
          </div>

          {/* RIGHT COLUMN: STATUS */}
          <div>
            {(batchId || preview || error) && (
              <div className="card" style={{ marginBottom: "24px" }}>
                <h2>Job Status</h2>
                {batchId && (
                  <div style={{ marginBottom: 16 }}>
                    <p style={{ margin: "0 0 8px 0" }}><strong>ID:</strong> <span style={{ fontFamily: "monospace", fontSize: 13 }}>{batchId}</span></p>
                    <p style={{ margin: "0 0 8px 0" }}><strong>Status:</strong> <span style={{ textTransform: "capitalize", color: status === "failed" ? "red" : status === "ready" || status === "completed" ? "green" : "#0066ff", fontWeight: 600 }}>{status || "running"}</span></p>
                    {lastRunMode === "direct" && status === "running" && (<div style={{ marginTop: 16, marginBottom: 16 }}><div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 4, fontWeight: 600 }}><span>Processing Chunks</span><span>{jobStats.completed} / {jobStats.total}</span></div><progress value={jobStats.completed} max={jobStats.total} /></div>)}
                  </div>
                )}
                
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {batchId && lastRunMode === "batch" && <button className="secondary" onClick={checkStatus}>Refresh</button>}
                  {batchId && lastRunMode === "direct" && status === "running" && <button className="danger" onClick={cancelDirectJob}>Abort Job</button>}
                  {batchId && <button onClick={() => window.location.href = downloadUrl || `${API_BASE}/batch-download?id=${encodeURIComponent(batchId)}`} disabled={!outputReady}>Download Merged CSV</button>}
                </div>
                {error && <div style={{ marginTop: 16, padding: 12, background: "#fee2e2", color: "#b91c1c", borderRadius: 6, fontSize: 13 }}><strong>Error:</strong> {error}</div>}
                {!batchId && preview && lastRunMode === "dry" && (<div style={{ marginTop: 16 }}><h3 style={{ fontSize: 14 }}>Dry Run Output:</h3><div className="json-preview" dangerouslySetInnerHTML={{ __html: highlightJSON(preview) }}></div></div>)}
              </div>
            )}

            <div className="console-header" onClick={() => setConsoleOpen(!consoleOpen)}><h3>Event Console</h3><span style={{fontSize: 12}}>{consoleOpen ? "▼ Hide" : "▲ Show"}</span></div>
            {consoleOpen && <div className="console-body" ref={logRef}>{logs.length ? logs.map((l, i) => <div key={i}>{l}</div>) : "Awaiting events..."}</div>}
          </div>
        </div>
      )}

      {/* RECONSTRUCT TAB */}
      {tab === "reconstruct" && (
        <div className="card" style={{ maxWidth: 900, margin: "0 auto" }}>
          <h2>Reconstruct & Repair Data</h2>
          <p style={{ fontSize: 14, color: "#555", marginBottom: 20 }}>Paste one or more OpenAI Batch IDs (comma separated) to rebuild the CSV. If data is missing, you can spawn a repair batch automatically.</p>
          
          <div className="form-group flex-row">
            <input value={reBatchIds} onChange={(e) => setReBatchIds(e.target.value)} placeholder="batch_XYZ, batch_ABC..." style={{flex: 3}}/>
            <button onClick={handleAnalyze} disabled={isAnalyzing || !reBatchIds.trim()} style={{flex: 1}}>{isAnalyzing ? "Analyzing..." : "Analyze File"}</button>
          </div>
          {error && <div style={{ marginBottom: 16, padding: 12, background: "#fee2e2", color: "#b91c1c", borderRadius: 6, fontSize: 13 }}>{error}</div>}

          {analysis && (
            <div style={{ marginTop: 24, borderTop: "1px solid #eee", paddingTop: 24 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
                <div>
                  <h3 style={{ margin: "0 0 4px 0" }}>Analysis Results</h3>
                  <span style={{ fontSize: 14, color: "#555" }}>Processed {analysis.totalRows} total rows.</span>
                </div>
                {analysis.missingCount === 0 ? (
                  <span className="badge success">100% Complete Data</span>
                ) : (
                  <span className="badge danger">{analysis.missingCount} Missing Rows Detected</span>
                )}
              </div>

              {analysis.missingCount > 0 && (
                <div style={{ background: "#fff5f5", border: "1px solid #fecaca", padding: 16, borderRadius: 8, marginBottom: 20, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <p style={{ margin: 0, fontSize: 14, color: "#991b1b" }}>OpenAI failed to return data for <strong>{analysis.missingCount}</strong> rows. You can automatically resubmit these specific rows as a new batch job.</p>
                  <button className="danger" onClick={handleRepair} disabled={isRepairing}>{isRepairing ? "Creating..." : "Resubmit Missing Rows"}</button>
                </div>
              )}

              <button onClick={() => window.location.href = `${API_BASE}/batch-reconstruct?id=${encodeURIComponent(reBatchIds)}`} style={{ width: "100%", marginBottom: 16 }}>
                Download Reconstructed CSV
              </button>

              <h4 style={{ margin: "24px 0 8px 0", fontSize: 14 }}>Data Preview (First 10 rows)</h4>
              <div className="table-container">
                <table>
                  <thead><tr>{analysis.headers.map(h => <th key={h}>{h}</th>)}</tr></thead>
                  <tbody>
                    {analysis.previewData.map((row, i) => (
                      <tr key={i}>{analysis.headers.map(h => <td key={h}>{row[h]}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
