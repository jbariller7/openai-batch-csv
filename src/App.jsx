import React, { useEffect, useRef, useState } from "react";

const API_BASE = "/.netlify/functions";

function generateId() { return Math.random().toString(36).substring(2, 9); }

function highlightJSON(json) {
  if (typeof json !== "string") json = JSON.stringify(json, undefined, 2);
  json = json.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, (match) => {
    let cls = "json-number";
    if (/^"/.test(match)) { cls = /:$/.test(match) ? "json-key" : "json-string"; } 
    else if (/true|false/.test(match)) cls = "json-boolean";
    else if (/null/.test(match)) cls = "json-null";
    return `<span class="${cls}">${match}</span>`;
  });
}

function ProjectWorkspace({ project, updateProject, isActive }) {
  const { 
    id, name, inputCol, skipCol, targetCols, prompt, contextDoc, model, chunkSize, reasoningEffort, mode, 
    concurrency, maxRows, batchIds, status, jobStats, analysis, lastRunMode 
  } = project;

  const [file, setFile] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [importId, setImportId] = useState("");
  const [batchIdInput, setBatchIdInput] = useState(batchIds.join(",\n")); // Use newlines for bigger box
  
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isRepairing, setIsRepairing] = useState(false);
  const [preview, setPreview] = useState(null);
  const [error, setError] = useState("");
  
  const [logs, setLogs] = useState([]);
  const [consoleOpen, setConsoleOpen] = useState(true);
  const logRef = useRef(null);

  const currentBatchId = batchIds.length > 0 ? batchIds[batchIds.length - 1] : null;
  const isJobActive = batchIds.length > 0;

  function log(msg) { setLogs((prev) => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`]); }
  useEffect(() => { if (logRef.current && consoleOpen) logRef.current.scrollTop = logRef.current.scrollHeight; }, [logs, consoleOpen]);

  useEffect(() => { setBatchIdInput(batchIds.join(",\n")); }, [batchIds]);

  const pollRef = useRef(null);
  function stopPolling() { if (pollRef.current) clearInterval(pollRef.current); pollRef.current = null; }
  function startPolling() { 
    stopPolling(); 
    pollRef.current = setInterval(lastRunMode === "direct" ? checkDirectStatus : checkStatus, 3000); 
  }
  
  useEffect(() => { 
    const terminal = ["completed", "failed", "cancelled", "expired", "ready"]; 
    if (status && terminal.includes(status)) stopPolling(); 
    else if (currentBatchId && !terminal.includes(status)) startPolling();
    return () => stopPolling();
  }, [status, currentBatchId]);

  const estTokens = Math.round((prompt.length + contextDoc.length) / 4);
  const isCachedHit = estTokens >= 1024;
  const barWidth = Math.min((estTokens / 1024) * 100, 100);

  const update = (fields) => updateProject(id, fields);

  async function handleImport(e) {
    e.preventDefault();
    if(!importId) return;
    update({ batchIds: [importId], status: "submitted", lastRunMode: "batch", name: `Imported (${importId.slice(6,12)})` });
    log(`Imported batch ${importId}. Starting tracking.`);
  }

  async function submitBatch(e) {
    e.preventDefault(); setError(""); setPreview(null); update({ status: "", analysis: null });

    if (!file) return setError("Please choose a CSV file.");
    try {
      setIsSubmitting(true); stopPolling();
      const fd = new FormData();
      fd.append("file", file); fd.append("inputCol", inputCol); fd.append("prompt", prompt);
      fd.append("model", model); fd.append("chunkSize", chunkSize); fd.append("reasoning_effort", reasoningEffort);
      if (skipCol) fd.append("skipCol", skipCol);
      if (targetCols) fd.append("targetCols", targetCols);
      if (contextDoc) fd.append("contextDoc", contextDoc);
      if (maxRows) fd.append("maxRows", maxRows);
      if (mode === "dry") fd.append("dryRun", "1");
      if (mode === "direct") { fd.append("direct", "1"); fd.append("concurrency", concurrency); }

      log(`Submitting Job → mode=${mode}, model=${model}`);
      const r = await fetch(`${API_BASE}/batch-create`, { method: "POST", body: fd });
      const bodyText = await r.text();
      if (!r.ok) throw new Error(JSON.parse(bodyText).error || `HTTP ${r.status}`);
      const j = JSON.parse(bodyText);

      if (j.mode === "dryRun") { 
        update({ status: "done" }); setPreview(j.parsed || j.results || j); log(`Dry run OK.`); return; 
      }
      
      const newId = j.mode === "direct" ? j.jobId : j.batchId;
      update({ 
        batchIds: [newId], 
        status: "running", 
        lastRunMode: j.mode, 
        name: file.name.length > 20 ? file.name.substring(0, 20) + "..." : file.name 
      });
      log(`Job queued successfully: ${newId}`);
    } catch (err) { setError(err.message); log(`Error: ${err.message}`); } finally { setIsSubmitting(false); }
  }

  async function checkStatus() {
    if (!currentBatchId) return;
    try {
      const r = await fetch(`${API_BASE}/batch-status?id=${encodeURIComponent(currentBatchId)}`);
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Status failed");
      
      const updates = { status: j.status };
      if (j.status === "failed" && j.errors?.data?.length > 0) { 
        log(`❌ Batch Failed: ${j.errors.data[0].message}`); 
        setError(`OpenAI rejected the batch: ${j.errors.data[0].message}`); 
        stopPolling();
      }
      if (j.request_counts) updates.jobStats = { completed: j.request_counts.completed + j.request_counts.failed, total: j.request_counts.total };
      if (j.status === "completed") log(j.request_counts?.failed > 0 ? `⚠️ Job completed with ${j.request_counts.failed} failed rows. Run Analysis to repair.` : "✅ Batch completed successfully.");
      
      update(updates);
    } catch (err) { setError(err.message); }
  }

  async function checkDirectStatus() {
    if (!currentBatchId) return;
    try {
      const r = await fetch(`${API_BASE}/direct-status?id=${encodeURIComponent(currentBatchId)}`);
      const j = await r.json();
      if (j.error) { update({ status: "failed" }); setError(j.error); stopPolling(); return; }
      
      const newStatus = j.ready ? "ready" : (j.status || "running");
      update({ status: newStatus, jobStats: { completed: j.completedChunks || 0, total: j.totalChunks || 0 } });
      if (j.ready) { log("✅ Job finished. CSV ready."); stopPolling(); }
    } catch (err) { log(`Status error: ${err.message}`); }
  }

  async function cancelDirectJob() {
    if (!currentBatchId) return;
    try { log("Sending abort request..."); await fetch(`${API_BASE}/direct-cancel?id=${encodeURIComponent(currentBatchId)}`); update({ status: "cancelled" }); log("Job aborted."); } catch(e) {}
  }

  async function handleAnalyze() {
    if (!batchIds.length) return;
    setIsAnalyzing(true); update({ analysis: null }); setError("");
    try {
      log(`Analyzing outputs...`);
      const r = await fetch(`${API_BASE}/batch-reconstruct?id=${encodeURIComponent(batchIds.join(','))}&analyze=1`);
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Analysis failed.");
      update({ analysis: j });
      log(`Analysis complete. Found ${j.missingCount} missing rows/cells.`);
    } catch (e) { setError(e.message); } finally { setIsAnalyzing(false); }
  }

  async function handleRepair() {
    if (!analysis?.missingIds || analysis.missingIds.length === 0) return;
    setIsRepairing(true); setError("");
    try {
      log(`Submitting repair batch for ${analysis.missingIds.length} rows...`);
      const r = await fetch(`${API_BASE}/batch-repair`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ batchIds: batchIds.join(','), missingIds: analysis.missingIds })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Repair failed.");
      
      update({ batchIds: [...batchIds, j.newBatchId], status: "submitted", analysis: null, lastRunMode: "batch" });
      log(`✅ Repair batch submitted! ID: ${j.newBatchId}`);
    } catch(e) { setError(e.message); log(`❌ Repair Error: ${e.message}`); } finally { setIsRepairing(false); }
  }

  const downloadLink = lastRunMode === "direct"
      ? `${API_BASE}/batch-download?id=${encodeURIComponent(currentBatchId)}`
      : `${API_BASE}/batch-reconstruct?id=${encodeURIComponent(batchIds.join(','))}`;

  return (
    <div style={{ display: isActive ? 'block' : 'none' }}>
      
      <input 
        className="project-name-input" 
        value={name} 
        onChange={(e) => update({ name: e.target.value })} 
        placeholder="Project Name..."
      />

      <div className="app-grid">
        {/* LEFT COLUMN: CONFIGURATION */}
        <div>
          {isJobActive ? (
            <details className="config-fold card" style={{padding: 0, overflow: "hidden"}}>
              <summary>View / Edit Job Configuration</summary>
              <div style={{padding: "0 24px 24px 24px"}}>
                <p style={{fontSize: 13, color: "#666"}}><em>Note: Changing these settings does not affect the currently running batch.</em></p>
                <ConfigForm file={file} setFile={setFile} isDragging={isDragging} setIsDragging={setIsDragging} update={update} inputCol={inputCol} skipCol={skipCol} targetCols={targetCols} prompt={prompt} contextDoc={contextDoc} model={model} chunkSize={chunkSize} mode={mode} maxRows={maxRows} concurrency={concurrency} isCachedHit={isCachedHit} estTokens={estTokens} barWidth={barWidth} submitBatch={submitBatch} isSubmitting={isSubmitting} />
              </div>
            </details>
          ) : (
            <div className="card">
              <h2>Setup Data Extraction</h2>
              <form onSubmit={handleImport} className="flex-row form-group" style={{background: "#f8fafc", padding: 12, borderRadius: 8, border: "1px dashed #cbd5e1"}}>
                <input value={importId} onChange={e=>setImportId(e.target.value)} placeholder="Or import existing Batch ID..." />
                <button type="submit" className="secondary">Track</button>
              </form>
              <ConfigForm file={file} setFile={setFile} isDragging={isDragging} setIsDragging={setIsDragging} update={update} inputCol={inputCol} skipCol={skipCol} targetCols={targetCols} prompt={prompt} contextDoc={contextDoc} model={model} chunkSize={chunkSize} mode={mode} maxRows={maxRows} concurrency={concurrency} isCachedHit={isCachedHit} estTokens={estTokens} barWidth={barWidth} submitBatch={submitBatch} isSubmitting={isSubmitting} />
            </div>
          )}

          {/* ANALYSIS & REPAIR CARD */}
          {isJobActive && (
            <div className="card" style={{ marginTop: "24px" }}>
              <h2>Data Analysis & Repair</h2>
              <p style={{ fontSize: 13, color: "#555" }}>Check for dropped rows/cells across all tracked batches.</p>
              
              <button onClick={handleAnalyze} disabled={isAnalyzing || status === "running" || status === "submitted"} style={{ width: "100%", marginBottom: 16 }}>
                {isAnalyzing ? "Analyzing Data..." : "Analyze Extracted Data"}
              </button>

              {analysis && (
                <div style={{ borderTop: "1px solid #eee", paddingTop: 16 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
                    <div><h3 style={{ margin: "0 0 4px 0", fontSize: 16 }}>Results</h3><span style={{ fontSize: 13, color: "#555" }}>Processed {analysis.totalRows} rows.</span></div>
                    {analysis.missingCount === 0 ? <span className="badge success">100% Complete</span> : <span className="badge danger">{analysis.missingCount} Missing Rows/Cells</span>}
                  </div>

                  {analysis.missingCount > 0 && (
                    <div style={{ background: "#fff5f5", border: "1px solid #fecaca", padding: 16, borderRadius: 8, marginBottom: 16 }}>
                      <p style={{ margin: "0 0 12px 0", fontSize: 13, color: "#991b1b" }}>OpenAI dropped data for <strong>{analysis.missingCount}</strong> rows. Resubmit these specific rows as a new repair batch.</p>
                      <button className="danger" onClick={handleRepair} disabled={isRepairing} style={{width: "100%"}}>{isRepairing ? "Creating..." : "Launch Auto-Repair Batch"}</button>
                    </div>
                  )}

                  {analysis.previewData && analysis.previewData.length > 0 && (
                    <>
                      <h4 style={{ margin: "16px 0 8px 0", fontSize: 13 }}>Preview</h4>
                      <div className="table-container">
                        <table>
                          <thead><tr>{analysis.headers.map(h => <th key={h}>{h}</th>)}</tr></thead>
                          <tbody>{analysis.previewData.map((row, i) => <tr key={i}>{analysis.headers.map(h => <td key={h}>{row[h]}</td>)}</tr>)}</tbody>
                        </table>
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>
          )}
        </div>

        {/* RIGHT COLUMN: STATUS & CONSOLE */}
        <div>
          {(isJobActive || preview || error) && (
            <div className="card" style={{ marginBottom: "24px" }}>
              <h2>Job Status</h2>
              {isJobActive && (
                <div style={{ marginBottom: 16 }}>
                  
                  <div className="form-group" style={{background: "#f8fafc", padding: "12px", borderRadius: "8px", border: "1px solid #e2e8f0"}}>
                    <label style={{fontSize: 12, color: "#475569"}}>Tracked Batch IDs (Output will be merged)</label>
                    <textarea 
                      rows={4}
                      value={batchIdInput}
                      onChange={(e) => setBatchIdInput(e.target.value)}
                      onBlur={() => update({ batchIds: batchIdInput.split(/[,\n]+/).map(s=>s.trim()).filter(Boolean) })}
                      style={{fontFamily: "monospace", fontSize: 12, padding: "8px", marginTop: "4px"}}
                    />
                  </div>

                  <p style={{ margin: "0 0 8px 0" }}><strong>Status ({currentBatchId?.slice(6,14)}...):</strong> <span style={{ textTransform: "capitalize", color: status === "failed" ? "red" : status === "ready" || status === "completed" ? "green" : "#0066ff", fontWeight: 600 }}>{status || "running"}</span></p>
                  
                  {(status === "running" || status === "submitted") && (
                    <div style={{ marginTop: 16, marginBottom: 16 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 4, fontWeight: 600 }}>
                        <span>Processing Chunks</span><span>{jobStats.completed} / {jobStats.total || "?"}</span>
                      </div>
                      <progress value={jobStats.completed} max={jobStats.total || 100} />
                    </div>
                  )}
                </div>
              )}
              
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                {isJobActive && lastRunMode === "batch" && <button className="secondary" onClick={checkStatus}>Refresh</button>}
                {isJobActive && lastRunMode === "direct" && status === "running" && <button className="danger" onClick={cancelDirectJob}>Abort</button>}
                {isJobActive && <button onClick={() => window.location.href = downloadLink} disabled={status !== "completed" && status !== "ready"}>Download Output CSV</button>}
              </div>
              
              {error && <div style={{ marginTop: 16, padding: 12, background: "#fee2e2", color: "#b91c1c", borderRadius: 6, fontSize: 13 }}><strong>Error:</strong> {error}</div>}
              {!isJobActive && preview && (<div style={{ marginTop: 16 }}><h3 style={{ fontSize: 14 }}>Dry Run Preview:</h3><div className="json-preview" dangerouslySetInnerHTML={{ __html: highlightJSON(preview) }}></div></div>)}
            </div>
          )}

          <div className="console-header" onClick={() => setConsoleOpen(!consoleOpen)}><h3>Event Console</h3><span style={{fontSize: 12}}>{consoleOpen ? "▼ Hide" : "▲ Show"}</span></div>
          {consoleOpen && <div className="console-body" ref={logRef}>{logs.length ? logs.map((l, i) => <div key={i}>{l}</div>) : "Awaiting events..."}</div>}
        </div>
      </div>
    </div>
  );
}

function ConfigForm({ file, setFile, isDragging, setIsDragging, update, inputCol, skipCol, targetCols, prompt, contextDoc, model, chunkSize, mode, maxRows, concurrency, isCachedHit, estTokens, barWidth, submitBatch, isSubmitting }) {
  const fileInputRef = useRef(null);

  return (
    <form onSubmit={submitBatch}>
      <div className={`dropzone ${isDragging ? "active" : ""}`} onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }} onDragLeave={() => setIsDragging(false)} onDrop={(e) => { e.preventDefault(); setIsDragging(false); setFile(e.dataTransfer.files[0]); }} onClick={() => fileInputRef.current.click()}>
        <input ref={fileInputRef} type="file" accept=".csv" style={{ display: "none" }} onChange={(e) => setFile(e.target.files[0])} />
        {file ? <p className="file-name">{file.name} ({(file.size/1024).toFixed(1)} KB)</p> : <p>Drag & Drop a CSV file here</p>}
      </div>

      <div className="flex-row form-group">
        <div><label>Input Column</label><input value={inputCol} onChange={(e) => update({inputCol: e.target.value})} placeholder="e.g. text" required /></div>
        <div><label>Skip Column (Optional) <span className="hint">Skip row if this column has data</span></label><input value={skipCol} onChange={(e) => update({skipCol: e.target.value})} placeholder="e.g. result" /></div>
      </div>

      <div className="form-group">
        <label>Multi-Column Targets (Optional) <span className="hint">Comma separated list (e.g. French, Spanish, German). Processes 1 cell at a time.</span></label>
        <input value={targetCols || ""} onChange={(e) => update({targetCols: e.target.value})} placeholder="e.g. French, Spanish" />
      </div>

      <div className="form-group"><label>Reference Context (Optional) <span className="hint">Paste large data here to trigger 50% Prompt Caching discount.</span></label><textarea rows={3} value={contextDoc} onChange={(e) => update({contextDoc: e.target.value})} /></div>
      
      <div className="form-group">
        <label>Instruction Prompt <span className="hint">Use <strong>{"${columnName}"}</strong> as a variable if using Multi-Column Targets.</span></label>
        <textarea rows={4} value={prompt} onChange={(e) => update({prompt: e.target.value})} placeholder="Translate the text to ${columnName}" required />
      </div>

      <div className="form-group" style={{ background: "#f8fafc", padding: "12px", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
        <div style={{display: "flex", justifyContent: "space-between"}}><label style={{margin:0}}>System Prompt Size</label>{isCachedHit && <span className="cached-badge">✓ Caching Active</span>}</div>
        <div className="token-bar-bg"><div className={`token-bar-fill ${isCachedHit ? "cached" : ""}`} style={{ width: `${barWidth}%` }}></div></div>
        <div className="token-status"><span style={{color: "#64748b"}}>~{estTokens} tokens</span><span style={{color: "#64748b"}}>Limit: 1,024</span></div>
      </div>

      <div className="flex-row form-group">
        <div><label>Model</label><select value={model} onChange={(e) => update({model: e.target.value})}>
            <optgroup label="Current Generation (5.4)"><option value="gpt-5.4-nano">gpt-5.4-nano</option><option value="gpt-5.4-mini">gpt-5.4-mini</option><option value="gpt-5.4">gpt-5.4</option></optgroup>
            <optgroup label="Mid-Generation (5.3)"><option value="gpt-5.3-chat-latest">gpt-5.3 Instant</option></optgroup>
            <optgroup label="Previous Generation"><option value="gpt-5-nano">gpt-5-nano</option><option value="gpt-5-mini">gpt-5-mini</option></optgroup>
          </select></div>
        <div><label>Mode</label><select value={mode} onChange={(e) => update({mode: e.target.value})}><option value="batch">Batch API (Cheapest)</option><option value="direct">Direct (Instant)</option><option value="dry">Dry Run</option></select></div>
      </div>

      <div className="flex-row form-group">
        <div><label>Rows/Request (K)</label><input type="number" min={1} max={1000} value={chunkSize} onChange={(e) => update({chunkSize: Number(e.target.value || 1)})} /></div>
        <div><label>Max Test Rows</label><input type="number" min={0} value={maxRows} onChange={(e) => update({maxRows: e.target.value})} /></div>
        {mode === "direct" && <div><label>Concurrency</label><input type="number" min={1} value={concurrency} onChange={(e) => update({concurrency: Number(e.target.value || 1)})} /></div>}
      </div>

      <button type="submit" disabled={isSubmitting} style={{ width: "100%", padding: "12px" }}>{isSubmitting ? "Processing..." : "Launch Data Job"}</button>
    </form>
  );
}

export default function App() {
  const [projects, setProjects] = useState(() => {
    const saved = localStorage.getItem("batch-csv-projects");
    if (saved) { try { return JSON.parse(saved); } catch (e) {} }
    return [{ id: generateId(), name: "New Project", inputCol: "text", skipCol: "", targetCols: "", prompt: "Translate the user input into English.", contextDoc: "", model: "gpt-5.4-nano", chunkSize: 500, reasoningEffort: "medium", mode: "batch", concurrency: 4, maxRows: "", batchIds: [], status: "", jobStats: { completed: 0, total: 0 }, analysis: null, lastRunMode: "batch" }];
  });
  const [activeId, setActiveId] = useState(projects[0]?.id);

  useEffect(() => { localStorage.setItem("batch-csv-projects", JSON.stringify(projects)); }, [projects]);

  const addProject = () => {
    const p = { id: generateId(), name: "New Project", inputCol: "text", skipCol: "", targetCols: "", prompt: "Translate...", contextDoc: "", model: "gpt-5.4-nano", chunkSize: 500, reasoningEffort: "medium", mode: "batch", concurrency: 4, maxRows: "", batchIds: [], status: "", jobStats: { completed: 0, total: 0 }, analysis: null, lastRunMode: "batch" };
    setProjects([...projects, p]);
    setActiveId(p.id);
  };

  const deleteProject = (id) => {
    if(!window.confirm("Are you sure you want to close this project tab?")) return;
    const newProjs = projects.filter(p => p.id !== id);
    setProjects(newProjs);
    if (activeId === id) setActiveId(newProjs[0]?.id || null);
  };

  const updateProject = (id, partialState) => {
    setProjects(prev => prev.map(p => p.id === id ? { ...p, ...partialState } : p));
  };

  return (
    <div className="container">
      
      <div className="tabs-container">
        {projects.map(p => (
          <div key={p.id} className={`project-tab ${activeId === p.id ? 'active' : ''}`} onClick={() => setActiveId(p.id)}>
            {p.name}
            {projects.length > 1 && <button className="close-btn" onClick={(e) => { e.stopPropagation(); deleteProject(p.id); }}>✕</button>}
          </div>
        ))}
        <button className="new-project-btn" onClick={addProject} style={{marginLeft: 12}}>+ New Project</button>
      </div>

      {projects.map(p => (
        <ProjectWorkspace key={p.id} project={p} updateProject={updateProject} isActive={activeId === p.id} />
      ))}
      
      {projects.length === 0 && <div style={{textAlign:"center", padding: 40}}><button onClick={addProject}>Create Project</button></div>}
    </div>
  );
}
