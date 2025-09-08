import React, { useState } from 'react';

export default function App() {
  const [file, setFile] = useState(null);
  const [inputCol, setInputCol] = useState('text');
  const [prompt, setPrompt] = useState('Translate the user input into English.');
  const [model, setModel] = useState('gpt-4.1-mini');
  const [chunkSize, setChunkSize] = useState(200); // NEW: rows per request (K)
  const [batchId, setBatchId] = useState('');
  const [status, setStatus] = useState('');
  const [outputReady, setOutputReady] = useState(false);
  const [error, setError] = useState('');

  async function submitBatch(e) {
    e.preventDefault();
    setError('');
    setOutputReady(false);

    if (!file) { setError('Please choose a CSV file.'); return; }

    const formData = new FormData();
    formData.append('file', file);
    formData.append('inputCol', inputCol);
    formData.append('prompt', prompt);
    formData.append('model', model);
    formData.append('chunkSize', String(chunkSize)); // NEW

    const r = await fetch('/api/batch-create', { method: 'POST', body: formData });
    const j = await r.json();
    if (!r.ok) { setError(j.error || 'Failed to create batch'); return; }
    setBatchId(j.batchId);
    setStatus('submitted');
  }

  async function checkStatus() {
    if (!batchId) return;
    const r = await fetch(`/api/batch-status?id=${encodeURIComponent(batchId)}`);
    const j = await r.json();
    if (!r.ok) { setError(j.error || 'Failed to get status'); return; }
    setStatus(j.status);
    setOutputReady(j.status === 'completed');
  }

  function downloadOutput() {
    if (!batchId) return;
    window.location.href = `/api/batch-download?id=${encodeURIComponent(batchId)}`;
  }

  return (
    <div className="container" style={{ maxWidth: 820, margin: '40px auto', fontFamily: 'system-ui, sans-serif' }}>
      <h1>OpenAI Batch CSV</h1>
      <p>Upload a CSV, set the input column, write your instruction, pick a model, set <em>rows per request (K)</em>, submit a Batch, then download the merged CSV.</p>

      <form onSubmit={submitBatch} style={{ display: 'grid', gap: 12 }}>
        <label>CSV File
          <input type="file" accept=".csv" onChange={(e) => setFile(e.target.files?.[0] || null)} />
        </label>

        <label>Input column name
          <input value={inputCol} onChange={(e) => setInputCol(e.target.value)} placeholder="text" />
        </label>

        <label>Instruction prompt
          <textarea rows={6} value={prompt} onChange={(e) => setPrompt(e.target.value)} placeholder="Describe what to do with each row..." />
        </label>

        <label>Model
          <select value={model} onChange={(e) => setModel(e.target.value)}>
            <option>gpt-4.1-mini</option>
            <option>gpt-4o-mini</option>
            <option>gpt-4.1</option>
          </select>
        </label>

        <label>Rows per request (K)
          <input
            type="number"
            min={1}
            max={1000}
            value={chunkSize}
            onChange={(e) => setChunkSize(Number(e.target.value || 1))}
          />
        </label>

        <button type="submit">Create Batch</button>
      </form>

      {batchId && (
        <div style={{ marginTop: 24, padding: 12, border: '1px solid #ddd', borderRadius: 8 }}>
          <div><strong>Batch ID:</strong> {batchId}</div>
          <div><strong>Status:</strong> {status || '(unknown)'} </div>
          <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
            <button onClick={checkStatus}>Refresh Status</button>
            <button onClick={downloadOutput} disabled={!outputReady}>Download merged CSV</button>
          </div>
          {error && <p style={{ color: 'crimson' }}>{error}</p>}
        </div>
      )}
    </div>
  );
}
