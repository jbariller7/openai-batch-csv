import { useState } from "react";

function App() {
  const [file, setFile] = useState(null);
  const [inputCol, setInputCol] = useState("text");
  const [prompt, setPrompt] = useState("Translate to English.");
  const [model, setModel] = useState("gpt-4.1-mini");
  const [status, setStatus] = useState("");
  const [batchId, setBatchId] = useState("");
  const [error, setError] = useState("");
  const [outputReady, setOutputReady] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    if (!file) {
      setError("Please select a CSV file.");
      return;
    }
    const formData = new FormData();
    formData.append("file", file);
    formData.append("inputCol", inputCol);
    formData.append("prompt", prompt);
    formData.append("model", model);
    try {
      const res = await fetch("/api/batch-create", { method: "POST", body: formData });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Error starting batch.");
        return;
      }
      setBatchId(data.batchId);
      setStatus("submitted");
    } catch (err) {
      setError(err.message);
    }
  };

  const checkStatus = async () => {
    if (!batchId) return;
    try {
      const res = await fetch(`/api/batch-status?id=${batchId}`);
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Error checking status.");
        return;
      }
      setStatus(data.status);
      if (data.status === "completed") {
        setOutputReady(true);
      }
    } catch (err) {
      setError(err.message);
    }
  };

  const download = () => {
    window.location.href = `/api/batch-download?id=${batchId}`;
  };

  return (
    <div style={{ maxWidth: "600px", margin: "0 auto", padding: "1rem" }}>
      <h1>OpenAI Batch CSV</h1>
      <form onSubmit={handleSubmit} style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
        <input type="file" accept=".csv" onChange={(e) => setFile(e.target.files[0])} />
        <label>
          Input column name:
          <input
            type="text"
            value={inputCol}
            onChange={(e) => setInputCol(e.target.value)}
            placeholder="Column to process"
            style={{ width: "100%" }}
          />
        </label>
        <label>
          Prompt:
          <textarea
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            style={{ width: "100%", height: "120px" }}
          />
        </label>
        <label>
          Model:
          <input
            type="text"
            value={model}
            onChange={(e) => setModel(e.target.value)}
            placeholder="gpt-4.1-mini"
            style={{ width: "100%" }}
          />
        </label>
        <button type="submit">Submit Batch</button>
      </form>
      {status && (
        <p>
          <strong>Status:</strong> {status}
        </p>
      )}
      {batchId && <button onClick={checkStatus}>Refresh Status</button>}
      {outputReady && <button onClick={download}>Download Result</button>}
      {error && <p style={{ color: "red" }}>{error}</p>}
    </div>
  );
}

export default App;
