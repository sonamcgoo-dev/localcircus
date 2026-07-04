/**
 * MarketplaceHome - Fully wired marketplace with live backend integration
 * Wire: Marketplace → /registry/search
 * Wire: Run button → /zoo/run (streaming)
 */
import React, { useState, useCallback } from "react";
import ModelCard from "../components/ModelCard";
import { useMarketplace, ModelInfo } from "../hooks/useMarketplace";
import { useZooRun, RunRequest } from "../hooks/useZooRun";
import type { ModelInfo as MarketplaceModelInfo } from "../hooks/useMarketplace";

export default function MarketplaceHome() {
  // Marketplace hook - wired to /registry/search
  const {
    models,
    loading,
    error,
    query,
    search,
    setQuery,
    refresh,
  } = useMarketplace({ baseUrl: '/api', autoSearch: true });

  // Stack state
  const [stack, setStack] = useState<MarketplaceModelInfo[]>([]);

  // Zoo run hook - wired to /zoo/run and /zoo/stream
  const zooRun = useZooRun({
    baseUrl: '/api',
    onComplete: (response) => {
      console.log('Execution complete:', response);
    },
    onError: (error) => {
      console.error('Execution error:', error);
    },
  });

  // Handle model run with streaming
  const handleRun = useCallback(async (model: MarketplaceModelInfo) => {
    const request: RunRequest = {
      model_id: model.id,
      input_data: `Hello ${model.name}, tell me about your capabilities.`,
      stream: true,
    };
    
    try {
      await zooRun.runStreaming(request);
    } catch (err) {
      console.error('Failed to run model:', err);
    }
  }, [zooRun]);

  // Handle add to stack
  const handleAddToStack = useCallback((model: MarketplaceModelInfo) => {
    setStack(prev => {
      if (prev.some(m => m.id === model.id)) {
        return prev; // Already in stack
      }
      return [...prev, model];
    });
  }, []);

  // Run entire stack
  const handleRunStack = useCallback(async () => {
    for (const model of stack) {
      const request: RunRequest = {
        model_id: model.id,
        input_data: `Execute with ${model.name}`,
        stream: true,
      };
      await zooRun.runStreaming(request);
    }
  }, [stack, zooRun]);

  return (
    <div style={{ padding: "24px", maxWidth: "1200px", margin: "0 auto" }}>
      {/* Header */}
      <div style={{ marginBottom: "24px" }}>
        <h1 style={{ fontSize: "32px", fontWeight: 700, margin: "0 0 8px 0" }}>
          🤖 RITUAL Marketplace
        </h1>
        <p style={{ color: "#6b7280", margin: 0 }}>
          Powered by Zoo Runtime • Streaming Execution • Tag-Indexed Registry
        </p>
      </div>

      {/* Search Bar */}
      <div style={{ 
        display: "flex", 
        gap: "12px", 
        marginBottom: "24px" 
      }}>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search models..."
          style={{
            flex: 1,
            padding: "12px 16px",
            border: "1px solid #e5e7eb",
            borderRadius: "8px",
            fontSize: "16px",
          }}
        />
        <button
          onClick={() => search(query)}
          disabled={loading}
          style={{
            padding: "12px 24px",
            backgroundColor: "#3b82f6",
            color: "#fff",
            border: "none",
            borderRadius: "8px",
            cursor: loading ? "not-allowed" : "pointer",
            fontWeight: 500,
          }}
        >
          {loading ? "Searching..." : "Search"}
        </button>
        <button
          onClick={refresh}
          style={{
            padding: "12px 16px",
            backgroundColor: "#fff",
            border: "1px solid #e5e7eb",
            borderRadius: "8px",
            cursor: "pointer",
          }}
        >
          ↻
        </button>
      </div>

      {/* Error Display */}
      {error && (
        <div style={{
          padding: "12px",
          backgroundColor: "#fee",
          border: "1px solid #f99",
          borderRadius: "8px",
          marginBottom: "16px",
          color: "#c00"
        }}>
          {error}
        </div>
      )}

      {/* Streaming Output Panel */}
      {zooRun.output && (
        <div style={{
          marginBottom: "24px",
          padding: "16px",
          backgroundColor: "#1f2937",
          borderRadius: "8px",
          color: "#fff",
          fontFamily: "monospace",
          fontSize: "14px",
          lineHeight: 1.6,
          minHeight: "120px",
        }}>
          <div style={{ 
            display: "flex", 
            justifyContent: "space-between",
            marginBottom: "8px",
            color: "#9ca3af",
            fontSize: "12px"
          }}>
            <span>Streaming Output {zooRun.isStreaming && "◉ Live"}</span>
            <span>{zooRun.tokensGenerated} tokens</span>
          </div>
          <div style={{ whiteSpace: "pre-wrap" }}>
            {zooRun.output}
            {zooRun.isStreaming && <span style={{ animation: "blink 1s infinite" }}>▊</span>}
          </div>
        </div>
      )}

      {/* Models Grid */}
      <div style={{ marginBottom: "24px" }}>
        <h2 style={{ fontSize: "20px", fontWeight: 600, marginBottom: "16px" }}>
          Available Models ({models.length})
        </h2>
        
        {loading && models.length === 0 ? (
          <div style={{ textAlign: "center", padding: "48px", color: "#6b7280" }}>
            Loading models from registry...
          </div>
        ) : (
          models.map(m => (
            <ModelCard
              key={m.id}
              model={m}
              onRun={handleRun}
              onAdd={handleAddToStack}
              isRunning={zooRun.isRunning && zooRun.executionId !== null}
            />
          ))
        )}
      </div>

      {/* Stack Panel */}
      <div style={{
        border: "1px solid #e5e7eb",
        borderRadius: "8px",
        padding: "16px",
        backgroundColor: "#f9fafb"
      }}>
        <div style={{ 
          display: "flex", 
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "12px"
        }}>
          <h3 style={{ margin: 0 }}>📦 Active Stack ({stack.length})</h3>
          <button
            onClick={handleRunStack}
            disabled={stack.length === 0 || zooRun.isRunning}
            style={{
              padding: "8px 16px",
              backgroundColor: stack.length === 0 ? "#d1d5db" : "#8b5cf6",
              color: "#fff",
              border: "none",
              borderRadius: "6px",
              cursor: stack.length === 0 || zooRun.isRunning ? "not-allowed" : "pointer",
              fontWeight: 500,
            }}
          >
            {zooRun.isRunning ? "Running Stack..." : "▶ Run Stack"}
          </button>
        </div>
        
        {stack.length === 0 ? (
          <p style={{ color: "#9ca3af", fontSize: "14px", margin: 0 }}>
            Click "Add to Stack" on models above to build your execution pipeline
          </p>
        ) : (
          <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
            {stack.map((m, i) => (
              <div
                key={m.id}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "8px",
                  padding: "8px 12px",
                  backgroundColor: "#fff",
                  border: "1px solid #e5e7eb",
                  borderRadius: "6px",
                  fontSize: "14px"
                }}
              >
                <span style={{ color: "#8b5cf6", fontWeight: 500 }}>#{i + 1}</span>
                <span>{m.name}</span>
                <button
                  onClick={() => setStack(prev => prev.filter(x => x.id !== m.id))}
                  style={{
                    background: "none",
                    border: "none",
                    color: "#9ca3af",
                    cursor: "pointer",
                    fontSize: "16px",
                    padding: "0 4px"
                  }}
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Style for cursor blink */}
      <style>{`
        @keyframes blink {
          0%, 50% { opacity: 1; }
          51%, 100% { opacity: 0; }
        }
      `}</style>
    </div>
  );
}