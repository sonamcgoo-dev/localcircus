/**
 * ModelCard - Updated with live runtime scores and Zoo execution
 * Connected: Marketplace feed → /registry/search, Run → /zoo/run
 */
import React from "react";
import type { ModelInfo } from "../hooks/useMarketplace";

interface ModelCardProps {
  model: ModelInfo;
  onRun: (model: ModelInfo) => void;
  onAdd: (model: ModelInfo) => void;
  isRunning?: boolean;
}

export default function ModelCard({ model, onRun, onAdd, isRunning = false }: ModelCardProps) {
  const metrics = model.runtime_metrics;
  
  // Color coding for usage velocity
  const getVelocityColor = (velocity: number): string => {
    if (velocity >= 0.7) return "#10b981"; // green - hot
    if (velocity >= 0.4) return "#f59e0b"; // amber - warm
    return "#6b7280"; // gray - cool
  };

  const getVelocityLabel = (velocity: number): string => {
    if (velocity >= 0.7) return "🔥 Hot";
    if (velocity >= 0.4) return "⚡ Active";
    if (velocity > 0) return "○ New";
    return "○ Unused";
  };

  return (
    <div className="card" style={{ 
      border: "1px solid #e5e7eb", 
      borderRadius: "8px", 
      padding: "16px",
      marginBottom: "12px",
      backgroundColor: "#fff",
      transition: "box-shadow 0.2s"
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div style={{ flex: 1 }}>
          <h3 style={{ margin: "0 0 8px 0", fontSize: "18px", fontWeight: 600 }}>
            {model.name}
          </h3>
          <p style={{ margin: "0 0 12px 0", color: "#6b7280", fontSize: "14px", lineHeight: 1.5 }}>
            {model.description}
          </p>
          
          {/* Tags */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: "6px", marginBottom: "12px" }}>
            {model.tags.map(tag => (
              <span 
                key={tag}
                style={{
                  backgroundColor: "#f3f4f6",
                  color: "#4b5563",
                  padding: "2px 8px",
                  borderRadius: "4px",
                  fontSize: "12px"
                }}
              >
                {tag}
              </span>
            ))}
          </div>
          
          {/* Runtime Metrics */}
          <div style={{ 
            display: "flex", 
            gap: "16px", 
            fontSize: "12px", 
            color: "#6b7280",
            marginBottom: "12px"
          }}>
            <span title="Total executions">
              📊 {metrics.total_runs} runs
            </span>
            <span title="Average latency">
              ⏱️ {metrics.avg_latency_ms.toFixed(0)}ms avg
            </span>
            <span title="Success rate">
              ✓ {metrics.success_rate > 0 ? `${(metrics.success_rate * 100).toFixed(0)}%` : "N/A"}
            </span>
            <span 
              title="Usage velocity (popularity score)"
              style={{ color: getVelocityColor(metrics.usage_velocity) }}
            >
              {getVelocityLabel(metrics.usage_velocity)}
            </span>
          </div>
          
          {/* Model specs */}
          <div style={{ fontSize: "11px", color: "#9ca3af" }}>
            {model.params_b}B params • {model.context_window.toLocaleString()} context
          </div>
        </div>
        
        {/* Actions */}
        <div style={{ display: "flex", flexDirection: "column", gap: "8px", marginLeft: "16px" }}>
          <button
            onClick={() => onRun(model)}
            disabled={isRunning}
            style={{
              backgroundColor: isRunning ? "#d1d5db" : "#10b981",
              color: "#fff",
              border: "none",
              borderRadius: "6px",
              padding: "8px 16px",
              cursor: isRunning ? "not-allowed" : "pointer",
              fontWeight: 500,
              fontSize: "14px",
              transition: "background-color 0.2s"
            }}
          >
            {isRunning ? "⏳ Running..." : "▶ Run"}
          </button>
          <button
            onClick={() => onAdd(model)}
            style={{
              backgroundColor: "#fff",
              color: "#4b5563",
              border: "1px solid #d1d5db",
              borderRadius: "6px",
              padding: "8px 16px",
              cursor: "pointer",
              fontWeight: 500,
              fontSize: "14px"
            }}
          >
            + Stack
          </button>
        </div>
      </div>
    </div>
  );
}