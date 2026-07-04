import React from "react";

export default function StackPanel({ stack, onRunStack }) {
  return (
    <div className="stack">
      <h3>Active Stack</h3>
      {stack.map((m, i) => <div key={i}>{m.name}</div>)}
      <button onClick={() => onRunStack(stack)}>Run Stack</button>
    </div>
  );
}