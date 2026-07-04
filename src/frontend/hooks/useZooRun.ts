/**
 * useZooRun - Zoo execution hook
 * Wire: Run button → /zoo/run, /zoo/stream
 * 
 * Provides both streaming and non-streaming execution modes.
 * Handles execution state, streaming tokens, and cancellation.
 */
import { useState, useCallback, useRef, useEffect } from 'react';

export interface RunRequest {
  model_id: string;
  input_data: string;
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
}

export interface RunResponse {
  execution_id: string;
  model_id: string;
  status: string;
  output: string;
  latency_ms: number;
  tokens_generated: number;
}

export interface StreamChunk {
  token: string;
  tokens_generated: number;
  is_final: boolean;
}

export interface UseZooRunOptions {
  baseUrl?: string;
  onChunk?: (chunk: StreamChunk) => void;
  onComplete?: (response: RunResponse) => void;
  onError?: (error: Error) => void;
}

export interface UseZooRunReturn {
  // State
  isRunning: boolean;
  isStreaming: boolean;
  output: string;
  tokensGenerated: number;
  latencyMs: number;
  error: string | null;
  executionId: string | null;
  
  // Actions
  run: (request: RunRequest) => Promise<RunResponse>;
  runStreaming: (request: RunRequest) => Promise<void>;
  cancel: () => Promise<void>;
  reset: () => void;
}

export function useZooRun(options: UseZooRunOptions = {}): UseZooRunReturn {
  const {
    baseUrl = '/api',
    onChunk,
    onComplete,
    onError,
  } = options;

  // State
  const [isRunning, setIsRunning] = useState(false);
  const [isStreaming, setIsStreaming] = useState(false);
  const [output, setOutput] = useState('');
  const [tokensGenerated, setTokensGenerated] = useState(0);
  const [latencyMs, setLatencyMs] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [executionId, setExecutionId] = useState<string | null>(null);

  // Refs
  const abortControllerRef = useRef<AbortController | null>(null);
  const outputBufferRef = useRef<string[]>([]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, []);

  // Reset state
  const reset = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    setIsRunning(false);
    setIsStreaming(false);
    setOutput('');
    setTokensGenerated(0);
    setLatencyMs(0);
    setError(null);
    setExecutionId(null);
    outputBufferRef.current = [];
  }, []);

  // Cancel current execution
  const cancel = useCallback(async () => {
    if (abortControllerRef.current && executionId) {
      abortControllerRef.current.abort();
      
      try {
        await fetch(`${baseUrl}/zoo/cancel/${executionId}`, {
          method: 'POST',
        });
      } catch {
        // Ignore cancel API errors
      }
    }
    
    setIsRunning(false);
    setIsStreaming(false);
  }, [baseUrl, executionId]);

  // Non-streaming execution
  const run = useCallback(async (request: RunRequest): Promise<RunResponse> => {
    reset();
    setIsRunning(true);
    setError(null);

    abortControllerRef.current = new AbortController();

    try {
      const response = await fetch(`${baseUrl}/zoo/run`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify({
          model_id: request.model_id,
          input_data: request.input_data,
          stream: false,
          temperature: request.temperature ?? 0.7,
          max_tokens: request.max_tokens ?? 2048,
        }),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        throw new Error(`Execution failed: ${response.status} ${response.statusText}`);
      }

      const data: RunResponse = await response.json();
      
      setOutput(data.output);
      setTokensGenerated(data.tokens_generated);
      setLatencyMs(data.latency_ms);
      setExecutionId(data.execution_id);
      setIsRunning(false);

      if (onComplete) {
        onComplete(data);
      }

      return data;
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        setError('Execution cancelled');
      } else {
        const errorMsg = err instanceof Error ? err.message : 'Execution failed';
        setError(errorMsg);
        if (onError) {
          onError(err instanceof Error ? err : new Error(errorMsg));
        }
      }
      setIsRunning(false);
      throw err;
    }
  }, [baseUrl, onComplete, onError, reset]);

  // Streaming execution - creates "alive" feeling
  const runStreaming = useCallback(async (request: RunRequest): Promise<void> => {
    reset();
    setIsRunning(true);
    setIsStreaming(true);
    setError(null);
    outputBufferRef.current = [];

    abortControllerRef.current = new AbortController();

    try {
      const params = new URLSearchParams({
        model_id: request.model_id,
        input_data: request.input_data,
        temperature: String(request.temperature ?? 0.7),
        max_tokens: String(request.max_tokens ?? 2048),
      });

      const response = await fetch(`${baseUrl}/zoo/stream?${params}`, {
        method: 'GET',
        headers: {
          'Accept': 'text/event-stream',
        },
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        throw new Error(`Streaming failed: ${response.status} ${response.statusText}`);
      }

      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error('Response body is not readable');
      }

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        
        // Process complete SSE events
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? ''; // Keep incomplete line in buffer

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            const eventType = line.slice(7).trim();
            continue;
          }
          
          if (line.startsWith('data: ')) {
            const dataStr = line.slice(6).trim();
            if (!dataStr) continue;

            try {
              const data = JSON.parse(dataStr);
              
              if (data.execution_id) {
                setExecutionId(data.execution_id);
              }
              
              if (data.token !== undefined) {
                // Streaming token
                const token = data.token;
                outputBufferRef.current.push(token);
                setOutput(outputBufferRef.current.join(''));
                setTokensGenerated(data.tokens_generated);
                
                if (onChunk) {
                  onChunk({
                    token,
                    tokens_generated: data.tokens_generated,
                    is_final: data.is_final,
                  });
                }
              }
              
              if (data.latency_ms !== undefined) {
                setLatencyMs(data.latency_ms);
              }
              
              if (data.error) {
                throw new Error(data.error);
              }
            } catch {
              // Ignore JSON parse errors for incomplete data
            }
          }
        }
      }

      // Process any remaining buffer
      if (buffer.trim()) {
        try {
          const data = JSON.parse(buffer);
          if (data.token) {
            outputBufferRef.current.push(data.token);
            setOutput(outputBufferRef.current.join(''));
          }
        } catch {
          // Ignore
        }
      }

      // Final response
      const finalResponse: RunResponse = {
        execution_id: executionId ?? '',
        model_id: request.model_id,
        status: 'completed',
        output: outputBufferRef.current.join(''),
        latency_ms: latencyMs,
        tokens_generated: tokensGenerated,
      };

      setIsRunning(false);
      setIsStreaming(false);

      if (onComplete) {
        onComplete(finalResponse);
      }
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        setError('Execution cancelled');
      } else {
        const errorMsg = err instanceof Error ? err.message : 'Streaming failed';
        setError(errorMsg);
        if (onError) {
          onError(err instanceof Error ? err : new Error(errorMsg));
        }
      }
      setIsRunning(false);
      setIsStreaming(false);
    }
  }, [baseUrl, onChunk, onComplete, onError, reset, executionId, latencyMs, tokensGenerated]);

  return {
    isRunning,
    isStreaming,
    output,
    tokensGenerated,
    latencyMs,
    error,
    executionId,
    run,
    runStreaming,
    cancel,
    reset,
  };
}

/**
 * Prefetch model into cache
 */
export async function prewarmModels(modelIds: string[], baseUrl = '/api'): Promise<void> {
  await fetch(`${baseUrl}/zoo/prewarm`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(modelIds),
  });
}

/**
 * Get cache status
 */
export async function getCacheStatus(baseUrl = '/api'): Promise<{
  cached_models: string[];
  total_cached: number;
  max_cached: number;
  active_executions: number;
}> {
  const response = await fetch(`${baseUrl}/health`);
  if (!response.ok) {
    throw new Error('Failed to get cache status');
  }
  const data = await response.json();
  return data.cache_status;
}
