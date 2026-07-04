/**
 * useMarketplace - Live query hook for marketplace search
 * Wire: Marketplace → /registry/search
 * 
 * Provides real-time search with tag filtering and runtime scores.
 */
import { useState, useCallback, useEffect, useRef } from 'react';

export interface RuntimeMetrics {
  total_runs: number;
  successful_runs: number;
  avg_latency_ms: number;
  total_tokens: number;
  last_used: number;
  usage_velocity: number;
  success_rate: number;
}

export interface ModelInfo {
  id: string;
  name: string;
  description: string;
  tags: string[];
  capabilities: string[];
  context_window: number;
  params_b: number;
  registry_url: string;
  checksum: string;
  runtime_metrics: RuntimeMetrics;
}

export interface SearchResult {
  models: ModelInfo[];
  total: number;
  query: string;
}

export interface UseMarketplaceOptions {
  baseUrl?: string;
  autoSearch?: boolean;
  debounceMs?: number;
}

export interface UseMarketplaceReturn {
  // State
  models: ModelInfo[];
  loading: boolean;
  error: string | null;
  query: string;
  selectedTags: string[];
  total: number;
  
  // Actions
  search: (q: string, tags?: string[]) => Promise<void>;
  setQuery: (q: string) => void;
  setSelectedTags: (tags: string[]) => void;
  refresh: () => Promise<void>;
  clearError: () => void;
}

export function useMarketplace(options: UseMarketplaceOptions = {}): UseMarketplaceReturn {
  const {
    baseUrl = '/api',
    autoSearch = false,
    debounceMs = 300,
  } = options;

  // State
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQueryState] = useState('');
  const [selectedTags, setSelectedTagsState] = useState<string[]>([]);
  const [total, setTotal] = useState(0);

  // Refs
  const abortControllerRef = useRef<AbortController | null>(null);
  const debounceTimerRef = useRef<NodeJS.Timeout | null>(null);

  // Cancel in-flight requests
  const cancelPending = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    abortControllerRef.current = new AbortController();
  }, []);

  // Main search function
  const search = useCallback(async (q: string, tags?: string[]) => {
    cancelPending();
    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams();
      if (q) params.set('q', q);
      if (tags && tags.length > 0) {
        params.set('tags', tags.join(','));
      }
      params.set('limit', '20');

      const response = await fetch(`${baseUrl}/registry/search?${params}`, {
        signal: abortControllerRef.current?.signal,
        headers: {
          'Accept': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Search failed: ${response.status} ${response.statusText}`);
      }

      const data: SearchResult = await response.json();
      setModels(data.models);
      setTotal(data.total);
      setQuery(q);
      if (tags) setSelectedTagsState(tags);
    } catch (err) {
      if (err instanceof Error && err.name === 'AbortError') {
        return; // Ignore abort errors
      }
      setError(err instanceof Error ? err.message : 'Search failed');
      setModels([]);
    } finally {
      setLoading(false);
    }
  }, [baseUrl, cancelPending]);

  // Debounced setQuery for auto-search
  const setQuery = useCallback((q: string) => {
    setQueryState(q);
    
    if (autoSearch) {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
      debounceTimerRef.current = setTimeout(() => {
        search(q, selectedTags);
      }, debounceMs);
    }
  }, [autoSearch, debounceMs, search, selectedTags]);

  const setSelectedTags = useCallback((tags: string[]) => {
    setSelectedTagsState(tags);
    if (autoSearch) {
      search(query, tags);
    }
  }, [autoSearch, query, search]);

  // Manual refresh
  const refresh = useCallback(async () => {
    await search(query, selectedTags);
  }, [query, selectedTags, search]);

  const clearError = useCallback(() => {
    setError(null);
  }, []);

  // Initial load
  useEffect(() => {
    if (autoSearch) {
      search('', []);
    }
    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
      cancelPending();
    };
  }, []);

  return {
    models,
    loading,
    error,
    query,
    selectedTags,
    total,
    search,
    setQuery,
    setSelectedTags,
    refresh,
    clearError,
  };
}

/**
 * Get all available tags from registry
 */
export async function fetchAvailableTags(baseUrl = '/api'): Promise<{ tags: string[]; counts: Record<string, number> }> {
  const response = await fetch(`${baseUrl}/registry/tags`);
  if (!response.ok) {
    throw new Error('Failed to fetch tags');
  }
  return response.json();
}

/**
 * Get a specific model by ID
 */
export async function fetchModel(modelId: string, baseUrl = '/api'): Promise<ModelInfo> {
  const response = await fetch(`${baseUrl}/registry/model/${modelId}`);
  if (!response.ok) {
    throw new Error(`Model ${modelId} not found`);
  }
  return response.json();
}
