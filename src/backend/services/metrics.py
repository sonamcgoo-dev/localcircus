"""
Prometheus Metrics - Observability for production monitoring.
Tracks execution counts, latencies, cache performance, and system health.
"""
import time
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict
import threading
import json

try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Summary, 
        generate_latest, CONTENT_TYPE_LATEST,
        CollectorRegistry, REGISTRY
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


# ==================== Metrics Definitions ====================

class Metrics:
    """
    Prometheus metrics definitions.
    
    Counters (only increment):
    - Total executions
    - Cache hits/misses
    - Errors
    - User actions
    
    Histograms (distribution):
    - Execution latency
    - Token generation time
    - API response time
    
    Gauges (can go up/down):
    - Active executions
    - Cached models
    - Connected WebSocket clients
    - Memory usage
    """
    
    def __init__(self, registry=None):
        self.registry = registry or REGISTRY
        
        if PROMETHEUS_AVAILABLE:
            self._init_prometheus()
        else:
            self._init_fallback()
    
    def _init_prometheus(self):
        """Initialize Prometheus metrics."""
        
        # Execution metrics
        self.executions_total = Counter(
            "zoo_executions_total",
            "Total number of model executions",
            ["model_id", "status"],
            registry=self.registry
        )
        
        self.execution_latency_seconds = Histogram(
            "zoo_execution_latency_seconds",
            "Model execution latency in seconds",
            ["model_id"],
            buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
            registry=self.registry
        )
        
        self.tokens_generated_total = Counter(
            "zoo_tokens_generated_total",
            "Total tokens generated",
            ["model_id"],
            registry=self.registry
        )
        
        # Cache metrics
        self.cache_hits_total = Counter(
            "zoo_cache_hits_total",
            "Total cache hits",
            registry=self.registry
        )
        
        self.cache_misses_total = Counter(
            "zoo_cache_misses_total",
            "Total cache misses",
            registry=self.registry
        )
        
        self.cached_models = Gauge(
            "zoo_cached_models",
            "Number of currently cached models",
            registry=self.registry
        )
        
        # API metrics
        self.api_requests_total = Counter(
            "zoo_api_requests_total",
            "Total API requests",
            ["endpoint", "method", "status"],
            registry=self.registry
        )
        
        self.api_latency_seconds = Histogram(
            "zoo_api_latency_seconds",
            "API request latency in seconds",
            ["endpoint", "method"],
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
            registry=self.registry
        )
        
        # WebSocket metrics
        self.ws_connections_total = Counter(
            "zoo_ws_connections_total",
            "Total WebSocket connections",
            registry=self.registry
        )
        
        self.ws_active_connections = Gauge(
            "zoo_ws_active_connections",
            "Number of active WebSocket connections",
            registry=self.registry
        )
        
        # System metrics
        self.active_executions = Gauge(
            "zoo_active_executions",
            "Number of currently running executions",
            registry=self.registry
        )
        
        self.memory_usage_bytes = Gauge(
            "zoo_memory_usage_bytes",
            "Memory usage in bytes",
            registry=self.registry
        )
        
        # User metrics
        self.user_executions_total = Counter(
            "zoo_user_executions_total",
            "Total executions per user",
            ["user_id", "model_id"],
            registry=self.registry
        )
        
        # Error metrics
        self.errors_total = Counter(
            "zoo_errors_total",
            "Total errors",
            ["error_type", "endpoint"],
            registry=self.registry
        )
    
    def _init_fallback(self):
        """Fallback metrics when Prometheus not available."""
        self._counters: Dict[str, int] = defaultdict(int)
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._gauges: Dict[str, float] = {}
    
    # ==================== Record Methods ====================
    
    def record_execution(self, model_id: str, latency_ms: float, tokens: int, status: str = "success"):
        """Record a model execution."""
        if PROMETHEUS_AVAILABLE:
            self.executions_total.labels(model_id=model_id, status=status).inc()
            self.execution_latency_seconds.labels(model_id=model_id).observe(latency_ms / 1000)
            self.tokens_generated_total.labels(model_id=model_id).inc(tokens)
        else:
            self._counters[f"executions_{model_id}_{status}"] += 1
            self._histograms[f"latency_{model_id}"].append(latency_ms)
            self._counters[f"tokens_{model_id}"] += tokens
    
    def record_cache_hit(self):
        """Record a cache hit."""
        if PROMETHEUS_AVAILABLE:
            self.cache_hits_total.inc()
        else:
            self._counters["cache_hits"] += 1
    
    def record_cache_miss(self):
        """Record a cache miss."""
        if PROMETHEUS_AVAILABLE:
            self.cache_misses_total.inc()
        else:
            self._counters["cache_misses"] += 1
    
    def set_cached_models(self, count: int):
        """Set number of cached models."""
        if PROMETHEUS_AVAILABLE:
            self.cached_models.set(count)
        else:
            self._gauges["cached_models"] = count
    
    def record_api_request(self, endpoint: str, method: str, status: int, latency_ms: float):
        """Record an API request."""
        if PROMETHEUS_AVAILABLE:
            self.api_requests_total.labels(
                endpoint=endpoint, method=method, status=str(status)
            ).inc()
            self.api_latency_seconds.labels(
                endpoint=endpoint, method=method
            ).observe(latency_ms / 1000)
        else:
            self._counters[f"api_{endpoint}_{method}_{status}"] += 1
            self._histograms[f"api_latency_{endpoint}"].append(latency_ms)
    
    def record_ws_connect(self):
        """Record a WebSocket connection."""
        if PROMETHEUS_AVAILABLE:
            self.ws_connections_total.inc()
            self.ws_active_connections.inc()
        else:
            self._counters["ws_connections"] += 1
            self._gauges["ws_active"] = self._gauges.get("ws_active", 0) + 1
    
    def record_ws_disconnect(self):
        """Record a WebSocket disconnection."""
        if PROMETHEUS_AVAILABLE:
            self.ws_active_connections.dec()
        else:
            self._gauges["ws_active"] = max(0, self._gauges.get("ws_active", 1) - 1)
    
    def set_active_executions(self, count: int):
        """Set number of active executions."""
        if PROMETHEUS_AVAILABLE:
            self.active_executions.set(count)
        else:
            self._gauges["active_executions"] = count
    
    def record_error(self, error_type: str, endpoint: str = "unknown"):
        """Record an error."""
        if PROMETHEUS_AVAILABLE:
            self.errors_total.labels(error_type=error_type, endpoint=endpoint).inc()
        else:
            self._counters[f"error_{error_type}"] += 1
    
    def record_user_execution(self, user_id: str, model_id: str):
        """Record execution by user."""
        if PROMETHEUS_AVAILABLE:
            self.user_executions_total.labels(user_id=user_id, model_id=model_id).inc()
        else:
            self._counters[f"user_exec_{user_id}_{model_id}"] += 1
    
    # ==================== Export ====================
    
    def export(self) -> bytes:
        """Export metrics in Prometheus format."""
        if PROMETHEUS_AVAILABLE:
            return generate_latest(self.registry)
        else:
            # Fallback: export as JSON
            return json.dumps({
                "counters": dict(self._counters),
                "histograms": {
                    k: {"count": len(v), "sum": sum(v), "avg": sum(v)/len(v) if v else 0}
                    for k, v in self._histograms.items()
                },
                "gauges": self._gauges
            }, indent=2).encode()
    
    def get_summary(self) -> Dict:
        """Get metrics summary as dict."""
        if PROMETHEUS_AVAILABLE:
            # Parse from export
            data = self.export().decode()
            # Simple parsing - in production use proper prometheus_client parsing
            return {"format": "prometheus", "size_bytes": len(data)}
        else:
            cache_total = self._counters.get("cache_hits", 0) + self._counters.get("cache_misses", 0)
            return {
                "counters": dict(self._counters),
                "histograms": {
                    k: {"count": len(v), "avg_ms": sum(v)/len(v) if v else 0}
                    for k, v in self._histograms.items()
                },
                "gauges": self._gauges,
                "cache_hit_rate": (
                    self._counters.get("cache_hits", 0) / cache_total 
                    if cache_total > 0 else 0
                )
            }


# ==================== Metrics Middleware ====================

class MetricsMiddleware:
    """
    FastAPI middleware for automatic API metrics collection.
    """
    
    def __init__(self, metrics: Metrics):
        self.metrics = metrics
    
    async def __call__(self, request, call_next):
        import time
        
        start_time = time.time()
        endpoint = request.url.path
        method = request.method
        
        try:
            response = await call_next(request)
            status = response.status_code
        except Exception as e:
            status = 500
            self.metrics.record_error(type(e).__name__, endpoint)
            raise
        finally:
            latency_ms = (time.time() - start_time) * 1000
            self.metrics.record_api_request(endpoint, method, status, latency_ms)
        
        return response


# ==================== Request Timing ====================

class RequestTimer:
    """Context manager for timing requests."""
    
    def __init__(self, metrics: Metrics, model_id: str = None):
        self.metrics = metrics
        self.model_id = model_id
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        if self.model_id:
            self.metrics.set_active_executions(
                self.metrics._gauges.get("active_executions", 0) + 1
            )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.model_id:
            latency_ms = (time.time() - self.start_time) * 1000
            status = "success" if exc_type is None else "error"
            self.metrics.record_execution(
                self.model_id, latency_ms, 0, status
            )
            self.metrics.set_active_executions(
                max(0, self.metrics._gauges.get("active_executions", 1) - 1)
            )
        return False  # Don't suppress exceptions


# ==================== Global Instance ====================

_metrics: Optional[Metrics] = None


def get_metrics() -> Metrics:
    """Get global metrics instance."""
    global _metrics
    if _metrics is None:
        _metrics = Metrics()
    return _metrics


def init_metrics() -> Metrics:
    """Initialize metrics with custom registry."""
    global _metrics
    _metrics = Metrics()
    return _metrics
