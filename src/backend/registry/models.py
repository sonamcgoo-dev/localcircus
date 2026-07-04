"""
Model Registry with tag-index and runtime scores.
Replaces stub arrays with real registry data structure.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time
import hashlib


@dataclass
class RuntimeMetrics:
    """Runtime performance metrics for models."""
    total_runs: int = 0
    successful_runs: int = 0
    avg_latency_ms: float = 0.0
    total_tokens: int = 0
    last_used: float = 0.0
    
    @property
    def success_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.successful_runs / self.total_runs
    
    @property
    def usage_velocity(self) -> float:
        """Calculated usage velocity score (0-1)."""
        if self.total_runs == 0:
            return 0.3  # Default low velocity for new models
        recency = 1.0 - min((time.time() - self.last_used) / 86400, 1.0)  # Decay over 24h
        return min((self.total_runs * 0.1 + recency * 0.5 + self.success_rate * 0.4), 1.0)


@dataclass
class ModelRegistryEntry:
    """Complete model registry entry with metadata and metrics."""
    id: str
    name: str
    description: str
    tags: List[str]
    capabilities: List[str]
    context_window: int
    params_b: float  # Parameters in billions
    registry_url: str
    checksum: str
    runtime_metrics: RuntimeMetrics = field(default_factory=RuntimeMetrics)
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "tags": self.tags,
            "capabilities": self.capabilities,
            "context_window": self.context_window,
            "params_b": self.params_b,
            "registry_url": self.registry_url,
            "checksum": self.checksum,
            "runtime_metrics": {
                "total_runs": self.runtime_metrics.total_runs,
                "successful_runs": self.runtime_metrics.successful_runs,
                "avg_latency_ms": self.runtime_metrics.avg_latency_ms,
                "total_tokens": self.runtime_metrics.total_tokens,
                "last_used": self.runtime_metrics.last_used,
                "usage_velocity": self.runtime_metrics.usage_velocity,
                "success_rate": self.runtime_metrics.success_rate,
            }
        }


class ModelRegistry:
    """
    Tag-indexed model registry with runtime score tracking.
    Provides fast lookup by tags and sorted results by usage velocity.
    """
    
    def __init__(self):
        self._models: Dict[str, ModelRegistryEntry] = {}
        self._tag_index: Dict[str, set] = {}  # tag -> set of model_ids
        self._initialize_default_models()
    
    def _initialize_default_models(self):
        """Populate registry with default model catalog."""
        default_models = [
            ModelRegistryEntry(
                id="llama-3.1-8b-instruct",
                name="LLaMA 3.1 8B Instruct",
                description="Meta's latest instruction-tuned model, excellent for chat and reasoning",
                tags=["chat", "instruction", "reasoning", "meta", "8b", "fast"],
                capabilities=["chat", "code", "reasoning", "writing"],
                context_window=128000,
                params_b=8.0,
                registry_url="hf://meta-llama/Llama-3.1-8B-Instruct",
                checksum="sha256:abc123"
            ),
            ModelRegistryEntry(
                id="llama-3.1-70b-instruct",
                name="LLaMA 3.1 70B Instruct", 
                description="Meta's large instruct model for complex tasks",
                tags=["chat", "instruction", "reasoning", "meta", "70b", "powerful"],
                capabilities=["chat", "code", "reasoning", "writing", "analysis"],
                context_window=128000,
                params_b=70.0,
                registry_url="hf://meta-llama/Llama-3.1-70B-Instruct",
                checksum="sha256:def456"
            ),
            ModelRegistryEntry(
                id="mistral-7b-instruct",
                name="Mistral 7B Instruct",
                description="High-performance 7B model from Mistral AI",
                tags=["chat", "instruction", "mistral", "7b", "fast", "efficient"],
                capabilities=["chat", "code", "reasoning"],
                context_window=32768,
                params_b=7.3,
                registry_url="hf://mistralai/Mistral-7B-Instruct-v0.3",
                checksum="sha256:ghi789"
            ),
            ModelRegistryEntry(
                id="codellama-13b-instruct",
                name="Code LLaMA 13B Instruct",
                description="Specialized code generation and completion model",
                tags=["code", "instruction", "programming", "llama", "13b"],
                capabilities=["code", "debugging", "refactoring", "documentation"],
                context_window=16384,
                params_b=13.0,
                registry_url="hf://codellama/CodeLLaMA-13B-Instruct",
                checksum="sha256:jkl012"
            ),
            ModelRegistryEntry(
                id="phi-3-mini-128k",
                name="Phi-3 Mini 128K",
                description="Microsoft's efficient small model with massive context",
                tags=["chat", "instruction", "microsoft", "3.8b", "fast", "long-context"],
                capabilities=["chat", "reasoning", "long-context"],
                context_window=128000,
                params_b=3.8,
                registry_url="hf://microsoft/Phi-3-mini-128k-instruct",
                checksum="sha256:mno345"
            ),
            ModelRegistryEntry(
                id="qwen2.5-72b-instruct",
                name="Qwen2.5 72B Instruct",
                description="Alibaba's powerful multilingual instruction model",
                tags=["chat", "instruction", "multilingual", "qwen", "72b", "powerful"],
                capabilities=["chat", "code", "reasoning", "multimodal", "analysis"],
                context_window=32768,
                params_b=72.0,
                registry_url="hf://qwen/Qwen2.5-72B-Instruct",
                checksum="sha256:pqr678"
            ),
            ModelRegistryEntry(
                id="deepseek-coder-33b",
                name="DeepSeek Coder 33B",
                description="Specialized code model with excellent generation",
                tags=["code", "programming", "deepseek", "33b", "agent"],
                capabilities=["code", "debugging", "refactoring", "agentic"],
                context_window=16384,
                params_b=33.0,
                registry_url="hf://deepseek-ai/DeepSeek-Coder-33B-Instruct",
                checksum="sha256:stu901"
            ),
            ModelRegistryEntry(
                id="wizardcoder-15b",
                name="WizardCoder 15B",
                description="Expert code generation with complex instruction following",
                tags=["code", "programming", "wizard", "15b", "instruction"],
                capabilities=["code", "debugging", "explanation", "refactoring"],
                context_window=8192,
                params_b=15.0,
                registry_url="hf://WizardLM/WizardCoder-15B-V1.0",
                checksum="sha256:vwx234"
            ),
        ]
        
        for model in default_models:
            self.register(model)
    
    def register(self, model: ModelRegistryEntry):
        """Register a model and update tag index."""
        self._models[model.id] = model
        for tag in model.tags:
            if tag not in self._tag_index:
                self._tag_index[tag] = set()
            self._tag_index[tag].add(model.id)
    
    def search(self, query: str, tags: Optional[List[str]] = None, limit: int = 20) -> List[ModelRegistryEntry]:
        """
        Search registry by query and/or tags.
        Returns models sorted by usage velocity (most popular first).
        """
        query_lower = query.lower()
        candidate_ids = set()
        
        # If tags provided, start with tag intersection
        if tags:
            tag_sets = [self._tag_index.get(t, set()) for t in tags]
            candidate_ids = set.intersection(*tag_sets) if tag_sets else set()
        
        # Also search by query in name, description, and tags
        query_matches = set()
        for mid, model in self._models.items():
            if (query_lower in model.name.lower() or 
                query_lower in model.description.lower() or
                any(query_lower in t.lower() for t in model.tags)):
                query_matches.add(mid)
        
        # Combine: tags AND query matches (if both specified), or either if one is empty
        if tags and query:
            candidate_ids = candidate_ids & query_matches
        elif tags:
            pass  # candidate_ids already has tag matches
        elif query:
            candidate_ids = query_matches
        else:
            candidate_ids = set(self._models.keys())
        
        # Sort by usage velocity (descending)
        results = [self._models[mid] for mid in candidate_ids if mid in self._models]
        results.sort(key=lambda m: m.runtime_metrics.usage_velocity, reverse=True)
        
        return results[:limit]
    
    def get(self, model_id: str) -> Optional[ModelRegistryEntry]:
        """Get a specific model by ID."""
        return self._models.get(model_id)
    
    def get_by_tag(self, tag: str, limit: int = 20) -> List[ModelRegistryEntry]:
        """Get all models with a specific tag, sorted by usage velocity."""
        model_ids = self._tag_index.get(tag.lower(), set())
        results = [self._models[mid] for mid in model_ids if mid in self._models]
        results.sort(key=lambda m: m.runtime_metrics.usage_velocity, reverse=True)
        return results[:limit]
    
    def record_execution(self, model_id: str, latency_ms: float, tokens: int, success: bool):
        """Record execution metrics for a model."""
        if model_id in self._models:
            m = self._models[model_id]
            m.runtime_metrics.total_runs += 1
            if success:
                m.runtime_metrics.successful_runs += 1
            # Update rolling average latency
            n = m.runtime_metrics.total_runs
            old_avg = m.runtime_metrics.avg_latency_ms
            m.runtime_metrics.avg_latency_ms = old_avg + (latency_ms - old_avg) / n
            m.runtime_metrics.total_tokens += tokens
            m.runtime_metrics.last_used = time.time()
    
    def get_all(self) -> List[ModelRegistryEntry]:
        """Get all models sorted by usage velocity."""
        results = list(self._models.values())
        results.sort(key=lambda m: m.runtime_metrics.usage_velocity, reverse=True)
        return results


# Singleton registry instance
_registry = ModelRegistry()


def get_registry() -> ModelRegistry:
    """Get the singleton registry instance."""
    return _registry
