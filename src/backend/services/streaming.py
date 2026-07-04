"""
Streaming execution service with SSE support.
Provides token-by-token output for "alive" feeling.
"""
import asyncio
import time
import uuid
import hashlib
from typing import Dict, Generator, Optional, AsyncGenerator, Callable
from dataclasses import dataclass
from enum import Enum
import random


class ExecutionStatus(Enum):
    PENDING = "pending"
    LOADING = "loading"
    RUNNING = "running"
    STREAMING = "streaming"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ExecutionRequest:
    """Request for model execution."""
    model_id: str
    input_data: str
    stream: bool = True
    temperature: float = 0.7
    max_tokens: int = 2048
    execution_id: Optional[str] = None
    
    def __post_init__(self):
        if self.execution_id is None:
            self.execution_id = str(uuid.uuid4())


@dataclass
class ExecutionResponse:
    """Response from model execution."""
    execution_id: str
    model_id: str
    status: ExecutionStatus
    output: str = ""
    latency_ms: float = 0.0
    tokens_generated: int = 0
    error: Optional[str] = None


@dataclass
class StreamChunk:
    """Single chunk in a stream response."""
    execution_id: str
    chunk: str
    tokens_generated: int
    is_final: bool = False


class ModelCache:
    """
    Model caching layer to avoid reloading llama models.
    Maintains persistent runtime pool with prewarming support.
    """
    
    def __init__(self, max_cached_models: int = 3):
        self._cache: Dict[str, Dict] = {}
        self._max_cached = max_cached_models
        self._access_times: Dict[str, float] = {}
    
    def get(self, model_id: str) -> Optional[Dict]:
        """Get cached model if available."""
        if model_id in self._cache:
            self._access_times[model_id] = time.time()
            return self._cache[model_id]
        return None
    
    def put(self, model_id: str, model_data: Dict):
        """Cache a model, evicting LRU if necessary."""
        if model_id in self._cache:
            self._cache[model_id] = model_data
            self._access_times[model_id] = time.time()
            return
        
        # Evict LRU if at capacity
        if len(self._cache) >= self._max_cached:
            lru_id = min(self._access_times, key=self._access_times.get)
            self.evict(lru_id)
        
        self._cache[model_id] = model_data
        self._access_times[model_id] = time.time()
    
    def evict(self, model_id: str):
        """Evict model from cache."""
        if model_id in self._cache:
            del self._cache[model_id]
            del self._access_times[model_id]
    
    def prewarm(self, model_ids: list):
        """Prewarm cache with specified models."""
        for model_id in model_ids[:self._max_cached]:
            if model_id not in self._cache:
                self.put(model_id, {"model_id": model_id, "prewarmed": True})
    
    def is_cached(self, model_id: str) -> bool:
        """Check if model is in cache."""
        return model_id in self._cache


class StreamingExecutor:
    """
    Streaming execution service with SSE support.
    Token-by-token output creates "alive" feeling.
    """
    
    def __init__(self):
        self._cache = ModelCache(max_cached_models=3)
        self._active_executions: Dict[str, ExecutionRequest] = {}
        self._execution_history: list = []
    
    async def execute(
        self, 
        request: ExecutionRequest,
        progress_callback: Optional[Callable] = None
    ) -> ExecutionResponse:
        """Execute a model synchronously (non-streaming)."""
        start_time = time.time()
        self._active_executions[request.execution_id] = request
        
        try:
            # Simulate model loading (if not cached)
            if not self._cache.is_cached(request.model_id):
                self._cache.put(request.model_id, {"model_id": request.model_id})
                await asyncio.sleep(0.1)  # Model load time
            
            # Simulate inference
            output = await self._simulate_inference(request)
            
            latency_ms = (time.time() - start_time) * 1000
            
            return ExecutionResponse(
                execution_id=request.execution_id,
                model_id=request.model_id,
                status=ExecutionStatus.COMPLETED,
                output=output,
                latency_ms=latency_ms,
                tokens_generated=len(output.split())
            )
        
        except Exception as e:
            return ExecutionResponse(
                execution_id=request.execution_id,
                model_id=request.model_id,
                status=ExecutionStatus.FAILED,
                error=str(e),
                latency_ms=(time.time() - start_time) * 1000
            )
        finally:
            if request.execution_id in self._active_executions:
                del self._active_executions[request.execution_id]
    
    async def execute_streaming(
        self, 
        request: ExecutionRequest
    ) -> AsyncGenerator[StreamChunk, None]:
        """
        Execute model with streaming output.
        Yields token-by-token for "alive" feeling.
        """
        self._active_executions[request.execution_id] = request
        start_time = time.time()
        tokens_count = 0
        
        try:
            # Simulate model loading (if not cached)
            if not self._cache.is_cached(request.model_id):
                yield StreamChunk(
                    execution_id=request.execution_id,
                    chunk="[loading model...]\n\n",
                    tokens_generated=0,
                    is_final=False
                )
                self._cache.put(request.model_id, {"model_id": request.model_id})
                await asyncio.sleep(0.3)  # Model load time
            
            # Yield loading complete indicator
            yield StreamChunk(
                execution_id=request.execution_id,
                chunk="[model ready]\n\n",
                tokens_generated=0,
                is_final=False
            )
            
            # Stream the actual output
            full_output = await self._simulate_inference(request)
            words = full_output.split()
            
            for i, word in enumerate(words):
                await asyncio.sleep(0.02)  # Simulate token delay
                tokens_count += 1
                
                # Add some punctuation formatting
                chunk_text = word
                if word.endswith('.') or word.endswith('!') or word.endswith('?'):
                    chunk_text += "\n"
                
                yield StreamChunk(
                    execution_id=request.execution_id,
                    chunk=chunk_text + " ",
                    tokens_generated=tokens_count,
                    is_final=False
                )
            
            # Send final chunk
            yield StreamChunk(
                execution_id=request.execution_id,
                chunk="",
                tokens_generated=tokens_count,
                is_final=True
            )
            
        except Exception as e:
            yield StreamChunk(
                execution_id=request.execution_id,
                chunk=f"\n[error: {str(e)}]\n",
                tokens_generated=tokens_count,
                is_final=True
            )
        finally:
            if request.execution_id in self._active_executions:
                del self._active_executions[request.execution_id]
    
    async def _simulate_inference(self, request: ExecutionRequest) -> str:
        """Simulate model inference with realistic responses."""
        # Model-specific responses
        model_responses = {
            "llama-3.1-8b-instruct": [
                "LLaMA 3.1 8B responding: This is a demonstration of the streaming execution system. "
                "The model is generating token-by-token output, creating an interactive and responsive feel. "
                "Notice how each word appears progressively, simulating real-time inference.",
                "Hello! I'm running on LLaMA 3.1 8B through the streaming pipeline. "
                "This execution showcases the integrated Zoo runtime with real-time token generation. "
                "The system maintains low latency while providing immediate feedback.",
            ],
            "llama-3.1-70b-instruct": [
                "LLaMA 3.1 70B processing your request through the Zoo runtime. "
                "This powerful model is generating a comprehensive response with streaming output. "
                "The execution demonstrates seamless integration between Marketplace and Zoo services.",
                "I'm LLaMA 3.1 70B, executing your prompt via the streaming endpoint. "
                "My large context window allows me to maintain coherence across long exchanges. "
                "The streaming infrastructure provides real-time token delivery.",
            ],
            "mistral-7b-instruct": [
                "Mistral 7B here, streaming your response through the unified execution layer. "
                "My efficient architecture delivers fast inference while maintaining quality. "
                "The token-by-token output creates that responsive, 'alive' feeling.",
            ],
            "codellama-13b-instruct": [
                "Code LLaMA 13B executing your request. Here's a Python function:\n\n"
                "```python\ndef streaming_demo():\n    for i in range(10):\n        yield f\"Token {i}\\n\"\n```\n\n"
                "This demonstrates code generation with proper formatting preserved.",
            ],
            "phi-3-mini-128k": [
                "Phi-3 Mini streaming response. Despite my compact size of 3.8B parameters, "
                "I deliver strong performance with impressive context understanding up to 128K tokens. "
                "The streaming system makes me feel responsive and immediate.",
            ],
        }
        
        responses = model_responses.get(request.model_id, [
            f"Executing {request.model_id} through streaming pipeline. "
            "The token-by-token output creates that responsive, interactive feel. "
            "Each word appears progressively, simulating real inference latency while maintaining engagement."
        ])
        
        return random.choice(responses)
    
    def cancel(self, execution_id: str) -> bool:
        """Cancel an active execution."""
        if execution_id in self._active_executions:
            self._active_executions[execution_id].cancelled = True
            return True
        return False
    
    def get_cache_status(self) -> Dict:
        """Get current cache status."""
        return {
            "cached_models": list(self._cache._cache.keys()),
            "total_cached": len(self._cache._cache),
            "max_cached": self._cache._max_cached,
            "active_executions": len(self._active_executions)
        }


# Singleton executor instance
_executor = StreamingExecutor()


def get_executor() -> StreamingExecutor:
    """Get the singleton executor instance."""
    return _executor
