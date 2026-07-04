"""
FastAPI routes for marketplace integration.
Wired endpoints: /registry/search, /zoo/run, /zoo/stream
Full ChatGPT-level system with:
- Multiple LLM backends (Ollama, vLLM, OpenAI)
- ACP permission enforcement
- Model prewarm pool
- WebSocket streaming
"""
import asyncio
import json
import time
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..registry.models import get_registry
from ..services.streaming import (
    get_executor, ExecutionRequest, ExecutionStatus
)
from ..services.model_pool import get_model_pool
from ..services.llm_backends import get_backend_manager
from ..services.acp_enforcer import get_acp_enforcer


router = APIRouter()


# Request/Response Models

class SearchRequest(BaseModel):
    query: str = Field(default="", description="Search query string")
    tags: Optional[List[str]] = Field(default=None, description="Filter by tags")
    limit: int = Field(default=20, ge=1, le=100, description="Max results")


class SearchResponse(BaseModel):
    models: List[dict]
    total: int
    query: str


class RunRequest(BaseModel):
    model_id: str = Field(..., description="Model ID to execute")
    input_data: str = Field(..., description="Input prompt/data")
    stream: bool = Field(default=False, description="Enable streaming")
    temperature: float = Field(default=0.7, ge=0, le=2)
    max_tokens: int = Field(default=2048, ge=1, le=65536)


class RunResponse(BaseModel):
    execution_id: str
    model_id: str
    status: str
    output: str
    latency_ms: float
    tokens_generated: int


class HealthResponse(BaseModel):
    status: str
    registry_models: int
    cache_status: dict


# Registry Endpoints

@router.get("/registry/search", response_model=SearchResponse)
async def search_registry(
    q: str = Query(default="", description="Search query"),
    tags: Optional[str] = Query(default=None, description="Comma-separated tags"),
    limit: int = Query(default=20, ge=1, le=100)
):
    """
    Wire: Marketplace → /registry/search
    
    Live query hook that searches the model registry.
    Replaces stub arrays with real tag-index driven search.
    """
    registry = get_registry()
    
    # Parse tags if provided
    tag_list = None
    if tags:
        tag_list = [t.strip().lower() for t in tags.split(",") if t.strip()]
    
    # Search with tag-index
    results = registry.search(query=q, tags=tag_list, limit=limit)
    
    return SearchResponse(
        models=[m.to_dict() for m in results],
        total=len(results),
        query=q
    )


@router.get("/registry/model/{model_id}")
async def get_model(model_id: str):
    """Get a specific model by ID."""
    registry = get_registry()
    model = registry.get(model_id)
    
    if not model:
        raise HTTPException(status_code=404, detail=f"Model {model_id} not found")
    
    return model.to_dict()


@router.get("/registry/tags")
async def list_tags():
    """List all available tags in the registry."""
    registry = get_registry()
    return {
        "tags": list(registry._tag_index.keys()),
        "counts": {tag: len(models) for tag, models in registry._tag_index.items()}
    }


# Zoo Execution Endpoints

@router.post("/zoo/run", response_model=RunResponse)
async def run_model(request: RunRequest):
    """
    Wire: Run button → /zoo/run
    
    Executes a model through the Zoo pipeline.
    Returns complete output (non-streaming).
    ACP enforced - permission checks before execution.
    """
    executor = get_executor()
    registry = get_registry()
    acp = get_acp_enforcer()
    
    # ACP permission check
    session_id = "http_api"  # Could be extracted from auth header
    allowed, reason = await acp.check_execution(session_id, request.model_id)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason)
    
    # Verify model exists
    model = registry.get(request.model_id)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model {request.model_id} not found")
    
    # Create execution request
    exec_request = ExecutionRequest(
        model_id=request.model_id,
        input_data=request.input_data,
        stream=False,
        temperature=request.temperature,
        max_tokens=request.max_tokens
    )
    
    # Execute
    response = await executor.execute(exec_request)
    
    # Record metrics
    registry.record_execution(
        model_id=request.model_id,
        latency_ms=response.latency_ms,
        tokens=response.tokens_generated,
        success=(response.status == ExecutionStatus.COMPLETED)
    )
    
    # Record ACP execution
    acp.record_execution(session_id, request.model_id)
    
    return RunResponse(
        execution_id=response.execution_id,
        model_id=response.model_id,
        status=response.status.value,
        output=response.output,
        latency_ms=response.latency_ms,
        tokens_generated=response.tokens_generated
    )


@router.get("/zoo/stream")
async def stream_model(
    model_id: str = Query(..., description="Model ID to execute"),
    input_data: str = Query(..., description="Input prompt"),
    temperature: float = Query(default=0.7, ge=0, le=2),
    max_tokens: int = Query(default=2048, ge=1, le=65536)
):
    """
    Wire: Streaming execution endpoint
    
    SSE streaming for token-by-token output.
    Creates "alive" feeling with real-time updates.
    
    Event format:
    - event: chunk
      data: {"token": "...", "tokens_generated": N, "is_final": false}
    
    - event: done
      data: {"execution_id": "...", "latency_ms": N}
    """
    executor = get_executor()
    registry = get_registry()
    pool = get_model_pool()
    acp = get_acp_enforcer()
    
    # ACP permission check
    session_id = "sse_stream"
    allowed, reason = await acp.check_execution(session_id, model_id)
    if not allowed:
        raise HTTPException(status_code=403, detail=reason)
    
    # Verify model exists
    model = registry.get(model_id)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model {model_id} not found")
    
    # Prewarm pool
    await pool.prewarm([model_id])
    
    # Create streaming execution request
    exec_request = ExecutionRequest(
        model_id=model_id,
        input_data=input_data,
        stream=True,
        temperature=temperature,
        max_tokens=max_tokens
    )
    
    async def event_generator():
        """Generate SSE events for streaming response."""
        start_time = time.time()
        
        try:
            async for chunk in executor.execute_streaming(exec_request):
                if chunk.is_final:
                    latency_ms = (time.time() - start_time) * 1000
                    yield f"event: done\ndata: {json.dumps({
                        'execution_id': chunk.execution_id,
                        'tokens_generated': chunk.tokens_generated,
                        'latency_ms': latency_ms
                    })}\n\n"
                else:
                    yield f"event: chunk\ndata: {json.dumps({
                        'token': chunk.chunk,
                        'tokens_generated': chunk.tokens_generated,
                        'is_final': False
                    })}\n\n"
                    
        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.post("/zoo/cancel/{execution_id}")
async def cancel_execution(execution_id: str):
    """Cancel an active streaming execution."""
    executor = get_executor()
    success = executor.cancel(execution_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
    
    return {"status": "cancelled", "execution_id": execution_id}


# System Endpoints

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """System health check with registry and cache status."""
    registry = get_registry()
    executor = get_executor()
    
    return HealthResponse(
        status="healthy",
        registry_models=len(registry._models),
        cache_status=executor.get_cache_status()
    )


@router.post("/zoo/prewarm")
async def prewarm_models(model_ids: List[str]):
    """
    Prewarm the model pool with specified models.
    Enables instant model switching.
    """
    pool = get_model_pool()
    await pool.prewarm(model_ids)
    
    return {
        "status": "prewarmed",
        "models": model_ids,
        "cached": pool.get_cached_models(),
        "pool_stats": pool.get_stats()
    }


@router.get("/backends/status")
async def backends_status():
    """Get status of all LLM backends."""
    backends = get_backend_manager()
    return {
        "health": await backends.health_check(),
        "available_models": await backends.get_available_models()
    }


@router.post("/pool/prewarm")
async def pool_prewarm(model_ids: List[str]):
    """Prewarm pool for instant model switching."""
    pool = get_model_pool()
    await pool.prewarm(model_ids)
    return {"status": "ok", "cached": pool.get_cached_models()}


@router.post("/pool/prewarm-priority")
async def pool_prewarm_priority(model_ids: List[str]):
    """Priority prewarm - load immediately."""
    pool = get_model_pool()
    await pool.prewarm_priority(model_ids)
    return {"status": "ok", "cached": pool.get_cached_models()}
