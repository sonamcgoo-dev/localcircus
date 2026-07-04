"""
Celery Tasks - Background execution tasks for distributed deployment.
These tasks run in Celery workers and can be distributed across machines.
"""
import time
import asyncio
from typing import Dict, List, Any

# This file is imported by Celery workers
# Run workers with: celery -A src.backend.services.celery_tasks worker


def create_celery_app():
    """Create and configure Celery app."""
    from celery import Celery
    
    celery_app = Celery(
        "ritual_zoo",
        broker="redis://localhost:6379",
        backend="redis://localhost:6379"
    )
    
    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
    )
    
    return celery_app


celery_app = create_celery_app()


@celery_app.task(bind=True, name="zoo.execute_model")
def execute_model_task(
    self,
    execution_id: str,
    model_id: str,
    prompt: str,
    temperature: float = 0.7,
    max_tokens: int = 2048,
    user_id: str = None,
    context: List[Dict] = None
) -> Dict[str, Any]:
    """
    Execute a model in a Celery worker.
    
    This runs asynchronously in a worker process,
    allowing the API to return immediately while
    the model executes in the background.
    """
    from .llm_backends import get_backend_manager, MockBackend
    import asyncio
    
    start_time = time.time()
    
    try:
        # Get backend
        backend_manager = get_backend_manager()
        backend = backend_manager.get_backend_for_model(model_id)
        
        # Run async code in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        full_output = []
        try:
            async def run():
                nonlocal full_output
                async for response in backend.generate(
                    prompt=prompt,
                    model=model_id,
                    stream=False,  # Non-streaming for background task
                    temperature=temperature,
                    max_tokens=max_tokens
                ):
                    full_output.append(response.content)
        finally:
            loop.close()
        
        output = "".join(full_output)
        latency_ms = (time.time() - start_time) * 1000
        tokens = len(output.split())
        
        return {
            "execution_id": execution_id,
            "model_id": model_id,
            "output": output,
            "tokens_generated": tokens,
            "latency_ms": latency_ms,
            "status": "success",
            "completed_at": time.time()
        }
        
    except Exception as e:
        return {
            "execution_id": execution_id,
            "model_id": model_id,
            "status": "failure",
            "error": str(e),
            "completed_at": time.time()
        }


@celery_app.task(bind=True, name="zoo.prewarm_model")
def prewarm_model_task(self, model_id: str, backend_name: str = "mock") -> Dict[str, Any]:
    """
    Prewarm a model by loading it into memory.
    Useful for keeping popular models ready.
    """
    from .llm_backends import get_backend_manager
    
    try:
        backend_manager = get_backend_manager()
        backend = backend_manager.get_backend_for_model(model_id)
        
        # Prewarm by running a dummy inference
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            async def run():
                count = 0
                async for _ in backend.generate(
                    prompt="warmup",
                    model=model_id,
                    stream=False
                ):
                    count += 1
        finally:
            loop.close()
        
        return {
            "model_id": model_id,
            "status": "success",
            "prewarmed": True
        }
        
    except Exception as e:
        return {
            "model_id": model_id,
            "status": "failure",
            "error": str(e)
        }


@celery_app.task(bind=True, name="zoo.cleanup_cache")
def cleanup_cache_task(self) -> Dict[str, Any]:
    """
    Periodic task to clean up model cache.
    Removes models that haven't been used recently.
    """
    from .model_pool import get_model_pool
    import asyncio
    
    try:
        pool = get_model_pool()
        
        # Get current stats before cleanup
        stats_before = pool.get_stats()
        
        # In a real implementation, you would:
        # 1. Check which models are idle
        # 2. Evict models over memory limit
        # 3. Log cleanup actions
        
        return {
            "status": "success",
            "models_before": stats_before["models_loaded"],
            "timestamp": time.time()
        }
        
    except Exception as e:
        return {
            "status": "failure",
            "error": str(e)
        }


# Celery Beat schedule for periodic tasks
celery_app.conf.beat_schedule = {
    "cleanup-cache-every-5-minutes": {
        "task": "zoo.cleanup_cache",
        "schedule": 300.0,  # 5 minutes
    },
}


if __name__ == "__main__":
    # Run worker: celery -A src.backend.services.celery_tasks worker --loglevel=info
    # Run beat: celery -A src.backend.services.celery_tasks beat --loglevel=info
    celery_app.start()
