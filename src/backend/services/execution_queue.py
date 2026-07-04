"""
Queue-Based Execution - Celery-based async execution for long-running tasks.
Handles background model execution, retries, and result retrieval.
"""
import asyncio
import json
import time
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

try:
    from celery import Celery
    from celery.result import AsyncResult
    CELERY_AVAILABLE = True
except ImportError:
    CELERY_AVAILABLE = False


class TaskStatus(Enum):
    PENDING = "pending"
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    REVOKED = "revoked"


@dataclass
class ExecutionRequest:
    """Request for model execution."""
    execution_id: str
    model_id: str
    prompt: str
    temperature: float = 0.7
    max_tokens: int = 2048
    user_id: str = None
    context: List[Dict] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)


@dataclass
class ExecutionResult:
    """Result from model execution."""
    execution_id: str
    status: str
    output: str = ""
    tokens_generated: int = 0
    latency_ms: float = 0.0
    error: str = None
    created_at: float = 0.0
    completed_at: float = 0.0


class InMemoryQueue:
    """
    In-memory execution queue for when Celery is unavailable.
    Useful for development and single-instance deployments.
    """
    
    def __init__(self):
        self._tasks: Dict[str, ExecutionResult] = {}
        self._results: Dict[str, str] = {}  # execution_id -> output
        self._status: Dict[str, TaskStatus] = {}
        self._running: Dict[str, asyncio.Task] = {}
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
    
    async def submit(
        self,
        execution_id: str,
        model_id: str,
        prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        executor_func: Callable = None
    ) -> str:
        """Submit a task for execution."""
        self._status[execution_id] = TaskStatus.PENDING
        
        # Store request
        self._tasks[execution_id] = ExecutionResult(
            execution_id=execution_id,
            status=TaskStatus.PENDING.value,
            created_at=time.time()
        )
        
        # Notify subscribers
        for queue in self._subscribers[execution_id]:
            await queue.put({"status": "pending"})
        
        # Create async task
        task = asyncio.create_task(
            self._execute(
                execution_id=execution_id,
                model_id=model_id,
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens,
                executor_func=executor_func
            )
        )
        self._running[execution_id] = task
        
        return execution_id
    
    async def _execute(
        self,
        execution_id: str,
        model_id: str,
        prompt: str,
        temperature: float,
        max_tokens: int,
        executor_func: Callable = None
    ):
        """Execute the task."""
        start_time = time.time()
        self._status[execution_id] = TaskStatus.STARTED
        self._tasks[execution_id].status = TaskStatus.STARTED.value
        
        # Notify subscribers
        for queue in self._subscribers[execution_id]:
            await queue.put({"status": "started"})
        
        try:
            # Simulate execution
            if executor_func:
                output = await executor_func(model_id, prompt)
            else:
                # Default mock execution
                await asyncio.sleep(0.5)  # Simulate latency
                output = f"[{model_id}] Response to: {prompt[:50]}..."
            
            latency_ms = (time.time() - start_time) * 1000
            tokens = len(output.split())
            
            self._results[execution_id] = output
            self._tasks[execution_id] = ExecutionResult(
                execution_id=execution_id,
                status=TaskStatus.SUCCESS.value,
                output=output,
                tokens_generated=tokens,
                latency_ms=latency_ms,
                created_at=self._tasks[execution_id].created_at,
                completed_at=time.time()
            )
            self._status[execution_id] = TaskStatus.SUCCESS
            
            # Notify subscribers
            for queue in self._subscribers[execution_id]:
                await queue.put({
                    "status": "success",
                    "output": output,
                    "tokens": tokens,
                    "latency_ms": latency_ms
                })
        
        except Exception as e:
            self._tasks[execution_id] = ExecutionResult(
                execution_id=execution_id,
                status=TaskStatus.FAILURE.value,
                error=str(e),
                created_at=self._tasks[execution_id].created_at,
                completed_at=time.time()
            )
            self._status[execution_id] = TaskStatus.FAILURE
            
            # Notify subscribers
            for queue in self._subscribers[execution_id]:
                await queue.put({"status": "failure", "error": str(e)})
        
        finally:
            self._running.pop(execution_id, None)
    
    def get_status(self, execution_id: str) -> TaskStatus:
        """Get current status of a task."""
        return self._status.get(execution_id, TaskStatus.PENDING)
    
    def get_result(self, execution_id: str) -> Optional[ExecutionResult]:
        """Get result of a completed task."""
        return self._tasks.get(execution_id)
    
    def cancel(self, execution_id: str) -> bool:
        """Cancel a running task."""
        if execution_id in self._running:
            self._running[execution_id].cancel()
            self._status[execution_id] = TaskStatus.REVOKED
            self._tasks[execution_id].status = TaskStatus.REVOKED.value
            return True
        return False
    
    async def subscribe(self, execution_id: str) -> asyncio.Queue:
        """Subscribe to task updates."""
        queue = asyncio.Queue()
        self._subscribers[execution_id].append(queue)
        return queue
    
    def unsubscribe(self, execution_id: str, queue: asyncio.Queue):
        """Unsubscribe from task updates."""
        if queue in self._subscribers[execution_id]:
            self._subscribers[execution_id].remove(queue)


class CeleryQueue:
    """
    Celery-based execution queue for distributed deployment.
    Use this in production with multiple workers.
    """
    
    def __init__(self, broker_url: str = "redis://localhost:6379", result_backend: str = "redis://localhost:6379"):
        if not CELERY_AVAILABLE:
            raise RuntimeError("Celery not installed. Run: pip install celery[redis]")
        
        self.celery = Celery(
            "ritual_zoo",
            broker=broker_url,
            backend=result_backend,
            include=["src.backend.services.celery_tasks"]
        )
        
        # Configure Celery
        self.celery.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
            timezone="UTC",
            enable_utc=True,
            task_track_started=True,
            task_time_limit=600,  # 10 minutes max
            task_soft_time_limit=300,  # 5 minutes soft limit
            result_expires=3600,  # Results expire after 1 hour
            worker_prefetch_multiplier=1,  # Don't prefetch tasks
            task_acks_late=True,  # Acknowledge after completion
            task_reject_on_worker_lost=True,  # Requeue if worker dies
        )
    
    def submit(
        self,
        model_id: str,
        prompt: str,
        user_id: str = None,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        context: List[Dict] = None
    ) -> str:
        """Submit a task to Celery."""
        from .celery_tasks import execute_model_task
        
        execution_id = str(uuid.uuid4())
        
        # Apply task
        task = execute_model_task.apply_async(
            args=[execution_id, model_id, prompt],
            kwargs={
                "temperature": temperature,
                "max_tokens": max_tokens,
                "user_id": user_id,
                "context": context or []
            },
            task_id=execution_id
        )
        
        return execution_id
    
    def get_status(self, execution_id: str) -> TaskStatus:
        """Get task status."""
        result = AsyncResult(execution_id, app=self.celery)
        
        status_map = {
            "PENDING": TaskStatus.PENDING,
            "STARTED": TaskStatus.STARTED,
            "SUCCESS": TaskStatus.SUCCESS,
            "FAILURE": TaskStatus.FAILURE,
            "RETRY": TaskStatus.RETRY,
            "REVOKED": TaskStatus.REVOKED,
        }
        
        return status_map.get(result.state, TaskStatus.PENDING)
    
    def get_result(self, execution_id: str, timeout: float = None) -> Optional[ExecutionResult]:
        """Get task result."""
        result = AsyncResult(execution_id, app=self.celery)
        
        if not result.ready():
            return None
        
        try:
            data = result.get(timeout=timeout)
            return ExecutionResult(
                execution_id=execution_id,
                status=TaskStatus.SUCCESS.value,
                output=data.get("output", ""),
                tokens_generated=data.get("tokens_generated", 0),
                latency_ms=data.get("latency_ms", 0),
                completed_at=data.get("completed_at", time.time())
            )
        except Exception as e:
            return ExecutionResult(
                execution_id=execution_id,
                status=TaskStatus.FAILURE.value,
                error=str(e)
            )
    
    def cancel(self, execution_id: str) -> bool:
        """Cancel a task."""
        result = AsyncResult(execution_id, app=self.celery)
        result.revoke(terminate=True)
        return True


class ExecutionQueue:
    """
    Unified execution queue interface.
    
    Automatically uses:
    - CeleryQueue if broker URL provided and Celery available
    - InMemoryQueue otherwise
    """
    
    def __init__(self, broker_url: str = None):
        self._queue = None
        self._broker_url = broker_url
    
    async def initialize(self):
        """Initialize the queue."""
        if self._broker_url and CELERY_AVAILABLE:
            self._queue = CeleryQueue(broker_url=self._broker_url)
        else:
            self._queue = InMemoryQueue()
    
    async def submit(
        self,
        model_id: str,
        prompt: str,
        user_id: str = None,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        context: List[Dict] = None
    ) -> str:
        """Submit an execution task."""
        if isinstance(self._queue, InMemoryQueue):
            return await self._queue.submit(
                execution_id=str(uuid.uuid4()),
                model_id=model_id,
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )
        else:
            return self._queue.submit(
                model_id=model_id,
                prompt=prompt,
                user_id=user_id,
                temperature=temperature,
                max_tokens=max_tokens,
                context=context
            )
    
    async def get_status(self, execution_id: str) -> TaskStatus:
        """Get task status."""
        return self._queue.get_status(execution_id)
    
    async def get_result(self, execution_id: str, timeout: float = None) -> Optional[ExecutionResult]:
        """Get task result."""
        if isinstance(self._queue, InMemoryQueue):
            return self._queue.get_result(execution_id)
        else:
            return self._queue.get_result(execution_id, timeout)
    
    async def cancel(self, execution_id: str) -> bool:
        """Cancel a task."""
        return self._queue.cancel(execution_id)
    
    async def subscribe(self, execution_id: str) -> asyncio.Queue:
        """Subscribe to task updates."""
        if isinstance(self._queue, InMemoryQueue):
            return await self._queue.subscribe(execution_id)
        return asyncio.Queue()  # Celery doesn't support this


# Factory function
def create_execution_queue(broker_url: str = None) -> ExecutionQueue:
    """Create execution queue."""
    queue = ExecutionQueue(broker_url=broker_url)
    return queue


# Global queue
_queue: Optional[ExecutionQueue] = None


async def get_execution_queue(broker_url: str = None) -> ExecutionQueue:
    """Get global execution queue."""
    global _queue
    if _queue is None:
        _queue = create_execution_queue(broker_url)
        await _queue.initialize()
    return _queue
