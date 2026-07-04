# RITUAL Marketplace - Expansion Roadmap

## Quick Wins (1-2 weeks)

### 1. Real Backend Integration

```python
# Connect to actual Ollama
export OLLAMA_BASE_URL=http://localhost:11434

# The adapter already exists in llm_backends.py
# Just needs real model loading
```

**Next steps:**
- Install Ollama: `curl -fsSL https://ollama.ai/install.sh | sh`
- Pull models: `ollama pull llama3.1:8b`
- Set env var and restart

### 2. Conversation Memory

Add persistent chat history:

```python
# src/backend/services/conversation_store.py
class ConversationStore:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = aioredis.from_url(redis_url)
    
    async def save_message(self, session_id: str, role: str, content: str):
        key = f"conversation:{session_id}"
        await self.redis.rpush(key, json.dumps({"role": role, "content": content}))
    
    async def get_history(self, session_id: str, limit: int = 50) -> List[dict]:
        key = f"conversation:{session_id}"
        messages = await self.redis.lrange(key, -limit, -1)
        return [json.loads(m) for m in messages]
```

### 3. Streaming UI Improvements

- Markdown rendering in chat (use `marked.js`)
- Syntax highlighting for code (use `highlight.js`)
- Token speed indicator (tokens/sec)
- Estimated time remaining

## Medium Term (1-2 months)

### 4. Persistence Layer

```sql
-- PostgreSQL schema
CREATE TABLE users (
    id UUID PRIMARY KEY,
    username VARCHAR(255) UNIQUE,
    tier VARCHAR(50),
    created_at TIMESTAMP
);

CREATE TABLE executions (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    model_id VARCHAR(255),
    input_tokens INT,
    output_tokens INT,
    latency_ms INT,
    created_at TIMESTAMP
);

CREATE TABLE conversations (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    messages JSONB,
    created_at TIMESTAMP
);
```

### 5. Authentication

```python
# src/backend/services/auth.py
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt

security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")

@router.post("/zoo/run")
async def run_model(request: RunRequest, user: dict = Depends(verify_token)):
    # Now you know WHO is running the model
    return {"user_id": user["sub"], ...}
```

### 6. Distributed Caching (Redis)

```python
# src/backend/services/distributed_pool.py
class DistributedModelPool:
    def __init__(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url)
        self.local_pool = ModelPool()
    
    async def is_cached(self, model_id: str) -> bool:
        # Check global cache first
        cached = await self.redis.sismember("cached_models", model_id)
        return cached or self.local_pool.is_cached(model_id)
    
    async def prewarm_global(self, model_ids: List[str]):
        # Broadcast prewarm to all workers
        await self.redis.publish("prewarm", json.dumps(model_ids))
```

### 7. Queue-Based Execution

```python
# src/backend/services/execution_queue.py
from celery import Celery

celery_app = Celery("zoo", broker="redis://localhost:6379")

@celery_app.task
def execute_model_task(model_id: str, prompt: str, session_id: str):
    # Run in worker process
    result = run_sync(model_id, prompt)
    # Store result
    redis.set(f"result:{session_id}", json.dumps(result))
    return result

@router.post("/zoo/run")
async def run_model(request: RunRequest):
    task = execute_model_task.delay(request.model_id, request.input_data, session_id)
    return {"task_id": task.id, "status": "queued"}

@router.get("/zoo/result/{task_id}")
async def get_result(task_id: str):
    result = AsyncResult(task_id)
    if result.ready():
        return result.get()
    return {"status": "processing"}
```

## Long Term (3-6 months)

### 8. Kubernetes Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ritual-marketplace
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ritual-marketplace
  template:
    spec:
      containers:
      - name: api
        image: ritual-marketplace:latest
        ports:
        - containerPort: 8000
        env:
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: ritual-secrets
              key: redis-url
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
      - name: ollama
        image: ollama/ollama:latest
        ports:
        - containerPort: 11434
        resources:
          limits:
            gpu: "1"
            memory: "16Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: ritual-marketplace
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 8000
  selector:
    app: ritual-marketplace
```

### 9. Observability Stack

```python
# src/backend/services/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Metrics
EXECUTION_COUNT = Counter("zoo_executions_total", "Total executions", ["model_id"])
EXECUTION_LATENCY = Histogram("zoo_execution_seconds", "Execution latency", ["model_id"])
ACTIVE_MODELS = Gauge("zoo_active_models", "Currently loaded models")
CACHE_HITS = Counter("zoo_cache_hits_total", "Cache hits")
CACHE_MISSES = Counter("zoo_cache_misses_total", "Cache misses")

@router.get("/metrics")
async def metrics():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
```

### 10. Multi-Model Pipelines

```python
# src/backend/services/pipeline.py
class ModelPipeline:
    def __init__(self, steps: List[PipelineStep]):
        self.steps = steps
    
    async def execute(self, input_data: str) -> str:
        context = {"input": input_data}
        
        for step in self.steps:
            result = await step.execute(context)
            context[step.name] = result
        
        return context[self.steps[-1].name]

# Example: Code Review Pipeline
pipeline = ModelPipeline([
    PipelineStep("llm-3.1-8b", "Generate code"),
    PipelineStep("codellama-13b", "Review code"),  
    PipelineStep("mistral-7b", "Write tests"),
])
```

## Feature Priorities Matrix

| Feature | Impact | Effort | Priority |
|---------|--------|--------|----------|
| Real Ollama connection | High | Low | P0 |
| Conversation memory | High | Low | P0 |
| User auth (JWT) | High | Medium | P1 |
| Redis caching | Medium | Medium | P1 |
| Prometheus metrics | Medium | Low | P1 |
| Queue execution | High | High | P2 |
| Kubernetes deploy | Medium | High | P2 |
| Multi-model pipeline | High | High | P3 |

## Quick Expansion Commands

```bash
# Add Redis for caching
docker run -d -p 6379:6379 redis:alpine

# Add PostgreSQL for persistence  
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=secret postgres:15

# Add Prometheus
docker run -d -p 9090:9090 prom/prometheus

# Install Ollama and pull a model
curl -fsSL https://ollama.ai/install.sh | sh
ollama pull llama3.1:8b

# Run with Redis
REDIS_URL=redis://localhost:6379 python run_server.py
```

## Next Action Items

1. **Today**: Set `OLLAMA_BASE_URL` and test real inference
2. **This week**: Add Redis, enable conversation memory
3. **Next month**: Add JWT auth, PostgreSQL persistence
4. **Next quarter**: Kubernetes deployment, Prometheus metrics
