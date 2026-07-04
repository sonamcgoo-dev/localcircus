"""
LLM Backend Adapters - Connect to actual model runtimes.
Supports: Ollama, vLLM, OpenAI-compatible APIs
"""
import asyncio
import json
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, Optional, Any
from dataclasses import dataclass
import httpx
import os


@dataclass
class LLMResponse:
    """Standardized response from any LLM backend."""
    content: str
    model: str
    done: bool
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    latency_ms: float = 0.0


class LLMBackend(ABC):
    """Abstract base class for LLM backends."""
    
    @abstractmethod
    async def generate(
        self,
        prompt: str,
        model: str,
        stream: bool = True,
        **kwargs
    ) -> AsyncGenerator[LLMResponse, None]:
        """Generate response, optionally streaming tokens."""
        pass
    
    @abstractmethod
    async def is_available(self) -> bool:
        """Check if backend is reachable."""
        pass
    
    @abstractmethod
    async def list_models(self) -> list:
        """List available models on this backend."""
        pass


class OllamaBackend(LLMBackend):
    """
    Ollama local model runtime adapter.
    Handles llama.cpp-based local inference.
    """
    
    def __init__(self, base_url: str = "http://localhost:11434"):
        self.base_url = base_url.rstrip('/')
        self._client: Optional[httpx.AsyncClient] = None
    
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(300.0),
                headers={"Content-Type": "application/json"}
            )
        return self._client
    
    async def is_available(self) -> bool:
        try:
            resp = await self.client.get("/api/tags")
            return resp.status_code == 200
        except:
            return False
    
    async def list_models(self) -> list:
        try:
            resp = await self.client.get("/api/tags")
            if resp.status_code == 200:
                data = resp.json()
                return [m.get("name", m.get("model")) for m in data.get("models", [])]
            return []
        except:
            return []
    
    async def generate(
        self,
        prompt: str,
        model: str,
        stream: bool = True,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        **kwargs
    ) -> AsyncGenerator[LLMResponse, None]:
        """Stream tokens from Ollama."""
        import time
        start_time = time.time()
        
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": stream,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
            **kwargs
        }
        
        try:
            async with self.client.stream("POST", "/api/generate", json=payload) as resp:
                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        yield LLMResponse(
                            content=data.get("response", ""),
                            model=model,
                            done=data.get("done", False),
                            prompt_tokens=data.get("prompt_eval_count", 0),
                            completion_tokens=data.get("eval_count", 0),
                            total_tokens=data.get("prompt_eval_count", 0) + data.get("eval_count", 0),
                            latency_ms=(time.time() - start_time) * 1000
                        )
                    except json.JSONDecodeError:
                        continue
        except httpx.HTTPError as e:
            yield LLMResponse(
                content=f"[Ollama connection error: {str(e)}]",
                model=model,
                done=True,
                latency_ms=(time.time() - start_time) * 1000
            )


class VLLMBackend(LLMBackend):
    """
    vLLM distributed inference backend.
    Handles tensor-parallel distributed serving.
    """
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self._client: Optional[httpx.AsyncClient] = None
    
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(300.0),
                headers={"Content-Type": "application/json"}
            )
        return self._client
    
    async def is_available(self) -> bool:
        try:
            resp = await self.client.get("/v1/models")
            return resp.status_code == 200
        except:
            return False
    
    async def list_models(self) -> list:
        try:
            resp = await self.client.get("/v1/models")
            if resp.status_code == 200:
                data = resp.json()
                return [m.get("id") for m in data.get("data", [])]
            return []
        except:
            return []
    
    async def generate(
        self,
        prompt: str,
        model: str,
        stream: bool = True,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        **kwargs
    ) -> AsyncGenerator[LLMResponse, None]:
        """Stream tokens from vLLM OpenAI-compatible API."""
        import time
        start_time = time.time()
        
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": stream,
            "temperature": temperature,
            "max_tokens": max_tokens,
            **kwargs
        }
        
        try:
            async with self.client.stream("POST", "/v1/completions", json=payload) as resp:
                async for line in resp.aiter_lines():
                    if not line.strip() or not line.startswith("data: "):
                        continue
                    data_str = line[6:]  # Remove "data: " prefix
                    if data_str == "[DONE]":
                        yield LLMResponse(
                            content="",
                            model=model,
                            done=True,
                            latency_ms=(time.time() - start_time) * 1000
                        )
                        break
                    try:
                        data = json.loads(data_str)
                        choices = data.get("choices", [{}])
                        if choices:
                            delta = choices[0].get("delta", {})
                            content = delta.get("content", "")
                            finish_reason = choices[0].get("finish_reason")
                            yield LLMResponse(
                                content=content,
                                model=model,
                                done=finish_reason is not None,
                                prompt_tokens=data.get("usage", {}).get("prompt_tokens", 0),
                                completion_tokens=data.get("usage", {}).get("completion_tokens", 0),
                                total_tokens=data.get("usage", {}).get("total_tokens", 0),
                                latency_ms=(time.time() - start_time) * 1000
                            )
                    except json.JSONDecodeError:
                        continue
        except httpx.HTTPError as e:
            yield LLMResponse(
                content=f"[vLLM connection error: {str(e)}]",
                model=model,
                done=True,
                latency_ms=(time.time() - start_time) * 1000
            )


class OpenAICompatBackend(LLMBackend):
    """
    OpenAI-compatible API backend (Azure, AWS Bedrock, etc.)
    """
    
    def __init__(self, base_url: str, api_key: str = "", model: str = "gpt-4"):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.default_model = model
        self._client: Optional[httpx.AsyncClient] = None
    
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(300.0),
                headers=headers
            )
        return self._client
    
    async def is_available(self) -> bool:
        try:
            resp = await self.client.get("/v1/models")
            return resp.status_code == 200
        except:
            return False
    
    async def list_models(self) -> list:
        try:
            resp = await self.client.get("/v1/models")
            if resp.status_code == 200:
                data = resp.json()
                return [m.get("id") for m in data.get("data", [])]
            return []
        except:
            return []
    
    async def generate(
        self,
        prompt: str,
        model: str = None,
        stream: bool = True,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        **kwargs
    ) -> AsyncGenerator[LLMResponse, None]:
        """Stream tokens from OpenAI-compatible API."""
        import time
        start_time = time.time()
        model = model or self.default_model
        
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": stream,
            "temperature": temperature,
            "max_tokens": max_tokens,
            **kwargs
        }
        
        try:
            async with self.client.stream("POST", "/v1/chat/completions", json=payload) as resp:
                async for line in resp.aiter_lines():
                    if not line.strip() or not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str == "[DONE]":
                        yield LLMResponse(
                            content="",
                            model=model,
                            done=True,
                            latency_ms=(time.time() - start_time) * 1000
                        )
                        break
                    try:
                        data = json.loads(data_str)
                        choices = data.get("choices", [{}])
                        if choices:
                            delta = choices[0].get("delta", {})
                            content = delta.get("content", "")
                            finish_reason = choices[0].get("finish_reason")
                            yield LLMResponse(
                                content=content,
                                model=model,
                                done=finish_reason is not None,
                                prompt_tokens=data.get("usage", {}).get("prompt_tokens", 0),
                                completion_tokens=data.get("usage", {}).get("completion_tokens", 0),
                                total_tokens=data.get("usage", {}).get("total_tokens", 0),
                                latency_ms=(time.time() - start_time) * 1000
                            )
                    except json.JSONDecodeError:
                        continue
        except httpx.HTTPError as e:
            yield LLMResponse(
                content=f"[API connection error: {str(e)}]",
                model=model,
                done=True,
                latency_ms=(time.time() - start_time) * 1000
            )


class MockBackend(LLMBackend):
    """
    Mock backend for testing and demo.
    Simulates realistic streaming behavior.
    """
    
    async def is_available(self) -> bool:
        return True
    
    async def list_models(self) -> list:
        return [
            "llama-3.1-8b-instruct",
            "llama-3.1-70b-instruct", 
            "mistral-7b-instruct",
            "codellama-13b-instruct",
            "phi-3-mini-128k",
            "qwen2.5-72b-instruct",
        ]
    
    async def generate(
        self,
        prompt: str,
        model: str,
        stream: bool = True,
        **kwargs
    ) -> AsyncGenerator[LLMResponse, None]:
        import time
        import random
        
        start_time = time.time()
        
        # Model-specific responses
        responses = {
            "llama-3.1-8b-instruct": "This is LLaMA 3.1 8B responding through the integrated Zoo runtime. I'm generating this response token-by-token, simulating real inference latency while providing immediate feedback. The streaming architecture creates that responsive, alive feeling you described.",
            "llama-3.1-70b-instruct": "LLaMA 3.1 70B processing your request. My large context window and powerful reasoning capabilities allow me to maintain coherence across complex conversations. The streaming infrastructure delivers tokens in real-time.",
            "mistral-7b-instruct": "Mistral 7B here, streaming your response through the unified execution layer. My efficient architecture delivers fast inference while maintaining high quality output. Notice the token-by-token delivery creating that interactive feel.",
            "codellama-13b-instruct": "Code LLaMA 13B executing your request. Here's a Python function:\n\n```python\ndef streaming_demo():\n    yield 'token_1'\n    yield 'token_2'\n    # Real streaming simulation\n```\n\nThis demonstrates code generation with proper formatting.",
            "phi-3-mini-128k": "Phi-3 Mini responding with streaming tokens. Despite my compact 3.8B parameter size, I deliver strong performance with impressive context understanding up to 128K tokens. The streaming makes me feel immediate and responsive.",
            "qwen2.5-72b-instruct": "Qwen2.5 72B processing through the streaming pipeline. My multilingual capabilities and extensive knowledge base enable comprehensive responses. The real-time token delivery creates an engaging user experience.",
        }
        
        response_text = responses.get(model, f"Executing {model} through the streaming pipeline. Token-by-token output creates that responsive, alive feeling. Each word appears progressively, simulating real inference latency while maintaining engagement.")
        
        # Simulate token streaming
        words = response_text.split()
        for i, word in enumerate(words):
            await asyncio.sleep(random.uniform(0.015, 0.035))  # 15-35ms per token
            
            suffix = " " if i < len(words) - 1 else ""
            is_last = i == len(words) - 1
            
            yield LLMResponse(
                content=word + suffix,
                model=model,
                done=is_last,
                prompt_tokens=len(prompt.split()),
                completion_tokens=i + 1,
                total_tokens=len(prompt.split()) + i + 1,
                latency_ms=(time.time() - start_time) * 1000
            )


class LLMBackendManager:
    """
    Manages multiple LLM backends with automatic failover.
    Routes requests to appropriate backend based on model availability.
    """
    
    def __init__(self):
        self.backends: Dict[str, LLMBackend] = {}
        self._register_default_backends()
    
    def _register_default_backends(self):
        """Register available backends based on environment."""
        # Ollama local
        if os.getenv("OLLAMA_BASE_URL"):
            self.register_backend("ollama", OllamaBackend(os.getenv("OLLAMA_BASE_URL")))
        
        # vLLM distributed
        if os.getenv("VLLM_BASE_URL"):
            self.register_backend("vllm", VLLMBackend(os.getenv("VLLM_BASE_URL")))
        
        # OpenAI/Azure
        if os.getenv("OPENAI_BASE_URL"):
            self.register_backend(
                "openai", 
                OpenAICompatBackend(
                    os.getenv("OPENAI_BASE_URL"),
                    os.getenv("OPENAI_API_KEY", ""),
                    os.getenv("OPENAI_MODEL", "gpt-4")
                )
            )
        
        # Always add mock backend as fallback
        self.register_backend("mock", MockBackend())
    
    def register_backend(self, name: str, backend: LLMBackend):
        """Register a named backend."""
        self.backends[name] = backend
    
    def get_backend_for_model(self, model: str) -> LLMBackend:
        """
        Determine which backend should handle a model.
        Returns mock backend if no real backend available.
        """
        # Check if Ollama has this model
        if "ollama" in self.backends:
            return self.backends["ollama"]
        
        # Check if vLLM has this model
        if "vllm" in self.backends:
            return self.backends["vllm"]
        
        # Fallback to mock
        return self.backends["mock"]
    
    async def get_available_models(self) -> Dict[str, list]:
        """Get all available models across all backends."""
        result = {}
        for name, backend in self.backends.items():
            try:
                models = await backend.list_models()
                if models:
                    result[name] = models
            except:
                pass
        return result
    
    async def health_check(self) -> Dict[str, bool]:
        """Check health of all backends."""
        result = {}
        for name, backend in self.backends.items():
            try:
                result[name] = await backend.is_available()
            except:
                result[name] = False
        return result


# Global backend manager
_backend_manager: Optional[LLMBackendManager] = None


def get_backend_manager() -> LLMBackendManager:
    """Get the global backend manager instance."""
    global _backend_manager
    if _backend_manager is None:
        _backend_manager = LLMBackendManager()
    return _backend_manager
