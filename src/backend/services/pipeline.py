"""
Multi-Model Pipeline Orchestration - Chain multiple models together.
Enables complex workflows like code generation → review → testing.
"""
import asyncio
import time
import uuid
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod


class PipelineStepType(Enum):
    """Types of pipeline steps."""
    GENERATE = "generate"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    CONDITIONAL = "conditional"


@dataclass
class PipelineStep:
    """A single step in a pipeline."""
    name: str
    model_id: str
    step_type: PipelineStepType = PipelineStepType.GENERATE
    prompt_template: str = "{input}"
    condition: str = None  # For CONDITIONAL type: Jinja-like condition
    timeout_seconds: float = 60.0
    retry_count: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def format_prompt(self, context: Dict[str, Any]) -> str:
        """Format prompt template with context."""
        prompt = self.prompt_template
        
        # Simple variable substitution
        for key, value in context.items():
            prompt = prompt.replace(f"{{{key}}}", str(value))
        
        return prompt


@dataclass
class PipelineResult:
    """Result from a pipeline execution."""
    pipeline_id: str
    status: str  # success, failed, cancelled
    steps_completed: int
    total_steps: int
    outputs: Dict[str, str]  # step_name -> output
    final_output: str
    total_latency_ms: float
    step_latencies: Dict[str, float]
    error: str = None
    created_at: float = 0.0
    completed_at: float = 0.0


@dataclass  
class Pipeline:
    """A pipeline of model steps."""
    id: str
    name: str
    description: str
    steps: List[PipelineStep]
    tags: List[str] = field(default_factory=list)
    
    @classmethod
    def create(cls, name: str, description: str = "") -> "PipelineBuilder":
        """Start building a new pipeline."""
        return PipelineBuilder(name, description)


class PipelineBuilder:
    """Builder for constructing pipelines."""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._steps: List[PipelineStep] = []
    
    def add_step(
        self,
        name: str,
        model_id: str,
        prompt_template: str = "{input}",
        step_type: PipelineStepType = PipelineStepType.GENERATE
    ) -> "PipelineBuilder":
        """Add a generation step."""
        self._steps.append(PipelineStep(
            name=name,
            model_id=model_id,
            prompt_template=prompt_template,
            step_type=step_type
        ))
        return self
    
    def add_conditional(
        self,
        name: str,
        condition: str,
        model_id: str,
        prompt_template: str = "{input}"
    ) -> "PipelineBuilder":
        """Add a conditional step."""
        self._steps.append(PipelineStep(
            name=name,
            model_id=model_id,
            prompt_template=prompt_template,
            step_type=PipelineStepType.CONDITIONAL,
            condition=condition
        ))
        return self
    
    def build(self) -> Pipeline:
        """Build the pipeline."""
        pipeline_id = str(uuid.uuid4())[:8]
        return Pipeline(
            id=pipeline_id,
            name=self.name,
            description=self.description,
            steps=self._steps
        )


class PipelineExecutor:
    """
    Executes pipelines with proper context passing between steps.
    """
    
    def __init__(self, backend_manager=None, metrics=None):
        self.backend_manager = backend_manager
        self.metrics = metrics
    
    async def execute(
        self,
        pipeline: Pipeline,
        initial_input: str,
        context: Dict[str, Any] = None,
        cancel_event: asyncio.Event = None
    ) -> PipelineResult:
        """
        Execute a pipeline with the given input.
        
        Args:
            pipeline: The pipeline to execute
            initial_input: Initial user input
            context: Additional context to pass through
            cancel_event: Event to check for cancellation
            
        Returns:
            PipelineResult with all outputs
        """
        start_time = time.time()
        cancel_event = cancel_event or asyncio.Event()
        
        # Initialize context
        ctx = context.copy() if context else {}
        ctx["input"] = initial_input
        outputs = {}
        step_latencies = {}
        
        for i, step in enumerate(pipeline.steps):
            step_start = time.time()
            
            # Check for cancellation
            if cancel_event.is_set():
                return PipelineResult(
                    pipeline_id=pipeline.id,
                    status="cancelled",
                    steps_completed=i,
                    total_steps=len(pipeline.steps),
                    outputs=outputs,
                    final_output=outputs.get(pipeline.steps[i-1].name, "") if i > 0 else "",
                    total_latency_ms=(time.time() - start_time) * 1000,
                    step_latencies=step_latencies,
                    completed_at=time.time()
                )
            
            try:
                # Format prompt
                prompt = step.format_prompt(ctx)
                
                # Execute step
                output = await self._execute_step(step, prompt, cancel_event)
                
                # Store output
                outputs[step.name] = output
                ctx[step.name] = output
                ctx["previous_output"] = output  # Alias for convenience
                
                # Record latency
                step_latencies[step.name] = (time.time() - step_start) * 1000
                
            except asyncio.CancelledError:
                return PipelineResult(
                    pipeline_id=pipeline.id,
                    status="cancelled",
                    steps_completed=i,
                    total_steps=len(pipeline.steps),
                    outputs=outputs,
                    final_output=outputs.get(pipeline.steps[i-1].name, "") if i > 0 else "",
                    total_latency_ms=(time.time() - start_time) * 1000,
                    step_latencies=step_latencies,
                    completed_at=time.time()
                )
                
            except Exception as e:
                return PipelineResult(
                    pipeline_id=pipeline.id,
                    status="failed",
                    steps_completed=i,
                    total_steps=len(pipeline.steps),
                    outputs=outputs,
                    final_output="",
                    total_latency_ms=(time.time() - start_time) * 1000,
                    step_latencies=step_latencies,
                    error=f"Step {step.name} failed: {str(e)}",
                    completed_at=time.time()
                )
        
        return PipelineResult(
            pipeline_id=pipeline.id,
            status="success",
            steps_completed=len(pipeline.steps),
            total_steps=len(pipeline.steps),
            outputs=outputs,
            final_output=outputs.get(pipeline.steps[-1].name, ""),
            total_latency_ms=(time.time() - start_time) * 1000,
            step_latencies=step_latencies,
            completed_at=time.time()
        )
    
    async def _execute_step(
        self,
        step: PipelineStep,
        prompt: str,
        cancel_event: asyncio.Event
    ) -> str:
        """Execute a single step."""
        if self.backend_manager:
            backend = self.backend_manager.get_backend_for_model(step.model_id)
            
            output_parts = []
            async for response in backend.generate(
                prompt=prompt,
                model=step.model_id,
                stream=False
            ):
                if cancel_event.is_set():
                    break
                output_parts.append(response.content)
            
            return "".join(output_parts)
        else:
            # Mock execution
            await asyncio.sleep(0.2)
            return f"[{step.name}] Response to: {prompt[:50]}..."


# ==================== Pre-built Pipelines ====================

class StandardPipelines:
    """Common pipeline templates."""
    
    @staticmethod
    def code_review() -> Pipeline:
        """Code generation → review → test generation pipeline."""
        return Pipeline.create(
            name="Code Review Pipeline",
            description="Generate code, review it, then generate tests"
        ).add_step(
            name="generate",
            model_id="llama-3.1-8b-instruct",
            prompt_template="Write {language} code for: {task}"
        ).add_step(
            name="review",
            model_id="codellama-13b-instruct",
            prompt_template="Review this code for bugs and improvements:\n{generate}"
        ).add_step(
            name="test",
            model_id="llama-3.1-8b-instruct",
            prompt_template="Write unit tests for this code:\n{generate}"
        ).build()
    
    @staticmethod
    def analysis() -> Pipeline:
        """Data analysis with multiple models."""
        return Pipeline.create(
            name="Analysis Pipeline",
            description="Analyze data with multiple specialized models"
        ).add_step(
            name="extract",
            model_id="llama-3.1-8b-instruct",
            prompt_template="Extract key insights from: {input}"
        ).add_step(
            name="analyze",
            model_id="qwen2.5-72b-instruct",
            prompt_template="Deep analysis of these insights:\n{extract}"
        ).add_step(
            name="summarize",
            model_id="mistral-7b-instruct",
            prompt_template="Summarize this analysis concisely:\n{analyze}"
        ).build()
    
    @staticmethod
    def creative_writing() -> Pipeline:
        """Creative writing with multiple passes."""
        return Pipeline.create(
            name="Creative Writing Pipeline",
            description="Brainstorm → Draft → Edit"
        ).add_step(
            name="brainstorm",
            model_id="llama-3.1-70b-instruct",
            prompt_template="Brainstorm ideas for: {input}"
        ).add_step(
            name="draft",
            model_id="llama-3.1-8b-instruct",
            prompt_template="Write a creative piece based on:\n{brainstorm}"
        ).add_step(
            name="edit",
            model_id="mistral-7b-instruct",
            prompt_template="Edit and polish this text:\n{draft}"
        ).build()


# ==================== Pipeline Registry ====================

class PipelineRegistry:
    """Registry of available pipelines."""
    
    def __init__(self):
        self._pipelines: Dict[str, Pipeline] = {}
        self._init_defaults()
    
    def _init_defaults(self):
        """Register default pipelines."""
        self.register("code_review", StandardPipelines.code_review())
        self.register("analysis", StandardPipelines.analysis())
        self.register("creative_writing", StandardPipelines.creative_writing())
    
    def register(self, name: str, pipeline: Pipeline):
        """Register a pipeline."""
        self._pipelines[name] = pipeline
    
    def get(self, name: str) -> Optional[Pipeline]:
        """Get a pipeline by name."""
        return self._pipelines.get(name)
    
    def list_pipelines(self) -> List[Dict[str, Any]]:
        """List all available pipelines."""
        return [
            {
                "name": p.name,
                "description": p.description,
                "steps": len(p.steps),
                "tags": p.tags,
                "models": [s.model_id for s in p.steps]
            }
            for p in self._pipelines.values()
        ]


# Global registry
_registry: Optional[PipelineRegistry] = None


def get_pipeline_registry() -> PipelineRegistry:
    """Get the global pipeline registry."""
    global _registry
    if _registry is None:
        _registry = PipelineRegistry()
    return _registry
