from typing import Dict, Any, Callable, Optional
from dataclasses import dataclass

@dataclass
class MasterLoadDataBody:
    fileName: str
    loadMode: str = "INSERT"
    data: Any


@dataclass
class WorkflowFilterBody:
    filePath: str
    fileName: str
    fileExtension: str
    project: str
    source: str

@dataclass
class WorkflowSessionStartBody:
    workflowId: str
    celeryId: str
    filePath: str

@dataclass
class WorkflowStepStartBody:
    sessionId: str
    stepId: str

