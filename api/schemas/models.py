from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional

DatasetType = Literal["hotel", "property", "hris"]
Severity = Literal["low", "medium", "high"]


class RunRequest(BaseModel):
    dataset_type: DatasetType
    file_id: str
    options: Dict[str, Any] = Field(default_factory=dict)


class Issue(BaseModel):
    # Unique short code for the type of problem
    code: str
    severity: Severity
    message: str
    count: Optional[int] = None
    sample_rows: List[Dict[str, Any]] = Field(default_factory=list)


class Anomaly(BaseModel):
    metric: str
    bucket: str                 # e.g., "2026-03" or "2024-01"
    value: float
    baseline: Optional[float] = None
    severity: Severity
    explanation: Optional[str] = None


class MetricsSummary(BaseModel):
    row_count: int
    column_count: int
    key_metrics: Dict[str, float] = Field(default_factory=dict)


class RunResult(BaseModel):
    dataset_type: DatasetType
    file_id: str
    metrics: MetricsSummary
    issues: List[Issue] = Field(default_factory=list)
    anomalies: List[Anomaly] = Field(default_factory=list)