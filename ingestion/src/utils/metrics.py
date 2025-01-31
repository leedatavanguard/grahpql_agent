import time
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskMetrics:
    """Collects and manages metrics for ingestion tasks."""
    
    def __init__(self, task_id: str):
        """Initialize task metrics."""
        self.task_id = task_id
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.status = None
        self.error = None
        self.records_processed = 0
        self.bytes_processed = 0
        self.additional_metrics: Dict[str, Any] = {}

    def start(self):
        """Start tracking task metrics."""
        self.start_time = datetime.now(timezone.utc)
        self.status = "running"

    def stop(self, success: bool = True, error: Optional[str] = None):
        """Stop tracking task metrics."""
        self.end_time = datetime.now(timezone.utc)
        self.duration = (self.end_time - self.start_time).total_seconds()
        self.status = "success" if success else "failed"
        self.error = error

    def add_metric(self, name: str, value: Any):
        """Add a custom metric."""
        self.additional_metrics[name] = value

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary format."""
        return {
            "task_id": self.task_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "status": self.status,
            "error": self.error,
            "records_processed": self.records_processed,
            "bytes_processed": self.bytes_processed,
            **self.additional_metrics
        }

class MetricsCollector:
    """Collects and manages metrics for ingestion handlers."""
    
    def __init__(self):
        """Initialize the metrics collector."""
        self.metrics: Dict[str, Dict[str, Any]] = {}
        
    def record_metric(self, name: str, value: float, tags: Optional[Dict[str, str]] = None):
        """
        Record a metric value.
        
        Args:
            name: Name of the metric
            value: Value to record
            tags: Optional tags to associate with the metric
        """
        timestamp = datetime.now(timezone.utc)
        
        if name not in self.metrics:
            self.metrics[name] = {
                'values': [],
                'tags': tags or {}
            }
            
        self.metrics[name]['values'].append({
            'value': value,
            'timestamp': timestamp.isoformat()
        })
        
    @contextmanager
    def measure_time(self, name: str, tags: Optional[Dict[str, str]] = None):
        """
        Context manager to measure execution time of a block of code.
        
        Args:
            name: Name of the metric
            tags: Optional tags to associate with the metric
        """
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.record_metric(
                f"{name}_duration_seconds",
                duration,
                tags
            )
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return self.metrics
        
    def clear_metrics(self):
        """Clear all collected metrics."""
        self.metrics = {}
