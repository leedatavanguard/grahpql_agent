"""Utilities for exporting data to various formats."""

from typing import Dict, Any
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class DataExporter:
    """Handles exporting data to various formats.
    
    This class provides utilities for exporting data to different formats
    with consistent naming and organization.
    
    Example:
        exporter = DataExporter(output_dir="/path/to/output")
        paths = exporter.export_data(df, "my_data")
    """
    
    def __init__(self, output_dir: Path):
        """Initialize the exporter.
        
        Args:
            output_dir: Base directory for exports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def export_data(self, df: pd.DataFrame, name: str) -> Dict[str, Path]:
        """Export DataFrame to multiple formats.
        
        Args:
            df: DataFrame to export
            name: Base name for export files
            
        Returns:
            Dictionary of format to export path
        """
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        export_info = {}
        
        # CSV export (good for most uses)
        csv_path = self.output_dir / f"{name}_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        export_info['csv'] = csv_path
        
        # JSON export (good for nested structures)
        json_path = self.output_dir / f"{name}_{timestamp}.json"
        df.to_json(json_path, orient='records', indent=2)
        export_info['json'] = json_path
        
        logger.info(f"\nExported data to:")
        for format_name, path in export_info.items():
            logger.info(f"- {format_name}: {path}")
            
        return export_info
