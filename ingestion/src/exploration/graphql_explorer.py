"""GraphQL API exploration utilities for data quality analysis and schema exploration."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime
import os
from pathlib import Path
import json
import logging
import random
from pydantic import BaseModel, Field
import pandas as pd
from tabulate import tabulate

from ..utils.graphql_client import GraphQLOAuthClient
from ..utils.data_analysis import (
    analyze_structure,
    profile_data,
    infer_type,
    get_field_stats
)
from ..utils.data_analysis.flattener import DataFlattener, CircularReferenceError
from ..utils.data_analysis.statistical_eda import StatisticalEDA
from .constants import (
    RAW_DATA_FILE,
    FLAT_DATA_FILE,
    DATA_REPORT_FILE,
    REPORT_SECTIONS,
    TIMESTAMP_FORMAT,
    NULL_THRESHOLD_PCT,
    SAMPLE_SIZE,
    MIN_RECORDS_FOR_SAMPLING,
    DEV_MODE,
    MAX_NESTED_DEPTH
)

logger = logging.getLogger(__name__)

class GraphQLError(Exception):
    """Custom exception for GraphQL-specific errors."""
    pass

@dataclass
class ExplorationConfig:
    """Configuration for GraphQL exploration."""
    graphql_url: str
    token_url: str
    client_id: str
    client_secret: str
    scope: Optional[str] = None
    output_dir: str = "exploration_outputs"
    query_name: str = "query"
    max_depth: int = MAX_NESTED_DEPTH
    
    def validate(self) -> None:
        """Validate configuration parameters."""
        if not self.graphql_url:
            raise ValueError("GraphQL URL is required")
        if not self.token_url:
            raise ValueError("Token URL is required")
        if not self.client_id or not self.client_secret:
            raise ValueError("Client credentials are required")
        if self.max_depth < 1:
            raise ValueError("max_depth must be positive")

class GraphQLExplorer:
    """Explorer for GraphQL APIs with data analysis capabilities."""
    
    def __init__(self, config: ExplorationConfig):
        """Initialize the explorer with configuration.
        
        Args:
            config: Configuration for the explorer
            
        Raises:
            ValueError: If configuration is invalid
        """
        config.validate()
        self.config = config
        self.client = GraphQLOAuthClient(
            graphql_url=config.graphql_url,
            token_url=config.token_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            scope=config.scope
        )
        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def flatten_response(self, response: Dict[str, Any], prefix: str = '') -> List[Dict[str, Any]]:
        """Flatten the GraphQL response dynamically."""
        flat_data = []
        
        def flatten(obj: Any, current_prefix: str):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_prefix = f"{current_prefix}{key}." if current_prefix else key
                    flatten(value, new_prefix)
            elif isinstance(obj, list):
                for item in obj:
                    flatten(item, current_prefix)
            else:
                flat_data.append({current_prefix[:-1]: obj})  # Remove trailing dot

        flatten(response, prefix)
        return flat_data

    def dynamic_data_parser(self, response: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Parse athlete data and properties from the response."""
        athletes_data = []
        properties_data = []
        
        # Start with the list of athletes
        organisation_athletes = response.get('data', {}).get('organisationAthletes', {}).get('athletes', [])
        
        for athlete in organisation_athletes:
            athlete_id = athlete.get('id')
            athlete_info = {
                'id': athlete_id,
                'dob': athlete.get('dob'),
                'name': athlete.get('name'),
                'memberships': athlete.get('memberships')
            }
            athletes_data.append(athlete_info)
            
            # Process properties if they exist
            properties = athlete.get('properties')
            if properties:
                for prop_id, prop_info in properties.items():
                    properties_data.append({
                        'member_id': athlete_id,
                        'uuid': prop_info.get('uuid'),
                        'label': prop_info.get('label'),
                        'value': prop_info.get('value')
                    })
        return athletes_data, properties_data

    def _sample_data(self, data: List[Dict[str, Any]], preserve_groups: Optional[str] = None) -> List[Dict[str, Any]]:
        """Sample data if needed based on configuration.
        
        Args:
            data: List of records to potentially sample
            preserve_groups: Optional field to use for stratified sampling
            
        Returns:
            Sampled or original data
        """
        if not SAMPLE_SIZE or len(data) <= MIN_RECORDS_FOR_SAMPLING:
            return data
            
        sample_size = min(SAMPLE_SIZE, len(data))
        
        if preserve_groups and preserve_groups in data[0]:
            # Perform stratified sampling
            groups = {}
            for record in data:
                group = record[preserve_groups]
                groups.setdefault(group, []).append(record)
                
            # Sample from each group proportionally
            sampled = []
            for group, records in groups.items():
                group_size = max(1, int(sample_size * len(records) / len(data)))
                sampled.extend(random.sample(records, min(group_size, len(records))))
            return sampled[:sample_size]
        else:
            return random.sample(data, sample_size)
        
    def _generate_markdown_report(
        self,
        query_name: str,
        paths: List[str],
        flattened_data: List[Dict[str, Any]],
        profile: Dict[str, Any],
        metadata: Dict[str, Any],
        is_sampled: bool,
        performance_metrics: Dict[str, float]
    ) -> str:
        """Generate a markdown report with data analysis results."""
        
        # Convert flattened data to DataFrame and optimize memory
        df = pd.DataFrame(flattened_data)
        df = StatisticalEDA.reduce_mem_usage(df)
        
        # Identify continuous and categorical features using pandas type inference
        continuous_features, categorical_features = StatisticalEDA.identify_feature_types(df)
                
        # Generate statistical summaries
        continuous_summary = StatisticalEDA.generate_continuous_summary(df, continuous_features)
        categorical_summary = StatisticalEDA.generate_categorical_summary(df, categorical_features)
        
        # Start building the report
        report = []
        report.append(f"# Data Analysis Report for {query_name}")
        report.append("")
        
        # Metadata section
        report.append("## Metadata")
        report.append("")
        report.append(f"- Total Records: {metadata['record_count']}")
        if is_sampled:
            report.append(f"- Sample Size: {len(flattened_data)}")
        report.append(f"- Query Time: {performance_metrics.get('query_time', 0):.2f}s")
        report.append(f"- Flatten Time: {performance_metrics.get('flatten_time', 0):.2f}s")
        report.append(f"- Analysis Time: {performance_metrics.get('analysis_time', 0):.2f}s")
        report.append(f"- Total Time: {sum(performance_metrics.values()):.2f}s")
        report.append("")
        
        # Data paths section
        report.append("## Data Paths")
        report.append("")
        for path in paths:
            report.append(f"- {path}")
        report.append("")
        
        # Continuous features section
        if not continuous_features:
            report.append("## Continuous Features")
            report.append("")
            report.append("No continuous features found in the data.")
            report.append("")
        else:
            report.append("## Continuous Features")
            report.append("")
            # Format numbers in continuous summary
            formatted_continuous = continuous_summary.copy()
            numeric_cols = ['missing_percentage', 'minimum', '25th Percentile', 'mean', 
                          'median', '75th Percentile', 'maximum', 'IQR', 'stand_dev']
            for col in numeric_cols:
                formatted_continuous[col] = formatted_continuous[col].apply(lambda x: f"{x:.2f}")
            formatted_continuous['missing_percentage'] = formatted_continuous['missing_percentage'] + '%'
            
            # Generate markdown table
            report.append(formatted_continuous.to_markdown(index=False))
            report.append("")
        
        # Categorical features section
        if not categorical_features:
            report.append("## Categorical Features")
            report.append("")
            report.append("No categorical features found in the data.")
            report.append("")
        else:
            report.append("## Categorical Features")
            report.append("")
            # Format numbers in categorical summary
            formatted_categorical = categorical_summary.copy()
            
            # Handle percentage columns that might be strings
            def format_percentage(x):
                if isinstance(x, (int, float)):
                    return f"{float(x):.1f}%"
                return str(x)
            
            formatted_categorical['missing_percentage'] = formatted_categorical['missing_percentage'].apply(format_percentage)
            formatted_categorical['first_mode_percentage'] = formatted_categorical['first_mode_percentage'].apply(format_percentage)
            formatted_categorical['second_mode_percentage'] = formatted_categorical['second_mode_percentage'].apply(format_percentage)
            
            # Rename columns for better readability
            formatted_categorical = formatted_categorical.rename(columns={
                'first_mode': 'Most Common',
                'first_mode_count': 'Count',
                'first_mode_percentage': 'Percentage',
                'second_mode': 'Second Most',
                'second_mode_count': 'Count 2',
                'second_mode_percentage': 'Percentage 2'
            })
            
            # Generate markdown table
            report.append(formatted_categorical.to_markdown(index=False))
            report.append("")
        
        # Data quality notes
        report.append("## Data Quality Notes")
        report.append("")
        notes = []
        
        for field, metrics in profile.items():
            field_notes = []
            # Ensure metrics is a dictionary
            if isinstance(metrics, dict):
                missing_count = metrics.get('missing_values', 0)
                null_pct = (missing_count / metadata['record_count']) * 100 if missing_count is not None else 0
                
                if null_pct > NULL_THRESHOLD_PCT:
                    field_notes.append(f"High null rate ({null_pct:.1f}%)")
                
                max_length = metrics.get('max_length')
                if max_length is not None and max_length > 1000:
                    field_notes.append(f"Long values (max: {max_length})")
                
                if field_notes:
                    notes.append(f"- {field}: {', '.join(field_notes)}")
            else:
                logger.warning(f"Metrics for {field} is not a dictionary: {metrics}")
        
        if notes:
            report.extend(notes)
        else:
            report.append("No significant data quality issues found.")
        
        return "\n".join(report)
        
    def execute_query(self, query: str, query_name: str) -> Tuple[Dict[str, Any], Path]:
        """Execute a GraphQL query and save the raw response data.
        
        Args:
            query: GraphQL query string
            query_name: Name of the query for file naming
            
        Returns:
            Tuple of (response_data, output_directory)
            
        Raises:
            GraphQLError: If query execution fails
        """
        logger.info(f"Executing query: {query_name}")
        start_time = datetime.now()
        
        try:
            response = self.client.execute_query(query)
        except Exception as e:
            raise GraphQLError(f"Query execution failed: {str(e)}")
            
        if not response:
            raise GraphQLError("Empty response received")
            
        if 'errors' in response:
            raise GraphQLError(f"GraphQL errors: {json.dumps(response['errors'])}")
            
        # Create output directory
        timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
        output_dir = self.output_dir / f"{query_name}_{timestamp}"
        output_dir.mkdir(exist_ok=True)
        
        # Save raw response
        with open(output_dir / RAW_DATA_FILE, "w") as f:
            json.dump(response, f, indent=2)
            
        logger.info(f"Query executed in {(datetime.now() - start_time).total_seconds():.2f}s")
        return response, output_dir
        
    def analyze_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        query_name: str = "query",
        preserve_groups: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze a GraphQL query response.
        
        Args:
            query: GraphQL query string
            variables: Optional query variables
            query_name: Name for the query
            preserve_groups: Optional field to preserve in sampling
            
        Returns:
            Analysis results including output paths
        """
        performance_metrics = {}
        
        try:
            # Execute query and get output directory
            start_time = datetime.now()
            response, output_dir = self.execute_query(query, query_name)
            performance_metrics['query_time'] = (datetime.now() - start_time).total_seconds()
            
            # Parse athletes and properties
            athletes_data, properties_data = self.dynamic_data_parser(response)
            
            # Flatten the data
            start_time = datetime.now()
            flattened_data = self.flatten_response(response)
            performance_metrics['flatten_time'] = (datetime.now() - start_time).total_seconds()
            
            if not flattened_data:
                raise ValueError("No records found after flattening")
                
            # Cleanup column names
            flattened_data = [{k.replace('data.organisationAthletes.', ''): v for k, v in item.items()} for item in flattened_data]
            
            # Sample the flattened data if needed
            is_sampled = False
            if len(flattened_data) > MIN_RECORDS_FOR_SAMPLING and SAMPLE_SIZE:
                flattened_data = self._sample_data(flattened_data, preserve_groups)
                is_sampled = True
                logger.info(f"Analyzing sample of {len(flattened_data)} records")
                
            # Save flattened data
            with open(output_dir / FLAT_DATA_FILE, "w") as f:
                json.dump(flattened_data, f, indent=2)
                
            # Analyze structure and generate profile
            start_time = datetime.now()
            paths = analyze_structure(flattened_data)
            profile = profile_data(flattened_data)
            performance_metrics['analysis_time'] = (datetime.now() - start_time).total_seconds()
            
            # Generate and save markdown report
            report = self._generate_markdown_report(
                query_name=query_name,
                paths=paths,
                flattened_data=flattened_data,
                profile=profile,
                metadata={'record_count': len(flattened_data)},
                is_sampled=is_sampled,
                performance_metrics=performance_metrics
            )
            
            with open(output_dir / DATA_REPORT_FILE, "w") as f:
                f.write(report)
                
            return {
                'success': True,
                'output_dir': str(output_dir),
                'files': {
                    'raw_data': str(output_dir / RAW_DATA_FILE),
                    'flat_data': str(output_dir / FLAT_DATA_FILE),
                    'report': str(output_dir / DATA_REPORT_FILE)
                },
                'metrics': performance_metrics
            }
            
        except (GraphQLError, CircularReferenceError, ValueError) as e:
            logger.error(str(e))
            return {'success': False, 'error': str(e)}
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {'success': False, 'error': str(e)}
