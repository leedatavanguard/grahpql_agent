"""Logging utilities for consistent and beautiful console output."""

import logging
import json
from typing import Optional
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table
from rich.json import JSON
from rich.syntax import Syntax
from rich import print as rprint

console = Console()

def setup_logging(level: int = logging.INFO) -> None:
    """Set up logging with rich formatting.
    
    Args:
        level: Logging level to use
    """
    logging.basicConfig(
        level=level,
        handlers=[RichHandler(show_time=False, show_path=False, omit_repeated_times=False)]
    )

def print_json(data: dict, title: Optional[str] = None) -> None:
    """Print JSON data with syntax highlighting.
    
    Args:
        data: JSON data to print
        title: Optional title to display above the JSON
    """
    if title:
        console.print(f"\n[bold blue]{title}[/bold blue]")
    console.print(Syntax(json.dumps(data, indent=2), "json", theme="monokai"))
    
def print_table(data: list, columns: list, title: Optional[str] = None) -> None:
    """Print data in a formatted table.
    
    Args:
        data: List of dictionaries containing the data
        columns: List of column names to include
        title: Optional title to display above the table
    """
    table = Table(show_header=True, header_style="bold magenta")
    
    # Add columns
    for col in columns:
        table.add_column(col)
        
    # Add rows (limit to first 5)
    for row in data[:5]:
        table.add_row(*[str(row.get(col, '')) for col in columns])
        
    if title:
        console.print(f"\n[bold blue]{title}[/bold blue]")
    console.print(table)
    
    if len(data) > 5:
        console.print(f"\n[dim]... {len(data) - 5} more rows not shown[/dim]")
        
def print_query(query: str, title: Optional[str] = None) -> None:
    """Print a GraphQL query with syntax highlighting.
    
    Args:
        query: GraphQL query string
        title: Optional title to display above the query
    """
    if title:
        console.print(f"\n[bold blue]{title}[/bold blue]")
    syntax = Syntax(query, "graphql", theme="monokai", line_numbers=True)
    console.print(syntax)
