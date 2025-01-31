#!/usr/bin/env python3
"""Script to test ingestion handlers locally."""

import os
import json
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from ingestion.runner import get_handler_class

async def main():
    # Load environment variables
    load_dotenv('.env.liveheats')
    
    # Load test config
    with open('tests/config/test_config.json', 'r') as f:
        config = json.load(f)
    
    # Set up test environment
    test_data_dir = Path('test_data')
    test_data_dir.mkdir(exist_ok=True)
    
    # Create handler environment
    handler_config = config['ingestion_handlers']['liveheats_orgs']
    env_config = {
        'TARGET_PLATFORM': 'dev',
        'AWS_REGION': 'ap-southeast-2',
        'HANDLER_TYPE': handler_config['type'],
        'HANDLER_MODULE': handler_config['handler_module']
    }
    env_config.update(handler_config.get('environment', {}))
    
    # Initialize handler
    handler_class = get_handler_class(handler_config['type'])
    handler = handler_class(env_config)
    
    # Run handler
    await handler.run()

if __name__ == '__main__':
    asyncio.run(main())
