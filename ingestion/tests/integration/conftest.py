import os
import pytest
from pathlib import Path
from dotenv import load_dotenv

def pytest_configure(config):
    """Configure pytest."""
    # Load environment variables from the platform config
    platform_config_dir = Path(__file__).parent.parent.parent.parent / '.platform_config'
    target_platform = os.getenv('TARGET_PLATFORM', 'dev_platform')
    env_file = platform_config_dir / target_platform / '.env'
    
    if env_file.exists():
        load_dotenv(env_file)
    else:
        pytest.skip(f"Environment file not found: {env_file}", allow_module_level=True)
        
@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture to provide test data directory."""
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)
    return data_dir
