import pytest
from pathlib import Path
from ingestion.utils.graphql_client import GraphQLQueryLoader

@pytest.fixture
def sample_query_config(tmp_path):
    """Create a sample query config file for testing."""
    config_content = """
    organisations:
      query: |
        query getOrganisation($id: ID!) {
          organisation(id: $id) {
            id
            name
            sportType
          }
        }
      variables:
        id: ${ORG_ID}
        
    members:
      query: |
        query getMembers($id: ID!, $page: Int!) {
          members(orgId: $id, page: $page) {
            id
            name
          }
        }
      variables:
        id: ${ORG_ID}
        page: 1
    """
    
    config_file = tmp_path / "test_queries.yaml"
    config_file.write_text(config_content)
    return str(config_file)

def test_query_loader_initialization(sample_query_config):
    """Test GraphQLQueryLoader initialization."""
    loader = GraphQLQueryLoader(sample_query_config)
    assert loader.config_path == Path(sample_query_config)
    assert hasattr(loader, 'queries')
    assert 'organisations' in loader.queries
    assert 'members' in loader.queries

def test_get_query_with_valid_name(sample_query_config):
    """Test getting a valid query."""
    loader = GraphQLQueryLoader(sample_query_config)
    query, variables = loader.get_query('organisations')
    
    # Check query structure
    assert 'query getOrganisation' in query
    assert 'organisation(id: $id)' in query
    assert 'sportType' in query
    
    # Check variables
    assert variables == {'id': '${ORG_ID}'}

def test_get_query_with_invalid_name(sample_query_config):
    """Test getting an invalid query raises KeyError."""
    loader = GraphQLQueryLoader(sample_query_config)
    with pytest.raises(KeyError, match="Query invalid_query not found in config"):
        loader.get_query('invalid_query')

def test_query_variable_substitution(sample_query_config):
    """Test variable substitution in queries."""
    loader = GraphQLQueryLoader(sample_query_config)
    query, default_vars = loader.get_query('members')
    
    # Override default variables
    variables = {**default_vars, 'id': '123', 'page': 2}
    
    assert variables['id'] == '123'
    assert variables['page'] == 2

def test_query_validation(sample_query_config):
    """Test that loaded queries have required fields."""
    loader = GraphQLQueryLoader(sample_query_config)
    
    for query_name, query_data in loader.queries.items():
        assert 'query' in query_data, f"Query {query_name} missing 'query' field"
        assert isinstance(query_data['query'], str), f"Query {query_name} 'query' must be string"
        
        if 'variables' in query_data:
            assert isinstance(query_data['variables'], dict), \
                f"Query {query_name} 'variables' must be dictionary"
