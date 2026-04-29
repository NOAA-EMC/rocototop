import pytest
from rocototop.parser import RocotoParser

@pytest.fixture(autouse=True)
def clear_parser_cache():
    """Clear the LRU cache for cycle parsing to ensure test isolation."""
    RocotoParser._parse_cycle.cache_clear()
