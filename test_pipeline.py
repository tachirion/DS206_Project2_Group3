"""
Test script for the Dimensional Data Pipeline.

This script tests the pipeline configuration and provides a simple way to run the pipeline.
"""

import sys
import os
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    try:
        from pipeline_dimensional_data import config
        from pipeline_dimensional_data import tasks
        from pipeline_dimensional_data import flow
        from utils import get_sql_config, create_connection_string, load_query
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_config():
    """Test that config values are properly set."""
    print("\nTesting configuration...")
    try:
        from pipeline_dimensional_data import config
        
        # Check required config values
        assert hasattr(config, 'SQL_CFG_FILE'), "SQL_CFG_FILE missing"
        assert hasattr(config, 'SQL_CFG_SECTION'), "SQL_CFG_SECTION missing"
        assert hasattr(config, 'QUERIES_DIR'), "QUERIES_DIR missing"
        assert hasattr(config, 'DIM_ORDER'), "DIM_ORDER missing"
        assert hasattr(config, 'queries_map'), "queries_map missing"
        assert hasattr(config, 'dim_tables'), "dim_tables missing"
        
        print(f"✓ SQL Config File: {config.SQL_CFG_FILE}")
        print(f"✓ SQL Config Section: {config.SQL_CFG_SECTION}")
        print(f"✓ Queries Directory: {config.QUERIES_DIR}")
        print(f"✓ Dimension Order: {len(config.DIM_ORDER)} dimensions")
        print(f"✓ Query Mappings: {len(config.queries_map)} queries")
        print(f"✓ Dimension Tables: {len(config.dim_tables)} tables")
        
        # Check if files exist
        if os.path.exists(config.SQL_CFG_FILE):
            print(f"✓ SQL config file exists")
        else:
            print(f"✗ SQL config file not found: {config.SQL_CFG_FILE}")
            return False
            
        if os.path.exists(config.QUERIES_DIR):
            print(f"✓ Queries directory exists")
        else:
            print(f"✗ Queries directory not found: {config.QUERIES_DIR}")
            return False
        
        return True
    except Exception as e:
        print(f"✗ Config test error: {e}")
        return False

def test_sql_config():
    """Test SQL configuration file reading."""
    print("\nTesting SQL configuration...")
    try:
        from pipeline_dimensional_data import config
        from utils import get_sql_config, create_connection_string
        
        cfg = get_sql_config(config.SQL_CFG_FILE, config.SQL_CFG_SECTION)
        print(f"✓ SQL config loaded successfully")
        print(f"  Driver: {cfg.get('Driver', 'N/A')}")
        print(f"  Server: {cfg.get('Server', 'N/A')}")
        print(f"  Database: {cfg.get('Database', 'N/A')}")
        
        conn_str = create_connection_string(cfg)
        print(f"✓ Connection string created")
        print(f"  (Length: {len(conn_str)} characters)")
        
        return True
    except Exception as e:
        print(f"✗ SQL config test error: {e}")
        return False

def test_queries():
    """Test that query files exist."""
    print("\nTesting query files...")
    try:
        from pipeline_dimensional_data import config
        from utils import load_query
        
        missing_queries = []
        for query_key, query_file in config.queries_map.items():
            query_path = os.path.join(config.QUERIES_DIR, query_file)
            if os.path.exists(query_path):
                # Try to load it
                sql_content = load_query(query_file, config.QUERIES_DIR)
                if sql_content:
                    print(f"✓ {query_file} found and loaded")
                else:
                    print(f"⚠ {query_file} found but empty or not loaded")
            else:
                print(f"✗ {query_file} not found")
                missing_queries.append(query_file)
        
        if missing_queries:
            print(f"\n✗ Missing queries: {', '.join(missing_queries)}")
            return False
        
        return True
    except Exception as e:
        print(f"✗ Query test error: {e}")
        return False

def test_flow_creation():
    """Test that the flow can be instantiated."""
    print("\nTesting flow creation...")
    try:
        from pipeline_dimensional_data.flow import DimensionalDataFlow
        
        flow = DimensionalDataFlow()
        print(f"✓ DimensionalDataFlow created successfully")
        print(f"  Execution ID: {flow.execution_id}")
        
        return True
    except Exception as e:
        print(f"✗ Flow creation error: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_pipeline_test(start_date="1996-01-01", end_date="1996-12-31"):
    """Test running the pipeline (dry run - checks structure only)."""
    print(f"\n{'='*60}")
    print("PIPELINE TEST MODE")
    print(f"{'='*60}")
    print(f"Start Date: {start_date}")
    print(f"End Date: {end_date}")
    print("\nNote: This is a structure test. To actually run the pipeline,")
    print("      ensure your database is configured and accessible.")
    print(f"{'='*60}\n")

def main():
    """Run all tests."""
    print("="*60)
    print("DIMENSIONAL DATA PIPELINE - TEST SUITE")
    print("="*60)
    
    tests = [
        ("Imports", test_imports),
        ("Configuration", test_config),
        ("SQL Configuration", test_sql_config),
        ("Query Files", test_queries),
        ("Flow Creation", test_flow_creation),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ {test_name} test crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ All tests passed! Pipeline is ready to use.")
        print("\nTo run the pipeline:")
        print("  from pipeline_dimensional_data.flow import DimensionalDataFlow")
        print("  flow = DimensionalDataFlow()")
        print("  result = flow.exec('1996-01-01', '1996-12-31')")
    else:
        print("\n✗ Some tests failed. Please fix the issues above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

