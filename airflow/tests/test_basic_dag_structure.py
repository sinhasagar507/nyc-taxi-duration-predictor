"""
Basic DAG Structure Tests (TDD Approach)

These are the first tests we write to define the expected behavior
of our DAG structure before implementing the actual DAGs.
"""

import pytest
import os
import sys
from pathlib import Path


class TestBasicDAGStructure:
    """Basic tests to ensure our DAG files exist and have correct structure."""
    
    def test_dags_directory_exists(self):
        """Test that the dags directory exists."""
        # GIVEN: The project structure
        dags_dir = Path("dags")
        
        # THEN: The dags directory should exist
        assert dags_dir.exists(), "dags directory should exist"
        assert dags_dir.is_dir(), "dags should be a directory"
    
    def test_yellow_taxi_dag_file_exists(self):
        """Test that the yellow taxi DAG file exists."""
        # GIVEN: The dags directory
        dag_file = Path("dags/nyc_taxi_gcs_yellow_dag.py")
        
        # THEN: The DAG file should exist
        assert dag_file.exists(), f"DAG file {dag_file} should exist"
        assert dag_file.is_file(), f"{dag_file} should be a file"
    
    def test_yellow_taxi_dag_has_dag_definition(self):
        """Test that the yellow taxi DAG file contains a DAG definition."""
        # GIVEN: The DAG file
        dag_file = Path("dags/nyc_taxi_gcs_yellow_dag.py")
        
        # WHEN: We read the file content
        content = dag_file.read_text()
        
        # THEN: It should contain DAG-related imports and definitions
        assert "from airflow import DAG" in content, "Should import DAG"
        assert "with DAG(" in content, "Should have DAG context manager"
        assert "dag_id=" in content, "Should have dag_id parameter"
    
    def test_yellow_taxi_dag_has_required_functions(self):
        """Test that the yellow taxi DAG has required task functions."""
        # GIVEN: The DAG file
        dag_file = Path("dags/nyc_taxi_gcs_yellow_dag.py")
        content = dag_file.read_text()
        
        # THEN: It should have the expected functions
        expected_functions = [
            "def download_file(",
            "def upload_to_gcs(",
            "def remove_local_file("
        ]
        
        for func in expected_functions:
            assert func in content, f"Should have function: {func}"
    
    def test_yellow_taxi_dag_has_task_operators(self):
        """Test that the DAG defines task operators."""
        # GIVEN: The DAG file
        dag_file = Path("dags/nyc_taxi_gcs_yellow_dag.py")
        content = dag_file.read_text()
        
        # THEN: It should define task operators
        expected_tasks = [
            "download_dataset_task",
            "local_to_gcs_task", 
            "cleanup_local_file_task"
        ]
        
        for task in expected_tasks:
            assert task in content, f"Should define task: {task}"
    
    def test_climate_dag_file_exists(self):
        """Test that the climate DAG file exists."""
        # GIVEN: The dags directory
        dag_file = Path("dags/nyc_climate_gcs_dag.py")
        
        # THEN: The DAG file should exist
        assert dag_file.exists(), f"DAG file {dag_file} should exist"
    
    def test_external_table_dags_exist(self):
        """Test that external table DAG files exist."""
        # GIVEN: Expected external table DAGs
        expected_external_table_dags = [
            "green_taxi_external_table.py",
            "yellow_taxi_external_table.py",
            "climate_data_external_table.py"
        ]
        
        # THEN: Each external table DAG should exist
        for dag_file in expected_external_table_dags:
            dag_path = Path(f"dags/{dag_file}")
            assert dag_path.exists(), f"External table DAG {dag_file} should exist"


class TestDAGImportability:
    """Test that DAG files can be imported without errors."""
    
    def test_yellow_taxi_dag_imports_successfully(self):
        """Test that the yellow taxi DAG can be imported."""
        # GIVEN: The DAG file path
        dag_path = Path("dags")
        
        # WHEN: We try to import the DAG module
        # Add the dags directory to Python path
        sys.path.insert(0, str(dag_path.absolute()))
        
        try:
            # THEN: The import should succeed
            import nyc_taxi_gcs_yellow_dag
            
            # And it should have the expected attributes
            assert hasattr(nyc_taxi_gcs_yellow_dag, 'download_file')
            assert hasattr(nyc_taxi_gcs_yellow_dag, 'upload_to_gcs')
            assert hasattr(nyc_taxi_gcs_yellow_dag, 'remove_local_file')
            
        except ImportError as e:
            pytest.fail(f"Failed to import yellow taxi DAG: {e}")
        finally:
            # Clean up
            if str(dag_path.absolute()) in sys.path:
                sys.path.remove(str(dag_path.absolute()))
    
    def test_climate_dag_imports_successfully(self):
        """Test that the climate DAG can be imported."""
        # GIVEN: The DAG file path
        dag_path = Path("dags")
        sys.path.insert(0, str(dag_path.absolute()))
        
        try:
            # WHEN/THEN: The import should succeed
            import nyc_climate_gcs_dag
            
            # And it should have expected functions
            assert hasattr(nyc_climate_gcs_dag, 'format_to_parquet')
            assert hasattr(nyc_climate_gcs_dag, 'upload_to_gcs')
            assert hasattr(nyc_climate_gcs_dag, 'remove_local_files')
            
        except ImportError as e:
            pytest.fail(f"Failed to import climate DAG: {e}")
        finally:
            if str(dag_path.absolute()) in sys.path:
                sys.path.remove(str(dag_path.absolute()))


class TestDAGConfiguration:
    """Test DAG configuration parameters."""
    
    def test_environment_variables_are_used(self):
        """Test that DAGs use environment variables for configuration."""
        # GIVEN: The yellow taxi DAG file
        dag_file = Path("dags/nyc_taxi_gcs_yellow_dag.py")
        content = dag_file.read_text()
        
        # THEN: It should use environment variables
        expected_env_vars = [
            'os.environ.get("GCP_PROJECT_ID")',
            'os.environ.get("GCP_GCS_BUCKET")',
            'os.environ.get("AIRFLOW_HOME"'
        ]
        
        for env_var in expected_env_vars:
            assert env_var in content, f"Should use environment variable: {env_var}"
    
    def test_dag_has_proper_scheduling(self):
        """Test that DAGs have proper scheduling configuration."""
        # GIVEN: DAG files
        dag_files = [
            Path("dags/nyc_taxi_gcs_yellow_dag.py"),
            Path("dags/nyc_climate_gcs_dag.py")
        ]
        
        for dag_file in dag_files:
            content = dag_file.read_text()
            
            # THEN: Should have schedule_interval defined
            assert "schedule_interval" in content, f"{dag_file.name} should have schedule_interval"
            
            # And should have proper DAG arguments
            dag_config_elements = [
                "default_args",
                "start_date",
                "catchup"
            ]
            
            for element in dag_config_elements:
                assert element in content, f"{dag_file.name} should have {element}"


class TestTaskDependencies:
    """Test that task dependencies are properly defined."""
    
    def test_yellow_taxi_dag_has_task_dependencies(self):
        """Test that the yellow taxi DAG has proper task dependencies."""
        # GIVEN: The yellow taxi DAG file
        dag_file = Path("dags/nyc_taxi_gcs_yellow_dag.py")
        content = dag_file.read_text()
        
        # THEN: Should have task dependency definitions
        # Look for the >> operator which defines dependencies
        assert ">>" in content, "Should have task dependencies defined with >> operator"
        
        # Should have the expected dependency chain
        dependency_patterns = [
            "download_dataset_task >> local_to_gcs_task",
            "local_to_gcs_task >> cleanup_local_file_task"
        ]
        
        for pattern in dependency_patterns:
            assert pattern in content, f"Should have dependency: {pattern}"
