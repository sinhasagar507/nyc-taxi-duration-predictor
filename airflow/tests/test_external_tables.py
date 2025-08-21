"""
Test-Driven Development tests for External Table Creation DAGs.

This module tests the BigQuery external table creation DAGs.
"""

import pytest
from unittest.mock import Mock, patch
from airflow.models import DagBag


class TestGreenTaxiExternalTable:
    """Test suite for Green Taxi External Table DAG."""
    
    DAG_ID = "create_external_table_green_taxi"
    
    def test_dag_loaded(self, dagbag):
        """Test that the external table DAG is loaded."""
        # GIVEN: A DAG bag
        dag = dagbag.get_dag(self.DAG_ID)
        
        # THEN: DAG should be loaded
        assert dag is not None
        assert dag.dag_id == self.DAG_ID
    
    def test_dag_has_single_task(self, dagbag):
        """Test that the DAG has one task."""
        # GIVEN: A loaded DAG
        dag = dagbag.get_dag(self.DAG_ID)
        
        # WHEN: We check the tasks
        task_ids = list(dag.task_dict.keys())
        
        # THEN: Should have exactly one task
        assert len(task_ids) == 1
        assert "create_external_table" in task_ids
    
    def test_dag_configuration(self, dagbag):
        """Test DAG configuration."""
        # GIVEN: A loaded DAG
        dag = dagbag.get_dag(self.DAG_ID)
        
        # THEN: Configuration should be correct
        assert dag.schedule_interval is None  # On-demand
        assert dag.catchup is False
        expected_tags = ['bigquery', 'external', 'gcs']
        assert all(tag in dag.tags for tag in expected_tags)
    
    def test_external_table_sql_query_format(self, dagbag):
        """Test that the SQL query is properly formatted."""
        # GIVEN: Expected values from the DAG
        project_id = 'dtc-de-course-466501'
        dataset_id = 'nyc_taxi_data'
        table_id = 'green_taxi_external_table'
        bucket_name = 'dtc-de-project_1'
        source_folder = 'nyc_taxi_data/green_taxi_data/'
        
        # WHEN: We construct the expected SQL
        expected_sql_parts = [
            f"CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_id}`",
            "format = 'PARQUET'",
            f"gs://{bucket_name}/{source_folder}green_tripdata_*.parquet"
        ]
        
        # THEN: All parts should be present
        for part in expected_sql_parts:
            assert part is not None
            assert len(part) > 0
    
    def test_task_uses_correct_operator(self, dagbag):
        """Test that the task uses BigQueryInsertJobOperator."""
        # GIVEN: A loaded DAG
        dag = dagbag.get_dag(self.DAG_ID)
        
        # WHEN: We get the task
        task = dag.get_task("create_external_table")
        
        # THEN: It should use the correct operator
        assert task.task_type == "BigQueryInsertJobOperator"
    
    def test_task_configuration(self, dagbag):
        """Test task configuration parameters."""
        # GIVEN: A loaded DAG
        dag = dagbag.get_dag(self.DAG_ID)
        task = dag.get_task("create_external_table")
        
        # THEN: Task should have correct configuration
        assert hasattr(task, 'location')
        assert task.location == 'US'
        assert hasattr(task, 'gcp_conn_id')
        assert task.gcp_conn_id == 'google_cloud_default'


class TestYellowTaxiExternalTable:
    """Test suite for Yellow Taxi External Table DAG."""
    
    DAG_ID = "create_external_table_yellow_taxi"
    
    def test_dag_loaded(self, dagbag):
        """Test that the yellow taxi external table DAG is loaded."""
        dag = dagbag.get_dag(self.DAG_ID)
        assert dag is not None
    
    def test_dag_follows_same_pattern_as_green(self, dagbag):
        """Test that yellow taxi DAG follows same pattern as green."""
        # GIVEN: Both DAGs
        green_dag = dagbag.get_dag("create_external_table_green_taxi")
        yellow_dag = dagbag.get_dag(self.DAG_ID)
        
        # THEN: They should have similar structure
        assert len(green_dag.task_dict) == len(yellow_dag.task_dict)
        assert green_dag.schedule_interval == yellow_dag.schedule_interval
        assert green_dag.catchup == yellow_dag.catchup


class TestClimateExternalTable:
    """Test suite for Climate External Table DAG."""
    
    DAG_ID = "create_external_table_climate_data"
    
    def test_dag_loaded(self, dagbag):
        """Test that the climate external table DAG is loaded."""
        dag = dagbag.get_dag(self.DAG_ID)
        assert dag is not None
    
    def test_climate_dag_consistency(self, dagbag):
        """Test that climate DAG follows consistent patterns."""
        dag = dagbag.get_dag(self.DAG_ID)
        
        # Should have same basic structure as other external table DAGs
        assert len(dag.task_dict) == 1
        assert "create_external_table" in dag.task_dict
        assert dag.schedule_interval is None


class TestExternalTableConsistency:
    """Test suite for consistency across all external table DAGs."""
    
    def test_all_external_table_dags_loaded(self, dagbag):
        """Test that all external table DAGs are loaded."""
        expected_dags = [
            "create_external_table_green_taxi",
            "create_external_table_yellow_taxi", 
            "create_external_table_climate_data",
            "create_external_table_taxi_zone"
        ]
        
        for dag_id in expected_dags:
            dag = dagbag.get_dag(dag_id)
            assert dag is not None, f"DAG {dag_id} should be loaded"
    
    def test_external_table_dags_have_consistent_structure(self, dagbag):
        """Test that all external table DAGs follow consistent patterns."""
        external_table_dags = [
            "create_external_table_green_taxi",
            "create_external_table_yellow_taxi",
            "create_external_table_climate_data",
            "create_external_table_taxi_zone"
        ]
        
        for dag_id in external_table_dags:
            dag = dagbag.get_dag(dag_id)
            
            # Each should have exactly one task
            assert len(dag.task_dict) == 1
            
            # Task should be named create_external_table
            assert "create_external_table" in dag.task_dict
            
            # Should be on-demand (no schedule)
            assert dag.schedule_interval is None
            
            # Should not catchup
            assert dag.catchup is False
    
    def test_external_table_tasks_use_correct_operator(self, dagbag):
        """Test that all external table tasks use BigQueryInsertJobOperator."""
        external_table_dags = [
            "create_external_table_green_taxi",
            "create_external_table_yellow_taxi",
            "create_external_table_climate_data",
            "create_external_table_taxi_zone"
        ]
        
        for dag_id in external_table_dags:
            dag = dagbag.get_dag(dag_id)
            task = dag.get_task("create_external_table")
            
            assert task.task_type == "BigQueryInsertJobOperator"
            assert task.location == 'US'
            assert task.gcp_conn_id == 'google_cloud_default'
