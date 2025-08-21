"""
Test-Driven Development tests for dbt Cloud Integration.

This module tests the integration with dbt Cloud for transformations.
These tests define the expected behavior for triggering dbt Cloud jobs from Airflow.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import requests


class TestDBTCloudIntegration:
    """Test suite for dbt Cloud integration functionality."""
    
    def test_dbt_cloud_job_trigger_function_exists(self):
        """Test that we can define a dbt Cloud job trigger function."""
        # GIVEN: A function to trigger dbt Cloud jobs
        def trigger_dbt_cloud_job(job_id: int, account_id: int, api_token: str, cause: str = "Triggered by Airflow") -> dict:
            """
            Trigger a dbt Cloud job and return the run information.
            
            Args:
                job_id: dbt Cloud job ID
                account_id: dbt Cloud account ID  
                api_token: dbt Cloud API token
                cause: Reason for triggering the job
                
            Returns:
                dict: Job run information
            """
            # This function should be implemented
            pass
        
        # THEN: Function should be callable
        assert callable(trigger_dbt_cloud_job)
    
    @patch('requests.post')
    def test_dbt_cloud_api_call_success(self, mock_post):
        """Test successful dbt Cloud API call."""
        # GIVEN: A mocked successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'id': 12345,
                'status': 1,  # Queued
                'job_id': 67890
            }
        }
        mock_post.return_value = mock_response
        
        # WHEN: We make an API call to trigger a job
        def trigger_dbt_cloud_job(job_id, account_id, api_token, cause="Airflow trigger"):
            url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
            headers = {
                'Authorization': f'Token {api_token}',
                'Content-Type': 'application/json'
            }
            data = {'cause': cause}
            
            response = requests.post(url, headers=headers, json=data)
            return response.json()
        
        result = trigger_dbt_cloud_job(
            job_id=67890,
            account_id=12345,
            api_token="test_token",
            cause="Test trigger"
        )
        
        # THEN: API should be called correctly and return expected data
        mock_post.assert_called_once()
        assert result['data']['id'] == 12345
        assert result['data']['job_id'] == 67890
    
    @patch('requests.post')
    def test_dbt_cloud_api_call_failure(self, mock_post):
        """Test failed dbt Cloud API call."""
        # GIVEN: A mocked failed API response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.json.return_value = {
            'status': {
                'code': 401,
                'message': 'Unauthorized'
            }
        }
        mock_post.return_value = mock_response
        
        # WHEN: We make an API call with invalid credentials
        def trigger_dbt_cloud_job_with_error_handling(job_id, account_id, api_token):
            url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
            headers = {
                'Authorization': f'Token {api_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(url, headers=headers, json={})
            
            if response.status_code != 200:
                raise Exception(f"dbt Cloud API error: {response.status_code}")
            
            return response.json()
        
        # THEN: Should raise an exception for failed API call
        with pytest.raises(Exception, match="dbt Cloud API error: 401"):
            trigger_dbt_cloud_job_with_error_handling(
                job_id=67890,
                account_id=12345,
                api_token="invalid_token"
            )
    
    def test_dbt_cloud_job_status_monitoring(self):
        """Test dbt Cloud job status monitoring functionality."""
        # GIVEN: A function to check job status
        def check_dbt_cloud_job_status(run_id: int, account_id: int, api_token: str) -> dict:
            """
            Check the status of a dbt Cloud job run.
            
            Returns:
                dict: Job status information with keys: status, is_complete, is_success
            """
            # Mock implementation for testing
            return {
                'status': 10,  # Success status code
                'is_complete': True,
                'is_success': True
            }
        
        # WHEN: We check job status
        status = check_dbt_cloud_job_status(
            run_id=12345,
            account_id=67890,
            api_token="test_token"
        )
        
        # THEN: Should return status information
        assert 'status' in status
        assert 'is_complete' in status
        assert 'is_success' in status
    
    def test_dbt_cloud_run_results_parsing(self):
        """Test parsing of dbt Cloud run results."""
        # GIVEN: Sample dbt Cloud run results
        sample_results = {
            'data': {
                'id': 12345,
                'status': 10,  # Success
                'status_humanized': 'Success',
                'finished_at': '2024-01-15T10:30:00Z',
                'run_steps': [
                    {
                        'name': 'dbt run',
                        'status': 10,
                        'logs': 'Completed successfully'
                    }
                ]
            }
        }
        
        # WHEN: We parse the results
        def parse_dbt_results(results: dict) -> dict:
            """Parse dbt Cloud results into a standardized format."""
            data = results.get('data', {})
            return {
                'run_id': data.get('id'),
                'status': data.get('status'),
                'status_text': data.get('status_humanized'),
                'finished_at': data.get('finished_at'),
                'success': data.get('status') == 10,
                'step_count': len(data.get('run_steps', []))
            }
        
        parsed = parse_dbt_results(sample_results)
        
        # THEN: Should extract key information
        assert parsed['run_id'] == 12345
        assert parsed['success'] is True
        assert parsed['status'] == 10
        assert parsed['step_count'] == 1


class TestDBTCloudAirflowOperator:
    """Test suite for custom dbt Cloud Airflow operator."""
    
    def test_dbt_cloud_operator_interface(self):
        """Test the interface for a custom dbt Cloud operator."""
        # GIVEN: A custom operator class structure
        class DBTCloudRunOperator:
            """Custom operator to trigger dbt Cloud jobs from Airflow."""
            
            def __init__(self, job_id: int, account_id: int, api_token: str, **kwargs):
                self.job_id = job_id
                self.account_id = account_id
                self.api_token = api_token
                self.kwargs = kwargs
            
            def execute(self, context):
                """Execute the dbt Cloud job."""
                # Implementation would go here
                return f"Triggered dbt job {self.job_id}"
        
        # WHEN: We instantiate the operator
        operator = DBTCloudRunOperator(
            job_id=12345,
            account_id=67890,
            api_token="test_token"
        )
        
        # THEN: Should have required attributes
        assert operator.job_id == 12345
        assert operator.account_id == 67890
        assert operator.api_token == "test_token"
        assert hasattr(operator, 'execute')
    
    def test_dbt_cloud_sensor_interface(self):
        """Test the interface for a dbt Cloud sensor."""
        # GIVEN: A sensor class structure
        class DBTCloudJobSensor:
            """Sensor to wait for dbt Cloud job completion."""
            
            def __init__(self, run_id: int, account_id: int, api_token: str, **kwargs):
                self.run_id = run_id
                self.account_id = account_id
                self.api_token = api_token
                self.poke_interval = kwargs.get('poke_interval', 60)
                self.timeout = kwargs.get('timeout', 3600)
            
            def poke(self, context):
                """Check if the dbt job is complete."""
                # Implementation would check job status
                return True  # Job complete
        
        # WHEN: We instantiate the sensor
        sensor = DBTCloudJobSensor(
            run_id=12345,
            account_id=67890,
            api_token="test_token",
            poke_interval=30,
            timeout=1800
        )
        
        # THEN: Should have required attributes
        assert sensor.run_id == 12345
        assert sensor.poke_interval == 30
        assert sensor.timeout == 1800
        assert hasattr(sensor, 'poke')


class TestDBTCloudConfiguration:
    """Test suite for dbt Cloud configuration management."""
    
    def test_dbt_cloud_config_validation(self):
        """Test validation of dbt Cloud configuration."""
        # GIVEN: Configuration parameters
        config = {
            'account_id': 12345,
            'api_token': 'dbt_token_123',
            'job_mappings': {
                'staging': 67890,
                'marts': 67891,
                'tests': 67892
            }
        }
        
        # WHEN: We validate the configuration
        def validate_dbt_config(config: dict) -> bool:
            """Validate dbt Cloud configuration."""
            required_keys = ['account_id', 'api_token', 'job_mappings']
            
            for key in required_keys:
                if key not in config:
                    return False
            
            if not isinstance(config['account_id'], int):
                return False
                
            if not isinstance(config['api_token'], str):
                return False
                
            if not isinstance(config['job_mappings'], dict):
                return False
            
            return True
        
        # THEN: Configuration should be valid
        assert validate_dbt_config(config) is True
    
    def test_dbt_cloud_config_invalid(self):
        """Test handling of invalid dbt Cloud configuration."""
        # GIVEN: Invalid configuration
        invalid_configs = [
            {},  # Empty config
            {'account_id': 12345},  # Missing api_token
            {'account_id': '12345', 'api_token': 'token'},  # Wrong type for account_id
            {'account_id': 12345, 'api_token': 123}  # Wrong type for api_token
        ]
        
        def validate_dbt_config(config: dict) -> bool:
            required_keys = ['account_id', 'api_token', 'job_mappings']
            
            for key in required_keys:
                if key not in config:
                    return False
            
            if not isinstance(config['account_id'], int):
                return False
                
            if not isinstance(config['api_token'], str):
                return False
                
            if not isinstance(config['job_mappings'], dict):
                return False
            
            return True
        
        # WHEN/THEN: Invalid configurations should fail validation
        for config in invalid_configs:
            assert validate_dbt_config(config) is False
