#!/bin/bash

# NYC Taxi Data Pipeline - Test Runner Script
# This script runs all tests with proper configuration and reporting

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 NYC Taxi Data Pipeline - Test Suite${NC}"
echo "=================================================="

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo -e "${GREEN}✅ Virtual environment detected: $VIRTUAL_ENV${NC}"
else
    echo -e "${YELLOW}⚠️  No virtual environment detected. Consider activating .venv${NC}"
fi

# Install test dependencies
echo -e "${BLUE}📦 Installing test dependencies...${NC}"
pip install -r test-requirements.txt

# Set PYTHONPATH to include the dags directory
export PYTHONPATH="${PYTHONPATH}:$(pwd)/dags"
export AIRFLOW_HOME=$(pwd)

# Create test reports directory
mkdir -p test-reports

echo -e "${BLUE}🔍 Running tests...${NC}"

# Run different test categories
echo -e "${YELLOW}Running Unit Tests...${NC}"
pytest tests/ -m "unit" -v --tb=short --cov=dags --cov-report=html:test-reports/coverage-html --cov-report=xml:test-reports/coverage.xml --html=test-reports/unit-tests.html --self-contained-html

echo -e "${YELLOW}Running DAG Validation Tests...${NC}"
pytest tests/ -m "dag_validation" -v --tb=short --html=test-reports/dag-validation.html --self-contained-html

echo -e "${YELLOW}Running Integration Tests...${NC}"
pytest tests/ -m "integration" -v --tb=short --html=test-reports/integration-tests.html --self-contained-html

echo -e "${YELLOW}Running All Tests with Coverage...${NC}"
pytest tests/ -v --tb=short --cov=dags --cov-report=html:test-reports/coverage-html --cov-report=xml:test-reports/coverage.xml --cov-report=term-missing --html=test-reports/all-tests.html --self-contained-html

# Check test results
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed!${NC}"
    echo -e "${GREEN}📊 Test reports generated in test-reports/directory${NC}"
    echo -e "${GREEN}📈 Coverage report: test-reports/coverage-html/index.html${NC}"
else
    echo -e "${RED}❌ Some tests failed!${NC}"
    exit 1
fi

echo "=================================================="
echo -e "${BLUE}🎉 Test suite completed successfully!${NC}"
