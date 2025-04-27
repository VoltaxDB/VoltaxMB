#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
BROKER_HOST="http://localhost:8080"
METRICS_HOST="http://localhost:9091"
TEST_CLIENT="./voltaxmb-test-client.exe"

# Function to check if a service is running
check_service() {
    local url=$1
    local max_retries=5
    local retry_count=0
    
    echo -e "${YELLOW}Checking service at $url...${NC}"
    
    while [ $retry_count -lt $max_retries ]; do
        if curl -s "$url" > /dev/null; then
            echo -e "${GREEN}Service is running at $url${NC}"
            return 0
        fi
        
        echo -e "${YELLOW}Service not ready, retrying... ($((retry_count + 1))/$max_retries)${NC}"
        sleep 2
        retry_count=$((retry_count + 1))
    done
    
    echo -e "${RED}Service not available at $url after $max_retries attempts${NC}"
    return 1
}

# Function to run tests
run_tests() {
    echo -e "${YELLOW}Starting test suite...${NC}"
    
    # Test with default settings
    echo -e "${YELLOW}Running tests with default settings...${NC}"
    if $TEST_CLIENT; then
        echo -e "${GREEN}Default tests passed${NC}"
    else
        echo -e "${RED}Default tests failed${NC}"
        return 1
    fi
    
    # Test with custom broker host
    echo -e "${YELLOW}Running tests with custom broker host...${NC}"
    if $TEST_CLIENT -broker="$BROKER_HOST"; then
        echo -e "${GREEN}Custom broker host tests passed${NC}"
    else
        echo -e "${RED}Custom broker host tests failed${NC}"
        return 1
    fi
    
    # Test without cleanup
    echo -e "${YELLOW}Running tests without cleanup...${NC}"
    if $TEST_CLIENT -cleanup=false; then
        echo -e "${GREEN}No cleanup tests passed${NC}"
    else
        echo -e "${RED}No cleanup tests failed${NC}"
        return 1
    fi
    
    # Test with invalid broker host
    echo -e "${YELLOW}Testing with invalid broker host...${NC}"
    if ! $TEST_CLIENT -broker="http://invalid-host:8080" 2>/dev/null; then
        echo -e "${GREEN}Invalid broker host test passed (expected failure)${NC}"
    else
        echo -e "${RED}Invalid broker host test failed (unexpected success)${NC}"
        return 1
    fi
    
    echo -e "${GREEN}All test scenarios completed successfully${NC}"
    return 0
}

# Main execution
echo -e "${YELLOW}Starting VoltaxMB Test Client Verification${NC}"

# Check if test client exists
if [ ! -f "$TEST_CLIENT" ]; then
    echo -e "${RED}Test client executable not found at $TEST_CLIENT${NC}"
    exit 1
fi

# Check if broker is running
if ! check_service "$BROKER_HOST"; then
    echo -e "${RED}Broker service is not available${NC}"
    exit 1
fi

# Check if metrics service is running
if ! check_service "$METRICS_HOST/metrics"; then
    echo -e "${RED}Metrics service is not available${NC}"
    exit 1
fi

# Run tests
if run_tests; then
    echo -e "${GREEN}All tests completed successfully${NC}"
    exit 0
else
    echo -e "${RED}Test suite failed${NC}"
    exit 1
fi 