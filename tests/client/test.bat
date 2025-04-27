@echo off
setlocal enabledelayedexpansion

:: Configuration
set BROKER_HOST=http://localhost:8080
set METRICS_HOST=http://localhost:9091
set TEST_CLIENT=voltaxmb-test-client.exe

echo Starting VoltaxMB Test Client Verification

:: Check if test client exists
if not exist "%TEST_CLIENT%" (
    echo Test client executable not found at %TEST_CLIENT%
    exit /b 1
)

:: Check if broker is running
echo Checking broker service...
set /a retry_count=0
:check_broker
curl -s "%BROKER_HOST%" >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] Broker service is running
    goto check_metrics
)
set /a retry_count+=1
if %retry_count% lss 5 (
    echo [WAIT] Broker not ready, retrying... (%retry_count%/5)
    timeout /t 2 >nul
    goto check_broker
)
echo [ERROR] Broker service is not available
exit /b 1

:check_metrics
:: Check if metrics service is running
echo Checking metrics service...
set /a retry_count=0
:check_metrics_loop
curl -s "%METRICS_HOST%/metrics" >nul 2>&1
if %errorlevel% equ 0 (
    echo [SUCCESS] Metrics service is running
    goto run_tests
)
set /a retry_count+=1
if %retry_count% lss 5 (
    echo [WAIT] Metrics not ready, retrying... (%retry_count%/5)
    timeout /t 2 >nul
    goto check_metrics_loop
)
echo [ERROR] Metrics service is not available
exit /b 1

:run_tests
echo Starting test suite...

:: Test with default settings
echo Running tests with default settings...
%TEST_CLIENT%
if %errorlevel% equ 0 (
    echo [SUCCESS] Default tests passed
) else (
    echo [ERROR] Default tests failed
    exit /b 1
)

:: Test with custom broker host
echo Running tests with custom broker host...
%TEST_CLIENT% -broker="%BROKER_HOST%"
if %errorlevel% equ 0 (
    echo [SUCCESS] Custom broker host tests passed
) else (
    echo [ERROR] Custom broker host tests failed
    exit /b 1
)

:: Test without cleanup
echo Running tests without cleanup...
%TEST_CLIENT% -cleanup=false
if %errorlevel% equ 0 (
    echo [SUCCESS] No cleanup tests passed
) else (
    echo [ERROR] No cleanup tests failed
    exit /b 1
)

:: Test with invalid broker host
echo Testing with invalid broker host...
%TEST_CLIENT% -broker="http://invalid-host:8080" 2>nul
if %errorlevel% neq 0 (
    echo [SUCCESS] Invalid broker host test passed (expected failure)
) else (
    echo [ERROR] Invalid broker host test failed (unexpected success)
    exit /b 1
)

echo [SUCCESS] All test scenarios completed successfully
exit /b 0 