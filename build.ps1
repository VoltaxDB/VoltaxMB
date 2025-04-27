# Build script for VoltaxMB

Write-Host "Building VoltaxMB..."
Write-Host "VoltaxMB By Avijit Sen"

if (-not (Test-Path "build")) {
    New-Item -ItemType Directory -Path "build"
}

Write-Host "Building broker..."
go build -o build/voltaxmb.exe cmd/broker/main.go

Write-Host "Building test client..."
go build -o build/voltaxmb-test.exe tests/client/main.go

Write-Host "Build completed successfully!"
Write-Host "Executables are in the 'build' directory:"
Write-Host "- voltaxmb.exe (broker)"
Write-Host "- voltaxmb-test.exe (test client)" 