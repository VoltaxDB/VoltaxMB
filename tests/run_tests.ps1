# Build the test client
Write-Host "Building test client..."
go build -o test_client.exe ./tests/client/main.go

# Run multiple instances of the test client
Write-Host "Running test clients..."
$jobs = @()

# Start 5 concurrent test clients
for ($i = 1; $i -le 5; $i++) {
    $jobs += Start-Job -ScriptBlock {
        param($clientNum)
        Write-Host "Starting test client $clientNum"
        & ".\test_client.exe"
    } -ArgumentList $i
}

# Wait for all jobs to complete
Write-Host "Waiting for test clients to complete..."
$jobs | Wait-Job

# Get the results
Write-Host "`nTest Results:"
$jobs | ForEach-Object {
    Write-Host "Client $($_.Id) output:"
    Receive-Job -Job $_
    Write-Host "-----------------"
}

# Clean up
$jobs | Remove-Job
Remove-Item test_client.exe

Write-Host "All tests completed!" 