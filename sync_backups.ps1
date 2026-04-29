# MES CRM Backup Sync Script

$SERVER     = "root@103.74.93.188"
$KEY        = "$env:USERPROFILE\.ssh\mes_vps"
$REMOTE_DIR = "/opt/mes/backups/"
$LOCAL_DIR  = "$env:USERPROFILE\Documents\MES_Backups"

# Find scp.exe
$scpPath = $null
$candidates = @(
    "C:\Windows\System32\OpenSSH\scp.exe",
    "C:\Program Files\Git\usr\bin\scp.exe",
    "C:\Program Files (x86)\Git\usr\bin\scp.exe"
)
foreach ($c in $candidates) {
    if (Test-Path $c) { $scpPath = $c; break }
}
if (!$scpPath) {
    $cmd = Get-Command scp -ErrorAction SilentlyContinue
    if ($cmd) { $scpPath = $cmd.Source }
}
if (!$scpPath) {
    Write-Host "ERROR: scp.exe not found. Install OpenSSH Client via Settings -> Apps -> Optional Features" -ForegroundColor Red
    pause
    exit 1
}

Write-Host "scp found: $scpPath" -ForegroundColor Gray

# Create local folder if not exists
if (!(Test-Path $LOCAL_DIR)) {
    New-Item -ItemType Directory -Path $LOCAL_DIR | Out-Null
    Write-Host "Created folder: $LOCAL_DIR" -ForegroundColor Gray
}

Write-Host "Downloading backups from server..." -ForegroundColor Cyan

# Run scp with visible output
& $scpPath -i $KEY -o StrictHostKeyChecking=no `
    "${SERVER}:${REMOTE_DIR}*.db" `
    "$LOCAL_DIR\"

$exitCode = $LASTEXITCODE
Write-Host "scp exit code: $exitCode" -ForegroundColor Gray

if ($exitCode -eq 0) {
    # Keep only 5 latest files locally
    $files = Get-ChildItem -Path $LOCAL_DIR -Filter "*.db" | Sort-Object LastWriteTime -Descending
    if ($files.Count -gt 5) {
        $files | Select-Object -Skip 5 | Remove-Item -Force
        Write-Host "Removed old backups (kept 5)" -ForegroundColor Yellow
    }

    $files = Get-ChildItem -Path $LOCAL_DIR -Filter "*.db" | Sort-Object LastWriteTime -Descending
    Write-Host "SUCCESS: $($files.Count) backup(s) in $LOCAL_DIR" -ForegroundColor Green
    $files | ForEach-Object { Write-Host "  $($_.Name)  ($([math]::Round($_.Length/1MB,2)) MB)" }
} else {
    Write-Host "ERROR: download failed (exit code $exitCode)" -ForegroundColor Red
}

# Write log
$logFile = "$LOCAL_DIR\sync.log"
$logEntry = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | $(if($exitCode -eq 0){'OK'}else{'ERROR'}) | Files: $((Get-ChildItem $LOCAL_DIR -Filter '*.db').Count)"
Add-Content -Path $logFile -Value $logEntry

Write-Host "Done. Press Enter to close..."
Read-Host
