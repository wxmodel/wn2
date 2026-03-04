param(
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Write-Host "Cleaning repo root: $repoRoot"

$targets = Get-ChildItem -Path $repoRoot -Force | Where-Object {
    $_.Name -like ".tmp_*" -or ($_.PSIsContainer -eq $false -and $_.Name -like "*.log")
}

if (-not $targets) {
    Write-Host "No matching .tmp_* or *.log items found in repo root."
    return
}

foreach ($item in $targets) {
    Remove-Item -LiteralPath $item.FullName -Recurse -Force -WhatIf:$WhatIf
}

Write-Host "Removed $($targets.Count) item(s)."
