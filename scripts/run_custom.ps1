param(
    [string]$Products = "nh_z500a,na_z500a,conus_t2m_anom",
    [string]$HoursCsv = "6",
    [string]$MaxDim = "900",
    [ValidateSet("era5", "merra2")]
    [string]$Climo = "era5",
    [string]$EeProject = "snowcast-1"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    $env:EE_PROJECT = $EeProject
    $env:GITHUB_EVENT_NAME = "workflow_dispatch"
    $env:WN2_PRODUCT_MODE = "custom"
    $env:WN2_SELECTED_PRODUCTS = $Products
    $env:HOURS_CSV = $HoursCsv
    $env:WN2_MAX_DIMENSION = $MaxDim
    $env:WN2_CLIMO_SOURCE = $Climo

    Write-Host "Running custom map generation..."
    Write-Host "  EE_PROJECT=$($env:EE_PROJECT)"
    Write-Host "  WN2_SELECTED_PRODUCTS=$($env:WN2_SELECTED_PRODUCTS)"
    Write-Host "  HOURS_CSV=$($env:HOURS_CSV)"
    Write-Host "  WN2_MAX_DIMENSION=$($env:WN2_MAX_DIMENSION)"
    Write-Host "  WN2_CLIMO_SOURCE=$($env:WN2_CLIMO_SOURCE)"

    python main.py
}
finally {
    Pop-Location
}
