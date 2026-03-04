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
    & "$PSScriptRoot\run_custom.ps1" `
        -Products $Products `
        -HoursCsv $HoursCsv `
        -MaxDim $MaxDim `
        -Climo $Climo `
        -EeProject $EeProject

    $env:WN2_MAX_DIMENSION = $MaxDim
    python tests/smoke_outputs.py
}
finally {
    Pop-Location
}
