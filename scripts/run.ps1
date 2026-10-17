$ErrorActionPreference = 'Stop'
$marketDeckRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$marketDeckPython = Join-Path $marketDeckRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $marketDeckPython)) {
    throw 'Create .venv and install requirements.txt before launching MarketDeck.'
}
$marketDeckBackend = Start-Process -FilePath $marketDeckPython -ArgumentList @('-m', 'marketdeck', 'serve') -WorkingDirectory $marketDeckRoot -WindowStyle Hidden -PassThru
$marketDeckPreviousLocation = (Get-Location).Path
try {
    Set-Location -LiteralPath (Join-Path $marketDeckRoot 'web')
    npm run dev
}
finally {
    Set-Location -LiteralPath $marketDeckPreviousLocation
    if (-not $marketDeckBackend.HasExited) {
        Stop-Process -Id $marketDeckBackend.Id
    }
}
