@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  BLite — Mutation Testing Runner
::  Usa: run-mutation-tests.bat [bson|core|netstandard|all]
::       Default: all
:: ============================================================

set "ROOT=%~dp0"
set "ROOT=%ROOT:~0,-1%"
set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=all"

:: Timestamp per la cartella dei risultati
for /f "tokens=1-6 delims=/: " %%a in ("%DATE% %TIME%") do (
    set "YY=%%a"
    set "MM=%%b"
    set "DD=%%c"
    set "HH=%%d"
    set "MI=%%e"
)
set "MI=%MI: =0%"
set "TIMESTAMP=%YY%%MM%%DD%_%HH%%MI%"

set "OUT_ROOT=%ROOT%\mutation-testing\StrykerOutput"
set "LOG_DIR=%OUT_ROOT%\logs"
set "SUMMARY=%OUT_ROOT%\summary_%TIMESTAMP%.txt"

:: Assicura che dotnet tool sia ripristinato
echo [INFO] Ripristino dotnet tools...
cd /d "%ROOT%"
dotnet tool restore >nul 2>&1
if errorlevel 1 (
    echo [ERROR] dotnet tool restore fallito.
    exit /b 1
)

:: Crea cartella log
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo.
echo ============================================================
echo  BLite Mutation Testing
echo  Target  : %TARGET%
echo  Output  : %OUT_ROOT%
echo  Avvio   : %DATE% %TIME%
echo ============================================================
echo.

set "EXITCODE=0"

:: ---- Funzione helper (goto-based) ----
goto :run_%TARGET% 2>nul || (
    echo [ERROR] Target non riconosciuto: "%TARGET%"
    echo         Valori validi: bson, core, netstandard, all
    exit /b 1
)
goto :collect_and_exit

:: ============================================================
:run_bson
    echo [RUN] BLite.Bson (via BLite.Tests)
    call :run_stryker "%ROOT%\tests\BLite.Tests" "BLite.Bson.csproj" "BLite.Bson" "%LOG_DIR%\bson.log"
    if "%TARGET%"=="bson" goto :collect_and_exit
    goto :eof

:run_core
    echo [RUN] BLite.Core (via BLite.Tests)
    call :run_stryker "%ROOT%\tests\BLite.Tests" "BLite.Core.csproj" "BLite.Core" "%LOG_DIR%\core.log"
    if "%TARGET%"=="core" goto :collect_and_exit
    goto :eof

:run_netstandard
    echo [RUN] BLite.Core — .NET Standard 2.1 (via BLite.NetStandard21.Tests)
    call :run_stryker "%ROOT%\tests\BLite.NetStandard21.Tests" "BLite.Core.csproj" "BLite.NetStandard21" "%LOG_DIR%\netstandard.log"
    if "%TARGET%"=="netstandard" goto :collect_and_exit
    goto :eof

:run_all
    call :run_bson
    call :run_core
    call :run_netstandard
    goto :collect_and_exit

:: ============================================================
:: Subroutine: run_stryker <test-dir> <project> <out-subdir> <log-file>
:run_stryker
    set "_DIR=%~1"
    set "_PROJ=%~2"
    set "_OUTDIR=%OUT_ROOT%\%~3"
    set "_LOG=%~4"

    echo   Directory : %_DIR%
    echo   Progetto  : %_PROJ%
    echo   Output    : %_OUTDIR%

    :: Pulizia eventuale StrykerOutput residuo prima di avviare
    if exist "%_DIR%\StrykerOutput" rmdir /s /q "%_DIR%\StrykerOutput"

    cd /d "%_DIR%"
    dotnet tool run dotnet-stryker -- --project "%_PROJ%" 2>&1 | tee "%_LOG%"

    if errorlevel 1 (
        echo [WARN] Stryker ha restituito un errore per %_PROJ% — controlla %_LOG%
        set "EXITCODE=1"
    ) else (
        echo [OK]   %_PROJ% completato.
    )

    :: Sposta StrykerOutput nella cartella centralizzata
    if exist "%_DIR%\StrykerOutput" (
        if exist "%_OUTDIR%" rmdir /s /q "%_OUTDIR%"
        move "%_DIR%\StrykerOutput" "%_OUTDIR%" >nul
        echo [INFO] Report spostato in: %_OUTDIR%
    ) else (
        echo [WARN] Nessun StrykerOutput trovato in %_DIR%
    )
    echo.
    cd /d "%ROOT%"
    goto :eof

:: ============================================================
:collect_and_exit
    echo.
    echo ============================================================
    echo  Raccolta risultati...
    echo ============================================================

    :: Genera un sommario testuale con i percorsi dei report HTML
    echo BLite Mutation Testing — %DATE% %TIME% > "%SUMMARY%"
    echo Target: %TARGET% >> "%SUMMARY%"
    echo. >> "%SUMMARY%"

    set "FOUND=0"
    for /r "%OUT_ROOT%" %%f in (mutation-report.html) do (
        echo Report: %%f >> "%SUMMARY%"
        set /a "FOUND+=1"
    )

    if "%FOUND%"=="0" (
        echo (nessun report HTML trovato) >> "%SUMMARY%"
    )

    echo.
    echo [INFO] Riepilogo scritto in:
    echo        %SUMMARY%
    echo.

    :: Apri la cartella di output in Explorer
    echo [INFO] Apertura cartella risultati...
    start "" explorer "%OUT_ROOT%"

    echo.
    echo ============================================================
    echo  Fine — exitcode: %EXITCODE%
    echo ============================================================
    exit /b %EXITCODE%
