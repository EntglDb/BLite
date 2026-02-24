@echo off
setlocal

set VERSION=%1
if "%VERSION%"=="" set VERSION=1.9.0

set ROOT=%~dp0
set PUBLISH_DIR=%ROOT%publish\win-x64
set DIST_DIR=%ROOT%dist
set WXS=%ROOT%tools\BLite.Studio\installer\windows\BLite.Studio.wxs
set MSI=%DIST_DIR%\BLite.Studio-%VERSION%-win-x64.msi

echo.
echo [1/3] Publishing BLite.Studio v%VERSION%...
dotnet publish "%ROOT%tools\BLite.Studio\BLite.Studio.csproj" ^
    -c Release ^
    -r win-x64 ^
    --self-contained true ^
    -p:Version=%VERSION% ^
    -o "%PUBLISH_DIR%"
if errorlevel 1 ( echo FAILED: publish & exit /b 1 )

echo.
echo [2/3] Building MSI...
if not exist "%DIST_DIR%" mkdir "%DIST_DIR%"
wix build "%WXS%" ^
    -d AppVersion=%VERSION% ^
    "-d SourceDir=%PUBLISH_DIR%" ^
    -arch x64 ^
    -ext WixToolset.UI.wixext ^
    -o "%MSI%"
if errorlevel 1 ( echo FAILED: wix build & exit /b 1 )

echo.
echo [3/3] Done!
echo   MSI : %MSI%
echo.
echo   Install : msiexec /i "%MSI%" /l*v install.log
echo   Silent  : msiexec /i "%MSI%" /qn
echo   Remove  : msiexec /x "%MSI%" /qn
echo.
endlocal
