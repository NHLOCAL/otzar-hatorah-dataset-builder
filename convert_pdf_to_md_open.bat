@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
if "%~1"=="" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%convert_pdf_to_md_open.ps1"
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%convert_pdf_to_md_open.ps1" "%~1"
)
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo Conversion failed.
    pause
)

endlocal
exit /b %EXIT_CODE%
