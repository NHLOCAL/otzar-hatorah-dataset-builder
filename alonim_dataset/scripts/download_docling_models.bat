@echo off
setlocal

chcp 65001 >nul

set "SCRIPT_DIR=%~dp0"
set "DEFAULT_ARTIFACTS=%USERPROFILE%\.cache\docling\artifacts"

if not defined DOCLING_ARTIFACTS_PATH (
    set "DOCLING_ARTIFACTS_PATH=%DEFAULT_ARTIFACTS%"
)

set "HF_HUB_DISABLE_XET=1"

echo Using DOCLING_ARTIFACTS_PATH=%DOCLING_ARTIFACTS_PATH%
echo Using HF_HUB_DISABLE_XET=%HF_HUB_DISABLE_XET%
echo.

python -c "import huggingface_hub" >nul 2>nul
if errorlevel 1 (
    echo Missing Python package: huggingface_hub
    echo Install dependencies first:
    echo   python -m pip install -r alonim_dataset\requirements.txt
    exit /b 1
)

python "%SCRIPT_DIR%download_docling_models.py"
if errorlevel 1 exit /b %errorlevel%

setx DOCLING_ARTIFACTS_PATH "%DOCLING_ARTIFACTS_PATH%" >nul
setx HF_HUB_DISABLE_XET "1" >nul

echo.
echo Saved user environment variables:
echo   DOCLING_ARTIFACTS_PATH=%DOCLING_ARTIFACTS_PATH%
echo   HF_HUB_DISABLE_XET=1
echo.
echo Open a new terminal before relying on these variables outside this script.

endlocal
