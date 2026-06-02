@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
pushd "%ROOT%" >nul

if "%~1"=="" (
    call :choose_pdf
) else (
    set "PDF_PATH=%~1"
)

if not defined PDF_PATH (
    echo No PDF selected.
    goto :done
)

if not exist "%PDF_PATH%" (
    echo PDF not found:
    echo "%PDF_PATH%"
    goto :fail
)

call :calculate_output_paths "%PDF_PATH%"

if not defined JSON_PATH (
    echo Could not calculate JSON output path.
    goto :fail
)

if not defined MD_PATH (
    echo Could not calculate Markdown output path.
    goto :fail
)

echo Converting PDF to JSON...
python alonim_dataset/scripts/pdf_to_json.py "%PDF_PATH%" --output-dir "alonim_dataset/intermediate/docling_json"
if errorlevel 1 goto :fail

echo Converting JSON to Markdown...
python alonim_dataset/scripts/json_to_md.py "%JSON_PATH%" -o "%MD_PATH%"
if errorlevel 1 goto :fail

if not exist "%MD_PATH%" (
    echo Markdown file was not created:
    echo "%MD_PATH%"
    goto :fail
)

echo Opening Markdown in Notepad++...
where notepad++ >nul 2>nul
if not errorlevel 1 (
    start "" notepad++ "%MD_PATH%"
    goto :done
)

if exist "%ProgramFiles%\Notepad++\notepad++.exe" (
    start "" "%ProgramFiles%\Notepad++\notepad++.exe" "%MD_PATH%"
    goto :done
)

if exist "%ProgramFiles(x86)%\Notepad++\notepad++.exe" (
    start "" "%ProgramFiles(x86)%\Notepad++\notepad++.exe" "%MD_PATH%"
    goto :done
)

echo Notepad++ was not found. Opening with Windows Notepad instead.
start "" notepad "%MD_PATH%"
goto :done

:fail
echo.
echo Conversion failed.
pause
popd >nul
endlocal
exit /b 1

:done
popd >nul
endlocal
exit /b 0

:choose_pdf
for /f "usebackq delims=" %%F in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new(); Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.OpenFileDialog; $d.Filter = 'PDF files (*.pdf)|*.pdf|All files (*.*)|*.*'; $d.Title = 'Choose PDF to convert'; if ($d.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) { $d.FileName }"`) do set "PDF_PATH=%%F"
exit /b 0

:calculate_output_paths
for /f "usebackq delims=" %%F in (`python -c "from pathlib import Path; import sys; sys.path.insert(0, r'alonim_dataset/scripts'); import _bootstrap; from alonim.docling_convert import docling_json_output_path; from alonim.markdown_convert import _default_markdown_output_path; json_path = docling_json_output_path(Path(sys.argv[1]), Path(r'alonim_dataset/intermediate/docling_json')); print(json_path); print(_default_markdown_output_path(json_path))" "%~1"`) do (
    if not defined JSON_PATH (
        set "JSON_PATH=%%F"
    ) else if not defined MD_PATH (
        set "MD_PATH=%%F"
    )
)
exit /b 0
