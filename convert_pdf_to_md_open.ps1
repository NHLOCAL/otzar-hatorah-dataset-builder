param(
    [Parameter(Position = 0)]
    [string]$PdfPath
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$Root = $PSScriptRoot
Set-Location -LiteralPath $Root

function Select-PdfFile {
    Add-Type -AssemblyName System.Windows.Forms
    $dialog = New-Object System.Windows.Forms.OpenFileDialog
    $dialog.Filter = "PDF files (*.pdf)|*.pdf|All files (*.*)|*.*"
    $dialog.Title = "Choose PDF to convert"

    if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
        return $dialog.FileName
    }

    return $null
}

function Get-OutputPaths {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourcePdf
    )

    $pdfFullPath = (Resolve-Path -LiteralPath $SourcePdf).ProviderPath
    $sourceRoot = (Resolve-Path -LiteralPath (Join-Path $Root "alonim_dataset\source_data\pdf")).ProviderPath
    $sourceRootPrefix = $sourceRoot.TrimEnd("\") + "\"

    if ($pdfFullPath.StartsWith($sourceRootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePath = $pdfFullPath.Substring($sourceRootPrefix.Length)
    } else {
        $relativePath = Split-Path -Leaf $pdfFullPath
    }

    $relativeJson = [System.IO.Path]::ChangeExtension($relativePath, ".json")
    $relativeMarkdown = [System.IO.Path]::ChangeExtension($relativePath, ".md")

    [pscustomobject]@{
        Pdf = $pdfFullPath
        Json = Join-Path $Root (Join-Path "alonim_dataset\intermediate\docling_json" $relativeJson)
        Markdown = Join-Path $Root (Join-Path "alonim_dataset\intermediate\markdown" $relativeMarkdown)
    }
}

function Open-MarkdownFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$MarkdownPath
    )

    $editorArgument = '"' + $MarkdownPath + '"'

    $notepadPlusPlus = Get-Command "notepad++" -ErrorAction SilentlyContinue
    if ($notepadPlusPlus) {
        Start-Process -FilePath $notepadPlusPlus.Source -ArgumentList $editorArgument
        return
    }

    $installPaths = @(
        (Join-Path $env:ProgramFiles "Notepad++\notepad++.exe"),
        (Join-Path ${env:ProgramFiles(x86)} "Notepad++\notepad++.exe")
    )

    foreach ($path in $installPaths) {
        if ($path -and (Test-Path -LiteralPath $path)) {
            Start-Process -FilePath $path -ArgumentList $editorArgument
            return
        }
    }

    Write-Host "Notepad++ was not found. Opening with Windows Notepad instead."
    Start-Process -FilePath "notepad.exe" -ArgumentList $editorArgument
}

if ([string]::IsNullOrWhiteSpace($PdfPath)) {
    $PdfPath = Select-PdfFile
}

if ([string]::IsNullOrWhiteSpace($PdfPath)) {
    Write-Host "No PDF selected."
    exit 0
}

if (-not (Test-Path -LiteralPath $PdfPath -PathType Leaf)) {
    Write-Host "PDF not found:"
    Write-Host $PdfPath
    exit 1
}

$paths = Get-OutputPaths -SourcePdf $PdfPath
$jsonDir = Split-Path -Parent $paths.Json
$markdownDir = Split-Path -Parent $paths.Markdown
New-Item -ItemType Directory -Force -Path $jsonDir, $markdownDir | Out-Null

Write-Host "JSON target: $($paths.Json)"
Write-Host "Markdown target: $($paths.Markdown)"

Write-Host "Converting PDF to JSON..."
& python "alonim_dataset/scripts/pdf_to_json.py" $paths.Pdf --output-dir "alonim_dataset/intermediate/docling_json"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Converting JSON to Markdown..."
& python "alonim_dataset/scripts/json_to_md.py" $paths.Json -o $paths.Markdown
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

if (-not (Test-Path -LiteralPath $paths.Markdown -PathType Leaf)) {
    Write-Host "Markdown file was not created:"
    Write-Host $paths.Markdown
    exit 1
}

Write-Host "Opening Markdown in Notepad++..."
Open-MarkdownFile -MarkdownPath $paths.Markdown
exit 0
