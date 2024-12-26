@echo off
SET report_file=analyze_code.report.txt

setlocal enabledelayedexpansion enableextensions
set FILE_LIST=
for /R src %%F IN (*.py) do (
    set FILE_LIST=!FILE_LIST! "%%F"
)
set FILE_LIST=%FILE_LIST:~1%

ECHO Analyzing with MYPY
ECHO ==== MYPY ==== >%report_file%
ECHO (Disable false positives with inline comment "# type: ignore[<ERROR_NAME>]") >>%report_file%
mypy %FILE_LIST% >>%report_file%

ECHO Analyzing with BANDIT
ECHO ==== BANDIT ==== >>%report_file%
ECHO (Disable false positives with inline comment "# nosec <ERROR_CODE>") >>%report_file%
bandit %FILE_LIST% 1>>%report_file% 2>NUL

ECHO Analyzing with PYLINT
ECHO ==== PYLINT ==== >>%report_file%
:: Disable rule with --disable (ex: --disable=C0301)
pylint %FILE_LIST% >>%report_file%

ECHO Analyzing with FLAKE8
ECHO ==== FLAKE8 ==== >>%report_file%
:: Disable rule with --extend-ignore (ex: --extend-ignore=E501)
python -m flake8 %FILE_LIST% >>%report_file%
