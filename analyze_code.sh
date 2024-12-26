report_file=analyze_code.report.txt

FILE_LIST=$(find src -type f -iname "*.py" -printf '%p ')

echo Analyzing with MYPY
echo ==== MYPY ==== >${report_file}
echo "(Disable false positives with inline comment '# type: ignore[<ERROR_NAME>]')" >>${report_file}
python3.11 -m mypy $FILE_LIST >>${report_file}

echo Analyzing with BANDIT
echo ==== BANDIT ==== >>${report_file}
echo "(Disable false positives with inline comment '# nosec <ERROR_CODE>')" >>${report_file}
python3.11 -m bandit $FILE_LIST 1>>${report_file} 2>NUL

echo Analyzing with PYLINT
echo ==== PYLINT ==== >>${report_file}
# Disable rule with --disable (ex: --disable=C0301)
python3.11 -m pylint --disable=C0301,W0718,C0114 $FILE_LIST >>${report_file}

echo Analyzing with FLAKE8
echo ==== FLAKE8 ==== >>${report_file}
# Disable rule with --extend-ignore (ex: --extend-ignore=E501)
python3.11 -m flake8 --extend-ignore=E501 $FILE_LIST >>${report_file}
