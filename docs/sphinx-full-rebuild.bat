@echo off

echo Rebuilding source directory
RD /S /Q ".\source"
python3.11 -m sphinx-apidoc -f --maxdepth 4 --separate --doc-project "drs.pypeline" --doc-author "DavidRodriguezSoaresCUI" --full -o source ../src/pypeline
python3.11 sphinx-patch-conf.py

echo Rebuilding HTML build
RD /S /Q ".\build"
python3.11 -m sphinx-build source build
echo Done ! Open build/index.html to see documentation