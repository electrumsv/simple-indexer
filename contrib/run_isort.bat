@echo off
set SDKDIR=%~dp0..
py -m pip install isort
py -m isort %SDKDIR%\simple_indexer --force-sort-within-sections --force-alphabetical-sort-within-sections -m 5 --line-length 100
