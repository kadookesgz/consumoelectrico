@echo off
chcp 65001 >nul
title Shelly Pro EM-50 PVPC
cd /d "%~dp0"
where py >nul 2>&1
if errorlevel 1 (
  echo No se encuentra Python Launcher. Instala Python 3.11 o superior.
  pause
  exit /b 1
)
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
if errorlevel 1 (
  echo Error instalando dependencias.
  pause
  exit /b 1
)
start "" http://127.0.0.1:5000
py shelly_pvpc_web.py
pause
