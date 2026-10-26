@echo off
chcp 65001 >nul
title Quitar el arranque automático del monitor de consumo
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo Solicitar permisos de administrador...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)
powershell -NoProfile -ExecutionPolicy Bypass -Command "Stop-ScheduledTask -TaskName 'Monitor consumo electrico' -ErrorAction SilentlyContinue; Unregister-ScheduledTask -TaskName 'Monitor consumo electrico' -Confirm:$false"
if %errorlevel% equ 0 (
  echo Arranque automático quitado y monitor detenido.
) else (
  echo No había ninguna tarea de arranque automático.
)
echo.
pause
