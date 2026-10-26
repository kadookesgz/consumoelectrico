@echo off
chcp 65001 >nul
title Arranque automático del monitor de consumo
cd /d "%~dp0"
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo Solicitar permisos de administrador...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)
echo Este paso crea una tarea de Windows que arranca el monitor al encender el
echo ordenador, sin ventana y sin iniciar sesión, y lo reinicia si se cierra por un error.
echo Antes, cierra la ventana del monitor si lo tienes abierto con INSTALAR_Y_ARRANCAR.bat.
echo.
set "PYW="
for /f "usebackq delims=" %%i in (`py -c "import sys, pathlib; print(pathlib.Path(sys.executable).with_name('pythonw.exe'))"`) do set "PYW=%%i"
if not defined PYW (
  echo No se encuentra Python. Ejecuta antes INSTALAR_Y_ARRANCAR.bat.
  pause
  exit /b 1
)
if not exist "%PYW%" (
  echo No se encuentra %PYW%.
  pause
  exit /b 1
)
py -m pip install -r requirements.txt
powershell -NoProfile -ExecutionPolicy Bypass -Command "$a=New-ScheduledTaskAction -Execute '%PYW%' -Argument '\"%~dp0shelly_pvpc_web.py\"' -WorkingDirectory '%~dp0'; $t=New-ScheduledTaskTrigger -AtStartup; $t.Delay='PT1M'; $s=New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable; $p=New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest; Register-ScheduledTask -TaskName 'Monitor consumo electrico' -Action $a -Trigger $t -Settings $s -Principal $p -Force | Out-Null; Start-ScheduledTask -TaskName 'Monitor consumo electrico'"
if %errorlevel% equ 0 (
  echo.
  echo Tarea creada y arrancada. Abre http://127.0.0.1:5000 en unos segundos.
  echo Los mensajes del programa se guardan en app.log, en esta carpeta.
) else (
  echo No se pudo crear la tarea. Comprueba los permisos de administrador.
)
echo.
pause
