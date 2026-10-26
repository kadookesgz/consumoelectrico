@echo off
chcp 65001 >nul
title Permitir acceso al panel Shelly desde la red
echo Este paso abre el puerto TCP 5000 en el Firewall de Windows
echo para permitir conexiones desde otros equipos de la red local.
echo.
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo Solicitar permisos de administrador...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)
netsh advfirewall firewall delete rule name="Shelly PVPC web TCP 5000" >nul 2>&1
netsh advfirewall firewall add rule name="Shelly PVPC web TCP 5000" dir=in action=allow protocol=TCP localport=5000 profile=private
if %errorlevel% equ 0 (
  echo.
  echo Regla creada. Asegurate de que la red de Windows esta configurada como Privada.
) else (
  echo No se pudo crear la regla. Comprueba los permisos de administrador.
)
echo.
pause
