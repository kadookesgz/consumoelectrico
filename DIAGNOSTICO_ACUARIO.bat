@echo off
chcp 65001 >nul
title Diagnóstico Shelly Acuario
echo Probando Shelly PM Mini Gen 3 en 192.168.4.203
echo.
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Continue'; foreach($u in @('http://192.168.4.203/rpc/Shelly.GetDeviceInfo','http://192.168.4.203/rpc/PM1.GetStatus?id=0','http://192.168.4.203/rpc/PM1.GetConfig?id=0')) { Write-Host ('--- '+$u+' ---'); try { $r=Invoke-RestMethod -Uri $u -TimeoutSec 5; $r | ConvertTo-Json -Depth 8 } catch { Write-Host ('ERROR: '+$_.Exception.Message) }; Write-Host '' }"
echo.
echo Si falla, copia el resultado completo para diagnosticarlo.
pause
