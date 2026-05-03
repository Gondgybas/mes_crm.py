@echo off
chcp 65001 > nul
echo ========================================
echo   Deploy MES CRM na vitaka.ru
echo ========================================
echo.
set KEY=%USERPROFILE%\.ssh\mes_vps

echo Kopèðóåì mes_crm.py na server...
scp -i "%KEY%" "%~dp0mes_crm.py" root@103.74.93.188:/opt/mes/mes_crm.py
if %errorlevel% neq 0 (
    echo OSHIBKA: ne udalos skopirovt fayl!
    pause
    exit /b 1
)
echo OK

echo Perezapuskaem servis mes...
ssh -i "%KEY%" root@103.74.93.188 "chown www-data:www-data /opt/mes/mes_crm.py; systemctl restart mes; sleep 2; systemctl status mes --no-pager"
if %errorlevel% neq 0 (
    echo OSHIBKA: ne udalos perezapustit servis!
    pause
    exit /b 1
)

echo.
echo ========================================
echo   Gotovo! Sayt obnovlen na vitaka.ru
echo ========================================
pause