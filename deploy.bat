@echo off
chcp 65001 > nul
echo ========================================
echo   Деплой MES CRM на vitaka.ru
echo ========================================
echo.
set KEY=%USERPROFILE%\.ssh\mes_vps

echo Копируем файл на сервер...
scp -i "%KEY%" "mes_crm.py" root@103.74.93.188:/opt/mes/mes_crm.py
if %errorlevel% neq 0 (
    echo ОШИБКА: не удалось скопировать файл!
    pause
    exit /b 1
)
echo OK

echo Перезапускаем сервис...
ssh -i "%KEY%" root@103.74.93.188 "chown www-data:www-data /opt/mes/mes_crm.py && systemctl restart mes && sleep 2 && systemctl status mes --no-pager"
if %errorlevel% neq 0 (
    echo ОШИБКА: не удалось перезапустить сервис!
    pause
    exit /b 1
)

echo.
echo ========================================
echo   Готово! Сайт обновлён на vitaka.ru
echo ========================================
pause

