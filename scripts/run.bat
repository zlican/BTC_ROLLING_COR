@echo off
cd /d "%~dp0\.."
:loop
go run .
set "EXITCODE=%ERRORLEVEL%"
if "%EXITCODE%"=="0" goto :eof
echo Program exited with code %EXITCODE%, restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto loop