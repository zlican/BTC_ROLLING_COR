@echo off
:loop
go run .
echo 程序崩溃，5 秒后重启...
timeout /t 5
goto loop
