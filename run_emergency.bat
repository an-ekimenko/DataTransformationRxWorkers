@echo off
title Emergency Restart - RxDataTransformationWorkers
echo =======================================================================
echo     RUNNING INTEGRATION WORKERS IN EMERGENCY DISASTER RECOVERY MODE
echo     Schedule constraints are bypassed. Retry policies remain active.
echo =======================================================================
echo.

:: Проверка наличия исполняемого файла перед запуском
if not exist "AppLauncher.jar" (
    color 0C
    echo ERROR: AppLauncher.jar was not found in the root directory!
    echo Please execute build.bat first to compile and package the modules.
    echo.
    pause
    exit /b 1
)

:: Запуск JVM с передачей кризисного аргумента xn
java -jar AppLauncher.jar xn

echo.
echo =======================================================================
echo     Emergency worker tasks and all connection retry policies completed.
echo =======================================================================
pause
