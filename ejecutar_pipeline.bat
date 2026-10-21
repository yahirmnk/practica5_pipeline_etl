@echo off
REM Ejecuta el pipeline ETL. Se puede programar con el Programador de tareas de Windows.
cd /d "%~dp0"
call venv\Scripts\activate.bat
python ejecutar_pipeline.py
exit /b %ERRORLEVEL%
