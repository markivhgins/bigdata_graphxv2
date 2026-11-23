@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Creating Python virtual environment...
  py -3.11 -m venv .venv
  if errorlevel 1 (echo Python 3.11 not found. Install Python 3.11 and enable Add to PATH. & pause & exit /b 1)
)
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
if not exist "spark\target\scala-2.12\social-network-graphx_2.12-1.0.0.jar" (
  echo Building Spark GraphX backend...
  pushd spark
  sbt clean package
  if errorlevel 1 (popd & echo Build failed. Check JDK 17 and sbt installation. & pause & exit /b 1)
  popd
)
if "%SPARK_HOME%"=="" (
  where spark-submit >nul 2>nul
  if errorlevel 1 (echo SPARK_HOME/spark-submit not configured. See README.md. & pause & exit /b 1)
)
streamlit run app.py
endlocal
