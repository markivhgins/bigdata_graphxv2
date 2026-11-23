@echo off
setlocal
cd /d "%~dp0.."
if not exist "spark\target\scala-2.12\social-network-graphx_2.12-1.0.0.jar" (
  echo Building Scala GraphX backend...
  pushd spark
  sbt clean package
  if errorlevel 1 (popd & echo ERROR: sbt build failed. & exit /b 1)
  popd
)
if "%SPARK_HOME%"=="" (
  where spark-submit >nul 2>nul
  if errorlevel 1 (echo ERROR: spark-submit not found. Set SPARK_HOME and add %%SPARK_HOME%%\bin to PATH. & exit /b 1)
  set SPARK_SUBMIT=spark-submit
) else (set SPARK_SUBMIT=%SPARK_HOME%\bin\spark-submit.cmd)
if not exist "input\current_edges.csv" (copy /y "data\sample_edges.txt" "input\current_edges.csv" >nul)
"%SPARK_SUBMIT%" --class com.socialanalyzer.SocialNetworkAnalyzer --master local[*] --conf spark.driver.memory=2g "spark\target\scala-2.12\social-network-graphx_2.12-1.0.0.jar" "input\current_edges.csv" "output"
if errorlevel 1 (echo ERROR: Spark GraphX execution failed. See messages above. & exit /b 1)
echo GraphX processing completed.
endlocal
