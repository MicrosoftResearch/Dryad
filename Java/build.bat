@echo off 
if exist %DRYAD_HOME%\DryadYarnBridge.jar del %DRYAD_HOME%\DryadYarnBridge.jar

@rem compile java classes
javac -d . DryadLinqYarnApp.java DryadAppMaster.java HdfsBridge.java 

@rem package java classes into jar
if errorlevel 1 goto failed
	jar -cvfe %DRYAD_HOME%\DryadYarnBridge.jar com.microsoft.research.DryadLinqYarnApp com GSLHDFS 
	exit /b

:failed
    echo Java Compilation failed. skipping jar creation

