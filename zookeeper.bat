@echo off

set KAFKA_PATH=C:\kafka

echo Starting Zookeeper...
start "Zookeeper" cmd /c %KAFKA_PATH%\bin\windows\zookeeper-server-start.bat %KAFKA_PATH%\config\zookeeper.properties

echo Started Zookeeper