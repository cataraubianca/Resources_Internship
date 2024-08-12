@echo off

set KAFKA_PATH=C:\kafka

echo Starting Kafka...
start "Kafka" cmd /c %KAFKA_PATH%\bin\windows\kafka-server-start.bat %KAFKA_PATH%\config\server.properties

echo Started Kafka