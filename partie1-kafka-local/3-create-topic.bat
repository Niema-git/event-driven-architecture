@echo off
cd /d C:\kafka
.\bin\windows\kafka-topics.bat --create --topic R1 --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
pause
