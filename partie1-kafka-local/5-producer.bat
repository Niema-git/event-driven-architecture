@echo off
cd /d C:\kafka
.\bin\windows\kafka-console-producer.bat --topic R1 --bootstrap-server localhost:9092
