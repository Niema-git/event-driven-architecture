@echo off
cd /d C:\kafka
.\bin\windows\kafka-console-consumer.bat --topic R1 --from-beginning --bootstrap-server localhost:9092
