# Partie 1 — Kafka en local (Windows)

1. Télécharger Kafka 3.9.1 (binaire Scala 2.13) : https://kafka.apache.org/downloads
   et l'extraire dans `C:\kafka` (chemin court, sinon Windows refuse les commandes trop longues).
2. Lancer les scripts de ce dossier, chacun dans son propre terminal, dans cet ordre :

| Script | Rôle |
|---|---|
| `1-start-zookeeper.bat` | Démarre Zookeeper (port 2181) |
| `2-start-kafka.bat` | Démarre le broker Kafka (port 9092) |
| `3-create-topic.bat` | Crée le topic `R1` |
| `4-consumer.bat` | Lance `kafka-console-consumer` sur `R1` |
| `5-producer.bat` | Lance `kafka-console-producer` sur `R1` : chaque ligne tapée apparaît dans le consumer |
