# Partie 2 — Kafka avec Docker

Démarrer Zookeeper et le broker :

```
docker compose up -d
docker ps
```

Tester (deux terminaux) :

```
docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 --topic R2 --from-beginning
docker exec -it broker kafka-console-producer --bootstrap-server broker:9092 --topic R2
```
