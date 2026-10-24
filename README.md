# Big Data

## Producer
### Build img producer

```bash
docker compose --profile programmes build producer
```
Re-make this command each time there a modification inside the Dockerfile

### Start kafka

```bash
docker compose up -d kafka kafka-ui
```

Sure kafka is running
```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Create the topic

```bash
 docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic irail --partitions 1
```

### Start the producer

```bash
docker compose run --rm producer
```

### Stop the service
```bash
docker compose down
```