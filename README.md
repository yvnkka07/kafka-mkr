# МКР: Apache Kafka — request–reply

Реалізовано два окремі Python-сервіси з власними `Dockerfile`:

- `producer` створює топіки через Kafka Admin API, підписується на `demo-responses` до надсилання запиту, надсилає `10,100` у `demo-requests` з header `correlation-id`, очікує відповідь із тим самим `correlation-id` з таймаутом приблизно 3000 секунд і залишається працювати.
- `consumer` створює ті самі топіки через Kafka Admin API, слухає `demo-requests` у групі `demo-responder-group`, обчислює середню кількість кроків послідовностей Колатца та відповідає у `demo-responses` з тим самим `correlation-id`.

## Структура

```text
kafka-mkr-final/
├── consumer/
│   ├── consumer.py
│   ├── Dockerfile
│   ├── .dockerignore
│   └── requirements.txt
├── producer/
│   ├── producer.py
│   ├── Dockerfile
│   ├── .dockerignore
│   └── requirements.txt
├── docker-compose.yml
├── logs-example.txt
└── README.md
```

## Швидкий запуск через Docker Compose

```bash
docker compose up --build -d
```

## Перевірка логів

```bash
docker logs kafka-producer
docker logs kafka-consumer
```

Очікуваний вивід Producer:

```text
-> Запит надіслано: start=10 finish=100 (id=...)
<- Отримано відповідь: avgSteps=34
Готово. Контейнер живе.
```

Очікуваний вивід Consumer:

```text
Чекаю запитів у 'demo-requests'.
<- Отримано запит: start=10 finish=100
-> Надіслано відповідь: avgSteps=34
```

## Зупинка

```bash
docker compose down -v
```

## Ручний запуск без Compose

### 1. Створити docker-мережу

```bash
docker network create kafka-net
```

### 2. Запустити Kafka KRaft без Zookeeper

```bash
docker run -d --name kafka --network kafka-net -p 9092:9092 \
-e KAFKA_NODE_ID=1 \
-e KAFKA_PROCESS_ROLES=broker,controller \
-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9092 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
-e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
-e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
-v kafka-data:/var/lib/kafka/data \
confluentinc/cp-kafka:7.7.1
```

### 3. Зібрати образи

```bash
docker build -t kafka-demo-consumer -f consumer/Dockerfile consumer
docker build -t kafka-demo-producer -f producer/Dockerfile producer
```

### 4. Запустити сервіси

```bash
docker run -d --name kafka-consumer --network kafka-net kafka-demo-consumer
docker run -d --name kafka-producer --network kafka-net kafka-demo-producer
```

## Відповіді на питання для захисту

1. `correlation-id` — це унікальний ідентифікатор запиту. Він потрібен, щоб Producer міг знайти саме свою відповідь у спільному топіку `demo-responses`.
2. Producer підписується на `demo-responses` до надсилання запиту, щоб не пропустити відповідь, якщо Consumer обробить запит дуже швидко.
3. `localhost:9092` використовується з хост-машини, а `kafka:29092` — між Docker-контейнерами в одній docker-мережі, де `kafka` є DNS-іменем контейнера брокера.
