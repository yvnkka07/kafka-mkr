import os
import time
from typing import Iterable

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# Константи згідно з умовою МКР
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:29092")
REQUEST_TOPIC = "demo-requests"
RESPONSE_TOPIC = "demo-responses"
GROUP_ID = "demo-responder-group"


def wait_for_kafka() -> None:
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            producer.close()
            return
        except NoBrokersAvailable:
            print("Kafka ще недоступна, очікую...", flush=True)
            time.sleep(2)


def create_topics_if_missing(topic_names: Iterable[str]) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="demo-consumer-admin",
    )
    try:
        existing_topics = set(admin.list_topics())
        topics_to_create = [
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
            for topic in topic_names
            if topic not in existing_topics
        ]
        if topics_to_create:
            try:
                admin.create_topics(new_topics=topics_to_create, validate_only=False)
                print(
                    "Створено топіки: " + ", ".join(topic.name for topic in topics_to_create),
                    flush=True,
                )
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def collatz_steps(number: int) -> int:
    if number < 1:
        raise ValueError("Числа для послідовності Колатца мають бути додатними")

    steps = 0
    current = number
    while current != 1:
        if current % 2 == 0:
            current //= 2
        else:
            current = 3 * current + 1
        steps += 1
    return steps


def average_collatz_steps(start: int, finish: int) -> int:
    if start > finish:
        start, finish = finish, start

    total_steps = 0
    numbers_count = finish - start + 1
    for number in range(start, finish + 1):
        total_steps += collatz_steps(number)

    return round(total_steps / numbers_count)


def get_correlation_id(message) -> str:
    headers = dict(message.headers or [])
    raw_value = headers.get("correlation-id")
    return raw_value.decode("utf-8") if raw_value else ""


def main() -> None:
    wait_for_kafka()
    create_topics_if_missing([REQUEST_TOPIC, RESPONSE_TOPIC])

    request_consumer = KafkaConsumer(
        REQUEST_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
    )

    response_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: str(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )

    print(f"Чекаю запитів у '{REQUEST_TOPIC}'.", flush=True)
    for message in request_consumer:
        request = message.value.strip()
        try:
            start_text, finish_text = request.split(",", maxsplit=1)
            start = int(start_text.strip())
            finish = int(finish_text.strip())
            correlation_id = get_correlation_id(message)

            avg_steps = average_collatz_steps(start, finish)

            print(f"<- Отримано запит: start={start} finish={finish}", flush=True)
            response_producer.send(
                RESPONSE_TOPIC,
                key=correlation_id,
                value=avg_steps,
                headers=[("correlation-id", correlation_id.encode("utf-8"))],
            )
            response_producer.flush()
            print(f"-> Надіслано відповідь: avgSteps={avg_steps}", flush=True)
        except Exception as error:
            print(f"Помилка обробки запиту '{request}': {error}", flush=True)


if __name__ == "__main__":
    main()
