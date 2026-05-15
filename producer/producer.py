import os
import sys
import time
import uuid
from typing import Iterable

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# Константи згідно з умовою МКР
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:29092")
REQUEST_TOPIC = "demo-requests"
RESPONSE_TOPIC = "demo-responses"
REQUEST_START = 10
REQUEST_FINISH = 100
TIMEOUT_MS = 3_000_000  # приблизно 3000 секунд


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
        client_id="demo-producer-admin",
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


def header_value(headers, name: str) -> str:
    values = dict(headers or [])
    raw_value = values.get(name)
    return raw_value.decode("utf-8") if raw_value else ""


def wait_until_subscription_is_ready(consumer: KafkaConsumer) -> None:
    """Гарантує, що підписка на demo-responses активна до надсилання запиту."""
    consumer.poll(timeout_ms=0)
    deadline = time.time() + 30
    while not consumer.assignment():
        consumer.poll(timeout_ms=500)
        if time.time() > deadline:
            raise TimeoutError("Не вдалося активувати підписку на demo-responses")
    consumer.seek_to_end(*consumer.assignment())


def main() -> None:
    wait_for_kafka()
    create_topics_if_missing([REQUEST_TOPIC, RESPONSE_TOPIC])

    correlation_id = str(uuid.uuid4())
    request_body = f"{REQUEST_START},{REQUEST_FINISH}"

    response_consumer = KafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"demo-producer-{correlation_id}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8"),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
    )
    wait_until_subscription_is_ready(response_consumer)

    request_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: value.encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )

    request_producer.send(
        REQUEST_TOPIC,
        key=correlation_id,
        value=request_body,
        headers=[("correlation-id", correlation_id.encode("utf-8"))],
    )
    request_producer.flush()

    print(
        f"-> Запит надіслано: start={REQUEST_START} finish={REQUEST_FINISH} (id={correlation_id})",
        flush=True,
    )

    deadline = time.time() + TIMEOUT_MS / 1000
    try:
        while time.time() < deadline:
            records = response_consumer.poll(timeout_ms=1000)
            for messages in records.values():
                for message in messages:
                    response_correlation_id = header_value(message.headers, "correlation-id")
                    if response_correlation_id == correlation_id:
                        print(f"<- Отримано відповідь: avgSteps={message.value}", flush=True)
                        print("Готово. Контейнер живе.", flush=True)
                        while True:
                            time.sleep(60)

        print("Не отримано відповідь за відведений час.", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        request_producer.close()
        response_consumer.close()


if __name__ == "__main__":
    main()
