
import hashlib
import json
import os
import time
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

from hdfs import InsecureClient
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales-transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sales-hdfs-writer")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_INPUT_DIR = os.getenv("HDFS_INPUT_DIR", "/sales/input")

MAX_CONNECT_ATTEMPTS = 30
MAX_HDFS_WRITE_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2


def connect_hdfs():
    """Connect to HDFS and make sure the input directory exists."""
    client = InsecureClient(HDFS_URL, user="root")

    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            client.makedirs(HDFS_INPUT_DIR)
            print("HDFS input directory ready: {}".format(HDFS_INPUT_DIR))
            return client
        except Exception as error:
            print(
                "HDFS connection attempt {}/{} failed: {}".format(
                    attempt, MAX_CONNECT_ATTEMPTS, error
                )
            )
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            time.sleep(RETRY_DELAY_SECONDS)

    raise RuntimeError("Could not connect to HDFS.")


def connect_kafka():
    """Create a Kafka consumer with automatic commits disabled."""
    for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            print("Connected to Kafka topic: {}".format(TOPIC))
            print("Consumer group: {}".format(GROUP_ID))
            return consumer
        except KafkaError as error:
            print(
                "Kafka connection attempt {}/{} failed: {}".format(
                    attempt, MAX_CONNECT_ATTEMPTS, error
                )
            )
            if attempt == MAX_CONNECT_ATTEMPTS:
                raise
            time.sleep(RETRY_DELAY_SECONDS)

    raise RuntimeError("Could not create Kafka consumer.")


def validate_transaction(message_value):
    """Decode and validate the transaction sent by the producer."""
    try:
        transaction = json.loads(message_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Message is not valid UTF-8 JSON: {}".format(error))

    if not isinstance(transaction, dict):
        raise ValueError("Kafka message must contain a JSON object.")

    product_value = transaction.get("product_name")
    units_value = transaction.get("units_sold")

    if not isinstance(product_value, str):
        raise ValueError("product_name must be text.")

    product = product_value.strip()

    if not product:
        raise ValueError("product_name cannot be empty.")

    if any(char in product for char in ("\t", "\n", "\r")):
        raise ValueError("product_name cannot contain tabs or line breaks.")

    if units_value is None or isinstance(units_value, bool):
        raise ValueError("units_sold must be a whole number.")

    try:
        units_decimal = Decimal(str(units_value))
    except (InvalidOperation, ValueError):
        raise ValueError("units_sold must be a valid number.")

    if (
        not units_decimal.is_finite()
        or units_decimal < 0
        or units_decimal != units_decimal.to_integral_value()
    ):
        raise ValueError("units_sold must be a non-negative whole number.")

    # Preserve transaction_id, sold_at, and any other original fields.
    transaction["product_name"] = product
    transaction["units_sold"] = int(units_decimal)

    return transaction


def save_to_hdfs(hdfs, transaction, topic, partition, offset):
    """Write one transaction file, retrying temporary HDFS failures."""
    transaction_id = transaction.get("transaction_id")
    if isinstance(transaction_id, str) and transaction_id.strip():
        transaction_key = hashlib.sha256(
            transaction_id.strip().encode("utf-8")
        ).hexdigest()
        filename = "{}-transaction-{}.json".format(
            quote(topic, safe=""), transaction_key
        )
    else:
        filename = "{}-{}-{}.json".format(
            quote(topic, safe=""), partition, offset
        )
    path = "{}/{}".format(HDFS_INPUT_DIR.rstrip("/"), filename)
    last_error = None

    for attempt in range(1, MAX_HDFS_WRITE_ATTEMPTS + 1):
        try:
            with hdfs.write(
                path,
                encoding="utf-8",
                overwrite=True,
            ) as output:
                output.write(
                    json.dumps(transaction, ensure_ascii=False) + "\n"
                )

            print("Saved {} to HDFS.".format(filename))
            return

        except Exception as error:
            last_error = error
            print(
                "HDFS write attempt {}/{} failed for {}: {}".format(
                    attempt,
                    MAX_HDFS_WRITE_ATTEMPTS,
                    filename,
                    error,
                )
            )

            if attempt < MAX_HDFS_WRITE_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)

    # Raising an error here prevents the valid message from being committed.
    raise RuntimeError(
        "Failed to save {} after {} attempts: {}".format(
            filename, MAX_HDFS_WRITE_ATTEMPTS, last_error
        )
    )


def commit_message(consumer, message):
    """Commit only the next offset for the message just handled."""
    partition = TopicPartition(message.topic, message.partition)
    consumer.commit(
        {
            partition: OffsetAndMetadata(message.offset + 1, ""),
        }
    )


def main():
    hdfs = connect_hdfs()
    consumer = connect_kafka()

    saved_count = 0
    invalid_count = 0

    print("Consumer is running. Press Ctrl+C to stop.")

    try:
        for message in consumer:
            try:
                transaction = validate_transaction(message.value)

            except ValueError as error:
                print(
                    "Skipping invalid message at partition {}, offset {}: {}"
                    .format(message.partition, message.offset, error)
                )

                # This record is intentionally rejected. Commit its offset
                # so the same invalid record is not processed forever.
                commit_message(consumer, message)
                invalid_count += 1
                continue

            # Save first. Commit only after HDFS confirms the write.
            save_to_hdfs(
                hdfs,
                transaction,
                message.topic,
                message.partition,
                message.offset,
            )

            commit_message(consumer, message)
            saved_count += 1

            print(
                "Progress: saved={}, invalid_skipped={}".format(
                    saved_count, invalid_count
                )
            )

    except KeyboardInterrupt:
        print("\nStop requested.")

    finally:
        consumer.close()
        print(
            "Consumer closed. Saved {} record(s); skipped {} invalid "
            "record(s) in this run.".format(saved_count, invalid_count)
        )


if __name__ == "__main__":
    main()
