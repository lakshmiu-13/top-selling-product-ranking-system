import csv
import datetime
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales-transactions")
INPUT_FILE = Path(os.getenv("SALES_CSV", "/app/data/sales.csv"))
SALES_DATASET = os.getenv("SALES_DATASET", "csv").strip().lower()
INSTACART_PRODUCTS_FILE = Path(
    os.getenv(
        "INSTACART_PRODUCTS_CSV",
        "/app/data/instacart/products.csv/products.csv",
    )
)
INSTACART_ORDER_PRODUCTS_FILE = Path(
    os.getenv(
        "INSTACART_ORDER_PRODUCTS_CSV",
        "/app/data/instacart/order_products__prior.csv/order_products__prior.csv",
    )
)


def main():
    producer = connect_kafka()
    try:
        if SALES_DATASET == "csv":
            sent = publish_sales_csv(producer)
        elif SALES_DATASET == "instacart":
            sent = publish_instacart_replay(producer)
        else:
            raise ValueError(
                "SALES_DATASET must be either 'csv' or 'instacart'."
            )

        producer.flush()
        if sent == 0:
            raise ValueError("No sales transactions were published.")
        print("Published {} transaction(s) to Kafka topic '{}'.".format(sent, TOPIC))
    finally:
        producer.close()

def publish_sales_csv(producer, input_file=INPUT_FILE):
    if not input_file.is_file():
        raise FileNotFoundError(
            "Sales transaction CSV not found: {}".format(input_file)
        )

    sent = 0
    with input_file.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        fields = {field.strip() for field in (reader.fieldnames or [])}
        if not {"product_name", "units_sold"}.issubset(fields):
            raise ValueError(
                "Sales CSV must contain product_name and units_sold columns."
            )

        for line_number, row in enumerate(reader, start=2):
            transaction = {
                key.strip(): value for key, value in row.items() if key is not None
            }
            product_name = (transaction.get("product_name") or "").strip()
            try:
                units_sold = int(transaction.get("units_sold", ""))
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "Invalid units_sold value on CSV line {}.".format(line_number)
                ) from error
            if (
                not product_name
                or units_sold < 0
                or "\t" in product_name
                or "\n" in product_name
            ):
                raise ValueError(
                    "CSV line {} must have a single-line product name and "
                    "non-negative units_sold.".format(line_number)
                )

            transaction["product_name"] = product_name
            transaction["units_sold"] = units_sold
            producer.send(TOPIC, transaction).get(timeout=30)
            sent += 1

    return sent


def publish_instacart_replay(
    producer,
    products_file=INSTACART_PRODUCTS_FILE,
    order_products_file=INSTACART_ORDER_PRODUCTS_FILE,
    max_events=None,
    delay_seconds=None,
    max_cycles=None,
):
    """Replay prior order-product rows sequentially in simulated event batches."""
    if max_events is None:
        max_events = int(os.getenv("INSTACART_MAX_EVENTS", "10000"))
    if delay_seconds is None:
        delay_seconds = float(os.getenv("INSTACART_REPLAY_DELAY_SECONDS", "0.05"))
    if max_events < 1:
        raise ValueError("INSTACART_MAX_EVENTS must be a positive integer.")
    if delay_seconds < 0:
        raise ValueError("INSTACART_REPLAY_DELAY_SECONDS cannot be negative.")
    if not products_file.is_file():
        raise FileNotFoundError(
            "Instacart products CSV not found: {}".format(products_file)
        )
    if not order_products_file.is_file():
        raise FileNotFoundError(
            "Instacart order-products CSV not found: {}".format(
                order_products_file
            )
        )

    with products_file.open(newline="", encoding="utf-8-sig") as products_csv:
        products_reader = csv.DictReader(products_csv)
        if not {"product_id", "product_name"}.issubset(
            products_reader.fieldnames or []
        ):
            raise ValueError(
                "Instacart products CSV must contain product_id and product_name."
            )
        products = {
            row["product_id"]: row["product_name"].strip()
            for row in products_reader
            if row.get("product_id") and row.get("product_name")
        }

    total_sent = 0
    batch = 0
    dataset_pass = 0
    with order_products_file.open(
        newline="", encoding="utf-8-sig"
    ) as order_products_csv:
        reader = csv.DictReader(order_products_csv)
        if not {"order_id", "product_id", "add_to_cart_order"}.issubset(
            reader.fieldnames or []
        ):
            raise ValueError(
                "Instacart order-products CSV must contain order_id, "
                "product_id, and add_to_cart_order."
            )

        while max_cycles is None or batch < max_cycles:
            sent_this_batch = 0
            for _ in range(max_events):
                row = next(reader, None)
                if row is None:
                    order_products_csv.seek(0)
                    reader = csv.DictReader(order_products_csv)
                    dataset_pass += 1
                    row = next(reader, None)
                    if row is None:
                        break

                line_number = reader.line_num
                order_id = (row.get("order_id") or "").strip()
                product_id = (row.get("product_id") or "").strip()
                cart_order = (row.get("add_to_cart_order") or "").strip()
                product_name = products.get(product_id)
                if not order_id or not product_id or not cart_order:
                    raise ValueError(
                        "Incomplete Instacart order-product row at line {}."
                        .format(line_number)
                    )
                if not product_name:
                    raise ValueError(
                        "Unknown Instacart product_id '{}' at line {}."
                        .format(product_id, line_number)
                    )

                sold_at = datetime.datetime.now(
                    datetime.timezone.utc
                ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                transaction = {
                    "transaction_id": "instacart-{}-{}-{}-{}".format(
                        dataset_pass, order_id, product_id, cart_order
                    ),
                    "order_id": order_id,
                    "product_id": product_id,
                    "product_name": product_name,
                    "units_sold": 1,
                    "sold_at": sold_at,
                    "source": "instacart-replay",
                }
                producer.send(TOPIC, transaction).get(timeout=30)
                sent_this_batch += 1
                total_sent += 1
                if delay_seconds:
                    time.sleep(delay_seconds)

            if sent_this_batch == 0:
                if total_sent == 0:
                    raise ValueError(
                        "No Instacart order-product records were available."
                    )
                break

            print(
                "Instacart replay batch {} published {} event(s).".format(
                    batch + 1, sent_this_batch
                )
            )
            batch += 1

    return total_sent


def connect_kafka():
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=10000,
            )
        except KafkaError:
            if attempt == 29:
                raise
            time.sleep(2)
    raise RuntimeError("Kafka producer connection attempts ended unexpectedly.")


if __name__ == "__main__":
    main()
