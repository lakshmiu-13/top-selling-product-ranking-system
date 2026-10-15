import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def load_producer_module():
    kafka_module = types.ModuleType("kafka")
    kafka_module.KafkaProducer = object
    kafka_errors = types.ModuleType("kafka.errors")
    kafka_errors.KafkaError = Exception

    with mock.patch.dict(
        sys.modules,
        {"kafka": kafka_module, "kafka.errors": kafka_errors},
    ):
        spec = importlib.util.spec_from_file_location(
            "producer_under_test", ROOT / "producer" / "producer.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class FakeFuture:
    def get(self, timeout):
        return None


class FakeProducer:
    def __init__(self):
        self.messages = []

    def send(self, topic, value):
        self.messages.append((topic, value))
        return FakeFuture()


class InstacartProducerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.producer_module = load_producer_module()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.products_file = self.root / "products.csv"
        self.order_products_file = self.root / "order_products.csv"
        self.products_file.write_text(
            "product_id,product_name,aisle_id,department_id\n"
            "11,Blueberries,1,1\n"
            "12,Whole Milk,2,2\n",
            encoding="utf-8",
        )
        self.order_products_file.write_text(
            "order_id,product_id,add_to_cart_order,reordered\n"
            "500,11,1,0\n"
            "500,12,2,1\n",
            encoding="utf-8",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_maps_order_products_to_unit_occurrence_events(self):
        producer = FakeProducer()

        count = self.producer_module.publish_instacart_replay(
            producer,
            self.products_file,
            self.order_products_file,
            max_events=2,
            delay_seconds=0,
            max_cycles=1,
        )

        self.assertEqual(count, 2)
        self.assertEqual(len(producer.messages), 2)
        first = producer.messages[0][1]
        self.assertEqual(first["product_name"], "Blueberries")
        self.assertEqual(first["units_sold"], 1)
        self.assertEqual(first["transaction_id"], "instacart-0-500-11-1")
        self.assertEqual(first["source"], "instacart-replay")
        self.assertTrue(first["sold_at"].endswith("Z"))

    def test_batches_advance_to_the_next_rows(self):
        producer = FakeProducer()

        count = self.producer_module.publish_instacart_replay(
            producer,
            self.products_file,
            self.order_products_file,
            max_events=1,
            delay_seconds=0,
            max_cycles=2,
        )

        self.assertEqual(count, 2)
        self.assertEqual(
            [message[1]["transaction_id"] for message in producer.messages],
            ["instacart-0-500-11-1", "instacart-0-500-12-2"],
        )

    def test_replay_wraps_to_first_row_with_a_new_dataset_pass_id(self):
        producer = FakeProducer()

        count = self.producer_module.publish_instacart_replay(
            producer,
            self.products_file,
            self.order_products_file,
            max_events=2,
            delay_seconds=0,
            max_cycles=2,
        )

        self.assertEqual(count, 4)
        self.assertEqual(
            [message[1]["transaction_id"] for message in producer.messages],
            [
                "instacart-0-500-11-1",
                "instacart-0-500-12-2",
                "instacart-1-500-11-1",
                "instacart-1-500-12-2",
            ],
        )

    def test_restarting_replay_starts_with_same_event_ids(self):
        first_run = FakeProducer()
        second_run = FakeProducer()
        for producer in (first_run, second_run):
            self.producer_module.publish_instacart_replay(
                producer,
                self.products_file,
                self.order_products_file,
                max_events=1,
                delay_seconds=0,
                max_cycles=1,
            )

        self.assertEqual(
            first_run.messages[0][1]["transaction_id"],
            second_run.messages[0][1]["transaction_id"],
        )


if __name__ == "__main__":
    unittest.main()