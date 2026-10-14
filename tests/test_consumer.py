import contextlib
import importlib.util
import io
from pathlib import Path
import sys
import types
import unittest
from collections import namedtuple
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def load_consumer_module():
    kafka_module = types.ModuleType("kafka")
    kafka_module.__path__ = []
    kafka_module.KafkaConsumer = object

    kafka_errors = types.ModuleType("kafka.errors")
    kafka_errors.KafkaError = Exception

    kafka_structs = types.ModuleType("kafka.structs")
    kafka_structs.TopicPartition = namedtuple("TopicPartition", "topic partition")
    kafka_structs.OffsetAndMetadata = namedtuple(
        "OffsetAndMetadata", "offset metadata"
    )

    hdfs_module = types.ModuleType("hdfs")
    hdfs_module.InsecureClient = object

    dependencies = {
        "kafka": kafka_module,
        "kafka.errors": kafka_errors,
        "kafka.structs": kafka_structs,
        "hdfs": hdfs_module,
    }
    with mock.patch.dict(sys.modules, dependencies):
        spec = importlib.util.spec_from_file_location(
            "consumer_under_test", ROOT / "consumer" / "consumer.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module, kafka_structs


class FakeConsumer:
    def __init__(self):
        self.commits = []

    def commit(self, offsets):
        self.commits.append(offsets)


class FakeHdfs:
    def __init__(self):
        self.paths = []
        self.contents = {}

    @contextlib.contextmanager
    def write(self, path, encoding, overwrite):
        self.paths.append(path)
        output = io.StringIO()
        yield output
        self.contents[path] = output.getvalue()


class ConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.consumer_module, cls.kafka_structs = load_consumer_module()

    def test_commit_advances_only_the_processed_partition_offset(self):
        consumer = FakeConsumer()
        message = types.SimpleNamespace(
            topic="sales-transactions", partition=2, offset=41
        )

        self.consumer_module.commit_message(consumer, message)

        expected = {
            self.kafka_structs.TopicPartition("sales-transactions", 2):
                self.kafka_structs.OffsetAndMetadata(42, "")
        }
        self.assertEqual(consumer.commits, [expected])

    def test_transaction_id_makes_hdfs_write_idempotent_across_offsets(self):
        hdfs = FakeHdfs()
        transaction = {
            "transaction_id": "TX-001",
            "product_name": "Widget",
            "units_sold": 2,
        }

        self.consumer_module.save_to_hdfs(
            hdfs, transaction, "sales-transactions", 0, 10
        )
        self.consumer_module.save_to_hdfs(
            hdfs, transaction, "sales-transactions", 1, 99
        )

        self.assertEqual(hdfs.paths[0], hdfs.paths[1])
        self.assertEqual(len(hdfs.contents), 1)
        self.assertEqual(
            hdfs.contents[hdfs.paths[0]],
            '{"transaction_id": "TX-001", "product_name": "Widget", '
            '"units_sold": 2}\n',
        )

    def test_records_without_transaction_id_keep_offset_based_paths(self):
        hdfs = FakeHdfs()
        transaction = {"product_name": "Widget", "units_sold": 2}

        self.consumer_module.save_to_hdfs(
            hdfs, transaction, "sales-transactions", 0, 10
        )
        self.consumer_module.save_to_hdfs(
            hdfs, transaction, "sales-transactions", 0, 11
        )

        self.assertNotEqual(hdfs.paths[0], hdfs.paths[1])


if __name__ == "__main__":
    unittest.main()