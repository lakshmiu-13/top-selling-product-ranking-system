# Member 3: Hadoop Streaming MapReduce Guide

## Responsibilities

Member 3 owns the Hadoop Streaming MapReduce portion of this repository:

- Read and validate transaction records from HDFS.
- Emit mapper key/value records compatible with Hadoop Streaming.
- Aggregate units sold by product and calculate the global top ten.
- Keep output deterministic and valid CSV for the dashboard and result file.
- Maintain automated tests and document the MapReduce contract and integration.

The producer, Kafka consumer, Compose services, image, and scheduler are
integrated as part of the full pipeline.

## Existing Architecture

The current pipeline is wired as follows:

1. `producer/producer.py` defaults to replaying rows from the Instacart
  order-products data in `data/instacart/` as JSON Kafka messages to
  `sales-transactions`. A one-shot CSV mode remains available.
2. `consumer/consumer.py` validates each message and writes it to HDFS below
   `/sales/input`.
3. `scheduler/run.sh` runs Hadoop Streaming against that HDFS directory,
   using `mapreduce/mapper.py` and `mapreduce/reducer.py`.
4. The scheduler copies the reducer part file to
   `results/top10_products.csv`.

Docker Compose runs Kafka, HDFS, the pipeline, and dashboard. The pipeline
waits for its configured initial event count to reach HDFS before the first
ranking run.

## Instacart Replay Source

The supplied files are nested under `data/instacart/`:

- `products.csv/products.csv` maps product IDs to names.
- `order_products__prior.csv/order_products__prior.csv` contains order ID,
  product ID, and add-to-cart order.
- `orders.csv/orders.csv` contains order day and hour, but no calendar date.

The producer joins the first two files and treats each order-product row as one
product occurrence with `units_sold=1`. It does not infer package quantity.
Because the source has no true transaction timestamp, each event gets a UTC
timestamp at publish time. The producer defaults to 100 rows per batch and
continues from the next source row in the following batch. At end of file it
wraps to the beginning and advances the dataset-pass ID. `INSTACART_MAX_EVENTS`
sets batch size and `INSTACART_REPLAY_DELAY_SECONDS` controls event pacing.
Restarting begins from the first source row, so IDs for already-seen rows in
the same dataset pass overwrite their previous HDFS files.

This is a streaming demonstration using historical Instacart data, not a live
Instacart feed. The raw files are intentionally excluded from Git and Docker
build context; Compose mounts the local `data/` directory into the pipeline.
Set `SALES_DATASET=csv` to use the original one-shot `data/sales.csv` producer
instead.

To inject one manual event while Compose is running, start
`docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh
--bootstrap-server kafka:9092 --topic sales-transactions` and enter a JSON
object with `transaction_id`, `product_name`, and nonnegative integer
`units_sold`. It will be consumed into HDFS and included in a subsequent
10-minute ranking run.

## Input Contract: HDFS JSON Lines

The consumer creates a distinct `.json` file for each Kafka topic, partition,
and offset. Each file contains one UTF-8 JSON object followed by a newline.
The consumer preserves transaction fields and normalizes `product_name` and
`units_sold`. For example:

```json
{"transaction_id":"TX-001","product_name":"Wireless Headphones","units_sold":900,"sold_at":"2026-10-01T09:15:00Z"}
```

The mapper requires:

- A JSON object with a non-empty string `product_name`.
- `units_sold` represented as a nonnegative JSON integer. Booleans and JSON
  floating-point values are rejected, including values such as `2.0`.
- A product name without tabs or line breaks, because tabs delimit the
  Streaming key/value record and line breaks delimit input records.

Other transaction fields are ignored by aggregation. The mapper does not
coerce names or quantities into valid values; malformed records fail with a
line-numbered diagnostic on standard error and a nonzero exit status.

## Mapper

`mapreduce/mapper.py` reads one JSON object per input line, validates the
required fields, trims surrounding whitespace from the product name, and emits
one record per transaction:

```text
product_name<TAB>units_sold
```

Example:

```text
Pear<TAB>4
Apple<TAB>3
Pear<TAB>5
```

Hadoop Streaming uses the text before the first tab as the map-output key, so
both Pear records are sent to the same reducer key.

## Reducer and Ranking

`mapreduce/reducer.py` validates each tab-delimited mapper record and sums its
nonnegative integer quantity by product. It then sorts all product totals by:

1. Total units sold, descending.
2. Product name, ascending, to make equal totals deterministic.

It keeps the first ten products, assigns ranks starting at 1, and writes CSV
using Python's `csv` module. CSV quoting correctly preserves commas and quotes
within product names. The output columns are exactly:

```text
rank,product_name,units_sold
```

With the example mapper records above, the reducer output is:

```csv
rank,product_name,units_sold
1,Pear,9
2,Apple,3
```

Fewer than ten distinct products are all returned. Empty reducer input produces
the header only.

## Run Locally

From the repository root, with Python 3 available, provide JSONL to the mapper
and pipe its tab-delimited output to the reducer:

```sh
python mapreduce/mapper.py < transactions.jsonl | python mapreduce/reducer.py
```

The result is CSV on standard output. A mapper or reducer validation error is
reported on standard error and exits nonzero. The scripts use only Python
standard-library modules.

## Automated Tests

Run the test suite from the repository root:

```sh
python -m unittest discover -s tests -v
```

`tests/test_mapreduce.py` uses `unittest` and launches the actual mapper and
reducer scripts as subprocesses. It covers valid transactions, required
fields, invalid JSON and record shapes, product-name validation, boolean,
fractional and negative quantities, aggregation, ranking and tie order, top-ten
truncation, fewer than ten products, empty input, malformed reducer records,
CSV escaping, and a complete mapper-to-reducer run. `tests/test_consumer.py`
checks precise Kafka offset commits and stable HDFS paths on replay with
mocked Kafka and HDFS dependencies. `tests/test_producer.py` verifies the
Instacart product join, replay event fields, sample limits, and stable IDs.

**Verified result for this implementation:** 26 tests ran and passed; 0 failed.
Command: `python -m unittest discover -s tests -v`.

## Hadoop Streaming Integration

The existing `scheduler/run.sh` locates the Hadoop Streaming JAR under
`HADOOP_HOME`, submits `/sales/input`, and distributes the scripts with
`-file`. The command invokes them as `python3 mapper.py` and `python3
reducer.py`, writes to `/sales/output/top-products`, and concatenates the part
file into `/app/results/top10_products.csv`.

The scheduler sets `mapreduce.job.reduces=1`. **Keep one reducer** for this
implementation: each reducer independently sees only its assigned keys, so
multiple reducers would each produce a partial ranking and CSV header. A
single reducer receives all product keys, calculates the global totals and
top ten, and produces one correctly ranked CSV.

With the default `SCHEDULER_INTERVAL_SECONDS=600`, ranking runs are scheduled
on a 10-minute start-to-start cadence after the first eligible run. Runtime of
the MapReduce job is subtracted from the wait; if a job itself takes longer
than 10 minutes, the next run starts as soon as it finishes. The dashboard
polls the output CSV every 15 seconds, so it should display a completed ranking
within about 15 seconds instead of waiting for another 10-minute UI refresh.

## Docker Compatibility

The pipeline image is based on `bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8`,
installs Python 3 and `pipeline-requirements.txt`, and copies `mapreduce/` into
the image. Raw `data/instacart/` files are excluded from the Docker build
context and supplied through the Compose bind mount. The mapper and reducer
themselves need only Python's standard library. Hadoop Streaming must be able
to find `python3` on its task PATH and the scripts must remain compatible with
the Python version provided by the image. The test suite uses
`subprocess.run` with `universal_newlines` for compatibility with older Python
3 releases.

The tests validate script behavior locally; they do not start Docker, Kafka,
or Hadoop. Consumer unit tests mock Kafka and HDFS, including offset commits
and replay-safe writes. A Compose run is a separate end-to-end integration
check.

## Integration Dependencies and Known Risks

- **Producer/data:** Instacart order-product rows become one unit-occurrence
  event each. This is a proxy for product counts because the source has no
  explicit quantity; replay timestamps are synthetic. The original CSV mode
  still requires `product_name` and `units_sold` and publishes once.
- **Member 2 consumer:** writes the records in the JSONL format described
  above, preserving transaction fields. If its field names, normalization, or
  HDFS directory change, update and retest the mapper contract with the team.
- **Kafka offset commits:** the consumer now explicitly commits only the next
  offset for the message just processed, and only for that message's topic and
  partition. HDFS is written before committing; a crash in between can replay
  a message, so HDFS writes use a stable transaction-ID filename when the
  record has a non-empty string `transaction_id`.
- **Repeated ingestion:** the Instacart producer advances through consecutive
  order-product rows in each batch. At EOF it wraps to row 1 and increments the
  dataset-pass ID, so a later pass represents new simulated occurrences. A
  process restart begins at row 1/pass zero; those event IDs overwrite that
  pass's files, while any later dataset-pass files from an earlier run remain
  in HDFS. Records without IDs use Kafka topic/partition/offset filenames and
  are not replay-idempotent.
- **Legacy HDFS data:** existing offset-named files from earlier runs are not
  automatically migrated or deleted. Back them up and deliberately clear
  `/sales/input` (or reset Docker volumes only when their data can be discarded)
  before expecting a clean recomputation. Do not remove persisted volumes
  without confirming that their Kafka/HDFS contents are disposable.
- **Legacy results/HDFS:** the result artifact previously contained inflated
  totals, and old offset-named HDFS files are not automatically migrated. The
  isolated demo run regenerated the local CSV from a fresh HDFS volume. Back up
  and deliberately clear old `/sales/input` data before expecting clean totals
  when reusing an older volume.
- **Verification scope:** both Docker images built and the isolated Compose
  pipeline completed a Hadoop Streaming ranking from 100 Instacart replay
  events. The dashboard health endpoint returned HTTP 200. The full 10-minute
  recurring interval was not waited through during verification.

## Viva Questions and Answers

**Why use JSON Lines for HDFS input?**
Each transaction is a self-contained JSON object on one line, so Hadoop
Streaming can process records line by line while retaining the producer's
transaction fields.

**Why is product name the mapper key?**
The ranking aggregates quantities per product. Using the product name as the
key lets Hadoop shuffle records for the same product to the same reducer.

**Why validate booleans separately from integers?**
In Python, `bool` is a subclass of `int`. Explicitly rejecting booleans ensures
JSON `true` and `false` cannot be interpreted as unit counts.

**Why reject floating-point values instead of converting them?**
Converting a fraction to an integer can silently change a sale quantity. This
contract accepts only whole JSON integer values and fails malformed input
clearly.

**Why must there be exactly one reducer?**
The global top ten requires totals for every product. Multiple reducers would
produce independent partial rankings; one reducer sees all product keys and
creates one global result.

**How are ties handled?**
Equal totals are ordered alphabetically by product name, which makes results
repeatable.

**How is a product name containing a comma kept valid in output?**
The reducer writes rows with Python's CSV writer, which quotes fields when
needed and escapes embedded quotes.

**What is the difference between mapper output and final output?**
Mapper output is tab-delimited key/value text for Hadoop's shuffle. Reducer
output is CSV intended for the result file and dashboard.

**What integration issues remain?**
The replay is simulated historical data rather than a live feed, and old
offset-named HDFS files still need deliberate cleanup before trusting a fresh
total. The consumer's current commits target individual processed offsets.

## Pull Request Instructions

Before creating a follow-up PR, verify the branch and stage only the intended
dataset-integration and replay-safety files:

```sh
git status --short --branch
git add .dockerignore .gitignore docker-compose.yml producer/producer.py consumer/consumer.py scheduler/run-pipeline.sh scheduler/run.sh dashboard/app.py tests/test_producer.py tests/test_consumer.py README.md MEMBER3_MAPREDUCE_GUIDE.md
git commit -m "Add Instacart replay demo and safe ingestion"
git push -u origin fix/replay-safe-ingestion
```

Then create a GitHub Pull Request from `fix/replay-safe-ingestion` into
`master`. Include the test command and result, summarize the JSONL-to-CSV
contract changes, and call out the simulated dataset semantics and legacy HDFS
cleanup requirement. Review the staged diff to ensure the raw dataset and
generated results are not included. Do not merge until the team has reviewed
the integration dependencies.
