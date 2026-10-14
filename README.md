# Sales pipeline

This repository's Docker Compose setup runs a sales pipeline and interactive
Streamlit dashboard:

1. The Python producer replays Instacart order-product rows from
   `data/instacart/` as simulated sale events by default.
2. It publishes each transaction to Kafka's `sales-transactions` topic.
3. The Python consumer writes each message to HDFS.
4. Hadoop Streaming runs the mapper and reducer and writes the ranked CSV to
   `results/top10_products.csv`.
5. The dashboard reads the results file and presents the product rankings and
   sales analysis in a browser.

Kafka, HDFS, and the pipeline run as Docker Compose services. Producer,
consumer, scheduler, and MapReduce scripts are packaged in the pipeline image;
the dashboard runs in a separate, lightweight Python image built from the same
`Dockerfile`. No local Python, Kafka, Hadoop, or dashboard dependency
installation is required. Pipeline dependencies are kept in
`pipeline-requirements.txt`; Streamlit dashboard dependencies are installed
only from `dashboard/requirements.txt`.

## Run

Install and start Docker Desktop, then run from this directory:

```sh
docker compose up --build
```

By default, the pipeline replays 100 prior Instacart order-product rows from
`data/instacart/` as simulated events, repeating the sample continuously. Each
row is treated as one product occurrence (`units_sold=1`), not a recorded item
quantity or live sale. The dataset does not provide calendar transaction
timestamps, so `sold_at` is assigned at replay time. The raw files are local
inputs and are excluded from Git and the Docker build context. To use the
original sample CSV instead, set `SALES_DATASET=csv`; that mode publishes
`data/sales.csv` once. CSV mode requires `product_name` and `units_sold`.

Compose waits for Kafka, the NameNode, and the DataNode health checks before
starting the pipeline. The scheduler waits for the initial event sample to
reach HDFS, writes the ranked CSV atomically, and runs rankings every 10 minutes
by default. Set `SCHEDULER_INTERVAL_SECONDS` to change the ranking cadence. The
dashboard checks for a new result every 15 seconds and can also be refreshed
manually. If the default host ports are occupied, use alternate ports. This
PowerShell example also speeds up the initial replay for a quicker demo:

```powershell
$env:KAFKA_HOST_PORT = "19092"
$env:NAMENODE_HOST_PORT = "19870"
$env:DASHBOARD_PORT = "18502"
$env:INSTACART_MAX_EVENTS = "20"
$env:INSTACART_REPLAY_DELAY_SECONDS = "0.1"
docker compose up --build
```

Watch the pipeline with `docker compose logs -f pipeline` and open the
dashboard at the configured `DASHBOARD_PORT` (18502 in this example). This demonstrates simulated streaming
and periodic ranking, not current real-world sales. Instacart order rows
represent product occurrence in an order; this source has no explicit quantity
per item.
The dashboard is available at [http://localhost:8502](http://localhost:8502)
by default; set `DASHBOARD_PORT` to use a different available host port. The
NameNode UI is available at [http://localhost:9870](http://localhost:9870).

Stop the services with `Ctrl+C` or `docker compose down`. Kafka and HDFS data
persist in Docker volumes; `docker compose down -v` removes those volumes and
their data.

## MapReduce ranking

HDFS input is one transaction JSON object per line. The mapper emits
tab-separated product and unit pairs; the reducer aggregates products and
outputs CSV columns `rank,product_name,units_sold`, ranked by units sold
descending with product names alphabetically breaking ties. Run the automated
tests with `python -m unittest discover -s tests -v`. See
[MEMBER3_MAPREDUCE_GUIDE.md](MEMBER3_MAPREDUCE_GUIDE.md) for the data contract,
implementation, testing, and Hadoop Streaming details.
