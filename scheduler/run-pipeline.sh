#!/bin/bash
set -euo pipefail

SALES_CSV="${SALES_CSV:-/app/data/sales.csv}"
SALES_DATASET="${SALES_DATASET:-csv}"
if [[ "$SALES_DATASET" == "instacart" ]]; then
    INSTACART_ORDER_PRODUCTS_CSV="${INSTACART_ORDER_PRODUCTS_CSV:-/app/data/instacart/order_products__prior.csv/order_products__prior.csv}"
    INSTACART_MAX_EVENTS="${INSTACART_MAX_EVENTS:-100}"
    EXPECTED_TRANSACTION_COUNT="$(
        python3 -c 'import csv, sys; limit = int(sys.argv[2]); source = open(sys.argv[1], newline="", encoding="utf-8-sig"); print(sum(1 for _ in zip(range(limit), csv.DictReader(source))))' \
            "$INSTACART_ORDER_PRODUCTS_CSV" "$INSTACART_MAX_EVENTS"
    )"
    export INSTACART_ORDER_PRODUCTS_CSV INSTACART_MAX_EVENTS
elif [[ "$SALES_DATASET" == "csv" ]]; then
    EXPECTED_TRANSACTION_COUNT="$(
        python3 -c 'import csv, sys; source = open(sys.argv[1], newline="", encoding="utf-8-sig"); print(sum(1 for _ in csv.DictReader(source)))' \
            "$SALES_CSV"
    )"
else
    echo "SALES_DATASET must be either 'csv' or 'instacart'." >&2
    exit 1
fi
export SALES_CSV SALES_DATASET EXPECTED_TRANSACTION_COUNT

python3 -u /app/consumer/consumer.py &
consumer_pid=$!

/bin/bash /app/scheduler/run.sh &
scheduler_pid=$!
producer_pid=""

cleanup() {
    local pipeline_pids=("$consumer_pid" "$scheduler_pid")
    if [[ -n "$producer_pid" ]]; then
        pipeline_pids+=("$producer_pid")
    fi
    kill "${pipeline_pids[@]}" 2>/dev/null || true
    wait "${pipeline_pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT

if [[ "$SALES_DATASET" == "instacart" ]]; then
    python3 -u /app/producer/producer.py &
    producer_pid=$!
else
    python3 -u /app/producer/producer.py
fi

set +e
if [[ -n "$producer_pid" ]]; then
    wait -n "$consumer_pid" "$scheduler_pid" "$producer_pid"
else
    wait -n "$consumer_pid" "$scheduler_pid"
fi
status=$?
set -e

if [ "$status" -eq 0 ]; then
    echo "A long-running pipeline process exited unexpectedly." >&2
    exit 1
fi
echo "A long-running pipeline process exited with status $status." >&2
exit "$status"
