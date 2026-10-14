#!/bin/bash
set -euo pipefail

INPUT_PATH="${HDFS_INPUT_DIR:-/sales/input}"
OUTPUT_PATH="${HDFS_OUTPUT_DIR:-/sales/output/top-products}"
RESULT_PATH="${RESULT_PATH:-/app/results/top10_products.csv}"
TEMP_RESULT="${RESULT_PATH}.tmp"
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop-3.2.1}"
INTERVAL="${SCHEDULER_INTERVAL_SECONDS:-60}"
POLL_INTERVAL="${SCHEDULER_EMPTY_POLL_SECONDS:-5}"
EXPECTED_COUNT="${EXPECTED_TRANSACTION_COUNT:-0}"
STREAMING_JAR=$(find "$HADOOP_HOME" -path '*/share/hadoop/tools/lib/hadoop-streaming-*.jar' -print -quit)

if ! [[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
    echo "SCHEDULER_INTERVAL_SECONDS must be a positive integer." >&2
    exit 1
fi
if ! [[ "$POLL_INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
    echo "SCHEDULER_EMPTY_POLL_SECONDS must be a positive integer." >&2
    exit 1
fi
if ! [[ "$EXPECTED_COUNT" =~ ^[0-9]+$ ]]; then
    echo "EXPECTED_TRANSACTION_COUNT must be a non-negative integer." >&2
    exit 1
fi
if [ -z "$STREAMING_JAR" ]; then
    echo "Could not find the Hadoop Streaming JAR under $HADOOP_HOME." >&2
    exit 1
fi

mkdir -p "$(dirname "$RESULT_PATH")"
trap 'rm -f "$TEMP_RESULT"' EXIT

while true; do
    cycle_started=$SECONDS
    input_count=0
    if hdfs dfs -test -d "$INPUT_PATH" 2>/dev/null; then
        input_listing=$(hdfs dfs -ls "$INPUT_PATH" 2>/dev/null) || input_listing=""
        input_count=$(printf '%s\n' "$input_listing" \
            | awk '$NF ~ /\.json$/ { count++ } END { print count + 0 }')
    fi

    if [ "$input_count" -gt 0 ] && [ "$input_count" -ge "$EXPECTED_COUNT" ]; then
        hdfs dfs -rm -r -f "$OUTPUT_PATH"
        hadoop jar "$STREAMING_JAR" \
            -D mapreduce.framework.name=local \
            -D mapreduce.job.reduces=1 \
            -input "$INPUT_PATH" \
            -output "$OUTPUT_PATH" \
            -mapper "python3 mapper.py" \
            -reducer "python3 reducer.py" \
            -file /app/mapreduce/mapper.py \
            -file /app/mapreduce/reducer.py

        hdfs dfs -cat "$OUTPUT_PATH"/part-* | sed 's/\t$//' > "$TEMP_RESULT"
        mv "$TEMP_RESULT" "$RESULT_PATH"
        echo "Updated product rankings at $RESULT_PATH."
        elapsed=$((SECONDS - cycle_started))
        remaining=$((INTERVAL - elapsed))
        if [ "$remaining" -gt 0 ]; then
            sleep "$remaining"
        fi
    else
        echo "Waiting for sales transactions in HDFS at $INPUT_PATH (${input_count}/${EXPECTED_COUNT} expected transaction files); checking again in ${POLL_INTERVAL}s."
        sleep "$POLL_INTERVAL"
    fi
done
