import json

import numpy as np
import pandas as pd
import vaex
from confluent_kafka import Producer

"""
STREAM_MEASUREMENT_COLUMNS = [
    "id",
    "source",
    "image_id",
    "time",
    "ra",
    "dec",
    "flux_peak",
    "flux_int",
    "flux_peak_err",
    "flux_int_err",
    "snr",
]
"""
STREAM_MEASUREMENT_COLUMNS = None

CHUNK_SIZE = 10_000


def open_measurements(path, columns=None):
    """Lazily open measurements.arrow with vaex (memory-mapped, no full load)."""
    df = vaex.open(path)
    if columns:
        df = df[columns]
    return df


def load_sources(path):
    """Read sources.parquet (small enough to hold in memory), indexed by source id."""
    return pd.read_parquet(path)


def _clean(value):
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def row_to_message(row, sources=None):
    """Convert a row dict into a JSON-safe dict, optionally attaching source stats."""
    message = {k: _clean(v) for k, v in row.items()}
    if sources is not None and "source" in message and message["source"] in sources.index:
        src = sources.loc[message["source"]]
        message.update({f"src_{k}": _clean(v) for k, v in src.items()})
    return message


def make_producer(bootstrap_servers="localhost:9092"):
    return Producer({"bootstrap.servers": bootstrap_servers})


def _delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def stream_measurements(
    producer,
    measurements_path,
    topic,
    columns=None,
    sources=None,
    key_field="source",
    chunk_size=CHUNK_SIZE,
):
    """Stream measurements.arrow to a Kafka topic in chunks, without loading it all into memory."""
    if columns is None:
        measurements_df = open_measurements(measurements_path)
    else:
        measurements_df = open_measurements(measurements_path, columns=columns)
    for _, _, chunk in measurements_df.to_pandas_df(chunk_size=chunk_size):
        for _, row in chunk.iterrows():
            message = row_to_message(row.to_dict(), sources=sources)
            producer.produce(
                topic,
                key=str(row[key_field]),
                value=json.dumps(message),
                callback=_delivery_report,
            )
            producer.poll(0)
    producer.flush()


def run(
    measurements_path,
    sources_path,
    topic,
    bootstrap_servers="localhost:9092",
    columns=STREAM_MEASUREMENT_COLUMNS
):
    sources = pd.read_parquet(sources_path)
    producer = make_producer(bootstrap_servers)
    stream_measurements(producer, measurements_path, topic, columns=columns, sources=sources)


if __name__ == "__main__":
    run(
        "example_subset_measurements.arrow",
        "example_subset_sources.parquet",
        topic="casda-as331-filtered.measurements",
    )
