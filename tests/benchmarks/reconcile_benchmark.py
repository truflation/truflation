"""Benchmark for reconcile_dataframes: vectorized vs naive implementations.

This script is not part of the unittest discovery (filename doesn't start with test_)
Run it manually to see a simple timing comparison:

PYTHONPATH=/Users/geeku/Documents/projects/truflation/truflation \
  /Users/geeku/Documents/projects/truflation/truflation/.venv/bin/python tests/benchmarks/reconcile_benchmark.py

"""
import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from truflation.data.exporter import Exporter


def make_data(num_days=50, ids_per_day=20, revisions=3):
    # Create base dataframe with several revisions per (date, id)
    rows = []
    start_date = datetime(2025, 1, 1)
    for d in range(num_days):
        date = (start_date + timedelta(days=d)).date()
        for i in range(ids_per_day):
            for r in range(revisions):
                created_at = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc) + timedelta(days=d, seconds=r)
                value = float(i + r)  # vary value by revision
                rows.append({
                    'date': pd.Timestamp(date),
                    'region': f'region_{i % 5}',
                    'source': f'src_{i % 3}',
                    'value': value,
                    'created_at': created_at.replace(tzinfo=None),
                })
    df_base = pd.DataFrame(rows)

    # Create incoming: one row per date+id with potentially different value
    inc_rows = []
    for d in range(num_days):
        date = (start_date + timedelta(days=d)).date()
        for i in range(ids_per_day):
            # alternate between matching and different value
            val = float(i + (0 if (d + i) % 3 == 0 else 999))
            created_at = datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)
            inc_rows.append({
                'date': pd.Timestamp(date),
                'region': f'region_{i % 5}',
                'source': f'src_{i % 3}',
                'value': val,
                'created_at': created_at.replace(tzinfo=None),
            })
    df_incoming = pd.DataFrame(inc_rows)

    return df_base, df_incoming


def naive_reconcile(df_base, df_incoming):
    # Mimic the older row-wise approach used previously
    # find latest per id combination
    df_base_latest = (
        df_base.sort_values('created_at', ascending=False)
               .groupby(['date', 'region', 'source'], as_index=False)
               .first()
    )

    result_rows = []
    for _, inc in df_incoming.iterrows():
        mask = (
            (df_base_latest['date'] == inc['date']) &
            (df_base_latest['region'] == inc['region']) &
            (df_base_latest['source'] == inc['source'])
        )
        match = df_base_latest[mask]
        if match.empty or match.iloc[0]['value'] != inc['value']:
            result_rows.append(inc)

    return pd.DataFrame(result_rows)


def run_benchmark():
    df_base, df_incoming = make_data(num_days=60, ids_per_day=50, revisions=3)
    exporter = Exporter()

    # Warm up
    _ = exporter.reconcile_dataframes(df_base, df_incoming)

    t0 = time.perf_counter()
    vec = exporter.reconcile_dataframes(df_base, df_incoming)
    t1 = time.perf_counter()

    t2 = time.perf_counter()
    naive = naive_reconcile(df_base, df_incoming)
    t3 = time.perf_counter()

    print("Vectorized rows returned:", len(vec), "time:", round(t1 - t0, 4), "s")
    print("Naive rows returned:     ", len(naive), "time:", round(t3 - t2, 4), "s")
    if len(naive) > 0:
        print("Ratio naive/vectorized:", round((t3 - t2) / (t1 - t0), 2))


if __name__ == '__main__':
    run_benchmark()
