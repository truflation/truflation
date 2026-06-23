import unittest
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from truflation.data.exporter import Exporter

# Regression test for a real production bug: pandas.util.hash_pandas_object's
# categorize=True codepath (factorize-based hashing of object-dtype columns)
# was proven to return a *different* hash for the *same* value depending on
# what else shared the array at scale - confirmed directly against a real
# 17M-row table where reconcile_dataframes(df_base, df_incoming) flipped from
# correctly returning 0 new rows to wrongly returning duplicates as "new"
# somewhere between 668,055 and 668,056 base rows, with no change in the row
# content itself. reconcile_dataframes must never again rely on
# hash_pandas_object/set-membership for row equality; it must use an exact-key
# merge instead. This test exercises dedup at a scale far beyond what the
# small fixture-based tests cover.


class TestReconcileScale(unittest.TestCase):
    def setUp(self):
        self.exporter = Exporter()

    def test_exact_duplicates_not_reinserted_at_scale(self):
        n_unique = 200_000
        repeat = 5  # mimics the ~91% duplicate-row rate seen in production

        rng = np.random.default_rng(0)
        dates = pd.date_range("2015-01-01", periods=3000, freq="D")
        unique_df = pd.DataFrame({
            "date": dates[rng.integers(0, len(dates), n_unique)],
            "category": [f"cat_{i % 80}" for i in range(n_unique)],
            "subcategory": [f"sub_{i % 40}" for i in range(n_unique)],
            "location": [f"loc_{i % 15}" for i in range(n_unique)],
            "value": rng.uniform(0, 200, n_unique),
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None),
        })

        df_base = pd.concat([unique_df] * repeat, ignore_index=True)
        self.assertGreater(len(df_base), 668_056 // 6)  # meaningfully large, not a 10-row fixture

        # incoming: one row that is bit-identical to an existing stored row
        target = unique_df.iloc[12345]
        df_incoming = pd.DataFrame([{
            "date": target["date"],
            "category": target["category"],
            "subcategory": target["subcategory"],
            "location": target["location"],
            "value": target["value"],
            "created_at": datetime(2026, 6, 22, tzinfo=timezone.utc).replace(tzinfo=None),
        }])

        result = self.exporter.reconcile_dataframes(df_base, df_incoming)
        self.assertTrue(
            result.empty,
            f"Bit-identical duplicate row was wrongly reinserted at scale "
            f"(returned {len(result)} new row(s))"
        )

    def test_genuinely_new_row_still_detected_at_scale(self):
        n_unique = 200_000
        rng = np.random.default_rng(1)
        dates = pd.date_range("2015-01-01", periods=3000, freq="D")
        df_base = pd.DataFrame({
            "date": dates[rng.integers(0, len(dates), n_unique)],
            "category": [f"cat_{i % 80}" for i in range(n_unique)],
            "subcategory": [f"sub_{i % 40}" for i in range(n_unique)],
            "location": [f"loc_{i % 15}" for i in range(n_unique)],
            "value": rng.uniform(0, 200, n_unique),
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None),
        })

        df_incoming = pd.DataFrame([{
            "date": pd.Timestamp("2030-01-01"),
            "category": "brand_new_category",
            "subcategory": "brand_new_subcategory",
            "location": "loc_0",
            "value": 999.999,
            "created_at": datetime(2026, 6, 22, tzinfo=timezone.utc).replace(tzinfo=None),
        }])

        result = self.exporter.reconcile_dataframes(df_base, df_incoming)
        self.assertFalse(result.empty, "Genuinely new row was wrongly dropped at scale")
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
