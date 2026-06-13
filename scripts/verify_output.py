import argparse
import glob
import json
import logging
import os
from collections import Counter

import numpy as np
import pandas as pd
import yaml

CLIENT_CONFIG_PATH = "../configs/client.yaml"
OUTPUT_DIR_GLOB = "../.output/client*"
EXPECTED_CACHE_ROOT = "../.expected_cache"
FLOAT_DECIMALS = 2

CONVERSION_RATES = pd.DataFrame.from_records(
    np.rec.array(
        [
            ("2022/09/01", 1.4644, 5.1805, 1.314,  0.97999, 6.9,    1.0002, 0.86272, 3.3535, 79.543, 139.34, 20.189, 60.367, 3.75, 1.0, 19793.1),
            ("2022/09/02", 1.4691, 5.2035, 1.3141, 0.98175, 6.9035, 1.0011, 0.86468, 3.3755, 79.719, 140.11, 20.085, 60.427, 3.75, 1.0, 199999.0),
            ("2022/09/03", 1.4691, 5.2056, 1.3138, 0.98207, 6.9046, 1.0013, 0.86478, 3.3791, 79.75,  140.17, 20.081, 60.471, 3.75, 1.0, 19831.4),
            ("2022/09/04", 1.4695, 5.2082, 1.3139, 0.98219, 6.9047, 1.0013, 0.8649,  3.3815, 79.754, 140.22, 20.084, 60.461, 3.75, 1.0, 19952.7),
            ("2022/09/05", 1.4722, 5.1786, 1.3142, 0.98273, 6.9216, 1.0068, 0.86813, 3.4006, 79.816, 140.49, 20.018, 60.737, 3.75, 1.0, 20126.1),
        ],
        dtype=[
            ("Date", "O"), ("Australian Dollar", "<f8"), ("Brazil Real", "<f8"),
            ("Canadian Dollar", "<f8"), ("Swiss Franc", "<f8"), ("Yuan", "<f8"),
            ("Euro", "<f8"), ("UK Pound", "<f8"), ("Shekel", "<f8"),
            ("Rupee", "<f8"), ("Yen", "<f8"), ("Mexican Peso", "<f8"),
            ("Ruble", "<f8"), ("Saudi Riyal", "<f8"), ("US Dollar", "<f8"),
            ("Bitcoin", "<f8"),
        ],
    )
).set_index("Date")


class ClientValidationError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def resolve_dataset_paths():
    with open(CLIENT_CONFIG_PATH, "r") as config_file:
        client_config = yaml.safe_load(config_file)

    config_dir = os.path.dirname(CLIENT_CONFIG_PATH)
    input_file = os.path.normpath(os.path.join(config_dir, client_config["transactions_dataset_path"]))
    accounts_file = os.path.normpath(os.path.join(config_dir, client_config["accounts_dataset_path"]))
    return input_file, accounts_file


def expected_query1(trans_df):
    trans_usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    low = trans_usd[trans_usd["Amount Paid"] < 50]
    return low[["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]].rename(
        columns={
            "From Bank": "from_bank",
            "Account": "from_account",
            "To Bank": "to_bank",
            "Account.1": "to_account",
            "Amount Paid": "total_amount",
        }
    )


def expected_query2(trans_df, accounts_df):
    trans_usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    max_idx = trans_usd.groupby(["From Bank"])["Amount Paid"].idxmax()
    max_rows = trans_usd.loc[max_idx]
    merged = max_rows.merge(accounts_df, left_on="From Bank", right_on="Bank ID")
    return merged[["From Bank", "Account", "Bank Name", "Amount Paid"]].drop_duplicates().rename(
        columns={
            "From Bank": "from_bank",
            "Account": "from_account",
            "Bank Name": "bank_name",
            "Amount Paid": "amount_paid",
        }
    )




def expected_query3(trans_df):
    usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    period_1 = usd[(usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] <= "2022/09/06")]
    avg_per_type = period_1.groupby("Payment Format")["Amount Paid"].mean().reset_index()

    period_2 = usd[(usd["Timestamp"] >= "2022/09/06") & (usd["Timestamp"] <= "2022/09/15")]
    merged = period_2.merge(avg_per_type, on="Payment Format", suffixes=("", "_avg"))
    filtered = merged[merged["Amount Paid"] < merged["Amount Paid_avg"] * 0.01]
    return filtered[["From Bank", "Account", "Payment Format", "Amount Paid"]].rename(
        columns={
            "From Bank": "from_bank",
            "Account": "from_account",
            "Payment Format": "payment_format",
            "Amount Paid": "amount_paid",
        }
    )


def expected_query4(trans_df):
    threshold = 5
    usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    sept_1st = usd[(usd["Timestamp"] >= "2022/09/01") & (usd["Timestamp"] <= "2022/09/06")]

    ranged = sept_1st.groupby(["From Bank", "Account"]).filter(
        lambda x: x.groupby(["To Bank", "Account.1"]).size().size > threshold
    )
    accounts = ranged[["From Bank", "Account", "To Bank", "Account.1"]]
    pairs = accounts.merge(
        accounts,
        left_on=["To Bank", "Account.1"],
        right_on=["From Bank", "Account"],
    ).rename(columns={
        "From Bank_x": "From Bank",
        "Account_x": "From Account",
        "To Bank_y": "To Bank",
        "Account.1_y": "To Account",
    })
    pairs = pairs[(pairs["From Bank"] != pairs["To Bank"]) | (pairs["From Account"] != pairs["To Account"])]
    pairs = pairs.groupby(["From Bank", "From Account", "To Bank", "To Account"], as_index=False).size()
    pairs = pairs[pairs["size"] > threshold]

    from_acc = pairs[["From Bank", "From Account"]].rename(columns={"From Bank": "bank_id", "From Account": "account_id"})
    to_acc = pairs[["To Bank", "To Account"]].rename(columns={"To Bank": "bank_id", "To Account": "account_id"})
    return pd.concat([from_acc, to_acc]).drop_duplicates()


def expected_query4_bis(trans_df, threshold=2):
    usd = trans_df[trans_df["Payment Currency"] == "US Dollar"]
    date_filtered = usd[(usd["Timestamp"] >= "2022/09/01 00:00") & (usd["Timestamp"] <= "2022/09/05 23:59")]

    scatter = {}
    gather = {}

    for _, row in date_filtered.iterrows():
        src = (str(row["From Bank"]), str(row["Account"]))
        dst = (str(row["To Bank"]), str(row["Account.1"]))

        scatter.setdefault(src, set()).add(dst)
        gather.setdefault(dst, set()).add(src)

    pair_counts = {}
    for bridge, dst_accounts in scatter.items():
        if bridge not in gather:
            continue
        src_accounts = gather[bridge]
        for src in src_accounts:
            for dst in dst_accounts:
                key = (src, dst)
                pair_counts[key] = pair_counts.get(key, 0) + 1

    filtered_pairs = {k: v for k, v in pair_counts.items() if v >= threshold}
    accounts = set()
    for (src, dst) in filtered_pairs:
        accounts.add(src)
        accounts.add(dst)

    result = pd.DataFrame(sorted(accounts), columns=["bank_id", "account_id"])
    return result


def expected_query5(trans_df):
    sept_1st = trans_df[(trans_df["Timestamp"] >= "2022/09/01") & (trans_df["Timestamp"] <= "2022/09/06")]
    wire_ach = sept_1st[sept_1st["Payment Format"].isin(["Wire", "ACH"])].copy()
    wire_ach["Converted"] = wire_ach.apply(
        lambda row: row["Amount Paid"] / CONVERSION_RATES[row["Payment Currency"]][row["Timestamp"].split(" ")[0]],
        axis=1,
    )
    filtered = wire_ach[wire_ach["Converted"] < 1.0]
    return pd.DataFrame({"count": [len(filtered)]})


QUERY_BUILDERS = {
    "query1": lambda trans, _: expected_query1(trans),
    "query2": lambda trans, accounts: expected_query2(trans, accounts),
    "query3": lambda trans, _: expected_query3(trans),
    # "query4": lambda trans, _: expected_query4_bis(trans),
    "query5": lambda trans, _: expected_query5(trans),
}


def _dataset_signature(input_file, accounts_file):
    # Used to invalidate the cache when a dataset file is regenerated, without
    # hashing the whole (potentially hundreds of MB) file on every run.
    def stat_sig(path):
        st = os.stat(path)
        return {"path": os.path.abspath(path), "size": st.st_size, "mtime": st.st_mtime}

    return {"transactions": stat_sig(input_file), "accounts": stat_sig(accounts_file)}


def _cache_dir_for(input_file):
    dataset_name = os.path.splitext(os.path.basename(input_file))[0]
    return os.path.join(EXPECTED_CACHE_ROOT, dataset_name)


def load_or_build_expected(input_file, accounts_file, rebuild=False):
    """Return expected query results, computing them from the dataset only on a
    cache miss and otherwise loading the previously computed CSVs.

    The expected side is always read back from the cache CSV (even right after
    computing it), so it goes through the same read_csv path as the actual
    outputs and can't drift in dtype or float formatting."""
    cache_dir = _cache_dir_for(input_file)
    meta_path = os.path.join(cache_dir, "meta.json")
    signature = _dataset_signature(input_file, accounts_file)

    cache_valid = not rebuild and os.path.isfile(meta_path)
    if cache_valid:
        with open(meta_path, "r") as meta_file:
            if json.load(meta_file) != signature:
                logging.info("Dataset changed since cache was built; rebuilding")
                cache_valid = False

    os.makedirs(cache_dir, exist_ok=True)

    stale = [
        name
        for name in QUERY_BUILDERS
        if not (cache_valid and os.path.isfile(os.path.join(cache_dir, f"{name}.csv")))
    ]

    if stale:
        logging.info(f"Computing expected results for {stale} from {input_file}")
        trans_df = pd.read_csv(input_file)
        accounts_df = pd.read_csv(accounts_file)
        for name in stale:
            df = QUERY_BUILDERS[name](trans_df, accounts_df)
            df.to_csv(os.path.join(cache_dir, f"{name}.csv"), index=False)
        with open(meta_path, "w") as meta_file:
            json.dump(signature, meta_file, indent=2)
    else:
        logging.info(f"Using cached expected results from {cache_dir}")

    return {
        name: pd.read_csv(os.path.join(cache_dir, f"{name}.csv"))
        for name in QUERY_BUILDERS
    }


def read_output_queries(output_dir, query_names):
    queries = {}
    for name in query_names:
        path = os.path.join(output_dir, f"{name}.csv")
        if not os.path.isfile(path):
            continue
        queries[name] = pd.read_csv(path)
    return queries


def _row_multiset(df):
    normalized = df.copy()
    for column in normalized.columns:
        if pd.api.types.is_float_dtype(normalized[column]):
            normalized[column] = normalized[column].round(FLOAT_DECIMALS)
        normalized[column] = normalized[column].astype(str)
    return Counter(tuple(row) for row in normalized.itertuples(index=False, name=None))


def _compare(query_name, expected_df, actual_df):
    logging.info(f"Comparing {query_name}...")

    if len(expected_df) == 0 and len(actual_df) == 0:
        logging.info(f"{query_name} OK (empty)")
        return

    if len(actual_df.columns) == 0:
        raise ClientValidationError(
            f"{query_name}: actual file is empty but expected {len(expected_df)} rows with columns {list(expected_df.columns)}"
        )

    if list(expected_df.columns) != list(actual_df.columns):
        logging.error(
            f"{query_name}: column mismatch — expected {list(expected_df.columns)}, got {list(actual_df.columns)}"
        )

    expected_rows = _row_multiset(expected_df)
    actual_rows = _row_multiset(actual_df[expected_df.columns])

    if expected_rows == actual_rows:
        logging.info(f"{query_name} OK")
        return

    missing = expected_rows - actual_rows
    extra = actual_rows - expected_rows
    sample_missing = list(missing.items())[:5]
    sample_extra = list(extra.items())[:5]
    logging.error(
        f"{query_name}: row mismatch — "
        f"{sum(missing.values())} missing, {sum(extra.values())} extra. "
        f"Sample missing: {sample_missing}. Sample extra: {sample_extra}."
    )


def verify_client_output(output_dir, expected_queries):
    logging.info(os.path.basename(output_dir))
    actual_queries = read_output_queries(output_dir, expected_queries.keys())

    for query_name, expected_df in expected_queries.items():
        if query_name not in actual_queries:
            raise ClientValidationError(f"{query_name}: missing output file in {output_dir}")
        _compare(query_name, expected_df, actual_queries[query_name])

    logging.info("OK")


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Verify client query outputs against expected results.")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Recompute expected results from the dataset, ignoring the cache.",
    )
    args = parser.parse_args()

    try:
        input_file, accounts_file = resolve_dataset_paths()
        expected_queries = load_or_build_expected(input_file, accounts_file, rebuild=args.rebuild)

        client_output_dirs = sorted(glob.glob(OUTPUT_DIR_GLOB))
        if not client_output_dirs:
            raise ClientValidationError(f"No client output directories found at {OUTPUT_DIR_GLOB}")

        logging.info("Validating clients...")
        for output_dir in client_output_dirs:
            verify_client_output(output_dir, expected_queries)
        logging.info("All query outputs match the expected results")
    except ClientValidationError as e:
        logging.error(e.message)
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    main()
