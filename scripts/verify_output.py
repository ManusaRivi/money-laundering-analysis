import glob
import logging
import os
import re
from collections import Counter

import numpy as np
import pandas as pd
import yaml

CLIENT_CONFIG_PATH = "../configs/client.yaml"
OUTPUT_DIR_GLOB = "../.output/client*"
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


def expected_query5(trans_df):
    sept_1st = trans_df[(trans_df["Timestamp"] >= "2022/09/01") & (trans_df["Timestamp"] <= "2022/09/06")]
    wire_ach = sept_1st[sept_1st["Payment Format"].isin(["Wire", "ACH"])].copy()
    wire_ach["Converted"] = wire_ach.apply(
        lambda row: row["Amount Paid"] / CONVERSION_RATES[row["Payment Currency"]][row["Timestamp"].split(" ")[0]],
        axis=1,
    )
    filtered = wire_ach[wire_ach["Converted"] < 1.0]
    return filtered[["Amount Paid"]].rename(columns={"Amount Paid": "amount"})


QUERY_BUILDERS = {
    "query1": lambda trans, _: expected_query1(trans),
    "query2": lambda trans, accounts: expected_query2(trans, accounts),
    "query5": lambda trans, _: expected_query5(trans),
}


def build_input_queries(input_file, accounts_file):
    trans_df = pd.read_csv(input_file)
    accounts_df = pd.read_csv(accounts_file)
    return {name: builder(trans_df, accounts_df) for name, builder in QUERY_BUILDERS.items()}


def read_output_queries(output_dir):
    queries = {}
    pattern = re.compile(r"^(query\d+)\.csv$")
    for entry in sorted(os.listdir(output_dir)):
        match = pattern.match(entry)
        if not match:
            continue
        queries[match.group(1)] = pd.read_csv(os.path.join(output_dir, entry))
    return queries


def _row_multiset(df):
    normalized = df.copy()
    for column in normalized.columns:
        if pd.api.types.is_float_dtype(normalized[column]):
            normalized[column] = normalized[column].round(FLOAT_DECIMALS)
    return Counter(tuple(row) for row in normalized.itertuples(index=False, name=None))


def _compare(query_name, expected_df, actual_df):
    logging.info(f"Comparing {query_name}...")
    if list(expected_df.columns) != list(actual_df.columns):
        raise ClientValidationError(
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
    raise ClientValidationError(
        f"{query_name}: row mismatch — "
        f"{sum(missing.values())} missing, {sum(extra.values())} extra. "
        f"Sample missing: {sample_missing}. Sample extra: {sample_extra}."
    )


def verify_client_output(output_dir, expected_queries):
    logging.info(os.path.basename(output_dir))
    actual_queries = read_output_queries(output_dir)

    for query_name, expected_df in expected_queries.items():
        if query_name not in actual_queries:
            raise ClientValidationError(f"{query_name}: missing output file in {output_dir}")
        _compare(query_name, expected_df, actual_queries[query_name])

    logging.info("OK")


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        input_file, accounts_file = resolve_dataset_paths()
        logging.info(f"Computing expected results from {input_file}")
        expected_queries = build_input_queries(input_file, accounts_file)

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
