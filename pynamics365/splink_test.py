import pyarrow
from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import altair as alt
import pandas as pd
from pyarrow import csv, json
import duckdb
json_file = r"C:\Users\DanielLawson\IdeaProjects\MIP-CRM-Migration\data\database-dump\account.json"

df = pd.read_json(json_file, orient="records")
table = pyarrow.Table.from_pandas(df)

df = df[["accountid", "name"]]
settings = {
    "link_type": "link_and_dedupe",
    "blocking_rules_to_generate_predictions": [
        "substr(upper(name), 0, 3) = substr(upper(name), 0, 3)"
    ],
    "unique_id_column_name": "accountid",
    "comparisons": [
        cl.jaro_winkler_at_thresholds("name", [0.9, 0.7], term_frequency_adjustments=True)
    ],
    "retain_matching_columns": True,

}

linker = DuckDBLinker(df, settings, connection=":memory:")

linker.estimate_u_using_random_sampling(max_pairs=5e6)
...
