import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pyarrow as pa
import pandas as pd

from pynamics365.extract_to_load_table import load_to_table_replace, load_to_table
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.duckdb import duckdb_comparison_library as cl

crm_data_path = Path("../data/cleaned")
salesloft_data_path = Path("../data/salesloft_cleaned")


def get_top_level_crm_files(crm_data_path=crm_data_path):
    crm_cleaned_files = {}
    for file in crm_data_path.rglob("*.csv"):
        entity_name_pat = re.compile(r"(\w+)_L(\d+)_Cleaned.csv")
        match = entity_name_pat.search(file.name)
        if match:
            entity_name = match.group(1)
            level_num = match.group(2)
            if entity_name not in crm_cleaned_files:
                crm_cleaned_files[entity_name] = {}
            crm_cleaned_files[entity_name][int(level_num)] = file
    # For each key in crm_cleaned_files get the highest level number
    crm_top_level_cleaned_files = {}
    for entity, files in crm_cleaned_files.items():
        top_level = max(files.keys())
        if entity not in crm_top_level_cleaned_files:
            crm_top_level_cleaned_files[entity] = {}
        crm_top_level_cleaned_files[entity]["path"] = files[top_level]
    drop_keys = []
    for key in crm_top_level_cleaned_files.keys():
        if "_L" in key:
            drop_keys.append(key)
    for key in drop_keys:
        crm_top_level_cleaned_files.pop(key)
    return crm_top_level_cleaned_files


def get_top_level_salesloft_files(salesloft_data_path=salesloft_data_path):
    salesloft_cleaned_files = {}
    for file in salesloft_data_path.rglob("*.csv"):
        entity_name_pat = re.compile(r"(.*)_L(\d+)_Cleaned.csv")
        match = entity_name_pat.search(file.name)
        if match:
            entity_name = match.group(1)
            level_num = match.group(2)
            if entity_name not in salesloft_cleaned_files:
                salesloft_cleaned_files[entity_name] = {}
            salesloft_cleaned_files[entity_name][int(level_num)] = file
    salesloft_top_level_cleaned_files = {}
    for entity, files in salesloft_cleaned_files.items():
        top_level = max(files.keys())
        if entity not in salesloft_top_level_cleaned_files:
            salesloft_top_level_cleaned_files[entity] = {}
        salesloft_top_level_cleaned_files[entity]["path"] = files[top_level]
    return salesloft_top_level_cleaned_files


def top_level_files_to_df(top_level_files, drop_empty=True):
    for entity, file in top_level_files.items():
        df = pd.read_csv(file["path"], low_memory=False)
        if "id" in df.columns:
            df.set_index("id", inplace=True)
        if f"{entity}id" in df.columns:
            df.set_index(f"{entity}id", inplace=True)
        if f"activityid" in df.columns:
            df.set_index(f"activityid", inplace=True)
        top_level_files[entity]["df"] = df
    if drop_empty:
        drop_list = []
        for entity, file in top_level_files.items():
            if file["df"].empty:
                drop_list.append(entity)
        for entity in drop_list:
            top_level_files.pop(entity)
    return top_level_files


def load_top_level_files_to_db(entity, df, prefix="crm", schema="cleaned"):
    load_to_table(df, f"{prefix}_{entity}", schema=schema)


def top_level_crm_salesloft_to_sql():
    crm_files = top_level_files_to_df(get_top_level_crm_files())
    salesloft_files = top_level_files_to_df(get_top_level_salesloft_files())
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:

        for entity, file in salesloft_files.items():
            futures.append(
                executor.submit(
                    load_top_level_files_to_db, entity, file["df"], prefix="salesloft"
                )
            )
        for entity, file in crm_files.items():
            futures.append(
                executor.submit(load_top_level_files_to_db, entity, file["df"], prefix="crm")
            )
    ...


def get_crm_sl_accounts_dfs():
    crm_top_level_files = get_top_level_crm_files()
    crma_file = {k:v for k,v in crm_top_level_files.items() if k == "account"}
    df_crma = top_level_files_to_df(crma_file)["account"]["df"]
    salesloft_top_level_files = get_top_level_salesloft_files()
    sla_file = {k:v for k,v in salesloft_top_level_files.items() if k == "accounts"}
    df_sla = top_level_files_to_df(sla_file)["accounts"]["df"]

    df_crma.reset_index(inplace=True)
    df_sla.reset_index(inplace=True)

    crma_cols = {
        "address1_stateorprovince": "state",
        "address1_city": "city",
        "accountid": "unique_id"
    }
    df_crma.rename(columns=crma_cols, inplace=True)
    sla_cols = {
        "website": "websiteurl",
        "id": "unique_id"
    }
    df_sla.rename(columns=sla_cols, inplace=True)

    df_crma.replace("Victoria", "VIC", inplace=True)
    df_sla.replace("Victoria", "VIC", inplace=True)

    df_crma.replace("New South Wales", "NSW", inplace=True)
    df_sla.replace("New South Wales", "NSW", inplace=True)

    df_crma.replace("Queensland", "QLD", inplace=True)
    df_sla.replace("Queensland", "QLD", inplace=True)

    df_crma.replace("South Australia", "SA", inplace=True)
    df_sla.replace("South Australia", "SA", inplace=True)

    df_crma.replace("Western Australia", "WA", inplace=True)
    df_sla.replace("Western Australia", "WA", inplace=True)

    return df_crma, df_sla


def try_match_accounts():
    df_crma, df_sla = get_crm_sl_accounts_dfs()
    df_crma = pa.Table.from_pandas(df_crma[["unique_id", "name", "city", "state", "websiteurl"]])
    df_sla = pa.Table.from_pandas(df_sla[["unique_id", "name", "city", "state", "websiteurl"]])
    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            "lower(left(l.name, 2)) = lower(left(r.name, 2))",
            "lower(left(l.state, 1)) = lower(left(r.state, 1))",
        ],
        "comparisons": [
            cl.jaro_winkler_at_thresholds("name", term_frequency_adjustments=True),
            cl.exact_match("state", term_frequency_adjustments=True),
            cl.exact_match("city", term_frequency_adjustments=True),
            cl.jaccard_at_thresholds("websiteurl"),
        ],
    }

    linker = DuckDBLinker([df_crma, df_sla], settings)
    deterministic_rules = [
        "lower(l.name) = lower(r.name)",
        "lower(l.state) = lower(r.state)",
        "lower(l.city) = lower(r.city)",
    ]

    linker.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

    results = linker.predict(threshold_match_probability=0.98)
    df_results = results.as_pandas_dataframe()
    ...


if __name__ == "__main__":
    try_match_accounts()
    top_level_crm_salesloft_to_sql()

