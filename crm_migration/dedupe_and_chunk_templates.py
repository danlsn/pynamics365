import csv
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import logging

import sqlalchemy


from pynamics365.extract_to_load_table import get_engine
import pendulum

from pynamics365.config import TEMPLATE_ENTITIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_tables_from_sql(
    engine=None, schema="migration", tables={entity for entity in TEMPLATE_ENTITIES}
):
    if not engine:
        engine = get_engine(echo=True)
    dfs = {}
    with engine.connect() as con:
        for table in tables:
            logger.info(f"Loading {table} from SQL")
            dfs[table] = pd.read_sql_table(
                table, con, schema=schema, dtype_backend="pyarrow"
            )
            logger.info(f"Loading {table} from SQL - Done")
    return dfs


def get_dedupe_keys(entity=None):
    dedupe_keys = {
        "account": ["Account Name"],
        "contact": ["First Name", "Last Name"],
        "lead": ["First Name", "Last Name"],
        "opportunity": ["Topic"],
        "opportunityclose": None,
        "opportunityproduct": None,
        "quote": ["Quote ID"],
        "quoteclose": None,
        "quotedetail": None,
    }
    if not entity:
        return dedupe_keys
    elif entity in dedupe_keys:
        return dedupe_keys[entity]
    else:
        return {}


def dedupe_df(df, keys):
    logger.info(f"Deduping {df} on {keys}")
    # Check all keys are in columns of df
    if not keys:
        return df, pd.DataFrame(columns=df.columns)
    for key in keys:
        if key not in df.columns:
            raise ValueError(f"Key {key} not in columns of {df}")
    # Parse (Do Not Modify) Modified On to datetime
    if "(Do Not Modify) Modified On" in df.columns:
        df["(Do Not Modify) Modified On"] = df["(Do Not Modify) Modified On"].apply(
            lambda x: pendulum.from_format(x, "DD/MM/YYYY hh:mm A")
        )

    else:
        logger.error(f"(Do Not Modify) Modified On not in {df.columns}")
        raise
    # Sort on (Do Not Modify) Modified On
    df = df.sort_values("(Do Not Modify) Modified On", ascending=False)
    for key in keys:
        df[f"{key}_upper"] = df[key].str.upper()
    df_unique = df.drop_duplicates(
        subset=[f"{key}_upper" for key in keys], keep="first"
    )
    df_dupes = df[~df.index.isin(df_unique.index)]
    for key in keys:
        df_unique = df_unique.drop(columns=[f"{key}_upper"])
        df_dupes = df_dupes.drop(columns=[f"{key}_upper"])
    logger.info(f"Deduping {df} on {keys} - Done")
    logger.info(f"Unique: {len(df_unique)}, Dupes: {len(df_dupes)}")
    return df_unique, df_dupes


def drop_empty_from_df(df, keys):
    logger.info(f"Dropping empty from {df} on {keys}")
    # Check all keys are in columns of df
    if not keys:
        return df, pd.DataFrame(columns=df.columns)
    for key in keys:
        if key not in df.columns:
            raise ValueError(f"Key {key} not in columns of {df}")
    # Drop empty keys
    df_notna = df.dropna(subset=keys, how="all")
    df_empty = df[~df.index.isin(df_notna.index)]
    logger.info(f"Dropping empty from {df} on {keys} - Done")
    logger.info(f"Non-empty: {len(df_notna)}, Empty: {len(df_empty)}")
    return df_notna, df_empty


def drop_empty_dfs_tables(dfs):
    dfs_empty = {}
    for table, df in dfs.items():
        logger.info(f"Dropping empty from {table}")
        df_notna, df_empty = drop_empty_from_df(df, get_dedupe_keys(table))
        dfs[table] = df_notna
        dfs_empty[table] = df_empty
        logger.info(f"Dropping empty from {table} - Done")
    return dfs, dfs_empty


def dedupe_dfs_tables(dfs):
    dfs_dupes = {}
    for table, df in dfs.items():
        logger.info(f"Deduping {table}")
        df_unique, df_dupes = dedupe_df(df, get_dedupe_keys(table))
        dfs[table] = df_unique
        dfs_dupes[table] = df_dupes
        logger.info(f"Deduping {table} - Done")
    return dfs, dfs_dupes


def chunk_df(df, chunk_size=1000):
    logger.info(f"Chunking {df} into {chunk_size} records")
    dfs_chunked = {}
    for i in range(0, len(df), chunk_size):
        dfs_chunked[i + chunk_size] = df[i : i + chunk_size]
    logger.info(f"Chunking {df} into {chunk_size} records - Done")
    return dfs_chunked


def save_chunked_dfs_to_csv(dfs_chunked, output_path="../data/chunks"):
    for table, chunks in dfs_chunked.items():
        logger.info(f"Saving {table} chunk {chunks}")
        for chunk_index, chunk_df in chunks.items():
            output_file = Path(f"{output_path}/{table}_{chunk_index}.csv")
            output_file.parent.mkdir(parents=True, exist_ok=True)
            do_not_modify_cols = [
                col for col in chunk_df.columns if "(Do Not Modify)" in col
            ]
            chunk_df = chunk_df.drop(columns=do_not_modify_cols)
            table_parent = table.split("_")[0].title()
            chunk_file = Path(f"{output_path}/{table_parent}/{table}_{chunk_index}.csv")
            chunk_file.parent.mkdir(parents=True, exist_ok=True)
            chunk_df.to_csv(
                chunk_file, index=False, header=True, quoting=csv.QUOTE_ALL
            )
            logger.info(
                f"Saving {table} chunk {chunk_index // 1000}/{len(chunks)} - Done"
            )
        logger.info(f"Saving {table} - Done")


def chunk_dfs_tables(dfs):
    dfs_chunked = {}
    for table, df in dfs.items():
        logger.info(f"Chunking {table}")
        dfs_chunked[table] = chunk_df(df)
        logger.info(f"Chunking {table} - Done")
    return dfs, dfs_chunked


def separate_records_with_parents(dfs):
    dfs_output = {}
    for table, df in dfs.items():
        if "Parent Account" in df.columns:
            dfs_output[f"{table}_no_parent"] = df[df["Parent Account"].isna()]
            dfs_output[f"{table}_with_parent"] = df[~df["Parent Account"].isna()]
        else:
            dfs_output[table] = df
    return dfs_output


def save_whole_dfs_to_csv(dfs, suffix=None):
    for table, df in dfs.items():
        output_file = Path(f"../data/chunks/_whole/{table}{suffix}.csv")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False, header=True, quoting=csv.QUOTE_ALL)
    return dfs


def create_empty_templates_for_chunks(
    dfs_chunked,
    template_path="../data/import_templates",
    output_path="../data/chunks/_templates",
):
    import_template_map = {
        "account": "Account.xlsx",
        "account_no_parent": "Account.xlsx",
        "account_with_parent": "Account.xlsx",
        "campaign": "Campaign.xlsx",
        "contact": "Contact.xlsx",
        "lead": "Lead.xlsx",
        "opportunity": "Opportunity.xlsx",
        "opportunityclose": "Opportunity Close.xlsx",
        "opportunityproduct": "Opportunity Product.xlsx",
        "quote": "Quote.xlsx",
        "quoteclose": "Quote Close.xlsx",
        "quotedetail": "Quote Product.xlsx",
    }
    for table, chunks in dfs_chunked.items():
        try:
            template_file = Path(f"{template_path}/{import_template_map[table]}")
        except KeyError:
            logger.warning(f"No template found for {table}")
            continue
        for chunk_index, chunk_df in chunks.items():
            table_parent = table.split("_")[0].title()
            output_file = Path(f"{output_path}/{table_parent}/{table}_{chunk_index}.xlsx")
            output_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(template_file, output_file)
    return dfs_chunked


def save_whole_dfs_to_sql(dfs, suffix, schema="deduped"):
    from crm_migration.migration_etl import load_to_table_chunked
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for table, df in dfs.items():
            dtype = {col: sqlalchemy.types.VARCHAR for col in df.columns}
            futures.append(
                executor.submit(
                    load_to_table_chunked,
                    df=df,
                    table_name=f"{table}{suffix}",
                    dtype=dtype,
                    schema=schema,
                    echo=True,
                    index=True
                )
            )
        for future in as_completed(futures):
            print(future.result())


if __name__ == "__main__":
    tables = TEMPLATE_ENTITIES
    for table in tables:
        dfs = load_tables_from_sql(tables={table})
        dfs, dfs_dupes = dedupe_dfs_tables(dfs)
        dfs, dfs_empty = drop_empty_dfs_tables(dfs)
        dfs = separate_records_with_parents(dfs)
        dfs, dfs_chunked = chunk_dfs_tables(dfs)
        dfs_chunked = create_empty_templates_for_chunks(dfs_chunked)
        save_chunked_dfs_to_csv(dfs_chunked)
        save_whole_dfs_to_csv(dfs, suffix="_deduped")
        save_whole_dfs_to_csv(dfs_dupes, suffix="_dupes")
        save_whole_dfs_to_csv(dfs_empty, suffix="_empty")
        save_whole_dfs_to_sql(dfs, suffix="_deduped")
        save_whole_dfs_to_sql(dfs_dupes, suffix="_dupes")
        save_whole_dfs_to_sql(dfs_empty, suffix="_empty")

