import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import numpy as np
from mssql_dataframe import SQLServer
import pandas as pd
import logging
import pynamics365.prod_environment
import sqlalchemy
from dotenv import load_dotenv

from pynamics365 import prod_environment
from pynamics365.config import ENTITY_LIST

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
# ch = logging.StreamHandler()
# ch.setFormatter(logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s"))
# fh = logging.FileHandler('../logs/pynamics365.log')
# fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
# fh.setLevel(logging.INFO)
# logger.addHandler(ch)
# logger.addHandler(fh)


dfh = logging.FileHandler("../logs/extract_to_load_table.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)


# Step 1: Walk through the extract directory and get a list of all the entities
def get_entity_list(extract_path):
    entities = []
    logger.info(f"Getting a list of entities from {extract_path}")
    for entity in extract_path.iterdir():
        if entity.is_dir():
            entities.append(entity.name)
    logger.info(f"Found {len(entities)} entities")
    return entities


# Step 2: For each entity, get a list of all the files
def get_file_list(entity_path):
    files = []
    logger.info(f"Getting a list of files from {entity_path}")
    try:
        for file in entity_path.iterdir():
            if file.is_file():
                files.append(file.name)
        logger.info(f"Found {len(files)} files")
        return files
    except FileNotFoundError:
        logger.error(f"Could not find {entity_path}")
        return []


# Step 3: For each file, get records from 'value'
def get_records(file_path):
    logger.info(f"Getting records from {file_path.name}")
    with open(file_path, "r") as f:
        try:
            content = json.load(f)
            records = content['value']
        except json.decoder.JSONDecodeError:
            logger.error(f"Could not decode {file_path.name}")
            records = []
    logger.info(f"Found {len(records)} records")
    return records


# Step 4: Load the records into a pandas dataframe
def records_to_dataframe(records):
    logger.info(f"Loading {len(records)} records into a dataframe")
    df = pd.json_normalize(records)
    return df


# Step 5: Get a list of all the fields
def prepare_column_names(df):
    logger.info(f"Preparing column names")
    col_names = df.columns
    keys = {}
    for col_name in col_names:
        if '@odata.etag' in col_name:
            keys.update({col_name: 'odata_etag'})
        elif '@' in col_name:
            key, suffix = col_name.split("@", 2)
            if key == '' and suffix != '':
                key = suffix
                suffix = ''
            suffix = suffix.split(".")[-1]
            keys.update({col_name: f"{key}_{suffix}"})
        else:
            keys.update({col_name: col_name})
    df.rename(columns=keys, inplace=True)
    return df


# Step 6: Get EntityDefinitions, and use it to set the primary key
def set_primary_key(df, entity, definitions):
    primary_key = definitions.loc[definitions['LogicalName'] == entity, 'PrimaryIdAttribute'].iloc[0]
    logger.info(f"Setting primary key for {entity} to {primary_key}")
    if "versionnumber" not in df.columns:
        df["versionnumber"] = 1
    try:
        df.set_index([primary_key, "versionnumber"], inplace=True)
    except KeyError:
        logger.warning(f"Could not set primary key for {entity} to {primary_key}")
    return df


def extract_to_dfs(extract_path, resource_path_name, shuffle=False):
    base_path = Path(extract_path)
    extract_path = Path(base_path) / resource_path_name / 'current' / 'json-5k'
    defs_path = Path(base_path) / resource_path_name / '_Definitions' / 'EntityDefinitions.csv'
    entities = get_entity_list(extract_path)
    definitions = pd.read_csv(defs_path)
    if shuffle:
        import random
        random.shuffle(entities)
    for entity in entities:
        entity_path = extract_path / entity
        files = get_file_list(entity_path)
        records = []
        for file in files:
            file_path = entity_path / file
            records.extend(get_records(file_path))
        df = records_to_dataframe(records)
        df = prepare_column_names(df)
        df = set_primary_key(df, entity, definitions)
        yield entity, df


def records_to_df(extract_path, resource_path_name=None, entity=None):
    defs_path = Path(extract_path) / resource_path_name / '_Definitions' / 'EntityDefinitions.csv'
    definitions = pd.read_csv(defs_path)
    entity_path = Path(extract_path) / resource_path_name / "current" / "json-5k" / entity
    files = get_file_list(entity_path)
    records = []
    for file in files:
        file_path = entity_path / file
        records.extend(get_records(file_path))
    df = records_to_dataframe(records)
    df = prepare_column_names(df)
    df = set_primary_key(df, entity, definitions)
    return entity, df

...


def save_to_csv(df, entity, extract_path):
    out_file = extract_path.parent / 'csv' / f"{entity}.csv"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving {entity} to {out_file.name}")
    df.to_csv(out_file, index=True)


def load_to_table(df, entity, schema="staging", engine=None, **kwargs):
    if engine is None:
        engine = get_engine()
    with engine.connect() as conn:
        while not df.empty:
            if f"{entity}id" in df.columns:
                primary_id_col = f"{entity}id"
            elif "activityid" in df.columns:
                primary_id_col = "activityid"
            else:
                primary_id_col = df.index.names[0]
            dtypes = {primary_id_col: sqlalchemy.types.VARCHAR(100),
                      "versionnumber": sqlalchemy.types.Integer}
            logger.info(f"Loading {entity} to table")
            try:
                logger.info(f"Loading {entity} to DataFrame from table")
                df_existing = pd.read_sql_table(entity, conn, schema=schema)
                logger.info(f"Loaded {len(df_existing)} {entity} records from table")
            except ValueError:
                logger.info(f"Table {entity} does not exist")
                df_existing = pd.DataFrame()
            df_existing_index = df_existing.index
            df_index = df.index
            try:
                if "versionnumber" in df_existing.columns and "versionnumber" in df.columns:
                    df_existing.reset_index(inplace=True)
                    df.reset_index(inplace=True)
                    logger.debug(f"Comparing {entity} version numbers. Existing: {len(df_existing_index)}, New: {len(df_index)}")
                    df_existing.set_index([primary_id_col, "versionnumber"], inplace=True)
                    df_existing_index = df_existing.index
                    df.set_index([primary_id_col, "versionnumber"], inplace=True)
                    df_index = df.index
                    df = df[~df.index.isin(df_existing.index)]
                    logger.debug(f"Comparing {entity} modifiedon dates. Found {len(df)} new records for {entity}")
                elif "modifiedon" in df_existing.columns and "modifiedon" in df.columns:
                    df_existing.reset_index(inplace=True)
                    df.reset_index(inplace=True)
                    logger.debug(f"Comparing {entity} modifiedon dates. Existing: {len(df_existing_index)}, New: {len(df_index)}")
                    df_existing.set_index([primary_id_col, "modifiedon"], inplace=True)
                    df_existing_index = df_existing.index
                    df.set_index([primary_id_col, "modifiedon"], inplace=True)
                    df = df[~df.index.isin(df_existing.index)]
                    logger.debug(f"Comparing {entity} modifiedon dates. Found {len(df)} new records for {entity}")
                elif "updated_at" in df.columns and "updated_at" in df_existing.columns:
                    df_existing.reset_index(inplace=True)
                    df.reset_index(inplace=True)
                    logger.debug(f"Comparing {entity} updated_at dates. Existing: {len(df_existing_index)}, New: {len(df_index)}")
                    df_existing.set_index([primary_id_col, "updated_at"], inplace=True)
                    df_existing_index = df_existing.index
                    df.set_index([primary_id_col, "updated_at"], inplace=True)
                    df = df[~df.index.isin(df_existing.index)]
                    logger.debug(f"Comparing {entity} updated_at dates. Found {len(df)} new records for {entity}")
                else:
                    logger.debug(f"Comparing {entity} primary keys. Existing: {len(df_existing_index)}, New: {len(df_index)}")
                    df_existing.set_index([primary_id_col], inplace=True)
                    # Compare the index of the extract to the index of the database
                    # If the index of the extract is not in the index of the database, then it is new
                    # If index is a match and the version number is higher, then it is new
                    # If index is a match and the version number is lower, then it is old
                    # If index is a match and the version number is the same, then it is a duplicate

                    # Records in the extract that are not in the database
                    df = df[~df.index.isin(df_existing.index)]
                    logger.debug(f"Comparing {entity} primary keys. Found {len(df)} new records for {entity}")
            except KeyError as e:
                logger.warning(f"Could not set primary key for {entity} to {primary_id_col}\n{e}")
            logger.info(f"Found {len(df)} new records for {entity}")
            # # Get first 1000 records
            # sample_size = 1000
            sample_size = 2000
            if len(df) < sample_size:
                sample_size = len(df)
            df_new = df.head(sample_size)
            # # Load using chunksize of 100
            # df_new.to_sql(entity, conn, dtype=dtypes, schema=schema, if_exists=kwargs.get("if_exists", None), chunksize=100, index=True)
            df_new = df_new.copy().reset_index()
            df_new.set_index([primary_id_col], inplace=True)
            logger.debug(f"Index for df_new set to {df_new.index.name}")
            df_new.to_sql(entity, conn, dtype=dtypes, schema=schema, if_exists=kwargs.get("if_exists", None), chunksize=500, index=True)
            if len(df) - sample_size > 0:
                df = df.tail(len(df) - sample_size)
            else:
                df = pd.DataFrame()


def load_to_table_replace(df, entity, schema="staging", engine=None, **kwargs):
    if engine is None:
        engine = get_engine(use_mssql=kwargs.get("use_mssql", False))
    with engine.connect() as conn:
        primary_id_col = df.index.names[0]
        dtypes = {primary_id_col: sqlalchemy.types.VARCHAR(100)}
        logger.info(f"Loading {entity} to table")
        first_run = True
        while not df.empty:
            primary_id_col = df.index.names[0]
            dtypes = {primary_id_col: sqlalchemy.types.VARCHAR(100)}
            sample_size = 2000
            logger.info(f"Found {len(df)} new records for {entity}. Loading {sample_size} records...")
            if len(df) < sample_size:
                logger.info(f"Found {len(df)} records for {entity}. Loading remaining {len(df)} records...")
                sample_size = len(df)
            df_new = df.head(sample_size)
            if first_run:
                df_new.to_sql(entity, conn, dtype=dtypes, schema=schema, if_exists="replace", index=kwargs.get("index", True))
                first_run = False
            else:
                df_new.to_sql(entity, conn, dtype=dtypes, schema=schema, if_exists="append", index=kwargs.get("index", True))
            conn.commit()
            logger.info(f"Loaded {sample_size} records for {entity} to table.")
            if len(df) - sample_size > 0:
                df = df.tail(len(df) - sample_size)
            else:
                logger.info(f"Loaded all records for {entity} to table.")
                df = pd.DataFrame()
        logger.info(f"Finished Loading {entity} to table")
        return entity


def ddb_to_pandas(ddb_json=Path("../data/ddb/org30a87_crm5_dynamics_com/account.json")):
    with open(ddb_json, "rb") as f:
        ddb = json.load(f)
    df = pd.json_normalize([*ddb.values()])
    return df


def get_engine(**kwargs):
    load_dotenv()
    host = os.getenv("SQL_HOST")
    db = os.getenv("SQL_DB")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    conn_str = f"DSN=MIPCRM_Sandbox;UID={user};PWD={password};DATABASE={db};"
    if kwargs.get("use_mssql"):
        engine = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{password}@{host}/{db}")
    else:
        engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}", fast_executemany=True,
                                      execution_options={"timeout": 30}, echo=kwargs.get("echo", kwargs.get("echo", False)))
    return engine


def main(use_sqlite=False):
    load_dotenv()
    host = os.getenv("SQL_HOST")
    db = os.getenv("SQL_DB")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    sql = SQLServer(database=db, server=host, username=user, password=password, autoadjust_sql_objects=True)
    extract_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract")
    rsrc_path_names = ['mipau_crm6_dynamics_com', 'mipdev_crm6_dynamics_com', 'org30a87_crm5_dynamics_com']
    # for rsrc_path_name in ['org30a87_crm5_dynamics_com']:
        # with ThreadPoolExecutor(max_workers=4) as executor:
        #     for entity_name, df in extract_to_dfs(extract_path, rsrc_path_name, shuffle=True):
        #         executor.submit(save_to_csv, df, entity_name, extract_path)
        #         executor.submit(load_to_table, df, entity_name)
        # for entity_name, df in extract_to_dfs(extract_path, rsrc_path_name, shuffle=True):
        #     try:
        #         save_to_csv(df, entity_name, extract_path)
        #     except Exception:
        #         logger.exception(f"Error saving {entity_name} to CSV")
        #     try:
        #         load_to_table(df, entity_name)
        #     except Exception:
        #         logger.exception(f"Error loading {entity_name} to SQL")
        #     table_name = f"[load].{entity_name}"
        #     merged_df = sql.create.table_from_dataframe(table_name, df, primary_key="index")
        #     ...
    entity_list = ENTITY_LIST
    try:
        prod_environment.main(threaded=True)
    except Exception:
        logger.exception("Error loading production environment")
    for entity in entity_list:
        entity_name, df = records_to_df(extract_path, rsrc_path_names[2], entity)
        # try:
        #     save_to_csv(df, entity_name, extract_path)
        # except Exception:
        #     logger.exception(f"Error saving {entity_name} to CSV")
        try:
            load_to_table(df, entity_name)
        except Exception:
            logger.exception(f"Error loading {entity_name} to SQL")
        table_name = f"[load].{entity_name}"
        ...


def load_template_column_mappings(template_json=Path("../data/json_templates/import_templates/account.json")):
    with open(template_json, "rb") as f:
        template_json = json.load(f)
    column_mappings = template_json["column_name_map"]
    return column_mappings


def update_df_description_with_legacy_id(entity_name, df_row):
    description = df_row["description"]
    if str(description) == "nan" or description is None:
        description = ""
    legacy_id = None
    if f"{entity_name}id" in df_row:
        legacy_id = df_row[f"{entity_name}id"]
    elif f"activityid" in df_row:
        legacy_id = df_row[f"activityid"]
    if legacy_id:
        description = f"{description}\n\n(Legacy CRM ID: {legacy_id})"
        description = description.strip()
    return description


def map_df_to_template(df, entity_name, templates_root=Path("../data/json_templates/import_templates")):
    template_json = templates_root / f"{entity_name}.json"
    if not template_json.exists():
        return pd.DataFrame()
    column_mappings = load_template_column_mappings(template_json)
    df_cols = [*df.columns]
    template_cols = [*column_mappings.keys()]
    matched_cols = {}
    unmatched_cols = {}
    if "description" in df_cols:
        logger.info(f"Updating description column with legacy ID for {entity_name}")
        df["description"] = df.apply(partial(update_df_description_with_legacy_id, entity_name), axis=1)
        logger.info(f"Updating description column with legacy ID for {entity_name} - Done.")
    for col in template_cols:
        if f"{col}_FormattedValue" in df_cols:
            matched_cols[col] = f"{col}_FormattedValue"
        elif f"_{col}_value_FormattedValue" in df_cols:
            matched_cols[col] = f"_{col}_value_FormattedValue"
        elif f"_{col}_value" in df_cols:
            matched_cols[col] = f"_{col}_value"
        elif col in df_cols:
            matched_cols[col] = col
        else:
            unmatched_cols[col] = None
    logger.info(f"Matched {len(matched_cols)} columns and {len(unmatched_cols)} columns")
    new_df = pd.DataFrame()
    for left, right in matched_cols.items():
        try:
            if right not in df.columns:
                new_df[left] = None
            else:
                new_df[left] = df[right]
        except Exception:
            logger.exception(f"Error mapping {left} to {right}")
    logger.info(f"New DataFrame has {len(new_df.columns)} columns")
    new_df = new_df.rename(columns=column_mappings)
    # Reorder Columns as in column_mappings
    # newdf_cols_not_in_mappings = [col for col in new_df.columns if col not in column_mappings.values()]
    mapping_cols_not_in_newdf = [col for col in column_mappings.values() if col not in new_df.columns]
    for col in mapping_cols_not_in_newdf:
        new_df[col] = None
    new_df = new_df[column_mappings.values()]
    index_cols = [col for col in new_df.columns if col.startswith("(Do Not Modify)")]
    new_df = new_df.set_index(index_cols)
    return new_df


def get_all_ddb_to_csv(ddb_root=Path("../data/ddb/org30a87_crm5_dynamics_com"), templates_root=Path("../data/json_templates/import_templates")):
    ddb_paths = [*ddb_root.glob("**/*.json")]
    template_paths = [*templates_root.glob("*.json")]
    logger.info(f"Found {len(ddb_paths)} DDB files and {len(template_paths)} template files")
    for ddb_path in ddb_paths:
        if ddb_path.stem not in [template_path.stem for template_path in template_paths]:
            continue
        logger.info(f"Processing {ddb_path.stem}")
        df = ddb_to_pandas(ddb_path)
        df = prepare_column_names(df)
        column_mappings = load_template_column_mappings(template_json=templates_root / f"{ddb_path.stem}.json")
        df_cols = [*df.columns]
        template_cols = [*column_mappings.keys()]
        matched_cols = {}
        unmatched_cols = {}
        for col in template_cols:
            if f"{col}_FormattedValue" in df_cols:
                matched_cols[col] = f"{col}_FormattedValue"
            elif f"_{col}_value_FormattedValue" in df_cols:
                matched_cols[col] = f"_{col}_value_FormattedValue"
            elif f"_{col}_value" in df_cols:
                matched_cols[col] = f"_{col}_value"
            else:
                matched_cols[col] = col
                unmatched_cols[col] = None
        logger.info(f"Matched {len(matched_cols)} columns and {len(unmatched_cols)} columns")
        new_df = pd.DataFrame()
        for left, right in matched_cols.items():
            try:
                if right not in df.columns:
                    new_df[left] = None
                else:
                    new_df[left] = df[right]
            except Exception:
                logger.exception(f"Error mapping {left} to {right}")
        logger.info(f"New DataFrame has {len(new_df.columns)} columns")
        new_df = new_df.rename(columns=column_mappings)
        # Reorder Columns as in column_mappings
        new_df = new_df[column_mappings.values()]
        index_cols = [col for col in new_df.columns if col.startswith("(Do Not Modify)")]
        new_df = new_df.set_index(index_cols)

        logger.info(f"Saving {ddb_path.stem}.csv")
        new_df.to_csv(Path(f"../data/csv/{ddb_path.stem}.csv"), quoting=csv.QUOTE_NONNUMERIC, index=False)
        new_df.to_csv(Path(f"../data/tsv/{ddb_path.stem}.tsv"), quoting=csv.QUOTE_NONNUMERIC, sep="\t", index=False)
        new_df.to_excel(Path(f"../data/xlsx/{ddb_path.stem}.xlsx"), index=False)
        logger.info(f"Saved {ddb_path.stem}.csv")


def staging_table_to_migration(entity_name, schema_r="staging", schema_w="itknocks_migration"):
    ...


def get_all_ddb_to_dfs(ddb_root, entity_list):
    dfs = {entity: ddb_extract_to_df(ddb_root, entity) for entity in entity_list}
    return dfs


def ddb_extract_to_df(ddb_root, entity_name, templates_root=Path("../data/json_templates/import_templates"), env="prod"):
    if env == "prod":
        ddb_root = Path(ddb_root) / "org30a87_crm5_dynamics_com"
    ddb_path = Path(ddb_root) / f"{entity_name}.json"
    logger.info(f"Processing {ddb_path.stem}")
    df = ddb_to_pandas(ddb_path)
    df = prepare_column_names(df)
    # Set index to {entity_name}id
    try:
        if f"{entity_name}id" in df.columns:
            logger.debug(f"Setting index to {entity_name}id for {entity_name}")
            df.set_index(f"{entity_name}id", inplace=True)
        elif "activityid" in df.columns:
            logger.debug(f"Setting index to activityid for {entity_name}")
            df.set_index("activityid", inplace=True)
        else:
            logger.error(f"Could not set index to {entity_name}id or activityid")
    except KeyError:
        logger.exception(f"Error setting index to {entity_name}id. Trying 'activityid'.")
    logger.info(f"Loaded {entity_name} to DataFrame with {len(df)} rows.")
    return df


if __name__ == "__main__":
    get_all_ddb_to_csv()


# if __name__ == "__main__":
#     df = ddb_to_pandas()
#     df = prepare_column_names(df)
#     column_mappings = load_template_column_mappings()
#     df_cols = [*df.columns]
#     template_cols = [*column_mappings.keys()]
#     matched_cols = {}
#     unmatched_cols = {}
#     for col in template_cols:
#         if f"{col}_FormattedValue" in df_cols:
#             matched_cols[col] = f"{col}_FormattedValue"
#         elif f"_{col}_value_FormattedValue" in df_cols:
#             matched_cols[col] = f"_{col}_value_FormattedValue"
#         elif f"_{col}_value" in df_cols:
#             matched_cols[col] = f"_{col}_value"
#         else:
#             matched_cols[col] = col
#             unmatched_cols[col] = None
#
#     new_df = pd.DataFrame()
#     for left, right in matched_cols.items():
#         try:
#             if right not in df.columns:
#                 new_df[left] = None
#             else:
#                 new_df[left] = df[right]
#         except Exception:
#             logger.exception(f"Error mapping {left} to {right}")
#     new_df = new_df.rename(columns=column_mappings)
#     # Reorder Columns as in column_mappings
#     new_df = new_df[column_mappings.values()]
#     new_df.to_csv(Path("../data/csv/account.csv"), index=False)
#     ...
#
#     cols_in_template_not_in_df = [col for col in template_cols if col not in df_cols]
#     cols_in_df_not_in_template = [col for col in df_cols if col not in template_cols]
#     unmatched_cols = []
#     matched_cols = [col for col in df_cols if col in template_cols]
#     for col in cols_in_template_not_in_df:
#         if f"_{col}_value_FormattedValue" in df_cols:
#             matched_cols.append(f"_{col}_value_FormattedValue")
#         elif f"_{col}_value" in df_cols:
#             matched_cols.append(f"_{col}_value")
#         else:
#             unmatched_cols.append(col)
#
#
#     ...
#     # main()
