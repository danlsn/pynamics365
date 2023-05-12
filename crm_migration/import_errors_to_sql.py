import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import dictdatabase as DDB
import pandas as pd
import pyarrow as pa
import sqlalchemy.types
from dotenv import load_dotenv

from crm_migration.migration_etl import load_to_table_chunked
from pynamics365.config import DDB_PATH
from pynamics365.extract_to_load_table import prepare_column_names, get_engine
from pynamics365.prod_environment import full_extract_to_ddb

DDB.config.storage_directory = DDB_PATH


def load_import_logs_to_ddb():
    load_dotenv()
    full_extract_to_ddb(
        entity_list=["import", "importdata", "importfile", "importlog"],
        resource=os.getenv("MSDYN_UAT_RESOURCE"),
        multi_threaded=True,
        max_workers=4,
        shuffle=True,
    )


def load_data_from_ddb(parent="mipau_crm6_dynamics_com", table="importdata"):
    records = []
    with DDB.at(f"{parent}/{table}").session() as (session, archive):
        records.extend(archive.values())
    df = prepare_column_names(pa.Table.from_pylist(records).to_pandas())
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def prepare_import_error_report(dfs):
    if "importlog" not in dfs and "importfile" not in dfs:
        raise ValueError("importlog and importfile not in dfs")

    import_files = dfs["importfile"].to_dict(orient="records")
    target_entities = {i["importfileid"]: i["targetentityname"] for i in import_files}
    df = dfs["importlog"].copy()
    df["targetentityname"] = df["_importfileid_value"].map(target_entities)
    df["legacyid"] = df["_importdataid_value_FormattedValue"].str.extract(r'\(Legacy CRM ID: (.{36})\)')
    try:
        df.set_index(
            ["targetentityname", "_importfileid_value_FormattedValue", "linenumber", "legacyid"],
            inplace=True,
        )
        df.sort_index(inplace=True)
    except KeyError:
        pass
    dfs.update({"import_error_report": df})
    return dfs


def prepare_import_report(dfs):
    if "importdata" not in dfs and "importfile" not in dfs:
        raise ValueError("importdata and importfile not in dfs")

    import_files = dfs["importfile"].to_dict(orient="records")
    target_entities = {i["importfileid"]: i["targetentityname"] for i in import_files}
    df = dfs["importdata"].copy()
    df["targetentityname"] = df["_importfileid_value"].map(target_entities)
    df["legacyid"] = df["data"].str.extract(r'\(Legacy CRM ID: (.{36})\)')
    try:
        df.set_index(
            ["targetentityname", "_importfileid_value_FormattedValue", "linenumber", "legacyid"],
            inplace=True,
        )
        df.sort_index(inplace=True)
    except KeyError:
        pass
    dfs.update({"import_report": df})
    return dfs


def load_import_report_to_sql(dfs, schema="import_report", sql_engine=None):
    futures = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for table, df in dfs.items():
            if f"{table}id" in df.columns and f"{table}id" not in df.index.names:
                df.set_index(f"{table}id", inplace=True)
            dtype = {col: sqlalchemy.types.NVARCHAR(1000) for col in df.index.names}
            futures.append(
                executor.submit(
                    load_to_table_chunked,
                    df=df,
                    table_name=table,
                    dtype=dtype,
                    schema=schema,
                    echo=True,
                    index=True
                )
            )
        for future in as_completed(futures):
            print(future.result())


def save_dfs_to_csv(dfs, output_dir="../data"):
    for table, df in dfs.items():
        df.to_csv(
            f"{output_dir}/{table}.csv", index=True, header=True, quoting=csv.QUOTE_ALL
        )


if __name__ == "__main__":
    load_import_logs_to_ddb()
    import_entities = ["import", "importdata", "importfile", "importlog"]
    dfs = {}
    for entity in import_entities:
        dfs.update({entity: load_data_from_ddb(table=entity)})
    dfs = prepare_import_error_report(dfs)
    dfs = prepare_import_report(dfs)
    load_import_report_to_sql(dfs)
    save_dfs_to_csv(dfs, "../data/import_logs")
    ...
