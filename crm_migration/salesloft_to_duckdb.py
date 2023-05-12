import json
from pathlib import Path
import dictdatabase as DDB
import duckdb
import pandas as pd
import sqlalchemy

from pynamics365.extract_to_load_table import get_engine
salesloft_extract_path = Path("../data/salesloft-extract/full")


def get_extract_folders_files(extract_path):
    extracts = {}
    for folder in extract_path.iterdir():
        print(f"Getting files for {folder.name}")
        if folder.is_dir():
            if folder.name not in extracts:
                extracts[folder.name] = {}
            extracts[folder.name]["path"] = str(folder)
            extracts[folder.name]["files"] = [
                str(file_path) for file_path in folder.rglob("*.json")
            ]
    return extracts


def extracts_to_df(extracts=None, salesloft_extract_path=salesloft_extract_path, **kwargs):
    if extracts is None:
        extracts = get_extracts(salesloft_extract_path, **kwargs)
    for entity, extract in extracts.items():
        print(f"Converting {entity} to df")
        df = pd.json_normalize(extract["records"], sep="_")
        extracts[entity]["df"] = df
    return extracts


def get_extracts(extract_path, **kwargs):
    if kwargs.get("use_ddb"):
        ddb_path = "salesloft"
        extract_tables = Path("../data/ddb/salesloft").glob("*.json")
        extracts = {}
        for table in extract_tables:
            print(f"Getting records for {table.name}")
            extract_name = table.name.replace(".json", "")
            extracts[extract_name] = {}
            with open(table, "rb") as f:
                records = json.load(f)
            extracts[extract_name]["records"] = [*records.values()]
    else:
        extracts = get_extract_folders_files(extract_path)
        for entity, extract in extracts.items():
            print(f"Getting records for {entity}")
            records = []
            for file in extracts[entity]["files"]:
                try:
                    with open(file, "r") as f:
                        data = json.load(f)
                        records.extend(data["data"])
                except Exception as e:
                    print(f"Error: {e}")
            extracts[entity]["records"] = records
    return extracts


def save_extracts_to_csv(extracts, output_path):
    for entity, extract in extracts.items():
        print(f"Saving {entity} to csv")
        file_path = Path(output_path) / f"{entity}.csv"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        extract["df"].to_csv(file_path, index=False)


def save_extracts_to_duckdb(extracts, duckdb_path):
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for entity, extract in extracts.items():
            print(f"Saving {entity} to duckdb")
            df = extract["df"]
            entity = entity.replace("-", "_")
            try:
                con.sql(f"DROP TABLE IF EXISTS {entity}")
                con.sql(f"CREATE TABLE IF NOT EXISTS {entity} AS SELECT * FROM df")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                con.commit()


def salesloft_extracts_to_sql(schema="salesloft", extracts=None, extract_path=salesloft_extract_path, conn=None, **kwargs):
    if conn is None:
        sql_conn = get_engine(echo=True)
    if extract_path and not extracts:
        extracts = get_extracts(salesloft_extract_path, **kwargs)
        extracts = extracts_to_df(extracts)
    elif extracts:
        pass
    for extract in extracts:
        entity = extract.replace("-", "_")
        df = extracts[extract]["df"]
        try:
            if "id" in df.columns and "updated_at" in df.columns:
                # df["updated_at"] = pd.to_datetime(df["updated_at"], infer_datetime_format=True)
                # df["created_at"] = pd.to_datetime(df["created_at"], infer_datetime_format=True)
                df.set_index(["id", "updated_at"], inplace=True)
                dtypes = {"id": sqlalchemy.types.BigInteger, "updated_at": sqlalchemy.types.NVARCHAR(100)}
            elif "id" in df.columns:
                df.set_index("id", inplace=True)
                dtypes = {"id": sqlalchemy.types.Integer}
            else:
                ...
        except KeyError as e:
            print(f"Error: {e}")
            pass
        for df_col in df.columns:
            if df[df_col].dtype == "object":
                df[df_col] = df[df_col].astype("string")
            if df_col not in dtypes.keys():
                dtypes[df_col] = sqlalchemy.types.NVARCHAR("max")
            if ".id" in df_col:
                dtypes[df_col] = sqlalchemy.types.BigInteger
        try:
            df.to_sql(entity, sql_conn, schema=schema, if_exists="replace", index=True, dtype=dtypes, method=None)
        except Exception as e:
            print(f"Error: {e}")
            pass


def main():
    extracts = get_extracts(salesloft_extract_path)
    extracts = extracts_to_df(extracts)
    # save_extracts_to_csv(extracts, "../data/salesloft_csv")
    # save_extracts_to_duckdb(extracts, "../data/salesloft_duck.db")
    salesloft_extracts_to_sql(extracts=extracts, schema="salesloft")


if __name__ == "__main__":
    main()
