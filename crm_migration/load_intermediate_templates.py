from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import pandas as pd
import sqlalchemy

from pynamics365.extract_to_load_table import load_to_table_replace, get_engine

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_intermediate_templates():
    intermediate_templates_path = Path("../data/intermediate_tables/templates")
    templates = intermediate_templates_path.glob("*.csv")
    return templates


def get_duckdb_intermediate_templates(
    duckdb_path=r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\duckdb_v7_backup.db",
):
    templates = {}
    with duckdb.connect(duckdb_path) as conn:
        tables = conn.sql("SHOW tables").fetchall()
        template_tables = [table[0] for table in tables if "_Template" in table[0]]
        for table_name in template_tables:
            logger.info(f"Loading {table_name} to DataFrame...")
            df = conn.sql(f"SELECT * FROM {table_name}").to_df()
            templates[table_name] = df
    logger.info("Returning intermediate templates from DuckDB...")
    return templates


def merge_intermediate_templates(templates):
    output_templates = {}
    for template_name, df in templates.items():
        logger.info(f"Merging {template_name}...")
        entity_name, level, table_type = template_name.split("_")
        do_not_modify_cols = [col for col in df.columns if "(Do Not Modify)" in col]
        df["table_level"] = f"{level}_{table_type}"
        df["primary_id"] = df[do_not_modify_cols[0]]
        try:
            index_cols = ["primary_id", "table_level"] + do_not_modify_cols
            df.set_index(index_cols, inplace=True)
        except Exception as e:
            df.set_index(["table_level"], inplace=True)
        if entity_name not in output_templates:
            output_templates[entity_name] = []
        df.reset_index(inplace=True)
        output_templates[entity_name].extend(df.to_dict(index=True, orient="records"))
    output_dfs = {}
    for template_name, records in output_templates.items():
        df = pd.DataFrame(records)
        df.set_index(["primary_id", "table_level"], inplace=True)
        df.sort_index(inplace=True)
        output_dfs[f"{template_name.title()}_Templates"] = df
    return output_dfs


def load_intermediate_templates_to_sql(templates):
    with ThreadPoolExecutor(max_workers=4) as executor:
        for template_name, df in templates.items():
            dtypes = {
                "table_level": sqlalchemy.NVARCHAR(50),
                "primary_id": sqlalchemy.NVARCHAR(50),
            }
            executor.submit(
                load_to_table_replace(
                    df,
                    template_name,
                    engine=get_engine(echo=True),
                    schema="intermediate_templates",
                    index=True,
                    dtypes=dtypes,
                )
            )


def main():
    templates = get_duckdb_intermediate_templates()
    templates = merge_intermediate_templates(templates)
    load_intermediate_templates_to_sql(templates)


if __name__ == "__main__":
    main()
