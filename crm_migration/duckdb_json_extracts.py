import json
from pathlib import Path

import duckdb
import pandas as pd

from pynamics365.config import TEMPLATE_ENTITIES


# example setting the sample size to 100000
duckdb.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")

EXTRACT_PATH = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\org30a87_crm5_dynamics_com\current\json-5k"
)


def get_all_records(extract_path, entity_name):
    records = []
    extract_path = extract_path / entity_name
    for file in extract_path.glob(f"{entity_name}-extract-page-*.json"):
        with open(file, "r") as f:
            data = json.load(f)
            records.extend(data["value"])
    return records


def get_all_records_to_df(extract_path, entity_name):
    records = get_all_records(extract_path, entity_name)
    df = pd.json_normalize(records)
    return df


def main():
    with duckdb.connect("../data/duckdb.db") as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        con.sql(
            """
                select 'unique_contacts' as "unique", count(*) as "unique_contacts" from contact group by 'contactid'
                union all
                select 'unique_accounts', count(*) as "unique_accounts" from account group by 'accountid'
                union all
                select 'unique_leads', count(*) as "unique_leads" from lead group by 'leadid'
            """
        ).show()
        df_spec = con.sql("""DESCRIBE;""").df()
        for entity_name in TEMPLATE_ENTITIES:
            df = get_all_records_to_df(EXTRACT_PATH, entity_name)
            cols = df.columns.tolist()
            con.sql(f"DROP TABLE IF EXISTS {entity_name}")
            con.sql(f"CREATE TABLE IF NOT EXISTS {entity_name} AS SELECT * FROM df")
            con.commit()
            ...
        con.close()
        duck = duckdb.sql("SELECT * FROM df").fetchall()
        ddf = duckdb.sql("SELECT * FROM df").fetchdf()
        ...


if __name__ == "__main__":
    main()
