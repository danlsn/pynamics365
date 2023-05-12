import re
from collections import namedtuple
import dictdatabase as DDB
from pynamics365.config import TEMPLATE_ENTITIES
from crm_migration.migration_etl import load_to_table_chunked
import pandas as pd
import sqlalchemy
DDB.config.storage_directory = '../data/ddb'


def get_ddb_records(parent="mipau_crm6_dynamics_com", table=None):
    try:
        with DDB.at(f"{parent}/{table}").session() as (session, records):
            for id, record in records.items():
                yield record
    except FileNotFoundError:
        yield None


def parse_legacyid_from_ddb_records(parent="mipau_crm6_dynamics_com", table='account'):
    legacyid_pat = re.compile(r'\(Legacy CRM ID: (\S{36})\)')
    search_field = None
    for record in get_ddb_records(table=table):
        if record is None:
            continue
        new_id = None
        if f"{table}id" in record:
            new_id = record[f"{table}id"]
        elif "activityid" in record:
            new_id = record["activityid"]

        legacy_id = None
        if record.get('description') is not None:
            match = legacyid_pat.search(record['description'])
            if match is not None:
                legacy_id = match.group(1)

        if not legacy_id:
            ...
        yield table, new_id, legacy_id


if __name__ == '__main__':
    legacyid_mapping = namedtuple('legacyid_mapping', ['table', 'new_id', 'legacy_id', 'new_url', 'old_url'])
    mappings = set()
    for entity in TEMPLATE_ENTITIES:
        for table, new_id, legacy_id in parse_legacyid_from_ddb_records(table=entity):
            new_url = None
            old_url = None
            if new_id is not None:
                new_url = f"https://mipau.crm6.dynamics.com/main.aspx" \
                          f"?appid=5144605b-14ec-ed11-8848-000d3a798df2" \
                          f"&forceUCI=1" \
                          f"&pagetype=entityrecord" \
                          f"&etn={table}" \
                          f"&id={new_id}"
            if legacy_id is not None:
                old_url = f"https://org30a87.crm5.dynamics.com/main.aspx" \
                          f"?appid=c8453e2b-eb35-eb11-a813-000d3a8591e2" \
                          f"&forceUCI=1" \
                          f"&pagetype=entityrecord" \
                          f"&etn={table}" \
                          f"&id={legacy_id}"
            mappings.add(legacyid_mapping(table, new_id, legacy_id, new_url, old_url))
            ...
    df = pd.DataFrame(mappings)
    df.sort_values(by=['table', 'new_id'], inplace=True)
    df.to_csv('../data/import_logs/legacyid_mapping.csv', index=False)
    df.set_index(['table', 'new_id', 'legacy_id'], inplace=True)
    dtype = {col: sqlalchemy.types.NVARCHAR(36) for col in df.index.names}
    load_to_table_chunked(schema="import_report", df=df, dtype=dtype, table_name="legacyid_mapping", chunk_size=2000)
    ...
