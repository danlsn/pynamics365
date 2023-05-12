import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

from crm_migration.migration_etl import get_all_records_to_df_with_name
from pynamics365.client_windows import DynamicsSession
from pynamics365.config import TEMPLATE_ENTITIES, CDI_ENTITY_LIST
from pynamics365.extract_to_load_table import (
    prepare_column_names,
    load_to_table,
    load_to_table_replace,
)

DUCKDB_DB_PATH = "../data/ds20_tables.db"

logger = logging.getLogger(__name__)


def json_extracts_to_tables(entity_list, load_to_sql=True):
    dc = DynamicsSession()
    dc.authenticate()
    dc.set_page_size(5000)
    dfs = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for entity_name in entity_list:
            futures.append(
                executor.submit(get_all_records_to_df_with_name, dc, entity_name)
            )
        for future in as_completed(futures):
            logger.info(f"Future Completed: {entity_name}.")
            entity_name, df = future.result()
            df = prepare_column_names(df)
            dfs[entity_name] = df
            executor.submit(
                load_to_table,
                df,
                entity_name,
                schema="ds20",
                if_exists="append",
                use_mssql=False,
            )
    return dfs


def extract_tables_for_ds20():
    entity_list = (
        [
            "activitypointer",
            "activityparty",
            "phonecall",
            "task",
            "email",
            "territory",
            "systemuser",
            "commitment",
            "campaignactivity",
            "campaignactivityitem",
            "campaignitem",
            "appointment",
            "mip_bpf_3b7c34d35eb9488db89bb8ddbfa9ef95",
            "mip_bpf_aedd48254ef740ba9d97534092bb8248",
            "mip_bpf_d9b760ab65c146968995d39213e67cea",
            "opportunity",
        ]
        + CDI_ENTITY_LIST
        + TEMPLATE_ENTITIES
    )
    entity_set = set(entity_list)
    skip_list = ['cdi_iporganization', 'cdi_anonymousvisitor', ]
    entity_set = entity_set - set(skip_list)
    dfs = json_extracts_to_tables(entity_set)


if __name__ == "__main__":
    extract_tables_for_ds20()
