import concurrent
import logging
import os
import sys
from pathlib import Path
from random import random, shuffle

import pandas as pd

from client_windows import DynamicsSession, entity_attrs_to_csv, entity_defs_to_csv, extract_all_pages_to_json, \
    extract_entities_to_jsonl
from dotenv import load_dotenv

from pynamics365.config import DEV_DEFS_PATH
from pynamics365.ddb_loader import extract_all_pages_to_ddb
from pynamics365.ddb_test import load_definitions
from pynamics365.prod_environment import full_extract_to_ddb

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
fh = logging.FileHandler('../logs/pynamics365.log')
fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
fh.setLevel(logging.INFO)
logger.addHandler(ch)
logger.addHandler(fh)


def main():
    dc = DynamicsSession(auth_url=os.getenv("MSDYN_AUTH_URL"), client_id=os.getenv("MSDYN_CLIENT_ID"),
                         username=os.getenv("MSDYN_USERNAME"), password=os.getenv("MSDYN_PASSWORD"),
                         resource=os.getenv("MSDYN_DEV_RESOURCE"))
    dc.authenticate()
    dc.set_page_size(5000)
    resource_path_name = dc.resource_path_name

    output_path = Path(r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract")
    entity_definitions = dc.get_all_records("EntityDefinitions")
    df = pd.json_normalize(entity_definitions)
    df.set_index(["LogicalName", "SchemaName", "LogicalCollectionName", "ObjectTypeCode", "IsLogicalEntity",
                  "HasActivities", "TableType", "Description.UserLocalizedLabel.Label", ],
                 inplace=True)
    output_path = output_path
    definitions_path = output_path / resource_path_name / "_Definitions" / "EntityDefinitions.csv"
    definitions_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(definitions_path, index=True)
    entity_definitions = load_definitions(defs_path=definitions_path)
    logical_names = entity_definitions.index.tolist()
    for logical_name in logical_names:
        try:
            primary_key = entity_definitions.loc[logical_name].PrimaryIdAttribute
            extract_all_pages_to_ddb(dc, logical_name, primary_key=primary_key)
        except KeyError:
            logger.error(f"KeyError: {logical_name}")
            continue
    # for entity_definition in entity_definitions:
    #     entity_attrs_to_csv(dc, entity_definition['LogicalName'], output_path)
    #     entity_defs_to_csv(dc, entity_definition['LogicalName'], output_path)
    #     extract_all_pages_to_json(dc, entity_definition, output_path)
    #     ...

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        # shuffle(entity_definitions)
        for entity_definition in entity_definitions:
            futures.append(executor.submit(entity_attrs_to_csv, dc, entity_definition['LogicalName'], output_path))
            futures.append(executor.submit(entity_defs_to_csv, dc, entity_definition['LogicalName'], output_path))
            # futures.append(executor.submit(extract_entities_to_jsonl, dc, entity_definition, output_path))
            # futures.append(executor.submit(extract_all_pages_to_csv, dc, entity_definition, output_path))
            futures.append(executor.submit(extract_all_pages_to_json, dc, entity_definition, output_path))
        for future in concurrent.futures.as_completed(futures):
            try:
                logger.info(f"Completed: {future.result()}")
            except KeyboardInterrupt:
                print("Exiting")
                executor.shutdown(wait=False)
                sys.exit(1)


if __name__ == "__main__":
    load_dotenv()
    full_extract_to_ddb(entity_list=None, resource=os.getenv("MSDYN_DEV_RESOURCE"), multi_threaded=True)
    main()
