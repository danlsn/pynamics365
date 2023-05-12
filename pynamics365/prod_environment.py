import concurrent
import logging
import os
import sys
from pathlib import Path
from random import random, shuffle

import pandas as pd

from pynamics365.client_windows import (
    DynamicsSession,
    entity_attrs_to_csv,
    entity_defs_to_csv,
    extract_all_pages_to_json,
    extract_entities_to_jsonl,
    extract_all_pages_to_csv,
)
from dotenv import load_dotenv

from pynamics365.config import (
    ENTITY_LIST,
    TEMPLATE_ENTITIES,
    DDB_PATH,
    CDI_ENTITY_LIST,
    ADOBE_ENTITIES,
    MIP_ENTITIES,
)
from pynamics365.ddb_loader import load_many_entity_records_to_ddb, extract_all_pages_to_ddb
import dictdatabase as DDB

DDB.config.storage_directory = DDB_PATH

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
)
fh = logging.FileHandler("../logs/pynamics365.log")
fh.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
)
fh.setLevel(logging.INFO)
logger.addHandler(ch)
logger.addHandler(fh)

dfh = logging.FileHandler("../logs/prod_environment.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)

def full_extract_to_ddb(entity_list=TEMPLATE_ENTITIES, multi_threaded=True, **kwargs):
    dc = DynamicsSession(
        auth_url=os.getenv("MSDYN_AUTH_URL"),
        client_id=os.getenv("MSDYN_CLIENT_ID"),
        username=os.getenv("MSDYN_USERNAME"),
        password=os.getenv("MSDYN_PASSWORD"),
        resource=kwargs.get("resource", os.getenv("MSDYN_RESOURCE")),
    )
    dc.authenticate()
    dc.set_page_size(5000)
    if not entity_list:
        entity_list = dc.get_all_records("EntityDefinitions")
        entity_list = [e["LogicalName"] for e in entity_list]
    if kwargs.get("shuffle", False):
        shuffle(entity_list)
    if multi_threaded:
        with concurrent.futures.ThreadPoolExecutor(max_workers=kwargs.get("max_workers", 6)) as executor:
            futures = [
                executor.submit(
                    extract_all_pages_to_ddb, dc, logical_name, chunk_size=kwargs.get("chunk_size", 200_000_000)
                )
                for logical_name in entity_list
            ]
            futures_remaining = len(futures)
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                    futures_remaining -= 1
                    logger.info(
                        f"Completed {future.result()}: {futures_remaining} remaining"
                    )
                except Exception as e:
                    logger.error(e)
    else:
        for logical_name in entity_list:
            extract_all_pages_to_ddb(dc, logical_name)


def full_extract_to_json(entity_list=TEMPLATE_ENTITIES, multi_threaded=True, **kwargs):
    dc = DynamicsSession(
        auth_url=os.getenv("MSDYN_AUTH_URL"),
        client_id=os.getenv("MSDYN_CLIENT_ID"),
        username=os.getenv("MSDYN_USERNAME"),
        password=os.getenv("MSDYN_PASSWORD"),
        resource=kwargs.get("resource", os.getenv("MSDYN_RESOURCE")),
    )
    dc.authenticate()
    dc.set_page_size(5000)
    if not entity_list:
        entity_list = dc.get_all_records("EntityDefinitions")
        entity_list = [e["LogicalName"] for e in entity_list]
    if kwargs.get("shuffle", False):
        shuffle(entity_list)
    output_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract"
    )
    entity_definitions = dc.get_all_records("EntityDefinitions")
    df = pd.json_normalize(entity_definitions)
    df.set_index(
        [
            "LogicalName",
            "SchemaName",
            "LogicalCollectionName",
            "ObjectTypeCode",
            "IsLogicalEntity",
            "HasActivities",
            "TableType",
            "Description.UserLocalizedLabel.Label",
        ],
        inplace=True,
    )
    output_path = output_path
    definitions_path = output_path / "_Definitions" / "EntityDefinitions.csv"
    definitions_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(definitions_path, index=True)
    # for entity_definition in entity_definitions:
    #     entity_attrs_to_csv(dc, entity_definition['LogicalName'], output_path)
    #     entity_defs_to_csv(dc, entity_definition['LogicalName'], output_path)
    #     extract_all_pages_to_json(dc, entity_definition, output_path)
    #     ...
    if multi_threaded:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            shuffle(entity_definitions)
            for entity_definition in entity_definitions:
                if entity_definition["LogicalName"] not in entity_list:
                    continue
                futures.append(
                    executor.submit(
                        entity_attrs_to_csv,
                        dc,
                        entity_definition["LogicalName"],
                        output_path,
                    )
                )
                futures.append(
                    executor.submit(
                        entity_defs_to_csv,
                        dc,
                        entity_definition["LogicalName"],
                        output_path,
                    )
                )
                futures.append(
                    executor.submit(
                        extract_all_pages_to_json, dc, entity_definition, output_path
                    )
                )
            for future in concurrent.futures.as_completed(futures):
                try:
                    logger.info(f"Completed: {future.result()}")
                except KeyboardInterrupt:
                    print("Exiting")
                    executor.shutdown(wait=False)
                    sys.exit(1)
                except Exception as e:
                    logger.error(f"Error: {e}")
    else:
        shuffle(entity_definitions)
        for entity_definition in entity_definitions:
            if entity_definition["LogicalName"] not in ENTITY_LIST:
                continue
            entity_attrs_to_csv(dc, entity_definition["LogicalName"], output_path)
            entity_defs_to_csv(dc, entity_definition["LogicalName"], output_path)
            extract_all_pages_to_json(dc, entity_definition, output_path)


def main(entity_list=TEMPLATE_ENTITIES, targets=['ddb', 'csv', 'json'], threaded=True):
    dc = DynamicsSession(
        auth_url=os.getenv("MSDYN_AUTH_URL"),
        client_id=os.getenv("MSDYN_CLIENT_ID"),
        username=os.getenv("MSDYN_USERNAME"),
        password=os.getenv("MSDYN_PASSWORD"),
        resource=os.getenv("MSDYN_RESOURCE"),
    )
    dc.authenticate()
    dc.set_page_size(5000)
    if 'ddb' in targets:
        for logical_name in entity_list:
            extract_all_pages_to_ddb(dc, logical_name)
    resource_path_name = dc.resource_path_name
    output_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract"
    )
    entity_definitions = dc.get_all_records("EntityDefinitions")
    df = pd.json_normalize(entity_definitions)
    df.set_index(
        [
            "LogicalName",
            "SchemaName",
            "LogicalCollectionName",
            "ObjectTypeCode",
            "IsLogicalEntity",
            "HasActivities",
            "TableType",
            "Description.UserLocalizedLabel.Label",
        ],
        inplace=True,
    )
    output_path = output_path
    definitions_path = output_path / "_Definitions" / "EntityDefinitions.csv"
    definitions_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(definitions_path, index=True)
    # for entity_definition in entity_definitions:
    #     entity_attrs_to_csv(dc, entity_definition['LogicalName'], output_path)
    #     entity_defs_to_csv(dc, entity_definition['LogicalName'], output_path)
    #     extract_all_pages_to_json(dc, entity_definition, output_path)
    #     ...
    if threaded:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            shuffle(entity_definitions)
            for entity_definition in entity_definitions:
                if entity_definition["LogicalName"] not in entity_list:
                    continue
                futures.append(
                    executor.submit(
                        entity_attrs_to_csv,
                        dc,
                        entity_definition["LogicalName"],
                        output_path,
                    )
                )
                futures.append(
                    executor.submit(
                        entity_defs_to_csv,
                        dc,
                        entity_definition["LogicalName"],
                        output_path,
                    )
                )
                if 'csv' in targets:
                    # futures.append(executor.submit(extract_entities_to_jsonl, dc, entity_definition, output_path))
                    futures.append(
                        executor.submit(
                            extract_all_pages_to_csv, dc, entity_definition, output_path
                        )
                    )
                if 'json' in targets:
                    futures.append(
                        executor.submit(
                            extract_all_pages_to_json, dc, entity_definition, output_path
                        )
                    )
            for future in concurrent.futures.as_completed(futures):
                try:
                    logger.info(f"Completed: {future.result()}")
                except KeyboardInterrupt:
                    print("Exiting")
                    executor.shutdown(wait=False)
                    sys.exit(1)
                except Exception as e:
                    logger.error(f"Error: {e}")
    else:
        shuffle(entity_definitions)
        for entity_definition in entity_definitions:
            if entity_definition["LogicalName"] not in ENTITY_LIST:
                continue
            entity_attrs_to_csv(dc, entity_definition["LogicalName"], output_path)
            entity_defs_to_csv(dc, entity_definition["LogicalName"], output_path)
            extract_all_pages_to_json(dc, entity_definition, output_path)


if __name__ == "__main__":
    full_extract_to_ddb(entity_list=None, multi_threaded=True, max_workers=10, shuffle=True)

    entity_list = [
        "transformationmapping",
        "transformationparametermapping",
        "importentitymapping",
        "entitymap",
        "attributemap",
        "post",
        "postrole",
        "postregarding",
        "postfollow",
        "postcomment",
        "postlike",
        "sharepointsite",
        "sharepointdocument",
        "sharepointdocumentlocation",
        "sharepointdata",
        "entity",
        "attribute",
        "optionset",
        "entitykey",
        "entityrelationship",
        "managedproperty",
        "relationship",
        "relationshipattribute",
        "entityindex",
        "indexattributes",
        "topicmodelconfiguration",
        "topicmodelexecutionhistory",
        "topicmodel",
        "textanalyticsentitymapping",
        "topichistory",
        "knowledgesearchmodel",
        "topic",
        "advancedsimilarityrule",
        "officegraphdocument",
        "msdyn_import",
        "msdyn_linkedanswer",
        "msdyn_page",
        "msdyn_question",
        "msdyn_questiongroup",
        "msdyn_questionresponse",
        "msdyn_questiontype",
        "msdyn_responseaction",
        "msdyn_responsecondition",
        "msdyn_responseoutcome",
        "msdyn_responserouting",
        "msdyn_section",
        "msdyn_survey",
        "msdyn_surveyinvite",
        "msdyn_surveylog",
        "msdyn_surveyresponse",
        "li_configuration",
        "li_inmail",
        "li_message",
        "li_pointdrivepresentationcreated",
        "li_pointdrivepresentationviewed",
        "customapi",
        "customapirequestparameter",
        "customapiresponseproperty",
        "comment",
        "report",
        "reportcategory",
        "reportentity",
        "reportlink",
        "reportvisibility",
    ]
    # full_extract_to_ddb(entity_list=TEMPLATE_ENTITIES, multi_threaded=True)
    # full_extract_to_ddb(entity_list=ADOBE_ENTITIES, multi_threaded=True)
    # full_extract_to_ddb(entity_list=CDI_ENTITY_LIST, multi_threaded=True)
    # full_extract_to_ddb(entity_list=MIP_ENTITIES, multi_threaded=True)
