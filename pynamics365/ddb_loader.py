import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import re
from pathlib import Path
from random import shuffle
from hashlib import md5

import dictdatabase as DDB
import json
import orjson
import pandas as pd
import logging

from tqdm import tqdm

from pynamics365.config import (
    ENTITY_LIST,
    PROD_EXTRACT_PATH,
    PROD_DEFS_PATH,
    PROD_DDB_PATH,
    DDB_PATH,
)
from pynamics365.ddb_test import get_primary_key_for_entity, get_primary_name_for_entity

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
)
logger.addHandler(ch)

dfh = logging.FileHandler("../logs/ddb_loader.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)

DDB.config.storage_directory = DDB_PATH


def get_entity_definitions(dc, logical_name=None):
    dc.authenticate()
    if logical_name:
        entity_definitions = dc.get_all_records(
            "EntityDefinitions", params={"$filter": f"LogicalName eq '{logical_name}'"}
        )
    else:
        entity_definitions = dc.get_all_records("EntityDefinitions")
    df = pd.json_normalize(entity_definitions)
    df.set_index(["LogicalName"])
    return df


def get_endpoint_name(dc, entity_definition):
    candidate_names = set()
    for key, value in entity_definition.items():
        if isinstance(value, dict):
            continue
        if "Name" in key and value:
            candidate_names.add(value.lower())
    entity_name = None
    # Sort candidate names by length descending
    for name in sorted(candidate_names, key=len, reverse=True):
        one_record = dc.get(name, params={"$top": 1})
        if one_record.status_code == 200:
            entity_name = name
            logger.debug(f"Found endpoint for entity: {entity_name}")
            break
    if not entity_name:
        logger.error(
            f"Could not find entity name for {entity_definition['LogicalName']}"
        )
        raise ValueError(
            f"Could not find entity name for {entity_definition['LogicalName']}"
        )
    return entity_name


def load_entity_record_to_ddb(logical_name, record, primary_key=None):
    if not primary_key:
        primary_key = get_primary_key_for_entity(logical_name)
    if primary_key not in record:
        raise Exception(f"Primary key {primary_key} not in record")

    if not DDB.at(logical_name).exists():
        DDB.at(logical_name).create()

    with DDB.at(logical_name).session() as (session, records):
        records[record[primary_key]] = record
        session.write()


def load_many_entity_records_to_ddb(logical_name, records, primary_key=None, **kwargs):
    logger.info(f"Loading {len(records)} records to {logical_name}")
    if not primary_key:
        primary_key = get_primary_key_for_entity(logical_name)
    if primary_key not in records[0]:
        raise Exception(f"Primary key {primary_key} not in record")
    primary_name = get_primary_name_for_entity(logical_name)
    if kwargs.get("environment", None):
        ddb_archive_path = f"{kwargs['environment']}/{logical_name}"
    else:
        ddb_archive_path = logical_name
    if not DDB.at(ddb_archive_path).exists():
        logger.info(f"Creating DDB at {ddb_archive_path}")
        DDB.at(ddb_archive_path).create()
    num_modified = 0
    num_inserted = 0
    with DDB.at(ddb_archive_path).session() as (session, archive):
        len_before = len(archive)
        logger.info(
            f"Opening DDB Session for {ddb_archive_path}: Loaded {len(archive)} records"
        )
        for record in records:
            modified_before = (
                archive[record[primary_key]].get("modifiedon", 0)
                if record[primary_key] in archive
                else None
            )
            modified_after = record.get("modifiedon", 0)
            try:
                if type(record[primary_key]) == str:
                    archive[record[primary_key]] = record
                elif record[primary_key] is None and record[primary_name]:
                    archive[record[primary_name]] = record
                else:
                    logger.warning(f"Primary Key and Primary Name not found for {record}, using md5 hash")
                    md5_key = f"md5:{md5(orjson.dumps(record)).hexdigest()}"
                    archive[md5_key] = record

            except TypeError:
                logger.error(f"Error loading record with primary key {record[primary_key]}: {record}")
                pass
            if modified_before == 0 or modified_before is None:
                num_inserted += 1
            elif modified_after != modified_before:
                num_modified += 1

        len_after = len(archive)
        logger.info(
            f"Closing DDB Session for {ddb_archive_path}: Total: {len_after} records, Inserted: {len_after - len_before}, Modified: {num_modified}"
        )
        session.write()
        logger.info(f"Closed DDB Session for {ddb_archive_path}: Returning ...")
    return


def extract_all_pages_to_ddb(dc, logical_name, **kwargs):
    dc.authenticate()
    environment = dc.resource_path_name
    entity_definition = dc.get_all_records(
        f"EntityDefinitions(LogicalName='{logical_name}')"
    )[0]
    try:
        entity_name = get_endpoint_name(dc, entity_definition)
    except ValueError:
        logger.error(f"Could not find endpoint for {logical_name}")
        return
    page_number = 0
    logger.info(f"Extracting all pages for {entity_name}")
    if kwargs.get("load_all_first", False):
        logger.info(f"Loading all records for {entity_name} first")
        all_records = dc.get_all_records(entity_name)
        num_records = len(all_records)
        size_records = sys.getsizeof(all_records)
        logger.info(
            f"Loaded {num_records} records ({size_records} bytes) for {entity_name}"
        )
        avg_size = size_records / num_records
        # Batch size is 1000 MB
        batch_size = 1_000_000_000 // round(avg_size)
        logger.info(f"Batch size: {batch_size}")
        num_batches = (num_records // batch_size) + 1
        for i in range(0, num_records, batch_size):
            record_batch = all_records[i : i + batch_size]
            logger.info(f"Extracting batch {i + 1} of {num_batches} for {entity_name}")
            load_many_entity_records_to_ddb(
                logical_name,
                all_records,
                primary_key=kwargs.get("primary_key", None),
                environment=environment,
            )
    elif kwargs.get("chunk_size", False):
        chunk_size = kwargs.get("chunk_size", 500_000_000)
        logger.info(
            f"Loading records in chunks of {chunk_size} bytes for {entity_name}"
        )
        records = []

        records_size = 0
        total_size = 0
        chunk_num = 1
        for record in dc.gen_all_records(entity_name):
            records.append(record)
            records_size += sys.getsizeof(record)
            total_size += sys.getsizeof(record)
            if records_size > chunk_size:
                logger.info(
                    f"Extracting chunk {chunk_num} of {len(records)} records ({records_size} bytes) for {entity_name}"
                )
                load_many_entity_records_to_ddb(
                    logical_name,
                    records,
                    primary_key=kwargs.get("primary_key", None),
                    environment=environment,
                )
                records = []
                records_size = 0
                chunk_num += 1
        if records:
            logger.info(
                f"Extracting last chunk {chunk_num} of {len(records)} records ({records_size} bytes) for {entity_name}"
            )
            load_many_entity_records_to_ddb(
                logical_name,
                records,
                primary_key=kwargs.get("primary_key", None),
                environment=environment,
            )
            logger.info(
                f"Extracted {chunk_num} chunk/s, a total of {total_size} bytes for {entity_name}"
            )
        return f"{entity_name}: Extracted {chunk_num} chunks, a total of {total_size} bytes"
    else:
        for page in dc.pages(entity_name):
            if not page["value"]:
                logger.warning(f"Empty page for {entity_name}")
                continue
            page_number += 1
            logger.info(f"{entity_name}: Extracting page {page_number}")
            load_many_entity_records_to_ddb(
                logical_name,
                page["value"],
                primary_key=kwargs.get("primary_key", None),
                environment=environment,
            )
        return f"{entity_name}: Extracted {page_number} pages"
