import signal
import json
import logging
import re
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path
from random import shuffle

import dictdatabase as DDB
import orjson
import pandas as pd
from tqdm import tqdm

from pynamics365.config import PROD_DEFS_PATH, PROD_DDB_PATH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)

DDB.config.storage_directory = PROD_DDB_PATH


def load_extract_dir_entities(prod_extract_path):
    for entity in prod_extract_path.iterdir():
        if entity.is_dir():
            yield entity


def load_extract_dir_entity_files(entity_dir):
    for entity_file in entity_dir.glob("*.json"):
        if entity_file.is_file():
            yield entity_file


def load_extract_dir_entity_file_records(entity_file):
    with open(entity_file, "r") as f:
        return json.load(f)['value']


def load_definitions(defs_path=PROD_DEFS_PATH):
    df = pd.read_csv(defs_path)
    df.set_index(["LogicalName"], inplace=True)
    return df


def load_extract_dir_entity_file_records(entity_dir):
    for entity_file in load_extract_dir_entity_files(entity_dir):
        yield load_extract_dir_entity_file_records(entity_file)


@lru_cache
def get_primary_key_for_entity(logical_name):
    defs = load_definitions()
    primary_id = defs.loc[logical_name]["PrimaryIdAttribute"]
    return primary_id


@lru_cache
def get_primary_name_for_entity(logical_name):
    defs = load_definitions()
    primary_id = defs.loc[logical_name]["PrimaryNameAttribute"]
    return primary_id


def load_extract_file_records(extract_file):
    with open(extract_file, "r") as f:
        return json.load(f)['value']


def get_ddb_key(logical_name, entity_record):
    primary_id = get_primary_key_for_entity(logical_name)
    version_key = None
    if primary_id not in entity_record:
        logger.warning(f"Primary key {primary_id} not found in record {entity_record}")
        return None
    if "versionnumber" in entity_record:
        version_key = "versionnumber"
    elif "@odata.etag" in entity_record:
        version_key = "@odata.etag"
    elif "modifiedon" in entity_record:
        version_key = "modifiedon"
    elif "createdon" in entity_record:
        version_key = "createdon"
    elif "createddatetime" in entity_record:
        version_key = "createddatetime"
    elif "changedon" in entity_record:
        version_key = "changedon"

    if version_key == "@odata.etag":
        version_key = "versionnumber"
        etag_pat = re.compile(r"^W/\"(\d+)\"$")
        version_number = etag_pat.match(entity_record["@odata.etag"]).group(1)
    if version_key is None:
        version_key = "___"
        version_number = 1
    else:
        version_number = entity_record[version_key]
    return primary_id, version_key, version_number


def entity_records_to_ddb(entity_dir):
    logical_name = entity_dir.name
    if not DDB.at(logical_name).exists():
        DDB.at(logical_name).create()
    entity_files = load_extract_dir_entity_files(entity_dir)
    with DDB.at(logical_name).session() as (session, records):
        num_ddb_records = len(records)
        for file in entity_files:
            logger.debug(f"Processing {file.name}...")
            with open(file, "r") as f:
                data = orjson.loads(f.read())
            for record in data["value"]:
                primary_id, version_key, version_number = get_ddb_key(logical_name, record)
                if records.get(primary_id) is None:
                    records[primary_id] = {}
                if records[primary_id].get(f"{version_key}:{version_number}") is None:
                    records[primary_id][f"{version_key}:{version_number}"] = {}
                records[primary_id][f"{version_key}:{version_number}"] = record
            session.write()


def load_extract_file_to_ddb_and_delete(extract_file):
    extract_filename_pat = re.compile(r"^(?P<logical_name>[\w_]+)-extract-page-(?P<page_number>\d+).json$")
    match = extract_filename_pat.match(extract_file.name)
    if match is None:
        logger.debug(f"Skipping {extract_file.name}...")
        return
    logical_name = match.group("logical_name")
    extract_file_records = load_extract_file_records(extract_file)
    if not DDB.at(logical_name).exists():
        DDB.at(logical_name).create()
    with DDB.at(logical_name).session() as (session, records):
        try:
            num_ddb_records_before = len(records)
            logger.debug(f"Loaded {num_ddb_records_before} records from DDB for {logical_name}.")
            for record in extract_file_records:
                primary_id, version_key, version_number = get_ddb_key(logical_name, record)
                if records.get(primary_id) is None:
                    records[primary_id] = {}
                if records[primary_id].get(f"{version_key}:{version_number}") is None:
                    records[primary_id][f"{version_key}:{version_number}"] = {}
                records[primary_id][f"{version_key}:{version_number}"] = record
            logger.info(f"Writing {len(records)} records to DDB for {logical_name}...")
            session.write()
            logger.info(f"Session Saved. Continuing...")
        except KeyboardInterrupt:
            logger.error(f"KeyboardInterrupt. Saving Session Then Exiting...")
            session.write()
            logger.error(f"Session Saved. Exiting...")
            raise
        finally:
            num_ddb_records_after = len(records)
            num_records_added = num_ddb_records_after - num_ddb_records_before
            logger.debug(f"Added {num_records_added} records to DDB from {'/'.join(extract_file.parts[-4:])}.")
    try:
        # extract_file.unlink()
        logger.info(f"Deleting {extract_file.name}...")
        # os.remove(extract_file)
        if not extract_file.exists():
            logger.info(f"Deleted {extract_file.name}.")
        else:
            logger.error(f"Unable to delete {extract_file.name}.")
    except PermissionError:
        logger.error(f"Unable to delete {extract_file.name}.")
        raise


def reindex_ddb(ddb_path=PROD_DDB_PATH):
    for entity in tqdm(ddb_path.glob("*.json")):
        if entity.suffix == ".json":
            entity_name = entity.stem
        else:
            return
        try:
            with DDB.at(entity_name).session() as (session, records):
                logger.debug(f"Reindexing {entity_name}...")
                records_to_delete = []
                records_to_add = {}
                for index, record in records.items():
                    try:
                        primary_id, version_key, version = index.split(":", 3)
                    except ValueError:
                        index_parts = index.split(":")
                        primary_id = index_parts[0]
                        if len(index_parts) == 1:
                            version_key = "___"
                            version = "1"
                        elif len(index_parts) == 2:
                            version_key = index_parts[1]
                            version = "1"
                        else:
                            version_key = index_parts[1] or "___"
                            version = ":".join(index_parts[2:])
                    if records_to_add.get(primary_id) is None:
                        records_to_add[primary_id] = {}
                    records_to_add[primary_id][f"{version_key}:{version}"] = record
                    records_to_delete.append(index)
                for index, record in records_to_add.items():
                    records[index] = record
                for index in records_to_delete:
                    del records[index]
                session.write()
        except Exception as e:
            logger.error(f"Error reindexing {entity_name}: {e}")


def multithreaded():
    # prod_extract_path = PROD_EXTRACT_PATH / "current" / "json-5k"
    # for entity_dir in load_extract_dir_entities(prod_extract_path):
    #     entity_records_to_ddb(entity_dir)
    futures = []
    exiting = threading.Event()

    def signal_handler(sig, frame):
        logger.error(f"KeyboardInterrupt. Exiting...")
        exiting.set()

    signal.signal(signal.SIGINT, signal_handler)

    with ThreadPoolExecutor(max_workers=4) as executor:
        incremental_extract_path = Path(r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract")
        json_files = list(incremental_extract_path.glob("**/*-extract-page-*.json"))
        shuffle(json_files)
        futures = executor.map(load_extract_file_to_ddb_and_delete, json_files)
        # for json_file in json_files:
        #     load_extract_file_to_ddb_and_delete(json_file)
        results = []
        for future in tqdm(futures):
            try:
                results.append(future.result())
            except KeyboardInterrupt:
                logger.error(f"KeyboardInterrupt. Exiting...")
                raise


def main():
    incremental_extract_path = Path(r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract")
    json_files = list(incremental_extract_path.glob("**/*-extract-page-*.json"))
    shuffle(json_files)
    for json_file in tqdm(json_files):
        load_extract_file_to_ddb_and_delete(json_file)


if __name__ == "__main__":
    main()
    # reindex_ddb()
