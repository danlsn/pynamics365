import re
from functools import lru_cache
from pathlib import Path

import dictdatabase as DDB
import json

extract_path = Path(r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract")
DDB.config.storage_directory = r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\org30a87_crm5_dynamics_com\versioned"



def extract_versionnumber_from_record(record):
    if 'versionnumber' in record:
        return record['versionnumber']
    elif '@odata.etag' in record:
        etag = record['@odata.etag']
        versionnumber_pat = re.compile(r'(?P<versionnumber>\d+)')
        match = versionnumber_pat.search(etag)
        if match is not None:
            return match.group('versionnumber')
    return None


def load_records_from_extract_file(extract_file):
    records = {}
    entity_name_pat = re.compile(r'(?P<entity>\w+)-extract-page-\d+\.json')
    entity = entity_name_pat.search(extract_file.name).group('entity')
    with open(extract_file, 'r') as f:
        extract = json.load(f)
    if not 'value' in extract:
        return
    for record in extract['value']:
        record_id = record.get(f"{entity}id") or record.get("activityid")
        if record_id is None:
            continue
        version_number = extract_versionnumber_from_record(record)
        if records.get(entity) is None:
            records[entity] = {}
        if records[entity].get(record_id) is None:
            records[entity][record_id] = {}
        if records[entity][record_id].get(version_number) is None:
            records[entity][record_id][version_number] = record
    return records


def load_records_from_extract_dir(extract_dir):
    records = {}
    for extract_file in extract_dir.iterdir():
        records = load_records_from_extract_file(extract_file)
        yield records


def load_extract_file_to_versioned_ddb(extract_file):
    with open(extract_file, 'r') as f:
        extract = json.load(f)
    for entity in extract:
        for record in extract[entity]:
            record_id = record[f"{entity}id"]
            with DDB.at(f"{entity}/{record_id}").session() as (session, records):
                records[record_id] = record


def main():
    records = load_records_from_extract_file(Path(r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract\account\2023-03-02\account-extract-page-2.json"))
    num_processed = 0
    for entity, record_id, version_number, record in load_records_from_extract_file(Path(r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract\account\2023-03-02\account-extract-page-2.json")):
        print(f"[{num_processed}] Processing {entity} {record_id} {version_number}")
        num_processed += 1
        if not DDB.at(f"{entity}").exists():
            DDB.at(f"{entity}").create()
        if not DDB.at(f"{entity}", key=record_id).exists():
            with DDB.at(f"{entity}").session() as (session, records):
                records.update({record_id: {version_number: record}})
                session.write()
            continue
        with DDB.at(f"{entity}").session() as (session, records):
            keys_existing = records[record_id].get(version_number, {}).keys()
            keys_new = record.keys()
            if len(keys_existing) == len(keys_new) and all(key in keys_existing for key in keys_new):
                continue
            elif len(keys_existing) < len(keys_new):
                records[record_id].update({version_number: record})
                session.write()
                continue
            else:
                continue

    extract_files = list(extract_path.rglob("*-extract-page-*.json"))
    ...


if __name__ == "__main__":
    main()
