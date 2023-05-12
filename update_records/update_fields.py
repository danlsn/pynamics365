import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from tqdm import tqdm

from pynamics365.client_windows import DynamicsSession
from pynamics365.config import (
    TEMPLATE_ENTITIES,
    ENTITY_LIST,
    CDI_ENTITY_LIST,
    DS20_ENTITIES,
)
from dotenv import load_dotenv

load_dotenv(dotenv_path="../crm_migration/.env")

logging.basicConfig(
    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("update_fields.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


class DynamicsUAT(DynamicsSession):
    def __init__(self, **kwargs):
        super().__init__(
            auth_url=os.getenv("MSDYN_AUTH_URL"),
            grant_type="password",
            resource=os.getenv("MSDYN_UAT_RESOURCE"),
            client_id=None,
            username=None,
            password=None,
            token_path="./token_uat.json",
            **kwargs,
        )
        self.authenticate()

    def update_entity_record(self, logical_name, record_id, payload):
        logical_name, entity = self.get_entity_endpoint(logical_name)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Prefer": 'return=representation,odata.include-annotations="*"',
        }
        url = f"{entity}({record_id})"
        response = self.request("PATCH", url, headers=headers, json=payload)
        return response.json()


class DynamicsProd(DynamicsSession):
    def __init__(self, **kwargs):
        auth_url = os.getenv("MSDYN_AUTH_URL")
        super().__init__(
            auth_url=os.getenv("MSDYN_AUTH_URL"),
            grant_type="password",
            resource=os.getenv("MSDYN_RESOURCE"),
            client_id=None,
            username=None,
            password=None,
            token_path="./token_prod.json",
        )
        self.authenticate()


def parse_legacy_crm_id_from_records(records, field="description"):
    crm_id_pat = re.compile(r"\(Legacy CRM ID: (.{36})\)")
    for record in records:
        try:
            match = crm_id_pat.search(record[field])
        except TypeError:
            match = None
        if match:
            record["legacy_crm_id"] = match.group(1)
        else:
            record["legacy_crm_id"] = None
    records = index_records_by_legacy_crm_id(records, field="legacy_crm_id")
    return records


def index_records_by_legacy_crm_id(records, field="legacy_crm_id"):
    index = {}
    for record in records:
        index[record[field]] = record
    return index


def pull_original_createdon_date_from_records(records):
    created_on_index = {}
    for id, record in records.items():
        created_on_index.update(
            {
                id: {
                    "createdon": record.get("createdon"),
                    "createdon_FormattedValue": record.get(
                        "createdon@OData.Community.Display.V1.FormattedValue"
                    ),
                }
            }
        )
    return created_on_index


def map_original_createdon_date_to_uat_records(uat_records, prod_createdon_index):
    for id, record in uat_records.items():
        try:
            record["itk_originalcreatedon"] = prod_createdon_index.get(id).get("createdon")
            record["itk_originalcreatedon_FormattedValue"] = prod_createdon_index.get(id).get("createdon_FormattedValue")
        except AttributeError:
            record["itk_originalcreatedon"] = None
            record["itk_originalcreatedon_FormattedValue"] = None
    return uat_records


def update_original_createdon(ds_prod, ds_uat, logical_name):
    entity_list = [logical_name]
    uat = {
        entity: ds_uat.get_all_records(
            entity, select=[f"{logical_name}id", "description", "createdon"]
        )
        for entity in entity_list
    }
    prod = {
        entity: ds_prod.get_all_records(
            entity, select=[f"{logical_name}id", "description", "createdon"]
        )
        for entity in entity_list
    }
    prod_ix = {
        entity: index_records_by_legacy_crm_id(records, field=f"{logical_name}id")
        for entity, records in prod.items()
    }
    uat_ix = {
        entity: parse_legacy_crm_id_from_records(records)
        for entity, records in uat.items()
    }
    prod_createdon_ix = {
        entity: pull_original_createdon_date_from_records(records)
        for entity, records in prod_ix.items()
    }
    uat = {
        entity: map_original_createdon_date_to_uat_records(
            records, prod_createdon_ix.get(entity)
        )
        for entity, records in uat_ix.items()
    }
    updated_records = {}
    for logical_name, records in uat.items():
        if logical_name not in updated_records:
            updated_records[logical_name] = {}
        futures = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            for record in records.values():
                futures.append(
                    executor.submit(
                        update_record_createdon_date,
                        ds_uat,
                        logical_name,
                        record,
                    )
                )
            for future in tqdm(as_completed(futures), total=len(futures)):
                if future.exception():
                    logger.error(future.exception())
                else:
                    record = future.result()
                    record_id = record.get(f"{logical_name}id")
                    updated_records[logical_name][record_id] = record
        with open(f"../{logical_name}_updated_records.json", 'w') as f:
            json.dump(updated_records, f, indent=4)
    ...

def update_record_createdon_date(ds_uat, logical_name, record):
    updated_record = None
    record_id = record.get(f"{logical_name}id") or record.get("activityid")
    if not record_id:
        raise ValueError(f"Record ID not found for {logical_name}")
    payload = {
        "itk_originalcreatedon": record.get("itk_originalcreatedon")
    }
    # original_record = ds_uat.get_entity_record(logical_name, record_id)
    # if not original_record:
    #     raise ValueError(f"Original record not found for {logical_name}")
    # original_records.update({record_id: original_record})
    updated_record = ds_uat.update_entity_record(
        logical_name, record_id, payload
    )
    if updated_record:
        updated_record = {record_id: updated_record}
        return updated_record


def main():
    ds_uat = DynamicsUAT()
    ds_prod = DynamicsProd()
    entity_set = set(
        [*ENTITY_LIST, *CDI_ENTITY_LIST, *DS20_ENTITIES, *TEMPLATE_ENTITIES]
    )
    update_original_createdon(ds_prod, ds_uat, "quote")
    update_original_createdon(ds_prod, ds_uat, "lead")
    # update_original_createdon(ds_prod, ds_uat, "opportunity")



if __name__ == "__main__":
    main()
