import json
import os
import pickle
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging

import aiofiles
import pandas as pd
import requests
import requests_cache
import asyncio
import aiohttp
from aiopath import AsyncPath
from tqdm.asyncio import tqdm

from pynamics365.client import DynamicsClient

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler(f"../logs/pynamics365_{__name__}.log")
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# logger.addHandler(ch)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)


class DynamicsEntity:
    def __init__(self, dc: DynamicsClient, logical_name, **kwargs):
        self.params = None
        self.dc = dc or DynamicsClient(**kwargs)
        self.environment = self.dc.environment
        self.entity_definition = None
        self.base_url = self.dc.base_url
        self.logical_name = logical_name
        self.names = self.get_entity_names()
        self.endpoint = self.get_endpoint()
        self.attributes = self.get_attributes()
        self.per_page = kwargs.get("per_page", 1000)
        self.headers = {
            "Authorization": self.dc.auth_token,
            "Content-Type": "application/json",
            "Prefer": f"odata.include-annotations=\"*\",odata.maxpagesize={self.per_page}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }
        self.record_count = self.get_record_count()
        self.est_pages = (self.record_count // self.per_page) + 1
        self.pages = None
        self.records = None
        self.last_updated = int(time.time())
        self.filter = kwargs.get("filter", None)
        logger.info(f"Initialized {self.__class__.__name__} for {self.logical_name}")

    def set_filter(self, type, value, unit):
        if type == 'before':
            type = 'lt'
        elif type == 'after':
            type = 'gt'
        else:
            return
        filter_candidates = ['modifiedon', 'createdon', 'scheduledstart']
        attribute_names = [a['LogicalName'] for a in self.attributes if a['LogicalName'] in filter_candidates]
        if attribute_names:
            attribute_name = attribute_names[0]
        else:
            logger.warning(f"Could not find a suitable attribute for filtering. Candidates: {filter_candidates}")
            return
        if unit == 'days':
            value = datetime.now() - timedelta(days=value)
            value = value.strftime('%Y-%m-%dT00:00:00Z')
        else:
            return
        self.filter = f"{attribute_name} {type} {value}"
        self.params = {"$filter": self.filter}

    def update(self):
        if int(time.time()) - self.last_updated < 3600:
            return
        self.record_count = self.get_record_count()
        self.est_pages = (self.record_count // self.per_page) + 1
        self.pages = None
        self.records = None
        self.last_updated = int(time.time())
        logger.info(f"Updated {self.__class__.__name__} for {self.logical_name}")

    def get_all_pages(self):
        page = 1
        if not self.endpoint:
            self.get_endpoint()
        url = f"{self.base_url}/{self.endpoint}"
        response = self.dc.get(url)
        response.raise_for_status()
        res_json = response.json()
        self.pages = [res_json]
        next_link = res_json.get('@odata.nextLink', None)
        while next_link:
            response = self.dc.get(next_link)
            response.raise_for_status()
            res_json = response.json()
            self.pages.append(res_json)
            next_link = res_json.get('@odata.nextLink', None)
        return self.pages

    def get_all_records(self):
        page = 1
        if not self.endpoint:
            endpoint = self.get_endpoint()
            if not endpoint:
                logger.warning(f"No endpoint found for {self.logical_name}")
                return
        url = f"{self.base_url}/{self.endpoint}"
        response = self.dc.get(url)
        response.raise_for_status()
        res_json = response.json()
        self.records = res_json['value']
        next_link = res_json.get('@odata.nextLink', None)
        while next_link:
            response = self.dc.get(next_link)
            response.raise_for_status()
            res_json = response.json()
            self.records.extend(res_json['value'])
            next_link = res_json.get('@odata.nextLink', None)
            page += 1
        return self.records

    def get_pages(self, params=None, **kwargs):
        if not params:
            params = self.params

        if not self.endpoint:
            endpoint = self.get_endpoint()
            if not endpoint:
                logger.warning(f"No endpoint found for {self.logical_name}")
                return
        url = f"{self.base_url}/{self.endpoint}"
        page_num = 0
        while url:
            page_num += 1
            if self.dc.token_expired():
                self.dc.refresh_token()
            self.headers['Authorization'] = self.dc.auth_token
            logger.info(f"Getting page {page_num}/{self.est_pages} of {self.logical_name}")
            response = requests.get(url, headers=self.headers, params=params, **kwargs)
            if response.status_code == 401:
                logger.info("Token expired. Refreshing token.")
                self.dc.refresh_token()
                response = requests.get(url, headers=self.headers, params=params, **kwargs)
            data = response.json()
            if isinstance(response, requests_cache.CachedResponse):
                logger.debug(f"Got page {page_num}/{self.est_pages} from cache")
            yield data
            url = data.get('@odata.nextLink')

    def save_pages(self, output_path="../data", params=None, **kwargs):
        if not params:
            params = self.params
        logger.info(f"Saving est. {self.est_pages} pages of {self.logical_name}")
        output_path = Path(output_path) / self.dc.environment / self.logical_name
        logger.info(f"Saving to {output_path}")
        output_path.mkdir(parents=True, exist_ok=True)
        page_number = 0
        num_records = 0
        for page in self.get_pages(params, **kwargs):
            page_number += 1
            num_records += len(page['value'])
            logger.info(
                f"Saving page {page_number}/{self.est_pages} of {self.logical_name}. Got {num_records} records so far.")
            filename = f"{output_path}/{self.logical_name}_extract_page_{page_number}.json"
            with open(filename, 'w') as f:
                json.dump(page, f, indent=2)
        logger.info(f"Save complete for {self.logical_name}. Got {num_records} records from {page_number} pages.")

    def get_records(self, params=None, **kwargs):
        if not params:
            params = self.params
        for page in self.get_pages(params, **kwargs):
            if 'value' in page:
                yield from page['value']

    def get_entity_definition(self):
        url = f"{self.base_url}/EntityDefinitions(LogicalName='{self.logical_name}')"
        response = self.dc.get(url)
        response.raise_for_status()
        self.entity_definition = response.json()
        return response.json()

    def get_entity_names(self):
        if not self.entity_definition:
            self.get_entity_definition()
        names = {
            "LogicalName": safeget(self.entity_definition, 'LogicalName'),
            "DisplayName": safeget(self.entity_definition, 'DisplayName', 'UserLocalizedLabel', 'Label'),
            # "DisplayName": self.entity_definition.get('DisplayName', {}).get('UserLocalizedLabel', {}).get('Label'),
            "DisplayCollectionName": safeget(self.entity_definition, 'DisplayCollectionName', 'UserLocalizedLabel',
                                             'Label'),
            # "DisplayCollectionName": self.entity_definition.get('DisplayCollectionName', {}).get('UserLocalizedLabel', {}).get('Label'),
            "SchemaName": self.entity_definition.get('SchemaName'),
            "CollectionName": self.entity_definition.get('CollectionName'),
            "CollectionSchemaName": self.entity_definition.get('CollectionSchemaName'),
            "LogicalCollectionName": self.entity_definition.get('LogicalCollectionName'),
            "PhysicalName": self.entity_definition.get('PhysicalName'),
            # "ObjectTypeCode": self.entity_definition['ObjectTypeCode'],
            # "BaseTableName": self.entity_definition.get('BaseTableName'],
            "EntitySetName": self.entity_definition.get('EntitySetName'),
            # "PrimaryIdAttribute": self.entity_definition['PrimaryIdAttribute'],
            # "PrimaryNameAttribute": self.entity_definition['PrimaryNameAttribute'],
        }
        self.names = names
        return names

    def get_one_row(self):
        ...

    def get_endpoint(self):
        if not self.names:
            self.get_entity_names()
        candidate_names = set()
        for name in self.names.values():
            if not name:
                continue
            candidate_names.add(name.lower())
            candidate_names.add(name.replace(" ", "").lower())
        candidate_names = sorted(candidate_names, key=len, reverse=True)
        for name in candidate_names:
            url = f"{self.base_url}/{name}"
            response = self.dc.get(url)
            if response.status_code == 200:
                self.endpoint = f"{name}"
                return self.endpoint
        return None

    def get_attributes(self):
        url = f"{self.base_url}/EntityDefinitions(LogicalName='{self.logical_name}')/Attributes"
        response = self.dc.get(url)
        response.raise_for_status()
        return response.json()['value']

    def get_record_count(self):
        url = f"{self.base_url}/recordcountsnapshots"
        params = {
            "$select": "count,lastupdated",
            "$filter": f"objecttypecode eq {self.entity_definition['ObjectTypeCode']}",
        }
        response = self.dc.get(url, params=params)
        response.raise_for_status()
        try:
            self.record_count = int(response.json()['value'][0]['count'])
        except IndexError:
            self.record_count = 0
        return self.record_count


class DynamicsExtractor:
    def __init__(self, de: DynamicsEntity, **kwargs):
        self.de = de

        self.output_path = kwargs.get("output_path", "../data")

    def save_all_pages(self):
        ...


def safeget(input, *args):
    if not input:
        return None
    if len(args) == 0:
        return input
    try:
        return safeget(input[args[0]], *args[1:])
    except (KeyError, IndexError, TypeError):
        return None


def extract_all_entities(dc, output_path="../data"):
    # requests_cache.install_cache("dynamics_cache", backend="sqlite", expire_after=3600)
    logger.info(f"Extracting all entities from {dc.environment}")
    url = f"{dc.base_url}/EntityDefinitions"
    response = dc.get(url)
    response.raise_for_status()
    entities = response.json()['value']
    entity_definitions = {}
    # entity_pkl_names = []
    # for pkl in Path("../pkl/entity_definitions").rglob("*.pkl"):
    #     with open(pkl, 'rb') as f:
    #         pkl = pickle.load(f)
    #         entity_pkl_names.append(pkl['LogicalName'])
    #         entity_definitions[pkl['LogicalName']] = pkl
    entities = [e for e in entities if e['LogicalName'] in ['contact', 'account', 'opportunity', 'lead', 'systemuser']]
    for entity in entities:
        logical_name = entity['LogicalName']
        if logical_name in entity_definitions.keys():
            logger.info(f"Unpickling {logical_name} as it has already been extracted")
            de = entity_definitions[logical_name]
        else:
            de = DynamicsEntity(dc=dc, logical_name=entity['LogicalName'])
            # pickle_entity(de)
        de.save_pages(output_path=output_path) if de.record_count > 0 else logger.info(
            f"Skipping {de.logical_name} as it has no records")

    for de in entity_definitions:
        de.save_pages(output_path=output_path) if de.record_count > 0 else logger.info(
            f"Skipping {de.logical_name} as it has no records")


def pickle_all_entities(dc, output_path="../pkl"):
    requests_cache.install_cache("dynamics_cache", backend="sqlite", expire_after=3600)
    logger.info(f"Extracting all entities from {dc.environment}")
    url = f"{dc.base_url}/EntityDefinitions"
    response = dc.get(url)
    response.raise_for_status()
    entities = response.json()['value']
    for entity in entities:
        de = DynamicsEntity(dc=dc, logical_name=entity['LogicalName'])
        # Pickle the entity definition
        filename = Path(f"{output_path}/entity_definitions/{de.logical_name}.pkl")
        filename.parent.mkdir(parents=True, exist_ok=True)
        with open(filename, 'wb') as f:
            logger.info(f"Pickling {de.logical_name} to {filename}")
            pickle.dump(de, f)


def pickle_entity(de, overwrite=True, output_path="../pkl"):
    logger.info(f"Pickling {de.logical_name} from {de.environment} to {output_path}")
    filename = Path(f"{output_path}/entity_definitions/{de.logical_name}.pkl")
    filename.parent.mkdir(parents=True, exist_ok=True)
    with open(filename, 'wb') as f:
        logger.info(f"Pickling {de.logical_name} to {filename}")
        pickle.dump(de, f)


async def apickle_entity(dc, logical_name, overwrite=True, output_path="../pkl"):
    de = DynamicsEntity(dc=dc, logical_name=logical_name)
    logger.info(f"Extracting {de.logical_name} from {de.environment} asynchronously")
    # Pickle the entity definition
    filename = AsyncPath(f"{output_path}/entity_definitions/{de.logical_name}.pkl")
    await filename.parent.mkdir(parents=True, exist_ok=True)
    if await filename.exists() and not overwrite:
        logger.info(f"Skipping {de.logical_name} as it has already been extracted")
        return
    async with aiofiles.open(filename, 'wb') as f:
        logger.info(f"Pickling {de.logical_name} to {filename}")
        await f.write(pickle.dumps(de))


async def apickle_all_entities(dc, output_path="../pkl"):
    logger.info(f"Extracting all entities from {dc.environment} asynchronously")
    url = f"{dc.base_url}/EntityDefinitions"
    response = await dc.get_async(url)
    entities = response['value']
    tasks = []
    for entity in entities:
        tasks.append(apickle_entity(dc, entity['LogicalName'], output_path=output_path))
    await asyncio.gather(*tasks)


def new_client_test():
    load_dotenv()
    dc_prod = DynamicsClient(
        auth_url=os.getenv("MSDYN_AUTH_URL"),
        grant_type=os.getenv("MSDYN_GRANT_TYPE"),
        resource=os.getenv("MSDYN_RESOURCE"),
        client_id=os.getenv("MSDYN_CLIENT_ID"),
        username=os.getenv("MSDYN_USERNAME"),
        password=os.getenv("MSDYN_PASSWORD"),
        token_path=os.getenv("MSDYN_TOKEN_PATH"),
    )
    dc_dev = DynamicsClient(
        auth_url=os.getenv("MSDYN_AUTH_URL"),
        base_url=os.getenv("MSDYN_DEV_BASE_URL"),
        grant_type=os.getenv("MSDYN_GRANT_TYPE"),
        resource=os.getenv("MSDYN_DEV_RESOURCE"),
        client_id=os.getenv("MSDYN_CLIENT_ID"),
        username=os.getenv("MSDYN_USERNAME"),
        password=os.getenv("MSDYN_PASSWORD"),
        token_path=os.getenv("MSDYN_DEV_TOKEN_PATH"),
    )

    prod_accounts = DynamicsEntity(dc_prod, logical_name="account")
    prod_accounts.save_pages()

    ...


def main():
    entities = ['account', 'contact', 'lead']
    for entity in entities:
        de = DynamicsEntity(entity)
        attributes = de.get_attributes()
        entity_names = de.get_entity_names()
        endpoint = de.get_endpoint()
        record_count = de.get_record_count()
        with open(f"../docs/attributes/{entity}_attributes.json", "w") as f:
            json.dump(attributes, f, indent=4)
            df = pd.json_normalize(attributes)
            # Keep only the attributes we care about
            keep_attrs = [
                "AttributeType",
                "DisplayName.UserLocalizedLabel.Label",
                "Format",
                "LogicalName",
                "SchemaName",
                "MaxLength",
                "DatabaseLength",
                "ImeMode",
                "AttributeOf",
                "Targets",
                "IsPrimaryId",
                "IsPrimaryName",
                "IsCustomAttribute",
                "EntityLogicalName",
                "Description.UserLocalizedLabel.Label",
                "MaxValue",
                "MinValue",
                "Precision",
                "MinSupportedValue",
                "MaxSupportedValue",
            ]
            df = df[keep_attrs]
            df.to_excel(f"../docs/attributes/{entity}_attributes.xlsx", index=False)
        ...


def test_extract():
    load_dotenv()
    dc = DynamicsClient()
    # pickle_all_entities(dc)
    extract_all_entities(dc)


if __name__ == "__main__":
    dc = DynamicsClient()
    # asyncio.run(apickle_all_entities(dc, output_path="../pkl"))
    test_extract()
