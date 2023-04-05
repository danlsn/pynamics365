import json
import os
from pathlib import Path
import logging
import pandas as pd
import requests

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
logger.addHandler(ch)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)


class DynamicsEntity:
    def __init__(self, dc: DynamicsClient, logical_name, **kwargs):
        self.dc = dc or DynamicsClient(**kwargs)
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
        self.pages = self.get_pages()
        self.records = self.get_records()
        logger.info(f"Initialized {self.__class__.__name__} for {self.logical_name}")

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
            self.get_endpoint()
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
        if self.dc.token_expired():
            self.dc.refresh_token()
        if not self.endpoint:
            self.get_endpoint()
        url = f"{self.base_url}/{self.endpoint}"
        while url:
            response = requests.get(url, headers=self.headers, params=params, **kwargs)
            response.raise_for_status()
            data = response.json()
            yield data
            url = data.get('@odata.nextLink')

    def save_pages(self, output_path="../data", params=None, **kwargs):
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
        for page in self.get_pages(params, **kwargs):
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
            "LogicalName": self.entity_definition['LogicalName'],
            "DisplayName": self.entity_definition['DisplayName']['UserLocalizedLabel']['Label'],
            "DisplayCollectionName": self.entity_definition['DisplayCollectionName']['UserLocalizedLabel']['Label'],
            "SchemaName": self.entity_definition['SchemaName'],
            # "CollectionName": self.entity_definition['CollectionName'],
            "CollectionSchemaName": self.entity_definition['CollectionSchemaName'],
            "LogicalCollectionName": self.entity_definition['LogicalCollectionName'],
            # "PhysicalName": self.entity_definition['PhysicalName'],
            # "ObjectTypeCode": self.entity_definition['ObjectTypeCode'],
            # "BaseTableName": self.entity_definition['BaseTableName'],
            "EntitySetName": self.entity_definition['EntitySetName'],
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


class DynamicsExtractor(DynamicsEntity):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_path = kwargs.get("output_path", "../data")

    def save_all_pages(self):
        ...


def extract_all_entities(dc, output_path="../data"):
    logger.info(f"Extracting all entities from {dc.environment}")
    url = f"{dc.base_url}/EntityDefinitions"
    response = dc.get(url)
    response.raise_for_status()
    entities = response.json()['value']
    for entity in entities:
        de = DynamicsExtractor(dc=dc, logical_name=entity['LogicalName'])
        de.save_pages(output_path=output_path) if de.record_count else logger.info(
            f"Skipping {de.logical_name} as it has no records")


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
    extract_all_entities(dc)


if __name__ == "__main__":
    test_extract()
