import json

import pandas as pd

from pynamics365.client import DynamicsClient


class DynamicsEntity(DynamicsClient):
    def __init__(self, logical_name, **kwargs):
        super().__init__(**kwargs)

        self.entity_definition = None
        self.logical_name = logical_name
        self.endpoint = f"/{self.logical_name}"
        self.attributes = self.get_attributes()
        self.per_page = kwargs.get("per_page", 1000)
        self.headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json",
            "Prefer": f"odata.include-annotations=\"*\",odata.maxpagesize={self.per_page}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }
        self.names = self.get_entity_names()
        self.record_count = self.get_record_count()
        self.est_pages = (self.record_count // self.per_page) + 1
        self.records = None

    def get_entity_definition(self):
        url = f"{self.base_url}/EntityDefinitions(LogicalName='{self.logical_name}')"
        response = self.get(url)
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
            candidate_names.add(name.lower())
            candidate_names.add(name.replace(" ", "").lower())
        candidate_names = sorted(candidate_names, key=len, reverse=True)
        for name in candidate_names:
            url = f"{self.base_url}/{name}"
            response = self.get(url)
            if response.status_code == 200:
                self.endpoint = f"{name}"
                return self.endpoint
        return None

    def get_attributes(self):
        url = f"{self.base_url}/EntityDefinitions(LogicalName='{self.logical_name}')/Attributes"
        response = self.get(url)
        response.raise_for_status()
        return response.json()['value']

    def get_record_count(self):
        url = f"{self.base_url}/recordcountsnapshots"
        params = {
            "$select": "count,lastupdated",
            "$filter": f"objecttypecode eq {self.entity_definition['ObjectTypeCode']}",
        }
        response = self.get(url, params=params)
        response.raise_for_status()
        self.record_count = int(response.json()['value'][0]['count'])
        return self.record_count

    def get_all_pages(self):
        page = 1
        if not self.endpoint:
            self.get_endpoint()
        url = f"{self.base_url}/{self.endpoint}"
        response = self.get(url)
        response.raise_for_status()
        res_json = response.json()
        self.pages = [res_json]
        next_link = res_json.get('@odata.nextLink', None)
        while next_link:
            response = self.get(next_link)
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
        response = self.get(url)
        response.raise_for_status()
        res_json = response.json()
        self.records = res_json['value']
        next_link = res_json.get('@odata.nextLink', None)
        while next_link:
            response = self.get(next_link)
            response.raise_for_status()
            res_json = response.json()
            self.records.extend(res_json['value'])
            next_link = res_json.get('@odata.nextLink', None)
            page += 1
        return self.records


class DynamicsExtractor(DynamicsEntity):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_path = kwargs.get("output_path", "../data")

    def save_all_pages(self):
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


if __name__ == "__main__":
    main()
