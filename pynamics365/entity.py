import asyncio
import json
from datetime import datetime, timedelta
from math import ceil
from pathlib import Path

import aiofiles as aiofiles
import httpx as httpx
import requests
# import requests_cache
from aiopath import AsyncPath
from tqdm import tqdm

from pynamics365.main import DynamicsRequest, DynamicsClient
import logging

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
fh = logging.FileHandler('entity.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

# requests_cache.install_cache('pynamics365_cache', backend='sqlite', expire_after=timedelta(days=1))

sem = asyncio.Semaphore(10)


class DynamicsEntity(DynamicsRequest):
    endpoint = None
    names = None
    date_fields = None
    time_fields = None
    record_count = None
    test_record = None
    records = None
    pages = None
    attributes = None

    def __init__(self, dc: DynamicsClient, **kwargs):
        self.entity_record = kwargs
        self.names = {
            'name': kwargs.get('Name'),
            'logical_name': kwargs.get('LogicalName'),
            'schema_name': kwargs.get('SchemaName'),
            'collection_name': kwargs.get('CollectionName'),
            'logical_collection_name': kwargs.get('LogicalCollectionName'),
            'physical_name': kwargs.get('PhysicalName'),
            'base_table_name': kwargs.get('BaseTableName'),
            'entity_set_name': kwargs.get('EntitySetName'),
        }
        try:
            self.names['display_name'] = kwargs.get('DisplayName')['UserLocalizedLabel']['Label']
            self.names['display_collection_name'] = kwargs.get('DisplayCollectionName')['UserLocalizedLabel']['Label']
        except TypeError:
            self.names['display_name'] = None
            self.names['display_collection_name'] = None
        self.object_type_code = kwargs.get('ObjectTypeCode')
        try:
            self.description = kwargs.get('Description')['UserLocalizedLabel']['Label']
        except TypeError:
            self.description = None
        self.dc = dc
        self.session = requests.Session()
        self.headers = dc.headers
        self.params = {}
        self._get_attributes()
        self._get_record_count()
        self._get_test_record()

    def __repr__(self):
        return f"{self.names['display_name']}<Endpoint={self.endpoint}, Count={self.record_count}>"

    def __dict__(self):
        return {
            'endpoint': self.endpoint,
            'names': self.names,
            'date_fields': self.date_fields,
            'time_fields': self.time_fields,
            'record_count': self.record_count,
            'test_record': self.test_record,
            'records': self.records,
            'pages': self.pages,
            'attributes': self.attributes,
        }

    def _get_attributes(self):
        metadata_id = self.entity_record['MetadataId']
        url = f"/EntityDefinitions({metadata_id})/Attributes/"
        response = self.dc.make_request(url)
        self.attributes = response.get('value')
        return self.attributes

    def _get_test_record(self):
        if not self.endpoint:
            self._get_endpoint()
        if not self.test_record:
            self.test_record = self.dc.get_one_record(self.endpoint)
        return self.test_record

    def _get_endpoint(self):
        candidate_endpoints = set()
        for _, name in self.names.items():
            if name and _ not in ['display_name', 'name']:
                candidate_endpoints.add(name.lower())

        # Order candidate endpoints by length, longest first
        candidate_endpoints = sorted(candidate_endpoints, key=len, reverse=True)

        for candidate in candidate_endpoints:
            test_record = self.dc.get_one_record(candidate)
            if "error" not in test_record:
                self.endpoint = candidate
                break
        return self.endpoint

    def _get_date_fields(self):
        test_record = self._get_test_record()
        date_fields = []
        time_fields = []
        for field in test_record:
            if "date" in field:
                date_fields.append(field)
            if "time" in field:
                time_fields.append(field)
        ...

    def _get_record_count(self):
        if not self.endpoint:
            self._get_endpoint()
        if not self.record_count:
            url_path = f"/recordcountsnapshots"
            params = {
                "$select": "count,lastupdated",
                "$filter": f"objecttypecode eq {self.object_type_code}",
            }
            response = self.dc.make_request(url_path, params=params)
            if response.get("value"):
                self.record_count = response.get("value")[0].get("count")
                self.record_last_updated = response.get("value")[0].get("lastupdated")
            else:
                self.record_count = 0
                self.record_last_updated = None
        return self.record_count


class DynamicsEntityExtractor(DynamicsEntity):
    records_last_fetched = None

    def __init__(self, dc: DynamicsClient, use_cache=False, **kwargs):
        super().__init__(dc, **kwargs)
        self.session = requests.Session()
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Prefer": 'odata.include-annotations="*",odata.maxpagesize=1000',
            "Accept": "application/json",
        }
        self._get_endpoint()
        self._get_record_count()
        self._get_test_record()
        self._get_date_fields()
        self.estimated_pages = self.estimated_pages()
        self.pages = []
        self.records = []

    def get_all_records(self, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        if not self.record_count:
            self._get_record_count()
        if not self.records or self.records_last_fetched < datetime.now() - timedelta(minutes=5):
            self.records = self.dc.get_all_records(self.endpoint)
            self.records_last_fetched = datetime.now()
        return self.records

    def estimated_pages(self):
        if not self.record_count:
            self._get_record_count()
        if not self.headers.get("Prefer"):
            return self.record_count / 5000
        else:
            prefer_headers = self.headers.get("Prefer").split(",")
            for header in [h for h in prefer_headers if "odata.maxpagesize" in h]:
                page_size = header.split("=")[1]
                return ceil(self.record_count / int(page_size))

    def get_one_page(self, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        response, next_link = self.get_first_page(**kwargs)
        yield response

    def get_first_page(self, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        params = kwargs.get("params", self.params)
        headers = kwargs.get("headers", self.headers)
        response = self.dc.make_request(self.endpoint, params=params, headers=headers)
        next_link = response.get("@odata.nextLink")
        return response, next_link or None

    async def aget_first_page(self, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        params = kwargs.get("params", self.params)
        headers = kwargs.get("headers", self.headers)
        async with httpx.AsyncClient() as client:
            request_url = self.dc.base_url + self.endpoint
            response = await client.get(self.endpoint, params=params, headers=headers)
            next_link = response.get("@odata.nextLink")
            return response, next_link or None

    def get_next_page(self, next_link, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        params = kwargs.get("params", self.params)
        headers = kwargs.get("headers", self.headers)
        response = self.dc.make_request(request_url=next_link, params=params, headers=headers)
        next_link = response.get("@odata.nextLink")
        return response, next_link or None

    async def aget_next_page(self, next_link, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        params = kwargs.get("params", self.params)
        headers = kwargs.get("headers", self.headers)
        async with httpx.AsyncClient() as client:
            response = await client.get(next_link, params=params, headers=headers)
            next_link = response.get("@odata.nextLink")
            return response, next_link or None

    def get_all_pages(self, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        params = kwargs.get("params", self.params)
        headers = {
            "Authorization": f"Bearer {self.dc.token}",
            "Prefer": 'odata.include-annotations="*",odata.maxpagesize=1000',
            "Accept": "application/json",
        }
        first_page = self.get_first_page(headers=headers, **kwargs)
        match first_page:
            case (response, next_link):
                self.pages = [response]
                yield response
                while next_link:
                    next_page = self.get_next_page(next_link, headers=headers, **kwargs)
                    match next_page:
                        case (response, next_link):
                            self.pages.append(response)
                            yield response
                        case (response):
                            self.pages.append(response)
                            yield response
                            next_link = None
            case (response):
                next_link = None
                self.pages = [response]
                yield response

    async def aget_all_pages(self, **kwargs):
        if not self.endpoint:
            self._get_endpoint()
        params = kwargs.get("params", self.params)
        headers = kwargs.get("headers", self.headers)
        first_page = await self.aget_first_page(**kwargs)
        pages = []
        match first_page:
            case (response, next_link):
                pages = [response]
                while next_link:
                    next_page = await self.aget_next_page(next_link, **kwargs)
                    match next_page:
                        case (response, next_link):
                            pages.append(response)
                        case (response):
                            pages.append(response)
                            next_link = None
            case (response):
                pages = [response]
                next_link = None
        return pages

    def save_all_pages_to_json(self, output_dir, **kwargs):
        logger.debug(f"Saving est. {self.estimated_pages} pages of {self.names['logical_name']} to {output_dir}.")
        page_number = 1
        file_path = Path(output_dir) / Path(self.names["logical_name"])
        file_path.mkdir(parents=True, exist_ok=True)
        for page in self.get_all_pages(**kwargs):
            with open(f"{file_path}/{self.names['logical_name']}_extract_page_{page_number}.json", "w") as f:
                json.dump(page, f, indent=2)
                logger.debug(f"Saved page {page_number}/{self.estimated_pages} of {self.names['logical_name']}")
            page_number += 1

    async def asave_all_pages_to_json(self, output_dir, **kwargs):
        logger.debug(f"Saving est. {self.estimated_pages} pages of {self.names['logical_name']} to {output_dir}.")
        page_number = 1
        file_path = AsyncPath(output_dir) / AsyncPath(self.names["logical_name"])
        all_pages = await self.aget_all_pages(**kwargs)
        async for page in all_pages:
            await file_path.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(f"{file_path}/{self.names['logical_name']}_extract_page_{page_number}.json",
                                     "w") as f:
                await f.write(json.dumps(page, indent=2))
                logger.debug(f"Saved page {page_number}/{self.estimated_pages} of {self.names['logical_name']}")
            page_number += 1


def main():
    logger.info("Starting")
    dc = DynamicsClient()
    logger.info("Got DynamicsClient")
    entity_dict = dc.get_entity_list(use_cache=False)
    logger.info(f"Got entity list of {len(entity_dict)} entities")
    entities = []
    for logical_name, entity in tqdm(entity_dict.items()):
        # if logical_name not in ["attribute", "account", "contact", "lead", "opportunity", "systemuser", "opportunityclose",
        #                         "opportunityproduct", "pricelevel", "product", "productpricelevel", "quote",
        #                         "quoteclose", "quotedetail", "uom", "uomschedule"]:
        #     continue
        logger.debug(f"Processing {logical_name}")
        de = DynamicsEntityExtractor(dc, use_cache=False, **entity)
        logger.debug(f"Got {de}")
        if de.endpoint:
            entities.append(de)
            # de.save_all_pages_to_json("../data")

    # Sort entities by record count
    entities.sort(key=lambda x: x.record_count, reverse=False)
    for de in entities:
        de.save_all_pages_to_json("../data")
    ...


async def get_entity(entity_dict, dc):
    async with sem:
        de = DynamicsEntityExtractor(dc, use_cache=False, **entity_dict)
        logger.debug(f"Got {de}")
        if de.endpoint:
            await de.asave_all_pages_to_json("../data")
        else:
            return None


async def amain():
    logger.info("Starting")
    dc = DynamicsClient()
    logger.info("Got DynamicsClient")
    entity_dict = dc.get_entity_list(use_cache=True)
    logger.info(f"Got entity list of {len(entity_dict)} entities")
    entities = []
    tasks_todo = []
    for logical_name, entity in entity_dict.items():
        # if logical_name not in ["runtimedependency"]:
        #     continue
        asyncio.create_task(get_entity(entity, dc))
        # tasks_todo.append(asyncio.create_task(get_entity(entity, dc)))
        # de = DynamicsEntityExtractor(dc, use_cache=True, **entity)
        # logger.debug(f"Got {de}")
        # if len(entities) > 20:
        #     break
        # if de.endpoint:
        #     entities.append(de)
        #     # de.save_all_pages_to_json("../data")
    # await asyncio.gather(*tasks_todo)


if __name__ == "__main__":
    # asyncio.run(amain())
    main()
