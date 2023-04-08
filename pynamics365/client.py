import csv
import sys
import concurrent
import json
import os
from datetime import datetime, timedelta
import time
from pathlib import Path
from urllib.parse import urlparse
import asyncio
import aiohttp
import aiopath
import aiofiles
import pandas as pd
# import requests_cache
import requests
from rich import print
from dotenv import load_dotenv
import logging
from pynamics365.auth import DynamicsAuth

load_dotenv()


# requests_cache.install_cache('pynamics365_cache')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
fh = logging.FileHandler('../logs/pynamics365.log')
fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
fh.setLevel(logging.INFO)
logger.addHandler(ch)
logger.addHandler(fh)


class DynamicsClient:
    def __init__(self, auth: DynamicsAuth = None, **kwargs):
        self.auth = auth or DynamicsAuth(**kwargs)
        self.base_url = f"{self.auth.resource}/api/data/v9.2"
        self.environment = self._set_environment()
        self.last_refresh = self.auth.last_refresh
        self.expires_on = self.auth.expires_on
        self.auth_token = f"Bearer {self.auth.token}"
        self.headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json",
            "Prefer": "odata.include-annotations=\"*\"",
            "Prefer": "odata.maxpagesize=1000",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

    def _set_environment(self):
        resource_url = urlparse(self.auth.resource)
        # Replace periods with underscores
        return resource_url.netloc.replace('.', '_')

    def get(self, url, params=None, **kwargs):
        if self.token_expired():
            self.refresh_token()
        try:
            return requests.get(url, headers=self.headers, params=params, **kwargs)
        except requests.exceptions.MissingSchema:
            logger.exception(f"Missing schema for url: {url}")
            return requests.get(f"{self.base_url}/{url}", headers=self.headers, params=params, **kwargs)

    async def get_async(self, url, params=None, **kwargs):
        if self.token_expired():
            self.refresh_token()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params=params, **kwargs) as response:
                return await response.json()

    def post(self, url, data=None, **kwargs):
        if self.token_expired():
            self.refresh_token()
        return requests.post(url, headers=self.headers, data=data, **kwargs)

    def refresh_token(self):
        self.auth.authenticate()
        self.auth_token = f"Bearer {self.auth.token}"
        self.last_refresh = self.auth.last_refresh
        self.expires_on = self.auth.expires_on

    def token_expired(self):
        return time.time() > self.expires_on

    def get_entities(self, entity_name, select=None, filter=None, expand=None, top=None, orderby=None, count=False):
        url = f"{self.base_url}/{entity_name}"
        params = {}
        if select:
            params['$select'] = select
        if filter:
            params['$filter'] = filter
        if expand:
            params['$expand'] = expand
        if top:
            params['$top'] = top
        if orderby:
            params['$orderby'] = orderby
        if count:
            params['$count'] = count
        response = self.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_entity(self, entity_name, entity_id, select=None, filter=None, expand=None, top=None, orderby=None,
                   count=False):
        url = f"{self.base_url}/{entity_name}({entity_id})"
        params = {}
        if select:
            params['$select'] = select
        if filter:
            params['$filter'] = filter
        if expand:
            params['$expand'] = expand
        if top:
            params['$top'] = top
        if orderby:
            params['$orderby'] = orderby
        if count:
            params['$count'] = count
        response = self.get(url, params=params)
        response.raise_for_status()
        return response.json()


class DynamicsSession(requests.Session):
    def __init__(self, auth_url=None, grant_type="password", resource=None, client_id=None, username=None,
                 password=None, token_path="./token.json", **kwargs):
        super().__init__(**kwargs)
        self.auth_url = auth_url or os.getenv("MSDYN_AUTH_URL")
        self.grant_type = grant_type
        self.resource = resource or os.getenv("MSDYN_RESOURCE")
        self.base_url = f"{self.resource}/api/data/v9.2"
        self.client_id = client_id or os.getenv("MSDYN_CLIENT_ID")
        self.username = username or os.getenv("MSDYN_USERNAME")
        self.password = password or os.getenv("MSDYN_PASSWORD")
        self.token_path = token_path
        self.page_size = kwargs.get('page_size', 1000)
        self.token = None
        self._load_token()
        self._set_auth_headers()
        self._set_odata_headers()
        # self.headers = {}

    def _get_token(self):
        data = {
            "grant_type": self.grant_type,
            "resource": self.resource,
            "client_id": self.client_id,
            "username": self.username,
            "password": self.password,
        }
        response = self.post(self.auth_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        response.raise_for_status()
        return response.json()

    def _save_token(self):
        if not self.token:
            self.token = self._get_token()
        with open(self.token_path, "w") as f:
            json.dump(self.token, f, indent=2)

    def _load_token(self):
        try:
            with open(self.token_path, "r") as f:
                token = json.load(f)
                if self._token_expired():
                    self._save_token()
                    return self._get_token()
                else:
                    return token
        except FileNotFoundError:
            self._save_token()
            return self._get_token()

    def _token_expired(self, token=None):
        token = token or self.token
        if not token:
            return True
        return time.time() > int(token['expires_on'])

    def _set_auth_headers(self):
        if not self.token or self._token_expired():
            self._load_token()
        self.headers.update({
            "Authorization": f"Bearer {self.token['access_token']}",
        })

    def _set_odata_headers(self):
        self.headers.update({
            "Content-Type": "application/json",
            "Prefer": f"odata.include-annotations=\"*\",odata.maxpagesize={self.page_size}",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        })

    def authenticate(self, *args, **kwargs):
        # logger.debug("Authenticating")
        if not self.token or self._token_expired():
            self.token = self._get_token()
            self._save_token()
        self._set_auth_headers()

    def set_page_size(self, page_size):
        self.page_size = int(page_size)
        self._set_odata_headers()

    def request(self, method, url, *args, **kwargs):
        if not url.startswith('http'):
            url = f"{self.base_url}/{url}"
        return super().request(method, url, **kwargs)

    def get(self, url, *args, **kwargs):
        self.authenticate()
        return super().get(url, **kwargs)

    def get_all_pages(self, url, params=None):
        params = params or self.params
        params['$top'] = self.page_size
        params['$count'] = True
        next_link = url
        pages = []
        while next_link:
            response = self.get(next_link, params=params)
            response.raise_for_status()
            data = response.json()
            pages.append(data)
            next_link = data.get('@odata.nextLink', None)
        return pages

    def pages(self, url, params=None):
        params = params or self.params
        next_link = url
        page_number = 0
        while next_link:
            page_number += 1
            logger.debug(f"Getting page {page_number} for {next_link}")
            try:
                response = self.get(next_link, params=params)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.error(f"Error getting page {page_number} for {next_link}: {e}")
                return None
            data = response.json()
            yield data
            next_link = data.get('@odata.nextLink', None)
        logger.debug(f"Finished getting pages for {url}")

    def get_all_records(self, url, params=None):
        params = params or self.params
        records = []
        for page in self.pages(url, params=params):
            try:
                records.extend(page['value'])
            except KeyError:
                records.append(page)
        return records

    def gen_all_records(self, url, params=None):
        params = params or self.params
        for page in self.pages(url, params=params):
            try:
                yield from page['value']
            except KeyError:
                if isinstance(page, list):
                    yield from page
                else:
                    yield page


def extract_all_pages_to_json(dc, entity_definition, output_path=None):
    dc.authenticate()
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
        logger.error(f"Could not find entity name for {entity_definition['LogicalName']}")
        return None
    logger.info(f"Extracting all pages for {entity_name}")
    resource_path_name = urlparse(dc.base_url).hostname.replace('.', '_')
    output_path = f"{output_path}/{resource_path_name}/{entity_definition['LogicalName']}" or f"../data/{resource_path_name}/{entity_definition['LogicalName']}"
    output_path = Path(output_path)
    page_number = 0
    for page in dc.pages(entity_name):
        if not page['value']:
            logger.warning(f"Empty page for {entity_name}")
            continue
        page_number += 1
        logger.info(f"{entity_name}: Extracting page {page_number}")
        output_file = output_path / f"{entity_definition['LogicalName']}-extract-page-{page_number}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(page, f, indent=2)
    return output_path.name


def prepare_key_for_header(key):
    if "@Microsoft.Dynamics.CRM." in key:
        return key.replace("@Microsoft.Dynamics.CRM.", "_")
    elif "@odata.etag" in key:
        return key.replace("@odata.etag", "odata_etag")
    elif "@OData.Community.Display.V1." in key:
        return key.replace("@OData.Community.Display.V1.", "_")
    else:
        return key


def extract_all_pages_to_csv(dc, entity_definition, output_path=None):
    dc = DynamicsSession()
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
        logger.error(f"Could not find entity name for {entity_definition['LogicalName']}")
        return None
    logger.info(f"Extracting all pages for {entity_name}")
    resource_path_name = urlparse(dc.base_url).hostname.replace('.', '_')
    output_path = f"{output_path}/{resource_path_name}/{entity_definition['LogicalName']}" or f"../data/{resource_path_name}/{entity_definition['LogicalName']}"
    output_path = Path(output_path)
    page_number = 0
    for page in dc.pages(entity_name):
        if not page['value']:
            logger.warning(f"Empty page for {entity_name}")
            continue
        page_number += 1
        logger.info(f"{entity_name}: Extracting page {page_number}")
        output_file = output_path / f"{entity_definition['LogicalName']}-extract-page-{page_number}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        file_exists = output_file.exists()
        records = page['value']
        with open(output_file, "w") as f:
            csv_writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            csv_writer.writerow([prepare_key_for_header(key) for key in records[0].keys()])
            for record in records:
                csv_writer.writerow([record[key] for key in record.keys()])
    return output_path.name


def entity_attrs_to_csv(dc, entity_name, filename=None, **kwargs):
    url = f"EntityDefinitions(LogicalName='{entity_name}')/Attributes"
    df = pd.json_normalize(dc.get_all_records(url, **kwargs))
    filename = filename or f"./{entity_name}.csv"
    filename = Path(filename)
    df.to_csv(filename, index=False, escapechar="\\")
    return filename


def extract_entities_to_jsonl(dc, entity_definition, filename=None, **kwargs):
    dc.authenticate()
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
        logger.error(f"Could not find entity name for {entity_definition['LogicalName']}")
        return None
    logger.info(f"Extracting all records for {entity_name}")
    resource_path_name = urlparse(dc.base_url).hostname.replace('.', '_')
    filename = filename or f"../data/jsonl/{resource_path_name}/{entity_definition['LogicalName']}.jsonl"
    filename = Path(filename)
    filename.parent.mkdir(parents=True, exist_ok=True)
    if entity_name is None:
        logger.error(f"Could not find entity name for {entity_definition['LogicalName']}")
        return None
    with open(filename, "a") as f:
        for record in dc.get_all_records(entity_name, **kwargs):
            prepared_record = {}
            for key, value in record.items():
                key = prepare_key_for_header(key)
                if isinstance(value, dict):
                    prepared_record[key] = json.dumps(value)
                else:
                    prepared_record[key] = value
            f.write(json.dumps(prepared_record))
            f.write('\n')
    # Delete filename if empty
    if filename.stat().st_size == 0:
        filename.unlink()
        logger.warning(f"Deleted empty file {filename}")
        return None
    else:
        # Deduplicate file
        logger.info(f"Deduplicating file {filename}")
        with open(filename, "r") as f:
            lines = f.readlines()
            length_original = len(lines)
            logger.info(f"Found {len(lines)} lines in file {filename}")
        lines = list(set(lines))
        length_deduplicated = len(lines)
        with open(f"{filename}.temp", "w") as f:
            f.writelines(lines)
        os.rename(f"{filename}.temp", filename)
        logger.info(f"Done deduplicating file {filename}. Removed {length_original - length_deduplicated} lines.")
    return filename


def main():
    dc = DynamicsSession()
    dc.set_page_size(1000)
    entity_definitions = dc.get_all_records("EntityDefinitions")
    df = pd.json_normalize(entity_definitions)
    df.set_index('LogicalName', inplace=True)
    df.to_csv("../docs/entity_definitions.csv")
    output_path = "/Users/danlsn/Library/CloudStorage/GoogleDrive-dan@pinchpoint.com.au/My Drive/DATABASE/MIP/mipcrm-extract/full"
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for entity_definition in entity_definitions:
            # futures.append(executor.submit(entity_attrs_to_csv, dc, entity_name))
            futures.append(executor.submit(extract_entities_to_jsonl, dc, entity_definition))
            # futures.append(executor.submit(extract_all_pages_to_csv, dc, entity_definition, output_path))
            # futures.append(executor.submit(extract_all_pages_to_json, dc, entity_definition, output_path))
        for future in concurrent.futures.as_completed(futures):
            try:
                logger.info(f"Completed: {future.result()}")
            except KeyboardInterrupt:
                print("Exiting")
                executor.shutdown(wait=False)
                sys.exit(1)


if __name__ == "__main__":
    main()
