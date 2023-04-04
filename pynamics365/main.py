import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
# import requests_cache
import requests
import requests_cache
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
# requests_cache.install_cache('pynamics365_cache', backend='sqlite', expire_after=7 * 24 * 60 * 60)
import asyncio

class DynamicsAuth:
    token = None
    auth_url = None
    grant_type = None
    resource = None
    client_id = None
    username = None
    password = None

    def __init__(self, auth_url=None, **kwargs):
        load_dotenv()
        self.auth_url = kwargs.get("auth_url") or os.getenv('MSDYN_AUTH_URL')
        self.grant_type = kwargs.get("grant_type") or os.getenv('MSDYN_GRANT_TYPE')
        self.resource = kwargs.get("resource") or os.getenv('MSDYN_RESOURCE')
        self.client_id = kwargs.get("client_id") or os.getenv('MSDYN_CLIENT_ID')
        self.username = kwargs.get("username") or os.getenv('MSDYN_USERNAME')
        self.password = kwargs.get("password") or os.getenv('MSDYN_PASSWORD')
        self.token_path = kwargs.get("token_path") or "./token.json"
        self.token = self.get_token()

    def get_token(self, use_saved_token=False, save_token=True):
        if use_saved_token:
            try:
                with open(self.token_path) as f:
                    token = json.load(f)
                    expires_on = int(token['expires_on'])
                    import time
                    if time.time() < expires_on:
                        return token['access_token']
            except FileNotFoundError:
                return self.get_token(use_saved_token=False, save_token=save_token)

        """Authenticate with Dynamics 365"""
        payload = {'grant_type': 'password', 'resource': self.resource,
                   'client_id': self.client_id, 'username': self.username,
                   'password': self.password}
        headers = {'Content-Type': "application/x-www-form-urlencoded", 'cache-control': "no-cache", }
        response = requests.request("POST", self.auth_url, data=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(response.text)
        if save_token:
            with open('token.json', 'w') as f:
                json.dump(response.json(), f)
        return json.loads(response.text)['access_token']


class DynamicsClient(DynamicsAuth):
    base_url = None
    headers = None
    last_updated = None
    session = None

    def __init__(self, base_url=None, use_cache=False, **kwargs):
        super().__init__(**kwargs)
        self.endpoints = None
        self.record_counts = None
        self.entities = None
        self.cached_env = kwargs.get("cached_env") or Path(__file__).parent / ".environment.json"
        self.base_url = kwargs.get("base_url") or os.getenv('MSDYN_BASE_URL')
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Prefer": 'odata.include-annotations="*",odata.maxpagesize=1000',
            "Accept": "application/json",
        }
        self.session = requests.Session()
        if not self.last_updated:
            self.update()
            self.last_updated = datetime.now()
        elif datetime.now() - self.last_updated > timedelta(minutes=5):
            self.update()
            self.last_updated = datetime.now()



    ...

    def make_request(self, url_path=None, params=None, headers=None, **kwargs):
        if not headers:
            headers = self.headers
        if not params:
            params = {}

        request_url = kwargs.get("request_url") or f"{self.base_url}/{url_path}"
        method = kwargs.get("method") or "GET"
        response = self.session.request(method, request_url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(response.text)
        data = response.json()
        return data

    def _get_next_page(self, next_link):
        response = self.session.request("GET", next_link, headers=self.headers)
        data = response.json()
        next_link = data.get("@odata.nextLink")
        return data, next_link

    def update(self):
        self.get_entity_list()
        # self.get_record_counts()

    def get_entity_list(self, **kwargs):
        if not self.entities:
            entities = self.get_all_records("EntityDefinitions", **kwargs)
            self.entities = {e['LogicalName']: e for e in entities}
        return self.entities

    def get_all_records(self, endpoint, **kwargs):
        params = kwargs.get("params") or {}
        headers = self.headers
        request_url = f"{self.base_url}/{endpoint}"
        # if kwargs.get("use_cache"):
        #     session = requests_cache.CachedSession(cache_name='pynamics365_cache', backend='sqlite', expire_after=7 * 24 * 60 * 60)
        #     response = session.request("GET", request_url, headers=headers)
        # else:
        #     response = self.session.request("GET", request_url, headers=headers)
        response = self.session.request("GET", request_url, headers=headers)
        if response.status_code != 200:
            raise Exception(response.text)
        data = response.json()
        records = [*data['value']]
        next_link = data.get("@odata.nextLink")
        while next_link:
            data, next_link = self._get_next_page(next_link)
            records.extend(data['value'])
        return records

    def _cache_environment_to_json(self, filename=None):
        if not filename:
            filename = self.cached_env
        else:
            filename = Path(filename)
            filename.parent.mkdir(parents=True, exist_ok=True)
        if not filename.exists():
            filename.touch()
        with open(filename, "r") as f:
            try:
                environment = json.load(f)
            except json.decoder.JSONDecodeError:
                environment = {}

        environment[self.base_url] = {
            "last_updated": str(self.last_updated),
            "endpoints": self.endpoints,
            "record_counts": self.record_counts,
            "entities": self.entities
        }
        with open(filename, "w") as f:
            json.dump(environment, f, indent=2)

    def _load_environment_from_json(self, filename=".environment.json"):
        if not filename:
            filename = Path(__file__).parent / ".environment.json"
        else:
            filename = Path(filename)
        with open(filename, "r") as f:
            try:
                environment = json.load(f)
            except json.decoder.JSONDecodeError:
                environment = {}
        try:
            self.endpoints = environment[self.base_url]["endpoints"]
            self.record_counts = environment[self.base_url]["record_counts"]
            self.entities = environment[self.base_url]["entities"]
            self.last_updated = datetime.strptime(environment[self.base_url]["last_updated"], "%Y-%m-%d %H:%M:%S.%f")
        except KeyError:
            pass

    def _get_one_page(self, endpoint, **kwargs):
        if not self.session:
            self.session = requests.Session()
        params = kwargs.get("params") or {}
        headers = kwargs.get("headers") or self.headers
        request_url = f"{self.base_url}/{endpoint}"
        response = self.session.request("GET", request_url, headers=headers)
        if response.status_code != 200:
            raise Exception(response.text)
        data = response.json()
        next_link = data.get("@odata.nextLink")
        return data, next_link

    def get_all_pages(self, endpoint, **kwargs):
        params = kwargs.get("params") or {}
        headers = kwargs.get("headers") or self.headers
        request_url = f"{self.base_url}/{endpoint}"
        response = self.session.request("GET", request_url, headers=headers)
        if response.status_code != 200:
            raise Exception(response.text)
        data = response.json()
        pages = [data]
        next_link = data.get("@odata.nextLink")
        while next_link:
            data, next_link = self._get_next_page(next_link)
            pages.append(data)
        return pages

    def get_one_record(self, endpoint):
        request_url = f"{self.base_url}/{endpoint}"
        params = {
            "$top": 1,
        }
        session = requests_cache.CachedSession(cache_name='pynamics365_cache', backend='sqlite', expire_after=1 * 24 * 60 * 60)
        response = session.request("GET", request_url, params=params, headers=self.headers)
        return response.json()

    def get_record_counts(self):
        record_count_snapshots = {}
        for rcss in self.get_all_records("recordcountsnapshots"):
            object_type_code = rcss.pop("objecttypecode")
            record_count_snapshots[object_type_code] = rcss

        record_counts = {}
        for entity_name, entity in self.get_entity_list().items():
            if not entity_name:
                entity_name = entity['LogicalName'] or entity['SchemaName']
            object_type = entity['ObjectTypeCode']
            try:
                record_count = record_counts.get(object_type)
                record_counts[entity_name] = {
                    'record_count': record_count_snapshots[object_type]['count'],
                    'last_updated': record_count_snapshots[object_type]['lastupdated']
                }
            except KeyError as e:
                print(f"KeyError: {e}")
                pass
        self.record_counts = record_counts
        return record_counts

    def get_entity_endpoint(self, entity_name):
        endpoints = self.endpoints or {}
        if entity_name not in self.entities:
            raise Exception(f"Entity {entity_name} not found.")
        if endpoints.get(entity_name):
            return endpoints[entity_name]
        candidate_endpoints = ['LogicalCollectionName', 'LogicalName', 'SchemaName']
        for candidate in candidate_endpoints:
            test_record = self.get_one_record(f"{self.entities[entity_name][candidate]}")
            if "error" not in test_record:
                endpoints[entity_name] = self.entities[entity_name][candidate]
                break
        self.endpoints = endpoints
        self._cache_environment_to_json()
        return endpoints[entity_name]

    def get_valid_entity_endpoints(self):
        endpoints = self.endpoints or {}
        candidate_endpoints = ['LogicalCollectionName', 'LogicalName', 'SchemaName']
        pbar = tqdm(self.entities.items())
        for entity_name, entity in pbar:
            try:
                pbar.set_description(f"Testing {entity_name}...")
                if entity_name in self.endpoints:
                    continue
                else:
                    for candidate in candidate_endpoints:
                        test_url = f"{self.base_url}/{entity[candidate]}"
                        pbar.set_description(f"Testing {entity_name} at {entity[candidate]}")
                        response = self.session.request("GET", test_url, headers=self.headers)
                        if response.status_code == 200:
                            endpoints[entity_name] = entity[candidate]
                            pbar.set_description(f"Found {entity['LogicalName']} at {entity[candidate]}")
                            break
                    endpoints[entity_name] = None
            except KeyboardInterrupt:
                break
        self.endpoints = endpoints
        self._cache_environment_to_json()
        return endpoints

    def save_entity_records_to_file(self, entity_name, output_path=None):
        if not output_path:
            filename = Path("../data") / entity_name / f"{entity_name}.json"
        records = self.get_all_records(entity_name)
        if not records:
            return
        filename.parent.mkdir(parents=True, exist_ok=True)

        with open(filename, "w") as f:
            json.dump(self.get_all_records(entity_name), f, indent=2)

    def save_entity_pages_to_file(self, entity_name, output_path="../data"):
        page = 1
        if not output_path:
            output_path = Path("../data")
        filename = entity_output_filename(output_path, entity_name, page)
        entity_page, next_link = self._get_one_page(entity_name)
        with open(filename, "w") as f:
            print(f"Saving page {page} of {entity_name} to {filename}...")
            json.dump(entity_page, f, indent=2)
        while next_link:
            page += 1
            filename = entity_output_filename(output_path, entity_name, page)
            entity_page, next_link = self._get_next_page(next_link)
            with open(filename, "w") as f:
                print(f"Saving page {page} of {entity_name} to {filename}...")
                json.dump(entity_page, f, indent=2)
            if not next_link or len(entity_page['value']) == 0:
                print(f"Finished saving {entity_name} to {output_path}.")
                break

def entity_output_filename(output_path, entity_name, page=None):
    if not output_path:
        filename = Path("../data") / entity_name / f"{entity_name}.json"
    else:
        filename = Path(output_path) / entity_name / f"{entity_name}.json"
    if page:
        filename = Path(output_path) / entity_name / f"{entity_name}_page_{page}.json"
    filename.parent.mkdir(parents=True, exist_ok=True)
    return filename

class AsyncDynamicsClient:
    ...


class DynamicsRequest(DynamicsClient):
    filters = None
    params = None
    headers = None

    def __init__(self, **kwargs):
        self.filters = kwargs.get('filters')
        self.params = kwargs.get('params')
        self.headers = kwargs.get('headers')
        super.__init__(**kwargs)
    ...


class DynamicsQuery:
    ...


def extract_all_entity_pages(dc, logical_name):
    entity_name = logical_name
    try:
        endpoint = dc.get_entity_endpoint(logical_name)
    except KeyError as e:
        endpoint = logical_name
        print(f"KeyError: {e}")
    print(f"{entity_name}: {endpoint}")
    try:
        dc.save_entity_pages_to_file(endpoint)
    except Exception as e:
        print(f"Error: {e}")
        pass
    return


async def main():
    auth = DynamicsAuth()
    print(auth.token)
    dc = DynamicsClient()
    dc.get_entity_list()
    dc.get_record_counts()
    account_endpoint = dc.get_entity_endpoint("account")
    df = pd.read_csv("entity_counts.csv")
    # sort by record count ascending
    df.sort_values(by="Count", ascending=True, inplace=True)
    # Output to dict
    entities = df.to_dict(orient="records")
    # Iterate through entities
    tasks_todo = []
    for entity in entities:
        if not entity['LogicalName'] or entity['Count'] == 0:
            continue
        print(f"Fetching {entity['LogicalName']}, {entity['Count']} records")
        entity_name = entity['LogicalName']
        tasks_todo.append(entity_name)
    await asyncio.gather(*[extract_all_entity_pages(dc, entity_name) for entity_name in tasks_todo])
    # ...
    # dc = DynamicsClient(resource="https://mipdev.crm6.dynamics.com", base_url="https://mipdev.crm6.dynamics.com/api/data/v9.2")
    # dc.get_valid_entity_endpoints()

if __name__ == '__main__':
    asyncio.run(main())
