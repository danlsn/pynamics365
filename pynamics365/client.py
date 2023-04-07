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
from dotenv import load_dotenv

from pynamics365.auth import DynamicsAuth


# requests_cache.install_cache('pynamics365_cache')


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
        return requests.get(url, headers=self.headers, params=params, **kwargs)

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


def main():
    ...


if __name__ == "__main__":
    main()

