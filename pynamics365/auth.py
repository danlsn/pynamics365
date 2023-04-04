import json
import os
from datetime import datetime
import time
import requests
from dotenv import load_dotenv


class DynamicsAuth:
    token = None
    auth_url = None
    grant_type = None
    resource = None
    client_id = None
    username = None
    password = None
    last_refresh = None
    expires_on = None
    header = None
    save_token = None

    def __init__(self, auth_url=None, save_token=True, **kwargs):
        self.token_path = None
        load_dotenv()
        self.authenticate(**kwargs)
        self.save_token = save_token or True
        self.token = self.get_token()
        mandatory_fields = ['auth_url', 'grant_type', 'resource', 'client_id', 'username', 'password']
        for field in mandatory_fields:
            if not getattr(self, field):
                raise Exception(f"Missing field: {field}")

    def authenticate(self, auth_url=None, grant_type="password", resource=None, client_id=None, username=None, password=None, token_path=None):
        load_dotenv()
        self.auth_url = auth_url or os.getenv('MSDYN_AUTH_URL')
        self.grant_type = grant_type or os.getenv('MSDYN_GRANT_TYPE')
        self.resource = resource or os.getenv('MSDYN_RESOURCE')
        self.client_id = client_id or os.getenv('MSDYN_CLIENT_ID')
        self.username = username or os.getenv('MSDYN_USERNAME')
        self.password = password or os.getenv('MSDYN_PASSWORD')
        self.token_path = token_path or os.getenv('MSDYN_TOKEN_PATH')
        self.token = self.get_token()

    def get_token(self, use_saved_token=True, save_token=True):
        try:
            with open(self.token_path) as f:
                token = json.load(f)
                self.last_refresh = int(time.time())
                self.expires_on = int(token['expires_on'])
                if time.time() < self.expires_on:
                    self.token = token['access_token']
                    self.header = {
                        "Authorization": f"Bearer {self.token}",
                    }
                    return token['access_token']
        except FileNotFoundError:
            return self.get_token(use_saved_token=False)
        payload = {'grant_type': 'password', 'resource': self.resource,
                   'client_id': self.client_id, 'username': self.username,
                   'password': self.password}
        headers = {'Content-Type': "application/x-www-form-urlencoded", 'cache-control': "no-cache", }
        response = requests.request("POST", self.auth_url, data=payload, headers=headers)
        if response.status_code != 200:
            raise Exception(response.text)
        if self.save_token:
            with open(self.token_path, 'w') as f:
                json.dump(response.json(), f)
        self.last_refresh = int(time.time())
        self.expires_on = int(response.json()['expires_on'])
        self.token = response.json()['access_token']
        self.header = {
                    "Authorization": f"Bearer {self.token}",
                }
        return self.token

    def get_header(self):
        if self.token:
            return self.header
        else:
            self.get_token()
            return self.header


def main():
    auth = DynamicsAuth()
    print(auth.token)


if __name__ == '__main__':
    main()
