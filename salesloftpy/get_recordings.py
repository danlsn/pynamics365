import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
import json

import requests_cache

logger = logging.getLogger("salesloft.get_recordings")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)

call_data_records_path = Path("../data/salesloft-extract/full/call_data_records")

requests_cache.install_cache("salesloft_cache", backend="sqlite", expire_after=3600)

session = requests.Session()

def load_call_data_records():
    for path in call_data_records_path.glob("*.json"):
        with open(path, "r") as f:
            page = json.load(f)
            for call_data_record in page.get("data", []):
                yield call_data_record


def download_recordings(*args):
    for call_data_record in args:
        logger.info(f"Processing {call_data_record['id']}")
        # Download recording with requests
        recording_url = call_data_record.get("recording", None).get("url", None)
        if recording_url is None:
            continue
        recording_id = call_data_record.get("id", None)
        call_uuid = call_data_record.get("call_uuid", None)
        filename = Path(f"{call_data_records_path}/_Recordings/{recording_id}_{call_uuid}.wav")
        filename.parent.mkdir(parents=True, exist_ok=True)
        if filename.exists():
            continue
        with open(filename, "wb") as f:
            logger.info(f"Downloading {recording_url} to {filename}")
            response = session.get(recording_url)
            f.write(response.content)


def main():
    call_data_records = list(load_call_data_records())
    with ThreadPoolExecutor(max_workers=10) as executor:
        for call in call_data_records:
            executor.map(download_recordings, [call])


if __name__ == "__main__":
    main()

