from pathlib import Path
import json
import requests
import requests_cache
import salesloft
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)


actions_path = Path("../data/salesloft-extract/full/actions")

requests_cache.install_cache("salesloft_cache", backend="sqlite", expire_after=3600)

session = requests.Session()


def load_actions():
    for path in actions_path.glob("*.json"):
        with open(path, "r") as f:
            page = json.load(f)
            for action in page.get("data", []):
                yield action


def get_action_details(sl, *args):
    for action in args:
        logger.info(f"Processing {action['id']}")
        # Download recording with requests
        action_id = action.get("id", None)
        url = action.get("action_details", None).get("_href", None)
        if url is None:
            continue
        action_details_id = action.get("action_details", None).get("id", None)
        filename = Path(f"{actions_path}/_ActionDetails/{action_details_id}.json")
        filename.parent.mkdir(parents=True, exist_ok=True)
        if filename.exists():
            continue
        logger.info(f"Downloading {action_id} to {filename}")

        response = sl.get(url)
        with open(filename, "w") as f:
            try:
                json.dump(response.json(), f, indent=4)
                logger.info(f"Success writing {filename}")
            except Exception as e:
                logger.error(f"Error writing {filename}")
                logger.error(e)
                f.write(response.text)


def main():
    sl = salesloft.Salesloft()
    get_action_details(sl, *load_actions())


if __name__ == "__main__":
    main()

