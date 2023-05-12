from pathlib import Path

import requests
from bs4 import BeautifulSoup
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)


email_templates_path = Path("../data/salesloft-extract/full/email_templates")
email_template_attachments_path = Path("../data/salesloft-extract/full/email_template_attachments")

def load_email_templates():
    for path in email_templates_path.glob("*.json"):
        with open(path, "r") as f:
            page = json.load(f)
            for email_template in page.get("data", []):
                yield email_template


def load_email_template_attachments():
    for path in email_template_attachments_path.glob("*.json"):
        with open(path, "r") as f:
            page = json.load(f)
            for email_template_attachment in page.get("data", []):
                yield email_template_attachment


def parse_email_template(email_template):
    soup = BeautifulSoup(email_template.get("body", ""), "html.parser")
    email_text = soup.get_text()
    ...


def download_attachments():
    template_attachments = load_email_template_attachments()
    for template_attachment in template_attachments:
        # Download attachment with requests
        download_url = template_attachment.get("download_url", None)
        if download_url is None:
            continue
        name = template_attachment.get("name", None)
        attachment_id = template_attachment.get("attachment_id", None)
        filename = Path(f"{email_template_attachments_path}/_Files/{attachment_id}_{name}")
        filename.parent.mkdir(parents=True, exist_ok=True)
        if filename.exists():
            continue
        with open(filename, "wb") as f:
            logger.info(f"Downloading {download_url} to {filename}")
            response = requests.get(download_url)
            f.write(response.content)


def main():
    email_templates = load_email_templates()
    for email_template in email_templates:
        parse_email_template(email_template)
        ...


if __name__ == "__main__":
    download_attachments()
    main()

