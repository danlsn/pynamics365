import json
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import sleep
import logging
import requests
from dotenv import load_dotenv
import requests_cache

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)


dfh = logging.FileHandler("../logs/salesloft_main.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)


requests_cache.install_cache("salesloft_cache", backend="sqlite", expire_after=3600)


def calc_ratelimit_cost(url=None, params=None):
    ratelimit_cost = 1
    if url and params is None:
        params = urllib.parse.parse_qs(url)
    page_number = params.get("page", 1)
    if 100 < page_number <= 150:
        ratelimit_cost = 3
    elif 150 < page_number <= 250:
        ratelimit_cost = 8
    elif 250 < page_number <= 500:
        ratelimit_cost = 10
    elif page_number > 500:
        ratelimit_cost = 30
    else:
        ratelimit_cost = 1
    return int(ratelimit_cost)


class Salesloft(requests.Session):
    base_url = None
    ratelimit_remaining_minute = 600

    def __init__(self, api_key=None, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv('SALESLOFT_API_KEY')
        self.base_url = "https://api.salesloft.com/v2"
        self._set_auth_header()

    def _set_auth_header(self):
        if not self.api_key:
            raise ValueError
        self.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def get(self, url, *args, **kwargs):
        if url.startswith("/"):
            url = self.base_url + url
        elif not url.startswith("http"):
            url = url = self.base_url + "/" + url
        else:
            pass
        params = kwargs.get("params", {})
        ratelimit_cost = calc_ratelimit_cost(url, params)
        ratelimit_remaining = self.get_ratelimit_remaining()
        while ratelimit_cost > ratelimit_remaining:
            sleep(1)
            ratelimit_remaining = self.get_ratelimit_remaining()
        self._set_auth_header()
        try:
            res = super().get(url=url, params=params, headers=self.headers)
            res.raise_for_status()
        finally:
            ...
        Salesloft.update_rate_limit(res.headers)
        logger.info(f"Rate Limit Remaining Minute: {self.get_ratelimit_remaining()}")
        return res

    def get_all_pages(self, url, *args, **kwargs):
        next_page = 1
        params = {"per_page": kwargs.get("per_page", 100), "page": next_page, "include_paging_counts": True}
        while params['page']:
            logger.debug(f"Getting page {params['page']} of {url}.")
            ratelimit_cost = int(calc_ratelimit_cost(url, params))
            ratelimit_remaining = int(self.get_ratelimit_remaining())
            if ratelimit_cost >= ratelimit_remaining:  # 100 is a magic number
                logger.info(
                    f"Request cost ({ratelimit_cost}) is greater than remaining ({ratelimit_remaining}): Sleeping for 30 seconds.")
                sleep(10)
            elif ratelimit_remaining < 200:  # 100 is a magic number
                logger.info(
                    f"Rate limit remaining is ({ratelimit_remaining}): Sleeping for {ratelimit_cost / 2} seconds.")
                sleep(ratelimit_cost / 2)
            elif ratelimit_remaining < 300:  # 300 is a magic number
                logger.info(
                    f"Rate limit remaining is ({ratelimit_remaining}): Sleeping for {ratelimit_cost / 4} seconds.")
                sleep(ratelimit_cost / 4)
            else:
                logger.info(f"Rate limit remaining is ({ratelimit_remaining}): Sleeping for {ratelimit_cost / 8} seconds.")
                sleep(ratelimit_cost / 8)
            response = self.get(url, params=params)
            response.raise_for_status()
            response = response.json()
            params["page"] = response.get("metadata", None).get("paging", None).get("next_page", None)
            yield response

    def save_all_pages(self, url, output_path="../data"):
        output_path: Path = Path(output_path) / "salesloft_extract" / "full"
        output_path.mkdir(parents=True, exist_ok=True)
        total_pages = 0
        for page_number, page in enumerate(self.get_all_pages(url)):
            total_pages = page.get("metadata", None).get("paging", None).get("total_pages", 0)
            page_number += 1
            url_name = url.replace(".", "-").replace("/", "-")
            # Replace leading _ with nothing
            url_name = url_name[1:] if url_name.startswith("-") else url_name
            # Remove trailing _json if it exists
            url_name = url_name[:-5] if url_name.endswith("-json") else url_name
            filename = output_path / url_name / f"{url_name}-extract-page-{page_number}.json"
            filename.parent.mkdir(parents=True, exist_ok=True)
            with open(filename, "w") as f:
                logger.info(f"Saving page {page_number}/{total_pages} for {url}")
                json.dump(page, f, indent=4)

    @classmethod
    def update_rate_limit(cls, res_headers):
        ratelimit_remaining_minute = res_headers.get("x-ratelimit-remaining-minute", None)
        if ratelimit_remaining_minute is not None:
            cls.ratelimit_remaining_minute = ratelimit_remaining_minute

    @classmethod
    def get_ratelimit_remaining(cls):
        return int(cls.ratelimit_remaining_minute)

    def get_all_email_ids(self):
        email_ids = []
        for page in self.get_all_pages("/activities/emails.json"):
            for email in page.get("data", []):
                yield email.get("id")

    def save_email_mime_content(self, email_id=None):
        if email_id is None:
            email_ids = self.get_all_email_ids()
        else:
            email_ids = [email_id]
        for index, email_id in enumerate(email_ids):
            email_bin = f"{int(email_id / 1000)}XXX"
            try:
                logger.debug(f"[{index}] Getting email {email_id}...")
                email = self.get(f"/mime_email_payloads/{email_id}.json")
            except requests.exceptions.HTTPError as e:
                logger.error(f"Error getting email {email_id}: {e}")
                continue
            email = email.json().get("data", None)
            filename = Path("../data/salesloft-extract/full/mime_email_payloads") / f"{email_bin}.jsonl"
            filename.parent.mkdir(parents=True, exist_ok=True)
            with open(filename, "a") as f:
                json.dump(email, f)
                f.write("\n")
            if index % 100 == 0:
                logger.info(f"De-duping files...")
                for jsonl_file in filename.parent.glob("*.jsonl"):
                    logger.info(f"De-duping {jsonl_file}...")
                    dedupe_jsonl_file(jsonl_file)


def fetch_all_mime_content(sl):
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = executor.map(sl.save_email_mime_content, sl.get_all_email_ids())

    for future in as_completed(futures):
        logger.debug(f"Future: {future}")


def dedupe_jsonl_file(filename):
    with open(filename, "r") as f:
        lines = f.readlines()
        lines_before = len(lines)
        logger.info(f"De-duping {filename}. Before: {lines_before}")
    lines = set(lines)
    lines_after = len(lines)
    logger.info(f"After: {lines_after}. Removed {lines_before - lines_after} lines.")
    with open(filename, "w") as f:
        f.writelines(lines)


def main():
    sl = Salesloft()
    # me = sl.get("/me.json")
    # Run in ThreadPoolExecutor
    # sl.save_email_mime_content()
    # fetch_all_mime_content(sl)
    to_do = ["/accounts.json", "/people.json", "/users.json", "/crm_users.json", "/activities/emails.json",
             "/imports.json", "/meetings.json", "/email_template_attachments.json", "/email_templates.json",
             "/caller_ids.json", "/groups.json", "/phone_number_assignments.json", "/call_instructions.json",
             "/account_stages.json", "/account_tiers.json", "/actions.json", "/activity_histories.json",
             "/activities/calls.json", "/call_data_records.json", "/call_dispositions.json", "/call_sentiments.json",
             "/calendar/events", "/cadences.json", "/cadence_memberships.json", "/crm_activities.json",
             "/crm_activity_fields.json",
             "/custom_fields.json", "/tasks.json", "/successes.json", "/notes.json", "/tags.json", "/steps.json",
             "/person_stages.json", "/team_template_attachments.json", "/team_templates.json"]
    with ThreadPoolExecutor() as executor:
        executor.map(sl.save_all_pages, to_do)
    # sl.save_all_pages("/accounts.json")
    # sl.save_all_pages("/people.json")
    # all_emails = sl.get_all_pages("/activities/emails.json")
    ...


if __name__ == '__main__':
    main()
