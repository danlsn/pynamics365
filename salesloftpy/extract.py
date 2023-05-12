from concurrent.futures import ThreadPoolExecutor

import requests_cache

from salesloftpy.salesloft import Salesloft

import logging

logger = logging.getLogger("salesloft.extract")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'))
logger.addHandler(ch)

dfh = logging.FileHandler("../logs/salesloft_extract.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)


# requests_cache.install_cache("salesloft_cache", backend="sqlite", expire_after=3600)


def main():
    sl = Salesloft()
    # Run in ThreadPoolExecutor
    to_do = ["/person_stages.json", "/phone_number_assignments.json", "/saved_list_views.json", "/accounts.json", "/people.json", "/users.json", "/crm_users.json", "/activities/emails.json",
             "/imports.json", "/meetings.json", "/email_template_attachments.json", "/email_templates.json",
             "/caller_ids.json", "/groups.json", "/phone_number_assignments.json", "/call_instructions.json",
             "/account_stages.json", "/account_tiers.json", "/actions.json", "/activity_histories.json",
             "/activities/calls.json", "/call_data_records.json", "/call_dispositions.json", "/call_sentiments.json",
             "/calendar/events", "/cadences.json", "/cadence_memberships.json", "/crm_activities.json",
             "/crm_activity_fields.json",
             "/custom_fields.json", "/tasks.json", "/successes.json", "/notes.json", "/tags.json", "/steps.json",
             "/person_stages.json", "/team_template_attachments.json", "/team_templates.json"]

    futures = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        for page in to_do:
            futures.append(executor.submit(sl.save_all_pages, page))
    for future in futures:
        logger.info(f"Future: {future}")


if __name__ == '__main__':
    main()
