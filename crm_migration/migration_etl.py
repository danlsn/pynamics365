import csv
import json
import logging
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path

import dictdatabase as DDB
import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import sqlalchemy

import cleansing_steps
from pynamics365.client_windows import DynamicsSession
from pynamics365.config import (
    TEMPLATE_ENTITIES,
    DUCKDB_DB_PATH,
    SALESLOFT_DUCKDB_DB_PATH,
    DDB_PATH,
)
from pynamics365.ddb_loader import get_endpoint_name
from pynamics365.extract_to_load_table import (
    get_all_ddb_to_csv,
    ddb_extract_to_df,
    load_to_table,
    get_engine,
    load_to_table_replace,
    map_df_to_template,
    prepare_column_names,
)
from pynamics365.prod_environment import full_extract_to_ddb
from pynamics365.templates import (
    get_import_templates,
    import_templates_to_json,
    extract_xlsx_to_template_dir,
)
from salesloftpy.extract import main as salesloft_extract
from salesloftpy.salesloft import Salesloft
from salesloft_crm_join import top_level_crm_salesloft_to_sql
from salesloft_to_duckdb import (
    extracts_to_df as salesloft_extracts_to_df,
    save_extracts_to_duckdb,
)
from template_validations import (
    check_all_columns_are_present,
    check_required_fields_are_present,
    check_value_is_in_option_set,
)

logging.basicConfig(
    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


fh = logging.FileHandler("migration_etl.log")
fh.setLevel(logging.INFO)
fh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(fh)


dfh = logging.FileHandler("../logs/_migration_etl.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)


sl = Salesloft()
DDB.config.storage_directory = DDB_PATH
# Get sqlalchemy logger
# sql_logger = logging.getLogger('sqlalchemy.engine')
# sql_logger.setLevel(logging.INFO)
# sql_logger.addHandler(ch)


DDB_ROOT = Path(r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\ddb")

TEMPLATES_ROOT = Path(
    r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\import_templates"
)
TEMPLATES_JSON = Path(
    r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\json_templates\import_templates"
)
TEMPLATES_HTML = Path(
    r"C:\Users\DanielLawson\IdeaProjects\pynamics365\docs\Templates for Data Import.html"
)

cookies = {
    "visid_incap_2029367": "ZDdF/BiBQyqDk+QZo9sQVNHp7mMAAAAAQUIPAAAAAABwpYpyuT48mAHwIGBL5X7m",
    "ReqClientId": "9e4c0b96-7a84-4620-934a-58b6e95d49d3",
    "orgId": "bcd4f0de-97c7-ed11-a10d-0022481536c6",
    "CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651SecuritySettingWebClientDeprecation": "HideMessage",
    "CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2303%2f31%2f2023%2009%3a34%3a20": "HideMessage",
    "CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651GetAppsForCrm": "HideMessage",
    "persistentSearchTypeCookieEx": "0",
    "persistentSearchTypeCookie": "1",
    "CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651LegacySettingsRedirectionExperience": "HideMessage",
    "lastUsedApp": "appId=4be68029-35c5-ed11-b597-002248144a90&userId=2bbb35de-0cc9-ed11-b595-00224818a651",
    "nlbi_2029367": "zSFhGpUQyUXkmY0feEPzrwAAAAAI5VCf7sVvsTmQd6B7HBB0",
    "incap_ses_341_2029367": "CEptDQFdHiNy/OvGnHu7BNtzQGQAAAAAaHldi8YXLAtg+5TSwXaeTA==",
    "_clck": "1cujtye|1|faw|1",
    "MSCC": "NR",
    "_clsk": "vex39j|1681945531209|1|0|j.clarity.ms/collect",
    "CrmOwinAuth": "MAAAAICME_zeCRHtqAkAIkiUKq4EpF9BuX8l7UW5OlwBz4vF80uYTRJgMB9TBlzJwianex-LCAAAAAAABADFV2uPo8gV7Um35rHJriKtNN9WakXzLcHm_RhllWCMn2BsMMbuLwiK4mFeNlUY278oPzO4e2azM1pFydhKSoCgoE6dW_fW5dz7u7u7H4wmKZSyTBMo1zh-dffq7k9t971XBx_IlyYQl-vLBXy--9xe3f0cY7xDH7tdhFGnBQvKBnUKiLswDFmKpAQCUGFIsIAmCc_36PaR8hmJIX2foruv7u4ThK7FuJ6Dh7-jeJGiJEoSpYeJZcyuxiz88OaY8Lj7hMmJN8L8eBl_GQ5imHuokyegKlEZ4g4o8y7IvCRHXa-NjSKHOC4DVMEQVrAAEN3vmuD6xb-GQIW8_y-BPLwBAf3fEmhQlyZJsUvy3SSABU7w6dekLj3Aw0lZJAXCXvE5kGlaIm8SIK-BG3soft-EQ4G3BXNGc00-Ao7gQ0A211v_81fWH_MMld6uU1bRJ9u5bnt8bTuqq8LL4WvNa1BZXE_jb99GI0oOsHgm0veKBGbXE3md7LwgqH6gSaZDk1J7Ch2Wvh734cLy-xeWj7data-d92Xofr1Ypb-FAL_0hgmsPggMzQUiJRIcx1MEC3lAeDTLE5xEgRAwoudR4g0s39VJ8EeKJJnWizQ9kJWBwCjX4_6uinmyI9uqPDj2ScF1e2P11Jqv-sZSEdQIyJ9b88tdryerauf6uf-Akqjd8W675TF8GxyAmxfRDbLxF7hvLrgg390Y9iHNUXI9Zv_bduxlG_wrBP-8rVTHdeIa0Q5ZLlvfscOnhCvI_jjga_apt1rvm-qYTdlTdD1l7r_aLhgWbT5Pgg__Cfr15FrpFzxnh072nB3-3v5cAg97LdmyzC4MO159_TTSt7mt3hX_E3r3NU7ex6uVyU4kxbWH_REIqFW9dGX5BuAHWN1TnRvI5ocmCdAHX5BCP2QDgoGhRLC8KBEixTKEwPuUxEKR40jp-rneHnPk5juQvKY5qfXPTWTFmwtoi_nA8DdCfKbZoAQ_UOKNIN-X7aAOqC6hhWNcpm1MnnbwN6qmV3fv2umDU-FW-B_vvu2vIEdyT5bV9rlPEH1NZlaGsKiCIJpEggMPlRz1WwNtWZqLLFXwmr4xvIkSmDsY2NvZEOhTpwnDEUnYzlrCTamV6jBk7SWtQYsfU4SoHviBZM6D2VTQ8WI1Zh1XWE8kZkhH_G5vA2hZ7CZbhntO4LS5kYeiMI4bLSRt0kJPdiQxPYdAa261l-t4qqhLTRknzn433jeYmVMHr4w1frA-uxJVku1-xht15HqV2yrneLZHc-3kydF4L6ZRDAcqU1j0quqzTczrrZQc2fLgXMgHb2fJxXi_tRqC9_JwRmGKHygpWMTIXGnMkWEaO91wGIub8-DpCQ0tfdlXqKfSrsiBPgdrRlA2tUucAuPpHLtmj48WE5U1xyTPQv9k6Rtfn4lZ0kqBHc7AKFT6_sGlxnBIzmAU-EZ2nPmJFNJT09VIq9Imp_Fs7elpHhtSCfqGzhgOJhMt6lF8ViuY70tNzxJlMCKRYigesjXMOvW0DGaLA7s-17SIgvX5UI1DQlv7TkUnFd2n5lU1wofZfsFJO3aar05uf2gbJ0ITDufUXUwNw4fCeM8v2rUGvmppi3jYyNusFy8nkqUtI7ylKSPVC3HrxtPNyd9M2myQBRMQFuZy3vpg3Bxt2bYPuiIDvT7lRMAXIdjvoNVmszUdpXa0PZ8aKvKtWtVnFqLOi0BU9lVekU59Ds_rwiHdgw9ZN4R2am_PGiBJZzsdrsVMCZ2h6lZjQY230lTyo6Hu5qo-oMeGMUm0SlPOaeP6YeAOWQGPiFrQkVma7qzYsrKUGbCAzGJ4WqHFpPHsPpnw1bRme0WvNAEwREfnLXtvxgKjzzZsaRy3q9TrVfsIurvFxJ6yIFek9JxvJaOHj_nRRCeHSFKhPJqTwRDVgNKWJ2WnYDVdF-ulu8HaUTvqI5M9Zu2q9c7OiGWSzcbeKA0hMCMkhvaoJtK1VSZyH5XV8LDePlEH0ZT2MtjMlsSGic8ZzoRF3_eZuZsuC-iXpmlunVOh2TWeqPvpASsLzLDgvBuAJ2z56Ty1XHcpjnDmQFJzHSdNh5M-IR3HC-xy5WYiRjK2Fys-qLZEQ89PEx5NwBmUjX4oFibhLHunxvOnrsMbT3SpM_s2YM_qcDafe_0hMDcSxQK04tKCTg2EpMPKGu-RSoBAMhfSCmz2keslintYjXMdCEdzTfGz8mmdzdT5vFq8ZK7vPmWuqHopHiWO5EXxy3fZb7y7tMv1dXu-61QwSKpW3j9-zrl5W8jUl-TJd1oEr9U66FnmfN9BEKG2YLUuYvADCyjgwYAlPIETCTYIYFsDeIAAtBe2PWwY0vDHTisWQGq9jBsPqlbD_fXzPFkZtUn6FylVFllSwJfavczzsuiWl-RNd58hPk39-84OVihBrcTCd2878LhruaOf9LL4yyNFPcq76rGtv5hHkvvIUR8Z_nGoL990EoRqGPy0rOGvvqK__OqfkFLR-lYTAAA",
    "MSCRM_LastServiceRequestId": "e6b3bc6d-658e-43b8-9243-ddfd05e91eef",
    "ARRAffinity": "852caf45eec4999125b21ae0c5772372cb456b9c75d96af4c693c6d502b03e5c0147f6ec7070d5f17d585a426c08e7b4ef49366e3b807fdd66fcd6bafae280ff08DB413B4D8FF685909169359",
}

headers = {
    "authority": "mipau.crm6.dynamics.com",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    # 'cookie': 'visid_incap_2029367=ZDdF/BiBQyqDk+QZo9sQVNHp7mMAAAAAQUIPAAAAAABwpYpyuT48mAHwIGBL5X7m; ReqClientId=9e4c0b96-7a84-4620-934a-58b6e95d49d3; orgId=bcd4f0de-97c7-ed11-a10d-0022481536c6; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651SecuritySettingWebClientDeprecation=HideMessage; CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2303%2f31%2f2023%2009%3a34%3a20=HideMessage; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651GetAppsForCrm=HideMessage; persistentSearchTypeCookieEx=0; persistentSearchTypeCookie=1; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651LegacySettingsRedirectionExperience=HideMessage; lastUsedApp=appId=4be68029-35c5-ed11-b597-002248144a90&userId=2bbb35de-0cc9-ed11-b595-00224818a651; nlbi_2029367=zSFhGpUQyUXkmY0feEPzrwAAAAAI5VCf7sVvsTmQd6B7HBB0; incap_ses_341_2029367=CEptDQFdHiNy/OvGnHu7BNtzQGQAAAAAaHldi8YXLAtg+5TSwXaeTA==; _clck=1cujtye|1|faw|1; MSCC=NR; _clsk=vex39j|1681945531209|1|0|j.clarity.ms/collect; CrmOwinAuth=MAAAAICME_zeCRHtqAkAIkiUKq4EpF9BuX8l7UW5OlwBz4vF80uYTRJgMB9TBlzJwianex-LCAAAAAAABADFV2uPo8gV7Um35rHJriKtNN9WakXzLcHm_RhllWCMn2BsMMbuLwiK4mFeNlUY278oPzO4e2azM1pFydhKSoCgoE6dW_fW5dz7u7u7H4wmKZSyTBMo1zh-dffq7k9t971XBx_IlyYQl-vLBXy--9xe3f0cY7xDH7tdhFGnBQvKBnUKiLswDFmKpAQCUGFIsIAmCc_36PaR8hmJIX2foruv7u4ThK7FuJ6Dh7-jeJGiJEoSpYeJZcyuxiz88OaY8Lj7hMmJN8L8eBl_GQ5imHuokyegKlEZ4g4o8y7IvCRHXa-NjSKHOC4DVMEQVrAAEN3vmuD6xb-GQIW8_y-BPLwBAf3fEmhQlyZJsUvy3SSABU7w6dekLj3Aw0lZJAXCXvE5kGlaIm8SIK-BG3soft-EQ4G3BXNGc00-Ao7gQ0A211v_81fWH_MMld6uU1bRJ9u5bnt8bTuqq8LL4WvNa1BZXE_jb99GI0oOsHgm0veKBGbXE3md7LwgqH6gSaZDk1J7Ch2Wvh734cLy-xeWj7data-d92Xofr1Ypb-FAL_0hgmsPggMzQUiJRIcx1MEC3lAeDTLE5xEgRAwoudR4g0s39VJ8EeKJJnWizQ9kJWBwCjX4_6uinmyI9uqPDj2ScF1e2P11Jqv-sZSEdQIyJ9b88tdryerauf6uf-Akqjd8W675TF8GxyAmxfRDbLxF7hvLrgg390Y9iHNUXI9Zv_bduxlG_wrBP-8rVTHdeIa0Q5ZLlvfscOnhCvI_jjga_apt1rvm-qYTdlTdD1l7r_aLhgWbT5Pgg__Cfr15FrpFzxnh072nB3-3v5cAg97LdmyzC4MO159_TTSt7mt3hX_E3r3NU7ex6uVyU4kxbWH_REIqFW9dGX5BuAHWN1TnRvI5ocmCdAHX5BCP2QDgoGhRLC8KBEixTKEwPuUxEKR40jp-rneHnPk5juQvKY5qfXPTWTFmwtoi_nA8DdCfKbZoAQ_UOKNIN-X7aAOqC6hhWNcpm1MnnbwN6qmV3fv2umDU-FW-B_vvu2vIEdyT5bV9rlPEH1NZlaGsKiCIJpEggMPlRz1WwNtWZqLLFXwmr4xvIkSmDsY2NvZEOhTpwnDEUnYzlrCTamV6jBk7SWtQYsfU4SoHviBZM6D2VTQ8WI1Zh1XWE8kZkhH_G5vA2hZ7CZbhntO4LS5kYeiMI4bLSRt0kJPdiQxPYdAa261l-t4qqhLTRknzn433jeYmVMHr4w1frA-uxJVku1-xht15HqV2yrneLZHc-3kydF4L6ZRDAcqU1j0quqzTczrrZQc2fLgXMgHb2fJxXi_tRqC9_JwRmGKHygpWMTIXGnMkWEaO91wGIub8-DpCQ0tfdlXqKfSrsiBPgdrRlA2tUucAuPpHLtmj48WE5U1xyTPQv9k6Rtfn4lZ0kqBHc7AKFT6_sGlxnBIzmAU-EZ2nPmJFNJT09VIq9Imp_Fs7elpHhtSCfqGzhgOJhMt6lF8ViuY70tNzxJlMCKRYigesjXMOvW0DGaLA7s-17SIgvX5UI1DQlv7TkUnFd2n5lU1wofZfsFJO3aar05uf2gbJ0ITDufUXUwNw4fCeM8v2rUGvmppi3jYyNusFy8nkqUtI7ylKSPVC3HrxtPNyd9M2myQBRMQFuZy3vpg3Bxt2bYPuiIDvT7lRMAXIdjvoNVmszUdpXa0PZ8aKvKtWtVnFqLOi0BU9lVekU59Ds_rwiHdgw9ZN4R2am_PGiBJZzsdrsVMCZ2h6lZjQY230lTyo6Hu5qo-oMeGMUm0SlPOaeP6YeAOWQGPiFrQkVma7qzYsrKUGbCAzGJ4WqHFpPHsPpnw1bRme0WvNAEwREfnLXtvxgKjzzZsaRy3q9TrVfsIurvFxJ6yIFek9JxvJaOHj_nRRCeHSFKhPJqTwRDVgNKWJ2WnYDVdF-ulu8HaUTvqI5M9Zu2q9c7OiGWSzcbeKA0hMCMkhvaoJtK1VSZyH5XV8LDePlEH0ZT2MtjMlsSGic8ZzoRF3_eZuZsuC-iXpmlunVOh2TWeqPvpASsLzLDgvBuAJ2z56Ty1XHcpjnDmQFJzHSdNh5M-IR3HC-xy5WYiRjK2Fys-qLZEQ89PEx5NwBmUjX4oFibhLHunxvOnrsMbT3SpM_s2YM_qcDafe_0hMDcSxQK04tKCTg2EpMPKGu-RSoBAMhfSCmz2keslintYjXMdCEdzTfGz8mmdzdT5vFq8ZK7vPmWuqHopHiWO5EXxy3fZb7y7tMv1dXu-61QwSKpW3j9-zrl5W8jUl-TJd1oEr9U66FnmfN9BEKG2YLUuYvADCyjgwYAlPIETCTYIYFsDeIAAtBe2PWwY0vDHTisWQGq9jBsPqlbD_fXzPFkZtUn6FylVFllSwJfavczzsuiWl-RNd58hPk39-84OVihBrcTCd2878LhruaOf9LL4yyNFPcq76rGtv5hHkvvIUR8Z_nGoL990EoRqGPy0rOGvvqK__OqfkFLR-lYTAAA; MSCRM_LastServiceRequestId=e6b3bc6d-658e-43b8-9243-ddfd05e91eef; ARRAffinity=852caf45eec4999125b21ae0c5772372cb456b9c75d96af4c693c6d502b03e5c0147f6ec7070d5f17d585a426c08e7b4ef49366e3b807fdd66fcd6bafae280ff08DB413B4D8FF685909169359',
    "pragma": "no-cache",
    "referer": "https://mipau.crm6.dynamics.com/tools/DataManagement/datamanagement.aspx?sitemappath=Settings%7cSystem_Setting%7cnav_datamanagement&pagemode=iframe",
    "sec-ch-ua": '"Chromium";v="112", "Microsoft Edge";v="112", "Not:A-Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48",
}

cookies = {
    'ReqClientId': '9e4c0b96-7a84-4620-934a-58b6e95d49d3',
    'CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651SecuritySettingWebClientDeprecation': 'HideMessage',
    'CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2303%2f31%2f2023%2009%3a34%3a20': 'HideMessage',
    'CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651GetAppsForCrm': 'HideMessage',
    'CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651LegacySettingsRedirectionExperience': 'HideMessage',
    'visid_incap_2029367': 'pFVyDB7FTCm7DhvmYN5GUCRrQ2QAAAAAQUIPAAAAAAAkc/x2nuummlUVWW5mB7p+',
    'CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2304%2f17%2f2023%2010%3a31%3a25': 'HideMessage',
    'CRM_MSG_BAR_033139fd-55e9-ed11-8848-00224814f1f2SecuritySettingWebClientDeprecation': 'HideMessage',
    'persistentTemplateTourCookie': 'HideTemplateTour',
    'CRM_MSG_BAR_UsersWithoutRoleAlert%23033139fd-55e9-ed11-8848-00224814f1f2%2305%2f03%2f2023%2014%3a34%3a36': 'HideMessage',
    'CRM_MSG_BAR_033139fd-55e9-ed11-8848-00224814f1f2GetAppsForCrm': 'HideMessage',
    'lastUsedApp': 'appId=5144605b-14ec-ed11-8848-000d3a798df2&userId=ca75f96b-89ed-ed11-8848-6045bd3d4733',
    'CRM_MSG_BAR_ca75f96b-89ed-ed11-8848-6045bd3d4733SecuritySettingWebClientDeprecation': 'HideMessage',
    'CRM_MSG_BAR_UsersWithoutRoleAlert%23ca75f96b-89ed-ed11-8848-6045bd3d4733%2305%2f08%2f2023%2020%3a16%3a49': 'HideMessage',
    'CRM_MSG_BAR_ca75f96b-89ed-ed11-8848-6045bd3d4733GetAppsForCrm': 'HideMessage',
    'CRM_MSG_BAR_ca75f96b-89ed-ed11-8848-6045bd3d4733LegacySettingsRedirectionExperience': 'HideMessage',
    'orgId': '4ab0c6d7-85ed-ed11-a80c-002248e344c2',
    'persistentSearchTypeCookieEx': '1',
    'persistentSearchTypeCookie': '0',
    'nlbi_2029367': 'CwgaEO98WgzytyRVeEPzrwAAAACsGw1rphOYOQvpSBs10rye',
    'MSCC': 'NR',
    'incap_ses_362_2029367': 'svX2Hw67yAYu46SaNRUGBQixXWQAAAAAr4OIiH8Jt5IbREqHFJkfqA==',
    '_clck': '1cujtye|2|fbj|1|1227',
    '_clsk': '1qawf7j|1683861772397|1|1|n.clarity.ms/collect',
    'ARRAffinity': 'a65a045f2000a061bd67c8c08fffce17a6db0e064da6e3dafd21d68221bb809d80edd05839c87ae17bc323a228a36e7a533327001b2849f23e7cf0835621dc5508DB52A32187F2FA1348323828',
    'CrmOwinAuth': 'MAAAAICME_zeCRHtqAkAIkiUKq4Ppm71Xca5teL1cd7VA1BDheozlbVNuX44lVqqvE1cqR-LCAAAAAAABADFV2uPo8gV7Um35rHJriKtNN9WakX9LcHm_RhllYDBD2yDbWxs-IJ4FBjzNMXL_oH5XcHd05ud0SpKxlZSAgQFde65Vbcu597f3d39oDZhOsiyKAR8Ve7f3L25-1PXfW9X3hP60hjkcn25uK93r-3N3c_7sszhp34flrDXgXlZA3spKPvA90kMxRjExXwfIV0cRWzHxrtHzCE4AnUcDO-_ubsPIbwW43oOdvkdRrMYxmEcyz3ImqpcjZk6_s0xQZt_xqTYG2F-uoy_DHf3ILFhLwndIoOZX_bcLOm7sR0msG93sZEmoNxnHiyADwqQugDe5413_eRfQ6CA9v-XQOLfgMD83xJoYB9HUbaP0v3QA2kZlqdfk7r0uHYZZmmYwtJOXwMZxzn0JgHy1rX2Ntx_bPwRQ2-YlYJTTTJ2t4wDXLS53vufv_K-TWKY2XkvK4LPvlP97vjad1gVqZ2AtzO7gVl6PY2_fRuNIKxB-kxEtNMQxNcTeRvmtucVP-Ao0cNRrjuZHolfj_twYfn9C8vHW83a14v3Zeh-PVmZcwBu-dLrh6B4Ygic8liMRSiKxhAS0C5i4ySNUBzm-i7B2jbG3sDzvAq9P2IoSnSriONDfjBkiMH1uL8r9jTa4zcSP2xFlLEsYSKdOvclR10PGClw-dfW_HInCLwk9a63_QcYBt2Ot7otX4L3Xu1aSRrcIBt_gfvugusm-Y1hH6IEhtdjit-2Yy_b4F8h-OdDIW2t7b6C-BbN1t3akSMzpFJUnHh0RZqCvjs2RRtPyVNwPWXqv9ouJUi7fB56T_8J-vXkOunnPWeHXvycHf7e_Vw8u7Q7slkWXxj27Op6M9y3LVuVp_8TevdVGX7c6_qKlLmBtRmJY9fD9Gpt8fwNwGtQ3GO9G8jmhyb04JPDcL7jkx5CAJ9DSJrlEBYjCYShHYwjAUtRKHe9rfdtAq0kd8O3OMV163MTWfHuAtphPhD0jRCfaTYwLB8w9kaQH7NuUM8tLqFV7sss6mLylIPfqJre3H3ozHun1CrKf3z4tr8CH_ACz0vds4gg4owndJVZFp4XyAGzBXXBB2Ln4IbnFixzBqPlWt-P156x5JTlbBJMAwrhBIZ2OW1CLOmJ5FUr_0ysXHDmJIPYmocdipJq2pzDUjwYe6cOEciKkegUchXMSHQ-xmjuhIkxCKOBsfLVjTRKdVmQcVEaq4XDcSt9AHQGi48RSrAVw2jhXptJpymUKzRdodxWGHvKEvVX55jSW6OUPG-6CPd6km_omasJ0ZnUnXmbL91mhSRp6fnCEmHTCpc3pobFQrRBZsPGH8ddcOvT7ZZvnXSOLTDE0xbUiFXjAhYKlrDmCaxheziu5kqtzmoZnUTSqCMr1XINsHiC8woZpxZmoZ0FikilyXwf6bGHUuhZsHGKdxQsrdhBNSVsP8zslQSmK7WenQ_icWzN56jVzMBhGmULxNAtcUGdLW1_CpdFwir6MUpHUiMzEuszpjrdrUFoljm5dgY-5Z_GRxgIO6rwyYE1CnCOHMy7WRHmylnjjVBZt3Wl44xRmocKmid3WLsC6YvKJA6jbBQON_GabvigCBob2xtHfzQ8FVVtBpKdLJkFOjuvyMJfqWlgy4m3L2cUwBVkc0750YgVx8cqJ6yG3ivHbeapIPIIAy_YjXxo8m18tMx8izOWKFNmVam7tsoz3LEZzEe83MnRHeqOK8WNGWK4RerFzsSyelpJYdtu1S6qN4rhY3ExHeXLtSp7Makh4XJVbMBcOSIMW210g2EpT0urHczXVNkagT5uSbMmbB3S9FDeoaLGtPKCMPUiisXz0FlzwnI-TWd1o634FYO1Q28C3W3krU3bm7WYusCbZG7O7VqeHmhmvsvi5W7GS_So9OeKMfGswULaLAUGM7NVVDoHwlbpjgUT7mdZOpT50iZ9GxddRaHsOGZcxtRTVOVT-aQdHVhq6gaNkTzfi_ZphsamhmQ6sVjImOBPhHyhmm69nS5oFgREQqIzW3bBIFYkoa1PoioXGnHYCSt0N0y2WcSnrMa1iCzXWgaGlrrckDOh3ZlxgEpE7PCICc2RvymnkTPMqcX-jOoIN9ZCkWZbUgzlFUjH5oQOxZ2EzrCpMfBhJNHYRhrw9YQZRYQZyFRQt4kw1ZRl6hjtLh0vToVnSlF2zA7tNiLHisnZJUcH28AgjdIQ87N1spwsO0gGaCqWmbsHjdkdJwsqnrUMLwSGaasbUzZQQUtPjsjvNJ7QClI-lDE8qC957bvPeS0onktLgmVwhuG-fBf_xrtLu1zfdueHXgG8sOjE_-NrRk66Mqe6pFa61yHYnRKCzyLo-x4EEHblrHaRik-ki7k28EjEZigWIT0PdBWC7SIubvtdD-n7OPix10kJN9Jexk2GRafw_vpqJ86CLoX_IrSyNA5T8FLZZ0mSpf3sktrx_jPEZ9O_7-WggCHsBFh5974H2rzjDn-aZ-lfHjHskc-Lx646Ix5R6hOFfSLox9F8_a4XQlgB76d1BX71Ff7lV_8EwL3MH3QTAAA',
}

headers = {
    'authority': 'mipau.crm6.dynamics.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'ReqClientId=9e4c0b96-7a84-4620-934a-58b6e95d49d3; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651SecuritySettingWebClientDeprecation=HideMessage; CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2303%2f31%2f2023%2009%3a34%3a20=HideMessage; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651GetAppsForCrm=HideMessage; CRM_MSG_BAR_2bbb35de-0cc9-ed11-b595-00224818a651LegacySettingsRedirectionExperience=HideMessage; visid_incap_2029367=pFVyDB7FTCm7DhvmYN5GUCRrQ2QAAAAAQUIPAAAAAAAkc/x2nuummlUVWW5mB7p+; CRM_MSG_BAR_UsersWithoutRoleAlert%232bbb35de-0cc9-ed11-b595-00224818a651%2304%2f17%2f2023%2010%3a31%3a25=HideMessage; CRM_MSG_BAR_033139fd-55e9-ed11-8848-00224814f1f2SecuritySettingWebClientDeprecation=HideMessage; persistentTemplateTourCookie=HideTemplateTour; CRM_MSG_BAR_UsersWithoutRoleAlert%23033139fd-55e9-ed11-8848-00224814f1f2%2305%2f03%2f2023%2014%3a34%3a36=HideMessage; CRM_MSG_BAR_033139fd-55e9-ed11-8848-00224814f1f2GetAppsForCrm=HideMessage; lastUsedApp=appId=5144605b-14ec-ed11-8848-000d3a798df2&userId=ca75f96b-89ed-ed11-8848-6045bd3d4733; CRM_MSG_BAR_ca75f96b-89ed-ed11-8848-6045bd3d4733SecuritySettingWebClientDeprecation=HideMessage; CRM_MSG_BAR_UsersWithoutRoleAlert%23ca75f96b-89ed-ed11-8848-6045bd3d4733%2305%2f08%2f2023%2020%3a16%3a49=HideMessage; CRM_MSG_BAR_ca75f96b-89ed-ed11-8848-6045bd3d4733GetAppsForCrm=HideMessage; CRM_MSG_BAR_ca75f96b-89ed-ed11-8848-6045bd3d4733LegacySettingsRedirectionExperience=HideMessage; orgId=4ab0c6d7-85ed-ed11-a80c-002248e344c2; persistentSearchTypeCookieEx=1; persistentSearchTypeCookie=0; nlbi_2029367=CwgaEO98WgzytyRVeEPzrwAAAACsGw1rphOYOQvpSBs10rye; MSCC=NR; incap_ses_362_2029367=svX2Hw67yAYu46SaNRUGBQixXWQAAAAAr4OIiH8Jt5IbREqHFJkfqA==; _clck=1cujtye|2|fbj|1|1227; _clsk=1qawf7j|1683861772397|1|1|n.clarity.ms/collect; ARRAffinity=a65a045f2000a061bd67c8c08fffce17a6db0e064da6e3dafd21d68221bb809d80edd05839c87ae17bc323a228a36e7a533327001b2849f23e7cf0835621dc5508DB52A32187F2FA1348323828; CrmOwinAuth=MAAAAICME_zeCRHtqAkAIkiUKq4Ppm71Xca5teL1cd7VA1BDheozlbVNuX44lVqqvE1cqR-LCAAAAAAABADFV2uPo8gV7Um35rHJriKtNN9WakX9LcHm_RhllYDBD2yDbWxs-IJ4FBjzNMXL_oH5XcHd05ud0SpKxlZSAgQFde65Vbcu597f3d39oDZhOsiyKAR8Ve7f3L25-1PXfW9X3hP60hjkcn25uK93r-3N3c_7sszhp34flrDXgXlZA3spKPvA90kMxRjExXwfIV0cRWzHxrtHzCE4AnUcDO-_ubsPIbwW43oOdvkdRrMYxmEcyz3ImqpcjZk6_s0xQZt_xqTYG2F-uoy_DHf3ILFhLwndIoOZX_bcLOm7sR0msG93sZEmoNxnHiyADwqQugDe5413_eRfQ6CA9v-XQOLfgMD83xJoYB9HUbaP0v3QA2kZlqdfk7r0uHYZZmmYwtJOXwMZxzn0JgHy1rX2Ntx_bPwRQ2-YlYJTTTJ2t4wDXLS53vufv_K-TWKY2XkvK4LPvlP97vjad1gVqZ2AtzO7gVl6PY2_fRuNIKxB-kxEtNMQxNcTeRvmtucVP-Ao0cNRrjuZHolfj_twYfn9C8vHW83a14v3Zeh-PVmZcwBu-dLrh6B4Ygic8liMRSiKxhAS0C5i4ySNUBzm-i7B2jbG3sDzvAq9P2IoSnSriONDfjBkiMH1uL8r9jTa4zcSP2xFlLEsYSKdOvclR10PGClw-dfW_HInCLwk9a63_QcYBt2Ot7otX4L3Xu1aSRrcIBt_gfvugusm-Y1hH6IEhtdjit-2Yy_b4F8h-OdDIW2t7b6C-BbN1t3akSMzpFJUnHh0RZqCvjs2RRtPyVNwPWXqv9ouJUi7fB56T_8J-vXkOunnPWeHXvycHf7e_Vw8u7Q7slkWXxj27Op6M9y3LVuVp_8TevdVGX7c6_qKlLmBtRmJY9fD9Gpt8fwNwGtQ3GO9G8jmhyb04JPDcL7jkx5CAJ9DSJrlEBYjCYShHYwjAUtRKHe9rfdtAq0kd8O3OMV163MTWfHuAtphPhD0jRCfaTYwLB8w9kaQH7NuUM8tLqFV7sss6mLylIPfqJre3H3ozHun1CrKf3z4tr8CH_ACz0vds4gg4owndJVZFp4XyAGzBXXBB2Ln4IbnFixzBqPlWt-P156x5JTlbBJMAwrhBIZ2OW1CLOmJ5FUr_0ysXHDmJIPYmocdipJq2pzDUjwYe6cOEciKkegUchXMSHQ-xmjuhIkxCKOBsfLVjTRKdVmQcVEaq4XDcSt9AHQGi48RSrAVw2jhXptJpymUKzRdodxWGHvKEvVX55jSW6OUPG-6CPd6km_omasJ0ZnUnXmbL91mhSRp6fnCEmHTCpc3pobFQrRBZsPGH8ddcOvT7ZZvnXSOLTDE0xbUiFXjAhYKlrDmCaxheziu5kqtzmoZnUTSqCMr1XINsHiC8woZpxZmoZ0FikilyXwf6bGHUuhZsHGKdxQsrdhBNSVsP8zslQSmK7WenQ_icWzN56jVzMBhGmULxNAtcUGdLW1_CpdFwir6MUpHUiMzEuszpjrdrUFoljm5dgY-5Z_GRxgIO6rwyYE1CnCOHMy7WRHmylnjjVBZt3Wl44xRmocKmid3WLsC6YvKJA6jbBQON_GabvigCBob2xtHfzQ8FVVtBpKdLJkFOjuvyMJfqWlgy4m3L2cUwBVkc0750YgVx8cqJ6yG3ivHbeapIPIIAy_YjXxo8m18tMx8izOWKFNmVam7tsoz3LEZzEe83MnRHeqOK8WNGWK4RerFzsSyelpJYdtu1S6qN4rhY3ExHeXLtSp7Makh4XJVbMBcOSIMW210g2EpT0urHczXVNkagT5uSbMmbB3S9FDeoaLGtPKCMPUiisXz0FlzwnI-TWd1o634FYO1Q28C3W3krU3bm7WYusCbZG7O7VqeHmhmvsvi5W7GS_So9OeKMfGswULaLAUGM7NVVDoHwlbpjgUT7mdZOpT50iZ9GxddRaHsOGZcxtRTVOVT-aQdHVhq6gaNkTzfi_ZphsamhmQ6sVjImOBPhHyhmm69nS5oFgREQqIzW3bBIFYkoa1PoioXGnHYCSt0N0y2WcSnrMa1iCzXWgaGlrrckDOh3ZlxgEpE7PCICc2RvymnkTPMqcX-jOoIN9ZCkWZbUgzlFUjH5oQOxZ2EzrCpMfBhJNHYRhrw9YQZRYQZyFRQt4kw1ZRl6hjtLh0vToVnSlF2zA7tNiLHisnZJUcH28AgjdIQ87N1spwsO0gGaCqWmbsHjdkdJwsqnrUMLwSGaasbUzZQQUtPjsjvNJ7QClI-lDE8qC957bvPeS0onktLgmVwhuG-fBf_xrtLu1zfdueHXgG8sOjE_-NrRk66Mqe6pFa61yHYnRKCzyLo-x4EEHblrHaRik-ki7k28EjEZigWIT0PdBWC7SIubvtdD-n7OPix10kJN9Jexk2GRafw_vpqJ86CLoX_IrSyNA5T8FLZZ0mSpf3sktrx_jPEZ9O_7-WggCHsBFh5974H2rzjDn-aZ-lfHjHskc-Lx646Ix5R6hOFfSLox9F8_a4XQlgB76d1BX71Ff7lV_8EwL3MH3QTAAA',
    'pragma': 'no-cache',
    'referer': 'https://mipau.crm6.dynamics.com/tools/DataManagement/datamanagement.aspx?sitemappath=Settings%7cSystem_Setting%7cnav_datamanagement&pagemode=iframe',
    'sec-ch-ua': '"Microsoft Edge";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.35',
}


COOKIES = cookies
HEADERS = headers


def step_1_extract_to_ddb(entity_list=TEMPLATE_ENTITIES):
    full_extract_to_ddb(entity_list=entity_list, multi_threaded=True)
    return True


def get_all_records_to_df(dc: DynamicsSession, logical_name):
    dc.authenticate()
    entity_definition = dc.get_all_records(
        f"EntityDefinitions(LogicalName='{logical_name}')"
    )[0]
    try:
        entity_name = get_endpoint_name(dc, entity_definition)
    except ValueError:
        logger.error(f"Could not find endpoint for {logical_name}")
        return
    page_number = 0
    logger.info(f"Extracting all pages for {entity_name}")
    df = pd.json_normalize([*dc.gen_all_records(entity_name)])
    return df


def get_all_records_to_df_with_name(dc: DynamicsSession, logical_name):
    dc.authenticate()
    entity_definition = dc.get_all_records(
        f"EntityDefinitions(LogicalName='{logical_name}')"
    )[0]
    try:
        entity_name = get_endpoint_name(dc, entity_definition)
    except ValueError:
        logger.error(f"Could not find endpoint for {logical_name}")
        return
    page_number = 0
    logger.info(f"Extracting all pages for {entity_name}")
    df = pd.json_normalize([*dc.gen_all_records(entity_name)])
    return logical_name, df


def step_x_json_extracts_to_duckdb(entity_list=TEMPLATE_ENTITIES, load_staging=True):
    dc = DynamicsSession()
    dc.authenticate()
    dc.set_page_size(5000)
    duck_db_path = str(DUCKDB_DB_PATH)
    dfs = {}
    with duckdb.connect(duck_db_path) as con:
        for entity_name in entity_list:
            df = get_all_records_to_df(dc, entity_name)
            df = prepare_column_names(df)
            con.execute("SET GLOBAL pandas_analyze_sample=100000")
            con.sql(f"DROP TABLE IF EXISTS {entity_name}")
            con.sql(f"CREATE TABLE IF NOT EXISTS {entity_name} AS SELECT * FROM df")
            con.sql(f"DESCRIBE {entity_name}").show()
            if load_staging:
                logger.info(f"Uploading {entity_name} to staging.")
                load_to_table(
                    df,
                    entity_name,
                    schema="staging",
                    if_exists="append",
                    use_mssql=True,
                )
                logger.info(f"Uploading {entity_name} to staging - Done")
            else:
                dfs[entity_name] = df
            con.commit()
    return dfs


def step_x_json_extracts_to_duckdb_async(
    entity_list=TEMPLATE_ENTITIES, load_staging=True
):
    dc = DynamicsSession()
    dc.authenticate()
    dc.set_page_size(5000)
    duck_db_path = str(DUCKDB_DB_PATH)
    dfs = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for entity_name in entity_list:
            futures.append(
                executor.submit(get_all_records_to_df_with_name, dc, entity_name)
            )
        for future in as_completed(futures):
            logger.info(f"Future Completed: {entity_name}.")
            entity_name, df = future.result()
            df = prepare_column_names(df)
            dfs[entity_name] = df
        if load_staging:
            for entity_name, df in dfs.items():
                logger.info(f"Uploading {entity_name} to staging.")
                executor.submit(
                    load_to_table,
                    df,
                    entity_name,
                    schema="staging",
                    if_exists="append",
                    use_mssql=True,
                )
                logger.info(f"Uploading {entity_name} to staging - Done")
    with duckdb.connect(duck_db_path) as con:
        for entity_name, df in dfs.items():
            con.execute("SET GLOBAL pandas_analyze_sample=100000")
            con.sql(f"DROP TABLE IF EXISTS {entity_name}")
            con.sql(f"CREATE TABLE IF NOT EXISTS {entity_name} AS SELECT * FROM df")
            con.sql(f"DESCRIBE {entity_name}").show()
            if load_staging:
                logger.info(f"Uploading {entity_name} to staging.")
                load_to_table(
                    df,
                    entity_name,
                    schema="staging",
                    if_exists="append",
                    use_mssql=True,
                )
                logger.info(f"Uploading {entity_name} to staging - Done")
            else:
                dfs[entity_name] = df
            con.commit()
    return dfs


def step_x_get_import_templates():
    get_import_templates(
        entity_list=TEMPLATE_ENTITIES,
        templates_html=TEMPLATES_HTML,
        headers=HEADERS,
        cookies=COOKIES,
    )
    pass


def step_x_extract_import_templates():
    for template_xlsx in TEMPLATES_ROOT.glob("*.xlsx"):
        logger.info(
            f"Extracting templates to template directory - {template_xlsx.name}"
        )
        extract_xlsx_to_template_dir(template_path=template_xlsx)
    logger.info("Extracting templates to template directory - Done")
    logger.info("Converting templates to json")
    import_templates_to_json(template_parent_path=TEMPLATES_ROOT)
    logger.info("Converting templates to json - Done")
    pass


def step_x_ddb_to_sql(entity_name):
    logger.info(f"Loading {entity_name} to DataFrame.")
    df = ddb_extract_to_df(DDB_ROOT, entity_name)
    logger.info(f"Uploading {entity_name} to staging.")
    load_to_table(df, entity_name, schema="staging", if_exists="append", use_mssql=True)
    logger.info(f"Uploading {entity_name} to staging - Done")
    return entity_name, df


def step_x_df_to_staging(entity_name, df):
    logger.info(f"Uploading {entity_name} to staging.")
    load_to_table(df, entity_name, schema="staging", if_exists="append", use_mssql=True)
    logger.info(f"Uploading {entity_name} to staging - Done")
    return entity_name, df


def get_duckdb_tables(con, duckdb_path=DUCKDB_DB_PATH):
    duck_db_path = str(duckdb_path)
    duckdb_tables = con.sql("""SHOW TABLES""").fetchall()
    return duckdb_tables


def clean_x000D_from_all_columns(con, table_name):
    # Get all columns
    columns = con.sql(
        f"""SELECT name FROM pragma_table_info('{table_name}')"""
    ).fetchall()
    for column in columns:
        try:
            column_name = column[0]
            logger.debug(f"Cleaning {column_name} from {table_name}")
            num_matches_before = con.sql(
                f"""SELECT COUNT(*) FROM {table_name} WHERE REGEXP_MATCHES({column_name}, '(_x\w{4}_x\w{4}_)|(_x\w{4}_)')"""
            ).fetchone()
            con.sql(
                f"""UPDATE {table_name} SET {column_name} = REGEXP_REPLACE({column_name}, '(_x\w{4}_x\w{4}_)', '')"""
            )
            con.sql(
                f"""UPDATE {table_name} SET {column_name} = REGEXP_REPLACE({column_name}, '(_x\w{4}_)', '')"""
            )
            con.commit()
            num_matches_after = con.sql(
                f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE REGEXP_MATCHES({column_name}, '(_x\w{4}_x\w{4}_)|(_x\w{4}_)')
                """
            ).fetchone()
            if num_matches_after != num_matches_before:
                logger.info(
                    f"Cleaning {column_name} from {table_name} - Done. {num_matches_before} matches before, {num_matches_after} matches after."
                )
            con.commit()
        except duckdb.BinderException as e:
            logger.error(f"Error cleaning {column_name} from {table_name}. {e}")
    ...


def step_x_clean_x000D_from_all_columns(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        duckdb_tables = get_duckdb_tables(con, duckdb_path)
        for table in duckdb_tables:
            table_name = table[0]
            logger.info(f"Cleaning table {table_name}")
            clean_x000D_from_all_columns(con, table_name)
            logger.info(f"Cleaning table {table_name} - Done")
    ...


def remove_deactivated_rows_from_df(df):
    df_raw = df.copy()
    status_col = [col for col in df.columns if "statuscode_FormattedValue" in col]
    master_id_col = [col for col in df.columns if "masterid" in col.lower()]
    merged_col = [col for col in df.columns if col == "merged"]
    df_inactive = pd.DataFrame()
    df_has_masterid = pd.DataFrame()
    df_merged = pd.DataFrame()
    # DataFrame of rows where masterid is not null
    if master_id_col:
        df_masterid_not_null = df[master_id_col][df[master_id_col[0]].notnull()]
        df_has_masterid = df_raw[df_raw[master_id_col[0]].notnull()]
        df = df[df[master_id_col[0]].isnull()]
    if status_col:
        df_status_not_null = df[status_col][df[status_col[0]] != "Active"]
        df_inactive = df_raw[df_raw[status_col[0]] != "Active"]
        # df = df[df[status_col[0]] == "Active"]
    if merged_col:
        df_is_merged = df[merged_col][df[merged_col[0]] == True]
        df_merged = df_raw[df_raw[merged_col[0]] == True]
        df = df[df[merged_col[0]] == False]
    return df, df_inactive, df_merged, df_has_masterid


def drop_duckdb_intermediate_tables(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        duckdb_tables = get_duckdb_tables(con, duckdb_path)
        intermediate_table_pat = re.compile(r"_L\d+_")
        for table in duckdb_tables:
            table_name = table[0]
            if "_L" in table_name:
                if intermediate_table_pat.search(table_name):
                    logger.info(f"Dropping intermediate table {table_name}")
                    con.execute(f"""DROP TABLE IF EXISTS {table_name}""")
                    con.commit()
                    logger.info(f"Dropping intermediate table {table_name} - Done")

    ...


def step_x_clean_duckdb_tables_level_1(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        duckdb_tables = get_duckdb_tables(con, duckdb_path)
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for table in duckdb_tables:
            table_name = table[0]
            if table_name not in TEMPLATE_ENTITIES:
                continue
            if "_Cleaned" in table_name or "_Template" in table_name:
                continue
            logger.info(f"Cleaning table {table_name}")
            df = con.sql(f"""SELECT * FROM {table_name}""").df()
            (
                df,
                df_inactive,
                df_merged,
                df_has_masterid,
            ) = remove_deactivated_rows_from_df(df)
            if df_inactive.empty:
                logger.info(f"Table {table_name} has no deactivated rows.")
            else:
                logger.info(
                    f"Table {table_name} has {len(df_inactive)} deactivated rows."
                )
                con.sql(f"""DROP TABLE IF EXISTS {table_name}_L1_Inactive""")
                con.sql(
                    f"""CREATE TABLE {table_name}_L1_Inactive AS SELECT * FROM df_inactive"""
                )
                con.sql(f"""SELECT * FROM {table_name}_L1_Inactive""").df().to_csv(
                    f"../data/cleaned/{table_name}_L1_Inactive.csv",
                    quoting=csv.QUOTE_NONNUMERIC,
                    index=False,
                )
                con.commit()
            if df_has_masterid.empty:
                logger.info(f"Table {table_name} has no rows with masterid.")
            else:
                logger.info(
                    f"Table {table_name} has {len(df_has_masterid)} rows with masterid."
                )
                con.sql(f"""DROP TABLE IF EXISTS {table_name}_L1_Masterid""")
                con.sql(
                    f"""CREATE TABLE {table_name}_L1_Masterid AS SELECT * FROM df_has_masterid"""
                )
                con.sql(f"""SELECT * FROM {table_name}_L1_Masterid""").df().to_csv(
                    f"../data/cleaned/{table_name}_L1_HasMasterid.csv",
                    quoting=csv.QUOTE_NONNUMERIC,
                    index=False,
                )
                con.commit()
            if df_merged.empty:
                logger.info(f"Table {table_name} has no merged rows.")
            else:
                logger.info(f"Table {table_name} has {len(df_merged)} merged rows.")
                con.sql(f"""DROP TABLE IF EXISTS {table_name}_L1_Merged""")
                con.sql(
                    f"""CREATE TABLE {table_name}_L1_Merged AS SELECT * FROM df_merged"""
                )
                con.sql(f"""SELECT * FROM {table_name}_L1_Merged""").df().to_csv(
                    f"../data/cleaned/{table_name}_L1_Merged.csv",
                    quoting=csv.QUOTE_NONNUMERIC,
                    index=False,
                )
                con.commit()
            con.sql(f"""DROP TABLE IF EXISTS {table_name}_L1_Cleaned""")
            con.sql(f"""CREATE TABLE {table_name}_L1_Cleaned AS SELECT * FROM df""")
            con.sql(f"""SELECT * FROM {table_name}_L1_Cleaned""").df().to_csv(
                f"../data/cleaned/{table_name}_L1_Cleaned.csv",
                quoting=csv.QUOTE_NONNUMERIC,
                index=False,
            )
            con.commit()
            logger.info(f"Cleaning table {table_name} - Done")
    ...


def step_x_clean_duckdb_tables_level_2(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    cleaned_table_pat = re.compile(r"_L\d+_Cleaned")
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for table in con.sql("""SHOW TABLES""").fetchall():
            table_name = table[0]
            if "L1_Cleaned" not in table_name:
                continue
            entity_name = table_name.replace("_L1_Cleaned", "")
            if entity_name not in TEMPLATE_ENTITIES:
                continue
            logger.info(f"Cleaning table {table_name}")
            df_leads = con.sql(f"""SELECT * FROM {table_name}""").df()
            for col in df_leads.columns:
                logger.debug(f"Cleaning column {col} from {table_name}")
                df_leads[col] = df_leads[col].apply(
                    lambda x: cleansing_steps.remove_x000d_from_values(x)
                )
                if "emailaddress" in col:
                    df_leads[col] = df_leads[col].apply(
                        lambda x: cleansing_steps.clean_email_address(x)
                    )
                elif "telephone" in col or "mobilephone" in col:
                    df_leads[col] = df_leads[col].apply(
                        lambda x: cleansing_steps.clean_phone_number(x)
                    )
                elif "websiteurl" in col:
                    df_leads[col] = df_leads[col].apply(
                        lambda x: cleansing_steps.clean_website_url(x)
                    )
                    ...
            con.sql(f"""DROP TABLE IF EXISTS {entity_name}_L2_Cleaned""")
            con.sql(
                f"""CREATE TABLE {entity_name}_L2_Cleaned AS SELECT * FROM df_leads"""
            )
            con.sql(f"""SELECT * FROM {entity_name}_L2_Cleaned""").df().to_csv(
                f"../data/cleaned/{entity_name}_L2_Cleaned.csv", index=True
            )
            con.commit()
            logger.info(f"Cleaning table {table_name} - Done")


def get_level_2_duckdb_dfs(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    cleaned_table_pat = re.compile(r"_L\d+_Cleaned")
    dfs = {}
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for table in con.sql("""SHOW TABLES""").fetchall():
            table_name = table[0]
            if "L2_Cleaned" not in table_name:
                continue
            entity_name = table_name.replace("_L2_Cleaned", "")
            if entity_name not in TEMPLATE_ENTITIES:
                continue
            logger.info(f"Cleaning table {table_name} to Level 3")
            if entity_name not in dfs:
                dfs[entity_name] = {}
            dfs[entity_name]["df"] = con.sql(f"""SELECT * FROM {table_name}""").df()
            dfs[entity_name]["entity_name"] = entity_name
            dfs[entity_name]["table_name"] = table_name
            dfs[entity_name]["cols"] = dfs[entity_name]["df"].columns
    return dfs


def get_level_3_duckdb_dfs(duckdb_path=DUCKDB_DB_PATH, **kwargs):
    duckdb_path = str(duckdb_path)
    cleaned_table_pat = re.compile(r"_L\d+_Cleaned")
    dfs = {}
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for table in con.sql("""SHOW TABLES""").fetchall():
            table_name = table[0]
            if "L3_Cleaned" not in table_name:
                continue
            entity_name = table_name.replace("_L3_Cleaned", "")
            if entity_name not in TEMPLATE_ENTITIES or entity_name not in kwargs.get(
                "select", TEMPLATE_ENTITIES
            ):
                continue
            logger.info(f"Cleaning table {table_name} to Level 4")
            if entity_name not in dfs:
                dfs[entity_name] = {}
            dfs[entity_name]["df"] = con.sql(f"""SELECT * FROM {table_name}""").df()
            dfs[entity_name]["entity_name"] = entity_name
            dfs[entity_name]["table_name"] = table_name
            dfs[entity_name]["cols"] = dfs[entity_name]["df"].columns
    return dfs


def clean_level_2_df_to_level_3_df(entity_name, table_name, df):
    logger.info(f"[{entity_name}] Cleaning table {entity_name} to Level 3")
    do_cleaning = False
    cols = df.columns
    for col in cols:
        if "_country" in col:
            do_cleaning = True
            break
        if "_state" in col:
            do_cleaning = True
            break
        if "firstname" in col or "lastname" in col:
            do_cleaning = True
            break
    if do_cleaning:
        start_time = time.perf_counter()
        logger.debug(f"[{entity_name}] Converting nans to None in {table_name}")
        # Convert all nans to None
        df = df.replace({np.nan: None})
        time_to_clean_na = time.perf_counter() - start_time
        logger.debug(
            f"[{entity_name}] Converting nans to None in {table_name} - Took {int(time_to_clean_na)} seconds"
        )

        if "firstname" in cols and "lastname" in cols:
            logger.debug(
                f"[{entity_name}] Inferring First and Last Name from Email in {table_name}"
            )
            # df = df.apply(
            #     lambda x: cleansing_steps.infer_first_last_name_from_email(
            #         x, entity_name=entity_name
            #     ),
            #     axis=1,
            # )
            df = cleansing_steps.infer_first_last_name_from_email_df(
                df, entity_name=entity_name
            )
            time_to_infer_first_last_name = (
                time.perf_counter() - start_time - time_to_clean_na
            )
            logger.debug(
                f"[{entity_name}] Inferring First and Last Name from Email in {table_name} - Took {int(time_to_infer_first_last_name)} seconds"
            )
        else:
            logger.debug(
                f"[{entity_name}] Inferring First and Last Name from Email in {table_name} - Skipping"
            )
            time_to_infer_first_last_name = (
                time.perf_counter() - start_time - time_to_clean_na
            )

        logger.debug(f"[{entity_name}] Cleaning country and state from {table_name}")
        # df = df.apply(
        #     lambda x: cleansing_steps.clean_country_state(x, entity_name=entity_name),
        #     axis=1,
        # )
        df = cleansing_steps.clean_country_state_df(df, entity_name=entity_name)
        time_to_clean_country_state = (
            time.perf_counter() - start_time - time_to_infer_first_last_name
        )
        logger.debug(
            f"Cleaning country and state from {table_name} - Took {int(time_to_clean_country_state)} seconds"
        )
        logger.debug(f"[{entity_name}] Cleaning telephone1 from {table_name}")
        df = cleansing_steps.set_telephone1_to_mobilephone_if_empty_df(
            df, entity_name=entity_name
        )
        time_to_clean_telephone1 = (
            time.perf_counter() - start_time - time_to_clean_country_state
        )
        logger.debug(
            f"[{entity_name}] Cleaning telephone1 from {table_name} - Took {int(time_to_clean_telephone1)} seconds"
        )

        time_to_clean_all = time.perf_counter() - start_time
        logger.debug(
            f"Cleaning table {table_name} - Took {int(time_to_clean_all)} seconds"
        )

    if entity_name == "opportunity":
        df = cleansing_steps.swap_next_steps_for_notes_df(df, entity_name=entity_name)
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="opportunityid",
            target_field="name",
            entity_name=entity_name,
        )
        ...
    elif entity_name == "opportunityclose":
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="_opportunityid_value",
            target_field="_opportunityid_value_FormattedValue",
            entity_name=entity_name,
        )
        ...
    elif entity_name == "opportunityproduct":
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="_opportunityid_value",
            target_field="_opportunityid_value_FormattedValue",
            entity_name=entity_name,
        )
        ...
    elif entity_name == "quote":
        # quoteid --> quotenumber
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="quoteid",
            target_field="quotenumber",
            entity_name=entity_name,
        )
        # opportunity --> name
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="quoteid",
            target_field="name",
            entity_name=entity_name,
        )
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="_opportunityid_value",
            target_field="_opportunityid_value_FormattedValue",
            entity_name=entity_name,
        )
        ...
    elif entity_name == "quoteclose":
        # quoteid --> subject
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="_quoteid_value",
            target_field="subject",
            entity_name=entity_name,
        )
        ...
    elif entity_name == "quotedetail":
        df = cleansing_steps.append_id_to_name_df(
            df,
            source_field="_quoteid_value",
            target_field="_quoteid_value_FormattedValue",
            entity_name=entity_name,
        )
        ...
    # Change column dtypes to string
    for col in df.columns:
        if str(df[col].dtype) == "bool":
            df[col] = df[col].astype(str)
    return entity_name, table_name, df


def load_level_3_dfs_to_duckdb(dfs, duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        for entity_name, data in dfs.items():
            table_name = data["table_name"]
            df = data["df"]
            if "L2_Cleaned" not in table_name:
                return
            else:
                table_name = table_name.replace("_L2_Cleaned", "_L3_Cleaned")
            logger.info(f"Loading table {table_name} to DuckDB")
            con.execute("SET GLOBAL pandas_analyze_sample=100000")
            df_arrow = pa.Table.from_pandas(df)
            df.to_csv(
                f"../data/cleaned/{entity_name}_L3_Cleaned.csv",
                index=True,
                quoting=csv.QUOTE_ALL,
            )
            con.sql(f"""DROP TABLE IF EXISTS {entity_name}_L3_Cleaned""")
            con.sql(
                f"""CREATE TABLE {entity_name}_L3_Cleaned AS SELECT * FROM df_arrow"""
            )
            con.commit()
            logger.info(f"Cleaning table {table_name} - Done")
    return entity_name


def step_x_clean_duckdb_tables_level_3_async(duckdb_path=DUCKDB_DB_PATH, **kwargs):
    start_time = time.perf_counter()
    logger.info(f"Cleaning DuckDB tables to Level 3 - Multi-threaded")
    dfs = get_level_2_duckdb_dfs(duckdb_path)
    futures = []
    with ThreadPoolExecutor(max_workers=kwargs.get("max_workers", 2)) as executor:
        for entity_name, data in dfs.items():
            table_name = data["table_name"]
            df = data["df"]
            futures.append(
                executor.submit(
                    clean_level_2_df_to_level_3_df,
                    entity_name=entity_name,
                    table_name=table_name,
                    df=df,
                )
            )
    time_to_submit_futures = time.perf_counter() - start_time
    logger.info(
        f"Cleaning DuckDB tables to Level 3 - Multi-threaded - All futures submitted in {int(time_to_submit_futures)} seconds"
    )
    for future in as_completed(futures):
        logger.info(
            f"Cleaning DuckDB tables to Level 3 - Multi-threaded - Future completed for {entity_name}."
        )
        entity_name, table_name, df = future.result()
        dfs[entity_name]["df"] = df
    time_to_complete_futures = time.perf_counter() - start_time - time_to_submit_futures
    logger.info(
        f"Cleaning DuckDB tables to Level 3 - Multi-threaded - All futures completed in {int(time_to_complete_futures)} seconds"
    )
    results = load_level_3_dfs_to_duckdb(dfs, duckdb_path=duckdb_path)
    logger.info(
        f"Cleaning DuckDB tables to Level 3 - Multi-threaded - Done in {int(time.perf_counter() - start_time)} seconds"
    )
    return results


def step_x_clean_duckdb_tables_level_3(duckdb_path=DUCKDB_DB_PATH, **kwargs):
    start_time = time.perf_counter()
    logger.info(f"Cleaning DuckDB tables to Level 3 - Single-Threaded")
    dfs = get_level_2_duckdb_dfs(duckdb_path)
    futures = []
    dfs_lvl3 = {}
    entity_list = kwargs.get("entity_list", list(dfs.keys()))
    if type(entity_list) == str:
        entity_list = [entity_list]
    for entity_name, data in dfs.items():
        if entity_name not in entity_list:
            continue
        dfs_lvl3[entity_name] = {}
        table_name = data["table_name"]
        dfs_lvl3[entity_name]["table_name"] = table_name
        dfs_lvl3[entity_name]["entity_name"] = entity_name
        df = data["df"]
        entity_name, table_name, df_lvl3 = clean_level_2_df_to_level_3_df(
            entity_name, table_name, df
        )
        dfs_lvl3[entity_name]["df"] = df_lvl3
    time_to_clean_tables = time.perf_counter() - start_time
    logger.info(
        f"Cleaning DuckDB tables to Level 3 - Single-threaded - All Cleansing Completed in {int(time_to_clean_tables)} seconds"
    )

    results = load_level_3_dfs_to_duckdb(dfs_lvl3, duckdb_path=duckdb_path)
    logger.info(
        f"Cleaning DuckDB tables to Level 3 - Single-threaded - Done in {int(time.perf_counter() - start_time)} seconds"
    )

    return results


def step_x_clean_duckdb_tables_level_3_sync(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    cleaned_table_pat = re.compile(r"_L\d_Cleaned")
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for table in con.sql("""SHOW TABLES""").fetchall():
            table_name = table[0]
            if "L2_Cleaned" not in table_name:
                continue
            entity_name = table_name.replace("_L2_Cleaned", "")
            if entity_name not in TEMPLATE_ENTITIES:
                continue
            logger.info(f"Cleaning table {table_name} to Level 3")
            df_lvl2 = con.sql(f"""SELECT * FROM {table_name}""").df()
            lvl2_cols = df_lvl2.columns
            do_cleaning = False
            for col in lvl2_cols:
                if "_country" in col:
                    do_cleaning = True
                    break
                if "_state" in col:
                    do_cleaning = True
                    break
                if "firstname" in col or "lastname" in col:
                    do_cleaning = True
                    break
            if do_cleaning:
                start_time = time.perf_counter()
                logger.debug(f"Converting nans to None in {table_name}")
                # Convert all nans to None
                df_lvl2 = df_lvl2.replace({np.nan: None})
                time_to_clean_na = time.perf_counter() - start_time
                logger.debug(
                    f"Converting nans to None in {table_name} - Took {int(time_to_clean_na)} seconds"
                )

                if "firstname" in lvl2_cols and "lastname" in lvl2_cols:
                    logger.debug(
                        f"Inferring First and Last Name from Email in {table_name}"
                    )
                    df_lvl2 = df_lvl2.apply(
                        lambda x: cleansing_steps.infer_first_last_name_from_email(x),
                        axis=1,
                    )
                    time_to_infer_first_last_name = (
                        time.perf_counter() - start_time - time_to_clean_na
                    )
                    logger.debug(
                        f"Inferring First and Last Name from Email in {table_name} - Took {int(time_to_infer_first_last_name)} seconds"
                    )
                else:
                    logger.debug(
                        f"Inferring First and Last Name from Email in {table_name} - Skipping"
                    )
                    time_to_infer_first_last_name = (
                        time.perf_counter() - start_time - time_to_clean_na
                    )

                logger.debug(f"Cleaning country and state from {table_name}")
                df_lvl2 = df_lvl2.apply(
                    lambda x: cleansing_steps.clean_country_state(x), axis=1
                )
                time_to_clean_country_state = (
                    time.perf_counter() - start_time - time_to_infer_first_last_name
                )
                logger.debug(
                    f"Cleaning country and state from {table_name} - Took {int(time_to_clean_country_state)} seconds"
                )

                time_to_clean_all = time.perf_counter() - start_time
                logger.debug(
                    f"Cleaning table {table_name} - Took {int(time_to_clean_all)} seconds"
                )

            # Change column dtypes to string
            for col in df_lvl2.columns:
                if str(df_lvl2[col].dtype) == "bool":
                    df_lvl2[col] = df_lvl2[col].astype(str)
            df_arrow = pa.Table.from_pandas(df_lvl2)
            df_lvl2.to_csv(
                f"../data/cleaned/{entity_name}_L3_Cleaned.csv",
                index=True,
                quoting=csv.QUOTE_ALL,
            )
            con.sql(f"""DROP TABLE IF EXISTS {entity_name}_L3_Cleaned""")
            con.sql(
                f"""CREATE TABLE {entity_name}_L3_Cleaned AS SELECT * FROM df_arrow"""
            )
            con.commit()
            logger.info(f"Cleaning table {table_name} - Done")


def step_x_get_cleaned_tables(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        duckdb_path = str(duckdb_path)
        duckdb_tables = get_duckdb_tables(con, duckdb_path)
        cleaned_table_pat = re.compile(r"(\w+)(_L(\d)_Cleaned)")
        cleaned_tables = {}
        for table in duckdb_tables:
            table_name = table[0]
            if "_Cleaned" not in table_name:
                continue
            entity_name = cleaned_table_pat.match(table_name).group(1)
            if "_Cleaned" in entity_name or "_Template" in entity_name:
                continue
            level = cleaned_table_pat.match(table_name).group(3)
            if entity_name not in cleaned_tables:
                cleaned_tables[entity_name] = {}
            cleaned_tables[entity_name][level] = table_name
    return cleaned_tables


def map_duckdb_table_to_template(duckdb_table, duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    pat = re.compile(r"(\w+)(_L(\d)_Cleaned)")
    entity_name = pat.match(duckdb_table).group(1)
    if (
        "_Cleaned" in entity_name
        or "_Template" in entity_name
        or "_Inactive" in entity_name
    ):
        return
    level = pat.match(duckdb_table).group(3)
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        logger.info(f"Reading table {duckdb_table} from duckdb.")
        df = con.sql(f"""SELECT * FROM {duckdb_table}""").df()
        logger.info(f"Reading table {duckdb_table} from duckdb - Done")
        logger.info(f"Mapping table {duckdb_table} to template.")
        template_df = map_df_to_template(df, entity_name)
        if template_df.empty:
            logger.warning(f"Mapping table {duckdb_table} to template - Failed")
            return
        template_df.reset_index(inplace=True)
        con.sql(f"""DROP TABLE IF EXISTS {entity_name}_L{level}_Template""")
        con.sql(
            f"""CREATE TABLE {entity_name}_L{level}_Template AS SELECT * FROM template_df"""
        )
        con.sql(f"""SELECT * FROM {entity_name}_L{level}_Template""").df().to_csv(
            f"../data/cleaned/{entity_name}_L{level}_Template.csv", index=True
        )
        con.commit()
        logger.info(f"Mapping table {duckdb_table} to template - Done")
        return template_df


def step_x_map_duckdb_tables_to_template(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    cleaned_tables = step_x_get_cleaned_tables(duckdb_path)

    with duckdb.connect(duckdb_path) as con:
        for entity_name, levels in cleaned_tables.items():
            for level, table in levels.items():
                map_duckdb_table_to_template(table, duckdb_path)


def step_x_ddb_extract_to_staging():
    dfs = {}
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [
            executor.submit(step_x_ddb_to_sql, entity) for entity in TEMPLATE_ENTITIES
        ]
        for future in as_completed(futures):
            entity_name, df = future.result()
            dfs[entity_name] = df
        return dfs


def step_x_map_table_to_migration(entity_name, schema="migration", engine=get_engine()):
    engine = get_engine()
    with engine.connect() as con:
        logger.info(f"Reading table {entity_name} from staging.")
        staging_df = pd.read_sql_table(entity_name, con, schema="staging")
        logger.info(f"Reading table {entity_name} from staging - Done")
        logger.info(f"Mapping table {entity_name} to migration.")
        template_df = map_df_to_template(staging_df, entity_name, TEMPLATES_JSON)
        logger.info(f"Loading table {entity_name} to migration.")
        load_to_table_replace(
            template_df,
            entity_name,
            schema="migration",
            if_exists="replace",
            engine=engine,
            index=False,
        )
        logger.info(f"Loading table {entity_name} to migration - Done")
        return entity_name, template_df


def step_x_map_staging_to_migration():
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(step_x_map_table_to_migration, entity, schema="migration")
            for entity in TEMPLATE_ENTITIES
        ]
        results = {}
        for future in as_completed(futures):
            entity_name, df = future.result()
            results[entity_name] = df
    return results


def step_x_ddb_extract_to_csv():
    get_all_ddb_to_csv(ddb_root=DDB_ROOT, templates_root=TEMPLATES_JSON)
    return True


def step_x_clean_csv():
    pass


def step_x_validate_relationships():
    pass


def get_highest_level_template_table(con):
    tables = get_duckdb_tables(con)
    pat = re.compile(r"(\w+)(_L(\d+)_Template)")
    template_tables = {}
    for table in tables:
        table_name = table[0]
        if "_Template" not in table_name:
            continue
        try:
            entity_name = pat.match(table_name).group(1)
        except AttributeError as e:
            logger.warning(f"Could not parse table name {table_name}")
            continue
        level = pat.match(table_name).group(3)
        if entity_name not in template_tables:
            template_tables[entity_name] = {}
            template_tables[entity_name][level] = table_name
        elif level not in template_tables[entity_name]:
            template_tables[entity_name][level] = table_name
    for template_table in template_tables:
        template_tables[template_table] = template_tables[template_table][
            max(template_tables[template_table].keys())
        ]
    return template_tables


def get_deduped_template_table(con):
    tables = get_duckdb_tables(con)
    pat = re.compile(r"(\w+)(_L(\d+)_Deduped)")
    template_tables = {}
    for table in tables:
        table_name = table[0]
        if "_Deduped" not in table_name:
            continue
        try:
            entity_name = pat.match(table_name).group(1)
        except AttributeError as e:
            logger.warning(f"Could not parse table name {table_name}")
            continue
        level = pat.match(table_name).group(3)
        if entity_name not in template_tables:
            template_tables[entity_name] = {}
            template_tables[entity_name][level] = table_name
        elif level not in template_tables[entity_name]:
            template_tables[entity_name][level] = table_name
    for template_table in template_tables:
        template_tables[template_table] = template_tables[template_table][
            max(template_tables[template_table].keys())
        ]
    return template_tables


def step_x_duckdb_template_to_sql(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    template_dfs = {}
    with duckdb.connect(duckdb_path) as con:
        template_tables = get_highest_level_template_table(con)
        for template_table in template_tables:
            logger.info(f"Reading table {template_table} from duckdb.")
            template_dfs[template_table] = con.sql(
                f"""SELECT * FROM {template_tables[template_table]}"""
            ).df()
            logger.info(f"Reading table {template_table} from duckdb - Done")
    futures = []
    with ThreadPoolExecutor() as executor:
        for entity_name, df in template_dfs.items():
            futures.append(
                executor.submit(
                    load_to_table_replace,
                    df,
                    entity_name,
                    schema="migration",
                    if_exists="replace",
                    index=False,
                )
            )
        for future in as_completed(futures):
            logger.info(f"Loading table {future.result()} to migration. - Done")


def step_x_duckdb_deduped_template_to_sql(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    template_dfs = {}
    with duckdb.connect(duckdb_path) as con:
        template_tables = get_deduped_template_table(con)
        for template_table in template_tables:
            logger.info(f"Reading table {template_table} from duckdb.")
            template_dfs[template_table] = con.sql(
                f"""SELECT * FROM {template_tables[template_table]}"""
            ).df()
            logger.info(f"Reading table {template_table} from duckdb - Done")
    futures = []
    with ThreadPoolExecutor() as executor:
        for entity_name, df in template_dfs.items():
            futures.append(
                executor.submit(
                    load_to_table_replace,
                    df,
                    entity_name,
                    schema="deduped",
                    if_exists="replace",
                    index=False,
                )
            )
        for future in as_completed(futures):
            logger.info(f"Loading table {future.result()} to migration. - Done")


def step_x_template_column_defs_to_csv(template_json=TEMPLATES_JSON):
    template_json = Path(template_json)
    template_dfs = {}
    for template_json_file in template_json.glob("*.json"):
        with open(template_json_file, "r") as f:
            template = json.load(f)
        entity_name = template_json_file.stem
        columns = [col for col in template["columns"].values()]
        template_dfs[entity_name] = pd.json_normalize(columns)
    for entity_name, df in template_dfs.items():
        df.to_csv(f"../data/cleaned/{entity_name}_Template_Columns.csv", index=False)
    return template_dfs


def step_x_top_level_template_tables(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    top_level_template_tables = {}
    with duckdb.connect(duckdb_path) as con:
        template_tables = get_highest_level_template_table(con)
        for name, template_table in template_tables.items():
            logger.info(f"Reading table {template_table} from duckdb.")
            top_level_template_tables[name] = {}
            top_level_template_tables[name]["table_name"] = template_table
            top_level_template_tables[name]["df"] = con.sql(
                f"""SELECT * FROM {template_tables[name]}"""
            ).df()
            logger.info(f"Reading table {template_table} from duckdb - Done")
    return top_level_template_tables


def step_x_validate_templates(top_level_templates=None, template_json=TEMPLATES_JSON):
    if not top_level_templates:
        top_level_templates = step_x_top_level_template_tables()
    template_json = Path(template_json)
    template_dfs = {}
    validations = {}
    for template_json_file in template_json.glob("*.json"):
        with open(template_json_file, "r") as f:
            template = json.load(f)
        entity_name = template_json_file.stem
        columns = [col for col in template["columns"].values()]
        template_dfs[entity_name] = pd.json_normalize(columns)
    for entity_name, template in top_level_templates.items():
        if entity_name not in validations:
            validations[entity_name] = {}
        template_df = template["df"]
        template_name = template["table_name"]
        validation_df = template_dfs[entity_name]
        validations[entity_name]["template_name"] = template_name
        validations[entity_name]["entity_name"] = entity_name
        validations[entity_name] = check_all_columns_are_present(
            validations[entity_name], template_df, validation_df
        )
        validations[entity_name] = check_required_fields_are_present(
            validations[entity_name], template_df, validation_df
        )
        validations[entity_name] = check_value_is_in_option_set(
            validations[entity_name], template_df, validation_df
        )
        ...
    for entity_name, validation in validations.items():
        logger.info(f"[{entity_name}] Exporting validations for {entity_name}")
        missing_required = set()
        not_in_option_set = set()
        failed_validation_records = set()
        for key, value in validation.items():
            if key.startswith("records_missing_required_fields"):
                for display_name, records in value.items():
                    for record in records:
                        record["error"] = f"Missing Required Field"
                        record["error_type"] = "required_field"
                        record["error_field"] = display_name
                        record["error_value"] = record[display_name]
                        missing_required.add(json.dumps(record))
                        failed_validation_records.add(json.dumps(record))
            if key.startswith("records_with_invalid_option_set_values"):
                for display_name, records in value.items():
                    for record in records:
                        record["error"] = f"Value not in Option Set for Field"
                        record["error_type"] = "option_set"
                        record["error_field"] = display_name
                        record["option_set_values"] = validation["option_set_values"][
                            display_name
                        ]
                        record["error_value"] = record[display_name]
                        not_in_option_set.add(json.dumps(record))
                        failed_validation_records.add(json.dumps(record))
        validation["missing_required"] = [
            json.loads(rec) for rec in list(missing_required)
        ]
        validation["not_in_option_set"] = [
            json.loads(rec) for rec in list(not_in_option_set)
        ]
        validation["failed_validation_records"] = [
            json.loads(rec) for rec in list(failed_validation_records)
        ]
        df_missing_required = pd.json_normalize(validation["missing_required"])
        df_not_in_option_set = pd.json_normalize(validation["not_in_option_set"])
        df_failed_validation_records = pd.json_normalize(
            validation["failed_validation_records"]
        )
        if "error" not in df_missing_required.columns:
            df_missing_required["error"] = ""
            df_missing_required["error_type"] = "required_field"
            df_missing_required["error_field"] = ""
            df_missing_required["error_value"] = ""
        if "error" not in df_not_in_option_set.columns:
            df_not_in_option_set["error"] = ""
            df_not_in_option_set["error_type"] = "option_set"
            df_not_in_option_set["error_field"] = ""
            df_not_in_option_set["option_set_values"] = ""
            df_not_in_option_set["error_value"] = ""
        if "error" not in df_failed_validation_records.columns:
            df_failed_validation_records["error"] = ""
            df_failed_validation_records["error_type"] = ""
            df_failed_validation_records["error_field"] = ""
            df_failed_validation_records["option_set_values"] = ""
            df_failed_validation_records["error_value"] = ""
        df_missing_required.set_index(["error", "error_field"], inplace=True)
        df_missing_required.sort_index(inplace=True)
        try:
            if "option_set_values" not in df_not_in_option_set.columns:
                df_not_in_option_set["option_set_values"] = ""
            df_not_in_option_set.set_index(
                [
                    "error",
                    "error_type",
                    "error_field",
                    "option_set_values",
                    "error_value",
                ],
                inplace=True,
            )
            df_not_in_option_set.sort_index(inplace=True)
            if "option_set_values" not in df_failed_validation_records.columns:
                df_failed_validation_records["option_set_values"] = ""
            df_failed_validation_records.set_index(
                [
                    "error",
                    "error_type",
                    "error_field",
                    "option_set_values",
                    "error_value",
                ],
                inplace=True,
            )
            df_failed_validation_records.sort_index(inplace=True)
        except KeyError as e:
            logger.warning(
                f"[{entity_name}] Error: {e}, Could not set index for {entity_name}"
            )
        logger.info(
            f"[{entity_name}] Exporting validation errors to CSV for {entity_name}"
        )
        df_missing_required.to_csv(
            f"../data/cleaned/_{entity_name}_missing_required.csv",
            index=True,
            quoting=csv.QUOTE_ALL,
        )
        df_not_in_option_set.to_csv(
            f"../data/cleaned/_{entity_name}_not_in_option_set.csv",
            index=True,
            quoting=csv.QUOTE_ALL,
        )
        df_failed_validation_records.to_csv(
            f"../data/cleaned/_{entity_name}_failed_validation_records.csv",
            index=True,
            quoting=csv.QUOTE_ALL,
        )
        with duckdb.connect("../data/crm_validations.db") as con:
            logger.info(f"[{entity_name}] Creating duckdb table for validation errors")
            con.execute("SET GLOBAL pandas_analyze_sample=100000")
            validation_table_name = f"_{entity_name}_failed_validation_records"
            con.sql(f"DROP TABLE IF EXISTS {validation_table_name}")
            if df_failed_validation_records.empty:
                logger.info(
                    f"[{entity_name}] No validation errors found. Skipping table creation"
                )
            else:
                df_failed_validation_records.reset_index(inplace=True)
                con.sql(
                    f"CREATE TABLE {validation_table_name} AS SELECT * FROM df_failed_validation_records"
                )
                logger.info(
                    f"[{entity_name}] Creating duckdb table for validation errors - Done"
                )
        # with open(
        #     f"../data/cleaned/_{entity_name}_template_validations.json", "w"
        # ) as f:
        #     logger.info(
        #         f"[[{entity_name}]] Exporting validation errors to JSON for {entity_name}"
        #     )
        #     json.dump(validation, f, indent=4)
        logger.info(
            f"[{entity_name}] Exporting validation errors for {entity_name} - Done"
        )
    return validations


def load_to_migration_table_replace(table_name, df, dtype=None):
    engine = get_engine(echo=False)
    logger.info(f"[{table_name}] Loading to migration table")
    num_records = len(df)
    logger.info(f"[{table_name}] Loading to migration table - {num_records} records")
    # Split df into chunks of 1000 records
    df_chunks = np.array_split(df, math.ceil(len(df) / 1000))
    logger.info(
        f"[{table_name}] Loading to migration table - Creating table from {len(df_chunks)} chunks"
    )
    # Drop table if exists
    with engine.connect() as sql_conn:

        for i, df_chunk in enumerate(df_chunks):
            logger.info(
                f"[{table_name}] Loading to migration table - Chunk {i+1}/{len(df_chunks)}"
            )
            if i == 0:
                if_exists = "replace"
            else:
                if_exists = "append"
            df_chunk.to_sql(
                table_name,
                sql_conn,
                dtype=dtype,
                if_exists=if_exists,
                index=True,
                schema="migration",
                chunksize=10000,
            )
            sql_conn.commit()
        logger.info(f"[{table_name}] Loading to migration table - Done")


def load_to_table_chunked(
    schema, table_name, df, dtype=None, chunk_size=1000, **kwargs
):
    engine = get_engine(echo=kwargs.get("echo", False))
    logger.info(f"[{table_name}] Loading to migration table")
    num_records = len(df)
    logger.info(f"[{table_name}] Loading to migration table - {num_records} records")
    # Split df into chunks of 1000 records
    if num_records < chunk_size:
        df_chunks = [df]
    else:
        df_chunks = np.array_split(df, math.ceil(len(df) / chunk_size))
    logger.info(
        f"[{table_name}] Loading to migration table - Creating table from {len(df_chunks)} chunks"
    )
    # Drop table if exists
    with engine.connect() as sql_conn:

        for i, df_chunk in enumerate(df_chunks):
            logger.info(
                f"[{table_name}] Loading to migration table - Chunk {i+1}/{len(df_chunks)}"
            )
            if i == 0:
                if_exists = "replace"
            else:
                if_exists = "append"
            df_chunk.to_sql(
                table_name,
                sql_conn,
                dtype=dtype,
                if_exists=if_exists,
                index=kwargs.get("index", True),
                schema=schema,
                chunksize=10000,
            )
            sql_conn.commit()
        logger.info(
            f"[{table_name}] Loading {len(df)} records to migration table - Done"
        )
    return f"[{table_name}] Loading {len(df)} records to migration table - Done"


def load_validation_errors_to_sql():
    duckdb_path = "../data/crm_validations.db"
    dfs = {}
    engine = get_engine(echo=True)
    with duckdb.connect(duckdb_path) as con:
        tables = con.execute("SHOW tables").fetchall()
        for table in tables:
            table_name = table[0]
            df = con.sql(f"SELECT * FROM {table_name}").df()
            dfs[table_name] = df
        ...
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for table, df in dfs.items():
            index_cols = [
                "error",
                "error_type",
                "error_field",
                "option_set_values",
                "error_value",
            ] + [col for col in df.columns if col.startswith("(Do Not Modify)")]
            df.set_index(index_cols, inplace=True)
            df.sort_index(inplace=True)
            dtype = {
                "error": sqlalchemy.types.VARCHAR(500),
                "error_type": sqlalchemy.types.VARCHAR(500),
                "error_field": sqlalchemy.types.VARCHAR(500),
                "option_set_values": sqlalchemy.types.VARCHAR(1000),
                "error_value": sqlalchemy.types.VARCHAR(1000),
            }
            for col in df.index.names:
                if col.startswith("(Do Not Modify)"):
                    dtype[col] = sqlalchemy.types.VARCHAR(200)
            futures.append(
                executor.submit(
                    load_to_migration_table_replace,
                    table_name=table,
                    df=df,
                    dtype=dtype,
                )
            )
        for future in as_completed(futures):
            result = future.result()
    return dfs


def get_inactive_users_rename_dict(target_user="Analytics @MIP"):
    active_users, inactive_users = get_active_inactive_users()
    inactive_users_rename_dict = {k: "Analytics @MIP" for k in inactive_users}
    return inactive_users_rename_dict


def reassign_records_owned_by_inactive_users(df):
    df = df.copy()
    inactive_users_rename_dict = get_inactive_users_rename_dict()
    if "Owner" not in df.columns:
        return df
    df.replace({"Owner": inactive_users_rename_dict}, inplace=True)
    return df


def step_x_clean_top_duckdb_template_tables(
    top_level_templates=None, duckdb_path=DUCKDB_DB_PATH
):
    if top_level_templates is None:
        top_level_templates = step_x_top_level_template_tables()
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for name, template in top_level_templates.items():
            table_name, df = template.values()
            template_level_pat = re.compile(
                r"^(?P<entity_name>\w+)_L(?P<template_level>\d+)_Template"
            )
            match = template_level_pat.match(table_name)
            changed = False
            if not match:
                logger.error(
                    f"Could not parse table name {table_name} into entity name and template level"
                )
                continue
            entity_name = match.group("entity_name")
            template_level = match.group("template_level")
            template_level_up = int(template_level) + 1
            logger.info(f"Cleaning {entity_name} template level {template_level}")
            logger.info(
                f"Re-assigning records owned by inactive users to Analytics @MIP"
            )
            df = reassign_records_owned_by_inactive_users(df)
            logger.info(
                f"Re-assigning records owned by inactive users to Analytics @MIP - Done"
            )
            if name == "account":
                # Category Field: High Potential --> Preferred Customer
                # Category Field: Marketing Account --> Standard
                # Category Field: RED and Tableau Customer --> Standard
                # Category Field: Red Customer --> Standard
                # Category Field: Tableau Customer --> Standard
                df["Category"] = df["Category"].replace(
                    {
                        "High Potential": "Preferred Customer",
                        "Marketing Account": "Standard",
                        "RED and Tableau Customer": "Standard",
                        "Red Customer": "Standard",
                        "Tableau Customer": "Standard",
                        "Previous Customer": "Preferred Customer",
                    }
                )
                # Ownership Field: SH --> Other
                df["Ownership"] = df["Ownership"].replace("SH", "Other")
                df = df.replace(
                    {
                        "Industry": {
                            "Agriculture, Forestry and Fishing": "Farming, Ranching, Forestry",
                            "Communications": "Technology, Information and Media",
                            "Mining, Construction and Manufacturing": None,
                            "Retail, Wholesale and Real Estate": None,
                            "Non classifiable": None,
                            "Public Sector": "Government Administration",
                            "Services": "Professional Services",
                            "Transport": "Transportation, Logistics, Supply Chain and Storage",
                        }
                    }
                )
                changed = True
            elif name == "contact":
                # Business Phone: if null, and Mobile Phone not null, then copy Mobile Phone to Business Phone
                df.loc[
                    df["Business Phone"].isna() & ~df["Mobile Phone"].isna(),
                    "Business Phone",
                ] = df["Mobile Phone"]
                # Preferred Method of Contact: is E-mail, then Email
                df["Preferred Method of Contact"] = df[
                    "Preferred Method of Contact"
                ].replace({"E-mail": "Email"})
                # Role: if in ["Accouting", "Contractor"] then Employee, if IT Procurement Officer then Decision Maker
                df["Role"] = df["Role"].replace(
                    {
                        "Accouting": "Employee",
                        "Contractor": "Employee",
                        "IT Procurement Officer": "Decision Maker",
                        "Technical User": "Employee",
                        "Key Business User": "Influencer",
                        "Gate Keeper": "Decision Maker",
                        "Admin": "Influencer",
                    }
                )
                changed = True
            elif name == "lead":
                # Lead Source: if WhereScape Web Site Request, then Web
                df = cleansing_steps.parse_first_and_last_name_from_record(
                    df, entity_name=entity_name
                )
                df["Lead Source"] = df["Lead Source"].replace(
                    "WhereScape Web Site Request", "Web"
                )
                # Status Reason: if Active, then New
                df["Status Reason"] = df["Status Reason"].replace("Active", "New")
                # Status Reason: if Converted, then Qualified
                df["Status Reason"] = df["Status Reason"].replace(
                    "Converted", "Qualified"
                )
                # Status Reason: if Dead, Bad Data
                df["Status Reason"] = df["Status Reason"].replace("Dead", "Bad Data")
                # Status Reason: if EDM, then "Re-nurture"
                df["Status Reason"] = df["Status Reason"].replace("EDM", "Re-nurture")
                df = df.replace(
                    {
                        "Industry": {
                            "Agriculture, Forestry and Fishing": "Farming, Ranching, Forestry",
                            "Communications": "Technology, Information and Media",
                            "Mining, Construction and Manufacturing": None,
                            "Retail, Wholesale and Real Estate": None,
                            "Non classifiable": None,
                            "Public Sector": "Government Administration",
                            "Services": "Professional Services",
                            "Transport": "Transportation, Logistics, Supply Chain and Storage",
                        }
                    }
                )
                changed = True
            elif name == "opportunity":
                # Est. close date: if nan, then 01/01/1970
                df["Est. close date"] = df["Est. close date"].fillna("01/01/1970")
                # Est. revenue: if nan, then $0.00
                df["Est. revenue"] = df["Est. revenue"].fillna("$0.00")
                # Rating: If Status = "Won", then Hot, else Cold
                df.loc[df["Status"] == "Won", "Rating"] = "Hot"
                df.loc[df["Status"] != "Won", "Rating"] = "Cold"
                # Status Reason: Alteryx Acquisition --> Direct with Vendor
                df["Status Reason"] = df["Status Reason"].replace(
                    ["Alteryx Acquisition", "Vendor Direct"], "Direct with Vendor"
                )
                # Status Reason: No decision --> Lack of Decision Maker (Authority)
                df["Status Reason"] = df["Status Reason"].replace(
                    "No decision", "Lack of Decision Maker (Authority)"
                )
                # Status Reason: Competitor --> Outsold (Competitor)
                df["Status Reason"] = df["Status Reason"].replace(
                    "Competitor", "Outsold (Competitor)"
                )
                # Status Reason: Out Sold --> Outsold (Competitor)
                df["Status Reason"] = df["Status Reason"].replace(
                    "Out Sold", "Outsold (Competitor)"
                )
                # Status Reason: No exec sponsor --> Lack of Decision Maker (Authority)
                df["Status Reason"] = df["Status Reason"].replace(
                    "No exec sponsor", "Lack of Decision Maker (Authority)"
                )
                # Status Reason: Lost momentum --> Poor Qualification (Time)
                df["Status Reason"] = df["Status Reason"].replace(
                    "Lost momentum", "Poor Qualification (Time)"
                )
                df["Status Reason"] = df["Status Reason"].replace(
                    "Lost funding", "Lack of Funding (Money)"
                )
                df["Price List"] = df["Price List"].replace(
                    {
                        "MIP Price List": "MIP Legacy Price List",
                    }
                )
                df = df.replace(
                    {
                        "Decision Maker?": {"mark complete": None},
                        "Send Thank You Email": {"Mark complete": None},
                        "Final Proposal Ready": {"Mark complete": None},
                        "Present Final Proposal": {"mark complete": None},
                        "Identify Competitors": {"mark complete": None},
                        "Identify Customer Contacts": {"mark complete": None},
                    }
                )
                changed = True
            elif name == "pricelevel":
                df["Name"] = df["Name"].replace(
                    {
                        "MIP Price List": "MIP Legacy Price List",
                    }
                )
                changed = True
            elif name == "product":
                df["Name"] = "[Legacy] " + df["Name"].astype(str)
                df["Product ID"] = "[Legacy] " + df["Product ID"].astype(str)
                changed = True
            elif name == "productpricelevel":
                df["Price List"] = df["Price List"].replace(
                    {
                        "MIP Price List": "MIP Legacy Price List",
                    }
                )
                df["Product"] = "[Legacy] " + df["Product"].astype(str)
                changed = True
            elif name in ["opportunityproduct", "quotedetail"]:
                df["Name"] = "[Legacy] " + df["Name"].astype(str)
                df.loc[
                    df["Select Product"] == "Existing", "Existing Product"
                ] = "[Legacy] " + df["Existing Product"].astype(str)
                changed = True
            elif name == "quote":
                df["Price List"] = df["Price List"].replace(
                    {
                        "MIP Price List": "MIP Legacy Price List",
                    }
                )
                changed = True
            elif name == "uom":
                df["Unit Schedule"] = df["Unit Schedule"].replace(
                    {
                        "561a3436-4df6-4536-802a-c8d2cb68ccce": "Default Unit",
                    }
                )
                changed = True
            else:
                logger.error(f"Could not find cleaning instructions for {entity_name}")
                continue
            if changed:
                # Load the next level template to duckdb
                logger.info(
                    f"Loading {entity_name} template level {template_level_up} to duckdb"
                )
                con.execute(
                    f"DROP TABLE IF EXISTS {entity_name}_L{template_level_up}_Template"
                )
                con.execute(
                    f"CREATE TABLE {entity_name}_L{template_level_up}_Template AS SELECT * FROM df"
                )
                con.execute(
                    f"SELECT * FROM {entity_name}_L{template_level_up}_Template"
                ).df().to_csv(
                    f"../data/cleaned/{entity_name}_L{template_level_up}_Template.csv",
                    index=True,
                    quoting=csv.QUOTE_ALL,
                )
                logger.info(
                    f"Loading {entity_name} template level {template_level_up} to duckdb complete"
                )
    pass


def step_x_salesloft_full_extract():
    salesloft_extract()


def step_x_salesloft_extract_to_duckdb(
    extracts=None, duckdb_path=SALESLOFT_DUCKDB_DB_PATH, **kwargs
):
    duckdb_path = str(duckdb_path)
    if not extracts:
        extracts = salesloft_extracts_to_df(**kwargs)
    save_extracts_to_duckdb(extracts=extracts, duckdb_path=duckdb_path)
    return extracts


def clean_salesloft_df(df, entity_name):
    if entity_name in ["people", "accounts"]:
        phone_number_cols = [col for col in df.columns if "phone" in col.lower()]
        for col in phone_number_cols:
            df[col] = df[col].apply(lambda x: cleansing_steps.clean_phone_number(x))
    return df


def step_x_clean_salesloft_tables_level_1(duckdb_path=SALESLOFT_DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        tables = con.execute("SHOW tables").fetchall()
        for table in tables:
            logger.info(f"Cleaning Salesloft {table} table to Level 1.")
            table = table[0]
            if "_L" in table:
                continue
            df = con.execute(f"SELECT * FROM {table}").fetchdf()
            # Clean the data
            df = clean_salesloft_df(df=df, entity_name=table)
            logger.info(f"Cleaning Salesloft {table} table to Level 1 complete.")
            # Load the cleaned data back to duckdb
            cleaned_table = f"{table}_L1_Cleaned"
            con.execute(f"DROP TABLE IF EXISTS {cleaned_table}")
            con.execute(f"CREATE TABLE {cleaned_table} AS SELECT * FROM df")

            logger.info(f"Saving cleaned Salesloft {table} table to CSV.")
            df.set_index("id", inplace=True)
            df.to_csv(f"../data/salesloft_cleaned/{cleaned_table}.csv", index=True)
        ...


@lru_cache()
def make_salesloft_request(url, field=None):
    if not url or not isinstance(url, str):
        return None
    response = sl.get(url)
    if response.status_code != 200:
        logger.error(f"Request to {url} failed with status code {response.status_code}")
        return None
    data = response.json()["data"]
    output = data.get(field, data)
    return data


def expand_salesloft_ids(df, duckdb_path=SALESLOFT_DUCKDB_DB_PATH):
    sl = Salesloft()
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        id_cols = [col for col in df.columns if ".id" in col.lower()]
        href_cols = [col for col in df.columns if "._href" in col.lower()]
        for col in id_cols:
            if col in ["owner.id", "creator.id", "last_contacted_by.id"]:
                ref = col.split(".")[0]
                df = con.sql(
                    f"""SELECT *, {ref}.name FROM df INNER JOIN users as {ref} ON df."{col}" = users.id"""
                )
                ...
            elif col in ["account.id"]:
                continue
        return df


def step_x_clean_salesloft_tables_level_2(duckdb_path=SALESLOFT_DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        tables = [table[0] for table in con.execute("SHOW tables").fetchall()]
        for table in tables:
            table_pat = re.compile(r"(.*)_L(\d+)_Cleaned")
            table_match = table_pat.match(table)
            if not table_match or int(table_match.group(2)) != 1:
                continue
            entity_name = table_match.group(1)
            template_level_up = 2
            con.execute(
                f"DROP TABLE IF EXISTS {entity_name}_L{template_level_up}_Cleaned"
            )
            df = con.execute(f"SELECT * FROM {table}").fetchdf()
            df = expand_salesloft_ids(df=df)
            con.execute(
                f"CREATE TABLE {entity_name}_L{template_level_up}_Cleaned AS SELECT * FROM df"
            )
            df.to_csv(
                f"../data/salesloft_cleaned/{entity_name}_L{template_level_up}_Cleaned.csv",
                index=True,
            )
            logger.info(f"Cleaning Salesloft {table} table to Level 2.")


def step_x_salesloft_extract_to_ddb(
    salesloft_extract_path="../data/salesloft-extract/full", ddb_path=DDB_ROOT
):
    salesloft_extract_path = Path(salesloft_extract_path)
    sl_extract_entities = []
    for entity_path in salesloft_extract_path.iterdir():
        if entity_path.is_dir():
            sl_extract_entities.append(entity_path)
    for entity_path in sl_extract_entities:
        entity_name = entity_path.name
        ddb_archive_path = f"salesloft/{entity_name}"
        logger.info(f"Extracting {entity_name} from Salesloft extract to DDB.")
        extract_pages = entity_path.glob("*-extract-page-*.json")
        records = []
        for page in extract_pages:
            content = json.loads(page.read_text())
            records.extend(content["data"])
        if not DDB.at(ddb_archive_path).exists():
            logger.info(f"Creating DDB at {ddb_archive_path}")
            DDB.at(ddb_archive_path).create()
        with DDB.at(ddb_archive_path).session() as (session, archive):
            logger.info(f"Writing {len(records)} records to DDB at {ddb_archive_path}")
            for record in records:
                try:
                    archive[str(record["id"])] = record
                except TypeError as e:
                    logger.error(
                        f"Error writing (key: {str(record['id'])}) {record} to DDB at {ddb_archive_path}"
                    )
                    logger.error(e)
                    continue
            session.write()
        logger.info(f"Extracting {entity_name} from Salesloft extract to DDB complete.")

    ddb_archive_path = "salesloft"


def step_x_salesloft_expand_ids_in_ddb(ddb_path=DDB_ROOT):
    records = {"accounts": {}, "people": {}, "users": {}}
    for entity in ["accounts", "people", "users"]:
        with DDB.at(f"salesloft/{entity}").session() as (session, archive):
            records[entity] = archive
    for entity in records.keys():
        for record in records[entity].values():
            if "owner" in record:
                if record["owner"]:
                    owner_id = str(record["owner"]["id"])
                    record["owner"]["name"] = records["users"][owner_id]["name"]
            if "creator" in record:
                if record["creator"]:
                    creator_id = str(record["creator"]["id"])
                    record["creator"]["name"] = records["users"][creator_id]["name"]
            if "last_contacted_by" in record:
                if record["last_contacted_by"]:
                    last_contacted_by_id = str(record["last_contacted_by"]["id"])
                    record["last_contacted_by"]["name"] = records["users"][
                        last_contacted_by_id
                    ]["name"]
            if "account" in record:
                if record["account"]:
                    account_id = str(record["account"]["id"])
                    record["account"]["name"] = records["accounts"][account_id]["name"]
            if "last_contacted_person" in record:
                try:
                    if record["last_contacted_person"]:
                        last_contacted_person_id = str(
                            record["last_contacted_person"]["id"]
                        )
                        record["last_contacted_person"]["name"] = records["people"][
                            last_contacted_person_id
                        ]["display_name"]
                except KeyError as e:
                    logger.error(f"KeyError for {record['last_contacted_person']}")
                    continue
    for entity in records.keys():
        with DDB.at(f"salesloft/{entity}").session() as (session, archive):
            for record in records[entity].values():
                archive[str(record["id"])] = record
            session.write()


def run_salesloft_etl_jobs():
    step_x_salesloft_full_extract()
    step_x_salesloft_extract_to_ddb()
    step_x_salesloft_expand_ids_in_ddb()
    step_x_salesloft_extract_to_duckdb(use_ddb=True)
    step_x_clean_salesloft_tables_level_1()
    top_level_crm_salesloft_to_sql()


def drop_duckdb_tables_not_in_template(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    con = duckdb.connect(duckdb_path)
    tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
    tables = tables["name"].to_list()
    for table in tables:
        if table not in TEMPLATE_ENTITIES:
            con.execute(f"DROP TABLE {table}")
            logger.info(f"Dropped {table} from DuckDB.")


def get_active_inactive_users():
    dfs = get_level_3_duckdb_dfs(select="systemuser")
    system_users = dfs["systemuser"]["df"].to_dict(orient="records")
    active_users = set()
    inactive_users = set()
    active_user_records = [
        user for user in system_users if user["isdisabled"] == "False"
    ]
    inactive_users_records = [
        user for user in system_users if user["isdisabled"] == "True"
    ]
    [
        active_users.add(user["fullname"])
        for user in system_users
        if user["isdisabled"] == "False"
    ]
    [
        inactive_users.add(user["fullname"])
        for user in system_users
        if user["isdisabled"] == "True"
    ]
    [inactive_users.remove(name) for name in active_users if name in inactive_users]
    return active_users, inactive_users


def step_x_deduplicate_template_tables(
    top_level_templates=None, duckdb_path=DUCKDB_DB_PATH
):
    logger.info("Deduplicating template tables.")
    if top_level_templates is None:
        top_level_templates = step_x_top_level_template_tables()
    duckdb_path = str(duckdb_path)
    dfs = {}
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        for name, template in top_level_templates.items():
            table_name, df = template.values()
            template_level_pat = re.compile(
                r"^(?P<entity_name>\w+)_L(?P<template_level>\d+)_Template"
            )
            match = template_level_pat.match(table_name)
            changed = False
            if not match:
                logger.error(
                    f"Could not parse table name {table_name} into entity name and template level"
                )
                continue
            entity_name = match.group("entity_name")
            template_level = match.group("template_level")
            template_level_up = int(template_level) + 1
            # TODO: Implement deduplication logic here
            if entity_name in ["account", "contact", "lead"]:
                logger.info(
                    f"[{entity_name}] Cleaning {entity_name} template level {template_level} to {template_level_up}"
                )
                logger.info(
                    f"[{entity_name}] Deduplicating {entity_name} template level {template_level} to {template_level_up}"
                )
                df = cleansing_steps.dedupe_table(df, entity_name)
                changed = True
            if changed:
                df.to_csv(
                    f"../data/cleaned/{entity_name}_L{template_level_up}_Deduped.csv",
                    index=True,
                )
                table_name = f"{entity_name}_L{template_level_up}_Deduped"
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                logger.info(f"[{entity_name}] Created {table_name} in DuckDB.")


def run_crm_etl_jobs(update_extract=False):
    if update_extract:
        step_x_json_extracts_to_duckdb_async(
            entity_list=TEMPLATE_ENTITIES, load_staging=False
        )
        # step_1_extract_to_ddb(entity_list=TEMPLATE_ENTITIES)
    drop_duckdb_tables_not_in_template()
    drop_duckdb_intermediate_tables()
    step_x_clean_duckdb_tables_level_1()
    step_x_clean_duckdb_tables_level_2()
    step_x_clean_duckdb_tables_level_3()
    step_x_map_duckdb_tables_to_template()
    step_x_clean_top_duckdb_template_tables()
    step_x_validate_templates()
    step_x_deduplicate_template_tables()
    step_x_duckdb_template_to_sql()
    step_x_duckdb_deduped_template_to_sql()
    load_validation_errors_to_sql()
    # step_x_ddb_extract_to_staging()
    # step_x_map_staging_to_migration()
    # step_x_ddb_extract_to_csv()


def run_template_jobs():
    step_x_get_import_templates()
    step_x_extract_import_templates()
    step_x_template_column_defs_to_csv()


if __name__ == "__main__":
    run_template_jobs()
    run_crm_etl_jobs(update_extract=True)
    run_salesloft_etl_jobs()
