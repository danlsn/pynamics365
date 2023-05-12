import os

from dotenv import load_dotenv

import prod_environment
import dev_environment
import uat_environment
from pynamics365.prod_environment import full_extract_to_ddb, full_extract_to_json

from pynamics365.config import TEMPLATE_ENTITIES, CDI_ENTITY_LIST


def extract_all():
    prod_environment.main(entity_list=CDI_ENTITY_LIST, targets=["json"], threaded=True)
    # dev_environment.main()
    uat_environment.main()


def extract_all_to_json():
    load_dotenv()
    # full_extract_to_json(entity_list=None, multi_threaded=True, resource=os.getenv("MSDYN_UAT_RESOURCE"), shuffle=True)
    # full_extract_to_json(entity_list=None, multi_threaded=True, resource=os.getenv("MSDYN_DEV_RESOURCE"), shuffle=True)
    full_extract_to_json(entity_list=None, multi_threaded=True, resource=os.getenv("MSDYN_RESOURCE"), shuffle=True)


def extract_all_to_ddb():
    # load_dotenv()
    full_extract_to_ddb(entity_list=["quote", "quoteclose", "quotedetail"], resource=os.getenv("MSDYN_UAT_RESOURCE"), multi_threaded=True, max_workers=10, shuffle=True)
    # full_extract_to_ddb(entity_list=None, resource=os.getenv("MSDYN_DEV_RESOURCE"), multi_threaded=True, max_workers=4, shuffle=True)
    # full_extract_to_ddb(entity_list=None, resource=os.getenv("MSDYN_RESOURCE"), multi_threaded=True, max_workers=4, shuffle=True)


if __name__ == "__main__":
    # extract_all_to_json()
    extract_all_to_ddb()
