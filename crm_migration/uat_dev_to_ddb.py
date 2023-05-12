import os

from pynamics365.prod_environment import full_extract_to_ddb
from dotenv import load_dotenv

load_dotenv()


def main():
    full_extract_to_ddb(entity_list=None, resource=os.getenv("MSDYN_UAT_RESOURCE"), multi_threaded=True, max_workers=10)
    # full_extract_to_ddb(entity_list=None, resource=os.getenv("MSDYN_DEV_RESOURCE"), multi_threaded=True, max_workers=10)


if __name__ == "__main__":
    main()

