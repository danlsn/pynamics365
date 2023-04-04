from pynamics365.auth import DynamicsAuth
from pynamics365.client import DynamicsClient
from pynamics365.models import DynamicsEntity
from dotenv import load_dotenv

load_dotenv()


def main():
    da = DynamicsAuth()
    da.authenticate()
    dc = DynamicsClient(auth=da)
    ...


if __name__ == "__main__":
    main()
