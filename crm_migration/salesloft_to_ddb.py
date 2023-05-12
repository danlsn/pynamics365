from migration_etl import step_x_salesloft_extract_to_ddb as salesloft_extract_to_ddb
from migration_etl import step_x_salesloft_full_extract as salesloft_full_extract


def main():
    salesloft_full_extract()
    salesloft_extract_to_ddb()


if __name__ == "__main__":
    main()
