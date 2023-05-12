import json
import urllib
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd

from migration_etl import (
    step_x_top_level_template_tables as get_top_level_template_tables,
)

import logging

logging.basicConfig(
    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


dfh = logging.FileHandler("infer_company_name.log")
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
dfh.setLevel(logging.DEBUG)
logger.addHandler(dfh)


def get_template_tables_dfs():
    logger.info("Getting top level template tables")
    top_level_templates_dfs = get_top_level_template_tables()
    logger.info(f"Got top level template tables: {top_level_templates_dfs.keys()}")
    return top_level_templates_dfs


def get_accounts_contacts_leads_dfs():
    logger.info("Getting top level template tables for accounts, contacts, and leads")
    template_dfs = get_template_tables_dfs()
    template_dfs = {
        k: v for k, v in template_dfs.items() if k in ["account", "contact", "lead"]
    }
    logger.info(f"Got top level template tables for accounts, contacts, and leads")
    return template_dfs


def get_account_cols_for_infer_company_name(df):
    logger.info("Getting account cols to infer company name")
    cols_to_keep = [
        "Account Name",
        "Primary Contact",
        "Website",
        "Parent Account",
        "Originating Lead",
        "Email",
        "Industry",
        "Address 1: State/Province",
        "Address 1: Country/Region",
    ]
    df = df[cols_to_keep]
    logger.info("Got account cols to infer company name")
    return df


def get_contact_cols_for_infer_company_name(df):
    logger.info("Getting contact cols to infer company name")
    cols_to_keep = [
        "First Name",
        "Middle Name",
        "Last Name",
        "Company Name",
        "Email",
        "Address 1: State/Province",
        "Address 1: Country/Region",
        "Website",
    ]
    df = df[cols_to_keep]
    logger.info("Got contact cols to infer company name")
    return df


def get_lead_cols_for_infer_company_name(df):
    logger.info("Getting lead cols to infer company name")
    cols_to_keep = [
        "First Name",
        "Last Name",
        "Company Name",
        "Email",
        "Industry",
        "State/Province",
        "Country/Region",
        "Website",
    ]
    df = df[cols_to_keep]
    logger.info("Got lead cols to infer company name")
    return df


def keep_cols_for_infer_company_name(dfs):
    logger.info("Keeping cols for infer company name")
    if "account" in dfs:
        dfs["account"]["df"] = get_account_cols_for_infer_company_name(
            dfs["account"]["df"]
        )
    if "contact" in dfs:
        dfs["contact"]["df"] = get_contact_cols_for_infer_company_name(
            dfs["contact"]["df"]
        )
    if "lead" in dfs:
        dfs["lead"]["df"] = get_lead_cols_for_infer_company_name(dfs["lead"]["df"])
    logger.info("Kept cols for infer company name")
    return dfs


@lru_cache
def parse_domain_from_email(email):
    logger.debug(f"Parsing domain from email: {email}")
    if type(email) != str:
        return email
    if "@" not in email:
        return email
    parts = email.split("@")
    domain = parts[1]
    return domain


@lru_cache
def parse_domain_from_website(website):
    logger.debug(f"Parsing domain from website: {website}")
    if type(website) != str:
        return website
    parts = urllib.parse.urlparse(website)
    domain = parts.netloc.replace("www.", "")
    return domain


def get_domain_from_email_df(df):
    logger.info("Getting domain from email df")
    if "Email" not in df.columns:
        return df
    df["Email Domain"] = df["Email"].apply(lambda x: parse_domain_from_email(x))
    logger.info("Got domain from email df")
    return df


def get_domain_from_website_df(df):
    logger.info("Getting domain from website df")
    if "Website" not in df.columns:
        return df
    df["Website Domain"] = df["Website"].apply(lambda x: parse_domain_from_website(x))
    logger.info("Got domain from website df")
    return df


def map_email_domains_to_company_names(dfs):
    logger.info("Mapping email domains to company names")
    for key, value in dfs.items():
        logger.info(f"Mapping email domains to company names for {key}")
        df = value["df"].copy()
        if "Email Domain" not in value["df"].columns:
            continue
        if "Account Name" in value["df"].columns:
            df.rename(columns={"Account Name": "Company Name"}, inplace=True)
            company_name_key = "Company Name"
        elif "Company Name" in value["df"].columns:
            company_name_key = "Company Name"
        else:
            continue
        email_domains = value["df"]["Email Domain"].unique()
        # Drop email domains that are null
        email_domains = [x for x in email_domains if type(x) == str]
        value["email_domains_to_company_names"] = {}
        email_domains_to_company_names = {k: {} for k in email_domains}
        df = df[[company_name_key, "Email Domain"]]
        # Drop rows where company name is null
        df = df[~df[company_name_key].isna()]
        df = df[df[company_name_key] != ""]
        df = df[df[company_name_key].notnull()]
        for email_domain, value in email_domains_to_company_names.items():
            logger.debug(f"Mapping email domain {email_domain} to company names")
            company_names = df[df["Email Domain"] == email_domain][company_name_key]
            value["_count_email_domains"] = 0
            if "company_names" not in value:
                value["company_names"] = {}
            for company_name in company_names:
                if company_name not in value["company_names"]:
                    value["company_names"][company_name] = {"count": 0}
                value["company_names"][company_name]["count"] += 1
                value["_count_email_domains"] += 1
            value["_count_company_names"] = len(value["company_names"])
            # Order keys in company_names by count
            value["company_names"] = dict(
                sorted(
                    value["company_names"].items(),
                    key=lambda item: item[1]["count"],
                    reverse=True,
                )
            )
        dfs[key]["email_domains_to_company_names"] = email_domains_to_company_names
        logger.info(f"Mapped email domains to company names for {key}")
    return dfs


def map_website_domains_to_company_names(dfs):
    logger.info("Mapping website domains to company names")
    for key, value in dfs.items():
        logger.info(f"Mapping website domains to company names for {key}")
        df = value["df"].copy()
        if "Website Domain" not in value["df"].columns:
            continue
        if "Account Name" in value["df"].columns:
            df.rename(columns={"Account Name": "Company Name"}, inplace=True)
            company_name_key = "Company Name"
        elif "Company Name" in value["df"].columns:
            company_name_key = "Company Name"
        else:
            continue
        website_domains = value["df"]["Website Domain"].unique()
        # Drop website domains that are null
        website_domains = [x for x in website_domains if type(x) == str]
        value["website_domains_to_company_names"] = {}
        website_domains_to_company_names = {k: {} for k in website_domains}
        df = df[[company_name_key, "Website Domain"]]
        # Drop rows where company name is null
        df = df[~df[company_name_key].isna()]
        df = df[df[company_name_key] != ""]
        df = df[df[company_name_key].notnull()]
        for website_domain, value in website_domains_to_company_names.items():
            company_names = df[df["Website Domain"] == website_domain][company_name_key]
            value["_count_website_domains"] = 0
            if "company_names" not in value:
                value["company_names"] = {}
            for company_name in company_names:
                if company_name not in value["company_names"]:
                    value["company_names"][company_name] = {"count": 0}
                value["company_names"][company_name]["count"] += 1
                value["_count_website_domains"] += 1
            value["_count_company_names"] = len(value["company_names"])
            # Order keys in company_names by count
            value["company_names"] = dict(
                sorted(
                    value["company_names"].items(),
                    key=lambda item: item[1]["count"],
                    reverse=True,
                )
            )
        dfs[key]["website_domains_to_company_names"] = website_domains_to_company_names
        logger.info(f"Mapped website domains to company names for {key}")
    return dfs
    ...


def map_company_names_to_email_domains(dfs):
    logger.info("Mapping company names to email domains")
    for key, value in dfs.items():
        logger.info(f"Mapping company names to email domains for {key}")
        df = value["df"].copy()
        if "Email Domain" not in value["df"].columns:
            continue
        if "Account Name" in value["df"].columns:
            df.rename(columns={"Account Name": "Company Name"}, inplace=True)
            company_name_key = "Company Name"
        elif "Company Name" in value["df"].columns:
            company_name_key = "Company Name"
        else:
            continue
        company_names = df[company_name_key].unique()
        # Drop company names that are null
        company_names = [x for x in company_names if type(x) == str]
        value["company_names_to_email_domains"] = {}
        company_names_to_email_domains = {k: {} for k in sorted(company_names)}
        df = df[[company_name_key, "Email Domain"]]
        # Drop rows where email domain is null
        df = df[~df["Email Domain"].isna()]
        df = df[df["Email Domain"] != ""]
        df = df[df["Email Domain"].notnull()]
        for company_name, value in company_names_to_email_domains.items():
            email_domains = df[df[company_name_key] == company_name]["Email Domain"]
            value["_count_email_domains"] = 0
            if "email_domains" not in value:
                value["email_domains"] = {}
            for email_domain in email_domains:
                if email_domain not in value["email_domains"]:
                    value["email_domains"][email_domain] = {"count": 0}
                value["email_domains"][email_domain]["count"] += 1
                value["_count_email_domains"] += 1
            value["_count_email_domains"] = len(value["email_domains"])
            # Order keys in email_domains by count
            value["email_domains"] = dict(
                sorted(
                    value["email_domains"].items(),
                    key=lambda item: item[1]["count"],
                    reverse=True,
                )
            )
        dfs[key]["company_names_to_email_domains"] = company_names_to_email_domains
        logger.info(f"Mapped company names to email domains for {key}")
    return dfs


def map_company_names_to_website_domains(dfs):
    logger.info("Mapping company names to website domains")
    for key, value in dfs.items():
        logger.info(f"Mapping company names to website domains for {key}")
        df = value["df"].copy()
        if "Website Domain" not in value["df"].columns:
            continue
        if "Account Name" in value["df"].columns:
            df.rename(columns={"Account Name": "Company Name"}, inplace=True)
            company_name_key = "Company Name"
        elif "Company Name" in value["df"].columns:
            company_name_key = "Company Name"
        else:
            continue
        company_names = df[company_name_key].unique()
        # Drop company names that are null
        company_names = [x for x in company_names if type(x) == str]
        value["company_names_to_website_domains"] = {}
        company_names_to_website_domains = {k: {} for k in sorted(company_names)}
        df = df[[company_name_key, "Website Domain"]]
        # Drop rows where website domain is null
        df = df[~df["Website Domain"].isna()]
        df = df[df["Website Domain"] != ""]
        df = df[df["Website Domain"].notnull()]
        for company_name, value in company_names_to_website_domains.items():
            website_domains = df[df[company_name_key] == company_name]["Website Domain"]
            value["_count_website_domains"] = 0
            if "website_domains" not in value:
                value["website_domains"] = {}
            for website_domain in website_domains:
                if website_domain not in value["website_domains"]:
                    value["website_domains"][website_domain] = {"count": 0}
                value["website_domains"][website_domain]["count"] += 1
                value["_count_website_domains"] += 1
            value["_count_website_domains"] = len(value["website_domains"])
            # Order keys in email_domains by count
            value["website_domains"] = dict(
                sorted(
                    value["website_domains"].items(),
                    key=lambda item: item[1]["count"],
                    reverse=True,
                )
            )
        dfs[key]["company_names_to_website_domains"] = company_names_to_website_domains
        logger.info(f"Mapped company names to website domains for {key}")
    return dfs


def merge_map_dictionaries(target_dict, source_dict, key_type=None):
    source_keys = source_dict.keys()
    target_keys = target_dict.keys()
    merge_key = None
    for key, value in source_dict.items():
        if "_count_email_domains" in value.keys():
            del value["_count_email_domains"]
        if "_count_website_domains" in value.keys():
            del value["_count_website_domains"]
        if "_count_company_names" in value.keys():
            del value["_count_company_names"]
        if key not in target_keys:
            target_dict[key] = source_dict[key]
        else:
            if "company_names" in value.keys():
                merge_key = "company_names"
            elif "email_domains" in value.keys():
                merge_key = "email_domains"
            elif "website_domains" in value.keys():
                merge_key = "website_domains"
            if merge_key is None:
                continue
            for k, v in value[merge_key].items():
                if k not in target_dict[key][merge_key]:
                    target_dict[key][merge_key][k] = v
                else:
                    target_dict[key][merge_key][k]["count"] += v["count"]
    if merge_key in ["company_names", "email_domains", "website_domains"]:
        drop_keys_without_records = []
        for key, value in target_dict.items():
            del_keys = []
            for value_key in value.keys():
                if value_key not in [
                    "_count_records",
                    merge_key,
                    f"_count_{merge_key}",
                ]:
                    del_keys.append(value_key)
            if del_keys is not None:
                for del_key in del_keys:
                    del value[del_key]
            try:
                value[f"_count_{merge_key}"] = len(value[merge_key])
            except KeyError as e:
                logger.error(f"{e}")
                continue
            try:
                merge_key_values = value[merge_key].values()
                value[f"_count_records"] = sum([x["count"] for x in merge_key_values])
                if value[f"_count_records"] == 0:
                    drop_keys_without_records.append(key)
            except KeyError as e:
                logger.error(f"{e}")
                continue
            try:
                value[merge_key] = dict(
                    sorted(
                        value[merge_key].items(),
                        key=lambda item: item[1]["count"],
                        reverse=True,
                    )
                )
            except KeyError as e:
                logger.error(f"{e} - Key: {key}, Merge Key: {merge_key}")
                continue
        if drop_keys_without_records is not None:
            for key in drop_keys_without_records:
                del target_dict[key]
    return target_dict


def parse_json_mapping_to_csv(json_mapping):
    json_path = Path(json_mapping)
    mapping_name = json_path.stem
    lookup_key = None
    value_key = None
    if mapping_name.startswith("company_names"):
        lookup_key = "company_names"
    elif mapping_name.startswith("email_domains"):
        lookup_key = "email_domains"
    elif mapping_name.startswith("website_domains"):
        lookup_key = "website_domains"
    if mapping_name.endswith("company_names"):
        value_key = "company_names"
    elif mapping_name.endswith("email_domains"):
        value_key = "email_domains"
    elif mapping_name.endswith("website_domains"):
        value_key = "website_domains"
    if lookup_key is None or value_key is None:
        logger.error(
            f"Could not parse lookup_key and value_key from mapping_name: {mapping_name}"
        )
        return
    records = []
    with open(json_mapping, "r") as f:
        mapping = json.load(f)
    for key, value in mapping.items():
        if value_key not in value.keys():
            continue
        for k, v in value[value_key].items():
            records.append(
                {
                    lookup_key: key,
                    f"{lookup_key}_count_of_{value_key}": value[f"_count_{value_key}"],
                    f"{lookup_key}_count_records": value["_count_records"],
                    value_key: k,
                    "count_records": v["count"],
                    "pct_of_records": v["count"] / value["_count_records"],
                }
            )
    df = pd.DataFrame(records)
    df.sort_values(by=[lookup_key, "count_records"], inplace=True)
    csv_file_path = json_mapping.with_suffix(".csv")
    df.to_csv(csv_file_path)


def parse_account_domain_mapping_to_csv(account_domain_mapping="../data/domains_to_company_names/_account_domains_to_company_names.json"):
    json_path = Path(account_domain_mapping)
    mapping_name = json_path.stem
    lookup_key = "email_domains"
    value_key = "company_names"
    records = []
    with open(json_path, "r") as f:
        mapping = json.load(f)["email_domains_to_company_names"]
    for key, value in mapping.items():
        if value_key not in value.keys():
            continue
        for k, v in value[value_key].items():
            records.append(
                {
                    lookup_key: key,
                    f"{lookup_key}_count_of_{value_key}": value[f"_count_{value_key}"],
                    f"{lookup_key}_count_records": value["_count_records"],
                    value_key: k,
                    "count_records": v["count"],
                    "pct_of_records": v["count"] / value["_count_records"],
                }
            )
    df = pd.DataFrame(records)
    df = df[df["email_domains"] != ""]
    df.sort_values(by=[lookup_key, "count_records"], inplace=True)
    csv_file_path = json_path.parent / f"{mapping_name}_email_lookup.csv"
    df.to_csv(csv_file_path)


def drop_email_domain_mapping_for_missing_companies():
    email_to_company_names = pd.read_csv("../data/domains_to_company_names/email_domains_to_company_names.csv")
    current_accounts = set()
    template_accounts = pd.read_csv("../data/cleaned/account_L4_Template.csv")
    current_accounts.update(template_accounts["Account Name"].values)
    email_to_company_names = email_to_company_names[email_to_company_names["company_names"].isin(current_accounts)]
    # Group email_to_company_names by email_domains, get max pct_of_records
    email_to_company_names_top = email_to_company_names.groupby(["email_domains"]).agg({"pct_of_records": "max"}).reset_index()
    # Merge email_to_company_names_top with email_to_company_names to get the company_names
    email_to_company_names_top = email_to_company_names_top.merge(email_to_company_names, on=["email_domains", "pct_of_records"], how="left")
    ...


def main():
    logger.info("Getting accounts, contacts, and leads dataframes")
    dfs = get_accounts_contacts_leads_dfs()
    dfs = keep_cols_for_infer_company_name(dfs)
    for key, value in dfs.items():
        if "Website" in value["df"].columns:
            dfs[key]["df"] = get_domain_from_website_df(value["df"])
        if "Email" in value["df"].columns:
            dfs[key]["df"] = get_domain_from_email_df(value["df"])
    dfs = map_email_domains_to_company_names(dfs)
    dfs = map_website_domains_to_company_names(dfs)
    dfs = map_company_names_to_email_domains(dfs)
    dfs = map_company_names_to_website_domains(dfs)
    email_domains_to_company_names = {}
    website_domains_to_company_names = {}
    company_names_to_email_domains = {}
    company_names_to_website_domains = {}
    for entity_name, value in dfs.items():
        if email_domains_to_company_names == {}:
            email_domains_to_company_names = (
                value["email_domains_to_company_names"]
                if "email_domains_to_company_names" in value
                else {}
            )
        else:
            email_domains_to_company_names = (
                merge_map_dictionaries(
                    email_domains_to_company_names,
                    value["email_domains_to_company_names"],
                )
                if "email_domains_to_company_names" in value
                else {}
            )
        if website_domains_to_company_names == {}:
            website_domains_to_company_names = (
                value["website_domains_to_company_names"]
                if "website_domains_to_company_names" in value
                else {}
            )
        else:
            website_domains_to_company_names = (
                merge_map_dictionaries(
                    website_domains_to_company_names,
                    value["website_domains_to_company_names"],
                )
                if "website_domains_to_company_names" in value
                else {}
            )
        if company_names_to_email_domains == {}:
            company_names_to_email_domains = (
                value["company_names_to_email_domains"]
                if "company_names_to_email_domains" in value
                else {}
            )
        else:
            company_names_to_email_domains = (
                merge_map_dictionaries(
                    company_names_to_email_domains,
                    value["company_names_to_email_domains"],
                )
                if "company_names_to_email_domains" in value
                else {}
            )
        if company_names_to_website_domains == {}:
            company_names_to_website_domains = (
                value["company_names_to_website_domains"]
                if "company_names_to_website_domains" in value
                else {}
            )
        else:
            company_names_to_website_domains = (
                merge_map_dictionaries(
                    company_names_to_website_domains,
                    value["company_names_to_website_domains"],
                )
                if "company_names_to_website_domains" in value
                else {}
            )
    for mapping_dict_name, mapping_dict in {
        "company_names_to_email_domains": company_names_to_email_domains,
        "company_names_to_website_domains": company_names_to_website_domains,
        "email_domains_to_company_names": email_domains_to_company_names,
        "website_domains_to_company_names": website_domains_to_company_names,
    }.items():
        with open(
            f"../data/domains_to_company_names/{mapping_dict_name}.json", "w"
        ) as f:
            json.dump(
                mapping_dict,
                f,
                indent=4,
                sort_keys=True,
                default=str,
            )

    for entity_name, values in dfs.items():
        output = {}
        for key, value in values.items():
            if key == "df":
                output["records"] = value.to_dict(orient="records")
            output[key] = value
        with open(
            f"../data/domains_to_company_names/_{entity_name}_domains_to_company_names.json",
            "w",
        ) as f:
            json.dump(
                output,
                f,
                indent=4,
                sort_keys=True,
                default=str,
            )
    ...


if __name__ == "__main__":
    drop_email_domain_mapping_for_missing_companies()
    # parse_account_domain_mapping_to_csv()
    # for json_mapping in Path("../data/domains_to_company_names").glob("*.json"):
    #     parse_json_mapping_to_csv(
    #         json_mapping=json_mapping,
    #     )
    # main()
