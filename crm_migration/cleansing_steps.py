import datetime
import hashlib
import re
import urllib
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path

import dateutil
import duckdb
import numpy as np
import pandas as pd
import phonenumbers
import pycountry
import logging

from nameparser import HumanName
from phonenumbers import NumberParseException

from crm_migration.dedupe_and_chunk_templates import load_tables_from_sql
from pynamics365.config import DUCKDB_DB_PATH
from pynamics365.extract_to_load_table import get_engine

pd.options.mode.chained_assignment = None

logging.basicConfig(
    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

dfh = logging.FileHandler("../logs/cleansing_steps.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)


email_search_pat = re.compile(
    r"(?P<first_name>[a-z]+)[\._](?P<middle_initial>[a-z](?:[\._]))?(?P<last_name>[a-z\-]+)(?:\d{1,2})?@(?P<domain>.*)"
)


@lru_cache()
def remove_x000d_from_values(value):
    if isinstance(value, str):
        pat = re.compile(r"(_x\w{4}_x\w{4}_)|(_x\w{4}_)")
        output = pat.sub("", value)
        if output != value:
            logger.debug(f"Removed _x000D_ from {value} to {output}")
        return output.strip()
    return value


@lru_cache()
def clean_email_address(value):
    if isinstance(value, str):
        output = value.lower().strip()
        if output != value:
            logger.debug(f"Cleaned email address {value} to {output}")
        return output
    return value


@lru_cache
def get_country_code(country_name):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_2
    except Exception as e:
        logger.error(f"Error getting country code for {country_name}: {e}")
        return None


@lru_cache()
def clean_phone_number(value):
    if isinstance(value, str):
        country_code = None
        try:
            pat = re.compile(r"[^+\d]")
            us_num_pat = re.compile(r"^\(\d{3}\) \d{3}-\d{4}$")
            au_num_us_fmt_pat = re.compile(r"^\(0[23478]\d\) \d{3}-\d{4}$")
            try:
                digits_only = re.sub(pat, "", value)
            except re.error:
                digits_only = ""
            if re.match(us_num_pat, value) and not re.match(au_num_us_fmt_pat, value):
                digits_only = f"+1 {value}"
                country_code = "US"
            elif re.match(au_num_us_fmt_pat, value):
                digits_only = f"+61 {value[1:4]} {value[6:9]} {value[10:]}"
                country_code = "AU"
            elif digits_only.startswith("04") and len(digits_only) == 10:
                digits_only = "+61" + digits_only[1:]
                country_code = "AU"
            elif digits_only.startswith("4") and len(digits_only) == 9:
                digits_only = "+61" + digits_only
                country_code = "AU"
            elif re.match(r"^0[23789]\d{8}$", re.sub(pat, "", value)):
                digits_only = "+61" + digits_only[1:]
                country_code = "AU"
            elif digits_only.startswith("610") and len(digits_only) == 12:
                digits_only = "+61" + digits_only[3:]
                country_code = "AU"
            elif digits_only.startswith("61") and len(digits_only) == 11:
                digits_only = "+61" + digits_only[2:]
                country_code = "AU"
            elif not digits_only.startswith("+"):
                digits_only = "+" + digits_only
            phone_number = phonenumbers.parse(digits_only, country_code)
            if phonenumbers.is_valid_number(
                phone_number
            ) and phonenumbers.is_possible_number(phone_number):
                output = phonenumbers.format_number(
                    phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL
                )
                if output != value:
                    logger.debug(f"Cleaned phone number {value} to {output}")
                return output
            else:
                logger.debug(f"Phone number {value} ({digits_only}) is not valid")
                return value
        except NumberParseException as e:
            logger.debug(f"Error parsing phone number {value}: {e}")
            return value
    return value


def clean_country_state(df_row, **kwargs):

    df_row_before = OrderedDict(df_row)
    df_row = OrderedDict(df_row)
    cols = df_row.keys()
    address1_cols = [col for col in cols if "address1" in col.lower()]
    address2_cols = [col for col in cols if "address2" in col.lower()]
    country = {}
    state = {}
    for col in cols:
        if "_country" in col:
            df_row[col] = clean_country(df_row[col])
            if df_row[col] is not None:
                country[col] = df_row[col]
        elif "_state" in col:
            df_row[col] = clean_state(df_row[col])
            if df_row[col] is not None:
                state[col] = df_row[col]
    if len(country) == 0 and len(state) != 0:
        au_states = get_au_states()
        if "address1_stateorprovince" in cols:
            if df_row["address1_stateorprovince"] in au_states:
                country["address1_country"] = "Australia"
                df_row["address1_country"] = "Australia"
    if df_row_before != df_row:
        logger.debug(
            f"[{kwargs.get('entity_name', '...')}] Cleaned country/state in row from {df_row_before.get('address1_country', None)}/{df_row_before['address1_stateorprovince']} to {df_row.get('address1_country', None)}/{df_row['address1_stateorprovince']}"
        )
    return dict(df_row)


@lru_cache()
def clean_country(x):
    if str(x) == "nan":
        return None
    if isinstance(x, str):
        try:
            country = pycountry.countries.search_fuzzy(x)[0]
        except LookupError:
            logger.debug(f"Country {x} not found")
            return x
        if country.name != x:
            logger.debug(f"Cleaned country {x} to {country.name}")
        return country.name
    else:
        return x


@lru_cache()
def clean_state(x):
    au_states = get_au_states()
    if str(x) == "nan":
        return None
    if isinstance(x, str):
        search_x = x.upper()
        if search_x in au_states:
            result = au_states[search_x]
            if result != x:
                logger.debug(f"Cleaned state {x} to {result}")
            return result
        else:
            return x
    else:
        return x


@lru_cache()
def get_au_states():
    state_lookup = {}
    au_states = pycountry.subdivisions.get(country_code="AU")
    for state in au_states:
        state_code = state.code.replace("AU-", "")
        state_lookup[state_code.upper()] = state_code
        state_lookup[state.name.upper()] = state_code
    return state_lookup


def main():
    au_states = pycountry.subdivisions.get(country_code="AU")
    ...


if __name__ == "__main__":
    main()


@lru_cache()
def clean_website_url(url):
    if str(url) == "nan":
        return None
    if isinstance(url, str):
        split = urllib.parse.urlparse(url)
        clean_url = f"{split.scheme or 'https'}://{split.netloc}{split.path}"
        if url != clean_url:
            logger.debug(f"Cleaned website URL {url} to {clean_url}")
        return clean_url
    else:
        return url


@lru_cache()
def first_last_name_from_email(first_name, last_name, email, **kwargs):
    for name in [first_name, last_name]:
        if name is not None:
            return first_name, last_name
    if email is None:
        return first_name, last_name
    match = email_search_pat.match(email.lower())
    if match is None:
        logger.warning(
            f"[{kwargs.get('entity_name', '...')}] Could not parse first/last name from email {email}"
        )
        return first_name, last_name
    else:
        first_name_match = match.group("first_name")
        middle_initial_match = match.group("middle_initial")
        last_name_match = match.group("last_name")
        if first_name_match:
            first_name = first_name_match.title()
        if last_name_match:
            last_name = last_name_match.title()
        logger.debug(
            f"[{kwargs.get('entity_name', '...')}] Extracted first/last name from email {email} - {first_name} {last_name}."
        )
        return first_name, last_name


@lru_cache()
def convert_nans_to_none(df_row):
    for col in df_row.index:
        if pd.isna(df_row[col]):
            df_row[col] = None
    return df_row


def infer_first_last_name_from_email(df_row, **kwargs):
    check_nulls = ["firstname", "lastname"]
    check_not_null = ["emailaddress1"]
    check_exists = check_nulls + check_not_null
    df_row_cols = df_row.keys()
    all_cols_present = [col for col in check_exists if col in df_row_cols]
    if len(all_cols_present) != len(check_exists):
        return df_row
    email = df_row["emailaddress1"]
    inferred = False
    if (not df_row["firstname"] and not df_row["lastname"]) and email:
        first_name, last_name = first_last_name_from_email(
            df_row["firstname"], df_row["lastname"], df_row["emailaddress1"], **kwargs
        )
        if (first_name and "," in first_name) or (last_name and "," in last_name):
            logger.warning(
                f"[{kwargs.get('entity_name', '...')}] First/last name {first_name} {last_name} contains comma, skipping."
            )
            return df_row
        if not df_row["firstname"]:
            df_row["firstname"] = first_name
            inferred = True
        if not df_row["lastname"]:
            df_row["lastname"] = last_name
            inferred = True
    if inferred:
        logger.debug(
            f"[{kwargs.get('entity_name', '...')}] Inferred first/last name from email {email} - {df_row['firstname']} {df_row['lastname']}."
        )
        ...
    return df_row


def infer_first_last_name_from_email_df(df, **kwargs):
    first_name_nulls_before = len(df[df["firstname"].isnull()])
    last_name_nulls_before = len(df[df["lastname"].isnull()])
    if "emailaddress1" not in df.columns:
        return df
    df_records = df.to_dict(orient="records")
    df_records = [
        infer_first_last_name_from_email(df_row, **kwargs) for df_row in df_records
    ]
    df = pd.DataFrame(df_records)
    first_name_nulls_after = len(df[df["firstname"].isnull()])
    last_name_nulls_after = len(df[df["lastname"].isnull()])
    if first_name_nulls_after < first_name_nulls_before:
        logger.info(
            f"[{kwargs.get('entity_name', '...')}] Inferred first name from email for {first_name_nulls_before - first_name_nulls_after} records."
        )
    if last_name_nulls_after < last_name_nulls_before:
        logger.info(
            f"[{kwargs.get('entity_name', '...')}] Inferred last name from email for {last_name_nulls_before - last_name_nulls_after} records."
        )
    return df


def clean_country_state_df(df, **kwargs):
    if "address1_country" not in df.columns:
        return df
    df_records = df.to_dict(orient="records")
    df_records = [clean_country_state(df_row, **kwargs) for df_row in df_records]
    df = pd.DataFrame(df_records)
    return df


def set_telephone1_to_mobilephone_if_empty_df(df, **kwargs):
    if "telephone1" not in df.columns or "mobilephone" not in df.columns:
        logger.warning(
            f"[{kwargs.get('entity_name', '...')}] Columns not found: telephone1, mobilephone. Returning..."
        )
        return df
    empty_telephone1_before = len(df[df["telephone1"].isnull()])
    logger.info(
        f"[{kwargs.get('entity_name', '...')}] Setting telephone1 to mobilephone if empty for {len(df[df['telephone1'].isnull()])} records."
    )
    logger.debug(
        f"[{kwargs.get('entity_name', '...')}] Expanding df to {len(df[df['telephone1'].isnull()])} records."
    )
    records = df.to_dict(orient="records")
    for record in records:
        if not record["telephone1"] and record["mobilephone"]:
            record["telephone1"] = record["mobilephone"]
    df = pd.DataFrame(records)
    empty_telephone1_after = len(df[df["telephone1"].isnull()])
    if empty_telephone1_after < empty_telephone1_before:
        logger.info(
            f"[{kwargs.get('entity_name', '...')}] Set telephone1 to mobilephone if empty for {empty_telephone1_before - empty_telephone1_after}/{empty_telephone1_before} ({int(((empty_telephone1_before-empty_telephone1_after)/empty_telephone1_before)*100)}%) records. Returning..."
        )
    else:
        logger.info(
            f"[{kwargs.get('entity_name', '...')}] No empty telephone1 set with mobilephone. Returning..."
        )
    return df


def parse_first_and_last_name_from_record(df, **kwargs):
    logger.info(
        f"[{kwargs.get('entity_name', '...')}] Parsing first and last name from first & last name or email."
    )
    first_name_in_last_name_pat = re.compile(
        r"^(?P<last_name>[\w\s\-]+), (?P<first_name>[\w\s\-]+)$"
    )
    name_in_email_pat = re.compile(r"^(?P<username>[A-Za-z'\.\_\-]+)@(?P<domain>.*)$")
    records = df.to_dict(orient="records")
    count_parsed = 0
    for record in records:
        if record["Last Name"] is np.nan or record["Email"] is np.nan:
            continue
        elif ", " in record["Last Name"] and record["First Name"] is np.nan:
            hn = HumanName(record["Last Name"])
            if not hn.unparsable:
                input_name = record["Last Name"]
                record["First Name"] = hn.first
                record["Last Name"] = hn.last
                count_parsed += 1
        elif name_in_email_pat.match(record["Email"]) and (
            record["First Name"] is np.nan or record["Last Name"] is np.nan
        ):
            match = name_in_email_pat.match(record["Email"])
            name_input = ""
            if not match:
                continue
            username = match.group("username")
            split_period = username.split(".")
            if len(split_period) > 1:
                name_input = " ".join(split_period)
            split_underscore = username.split("_")
            if len(split_underscore) > 1:
                name_input = " ".join(split_underscore)
            if name_input:
                hn = HumanName(name_input)
                if not hn.unparsable:
                    hn.capitalize()
                    record["First Name"] = hn.first
                    record["Last Name"] = hn.last
                    count_parsed += 1
                else:
                    logger.debug(
                        f"[{kwargs.get('entity_name', '...')}] Could not parse first and last name from email {record['Email']}, or last name {record['Last Name']}."
                    )
    df_after = pd.DataFrame(records)
    logger.info(
        f"[{kwargs.get('entity_name', '...')}] Parsed first and last name from first & last name or email for {count_parsed}/{len(df)} ({int((count_parsed/len(df))*100)}%) records."
    )
    return df_after


def test_parse_first_last_name_from_record(duckdb_path=DUCKDB_DB_PATH):
    duckdb_path = str(duckdb_path)
    with duckdb.connect(duckdb_path) as con:
        con.execute("SET GLOBAL pandas_analyze_sample=100000")
        df = con.execute("SELECT * FROM lead_L3_Template").df()
        table_name = "lead_L1_Template"
        entity_name = "lead"
        template_level_pat = re.compile(
            r"^(?P<entity_name>\w+)_L(?P<template_level>\d+)_Template"
        )
        match = template_level_pat.match(table_name)
        changed = False
        if not match:
            logger.error(
                f"Could not parse table name {table_name} into entity name and template level"
            )
        entity_name = match.group("entity_name")
        template_level = match.group("template_level")
        template_level_up = int(template_level) + 1
        logger.info(
            f"Re-assigning records owned by inactive users to Analytics @MIP - Done"
        )
        if entity_name == "lead":
            # Lead Source: if WhereScape Web Site Request, then Web
            df = parse_first_and_last_name_from_record(df, entity_name=entity_name)


def swap_next_steps_for_notes_df(df, entity_name):
    logger.info(f"[{entity_name}] Swapping next steps for notes for {len(df)} records.")
    records = df.to_dict(orient="records")
    for record in records:
        if record["statecode_FormattedValue"] == "Open":
            notes = f"[NOTES]:\n{record['mip_notes']}" if record["mip_notes"] else None
            next_steps = (
                f"[NEXT STEPS]:\n{record['description']}"
                if record["description"]
                else None
            )
            description = (
                f"{notes}\n\n{next_steps}"
                if notes and next_steps
                else notes or next_steps
            )
            record["description"] = description
            record["mip_notes"] = next_steps
        else:
            notes = record["mip_notes"]
            next_steps = record["description"]
            record["description"] = f"[NOTES]:\n{notes}" if notes else ""
            record["mip_notes"] = next_steps
        if type(record["description"]) == str:
            record["description"] += (
                f"\n\n[OP TYPE]: {record['mip_optype_FormattedValue']}"
                if record["mip_optype_FormattedValue"]
                else ""
            )
            record["description"] = record["description"].strip()
        elif type(record["description"]) == float:
            record["description"] = (
                f"[OP TYPE]: {record['mip_optype_FormattedValue']}"
                if record["mip_optype_FormattedValue"]
                else ""
            )
        elif record["description"] is None or record["description"] is np.nan:
            record["description"] = (
                f"[OP TYPE]: {record['mip_optype_FormattedValue']}"
                if record["mip_optype_FormattedValue"]
                else ""
            )
        record["description"] = record["description"].strip()
    df = pd.DataFrame(records)
    logger.info(
        f"[{entity_name}] Swapping next steps for notes for {len(df)} records - Done."
    )
    return df


@lru_cache()
def create_hash_key(input: tuple):
    hash_key_input = []
    for arg in input:
        if arg is np.nan:
            hash_key_input.append("")
        elif arg is None:
            hash_key_input.append("")
        else:
            prepared_arg = str(arg).strip().lower()
            hash_key_input.append(prepared_arg)
    hash_key = "".join(hash_key_input)
    hashed = hashlib.md5(hash_key.encode("utf-8")).hexdigest()
    return hashed


@lru_cache()
def parse_modified_on_date(modified_on_date):
    if not modified_on_date:
        return None
    if isinstance(modified_on_date, datetime.datetime):
        return modified_on_date
    if isinstance(modified_on_date, str):
        try:
            modified_on_date = dateutil.parser.parse(modified_on_date)
        except Exception as e:
            logger.error(f"Could not parse modified on date {modified_on_date}")
            return None
        return modified_on_date


def dedupe_hash_table(hash_table):
    count_records_before = sum([len(records) for records in hash_table.values()])
    unique_records = {}
    duplicate_records = {}
    most_recent_records = {}
    original_most_recent_records = {}
    for hash_key, records in hash_table.items():
        if len(records) == 1:
            unique_records[hash_key] = records
        else:
            duplicate_records[hash_key] = records
    for hash_key, records in duplicate_records.items():
        most_recent_record = max(
            records,
            key=lambda x: parse_modified_on_date(x["(Do Not Modify) Modified On"]),
        )
        if hash_key not in original_most_recent_records:
            original_most_recent_records[hash_key] = []
        original_most_recent_records[hash_key].append(most_recent_record)
        # Remove the most recent record from the list of duplicates
        records.remove(most_recent_record)
        other_records = {
            str(parse_modified_on_date(record["(Do Not Modify) Modified On"])): record
            for record in records
            if record != most_recent_record
        }
        for key, value in most_recent_record.items():
            if value is None or value is np.nan:
                for other_record in other_records.values():
                    if (
                        other_record[key] is not None
                        and other_record[key] is not np.nan
                    ):
                        most_recent_record[key] = other_record[key]
                        break
        most_recent_records[hash_key] = most_recent_record
    count_records_check = (
        len(unique_records)
        + sum([len(records) for records in duplicate_records.values()])
        + len(most_recent_records)
    )
    if count_records_before != count_records_check:
        logger.error(
            f"Count of records before deduping ({count_records_before}) does not match count of records after deduping ({count_records_check})."
        )
        ...
    count_records_after = len(unique_records) + len(most_recent_records)
    logger.info(
        f"Deduped {count_records_before} records into {count_records_after} unique records, removing {len(duplicate_records)} duplicate records."
    )
    return (
        unique_records,
        duplicate_records,
        most_recent_records,
        original_most_recent_records,
    )


def dedupe_contacts(df, output_path=None):
    if not output_path:
        output_path = Path("../data/deduped")
    output_path.mkdir(parents=True, exist_ok=True)
    records = df.to_dict(orient="records")
    hash_table = {}
    for record in records:
        hash_key_input = (
            record["First Name"] or "",
            record["Middle Name"] or "",
            record["Last Name"] or "",
            record["Email"] or "",
        )
        hash_key = create_hash_key(hash_key_input)
        if hash_key not in hash_table:
            hash_table[hash_key] = [record]
        else:
            hash_table[hash_key].append(record)
    (
        unique_records,
        duplicate_records,
        most_recent_records,
        original_most_recent_records,
    ) = dedupe_hash_table(hash_table)
    dedupe_dicts = {
        "unique_records": unique_records,
        "duplicate_records": duplicate_records,
        "most_recent_records": most_recent_records,
        "original_most_recent_records": original_most_recent_records,
    }
    for dedupe_type, values in dedupe_dicts.items():
        df_records = []
        for hash_key, records in values.items():
            if type(records) != list:
                records = [records]
            for record in records:
                record["hash_key"] = hash_key
                record["dedupe_type"] = dedupe_type
                df_records.append(record)
        df = pd.DataFrame(df_records)
        df.set_index(["hash_key", "dedupe_type"], inplace=True)
        df.to_csv(output_path / f"contacts_{dedupe_type}.csv", index=True)
    records = list(unique_records.values()) + list(most_recent_records.values())
    df = pd.DataFrame(records)
    return df


def dedupe_table(df, entity_name, output_path=None):
    if not output_path:
        output_path = Path("../data/deduped")
    output_path.mkdir(parents=True, exist_ok=True)
    records = df.to_dict(orient="records")
    hash_table = {}
    for record in records:
        if entity_name == "account":
            hash_key_input = (
                record["Account Name"] or "",
                record["Address 1: State/Province"] or "",
                record["Address 1: Country/Region"] or "",
            )
        elif entity_name == "contact":
            hash_key_input = (
                record["First Name"] or "",
                record["Middle Name"] or "",
                record["Last Name"] or "",
                record["Email"] or "",
            )
        elif entity_name == "lead":
            hash_key_input = (
                record["First Name"] or "",
                record["Last Name"] or "",
                record["Email"] or "",
            )
        else:
            logger.warning(f"De-duplication not implemented for entity {entity_name}")
            return df

        hash_key = create_hash_key(hash_key_input)
        if hash_key not in hash_table:
            hash_table[hash_key] = [record]
        else:
            hash_table[hash_key].append(record)
    (
        unique_records,
        duplicate_records,
        most_recent_records,
        original_most_recent_records,
    ) = dedupe_hash_table(hash_table)
    dedupe_dicts = {
        "unique_records": unique_records,
        "duplicate_records": duplicate_records,
        "most_recent_records": most_recent_records,
        "original_most_recent_records": original_most_recent_records,
    }
    output_records = []
    for hash_key in unique_records:
        output_records.extend(unique_records[hash_key])
    for hash_key in most_recent_records:
        output_records.append(most_recent_records[hash_key])
    for dedupe_type, values in dedupe_dicts.items():
        df_records = []
        for hash_key, records in values.items():
            if type(records) != list:
                records = [records]
            for record in records:
                record["hash_key"] = hash_key
                record["dedupe_type"] = dedupe_type
                df_records.append(record)
        df = pd.DataFrame(df_records)
        df.set_index(["hash_key", "dedupe_type"], inplace=True)
        df.to_csv(output_path / f"{entity_name}_{dedupe_type}.csv", index=True)
    df = pd.DataFrame(output_records)
    return df


def append_id_to_name_df(
    df, source_field, target_field, entity_name, fallback_fields=None
):
    logger.info(
        f"Appending {source_field} to {target_field} field for {entity_name} records"
    )
    records = df.to_dict(orient="records")
    for i, record in enumerate(records):
        source_key = source_field
        if i % 1000 == 0 and i > 0:
            logger.info(
                f"Appending {source_field} to {target_field} field for {entity_name} records - Processed {i} records"
            )
        target_value = record.get(target_field)
        source_value = record.get(source_field)
        if not target_value:
            continue
        elif not source_value and fallback_fields:
            if type(fallback_fields) != list:
                fallback_fields = [fallback_fields]
            for fallback_field in fallback_fields:
                source_key = fallback_field
                source_value = record.get(fallback_field)
                if source_value:
                    break
        if not source_value:
            continue
        if "_value" in source_key:
            source_key = source_key.replace("_value", "")
            source_key = source_key[1:]
            ...
        record[target_field] = f"{target_value} |~{source_key}:{source_value}~|"
        ...
    df = pd.DataFrame(records)
    logger.info(
        f"Appending {source_field} to {target_field} field for {entity_name} records - Done."
    )
    return df


def test_append_id_to_name_df():
    tables = ["quoteclose"]
    for table in tables:
        dfs = load_tables_from_sql(engine=get_engine(), schema="staging", tables={table})
        for entity_name, df in dfs.items():
            if entity_name == "quote":
                # quoteid --> quotenumber
                df = append_id_to_name_df(
                    df,
                    source_field="quoteid",
                    target_field="quotenumber",
                    entity_name=entity_name,
                )
                # opportunity --> name
                df = append_id_to_name_df(
                    df,
                    source_field="_quoteid_value",
                    target_field="name",
                    entity_name=entity_name,
                )
                df = append_id_to_name_df(
                    df,
                    source_field="_opportunity_id_value",
                    target_field="_opportunityid_value_FormattedValue",
                    entity_name=entity_name,
                )
                ...
            elif entity_name == "quoteclose":
                # quoteid --> subject
                df = append_id_to_name_df(
                    df,
                    source_field="_quoteid_value",
                    target_field="subject",
                    entity_name=entity_name,
                )
                ...
            elif entity_name == "quotedetail":
                df = append_id_to_name_df(
                    df,
                    source_field="_quoteid_value",
                    target_field="_quoteid_value_FormattedValue",
                    entity_name=entity_name,
                )
                ...
        ...


if __name__ == "__main__":
    test_append_id_to_name_df()
