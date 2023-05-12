import csv
from pathlib import Path
import json
import re

import sqlalchemy
import pandas as pd

extract_path = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\current\json-5k\org30a87_crm5_dynamics_com")

# Get list of directories in extract_path
extract_entities = [x for x in extract_path.iterdir() if x.is_dir()]
extract_entity_names = [x.name for x in extract_entities]
extracts = {}
for name, extract in zip(extract_entity_names, extract_entities):
    extracts[name] = extract


def get_defs_attrs(defs_path, attrs_path):
    definitions_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\_Definitions\Definitions")
    entity_definitions = [x for x in definitions_path.glob("*.csv") if "DSDU-" not in x.name]
    pat = re.compile(r"^(?P<entity_name>[a-zA-Z0-9_]+)-Definitions\.csv")
    entity_names_w_definitions = [re.match(pat, x.name).group("entity_name") for x in entity_definitions]

    definitions = {}
    for defn in entity_definitions:
        entity_name = re.match(pat, defn.name).group("entity_name")
        definitions[entity_name] = pd.read_csv(defn)

    attribute_definitions_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\_Definitions\Attributes")
    attribute_definitions = [x for x in attribute_definitions_path.glob("*.csv") if "DSDU-" not in x.name]
    pat = re.compile(r"^(?P<entity_name>[a-zA-Z0-9_]+)-Attributes\.csv")
    entity_names_w_attributes = [re.match(pat, x.name).group("entity_name") for x in attribute_definitions]

    attributes = {}
    for attr_def in attribute_definitions:
        entity_name = re.match(pat, attr_def.name).group("entity_name")
        attributes[entity_name] = pd.read_csv(attr_def)
    entities_w_extracts_and_attributes = set(extract_entity_names).intersection(entity_names_w_attributes)


def method_name():
    global extracts
    json_extract_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\jsonl\org30a87_crm5_dynamics_com")
    extracts = {}
    for entity in json_extract_path.iterdir():
        entity_name = entity.stem
        attribute_definitions_path = Path(
            r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\_Definitions\Attributes")
        with open(attribute_definitions_path / f"{entity_name}-Attributes.csv", "r") as f:
            attributes = [*csv.DictReader(f)]
        records = []
        with open(entity, "r") as f:
            for line in f:
                records.append(json.loads(line))
        df = pd.DataFrame(records)
        # Sort by versionnumber descending, get first row for each accountid
        primary_id = [x["LogicalName"] for x in attributes if x["IsPrimaryId"] == "True"][0]
        df = df.sort_values("versionnumber", ascending=False).drop_duplicates(primary_id)
        df_keys = df.keys()
        keys_not_in_attrs = set(df_keys).difference([x["LogicalName"] for x in attributes])
        keys_in_attrs = set(df_keys).intersection([x["LogicalName"] for x in attributes])
        keys_in_attrs_not_in_df = set([x["LogicalName"] for x in attributes]).difference(df_keys)
        for key in keys_in_attrs_not_in_df:
            lookup_value = f"_{key}_value"
            formatted_value = f"{key}_FormattedValue"
            lookup_logical_name = f"{key}_value_lookuplogicalname"
            associated_navigation_property = f"{key}_value_associatednavigationproperty"
            if lookup_value in df_keys:
                # Add to keys_in_attrs
                keys_in_attrs.add(key)
                keys_in_attrs.add(lookup_value)
            if formatted_value in df_keys:
                keys_in_attrs.add(key)
                keys_in_attrs.add(formatted_value)
            if lookup_logical_name in df_keys:
                keys_in_attrs.add(key)
                keys_in_attrs.add(lookup_logical_name)
            if associated_navigation_property in df_keys:
                keys_in_attrs.add(key)
                keys_in_attrs.add(associated_navigation_property)
        df_keys_not_in_attrs = set(df_keys).difference(keys_in_attrs)
        ...


def match_keys_to_attrs(entity_name="account"):
    jsonl_extract = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\jsonl\org30a87_crm5_dynamics_com\account.jsonl")
    attribute_definitions_path = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\_Definitions\Attributes\account-Attributes.csv")
    entity_definition = Path(
        r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\_Definitions\Definitions\account-Definitions.csv")
    with open(attribute_definitions_path, "r") as f:
        attributes = [*csv.DictReader(f)]
    with open(entity_definition, "r") as f:
        entity_def = [*csv.DictReader(f)]
    records = []
    with open(jsonl_extract, "r") as f:
        for line in f:
            records.append(json.loads(line))
    df = pd.DataFrame(records)
    df_parsed_attributes = set()
    lookup_value_pat = re.compile(r"^_?(?P<key>.+)_value$")
    formatted_value_pat = re.compile(r"^_?(?P<key>.+)_FormattedValue$")
    lookup_logical_name_pat = re.compile(r"^_?(?P<key>.+)_value_lookuplogicalname$")
    associated_navigation_property_pat = re.compile(r"^_?(?P<key>.+)_value_associatednavigationpropertyname$")
    lookup_name_pat = re.compile(r"^(?P<key>.+)name$")
    df_keys = df.keys()
    for key in df.keys():
        if lookup_value_pat.match(key):
            df_parsed_attributes.add(lookup_value_pat.match(key).group("key"))
            df_parsed_attributes.add(lookup_value_pat.match(key).group("key") + "name")
        elif formatted_value_pat.match(key):
            df_parsed_attributes.add(formatted_value_pat.match(key).group("key"))
        elif lookup_logical_name_pat.match(key):
            df_parsed_attributes.add(lookup_logical_name_pat.match(key).group("key"))
        elif associated_navigation_property_pat.match(key):
            df_parsed_attributes.add(associated_navigation_property_pat.match(key).group("key"))
        elif lookup_name_pat.match(key):
            df_parsed_attributes.add(lookup_name_pat.match(key).group("key"))
        else:
            df_parsed_attributes.add(key)
        # df_parsed_attributes.add(key)
    attrs_not_in_df = set([x["LogicalName"] for x in attributes if x["IsValidODataAttribute"] != 'False']).difference(df_parsed_attributes)
    valid_odata_attrs = [x for x in attributes if x["IsValidODataAttribute"] != 'False']
    ...


if __name__ == "__main__":
    # main()
    # method_name()
    match_keys_to_attrs()
