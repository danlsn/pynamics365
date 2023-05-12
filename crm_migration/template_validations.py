import json
import logging

import numpy as np

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s"))
ch.setLevel(logging.INFO)
logger.addHandler(ch)

dfh = logging.FileHandler("../logs/template_validations.log")
dfh.setLevel(logging.DEBUG)
dfh.setFormatter(
    logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(funcName)s\t%(message)s")
)
logger.addHandler(dfh)


def check_all_columns_are_present(entity_validations, template_df, validation_df):
    template_name = entity_validations["template_name"]
    entity_name = entity_validations["entity_name"]

    # Check if all columns are present in display_name column
    template_columns = set(template_df.columns)
    validation_columns = [col for col in validation_df["display_name"] if "(Do Not Modify)" not in col]
    if not template_columns.issubset(validation_columns):
        logger.warning(f"[{entity_name}] Columns missing from {template_name} template: {template_columns.difference(validation_columns)}")
    else:
        logger.info(f"[{entity_name}] All columns present in {template_name} template.")
        entity_validations["all_columns_present"] = True
    return entity_validations


def check_required_fields_are_present(entity_validations, template_df, validation_df):
    template_name = entity_validations["template_name"]
    entity_name = entity_validations["entity_name"]
    required_fields = validation_df[validation_df["required"] == True]["display_name"].tolist()
    entity_validations["num_records_missing_required_fields"] = {}
    entity_validations["records_missing_required_fields"] = {}

    for field in required_fields:
        df_missing_required_fields = template_df[template_df[field].isna()]
        if not df_missing_required_fields.empty:
            entity_validations["records_missing_required_fields"][field] = df_missing_required_fields.to_dict("records")
            entity_validations["num_records_missing_required_fields"][field] = len(df_missing_required_fields)
            logger.warning(f"[{entity_name}] {len(df_missing_required_fields)} Records missing required field: {field}")
        else:
            logger.info(f"[{entity_name}] No records missing required field: {field}")
            entity_validations["records_missing_required_fields"][field] = []
    return entity_validations


def check_value_is_in_option_set(entity_validations, template_df, validation_df):
    template_name = entity_validations["template_name"]
    entity_name = entity_validations["entity_name"]
    entity_validations["records_with_invalid_option_set_values"] = {}
    try:
        option_set_validations = validation_df[validation_df["option_set"].notna()].to_dict("records")
    except KeyError:
        logger.info(f"No option set validations for {template_name} template.")
        return entity_validations
    entity_validations["num_records_with_invalid_option_set_values"] = {}
    entity_validations["records_with_invalid_option_set_values"] = {}
    for field in option_set_validations:
        field_name = field["display_name"]
        option_set = field["option_set"]
        if field["required"] != True:
            option_set.append(np.nan)
        df_invalid_option_set_values = template_df[~template_df[field_name].isin(option_set)]
        if not df_invalid_option_set_values.empty:
            entity_validations["records_with_invalid_option_set_values"][field_name] = df_invalid_option_set_values.to_dict("records")
            entity_validations["num_records_with_invalid_option_set_values"][field_name] = len(df_invalid_option_set_values)
            logger.warning(f"[{entity_name}] {len(df_invalid_option_set_values)} Records with invalid option set value for field: {field_name}")
        else:
            logger.info(f"[{entity_name}] No records with invalid option set value for field: {field_name}")
            entity_validations["records_with_invalid_option_set_values"][field_name] = []
        if "option_set_values" not in entity_validations:
            entity_validations["option_set_values"] = {}
        # Remove nan from option set
        option_set = [x for x in option_set if isinstance(x, str)]
        entity_validations["option_set_values"][field_name] = json.dumps(option_set)
    return entity_validations

