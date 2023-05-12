import re
from pathlib import Path
import json
import pandas as pd

extract_path = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\org30a87_crm5_dynamics_com\current\json-5k\org30a87_crm5_dynamics_com"
)


def get_entity_attributes_table():
    ...


# Get list of directories in extract_path
extract_entities = [x for x in extract_path.iterdir() if x.is_dir()]
extract_entity_names = [x.name for x in extract_entities]

sample_grouping = {
    "odata.etag": {"odata.etag": 'W/"225442334"'},
    "industrycode": {"industrycode": 913240008,
                     "industrycode_FormattedValue": 'Services'},
    "ownerid": {"_ownerid_value": '634622c6-b725-e811-8186-e0071b6927c1',
                "_ownerid_value_lookuplogicalname": 'systemuser',
                "_ownerid_value_FormattedValue": 'Analytics @MIP',
                "_ownerid_value_associatednavigationproperty": 'ownerid'},
    "donotemail": {"donotemail": False},
}
for entity in extract_entities:
    extract_files = [x for x in entity.iterdir() if x.is_file()]
    entity_records = []
    for extract_file in extract_files:
        with open(extract_file, "r") as f:
            content = json.load(f)
            entity_records.extend(content['value'])
    fields = [*entity_records[0].keys()]
    keys = {}
    for field in fields:
        if '@' in field:
            key, suffix = field.split("@", 2)
            if key == '' and suffix != '':
                key = suffix
                suffix = ''
            suffix = suffix.split(".")[-1]
        else:
            key = field
            suffix = ''
        if key not in keys:
            keys[key] = {}
        if key != '' and suffix != '':
            keys[key][suffix] = f"{key}_{suffix}"
        elif key != '' and suffix == '':
            keys[key]["_"] = f"{key}"
        elif key == '' and suffix != '':
            keys[key]["_"] = f"{suffix}"
        else:
            raise ValueError(f"key and suffix are both empty: {field}")
        ...
    ...
...
