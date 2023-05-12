import dictdatabase as DDB
import json
from pathlib import Path


def get_validated_address_json_files(extract_dir=None):
    if not extract_dir:
        extract_dir = Path(
            r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract\customeraddress-validated"
        )
    for file in extract_dir.rglob("*.json"):
        with open(file, "r") as f:
            yield json.load(f)


def get_records_from_json_files(json_files):
    for file in json_files:
        for record in file["value"]:
            yield record


def sort_records_by_parent_entity(records):
    entities = {}
    for record in records:
        parent_entity = record.get(
            "_parentid_value@Microsoft.Dynamics.CRM.lookuplogicalname", None
        )
        parent_id = record.get("_parentid_value", None)
        if not parent_entity:
            continue
        if parent_entity not in entities:
            entities[parent_entity] = {}
        if parent_id not in entities[parent_entity]:
            entities[parent_entity][parent_id] = record
        elif entities[parent_entity][parent_id]["createdon"] < record["createdon"]:
            entities[parent_entity][parent_id] = record
    return entities


def output_grouped_records_to_json(records):
    for entity, entity_records in records.items():
        records = []
        for record in entity_records.values():
            records.append(record)
        output_path = Path(
            f"../data/validated_addresses/{entity}_validated_addresses.json"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(records, f, indent=4, sort_keys=True)


if __name__ == "__main__":
    files = list(get_validated_address_json_files())
    files += list(
        get_validated_address_json_files(
            Path(
                r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract\customeraddress_validated"
            )
        )
    )
    files += list(
        get_validated_address_json_files(
            Path(
                r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract\leadaddress-validated"
            )
        )
    )
    files += list(
        get_validated_address_json_files(
            Path(
                r"D:\DANLSN-TDS\IdeaProjects\MIP-CRM-Migration\data\mipcrm-extract\leadaddress_validated"
            )
        )
    )

    records = sort_records_by_parent_entity(get_records_from_json_files(files))
    output_grouped_records_to_json(records)
    ...
