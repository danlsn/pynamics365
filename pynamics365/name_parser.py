import json

import pandas as pd
import sqlalchemy
from nameparser import HumanName
from nameparser.config import CONSTANTS
from config import CONN_STR
# CONSTANTS.force_mixed_case_capitalization = True

name = HumanName(first="daniel", middle="lee", last="lawson")
name.capitalize()

engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={CONN_STR}&charset=utf8", fast_executemany=True,
                                  execution_options={"autocommit": True, "timeout": 30})
with engine.connect() as conn:
    contacts_df = pd.read_sql_table("contact", conn, schema="load", dtype_backend="pyarrow")


contacts_json = r"C:\Users\DanielLawson\IdeaProjects\MIP-CRM-Migration\data\database-dump\contact.json"
leads_json = r"C:\Users\DanielLawson\IdeaProjects\MIP-CRM-Migration\data\database-dump\lead.json"
with open(contacts_json, "rb") as f:
    contacts = json.load(f)

with open(leads_json, "rb") as f:
    leads = json.load(f)


for lead in leads:
    parent_contact = lead["_parentcontactid_value_FormattedValue"]
    if parent_contact is None:
        continue
    hn = HumanName(parent_contact)
    hn.capitalize()
    if parent_contact == hn.full_name:
        continue
    ...



changed_list = []
for contact in contacts:
    if contact.get("_originatingleadid_value_FormattedValue", None) is None:
        continue
    hn = HumanName(contact["yomifullname"])
    hn.capitalize()
    old = {
        "fullname": contact["yomifullname"],
        "firstname": contact["firstname"] or "",
        "middlename": contact["middlename"] or "",
        "lastname": contact["lastname"] or ""
    }
    new = {
        "fullname": hn.full_name,
        "firstname": hn.first,
        "middlename": hn.middle,
        "lastname": hn.last
    }
    changed = False
    if old["firstname"] != new["firstname"]:
        changed = True
    elif old["middlename"] != new["middlename"]:
        changed = True
    elif old["lastname"] != new["lastname"]:
        changed = True
    else:
        changed = False
    if changed is True:
        changed_list.append({
            "old": old,
            "new": new,
            "contact": contact
        })

...
