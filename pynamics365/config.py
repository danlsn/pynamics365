import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ENTITY_LIST = [
    "msdyn_connector",
    "connector",
    "environmentvariabledefinition",
    "environmentvariablevalue",
    "contract",
    "contractdetail",
    "product",
    "productassociation",
    "productpricelevel",
    "productsubstitute",
    "stringmap",
    "dynamicproperty",
    "dynamicpropertyoptionsetitem",
    "statusmap",
    "salesorder",
    "salesorderdetail",
    "invoice",
    "invoicedetail",
    "license",
    "territory",
    "theme",
    "equipment",
    "service",
    "resource",
    "calendar",
    "calendarrule",
    "connectionrole",
    "connectionroleassociation",
    "connectionroleobjecttypecode",
    "displaystringmap",
    "displaystring",
    "task",
    "list",
    "listmember",
    "importdata",
    "columnmapping",
    "picklistmapping",
    "lookupmapping",
    "ownermapping",
    "account",
    "contact",
    "lead",
    "opportunity",
    "campaign",
    "opportunityclose",
    "opportunityproduct",
    "productpricelevel",
    "systemuser",
    "pricelevel",
    "productpricelevel",
    "product",
    "quote",
    "quoteclose",
    "quotedetail",
    "uom",
    "uomschedule",
    "email",
    "recordcountsnapshot",
    "list",
    "listmember",
    "campaignactivity",
    "campaignactivityitem",
    "campaignitem",
    "campaignresponse",
]

CDI_ENTITY_LIST = [
    "cdi_anonymousvisitor",
    "cdi_automation",
    "cdi_bulktxtmessage",
    "cdi_bulktxtmessage_list",
    "cdi_category",
    "cdi_cdi_datasync_list",
    "cdi_datasync",
    "cdi_domain",
    "cdi_emailcname",
    "cdi_emailevent",
    "cdi_emailsend",
    "cdi_emailsend_account",
    "cdi_emailsend_cdi_webcontent",
    "cdi_emailsend_contact",
    "cdi_emailsend_lead",
    "cdi_emailsend_list",
    "cdi_emailsend_suppressed_list",
    "cdi_emailstatistics",
    "cdi_emailtemplate",
    "cdi_event",
    "cdi_event_cdi_automation",
    "cdi_eventparticipation",
    "cdi_excludedemail",
    "cdi_executesend",
    "cdi_executesocialpost",
    "cdi_filter",
    "cdi_formcapture",
    "cdi_formcapturefield",
    "cdi_formfield",
    "cdi_import",
    "cdi_importlog",
    "cdi_iporganization",
    "cdi_lead_product",
    "cdi_nurturebuilder",
    "cdi_nurturebuilder_list",
    "cdi_optionmapping",
    "cdi_pageview",
    "cdi_postedfield",
    "cdi_postedform",
    "cdi_postedsubscription",
    "cdi_postedsurvey",
    "cdi_profile",
    "cdi_quicksendprivilege",
    "cdi_runnurture",
    "cdi_scoremodel",
    "cdi_securitysession",
    "cdi_sendemail",
    "cdi_sentemail",
    "cdi_setting",
    "cdi_socialclick",
    "cdi_socialpost",
    "cdi_subscriptionlist",
    "cdi_subscriptionpreference",
    "cdi_surveyanswer",
    "cdi_surveyquestion",
    "cdi_transactionalemail",
    "cdi_txtmessage",
    "cdi_unsubscribe",
    "cdi_unsubscribe_cdi_subscriptionlist",
    "cdi_usersession",
    "cdi_visit",
    "cdi_visitorscorepermodel",
    "cdi_webcontent",
]

TEMPLATE_ENTITIES = [
    "account",
    "campaign",
    "contact",
    "lead",
    "opportunity",
    "opportunityclose",
    "opportunityproduct",
    "productpricelevel",
    "pricelevel",
    "product",
    "quote",
    "quotedetail",
    "quoteclose",
    "uom",
    "uomschedule",
    "systemuser",
]

ADOBE_ENTITIES = [
    "adobe_adobe_agreementtemplate_systemuser",
    "adobe_agreement",
    "adobe_agreementdocument",
    "adobe_agreementmappingtemplate",
    "adobe_agreementtemplate",
    "adobe_datamap",
    "adobe_datamappingattachment",
    "adobe_datamapreverse",
    "adobe_integrationfeatures",
    "adobe_integrationsettings",
    "adobe_migratedrecord",
    "adobe_recipient",
    "adobe_templatedocument",
    "adobe_templaterecipient",
    "adobe_transactionperformance",
    "adobe_workflow_activity",
]

MIP_ENTITIES = [
    "mip_bpf_3b7c34d35eb9488db89bb8ddbfa9ef95",
    "mip_bpf_aedd48254ef740ba9d97534092bb8248",
    "mip_bpf_d9b760ab65c146968995d39213e67cea",
    "mip_discountsettings",
    "mip_edm",
    "mip_exchangerate",
    "mip_history",
    "mip_license",
    "mip_meeting",
    "mip_mip_technology_account",
    "mip_opportunity_contributor",
    "mip_opportunity_presalesrep",
    "mip_schedulejob",
    "mip_technology",
    "mip_timesheet",
    "mip_timesheetdetail",
    "mip_trainingcourse",
    "mip_triallicense",
    "mip_zendesk",
]

PROD_EXTRACT_PATH = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\org30a87_crm5_dynamics_com"
)

PROD_DEFS_PATH = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\org30a87_crm5_dynamics_com\_Definitions\EntityDefinitions.csv"
)

UAT_DEFS_PATH = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\mipau_crm6_dynamics_com\_Definitions\EntityDefinitions.csv"
)

DEV_DEFS_PATH = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\mipcrm-extract\mipdev_crm6_dynamics_com\_Definitions\EntityDefinitions.csv"
)
DDB_PATH = Path("../data/ddb")

PROD_DDB_PATH = Path(
    r"C:\Users\DanielLawson\OneDrive - MIP (Aust) Pty Ltd\Documents\Projects\MIP-CRM-Migration\data\ddb"
)

CONN_STR = f'DSN=MIPCRM_Sandbox;UID={os.getenv("SQL_USER")};PWD={os.getenv("SQL_PASSWORD")};DATABASE={os.getenv("SQL_DB")};'


DUCKDB_DB_PATH = Path(r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\duckdb.db")
SALESLOFT_DUCKDB_DB_PATH = Path(r"C:\Users\DanielLawson\IdeaProjects\pynamics365\data\salesloft_duck.db")
