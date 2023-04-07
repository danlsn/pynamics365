import base64
import os
from functools import lru_cache
import emoji
import pyodbc
import requests_cache
import sqlalchemy
import pandas as pd
from dotenv import load_dotenv
from pandas._libs.missing import NAType
from requests import HTTPError
from sqlalchemy import event, Pool
from sqlalchemy.exc import DBAPIError
from unidecode import unidecode
from pynamics365.client import DynamicsClient
from pynamics365.models import DynamicsEntity
import random
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
fh = logging.FileHandler('../logs/sqlalchemy.log')
fh.formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s')
fh.setLevel(logging.INFO)
efh = logging.FileHandler('../logs/sqlalchemy_errors.log')
fh.formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s')
efh.setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.engine').addHandler(efh)
logging.getLogger('sqlalchemy.engine').addHandler(fh)
logging.getLogger('pynamics365.client').setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s')
ch.setLevel(logging.DEBUG)
logging.getLogger(__name__).addHandler(ch)

fh = logging.FileHandler('../logs/pynamics365.log')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
fh.setLevel(logging.INFO)
efh = logging.FileHandler('../logs/pynamics365_errors.log')
efh.setLevel(logging.ERROR)

logger = logging.getLogger('pynamics365.models')
fh = logging.FileHandler('../pynamics365_models.log')
# Set formatter to asctime, levelname, module, funcName, message
fh.formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s')
fh.setLevel(logging.INFO)
efh = logging.FileHandler('../pynamics365_models_errors.log')
fh.formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s')
efh.setLevel(logging.ERROR)
logger.addHandler(efh)
logger.addHandler(fh)

load_dotenv()

host = os.getenv("SQL_HOST")
db = os.getenv("SQL_DB")
user = os.getenv("SQL_USER")
password = os.getenv("SQL_PASSWORD")
conn_str = f"DSN=MIPCRM_Sandbox;UID={user};PWD={password};DATABASE={db};"
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}&charset=utf8", fast_executemany=True,
                                  execution_options={"autocommit": True, "timeout": 30})
# engine = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{password}@{host}/{db}", execution_options={"autocommit": True})
requests_cache.install_cache("dynamics_cache", backend="sqlite", expire_after=3600, match_headers=['Prefer'])


@lru_cache(maxsize=128)
def clean_record_column_names(column_name):
    if "@OData.Community.Display.V1." in column_name:
        return column_name.replace("@OData.Community.Display.V1.", "_")
    elif "@Microsoft.Dynamics.CRM." in column_name:
        return column_name.replace("@Microsoft.Dynamics.CRM.", "_")
    elif "@odata." in column_name:
        return column_name.replace("@odata.", "odata_")
    else:
        return column_name


def entity_to_load_table(dc, logical_name, filters=None, overwrite=False):
    logger.info(f"Loading {logical_name} entity...")
    de = DynamicsEntity(dc, logical_name=logical_name)
    for filter in filters or []:
        try:
            type, value, unit = filter
            de.set_filter(type=type, value=value, unit=unit)
        except KeyError:
            logger.error(f"Filter {filter} is missing a key. Skipping...")
            continue
        except ValueError:
            logger.error(f"Filter {filter} has an invalid value. Skipping...")
            continue
        ...
    logger.info(f"Loaded {logical_name} entity. Getting records...")
    records = [*de.get_records()]
    logger.info(f"Got {len(records)} records for {logical_name} entity.")
    if not records:
        return
    # # Replace @OData.Community.Display.V1. with _ in column names
    # logger.debug(f"Replacing @OData... and @Microsoft... with _ in column names...")
    # for record in records:
    #     for key in [*record]:
    #         new_key = clean_record_column_names(key)
    #         if new_key != key:
    #             record[new_key] = record.pop(key)
    # logger.debug(f"Replaced @OData... and @Microsoft... with _ in column names.")
    logger.debug(f"Converting records to DataFrame...")
    df_records = pd.DataFrame(records)
    # Run every column through encode_unicode to avoid errors
    logger.info(f"Removing emojis from records...")
    for col in df_records.columns:
        df_records[col] = df_records[col].apply(lambda x: encode_unicode(x))
    del records
    logger.debug(f"Converted records to DataFrame. Inferring dtypes...")
    df_records.infer_objects(copy=True)
    attr_dtypes = {}
    dtypes = {de.entity_definition["PrimaryIdAttribute"]: sqlalchemy.types.VARCHAR(36),
              de.entity_definition["PrimaryNameAttribute"]: sqlalchemy.types.VARCHAR(500)}
    df_cols = [*df_records.columns]
    # Replace @OData.Community.Display.V1. with _ in column names
    logger.debug(f"Replacing @OData... and @Microsoft... with _ in column names...")
    for col in df_cols:
        new_col = clean_record_column_names(col)
        if new_col != col:
            df_records.rename(columns={col: new_col}, inplace=True)
    logger.debug(f"Replaced @OData... and @Microsoft... with _ in column names.")
    for attr in de.attributes:
        col_name = attr["LogicalName"]
        format = attr.get("Format")
        if col_name not in df_cols:
            col_name = f"_{attr['LogicalName']}_value"
            if col_name not in df_cols:
                continue
        if format == "DateAndTime":
            attr_dtypes[col_name] = "datetime64[ns]"
            dtypes[col_name] = sqlalchemy.types.DATETIME()
        elif format == "DateOnly":
            attr_dtypes[col_name] = "datetime64[ns]"
            dtypes[col_name] = sqlalchemy.types.DATETIME()
        elif attr["AttributeType"] == "Decimal":
            attr_dtypes[col_name] = "float64"
            scale = attr["Precision"]
            precision = len(str(attr["MaxValue"])) + scale + 2
            dtypes[col_name] = sqlalchemy.types.DECIMAL(precision=precision, scale=scale)
        elif attr["AttributeType"] == "Integer":
            attr_dtypes[col_name] = "int64"
            dtypes[col_name] = sqlalchemy.types.BIGINT()
        elif attr["AttributeType"] == "BigInt":
            attr_dtypes[col_name] = "int64"
            dtypes[col_name] = sqlalchemy.types.BIGINT()
        elif attr["AttributeType"] == "String":
            attr_dtypes[col_name] = "string"
            database_length = attr.get("DatabaseLength")
            max_length = attr.get("MaxLength")
            if database_length == -1 or max_length == -1:
                dtypes[col_name] = sqlalchemy.types.Text()
                dtypes[col_name] = sqlalchemy.types.NVARCHAR()
            else:
                if database_length > 4000:
                    dtypes[col_name] = sqlalchemy.types.NVARCHAR()
                else:
                    dtypes[col_name] = sqlalchemy.types.VARCHAR(round(database_length) or round(max_length))
        elif attr["AttributeType"] == "Uniqueidentifier":
            attr_dtypes[col_name] = sqlalchemy.types.VARCHAR(36)
        elif attr["AttributeType"] == "Boolean":
            attr_dtypes[col_name] = "boolean"
        elif attr["AttributeType"] == "Double":
            attr_dtypes[col_name] = "float64"
            scale = attr["Precision"]
            precision = len(str(attr["MaxValue"])) + scale
            dtypes[col_name] = sqlalchemy.types.DECIMAL(precision=precision, scale=scale)
        elif attr["AttributeType"] == "Lookup":
            attr_dtypes[col_name] = sqlalchemy.types.VARCHAR(36)
        elif attr["AttributeType"] == "Money":
            attr_dtypes[col_name] = "float64"
            scale = attr["Precision"]
            precision = len(str(attr["MaxValue"])) + scale
            dtypes[col_name] = sqlalchemy.types.DECIMAL(precision=precision, scale=scale)
    logger.debug(f"Inferred dtypes. Converting dtypes...")
    for key in attr_dtypes:
        try:
            df_records[key] = df_records[key].astype(attr_dtypes[key], errors="ignore")
        except KeyError:
            pass
        except TypeError:
            pass
    logger.debug(f"Converted dtypes. Converting dtypes for remaining columns...")
    for col in df_records.columns:
        if "_value" in col:
            df_records[col] = df_records[col].astype("string")
        elif "odata_etag" in col:
            dtypes[col] = sqlalchemy.types.VARCHAR(100)
        elif "_FormattedValue" in col:
            dtypes[col] = sqlalchemy.types.VARCHAR(2000)
        elif "_lookuplogicalname" in col:
            dtypes[col] = sqlalchemy.types.VARCHAR(200)
        elif "_associatednavigationproperty" in col:
            dtypes[col] = sqlalchemy.types.VARCHAR(200)
        elif df_records[col].dtype == "object":
            df_records[col] = df_records[col].astype("string")
            dtypes[col] = sqlalchemy.types.NVARCHAR()

    # dtypes[col] = sqlalchemy.types.VARCHAR("MAX")
    # logger.debug(f"Converting dtype_backend to pyarrow...")
    # df_records.convert_dtypes(dtype_backend="pyarrow")
    # Change data type of PrimaryIdAttribute to varchar(36)
    for k, v in attr_dtypes.items():
        if isinstance(v, sqlalchemy.types.VARCHAR):
            dtypes[k] = v
    with engine.connect() as conn:
        logger.debug(f"Created connection to {engine.url}")
        logger.info(f"Loading {de.endpoint} to load.{de.endpoint}")
        logger.info(f"Loading existing records from load.{de.endpoint}")
        dtypes[de.entity_definition["PrimaryIdAttribute"]] = sqlalchemy.types.VARCHAR(36)
        try:
            logger.debug(
                f"Trying to set index to: {de.entity_definition['PrimaryIdAttribute']}, {de.entity_definition['PrimaryNameAttribute']}")
            index_cols = [de.entity_definition["PrimaryIdAttribute"], de.entity_definition['PrimaryNameAttribute'], "odata_etag"]
            for col in index_cols:
                if col not in df_records.columns:
                    index_cols.remove(col)
            df_records.set_index(
                index_cols,
                inplace=True)
        except KeyError:
            logger.error(
                f"Could not set index to: {de.entity_definition['PrimaryIdAttribute']}, {de.entity_definition['PrimaryNameAttribute']}")
            logger.debug(f"Trying to set index to: {de.entity_definition['PrimaryIdAttribute']}")
            df_records.set_index([de.entity_definition["PrimaryIdAttribute"]], inplace=True)
        # Check if table exists
        logger.debug(f"Checking if table load.{de.endpoint} exists...")
        if not engine.dialect.has_table(conn, de.endpoint, schema="load") or overwrite:
            logger.info(f"Table load.{de.endpoint} does not exist.")
            try:
                df_sample = df_records[:100].copy()
                df_sample.to_sql(de.endpoint, conn, schema="load", dtype=dtypes, if_exists="replace",
                                 index=True)
                conn.commit()
            except KeyboardInterrupt:
                return None
            logger.info(f"Created table load.{de.endpoint}")
            logger.debug(f"Going to commit changes to {de.endpoint}. Length: {len(df_records)}")
            conn.commit()
            logger.info(f"Finished loading {de.endpoint} to load.{de.endpoint}")
        df_existing = pd.read_sql_table(de.endpoint, conn, schema="load")
        try:
            df_existing.set_index(df_existing.index.names, inplace=True)
        except KeyError as e:
            if "odata_etag" in str(e):
                df_existing.set_index([de.entity_definition["PrimaryIdAttribute"], "odata_etag"], inplace=True)
            else:
                df_existing.set_index([de.entity_definition["PrimaryIdAttribute"]], inplace=True)
        logger.info(f"Loaded existing records from load.{de.endpoint}")
        logger.debug(f"Trying to drop existing records from df_records. Length before: {len(df_records)}")
        df_records = df_records[~df_records.index.isin(df_existing.index)]
        logger.debug(f"Dropped existing records from df_records. Length after: {len(df_records)}")
        if len(df_records) == 0:
            logger.info(f"No new records to load to {de.endpoint}")
            conn.rollback()
            return None
        try:
            if_exists_action = "append" if df_existing is not None else "replace"
            df_records.to_sql(de.endpoint, conn, schema="load", dtype=dtypes,
                              if_exists=if_exists_action,
                             index=True)
        except KeyboardInterrupt as e:
            logging.error("Exiting...")
            conn.commit()
            conn.rollback()
            raise e
        except pyodbc.DataError as e:
            logging.error(e)
            raise e
        except DBAPIError as e:
            logging.error(e)
            raise e
        finally:
            conn.commit()
            # logger.debug(f"{conn.info}")
            logger.info(f"Finished loading {de.endpoint} to load.{de.endpoint}")
            conn.rollback()
            conn.close()


@lru_cache()
def encode_unicode(value):

    if isinstance(value, str):
        # value = value.encode('utf-8', 'backslashreplace')
        # if len(value) % 2 != 0:
        #     value = value + b'\x00'
        # return str(value)
        # Encode as base64
        # value = base64.b64encode(value.encode('utf-8')).decode('utf-8')
        value = emoji.demojize(value)
        value = unidecode(value)
        return value
    elif isinstance(value, NAType):
        return value


def main(sort_first=False, overwrite=False):
    done = ['cdi_unsubscribe', 'cdi_subscriptionlist', 'cdi_subscriptionpreference', 'list', 'listmember',
            'emailsearch', 'solutioncomponent', 'annotation', 'cdi_subscriptionpreference',
            'postregarding', 'subject', 'import',
            'importlog', 'importfile', 'importentitymapping', 'cdi_executesend', 'cdi_import', 'cdi_importlog',
            'mip_bpf_d9b760ab65c146968995d39213e67cea', 'mip_bpf_3b7c34d35eb9488db89bb8ddbfa9ef95',
            'mip_bpf_aedd48254ef740ba9d97534092bb8248',
            'cdi_pageview', 'cdi_emailsend_contact', 'cdi_emailsend_lead', 'cdi_emailevent', 'cdi_eventparticipation',
            'cdi_sentemail', 'adobe_agreement', 'adobe_agreementdocument', 'adobe_datamap',
            'adobe_datamapreverse', 'adobe_recipient', 'campaignactivityitem', 'campaignitem', 'category',
            'cdi_emailsend_list', 'cdi_emailsend']
    entities = ['lead', 'systemuser', 'campaign', 'pricelevel', 'productpricelevel', 'lead', 'opportunity',
                'opportunityproduct', 'quote', 'quoteclose', 'quotedetail', 'product', 'uom', 'uomschedule',
                'opportunityclose', 'email', 'account', 'campaign', 'contact', 'cdi_usersession', 'cdi_excludedemail',
                'mip_history', 'activitypointer', 'leadtoopportunitysalesprocess', 'connection', 'columnmapping',
                'task', 'phonecall', 'principalentitymap', 'displaystringmap', 'salesorder', 'salesorderdetail',
                'attributemap', 'lookupmapping', 'picklistmapping', 'entitymap', 'mailbox', 'appointment',
                'activityparty', "cdi_emailsend_suppressed_list", "cdi_import", "campaignitem", "campaignactivity"]

    entity_set = set([*entities, *done])
    entities = []
    if sort_first:
        dc = DynamicsClient()
        for entity in [*entity_set][:15]:
            try:
                de = DynamicsEntity(dc, entity)
            except HTTPError as e:
                logger.error(e)
                continue
            if de.entity_definition is not None:
                entities.append(de)
        # Sort entities by de.record_count
        entities.sort(key=lambda x: x.record_count, reverse=False)
        for entity in entities:
            dc = DynamicsClient()
            dc.refresh_token()
            try:
                entity_to_load_table(dc, entity.logical_name, overwrite=overwrite)
            except Exception as e:
                logging.error(e)
                continue
    else:
        entities = [e for e in entity_set]
        # Sort alphabetically
        entities.sort(reverse=False)
        # Randomise order
        random.shuffle(entities)
        for entity in entities:
            dc = DynamicsClient()
            dc.refresh_token()
            for i in range(0, 11)[::-2]:
                try:
                    # entity_to_load_table(dc, entity, filter={'value': 365, 'unit': "days"}, overwrite=overwrite)
                    entity_to_load_table(dc, entity, filters=[('before', i * 365, 'days')], overwrite=overwrite)
                except Exception as e:
                    logging.error(e)
                    continue


if __name__ == "__main__":
    main(sort_first=True, overwrite=False)
