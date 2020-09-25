import os
import sys
import pandas as pd
import tempfile
import boto3
import csv
import snowflake.connector

from .misc_utils import get_logger

# from .misc_utils import initialize_env
# from .settings import S3_BUCKET_NAME
# from .settings import S3_PREFIX_KEY
# from .settings import get_db_name

logger = get_logger(__name__)

from misc_utils import initialize_env
from settings import S3_BUCKET_NAME
from settings import S3_PREFIX_KEY


def _get_connection(auto_commit=False):
    initialize_env()
    conn = snowflake.connector.connect(user=os.environ['DATA_ETL_SNOWFLAKE_USER'],
                                       password=os.environ['DATA_ETL_SNOWFLAKE_PW'],
                                       account=os.environ['DATA_ETL_SNOWFLAKE_ACCOUNT'])
    conn.autocommit = auto_commit
    cursor = conn.cursor()
    return conn, cursor


def write_s3_to_redshift(s3_bucket, s3_key, db_schema, table, delimiter, filter_text_to_delete=None,
                         additional_copy_options=''):
    copy_conn, copy_cursor = _get_connection(True)
    try:
        if filter_text_to_delete:
            sql = "delete from {} where {};".format(table, filter_text_to_delete)
        else:
            # sql = "delete from {};".format(table)
            sql = "truncate table if exists {};".format(table)
        logger.info("Deleting existing data using sql \n{}".format(sql))
        copy_cursor.execute(sql)
        logger.info("Loading data from s3: {}://{} to redshift : {}".format(s3_bucket, s3_key, table))
        sql = "copy into {} from s3://{}/{} credentials=(aws_key_id='{}' aws_secret_key='{}') file_format=(type=csv " \
              "field_delimiter='{}') ".format(table, s3_bucket, s3_key, os.environ['DATA_ETL_AWS_ACCESS_KEY_ID'],
                                              os.environ['DATA_ETL_AWS_SECRET_KEY_ID'], delimiter)
        sql += additional_copy_options
        copy_cursor.execute(sql)
        copy_cursor.close()
        copy_conn.close()
    except:
        e = sys.exc_info()
        logger.error("Error occurred while copying the metrics to redshift... {} ".format(e))
        raise


def write_redshift_to_s3(s3_bucket, s3_key, query, delimiter, header=False, single_file=False,
                         additional_copy_options=''):
    copy_conn, copy_cursor = _get_connection(True)

    sql = "copy into s3://{}/{} from {} credentials=(aws_key_id='{}' aws_secret_key='{}') " \
          "file_format = (type=CSV COMPRESSION = NONE FIELD_DELIMITER ='{}' " \
          "NULL_IF=()) OVERWRITE=TRUE ".format(s3_bucket, s3_key, query, os.environ['DATA_ETL_AWS_ACCESS_KEY_ID'],
                                               os.environ['DATA_ETL_AWS_SECRET_KEY_ID'], delimiter)
    if header:
        sql += "HEADER={} ".format(header)
    if single_file:
        sql += "SINGLE={} ".format(single_file)
    sql += additional_copy_options
    sql += ';'
    try:
        copy_cursor.execute(sql)
        copy_cursor.close()
        copy_conn.close()
    except:
        e = sys.exc_info()
        logger.error("Error occurred while copying the data from redshift to s3... {} ".format(e))
        raise


def get_first_col_from_db(sql):
    conn, cursor = _get_connection()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    res = [r[0] for r in rows]
    return res


def get_single_value(sql):
    conn, cursor = _get_connection()
    cursor.execute(sql)
    rows = cursor.fetchall()
    if rows:
        return rows[0][0]
    else:
        return None


def execute_queries(db_statements):
    conn, cursor = _get_connection()
    cursor.execute('begin')
    try:
        for statement in db_statements:
            cursor.execute(statement)
        cursor.execute('commit')
    except:
        logger.error("Exception occurred, rolling back the transaction {}".format(sys.exc_info()[0]))
        cursor.execute('rollback')
        raise
    finally:
        cursor.close()
        conn.close()


def get_rows(sql, return_as_pandas_df=False, return_as_array_dicts=False):
    if return_as_pandas_df and return_as_array_dicts:
        raise Exception("Both return_as_pandas_df and return_as_array_dicts cannot be true")
    conn, cursor = _get_connection()
    cursor.execute(sql)
    rows = cursor.fetchall()
    col_names = [x[0] for x in cursor.description]
    rows = list(rows)
    cursor.close()

    conn.close()
    if return_as_pandas_df:
        df = pd.DataFrame(rows, columns=col_names)
        return df
    elif return_as_array_dicts:
        row_hashes = []
        for r in rows:
            r_h = {}
            for col_pos, col in enumerate(r):
                r_h[col_names[col_pos]] = col
            row_hashes.append(r_h)
        return row_hashes
    else:
        return rows


def get_all_rows(table, return_as_pandas_df=False):
    return get_rows("SELECT * FROM {}".format(table), return_as_pandas_df=return_as_pandas_df)


def write_rows_to_redshift(rows, delimiter, data_name, db_schema, table, filter_text_to_delete=None,
                           additional_copy_options=''):
    with tempfile.NamedTemporaryFile() as temp:
        fn = temp.name
        with open(fn, "w") as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerows(rows)
        initialize_env()
        logger.info("Loading data to s3")
        session = boto3.Session(aws_access_key_id=os.environ['DATA_ETL_AWS_ACCESS_KEY_ID'],
                                aws_secret_access_key=os.environ['DATA_ETL_AWS_SECRET_KEY_ID'])
        s3 = session.resource('s3')
        s3_bucket = S3_BUCKET_NAME
        s3_key = '{}/temp/{}.csv'.format(S3_PREFIX_KEY, data_name)
        object = s3.Object(s3_bucket, s3_key)
        object.upload_file(fn)
        write_s3_to_redshift(s3_bucket, s3_key, db_schema, table, delimiter, filter_text_to_delete,
                             additional_copy_options=additional_copy_options)
        object.delete()


def get_delete_where_conditions(table, stage_table, cols, condition):
    """
		Below is the sample query from aws to delete from stage table in redshift merge feature.
		This function generates the where condition for delete query.
		Refer this link for more details https://docs.aws.amazon.com/redshift/latest/dg/merge-examples.html
		delete from stage_table
			using main_table
			where main_table.id = stage_table.id
			and stage_table.is_active = 'value1;
		input:
			table: main_table
			stage_table: stage_table
			cols: [{'col_name': 'id', 'condition': '=', 'value'=None},
					{'col_name': 'is_active', 'condition': '=', 'value'='value1'}]
			condition: 'and'
		output: main_table.id = stage_table.id
			and stage_table.is_active = 'value1;
		"""

    condition = " %s " % condition
    where_def = condition.join(
        map(lambda x: table + "." + x['col_name'] + x['condition'] + stage_table + "." + x['col_name']
        if x['value'] is None else stage_table + "." + x['col_name'] + x['condition'] + str(x['value']), cols))
    return "%s" % where_def


def get_upsert_update_cols(cols):
    """
        Below is the sample query from aws to update main table from from stage table in redshift merge feature.
        This function generates the update column statements for update query
        Refer this link for more details about merge feature
        https://docs.aws.amazon.com/redshift/latest/dg/merge-examples.html
        update main_table
        set col1 = B.col1, col2 = 'value1'
        from stage_table B
        where main_table.id = B.id
        and B.is_active = 'value2';
        input:
            cols: [{'col_name': 'id', 'condition': '=', 'value'=None},
                    {'col_name': 'is_active', 'condition': '=', 'value'='value2'}]
        output:
            col1 = B.col1, col2 = 'value1'
    """
    return ", ".join(map(lambda x: x['name'] + '=' + 'B' + '.' + x['name']
    if x['value'] is None else x['name'] + '=' + str(x['value']), cols))


def get_update_where_conditions(table, cols, condition):
    """
        Below is the sample query from aws to update main table from from stage table in redshift merge feture.
        This function generates the where condition for update query
        Refer this link for more details about merge feature
        https://docs.aws.amazon.com/redshift/latest/dg/merge-examples.html
        update main_table
        set col1 = B.col1, col2 = 'value1'
        from stage_table B
        where main_table.id = B.id
        and B.is_active = 'value2';
        input:
            table: main_table
            cols: [{'col_name': 'id', 'condition': '=', 'value'=None},
                    {'col_name': 'is_active', 'condition': '=', 'value'='value2'}]
            condition: 'and'
        output:
            main_table.id = B.id and B.is_active = 'value2'
    """
    condition = " %s " % condition
    where_def = condition.join(
        map(lambda x: table + "." + x['col_name'] + x['condition'] + 'B' + '.' + x['col_name']
        if x['value'] is None else table + "." + x['col_name'] + x['condition'] + str(x['value']), cols))
    return "%s" % where_def


def merge_unload(data, delimiter, file_name, table, stage_table, upserts_dml_statements, unload_query,
                 unload_bucket_name, unload_key_name, unload_status_query='', additional_copy_options='',
                 additional_unload_options=''):
    """
    Refer this link for more details about redshift merge feature.
    https://docs.aws.amazon.com/redshift/latest/dg/merge-examples.html
    This function handles merge and unload to s3 in one single transaction.
    It creates the temp stage table identical to main table.
    Copy data from memory to s3 as a file and load the file is s3 using Redshfit copy command.
    Apply merge queries to update / insert into main table. The callee function has to pass the merge queries.
    Also callee function has to pass unload query and the query needs to be run to mark the rows being copied to s3.
    """
    try:

        with tempfile.NamedTemporaryFile() as temp:
            fn = temp.name
            with open(fn, "w") as f:
                writer = csv.writer(f, delimiter=delimiter)
                writer.writerows(data)

            logger.info("Loading data to s3")
            session = boto3.Session(aws_access_key_id=os.environ['DATA_ETL_AWS_ACCESS_KEY_ID'],
                                    aws_secret_access_key=os.environ['DATA_ETL_AWS_SECRET_KEY_ID'])
            s3 = session.resource('s3')
            s3_bucket_load = S3_BUCKET_NAME
            s3_key_load = '{}/temp/{}.csv'.format(S3_PREFIX_KEY, file_name)
            object = s3.Object(s3_bucket_load, s3_key_load)
            object.upload_file(fn)
            upsert_statements = []
            create_stage_table_sql = "create temp table %s like %s;" % (stage_table, table)

            sql_copy = "copy into {} from s3://{}/{} credentials=(aws_key_id='{}' aws_secret_key='{}') " \
                       "file_format=(type=csv " \
                       "field_delimiter='{}') ".format(stage_table, s3_bucket_load, s3_key_load,
                                                       os.environ['DATA_ETL_AWS_ACCESS_KEY_ID'],
                                                       os.environ['DATA_ETL_AWS_SECRET_KEY_ID'], delimiter)
            sql_copy += additional_copy_options

            sql_unload = "copy into s3://{}/{} from {} credentials=(aws_key_id='{}' aws_secret_key='{}') " \
                         "file_format = (type=CSV COMPRESSION = NONE FIELD_DELIMITER ='{}' NULL_IF=()) " \
                         "OVERWRITE=TRUE ".format(unload_bucket_name, unload_key_name, unload_query,
                                                  os.environ['DATA_ETL_AWS_ACCESS_KEY_ID'],
                                                  os.environ['DATA_ETL_AWS_SECRET_KEY_ID'], delimiter)
            sql_unload += additional_unload_options
            sql_unload += ';'

            upsert_statements.extend([create_stage_table_sql, sql_copy, ])
            upsert_statements.extend(upserts_dml_statements)
            upsert_statements.append(sql_unload)
            if unload_status_query is not None:
                upsert_statements.append(unload_status_query)
            execute_queries(upsert_statements)
            object.delete()
    except:
        e = sys.exc_info()
        logger.error("Error occurred in load_extract function ".format(e))
        raise
