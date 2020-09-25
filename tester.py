from db_utils import _get_connection, get_all_rows, merge_unload

conn, cur = _get_connection()
cur.execute("SELECT current_version()")
one_row = cur.fetchone()
print(one_row)

db_schema = None
# write_s3_to_redshift('red2snow', 'csvs4snow/tmp1.csv', db_schema, 'RED2SNOW.PUBLIC.TESTTABLE', ',')
# write_s3_to_redshift('red2snow', 'csvs4snow', db_schema, 'RED2SNOW.PUBLIC.DEMO', ',', "PARID='PARID'")
# write_s3_to_redshift('red2snow', 'csvs4snow/tmp1.csv', db_schema, 'RED2SNOW.PUBLIC.TESTTABLE1', ',')


query = 'RED2SNOW.PUBLIC.DEMO'
# write_redshift_to_s3('red2snow', 'csvs4snow/tmp.csv', query, ',')
# write_redshift_to_s3('red2snow', 'csvs4snow/tmp.csv', query, ',', False, True)


# rst = get_first_col_from_db("select * from RED2SNOW.PUBLIC.DEMO")
# rst = get_single_value("select * from RED2SNOW.PUBLIC.DEMO")
# print(rst)


# qry_set = [
#     "delete from RED2SNOW.PUBLIC.DEMO where PROPERTYHOUSENUM='951';",
#     "delete from RED2SNOW.PUBLIC.DEMO where PROPERTYHOUSENUM='912';"
# ]
# execute_queries(qry_set)


# rst = get_rows("select * from RED2SNOW.PUBLIC.DEMO")
# rst = get_rows("select * from RED2SNOW.PUBLIC.DEMO", return_as_pandas_df=True)
# rst = get_rows("select * from RED2SNOW.PUBLIC.DEMO", return_as_array_dicts=True)


# rst = get_all_rows("RED2SNOW.PUBLIC.TESTTABLE2")
# rst = get_all_rows("RED2SNOW.PUBLIC.DEMO", return_as_pandas_df=True)
# print(rst)


# data_name = 'testing123'
# rst = get_all_rows("RED2SNOW.PUBLIC.TESTTABLE2")
# write_rows_to_redshift(rst, ',', data_name, db_schema, 'RED2SNOW.PUBLIC.TESTTABLE1')


# """table: main_table
#    stage_table: stage_table
#    cols: [{'col_name': 'id', 'condition': '=', 'value'=None},
# 		    {'col_name': 'is_active', 'condition': '=', 'value'='value1'}]
#    condition: 'and'"""
# cols = [{'col_name': 'PROPERTYHOUSENUM', 'condition': '=', 'value':None},
#         {'col_name': 'SCHOOLCODE', 'condition': '=', 'value':9}]
# rst = get_delete_where_conditions('RED2SNOW.PUBLIC.TESTTABLE2', 'RED2SNOW.PUBLIC.TESTTABLE', cols, 'and')
# print(rst)


# """cols: [{'col_name': 'id', 'condition': '=', 'value'=None},
#           {'col_name': 'is_active', 'condition': '=', 'value'='value2'}]"""
# cols = [{'name': 'PROPERTYHOUSENUM', 'value':'1903'}]
# rst = get_upsert_update_cols(cols)
# print(rst)


# """input:
#             table: main_table
#             cols: [{'col_name': 'id', 'condition': '=', 'value'=None},
#                     {'col_name': 'is_active', 'condition': '=', 'value'='value2'}]
#             condition: 'and'"""
# cols= [{'col_name': 'PARID', 'condition': '=', 'value':'011232123123'}]
# rst = get_update_where_conditions('RED2SNOW.PUBLIC.TESTTABLE2', cols, 'and')
# print(rst)


# data = get_all_rows("RED2SNOW.PUBLIC.TESTTABLE1")
# upserts_dml_statements = [
#     "delete from RED2SNOW.PUBLIC.TESTTABLE2 where PROPERTYHOUSENUM='951';",
#     "delete from RED2SNOW.PUBLIC.TESTTABLE2 where PROPERTYHOUSENUM='912';"
# ]
# unload_query = 'RED2SNOW.PUBLIC.DEMO'
# merge_unload(data, ',', 'tmprows', 'RED2SNOW.PUBLIC.TESTTABLE2', 'RED2SNOW.PUBLIC.TESTTABLE9',
#              upserts_dml_statements, unload_query,
#              'red2snow', 'csvs4snow/tmpmerge.csv', unload_status_query='', additional_copy_options='',
#              additional_unload_options='')
