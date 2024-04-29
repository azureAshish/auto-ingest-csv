import os
from snowflake.snowpark import Session
from datetime import datetime

def snowpark_basic_auth():
    connection_parameters = {
        "account": '',
        "user": '',
        "password": ''
    }

    return Session.builder.configs(connection_parameters).create()

def generate_ddl_statement(column_names,data_types,table_name):
    ddl_template = "CREATE TABLE IF NOT EXISTS {} (\n{});"
    columns = []
    for name, data_type in zip(column_names,data_types):
        column_definition = f"{name} {data_type}"
        columns.append(column_definition)

    ddl_statement = ddl_template.format(table_name, ",\n".join(columns))
    return ddl_statement

def generate_copy_statement(table_name,stage_name,csv_file_path,file_format):
    copy_command = f"""
    COPY INTO {table_name}
    FROM @{stage_name}/{csv_file_path}
    FILE_FORMAT = (FORMAT_NAME = '{file_format}')
    """
    return copy_command

if __name__ == '__main__':
    utc_start_time = datetime.utcnow()
    session_with_pwd = snowpark_basic_auth()

    session_with_pwd.sql("use role sysadmin").collect()
    session_with_pwd.sql("use database SNOWINGEST").collect()
    session_with_pwd.sql("use schema AUTOCSVINGESTION").collect()
    session_with_pwd.sql("use warehouse compute_wh").collect()

    stg_files = session_with_pwd.sql("list @my_stage").collect()

    for row in stg_files:
        row_value = row.as_dict()
        stg_file_path_value = row_value.get('name')

        file_path, file_name = os.path.split(stg_file_path_value)
        stg_location = "@" + file_path

        infer_schema_sql = """ \
            SELECT *
            FROM TABLE(
            INFER_SCHEMA(
            LOCATION => '{}/',
            files => '{}',
            FILE_FORMAT => 'file_format_parse_header'
            )
        )
        """.format(stg_location,file_name)


        inferred_schema_rows = session_with_pwd.sql(infer_schema_sql).collect()
        col_name_lst = []
        col_data_type_lst = []

        for row in inferred_schema_rows:
            row_value = row.as_dict()
            column_name = row_value.get('COLUMN_NAME')
            column_type = row_value.get('TYPE')
            col_name_lst.append(column_name)
            col_data_type_lst.append(column_type)


        table_name = file_name.split('.')[0]
        create_ddl_stmt = generate_ddl_statement(col_name_lst,col_data_type_lst,table_name.upper())
        copy_stmt = generate_copy_statement(table_name,'my_stage',file_name,'file_format_csv')


        sql_file_path = table_name + ".sql"
        with open(sql_file_path,"w") as sql_file:
            sql_file.write("- Creating table\n")
            sql_file.write(create_ddl_stmt)
            sql_file.write("\n- Execute copy command\n")
            sql_file.write(copy_stmt)
            

        session_with_pwd.sql(create_ddl_stmt).collect()
        session_with_pwd.sql(copy_stmt).collect()
    
    utc_end_time = datetime.utcnow()
        
    print(utc_end_time-utc_start_time)