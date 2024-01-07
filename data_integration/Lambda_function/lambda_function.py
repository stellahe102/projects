import toml
from dotenv import load_dotenv
import json
import os
import requests
import snowflake.connector as sf
import datetime

def lambda_handler(event, context):

    # load parameters from toml file
    app_config = toml.load('config.toml') 
    url = app_config['url']
    destination_folder = app_config['destination_folder']
    filename = app_config['filename']

    account = app_config['account']
    warehouse = app_config['warehouse']
    database = app_config['database']
    schema = app_config['schema']
    role = app_config['role']
    file_format = app_config['file_format']
    stage_name = app_config['stage']
    table = app_config['table']

    
    # load secrets
    load_dotenv()
    user = os.getenv('user')
    password = os.getenv('password')

    # 1. grab data from S3 bucket
    response = requests.get(url)
    response.raise_for_status() #raise error 
    
    
    # 2. store data into my lambda instance /tmp/    
    file_path = os.path.join(destination_folder, filename)
    
    # wb: write binary, don't touch the original file/data format read as is
    with open(file_path, 'wb') as file:  
        file.write(response.content)
    
    # read the content that's written into file
    # with open(file_path, 'r') as file:
        # file_content = file.read()
        # print('file content:')
        # print(file_content)
    
    # 3. connect to snowflake
    conn = sf.connect(user = user, password = password, \
                 account = account, warehouse = warehouse, \
                  database = database,  schema = schema,  role = role)  
    cursor = conn.cursor()
    
    # 4. provide warehouse name
    use_warehouse = f"use warehouse {warehouse};"
    cursor.execute(use_warehouse)
    
    # 5. provide scheme
    use_schema = f"use schema {schema};"
    cursor.execute(use_schema)
    
    # 6. create file format
    create_csv_format = f"create or replace file format {file_format} \
        type = 'CSV' field_delimiter = ',';"
    cursor.execute(create_csv_format)
    
    # 7. create stage
    create_stage = f"create or replace stage {stage_name} \
        file_format = {file_format};"
    cursor.execute(create_stage)
    
    # upload file
    upload_file = f"put 'file://{file_path}' @{stage_name};"
    cursor.execute(upload_file)
    
    # 8. list stage
    list_stage = f"list @{stage_name};"
    cursor.execute(list_stage)
    
    # 9. truncate table
    truncate_table = f"truncate table {table};"
    cursor.execute(truncate_table)
    
    # 10. copy into table
    
    copy_into_table = f"copy into {table} from @{stage_name}/{filename} \
        FILE_format = {file_format} on_error='CONTINUE';" 
    cursor.execute(copy_into_table)
    
    # add a log message
    current_datetime = datetime.datetime.now()
    print(f'data loaded sucessfully at {current_datetime}') 
    
    return {
        'statusCode': 200,
        'body': json.dumps('file uploaded to snowflake successfully!')
    }
    
    cursor.close()
    conn.close()
