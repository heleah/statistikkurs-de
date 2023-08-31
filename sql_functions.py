# We import a method from the modules to address environment variables and 
# we use that method in a function that will return the variables we need from .env 
# to a dictionary we call sql_config

from dotenv import dotenv_values
import pandas as pd
import psycopg2
import os
import hashlib
import numpy as np

def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database','user','password']
    dotenv_dict = dotenv_values(".env")
    sql_config = {key:dotenv_dict[key] for key in needed_keys if key in dotenv_dict}
    return sql_config

def get_dataframe(sql_query):
    connection = get_connection()
    with connection.cursor() as cursor:
        try:
            result = pd.read_sql(sql_query, connection)
        except psycopg2.Error as e:
            print(f"Error creating the table: {e}")
        
    return result

def get_connection():
    # Create a connection to the PostgreSQL database
    connection = psycopg2.connect(
                                    host = os.getenv('host'),
                                    port = os.getenv('port'),
                                    database = os.getenv('database'),
                                    user = os.getenv('user'),
                                    password = os.getenv('password')
                                 )
    return(connection)

def unix_to_timestamp(data_frame, column_name_unix, unit="s"):
    # column_name_new = string
    column_name_new = column_name_unix + "_timestamp"

    data_frame[column_name_new] = pd.to_datetime(data_frame[column_name_unix], unit=unit)

    # Display updated DataFrame
    return data_frame

#Encrypt a column
def encrypt(df, column):
    hashes = []
    column_new_name = column + "_encrypted"

    for i in df[column]:
        if type(i) != str:
            hashes.append(None)
            continue
        i = i.encode()
        i = hashlib.sha256(i).hexdigest()
        hashes.append(i)
    df[column_new_name] = hashes


# Upload a dataframe to Database

# Important: Uses predefined Connection; Does not replace tables; appends new data if Data exists
def upload_dataframe(upload_frame, schema_name, table_name):
    connection = get_connection()

    # Function to get PostgreSQL data type from Pandas data type
    def get_pg_data_type(pandas_data_type):
        data_type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'bool': 'BOOLEAN',
            'object': 'TEXT',
            'datetime64': 'TIMESTAMP'
        }
        return data_type_mapping.get(str(pandas_data_type), 'TEXT')

    # Create the table if it doesn't exist in the "public" schema
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

            # Get the DataFrame column names and data types to create the table
            columns_info = ', '.join([f"{col} {get_pg_data_type(upload_frame[col].dtype)}" for col in upload_frame.columns])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns_info})")
        except psycopg2.Error as e:
            print(f"Error creating the table: {e}")

    # Insert the DataFrame data into the table in the "public" schema
    with connection.cursor() as cursor:
        try:
            for row in upload_frame.itertuples(index=False, name=None):
                cursor.execute(
                    f"INSERT INTO {schema_name}.{table_name} ({', '.join(upload_frame.columns)}) VALUES ({', '.join(['%s' for _ in upload_frame.columns])})",
                    row
                )
            connection.commit()
        except psycopg2.Error as e:
            connection.rollback()
            print(f"Error inserting data into the table: {e}")

    # Close the connection
    connection.close()

# Effect size by cohen when groups have different sample size and standard deviations
def cohen_d(x, y):
    nx = len(x)
    ny = len(y)
    dof = nx + ny - 2
    return (np.mean(x) - np.mean(y)) / np.sqrt(((nx-1)*np.std(x, ddof=1) ** 2 + (ny-1)*np.std(y, ddof=1) ** 2) / dof)

def connection_test():
    try:
        get_dataframe("SELECT * FROM public.test_table")
        print("Connection zur Database funktionsfähig")
    except:
        print("Connection funktioniert nicht, überprüfe deine Credentials und ggf. IP")
