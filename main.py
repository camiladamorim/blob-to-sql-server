
import azure.functions as func
import logging
import pyodbc
import pandas as pd
from azure.storage.blob import BlobServiceClient 
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import os
  

# environment variables:
CONN_STR_BLOB = os.getenv('CONN_STR_BLOB')
CONN_STR_DB = os.getenv('CONN_STR_DB')
# modificar:
line_where_table_start = 9
TABLE_NAME = "my_table"
CONTAINER_NAME = 'my_container'


def test_db():
    cnxn = pyodbc.connect(CONN_STR_DB)
    cursor = cnxn.cursor()
    query="select * from " + TABLE_NAME + " limit 10;"
    cursor.execute(query)
    row = cursor.fetchall() 
    rows = []
    if row: 
        rows.append(row)
    print(rows)
    return rows

# drop unnamed columns in the table
def drop_unnamed(df):
    df=[c for c in range(len(df.columns)) if ('Unnamed' in str(df.columns[c]))]
    return df

def main(myblob: func.InputStream):
    blob = myblob.name.split('/',1)[1]
    logging.info(f"blob: {blob}")
    blob_service_client = BlobServiceClient.from_connection_string(CONN_STR_BLOB)
    source_blob = blob_service_client.get_blob_client(container = CONTAINER_NAME, blob=blob)
    content = source_blob.download_blob().readall()
    df_data = pd.read_excel(content, header=line_where_table_start).dropna(how='all').reset_index(drop=True)
    df_data.drop(columns=drop_unnamed(df_data), inplace=True) 
    connection_string = CONN_STR_DB
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url, fast_executemany=True)
    conn = engine.connect()
    df_data.to_sql(name=TABLE_NAME, con=conn, if_exists='replace', chunksize=100, index=False )
    conn.commit()
    test = test_db()
    try:
        if test:
            print("------elements in table------")
        else:
            print("------empty table------")
    except:
        print("ERROR")

