
import threading
import os
import glob
import mysql.connector
import pandas as pd
import numpy as np
from tqdm import tqdm

class mysqlThread (threading.Thread):
    def __init__(self, threadID, counter,numberThread,env_source,tables_mapping):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.counter = counter
      self.numberThread=numberThread
      self.env_source=env_source
      self.tables_mapping = tables_mapping
    def run(self):
        self._handle_csv_on_thread(self.threadID, self.counter,self.numberThread,self.env_source,self.tables_mapping)

    def _handle_csv_on_thread(self,ID, counter,numberThread,env_source,tables_mapping):
        while counter < len(tables_mapping):
            table_name = tables_mapping[counter].strip()
            columns = fetch_columns(env_source, table_name= table_name)
            mapping_table(env_source=env_source,table_name=table_name,columns= columns)
            counter+=numberThread

def fetch_columns(env_config,table_name):
    db_connection = get_db_connection(env_config=env_config)
    with db_connection :
        try:
            cursor = db_connection.cursor()
            cursor.execute(f"show columns from {table_name}")
            return [obj[0] for obj in cursor]
        except:
            print(f"{table_name} doen't exist")
             
def get_db_connection(env_config):
    return  mysql.connector.connect(
            host=env_config['host'],
            user=env_config['user'],
            password=env_config['password'],
            port= env_config['port'])
    
def build_insert_statement(table_name,columns,values):
    statement = f'''INSERT IGNORE INTO  {table_name} ({",".join(columns)}) values {",".join([ str(tuple([x.decode("utf-8") if x is not None else 'NULL' for x in value])) for value in values])} ;'''
    return  statement.replace("'NULL'","NULL")

def pagingId(env_config,table_name):
    connection = get_db_connection(env_config=env_config)
    with connection:
        cursor = connection.cursor()
        try:
            cursor.execute(f'SELECT MIN(g.id) AS start_key,MAX(g.id) AS end_key ,COUNT(g.id) AS page_size FROM (SELECT id,ROW_NUMBER() over (ORDER BY id) AS row_num FROM {table_name} a  ) g  GROUP BY floor((g.row_num-1)/500) ORDER BY start_key')
            return [{"startId":x[0],"endId":x[1]} for x in cursor]
        except:
            return []
def mapping_table(env_source,table_name,columns):
    if columns is not None and len(columns) > 0:
        values = list()
        select_columns = ",".join(columns)
        pages = pagingId(env_source,table_name)

        progress_bar = tqdm(total=(len(pages)), desc=f"{table_name} backup....", unit="iteration")
        for page in pages :
            source_connection = get_db_connection(env_source)
            startId = str(page["startId"])
            endId = str(page["endId"])
            with source_connection :
                cursor = source_connection.cursor(raw=True)
                cursor.execute(f" select {select_columns} from {table_name} where id between '{startId}' and '{endId}' ")
                values = [x for x in cursor]
                with open(f'backup/{table_name}.sql','a',encoding='utf-8') as fh:
                    fh.write(build_insert_statement(table_name=table_name,columns=columns,values=values))
            
            progress_bar.update(1)
                    # target_connection.commit()
                # cursor.execute()
        progress_bar.close()
            


def main(env_source,tables_files,numberThread):
    with open(tables_files,'r') as fh:
    
        tables = list(filter(lambda n: n.strip() not in ['bak','clone','mirror','ori','julb','testing'], fh.readlines()))
        for i in range(numberThread) :
            mysqlThread(int(i)+1,int(i) ,numberThread,env_source=env_source,tables_mapping = tables).start()


env_source={
    "host":"",
    "user":"",
    "password":"",
    "port": 3606
}

main(env_source=env_source,tables_files = 'tables.txt',numberThread = 12)