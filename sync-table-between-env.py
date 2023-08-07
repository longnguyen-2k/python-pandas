
import threading
import os
import glob
import mysql.connector
import pandas as pd
import numpy as np
from tqdm import tqdm

class mysqlThread (threading.Thread):
    def __init__(self, threadID, counter,numberThread,env_source,env_target,tables_mapping):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.counter = counter
      self.numberThread=numberThread
      self.env_source=env_source
      self.env_target= env_target
      self.tables_mapping = tables_mapping
    def run(self):
        self._handle_on_thread(self.threadID, self.counter,self.numberThread,self.env_source,self.env_target,self.tables_mapping)

    def _handle_on_thread(self,ID, counter,numberThread,env_source,env_target,tables_mapping):
        while counter < len(tables_mapping):
            table_name = tables_mapping[counter].strip()
            truncate_table(env_config=env_target,table_name=table_name)
            columns = fetch_columns(env_target, table_name= table_name)
            mapping_table(env_source=env_source,env_target=env_target,table_name=table_name,columns= columns)
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
def truncate_table(env_config,table_name):
    db_connection = get_db_connection(env_config=env_config)
    with db_connection:
        cursor = db_connection.cursor()
        cursor.execute(f"truncate table {table_name}")
        
def get_db_connection(env_config):
    return  mysql.connector.connect(
            host=env_config['host'],
            user=env_config['user'],
            password=env_config['password'],
            port= env_config['port'])
    
def build_insert_statement(table_name,columns,values):
    statement = f'''INSERT IGNORE INTO  {table_name} ({columns}) values {",".join([ str(tuple([x.decode("utf-8") if x is not None else 'NULL' for x in value])) for value in values])} ;'''
    return  statement.replace("'NULL'","NULL")

def pagingId(env_config,table_name):
    connection = get_db_connection(env_config=env_config)
    with connection:
        cursor = connection.cursor()
        try:
            cursor.execute(f'SELECT MIN(g.id) AS start_key,MAX(g.id) AS end_key ,COUNT(g.id) AS page_size FROM (SELECT id,ROW_NUMBER() over (ORDER BY id) AS row_num FROM {table_name} a  ) g  GROUP BY floor((g.row_num-1)/1000) ORDER BY start_key')
            return [{"startId":x[0],"endId":x[1]} for x in cursor]
        except:
            return []
def mapping_table(env_source,env_target,table_name,columns):
    if columns is not None and len(columns) > 0:
        values = list()
        select_columns = ",".join(columns)
        number_paralles = 24
        pages = pagingId(env_source,table_name)
        # Calculate the approximate length of each part
        split_result = split_array(pages, number_paralles)
        progress_bar = tqdm(total=(len(pages)), desc=f"{table_name} beginning copy ....", unit="iteration")
        wokers = []
        for i, part in enumerate(split_result, 1):
            woker = threading.Thread(target=thread_woker, args=(part, table_name,select_columns,progress_bar))
            wokers.append(woker)
            woker.start()
        for process in wokers:
            process.join()    
        progress_bar.close()

def thread_woker(pages,table_name,select_columns,progress_bar):
    for page in pages :
            source_connection = get_db_connection(env_source)
            target_connection = get_db_connection(env_target)
            startId = str(page["startId"])
            endId = str(page["endId"])
            with source_connection :
                cursor = source_connection.cursor(raw=True)
                cursor.execute(f" select {select_columns} from {table_name} where id between '{startId}' and '{endId}' ")
                values = [x for x in cursor]
            with target_connection:
                cursor = target_connection.cursor()
                cursor.execute(build_insert_statement(table_name,select_columns,values))
                target_connection.commit()
            progress_bar.update(1)
def split_array(arr, n):
    length = len(arr)
    part_length = length // n
    remainder = length % n
    
    split_arrays = [arr[i * part_length + min(i, remainder):(i + 1) * part_length + min(i + 1, remainder)] for i in range(n)]
    
    return split_arrays

def main(env_source,env_target,tables_files,numberThread):
    with open(tables_files,'r') as fh:
        tables = list(filter(lambda n: n not in ['bak','clone','mirror','ori','julb','testing'], fh.readlines()))
        for i in range(numberThread) :
            mysqlThread(int(i)+1,int(i) ,numberThread,env_source=env_source,env_target=env_target,tables_mapping = tables).start()


env_source={

}
env_target={

}
main(env_source=env_source,env_target=env_target,tables_files = 'tables.txt',numberThread = 3)