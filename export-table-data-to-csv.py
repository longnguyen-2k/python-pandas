import mysql.connector
import threading
import pandas as pd
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  port= 3306
)
mycursor = mydb.cursor()

def extract_table_data(schema_name,table_name):
    print("process %s."%schema_name +table_name)
    mycursor.execute("SELECT * FROM "+schema_name+"."+table_name)
    d={}
    for column_name in mycursor.column_names:
        #remove fields are not compare
        if all([ item not in column_name for item in ['date_and_time','created_by','created_datetime','created_domain','updated_by','updated_datetime','updated_domain','db_version']]):
            d[column_name] = []
    for data in mycursor:
        i=0
        for x in data:
            if mycursor.column_names[i] not in ['date_and_time','created_by','created_datetime','created_domain','updated_by','updated_datetime','updated_domain','db_version']:
                d[mycursor.column_names[i]].append(str(x))
            i=i+1
    create_data_frame(table_name=table_name,data_dict=d)

def create_data_frame(table_name,data_dict):
    dataFrame = pd.DataFrame(data_dict)
    dataFrame.to_csv('folder1/outfile/%s.csv'%table_name)

def extract_table_name(schema_name):
    mycursor.execute("select * FROM  INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='%s'"%schema_name)
    list_tables=list()
    for table in mycursor:
        list_tables.append(table[2])
    mycursor.reset()
    for table in list_tables:
        #don't proceed backup table
        if(all([ item not in table for item in ['bak','clone','mirror','ori','julb','snapshot','testing']])):
            extract_table_data(schema_name,table)

extract_table_name('schema_name')