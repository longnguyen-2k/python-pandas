
import threading
import os
import glob
import mysql.connector
import pandas as pd
import numpy as np

class mysqlThread (threading.Thread):
    def __init__(self, threadID, counter,numberThread,path,files,fieldRPG):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.counter = counter
      self.numberThread=numberThread
      self.files=files
      self.path= path
      self.fieldRPG= fieldRPG
    def run(self):
        self._handle_csv_on_thread(self.threadID, self.counter,self.numberThread,self.path,self.files,self.fieldRPG)

    def _handle_csv_on_thread(self,ID, counter,numberThread,path,files,fieldRPG):
        while counter < len(files):
            mapping_csv(path,files[counter],fieldRPG)
            counter+=numberThread

def mapping_csv(path,filePath,fieldRPG):
    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            port= 3306)
    mycursor = mydb.cursor()
    df = pd.read_csv(filePath, header=0)
    number_name_miss= 0
    df_fields= list()
    df_tables= list()
    for line in df.loc[:,fieldRPG]:
        if pd.isna(line):
            number_name_miss = number_name_miss+1
            #is to much number of name value is missed, greate than 5; just break to end query
            if number_name_miss >5:
                break
            continue
        mycursor.execute("select TABLE_SCHEMA, COLUMN_NAME, TABLE_NAME FROM   INFORMATION_SCHEMA.COLUMNS where COLUMN_COMMENT like '%"+str(line).strip()+"%' ")
        setFields = set()
        setTables = set()
        for x in mycursor:
            setFields.add(x[1])
            if all([ item not in str(x[2]) for item in ['bak','clone','mirror']]) :
                setTables.add(x[0]+ "." +x[2])
        fields = '; '.join(setFields)
        tables = '; '.join(setTables)
        if(len(setFields)==0):
                df_fields.append('Not found')
        else:
                df_fields.append(fields)
        if(len(setTables)==0):
                df_tables.append('Not found')
        else:
                df_tables.append(tables)
    dictionary= {
        'Fields':df_fields,
        'Tables':df_tables
    }

    df_result= pd.DataFrame(dictionary)
    df=df.join(df_result)
    df.to_csv(path+ os.path.basename(filePath))


def main(folder_input,folder_output,fieldRPG,numberThread):
    files = glob.glob(folder_input+"*.csv")
    for i in range(numberThread) :
        mysqlThread(int(i)+1,int(i) ,numberThread,path=folder_output,files=files,fieldRPG=fieldRPG).start()


main(folder_input ='input/',folder_output='output/',fieldRPG= 'Name',numberThread = 10)