import mysql.connector
import threading

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  port= 3306
)
mycursor = mydb.cursor()

with open('list.txt','r', encoding="utf-8") as fh:
        lines=fh.readlines()
for line in lines:
    mycursor.execute("select TABLE_SCHEMA, COLUMN_NAME, TABLE_NAME FROM   INFORMATION_SCHEMA.COLUMNS where COLUMN_COMMENT like '%"+line.strip()+"%' ")
    setFields =  set()
    setTables = set()
    for x in mycursor:
        setFields.add(x[1])
        if(all([ item not in str(x[2]) for item in ['bak','clone','mirror','ori','julb','snapshot','testing']])):
            setTables.add(x[0]+ "." +x[2])
    fields = '; '.join(setFields)
    tables = '; '.join(setTables)
    with open('fields.txt','a', encoding="utf-8") as fh:
        if(len(setFields)==0):
            fh.write('Not found'+'\n')
        else:
            fh.write(fields+'\n')
    with open('tables.txt','a', encoding="utf-8") as fh:
        if(len(setFields)==0):
            fh.write('Not found'+'\n')
        else:
            fh.write(tables+'\n')

            