import mysql.connector
import threading

bank_uat2 = mysql.connector.connect(

)
bank_sit2 = mysql.connector.connect(
)
bank_uat2_cursor = bank_uat2.cursor()
bank_sit2_cursor = bank_sit2.cursor()

def retrieveTableNames(cursor,schema_name):
    cursor.execute(f" select TABLE_NAME from INFORMATION_SCHEMA.Tables where TABLE_SCHEMA = '{schema_name}' ")
    tables = list()
    for x in cursor:
        tables.append(x[0])
    return tables
def checkTables(source,target):
    for i in target:
        if i not in source:
            print(i)
schema_name='schema'
bank_uat2_tables = retrieveTableNames(bank_uat2_cursor,schema_name=schema_name)
bank_sit2_tables = retrieveTableNames(bank_sit2_cursor,schema_name=schema_name)

print(f"UAT2:{len(bank_uat2_tables)} compare to (more than) SIT2 : ")
checkTables(bank_sit2_tables,bank_uat2_tables)
print(f"SIT2:{len(bank_sit2_tables)} compare to (more than) UAT2 : ")
checkTables(bank_uat2_tables,bank_sit2_tables)



