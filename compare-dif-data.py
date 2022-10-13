import pandas as pd
import threading
import glob
import pandas as pd
import numpy as np
import os
class CompareDataThread (threading.Thread):
    def __init__(self, threadID, counter,numberThread,files,files2):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.counter = counter
      self.numberThread=numberThread
      self.files=files
      self.files2= files2
    def run(self):
        self._handle_csv_on_thread(self.threadID, self.counter,self.numberThread,self.files,self.files2)

    def _handle_csv_on_thread(self,ID, counter,numberThread,files,files2):
        while counter < len(files):
            for filename in files2:
                if os.path.basename(filename) in files[counter]:
                    compare_equals(files[counter],filename)
            counter+=numberThread

def compare_equals(file_1,file_2):
    try:
        df = pd.read_csv(file_1).astype(str, copy=True, errors='raise')
        df2 = pd.read_csv(file_2).astype(str, copy=True, errors='raise')
        rename(df=df,df2=df2)
        df_columns = list(df.columns.values.tolist())
        df2_columns = list(df2.columns.values.tolist())
        columns=list()
        for element in df2_columns :
            if element in df_columns:
                columns.append(element)
        if   'ID' in columns:
            columns.remove('ID')
        df = df[columns].copy()
        df2 = df2[columns].copy()
        df=df.sort_values(df.columns.values.tolist()[1:], ascending = False)
        df2=df2.sort_values(df.columns.values.tolist()[1:], ascending = False)
        df_compare = df.compare(df2, keep_equal=True)
        if  not df_compare.empty:
            print(df_compare)
            print(file_2)

    except:
        check_exist(file_1=file_1,file_2=file_2)

def rename(df:pd.DataFrame,df2:pd.DataFrame):
    df.rename(columns = {'id':'ID'}, inplace = True)
    df2.rename(columns = {'id':'ID'}, inplace = True)

def check_exist(file_1,file_2):
    df = pd.read_csv(file_1).astype(str, copy=True, errors='raise')
    df2 = pd.read_csv(file_2).astype(str, copy=True, errors='raise')
    rename(df=df,df2=df2)
    df_columns = list(df.columns.values.tolist())
    df2_columns = list(df2.columns.values.tolist())
    columns=list()
    for element in df2_columns :
        if element in df_columns:
            columns.append(element)
    if   'ID' in columns:
        columns.remove('ID')
    df = df[columns].copy()
    df2 = df2[columns].copy()
    df=df.sort_values(by=df.columns.values.tolist()[1:], ascending = True)
    df2=df2.sort_values(by=df.columns.values.tolist()[1:], ascending = True)
    all_df = pd.merge(df, df2,on=df.columns.values.tolist()[1:],how='right', indicator='exists')
    all_df['exists'] = np.where(all_df.exists == 'right_only', True, False)
    all_df=df = all_df[all_df.exists == True]
    if 'tbl_cardcore_param_network_affiliation' in file_2:
        print(all_df)
    if not all_df.empty:
        # print(all_df)
        print(file_2)


def main(folder_source,folder_compare,numberThread):
    files = glob.glob(folder_source+"*.csv")
    files2=glob.glob(folder_compare+"*.csv")
    for i in range(numberThread) :
        CompareDataThread(int(i)+1,int(i) ,numberThread,files =files,files2=files2).start()

main(folder_source ='uat/outfile/',folder_compare='sit2/outfile/',numberThread = 10)