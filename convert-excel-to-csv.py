import pandas as pd
df = pd.read_excel('xyz.xlsx', sheet_name=None)  
for key in df.keys(): 
	df[key].to_csv('output/%s.csv' %key)
