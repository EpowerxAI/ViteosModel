# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 20:03:20 2018

@author: rangenine.chandar
"""

import pandas as pd
import sys
pd.options.display.max_rows = 9999
import numpy as np
import csv
import h2o
from pymongo import MongoClient
import datetime
import databaseconfig as cfg
#import ipdb


#========================= Take inputs ========================================

# FIELDS_TO_IGNORE_FILE = "./ReconML_FieldsToIgnore_withColType.xlsx"
FIELDS_TO_IGNORE_FILE = "/home/reconadmin/Viteos_Recon_ML/SRC/ReconML_FieldsToIgnore_withColType_Cash.csv"

def print_usage():
    print('Script to make predictions on given MEO data using a Cash recon model for a specific client')
    print('\nUsage: python3 {} {} {} {} {}\n'.format(sys.argv[0],'<TaskId>', '<ClientShortCode>', '<ReconPurpose>', '<meocollectionname>', '<Model>', '<RequestId>'))
    print('<TaskId>\t: TaskId of the data')
    print('<ClientShortCode>\t: ClientShortCode of the client')
    print('<ReconPurpose>\t\t: ReconPurpose for MEO data')
    print('<collection_meo>\t\t: collection_meo for MEO data')
    print('<Model>\t\t\t: Full path to the model file to be used for prediction')
    print('<RequestId>\t: RequestId of the prediction')
    print('\nExpects a "fields-to-ignore-file" at {}\n'.format(FIELDS_TO_IGNORE_FILE))


args = sys.argv[1:]
print('Arguments passed in: {}'.format(args))
if len(args) != 6:
    print_usage()
    sys.exit(1)

TaskId = int(args[0])
csc = args[1]
ReconPurpose = args[2]
meocollectionname = args[3]
model ='/home/reconadmin/Viteos_Recon_ML/SRC/'+csc+ReconPurpose+'.model'
Process_ID = int(args[5])
Query = cfg.Query

#========================= Create MongoDB connection ========================================
#ipdb.set_trace()
client = MongoClient(cfg.dbconnection['meo_connection_host'],username='mongouseradmin',password='@L0ck&Key')
dbname = cfg.dbname['meo_db']
db_meo = client[dbname]   # database to be connected
#collection_meo_name = cfg.collection_names['collection_meo']
collection_meo = db_meo[meocollectionname]
print('Using collection {} to get MEO data'.format(collection_meo))

    
# # creating connectioons for communicating with Mongo DB
# meo_connection = MongoClient('localhost:27017')
# db_meo = meo_connection.Viteos   # database to be connected

#Read meo frame with MongoDB
try:
    cursor = collection_meo.find( {"TaskInstanceID": TaskId, "ViewData":{"$ne":None}, "MatchStatus":{"$ne":21}, "ViewData.CombinedAndIsPaired" : False}, Query)
    df1 = pd.DataFrame(list(cursor))
    print ('\n Cash data - {} rows,cols loaded from mongodb\n'.format(df1.shape))

except Exception as e:
    print( str(e))
        
if df1.shape[0] == 0:
    raise ValueError('empty dataframe - no data to make predictions on!')

df = df1['ViewData'].apply(pd.Series)
df['_id'] = df1['_id']
print(df.shape)
df.rename(columns={"_id":'ID'}, inplace=True)


# Remove columns which are not required
df = df.drop(df.columns[df.columns.str.startswith(('InternalComment'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('Date'))], axis=1)
#df = df.drop(df.columns[df.columns.str.contains(('Color|color'))], axis=1)
#df = df.drop(df.columns[df.columns.str.contains('ID|Id|id')], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('SEDOL'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('ISIN'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('Sedol'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('Cusip'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('CUSIP'))], axis=1)
#df = df.drop(df.columns[df.columns.str.endswith(('Ticker'))], axis=1)
#df = df.drop(df.columns[df.columns.str.contains('custom')], axis=1)
#df = df.drop(df.columns[df.columns.str.contains('Custom')], axis=1)


# below was retained as code below uses it.. should not cause any harm
colData = pd.read_csv(FIELDS_TO_IGNORE_FILE)

# Remove rows with Status as [] and source combination as UNMAPPED
status_to_ignore = ['Archive', 'HST', 'OC', np.NaN]
df = df[~df.Status.isin(status_to_ignore)]
df = df[df['Source Combination'] != 'UNMAPPED']

# Filter rows for Client
# df=df[df["ClientShortCode"] == csc]

# reset index as many rows have been dropped
df.reset_index(drop=True, inplace=True)


# Change datatype columns ending with "Absolute" and having DataType as String to bool
lst1 = colData[(colData["Field Name"].str.endswith("Absolute")) & (colData["DataType"] == "String")]["Field Name"].tolist()

for i in range(colData.shape[0]):
    fname = colData["Field Name"].iat[i]
    if fname in (lst1):
        colData.loc[(colData["Field Name"] == fname), "DataType"] = "Bool"
  
    
# Change datatype columns ending with "Difference" and having DataType as String to bool
lst1 = colData[(colData["Field Name"].str.endswith("Difference")) & (colData["DataType"] == "String")]["Field Name"].tolist()

for i in range(colData.shape[0]) :
    fname=colData["Field Name"].iat[i]
    if fname in (lst1) :
        colData.loc[(colData["Field Name"] == fname), "DataType"]="Bool"  

# Replace datatype specified in excelsheet to pandas compatible datatype
# Create a dict using 'Field Name' and 'DataType'
# Use this dictonary to apply datatype
colData["DataType"]=colData["DataType"].replace("String", 'np.str')
colData["DataType"]=colData["DataType"].replace("Numeric", 'np.float64')
colData["DataType"]=colData["DataType"].replace("Decimal", 'np.float64')
colData["DataType"]=colData["DataType"].replace("Bool", 'np.bool')
colData["DataType"]=colData["DataType"].replace("Date", 'np.str')

colData=colData[['Field Name','DataType']]
col_dict = colData.set_index('Field Name').to_dict()

# ipdb.set_trace()

for k,v in col_dict['DataType'].items():
    if k in df.columns:
        # print(k,v,"\n")
        try:
            df[k]=df[k].astype(eval(v))
        except ValueError as e:
            print('Error encountered while converting datatype of column {} to {}.. dropping that column.. and continuing!'.format(k,v))
            df.drop(k, axis=1, inplace=True, errors='ignore') 
            pass


# Create a list of column datatypes as will be used by h20
temp = df.ftypes.tolist()
replace_dict = {'object': 'categorical', 'float64': 'double', 'int64': 'double' , 'bool': 'categorical'}
temp_lst = [x for x in map(lambda x: x.split(':')[0], temp)]
new_col_types = [y for y in map(lambda x: replace_dict.get(x), temp_lst)]
new_col_types = ['double'] + new_col_types

df.to_csv("./temp_predict.csv", index=True, na_rep="", quoting=csv.QUOTE_ALL)

# init H2o and read file
h2o.init(nthreads = 1, max_mem_size='9g')

dfh2o = h2o.import_file(path="./temp_predict.csv", col_types=new_col_types)

saved_model = h2o.load_model(model)
pred=saved_model.predict(dfh2o)

pred_y = pred['predict']

dfh2o=dfh2o.cbind(pred_y)

dfh2o.set_name('predict', 'predicted_label')

## predict_filename = '/home/algolaptop3/Desktop/viteos/predicted_data/Cash_temp.csv'
#print("h2o shape:", dfh2o.shape)
#h2o.export_file(dfh2o, predict_filename, force=True, parts=1)


#ipdb.set_trace()

# Insert the pandas dataframe to momgodb
dbname = cfg.dbname['pred_db']
client = MongoClient(cfg.dbconnection['pred_connection_host'],username='mongouseradmin',password='@L0ck&Key')
#client = MongoClient(cfg.dbconnection['pred_connection_host'])
db_pred = client[dbname]   # database to be connected
#collection_pred_name = cfg.collection_names['collection_to_save_predictions_cash']
#collection_pred = db_pred[collection_pred_name]
Pred_Collection_Name = 'MLPredictions_'+ReconPurpose
collection_pred = db_pred[Pred_Collection_Name]


# aua_connection = MongoClient(cfg.collection_names[])
# db = cfg.dbname['aua_db']
# db_aua = meo_connection.db   # database to be connected
# colleection_aua = cfg.dbname['colleection_aua']

timestamp_of_run = datetime.datetime.now()
#Limiting Columns to save in DB
dfh = dfh2o[:, ["Task ID", "Side0_UniqueIds", "Side1_UniqueIds", "Status", "CombinedAndIsPaired", "Recon Setup Code", "Source Combination Code", "Task Business Date", "Investment Type",
 "Client", "Recon Setup", "Recon Purpose", "Currency", "Mapped Custodian Account", "Source", "Source Combination", "Net Amount Difference", "Net Amount Difference Absolute", "BreakID",
 "ClientShortCode", "Task Knowledge Date", "predicted_label"]]

df = dfh.as_data_frame()
df=df[pd.to_numeric(df['Task ID'], errors='coerce').notnull()]
df.loc[:, 'timestamp_of_run'] = timestamp_of_run
df.loc[:, 'Process ID'] = Process_ID
data = df.to_dict(orient='records') 

collection_pred.insert_many(data)
#ipdb.set_trace()
