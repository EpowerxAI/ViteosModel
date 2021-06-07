#!/usr/bin/env python
# coding: utf-8

# In[379]:


import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE

import os
os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')



# In[380]:

import os
os.chdir('D:\\ViteosModel')


import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE
from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
from tqdm import tqdm
import pickle
import datetime as dt
import sys
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import dateutil.parser
from difflib import SequenceMatcher
import pprint
import json
from pandas import merge
import re

import dask.dataframe as dd
import glob
import math
from sklearn.feature_extraction.text import TfidfVectorizer
from dateutil.parser import parse
import operator
import itertools
from sklearn.feature_extraction.text import CountVectorizer

import xgboost as xgb
from sklearn.preprocessing import LabelEncoder

cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
#'Activity Code',
'Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount',
#'Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
#'Derived Source',
'Description','Department',
        #'ExpiryDate','ExternalComment1','ExternalComment2',
'ExternalComment3','Fund',
#'FX Rate',
#'Interest Amount',
'InternalComment1','InternalComment2',
'InternalComment3','Investment Type','Is Combined Data','ISIN','Keys',
'Mapped Custodian Account','Net Amount Difference','Net Amount Difference Absolute','Non Trade Description',
#'OTE Custodian Account',
#'Predicted Action','Predicted Status','Prediction Details',
'Price','Prime Broker',
'Quantity','SEDOL','Settle Date','SPM ID','Status',
#'Strike Price',
'System Comments','Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


# In[381]:


new_cols = ['ViewData.' + x for x in cols] + add


# In[18]:


#df_170.shape


# ## Close Prediction Soros

# In[382]:


cols_for_closed = ['Status','Source Combination','Mapped Custodian Account','Accounting Currency','B-P Currency',                   'Transaction ID','Transaction Type','Description','Investment ID',                   'Accounting Net Amount','B-P Net Amount',                   'InternalComment2','Custodian','Fund']
cols_for_closed = ['ViewData.' + x for x in cols_for_closed]
cols_for_closed_x = [x + '_x' for x in cols_for_closed] + ['ViewData.Side0_UniqueIds_x','ViewData.Side1_UniqueIds_x']
cols_for_closed_y = [x + '_y' for x in cols_for_closed] + ['ViewData.Side0_UniqueIds_y','ViewData.Side1_UniqueIds_y']
cols_for_closed_x_y = cols_for_closed_x + cols_for_closed_y

date_numbers_list = [16]
                     #2,3,4,
                    # 7,8,9,10,11,
                    # 14,15,16,17,18,
                    # 21,22,23,24,25,
                    # 28,29,30]

client = 'Soros'

setup_code = '153'

today = date.today()
d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
desired_date = d1 - timedelta(days=4)
desired_date_str = desired_date.strftime("%Y-%m-%d")
date_input = desired_date_str

filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections_SOROS.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections_SOROS.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_input) + '.csv'


mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
#db_3_for_writing_in_ml_server = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['MEO_AUA_Collections']
#db_4_for_MEO_data = mngdb_obj_3_for_writing_in_uat_server.client['MeoCollections']
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_Soros_ML']
#db_5_for_AUA_data = mngdb_obj_3_for_writing_in_uat_server.client['AUACollections']
#db_6_for_prediction = mngdb_obj_3_for_writing_in_uat_server.client['MLPrediction_Cash']


query_1_for_MEO_data = db_1_for_MEO_data['RecData_' + setup_code].find({ 
                                                                     "LastPerformedAction": 31
                                                             },
                                                             {
                                                                     "DataSides" : 1,
                                                                     "BreakID" : 1,
                                                                     "LastPerformedAction" : 1,
                                                                     "TaskInstanceID" : 1,
                                                                     "SourceCombinationCode" : 1,
                                                                     "MetaData" : 1, 
                                                                     "ViewData" : 1
                                                             })
list_of_dicts_query_result_1 = list(query_1_for_MEO_data)

meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 

df_153_june_25_meo = meo_df[new_cols]

df_153_june_25_meo = df_153_june_25_meo.drop_duplicates()
df_153_june_25_meo = df_153_june_25_meo.reset_index()
df_153_june_25_meo = df_153_june_25_meo.drop('index',1)



#df_153_june_25_meo = df_153_june_25_meo.rename(columns ={'ViewData.Cust Net Amount': 'ViewData.B-P Net Amount'})

meo = df_153_june_25_meo[~df_153_june_25_meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
meo = meo[~meo['ViewData.Status'].isnull()]
meo = meo.reset_index()
meo = meo.drop('index',1)
del df_153_june_25_meo

meo['Date'] = pd.to_datetime(meo['ViewData.Task Business Date'])
meo = meo[~meo['Date'].isnull()]
meo = meo.reset_index()
meo = meo.drop('index',1)
meo['Date'] = pd.to_datetime(meo['Date']).dt.date
meo['Date'] = meo['Date'].astype(str)

meo['ViewData.Side0_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str)
meo['ViewData.Side1_UniqueIds'] = meo['ViewData.Side1_UniqueIds'].astype(str)
meo['flag_side0'] = meo.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
meo['flag_side1'] = meo.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
meo.loc[meo['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
meo.loc[meo['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
meo.loc[meo['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
meo.loc[meo['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(int)
meo = meo[meo['ViewData.BreakID']!=-1]
meo = meo.reset_index()
meo = meo.drop('index',1)

meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)

BP_meo = meo[(meo['flag_side1'] >= 1) & (meo['flag_side0'] == 0)]
Acct_meo = meo[(meo['flag_side1'] == 0) & (meo['flag_side0'] >= 1)]



#BP_meo = BP_meo.rename(columns={'ViewData.Currency':'ViewData.B-P Currency'})
#Acct_meo = Acct_meo.rename(columns={'ViewData.Currency':'ViewData.Accounting Currency'})
#BP_meo = BP_meo.rename(columns ={'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})

BP_meo['filter_key'] = BP_meo['ViewData.Source Combination'].astype(str) + BP_meo['ViewData.Mapped Custodian Account'].astype(str) + BP_meo['ViewData.B-P Currency'].astype(str)

Acct_meo['filter_key'] = Acct_meo['ViewData.Source Combination'].astype(str) + Acct_meo['ViewData.Mapped Custodian Account'].astype(str) + Acct_meo['ViewData.Accounting Currency'].astype(str)




# BP_meo Side M X M architecture
BP_meo_training_df =[]
for key in (list(np.unique(np.array(list(BP_meo['filter_key'].values))))):
    BP_meo_filter_slice = BP_meo[BP_meo['filter_key']==key]
    if BP_meo_filter_slice.empty == False:

        BP_meo_filter_slice = BP_meo_filter_slice.reset_index()
        BP_meo_filter_slice = BP_meo_filter_slice.drop('index', 1)


#         df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
#         df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

        BP_meo_filter_joined = pd.merge(BP_meo_filter_slice, BP_meo_filter_slice, on='filter_key')
        BP_meo_training_df.append(BP_meo_filter_joined)

BP_meo_combination_df =pd.concat(BP_meo_training_df)

JNL_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Custodian_x'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Custodian_y'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'JNL') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'JNL') &  (BP_meo_combination_df['ViewData.Transaction ID_x'].astype(str) == BP_meo_combination_df['ViewData.Transaction ID_y'].astype(str)) &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

MTM_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Custodian_x'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Custodian_y'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'MTM') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'MTM') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

Collateral_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'Collateral') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'Collateral') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

DEB_CRED_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(['DEB','CRED'])) &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(['DEB','CRED'])) &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

DEP_WDRL_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(['DEP','WDRL'])) &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(['DEP','WDRL'])) &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

Miscellaneous_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'Miscellaneous') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'Miscellaneous') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

REORG_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'REORG') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'REORG') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]




P_and_I_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'P&I') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'P&I') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

Acct_meo_training_df =[]
for key in (list(np.unique(np.array(list(Acct_meo['filter_key'].values))))):
    Acct_meo_filter_slice = Acct_meo[Acct_meo['filter_key']==key]
    if Acct_meo_filter_slice.empty == False:

        Acct_meo_filter_slice = Acct_meo_filter_slice.reset_index()
        Acct_meo_filter_slice = Acct_meo_filter_slice.drop('index', 1)


#         df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
#         df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

        Acct_meo_filter_joined = pd.merge(Acct_meo_filter_slice, Acct_meo_filter_slice, on='filter_key')
        Acct_meo_training_df.append(Acct_meo_filter_joined)

Acct_meo_combination_df =pd.concat(Acct_meo_training_df)

Transfer_closed_breaks = Acct_meo_combination_df[(Acct_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'Transfer') &  (Acct_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'Transfer') & # (Acct_meo_combination_df['ViewData.Transaction ID_x'].astype(str) == Acct_meo_combination_df['ViewData.Transaction ID_y'].astype(str)) & \
 (abs(Acct_meo_combination_df['ViewData.Accounting Net Amount_x']).astype(str) == abs(Acct_meo_combination_df['ViewData.Accounting Net Amount_y']).astype(str)) & \
 (Acct_meo_combination_df['ViewData.Side0_UniqueIds_x'].astype(str) != Acct_meo_combination_df['ViewData.Side0_UniqueIds_y'].astype(str))] \
[cols_for_closed_x_y]





all_closed_df = pd.concat([JNL_closed_breaks,
                MTM_closed_breaks,
                Collateral_closed_breaks,
                DEB_CRED_closed_breaks,
                DEP_WDRL_closed_breaks,
                Miscellaneous_closed_breaks,
                REORG_closed_breaks,
                Transfer_closed_breaks], axis=0)

all_closed_df

 #list(all_closed_df['ViewData.Side0_UniqueIds_y']) + list(all_closed_df['ViewData.Side1_UniqueIds_y'])

closed_x = set(
                all_closed_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
                all_closed_df['ViewData.Side1_UniqueIds_x'].astype(str) 
               )

closed_y = set(all_closed_df['ViewData.Side0_UniqueIds_y'].astype(str) +                 all_closed_df['ViewData.Side1_UniqueIds_y'].astype(str) 
               )

all_closed = closed_x.union(closed_y)

## Read testing data 


# In[383]:


all_closed


# In[384]:


new_closed_keys = [i.replace('nan','') for i in all_closed]


# ## Read testing data 

# In[385]:


#MeoCollections.MEO_HST_RecData_379_2020-06-18
meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/MEO/MeoCollections_SOROS.MEO_HST_RecData_153_2020-06-24.csv",usecols=new_cols)


# In[20]:


#df['ViewData.Task Business Date']


# In[386]:


meo['ViewData.Status'].value_counts()


# In[387]:


df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)


# In[388]:


#df1[(df1['Date']=='2020-04-10') & (df1['ViewData.Side1_UniqueIds']=='996_125813417_Goldman Sachs')]
df1.shape


# In[389]:


#df1['close_key'] = df1['ViewData.Side0_UniqueIds'].astype(str) + df1['ViewData.Side1_UniqueIds'].astype(str)


# In[390]:



## Output for Closed breaks

closed_df = df1[(df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys))]


# In[391]:


closed_df.shape


# ## Machine generated output

# In[392]:


#df2 = df1[~df1['close_key'].isin(list(all_closed))]
df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]


# In[393]:


df = df2.copy()


# In[394]:


df = df.reset_index()
df = df.drop('index',1)


# In[395]:


df.shape


# In[396]:


#pd.set_option('display.max_columns', 500)


# In[397]:


df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[398]:


#df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[399]:


df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[400]:


pd.to_datetime(df['Date'])


# In[401]:


df['Date'] = pd.to_datetime(df['Date']).dt.date


# In[402]:


df['Date'] = df['Date'].astype(str)


# In[403]:


#df['ViewData.Status'].value_counts()


# In[404]:


df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB'])]
df = df.reset_index()
df = df.drop('index',1)


# In[405]:


#df1[df1['ViewData.Status']=='SMB']
df['ViewData.Status'].value_counts()


# In[406]:


df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[407]:


#df_170[(df_170['ViewData.Status']=='UMR')]


# In[408]:


df['Date'].value_counts()


# ## Sample data on one date

# In[51]:


#df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})


# In[409]:


sample = df[df['Date'] =='2020-06-25']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[410]:


smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
smb = smb.drop('index',1)


# In[411]:


smb_pb = smb.copy()
smb_acc = smb.copy()


# In[412]:


smb_pb['ViewData.Accounting Net Amount'] = np.nan
smb_pb['ViewData.Side0_UniqueIds'] = np.nan
smb_pb['ViewData.Status'] ='SMB-OB'

smb_acc['ViewData.B-P Net Amount'] = np.nan
smb_acc['ViewData.Side1_UniqueIds'] = np.nan
smb_acc['ViewData.Status'] ='SMB-OB'


# In[413]:


sample = sample[sample['ViewData.Status']!='SMB']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[414]:


sample.shape


# In[415]:


sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[416]:


#sample['ViewData.Status'].value_counts()


# In[417]:


sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


# In[418]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0


# In[419]:


#sample['ViewData.Status'].value_counts()


# In[420]:


#sample['flag_side1'].value_counts()


# In[421]:


sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'

sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 

sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
#sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)


sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','SMB-OB','CNF','CMF']))]

sample1 = sample1.reset_index()
sample1 = sample1.drop('index', 1)


# In[422]:


sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)


# In[423]:


sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[424]:


sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[425]:


#sample1['ViewData.Status'].value_counts()


# In[426]:


aa = sample1[sample1['Trans_side']=='A_side']
bb = sample1[sample1['Trans_side']=='B_side']


# In[427]:


#bb['ViewData.Source Combination'].value_counts()


# In[428]:


aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)

bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)


# In[429]:


aa = aa.reset_index()
aa = aa.drop('index', 1)
bb = bb.reset_index()
bb = bb.drop('index', 1)


# In[430]:


#'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds'
common_cols = ['ViewData.Accounting Net Amount', 'ViewData.Age',
'ViewData.Age WK', 'ViewData.Asset Type Category',
'ViewData.B-P Net Amount', 'ViewData.Base Net Amount','ViewData.CUSIP', 
 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag',
#'ViewData.Commission',
        'ViewData.Currency', 'ViewData.Custodian',
       'ViewData.Custodian Account',
       'ViewData.Description','ViewData.Department', 
              # 'ViewData.ExpiryDate', 
               'ViewData.Fund',
       'ViewData.ISIN',
       'ViewData.Investment Type',
      # 'ViewData.Keys',
       'ViewData.Mapped Custodian Account',
       'ViewData.Net Amount Difference',
       'ViewData.Net Amount Difference Absolute',
        #'ViewData.OTE Ticker',
        'ViewData.Price',
       'ViewData.Prime Broker', 'ViewData.Quantity',
       'ViewData.SEDOL', 'ViewData.SPM ID', 'ViewData.Settle Date',
       
  #  'ViewData.Strike Price',
               'Date',
       'ViewData.Ticker', 'ViewData.Trade Date',
       'ViewData.Transaction Category',
       'ViewData.Transaction Type', 'ViewData.Underlying Cusip',
       'ViewData.Underlying ISIN',
       'ViewData.Underlying Sedol','filter_key','ViewData.Status','ViewData.BreakID',
              'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData._ID']


# In[431]:


bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
bb = bb.reset_index()
bb = bb.drop('index',1)


# In[432]:


bb['ViewData.Status'].value_counts()


# In[433]:


bb.shape


###################### loop m*n ###############################
from pandas import merge
from tqdm import tqdm

pool =[]
key_index =[]
training_df =[]

no_pair_ids = []
#max_rows = 5

for d in tqdm(aa['Date'].unique()):
    aa1 = aa.loc[aa['Date']==d,:][common_cols]
    bb1 = bb.loc[bb['Date']==d,:][common_cols]
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    
    bb1 = bb1.sort_values(by='filter_key',ascending =True)
    
    for key in (list(np.unique(np.array(list(aa1['filter_key'].values) + list(bb1['filter_key'].values))))):
        
        df1 = aa1[aa1['filter_key']==key]
        df2 = bb1[bb1['filter_key']==key]

        if df1.empty == False and df2.empty == False:
            #aa_df = pd.concat([aa1[aa1.index==i]]*repeat_num, ignore_index=True)
            #bb_df = bb1.loc[pool[len(pool)-1],:][common_cols].reset_index()
            #bb_df = bb_df.drop('index', 1)

            df1 = df1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
            df2 = df2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})

            #dff  = pd.concat([aa[aa.index==i],bb.loc[pool[i],:][accounting_vars]],axis=1)

            df1 = df1.reset_index()
            df2 = df2.reset_index()
            df1 = df1.drop('index', 1)
            df2 = df2.drop('index', 1)

            df1.columns = ['SideA.' + x  for x in df1.columns] 
            df2.columns = ['SideB.' + x  for x in df2.columns]

            df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
            df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

            #dff = pd.concat([aa_df,bb_df],axis=1)
            dff = merge(df1, df2, on='filter_key')
            training_df.append(dff)
                #key_index.append(i)
            #else:
            #no_pair_ids.append([aa1[(aa1['filter_key']=='key') & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0]])
               # no_pair_ids.append(aa1[(aa1['filter_key']== key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0])
    
        else:
            no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
            no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
            

no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[435]:


#no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[436]:


#pd.DataFrame(no_pair_ids).rename


# In[437]:


len(no_pair_ids)


# In[438]:


#test_file['SideA.ViewData.Status'].value_counts()


# In[439]:


test_file = pd.concat(training_df)


# In[440]:


test_file = test_file.reset_index()
test_file = test_file.drop('index',1)


# In[441]:


test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')


# In[442]:


test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]


# In[443]:


test_file['SideA.ViewData.ISIN'] = test_file['SideA.ViewData.ISIN'].astype(str)
test_file['SideB.ViewData.ISIN'] = test_file['SideB.ViewData.ISIN'].astype(str)
test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].astype(str)
test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].astype(str)
test_file['SideA.ViewData.Currency'] = test_file['SideA.ViewData.Currency'].astype(str)
test_file['SideB.ViewData.Currency'] = test_file['SideB.ViewData.Currency'].astype(str)


test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(str)
test_file['SideB.ViewData.Trade Date'] = test_file['SideB.ViewData.Trade Date'].astype(str)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(str)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(str)
test_file['SideA.ViewData.Fund'] = test_file['SideA.ViewData.Fund'].astype(str)
test_file['SideB.ViewData.Fund'] = test_file['SideB.ViewData.Fund'].astype(str)


# In[444]:


#test_file[['SideA.ViewData.ISIN','SideB.ViewData.ISIN']]

def equals_fun(a,b):
    if a == b:
        return 1
    else:
        return 0

vec_equals_fun = np.vectorize(equals_fun)
values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
#test_file['ISIN_match'] = vec_equals_fun(values_ISIN_A_Side,values_ISIN_B_Side)

values_CUSIP_A_Side = test_file['SideA.ViewData.CUSIP'].values
values_CUSIP_B_Side = test_file['SideB.ViewData.CUSIP'].values
#
# values_CUSIP_A_Side = test_file['SideA.ViewData.Currency'].values
# values_CUSIP_B_Side = test_file['SideB.ViewData.Currency'].values

values_Currency_match_A_Side = test_file['SideA.ViewData.Currency'].values
values_Currency_match_B_Side = test_file['SideA.ViewData.Currency'].values

values_Trade_Date_match_A_Side = test_file['SideA.ViewData.Trade Date'].values
values_Trade_Date_match_B_Side = test_file['SideB.ViewData.Trade Date'].values

values_Settle_Date_match_A_Side = test_file['SideA.ViewData.Settle Date'].values
values_Settle_Date_match_B_Side = test_file['SideB.ViewData.Settle Date'].values

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

test_file['ISIN_match'] = vec_equals_fun(values_ISIN_A_Side,values_ISIN_B_Side)
test_file['CUSIP_match'] = vec_equals_fun(values_CUSIP_A_Side,values_CUSIP_B_Side)
test_file['Currency_match'] = vec_equals_fun(values_Currency_match_A_Side,values_Currency_match_B_Side)
test_file['Trade_Date_match'] = vec_equals_fun(values_Trade_Date_match_A_Side,values_Trade_Date_match_B_Side)
test_file['Settle_Date_match'] = vec_equals_fun(values_Settle_Date_match_A_Side,values_Settle_Date_match_B_Side)
test_file['Fund_match'] = vec_equals_fun(values_Fund_match_A_Side,values_Fund_match_B_Side)


# In[445]:


#test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)


# In[446]:


#test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)


# In[447]:


test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']


# ## Description code

# In[448]:


import os


# In[449]:


# In[450]:


print(os.getcwd())


# In[451]:


## TODO - Import a csv file for description category mapping

com = pd.read_csv('desc cat with naveen oaktree.csv')
#com


# In[452]:


cat_list = list(set(com['Pairing']))


# In[453]:


import re

def descclean(com,cat_list):
    cat_all1 = []
    list1 = cat_list
    m = 0
    if (type(com) == str):
        com = com.lower()
        com1 =  re.split("[,/. \-!?:]+", com)
        
        
        
        for item in list1:
            if (type(item) == str):
                item = item.lower()
                item1 = item.split(' ')
                lst3 = [value for value in item1 if value in com1] 
                if len(lst3) == len(item1):
                    cat_all1.append(item)
                    m = m+1
            
                else:
                    m = m
            else:
                    m = 0
    else:
        m = 0
    

            
    if m >0 :
        return list(set(cat_all1))
    else:
        if ((type(com)==str)):
            if (len(com1)<4):
                if ((len(com1)==1) & com1[0].startswith('20')== True):
                    return 'swap id'
                else:
                    return com
            else:
                return 'NA'
        else:
            return 'NA'


# In[454]:


#df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

test_file['SideA.desc_cat'] = test_file['SideA.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
test_file['SideB.desc_cat'] = test_file['SideB.ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[455]:


def currcln(x):
    if (type(x)==list):
        return x
      
    else:
       
        
        if x == 'NA':
            return "NA"
        elif (('dollar' in x) | ('dollars' in x )):
            return 'dollar'
        elif (('pound' in x) | ('pounds' in x)):
            return 'pound'
        elif ('yen' in x):
            return 'yen'
        elif ('euro' in x) :
            return 'euro'
        else:
            return x
        


# In[456]:



#df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))

test_file['SideA.desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : currcln(x))
test_file['SideB.desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : currcln(x))


# In[457]:


com = com.drop(['var','Catogery'], axis = 1)

com = com.drop_duplicates()

com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[458]:


def catcln1(cat,df):
    ret = []
    if (type(cat)==list):
        
        if 'equity swap settlement' in cat:
            ret.append('equity swap settlement')
        #return 'equity swap settlement'
        elif 'equity swap' in cat:
            ret.append('equity swap settlement')
        #return 'equity swap settlement'
        elif 'swap settlement' in cat:
            ret.append('equity swap settlement')
        #return 'equity swap settlement'
        elif 'swap unwind' in cat:
            ret.append('swap unwind')
        #return 'swap unwind'
   
    
    
    
        else:
        
       
            for item in cat:
            
                a = df[df['Pairing']==item]['replace'].values[0]
                if a not in ret:
                    ret.append(a)
        return list(set(ret))
      
    else:
        return cat


# In[459]:


test_file['SideA.new_desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : catcln1(x,com))
test_file['SideB.new_desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : catcln1(x,com))


# In[460]:


comp = ['inc','stk','corp ','llc','pvt','plc']
#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[461]:


#df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

def desccat(x):
    if isinstance(x, list):
        
        if 'equity swap settlement' in x:
            return 'swap settlement'
        elif 'collateral transfer' in x:
            return 'collateral transfer'
        elif 'dividend' in x:
            return 'dividend'
        elif (('loan' in x) & ('option' in x)):
            return 'option loan'
        
        elif (('interest' in x) & ('corp' in x) ):
            return 'corp loan'
        elif (('interest' in x) & ('loan' in x) ):
            return 'interest'
        else:
            return x[0]
    else:
        return x


# In[462]:


#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : desccat(x))
test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : desccat(x))


# In[463]:


#test_file['SideB.new_desc_cat'].value_counts()


# ## Prime Broker

# In[464]:


test_file['new_pb'] = test_file['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[465]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[466]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[467]:


test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].fillna('kkk')


# In[468]:


test_file['new_pb1'] = test_file.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)


# In[469]:


#test_file = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/X_test_files_after_loop/meo_testing_HST_RecData_379_06_19_2020_test_file_with_ID.csv')


# In[470]:


#test_file = test_file.drop('Unnamed: 0',1)


# In[471]:


test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days

test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days


# In[472]:


def mhreplaced(item):
    word1 = []
    word2 = []
    if (type(item) == str):
    
        for items in item.split(' '):
            if (type(items) == str):
                items = items.lower()
                if items.isdigit() == False:
                    word1.append(items)
        
            
                for c in word1:
                    if c.endswith('MH')==False:
                        word2.append(c)
    
                words = ' '.join(word2)
                return words
    else:
        return item
    

def fundmatch(item):
    items = item.lower()
    items = item.replace(' ','') 
    return items


# In[473]:



############ Fund match new ########

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

vec_fund_match = np.vectorize(fundmatch)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)


# In[474]:


### New code for cleaning text variables 

import pandas as pd
#import dask.dataframe as dd
#import glob
#import math
#from sklearn.feature_extraction.text import TfidfVectorizer
from dateutil.parser import parse
#import operator
#import itertools
#from sklearn.feature_extraction.text import CountVectorizer
#import re
import os
import numpy as np


#column_names = ['SideA.ViewData.Transaction Type', 'ViewData.Investment Type', 'ViewData.Asset Type Category', 'ViewData.Prime Broker', 'ViewData.Description']

trans_type_A_side = test_file['SideA.ViewData.Transaction Type']
trans_type_B_side = test_file['SideB.ViewData.Transaction Type']

asset_type_cat_A_side = test_file['SideA.ViewData.Asset Type Category']
asset_type_cat_B_side = test_file['SideB.ViewData.Asset Type Category']

invest_type_A_side = test_file['SideA.ViewData.Investment Type']
invest_type_B_side = test_file['SideB.ViewData.Investment Type']

prime_broker_A_side = test_file['SideA.ViewData.Prime Broker']
prime_broker_B_side = test_file['SideB.ViewData.Prime Broker']


# In[475]:


# LOWER CASE
trans_type_A_side = [str(item).lower() for item in trans_type_A_side]
trans_type_B_side = [str(item).lower() for item in trans_type_B_side]

asset_type_cat_A_side = [str(item).lower() for item in asset_type_cat_A_side]
asset_type_cat_B_side = [str(item).lower() for item in asset_type_cat_B_side]

invest_type_A_side = [str(item).lower() for item in invest_type_A_side]
invest_type_B_side = [str(item).lower() for item in invest_type_B_side]

prime_broker_A_side = [str(item).lower() for item in prime_broker_A_side]
prime_broker_B_side = [str(item).lower() for item in prime_broker_B_side]


# In[476]:


split_trans_A_side = [item.split() for item in trans_type_A_side]
split_trans_B_side = [item.split() for item in trans_type_B_side]


split_asset_A_side = [item.split() for item in asset_type_cat_A_side]
split_asset_B_side = [item.split() for item in asset_type_cat_B_side]


split_invest_A_side = [item.split() for item in invest_type_A_side]
split_invest_B_side = [item.split() for item in invest_type_B_side]

split_prime_A_side = [item.split() for item in prime_broker_A_side]
split_prime_b_side = [item.split() for item in prime_broker_B_side]

# In[310]:


def is_num(item):
    try:
        float(item)
        return True
    except ValueError:
        return False

def is_date_format(item):
    try:
        parse(item, fuzzy=False)
        return True
    
    except ValueError:
        return False
    
def date_edge_cases(item):
    if len(item) == 5 and item[2] =='/' and is_num(item[:2]) and is_num(item[3:]):
        return True
    return False


# In[477]:


## Transacion type

remove_nums_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_trans_A_side]
remove_nums_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_trans_B_side]

#remove_dates_A_side = [[item for item in sublist if not is_date(item)] for sublist in remove_nums_A_side]

#remove_dates_B_side = [[item for item in sublist if not is_date(item)] for sublist in remove_nums_B_side]

remove_dates_A_side = [[item for item in sublist if not (is_date_format(item) or date_edge_cases(item))] for sublist in remove_nums_A_side]
remove_dates_B_side = [[item for item in sublist if not (is_date_format(item) or date_edge_cases(item))] for sublist in remove_nums_B_side]


# Specific to clients already used on, will have to be edited for other edge cases
remove_amts_A_side = [[item for item in sublist if item[0] != '$'] for sublist in remove_dates_A_side]
remove_amts_B_side = [[item for item in sublist if item[0] != '$'] for sublist in remove_dates_B_side]


clean_adr_A_side = [(['ADR'] if 'adr' in item else item) for item in remove_amts_A_side]
clean_adr_B_side = [(['ADR'] if 'adr' in item else item) for item in remove_amts_B_side]

clean_tax_A_side = [(item[:2] if '30%' in item else item) for item in clean_adr_A_side]
clean_tax_B_side = [(item[:2] if '30%' in item else item) for item in clean_adr_B_side]

remove_ons_A_side = [(item[:item.index('on')] if 'on' in item else item) for item in clean_tax_A_side]
remove_ons_B_side = [(item[:item.index('on')] if 'on' in item else item) for item in clean_tax_B_side]

clean_eqswap_A_side = [(item[1:] if 'eqswap' in item else item) for item in remove_ons_A_side]
clean_eqswap_B_side = [(item[1:] if 'eqswap' in item else item) for item in remove_ons_B_side]

remove_mh_A_side = [[item for item in sublist if 'mh' not in item] for sublist in clean_eqswap_A_side]
remove_mh_B_side = [[item for item in sublist if 'mh' not in item] for sublist in clean_eqswap_B_side]

remove_ats_A_side = [(item[:item.index('@')] if '@' in item else item) for item in remove_mh_A_side]
remove_ats_B_side = [(item[:item.index('@')] if '@' in item else item) for item in remove_mh_B_side]

#remove_blanks_A_side = [item for item in remove_ats_A_side if item]
#remove_blanks_B_side = [item for item in remove_ats_B_side if item]


# In[319]:

cleaned_trans_types_A_side = [' '.join(item) for item in remove_ats_A_side]
cleaned_trans_types_B_side = [' '.join(item) for item in remove_ats_B_side]


# In[478]:


# # INVESTMENT TYPE

# # In[322]:


remove_nums_i_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_A_side]
remove_nums_i_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_B_side]

remove_dates_i_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_A_side]
remove_dates_i_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_B_side]

#remove_blanks_i_A_side = [item for item in remove_dates_i_A_side if item]
#remove_blanks_i_B_side = [item for item in remove_dates_i_B_side if item]
#remove_blanks_i[:10]


# # In[323]:

cleaned_invest_A_side = [' '.join(item) for item in remove_dates_i_A_side]
cleaned_invest_B_side = [' '.join(item) for item in remove_dates_i_B_side]


# In[479]:



remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]
# remove_blanks_a = [item for item in remove_dates_a if item]
# # remove_blanks_a[:10]


# # In[321]:

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]


# In[480]:


test_file['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
test_file['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

test_file['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
test_file['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

test_file['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
test_file['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side


# In[481]:


#test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[482]:


#test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[483]:


##############

values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values

vec_tt_match = np.vectorize(mhreplaced)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)


# In[484]:


test_file.loc[test_file['SideA.ViewData.Transaction Type']=='int','SideA.ViewData.Transaction Type'] = 'interest'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='wires','SideA.ViewData.Transaction Type'] = 'wire'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='dividends','SideA.ViewData.Transaction Type'] = 'dividend'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='miscellaneous','SideA.ViewData.Transaction Type'] = 'misc'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='div','SideA.ViewData.Transaction Type'] = 'dividend'


# In[485]:


test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[ ]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[486]:


test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)


# In[487]:


#train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)


# In[488]:


test_file['Combined_Investment_Type'] = test_file['SideA.ViewData.Investment Type'].astype(str) + test_file['SideB.ViewData.Investment Type'].astype(str)


# In[489]:


test_file['Combined_Asset_Type_Category'] = test_file['SideA.ViewData.Asset Category Type'].astype(str) + test_file['SideB.ViewData.Asset Category Type'].astype(str)


# In[490]:


def nan_fun(x):
    if x=='nan':
        return 1
    else:
        return 0
    
vec_nan_fun = np.vectorize(nan_fun)
values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
test_file['SideA.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)
test_file['SideB.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)

#test_file['SideA.ISIN_NA'] =  test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']=='nan' else 0, axis=1)
#test_file['SideB.ISIN_NA'] =  test_file.apply(lambda x: 1 if x['SideB.ViewData.ISIN']=='nan' else 0, axis=1)


# In[491]:


len(test_file['SideB.ViewData.CUSIP'].values)


# In[ ]:





# In[492]:


def a_keymatch(a_cusip, a_isin):
    
    pb_nan = 0
    a_common_key = 'NA' 
    if a_cusip=='nan' and a_isin =='nan':
        pb_nan =1
    elif(a_cusip!='nan' and a_isin == 'nan'):
        a_common_key = a_cusip
    elif(a_cusip =='nan' and a_isin !='nan'):
        a_common_key = a_isin
    else:
        a_common_key = a_isin
        
    return (pb_nan, a_common_key)

def b_keymatch(b_cusip, b_isin):
    accounting_nan = 0
    b_common_key = 'NA'
    if b_cusip =='nan' and b_isin =='nan':
        accounting_nan =1
    elif (b_cusip!='nan' and b_isin == 'nan'):
        b_common_key = b_cusip
    elif(b_cusip =='nan' and b_isin !='nan'):
        b_common_key = b_isin
    else:
        b_common_key = b_isin
    return (accounting_nan, b_common_key)

    
vec_a_key_match_fun = np.vectorize(a_keymatch)
vec_b_key_match_fun = np.vectorize(b_keymatch)

values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values

values_CUSIP_A_Side = test_file['SideA.ViewData.CUSIP'].values
values_CUSIP_B_Side = test_file['SideB.ViewData.CUSIP'].values

test_file['SideB.ViewData.key_NAN']= vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[0]
test_file['SideB.ViewData.Common_key'] = vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[1]
test_file['SideA.ViewData.key_NAN'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[0]
test_file['SideA.ViewData.Common_key'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[1]


# In[493]:


#test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
#test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[494]:


def nan_equals_fun(a,b):
    if a==1 and b==1:
        return 1
    else:
        return 0
    
vec_nan_equal_fun = np.vectorize(nan_equals_fun)
values_key_NAN_B_Side = test_file['SideB.ViewData.key_NAN'].values
values_key_NAN_A_Side = test_file['SideA.ViewData.key_NAN'].values
test_file['All_key_nan'] = vec_nan_equal_fun(values_key_NAN_B_Side,values_key_NAN_A_Side )

#test_file['All_key_nan'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.key_NAN']==1 and x['SideA.ViewData.key_NAN']==1 else 0, axis=1)


# In[495]:


test_file['SideB.ViewData.Common_key'] = test_file['SideB.ViewData.Common_key'].astype(str)
test_file['SideA.ViewData.Common_key'] = test_file['SideA.ViewData.Common_key'].astype(str)


def new_key_match_fun(a,b,c):
    if a==b and c==0:
        return 1
    else:
        return 0
    
vec_new_key_match_fun = np.vectorize(new_key_match_fun)
values_Common_key_B_Side = test_file['SideB.ViewData.Common_key'].values
values_Common_key_A_Side = test_file['SideA.ViewData.Common_key'].values
values_All_key_NAN = test_file['All_key_nan'].values

test_file['new_key_match']= vec_new_key_match_fun(values_Common_key_B_Side,values_Common_key_A_Side,values_All_key_NAN)


# In[496]:


test_file['amount_percent'] = (test_file['SideA.ViewData.B-P Net Amount']/test_file['SideB.ViewData.Accounting Net Amount']*100)


# In[497]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())


# In[498]:


test_file['SideB.ViewData.Prime Broker'] = test_file['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())


# In[499]:


test_file['SideB.ViewData.Asset Type Category'] = test_file['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Asset Type Category'] = test_file['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())


# In[ ]:


test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))


# In[500]:


#test_file

test_file['ViewData.Combined Transaction Type'] = test_file['ViewData.Combined Transaction Type'].apply(lambda x: x.replace('jnl','journal'))


# In[501]:



#test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)


# In[502]:


#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/X_test_files_after_loop/meo_testing_HST_RecData_170_06-18-2020_test_file.csv")


# In[503]:





# In[504]:


trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']


# In[505]:


test_file['SideA.TType'] = test_file.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)
test_file['SideB.TType'] = test_file.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)


# In[506]:


test_file['Combined_Desc'] = test_file['SideA.new_desc_cat'] + test_file['SideB.new_desc_cat']


# In[507]:


test_file['Combined_TType'] = test_file['SideA.TType'].astype(str) + test_file['SideB.TType'].astype(str)


# In[508]:


from fuzzywuzzy import fuzz


# In[509]:


#import re
def  clean_text(df, text_field, new_text_field_name):
    df[text_field] = df[text_field].astype(str)
    df[new_text_field_name] = df[text_field].str.lower()
    
    
    
    df[new_text_field_name] = df[new_text_field_name].apply(lambda x: re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", x))  
    # remove numbers
    df[new_text_field_name] = df[new_text_field_name].apply(lambda x: re.sub(r"\d+", "", x))
    df[new_text_field_name] = df[new_text_field_name].str.replace('usd','')
    df[new_text_field_name] = df[new_text_field_name].str.replace('eur0','')
    df[new_text_field_name] = df[new_text_field_name].str.replace(' usd','')
    df[new_text_field_name] = df[new_text_field_name].str.replace(' euro','')

    df[new_text_field_name] = df[new_text_field_name].str.replace(' eur','')
    df[new_text_field_name] = df[new_text_field_name].str.replace('eur','')
    
    return df


# In[510]:


test_file =  clean_text(test_file,'SideA.ViewData.Description', 'SideA.ViewData.Description_new') 
test_file =  clean_text(test_file,'SideB.ViewData.Description', 'SideB.ViewData.Description_new') 


# In[511]:


test_file['description_similarity_score'] = test_file.apply(lambda x: fuzz.token_sort_ratio(x['SideA.ViewData.Description_new'], x['SideB.ViewData.Description_new']), axis=1)


# In[512]:


import pandas as pd
#import xgboost as xgb
#from sklearn.preprocessing import LabelEncoder
import numpy as np

#le = LabelEncoder()
for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    test_file[feature] = pd.to_datetime(test_file[feature],errors = 'coerce').dt.weekday


# In[513]:


model_cols = ['SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
             # 'SideA.new_desc_cat',
              #'SideA.ViewData.Description',
            # 'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              
              'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
              #'SideB.ViewData.Description',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
            'Amount_diff_2', 
              'Trade_date_diff', 
            'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Investment_Type','Combined_Asset_Type_Category',
              'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
              'All_key_nan','new_key_match', 'new_pb1','Combined_TType',
              #'SideB.Date',
                 'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',          
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side']
             # 'label']


# ## UMR Mapping

# In[514]:



## TODO Import HIstorical UMR FILE for Transaction Type mapping

Soros_umr = pd.read_csv('Soros_UMR.csv')


# In[515]:


#soros_umr['ViewData.Combined Transaction Type'].unique()


# In[516]:


test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in Soros_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)


# In[517]:


test_file['abs_amount_flag'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)


# In[518]:


test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]

test_file = test_file.reset_index().drop('index',1)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int)


# In[994]:


test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))


# In[519]:


#test_file2 = test_file[((test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")) | ((test_file['SideA.TType']!="Trade") & (test_file['SideB.TType']!="Trade")) ]


# In[520]:


#test_file2 = test_file[(test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")]


# In[141]:


#test_file[(test_file['SideA.TType']==test_file['SideB.TType'])]['SideB.TType']


# In[142]:


#test_file2 = test_file2.reset_index()
#test_file2 = test_file2.drop('index',1)


# In[143]:


#test_file['SideA.ViewData.BreakID_A_side'].value_counts()
#test_file[model_cols]


# In[521]:


test_file


# ## Test file served into the model

# In[698]:


test_file2 = test_file.copy()


# In[699]:


X_test = test_file2[model_cols]


# In[700]:


X_test = X_test.reset_index()
X_test = X_test.drop('index',1)
X_test = X_test.fillna(0)


# In[701]:


X_test = X_test.fillna(0)


# In[702]:


X_test.shape


# In[703]:


X_test = X_test.drop_duplicates()
X_test = X_test.reset_index()
X_test = X_test.drop('index',1)


# In[704]:


X_test.shape


# ## Model Pickle file import

# In[705]:



## TODO Import Pickle file for 1st Model

import pickle


# In[706]:


#filename = 'Oak_W125_model_with_umb.sav'
#filename = '125_with_umb_without_des_and_many_to_many.sav'
#filename = '125_with_umb_and_price_without_des_and_many_to_many_tdsd2.sav'
#filename = 'Weiss_new_model_V1.sav'
#filename = 'Soros_new_model_V1_with_close.sav'
#filename = 'Soros_full_model_smote.sav'

#filename = 'Soros_full_model_best_cleaned_tt_without_date.sav'
#filename = 'Soros_full_model_version2.sav'
#filename = 'OakTree_final_model2.sav'
filename = 'Soros_final_model2.sav'

#filename = 'Soros_full_model_umr_umt.sav'

clf = pickle.load(open(filename, 'rb'))


# In[707]:


X_test


# ## Predictions

# In[708]:


# Actual class predictions
rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[709]:


probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[710]:


X_test['Predicted_action'] = rf_predictions
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test['probability_No_pair'] = probability_class_0
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test['probability_UMB'] = probability_class_1
X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[711]:


X_test['Predicted_action'].value_counts()


# In[712]:


X_test['Predicted_action'].value_counts()


# ## Two Step Modeling

# In[713]:


model_cols_2 =[
    #'SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
             # 'SideA.new_desc_cat',
              #'SideA.ViewData.Description',
            # 'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              
             # 'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
              #'SideB.ViewData.Description',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
           # 'Amount_diff_2', 
              'Trade_date_diff', 
            'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Investment_Type','Combined_Asset_Type_Category',
              'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
              'All_key_nan','new_key_match', 'new_pb1','Combined_TType',
              #'SideB.Date',
                 'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',          
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side']
             # 'label']
         


# In[714]:


X_test2 = test_file[model_cols_2]


# In[715]:


X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)
X_test2 = X_test2.fillna(0)


# In[716]:


X_test2.shape


# In[717]:


X_test2 = X_test2.drop_duplicates()
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)


# In[718]:


X_test2.shape


# In[719]:


#filename2 = 'Soros_full_model_all_two_step.sav'

## TODO Import MOdel2 as per the two step modelling process

#filename2 = 'OakTree_final_model2_step_two.sav'
filename2 = 'Soros_final_model2_step_two.sav'
clf2 = pickle.load(open(filename2, 'rb'))


# In[720]:


# Actual class predictions
rf_predictions2 = clf2.predict(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs2 = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[721]:


probability_class_0_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

#probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[722]:


X_test2['Predicted_action_2'] = rf_predictions2
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test2['probability_No_pair_2'] = probability_class_0_two
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test2['probability_UMB_2'] = probability_class_1_two
#X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[723]:


X_test2['Predicted_action_2'].value_counts()


# In[724]:


X_test.shape


# In[725]:


X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)


# In[726]:


X_test


# ## New Aggregation

# In[727]:


X_test['Tolerance_level'] = np.abs(X_test['probability_UMB_2'] - X_test['probability_No_pair_2'])


# In[728]:


#X_test[X_test['Tolerance_level']<0.1]['Predicted_action'].value_counts()


# In[729]:


b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# ## UMR segregation

# In[730]:


def umr_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']==1) & (b_count2['len']<=3)]
    return umr_table['SideB.ViewData.Side0_UniqueIds'].values
    


# In[731]:


umr_ids_0 = umr_seg(X_test)


# ## 1st Prediction Table for One to One UMR

# In[732]:


final_umr_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')]


# In[733]:


final_umr_table = final_umr_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]


# In[734]:


final_umr_table.shape


# In[735]:


final_umr_table


# ## No-Pair segregation

# In[736]:


#b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
#a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[737]:


def no_pair_seg(X_test):
    
    b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
    a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()
    
    b_side_agg['len'] = b_side_agg['Predicted_action_2'].str.len()
    b_side_agg['No_Pair_flag'] = b_side_agg['Predicted_action_2'].apply(lambda x: 1 if 'No-Pair' in x else 0)

    a_side_agg['len'] = a_side_agg['Predicted_action_2'].str.len()
    a_side_agg['No_Pair_flag'] = a_side_agg['Predicted_action_2'].apply(lambda x: 1 if 'No-Pair' in x else 0)
    
    no_pair_ids_b_side = b_side_agg[(b_side_agg['len']==1) & (b_side_agg['No_Pair_flag']==1)]['SideB.ViewData.Side0_UniqueIds'].values

    no_pair_ids_a_side = a_side_agg[(a_side_agg['len']==1) & (a_side_agg['No_Pair_flag']==1)]['SideA.ViewData.Side1_UniqueIds'].values
    
    return no_pair_ids_b_side, no_pair_ids_a_side


# In[738]:


no_pair_ids_b_side, no_pair_ids_a_side = no_pair_seg(X_test)


# In[739]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]['Predicted_action_2'].value_counts()


# In[740]:


X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[741]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]['Predicted_action_2'].value_counts()


# In[742]:


final_open_table = X_test[(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]


# In[743]:


final_open_table = final_open_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[744]:


final_open_table['probability_UMR'] = 0.00010
final_open_table = final_open_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[745]:


final_open_table.shape


# In[746]:


#final_open_table.head()

len(no_pair_ids_b_side)


# In[747]:


b_side_open_table = final_open_table.groupby('SideB.ViewData.Side0_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()
a_side_open_table = final_open_table.groupby('SideA.ViewData.Side1_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()


# In[748]:


a_side_open_table = a_side_open_table[a_side_open_table['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]
b_side_open_table = b_side_open_table[b_side_open_table['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)]


# In[749]:


b_side_open_table = b_side_open_table.reset_index().drop('index',1)
a_side_open_table = a_side_open_table.reset_index().drop('index',1)


# In[750]:


final_no_pair_table = pd.concat([a_side_open_table,b_side_open_table], axis=0)
final_no_pair_table = final_no_pair_table.reset_index().drop('index',1)


# In[751]:


final_open_table


# In[752]:


final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.BreakID_A_side']].drop_duplicates(), on = 'SideA.ViewData.Side1_UniqueIds', how='left')
final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideB.ViewData.Side0_UniqueIds','SideB.ViewData.BreakID_B_side']].drop_duplicates(), on = 'SideB.ViewData.Side0_UniqueIds', how='left')


# In[753]:


final_no_pair_table


# In[754]:


#actual_closed = pd.read_csv('D:\Raman  Strategy ML 2.0\All_Data\OakTree\JuneData\Final_Predictions_379\Final_Predictions_Table_HST_RecData_379_2020-06-14.csv')


# In[755]:


#actual_closed_array = np.array(list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side0_UniqueIds'].unique()) + list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side1_UniqueIds'].unique()))


# In[756]:


#X_test_umb3 = X_test_umb[~((X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(actual_closed_array)) | (X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(actual_closed_array)))]


# ## Remove Open Ids

# In[878]:


umr_ids_a_side = final_umr_table['SideA.ViewData.Side1_UniqueIds'].unique()
umr_ids_b_side = final_umr_table['SideB.ViewData.Side0_UniqueIds'].unique()

### Remove Open IDs

X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

## Remove UMR IDs

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]



X_test_left = X_test_left.reset_index().drop('index',1)


# In[879]:


X_test.shape


# In[880]:


X_test_left.shape


# In[881]:


X_test_left['Predicted_action_2'].value_counts()


# ## One to One UMB segregation

# In[579]:


X_test_left['Predicted_action_2'].value_counts()


# In[580]:


### IDs left after removing UMR ids from 0 and 1 side

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]


# In[581]:


X_test_left.shape


# In[582]:


X_test_left['Predicted_action_2'].value_counts()


# In[583]:


X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
X_test_left = X_test_left.reset_index().drop('index',1)


# In[584]:


for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():
    umb_ids_1 = X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.Side1_UniqueIds'].unique()


# In[585]:


X_test_left['SideB.ViewData.Side0_UniqueIds'].value_counts()


# ## UMR One to Many and Many to One 

# ### One to Many

# In[601]:


#X_test_left = X_test.copy()


# In[768]:


cliff_for_loop = 16


# In[769]:


threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()


# In[770]:


exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()


# In[771]:


import numpy as np

def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total,atol=0.25).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
        

        
        
#null_value ='No'
many_ids_1 = []
one_id_0 = []
amount_array =[]
for key in X_test[~X_test['SideB.ViewData.Side0_UniqueIds'].isin(exceptions_0_umb_ids)]['SideB.ViewData.Side0_UniqueIds'].unique():
    print(key)
    
    if key in threshold_0_umb:

        values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
        net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass
            
    else:
        values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.B-P Net Amount'].values
        net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass


# In[772]:


umr_otm_table = pd.DataFrame(one_id_0)
umr_otm_table.columns = ['SideB.ViewData.Side0_UniqueIds']

umr_otm_table['SideA.ViewData.Side1_UniqueIds'] =many_ids_1


# In[774]:


umr_otm_table


# In[778]:


X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds'] =='1146_153157324_Advent Geneva') & (X_test_left['SideA.ViewData.Side1_UniqueIds']=='1374_153157324_CITI')][['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.B-P Net Amount']]


# ### Many to One

# In[ ]:


cliff_for_loop = 16


# In[824]:


threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()


# In[825]:


exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()


# In[826]:


import numpy as np

def subSum(numbers,total):
    for length in range(1, 4):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total,atol=0.25).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
        

#null_value ='No'
many_ids_0 = []
one_id_1 = []
amount_array2 =[]
for key in X_test[~X_test['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)]['SideA.ViewData.Side1_UniqueIds'].unique():
    #if key not in ['1174_379879573_State Street','201_379823765_State Street']:
    print(key)
    if key in threshold_1_umb:

        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()

        if subSum(values2,net_sum2) == []: 
            amount_array2 =[]
            #print("There are no valid subsets.")

        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass

    else:
        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()

        if subSum(values2,net_sum2) == []: 
            amount_array2 =[]
            #print("There are no valid subsets.")

        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass
            


# In[827]:


#many_ids_0


# In[828]:


one_id_1


# In[829]:


umr_mto_table = pd.DataFrame(one_id_1)
umr_mto_table.columns = ['SideA.ViewData.Side1_UniqueIds']

umr_mto_table['SideB.ViewData.Side0_UniqueIds'] =many_ids_0 


# In[830]:


umr_mto_table


# In[831]:


#sample_array = ['192_153157324_CITI','543_153157324_CITI','561_153157324_CITI','705_153157324_CITI','719_153157324_CITI',
# '775_153157324_CITI','1179_153157324_CITI','294_153157324_CITI','989_153157324_CITI','697_153157324_CITI',
# '1027_153157324_CITI','756_153157324_CITI','949_153157324_CITI','963_153157324_CITI','925_153157324_CITI',
# '395_153157324_CITI','362_153157324_CITI','413_153157324_CITI','1331_153157324_CITI','1077_153157324_CITI',
# '1092_153157324_CITI','1170_153157324_CITI','1066_153157324_CITI','793_153157324_CITI','1116_153157324_CITI',
# '1098_153157324_CITI','1176_153157324_CITI','1237_153157324_CITI','1354_153157324_CITI','1061_153157324_CITI',
# '1361_153157324_CITI','1347_153157324_CITI','842_153157324_CITI','1200_153157324_CITI','732_153157324_CITI',
# '857_153157324_CITI','679_153157324_CITI','779_153157324_CITI','967_153157324_CITI','776_153157324_CITI','843_153157324_CITI',
# '809_153157324_CITI','1139_153157324_CITI','662_153157324_CITI','789_153157324_CITI','741_153157324_CITI',
# '748_153157324_CITI','1005_153157324_CITI','916_153157324_CITI','847_153157324_CITI','483_153157324_CITI',
# '567_153157324_CITI','995_153157324_CITI','904_153157324_CITI','858_153157324_CITI','1070_153157324_CITI',
# '1181_153157324_CITI','549_153157324_CITI','1108_153157324_CITI','747_153157324_CITI','480_153157324_CITI',
# '890_153157324_CITI','1022_153157324_CITI','112_153157324_CITI','765_153157324_CITI','838_153157324_CITI',
# '1172_153157324_CITI','1057_153157324_CITI','999_153157324_CITI','976_153157324_CITI','1351_153157324_CITI',
# '1338_153157324_CITI','1352_153157324_CITI','416_153156564_Credit suisse']


# In[832]:


#len(sample_array)
#
#
## In[833]:
#
#
#X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(sample_array)]['SideA.ViewData.Side1_UniqueIds'].nunique()
#
#
## In[834]:
#
#
#umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(sample_array)].shape


# In[942]:


umr_mto_table


# In[957]:



# ## Removing all the OTM and MTO Ids

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(list(np.concatenate(many_ids_0))))]
X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(one_id_1)))]
X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(np.concatenate(many_ids_1))))]
X_test_left2 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(list(one_id_0)))]
X_test_left2 = X_test_left2.reset_index().drop('index',1)


# In[884]:


X_test_left2.shape


# ## UMB one to one (final)

# In[885]:


X_test_left2.shape


# In[886]:


X_test_umb = X_test_left2[X_test_left2['Predicted_action_2']=='UMB_One_to_One']
X_test_umb = X_test_umb.reset_index().drop('index',1)


# In[887]:


X_test_umb.shape


# In[888]:


#X_test_umb['UMB_key_OTO'] = X_test_umb['SideA.ViewData.Side1_UniqueIds'] + X_test_umb['SideB.ViewData.Side0_UniqueIds']


# In[889]:


def one_to_one_umb(data):
    
    count = data['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count0')
    id0s = count[count['count0']==1]['index'].unique()
    id1s = data[data['SideB.ViewData.Side0_UniqueIds'].isin(id0s)]['SideA.ViewData.Side1_UniqueIds']
    
    count1 = data['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count1')
    final_ids = count1[(count1['count1']==1) & (count1['index'].isin(id1s))]['index'].unique()
    return final_ids
    
    


# In[890]:


one_side_unique_umb_ids = one_to_one_umb(X_test_umb)


# In[891]:


final_oto_umb_table = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]


# In[892]:


final_oto_umb_table = final_oto_umb_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[893]:


final_oto_umb_table['probability_UMR'] = 0.00010
final_oto_umb_table = final_oto_umb_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[898]:


final_oto_umb_table


# ## Removing IDs from OTO UMB

# In[901]:


X_test_left2.shape


# In[902]:


X_test_left3 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(final_oto_umb_table['SideB.ViewData.Side0_UniqueIds']))]
X_test_left3 = X_test_left3[~(X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(final_oto_umb_table['SideA.ViewData.Side1_UniqueIds']))]


X_test_left3 = X_test_left3.reset_index().drop('index',1)


# In[903]:


X_test_left3.shape


# In[1026]:


# ## UMB One to Many and Many to One

## Total IDs 

def no_pair_seg2(X_test):
    
    b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_side_agg['len'] = b_side_agg['Predicted_action'].str.len()
    b_side_agg['No_Pair_flag'] = b_side_agg['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)

    a_side_agg['len'] = a_side_agg['Predicted_action'].str.len()
    a_side_agg['No_Pair_flag'] = a_side_agg['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
    
    no_pair_ids_b_side = b_side_agg[(b_side_agg['len']==1) & (b_side_agg['No_Pair_flag']==1)]['SideB.ViewData.Side0_UniqueIds'].values

    no_pair_ids_a_side = a_side_agg[(a_side_agg['len']==1) & (a_side_agg['No_Pair_flag']==1)]['SideA.ViewData.Side1_UniqueIds'].values
    
    return no_pair_ids_b_side, no_pair_ids_a_side


# In[917]:


open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)


# In[918]:


X_test_left3[~X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)]


# In[ ]:





# In[921]:


X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]


# In[923]:


X_test_left4 = X_test_left4.reset_index().drop('index',1)


# In[ ]:




#rr2 = X_test_umb2.groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
#rr2['SideA.ViewData.Side1_UniqueIds'] = rr2['SideA.ViewData.Side1_UniqueIds'].apply(tuple)
#
#rr2.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()
#
