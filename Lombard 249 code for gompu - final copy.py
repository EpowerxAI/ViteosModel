#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 16:11:24 2020

@author: consultant138
"""
import timeit
start = timeit.default_timer()

import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE

import os
os.chdir('D:\\ViteosModel')

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
from pandas import merge,DataFrame

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

from fuzzywuzzy import fuzz
import random
import decimal

# Function1
def subSum(numbers,total):
    length = len(numbers)
    
    
    
    if length <16:
      
            
      
        
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=5.0).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers

def amt_marker(x,y,z):
    if type(y)==list:
        if ((x in y) & ((z<16) & (z>=2))) :
            return 1
        else:
            return 0
    else:
        return 0

def remove_mark(x,z,k):
    
   
    if ((x>1) & (x<16)):
        if ((k<6.0)):
            return 1
#         elif ((k==0.0) & (z!=0)):
#             return 1
        else:
            return 0
    else:
        return 0

def mtm(x,y):
    if ((pd.isnull(x)==False) & (pd.isnull(y)==False)):
        y1 = y.split(',')
        x1 = x.split(',')
        return pd.Series([len(x1),len(y1)], index=['len_0', 'len_1'])
    elif ((pd.isnull(x)==False) & (pd.isnull(y)==True)):
        x1 = x.split(',')
        
        return pd.Series([len(x1),0], index=['len_0', 'len_1'])
    elif ((pd.isnull(x)==True) & (pd.isnull(y)==False)):
        y1 = y.split(',')
        
        return pd.Series([0,len(y1)], index=['len_0', 'len_1'])
        
    else:
        
        
        return pd.Series([0,0], index=['len_0', 'len_1'])

def mtm_mark(x,y):
    if ((x>1) &(y>1)):
        return 'MTM'
    elif((x==1) &(y==1)):
        return 'OTO'
    elif((x>1) &(y==1)):
        return 'MTO'
    elif((x==1) &(y>1)):
        return 'OTM'
    else:
        return 'OB'

def normalize_bp_acct_col_names(fun_df):
    bp_acct_col_names_mapping_dict = {
                                      'ViewData.Cust Net Amount' : 'ViewData.B-P Net Amount',
                                      'ViewData.Cust Net Amount Difference' : 'ViewData.B-P Net Amount Difference',
                                      'ViewData.Cust Net Amount Difference Absolute' : 'ViewData.B-P Net Amount Difference Absolute',
                                      'ViewData.CP Net Amount' : 'ViewData.B-P Net Amount',
                                      'ViewData.CP Net Amount Difference' : 'ViewData.B-P Net Amount Difference',
                                      'ViewData.CP Net Amount Difference Absolute' : 'ViewData.B-P Net Amount Difference Absolute',
                                      'ViewData.PMSVendor Net Amount' : 'ViewData.Accounting Net Amount'
                                        }
    fun_df.rename(columns = bp_acct_col_names_mapping_dict, inplace = True)
    return(fun_df)

client = 'Lombard'

setup = '249'
setup_code = '249'
#filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]

mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
today = date.today()
d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
desired_date = d1 - timedelta(days=4)
desired_date_str = desired_date.strftime("%Y-%m-%d")
date_input = desired_date_str


#for setup_code in setup_code_list:
print('Starting predictions for Weiss, setup_code = ')
print(setup_code)


#filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_input) + '.csv'

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
#
#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/meo_df_setup_125_date_2020-11-25.csv'
#meo_df = pd.read_csv(meo_filename)
meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)

meo_df['Date'] = pd.to_datetime(meo_df['ViewData.Task Business Date'])

meo_df = meo_df[~meo_df['Date'].isnull()]
meo_df = meo_df.reset_index()
meo_df = meo_df.drop('index',1)

meo_df['Date'] = pd.to_datetime(meo_df['Date']).dt.date

meo_df['Date'] = meo_df['Date'].astype(str)

## ## Sample data on one date
#
print('The Date value count is:')
print(meo_df['Date'].value_counts())

date_i = meo_df['Date'].mode()[0]

print('Choosing the date : ' + date_i)

uni2 = meo_df[meo_df['Date'] == date_i]

uni2 = uni2.reset_index()
uni2 = uni2.drop('index',1)


os.chdir('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files')
#uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')

#Change made by Rohit on 09-12-2020 to make dynamic directories
# base dir
base_dir = os.getcwd()       

# create dynamic name with date as folder
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code +'\\BD_of_' + str(date_i))
# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

os.chdir(base_dir)

# create dynamic name with date as folder
base_dir_plus_Lombard = os.path.join(base_dir, client)

# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir_plus_Lombard):
    os.makedirs(base_dir_plus_Lombard)

# create dynamic name with date as folder
base_dir_plus_Lombard_plus_249 = os.path.join(base_dir_plus_Lombard, setup_code)

# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir_plus_Lombard_plus_249):
    os.makedirs(base_dir_plus_Lombard_plus_249)


filter_col = ['ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account','ViewData.BreakID','ViewData.Fund','ViewData.Task Business Date',
 'ViewData.Currency',
 'ViewData.Asset Type Category',
 'ViewData.Transaction Type',
 'ViewData.Investment Type',
 'ViewData.Prime Broker',
 'ViewData.Ticker',
 'ViewData.Sec Fees',
 'ViewData.Settle Date',
 'ViewData.Trade Date',
 'ViewData.Description',
 'ViewData.CUSIP',
 'ViewData.Call Put Indicator',
 'ViewData.Cancel Flag',
 'ViewData.Commission',
 'ViewData.ISIN',
 'ViewData.Investment ID',
 
 'ViewData.Interest Amount',
 'ViewData.InternalComment1',
 'ViewData.InternalComment2',
 'ViewData.InternalComment3',
             
'ViewData.Accounting Net Amount',
'ViewData.B-P Net Amount',
              'ViewData.Net Amount Difference','ViewData.Status'
#Change made on 09-12-2020 by Rohit
,'ViewData.Task Business Date', 'Date' 
            ]

uni2 = uni2[filter_col]
#uni2['ViewData.Side1_UniqueIds'] = uni2['ViewData.Side1_UniqueIds'].fillna('BB')
#uni2['ViewData.Side0_UniqueIds'] = uni2['ViewData.Side0_UniqueIds'].fillna('AA')

#uni2[['len_0','len_1']] = uni2.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)

#uni2['MTM_mark'] = uni2.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)

# We preserve Actual copy of the file and move to make changes on copy
uni3 = uni2.copy()

# Function for reconciliation involving single sides only
def common_matching_engine_single(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
    #print(dummy['sel_mark'].value_counts())
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
        #print(df4.shape)
    
   
        if df4.shape[0]!=0:
       
            df4['predicted status'] = 'close'
            df4['predicted action'] = 'close'
            df4['predicted category'] = 'close'
            df4['predicted comment'] = 'close'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+ str(serial_num)
            filename = 'Lombard/Lombard 249 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
    else:
        df5 = df.copy()
        
    return df5


# In[107]:


# Function for reconciliation involving both sides
def common_matching_engine_double(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
            k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
            k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
            k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
            k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
            k = pd.merge(k1, k2 , on = filters, how = 'left')
            k = pd.merge(k, k3 , on = filters, how = 'left')
            k = pd.merge(k, k4 , on = filters, how = 'left')
        
            k['predicted status'] = 'pair'
            k['predicted action'] = 'UMR'
            k['predicted category'] = 'match'
            k['predicted comment'] = 'match'
            k = k[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Lombard/Lombard 249 ' + string_name + '.csv'
            k.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# In[108]:


dummy_filter = ['remove_mark','sel_mark']
columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
amount_column = 'ViewData.Net Amount Difference'


# In[109]:


filters = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
serial_num = 1


# In[110]:


double1 = common_matching_engine_double(uni3,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[111]:


filters = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']
serial_num = 2


# In[112]:


double2 = common_matching_engine_double(double1,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[113]:


filters = ['ViewData.Mapped Custodian Account','ViewData.Currency']
serial_num = 3


# In[114]:


double3 = common_matching_engine_double(double2, filters, columns_to_output, amount_column, dummy_filter,serial_num)

#11111
# In[115]:


# Let's start single side reconciliation


# In[116]:


filters = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Description']
serial_num = 4


# In[117]:


double4 = common_matching_engine_single(double3, filters, columns_to_output, amount_column, dummy_filter,serial_num)


# In[118]:


filters = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
serial_num = 5


# In[119]:


double5 = common_matching_engine_single(double4, filters, columns_to_output, amount_column, dummy_filter,serial_num)


# In[120]:


filters = ['ViewData.Mapped Custodian Account','ViewData.Currency']
serial_num = 6


# In[121]:


double6 = common_matching_engine_single(double5, filters, columns_to_output, amount_column, dummy_filter,serial_num)


# In[122]:


# Aggregation filters applied. Custodian account | Currency | Description

# dfk = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Net Amount Difference'].apply(list).reset_index()
# dfk1 = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['len_0'].sum().reset_index()
# dfk2 = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['len_1'].sum().reset_index()


# In[123]:


# df_merge = pd.merge(dfk,dfk1, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left')
# df_merge = pd.merge(df_merge,dfk2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left')


# In[124]:


# df_merge['single_sided'] = df_merge.apply(lambda x : 1 if ((x['len_0']==0) | (x['len_1']==0)) else 0, axis =1 )


# In[125]:


# df_merge['len_amount'] = df_merge['ViewData.Net Amount Difference'].apply(lambda x : len(x) )


# In[126]:


# We seaparte single sided and double sided reconcialiation here. 1 being single sided. We reconcile single sided first
# df_merge1 = df_merge[df_merge['single_sided']==1]
# df_merge2 = df_merge[df_merge['single_sided']==0]


# In[127]:


# if df_merge1.shape[0]!=0:
#     df_merge1['zero_list'] = df_merge1['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
#     df_merge1['len_zero_list'] = df_merge1['zero_list'].apply(lambda x : len(x))
#     df_merge1 = df_merge1.drop(['len_0','len_1','single_sided','ViewData.Net Amount Difference'], axis = 1)
#     uni4 = pd.merge(uni3, df_merge1, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
#     uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)

#     if uni4[uni4['amt_marker'] != 0].shape[0]!=0:
#         k = uni4[(uni4['amt_marker'] == 1)]
#         k['predicted status'] = 'UCB'
#         k['predicted action'] = 'Close'
#         k['predicted category'] = 'close'
#         k['predicted comment'] = 'Match'
#         sel_col_1 = ['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted status','predicted action','predicted category','predicted comment']
#         k = k[sel_col_1]
#         k.to_csv('prediction result lombard 249 P1.csv')
#         uni5 = uni4[(uni4['amt_marker'] == 0)]
    
        
#         uni5 = uni5.drop(['len_0','len_1','len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
#         dummy = uni5.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency'])['ViewData.Net Amount Difference'].apply(list).reset_index()
#         dummy['len_amount'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : len(x))
#         dummy['zero_list'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
#         dummy['len_zero_list'] = dummy['zero_list'].apply(lambda x : len(x))
#         dummy['diff_len'] = dummy['len_amount'] - dummy['len_zero_list']
#         dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
#     #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
#         dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['len_zero_list'],x['diff_len'],x['zero_list_sum']),axis = 1)
#         dummy = dummy[['ViewData.Mapped Custodian Account','ViewData.Currency',  'zero_list', 'len_zero_list', 'remove_mark']]
#         uni4 = pd.merge(uni3, dummy, on = ['ViewData.Mapped Custodian Account','ViewData.Currency'], how = 'left')
#         uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)
        
        
#         if uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0) ].shape[0]!=0:
#             k = uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0)]
#             k['predicted status'] = 'UCB'
#             k['predicted action'] = 'Close'
#             k['predicted category'] = 'close'
#             k['predicted comment'] = 'Match'
#             sel_col_1 = ['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted status','predicted action','predicted category','predicted comment']
#             k = k[sel_col_1]
#             k.to_csv('prediction result lombard 249 P2.csv')
#             uni5 = uni4[(uni4['amt_marker'] == 0)]
#             uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
#             remain_df1 = uni5.copy()
#         else:
#             uni5 = uni4.copy()
#             uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
#             remain_df1 = uni5.copy()
            
        
    
#     else:
#         uni5 = uni4.copy()
#         uni5 = uni5.drop(['len_0','len_1','len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
#         remain_df1 = uni5.copy()
# else:
#     m = 1
    


# In[128]:


#remain_df1 = remain_df1.drop(['len_0','len_1', 'MTM_mark'], axis = 1)


# In[129]:


# if df_merge2.shape[0]!=0:
#     df_merge2['zero_list'] = df_merge2['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
#     df_merge2['len_zero_list'] = df_merge2['zero_list'].apply(lambda x : len(x))
#     df_merge2 = df_merge2.drop(['len_0','len_1','single_sided','ViewData.Net Amount Difference'], axis = 1)
#     uni4 = pd.merge(uni3, df_merge2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
#     uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)

#     if uni4[uni4['amt_marker'] != 0].shape[0]!=0:
#         k = uni4[(uni4['amt_marker'] == 1)]
#         k1 = k.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#         k2 = k.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#         k3 = pd.merge(k1,k2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
        
        
#         k3['predicted status'] = 'UMR'
#         k3['predicted action'] = 'UMR'
#         k3['predicted category'] = 'match'
#         k3['predicted comment'] = 'match'
#         sel_col_1 = ['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted status','predicted action','predicted category','predicted comment']
#         k3 = k3[sel_col_1]
#         k3.to_csv('prediction result lombard 249 P3.csv')
#         uni5 = uni4[(uni4['amt_marker'] == 0)]
    
        
#         uni5 = uni5.drop(['len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
#         dummy = uni5.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency'])['ViewData.Net Amount Difference'].apply(list).reset_index()
#         dummy['len_amount'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : len(x))
#         dummy['zero_list'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
#         dummy['len_zero_list'] = dummy['zero_list'].apply(lambda x : len(x))
#         dummy['diff_len'] = dummy['len_amount'] - dummy['len_zero_list']
#         dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
#     #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
#         dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['len_zero_list'],x['diff_len'],x['zero_list_sum']),axis = 1)
#         dummy = dummy[['ViewData.Mapped Custodian Account','ViewData.Currency',  'zero_list', 'len_zero_list', 'remove_mark']]
#         uni4 = pd.merge(uni3, dummy, on = ['ViewData.Mapped Custodian Account','ViewData.Currency'], how = 'left')
#         uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)
        
        
#         if uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0) ].shape[0]!=0:
#             k = uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0)]
            
#             k1 = k.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = k.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = pd.merge(k1,k2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
        
#             k3['predicted status'] = 'UMR'
#             k3['predicted action'] = 'UMR'
#             k3['predicted category'] = 'match'
#             k3['predicted comment'] = 'match'
#             sel_col_1 = ['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted status','predicted action','predicted category','predicted comment']
#             k3 = k3[sel_col_1]
#             k3.to_csv('prediction result lombard 249 P4.csv')
#             uni5 = uni4[(uni4['amt_marker'] == 0)]
#             uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
#             remain_df1 = uni5.copy()
#         else:
#             uni5 = uni4.copy()
#             uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
#             remain_df2 = uni5.copy()
            
        
    
#     else:
#         uni5 = uni4.copy()
#         uni5 = uni5.drop(['len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
#         remain_df2 = uni5.copy()
# else:
#     m = 1
    


# ### M cross n architecture for UMB finding using desc

# In[130]:


import re


# In[131]:


def desc_match(x,y):
    if ((type(x) == str) & (type(y)==str)):
        x = x.lower()
        y = y.lower()
        x1 =  re.split("[,/. \- !?:]+", x)
        y1 =  re.split("[,/. \- !?:]+", y)
        if len(x1)<len(y1):
            lst3 = [value for value in x1 if value in y1]
            
            if len(lst3)>0:
                score = len(lst3)/len(x1)
            else:
                score = 0
        else:
            lst3 = [value for value in y1 if value in x1]
            
            if len(lst3)>0:
                
                score = len(lst3)/len(y1)
            else:
                score = 0
    else:
        score = 2
    return score


# #### Finding OB for the architecture

# In[132]:


double6.columns


# In[133]:


# this copying is done so that we can add as many filters we can. We start code for UMB here.
uni5 = double6.copy()


# In[134]:


# filter_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account','ViewData.Fund','ViewData.Task Business Date',
#  'ViewData.Currency',
#  'ViewData.Asset Type Category',
#  'ViewData.Transaction Type',
#  'ViewData.Investment Type',
#  'ViewData.Prime Broker',
#  'ViewData.Ticker',
#  'ViewData.Sec Fees',
#  'ViewData.Settle Date',
#  'ViewData.Trade Date',
#  'ViewData.Description']


# In[135]:


#uni5 = uni5[filter_col]


# In[136]:


uni5[['len_0','len_1']] = uni5.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)


# In[137]:


uni5['MTM_mark'] = uni5.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)


# In[138]:


uni5['MTM_mark'].value_counts()


# In[139]:


ob_df= uni5[uni5['MTM_mark'] == 'OB']
side_0 = ob_df[ob_df['ViewData.Side1_UniqueIds'].isna()]
side_0['final_id'] = side_0['ViewData.Side0_UniqueIds']
side_1 = ob_df[ob_df['ViewData.Side0_UniqueIds'].isna()]
side_1['final_id'] = side_1['ViewData.Side1_UniqueIds']
    


# In[140]:


side0_otm= uni5[uni5['MTM_mark'] != 'OB']
side0_otm['final_id'] = side0_otm['ViewData.Side0_UniqueIds'].astype(str) + '|' + side0_otm['ViewData.Side1_UniqueIds'].astype(str) 


# In[141]:


umb_0 = pd.concat([side_0,side0_otm], axis = 0)
umb_0 = umb_0.reset_index()
umb_0.drop('index', axis = 1, inplace =True )


# In[142]:


umb_1 = side_1.copy()


# In[143]:


from pandas import merge
from tqdm import tqdm

pool =[]
key_index =[]
training_df =[]
call1 = []

appended_data = []

no_pair_ids = []
#max_rows = 5

k = list(set(list(set(umb_0['ViewData.Task Business Date'])) + list(set(umb_1['ViewData.Task Business Date']))))
k1 = k

for d in tqdm(k1):
    aa1 = umb_0.copy()#[umb_0['ViewData.Task Business Date']==d]
    bb1 = umb_1.copy()#[umb_1['ViewData.Task Business Date']==d]
#     aa1['marker'] = 1
#     bb1['marker'] = 1
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    print(aa1.shape)
    print(bb1.shape)
    
    aa1.columns = ['SideB.' + x  for x in aa1.columns] 
    bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
    cc1 = pd.merge(aa1,bb1, left_on = ['SideB.ViewData.Mapped Custodian Account','SideB.ViewData.Currency'], right_on = ['SideA.ViewData.Mapped Custodian Account','SideA.ViewData.Currency'], how = 'outer')
    appended_data.append(cc1)
 


# In[144]:


umbmn = pd.concat(appended_data)


# In[145]:


umbmn['desc_score'] =  umbmn.apply(lambda x : desc_match(x['SideA.ViewData.Description'],x['SideB.ViewData.Description']), axis = 1)


# ### Input for UMB model

# In[146]:


### Now remove those pair where one side is absent
ab_a = umbmn[(umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna())]
ab_b = umbmn[~((umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna()))]


# In[147]:


ab_b.columns


# In[148]:


umbk = ab_b[['SideB.ViewData.Side0_UniqueIds', 'SideB.ViewData.Side1_UniqueIds','SideB.final_id','SideB.ViewData.BreakID','SideB.ViewData.Status','SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideA.final_id','SideA.ViewData.BreakID','SideA.ViewData.Status']]


# In[149]:


col_umb = ['SideB.ViewData.Asset Type Category',
        'SideB.ViewData.Fund',
       'SideB.ViewData.Investment Type',
        'SideB.ViewData.Ticker','SideB.ViewData.Transaction Type',
      'SideB.ViewData.Mapped Custodian Account','SideB.ViewData.Currency', 'SideA.ViewData.Asset Type Category',
        'SideA.ViewData.Fund','SideA.ViewData.Investment Type',
        'SideA.ViewData.Ticker','SideA.ViewData.Transaction Type',
      'SideA.ViewData.Mapped Custodian Account','SideA.ViewData.Currency', 'desc_score']


# In[150]:


umb_file = ab_b[col_umb]


# In[151]:


# cat_vars = [ 
      
#       'SideB.ViewData.Asset Type Category',
#         'SideB.ViewData.Fund',
#        'SideB.ViewData.Investment Type',
#         'SideB.ViewData.Ticker','SideB.ViewData.Transaction Type',
#       'SideB.ViewData.Currency', 'SideB.ViewData.Mapped Custodian Account','SideA.ViewData.Asset Type Category',
#         'SideA.ViewData.Fund','SideA.ViewData.Investment Type',
#         'SideA.ViewData.Ticker','SideA.ViewData.Transaction Type',
#       'SideA.ViewData.Currency', 'SideA.ViewData.Mapped Custodian Account'
#        ]


# In[152]:


umb_file['SideB.ViewData.Asset Type Category'] = umb_file['SideB.ViewData.Asset Type Category'].fillna('AA')
umb_file['SideB.ViewData.Fund'] = umb_file['SideB.ViewData.Fund'].fillna('BB')
umb_file['SideB.ViewData.Investment Type'] = umb_file['SideB.ViewData.Investment Type'].fillna('CC')
umb_file['SideB.ViewData.Ticker'] = umb_file['SideB.ViewData.Ticker'].fillna('DD')
umb_file['SideB.ViewData.Transaction Type'] = umb_file['SideB.ViewData.Transaction Type'].fillna('EE')
umb_file['SideB.ViewData.Currency'] = umb_file['SideB.ViewData.Currency'].fillna('FF')
umb_file['SideB.ViewData.Mapped Custodian Account'] = umb_file['SideB.ViewData.Mapped Custodian Account'].fillna('GG')


# In[153]:


umb_file['SideA.ViewData.Asset Type Category'] = umb_file['SideA.ViewData.Asset Type Category'].fillna('aa')
umb_file['SideA.ViewData.Fund'] = umb_file['SideA.ViewData.Fund'].fillna('bb')
umb_file['SideA.ViewData.Investment Type'] = umb_file['SideA.ViewData.Investment Type'].fillna('cc')
umb_file['SideA.ViewData.Ticker'] = umb_file['SideA.ViewData.Ticker'].fillna('dd')
umb_file['SideA.ViewData.Transaction Type'] = umb_file['SideA.ViewData.Transaction Type'].fillna('ee')
umb_file['SideA.ViewData.Currency'] = umb_file['SideA.ViewData.Currency'].fillna('ff')
umb_file['SideA.ViewData.Mapped Custodian Account'] = umb_file['SideA.ViewData.Mapped Custodian Account'].fillna('gg')


# In[154]:


for item in list(umb_file.columns):
    
    x1 = item.split('.')
    if 'desc_score' not in x1:
    
        if x1[0]=='SideB':
            m = 'ViewData.' + 'Accounting'+ " " + x1[2]
            umb_file = umb_file.rename(columns = {item:m})
        else:
            m = 'ViewData.' + 'B-P'+ " " + x1[2]
            umb_file =umb_file.rename(columns = {item:m})


# In[155]:


import pickle
filename = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files\\finalized_model_lombard_249_umb_v3.sav'
clf = pickle.load(open(filename, 'rb'))

cb_predictions = clf.predict(umb_file)


# In[157]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[158]:


umb_file['predicted'] = pd.Series(demo)
#result['Actual'] = pd.Series(list1)


# In[159]:


umb_file['predicted'].value_counts()


# In[160]:


umb_file.columns


# In[222]:


umbpred = pd.concat([umbk,umb_file], axis = 1)


# In[223]:


umbpred =umbpred.drop_duplicates()


# In[224]:


# We first segragate ab_a, then next
side0_ab_a = ab_a[ab_a['SideB.final_id'].isna()]
side1_ab_a = ab_a[~ab_a['SideB.final_id'].isna()]

if ((side0_ab_a.shape[0]!=0) & (side1_ab_a.shape[0]!=0)):
    list_id_0_ab_a = list(set(ab_a['SideA.final_id']))
    list_id_1_ab_a = list(set(ab_a['SideB.final_id']))
    side0_ob = umb_0[umb_0['final_id'].isin(list_id_1_ab_a)]
    side1_ob = umb_1[umb_1['final_id'].isin(list_id_0_ab_a)]
    ob_1st_set = pd.concat([side0_ob,side1_ob], axis = 0)
    ob_1st_set = ob_1st_set.reset_index()
    ob_1st_set = ob_1st_set.drop('index', axis = 1)
elif (side0_ab_a.shape[0]!=0):
    list_id_0_ab_a = list(set(ab_a['SideA.final_id']))
    
    side0_ob = umb_0[umb_0['final_id'].isin(list_id_1_ab_a)]
    
    ob_1st_set = side0_ob.copy()
    ob_1st_set = ob_1st_set.reset_index()
    ob_1st_set = ob_1st_set.drop('index', axis = 1)
else:
    list_id_1_ab_a = list(set(ab_a['SideB.final_id']))
    
    side1_ob = umb_1[umb_1['final_id'].isin(list_id_0_ab_a)]
    
    ob_1st_set = side1_ob.copy()
    ob_1st_set = ob_1st_set.reset_index()
    ob_1st_set = ob_1st_set.drop('index', axis = 1)
    
    


# In[225]:


# We will segragate IDs on both sides which were just OB.
k1 = umbpred.groupby('SideB.final_id')['predicted'].apply(list).reset_index()
k2 = umbpred.groupby('SideA.final_id')['predicted'].apply(list).reset_index()


# In[226]:


def ob_umb(x):
    x1 = list(set(x))
    if ((len(x1)==1) & ('OB' in x1)):
        return 'OB'
    elif ((len(x1)==1) & ('UMB' in x1)):
        return 'UMB'
    else:
        return 'Both'


# In[227]:


k1['State'] = k1['predicted'].apply(lambda x : ob_umb(x) )
k2['State'] = k2['predicted'].apply(lambda x : ob_umb(x) )


# In[228]:


list_id_0_k1 = list(set(k1[k1['State']=='OB']['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['State']=='OB']['SideA.final_id']))
side0_ob = umb_0[umb_0['final_id'].isin(list_id_0_k1)]
side1_ob = umb_1[umb_1['final_id'].isin(list_id_1_k2)]


# In[229]:


ob_2nd_set = pd.concat([side0_ob,side1_ob], axis = 0)
ob_2nd_set = ob_2nd_set.reset_index()
ob_2nd_set = ob_2nd_set.drop('index', axis = 1)


# In[230]:


ob_2nd_set.shape


# In[231]:


if ((ob_1st_set.shape[0]!=0) & (ob_2nd_set.shape[0]!=0)):
    ob_for_comment = pd.concat([ob_1st_set,ob_2nd_set], axis = 0)
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
elif ((ob_1st_set.shape[0]!=0)):
    ob_for_comment = ob_1st_set.copy()
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
else:
    
    ob_for_comment = ob_2nd_set.copy()
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
            


# In[232]:


ob_for_comment.to_csv('Ob for comment daily lombard 249.csv')


# In[233]:


umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]


# In[234]:


# Difficult part segragation of UMBs in OTO, OTM, MTO and MTM
# OTO

import collections

def umb_counter(x):
    counter=collections.Counter(x)
    if counter['UMB'] == 1:
        return 1
    else:
        return counter['UMB']
        
k1['umb_counter'] = k1['predicted'].apply(lambda x : umb_counter(x) )
k2['umb_counter'] = k2['predicted'].apply(lambda x : umb_counter(x) )


# In[235]:


list_id_0_k1 = list(set(k1[k1['umb_counter']==1]['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['umb_counter']==1]['SideA.final_id']))
if ((len(list_id_0_k1)>0) & (len(list_id_1_k2)>0)):
    umb_oto = umbpred[(umbpred['SideB.final_id'].isin(list_id_0_k1)) & (umbpred['SideA.final_id'].isin(list_id_1_k2))]
    umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
    umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]


# In[236]:


def breakid(x,y):
    
    brk = []
    for item in x:
        brk.append(item)
    brk.append(y)
    
    return brk


# In[237]:


# Now We write the hierarchy for many to one
k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
k4 = umbpred.groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
mn = umbpred[['SideB.final_id','SideB.ViewData.BreakID']]
mn = mn.drop_duplicates()
k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
stat = umbpred.groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

mn1 = umbpred[['SideB.final_id','SideB.ViewData.Status']]
mn1 = mn1.drop_duplicates()
k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )


# In[238]:


k3.shape


# In[239]:


def intersection_(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return round((len(lst3)/len(lst1)),1)


# In[240]:


def Diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif


# In[241]:


from functools import reduce


# In[242]:


ob_stage_df = pd.DataFrame()
umb_mtm = pd.DataFrame()
umb_mtm_list = []
umb_otm_list = []

for i, row in k3.iterrows():
    if k3.shape[0]!=0:
    
        k5 = k3.copy()
        mid = row['SideB.final_id']
        midlist = row['SideA.final_id']
        midlen = row['id_len']
        all_brk = row['ViewData.BreakID']
        all_sts = row['ViewData.Status']
    
        k6 = k5[(k5['id_len']<midlen+3) & (k5['id_len']>midlen-4)]
        k6['match'] = k6['SideA.final_id'].apply(lambda x:intersection_(x,midlist) )
    
    
        k7 =list(set(k6[k6['match']>0.9]['SideB.final_id']))
        if len(k7)>0:
        
            set_for_int = list((k6[k6['match']>0.7]['SideA.final_id']))
            k8 = list(reduce(set.intersection, [set(item) for item in set_for_int]))
        
            int1 = umbpred[umbpred['SideB.final_id'].isin(k7)]
            br7 =list((int1['SideB.ViewData.BreakID']))
            br8 =list((umbpred[umbpred['SideA.final_id'].isin(k8)]['SideA.ViewData.BreakID']))
            br7_8 = br7 + br8
            vi7 =list(set(int1['SideB.ViewData.Status']))
            vi8 =list(set(umbpred[umbpred['SideA.final_id'].isin(k8)]['SideA.ViewData.Status']))
            vi7_8 = vi7 + vi8
            k9 =list(set(int1['SideA.final_id']))
            if ((len(k8)>0) & (len(k9)>0)):
                umb_mtm_list_temp = []
                umb_mtm_list_temp.append(k7)
                umb_mtm_list_temp.append(k8)
                umb_mtm_list_temp.append(br7_8)
                umb_mtm_list_temp.append(vi7_8)
                umb_mtm_list.append(umb_mtm_list_temp)
            
                k10 = Diff(k9,k8)
                umbpred = umbpred[~umbpred['SideB.final_id'].isin(k7)]
                k11 = list(set(umbpred['SideA.final_id']))
                k12 = Diff(k10,k11)
                ob_3rd_set = umb_1[umb_1['final_id'].isin(k12)]
                ob_stage_df = pd.concat([ob_stage_df,ob_3rd_set], axis = 0)
                ob_stage_df = ob_stage_df.reset_index()
                ob_stage_df = ob_stage_df.drop('index', axis = 1)
            
                int1 = umbpred[umbpred['SideA.final_id'].isin(k8)]
            
#             k15 =list(set(int1['SideB.final_id']))
#             k16 = Diff(k15,k7)
#             umbpred = umbpred[~umbpred['SideA.final_id'].isin(k8)]
#             k17 = list(set(umbpred['SideB.final_id']))
#             k18 = Diff(k16,k17)
#             ob_4th_set = umb_0[umb_0['final_id'].isin(k18)]
#             ob_stage_df = pd.concat([ob_stage_df,ob_4th_set], axis = 0)
#             ob_stage_df = ob_stage_df.reset_index()
#             ob_stage_df = ob_stage_df.drop('index', axis = 1)
            if umbpred.shape[0]!=0:
                k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
                k4 = umbpred.groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
                k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
                k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
                mn = umbpred[['SideB.final_id','SideB.ViewData.BreakID']]
                mn = mn.drop_duplicates()
                k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
                k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
                stat = umbpred.groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
                k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

                mn1 = umbpred[['SideB.final_id','SideB.ViewData.Status']]
                mn1 = mn1.drop_duplicates()
                k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
                k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )
            else:
                break
            
        else:
        #k8 = list(reduce(set.intersection, [set(item) for item in set_for_int]))
#         int1 = k3[k3['SideB.final_id']==mid]
#         print(int1.shape[0])
            mk = list(mid)
  
            if (len(midlist)>0):
            
#             int1 = umbpred[umbpred['SideB.final_id']==mid]
#             print(int1.shape)
#             br7 = list(umbpred[umbpred['SideB.final_id']==mid]['SideB.ViewData.BreakID'])
#             print(br7)
#             br8 =list((umbpred[umbpred['SideA.final_id'].isin(midlist)]['SideA.ViewData.BreakID']))
#             #print(br8)
#             br7_8 = br7 + br8
#             vi7 =list(umbpred[umbpred['SideB.final_id']==mid]['SideB.ViewData.Status'])
#             vi8 =list(set(umbpred[umbpred['SideA.final_id'].isin(midlist)]['SideA.ViewData.Status']))
#             vi7_8 = vi7 + vi8
                umb_otm_list_temp = []
                umb_otm_list_temp.append(mid)
                umb_otm_list_temp.append(midlist)
                umb_otm_list_temp.append(all_brk)
                umb_otm_list_temp.append(all_sts)
            
                umb_otm_list.append(umb_otm_list_temp)
            #k10 = Diff(midlist,mk) 
                knn = umbpred[umbpred['SideA.final_id'].isin(midlist)]
                k11 = list(set(knn['SideB.final_id']))
                k12 = Diff(k11,mk)
            
                umbpred = umbpred[~umbpred['SideA.final_id'].isin(midlist)]
                k13 = list(set(umbpred['SideB.final_id']))
                k14 = Diff(k12,k13)
            
                ob_4th_set = umb_0[umb_0['final_id'].isin(k14)]
                ob_stage_df = pd.concat([ob_stage_df,ob_4th_set], axis = 0)
                ob_stage_df = ob_stage_df.reset_index()
                ob_stage_df = ob_stage_df.drop('index', axis = 1)
            if umbpred.shape[0]!=0:
                k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
                k4 = umbpred.groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
                k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
                k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
                mn = umbpred[['SideB.final_id','SideB.ViewData.BreakID']]
                mn = mn.drop_duplicates()
                k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
                k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
                stat = umbpred.groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
                k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

                mn1 = umbpred[['SideB.final_id','SideB.ViewData.Status']]
                mn1 = mn1.drop_duplicates()
                k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
                k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )
            else:
                break
    else:
        break
            


# In[243]:


umbpred.shape


# In[244]:


umb_mtm = pd.DataFrame(umb_mtm_list)


# In[245]:


umb_otm = pd.DataFrame(umb_otm_list)


# In[246]:


umb_otm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status']
umb_mtm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status']


# In[247]:


umb_otm['predicted status'] = 'UMB'
umb_otm['predicted action'] = 'UMB one to many'
umb_otm['predicted category'] = 'UMB'
umb_otm['predicted comment'] = 'Difference in amount'


# In[248]:


umb_mtm['predicted status'] = 'UMB'
umb_mtm['predicted action'] = 'UMB many to many'
umb_mtm['predicted category'] = 'UMB'
umb_mtm['predicted comment'] = 'Difference in amount'


# In[249]:


umb_mtm = umb_mtm.reset_index()
umb_mtm = umb_mtm.drop('index', axis = 1)

umb_otm = umb_otm.reset_index()
umb_otm = umb_otm.drop('index', axis = 1)


# In[250]:


umb_oto1 = umb_oto[['SideB.final_id','SideB.ViewData.BreakID','SideB.ViewData.Status','SideA.final_id','SideA.ViewData.BreakID','SideA.ViewData.Status']]


# In[251]:


umb_oto1 = umb_oto1.rename(columns = {'SideB.final_id':'ViewData.Side0_UniqueIds',
                                     'SideA.final_id':'ViewData.Side1_UniqueIds'})


# In[252]:


def combining(x,y):
    blank_list = []
    blank_list.append(x)
    blank_list.append(y)
    
    return blank_list


# In[253]:


umb_oto1['ViewData.BreakID'] = umb_oto1.apply(lambda x : combining(x['SideB.ViewData.BreakID'], x['SideA.ViewData.BreakID']), axis = 1)
umb_oto1['ViewData.Status'] = umb_oto1.apply(lambda x : combining(x['SideB.ViewData.Status'], x['SideA.ViewData.Status']), axis = 1)


# In[254]:


umb_oto1 = umb_oto1.reset_index()
umb_oto1.drop(['index','SideB.ViewData.BreakID','SideA.ViewData.BreakID','SideB.ViewData.Status','SideA.ViewData.Status'], axis = 1, inplace = True)


# In[255]:


umb_oto1['predicted status'] = 'UMB'
umb_oto1['predicted action'] = 'UMB one to one'
umb_oto1['predicted category'] = 'UMB'
umb_oto1['predicted comment'] = 'Difference in amount'


# In[256]:

os.chdir(base_dir)
umb_oto1.to_csv('Lombard/' + setup_code + '/umb lombard 249 oto.csv')
umb_mtm.to_csv('Lombard/' + setup_code + '/umb lombard 249 mtm.csv')
umb_otm.to_csv('Lombard/' + setup_code + '/umb lombard 249 otm.csv')


# In[257]:


k = ob_stage_df.drop_duplicates()


# In[258]:


k.shape


# In[259]:


ob_for_comment = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files\\Ob for comment daily lombard 249.csv')


# In[260]:


ob_for_comment = ob_for_comment.drop('Unnamed: 0', axis = 1)


# In[261]:


ob_for_comment.shape


# In[262]:


ob_for_comment.drop('ViewData.Task Business Date', axis =1 , inplace = True)
k.drop('ViewData.Task Business Date', axis =1 , inplace = True)


# In[263]:


k.columns


# In[264]:


ob_for_comment_model = pd.concat([ob_for_comment,k], axis = 0)


# In[265]:


ob_for_comment_model = ob_for_comment_model.reset_index()
ob_for_comment_model = ob_for_comment_model.drop('index', axis = 1)


# In[266]:


# Now we take all OBs to single side model


# In[267]:


import pandas as pd
import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[268]:


df3 = ob_for_comment_model.copy()


# In[269]:


df3.columns


# In[270]:





df = pd.read_excel('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files\\Mapping variables for variable cleaning.xlsx', sheet_name='General')
def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys
df['tuple'] = df.apply(make_dict, axis=1)
clean_map_dict = df.set_index('tuple')['Value'].to_dict()

df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Asset Type Category'] = df3['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Investment Type'] = df3['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Prime Broker'] = df3['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)

def clean_mapping(item):
    item1 = item.split()
    
    
    ttype = []
    
    
    for x in item1:
        ttype1 = []
        for key, value in clean_map_dict.items():
            
    
        
        
            if x in key:
                a = value
                ttype1.append(a)
           
        if len(ttype1)==0:
            ttype1.append(x)
        ttype = ttype + ttype1
        
    return ' '.join(ttype)

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)

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

def comb_clean(x):
    k = []
    for item in x.split():
        if ((is_num(item)==False) and (is_date_format(item)==False) and (date_edge_cases(item)==False)):
            k.append(item)
    return ' '.join(k)

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[271]:


import re

com = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files\\desc cat with naveen oaktree.csv')
cat_list = list(set(com['Pairing']))

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

df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

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


# In[272]:


df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
com = com.drop(['var','Catogery'], axis = 1)
com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())

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
    
df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

comp = ['inc','stk','corp ','llc','pvt','plc']
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
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
        if x == 'db_int':
            return 'interest'
        else:
            return x
        
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}
def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x
df3['new_pb'] = df3['new_pb'].apply(lambda x : new_pf_mapping(x))
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].fillna('kkk')
df3['new_pb1'] = df3.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)
df3['new_pb1'] = df3['new_pb1'].apply(lambda x : x.lower())


# In[273]:


df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])
days = [1,30,31,29]
df3['monthend marker'] = df3['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[274]:


df3['comm_marker'] = 'zero'
df3['new_pb2'] = df3.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
df3['new_pb2'] = df3['new_pb2'].apply(lambda x : x.lower())


# In[275]:


df3.columns


# In[276]:


cols = ['ViewData.Transaction Type1',
 'ViewData.Asset Type Category1',
 'ViewData.Investment Type1',
 'new_desc_cat',
 'new_pb2',
 'new_pb1',
 'comm_marker',
 'monthend marker']


# In[277]:


df4 = df3[cols]


# In[278]:


df4['ViewData.Transaction Type1'] = df4['ViewData.Transaction Type1'].fillna('aa')
df4['ViewData.Asset Type Category1'] = df4['ViewData.Asset Type Category1'].fillna('aa')
df4['ViewData.Investment Type1'] = df4['ViewData.Investment Type1'].fillna('aa')
df4['new_desc_cat'] = df4['new_desc_cat'].fillna('aa')
df4['new_pb2'] = df4['new_pb2'].fillna('aa')
df4['new_pb1'] = df4['new_pb1'].fillna('aa')
df4['comm_marker'] = df4['comm_marker'].fillna('aa')
df4['monthend marker'] = df4['monthend marker'].fillna('aa')


# In[279]:


import pickle
filename = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files\\finalized_model_lombard_249_v1.sav'
clf = pickle.load(open(filename, 'rb'))
cb_predictions = clf.predict(df4)

demo = []
for item in cb_predictions:
    demo.append(item[0])
df3['predicted category'] = pd.Series(demo)

com_temp = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files\\lobard 249 comment template for delivery.csv')
com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
result_non_trade = df3.copy()
result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
def comgen(x,y,z,k):
    if x == 'Geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "Geneva" + ' ' +y + ' ' + str(z)
        
    return com

result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
result_non_trade['predicted status'] = 'comment'
result_non_trade['predicted action'] = 'OB'
result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
result_non_trade.to_csv('Lombard/' + setup_code + '/Comment file for lombard 249.csv')

#Change made on 09-12-2020 by Rohit to read files made by Abhijeet code in case file exists
os.chdir(base_dir)

def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
    frames = []
    current_folder = os.getcwd()
    full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
    for full_filepath in full_filepath_list :
        if os.path.isfile(full_filepath) == True:
            frames.append(pd.read_csv(full_filepath))
    return pd.concat(frames)


# #### Combining all the files
final_df_filename_list_inside_Lombard_folder = ['Lombard/Lombard 249 p' + str(x) + '.csv' for x in [1,2,3,4,5,6]]
final_df_inside_Lombard_folder = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(final_df_filename_list_inside_Lombard_folder)

final_df_filename_list_inside_Lombard_249_folder = ['Lombard/249/umb lombard 249 ' + str(x) + '.csv' for x in ['mtm','otm','oto']]
final_df_filename_list_inside_Lombard_249_folder.append('Lombard/249/Comment file for lombard 249.csv')
final_df_inside_Lombard_249_folder = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(final_df_filename_list_inside_Lombard_249_folder)

final_df = final_df_inside_Lombard_folder.append(final_df_inside_Lombard_249_folder)
#Change made by Rohit to clean final_df before updating into db

final_df = final_df.reset_index()
final_df = final_df.drop('index', axis = 1)

if('Unnamed: 0' in list(final_df.columns)):
    final_df.drop(['Unnamed: 0'], axis = 1, inplace = True)

final_df = final_df.rename(columns = {'ViewData.BreakID' : 'BreakID',
                           'ViewData.Source Combination Code' : 'SourceCombinationCode',
                           'ViewData.Task ID' : 'TaskID',
                           'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                           'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                           'Predicted Comment' : 'PredictedComment',
                           'Predicted Category' : 'PredictedCategory'})

final_df.loc[(final_df['predicted status'] == 'comment'), 'Predicted_Status_to_insert_in_db'] = final_df['ViewData.Status']
final_df.loc[(final_df['predicted status'] != 'comment'), 'Predicted_Status_to_insert_in_db'] = final_df['predicted status']

final_df.loc[final_df['Predicted_Status_to_insert_in_db'] == 'close', 'Predicted_Status_to_insert_in_db'] = 'UCB'
final_df.loc[final_df['Predicted_Status_to_insert_in_db'] == 'pair', 'Predicted_Status_to_insert_in_db'] = 'UMR'

    
final_df['BreakID'] = final_df['BreakID'].astype(str)
final_df['BreakID'] = final_df['BreakID'].replace('\.0','',regex = True)

final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].astype(str)
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].astype(str)

final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df['BreakID'] = final_df['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('nan','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('nan','')
final_df['BreakID'] = final_df['BreakID'].replace(' ','')

cols_to_remove_newline_char_from = ['Side1_UniqueIds','Side0_UniqueIds','BreakID']

final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('\\n','',regex = True)
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('\\n','',regex = True)
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('BB','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('AA','')
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('None','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('None','')
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('nan','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('nan','')

def unlist_comma_separated_single_quote_string_lst(list_obj):
    new_list = []
    for i in list_obj:
        list_i = list(i.replace('\'','').split(', '))
        for j in list_i:
            new_list.append(j)
    return new_list


#BreakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['BreakID'].astype(str).unique().tolist())

def get_first_non_null_value(string_of_values_separated_by_comma):
    if(string_of_values_separated_by_comma != '' and string_of_values_separated_by_comma != 'nan' and string_of_values_separated_by_comma != 'None' ):
        if(string_of_values_separated_by_comma.partition(',')[0] != '' and string_of_values_separated_by_comma.partition(',')[0] != 'nan' and string_of_values_separated_by_comma.partition(',')[0] != 'None'):
            return(string_of_values_separated_by_comma.partition(',')[0])
        else:
            return(get_first_non_null_value(string_of_values_separated_by_comma.partition(',')[2]))
    else:
        return('Blank value')        


final_df['first_non_null_breakid_in_breakid_columns'] = final_df['BreakID'].apply(lambda x : get_first_non_null_value(str(x)))
final_df['first_non_null_breakid_in_breakid_columns'] = final_df['first_non_null_breakid_in_breakid_columns'].map(lambda x:x.lstrip('').rstrip(''))

meo_df['ViewData.BreakID'] = meo_df['ViewData.BreakID'].astype(str)
final_df_copy = pd.merge(final_df, meo_df[['ViewData.BreakID','ViewData.Task ID','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'first_non_null_breakid_in_breakid_columns', right_on = 'ViewData.BreakID', how='left')

final_df_copy['BusinessDate'] = date_i

final_df_copy.to_csv('final_df_just_after_appending_all_files.csv')
final_df_copy_2 = final_df_copy.copy()

final_df_copy['BusinessDate'] = pd.to_datetime(final_df_copy['BusinessDate'])
final_df_copy['BusinessDate'] = final_df_copy['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_copy['BusinessDate'] = pd.to_datetime(final_df_copy['BusinessDate'])

list(final_df_copy.columns)
final_df_copy['BreakID_list'] = final_df_copy['BreakID'].apply(lambda x : x.split(','))

final_df_copy['BreakID_to_insert_in_db'],final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['BreakID_list'].apply(lambda x : x[0]),final_df_copy['BreakID_list'].apply(lambda x : x[1:])

final_df_copy['BreakID_to_insert_in_db'] = final_df_copy['BreakID_to_insert_in_db'].astype(str)
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].astype(str)

final_df_copy['BreakID_to_insert_in_db'] = final_df_copy['BreakID_to_insert_in_db'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace('\' ','\'', regex = True)
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace(', ',',', regex = True)
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace('\'','', regex = True)

final_df_copy['ML_flag'] = 'ML'

final_df_copy['SetupID'] = setup_code

final_df_copy['probability_No_pair'] = ''
final_df_copy['probability_UMB'] = ''
final_df_copy['probability_UMR'] = ''
final_df_copy['probability_UMT'] = ''

#cols_for_database = ['BreakID', 'BusinessDate', 'Final_predicted_break', 'ML_flag',
#       'Predicted_Status', 'Predicted_action', 'SetupID',
#       'SourceCombinationCode', 'TaskID', 'probability_No_pair',
#       'probability_UMB', 'probability_UMR', 'probability_UMT',
#       'Side1_UniqueIds', 'PredictedComment', 'PredictedCategory',
#       'Side0_UniqueIds']    

filepaths_final_df_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df_copy.to_csv(filepaths_final_df_copy)


cols_for_database = ['Side1_UniqueIds','Side0_UniqueIds',
#'predicted status', 
'Predicted_Status_to_insert_in_db',
'predicted action', 'predicted category',
'predicted comment',
'ViewData.Task ID',
'ViewData.Source Combination Code', 'BusinessDate',
'BreakID_to_insert_in_db', 'Predicted_BreakID_to_insert_in_db',
'ML_flag', 'SetupID', 'probability_No_pair', 'probability_UMB',
'probability_UMR', 'probability_UMT']    


final_df_2 = final_df_copy[cols_for_database]

cols_for_database_rename_dict = {'Predicted_Status_to_insert_in_db' : 'Predicted_Status',
                                 'predicted action' : 'Predicted_action',
                                 'predicted category' : 'PredictedCategory',
                                 'predicted comment' : 'PredictedComment',
                                 'ViewData.Task ID' : 'TaskID',
                                 'ViewData.Source Combination Code' : 'SourceCombinationCode',
                                 'BreakID_to_insert_in_db' : 'BreakID',
                                 'Predicted_BreakID_to_insert_in_db' : 'Final_predicted_break'}

final_df_2 = final_df_2.rename(columns = cols_for_database_rename_dict)
#    Added more checks for database

final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].astype(str)
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].astype(str)
final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].astype(str)
final_df_2['probability_UMT'] = final_df_2['probability_UMT'].astype(str)
final_df_2['probability_UMR'] = final_df_2['probability_UMR'].astype(str)
final_df_2['probability_UMB'] = final_df_2['probability_UMB'].astype(str)
final_df_2['probability_No_pair'] = final_df_2['probability_No_pair'].astype(str)

final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['BreakID'] = final_df_2['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))

cols_to_remove_newline_char_from = ['Side1_UniqueIds','Side0_UniqueIds','BreakID','Final_predicted_break']
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('\\n','',regex = True)
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('\\n','',regex = True)
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('BB','')
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('AA','')
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('None','')
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('None','')
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].replace('nan','')
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].replace('nan','')

final_df_2['probability_No_pair'] = final_df_2['probability_No_pair'].replace('None','')
final_df_2['probability_No_pair'] = final_df_2['probability_No_pair'].replace('nan','')

final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('None','')
final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('nan','')

final_df_2['probability_UMR'] = final_df_2['probability_UMR'].replace('None','')
final_df_2['probability_UMR'] = final_df_2['probability_UMR'].replace('nan','')

final_df_2['probability_UMB'] = final_df_2['probability_UMB'].replace('None','')
final_df_2['probability_UMB'] = final_df_2['probability_UMB'].replace('nan','')

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\\n','',regex = True)

final_df_2['PredictedComment'] = final_df_2['PredictedComment'].astype(str)
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('nan','')
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('None','')
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('NA','')

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)

#final_df_2_UMR_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
#if(final_df_2_UMR_record_with_predicted_comment.shape[0] != 0):
#    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
#
#    Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side0_UniqueIds']
#    Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side1_UniqueIds']
#
#    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#
#    final_df_2_UMR_record_with_predicted_comment['PredictedComment'] = ''       
#    final_df_2 = final_df_2.append(final_df_2_UMR_record_with_predicted_comment)
#
#
#final_df_2_UMT_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
#if(final_df_2_UMT_record_with_predicted_comment.shape[0] != 0):
#    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
#    
#    Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side0_UniqueIds']
#    Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side1_UniqueIds']
#    
#    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#
#    final_df_2_UMT_record_with_predicted_comment['PredictedComment'] = ''
#    final_df_2 = final_df_2.append(final_df_2_UMT_record_with_predicted_comment)

#final_df_2['BusinessDate'] = final_df_2.apply(lambda x: get_BusinessDate_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_df_2['TaskID'] = final_df_2.apply(lambda x: get_TaskID_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_df_2['SourceCombinationCode'] = final_df_2.apply(lambda x: get_SourceCombinationCode_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)


final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df_2_copy_2 = final_df_2.copy()
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])

final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)
#def find_taskid_for_breakid_value_apply_function(fun_breakid_string_value,fun_meo_df):
#    return(fun_meo_df[fun_meo_df['ViewData.BreakID'] == int(fun_breakid_string_value)]['ViewData.Task ID'].unique())

meo_df['ViewData.BreakID'] = meo_df['ViewData.BreakID'].astype(str)
single_TaskID_value_for_455 = meo_df['ViewData.Task ID'].mode()[0]
final_df_2['TaskID'] = final_df_2['BreakID'].apply(lambda x : meo_df[meo_df['ViewData.BreakID'] == x]['ViewData.Task ID'].unique())

final_df_2['TaskID'] = final_df_2['TaskID'].astype(str)
final_df_2['TaskID'] = final_df_2['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))

#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
#
#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])

#final_df_2 =  final_df_2[~(final_df_2['TaskID'] == '')]
final_df_2 = final_df_2[final_df_2['TaskID'] != '']
final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))

final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)



final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(str)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('\'').rstrip('\''))

final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)

def apply_ui_action_column_249(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionType = 'No Prediction'
    else:
        if((fun_row['Predicted_action'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_action'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_action'] == 'close') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE'
        elif((fun_row['Predicted_action'] == 'close') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE WITH COMMENT'
        elif(((fun_row['Predicted_action'] == 'UMB many to many') or (fun_row['Predicted_action'] == 'UMB one to many') or (fun_row['Predicted_action'] == 'UMB one to one') or (fun_row['Predicted_action'] == 'UMR')) & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'PAIR'
        elif(((fun_row['Predicted_action'] == 'UMB many to many') or (fun_row['Predicted_action'] == 'UMB one to many') or (fun_row['Predicted_action'] == 'UMB one to one') or (fun_row['Predicted_action'] == 'UMR')) & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'PAIR WITH COMMENT'
    return ActionType

final_df_2['ActionType'] = final_df_2.apply(lambda row : apply_ui_action_column_249(fun_row = row), axis = 1,result_type="expand")            

final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace('\'','',regex = True)
final_df_2['BreakID'] = final_df_2['BreakID'].replace('\'','',regex = True)
final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df_2.to_csv(filepaths_final_df_2)

#data_dict = final_table_to_write.to_dict("records")
data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
coll_1_for_writing_prediction_data.insert_many(data_dict) 

print(setup_code)
print(date_i)