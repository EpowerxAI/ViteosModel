# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 18:59:03 2020

@author: consultant138
"""
import os
os.chdir('D:\\ViteosModel')

import pandas as pd
from pandas import merge
from tqdm import tqdm
import numpy as np
import os
import dask.dataframe as dd
import glob
import re
import pickle
import collections
import math
from dateutil.parser import parse
import operator
import itertools
import ast
import datetime as dt
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from pandas.io.json import json_normalize
from datetime import datetime,date,timedelta
#import timeit
from functools import reduce
cols = ['Currency','Account Type',
        'Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
#'Activity Code',
#'PMSVendor Net Amount','Cust Net Amount',
'Age','Age WK',
'Asset Type Category',
#'Base Currency','Base Net Amount',
#'Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
#'Derived Source',
'Description',
#'Department',
        #'ExpiryDate','ExternalComment1','ExternalComment2',
#'ExternalComment3',
'Fund',
#'FX Rate',
#'Interest Amount',
'InternalComment1','InternalComment2',
'InternalComment3',
    'Investment Type','Is Combined Data','ISIN','Keys',
'Mapped Custodian Account','Net Amount Difference','Net Amount Difference Absolute','Non Trade Description',
#'OTE Custodian Account',
'OTE Ticker',
#'Predicted Action','Predicted Status','Prediction Details',
'Price','Prime Broker',
'Quantity','SEDOL','Settle Date','SPM ID','Status',
#'Strike Price',
'System Comments',
'Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type','Investment ID',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


new_cols = ['ViewData.' + x for x in cols] + add


client = 'Lombard'

setup = '249'
setup_code = '249'
#filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
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
print('Starting predictions for Lombard, setup_code = ')
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

meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df['ViewData.Side0_UniqueIds'] = meo_df['ViewData.Side0_UniqueIds'].astype(str)
meo_df['ViewData.Side1_UniqueIds'] = meo_df['ViewData.Side1_UniqueIds'].astype(str)

meo_df.loc[meo_df['ViewData.Side0_UniqueIds'] == 'nan', 'ViewData.Side0_UniqueIds'] = 'None'
meo_df.loc[meo_df['ViewData.Side1_UniqueIds'] == 'nan', 'ViewData.Side1_UniqueIds'] = 'None'

meo_df.loc[meo_df['ViewData.Side0_UniqueIds'] == '', 'ViewData.Side0_UniqueIds'] = 'None'
meo_df.loc[meo_df['ViewData.Side1_UniqueIds'] == '', 'ViewData.Side1_UniqueIds'] = 'None'

meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)
meo = meo_df[new_cols]



basic_filepath = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\'
#uni2 = pd.read_csv(basic_filepath + 'Lombard\\ReconDB.HST_RecData_249_01_10.csv')
uni2 = meo_df[meo_df['ViewData.Task Business Date'] == '2020-12-02T00:00:00']
uni2['ViewData.Side1_UniqueIds'] = uni2['ViewData.Side1_UniqueIds'].fillna('BB')
uni2['ViewData.Side0_UniqueIds'] = uni2['ViewData.Side0_UniqueIds'].fillna('AA')
uni2.loc[uni2['ViewData.Side0_UniqueIds'] == 'None', 'ViewData.Side0_UniqueIds'] = 'AA'
uni2.loc[uni2['ViewData.Side1_UniqueIds'] == 'None', 'ViewData.Side1_UniqueIds'] = 'BB'

def mtm(x,y):
    if ((x !='AA') & (y !='BB')):
        y1 = y.split(',')
        x1 = x.split(',')
        return pd.Series([len(x1),len(y1)], index=['len_0', 'len_1'])
    elif ((x !='AA') & (y =='BB')):
        x1 = x.split(',')
        
        return pd.Series([len(x1),0], index=['len_0', 'len_1'])
    elif ((x =='AA') & (y !='BB')):
        y1 = y.split(',')
        
        return pd.Series([0,len(y1)], index=['len_0', 'len_1'])
        
    else:
        
        
        return pd.Series([0,0], index=['len_0', 'len_1'])

uni2[['len_0','len_1']] = uni2.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)
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

uni2['MTM_mark'] = uni2.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)
uni3 = uni2.copy()

# Aggregation filters applied. Custodian account | Currency | Description

dfk = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Net Amount Difference'].apply(list).reset_index()
dfk1 = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['len_0'].sum().reset_index()
dfk2 = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['len_1'].sum().reset_index()
df_merge = pd.merge(dfk,dfk1, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left')
df_merge = pd.merge(df_merge,dfk2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left')
df_merge['single_sided'] = df_merge.apply(lambda x : 1 if ((x['len_0']==0) | (x['len_1']==0)) else 0, axis =1 )
df_merge['len_amount'] = df_merge['ViewData.Net Amount Difference'].apply(lambda x : len(x) )
# We seaparte single sided and double sided reconcialiation here. 1 being single sided. We reconcile single sided first
df_merge1 = df_merge[df_merge['single_sided']==1]
df_merge2 = df_merge[df_merge['single_sided']==0]

# Three common functions
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
        if ((k<6.0) & (z==0)):
            return 1
        elif ((k==0.0) & (z!=0)):
            return 1
        else:
            return 0
    else:
        return 0

if df_merge1.shape[0]!=0:
    df_merge1['zero_list'] = df_merge1['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
    df_merge1['len_zero_list'] = df_merge1['zero_list'].apply(lambda x : len(x))
    df_merge1 = df_merge1.drop(['len_0','len_1','single_sided','ViewData.Net Amount Difference'], axis = 1)
    uni4 = pd.merge(uni3, df_merge1, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
    uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)

    if uni4[uni4['amt_marker'] != 0].shape[0]!=0:
        print('first if')
        k = uni4[(uni4['amt_marker'] == 1)]
        k['predicted status'] = 'UCB'
        k['predicted action'] = 'Close'
        k['predicted category'] = 'one sided close'
        k['predicted comment'] = 'Match'
        sel_col_1 = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
        k = k[sel_col_1]
        k.to_csv('prediction result lombard 249 P1.csv')
        uni5 = uni4[(uni4['amt_marker'] == 0)]
    
        
        uni5 = uni5.drop(['len_0','len_1','len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
        dummy = uni5.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency'])['ViewData.Net Amount Difference'].apply(list).reset_index()
        dummy['len_amount'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : len(x))
        dummy['zero_list'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
        dummy['len_zero_list'] = dummy['zero_list'].apply(lambda x : len(x))
        dummy['diff_len'] = dummy['len_amount'] - dummy['len_zero_list']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['len_zero_list'],x['diff_len'],x['zero_list_sum']),axis = 1)
        dummy = dummy[['ViewData.Mapped Custodian Account','ViewData.Currency',  'zero_list', 'len_zero_list', 'remove_mark']]
        uni4 = pd.merge(uni3, dummy, on = ['ViewData.Mapped Custodian Account','ViewData.Currency'], how = 'left')
        uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)
        
        
        if uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0) ].shape[0]!=0:
            print('second if')
            k = uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0)]
            k['predicted status'] = 'UCB'
            k['predicted action'] = 'Close'
            k['predicted category'] = 'one sided close'
            k['predicted comment'] = 'Match'
            sel_col_1 = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
            k = k[sel_col_1]
            k.to_csv('prediction result lombard 249 P2.csv')
            uni5 = uni4[(uni4['amt_marker'] == 0)]
            uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
            remain_df1 = uni5.copy()
        else:
            print('second else')
            uni5 = uni4.copy()
            uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
            remain_df1 = uni5.copy()
            
        
    
    else:
        print('third else')
        uni5 = uni4.copy()
        uni5 = uni5.drop(['len_0','len_1','len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
        remain_df1 = uni5.copy()
else:
    m = 1
    print('fourth else')
#TODO : Ask Abhijeet what should be the value of remain_df1 for else here
    
remain_df1 = remain_df1.drop(['len_0','len_1', 'MTM_mark'], axis = 1)
uni3 = remain_df1.copy()

if df_merge2.shape[0]!=0:
    df_merge2['zero_list'] = df_merge2['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
    df_merge2['len_zero_list'] = df_merge2['zero_list'].apply(lambda x : len(x))
    df_merge2 = df_merge2.drop(['len_0','len_1','single_sided','ViewData.Net Amount Difference'], axis = 1)
    uni4 = pd.merge(uni3, df_merge2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
    uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)

    if uni4[uni4['amt_marker'] != 0].shape[0]!=0:
        k = uni4[(uni4['amt_marker'] == 1)]
        k['predicted status'] = 'UCB'
        k['predicted action'] = 'Close'
        k['predicted category'] = 'one sided close'
        k['predicted comment'] = 'Match'
        sel_col_1 = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
        k = k[sel_col_1]
        k.to_csv('prediction result lombard 249 P3.csv')
        uni5 = uni4[(uni4['amt_marker'] == 0)]
    
        
        uni5 = uni5.drop(['len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
        dummy = uni5.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency'])['ViewData.Net Amount Difference'].apply(list).reset_index()
        dummy['len_amount'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : len(x))
        dummy['zero_list'] = dummy['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
        dummy['len_zero_list'] = dummy['zero_list'].apply(lambda x : len(x))
        dummy['diff_len'] = dummy['len_amount'] - dummy['len_zero_list']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['len_zero_list'],x['diff_len'],x['zero_list_sum']),axis = 1)
        dummy = dummy[['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency',  'zero_list', 'len_zero_list', 'remove_mark']]
        uni4 = pd.merge(uni3, dummy, on = ['ViewData.Mapped Custodian Account','ViewData.Currency'], how = 'left')
        uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)
        
        
        if uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0) ].shape[0]!=0:
            k = uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0)]
            k['predicted status'] = 'UCB'
            k['predicted action'] = 'Close'
            k['predicted category'] = 'one sided close'
            k['predicted comment'] = 'Match'
            sel_col_1 = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
            k = k[sel_col_1]
            k.to_csv('prediction result lombard 249 P4.csv')
            uni5 = uni4[(uni4['amt_marker'] == 0)]
            uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
            remain_df1 = uni5.copy()
        else:
            uni5 = uni4.copy()
            uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
            remain_df2 = uni5.copy()
            
        
    
    else:
        uni5 = uni4.copy()
        uni5 = uni5.drop(['len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
        remain_df2 = uni5.copy()
else:
    m = 1
    
# ### M cross n architecture for UMB finding using desc

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

filter_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account','ViewData.Fund','ViewData.Task Business Date',
 'ViewData.Currency',
 'ViewData.Asset Type Category',
 'ViewData.Transaction Type',
 'ViewData.Investment Type',
 'ViewData.Prime Broker',
 'ViewData.Ticker',
 'ViewData.Sec Fees',
 'ViewData.Settle Date',
 'ViewData.Trade Date',
 'ViewData.Description']

uni5 = uni5[filter_col]

uni5[['len_0','len_1']] = uni5.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)

uni5['MTM_mark'] = uni5.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)

ob_df= uni5[uni5['MTM_mark'] == 'OB']
side_0 = ob_df[ob_df['ViewData.Side1_UniqueIds']=='BB']
side_0['final_id'] = side_0['ViewData.Side0_UniqueIds']
side_1 = ob_df[ob_df['ViewData.Side0_UniqueIds']=='AA']
side_1['final_id'] = side_1['ViewData.Side1_UniqueIds']
    
side0_otm= uni5[uni5['MTM_mark'] != 'OB']
side0_otm['final_id'] = side0_otm['ViewData.Side0_UniqueIds'].astype(str) + '|' + side0_otm['ViewData.Side1_UniqueIds'].astype(str) 

umb_0 = pd.concat([side_0,side0_otm], axis = 0)

umb_1 = side_1.copy()

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
    aa1 = umb_0[umb_0['ViewData.Task Business Date']==d]
    bb1 = umb_1[umb_1['ViewData.Task Business Date']==d]
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
 
umbmn = pd.concat(appended_data)

umbmn['desc_score'] =  umbmn.apply(lambda x : desc_match(x['SideA.ViewData.Description'],x['SideB.ViewData.Description']), axis = 1)

# ### Input for UMB model

### Now remove those pair where one side is absent
ab_a = umbmn[(umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna())]
ab_b = umbmn[~((umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna()))]

umbk = ab_b[['SideB.ViewData.Side0_UniqueIds', 'SideB.ViewData.Side1_UniqueIds','SideB.final_id', 'SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideA.final_id']]

col_umb = ['SideB.ViewData.Asset Type Category',
        'SideB.ViewData.Fund',
       'SideB.ViewData.Investment Type',
        'SideB.ViewData.Ticker','SideB.ViewData.Transaction Type',
      'SideB.ViewData.Mapped Custodian Account','SideB.ViewData.Currency', 'SideA.ViewData.Asset Type Category',
        'SideA.ViewData.Fund','SideA.ViewData.Investment Type',
        'SideA.ViewData.Ticker','SideA.ViewData.Transaction Type',
      'SideA.ViewData.Mapped Custodian Account','SideA.ViewData.Currency', 'desc_score']

umb_file = ab_b[col_umb]

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

umb_file['SideB.ViewData.Asset Type Category'] = umb_file['SideB.ViewData.Asset Type Category'].fillna('AA')
umb_file['SideB.ViewData.Fund'] = umb_file['SideB.ViewData.Fund'].fillna('BB')
umb_file['SideB.ViewData.Investment Type'] = umb_file['SideB.ViewData.Investment Type'].fillna('CC')
umb_file['SideB.ViewData.Ticker'] = umb_file['SideB.ViewData.Ticker'].fillna('DD')
umb_file['SideB.ViewData.Transaction Type'] = umb_file['SideB.ViewData.Transaction Type'].fillna('EE')
umb_file['SideB.ViewData.Currency'] = umb_file['SideB.ViewData.Currency'].fillna('FF')
umb_file['SideB.ViewData.Mapped Custodian Account'] = umb_file['SideB.ViewData.Mapped Custodian Account'].fillna('GG')

umb_file['SideA.ViewData.Asset Type Category'] = umb_file['SideA.ViewData.Asset Type Category'].fillna('aa')
umb_file['SideA.ViewData.Fund'] = umb_file['SideA.ViewData.Fund'].fillna('bb')
umb_file['SideA.ViewData.Investment Type'] = umb_file['SideA.ViewData.Investment Type'].fillna('cc')
umb_file['SideA.ViewData.Ticker'] = umb_file['SideA.ViewData.Ticker'].fillna('dd')
umb_file['SideA.ViewData.Transaction Type'] = umb_file['SideA.ViewData.Transaction Type'].fillna('ee')
umb_file['SideA.ViewData.Currency'] = umb_file['SideA.ViewData.Currency'].fillna('ff')
umb_file['SideA.ViewData.Mapped Custodian Account'] = umb_file['SideA.ViewData.Mapped Custodian Account'].fillna('gg')

for item in list(umb_file.columns):
    
    x1 = item.split('.')
    if 'desc_score' not in x1:
    
        if x1[0]=='SideB':
            m = 'ViewData.' + 'Accounting'+ " " + x1[2]
            umb_file = umb_file.rename(columns = {item:m})
        else:
            m = 'ViewData.' + 'B-P'+ " " + x1[2]
            umb_file =umb_file.rename(columns = {item:m})

os.chdir('D:\\ViteosModel\\Lombard code for gompu\\')
filename = 'finalized_model_lombard_249_umb_v3.sav'
clf = pickle.load(open(filename, 'rb'))

cb_predictions = clf.predict(umb_file)

demo = []
for item in cb_predictions:
    demo.append(item[0])

umb_file['predicted'] = pd.Series(demo)
#result['Actual'] = pd.Series(list1)

umbpred = pd.concat([umbk,umb_file], axis = 1)

umbpred =umbpred.drop_duplicates()

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
    
# We will segragate IDs on both sides which were just OB.
k1 = umbpred.groupby('SideB.final_id')['predicted'].apply(list).reset_index()
k2 = umbpred.groupby('SideA.final_id')['predicted'].apply(list).reset_index()

def ob_umb(x):
    x1 = list(set(x))
    if ((len(x1)==1) & ('OB' in x1)):
        return 'OB'
    elif ((len(x1)==1) & ('UMB' in x1)):
        return 'UMB'
    else:
        return 'Both'

k1['State'] = k1['predicted'].apply(lambda x : ob_umb(x) )
k2['State'] = k2['predicted'].apply(lambda x : ob_umb(x) )

list_id_0_k1 = list(set(k1[k1['State']=='OB']['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['State']=='OB']['SideA.final_id']))
side0_ob = umb_0[umb_0['final_id'].isin(list_id_0_k1)]
side1_ob = umb_1[umb_1['final_id'].isin(list_id_1_k2)]

ob_2nd_set = pd.concat([side0_ob,side1_ob], axis = 0)
ob_2nd_set = ob_2nd_set.reset_index()
ob_2nd_set = ob_2nd_set.drop('index', axis = 1)

if ((ob_1st_set.shape[0]!=0) & (ob_2nd_set.shape[0]!=0)):
    ob_for_comment = pd.concat([ob_1st_set,ob_2nd_set], axis = 0)
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
elif ((ob_1st_set.shape[0]!=0)):
    ob_for_comment = ob_1st_set.copy()
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
#else:
elif ((ob_2nd_set.shape[0]!=0)):
    ob_for_comment = ob_2nd_set.copy()
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
            
ob_for_comment.to_csv('Ob for comment daily lombard 249.csv')

umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]

# Difficult part segragation of UMBs in OTO, OTM, MTO and MTM
# OTO

def umb_counter(x):
    counter=collections.Counter(x)
    if counter['UMB'] == 1:
        return 1
    else:
        return counter['UMB']
        
k1['umb_counter'] = k1['predicted'].apply(lambda x : umb_counter(x) )
k2['umb_counter'] = k2['predicted'].apply(lambda x : umb_counter(x) )

list_id_0_k1 = list(set(k1[k1['umb_counter']==1]['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['umb_counter']==1]['SideA.final_id']))
if ((len(list_id_0_k1)>0) & (len(list_id_1_k2)>0)):
    umb_oto = umbpred[(umbpred['SideB.final_id'].isin(list_id_0_k1)) & (umbpred['SideA.final_id'].isin(list_id_1_k2))]
    umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
    umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]

# Now We write the hierarchy for many to one
k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
k4 = umbpred.groupby('SideA.final_id')['SideB.final_id'].apply(list).reset_index()

k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
k4['id_len'] = k4['SideB.final_id'].apply(lambda x : len(x) )

def intersection_(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return round((len(lst3)/len(lst1)),1)

def Diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif

ob_stage_df = pd.DataFrame()
umb_mtm = pd.DataFrame()
umb_mtm_list = []
umb_otm_list = []

for i, row in k3.iterrows():
    k5 = k3.copy()
    mid = row['SideB.final_id']
    #print(mid)
    midlist = row['SideA.final_id']
    midlen = row['id_len']
    k6 = k5[(k5['id_len']<midlen+3) & (k5['id_len']>midlen-4)]
    k6['match'] = k6['SideA.final_id'].apply(lambda x:intersection_(x,midlist) )
    
    
    k7 =list(set(k6[k6['match']>0.7]['SideB.final_id']))
    if len(k7)>0:
        
        set_for_int = list((k6[k6['match']>0.7]['SideA.final_id']))
        k8 = list(reduce(set.intersection, [set(item) for item in set_for_int]))
        int1 = umbpred[umbpred['SideB.final_id'].isin(k7)]
        k9 =list(set(int1['SideA.final_id']))
        if ((len(k8)>0) & (len(k9)>0)):
            umb_mtm_list_temp = []
            umb_mtm_list_temp.append(k7)
            umb_mtm_list_temp.append(k8)
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
            
            k15 =list(set(int1['SideB.final_id']))
            k16 = Diff(k15,k7)
            umbpred = umbpred[~umbpred['SideA.final_id'].isin(k8)]
            k17 = list(set(umbpred['SideB.final_id']))
            k18 = Diff(k16,k17)
            ob_4th_set = umb_0[umb_0['final_id'].isin(k18)]
            ob_stage_df = pd.concat([ob_stage_df,ob_4th_set], axis = 0)
            ob_stage_df = ob_stage_df.reset_index()
            ob_stage_df = ob_stage_df.drop('index', axis = 1)
        
            k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
            k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
            
    else:
        #k8 = list(reduce(set.intersection, [set(item) for item in set_for_int]))
#         int1 = k3[k3['SideB.final_id']==mid]
#         print(int1.shape[0])
        mk = list(mid)
  
        if (len(midlist)>0):
#            print(1)
            umb_otm_list_temp = []
            umb_otm_list_temp.append(mid)
            umb_otm_list_temp.append(midlist)
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
        
            k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
            k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
        
umb_mtm = pd.DataFrame(umb_mtm_list)
umb_otm = pd.DataFrame(umb_otm_list)

umb_otm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']
umb_mtm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']

umb_otm['predicted status'] = 'UMB'
umb_otm['predicted action'] = 'UMB one to many'
umb_otm['predicted category'] = 'UMB'
umb_otm['predicted comment'] = 'Difference in amount'

umb_mtm['predicted status'] = 'UMB'
umb_mtm['predicted action'] = 'UMB one to many'
umb_mtm['predicted category'] = 'UMB'
umb_mtm['predicted comment'] = 'Difference in amount'

umb_oto1 = umb_oto[['SideB.final_id','SideA.final_id']]

umb_oto1 = umb_oto1.rename(columns = {'SideB.final_id':'ViewData.Side0_UniqueIds',
                                     'SideA.final_id':'ViewData.Side1_UniqueIds'})
umb_final = pd.concat([umb_otm,umb_mtm,umb_oto1], axis = 0)

umb_final = umb_final.reset_index()
umb_final = umb_final.drop('index', axis = 1)

umb_final.to_csv('umb lombard 249 v1.csv')

k = ob_stage_df.drop_duplicates()

ob_for_comment = pd.read_csv('Ob for comment daily lombard 249.csv')

ob_for_comment = ob_for_comment.drop('Unnamed: 0', axis = 1)

ob_for_comment_model = pd.concat([ob_for_comment,k], axis = 0)

ob_for_comment_model = ob_for_comment_model.reset_index()
ob_for_comment_model = ob_for_comment_model.drop('index', axis = 1)

ob_for_comment_model.to_csv('Ob for comment daily lombard 249 v1 final.csv')

# Now we take all OBs to single side model

df3 = ob_for_comment_model.copy()

df3 = pd.read_csv('Ob for comment daily lombard 249 v1 final.csv')
df3 = df3.drop('Unnamed: 0', axis = 1)

df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')
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


# In[10]:


import re
#TODO : Abhijeet will send file for Lombard for desc cat
com = pd.read_csv('desc cat with naveen oaktree.csv')
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


# In[11]:


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


# In[20]:


df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])
days = [1,30,31,29]
df3['monthend marker'] = df3['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[21]:


df3['comm_marker'] = 'zero'
df3['new_pb2'] = df3.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
df3['new_pb2'] = df3['new_pb2'].apply(lambda x : x.lower())


# In[22]:


df3.columns


# In[24]:


cols = ['ViewData.Transaction Type1',
 'ViewData.Asset Type Category1',
 'ViewData.Investment Type1',
 'new_desc_cat',
 'new_pb2',
 'new_pb1',
 'comm_marker',
 'monthend marker']


# In[25]:


df4 = df3[cols]


# In[31]:


df4['ViewData.Transaction Type1'] = df4['ViewData.Transaction Type1'].fillna('aa')
df4['ViewData.Asset Type Category1'] = df4['ViewData.Asset Type Category1'].fillna('aa')
df4['ViewData.Investment Type1'] = df4['ViewData.Investment Type1'].fillna('aa')
df4['new_desc_cat'] = df4['new_desc_cat'].fillna('aa')
df4['new_pb2'] = df4['new_pb2'].fillna('aa')
df4['new_pb1'] = df4['new_pb1'].fillna('aa')
df4['comm_marker'] = df4['comm_marker'].fillna('aa')
df4['monthend marker'] = df4['monthend marker'].fillna('aa')


# In[32]:


import pickle
filename = 'finalized_model_lombard_249_v1.sav'
clf = pickle.load(open(filename, 'rb'))
cb_predictions = clf.predict(df4)

demo = []
for item in cb_predictions:
    demo.append(item[0])
df3['predicted category'] = pd.Series(demo)

temp = df3['predicted category'] .value_counts().reset_index()

temp = temp.rename(columns = {'index':'Category'})

temp['template'] = 'to book' + ' ' + temp['Category'] + ' ' + "on sd "

temp.drop('predicted category', axis =1 , inplace = True)

temp.to_csv('lobard 249 comment template for delivery.csv')

df3.to_csv('lombard 1st comment.csv')

com_temp = pd.read_csv('lobard 249 comment template for delivery.csv')
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
result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']]
result_non_trade.to_csv('Comment file for lombard 249.csv')


# In[ ]:

if ('zero_list' in list(uni3.columns) == True):
    uni3.drop(['zero_list'], axis = 1, inplace = True)
if ('diff_len' in list(uni3.columns) == True):
    uni3.drop(['diff_len'], axis = 1, inplace = True)
if ('remove_mark' in list(uni3.columns) == True):
    uni3.drop(['remove_mark'], axis = 1, inplace = True)
if ('sel_mark' in list(uni3.columns) == True):
    uni3.drop(['sel_mark'], axis = 1, inplace = True)

# dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'] ,x['diff_len'], x['zero_list_sum']),axis = 1)
# dummy = dummy[['ViewData.Task Business Date','Custodian Account', 'Currency', 'zero_list',  'diff_len', 'remove_mark','sel_mark']]
df3 = pd.merge(uni3, dummy, on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
df4 = df3[(df3['sel_mark']==1) & (df3['sel_mark']==1)]
df4['Predicted Category'] = 'Match'
df4['Predicted Comment'] = 'Match'
df5 = df3[~((df3['sel_mark']==1) & (df3['sel_mark']==1))]


# In[ ]:


df3['remove_mark'].value_counts()


# #### Cleaning of description

# In[ ]:


uni3['ViewData.Description'] = uni3['ViewData.Description'].apply(lambda x : x.lower() if type(x)==str else x)


# In[ ]:


dollar = ['u.s. dollars','us dollar','']
pound = ['']


# In[ ]:


dummy = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()
dummy1 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'])['ViewData.Side0_UniqueIds'].count().reset_index()
dummy = pd.merge(dummy, dummy1 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'], how = 'left')
dummy2 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'])['ViewData.Side1_UniqueIds'].count().reset_index()
dummy = pd.merge(dummy, dummy2 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'], how = 'left')
dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
dummy = dummy[dummy['sel_mark']==1]


# ### Cleannig of the 4 variables in this

# In[ ]:


df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')


# In[ ]:


def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys


# In[ ]:


df['tuple'] = df.apply(make_dict, axis=1)


# In[ ]:


clean_map_dict = df.set_index('tuple')['Value'].to_dict()


# In[ ]:


df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Asset Type Category'] = df2['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Investment Type'] = df2['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Prime Broker'] = df2['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)


# In[ ]:


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
        


# In[ ]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)


# In[ ]:


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


# In[ ]:



import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[ ]:


def comb_clean(x):
    k = []
    for item in x.split():
        if ((is_num(item)==False) and (is_date_format(item)==False) and (date_edge_cases(item)==False)):
            k.append(item)
    return ' '.join(k)


# In[ ]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[ ]:


df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[ ]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : 'paydown' if x=='pay down' else x)


# In[ ]:


def divclient(x):
    if (type(x) == str):
        if ('eqswap div client tax' in x) :
            return 'eqswap div client tax'
        else:
            return x
    else:
        return 'float'


# In[ ]:



def mhreplace(item):
    item1 = item.split()
    for items in item1:
        if items.endswith('mh')==True:
            item1.remove(items)
    return ' '.join(item1).lower()


# In[ ]:


def compname(x):
    m = 0
    comp = ['corporate','stk','inc','lp','plc','inc.','inc','corp']
    if type(x)==str:
        x1 = x.split()
        for item in x1:
            if item in comp:
                m = m+1
    else:
        m = 0
    
    if m ==0:
        return x
    else:
        return 'Company'


# In[ ]:


def wht(x):
    if type(x)==str:
        x1 = x.split()
        if x1[0] =='30%':
            return 'Wht'
        else:
            return x
    else:
        return x


# In[ ]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : divclient(x))
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : mhreplace(x))
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : compname(x))
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : divclient(x))


# In[ ]:





# In[ ]:


df2['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply( lambda x : item[:2] if '30%' in x else x)


# ### Cleaning of Description

# In[ ]:


com = pd.read_csv('desc cat with naveen.csv')


# In[ ]:


cat_list = list(set(com['Pairing']))


# In[ ]:


import re


# In[ ]:



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
            


# In[ ]:


df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[ ]:


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
        


# In[ ]:


df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))


# In[ ]:


com = com.drop(['var','Catogery'], axis = 1)


# In[ ]:


com = com.drop_duplicates()


# In[ ]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[ ]:


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


# In[ ]:


df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))


# In[ ]:


comp = ['inc','stk','corp ','llc','pvt','plc']
df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[ ]:


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


# In[ ]:


df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : desccat(x))


# #### Prime Broker Creation

# In[ ]:


df2['new_pb'] = df2['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[ ]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[ ]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[ ]:


df2['new_pb'] = df2['new_pb'].apply(lambda x : new_pf_mapping(x))


# In[ ]:


df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].fillna('kkk')


# In[ ]:


df2['new_pb1'] = df2.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)


# In[ ]:


df2['new_pb1'] = df2['new_pb1'].apply(lambda x : x.lower())


# #### Cancelled Trade Removal

# In[ ]:


#trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']

#dfkk = df2[df2['ViewData.Transaction Type1'].isin(trade_types)]

#dfk_nontrade = df2[~df2['ViewData.Transaction Type1'].isin(trade_types)]

#dffk2 = dfkk[dfkk['ViewData.Side0_UniqueIds']=='AA']
#dffk3 = dfkk[dfkk['ViewData.Side1_UniqueIds']=='BB']


#dffk4 = dfk_nontrade[dfk_nontrade['ViewData.Side0_UniqueIds']=='AA']
#dffk5 = dfk_nontrade[dfk_nontrade['ViewData.Side1_UniqueIds']=='BB']


# #### Geneva side

def canceltrade(x,y):
    if x =='buy' and y>0:
        k = 1
    elif x =='sell' and y<0:
        k = 1
    else:
        k = 0
    return k

#dffk3['cancel_marker'] = dffk3.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Accounting Net Amount']), axis = 1)

def cancelcomment(x,y):
    com1 = 'This is original of cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com

def cancelcomment1(x,y):
    com1 = 'This is cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[ ]:


# if dffk3[dffk3['cancel_marker'] == 1].shape[0]!=0:
#     cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))
#     if len(cancel_trade)>0:
#         km = dffk3[dffk3['cancel_marker'] != 1]
#         original = km[km['ViewData.Transaction ID'].isin(cancel_trade)]
#         original['predicted category'] = 'Original of Cancelled trade'
#         original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancellation = dffk3[dffk3['cancel_marker'] == 1]
#         cancellation['predicted category'] = 'Cancelled trade'
#         cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancel_fin = pd.concat([original,cancellation])
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#         cancel_fin = cancel_fin[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing p1.csv')
#         dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
        
#     else:
#         cancellation = dffk3[dffk3['cancel_marker'] == 1]
#         cancellation['predicted category'] = 'Cancelled trade'
#         cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancel_fin = pd.concat([original,cancellation])
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#         cancel_fin = cancel_fin[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing no original p2.csv')
#         dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
# else:
#     dffk3 = dffk3.copy()

# #### Broker side


#dffk2['cancel_marker'] = dffk2.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Cust Net Amount']), axis = 1)

def amountelim(row):
   
    if (row['SideA.ViewData.Mapped Custodian Account'] == row['SideB.ViewData.Mapped Custodian Account']):
        a = 1
    else:
        a = 0
        
    if ((row['SideB.ViewData.Cust Net Amount']) == -(row['SideA.ViewData.Cust Net Amount'])):
        b = 1
    else:
        b = 0
    
    if (row['SideA.ViewData.Fund'] == row['SideB.ViewData.Fund']):
        c = 1
    else:
        c = 0
        
    if (row['SideA.ViewData.Currency'] == row['SideB.ViewData.Currency']):
        d = 1
    else:
        d = 0
    
    if (row['SideA.ViewData.Settle Date1'] == row['SideB.ViewData.Settle Date1']):
        e = 1
    else:
        e = 0
        
    if (row['SideA.ViewData.Transaction Type1'] == row['SideB.ViewData.Transaction Type1']):
        f = 1
    else:
        f = 0
        
    if (row['SideB.ViewData.Quantity'] == row['SideA.ViewData.Quantity']):
        g = 1
    else:
        g = 0
        
    if (row['SideB.ViewData.ISIN'] == row['SideA.ViewData.ISIN']):
        h = 1
    else:
        h = 0
        
    if (row['SideB.ViewData.CUSIP'] == row['SideA.ViewData.CUSIP']):
        i = 1
    else:
        i = 0
        
    if (row['SideB.ViewData.Ticker'] == row['SideA.ViewData.Ticker']):
        j = 1
    else:
        j = 0
        
    if (row['SideB.ViewData.Investment ID'] == row['SideA.ViewData.Investment ID']):
        k = 1
    else:
        k = 0
        
    return a, b, c ,d, e,f,g,h,i,j,k
    

def cancelcomment2(y):
    com1 = 'This is original of cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' '  + com2 +' ' + str(y)
    return com

def cancelcomment3(y):
    com1 = 'This is cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' ' + com2 + ' ' + str(y)
    return com

# if dffk2[dffk2['cancel_marker'] == 1].shape[0]!=0:
#     dummy1 = dffk2[dffk2['cancel_marker']!=1]
#     dummy1 = dffk2[dffk2['cancel_marker']==1]


#     pool =[]
#     key_index =[]
#     training_df =[]
#     call1 = []

#     appended_data = []

#     no_pair_ids = []
# #max_rows = 5

#     k = list(set(list(set(dummy['ViewData.Task Business Date1']))))
#     k1 = k

#     for d in tqdm(k1):
#         aa1 = dummy[dummy['ViewData.Task Business Date1']==d]
#         bb1 = dummy1[dummy1['ViewData.Task Business Date1']==d]
#         aa1['marker'] = 1
#         bb1['marker'] = 1
    
#         aa1 = aa1.reset_index()
#         aa1 = aa1.drop('index',1)
#         bb1 = bb1.reset_index()
#         bb1 = bb1.drop('index', 1)
#         #print(aa1.shape)
#         #print(bb1.shape)
    
#         aa1.columns = ['SideB.' + x  for x in aa1.columns] 
#         bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
#         cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
#         appended_data.append(cc1)
#         cancel_broker = pd.concat(appended_data)
#         cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
#         elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]
#         if elim1.shape[0]!=0:
#             id_listA = list(set(elim1['SideA.final_ID']))
#             c1 = dummy
#             c2 = dummy1[dummy1['final_ID'].isin(id_listA)]
#             c1['predicted category'] = 'Cancelled trade'
#             c2['predicted category'] = 'Original of Cancelled trade'
#             c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
#             c2['predicted comment'] = c2.apply(lambda x : cancelcomment3(x['ViewData.Settle Date1']))
#             cancel_fin = pd.concat([c1,c2])
#             cancel_fin = cancel_fin.reset_index()
#             cancel_fin = cancel_fin.drop('index', axis = 1)
#             sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#             cancel_fin = cancel_fin[sel_col_1]
#             cancel_fin.to_csv('Comment file soros 2 sep testing p3.csv')
#             id_listB = list(set(c1['final_ID']))
#             comb = id_listB + id_listA
#             dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
            
#     else:
#         c1 = dummy
#         c1['predicted category'] = 'Cancelled trade'
#         c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#         cancel_fin = c1[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing no original p4.csv')
#         id_listB = list(set(c1['final_ID']))
#         comb = id_listB
#         dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
        
# else:
#     dffk2 = dffk2.copy()

# #### Finding Pairs in Up and down

df2 = df2.rename(columns = {'ViewData.B-P Net Amount':'ViewData.Cust Net Amount'})

dffk2 = df2[df2['ViewData.Side0_UniqueIds']=='AA']
dffk3 = df2[df2['ViewData.Side1_UniqueIds']=='BB']

sel_col = ['final_ID',  'ViewData.Currency',
       'ViewData.Accounting Net Amount',
       
       'ViewData.Asset Type Category', 
       'ViewData.Cust Net Amount', 'ViewData.BreakID',
       'ViewData.ClusterID',
       'ViewData.CUSIP', 'ViewData.Description', 'ViewData.Fund',
        'ViewData.Investment ID',
       'ViewData.Investment Type', 
       'ViewData.ISIN', 'ViewData.Keys', 
       'ViewData.Mapped Custodian Account',  'ViewData.Prime Broker',
       
       'ViewData.Quantity','ViewData.Settle Date1', 
       'ViewData.Status', 'ViewData.Strategy', 
       'ViewData.Ticker', 'ViewData.Trade Date1', 
       'ViewData.Transaction Type', 
       'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds', 
      'ViewData.Task Business Date1','ViewData.InternalComment2'
      ]

dffpb = dffk2[sel_col]
dffacc = dffk3[sel_col]

bplist = dffpb.groupby('ViewData.Task Business Date1')['ViewData.Cust Net Amount'].apply(list).reset_index()
acclist = dffacc.groupby('ViewData.Task Business Date1')['ViewData.Accounting Net Amount'].apply(list).reset_index()

updlist = pd.merge(bplist, acclist, on = 'ViewData.Task Business Date1', how = 'inner')

updlist['upd_amt'] = updlist.apply(lambda x : [value for value in x['ViewData.Cust Net Amount'] if value in x['ViewData.Accounting Net Amount']], axis = 1)

updlist = updlist[['ViewData.Task Business Date1','upd_amt']]

dffpb = pd.merge(dffpb, updlist, on = 'ViewData.Task Business Date1', how = 'left')
dffacc = pd.merge(dffacc, updlist, on = 'ViewData.Task Business Date1', how = 'left')

dffpb['upd_amt']= dffpb['upd_amt'].fillna('MMM')
dffacc['upd_amt']= dffacc['upd_amt'].fillna('MMM')


# In[ ]:


def updmark(y,x):
    if x =='MMM':
        return 0
    else:
        if y in x:
            return 1
        else:
            return 0


# In[ ]:


dffpb['upd_mark'] = dffpb.apply(lambda x :  updmark(x['ViewData.Cust Net Amount'], x['upd_amt']) , axis= 1)
dffacc['upd_mark'] = dffacc.apply(lambda x : updmark(x['ViewData.Accounting Net Amount'], x['upd_amt']) , axis= 1)


# In[ ]:


dff4 = dffpb[dffpb['upd_mark']==1]
dff5 = dffacc[dffacc['upd_mark']==1]


# In[ ]:


#dff6 = dffk4[sel_col]
#dff7 = dffk5[sel_col]


# In[ ]:


# dff4 = pd.concat([dff4,dff6])
# dff4 = dff4.reset_index()
# dff4 = dff4.drop('index', axis = 1)


# In[ ]:


# dff5 = pd.concat([dff5,dff7])
# dff5 = dff5.reset_index()
# dff5 = dff5.drop('index', axis = 1)


# In[ ]:


def amountelim(row):
   
    if (row['SideA.ViewData.Mapped Custodian Account'] == row['SideB.ViewData.Mapped Custodian Account']):
        a = 1
    else:
        a = 0
        
    if (row['SideB.ViewData.Cust Net Amount'] == row['SideA.ViewData.Accounting Net Amount']):
        b = 1
    else:
        b = 0
    
    if (row['SideA.ViewData.Fund'] == row['SideB.ViewData.Fund']):
        c = 1
    else:
        c = 0
        
    if (row['SideA.ViewData.Currency'] == row['SideB.ViewData.Currency']):
        d = 1
    else:
        d = 0
        
    
        
        
        
    return a, b, c ,d


# In[ ]:


def updownat(a,b,c,d,):
    if a == 0:
        k = 'mapped custodian account'
    elif b==0:
        k = 'currency'
    elif c ==0 :
        k = 'Settle Date'
    elif d == 0:
        k = 'fund'    
  
    else :
        k = 'Investment type'
        
    com = 'up/down at'+ ' ' + k
    return com


# #### M cross N code

# In[ ]:


###################### loop 3 ###############################
from pandas import merge
from tqdm import tqdm
if ((dff4.shape[0]!=0) & (dff5.shape[0]!=0)):
    pool =[]
    key_index =[]
    training_df =[]
    call1 = []

    appended_data = []

    no_pair_ids = []
#max_rows = 5

    k = list(set(list(set(dff5['ViewData.Task Business Date1'])) + list(set(dff4['ViewData.Task Business Date1']))))
    k1 = k[0:3]

    for d in tqdm(k1):
        aa1 = dff4[dff4['ViewData.Task Business Date1']==d]
        bb1 = dff5[dff5['ViewData.Task Business Date1']==d]
        aa1['marker'] = 1
        bb1['marker'] = 1
    
        aa1 = aa1.reset_index()
        aa1 = aa1.drop('index',1)
        bb1 = bb1.reset_index()
        bb1 = bb1.drop('index', 1)
        print(aa1.shape)
        print(bb1.shape)
    
        aa1.columns = ['SideB.' + x  for x in aa1.columns] 
        bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
        cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
        appended_data.append(cc1)
        
    df_213_1 = pd.concat(appended_data)
    df_213_1[['map_match','amt_match','fund_match','curr_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
    df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['fund_match'] + df_213_1['curr_match']
    elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]
#     if elim1.shape[0]!=0:
#         elim1['SideA.predicted category'] = 'Updown'
#         elim1['SideB.predicted category'] = 'Updown'
#         elim1['SideA.Predicted_action'] = 'No-Pair'
#         elim1['SideB.Predicted_action'] = 'No-Pair'
#         elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
#         elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
#         elim_col = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment','Predicted_action']
    
    
#         elim_col = list(elim1.columns)

#         for items in elim_col:
#             item = 'SideA.'+items
#             sideA_col.append(item)
#             item = 'SideB.'+items
#             sideB_col.append(item)
        
#         elim2 = elim1[sideA_col]
#         elim3 = elim1[sideB_col]
    
#         elim2 = elim2.rename(columns= {'SideA.final_ID':'final_ID',
#                               'SideA.predicted category':'predicted category',
#                               'SideA.predicted comment':'predicted comment'})
#         elim3 = elim3.rename(columns= {'SideB.final_ID':'final_ID',
#                               'SideB.predicted category':'predicted category',
#                               'SideB.predicted comment':'predicted comment'})
#         frames = [elim2,elim3]
#         elim = pd.concat(frames)
#         elim = elim.reset_index()
#         elim = elim.drop('index', axis = 1)
#         elim.to_csv('Comment file soros 2 sep testing p5.csv')
        
#         ## TODO : Rohit to write elimination code here
        
#     else:
#         aa_new = aa_new.copy()
#         bb_new = bb_new.copy()
# else:
#     aa_new = aa_new.copy()
#     bb_new = bb_new.copy()
    


# In[ ]:


elim1.head(4)


# In[ ]:


elim1['SideA.ViewData.InternalComment2'].value_counts()


# #### Start of the single Side Commenting

# In[ ]:


data = df2.copy()


# In[ ]:


# data = pd.concat(frames)
# data = data.reset_index()
# data = data.drop('index', axis = 1)


# In[ ]:


#data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])


# In[ ]:


# days = [1,30,31,29]
# data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[ ]:


# data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')


# In[ ]:


def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k


# In[ ]:


#data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))


# In[ ]:


data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)


# In[ ]:


Pre_final = [
    
'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID',
 


 'ViewData.Currency',
 'ViewData.Custodian',
     'ViewData.ISIN',

 'ViewData.Mapped Custodian Account',
 
 'ViewData.Net Amount Difference Absolute',
 
 'ViewData.Portolio',
 'ViewData.Settle Date',
 
 'ViewData.Trade Date',
 'ViewData.Transaction Type1',
'new_desc_cat',
    'ViewData.Department',

 
 
 
 'ViewData.Accounting Net Amount',
 'ViewData.Asset Type Category1',
 
 
 'ViewData.CUSIP',
 'ViewData.Commission',
 
 'ViewData.Fund',
 
 
 'ViewData.Investment ID',
 'ViewData.Investment Type1',
 
 
 'ViewData.Price',
 'ViewData.Prime Broker1',

 'ViewData.Quantity',
 
'ViewData.InternalComment2', 'ViewData.Description','new_pb2','new_pb1'
   
 
]


# In[ ]:


data = data[Pre_final]


# In[ ]:


df_mod1 = data.copy()


# In[ ]:


df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].fillna('AA')
df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].fillna('bb')
df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].fillna(0)
df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].fillna(0)
df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].fillna(0)
df_mod1['ViewData.Asset Type Category1'] = df_mod1['ViewData.Asset Type Category1'].fillna('CC')
df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].fillna('DD')
df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].fillna('EE')
df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].fillna('FF')
df_mod1['ViewData.Investment Type1'] = df_mod1['ViewData.Investment Type1'].fillna('GG')
#df_mod1['ViewData.Knowledge Date'] = df_mod1['ViewData.Knowledge Date'].fillna(0)
df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].fillna(0)
df_mod1['ViewData.Prime Broker1'] = df_mod1['ViewData.Prime Broker1'].fillna("HH")
df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].fillna(0)
#df_mod1['ViewData.Sec Fees'] = df_mod1['ViewData.Sec Fees'].fillna(0)
#df_mod1['ViewData.Strike Price'] = df_mod1['ViewData.Strike Price'].fillna(0)
df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].fillna(0)
df_mod1['ViewData.Transaction Type1'] = df_mod1['ViewData.Transaction Type1'].fillna('kk')
df_mod1['ViewData.ISIN'] = df_mod1['ViewData.ISIN'].fillna('mm')
df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].fillna('nn')
#df_mod1['Category'] = df_mod1['Category'].fillna('NA')
df_mod1['ViewData.Description'] = df_mod1['ViewData.Description'].fillna('nn')
df_mod1['ViewData.Department'] = df_mod1['ViewData.Department'].fillna('nn')


# In[ ]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[ ]:


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[ ]:


data2 = df_mod1.copy()


# ### Separate Prediction of the Trade and Non trade

# #### 1st for Non Trade

# In[ ]:


#trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[ ]:


#data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]


# In[ ]:


cols = [
 


 'ViewData.Transaction Type1',
 
 

 'ViewData.Asset Type Category1',

 'new_desc_cat',

 'ViewData.Investment Type1',
 

 'new_pb2','new_pb1'
 
 
              
             ]


# In[ ]:


data211 = data2[cols]


# In[ ]:


filename = 'finalized_model_weiss_catrefine_v8.sav'


# In[ ]:


clf = pickle.load(open(filename, 'rb'))
# Actual class predictions
cb_predictions = clf.predict(data211)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]

# #### Testing of Model and final prediction file - Non Trade

demo = []
for item in cb_predictions:
    demo.append(item[0])

result_non_trade =data2.copy()
result_non_trade = result_non_trade.reset_index()
result_non_trade['predicted category'] = pd.Series(demo)
result_non_trade['predicted comment'] = 'NA'

#result_non_trade = result_non_trade.drop('predicted comment', axis = 1)

#com_temp = pd.read_csv('Soros comment template for delivery.csv')

#com_temp = com_temp.rename(columns = {'Category ':'predicted category','template':'predicted template'})

#result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')

def comgen(x,y,z,k):
    if x == 'Geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "Geneva" + ' ' +y + ' ' + str(z)
        
    return com

#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)

result_non_trade = result_non_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']]

result_non_trade.to_csv('Comment file Weiss 2 sep testing p6.csv')
