#!/usr/bin/env python
# coding: utf-8

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


#uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')

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


# In[116]:


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


# In[13]:


uni2['MTM_mark'] = uni2.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)


# In[233]:


uni3 = uni2.copy()


# In[ ]:





# In[234]:


# Aggregation filters applied. Custodian account | Currency | Description

dfk = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Net Amount Difference'].apply(list).reset_index()
dfk1 = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['len_0'].sum().reset_index()
dfk2 = uni3.groupby(['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['len_1'].sum().reset_index()


# In[235]:


df_merge = pd.merge(dfk,dfk1, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left')
df_merge = pd.merge(df_merge,dfk2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left')


# In[236]:


df_merge['single_sided'] = df_merge.apply(lambda x : 1 if ((x['len_0']==0) | (x['len_1']==0)) else 0, axis =1 )


# In[237]:


df_merge['len_amount'] = df_merge['ViewData.Net Amount Difference'].apply(lambda x : len(x) )


# In[238]:


# We seaparte single sided and double sided reconcialiation here. 1 being single sided. We reconcile single sided first
df_merge1 = df_merge[df_merge['single_sided']==1]
df_merge2 = df_merge[df_merge['single_sided']==0]


# In[239]:


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


# In[240]:


def amt_marker(x,y,z):
    if type(y)==list:
        if ((x in y) & ((z<16) & (z>=2))) :
            return 1
        else:
            return 0
    else:
        return 0


# In[241]:


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


# In[242]:


if df_merge1.shape[0]!=0:
    df_merge1['zero_list'] = df_merge1['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
    df_merge1['len_zero_list'] = df_merge1['zero_list'].apply(lambda x : len(x))
    df_merge1 = df_merge1.drop(['len_0','len_1','single_sided','ViewData.Net Amount Difference'], axis = 1)
    uni4 = pd.merge(uni3, df_merge1, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
    uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)

    if uni4[uni4['amt_marker'] != 0].shape[0]!=0:
#This is proper closed
        k = uni4[(uni4['amt_marker'] == 1)]
        k['predicted_Status'] = 'UCB'
        k['predicted_action'] = 'Close'
        k['predicted category'] = 'one sided close'
        k['predicted comment'] = 'Match'
        sel_col_1 = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted_Status','predicted_action','predicted category','predicted comment']
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
#This is proper closed
            k = uni4[(uni4['amt_marker'] != 0) & (uni4['remove_mark'] != 0)]
            k['predicted_Status'] = 'UCB'
            k['predicted_action'] = 'Close'
            k['predicted category'] = 'one sided close'
            k['predicted comment'] = 'Match'
            sel_col_1 = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted_Status','predicted_action','predicted category','predicted comment']
            k = k[sel_col_1]
            k.to_csv('prediction result lombard 249 P2.csv')
            uni5 = uni4[(uni4['amt_marker'] == 0)]
            uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
            remain_df1 = uni5.copy()
        else:
            uni5 = uni4.copy()
            uni5 = uni5.drop(['zero_list', 'len_zero_list', 'remove_mark','amt_marker'], axis = 1)
            remain_df1 = uni5.copy()
            
        
    
    else:
        uni5 = uni4.copy()
        uni5 = uni5.drop(['len_0','len_1','len_amount', 'zero_list', 'len_zero_list','amt_marker'], axis = 1)
        remain_df1 = uni5.copy()
else:
    m = 1
    remain_df1 = uni3.copy()
    


# In[243]:


remain_df1.shape


# In[206]:


df_merge2


# In[244]:


remain_df1.columns


# In[245]:


remain_df1 = remain_df1.drop(['len_0','len_1', 'MTM_mark'], axis = 1)


# In[246]:


uni3 = remain_df1.copy()


# In[247]:


df_merge2.columns


# In[248]:


if df_merge2.shape[0]!=0:
    df_merge2['zero_list'] = df_merge2['ViewData.Net Amount Difference'].apply(lambda x : subSum(x,0))
    df_merge2['len_zero_list'] = df_merge2['zero_list'].apply(lambda x : len(x))
    df_merge2 = df_merge2.drop(['len_0','len_1','single_sided','ViewData.Net Amount Difference'], axis = 1)
    uni4 = pd.merge(uni3, df_merge2, on = ['ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
    uni4['amt_marker'] = uni4.apply(lambda x : amt_marker(x['ViewData.Net Amount Difference'],x['zero_list'] ,x['len_zero_list']) , axis = 1)

    if uni4[uni4['amt_marker'] != 0].shape[0]!=0:

        k = uni4[(uni4['amt_marker'] == 1)]
        k['predicted status'] = 'UMR'
        k['predicted action'] = 'UMR_all_types'
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
        dummy = dummy[['ViewData.Mapped Custodian Account','ViewData.Currency',  'zero_list', 'len_zero_list', 'remove_mark']]
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
    remain_df2 = uni3.copy()


# In[212]:


uni5.columns


# ### M cross n architecture for UMB finding using desc

# In[225]:


import re


# In[226]:


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

# In[249]:


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


# In[250]:


uni5 = uni5[filter_col]


# In[251]:


uni5[['len_0','len_1']] = uni5.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)


# In[252]:


uni5['MTM_mark'] = uni5.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)


# In[253]:


uni5['MTM_mark'].value_counts()


# In[254]:


ob_df= uni5[uni5['MTM_mark'] == 'OB']
side_0 = ob_df[ob_df['ViewData.Side1_UniqueIds']=='BB']
side_0['final_id'] = side_0['ViewData.Side0_UniqueIds']
side_1 = ob_df[ob_df['ViewData.Side0_UniqueIds']=='AA']
side_1['final_id'] = side_1['ViewData.Side1_UniqueIds']
    


# In[255]:


side0_otm= uni5[uni5['MTM_mark'] != 'OB']
side0_otm['final_id'] = side0_otm['ViewData.Side0_UniqueIds'].astype(str) + '|' + side0_otm['ViewData.Side1_UniqueIds'].astype(str) 


# In[256]:


umb_0 = pd.concat([side_0,side0_otm], axis = 0)


# In[257]:


umb_1 = side_1.copy()


# In[259]:


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
 


# In[260]:


umbmn = pd.concat(appended_data)


# In[526]:


umbmn['desc_score'] =  umbmn.apply(lambda x : desc_match(x['SideA.ViewData.Description'],x['SideB.ViewData.Description']), axis = 1)


# ### Input for UMB model

# In[528]:


### Now remove those pair where one side is absent
ab_a = umbmn[(umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna())]
ab_b = umbmn[~((umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna()))]


# In[529]:


umbk = ab_b[['SideB.ViewData.Side0_UniqueIds', 'SideB.ViewData.Side1_UniqueIds','SideB.final_id', 'SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideA.final_id']]


# In[280]:


col_umb = ['SideB.ViewData.Asset Type Category',
        'SideB.ViewData.Fund',
       'SideB.ViewData.Investment Type',
        'SideB.ViewData.Ticker','SideB.ViewData.Transaction Type',
      'SideB.ViewData.Mapped Custodian Account','SideB.ViewData.Currency', 'SideA.ViewData.Asset Type Category',
        'SideA.ViewData.Fund','SideA.ViewData.Investment Type',
        'SideA.ViewData.Ticker','SideA.ViewData.Transaction Type',
      'SideA.ViewData.Mapped Custodian Account','SideA.ViewData.Currency', 'desc_score']


# In[530]:


umb_file = ab_b[col_umb]


# In[531]:


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


# In[532]:


umb_file['SideB.ViewData.Asset Type Category'] = umb_file['SideB.ViewData.Asset Type Category'].fillna('AA')
umb_file['SideB.ViewData.Fund'] = umb_file['SideB.ViewData.Fund'].fillna('BB')
umb_file['SideB.ViewData.Investment Type'] = umb_file['SideB.ViewData.Investment Type'].fillna('CC')
umb_file['SideB.ViewData.Ticker'] = umb_file['SideB.ViewData.Ticker'].fillna('DD')
umb_file['SideB.ViewData.Transaction Type'] = umb_file['SideB.ViewData.Transaction Type'].fillna('EE')
umb_file['SideB.ViewData.Currency'] = umb_file['SideB.ViewData.Currency'].fillna('FF')
umb_file['SideB.ViewData.Mapped Custodian Account'] = umb_file['SideB.ViewData.Mapped Custodian Account'].fillna('GG')


# In[533]:


umb_file['SideA.ViewData.Asset Type Category'] = umb_file['SideA.ViewData.Asset Type Category'].fillna('aa')
umb_file['SideA.ViewData.Fund'] = umb_file['SideA.ViewData.Fund'].fillna('bb')
umb_file['SideA.ViewData.Investment Type'] = umb_file['SideA.ViewData.Investment Type'].fillna('cc')
umb_file['SideA.ViewData.Ticker'] = umb_file['SideA.ViewData.Ticker'].fillna('dd')
umb_file['SideA.ViewData.Transaction Type'] = umb_file['SideA.ViewData.Transaction Type'].fillna('ee')
umb_file['SideA.ViewData.Currency'] = umb_file['SideA.ViewData.Currency'].fillna('ff')
umb_file['SideA.ViewData.Mapped Custodian Account'] = umb_file['SideA.ViewData.Mapped Custodian Account'].fillna('gg')


# In[534]:


for item in list(umb_file.columns):
    
    x1 = item.split('.')
    if 'desc_score' not in x1:
    
        if x1[0]=='SideB':
            m = 'ViewData.' + 'Accounting'+ " " + x1[2]
            umb_file = umb_file.rename(columns = {item:m})
        else:
            m = 'ViewData.' + 'B-P'+ " " + x1[2]
            umb_file =umb_file.rename(columns = {item:m})


# In[535]:
import os
os.chdir('D:\\ViteosModel\\Lombard code for gompu\\New Lombard code for gompu\\')

import pickle
filename = 'finalized_model_lombard_249_umb_v3.sav'
clf = pickle.load(open(filename, 'rb'))


# In[536]:


cb_predictions = clf.predict(umb_file)


# In[537]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[538]:


umb_file['predicted'] = pd.Series(demo)
#result['Actual'] = pd.Series(list1)


# In[539]:


umb_file['predicted'].value_counts()


# In[858]:


umb_file.columns


# In[859]:


umbpred = pd.concat([umbk,umb_file], axis = 1)


# In[860]:


umbpred =umbpred.drop_duplicates()


# In[52]:


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
    
    


# In[867]:


# We will segragate IDs on both sides which were just OB.
k1 = umbpred.groupby('SideB.final_id')['predicted'].apply(list).reset_index()
k2 = umbpred.groupby('SideA.final_id')['predicted'].apply(list).reset_index()


# In[868]:


def ob_umb(x):
    x1 = list(set(x))
    if ((len(x1)==1) & ('OB' in x1)):
        return 'OB'
    elif ((len(x1)==1) & ('UMB' in x1)):
        return 'UMB'
    else:
        return 'Both'


# In[869]:


k1['State'] = k1['predicted'].apply(lambda x : ob_umb(x) )
k2['State'] = k2['predicted'].apply(lambda x : ob_umb(x) )


# In[870]:


list_id_0_k1 = list(set(k1[k1['State']=='OB']['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['State']=='OB']['SideA.final_id']))
side0_ob = umb_0[umb_0['final_id'].isin(list_id_0_k1)]
side1_ob = umb_1[umb_1['final_id'].isin(list_id_1_k2)]


# In[871]:


ob_2nd_set = pd.concat([side0_ob,side1_ob], axis = 0)
ob_2nd_set = ob_2nd_set.reset_index()
ob_2nd_set = ob_2nd_set.drop('index', axis = 1)


# In[872]:


ob_2nd_set.shape


# In[873]:


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
            


# In[874]:


ob_for_comment.to_csv('Ob for comment daily lombard 249.csv')


# In[876]:


umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]


# In[877]:


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


# In[878]:


list_id_0_k1 = list(set(k1[k1['umb_counter']==1]['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['umb_counter']==1]['SideA.final_id']))
if ((len(list_id_0_k1)>0) & (len(list_id_1_k2)>0)):
    umb_oto = umbpred[(umbpred['SideB.final_id'].isin(list_id_0_k1)) & (umbpred['SideA.final_id'].isin(list_id_1_k2))]
    umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
    umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]


# In[882]:


# Now We write the hierarchy for many to one
k3 = umbpred.groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
k4 = umbpred.groupby('SideA.final_id')['SideB.final_id'].apply(list).reset_index()


# In[883]:


k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
k4['id_len'] = k4['SideB.final_id'].apply(lambda x : len(x) )


# In[884]:


def intersection_(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return round((len(lst3)/len(lst1)),1)


# In[885]:


def Diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif


# In[886]:


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
            print(1)
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
        
        


# In[887]:


umb_mtm = pd.DataFrame(umb_mtm_list)


# In[888]:


umb_otm = pd.DataFrame(umb_otm_list)


# In[896]:


umb_otm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']
umb_mtm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']


# In[ ]:


umb_otm['predicted status'] = 'UMB'
umb_otm['predicted action'] = 'UMB one to many'
umb_otm['predicted category'] = 'UMB'
umb_otm['predicted comment'] = 'Difference in amount'


# In[898]:


umb_mtm['predicted status'] = 'UMB'
umb_mtm['predicted action'] = 'UMB one to many'
umb_mtm['predicted category'] = 'UMB'
umb_mtm['predicted comment'] = 'Difference in amount'


# In[902]:


umb_oto1 = umb_oto[['SideB.final_id','SideA.final_id']]


# In[904]:


umb_oto1 = umb_oto1.rename(columns = {'SideB.final_id':'ViewData.Side0_UniqueIds',
                                     'SideA.final_id':'ViewData.Side1_UniqueIds'})


# In[905]:


umb_final = pd.concat([umb_otm,umb_mtm,umb_oto1], axis = 0)


# In[906]:


umb_final = umb_final.reset_index()
umb_final = umb_final.drop('index', axis = 1)


# In[907]:


umb_final.to_csv('umb lombard 249 v1.csv')


# In[891]:


ob_stage_df.shape


# In[893]:


k = ob_stage_df.drop_duplicates()


# In[913]:


k .shape


# In[909]:


ob_for_comment = pd.read_csv('Ob for comment daily lombard 249.csv')


# In[911]:


ob_for_comment = ob_for_comment.drop('Unnamed: 0', axis = 1)


# In[914]:


ob_for_comment_model = pd.concat([ob_for_comment,k], axis = 0)


# In[ ]:


ob_for_comment_model = ob_for_comment_model.reset_index()
ob_for_comment_model = ob_for_comment_model.drop('index', axis = 1)


# In[915]:


ob_for_comment_model.to_csv('Ob for comment daily lombard 249 v1 final.csv')


# In[ ]:


# Now we take all OBs to single side model


# In[3]:


import pandas as pd
import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[ ]:


df3 = ob_for_comment_model.copy()


# In[4]:


df3 = pd.read_csv('Ob for comment daily lombard 249 v1 final.csv')


# In[5]:


df3.columns


# In[6]:


df3 = df3.drop('Unnamed: 0', axis = 1)


# In[7]:





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


# In[33]:


df4.count()


# In[34]:


cb_predictions = clf.predict(df4)


# In[35]:


demo = []
for item in cb_predictions:
    demo.append(item[0])
df3['predicted category'] = pd.Series(demo)


# In[51]:


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

