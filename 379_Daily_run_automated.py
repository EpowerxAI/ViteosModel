# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 17:20:35 2020

@author: consultant138
"""

#!/usr/bin/env python
# coding: utf-8

# In[152]:


#!/usr/bin/env python
# coding: utf-8

# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 20:36:16 2020

@author: consultant138
"""
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

from fuzzywuzzy import fuzz
import random
import decimal

pd.set_option('mode.chained_assignment', None)

cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
'Task ID', 'Source Combination Code',
'Activity Code','Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount','Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
'Derived Source','Description','Department','ExpiryDate','ExternalComment1','ExternalComment2',
'ExternalComment3','Fund','FX Rate','Interest Amount','InternalComment1','InternalComment2',
'InternalComment3','Investment Type','Is Combined Data','ISIN','Keys',
'Mapped Custodian Account','Net Amount Difference','Net Amount Difference Absolute','Non Trade Description',
'OTE Custodian Account',
#'Predicted Action','Predicted Status','Prediction Details',
'Price','Prime Broker',
'Quantity','SEDOL','Settle Date','SPM ID','Status','Strike Price',
'System Comments','Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']





new_cols = ['ViewData.' + x for x in cols] + add

common_cols = ['ViewData.Accounting Net Amount', 'ViewData.Age',
'ViewData.Age WK', 'ViewData.Asset Type Category',
'ViewData.B-P Net Amount', 'ViewData.Base Net Amount','ViewData.CUSIP', 
 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag',
#'ViewData.Commission',
        'ViewData.Currency', 'ViewData.Custodian',
       'ViewData.Custodian Account',
       'ViewData.Description','ViewData.Department', 'ViewData.ExpiryDate', 'ViewData.Fund',
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

model_cols = [
            'SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
              #'SideA.new_desc_cat',
             # 'SideA.ViewData.Description',
             # 'SideA.ViewData.Department',
   
    
              
             # 'SideA.ViewData.Price',
             # 'SideA.ViewData.Quantity',
             #'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
             # 'SideB.ViewData.Description',
              # 'SideB.ViewData.Department',
              
             # 'SideB.ViewData.Price',
             # 'SideB.ViewData.Quantity',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
                'Amount_diff_2', 
              'Trade_date_diff', 'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             # 'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Desc','Combined_TType',
             # 'SideA.TType', 'SideB.TType', 
              'abs_amount_flag',
    'tt_map_flag', 
              'All_key_nan','new_key_match', 'new_pb1',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
            'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side'] 
              #'label']

model_cols_2 =[#'SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
              #'SideA.new_desc_cat',
             # 'SideA.ViewData.Description',
             # 'SideA.ViewData.Department',
   
    
              
             # 'SideA.ViewData.Price',
             # 'SideA.ViewData.Quantity',
             #'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              #'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
             # 'SideB.ViewData.Description',
              # 'SideB.ViewData.Department',
              
             # 'SideB.ViewData.Price',
             # 'SideB.ViewData.Quantity',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
              #  'Amount_diff_2', 
              'Trade_date_diff', 'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             # 'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Desc','Combined_TType',
             # 'SideA.TType', 'SideB.TType', 
              'abs_amount_flag',
    'tt_map_flag', 
              'All_key_nan','new_key_match', 'new_pb1',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
            'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side'] 
              #'label']

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

def fill_trade_date_equals_0_with_settle_date_apply_row_379(param_row,param_column_to_fill_for, param_column_to_fill_with):	
	column_to_fill_with_row_val = param_row[param_column_to_fill_with]
	column_to_fill_for_row_val = param_row[param_column_to_fill_for]
	if(column_to_fill_for_row_val == 0):
		if(column_to_fill_with_row_val != 0):
			return_val = column_to_fill_with_row_val
		else:
			return_val = param_row[column_to_fill_for_row_val + '_mode']
	else:
		return_val = column_to_fill_for_row_val
	return(return_val)

#### Closed break functions - Begin #### 

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def dictionary_exclude_keys(fun_dict, fun_keys_to_exclude):
    return {x: fun_dict[x] for x in fun_dict if x not in fun_keys_to_exclude}

def write_dict_at_top(fun_filename, fun_dict_to_add):
    with open(fun_filename, 'r+') as f:
        fun_existing_content = f.read()
        f.seek(0, 0)
        f.write(json.dumps(fun_dict_to_add, indent = 4))
        f.write('\n')
        f.write(fun_existing_content)

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



# M X M and N X N architecture for closed break prediction
def closed_cols():
    cols_for_closed_list = ['Status','Source Combination','Mapped Custodian Account',
                   'Accounting Currency','B-P Currency', 
                   'Transaction ID','Transaction Type','Description','Investment ID',
                   'Accounting Net Amount','B-P Net Amount', 
                   'InternalComment2','Custodian','Fund']
    cols_for_closed_list = ['ViewData.' + x for x in cols_for_closed_list]
    cols_for_closed_x_list = [x + '_x' for x in cols_for_closed_list] + ['ViewData.Side0_UniqueIds_x','ViewData.Side1_UniqueIds_x']
    cols_for_closed_y_list = [x + '_y' for x in cols_for_closed_list] + ['ViewData.Side0_UniqueIds_y','ViewData.Side1_UniqueIds_y']
    cols_for_closed_x_y_list = cols_for_closed_x_list + cols_for_closed_y_list
    return({
            'cols_for_closed' : cols_for_closed_list,
            'cols_for_closed_x' : cols_for_closed_x_list,
            'cols_for_closed_y' : cols_for_closed_y_list,
            'cols_for_closed_x_y' : cols_for_closed_x_y_list
            })

#Change added on New closed code on 10-01-2021
mapping_dict_trans_type = {
                        'STIF Interest_non_interacting' : ['STIF Interest','STIF Interest'],
                        'Same' : ['everthing_else','everthing_else']
                        }

def assign_Transaction_Type_for_closing_apply_row_379(fun_row, fun_transaction_type_col_name = 'ViewData.Transaction Type'):
    if(fun_row[fun_transaction_type_col_name] in ['STIF Interest']):
        Transaction_Type_for_closing = 'STIF Interest_non_interacting'
    else:
         Transaction_Type_for_closing = 'Same'
    return(Transaction_Type_for_closing)

def assign_PB_Acct_side_row_apply(fun_row):
    if((fun_row['flag_side1'] >= 1) & (fun_row['flag_side0'] == 0)):
        PB_or_Acct_Side_Value = 'PB_Side'
    elif((fun_row['flag_side1'] == 0) & (fun_row['flag_side0'] >= 1)):
        PB_or_Acct_Side_Value = 'Acct_Side'
    else:
        PB_or_Acct_Side_Value = 'Non OB'

    return(PB_or_Acct_Side_Value)

def cleaned_meo_379(#fun_filepath_meo, 
                fun_meo_df):

    meo = fun_meo_df
    meo = normalize_bp_acct_col_names(fun_df = meo)
    
#    Commened out below line on 26-11-2020 to exclude SPM from closed coverage, and added the line below the commened line
#    meo = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
    meo = meo[~meo['ViewData.Status'].isin(['SPM','SMT','HST', 'OC', 'CT', 'Archive','SMR','UMB','SMB'])] 
    meo = meo[~meo['ViewData.Status'].isnull()]\
                                     .reset_index()\
                                     .drop('index',1)
    
    meo['Date'] = pd.to_datetime(meo['ViewData.Task Business Date'])
    meo = meo[~meo['Date'].isnull()]\
                          .reset_index()\
                          .drop('index',1)
    
    meo['Date'] = pd.to_datetime(meo['Date']).dt.date
    meo['Date'] = meo['Date'].astype(str)

    meo['ViewData.Side0_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str)
    meo['ViewData.Side1_UniqueIds'] = meo['ViewData.Side1_UniqueIds'].astype(str)

    meo['flag_side0'] = meo.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
    meo['flag_side1'] = meo.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


    meo.loc[meo['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
    meo.loc[meo['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

    meo.loc[meo['ViewData.Side0_UniqueIds']=='None','flag_side0'] = 0
    meo.loc[meo['ViewData.Side1_UniqueIds']=='None','flag_side1'] = 0

    meo.loc[meo['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
    meo.loc[meo['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0

    meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(int)
    meo = meo[meo['ViewData.BreakID']!=-1] \
          .reset_index() \
          .drop('index',1)
          
    meo['Side_0_1_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str) + \
                                meo['ViewData.Side1_UniqueIds'].astype(str)
    meo['PB_or_Acct_Side'] = meo.apply(lambda row : assign_PB_Acct_side_row_apply(fun_row = row), axis = 1, result_type="expand")
    meo['ViewData.Transaction Type'] = meo['ViewData.Transaction Type'].astype(str)
    meo['Transaction_Type_for_closing'] = meo.apply(lambda row : assign_Transaction_Type_for_closing_apply_row_379(fun_row = row, fun_transaction_type_col_name = 'ViewData.Transaction Type'), axis = 1, result_type="expand")
    meo['abs_net_amount_difference'] = meo['ViewData.Net Amount Difference'].apply(lambda x : abs(x))
    meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
    return(meo)  

def interacting_closing_379(fun_df): #The name of the function contains 125 but it is also being used in 379
    fun_df['ViewData.Mapped Custodian Account'] = fun_df['ViewData.Mapped Custodian Account'].astype(str)
    fun_df['ViewData.Currency'] = fun_df['ViewData.Currency'].astype(str)    
    fun_df['ViewData.Source Combination Code'] = fun_df['ViewData.Source Combination Code'].astype(str)
    fun_df['abs_net_amount_difference'] = fun_df['abs_net_amount_difference'].astype(str)
    fun_df['filter'] = fun_df['ViewData.Source Combination Code'] + fun_df['ViewData.Mapped Custodian Account'] + fun_df['ViewData.Currency'] + fun_df['abs_net_amount_difference']
    grouped_by_filter_df = fun_df.groupby('filter').size().reset_index(name='counts_for_filter')
    merged_df_with_filter_counts = pd.merge(fun_df, grouped_by_filter_df, on = 'filter', how = 'left')
    merged_df_with_filter_counts_ge_1 = merged_df_with_filter_counts[merged_df_with_filter_counts['counts_for_filter'] > 1] 
    return(merged_df_with_filter_counts_ge_1)

def All_combination_file_379(fun_df):
    fun_df['filter_key'] = fun_df['ViewData.Source Combination'].astype(str) + \
                                       fun_df['ViewData.Mapped Custodian Account'].astype(str) + \
                                       fun_df['ViewData.Currency'].astype(str)                             

    all_training_df_for_transaction_type =[]
    for key in (list(np.unique(np.array(list(fun_df['filter_key'].values))))):
        all_training_df_for_transaction_type_filter_slice = fun_df[fun_df['filter_key']==key]
        if all_training_df_for_transaction_type_filter_slice.empty == False:

            all_training_df_for_transaction_type_filter_slice = all_training_df_for_transaction_type_filter_slice.reset_index()
            all_training_df_for_transaction_type_filter_slice = all_training_df_for_transaction_type_filter_slice.drop('index', 1)

            all_training_df_for_transaction_type_filter_joined = pd.merge(all_training_df_for_transaction_type_filter_slice, all_training_df_for_transaction_type_filter_slice, on='filter_key')
            all_training_df_for_transaction_type.append(all_training_df_for_transaction_type_filter_joined)
    if(len(all_training_df_for_transaction_type) == 0):
        return(pd.DataFrame())
    else:
        return(pd.concat(all_training_df_for_transaction_type))

def identifying_closed_breaks_379(fun_all_meo_combination_df, fun_setup_code_crucial, fun_Transaction_Type_for_closing):
    
    if(fun_setup_code_crucial == '379'):

        if(fun_Transaction_Type_for_closing == 'STIF Interest_non_interacting'):
            Matching_closed_break_df_1 = \
                fun_all_meo_combination_df[ \
                                            (fun_all_meo_combination_df['Transaction_Type_for_closing_x'].astype(str).isin([fun_Transaction_Type_for_closing])) & \
                                            (fun_all_meo_combination_df['Transaction_Type_for_closing_y'].astype(str).isin([fun_Transaction_Type_for_closing])) & \
    #                                         (fun_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
                                            (abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
                                            (fun_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != fun_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) & \
                                            (fun_all_meo_combination_df['PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str))]
        else:
            Matching_closed_break_df_1 = pd.DataFrame()
        if(fun_Transaction_Type_for_closing == 'Same'):
            Matching_closed_break_df_2 = \
                fun_all_meo_combination_df[ \
                                            (fun_all_meo_combination_df['Transaction_Type_for_closing_x'].astype(str).isin([fun_Transaction_Type_for_closing])) & \
                                            (fun_all_meo_combination_df['Transaction_Type_for_closing_y'].astype(str).isin([fun_Transaction_Type_for_closing])) & \
    #                                         (fun_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
                                            (abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
                                            (fun_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != fun_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) & \
                                            (fun_all_meo_combination_df['PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str))]
        else:
            Matching_closed_break_df_2 = pd.DataFrame()
        
        closed_df_list = [ \
                          Matching_closed_break_df_1 \
                          , \
                          Matching_closed_break_df_2
                            ]
        
        
        Transaction_type_closed_break_df = pd.concat(closed_df_list)
    if(Transaction_type_closed_break_df.shape[0] != 0):
        return(Transaction_type_closed_break_df)
    else:
        return(pd.DataFrame())
#     return(set(
#                 Transaction_type_closed_break_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
#                 Transaction_type_closed_break_df['ViewData.Side1_UniqueIds_x'].astype(str)
#                ))
    


#### Closed break functions - End #### 

#### Break Prediction functions - Begin #### 

def equals_fun(a,b):
    if a == b:
        return 1
    else:
        return 0

vec_equals_fun = np.vectorize(equals_fun)


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

def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x

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

def nan_fun(x):
    if x=='nan':
        return 1
    else:
        return 0

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


def nan_equals_fun(a,b):
    if a==1 and b==1:
        return 1
    else:
        return 0


def new_key_match_fun(a,b,c):
    if a==b and c==0:
        return 1
    else:
        return 0

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

def umr_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
#    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']==1) & (b_count2['len']<=2)]
#Change made on 12-01-2021 as per Pratik. This change will capture one to one umr which were otherwise going into updown obs
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']<=3) & (b_count2['len']<=3)]
    return umr_table['SideB.ViewData.Side0_UniqueIds'].values

def normalize_final_no_pair_table_col_names(fun_final_no_pair_table):
    final_no_pair_table_col_names_mapping_dict = {
                                      'SideA.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',
                                      'SideB.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
                                      'SideA.ViewData.BreakID_A_side' : 'ViewData.BreakID_Side1', 
                                      'SideB.ViewData.BreakID_B_side' : 'ViewData.BreakID_Side0'
                                      }
    fun_final_no_pair_table.rename(columns = final_no_pair_table_col_names_mapping_dict, inplace = True)
    return(fun_final_no_pair_table)

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

def one_to_one_umb(data):
    
    count = data['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count0')
    id0s = count[count['count0']==1]['index'].unique()
    id1s = data[data['SideB.ViewData.Side0_UniqueIds'].isin(id0s)]['SideA.ViewData.Side1_UniqueIds']
    
    count1 = data['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count1')
    final_ids = count1[(count1['count1']==1) & (count1['index'].isin(id1s))]['index'].unique()
    return final_ids

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

def return_int_list(list_x):
    return [int(i) for i in list_x]
    
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

def find_BreakID_and_other_cols_in_meo_for_Side_0_1_UniqueIds_value(fun_string_value_of_Side_0_1_UniqueIds, fun_meo_df, fun_side, fun_other_cols_list = None):
    if fun_other_cols_list is None:
        all_cols_to_find = ['ViewData.BreakID']
    else:
        all_cols_to_find = fun_other_cols_list + ['ViewData.BreakID']
    if(fun_side == 0):
        return(fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'] == fun_string_value_of_Side_0_1_UniqueIds][all_cols_to_find])
    elif(fun_side == 1):
        return(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'] == fun_string_value_of_Side_0_1_UniqueIds][all_cols_to_find])
    else:
        return 0

def contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row):
    
    if(',' in str(fun_row['ViewData.Side0_UniqueIds'])):
        Side_0_contains_comma = 1
    else:
        Side_0_contains_comma = 0

    if(',' in str(fun_row['ViewData.Side1_UniqueIds'])):
        Side_1_contains_comma = 1
    else:
        Side_1_contains_comma = 0
    
    if((str(fun_row['ViewData.Status']) in ['OB','SDB','UOB','CNF','CMF']) and ((Side_0_contains_comma == 1) or (Side_1_contains_comma == 1))):
        return('remove')
    else:
        return('keep')

def find_Side_0_1_UniqueIds_and_other_cols_in_meo_for_BreakID_value(fun_string_value_of_BreakID,fun_meo_df,fun_other_cols_list = None):
    if fun_other_cols_list is None:
        all_cols_to_find = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Status']
    else:
        all_cols_to_find = fun_other_cols_list + ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Status']
    fun_meo_df['ViewData.BreakID'] = fun_meo_df['ViewData.BreakID'].astype(str)
    return(fun_meo_df[fun_meo_df['ViewData.BreakID'] == fun_string_value_of_BreakID][all_cols_to_find])

def make_Side0_Side1_columns_for_final_smb_ob_table_row_apply(row, fun_side):
#    print(row)

    if(fun_side == 0):
        if(row['Side0_UniqueIds_OB'] == ''):
            return(row['Side0_UniqueIds_SMB'])
        else:
            return(row['Side0_UniqueIds_OB'] + ',' + row['Side0_UniqueIds_SMB'])
    elif(fun_side == 1):
        if(row['Side1_UniqueIds_OB'] == ''):
            return(row['Side1_UniqueIds_SMB'])
        else:
            return(row['Side1_UniqueIds_OB'] + ',' + row['Side1_UniqueIds_SMB'])
    
def make_Side0_Side1_columns_for_final_smb_ob_table(fun_final_smb_ob_table, fun_meo_df):
    fun_final_smb_ob_table = pd.merge(fun_final_smb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side0_UniqueIds']], left_on = 'BreakID_OB', right_on = 'ViewData.BreakID')
    fun_final_smb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_ob_table.rename(columns = {'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds_OB'}, inplace = True) 

    fun_final_smb_ob_table = pd.merge(fun_final_smb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side1_UniqueIds']], left_on = 'BreakID_OB', right_on = 'ViewData.BreakID')
    fun_final_smb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_ob_table.rename(columns = {'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds_OB'}, inplace = True) 

    fun_final_smb_ob_table = pd.merge(fun_final_smb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side0_UniqueIds']], left_on = 'BreakID_SMB', right_on = 'ViewData.BreakID')
    fun_final_smb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_ob_table.rename(columns = {'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds_SMB'}, inplace = True) 

    fun_final_smb_ob_table = pd.merge(fun_final_smb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side1_UniqueIds']], left_on = 'BreakID_SMB', right_on = 'ViewData.BreakID')
    fun_final_smb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_ob_table.rename(columns = {'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds_SMB'}, inplace = True) 

    fun_final_smb_ob_table['Side0_UniqueIds_OB'] = fun_final_smb_ob_table['Side0_UniqueIds_OB'].astype(str)            
    fun_final_smb_ob_table['Side1_UniqueIds_OB'] = fun_final_smb_ob_table['Side1_UniqueIds_OB'].astype(str)            
    fun_final_smb_ob_table['Side0_UniqueIds_SMB'] = fun_final_smb_ob_table['Side0_UniqueIds_SMB'].astype(str)            
    fun_final_smb_ob_table['Side1_UniqueIds_SMB'] = fun_final_smb_ob_table['Side1_UniqueIds_SMB'].astype(str)            

    fun_final_smb_ob_table['Side0_UniqueIds_OB'] = fun_final_smb_ob_table['Side0_UniqueIds_OB'].replace('None','')            
    fun_final_smb_ob_table['Side1_UniqueIds_OB'] = fun_final_smb_ob_table['Side1_UniqueIds_OB'].replace('None','')            
    fun_final_smb_ob_table['Side0_UniqueIds_SMB'] = fun_final_smb_ob_table['Side0_UniqueIds_SMB'].replace('None','')            
    fun_final_smb_ob_table['Side1_UniqueIds_SMB'] = fun_final_smb_ob_table['Side1_UniqueIds_SMB'].replace('None','')            

    fun_final_smb_ob_table['Side0_UniqueIds_OB'] = fun_final_smb_ob_table['Side0_UniqueIds_OB'].replace('nan','')            
    fun_final_smb_ob_table['Side1_UniqueIds_OB'] = fun_final_smb_ob_table['Side1_UniqueIds_OB'].replace('nan','')
    fun_final_smb_ob_table['Side0_UniqueIds_SMB'] = fun_final_smb_ob_table['Side0_UniqueIds_SMB'].replace('nan','') 
    fun_final_smb_ob_table['Side1_UniqueIds_SMB'] = fun_final_smb_ob_table['Side1_UniqueIds_SMB'].replace('nan','')

    fun_final_smb_ob_table['Side0_UniqueIds'] = fun_final_smb_ob_table.apply(lambda row : make_Side0_Side1_columns_for_final_smb_ob_table_row_apply(row, fun_side = 0),axis = 1,result_type="expand")
    fun_final_smb_ob_table['Side1_UniqueIds'] = fun_final_smb_ob_table.apply(lambda row : make_Side0_Side1_columns_for_final_smb_ob_table_row_apply(row, fun_side = 1),axis = 1,result_type="expand")
#    fun_final_smb_ob_table.iloc[fun_final_smb_ob_table['Side0_UniqueIds_OB'] == '', 'Side0_UniqueIds'] = fun_final_smb_ob_table['Side0_UniqueIds_SMB']
#    fun_final_smb_ob_table.iloc[fun_final_smb_ob_table['Side0_UniqueIds_OB'] != '', 'Side0_UniqueIds'] = fun_final_smb_ob_table['Side0_UniqueIds_OB'] + fun_final_smb_ob_table['Side0_UniqueIds_SMB']
#    fun_final_smb_ob_table.iloc[fun_final_smb_ob_table['Side1_UniqueIds_OB'] == '', 'Side1_UniqueIds'] = fun_final_smb_ob_table['Side1_UniqueIds_SMB']
#    fun_final_smb_ob_table.iloc[fun_final_smb_ob_table['Side1_UniqueIds_OB'] != '', 'Side1_UniqueIds'] = fun_final_smb_ob_table['Side1_UniqueIds_OB'] + fun_final_smb_ob_table['Side1_UniqueIds_SMB']

    fun_final_smb_ob_table.drop(['Side0_UniqueIds_OB','Side1_UniqueIds_OB','Side0_UniqueIds_SMB','Side1_UniqueIds_SMB'], axis = 1, inplace = True)

    return(fun_final_smb_ob_table)


def make_Side0_Side1_columns_for_final_smb_ob_or_umb_ob_table_row_apply_379(row, fun_side, fun_umb_or_smb_flag):
#    print(row)
    if(fun_umb_or_smb_flag == 'SMB'):
        Side0_UniqueIds_col_name = 'Side0_UniqueIds_SMB'
        Side1_UniqueIds_col_name = 'Side1_UniqueIds_SMB'
    elif(fun_umb_or_smb_flag == 'UMB'):
        Side0_UniqueIds_col_name = 'Side0_UniqueIds_UMB'
        Side1_UniqueIds_col_name = 'Side1_UniqueIds_UMB'
        
    if(fun_side == 0):
        if(row['Side0_UniqueIds_OB'] == ''):
            return(row[Side0_UniqueIds_col_name])
        else:
            return(row['Side0_UniqueIds_OB'] + ',' + row[Side0_UniqueIds_col_name])
    elif(fun_side == 1):
        if(row['Side1_UniqueIds_OB'] == ''):
            return(row[Side1_UniqueIds_col_name])
        else:
            return(row['Side1_UniqueIds_OB'] + ',' + row[Side1_UniqueIds_col_name])
    
def make_Side0_Side1_columns_for_final_smb_or_umb_ob_table_379(fun_final_smb_or_umb_ob_table, fun_meo_df, fun_umb_or_smb_flag):
    flag_value = fun_umb_or_smb_flag
    if(fun_umb_or_smb_flag == 'SMB'):
        Side0_UniqueIds_col_name = 'Side0_UniqueIds_SMB'
        Side1_UniqueIds_col_name = 'Side1_UniqueIds_SMB'
        BreakID_smb_umb_col_name = 'BreakID_SMB'
    elif(fun_umb_or_smb_flag == 'UMB'):
        Side0_UniqueIds_col_name = 'Side0_UniqueIds_UMB'
        Side1_UniqueIds_col_name = 'Side1_UniqueIds_UMB'
        BreakID_smb_umb_col_name = 'BreakID_UMB'

    fun_final_smb_or_umb_ob_table = pd.merge(fun_final_smb_or_umb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side0_UniqueIds']], left_on = 'BreakID_OB', right_on = 'ViewData.BreakID')
    fun_final_smb_or_umb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_or_umb_ob_table.rename(columns = {'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds_OB'}, inplace = True) 

    fun_final_smb_or_umb_ob_table = pd.merge(fun_final_smb_or_umb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side1_UniqueIds']], left_on = 'BreakID_OB', right_on = 'ViewData.BreakID')
    fun_final_smb_or_umb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_or_umb_ob_table.rename(columns = {'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds_OB'}, inplace = True) 

    fun_final_smb_or_umb_ob_table = pd.merge(fun_final_smb_or_umb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side0_UniqueIds']], left_on = BreakID_smb_umb_col_name, right_on = 'ViewData.BreakID')
    fun_final_smb_or_umb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_or_umb_ob_table.rename(columns = {'ViewData.Side0_UniqueIds' : Side0_UniqueIds_col_name}, inplace = True) 

    fun_final_smb_or_umb_ob_table = pd.merge(fun_final_smb_or_umb_ob_table,fun_meo_df[['ViewData.BreakID','ViewData.Side1_UniqueIds']], left_on = BreakID_smb_umb_col_name, right_on = 'ViewData.BreakID')
    fun_final_smb_or_umb_ob_table.drop('ViewData.BreakID', axis = 1, inplace = True)
    fun_final_smb_or_umb_ob_table.rename(columns = {'ViewData.Side1_UniqueIds' : Side1_UniqueIds_col_name}, inplace = True) 

    fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] = fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'].astype(str)            
    fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] = fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'].astype(str)            
    fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name] = fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name].astype(str)            
    fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name] = fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name].astype(str)            

    fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] = fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'].replace('None','')            
    fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] = fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'].replace('None','')            
    fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name] = fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name].replace('None','')            
    fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name] = fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name].replace('None','')            

    fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] = fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'].replace('nan','')            
    fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] = fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'].replace('nan','')
    fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name] = fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name].replace('nan','') 
    fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name] = fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name].replace('nan','')

    fun_final_smb_or_umb_ob_table['Side0_UniqueIds'] = fun_final_smb_or_umb_ob_table.apply(lambda row : make_Side0_Side1_columns_for_final_smb_ob_or_umb_ob_table_row_apply_379(row, fun_side = 0, fun_umb_or_smb_flag = flag_value),axis = 1,result_type="expand")
    fun_final_smb_or_umb_ob_table['Side1_UniqueIds'] = fun_final_smb_or_umb_ob_table.apply(lambda row : make_Side0_Side1_columns_for_final_smb_ob_or_umb_ob_table_row_apply_379(row, fun_side = 1, fun_umb_or_smb_flag = flag_value),axis = 1,result_type="expand")
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] == '', 'Side0_UniqueIds'] = fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name]
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] != '', 'Side0_UniqueIds'] = fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] + fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name]
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] == '', 'Side1_UniqueIds'] = fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name]
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] != '', 'Side1_UniqueIds'] = fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] + fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name]

    fun_final_smb_or_umb_ob_table.drop(['Side0_UniqueIds_OB','Side1_UniqueIds_OB',Side0_UniqueIds_col_name,Side1_UniqueIds_col_name], axis = 1, inplace = True)

    return(fun_final_smb_or_umb_ob_table)


date_numbers_list = [16]
                     #2,3,4,
                    # 7,8,9,10,11,
                    # 14,15,16,17,18,
                    # 21,22,23,24,25,
                    # 28,29,30]

client = 'OakTree'

setup_code = '379'

today = date.today()
d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
desired_date = d1 - timedelta(days=4)
desired_date_str = desired_date.strftime("%Y-%m-%d")
date_input = desired_date_str

#filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_input) + '.csv'
filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_input) + '.csv'

#Changes asked by Shitanshu for db on 24-03-2021
#Use 137 server
#Reading RecData collections (MEO data) DB name : ReconDB
#Writing ML Data DB name : ReconDB_ML
#Writing ML Data Collection Name : MLPrediction_ReconSetupCode. For eg. MLPrediction_125


mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
#db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
#Changes asked by Shitanshu for db on 24-03-2021
db_for_reading_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB']
db_for_writing_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']

db_2_for_MEO_data_MLReconDB_Testing = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML_Testing']


query_1_for_MEO_data = db_for_reading_MEO_data['RecData_' + setup_code].find({ 
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


#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + str(client) + '/meo_df_setup_' + str(setup_code) +'_date_' + date_to_analyze_ymd_format + '.csv'
##meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/meo_df_setup_379_date_' + date_to_analyze_ymd_format + '.csv'
#meo_df = pd.read_csv(meo_filename)


meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
print(meo_df.shape[0])
meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)

#Change added on 17-12-2020 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed

meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_df.apply(lambda row : contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row = row), axis = 1,result_type="expand")
meo_df = meo_df[~(meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]

meo = meo_df[new_cols]
print('meo size')
print(meo.shape[0])



meo_df['Date'] = pd.to_datetime(meo_df['ViewData.Task Business Date'])
meo_df = meo_df.reset_index()
meo_df = meo_df.drop('index',1)


meo_df['Date'] = pd.to_datetime(meo_df['Date']).dt.date

meo_df['Date'] = meo_df['Date'].astype(str)


date_i = meo_df['Date'].mode()[0]
print(str(date_i))

date_to_analyze_ymd_format = date_i
penultimate_date_to_analyze_ymd_format = str((pd.to_datetime(date_i,format='%Y-%m-%d') - timedelta(1)).strftime('%Y-%m-%d'))
penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'


def get_BreakID_from_list_of_Side_01_UniqueIds(fun_str_list_Side_01_UniqueIds, fun_meo_df, fun_side_0_or_1):
    list_BreakID_corresponding_to_Side_01_UniqueIds = []
    print(fun_str_list_Side_01_UniqueIds)
    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
        if(fun_side_0_or_1 == 0):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
        elif(fun_side_0_or_1 == 1):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    return(list_BreakID_corresponding_to_Side_01_UniqueIds)


# In[154]:


def get_first_non_null_value(string_of_values_separated_by_comma):
    if(string_of_values_separated_by_comma != '' and string_of_values_separated_by_comma != 'nan' and string_of_values_separated_by_comma != 'None' ):
        if(string_of_values_separated_by_comma.partition(',')[0] != '' and string_of_values_separated_by_comma.partition(',')[0] != 'nan' and string_of_values_separated_by_comma.partition(',')[0] != 'None'):
            return(string_of_values_separated_by_comma.partition(',')[0])
        else:
            return(get_first_non_null_value(string_of_values_separated_by_comma.partition(',')[2]))
    else:
        return('Blank value')        


# In[155]:



meo_df_taskids = list(meo_df['ViewData.Task ID'].unique())


# In[156]:


#Change made on 12-12-2020 as per Pratik to catch instances where a single SMB pairs off with a single OB. BreakIDs caught in this code piece will be removed from propogating down further. Also, these BreakIDs will be given the status of UMR with Predicted_action of UMR_One-Many_to_Many-One
#Begin change code made on 12-12-2020
meo2 = meo[meo['ViewData.Status'].isin(['OB','SMB','SPM','UMB'])]
meo2 = meo2.reset_index().drop('index',1)

meo2['ViewData.Net Amount Difference Absolute'] = np.round(meo2['ViewData.Net Amount Difference Absolute'],2)

abs_amount_count = meo2['ViewData.Net Amount Difference Absolute'].value_counts().reset_index()

duplicate_amount = abs_amount_count[abs_amount_count['ViewData.Net Amount Difference Absolute']==2]
duplicate_amount.columns = ['ViewData.Net Amount Difference Absolute','count']
duplicate_amount = duplicate_amount.reset_index().drop('index',1)

if duplicate_amount.shape[0]>0:
    meo3 = meo2[meo2['ViewData.Net Amount Difference Absolute'].isin(duplicate_amount['ViewData.Net Amount Difference Absolute'].unique())]
    meo3 = meo3.reset_index().drop('index',1)
    meo3 = meo3.sort_values(by='ViewData.Net Amount Difference Absolute')
    meo3 = meo3.reset_index().drop('index',1)
    
    smb_amount = meo3[meo3['ViewData.Status'].isin(['SMB'])]['ViewData.Net Amount Difference Absolute'].unique()
    umb_amount = meo3[meo3['ViewData.Status'].isin(['UMB'])]['ViewData.Net Amount Difference Absolute'].unique()
    
    smb_ob_table = meo3[meo3['ViewData.Net Amount Difference Absolute'].isin(smb_amount)]
    umb_ob_table = meo3[meo3['ViewData.Net Amount Difference Absolute'].isin(umb_amount)]
    
    ob_breakid = []
    smb_breakid = []
    if(smb_amount.size != 0):

        for amount in smb_amount:
            ob = smb_ob_table[(smb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (smb_ob_table['ViewData.Status']=='OB')]
            smb = smb_ob_table[(smb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (smb_ob_table['ViewData.Status']=='SMB')]
    #         if((ob.shape[0]==1) and (smb.shape[0]==1) and (ob['ViewData.Mapped Custodian Account'] == smb['ViewData.Mapped Custodian Account']) and (ob['ViewData.Currency'] == smb['ViewData.Currency']) and (ob['ViewData.Source Combination Code'] == smb['ViewData.Source Combination Code'])):
    
            if ob.shape[0]==1 and smb.shape[0]==1 :
    #Change added on 17-12-2020 by Rohit to include filter on ob and smb. Below if statement is commented out and new if statement is included
                if((ob['ViewData.Mapped Custodian Account'].iloc[0] == smb['ViewData.Mapped Custodian Account'].iloc[0]) and (ob['ViewData.Currency'].iloc[0] == smb['ViewData.Currency'].iloc[0]) and (ob['ViewData.Source Combination Code'].iloc[0] == smb['ViewData.Source Combination Code'].iloc[0])):
    
                    ob_breakid.append(ob['ViewData.BreakID'].values)
                    smb_breakid.append(smb['ViewData.BreakID'].values)
    else:
        ob = pd.DataFrame()
        smb = pd.DataFrame()                                
        
    if len(ob_breakid)>0:
        final_smb_ob_table = pd.DataFrame(ob_breakid)
        final_smb_ob_table.columns = ['BreakID_OB']
        final_smb_ob_table['BreakID_SMB'] = smb_breakid
        final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].apply(lambda x: str(x).replace("[",''))
        final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].apply(lambda x: str(x).replace("]",''))
        final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].astype(int)
    else:
        final_smb_ob_table = pd.DataFrame()
else:
    final_smb_ob_table = pd.DataFrame()
    umb_amount = np.array([])
    smb_amount = np.array([])

#Remove BreakIDs caught in final_smb_ob_table if final_smb_ob_table is not null
if(final_smb_ob_table.shape[0] != 0):
    final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].astype(np.int64)
    final_smb_ob_table['BreakID_OB'] = final_smb_ob_table['BreakID_OB'].astype(np.int64)
    
    final_smb_ob_table_BreakID_list =  list(final_smb_ob_table['BreakID_OB']) + list(final_smb_ob_table['BreakID_SMB'])
    meo = meo[~meo['ViewData.BreakID'].isin(final_smb_ob_table_BreakID_list)]
else:
    final_smb_ob_table_BreakID_list = []
#End change code made on 12-12-2020

#Change made on 12-12-2020 to incorporate final_smb_ob_table. The BreakIDs in this table will be given the Predicted_Status of UMR and Predicted_action of UMR_One-Many_to_Many-One
#Begin code change made on 12-12-2020 to incorporate final_smb_ob_table
#final_smb_ob_table
if(final_smb_ob_table.shape[0] != 0):

    final_smb_ob_table_copy = pd.merge(final_smb_ob_table,meo_df[['ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'BreakID_OB',right_on = 'ViewData.BreakID', how='left')
    final_smb_ob_table_copy.drop('ViewData.BreakID', axis = 1, inplace = True)
    
    final_smb_ob_table_copy['Predicted_Status'] = 'UMR'
    final_smb_ob_table_copy['Predicted_action'] = 'UMR_One-Many_to_Many-One'
    final_smb_ob_table_copy['ML_flag'] = 'ML'
    final_smb_ob_table_copy['SetupID'] = setup_code 
    final_smb_ob_table_copy['ViewData.Task Business Date'] = pd.to_datetime(final_smb_ob_table_copy['ViewData.Task Business Date'])
    final_smb_ob_table_copy['ViewData.Task Business Date'] = final_smb_ob_table_copy['ViewData.Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_smb_ob_table_copy['ViewData.Task Business Date'] = pd.to_datetime(final_smb_ob_table_copy['ViewData.Task Business Date'])
    final_smb_ob_table_copy = make_Side0_Side1_columns_for_final_smb_or_umb_ob_table_379(final_smb_ob_table_copy,meo_df, 'SMB')
    final_smb_ob_table_copy['probability_No_pair'] = ''
    final_smb_ob_table_copy['probability_UMB'] = ''
    final_smb_ob_table_copy['probability_UMR'] = ''
    final_smb_ob_table_copy['probability_UMT'] = ''
    final_smb_ob_table_copy['PredictedComment'] = ''
    final_smb_ob_table_copy['PredictedCategory'] = ''
    columns_rename_for_smb_ob_table_dict = {'BreakID_OB' : 'BreakID',
                                       'BreakID_SMB' : 'Final_predicted_break',
                                       'ViewData.Task ID' : 'TaskID',
                                       'ViewData.Task Business Date' : 'BusinessDate',
                                       'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                       }
    final_smb_ob_table_copy.rename(columns = columns_rename_for_smb_ob_table_dict, inplace = True)
    filepaths_final_smb_ob_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_smb_ob_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'

    final_smb_ob_table_copy.to_csv(filepaths_final_smb_ob_table_copy)


else:
    final_smb_ob_table_copy = pd.DataFrame()
#End code change made on 12-12-2020 to incorporate final_smb_ob_table

ob_breakid = []
umb_breakid = []

if(umb_amount.size != 0):
    for amount in umb_amount:
        ob = umb_ob_table[(umb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (umb_ob_table['ViewData.Status']=='OB')]
        umb = umb_ob_table[(umb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (umb_ob_table['ViewData.Status']=='UMB')]
    #         if((ob.shape[0]==1) and (smb.shape[0]==1) and (ob['ViewData.Mapped Custodian Account'] == smb['ViewData.Mapped Custodian Account']) and (ob['ViewData.Currency'] == smb['ViewData.Currency']) and (ob['ViewData.Source Combination Code'] == smb['ViewData.Source Combination Code'])):
    
        if ob.shape[0]==1 and umb.shape[0]==1 :
    #Change added on 17-12-2020 by Rohit to include filter on ob and smb. Below if statement is commented out and new if statement is included
            if((ob['ViewData.Mapped Custodian Account'].iloc[0] == umb['ViewData.Mapped Custodian Account'].iloc[0]) and (ob['ViewData.Currency'].iloc[0] == umb['ViewData.Currency'].iloc[0]) and (ob['ViewData.Source Combination Code'].iloc[0] == umb['ViewData.Source Combination Code'].iloc[0])):
    
                ob_breakid.append(ob['ViewData.BreakID'].values)
                umb_breakid.append(umb['ViewData.BreakID'].values)
else:
    ob = pd.DataFrame()
    umb = pd.DataFrame()
            
if len(ob_breakid)>0:
    final_umb_ob_table = pd.DataFrame(ob_breakid)
    final_umb_ob_table.columns = ['BreakID_OB']
    final_umb_ob_table['BreakID_UMB'] = umb_breakid
    final_umb_ob_table['BreakID_UMB'] = final_umb_ob_table['BreakID_UMB'].apply(lambda x: str(x).replace("[",''))
    final_umb_ob_table['BreakID_UMB'] = final_umb_ob_table['BreakID_UMB'].apply(lambda x: str(x).replace("]",''))
    final_umb_ob_table['BreakID_UMB'] = final_umb_ob_table['BreakID_UMB'].astype(int)
else:
    final_umb_ob_table = pd.DataFrame()

#Remove BreakIDs caught in final_umb_ob_table if final_umb_ob_table is not null
if(final_umb_ob_table.shape[0] != 0):
    final_umb_ob_table['BreakID_UMB'] = final_umb_ob_table['BreakID_UMB'].astype(np.int64)
    final_umb_ob_table['BreakID_OB'] = final_umb_ob_table['BreakID_OB'].astype(np.int64)
    
    final_umb_ob_table_BreakID_list =  list(final_umb_ob_table['BreakID_OB']) + list(final_umb_ob_table['BreakID_UMB'])
    meo = meo[~meo['ViewData.BreakID'].isin(final_umb_ob_table_BreakID_list)]
else:
    final_umb_ob_table_BreakID_list = []
#End change code made on 12-12-2020


# In[163]:


#Change made on 20-12-2020 to incorporate final_umb_ob_table. The BreakIDs in this table will be given the Predicted_Status of UMR and Predicted_action of UMR_One-Many_to_Many-One
#Begin code change made on 20-12-2020 to incorporate final_umb_ob_table
#final_umb_ob_table
if(final_umb_ob_table.shape[0] != 0):

    final_umb_ob_table_copy = pd.merge(final_umb_ob_table,meo_df[['ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'BreakID_OB',right_on = 'ViewData.BreakID', how='left')
    final_umb_ob_table_copy.drop('ViewData.BreakID', axis = 1, inplace = True)
    
    final_umb_ob_table_copy['Predicted_Status'] = 'UMR'
    final_umb_ob_table_copy['Predicted_action'] = 'UMR_One-Many_to_Many-One'
    final_umb_ob_table_copy['ML_flag'] = 'ML'
    final_umb_ob_table_copy['SetupID'] = setup_code 
    final_umb_ob_table_copy['ViewData.Task Business Date'] = pd.to_datetime(final_umb_ob_table_copy['ViewData.Task Business Date'])
    final_umb_ob_table_copy['ViewData.Task Business Date'] = final_umb_ob_table_copy['ViewData.Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_umb_ob_table_copy['ViewData.Task Business Date'] = pd.to_datetime(final_umb_ob_table_copy['ViewData.Task Business Date'])
    final_umb_ob_table_copy = make_Side0_Side1_columns_for_final_smb_or_umb_ob_table_379(final_umb_ob_table_copy,meo_df,'UMB')
    final_umb_ob_table_copy['probability_No_pair'] = ''
    final_umb_ob_table_copy['probability_UMB'] = ''
    final_umb_ob_table_copy['probability_UMR'] = ''
    final_umb_ob_table_copy['probability_UMT'] = ''
    final_umb_ob_table_copy['PredictedComment'] = ''
    final_umb_ob_table_copy['PredictedCategory'] = ''
    columns_rename_for_umb_ob_table_dict = {'BreakID_OB' : 'BreakID',
                                       'BreakID_UMB' : 'Final_predicted_break',
                                       'ViewData.Task ID' : 'TaskID',
                                       'ViewData.Task Business Date' : 'BusinessDate',
                                       'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                       }
    final_umb_ob_table_copy.rename(columns = columns_rename_for_umb_ob_table_dict, inplace = True)
    filepaths_final_umb_ob_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umb_ob_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'

    final_umb_ob_table_copy.to_csv(filepaths_final_umb_ob_table_copy)


else:
    final_umb_ob_table_copy = pd.DataFrame()
#End code change made on 12-12-2020 to incorporate final_smb_ob_table

# Change made on 20-12-2020. UMB_Carry_forward df used to be taken out from meo_df, but as per Pratik, it will now be taken out from meo where smb-ob and umb-ob break ids have already been taken out from meo
#umb_carry_forward_df = meo[meo['ViewData.Status'] == 'UMB']

#Change made on 10-01-2021 as per Rohit to add new closed code
#Begin new closed code 
normalized_meo_df = normalize_bp_acct_col_names(meo_df)
meo_for_closed = cleaned_meo_379(normalized_meo_df)


closed_df_list = []

for transaction_type_for_closing_value in mapping_dict_trans_type:
    meo_for_transaction_type_for_closing_value_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing'] == transaction_type_for_closing_value]
    meo_for_transaction_type_for_closing_value = interacting_closing_379(meo_for_transaction_type_for_closing_value_input)
    All_combination_df = All_combination_file_379(fun_df = meo_for_transaction_type_for_closing_value)
    if(All_combination_df.shape[0] != 0):
        closed_df_for_transaction_type_for_closing_value = identifying_closed_breaks_379(fun_all_meo_combination_df = All_combination_df, \
                                                                 fun_setup_code_crucial = setup_code, \
                                                                 fun_Transaction_Type_for_closing = transaction_type_for_closing_value)
        closed_df_list.append(closed_df_for_transaction_type_for_closing_value)
    else:
        closed_df_list.append(pd.DataFrame())
    del(meo_for_transaction_type_for_closing_value_input)
    del(meo_for_transaction_type_for_closing_value)
    del(All_combination_df)


closed_df_interacting = pd.concat(closed_df_list)

breakId_x = set(list(closed_df_interacting['ViewData.BreakID_x']))
breakId_y = set(list(closed_df_interacting['ViewData.BreakID_y']))

all_interacting_closed_breakIds = list(breakId_x.union(breakId_y))

all_predicted_close_breakids = all_interacting_closed_breakIds

int_all_predicted_close_breakids = [int(x) for x in all_predicted_close_breakids]


def make_Side01_UniqueIds_apply_row(fun_row):
    side0id = str(fun_row['ViewData.Side0_UniqueIds'])
    side1id = str(fun_row['ViewData.Side1_UniqueIds'])
    return(side0id + side1id)
    
closed_new_breakid_side01_ids_df2 = meo_df[meo_df['ViewData.BreakID'].isin(int_all_predicted_close_breakids)][['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]
closed_new_breakid_side01_ids_df2['Side0_1_UniqueIds'] = closed_new_breakid_side01_ids_df2.apply(lambda row : make_Side01_UniqueIds_apply_row(fun_row = row), axis = 1, result_type="expand")
Side_0_1_UniqueIds_closed_all_dates_list = closed_new_breakid_side01_ids_df2['Side0_1_UniqueIds'].tolist()

new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list]
new_closed_keys = [i.replace('None','') for i in new_closed_keys]


#End new closed code

df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)

## Output for Closed breaks

closed_df_side1 = df1[df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)]
closed_df_side0 = df1[df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)]
closed_df = closed_df_side1.append(closed_df_side0)


# In[172]:


df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]

df = df2.copy()
df = df.reset_index()
df = df.drop('index',1)
df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)

pd.to_datetime(df['Date'])

df['Date'] = pd.to_datetime(df['Date']).dt.date

df['Date'] = df['Date'].astype(str)

df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB','SPM'])]
df = df.reset_index()
df = df.drop('index',1)
df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)

df.loc[df['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
df.loc[df['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

df.loc[df['ViewData.Side0_UniqueIds']=='None','flag_side0'] = 0
df.loc[df['ViewData.Side1_UniqueIds']=='None','flag_side1'] = 0


df.loc[df['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
df.loc[df['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0

df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})

print('The Date value count is:')
print(df['Date'].value_counts())

date_i = df['Date'].mode()[0]

# Change added on 20-12-2020. Separate folder is made for each date daily run and csv files will be stored here for each date
# Change made by Rohit on 09-12-2020 to make dynamic directories
# base dir
base_dir = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client +'\\output_files'

# create dynamic name with date as folder
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code +'\\BD_of_' + str(date_i))
# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)


# # create dynamic name with date as folder
# base_dir_plus_Lombard = os.path.join(base_dir, client)

# # create 'dynamic' dir, if it does not exist
# if not os.path.exists(base_dir_plus_Lombard):
#     os.makedirs(base_dir_plus_Lombard)

# # create dynamic name with date as folder
# base_dir_plus_Lombard_plus_249 = os.path.join(base_dir_plus_Lombard, setup)

# # create 'dynamic' dir, if it does not exist
# if not os.path.exists(base_dir_plus_Lombard_plus_249):
#     os.makedirs(base_dir_plus_Lombard_plus_249)

print('Choosing the date : ' + date_i)

sample = df[df['Date'] == date_i]
sample = sample.reset_index()
sample = sample.drop('index',1)

#Change added on 11-01-2021 as per Pratik. This code will catch UMB and OB mtm and otm without disconneting UMBs. This is different from umb_ob table since umb_ob table catches only one umb with one ob pair. Whereas this code below will catch multiple umbs with multiple obs
#Begin code change added on 11-01-2021
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

sample.loc[sample['ViewData.Side0_UniqueIds']=='None','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='None','flag_side1'] = 0


sample.loc[sample['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0

sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'

sample.loc[sample['ViewData.Side1_UniqueIds']=='None','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='None','Trans_side'] = 'A_side'

sample.loc[sample['ViewData.Side1_UniqueIds']=='','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='','Trans_side'] = 'A_side'

sample['filter_key'] = sample['ViewData.Mapped Custodian Account'].astype(str) + sample['ViewData.Currency'].astype(str) + sample['ViewData.Source Combination Code'].astype(str)

def subSum_umb(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total, atol=0.1).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
    
    
zero_id_new = []
one_id_new = []
filter_key_umb_new = []
breakid_0_umb =[]
breakid_1_umb =[]
task_id =[]
source_combination_code =[]
task_business_date =[]

if(sample['filter_key'].unique().size != 0):

    for key in sample['filter_key'].unique():    
        sample_dummy = sample[sample['filter_key']==key]
        if (-0.1<= sample_dummy['ViewData.Net Amount Difference'].sum() <=0.1) & (sample_dummy.shape[0]>2) & (sample_dummy['Trans_side'].nunique()>1):
            #print(cc2_dummy.shape[0])
            #print(key)
            filter_key_umb_new.append(key)
        elif(sample_dummy.shape[0]<15):
            values3 =  sample_dummy['ViewData.Net Amount Difference'].values
            net_sum3 = 0
            
            if subSum_umb(values3,net_sum3) == []:
                
                amount_array3 = ['NULL']
            else:
                amount_array3 = subSum_umb(values3,net_sum3)
            
            #amount_array3 = subSum(values3,net_sum3)
            #if amount_array3 ==[]:
            #    amount_array3 = ['NULL']
            id0_aggregation_new = sample_dummy[(sample_dummy['ViewData.Net Amount Difference'].isin(amount_array3))]['ViewData.Side0_UniqueIds'].values
            id1_aggregation_new = sample_dummy[(sample_dummy['ViewData.Net Amount Difference'].isin(amount_array3))]['ViewData.Side1_UniqueIds'].values
            breakid0_new = sample_dummy[(sample_dummy['ViewData.Net Amount Difference'].isin(amount_array3))]['ViewData.BreakID'].values 
            breakid1_new = sample_dummy[(sample_dummy['ViewData.Net Amount Difference'].isin(amount_array3))]['ViewData.BreakID'].values
            
            if len(id0_aggregation_new)>0 and len(id1_aggregation_new)>0:
                #zero_id_new.append(id0_aggregation_new)
                #one_id_new.append(id1_aggregation_new)
                
                #print(zero_id_new)
                breakid_0_umb.append(sample_dummy[sample_dummy['ViewData.Side0_UniqueIds'].isin([i for i in id0_aggregation_new if i not in ['nan','None','']])]['ViewData.BreakID'].values)
                breakid_1_umb.append(sample_dummy[sample_dummy['ViewData.Side1_UniqueIds'].isin([i for i in id1_aggregation_new if i not in ['nan','None','']])]['ViewData.BreakID'].values)
                zero_id_new.append([i for i in id0_aggregation_new if i not in ['nan','None','']])
                one_id_new.append([i for i in id1_aggregation_new if i not in ['nan','None','']])
                
                task_id.append(sample_dummy[sample_dummy['ViewData.Side0_UniqueIds'].isin([i for i in id0_aggregation_new if i not in ['nan','None','']])]['ViewData.Task ID'].unique())
                source_combination_code.append(sample_dummy[sample_dummy['ViewData.Side0_UniqueIds'].isin([i for i in id0_aggregation_new if i not in ['nan','None','']])]['ViewData.Source Combination Code'].unique())
                task_business_date.append(sample_dummy[sample_dummy['ViewData.Side0_UniqueIds'].isin([i for i in id0_aggregation_new if i not in ['nan','None','']])]['ViewData.Task Business Date'].unique())

else:
    zero_id_new = []
    one_id_new = []
    filter_key_umb_new = []
    breakid_0_umb =[]
    breakid_1_umb =[]
    task_id =[]
    source_combination_code =[]
    task_business_date =[]




if not(zero_id_new == [[]] or zero_id_new == []):
    if(len(zero_id_new) == len(one_id_new) == len(breakid_0_umb) == len(breakid_1_umb) == 1):
        first_row_as_single_list_element = [[zero_id_new[0],one_id_new[0], breakid_0_umb[0].tolist(),breakid_1_umb[0].tolist()]]
        umb_subsum_df = pd.DataFrame(first_row_as_single_list_element, columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID_0','ViewData.BreakID_1'])
        umb_subsum_df['ViewData.Task ID'] = task_id
        umb_subsum_df['ViewData.Source Combination Code'] = source_combination_code
        umb_subsum_df['ViewData.Task Business Date'] = task_business_date

    else:
        umb_subsum_df = pd.DataFrame(np.array(zero_id_new))
        umb_subsum_df.columns = ['ViewData.Side0_UniqueIds']
        umb_subsum_df['ViewData.Side1_UniqueIds'] = np.array(one_id_new)
        umb_subsum_df['ViewData.BreakID_0'] = breakid_0_umb
        umb_subsum_df['ViewData.BreakID_1'] = breakid_1_umb
    
    umb_subsum_df['ViewData.Task ID'] = task_id
    umb_subsum_df['ViewData.Source Combination Code'] = source_combination_code
    umb_subsum_df['ViewData.Task Business Date'] = task_business_date
    
    
    umb_subsum_df['len0'] = umb_subsum_df['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
    umb_subsum_df['len1'] = umb_subsum_df['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
    umb_subsum_df = umb_subsum_df[(umb_subsum_df['len0']>0) &(umb_subsum_df['len1']>0)]
    umb_subsum_df = umb_subsum_df[~((umb_subsum_df['len0']==1) &(umb_subsum_df['len1']==1))]
    umb_subsum_df = umb_subsum_df.reset_index().drop('index',1)
    umb_subsum_df = umb_subsum_df.drop(['len0','len1'],1)
    
    
else:
     umb_subsum_df = pd.DataFrame()


if len(zero_id_new[0])>0:
    umb_subsum_df = pd.DataFrame(np.array(zero_id_new))
    umb_subsum_df.columns = ['ViewData.Side0_UniqueIds']
    umb_subsum_df['ViewData.Side1_UniqueIds'] = np.array(one_id_new)
    umb_subsum_df['ViewData.BreakID_0'] = breakid_0_umb
    umb_subsum_df['ViewData.BreakID_1'] = breakid_1_umb
    
    umb_subsum_df['ViewData.Task ID'] = task_id
    umb_subsum_df['ViewData.Source Combination Code'] = source_combination_code
    umb_subsum_df['ViewData.Task Business Date'] = task_business_date
    
    
    umb_subsum_df['len0'] = umb_subsum_df['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
    umb_subsum_df['len1'] = umb_subsum_df['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
    umb_subsum_df = umb_subsum_df[(umb_subsum_df['len0']>0) &(umb_subsum_df['len1']>0)]
    umb_subsum_df = umb_subsum_df[~((umb_subsum_df['len0']==1) &(umb_subsum_df['len1']==1))]
    umb_subsum_df = umb_subsum_df.reset_index().drop('index',1)
    umb_subsum_df = umb_subsum_df.drop(['len0','len1'],1)
    
    
else:
     umb_subsum_df = pd.DataFrame()

#1111
#umb_subsum_df = pd.DataFrame()


#############################################################################################################         
## Code for UMB MTM part 2#########

full_mtm_1_ids_umb = []
full_mtm_0_ids_umb = []

task_id =[]
source_combination_code =[]
task_business_date =[]

breakid_0_umb_final = []
breakid_1_umb_final =[]

if(len(filter_key_umb_new) != 0):
    for key in filter_key_umb_new:
        one_side = sample[sample['filter_key']== key]['ViewData.Side1_UniqueIds'].unique()
        zero_side = sample[sample['filter_key']== key]['ViewData.Side0_UniqueIds'].unique()
        breakid_0_umb_final.append(sample[sample['ViewData.Side0_UniqueIds'].isin([i for i in zero_side if i not in ['nan','None','']])]['ViewData.BreakID'].values)
        breakid_1_umb_final.append(sample[sample['ViewData.Side1_UniqueIds'].isin([i for i in one_side if i not in ['nan','None','']])]['ViewData.BreakID'].values)
                
        
        task_id.append(sample[sample['ViewData.Side1_UniqueIds'].isin([i for i in one_side if i not in ['nan','None','']])]['ViewData.Task ID'].unique())
        source_combination_code.append(sample[sample['ViewData.Side1_UniqueIds'].isin([i for i in one_side if i not in ['nan','None','']])]['ViewData.Source Combination Code'].unique())
        task_business_date.append(sample[sample['ViewData.Side1_UniqueIds'].isin([i for i in one_side if i not in ['nan','None','']])]['ViewData.Task Business Date'].unique())
        
        one_side = [i for i in one_side if i not in ['nan','None','']]
        zero_side = [i for i in zero_side if i not in ['nan','None','']]
        full_mtm_1_ids_umb.append(one_side)
        full_mtm_0_ids_umb.append(zero_side)
else:
    one_side = []
    zero_side = []
    breakid_0_umb_final = np.array([]).astype('int64')
    breakid_1_umb_final = np.array([]).astype('int64')
    task_id = np.array([]).astype('int64')
    source_combination_code = np.array([]).astype('object')
    task_business_date = np.array([]).astype('object')
    full_mtm_1_ids_umb = []
    full_mtm_0_ids_umb = []
    
if full_mtm_1_ids_umb !=[]:
    full_mtm_list_1_umb = list(np.concatenate(full_mtm_1_ids_umb))
else:
    full_mtm_list_1_umb = []

if full_mtm_0_ids_umb !=[]:
    full_mtm_list_0_umb = list(np.concatenate(full_mtm_0_ids_umb))
else:
    full_mtm_list_0_umb = []

############################################################################################################# 
## Data Frame for MTM from mapped custodian account and currency

if len(full_mtm_0_ids_umb)>0:
    mtm_df_full_umb = pd.DataFrame(np.arange(len(full_mtm_0_ids_umb)))
    mtm_df_full_umb.columns = ['index']

    mtm_df_full_umb['ViewData.Side0_UniqueIds'] = full_mtm_0_ids_umb
    mtm_df_full_umb['ViewData.Side1_UniqueIds'] = full_mtm_1_ids_umb
    mtm_df_full_umb = mtm_df_full_umb.drop('index',1)
    
    
    mtm_df_full_umb['ViewData.BreakID_0'] = breakid_0_umb_final
    mtm_df_full_umb['ViewData.BreakID_1'] = breakid_1_umb_final
    
    mtm_df_full_umb['ViewData.Task ID'] = task_id
    mtm_df_full_umb['ViewData.Source Combination Code'] = source_combination_code
    mtm_df_full_umb['ViewData.Task Business Date'] = task_business_date
    
    


    mtm_df_full_umb['len0'] = mtm_df_full_umb['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
    mtm_df_full_umb['len1'] = mtm_df_full_umb['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))

    mtm_df_full_umb = mtm_df_full_umb[(mtm_df_full_umb['len0']!=0) | (mtm_df_full_umb['len1']!=0)]
    mtm_df_full_umb = mtm_df_full_umb.drop(['len0','len1'],1)
    if mtm_df_full_umb.shape[0]==0:
        mtm_df_full_umb = pd.Dataframe()
else:
    mtm_df_full_umb = pd.DataFrame()

if(mtm_df_full_umb.shape[0] != 0):    
    mtm_list_0_full_umb = np.concatenate(mtm_df_full_umb['ViewData.Side0_UniqueIds'])
    mtm_list_1_full_umb = np.concatenate(mtm_df_full_umb['ViewData.Side1_UniqueIds'])
else:
    mtm_list_0_full_umb = []
    mtm_list_1_full_umb = []
    
if len(mtm_list_0_full_umb) == 0:
    mtm_list_0_full_umb = ['AAA','BBB']
if len(mtm_list_1_full_umb) == 0:
    mtm_list_1_full_umb = ['AAA','BBB']

#Removing Break ids caught in umb_subsum_df from sample
if(umb_subsum_df.shape[0] != 0):
    umb_subsum_df_all_breakids_list = list(np.concatenate(umb_subsum_df['ViewData.BreakID_0'])) + list(np.concatenate(umb_subsum_df['ViewData.BreakID_1']))
    umb_subsum_df_all_breakids_list_int = [int(x) for x in umb_subsum_df_all_breakids_list]
    sample = sample[~(sample['ViewData.BreakID'].isin(umb_subsum_df_all_breakids_list_int))]

#Removing Break ids caught in mtm_df_full_umb from sample
if(mtm_df_full_umb.shape[0] != 0):
    mtm_df_full_umb_all_breakids_list = list(np.concatenate(mtm_df_full_umb['ViewData.BreakID_0'])) + list(np.concatenate(mtm_df_full_umb['ViewData.BreakID_1']))
    mtm_df_full_umb_all_breakids_list_int = [int(x) for x in mtm_df_full_umb_all_breakids_list]
    sample = sample[~(sample['ViewData.BreakID'].isin(mtm_df_full_umb_all_breakids_list_int))]

#Make table for umb_subsum_df
if(umb_subsum_df.shape[0] != 0):
    umb_subsum_df_copy = umb_subsum_df.copy()
    umb_subsum_df_copy.rename(columns = {'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds','ViewData.Side0_UniqueIds' : 'Side0_UniqueIds', 'ViewData.BreakID_0' : 'BreakID', 'ViewData.BreakID_1' : 'Final_predicted_break', 'ViewData.Task ID' : 'TaskID','ViewData.Source Combination Code' : 'SourceCombinationCode', 'ViewData.Task Business Date' : 'BusinessDate'}, inplace = True)
    umb_subsum_df_copy['Side1_UniqueIds'] = umb_subsum_df_copy['Side1_UniqueIds'].astype(str)
    umb_subsum_df_copy['Side0_UniqueIds'] = umb_subsum_df_copy['Side0_UniqueIds'].astype(str)
    umb_subsum_df_copy['BreakID'] = umb_subsum_df_copy['BreakID'].astype(str)
    umb_subsum_df_copy['Final_predicted_break'] = umb_subsum_df_copy['Final_predicted_break'].astype(str)
    umb_subsum_df_copy['TaskID'] = umb_subsum_df_copy['TaskID'].astype(str)
    umb_subsum_df_copy['SourceCombinationCode'] = umb_subsum_df_copy['SourceCombinationCode'].astype(str)
    umb_subsum_df_copy['BusinessDate'] = umb_subsum_df_copy['BusinessDate'].astype(str)
    
    umb_subsum_df_copy['Side1_UniqueIds'] = umb_subsum_df_copy['Side1_UniqueIds'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'',',').replace('\'','').replace('.0',''))
    umb_subsum_df_copy['Side0_UniqueIds'] = umb_subsum_df_copy['Side0_UniqueIds'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'',',').replace('\'','').replace('.0',''))
    umb_subsum_df_copy['BreakID'] = umb_subsum_df_copy['BreakID'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',', ').replace('.0',''))
    umb_subsum_df_copy['Final_predicted_break'] = umb_subsum_df_copy['Final_predicted_break'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',', ').replace('.0',''))
    
    umb_subsum_df_copy['TaskID'] = umb_subsum_df_copy['TaskID'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',',').replace('.0','').replace('\'',''))
    umb_subsum_df_copy['SourceCombinationCode'] = umb_subsum_df_copy['SourceCombinationCode'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',',').replace('.0','').replace('\'',''))

    umb_subsum_df_copy['BusinessDate'] = umb_subsum_df_copy['BusinessDate'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\'',''))

    umb_subsum_df_copy['BusinessDate'] = pd.to_datetime(umb_subsum_df_copy['BusinessDate'])
    umb_subsum_df_copy['BusinessDate'] = umb_subsum_df_copy['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    umb_subsum_df_copy['BusinessDate'] = pd.to_datetime(umb_subsum_df_copy['BusinessDate'])

    umb_subsum_df_copy['ML_flag'] = 'ML'

    umb_subsum_df_copy['Predicted_Status'] = 'UMR'
    umb_subsum_df_copy['Predicted_action'] = 'UMR_Many_to_Many'
    umb_subsum_df_copy['probability_No_pair'] = ''
    umb_subsum_df_copy['probability_UMB'] = ''
    umb_subsum_df_copy['probability_UMR'] = ''    
    umb_subsum_df_copy['probability_UMT'] = ''    
    umb_subsum_df_copy['ML_flag'] = 'ML'
    umb_subsum_df_copy['SetupID'] = setup_code 
    umb_subsum_df_copy['PredictedComment'] = ''    
    umb_subsum_df_copy['PredictedCategory'] = ''    
    filepaths_umb_subsum_df_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_subsum_df_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    umb_subsum_df_copy.to_csv(filepaths_umb_subsum_df_copy)
else:
    umb_subsum_df_copy = pd.DataFrame()
  
#Make table for mtm_df_full_umb
if(mtm_df_full_umb.shape[0] != 0):
    mtm_df_full_umb_copy = mtm_df_full_umb.copy()
    mtm_df_full_umb_copy.rename(columns = {'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds','ViewData.Side0_UniqueIds' : 'Side0_UniqueIds', 'ViewData.BreakID_0' : 'BreakID', 'ViewData.BreakID_1' : 'Final_predicted_break', 'ViewData.Task ID' : 'TaskID','ViewData.Source Combination Code' : 'SourceCombinationCode', 'ViewData.Task Business Date' : 'BusinessDate'}, inplace = True)
    mtm_df_full_umb_copy['Side1_UniqueIds'] = mtm_df_full_umb_copy['Side1_UniqueIds'].astype(str)
    mtm_df_full_umb_copy['Side0_UniqueIds'] = mtm_df_full_umb_copy['Side0_UniqueIds'].astype(str)
    mtm_df_full_umb_copy['BreakID'] = mtm_df_full_umb_copy['BreakID'].astype(str)
    mtm_df_full_umb_copy['Final_predicted_break'] = mtm_df_full_umb_copy['Final_predicted_break'].astype(str)
    mtm_df_full_umb_copy['TaskID'] = mtm_df_full_umb_copy['TaskID'].astype(str)
    mtm_df_full_umb_copy['SourceCombinationCode'] = mtm_df_full_umb_copy['SourceCombinationCode'].astype(str)
    mtm_df_full_umb_copy['BusinessDate'] = mtm_df_full_umb_copy['BusinessDate'].astype(str)
    
    mtm_df_full_umb_copy['Side1_UniqueIds'] = mtm_df_full_umb_copy['Side1_UniqueIds'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'',',').replace('\'','').replace('.0',''))
    mtm_df_full_umb_copy['Side0_UniqueIds'] = mtm_df_full_umb_copy['Side0_UniqueIds'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'',',').replace('\'','').replace('.0',''))
    mtm_df_full_umb_copy['BreakID'] = mtm_df_full_umb_copy['BreakID'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',', ').replace('.0',''))
    mtm_df_full_umb_copy['Final_predicted_break'] = mtm_df_full_umb_copy['Final_predicted_break'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',', ').replace('.0',''))
    
    mtm_df_full_umb_copy['TaskID'] = mtm_df_full_umb_copy['TaskID'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',',').replace('.0','').replace('\'',''))
    mtm_df_full_umb_copy['SourceCombinationCode'] = mtm_df_full_umb_copy['SourceCombinationCode'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace(' ',',').replace('.0','').replace('\'',''))

    mtm_df_full_umb_copy['BusinessDate'] = mtm_df_full_umb_copy['BusinessDate'].apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\'',''))

    mtm_df_full_umb_copy['BusinessDate'] = pd.to_datetime(mtm_df_full_umb_copy['BusinessDate'])
    mtm_df_full_umb_copy['BusinessDate'] = mtm_df_full_umb_copy['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    mtm_df_full_umb_copy['BusinessDate'] = pd.to_datetime(mtm_df_full_umb_copy['BusinessDate'])

    mtm_df_full_umb_copy['ML_flag'] = 'ML'

    mtm_df_full_umb_copy['Predicted_Status'] = 'UMR'
    mtm_df_full_umb_copy['Predicted_action'] = 'UMR_Many_to_Many'
    mtm_df_full_umb_copy['probability_No_pair'] = ''
    mtm_df_full_umb_copy['probability_UMB'] = ''
    mtm_df_full_umb_copy['probability_UMR'] = ''    
    mtm_df_full_umb_copy['probability_UMT'] = ''    
    mtm_df_full_umb_copy['ML_flag'] = 'ML'
    mtm_df_full_umb_copy['SetupID'] = setup_code 
    mtm_df_full_umb_copy['PredictedComment'] = ''    
    mtm_df_full_umb_copy['PredictedCategory'] = ''    
    filepaths_mtm_df_full_umb_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\mtm_df_full_umb_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    mtm_df_full_umb_copy.to_csv(filepaths_mtm_df_full_umb_copy)
else:
    mtm_df_full_umb_copy = pd.DataFrame()

umb_carry_forward_df = sample[sample['ViewData.Status'] == 'UMB']

#End code change added on  11-01-2021

smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
smb = smb.drop('index',1)
smb_pb = smb.copy()
smb_acc = smb.copy()
smb_pb['ViewData.Accounting Net Amount'] = np.nan
smb_pb['ViewData.Side0_UniqueIds'] = np.nan
smb_pb['ViewData.Status'] ='SMB-OB'

smb_acc['ViewData.B-P Net Amount'] = np.nan
smb_acc['ViewData.Side1_UniqueIds'] = np.nan
smb_acc['ViewData.Status'] ='SMB-OB'

sample = sample[sample['ViewData.Status']!='SMB']
sample = sample.reset_index()
sample = sample.drop('index',1)

sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
sample = sample.reset_index()
sample = sample.drop('index',1)

sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


sample[sample['ViewData.Side1_UniqueIds']=='nan'].shape

sample[sample['ViewData.Side0_UniqueIds']=='None'].shape

sample[sample['ViewData.Side0_UniqueIds']=='NaN'].shape


# In[177]:


sample[sample['ViewData.Side0_UniqueIds']==''].shape


# In[178]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

sample.loc[sample['ViewData.Side0_UniqueIds']=='None','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='None','flag_side1'] = 0


sample.loc[sample['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0

sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'

sample.loc[sample['ViewData.Side1_UniqueIds']=='None','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='None','Trans_side'] = 'A_side'

sample.loc[sample['ViewData.Side1_UniqueIds']=='','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='','Trans_side'] = 'A_side'


# In[179]:


sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 

sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)

sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','SMB-OB','CNF','CMF']))]

sample1 = sample1.reset_index()
sample1 = sample1.drop('index', 1)

sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)

sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)

sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)

aa = sample1[sample1['Trans_side']=='A_side']
bb = sample1[sample1['Trans_side']=='B_side']

aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)

bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)

aa = aa.reset_index()
aa = aa.drop('index', 1)
bb = bb.reset_index()
bb = bb.drop('index', 1)

bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
bb = bb.reset_index()
bb = bb.drop('index',1)
# Change made on 20-12-2020 as asked by Pratik. For this particular code piece, on a given settle date, some breaks in many_to_many category knock off and will be removed from further down the line
# Begin change on 20-12-2020
cc = pd.concat([aa, bb], axis=0)

cc = cc.reset_index().drop('index',1)

cc['ViewData.Transaction Type'] = cc['ViewData.Transaction Type'].astype(str)
cc['ViewData.Settle Date'] = pd.to_datetime(cc['ViewData.Settle Date'])
cc['filter_key_with_sd'] = cc['filter_key'].astype(str) + cc['ViewData.Settle Date'].astype(str)


##########################
cc7 = cc.copy()

cc_new = cc7[cc7['ViewData.Status']!='SPM']
cc_new = cc_new.reset_index().drop('index',1)

#cc_new = cc_new[~((cc_new['ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])) | (cc_new['ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]

#cc_new = cc_new[~((cc_new['ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) | (cc_new['ViewData.Side1_UniqueIds'].isin(final_umt_table['SideA.ViewData.Side1_UniqueIds'])))]

cc_new = cc_new.reset_index().drop('index',1)

filter_key_umt_umb_sd = []
diff_in_amount_sd = []
diff_in_amount_key_sd = []
for key in cc_new['filter_key_with_sd'].unique():    
    cc_dummy = cc_new[cc_new['filter_key_with_sd']==key]
    if (-0.25<= cc_dummy['ViewData.Net Amount Difference'].sum() <=0.25) & (cc_dummy.shape[0]>2) & (cc_dummy['Trans_side'].nunique()>1):
        #print(cc2_dummy.shape[0])
        #print(key)
        filter_key_umt_umb_sd.append(key)
    else:
        if (cc_dummy.shape[0]>2) & (cc_dummy['Trans_side'].nunique()>1):
            diff = cc_dummy['ViewData.Net Amount Difference'].sum()
            diff_in_amount_sd.append(diff)
            diff_in_amount_key_sd.append(key)

## Equity Swap Many to many

sd_mtm_1_ids = []
sd_mtm_0_ids = []

for key in filter_key_umt_umb_sd:
    one_side = cc_new[cc_new['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
    zero_side = cc_new[cc_new['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
    one_side = [i for i in one_side if i not in ['nan','None','']]
    zero_side = [i for i in zero_side if i not in ['nan','None','']]
    sd_mtm_1_ids.append(one_side)
    sd_mtm_0_ids.append(zero_side)

if sd_mtm_1_ids !=[]:
    sd_mtm_list_1 = list(np.concatenate(sd_mtm_1_ids))
else:
    sd_mtm_list_1 = []

if sd_mtm_0_ids !=[]:
    sd_mtm_list_0 = list(np.concatenate(sd_mtm_0_ids))
else:
    sd_mtm_list_0 = []

## Data Frame for MTM from equity Swap

mtm_df_sd = pd.DataFrame(np.arange(len(sd_mtm_0_ids)))
mtm_df_sd.columns = ['index']

mtm_df_sd['ViewData.Side0_UniqueIds'] = sd_mtm_0_ids
mtm_df_sd['ViewData.Side1_UniqueIds'] = sd_mtm_1_ids
mtm_df_sd = mtm_df_sd.drop('index',1)

# TODO: remove mtm_df_sd ids from aa and bb
# TODO: include mtm_df_sd into final_df_2
# End change on 20-12-2020



sd_mtm_0_ids_flat = []
for sublist in sd_mtm_0_ids:
#     print(sublist)
    for item in sublist:
#         print(item)
        sd_mtm_0_ids_flat.append(item)
# sd_mtm_0_ids_flat = [item for sublist in sd_mtm_0_ids for item in sublist]
print(sd_mtm_0_ids_flat)


# In[184]:


sd_mtm_1_ids_flat = []
for sublist in sd_mtm_1_ids:
#     print(sublist)
    for item in sublist:
#         print(item)
        sd_mtm_1_ids_flat.append(item)
# sd_mtm_1_ids_flat = [item for sublist in sd_mtm_1_ids for item in sublist]
print(sd_mtm_1_ids_flat)


print(aa.shape)
print(bb.shape)

print(len(sd_mtm_1_ids_flat))
print(len(sd_mtm_0_ids_flat))


aa = aa[~((aa['ViewData.Side0_UniqueIds'].isin(sd_mtm_0_ids_flat)) |(aa['ViewData.Side1_UniqueIds'].isin(sd_mtm_1_ids_flat)))]
aa = aa.reset_index().drop('index',1)
print(aa.shape)

bb = bb[~((bb['ViewData.Side0_UniqueIds'].isin(sd_mtm_0_ids_flat)) |(bb['ViewData.Side1_UniqueIds'].isin(sd_mtm_1_ids_flat)))]
bb = bb.reset_index().drop('index',1)

print(bb.shape)


#Change made on 23-12-2020 as per Pratik. This code takes knock off at Mapped Custodian Account and Currency level
#Begin change made on 23-12-2020
filter_key_umt_umb_full = []
#diff_in_amount_sd = []
#diff_in_amount_key_sd = []
for key in cc['filter_key'].unique():    
    cc_dummy = cc[cc['filter_key']==key]
    if (-0.1<= cc_dummy['ViewData.Net Amount Difference'].sum() <=0.1) & (cc_dummy.shape[0]>2) & (cc_dummy['Trans_side'].nunique()>1):
        #print(cc2_dummy.shape[0])
        #print(key)
        filter_key_umt_umb_full.append(key)

        
#############################################################################################################         
## filter key MTM

full_mtm_1_ids = []
full_mtm_0_ids = []

for key in filter_key_umt_umb_full:
    one_side = cc[cc['filter_key']== key]['ViewData.Side1_UniqueIds'].unique()
    zero_side = cc[cc['filter_key']== key]['ViewData.Side0_UniqueIds'].unique()
    one_side = [i for i in one_side if i not in ['nan','None','']]
    zero_side = [i for i in zero_side if i not in ['nan','None','']]
    full_mtm_1_ids.append(one_side)
    full_mtm_0_ids.append(zero_side)

if full_mtm_1_ids !=[]:
    full_mtm_list_1 = list(np.concatenate(full_mtm_1_ids))
else:
    full_mtm_list_1 = []

if full_mtm_0_ids !=[]:
    full_mtm_list_0 = list(np.concatenate(full_mtm_0_ids))
else:
    full_mtm_list_0 = []


############################################################################################################# 
## Data Frame for MTM from mapped custodian account and currency

if len(full_mtm_0_ids)>0:
    mtm_df_full = pd.DataFrame(np.arange(len(full_mtm_0_ids)))
    mtm_df_full.columns = ['index']

    mtm_df_full['ViewData.Side0_UniqueIds'] = full_mtm_0_ids
    mtm_df_full['ViewData.Side1_UniqueIds'] = full_mtm_1_ids
    mtm_df_full = mtm_df_full.drop('index',1)


    mtm_df_full['len0'] = mtm_df_full['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
    mtm_df_full['len1'] = mtm_df_full['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))

    mtm_df_full = mtm_df_full[(mtm_df_full['len0']!=0) | (mtm_df_full['len1']!=0)]
    mtm_df_full = mtm_df_full.drop(['len0','len1'],1)
    if mtm_df_full.shape[0]==0:
        mtm_df_full = pd.Dataframe()
else:
    mtm_df_full = pd.DataFrame()

if(mtm_df_full.shape[0] != 0):
    mtm_list_0_full = np.concatenate(mtm_df_full['ViewData.Side0_UniqueIds'])
    mtm_list_1_full = np.concatenate(mtm_df_full['ViewData.Side1_UniqueIds'])
else:
    mtm_list_0_full = []
    mtm_list_1_full = []

if len(mtm_list_0_full) == 0:
    mtm_list_0_full = ['AAA','BBB']
if len(mtm_list_1_full) == 0:
    mtm_list_1_full = ['AAA','BBB']
    
    
#############################################################################################################    

aa = aa[~((aa['ViewData.Side0_UniqueIds'].isin(mtm_list_0_full)) |(aa['ViewData.Side1_UniqueIds'].isin(mtm_list_1_full)))]
aa = aa.reset_index().drop('index',1)
    
bb = bb[~((bb['ViewData.Side0_UniqueIds'].isin(mtm_list_0_full)) |(bb['ViewData.Side1_UniqueIds'].isin(mtm_list_1_full)))]
bb = bb.reset_index().drop('index',1)

#End change made on 23-12-2020

###################### loop m*n ###############################

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
#Change made on 26-11-2020 to include CMF and CNF as well, as long as they are single sided. For now, we are assuming they are single sided and no other precaution has been made to explicitely include single sided CNF and CMF
#Change made as per above and commenting below on 26-11-2020
#            no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
#            no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
#Change made on 26-11-2020 to include CNF and CMF
            no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB','CNF','CMF']))]['ViewData.Side1_UniqueIds'].values])
            no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB','CNF','CMF']))]['ViewData.Side0_UniqueIds'].values])
            


if len(no_pair_ids) != 0:
    no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])
    no_pair_ids_df = pd.DataFrame(no_pair_ids, columns = ['Side0_1_UniqueIds'])
#    no_pair_ids_df = pd.merge(no_pair_ids_df, meo_df[['ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side1_UniqueIds', how='left')
#    no_pair_ids_df = pd.merge(no_pair_ids_df, meo_df[['ViewData.Side0_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side0_UniqueIds', how='left')
#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
#    no_pair_ids_df['Predicted_Status'] = 'OB'
#    no_pair_ids_df['Predicted_action'] = 'No-Pair'
#    no_pair_ids_df['probability_No_pair'] = 0.9933
#    no_pair_ids_df['probability_UMB'] = 0.0033
#    no_pair_ids_df['probability_UMR'] = 0.0033    
#    no_pair_ids_df['ML_flag'] = 'ML'
#    no_pair_ids_df['TaskID'] = setup_code 
    no_pair_ids_df.to_csv(filepaths_no_pair_id_data)
else:
     with open(filepaths_no_pair_id_no_data_warning, 'w') as f:
         f.write('No no pair ids found for this setup and date combination')
         no_pair_ids = []
         no_pair_ids_df = pd.DataFrame()


test_file = pd.concat(training_df)

test_file = test_file.reset_index()
test_file = test_file.drop('index',1)


# In[192]:


mtm_df_sd


# In[193]:

if(mtm_df_sd.shape[0] != 0):
    if(mtm_df_full.shape[0] != 0):
        mtm_df_full_final = mtm_df_sd.append(mtm_df_full)
    else:
        mtm_df_full_final = mtm_df_sd.copy()
elif(mtm_df_full.shape[0] != 0):
    mtm_df_full_final = mtm_df_full.copy()
else:
    mtm_df_full_final = pd.DataFrame()


#final_mtm_table_copy = mtm_df_sd.copy()
if(mtm_df_full_final.shape[0] != 0):
	final_mtm_table_copy = mtm_df_full_final.copy()
	final_mtm_table_copy.rename(columns = {'ViewData.Side1_UniqueIds' : 'SideA.ViewData.Side1_UniqueIds',                                          'ViewData.Side0_UniqueIds' : 'SideB.ViewData.Side0_UniqueIds'}, inplace = True)
	
	final_mtm_table_copy['BreakID_Side1'] = final_mtm_table_copy['SideA.ViewData.Side1_UniqueIds'].apply(                                         lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df,                                                                                               fun_side_0_or_1 = 1,                                                                                               fun_str_list_Side_01_UniqueIds = x))
	
	final_mtm_table_copy['BreakID_Side0'] = final_mtm_table_copy['SideB.ViewData.Side0_UniqueIds'].apply(                                         lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df,                                                                                               fun_side_0_or_1 = 0,                                                                                               fun_str_list_Side_01_UniqueIds = x))
	
	
	
	final_mtm_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_mtm_table_copy)
	final_mtm_table_copy['ViewData.Side0_UniqueIds'] = final_mtm_table_copy['ViewData.Side0_UniqueIds'].astype(str)
	final_mtm_table_copy['ViewData.Side1_UniqueIds'] = final_mtm_table_copy['ViewData.Side1_UniqueIds'].astype(str)
	
	#
	#Change made on 10-12-2020. Below piece of code is wrong for merging. Commenting out below two lines of code and writing replacement code below it
	#Single_Side0_UniqueId_for_merging_with_meo_df = final_mtm_table_copy['ViewData.Side0_UniqueIds'][0][0]
	#final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = Single_Side0_UniqueId_for_merging_with_meo_df
	#Change made on 10-12-2020. Above piece of code is wrong for merging. Commenting out above two lines of code and writing replacement code below it
	final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = final_mtm_table_copy['ViewData.Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']')).apply(lambda x : get_first_non_null_value(str(x)))
	
	final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'].astype(str) 
	final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'].replace('\'','',regex = True)
	
	final_mtm_table_copy_new = pd.merge(final_mtm_table_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'ViewData.Side0_UniqueIds_for_merging', right_on = 'ViewData.Side0_UniqueIds', how='left')
	
	final_mtm_table_copy_new['Predicted_Status'] = 'UMR'
	final_mtm_table_copy_new['Predicted_action'] = 'UMR_Many_to_Many'
	final_mtm_table_copy_new['ML_flag'] = 'ML'
	final_mtm_table_copy_new['SetupID'] = setup_code 
	
	del final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging']
	del final_mtm_table_copy_new['ViewData.Side0_UniqueIds_for_merging']
	
	filepaths_final_mtm_table_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mtm_table_copy_new_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
	final_mtm_table_copy_new.to_csv(filepaths_final_mtm_table_copy_new)
	
	change_names_of_final_mtm_table_copy_new_mapping_dict = {
	                                            'ViewData.Side0_UniqueIds_x' : 'Side0_UniqueIds',
	                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
	                                            'BreakID_Side0' : 'BreakID',
	                                            'BreakID_Side1' : 'Final_predicted_break',
	                                            'ViewData.Task ID' : 'Task ID',
	                                            'ViewData.Task Business Date' : 'Task Business Date',
	                                            'ViewData.Source Combination Code' : 'Source Combination Code'
	                                        }
	
	
	final_mtm_table_copy_new.rename(columns = change_names_of_final_mtm_table_copy_new_mapping_dict, inplace = True)
	
	final_mtm_table_copy_new['Task Business Date'] = pd.to_datetime(final_mtm_table_copy_new['Task Business Date'])
	final_mtm_table_copy_new['Task Business Date'] = final_mtm_table_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
	final_mtm_table_copy_new['Task Business Date'] = pd.to_datetime(final_mtm_table_copy_new['Task Business Date'])
	
	
	final_mtm_table_copy_new['PredictedComment'] = ''
	
	#Changing data types of columns as follows:
	#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
	#BreakID, TaskID - int64
	#SetupID - int32
	final_mtm_table_copy_new['probability_UMB'] = 0.017
	final_mtm_table_copy_new['probability_No_pair'] = 0.017
	final_mtm_table_copy_new['probability_UMR'] = 0.95
	final_mtm_table_copy_new['probability_UMT'] = 0.017
	    
	for i in range(0,final_mtm_table_copy_new.shape[0]):
	    final_mtm_table_copy_new['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
	    final_mtm_table_copy_new['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
	    final_mtm_table_copy_new['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(950, 1000))/1000)
	    final_mtm_table_copy_new['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
	
	
	final_mtm_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_mtm_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
	
	
	#Note that BreakID now if more than one and in a list, so we cant convert it to float and int64
	final_mtm_table_copy_new[['Task ID']] = final_mtm_table_copy_new[['Task ID']].astype(float)
	final_mtm_table_copy_new[['Task ID']] = final_mtm_table_copy_new[['Task ID']].astype(np.int64)
	
	final_mtm_table_copy_new[['SetupID']] = final_mtm_table_copy_new[['SetupID']].astype(int)
	
	#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
	#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)
	
	change_col_names_final_mtm_table_copy_new_dict = {
	                        'Task ID' : 'TaskID',
	                        'Task Business Date' : 'BusinessDate',
	                        'Source Combination Code' : 'SourceCombinationCode'
	                        }
	final_mtm_table_copy_new.rename(columns = change_col_names_final_mtm_table_copy_new_dict, inplace = True)
	
	cols_for_database_new = ['Side0_UniqueIds',
	 'Side1_UniqueIds',
	 'BreakID',
	 'Final_predicted_break',
	 'Predicted_action',
	 'probability_No_pair',
	 'probability_UMB',
	 'probability_UMR',
	 'probability_UMT',
	 'TaskID',
	 'BusinessDate',
	 'PredictedComment',
	 'SourceCombinationCode',
	 'Predicted_Status',
	 'ML_flag',
	 'SetupID']
	
	final_mtm_table_copy_new_to_write = final_mtm_table_copy_new[cols_for_database_new]
	filepaths_final_mtm_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mtm_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
	
	final_mtm_table_copy_new_to_write.to_csv(filepaths_final_mtm_table_copy_new_to_write)
else:
	final_mtm_table_copy_new_to_write = pd.DataFrame()


# In[195]:


test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')

test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]

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

test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']


# ## Description code

os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')
print(os.getcwd())

## TODO - Import a csv file for description category mapping

#Change made on 18-02-2021 as per Abhijeet.
#com = pd.read_csv('desc cat with naveen oaktree.csv')
com = pd.read_csv('desc cat with naveen oaktree - final.csv')
cat_list = list(set(com['Pairing']))



test_file['SideA.desc_cat'] = test_file['SideA.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
test_file['SideB.desc_cat'] = test_file['SideB.ViewData.Description'].apply(lambda x : descclean(x,cat_list))

test_file['SideA.desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : currcln(x))
test_file['SideB.desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : currcln(x))

com = com.drop(['var','Catogery'], axis = 1)

com = com.drop_duplicates()

com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


test_file['SideA.new_desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : catcln1(x,com))
test_file['SideB.new_desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : catcln1(x,com))

comp = ['inc','stk','corp ','llc','pvt','plc']
test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : desccat(x))
test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : desccat(x))
# ## Prime Broker
test_file['new_pb'] = test_file['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].fillna('kkk')
test_file['new_pb1'] = test_file.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)
test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days

test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days

############ Fund match new ########

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

vec_fund_match = np.vectorize(fundmatch)

test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)

### New code for cleaning text variables 
trans_type_A_side = test_file['SideA.ViewData.Transaction Type']
trans_type_B_side = test_file['SideB.ViewData.Transaction Type']

asset_type_cat_A_side = test_file['SideA.ViewData.Asset Type Category']
asset_type_cat_B_side = test_file['SideB.ViewData.Asset Type Category']

invest_type_A_side = test_file['SideA.ViewData.Investment Type']
invest_type_B_side = test_file['SideB.ViewData.Investment Type']

prime_broker_A_side = test_file['SideA.ViewData.Prime Broker']
prime_broker_B_side = test_file['SideB.ViewData.Prime Broker']

# LOWER CASE
trans_type_A_side = [str(item).lower() for item in trans_type_A_side]
trans_type_B_side = [str(item).lower() for item in trans_type_B_side]

asset_type_cat_A_side = [str(item).lower() for item in asset_type_cat_A_side]
asset_type_cat_B_side = [str(item).lower() for item in asset_type_cat_B_side]

invest_type_A_side = [str(item).lower() for item in invest_type_A_side]
invest_type_B_side = [str(item).lower() for item in invest_type_B_side]

prime_broker_A_side = [str(item).lower() for item in prime_broker_A_side]
prime_broker_B_side = [str(item).lower() for item in prime_broker_B_side]

split_trans_A_side = [item.split() for item in trans_type_A_side]
split_trans_B_side = [item.split() for item in trans_type_B_side]

split_asset_A_side = [item.split() for item in asset_type_cat_A_side]
split_asset_B_side = [item.split() for item in asset_type_cat_B_side]

split_invest_A_side = [item.split() for item in invest_type_A_side]
split_invest_B_side = [item.split() for item in invest_type_B_side]

split_prime_A_side = [item.split() for item in prime_broker_A_side]
split_prime_b_side = [item.split() for item in prime_broker_B_side]

## Transacion type

remove_nums_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_trans_A_side]
remove_nums_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_trans_B_side]

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

cleaned_trans_types_A_side = [' '.join(item) for item in remove_ats_A_side]
cleaned_trans_types_B_side = [' '.join(item) for item in remove_ats_B_side]

# # INVESTMENT TYPE

remove_nums_i_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_A_side]
remove_nums_i_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_B_side]

remove_dates_i_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_A_side]
remove_dates_i_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_B_side]

cleaned_invest_A_side = [' '.join(item) for item in remove_dates_i_A_side]
cleaned_invest_B_side = [' '.join(item) for item in remove_dates_i_B_side]

remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]

test_file['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
test_file['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

test_file['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
test_file['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

test_file['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
test_file['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side

values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values

vec_tt_match = np.vectorize(mhreplaced)

test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)

test_file.loc[test_file['SideA.ViewData.Transaction Type']=='int','SideA.ViewData.Transaction Type'] = 'interest'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='wires','SideA.ViewData.Transaction Type'] = 'wire'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='dividends','SideA.ViewData.Transaction Type'] = 'dividend'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='miscellaneous','SideA.ViewData.Transaction Type'] = 'misc'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='div','SideA.ViewData.Transaction Type'] = 'dividend'

test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))

test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)

test_file['Combined_Investment_Type'] = test_file['SideA.ViewData.Investment Type'].astype(str) + test_file['SideB.ViewData.Investment Type'].astype(str)

test_file['Combined_Asset_Type_Category'] = test_file['SideA.ViewData.Asset Category Type'].astype(str) + test_file['SideB.ViewData.Asset Category Type'].astype(str)

    
vec_nan_fun = np.vectorize(nan_fun)
values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
test_file['SideA.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)
test_file['SideB.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)

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

vec_nan_equal_fun = np.vectorize(nan_equals_fun)
values_key_NAN_B_Side = test_file['SideB.ViewData.key_NAN'].values
values_key_NAN_A_Side = test_file['SideA.ViewData.key_NAN'].values
test_file['All_key_nan'] = vec_nan_equal_fun(values_key_NAN_B_Side,values_key_NAN_A_Side )

test_file['SideB.ViewData.Common_key'] = test_file['SideB.ViewData.Common_key'].astype(str)
test_file['SideA.ViewData.Common_key'] = test_file['SideA.ViewData.Common_key'].astype(str)

vec_new_key_match_fun = np.vectorize(new_key_match_fun)
values_Common_key_B_Side = test_file['SideB.ViewData.Common_key'].values
values_Common_key_A_Side = test_file['SideA.ViewData.Common_key'].values
values_All_key_NAN = test_file['All_key_nan'].values

test_file['new_key_match']= vec_new_key_match_fun(values_Common_key_B_Side,values_Common_key_A_Side,values_All_key_NAN)

test_file['amount_percent'] = (test_file['SideA.ViewData.B-P Net Amount']/test_file['SideB.ViewData.Accounting Net Amount']*100)

test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())

test_file['SideB.ViewData.Prime Broker'] = test_file['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())

test_file['SideB.ViewData.Asset Type Category'] = test_file['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Asset Type Category'] = test_file['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())

test_file['ViewData.Combined Transaction Type'] = test_file['ViewData.Combined Transaction Type'].apply(lambda x: x.replace('jnl','journal'))

test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))

trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']

test_file['SideA.TType'] = test_file.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)
test_file['SideB.TType'] = test_file.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)

test_file['Combined_Desc'] = test_file['SideA.new_desc_cat'] + test_file['SideB.new_desc_cat']

test_file['Combined_TType'] = test_file['SideA.TType'].astype(str) + test_file['SideB.TType'].astype(str)

for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    test_file[feature] = pd.to_datetime(test_file[feature],errors = 'coerce').dt.weekday


# In[196]:



# ## UMR Mapping
## TODO Import HIstorical UMR FILE for Transaction Type mapping
oaktree_umr = pd.read_csv('OakTree_UMR.csv')

test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in oaktree_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)

test_file['abs_amount_flag'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)

test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]

test_file = test_file.reset_index().drop('index',1)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int)


# ## Test file served into the model

test_file2 = test_file.copy()

X_test = test_file2[model_cols]

X_test = X_test.reset_index()
X_test = X_test.drop('index',1)
X_test = X_test.fillna(0)

X_test = X_test.fillna(0)

X_test.shape

X_test = X_test.drop_duplicates()
X_test = X_test.reset_index()
X_test = X_test.drop('index',1)

X_test.shape

# ## Model Pickle file import
## TODO Import Pickle file for 1st Model


# In[197]:


filename = 'OakTree_final_model2.sav'

clf = pickle.load(open(filename, 'rb'))

# ## Predictions

# Actual class predictions
rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]

X_test['Predicted_action'] = rf_predictions
X_test['probability_No_pair'] = probability_class_0
X_test['probability_UMB'] = probability_class_1
X_test['probability_UMR'] = probability_class_2
X_test['Predicted_action'].value_counts()

# ## Two Step Modeling

X_test2 = test_file[model_cols_2]
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)
X_test2 = X_test2.fillna(0)

X_test2.shape
X_test2 = X_test2.drop_duplicates()
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)

X_test2.shape

## TODO Import MOdel2 as per the two step modelling process

filename2 = 'OakTree_final_model2_step_two.sav'
clf2 = pickle.load(open(filename2, 'rb'))

# Actual class predictions
rf_predictions2 = clf2.predict(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))

# Probabilities for each class
rf_probs2 = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_0_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

X_test2['Predicted_action_2'] = rf_predictions2
X_test2['probability_No_pair_2'] = probability_class_0_two
X_test2['probability_UMB_2'] = probability_class_1_two

X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)


# In[198]:


#Changes made on 25-11-2020.
filepaths_X_test = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\X_Test_for_Pratik_setup_' + setup_code + '_date_' + str(date_i) + '_2_after_changes.csv'
X_test.to_csv(filepaths_X_test)

# ## New Aggregation
X_test['Tolerance_level'] = np.abs(X_test['probability_UMB_2'] - X_test['probability_No_pair_2'])
b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# ## UMR segregation
umr_ids_0 = umr_seg(X_test)


# In[199]:


len(umr_ids_0)


# In[200]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')].shape


# In[201]:


final_umr_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')]


# In[202]:


final_umr_table_Side0_count_ge_1 = final_umr_table['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index()
final_umr_table_Side1_count_ge_1 = final_umr_table['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index()

duplicate_ids_final_umr_table_Side0 = final_umr_table_Side0_count_ge_1[final_umr_table_Side0_count_ge_1['SideB.ViewData.Side0_UniqueIds'] > 1]['index'].unique()
duplicate_ids_final_umr_table_Side1 = final_umr_table_Side1_count_ge_1[final_umr_table_Side1_count_ge_1['SideA.ViewData.Side1_UniqueIds'] > 1]['index'].unique()


# In[203]:


if(len(duplicate_ids_final_umr_table_Side0) != 0):
    final_umr_table_duplicates_Side0 = final_umr_table[final_umr_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umr_table_Side0)]
    final_umr_table_duplicates_Side0 = final_umr_table_duplicates_Side0[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]#,'probability_UMT']]
    final_umr_table = final_umr_table[~final_umr_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umr_table_Side0)]
    final_umr_table_side0_ids = list(set(final_umr_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umr_ids_to_remove_from_final_open_table = final_umr_table_side0_ids + list(duplicate_ids_final_umr_table_Side0)
    
else:
    final_umr_table_duplicates_Side0 = pd.DataFrame()
    final_umr_table_side0_ids = list(set(final_umr_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umr_ids_to_remove_from_final_open_table = final_umr_table_side0_ids


# In[204]:


if(len(duplicate_ids_final_umr_table_Side1) != 0):
    final_umr_table_duplicates_Side1 = final_umr_table[final_umr_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umr_table_Side1)]
    final_umr_table_duplicates_Side1 = final_umr_table_duplicates_Side1[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]#,'probability_UMT']]
    final_umr_table = final_umr_table[~final_umr_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umr_table_Side1)]
    final_umr_table_side1_ids = list(set(final_umr_table['SideA.ViewData.Side1_UniqueIds'])) 
    side1_umr_ids_to_remove_from_final_open_table = final_umr_table_side1_ids + list(duplicate_ids_final_umr_table_Side1)

else:
    final_umr_table_duplicates_Side1 = pd.DataFrame()
    final_umr_table_side1_ids = list(set(final_umr_table['SideA.ViewData.Side1_UniqueIds']))
    side1_umr_ids_to_remove_from_final_open_table = final_umr_table_side1_ids



len(side1_umr_ids_to_remove_from_final_open_table)



# ## 1st Prediction Table for One to One UMR
    

final_umr_table = final_umr_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]


#Change added on 12-01-2021 as per Pratik. This code is for adding UMT table in oaktree, almost similar to what we did in Weiss 125
#Begin changes added on 12-01-2021
def umt_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umt_table = b_count2[(b_count2['Predicted_action']=='UMT_One_to_One')  & (b_count2['count']==1) & (b_count2['len']<=3)]
    return umt_table['SideB.ViewData.Side0_UniqueIds'].values

X_test.loc[(X_test['Predicted_action'] == 'UMB_One_to_One') & (X_test['Amount_diff_2'] > -0.05) & (X_test['Amount_diff_2'] < 0.05),'Predicted_action'] = 'UMT_One_to_One'
#X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_seg(X_test)) & (X_test['Predicted_action']=='UMT_One_to_One')]

umt_ids_0 = umt_seg(X_test)

final_umt_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_ids_0) & (X_test['Predicted_action']=='UMT_One_to_One')]

final_umt_table_Side0_count_ge_1 = final_umt_table['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index()
final_umt_table_Side1_count_ge_1 = final_umt_table['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index()

duplicate_ids_final_umt_table_Side0 = final_umt_table_Side0_count_ge_1[final_umt_table_Side0_count_ge_1['SideB.ViewData.Side0_UniqueIds'] > 1]['index'].unique()
duplicate_ids_final_umt_table_Side1 = final_umt_table_Side1_count_ge_1[final_umt_table_Side1_count_ge_1['SideA.ViewData.Side1_UniqueIds'] > 1]['index'].unique()

if(len(duplicate_ids_final_umt_table_Side0) != 0):
    final_umt_table_duplicates_Side0 = final_umt_table[final_umt_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umt_table_Side0)]
    final_umt_table_duplicates_Side0 = final_umt_table_duplicates_Side0[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]
    final_umt_table = final_umt_table[~final_umt_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umt_table_Side0)]
    final_umt_table_side0_ids = list(set(final_umt_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umt_ids_to_remove_from_final_open_table = final_umt_table_side0_ids + list(duplicate_ids_final_umt_table_Side0)
    
else:
    final_umt_table_duplicates_Side0 = pd.DataFrame()
    final_umt_table_side0_ids = list(set(final_umt_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umt_ids_to_remove_from_final_open_table = final_umt_table_side0_ids
    
if(len(duplicate_ids_final_umt_table_Side1) != 0):
    final_umt_table_duplicates_Side1 = final_umt_table[final_umt_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umt_table_Side1)]
    final_umt_table_duplicates_Side1 = final_umt_table_duplicates_Side1[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]
    final_umt_table = final_umt_table[~final_umt_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umt_table_Side1)]
    final_umt_table_side1_ids = list(set(final_umt_table['SideA.ViewData.Side1_UniqueIds'])) 
    side1_umt_ids_to_remove_from_final_open_table = final_umt_table_side1_ids + list(duplicate_ids_final_umt_table_Side1)

else:
    final_umt_table_duplicates_Side1 = pd.DataFrame()
    final_umt_table_side1_ids = list(set(final_umt_table['SideA.ViewData.Side1_UniqueIds']))
    side1_umt_ids_to_remove_from_final_open_table = final_umt_table_side1_ids


final_umt_table = final_umt_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]

umt_ids_a_side = final_umt_table['SideA.ViewData.Side1_UniqueIds'].unique()
umt_ids_b_side = final_umt_table['SideB.ViewData.Side0_UniqueIds'].unique()

X_test = X_test[~(X_test['SideA.ViewData.Side1_UniqueIds'].isin(umt_ids_a_side))]
X_test = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_ids_b_side))]

#End changes added on 12-01-2021




# ## No-Pair segregation

no_pair_ids_b_side, no_pair_ids_a_side = no_pair_seg(X_test)

X_test[(X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]['Predicted_action_2'].value_counts()

X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()

X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]['Predicted_action_2'].value_counts()

final_open_table = X_test[(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]


# In[207]:


final_open_table = final_open_table[~final_open_table['SideA.ViewData.Side1_UniqueIds'].isin(side1_umr_ids_to_remove_from_final_open_table)]
final_open_table = final_open_table[~final_open_table['SideB.ViewData.Side0_UniqueIds'].isin(side0_umr_ids_to_remove_from_final_open_table)]

final_open_table = final_open_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]

final_open_table['probability_UMR'] = 0.00010
final_open_table = final_open_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


b_side_open_table = final_open_table.groupby('SideB.ViewData.Side0_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()
a_side_open_table = final_open_table.groupby('SideA.ViewData.Side1_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()

a_side_open_table = a_side_open_table[a_side_open_table['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]
b_side_open_table = b_side_open_table[b_side_open_table['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)]

b_side_open_table = b_side_open_table.reset_index().drop('index',1)
a_side_open_table = a_side_open_table.reset_index().drop('index',1)

final_no_pair_table = pd.concat([a_side_open_table,b_side_open_table], axis=0)
final_no_pair_table = final_no_pair_table.reset_index().drop('index',1)


# In[208]:


final_no_pair_table


# In[209]:


#
#final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.BreakID_A_side']].drop_duplicates(), on = 'SideA.ViewData.Side1_UniqueIds', how='left')
#final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideB.ViewData.Side0_UniqueIds','SideB.ViewData.BreakID_B_side']].drop_duplicates(), on = 'SideB.ViewData.Side0_UniqueIds', how='left')
#

final_no_pair_table = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_no_pair_table)
final_no_pair_table_copy = final_no_pair_table.copy()

final_no_pair_table_copy['ViewData.Side0_UniqueIds'] = final_no_pair_table_copy['ViewData.Side0_UniqueIds'].astype(str)
final_no_pair_table_copy['ViewData.Side1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side1_UniqueIds'].astype(str)


# In[210]:


final_no_pair_table_copy


# In[211]:



final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side0_UniqueIds']=='None','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side1_UniqueIds']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side1_UniqueIds']=='None','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side0_UniqueIds']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side0_UniqueIds']=='nan','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side1_UniqueIds']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side1_UniqueIds']=='nan','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side0_UniqueIds']

del final_no_pair_table_copy['ViewData.Side0_UniqueIds']
del final_no_pair_table_copy['ViewData.Side1_UniqueIds']


# In[212]:


#OTM,MTO,OTO code begin

# ## Remove Open Ids

umr_ids_a_side = final_umr_table['SideA.ViewData.Side1_UniqueIds'].unique()
umr_ids_b_side = final_umr_table['SideB.ViewData.Side0_UniqueIds'].unique()


### Remove Open IDs from X_test

X_test = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test = X_test[~(X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

## Remove UMR IDs from X_test

X_test = X_test[~(X_test['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
X_test = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]


### Remove Open IDs from X_test_left

X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

## Remove UMR IDs from X_test_left

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]


X_test_left = X_test_left.reset_index().drop('index',1)

# ## One to One UMB segregation

X_test_left['Predicted_action_2'].value_counts()

### IDs left after removing UMR ids from 0 and 1 side

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]

X_test_left.shape
X_test_left['Predicted_action_2'].value_counts()

X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
X_test_left = X_test_left.reset_index().drop('index',1)

for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():
    umb_ids_1 = X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.Side1_UniqueIds'].unique()

X_test_left['SideB.ViewData.Side0_UniqueIds'].value_counts()


# In[213]:


# # Before changes on 17-12-2020
# # ## UMR One to Many and Many to One 

# # ### One to Many
# cliff_for_loop = 16

# threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
# threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
# threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()

# exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
# exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()

# many_ids_1 = []
# one_id_0 = []
# amount_array =[]
# for key in X_test[~((X_test['SideB.ViewData.Side0_UniqueIds'].isin(exceptions_0_umb_ids)) | (X_test['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])))]['SideB.ViewData.Side0_UniqueIds'].unique():
#     print(key)
    
#     if key in threshold_0_umb:

#         values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
#         net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

#         #memo = dict()
#         #print(values)
#         #print(net_sum)

#         if subSum(values,net_sum) == []: 
#             #print("There are no valid subsets.")
#             amount_array = ['NULL']
#         else:
#             amount_array = subSum(values,net_sum)

#             id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
#             id0_unique = key       

#             if len(id1_aggregation)>1: 
#                 many_ids_1.append(id1_aggregation)
#                 one_id_0.append(id0_unique)
#             else:
#                 pass
            
#     else:
#         values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.B-P Net Amount'].values
#         net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

#         #memo = dict()
#         #print(values)
#         #print(net_sum)

#         if subSum(values,net_sum) == []: 
#             #print("There are no valid subsets.")
#             amount_array = ['NULL']
#         else:
#             amount_array = subSum(values,net_sum)

#             id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
#             id0_unique = key       

#             if len(id1_aggregation)>1: 
#                 many_ids_1.append(id1_aggregation)
#                 one_id_0.append(id0_unique)
#             else:
#                 pass

# umr_otm_table = pd.DataFrame(one_id_0)

# if(umr_otm_table.empty == False):
#     umr_otm_table.columns = ['SideB.ViewData.Side0_UniqueIds']
#     umr_otm_table['SideA.ViewData.Side1_UniqueIds'] =many_ids_1 
# else:
#     temp_umr_otm_table_message = 'No One to many found'
#     print(temp_umr_otm_table_message)


# # ## Removing duplicate IDs from side 1

# if(len(many_ids_1) == 0):
#     unique_many_ids_1 = ['None']
# else:
#     unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))

# dup_ids_0 = []
# for i in unique_many_ids_1:
#     count =0
#     for j in many_ids_1:
#         if i in j:
#             count = count+1
#             if count==2:
#                 dup_ids_0.append(i)
#                 break             
            
# dup_array_0 = []
# for i in many_ids_1:
#     #print(i)
#     if any(x in dup_ids_0 for x in i):
#         dup_array_0.append(i)
        

# ### Converting array to list
# dup_array_0_list = []
# for i in dup_array_0:
#     dup_array_0_list.append(list(i))
    
# many_ids_1_list =[] 
# for j in many_ids_1:
#     many_ids_1_list.append(list(j))
    
    
# filtered_otm = [i for i in many_ids_1_list if not i in dup_array_0_list]

# one_id_0_final = []
# for i, j in zip(many_ids_1_list, one_id_0):
#     if i in filtered_otm:
#         one_id_0_final.append(j) 

# umr_otm_table = umr_otm_table[umr_otm_table['SideB.ViewData.Side0_UniqueIds'].isin(one_id_0_final)]

# filtered_otm_flat = [item for sublist in filtered_otm for item in sublist]


# # ## Including UMR double count into OTM
# umr_double_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
# umr_double_count = umr_double_count[(umr_double_count['Predicted_action']=='UMR_One_to_One') & (umr_double_count['count']==2)]

# umr_double_count_left = umr_double_count[~umr_double_count['SideB.ViewData.Side0_UniqueIds'].isin(umr_otm_table['SideB.ViewData.Side0_UniqueIds'].unique())]

# pb_ids_otm_left = []
# acc_id_single = []

# for key in umr_double_count_left['SideB.ViewData.Side0_UniqueIds'].unique():
#     acc_amount = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
#     pb_ids_otm = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & ((X_test['SideB.ViewData.Accounting Net Amount']==X_test['SideA.ViewData.B-P Net Amount']) | (X_test['SideB.ViewData.Accounting Net Amount']== (-1)*X_test['SideA.ViewData.B-P Net Amount']))]['SideA.ViewData.Side1_UniqueIds'].values
#     pb_ids_otm_left.append(pb_ids_otm)
#     acc_id_single.append(key)

# umr_otm_table_double_count = pd.DataFrame(acc_id_single)
# if(umr_otm_table_double_count.shape[0] != 0):
#     umr_otm_table_double_count.columns = ['SideB.ViewData.Side0_UniqueIds']

#     umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'] = pb_ids_otm_left

#     umr_otm_table_final = pd.concat([umr_otm_table, umr_otm_table_double_count], axis=0)
# else:
#     umr_otm_table_final = umr_otm_table.copy()
    
# if(umr_otm_table_final.empty == False):
#     umr_otm_table_final = umr_otm_table_final.reset_index().drop('index',1)


# In[219]:


# After changes on 17-12-2020
cliff_for_loop = 16

threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()

exceptions_0_umb = X_test[X_test['Predicted_action_2']!='UMR_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()

def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total, atol=0.02).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
               

#null_value ='No'
many_ids_1 = []
one_id_0 = []
amount_array =[]

loop_count = 0
for key in exceptions_0_umb['index'].unique():
#for key in ['553_1251128974_Advent Geneva','409_1251128952_Advent Geneva']:
    #print(key)
    loop_count = loop_count + 1
    print(loop_count)
    if key in exceptions_0_umb_ids:
        sort_data = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']!='UMR_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
        sort_data = sort_data.reset_index().drop('index',1)
#        Change made on 17-12-2020. As per Pratik, we will take the first 10 values, as oaktree is smaller data. For weiss, we take 8 as weiss is too big and it takes too much time with value of 15
        sort_data = sort_data.loc[0:12,:]
#         sort_data = sort_data.loc[0:15,:]
        sort_data = sort_data.drop_duplicates(subset=['SideA.ViewData.B-P Net Amount'])
        sort_data = sort_data.reset_index().drop('index',1)
        #print(sort_data)

        values =  sort_data['SideA.ViewData.B-P Net Amount'].values
        net_sum = sort_data['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = sort_data[(sort_data['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (sort_data['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass
            
    else:
        
        sort_data2 = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']!='UMR_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
        sort_data2 = sort_data2.reset_index().drop('index',1)

#        Change made on 08-12-2020. As per Pratik, we will take the first 10 values, not 15 in order to not overpredict otm and mto umrs
        sort_data2 = sort_data2.loc[0:12,:]
#         sort_data2 = sort_data2.loc[0:8,:]
        
        sort_data2 = sort_data2.drop_duplicates(subset=['SideA.ViewData.B-P Net Amount'])
        sort_data2 = sort_data2.reset_index().drop('index',1)
        

        values =  sort_data2['SideA.ViewData.B-P Net Amount'].values
        net_sum = sort_data2['SideB.ViewData.Accounting Net Amount'].max()
        #values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
        #net_sum = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key)& (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = sort_data2[(sort_data2['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (sort_data2['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass

umr_otm_table = pd.DataFrame(one_id_0)

#End change


if umr_otm_table.empty == False:
    umr_otm_table.columns = ['SideB.ViewData.Side0_UniqueIds']
    umr_otm_table['SideA.ViewData.Side1_UniqueIds'] =many_ids_1
else:
    print('No One to Many found')

# ## Removing duplicate IDs from side 1

if len(many_ids_1)!=0:
    unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
else:
    unique_many_ids_1 = np.array(['None'])

dup_ids_0 = []
for i in unique_many_ids_1:
    count =0
    for j in many_ids_1:
        if i in j:
            count = count+1
            if count==2:
                dup_ids_0.append(i)
                break             
            
dup_array_0 = []
for i in many_ids_1:
    #print(i)
    if any(x in dup_ids_0 for x in i):
        dup_array_0.append(i)
        

### Converting array to list
dup_array_0_list = []
for i in dup_array_0:
    dup_array_0_list.append(list(i))
    
many_ids_1_list =[] 
for j in many_ids_1:
    many_ids_1_list.append(list(j))
    
    
filtered_otm = [i for i in many_ids_1_list if not i in dup_array_0_list]

one_id_0_final = []
for i, j in zip(many_ids_1_list, one_id_0):
    if i in filtered_otm:
        one_id_0_final.append(j) 

#meo[meo['ViewData.Side0_UniqueIds'] =='162_153156748_Advent Geneva']

if len(one_id_0_final)!=0:
    #unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
    one_id_0_final = one_id_0_final
else:
    one_id_0_final = np.array(['None'])
    
if umr_otm_table.empty == False:    
    umr_otm_table = umr_otm_table[umr_otm_table['SideB.ViewData.Side0_UniqueIds'].isin(one_id_0_final)]

filtered_otm_flat = [item for sublist in filtered_otm for item in sublist]

# ## Including UMR double count into OTM

umr_double_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
umr_double_count = umr_double_count[(umr_double_count['Predicted_action']=='UMR_One_to_One') & (umr_double_count['count']==2)]


if umr_otm_table.empty == False:
    sideb_unique = umr_otm_table['SideB.ViewData.Side0_UniqueIds'].unique()
else:
    sideb_unique =['None']

if umr_double_count.empty == False:

    umr_double_count_left = umr_double_count[~umr_double_count['SideB.ViewData.Side0_UniqueIds'].isin(sideb_unique)]
else:
    umr_double_count_left = pd.DataFrame()

pb_ids_otm_left = []
acc_id_single = []

if(umr_double_count_left.shape[0] != 0):
    for key in umr_double_count_left['SideB.ViewData.Side0_UniqueIds'].unique():
        acc_amount = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
        pb_ids_otm = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & ((X_test['SideB.ViewData.Accounting Net Amount']==X_test['SideA.ViewData.B-P Net Amount']) | (X_test['SideB.ViewData.Accounting Net Amount']== (-1)*X_test['SideA.ViewData.B-P Net Amount']))]['SideA.ViewData.Side1_UniqueIds'].values
        pb_ids_otm_left.append(pb_ids_otm)
        acc_id_single.append(key)
    
    umr_otm_table_double_count = pd.DataFrame(acc_id_single)
    umr_otm_table_double_count.columns = ['SideB.ViewData.Side0_UniqueIds']
    
    umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'] = pb_ids_otm_left
else:
    umr_otm_table_double_count = pd.DataFrame()

if(umr_otm_table_double_count.shape[0] != 0):
    umr_otm_table_double_count['len_side1'] = umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'].apply(lambda x : len(x)) 
    umr_otm_table_double_count = umr_otm_table_double_count[umr_otm_table_double_count['len_side1'] != 0]
    umr_otm_table_double_count = umr_otm_table_double_count.reset_index().drop('index',1)
    umr_otm_table_double_count = umr_otm_table_double_count.drop('len_side1',1)    
    
umr_otm_table_final = pd.concat([umr_otm_table, umr_otm_table_double_count], axis=0)

umr_otm_table_final = umr_otm_table_final.reset_index().drop('index',1)




# In[224]:


# # Before changes on 17-12-2020
# # ### Many to One

# cliff_for_loop = 16

# threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
# threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
# threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()

# exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
# exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()

# def subSum(numbers,total):
#     for length in range(1, 4):
#         if len(numbers) < length or length < 1:
#             return []
#         for index,number in enumerate(numbers):
#             if length == 1 and np.isclose(number, total,atol=0.25).any():
#                 return [number]
#             subset = subSum(numbers[index+1:],total-number)
#             if subset: 
#                 return [number] + subset
#         return []

# many_ids_0 = []
# one_id_1 = []
# amount_array2 =[]
# for key in X_test[~((X_test['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)) |(X_test['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]['SideA.ViewData.Side1_UniqueIds'].unique():
#     #if key not in ['1174_379879573_State Street','201_379823765_State Street']:
#     print(key)
#     if key in threshold_1_umb:

#         values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].values
#         net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

#         #memo = dict()

#         if subSum(values2,net_sum2) == []: 
#             amount_array2 =[]
#             #print("There are no valid subsets.")

#         else:
#             amount_array2 = subSum(values2,net_sum2)

#             id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
#             id1_unique = key       

#             if len(id0_aggregation)>1: 
#                 many_ids_0.append(id0_aggregation)
#                 one_id_1.append(id1_unique)
#             else:
#                 pass

#     else:
#         values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Accounting Net Amount'].values
#         net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

#         #memo = dict()

#         if subSum(values2,net_sum2) == []: 
#             amount_array2 =[]
#             #print("There are no valid subsets.")

#         else:
#             amount_array2 = subSum(values2,net_sum2)

#             id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
#             id1_unique = key       

#             if len(id0_aggregation)>1: 
#                 many_ids_0.append(id0_aggregation)
#                 one_id_1.append(id1_unique)
#             else:
#                 pass

# umr_mto_table = pd.DataFrame(one_id_1)
# if(umr_mto_table.empty == False):
#     umr_mto_table.columns = ['SideA.ViewData.Side1_UniqueIds']
#     umr_mto_table['SideB.ViewData.Side0_UniqueIds'] =many_ids_0 
# #    umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]
# #    for i in range(0,umr_mto_table.shape[0]):
# #        umr_mto_table['BreakID_Side0'].iloc[i] = list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_mto_table['SideB.ViewData.Side0_UniqueIds'].values[i])]['ViewData.BreakID'])
#         #        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])

# else:
#     temp_umr_mto_table_message = 'No Many to One found'
#     print(temp_umr_mto_table_message)

# # ## Removing duplicate IDs from side 0

# if(len(many_ids_0) == 0):
#     unique_many_ids_0 = ['None']
# else:
#     unique_many_ids_0 = np.unique(np.concatenate(many_ids_0))

# dup_ids_1 = []
# for i in unique_many_ids_0:
#     count =0
#     for j in many_ids_0:
#         if i in j:
#             count = count+1
#             if count==2:
#                 dup_ids_1.append(i)
#                 break             
            
# dup_array_1 = []
# for i in many_ids_0:
#     #print(i)
#     if any(x in dup_ids_1 for x in i):
#         dup_array_1.append(i)
        

# ### Converting array to list
# dup_array_1_list = []
# for i in dup_array_1:
#     dup_array_1_list.append(list(i))
    
# many_ids_0_list =[] 
# for j in many_ids_0:
#     many_ids_0_list.append(list(j))
    
    
# filtered_mto = [i for i in many_ids_0_list if not i in dup_array_1_list]

# one_id_1_final = []
# for i, j in zip(many_ids_0_list, one_id_1):
#     if i in filtered_mto:
#         one_id_1_final.append(j) 


# #pd.set_option('max_columns',50)
# if(umr_mto_table.empty == False):
#     umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]
#     umr_mto_table['BreakID_Side0'] = umr_mto_table.apply(lambda x: list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_otm_table_final['SideB.ViewData.Side0_UniqueIds'])]['ViewData.BreakID']), axis=1)
#     for i in range(0,umr_mto_table.shape[0]):
#         umr_mto_table['BreakID_Side0'].iloc[i] = list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_mto_table['SideB.ViewData.Side0_UniqueIds'].values[i])]['ViewData.BreakID'])#        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])

# else:
#     temp_umr_mto_table_message = 'No Many to One found'
#     print(temp_umr_mto_table_message)


# filtered_mto_flat = [item for sublist in filtered_mto for item in sublist]


# In[225]:


cliff_for_loop = 17

threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()

exceptions_1_umb = X_test[X_test['Predicted_action_2']!='UMR_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()

def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total, atol=0.02).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
               
#null_value ='No'
many_ids_0 = []
one_id_1 = []
amount_array_2 =[]

loop_count = 0
for key in exceptions_1_umb['index'].unique():
#for key in ['553_1251128974_Advent Geneva','409_1251128952_Advent Geneva']:
    #print(key)
    loop_count = loop_count + 1
    print(loop_count)
    if key in exceptions_1_umb_ids:
        sort_data = X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']!='UMR_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
        sort_data = sort_data.reset_index().drop('index',1)
        
#        Change made on 08-12-2020. As per Pratik, we will take the first 10 values, as oaktree is smaller data. For weiss, we take 8 as weiss is too big and it takes too much time with value of 15
        sort_data = sort_data.loc[0:12,:]
#         sort_data = sort_data.loc[0:15,:]
        sort_data = sort_data.drop_duplicates(subset=['SideB.ViewData.Accounting Net Amount'])
        sort_data = sort_data.reset_index().drop('index',1)
        #print(sort_data)

        values2 =  sort_data['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = sort_data['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values2,net_sum2) == []: 
            #print("There are no valid subsets.")
            amount_array2 = ['NULL']
        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = sort_data[(sort_data['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (sort_data['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass
            
    else:
        
        sort_data2 = X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']!='UMR_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
        sort_data2 = sort_data2.reset_index().drop('index',1)
        
#        Change made on 08-12-2020. As per Pratik, we will take the first 10 values, not 15 in order to not overpredict otm and mto umrs
        sort_data2 = sort_data2.loc[0:12,:]
#         sort_data2 = sort_data2.loc[0:8,:]
        sort_data2 = sort_data2.drop_duplicates(subset=['SideB.ViewData.Accounting Net Amount'])
        sort_data2 = sort_data2.reset_index().drop('index',1)
        

        values2 =  sort_data2['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = sort_data2['SideA.ViewData.B-P Net Amount'].max()
        #values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
        #net_sum = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key)& (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)


        if subSum(values2,net_sum2) == []: 
            #print("There are no valid subsets.")
            amount_array2 = ['NULL']
        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = sort_data2[(sort_data2['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (sort_data2['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass

umr_mto_table = pd.DataFrame(one_id_1)

#End change

if umr_mto_table.empty == False:
    umr_mto_table.columns = ['SideA.ViewData.Side1_UniqueIds']
    umr_mto_table['SideB.ViewData.Side0_UniqueIds'] =many_ids_0
else:
    print('No Many to One found')

# ## Removing duplicate IDs from side 0

if len(many_ids_0)!=0:
    unique_many_ids_0 = np.unique(np.concatenate(many_ids_0))
else:
    unique_many_ids_0 = np.array(['None'])

dup_ids_1 = []
for i in unique_many_ids_0:
    count =0
    for j in many_ids_0:
        if i in j:
            count = count+1
            if count==2:
                dup_ids_1.append(i)
                break             
            
dup_array_1 = []
for i in many_ids_0:
    #print(i)
    if any(x in dup_ids_1 for x in i):
        dup_array_1.append(i)
 
# Converting array to list
dup_array_1_list = []
for i in dup_array_1:
    dup_array_1_list.append(list(i))
    
many_ids_0_list =[] 
for j in many_ids_0:
    many_ids_0_list.append(list(j))
    
    
filtered_mto = [i for i in many_ids_0_list if not i in dup_array_1_list]

one_id_1_final = []
for i, j in zip(many_ids_0_list, one_id_1):
    if i in filtered_mto:
        one_id_1_final.append(j) 

if len(one_id_1_final)!=0:
    #unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
    one_id_1_final = one_id_1_final
else:
    one_id_1_final = np.array(['None'])

#umr_otm_table = umr_otm_table[umr_otm_table['SideB.ViewData.Side0_UniqueIds'].isin(one_id_0_final)]
#umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]
#umr_mto_table = umr_mto_table.reset_index().drop('index',1)


if(umr_mto_table.empty == False):
    umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]
    umr_mto_table = umr_mto_table.reset_index().drop('index',1)
#TODO : Revisit this code later - start here
#    umr_mto_table['BreakID_Side0'] = umr_mto_table.apply(lambda x: list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_mto_table['SideB.ViewData.Side0_UniqueIds'])]['ViewData.BreakID']), axis=1)
#    for i in range(0,umr_mto_table.shape[0]):
#        umr_mto_table['BreakID_Side0'].iloc[i] = list(meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(umr_mto_table['SideB.ViewData.Side0_UniqueIds'].values[i])]['ViewData.BreakID'])#        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])
#TODO : Revisit this code later - end here
else:
    temp_umr_mto_table_message = 'No Many to One found'
    print(temp_umr_mto_table_message)




filtered_mto_flat = [item for sublist in filtered_mto for item in sublist]


# In[226]:


umr_mto_table


# In[227]:




# ## Removing all the OTM and MTO Ids

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_mto_flat))]

X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(one_id_1)))]

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_otm_flat))]
X_test_left2 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(list(one_id_0)))]

X_test_left2 = X_test_left2.reset_index().drop('index',1)

# ## UMB one to one (final)

X_test_umb = X_test_left2[X_test_left2['Predicted_action_2']=='UMB_One_to_One']
X_test_umb = X_test_umb.reset_index().drop('index',1)

one_side_unique_umb_ids = one_to_one_umb(X_test_umb)

final_oto_umb_table = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]

final_oto_umb_table = final_oto_umb_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]

final_oto_umb_table['probability_UMR'] = 0.00010
final_oto_umb_table = final_oto_umb_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})

# ## Removing IDs from OTO UMB

X_test_left3 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(final_oto_umb_table['SideB.ViewData.Side0_UniqueIds']))]
X_test_left3 = X_test_left3[~(X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(final_oto_umb_table['SideA.ViewData.Side1_UniqueIds']))]


X_test_left3 = X_test_left3.reset_index().drop('index',1)

# ## UMB One to Many and Many to One

## Total IDs 

X_test['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test['SideA.ViewData.Side1_UniqueIds'].nunique()
X_test_left3['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left3['SideA.ViewData.Side1_UniqueIds'].nunique()

open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)

X_test_left3[~X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)]

X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]

X_test_left4 = X_test_left4.reset_index().drop('index',1)
X_test_left4['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left4['SideA.ViewData.Side1_UniqueIds'].nunique()

# ## Many to Many new

#MANY TO MANY NEW
rr2 = X_test[X_test['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
rr2['SideA.ViewData.Side1_UniqueIds'] = rr2['SideA.ViewData.Side1_UniqueIds'].apply(tuple)

rr2.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()

rr2 = X_test_left4[X_test_left4['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()

acc_amount = X_test[X_test['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideB.ViewData.Accounting Net Amount'].max().reset_index()
pb_amount_sum =  X_test[X_test['Predicted_action_2']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.B-P Net Amount'].sum().reset_index()

rr3 = pd.merge(rr2, acc_amount, on='SideB.ViewData.Side0_UniqueIds', how='left')
rr4 = pd.merge(rr3, pb_amount_sum, on='SideB.ViewData.Side0_UniqueIds', how='left')

rr4['SideA.ViewData.Side1_UniqueIds'] = rr4['SideA.ViewData.Side1_UniqueIds'].apply(tuple)

rr5 = rr4.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()

rr6 = pd.merge(rr5, rr4.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Accounting Net Amount'].sum().reset_index(), on='SideA.ViewData.Side1_UniqueIds', how='left')

rr7 = pd.merge(rr6,rr4[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.B-P Net Amount']].drop_duplicates(), on='SideA.ViewData.Side1_UniqueIds',how='left')

rr7['diff'] = rr7['SideB.ViewData.Accounting Net Amount'] - rr7['SideA.ViewData.B-P Net Amount']

rr7['pb_len'] = rr7['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
rr7['acc_len'] = rr7['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: len(x))

rr8 = rr7[~((rr7['pb_len']==1)|(rr7['acc_len']==1))]
rr8 = rr8.reset_index().drop('index',1)


# In[228]:


def get_BreakID_from_list_of_Side_01_UniqueIds(fun_str_list_Side_01_UniqueIds, fun_meo_df, fun_side_0_or_1):
    list_BreakID_corresponding_to_Side_01_UniqueIds = []
    print(fun_str_list_Side_01_UniqueIds)
    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
        if(fun_side_0_or_1 == 0):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
        elif(fun_side_0_or_1 == 1):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    return(list_BreakID_corresponding_to_Side_01_UniqueIds)


# In[229]:


umr_otm_table_final


# In[230]:


# umr_otm_table_final
#final_otm_table
# final_otm_table_copy = final_otm_table.copy()
if(umr_otm_table_final.shape[0] != 0):
    final_otm_table_copy = umr_otm_table_final.copy()
    #final_otm_table_copy['BreakID_Side0'] = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(list(final_otm_table_copy['SideB.ViewData.Side0_UniqueIds']))]['ViewData.BreakID'].values
    #final_otm_table_copy['BreakID_Side0'] = final_otm_table_copy['BreakID_Side0'].astype(int)
    
    final_otm_table_copy['BreakID_Side0'] = final_otm_table_copy['SideB.ViewData.Side0_UniqueIds'].apply(lambda x : meo_df[meo_df['ViewData.Side0_UniqueIds'] == x]['ViewData.BreakID'].unique())
    
    
    final_otm_table_copy['BreakID_Side1'] = final_otm_table_copy['SideA.ViewData.Side1_UniqueIds'].apply(                                         lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df,                                                                                               fun_side_0_or_1 = 1,                                                                                               fun_str_list_Side_01_UniqueIds = x))
    
    final_otm_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_otm_table_copy)
    #
    final_otm_table_copy['ViewData.Side0_UniqueIds'] = final_otm_table_copy['ViewData.Side0_UniqueIds'].astype(str)
    final_otm_table_copy['ViewData.Side1_UniqueIds'] = final_otm_table_copy['ViewData.Side1_UniqueIds'].astype(str)
     
    final_otm_table_copy_new = pd.merge(final_otm_table_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side0_UniqueIds', how='left')
    final_otm_table_copy_new['Predicted_Status'] = 'UMR'
    final_otm_table_copy_new['Predicted_action'] = 'UMR_One-Many_to_Many-One'
    final_otm_table_copy_new['ML_flag'] = 'ML'
    final_otm_table_copy_new['SetupID'] = setup_code 
    
    filepaths_final_otm_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_otm_table_copy_new.csv'
    final_otm_table_copy_new.to_csv(filepaths_final_otm_table_copy)
    
    change_names_of_final_otm_table_copy_new_mapping_dict = {
                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                'BreakID_Side0' : 'BreakID',
                                                'BreakID_Side1' : 'Final_predicted_break',
                                                'ViewData.Task ID' : 'Task ID',
                                                'ViewData.Task Business Date' : 'Task Business Date',
                                                'ViewData.Source Combination Code' : 'Source Combination Code'
                                            }
    
    
    final_otm_table_copy_new.rename(columns = change_names_of_final_otm_table_copy_new_mapping_dict, inplace = True)
    
    final_otm_table_copy_new['Task Business Date'] = pd.to_datetime(final_otm_table_copy_new['Task Business Date'])
    final_otm_table_copy_new['Task Business Date'] = final_otm_table_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_otm_table_copy_new['Task Business Date'] = pd.to_datetime(final_otm_table_copy_new['Task Business Date'])
    
    
    final_otm_table_copy_new['PredictedComment'] = ''
    
    #Changing data types of columns as follows:
    #Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
    #BreakID, TaskID - int64
    #SetupID - int32
    final_otm_table_copy_new['probability_UMB'] = 0.017
    final_otm_table_copy_new['probability_No_pair'] = 0.017
    final_otm_table_copy_new['probability_UMR'] = 0.95
    final_otm_table_copy_new['probability_UMT'] = 0.017
        
    for i in range(0,final_otm_table_copy_new.shape[0]):
        final_otm_table_copy_new['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
        final_otm_table_copy_new['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
        final_otm_table_copy_new['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(950, 1000))/1000)
        final_otm_table_copy_new['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
    
    
    final_otm_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_otm_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
    
    final_otm_table_copy_new[['Task ID']] = final_otm_table_copy_new[['Task ID']].astype(float)
    final_otm_table_copy_new[['Task ID']] = final_otm_table_copy_new[['Task ID']].astype(np.int64)
    
    final_otm_table_copy_new[['SetupID']] = final_otm_table_copy_new[['SetupID']].astype(int)
    
    #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
    #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)
    
    change_col_names_final_otm_table_copy_new_dict = {
                            'Task ID' : 'TaskID',
                            'Task Business Date' : 'BusinessDate',
                            'Source Combination Code' : 'SourceCombinationCode'
                            }
    final_otm_table_copy_new.rename(columns = change_col_names_final_otm_table_copy_new_dict, inplace = True)
    
    cols_for_database_new = ['Side0_UniqueIds',
     'Side1_UniqueIds',
     'BreakID',
     'Final_predicted_break',
     'Predicted_action',
     'probability_No_pair',
     'probability_UMB',
     'probability_UMR',
     'probability_UMT',
     'TaskID',
     'BusinessDate',
     'PredictedComment',
     'SourceCombinationCode',
     'Predicted_Status',
     'ML_flag',
     'SetupID']
    
    final_otm_table_copy_new_to_write = final_otm_table_copy_new[cols_for_database_new]
    #final_otm_table_copy_new_to_write['BreakID'] = final_otm_table_copy_new_to_write['BreakID'].replace('[','',regex = True).replace(']','',regex = True)
    #final_otm_table_copy_new_to_write['Final_predicted_break'] = final_otm_table_copy_new_to_write['Final_predicted_break'].replace('[','',regex = True).replace(']','',regex = True)
    
    filepaths_final_otm_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_otm_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    final_otm_table_copy_new_to_write.to_csv(filepaths_final_otm_table_copy_new_to_write)

else:
    final_otm_table_copy_new_to_write = pd.DataFrame()
# In[231]:




# In[233]:


meo_df[meo_df['ViewData.Side1_UniqueIds'] == '17_3791152753_BNP Paribas']['ViewData.BreakID']


# In[234]:



# final_mto_table_copy = final_mto_table.copy()
if(umr_mto_table.shape[0] != 0):
    final_mto_table_copy = umr_mto_table.copy()
    #final_mto_table_copy['BreakID_Side1'] = meo_df[meo_df['ViewData.Side1_UniqueIds'] == final_mto_table_copy['SideA.ViewData.Side1_UniqueIds']]['ViewData.BreakID'].unique()
    
    final_mto_table_copy['BreakID_Side1'] = final_mto_table_copy['SideA.ViewData.Side1_UniqueIds'].apply(lambda x : meo_df[meo_df['ViewData.Side1_UniqueIds'] == x]['ViewData.BreakID'].unique())
    
    final_mto_table_copy['BreakID_Side1'] = final_mto_table_copy['BreakID_Side1'].astype(int)
    
    
    final_mto_table_copy['BreakID_Side0'] = final_mto_table_copy['SideB.ViewData.Side0_UniqueIds'].apply(                                         lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df,                                                                                               fun_side_0_or_1 = 0,                                                                                               fun_str_list_Side_01_UniqueIds = x))
    
    
    final_mto_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_mto_table_copy)
    #
    final_mto_table_copy['ViewData.Side0_UniqueIds'] = final_mto_table_copy['ViewData.Side0_UniqueIds'].astype(str)
    final_mto_table_copy['ViewData.Side1_UniqueIds'] = final_mto_table_copy['ViewData.Side1_UniqueIds'].astype(str)
     
    final_mto_table_copy_new = pd.merge(final_mto_table_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
    final_mto_table_copy_new['Predicted_Status'] = 'UMR'
    final_mto_table_copy_new['Predicted_action'] = 'UMR_One-Many_to_Many-One'
    final_mto_table_copy_new['ML_flag'] = 'ML'
    final_mto_table_copy_new['SetupID'] = setup_code 
    
    #filepaths_final_mto_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_mto_table_copy.csv'
    filepaths_final_mto_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mto_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    
    final_mto_table_copy.to_csv(filepaths_final_mto_table_copy)
    
    change_names_of_final_mto_table_copy_new_mapping_dict = {
                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                'BreakID_Side1' : 'BreakID',
                                                'BreakID_Side0' : 'Final_predicted_break',
                                                'ViewData.Task ID' : 'Task ID',
                                                'ViewData.Task Business Date' : 'Task Business Date',
                                                'ViewData.Source Combination Code' : 'Source Combination Code'
                                            }
    
    
    final_mto_table_copy_new.rename(columns = change_names_of_final_mto_table_copy_new_mapping_dict, inplace = True)
    
    final_mto_table_copy_new['Task Business Date'] = pd.to_datetime(final_mto_table_copy_new['Task Business Date'])
    final_mto_table_copy_new['Task Business Date'] = final_mto_table_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_mto_table_copy_new['Task Business Date'] = pd.to_datetime(final_mto_table_copy_new['Task Business Date'])
    
    
    final_mto_table_copy_new['PredictedComment'] = ''
    
    #Changing data types of columns as follows:
    #Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
    #BreakID, TaskID - int64
    #SetupID - int32
    final_mto_table_copy_new['probability_UMB'] = 0.017
    final_mto_table_copy_new['probability_No_pair'] = 0.017
    final_mto_table_copy_new['probability_UMR'] = 0.95
    final_mto_table_copy_new['probability_UMT'] = 0.017
        
    for i in range(0,final_mto_table_copy_new.shape[0]):
        final_mto_table_copy_new['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
        final_mto_table_copy_new['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
        final_mto_table_copy_new['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(950, 1000))/1000)
        final_mto_table_copy_new['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
    
    
    final_mto_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_mto_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
    
    final_mto_table_copy_new[['BreakID', 'Task ID']] = final_mto_table_copy_new[['BreakID', 'Task ID']].astype(float)
    final_mto_table_copy_new[['BreakID', 'Task ID']] = final_mto_table_copy_new[['BreakID', 'Task ID']].astype(np.int64)
    
    final_mto_table_copy_new[['SetupID']] = final_mto_table_copy_new[['SetupID']].astype(int)
    
    #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
    #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)
    
    change_col_names_final_mto_table_copy_new_dict = {
                            'Task ID' : 'TaskID',
                            'Task Business Date' : 'BusinessDate',
                            'Source Combination Code' : 'SourceCombinationCode'
                            }
    final_mto_table_copy_new.rename(columns = change_col_names_final_mto_table_copy_new_dict, inplace = True)
    
    cols_for_database_new = ['Side0_UniqueIds',
     'Side1_UniqueIds',
     'BreakID',
     'Final_predicted_break',
     'Predicted_action',
     'probability_No_pair',
     'probability_UMB',
     'probability_UMR',
     'probability_UMT',
     'TaskID',
     'BusinessDate',
     'PredictedComment',
     'SourceCombinationCode',
     'Predicted_Status',
     'ML_flag',
     'SetupID']
    
    final_mto_table_copy_new_to_write = final_mto_table_copy_new[cols_for_database_new]
    #final_mto_table_copy_new_to_write['BreakID'] = final_mto_table_copy_new_to_write['BreakID'].replace('[','',regex = True).replace(']','',regex = True)
    #final_mto_table_copy_new_to_write['Final_predicted_break'] = final_mto_table_copy_new_to_write['Final_predicted_break'].replace('[','',regex = True).replace(']','',regex = True)
    
    filepaths_final_mto_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mto_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    
    final_mto_table_copy_new_to_write.to_csv(filepaths_final_mto_table_copy_new_to_write)

else:
    final_mto_table_copy_new_to_write = pd.DataFrame()
# In[237]:


final_mto_table_copy_new_to_write


# In[238]:


meo_df[meo_df['ViewData.Side0_UniqueIds'] == '80_3791145344_Advent Geneva']['ViewData.BreakID']


# In[239]:


meo_df[meo_df['ViewData.Side1_UniqueIds'] == '145_3791147265_State Street']['ViewData.BreakID']


# In[240]:


final_oto_umb_table


# In[241]:


final_oto_umb_table = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_oto_umb_table)
#
final_oto_umb_table['ViewData.Side0_UniqueIds'] = final_oto_umb_table['ViewData.Side0_UniqueIds'].astype(str)
final_oto_umb_table['ViewData.Side1_UniqueIds'] = final_oto_umb_table['ViewData.Side1_UniqueIds'].astype(str)
#TODO : Remove SMB ids from oto_umb table 
final_oto_umb_table_new = pd.merge(final_oto_umb_table, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
final_oto_umb_table_new['Predicted_Status'] = 'UMB'
final_oto_umb_table_new['ML_flag'] = 'ML'
final_oto_umb_table_new['SetupID'] = setup_code 


filepaths_final_oto_umb_table_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_oto_umb_table_new_setup_' + setup_code + '_date_' + str(date_i) + '.csv'

final_oto_umb_table_new.to_csv(filepaths_final_oto_umb_table_new)

change_names_of_final_oto_umb_table_new_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID_Side1' : 'BreakID',
                                            'ViewData.BreakID_Side0' : 'Final_predicted_break',
                                            'ViewData.Task ID' : 'Task ID',
                                            'ViewData.Task Business Date' : 'Task Business Date',
                                            'ViewData.Source Combination Code' : 'Source Combination Code'
                                        }


final_oto_umb_table_new.rename(columns = change_names_of_final_oto_umb_table_new_mapping_dict, inplace = True)

final_oto_umb_table_new['Task Business Date'] = pd.to_datetime(final_oto_umb_table_new['Task Business Date'])
final_oto_umb_table_new['Task Business Date'] = final_oto_umb_table_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_oto_umb_table_new['Task Business Date'] = pd.to_datetime(final_oto_umb_table_new['Task Business Date'])


final_oto_umb_table_new['PredictedComment'] = ''

#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32

final_oto_umb_table_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_oto_umb_table_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)

final_oto_umb_table_new[['Task ID']] = final_oto_umb_table_new[['Task ID']].astype(float)
final_oto_umb_table_new[['Task ID']] = final_oto_umb_table_new[['Task ID']].astype(np.int64)

final_oto_umb_table_new[['SetupID']] = final_oto_umb_table_new[['SetupID']].astype(int)

change_col_names_final_oto_umb_table_new_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
final_oto_umb_table_new.rename(columns = change_col_names_final_oto_umb_table_new_dict, inplace = True)

cols_for_database_new = ['Side0_UniqueIds',
 'Side1_UniqueIds',
 'BreakID',
 'Final_predicted_break',
 'Predicted_action',
 'probability_No_pair',
 'probability_UMB',
 'probability_UMR',
 'TaskID',
 'BusinessDate',
 'SourceCombinationCode',
 'Predicted_Status',
 'ML_flag',
 'SetupID']

final_oto_umb_table_new_to_write = final_oto_umb_table_new[cols_for_database_new]

filepaths_final_oto_umb_table_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_oto_umb_table_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_oto_umb_table_new_to_write.to_csv(filepaths_final_oto_umb_table_new_to_write)
#OTM,MTO,OTO code end


# In[243]:


##Fill probability between 80 - 90 for no_pair 
#open_ids_1_last
#open_ids_0_last
no_pair_ids_last = list(open_ids_1_last)
for x in list(open_ids_0_last):
    no_pair_ids_last.append(x)
no_pair_ids_last_df = pd.DataFrame(no_pair_ids_last, columns = ['Side0_1_UniqueIds'])
no_pair_ids_last_df = no_pair_ids_last_df[~no_pair_ids_last_df['Side0_1_UniqueIds'].isin(['None'])]

final_no_pair_table_copy = final_no_pair_table_copy.append([no_pair_ids_df,no_pair_ids_last_df])

#final_no_pair_table_copy = final_no_pair_table_copy.append(no_pair_ids_df)
if(final_otm_table_copy_new_to_write.shape[0] != 0):
    umr_otm_table_Side1_ids = final_otm_table_copy_new_to_write['Side1_UniqueIds']
    umr_otm_table_Side1_ids_replaced = umr_otm_table_Side1_ids.apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'','').replace('\'',''))
    umr_otm_table_Side1_ids_list_series = umr_otm_table_Side1_ids_replaced.apply(lambda x : x.split(','))
    umr_otm_table_Side1_ids_list = []
    for umr_otm_table_Side1_id in umr_otm_table_Side1_ids_list_series:
        umr_otm_table_Side1_ids_list = umr_otm_table_Side1_ids_list + umr_otm_table_Side1_id
    
    umr_otm_table_Side0_ids = final_otm_table_copy_new_to_write['Side0_UniqueIds']
    umr_otm_table_Side0_ids_replaced = umr_otm_table_Side0_ids.apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'','').replace('\'',''))
    umr_otm_table_Side0_ids_list_series = umr_otm_table_Side0_ids_replaced.apply(lambda x : x.split(','))
    umr_otm_table_Side0_ids_list = []
    for umr_otm_table_Side0_id in umr_otm_table_Side0_ids_list_series:
        umr_otm_table_Side0_ids_list = umr_otm_table_Side0_ids_list + umr_otm_table_Side0_id
else:
    umr_otm_table_Side1_ids_list = []
    umr_otm_table_Side0_ids_list = []

if(final_mto_table_copy_new_to_write.shape[0] != 0):
    umr_mto_table_Side1_ids = final_mto_table_copy_new_to_write['Side1_UniqueIds']
    umr_mto_table_Side1_ids_replaced = umr_mto_table_Side1_ids.apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'','').replace('\'',''))
    umr_mto_table_Side1_ids_list_series = umr_mto_table_Side1_ids_replaced.apply(lambda x : x.split(','))
    umr_mto_table_Side1_ids_list = []
    for umr_mto_table_Side1_id in umr_mto_table_Side1_ids_list_series:
        umr_mto_table_Side1_ids_list = umr_mto_table_Side1_ids_list + umr_mto_table_Side1_id
    
    umr_mto_table_Side0_ids = final_mto_table_copy_new_to_write['Side0_UniqueIds']
    umr_mto_table_Side0_ids_replaced = umr_mto_table_Side0_ids.apply(lambda x : x.replace('\n','').replace('[','').replace(']','').replace('\' \'',',').replace('\', \'','').replace('\'',''))
    umr_mto_table_Side0_ids_list_series = umr_mto_table_Side0_ids_replaced.apply(lambda x : x.split(','))
    umr_mto_table_Side0_ids_list = []
    for umr_mto_table_Side0_id in umr_mto_table_Side0_ids_list_series:
        umr_mto_table_Side0_ids_list = umr_mto_table_Side0_ids_list + umr_mto_table_Side0_id
else:
    umr_mto_table_Side1_ids_list = []
    umr_mto_table_Side0_ids_list = []


if(len(umr_otm_table_Side1_ids_list) != 0):
    final_no_pair_table_copy = final_no_pair_table_copy[~(final_no_pair_table_copy['Side0_1_UniqueIds'].isin(umr_otm_table_Side1_ids_list))]

if(len(umr_otm_table_Side0_ids_list) != 0):
    final_no_pair_table_copy = final_no_pair_table_copy[~final_no_pair_table_copy['Side0_1_UniqueIds'].isin(umr_otm_table_Side0_ids_list)]

if(len(umr_mto_table_Side1_ids_list) != 0):
    final_no_pair_table_copy = final_no_pair_table_copy[~final_no_pair_table_copy['Side0_1_UniqueIds'].isin(umr_mto_table_Side1_ids_list)]
if(len(umr_mto_table_Side0_ids_list) != 0):
    final_no_pair_table_copy = final_no_pair_table_copy[~final_no_pair_table_copy['Side0_1_UniqueIds'].isin(umr_mto_table_Side0_ids_list)]


final_no_pair_table_copy = pd.merge(final_no_pair_table_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side1_UniqueIds', how='left')
final_no_pair_table_copy = pd.merge(final_no_pair_table_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side0_UniqueIds', how='left')
#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
final_no_pair_table_copy['Predicted_Status'] = 'OB'
final_no_pair_table_copy['Predicted_action'] = 'No-Pair'
final_no_pair_table_copy['ML_flag'] = 'ML'
final_no_pair_table_copy['SetupID'] = setup_code 


# In[246]:


final_no_pair_table_copy['ViewData.Task ID_x'] = final_no_pair_table_copy['ViewData.Task ID_x'].astype(str)
final_no_pair_table_copy['ViewData.Task ID_y'] = final_no_pair_table_copy['ViewData.Task ID_y'].astype(str)
 
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task ID_x']=='None','Task ID'] = final_no_pair_table_copy['ViewData.Task ID_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task ID_y']=='None','Task ID'] = final_no_pair_table_copy['ViewData.Task ID_x']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task ID_x']=='nan','Task ID'] = final_no_pair_table_copy['ViewData.Task ID_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task ID_y']=='nan','Task ID'] = final_no_pair_table_copy['ViewData.Task ID_x']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task ID_x']=='NaN','Task ID'] = final_no_pair_table_copy['ViewData.Task ID_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task ID_y']=='NaN','Task ID'] = final_no_pair_table_copy['ViewData.Task ID_x']

final_no_pair_table_copy['Task ID'] = final_no_pair_table_copy['Task ID'].replace('\.0','', regex = True)


# In[247]:


final_no_pair_table_copy['ViewData.BreakID_x'] = final_no_pair_table_copy['ViewData.BreakID_x'].astype(str)
final_no_pair_table_copy['ViewData.BreakID_y'] = final_no_pair_table_copy['ViewData.BreakID_y'].astype(str)
 
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.BreakID_x']=='None','BreakID'] = final_no_pair_table_copy['ViewData.BreakID_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.BreakID_y']=='None','BreakID'] = final_no_pair_table_copy['ViewData.BreakID_x']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.BreakID_x']=='nan','BreakID'] = final_no_pair_table_copy['ViewData.BreakID_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.BreakID_y']=='nan','BreakID'] = final_no_pair_table_copy['ViewData.BreakID_x']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.BreakID_x']=='NaN','BreakID'] = final_no_pair_table_copy['ViewData.BreakID_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.BreakID_y']=='NaN','BreakID'] = final_no_pair_table_copy['ViewData.BreakID_x']

final_no_pair_table_copy['BreakID'] = final_no_pair_table_copy['BreakID'].replace('\.0','', regex = True)


# In[248]:


final_no_pair_table_copy['ViewData.Task Business Date_x'] = final_no_pair_table_copy['ViewData.Task Business Date_x'].astype(str)
final_no_pair_table_copy['ViewData.Task Business Date_y'] = final_no_pair_table_copy['ViewData.Task Business Date_y'].astype(str)
 
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_x']=='None','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_y']=='None','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_x']


# In[249]:


final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_x']=='nan','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_y']=='nan','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_x']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_x']=='NaN','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_y']=='NaN','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_x']


# In[250]:




final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_x']=='NaT','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Task Business Date_y']=='NaT','Task Business Date'] = final_no_pair_table_copy['ViewData.Task Business Date_x']

final_no_pair_table_copy['ViewData.Source Combination Code_x'] = final_no_pair_table_copy['ViewData.Source Combination Code_x'].astype(str)
final_no_pair_table_copy['ViewData.Source Combination Code_y'] = final_no_pair_table_copy['ViewData.Source Combination Code_y'].astype(str)
 
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Source Combination Code_x']=='None','Source Combination Code'] = final_no_pair_table_copy['ViewData.Source Combination Code_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Source Combination Code_y']=='None','Source Combination Code'] = final_no_pair_table_copy['ViewData.Source Combination Code_x']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Source Combination Code_x']=='nan','Source Combination Code'] = final_no_pair_table_copy['ViewData.Source Combination Code_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Source Combination Code_y']=='nan','Source Combination Code'] = final_no_pair_table_copy['ViewData.Source Combination Code_x']


final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Source Combination Code_x']=='NaT','Source Combination Code'] = final_no_pair_table_copy['ViewData.Source Combination Code_y']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Source Combination Code_y']=='NaT','Source Combination Code'] = final_no_pair_table_copy['ViewData.Source Combination Code_x']


final_no_pair_table_copy['Final_predicted_break'] = ''

final_no_pair_table_copy['Task Business Date'] = pd.to_datetime(final_no_pair_table_copy['Task Business Date'])
final_no_pair_table_copy['Task Business Date'] = final_no_pair_table_copy['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_no_pair_table_copy['Task Business Date'] = pd.to_datetime(final_no_pair_table_copy['Task Business Date'])

filepaths_final_no_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_no_pair_table_copy.to_csv(filepaths_final_no_pair_table)

if(final_umr_table.shape[0] != 0):
    final_umr_table_copy = final_umr_table.copy()
    final_umr_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_umr_table_copy)
    filepaths_final_umr_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umr_table_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    
    final_umr_table_copy.to_csv(filepaths_final_umr_table)
    
    
    final_umr_table_copy = pd.merge(final_umr_table_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
    final_umr_table_copy['Predicted_Status'] = 'UMR'
    #final_umr_table_copy['Predicted_action'] = 'No-Pair'
    final_umr_table_copy['ML_flag'] = 'ML'
    final_umr_table_copy['SetupID'] = setup_code 
    
    change_names_of_umr_table_mapping_dict = {
                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                'ViewData.BreakID_Side0' : 'BreakID',
                                                'ViewData.BreakID_Side1' : 'Final_predicted_break',
                                                'ViewData.Task ID' : 'Task ID',
                                                'ViewData.Task Business Date' : 'Task Business Date',
                                                'ViewData.Source Combination Code' : 'Source Combination Code'
                                            }
    
    
    
    final_umr_table_copy.rename(columns = change_names_of_umr_table_mapping_dict, inplace = True)
    
    final_umr_table_copy['Task Business Date'] = pd.to_datetime(final_umr_table_copy['Task Business Date'])
    final_umr_table_copy['Task Business Date'] = final_umr_table_copy['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_umr_table_copy['Task Business Date'] = pd.to_datetime(final_umr_table_copy['Task Business Date'])
else:
    final_umr_table_copy = pd.DataFrame()

final_no_pair_table_copy.rename(columns = change_names_of_umr_table_mapping_dict, inplace = True)
cols_for_database = list(final_umr_table_copy.columns)


final_no_pair_table_to_write = final_no_pair_table_copy[cols_for_database]

final_table_to_write = final_no_pair_table_to_write.append(final_umr_table_copy)

final_table_to_write['PredictedComment'] = ''

#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32

final_table_to_write[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_table_to_write[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)

final_table_to_write[['BreakID', 'Task ID']] = final_table_to_write[['BreakID', 'Task ID']].astype(float)
final_table_to_write[['BreakID', 'Task ID']] = final_table_to_write[['BreakID', 'Task ID']].astype(np.int64)

final_table_to_write[['SetupID']] = final_table_to_write[['SetupID']].astype(int)

#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

change_col_names_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
final_table_to_write.rename(columns = change_col_names_dict, inplace = True)

final_table_to_write = final_table_to_write.append([final_oto_umb_table_new_to_write,                                                     final_mto_table_copy_new_to_write,                                                     final_otm_table_copy_new_to_write])


#filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write.csv'
#final_table_to_write.to_csv(filepaths_final_table_to_write)
#filepaths_final_no_pair_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table_copy.csv'
#filepaths_final_no_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table.csv'
#
#filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df.csv'
#
#final_no_pair_table.to_csv(filepaths_final_no_pair_table)
#
#
#final_no_pair_table_copy.to_csv(filepaths_final_no_pair_table_copy)
#meo_df.to_csv(filepaths_meo_df)


#Closed Begins
if(closed_df.shape != 0):
    closed_columns_for_updation = ['ViewData.BreakID','ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Source Combination Code','ViewData.Task ID']
    
    final_closed_df = closed_df[closed_columns_for_updation]
    final_closed_df['Predicted_Status'] = 'UCB'
    final_closed_df['Predicted_action'] = 'Closed'
    final_closed_df['ML_flag'] = 'ML'
    final_closed_df['SetupID'] = setup_code 
    final_closed_df['Final_predicted_break'] = ''
    final_closed_df['PredictedComment'] = ''
    final_closed_df['PredictedCategory'] = ''
    final_closed_df['probability_UMB'] = ''
    final_closed_df['probability_No_pair'] = ''
    final_closed_df['probability_UMR'] = ''
    
    final_closed_df[closed_columns_for_updation] = final_closed_df[closed_columns_for_updation].astype(str)
    change_names_of_final_closed_df_mapping_dict = {
                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                'ViewData.BreakID' : 'BreakID',
                                                'ViewData.Task ID' : 'TaskID',
                                                'ViewData.Task Business Date' : 'BusinessDate',
                                                'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                            }
    
    final_closed_df.rename(columns = change_names_of_final_closed_df_mapping_dict, inplace = True)
    
    final_closed_df['BusinessDate'] = pd.to_datetime(final_closed_df['BusinessDate'])
    final_closed_df['BusinessDate'] = final_closed_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_closed_df['BusinessDate'] = pd.to_datetime(final_closed_df['BusinessDate'])
    
    
    #final_closed_df[[\
    #                 'Side0_UniqueIds', \
    #                 'Side1_UniqueIds', \
    #                 'Final_predicted_break', \
    #                 'Predicted_action', \
    #                 'probability_No_pair', \
    #                 'probability_UMB', \
    #                 'probability_UMR', \
    #                 'SourceCombinationCode', \
    #                 'Predicted_Status', \
    #                 'ML_flag']] = \
    #                 final_table_to_write[[\
    #                                       'Side0_UniqueIds', \
    #                                       'Side1_UniqueIds', \
    #                                       'Final_predicted_break', \
    #                                       'Predicted_action', \
    #                                       'probability_No_pair', \
    #                                       'probability_UMB', \
    #                                       'probability_UMR', \
    #                                       'SourceCombinationCode', \
    #                                       'Predicted_Status', \
    #                                       'ML_flag']] \
    #                 .astype(str)
    
    final_closed_df['Side0_UniqueIds'] = final_closed_df['Side0_UniqueIds'].astype(str)
    final_closed_df['Side1_UniqueIds'] = final_closed_df['Side1_UniqueIds'].astype(str)
    final_closed_df['Final_predicted_break'] = final_closed_df['Final_predicted_break'].astype(str)
    final_closed_df['Predicted_action'] = final_closed_df['Predicted_action'].astype(str)
    final_closed_df['probability_No_pair'] = final_closed_df['probability_No_pair'].astype(str)
    final_closed_df['probability_UMB'] = final_closed_df['probability_UMB'].astype(str)
    final_closed_df['probability_UMR'] = final_closed_df['probability_UMR'].astype(str)
    final_closed_df['SourceCombinationCode'] = final_closed_df['SourceCombinationCode'].astype(str)
    final_closed_df['Predicted_Status'] = final_closed_df['Predicted_Status'].astype(str)
    final_closed_df['ML_flag'] = final_closed_df['ML_flag'].astype(str)
    
    
    final_closed_df[['BreakID', 'TaskID']] = final_closed_df[['BreakID', 'TaskID']].astype(float)
    final_closed_df[['BreakID', 'TaskID']] = final_closed_df[['BreakID', 'TaskID']].astype(np.int64)
    
    final_closed_df[['SetupID']] = final_closed_df[['SetupID']].astype(int)
    
    filepaths_final_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_closed_df_setup_' + setup_code + '_date_' + str(date_i) + '_after_changes.csv'
    final_closed_df.to_csv(filepaths_final_closed_df)
else:
    final_closed_df = pd.DataFrame()
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

final_table_to_write = final_table_to_write.append(final_closed_df)
#final_table_to_write = final_table_to_write.append([final_closed_df, \
#                                                    final_oto_umb_table_new_to_write, \
#                                                    umr_mto_table_new_to_write, \
#                                                    umr_otm_table_final_new_to_write])

#Closed Ends


#UMB Carry Forward Begins
umb_carry_forward_columns_to_select_from_meo_df = ['ViewData.BreakID',                                                    'ViewData.Task Business Date',                                                    'ViewData.Side0_UniqueIds',                                                    'ViewData.Side1_UniqueIds',                                                    'ViewData.Source Combination Code',                                                    'ViewData.Task ID']
umb_carry_forward_df = umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df]

umb_carry_forward_df['Predicted_Status'] = 'UMB'
# Change added on 20-12-2020 as per Pratik. Since there are a lot of UMB Carry Forwards, we will include them in our final status and show them as predicted UMBs. Therefore the ML_flah will be ML instead of Not_covered_by_ML
# umb_carry_forward_df['Predicted_action'] = 'UMB_Carry_Forward'
# umb_carry_forward_df['ML_flag'] = 'Not_Covered_by_ML'
umb_carry_forward_df['Predicted_action'] = 'UMB'
umb_carry_forward_df['ML_flag'] = 'ML'
umb_carry_forward_df['SetupID'] = setup_code 
umb_carry_forward_df['Final_predicted_break'] = ''
umb_carry_forward_df['PredictedComment'] = ''
umb_carry_forward_df['PredictedCategory'] = ''
umb_carry_forward_df['probability_UMB'] = ''
umb_carry_forward_df['probability_No_pair'] = ''
umb_carry_forward_df['probability_UMR'] = ''

umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df] = umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df].astype(str)
change_names_of_umb_carry_forward_df_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID' : 'BreakID',
                                            'ViewData.Task ID' : 'TaskID',
                                            'ViewData.Task Business Date' : 'BusinessDate',
                                            'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                        }

umb_carry_forward_df.rename(columns = change_names_of_umb_carry_forward_df_mapping_dict, inplace = True)

umb_carry_forward_df['BusinessDate'] = pd.to_datetime(umb_carry_forward_df['BusinessDate'])
umb_carry_forward_df['BusinessDate'] = umb_carry_forward_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
umb_carry_forward_df['BusinessDate'] = pd.to_datetime(umb_carry_forward_df['BusinessDate'])

umb_carry_forward_df['Side0_UniqueIds'] = umb_carry_forward_df['Side0_UniqueIds'].astype(str)
umb_carry_forward_df['Side1_UniqueIds'] = umb_carry_forward_df['Side1_UniqueIds'].astype(str)
umb_carry_forward_df['Final_predicted_break'] = umb_carry_forward_df['Final_predicted_break'].astype(str)
umb_carry_forward_df['Predicted_action'] = umb_carry_forward_df['Predicted_action'].astype(str)
umb_carry_forward_df['probability_No_pair'] = umb_carry_forward_df['probability_No_pair'].astype(str)
umb_carry_forward_df['probability_UMB'] = umb_carry_forward_df['probability_UMB'].astype(str)
umb_carry_forward_df['probability_UMR'] = umb_carry_forward_df['probability_UMR'].astype(str)
umb_carry_forward_df['SourceCombinationCode'] = umb_carry_forward_df['SourceCombinationCode'].astype(str)
umb_carry_forward_df['Predicted_Status'] = umb_carry_forward_df['Predicted_Status'].astype(str)
umb_carry_forward_df['ML_flag'] = umb_carry_forward_df['ML_flag'].astype(str)


umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(float)
umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(np.int64)

umb_carry_forward_df[['SetupID']] = umb_carry_forward_df[['SetupID']].astype(int)
#filepaths_umb_carry_forward_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_carry_forward_df.csv'
filepaths_umb_carry_forward_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_carry_forward_df_setup_' + setup_code + '_date_' + str(date_i) + '.csv'

umb_carry_forward_df.to_csv(filepaths_umb_carry_forward_df)

final_table_to_write = final_table_to_write.append(umb_carry_forward_df)

#UMB Carry Forward Ends

# Change added on 20-12-2020 to add the following three tables to final_table_to_write:
# 1. final_smb_ob_table_copy
# 2. final_umb_ob_table_copy
# 3. final_mtm_table_copy_new_to_write

# Append smb_ob to final_table_to_write
if(final_smb_ob_table_copy.shape[0] != 0):
    final_table_to_write = final_table_to_write.append(final_smb_ob_table_copy)
# Append umb_ob to final_table_to_write
if(final_umb_ob_table_copy.shape[0] != 0):
    final_table_to_write = final_table_to_write.append(final_umb_ob_table_copy)
# Append final_mtm_table_copy_new_to_write to final_table_to_write
if(final_mtm_table_copy_new_to_write.shape[0] != 0):
    final_table_to_write = final_table_to_write.append(final_mtm_table_copy_new_to_write)

# Append final_mtm_table_copy_new_to_write to final_table_to_write
if(umb_subsum_df_copy.shape[0] != 0):
    final_table_to_write = final_table_to_write.append(umb_subsum_df_copy)
# Append final_mtm_table_copy_new_to_write to final_table_to_write
if(mtm_df_full_umb_copy.shape[0] != 0):
    final_table_to_write = final_table_to_write.append(mtm_df_full_umb_copy)

#final_table_to_write['BreakID'] = final_table_to_write['BreakID'].replace('[','',regex = True).replace(']','',regex = True)
#final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].replace('[','',regex = True).replace(']','',regex = True)


filepaths_final_no_pair_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table_copy.csv'
final_no_pair_table_copy.to_csv(filepaths_final_no_pair_table_copy)

filepaths_final_no_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_no_pair_table.csv'
final_no_pair_table.to_csv(filepaths_final_no_pair_table)

filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_table_to_write.to_csv(filepaths_final_table_to_write)

filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
meo_df.to_csv(filepaths_meo_df)

def unlist_comma_separated_single_quote_string_lst(list_obj):
    new_list = []
    for i in list_obj:
        list_i = list(i.replace('\'','').replace('[','').replace(']','').split(', '))
        for j in list_i:
            new_list.append(j)
    return new_list

def get_remaining_breakids(fun_meo_df, fun_final_df_2):
    
#    fun_meo_df = fun_meo_df[~fun_meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR','SPM'])]
    fun_meo_df = fun_meo_df[~fun_meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]

    BreakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['BreakID'].astype(str).unique().tolist())
#    BreakId_final_df_2 =  final_df_2['BreakID'].astype(str).unique().tolist()
    
    Final_predicted_breakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['Final_predicted_break'].astype(str).unique().tolist())
#    for i in final_df_2['Final_predicted_break'].astype(str).unique().tolist():
#        if(',' in i):
#            print(i)
    BreakId_meo_df =  unlist_comma_separated_single_quote_string_lst(fun_meo_df['ViewData.BreakID'].astype(str).unique().tolist())
    all_breakids_in_final_df_2 = set(BreakId_final_df_2).union(set(Final_predicted_breakId_final_df_2))        
    fun_unpredicted_breakids = list(set(BreakId_meo_df) - set(all_breakids_in_final_df_2)) 
#    meo_df[meo_df['ViewData.BreakID'].isin(unpredicted_breakids)]['ViewData.Status'].value_counts()
    return(fun_unpredicted_breakids)

BreakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(final_table_to_write['BreakID'].astype(str).unique().tolist())
#    BreakId_final_df_2 =  final_df_2['BreakID'].astype(str).unique().tolist()

Final_predicted_breakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(final_table_to_write['Final_predicted_break'].astype(str).unique().tolist())
#    for i in final_df_2['Final_predicted_break'].astype(str).unique().tolist():
#        if(',' in i):
#            print(i)
BreakId_meo_df =  unlist_comma_separated_single_quote_string_lst(meo_df['ViewData.BreakID'].astype(str).unique().tolist())
all_breakids_in_final_df_2 = set(BreakId_final_df_2).union(set(Final_predicted_breakId_final_df_2))        
fun_unpredicted_breakids = list(set(BreakId_meo_df) - set(all_breakids_in_final_df_2)) 

unpredicted_breakids = get_remaining_breakids(fun_meo_df = meo_df, fun_final_df_2 = final_table_to_write)
#unpredicted_breakids_Predicted_Status = meo_df[meo_df['ViewData.BreakID'] == ]  
BusinessDate_df_to_append_value = final_table_to_write['BusinessDate'].iloc[1]

df_to_append= meo_df[meo_df['ViewData.BreakID'].isin(unpredicted_breakids)][['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Status','ViewData.Source Combination Code']]
change_names_of_df_to_append_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID' : 'BreakID',
                                            'ViewData.Task ID' : 'TaskID',
                                            'ViewData.Status' : 'Predicted_Status',
                                            'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                        }

df_to_append.rename(columns = change_names_of_df_to_append_mapping_dict, inplace = True)
#df_to_append = pd.DataFrame()

df_to_append['BusinessDate'] = BusinessDate_df_to_append_value 
df_to_append['Final_predicted_break'] = ''
df_to_append['ML_flag'] = 'ML'
df_to_append['Predicted_Status'] = df_to_append['Predicted_Status'].apply(lambda x : x.strip())
df_to_append.loc[df_to_append['Predicted_Status'] != 'OB', 'Predicted_Status'] = df_to_append['Predicted_Status'] + '_Not_Covered_by_ML'
df_to_append.loc[df_to_append['Predicted_Status'] != 'OB', 'Predicted_action'] = df_to_append['Predicted_Status'] + '_Not_Covered_by_ML'
df_to_append.loc[df_to_append['Predicted_Status'] == 'OB', 'Predicted_Status'] = df_to_append['Predicted_Status']
df_to_append.loc[df_to_append['Predicted_Status'] == 'OB', 'Predicted_action'] = 'No-Pair'
df_to_append['SetupID'] = setup_code
df_to_append['probability_No_pair'] = ''
df_to_append['probability_UMR'] = ''
df_to_append['probability_UMB'] = ''
if(setup_code == '125' or setup_code == '123'):
    df_to_append['probability_UMT'] = ''
df_to_append['PredictedComment'] = ''
df_to_append['PredictedCategory'] = ''

filepaths_df_to_append = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\df_to_append_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
df_to_append.to_csv(filepaths_df_to_append)

final_table_to_write = final_table_to_write.append(df_to_append)

#Change added on 12-01-2021 as per Pratik. This code is for adding UMT table in oaktree, almost similar to what we did in Weiss 125
#Begin changes added on 12-01-2021

if(final_umt_table.shape[0] != 0):
    final_umt_table_copy = final_umt_table.copy()   
    
    final_umt_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_umt_table_copy)
    #
    final_umt_table_copy['ViewData.Side0_UniqueIds'] = final_umt_table_copy['ViewData.Side0_UniqueIds'].astype(str)
    final_umt_table_copy['ViewData.Side1_UniqueIds'] = final_umt_table_copy['ViewData.Side1_UniqueIds'].astype(str)
     
    final_umt_table_copy_new = pd.merge(final_umt_table_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
    #    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
    final_umt_table_copy_new['Predicted_Status'] = 'UMT'
    final_umt_table_copy_new['ML_flag'] = 'ML'
    final_umt_table_copy_new['SetupID'] = setup_code 
    #
    #filepaths_final_umt_table_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\filepaths_final_umt_table_copy_new.csv'
    #final_umt_table_copy_new.to_csv(filepaths_final_umt_table_copy_new)
    
    change_names_of_filepaths_final_umt_table_copy_new_mapping_dict = {
                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                'ViewData.BreakID_Side1' : 'BreakID',
                                                'ViewData.BreakID_Side0' : 'Final_predicted_break',
                                                'ViewData.Task ID' : 'Task ID',
                                                'ViewData.Task Business Date' : 'Task Business Date',
                                                'ViewData.Source Combination Code' : 'Source Combination Code'
                                            }
    
    
    final_umt_table_copy_new.rename(columns = change_names_of_filepaths_final_umt_table_copy_new_mapping_dict, inplace = True)
    
    final_umt_table_copy_new['Task Business Date'] = pd.to_datetime(final_umt_table_copy_new['Task Business Date'])
    final_umt_table_copy_new['Task Business Date'] = final_umt_table_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_umt_table_copy_new['Task Business Date'] = pd.to_datetime(final_umt_table_copy_new['Task Business Date'])
    
    
    final_umt_table_copy_new['PredictedComment'] = ''
    
    #Changing data types of columns as follows:
    #Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
    #BreakID, TaskID - int64
    #SetupID - int32
    final_umt_table_copy_new['probability_UMT'] = ''
    final_umt_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR','probability_UMT', 'Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']] = final_umt_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'probability_UMT','Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']].astype(str)
    
    final_umt_table_copy_new[['BreakID', 'Task ID']] = final_umt_table_copy_new[['BreakID', 'Task ID']].astype(float)
    final_umt_table_copy_new[['BreakID', 'Task ID']] = final_umt_table_copy_new[['BreakID', 'Task ID']].astype(np.int64)
    
    final_umt_table_copy_new[['SetupID']] = final_umt_table_copy_new[['SetupID']].astype(int)
    
    #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
    #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)
    
    change_col_names_final_umt_table_copy_new_new_dict = {
                            'Task ID' : 'TaskID',
                            'Task Business Date' : 'BusinessDate',
                            'Source Combination Code' : 'SourceCombinationCode'
                            }
    final_umt_table_copy_new.rename(columns = change_col_names_final_umt_table_copy_new_new_dict, inplace = True)
    
    cols_for_database_new = ['Side0_UniqueIds',
     'Side1_UniqueIds',
     'BreakID',
     'Final_predicted_break',
     'Predicted_action',
     'probability_No_pair',
     'probability_UMB',
     'probability_UMR',
     'probability_UMT',
     'TaskID',
     'BusinessDate',
     'PredictedComment',
     'SourceCombinationCode',
     'Predicted_Status',
     'ML_flag',
     'SetupID']
    
    final_umt_table_copy_new_to_write = final_umt_table_copy_new[cols_for_database_new]
    filepaths_final_umt_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umt_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
    final_umt_table_copy_new_to_write.to_csv(filepaths_final_umt_table_copy_new_to_write)
else:
    final_umt_table_copy_new_to_write = pd.DataFrame()

if(final_umt_table_copy_new_to_write.shape[0] != 0):
    final_table_to_write = final_table_to_write.append(final_umt_table_copy_new_to_write)
#End changes added on 12-01-2021

filepaths_final_table_to_write_before_comment = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write_before_comment_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_table_to_write.to_csv(filepaths_final_table_to_write_before_comment)
#data_dict = final_table_to_write.to_dict("records")
#coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
#coll_1_for_writing_prediction_data.insert_many(data_dict) 
#


# In[260]:


#Comment
# Change added on 20-12-2020. Abhijeet added new outpul_cols 
output_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted status','predicted action','predicted category','predicted comment']

#Start of Commenting
os.chdir('D:\\ViteosModel\\Abhijeet - Comment')
comment_df_final_list = []
brk = final_table_to_write.copy()

brk = brk[brk['Predicted_action'] == 'No-Pair']

#meo_df = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\meo_df.csv')
filepaths_brk = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\brk_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
brk.to_csv(filepaths_brk)

brk = brk.rename(columns ={'Side0_UniqueIds':'ViewData.Side0_UniqueIds',
                         'Side1_UniqueIds':'ViewData.Side1_UniqueIds'})
meo_df = meo_df.rename(columns ={'ViewData.B-P Net Amount':'ViewData.Cust Net Amount'
                         })
brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].fillna('AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].fillna('BB')

brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].replace('nan','AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].replace('nan','BB')

brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].replace('None','AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].replace('None','BB')

brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].replace('','AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].replace('','BB')

def fid1(a,b,c):
    if a=='No-Pair':
        if b =='AA':
            return c
        else:
            return b
    else:
        return '12345'



brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1 )


brk['final_ID'].value_counts()


side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))

meo1 = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(side0_id)]
meo2 = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(side1_id)]

meo1['ViewData.Side1_UniqueIds'] = ''
meo2['ViewData.Side0_UniqueIds'] = ''

frames = [meo1, meo2]

df1 = pd.concat(frames)

df1['ViewData.Currency'] = df1['ViewData.Currency'].fillna('DDD')
df1 = df1.reset_index()
df1 = df1.drop('index', axis = 1)


# ### Duplicate OB removal
df1 = df1.drop_duplicates()

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].replace('nan','AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].replace('nan','BB')

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].replace('None','AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].replace('None','BB')

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].replace('','AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].replace('','BB')

filepaths_df1 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\df1_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
df1.to_csv(filepaths_df1)

def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b
        


df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)

df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])
# Change added on 20-12-2020 by Abhijeet. This piece of code is for eliminating 'Full Call' Transaction Type 
# Begin change added on 20-12-2020
# ### Treatment of Full Call

# #### Module 1

# In[129]:


def subSum(numbers,total):
    length = len(numbers)

    if length <10:
        
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=1).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers


# In[130]:


fullcal = df1.groupby(['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Fund'])['ViewData.Quantity'].apply(list).reset_index()


# In[131]:


fullcal['set_Qn_list'] = fullcal['ViewData.Quantity'].apply(lambda x : list(set([value for value in x if ((str(value) != 'nan') & (str(value) != 'None'))])))


# In[132]:


fullcal['zero_list'] = fullcal['set_Qn_list'].apply(lambda x : subSum(x,0))


# In[134]:


def list_filter(x):
    if ((len(x)>1) & (len(x)<11)):
        if ((0.0 not in x) & (sum(x)<0.5)):
            return 1
        else:
            return 0
    else:
        return 0


# In[135]:


fullcal['fcmark1'] = fullcal['zero_list'].apply(lambda x : list_filter(x))


# In[139]:


fullcal.drop(['ViewData.Quantity','set_Qn_list'], axis = 1, inplace = True)


# In[140]:


df1 = pd.merge(df1, fullcal, on = ['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Fund'], how = 'left')


# In[154]:


def fc_remover(x,y,z,m):
    ttype = ['corporate action','interest']
    if ((isinstance(y,list)) & (type(m)==str)):
        m = m.lower()
        if ((x in y) & (z == 1.0)  & (m in ttype)):
            return 1
        else:
            return 0
    else:
        return 0
        


# In[155]:


df1['final_fc'] = df1.apply(lambda row: fc_remover(row['ViewData.Quantity'],row['zero_list'],row['fcmark1'],row['ViewData.Transaction Type']), axis =1 )


# In[157]:

def comgen1(x,y,z,k):
    if x == None:
        com = 'Geneva to book full call. Corporate action team to post it' + ' ' + k + " " + 'already booked it.'
    else:
        com = k + ' ' + 'to book full call. Corporate action team to post it' + " " +  'Geneva already booked it.'

    return com

fc1 = df1[df1['final_fc']==1]

if(fc1.shape[0] != 0):
    # In[158]:


    fc1['predicted status'] = 'No-pair'
    fc1['predicted action'] = 'OB'
    fc1['predicted category'] = 'full call corp action'


    # In[159]:




    # In[160]:


    fc1['predicted comment'] = fc1.apply(lambda x : comgen1(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds'],x['ViewData.Settle Date'],x['ViewData.Prime Broker']), axis = 1)


    # In[161]:


    fc1 = fc1[output_col]


    # In[163]:

    df1 = df1[df1['final_fc']!=1]

    fc1.to_csv(base_dir + 'oaktree final prediction p1.csv')
    
    comment_df_final_list.append(fc1)
#    del(fc1)
else:
    df1 = df1.copy()
# #### Module 2

# In[164]:
#Ran till here with no error

df1.drop(['zero_list', 'fcmark1', 'final_fc'], axis =1 , inplace = True)

#Change made on 09-01-2021 as per Abhijeet. Module for fullcal2 commented out
#Begin comment out change on 09-01-2021
#fullcal2 = df1.groupby(['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Fund'])['ViewData.Quantity'].apply(list).reset_index()
#
#
#fullcal2['set_zero_list'] = fullcal2['ViewData.Quantity'].apply(lambda x : list([value for value in x if ((str(value) != 'nan') & (str(value) != 'None'))]))
#
#
#def list_filter1(x,y):
#    if ((len(y)>1) & (len(set(y))==1)):
#        if ((0.0 not in y)):
#            return 1
#        else:
#            return 0
#    else:
#        return 0
#
#fullcal2['fcmark1'] = fullcal2.apply(lambda x : list_filter1(x['ViewData.Quantity'],x['set_zero_list']), axis = 1)
#
#fullcal2.drop(['ViewData.Quantity','set_zero_list'], axis = 1, inplace = True)
#
#df1 = pd.merge(df1, fullcal2, on = ['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Fund'], how = 'left')
#
#
#fc1 =df1[df1['fcmark1']==1]
#
#if(fc1.shape[0] != 0):
#    fc1['predicted status'] = 'No-pair'
#    fc1['predicted action'] = 'OB'
#    fc1['predicted category'] = 'full call corp action'
#
#    fc1['predicted comment'] = fc1.apply(lambda x : comgen1(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds'],x['ViewData.Settle Date'],x['ViewData.Prime Broker']), axis = 1)
#    fc1 = fc1[output_col]
#    df1 = df1[df1['fcmark1']!=1]
#
#    fc1.to_csv(base_dir + 'oaktree final prediction p2.csv')
#    comment_df_final_list.append(fc1)
#
#else:
#    df1 = df1.copy()
## #### Moving Ahead
#
## In[177]:
#
#
#
#df1.drop('fcmark1', axis =1 , inplace = True)
#End comment out change on 09-01-2021
# End change added on 20-12-2020

#Change made on 09-01-2021 as per Abhijeet. Module for hedging activity
#Begin new code change on 09-01-2021

#Change added on 11-01-2021 as per Abhijeet. This code is for fullcal with only company name
#Begin code change here for 11-01-2021
df1['ViewData.Transaction Type'] = df1['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
fullcall2 = df1.groupby(['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Transaction Type'].apply(list).reset_index()
def fullcallcheck(x):
    if (('corp red payment' in x) & ('interest' in x)):
        return 1
    else:
        return 0
fullcall2['fcmark2'] = fullcall2['ViewData.Transaction Type'].apply(lambda x : fullcallcheck(x))
fullcall2.drop('ViewData.Transaction Type', axis = 1, inplace = True)
df1 = pd.merge(df1, fullcall2, on = ['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
rem_ttype = ['corp red payment','interest']

def fc_remove(x,y):
    if ((x ==1) &  (y in rem_ttype)):
        return 1
    else:
        return 0
    
if df1[df1['fcmark2']==1].shape[0]!=0:
    df1['fc_remove'] = df1.apply(lambda row: fc_remove(row['fcmark2'],row['ViewData.Transaction Type']), axis =1)
    if df1[df1['fc_remove']==1].shape[0]!=0:
        fc2 = df1[df1['fc_remove']==1]
        fc2['predicted comment'] = fc2.apply(lambda x : comgen1(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds'],x['ViewData.Settle Date'],x['ViewData.Prime Broker']), axis = 1)
        fc2['predicted status'] = 'No-pair'
        fc2['predicted action'] = 'OB'
        fc2['predicted category'] = 'full call corp action'
        fc2 = fc2[output_col]
#        fc2.to_csv('oaktree/oaktree final prediction fullcal p2.csv')
        fc2.to_csv(base_dir + 'oaktree final prediction p9.csv')
        
        comment_df_final_list.append(fc2)

        df1 = df1[df1['fc_remove']!=1]
        df1.drop(['fcmark2','fc_remove'], axis = 1, inplace = True)
    else:
        df1 = df1.copy()
        df1.drop(['fcmark2','fc_remove'], axis = 1, inplace = True)
else:
    df1 = df1.copy()
    df1.drop(['fcmark2'], axis = 1, inplace = True)
    


#End code change here for 11-01-2021


req_desc = ['third','party','fx','transaction']


# In[ ]:


def netting1(x,req_desc):
    if type(x)==str:
        x = x.lower()
        x1 = x.split()
        lst3 = [value for value in req_desc if value in x1]
        if len(lst3) == 4:
            return 1
        else:
            return 0
    else:
        return 0


# In[ ]:


df1['Netting_var'] = df1['ViewData.Description'].apply(lambda x : netting1(x,req_desc))


# In[ ]:


def netting_com(a,b):
    com = 'Netting is' + ' ' + str(a) + 'for' + " " + str(b) + " " + 'We have escalted this to client.'
    return com


# In[ ]:


if (df1[df1['Netting_var']==1].shape[0]!=0):
    netting = df1[df1['Netting_var']==1]
    net = netting.groupby(['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Settle Date'])['ViewData.Net Amount Difference'].sum().reset_index()
    net = net.rename(columns = {'ViewData.Net Amount Difference':'netting amount'})
    netting = pd.merge(netting, net, on = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Settle Date'], how = 'left')
    netting['predicted comment'] = netting.apply(lambda row: netting_com(row['netting amount'], row['ViewData.Settle Date']), axis =1)
    netting['predicted status'] = 'No-pair'
    netting['predicted action'] = 'OB'
    netting['predicted category'] = 'full call corp action'
    netting = netting[output_col]
#    netting.to_csv('oaktree/oaktree final prediction p2.csv')
    netting.to_csv(base_dir + 'oaktree final prediction p8.csv') #This is p8 since this code was added later once files till p7 had already formed. So just incrementenly added the next in p<number> series file naming

    df1 = df1[df1['Netting_var']!=1]    
    comment_df_final_list.append(netting)
else:
    df1 = df1.copy()

#End new code change on 09-01-2021

#uni2 = df1.groupby(['final_ID','ViewData.Task Business Date']).last().reset_index()
#
#uni2 = uni2.sort_values(['final_ID','ViewData.Task Business Date'], ascending = [True, True])
#
# #### Trade date vs Settle date and future dated trade
#df2 = uni2.copy()

df2 = df1.copy()
import datetime

df2['ViewData.Settle Date'] = pd.to_datetime(df2['ViewData.Settle Date'])
df2['ViewData.Trade Date'] = pd.to_datetime(df2['ViewData.Trade Date'])
df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])

df2['ViewData.Task Business Date1'] = df2['ViewData.Task Business Date'].dt.date

df2['ViewData.Settle Date1'] = df2['ViewData.Settle Date'].dt.date
df2['ViewData.Trade Date1'] = df2['ViewData.Trade Date'].dt.date

df2['ViewData.SettlevsTrade Date'] = (df2['ViewData.Settle Date1'] - df2['ViewData.Trade Date1']).dt.days
df2['ViewData.SettlevsTask Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Settle Date1']).dt.days
df2['ViewData.TaskvsTrade Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Trade Date1']).dt.days

# Change added on 20-12-2020 as per Abhijeet. New features added which will be given into the model

# Begin change added on 20-12-2020

df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])
df2['ViewData.Settle Date'] = pd.to_datetime(df2['ViewData.Settle Date'])

from datetime import datetime, timezone

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

df2['ViewData.Task Business Date'] = df2['ViewData.Task Business Date'].apply(lambda x :utc_to_local(x) )

df2['fut_date'] = round(((df2['ViewData.Settle Date'] - df2['ViewData.Task Business Date']).astype('timedelta64[h]'))/24,1)

# #### Creating of Some other features

import re

def fullcall(x):
    if type(x)==str:
        y = re.search("[0-9]{2}[A-Z]{3}[0-9]{2}", x)
        if y==None:
            return 0
        else:
            return 1
    else:
        return 0
    
    


# In[203]:


df2['full_call'] = df2['ViewData.Description'].apply(lambda x :fullcall(x))


# In[204]:


def hasdate(x):
    if type(x)==str:
        y = re.search("[0-9]+/[A-Za-z]+/[0-9]+", x)
        if y==None:
            return 0
        else: 
            y = re.search("[0-9]+-[A-Za-z]+-[0-9]+", x)
            if y==None:
                return 1
            else:
                return 0
    else:
        return 0


# In[205]:


df2['has_date'] = df2['ViewData.Description'].apply(lambda x :hasdate(x))


# In[206]:


def rate(x):
    if type(x)==str:
        item1 = x.split()
        for item in item1:
            if item.endswith('%'):
                return 1
            else:
                return 0
    else:
        return 0


# In[207]:


df2['rate_var'] = df2['ViewData.Description'].apply(lambda x :rate(x))
df2['rate_var_itype'] = df2['ViewData.Investment Type'].apply(lambda x :rate(x))


# In[208]:


month_end =[29,30,31,1]


# In[209]:


df2['month_end_mark'] = df2['ViewData.Settle Date'].apply(lambda x : 1 if x.day in month_end else 0)

# End change added on 20-12-2020
# ### Cleannig of the 4 variables in this
df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')

def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys

df['tuple'] = df.apply(make_dict, axis=1)

clean_map_dict = df.set_index('tuple')['Value'].to_dict()

df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Asset Type Category'] = df2['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Investment Type'] = df2['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Prime Broker'] = df2['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)

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
        
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)

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

df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)

df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)

df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : 'paydown' if x=='pay down' else x)

# Change added on 20-12-2020 as per Abhijeet
# Begin change added on 20-12-2020

def due_beo(x):
    if type(x)==str:
        x1 = x.split()
        if (('due' in x1) & ('beo' in x1)):
            return 'comp due beo'
        else:
            return x
    else:
        return x


# In[228]:


df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type1'].apply(lambda x :due_beo(x))


# #### start of keyword approach

# In[229]:


com = pd.read_csv('desc cat with naveen oaktree2.csv')


# In[231]:


cat_list = list(set(com['Pairing']))


# In[232]:


import re


# In[233]:



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
            


# In[234]:


df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[238]:


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
        


# In[239]:


df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))


# In[243]:


com = com.drop(['var','Catogery'], axis = 1)


# In[244]:


com = com.drop_duplicates()


# In[245]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[246]:


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


# In[247]:


def intereset_clean(x):
    k = 0
    i = 0
    if len(x)>0:
        for item in x:
            a = len(item)
            if a>k:
                k = a
                word = item
            #print(word)
    
        ret_item = []
        for item1  in x:
        
            item2 = item1.split()
            words = word.split()
            lst3 = [value for value in item2 if value in words]
        #print(lst3)
            if len(lst3)!=len(item2):
                ret_item.append(item1)
            
        ret_item.append(word)
    else:
        ret_item = x
    return list(set(ret_item))


# In[249]:


df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))


# In[250]:


df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : intereset_clean(x))

#Change made on 12-02-2021 as per Abhijeet
def lastprocess(x):
    if isinstance(x, list) == False:
        x1 = x.split()
        if 'corp' in x1:
            return ['company']
        else:
            return x
    else:
        return x

df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : lastprocess(x))


df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : ' '.join(x) if  isinstance(x, list) else x )


# End change added on 20-12-2020

# ### Cleaning of Description
# com = pd.read_csv('desc cat with naveen oaktree.csv')
# cat_list = list(set(com['Pairing']))

# def descclean(com,cat_list):
#     cat_all1 = []
#     list1 = cat_list
#     m = 0
#     if (type(com) == str):
#         com = com.lower()
#         com1 =  re.split("[,/. \-!?:]+", com)
        
        
        
#         for item in list1:
#             if (type(item) == str):
#                 item = item.lower()
#                 item1 = item.split(' ')
#                 lst3 = [value for value in item1 if value in com1] 
#                 if len(lst3) == len(item1):
#                     cat_all1.append(item)
#                     m = m+1
            
#                 else:
#                     m = m
#             else:
#                     m = 0
#     else:
#         m = 0
#     if m >0 :
#         return list(set(cat_all1))
#     else:
#         if ((type(com)==str)):
#             if (len(com1)<4):
#                 if ((len(com1)==1) & com1[0].startswith('20')== True):
#                     return 'swap id'
#                 else:
#                     return com
#             else:
#                 return 'NA'
#         else:
#             return 'NA'
            
# df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

# def currcln(x):
#     if (type(x)==list):
#         return x
      
#     else:
       
        
#         if x == 'NA':
#             return "NA"
#         elif (('dollar' in x) | ('dollars' in x )):
#             return 'dollar'
#         elif (('pound' in x) | ('pounds' in x)):
#             return 'pound'
#         elif ('yen' in x):
#             return 'yen'
#         elif ('euro' in x) :
#             return 'euro'
#         else:
#             return x
        
# df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))

# com = com.drop(['var','Catogery'], axis = 1)

# com = com.drop_duplicates()

# com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
# com['replace'] = com['replace'].apply(lambda x : x.lower())

# def catcln1(cat,df):
#     ret = []
#     if (type(cat)==list):
        
#         if 'equity swap settlement' in cat:
#             ret.append('equity swap settlement')
#         #return 'equity swap settlement'
#         elif 'equity swap' in cat:
#             ret.append('equity swap settlement')
#         #return 'equity swap settlement'
#         elif 'swap settlement' in cat:
#             ret.append('equity swap settlement')
#         #return 'equity swap settlement'
#         elif 'swap unwind' in cat:
#             ret.append('swap unwind')
#         #return 'swap unwind'
   
    
#         else:
        
       
#             for item in cat:
            
#                 a = df[df['Pairing']==item]['replace'].values[0]
#                 if a not in ret:
#                     ret.append(a)
#         return list(set(ret))
      
#     else:
#         return cat

# df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))

# comp = ['inc','stk','corp ','llc','pvt','plc']
# df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

# def desccat(x):
#     if isinstance(x, list):
        
#         if 'equity swap settlement' in x:
#             return 'swap settlement'
#         elif 'collateral transfer' in x:
#             return 'collateral transfer'
#         elif 'dividend' in x:
#             return 'dividend'
#         elif (('loan' in x) & ('option' in x)):
#             return 'option loan'
        
#         elif (('interest' in x) & ('corp' in x) ):
#             return 'corp loan'
#         elif (('interest' in x) & ('loan' in x) ):
#             return 'interest'
#         else:
#             return x[0]
#     else:
#         if x == 'db_int':
#             return 'interest'
#         else:
#             return x

# df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : desccat(x))

# #### Prime Broker Creation
df2['new_pb'] = df2['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)

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

df2['new_pb'] = df2['new_pb'].apply(lambda x : new_pf_mapping(x))

df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].fillna('kkk')

df2['new_pb1'] = df2.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)

df2['new_pb1'] = df2['new_pb1'].apply(lambda x : x.lower())



#Change added on 09-01-2021 as per Abhijeet. Cancel trade code was mipredicting and is very less in number, so we bulk removed the entire piece. dffk2 and dffk3 definitions were added later in the code with newer definition so that code does not break
#Begin change on 09-01-2021 

# #### Cancelled Trade Removal

#trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']
#
#dfkk = df2[df2['ViewData.Transaction Type1'].isin(trade_types)]
#
#dfk_nontrade = df2[~df2['ViewData.Transaction Type1'].isin(trade_types)]
#
#dffk2 = dfkk[dfkk['ViewData.Side0_UniqueIds']=='AA']
#dffk3 = dfkk[dfkk['ViewData.Side1_UniqueIds']=='BB']
#
#dffk4 = dfk_nontrade[dfk_nontrade['ViewData.Side0_UniqueIds']=='AA']
#dffk5 = dfk_nontrade[dfk_nontrade['ViewData.Side1_UniqueIds']=='BB']
## #### Geneva side
#def canceltrade(x,y):
#    if x =='buy' and y>0:
#        k = 1
#    elif x =='sell' and y<0:
#        k = 1
#    else:
#        k = 0
#    return k
#
#dffk3['cancel_marker'] = dffk3.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Accounting Net Amount']), axis = 1)
#
#def cancelcomment(x,y):
#    com1 = 'This is original of cancelled trade with tran id'
#    com2 = 'on settle date'
#    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
#    return com
#
#def cancelcomment1(x,y):
#    com1 = 'This is cancelled trade with tran id'
#    com2 = 'on settle date'
#    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
#    return com
#
#if dffk3[dffk3['cancel_marker'] == 1].shape[0]!=0:
#    cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))
#    if len(cancel_trade)>0:
#        km = dffk3[dffk3['cancel_marker'] != 1]
#        original = km[km['ViewData.Transaction ID'].isin(cancel_trade)]
#        original['predicted category'] = 'Original of Cancelled trade'
#        if(original.shape[0]!=0):
#            original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#        cancellation = dffk3[dffk3['cancel_marker'] == 1]
#        cancellation['predicted category'] = 'Cancelled trade'
#        cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#        cancel_fin = pd.concat([original,cancellation])
#        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#        cancel_fin = cancel_fin[sel_col_1]
#        cancel_fin.to_csv('Comment file oaktree 2 sep testing p1.csv')
#        comment_df_final_list.append(cancel_fin)
#        dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
#        
#    else:
#        cancellation = dffk3[dffk3['cancel_marker'] == 1]
#        cancellation['predicted category'] = 'Cancelled trade'
#        cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#        cancel_fin = pd.concat([original,cancellation])
#        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#        cancel_fin = cancel_fin[sel_col_1]
#        cancel_fin.to_csv('Comment file oaktree 2 sep testing no original p2.csv')
#        comment_df_final_list.append(cancel_fin)
#        dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
#else:
#    dffk3 = dffk3.copy()
#
## #### Broker side
#dffk2['cancel_marker'] = dffk2.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Cust Net Amount']), axis = 1)
#
#def amountelim(row):
#   
#   
#   
#    if (row['SideA.ViewData.Mapped Custodian Account'] == row['SideB.ViewData.Mapped Custodian Account']):
#        a = 1
#    else:
#        a = 0
#        
#    if ((row['SideB.ViewData.Cust Net Amount']) == -(row['SideA.ViewData.Cust Net Amount'])):
#        b = 1
#    else:
#        b = 0
#    
#    if (row['SideA.ViewData.Fund'] == row['SideB.ViewData.Fund']):
#        c = 1
#    else:
#        c = 0
#        
#    if (row['SideA.ViewData.Currency'] == row['SideB.ViewData.Currency']):
#        d = 1
#    else:
#        d = 0
#    
#    if (row['SideA.ViewData.Settle Date1'] == row['SideB.ViewData.Settle Date1']):
#        e = 1
#    else:
#        e = 0
#        
#    if (row['SideA.ViewData.Transaction Type1'] == row['SideB.ViewData.Transaction Type1']):
#        f = 1
#    else:
#        f = 0
#        
#    if (row['SideB.ViewData.Quantity'] == row['SideA.ViewData.Quantity']):
#        g = 1
#    else:
#        g = 0
#        
#    if (row['SideB.ViewData.ISIN'] == row['SideA.ViewData.ISIN']):
#        h = 1
#    else:
#        h = 0
#        
#    if (row['SideB.ViewData.CUSIP'] == row['SideA.ViewData.CUSIP']):
#        i = 1
#    else:
#        i = 0
#        
#    if (row['SideB.ViewData.Ticker'] == row['SideA.ViewData.Ticker']):
#        j = 1
#    else:
#        j = 0
#        
#    if (row['SideB.ViewData.Investment ID'] == row['SideA.ViewData.Investment ID']):
#        k = 1
#    else:
#        k = 0
#        
#    return a, b, c ,d, e,f,g,h,i,j,k
#    
#
#from pandas import merge
#from tqdm import tqdm
#
#def cancelcomment2(y):
#    com1 = 'This is original of cancelled trade'
#    com2 = 'on settle date'
#    com = com1 + ' '  + com2 +' ' + str(y)
#    return com
#
#def cancelcomment3(y):
#    com1 = 'This is cancelled trade'
#    com2 = 'on settle date'
#    com = com1 + ' ' + com2 + ' ' + str(y)
#    return com
#

#if dffk2[dffk2['cancel_marker'] == 1].shape[0]!=0:
#    dummy = dffk2[dffk2['cancel_marker']!=1]
#    dummy1 = dffk2[dffk2['cancel_marker']==1]
#
#
#    pool =[]
#    key_index =[]
#    training_df =[]
#    call1 = []
#
#    appended_data = []
#
#    no_pair_ids = []
##max_rows = 5
#
#    k = list(set(list(set(dummy['ViewData.Task Business Date1']))))
#    k1 = k
#
#    for d in tqdm(k1):
#        aa1 = dummy[dummy['ViewData.Task Business Date1']==d]
#        bb1 = dummy1[dummy1['ViewData.Task Business Date1']==d]
#        aa1['marker'] = 1
#        bb1['marker'] = 1
#    
#        aa1 = aa1.reset_index()
#        aa1 = aa1.drop('index',1)
#        bb1 = bb1.reset_index()
#        bb1 = bb1.drop('index', 1)
#        #print(aa1.shape)
#        #print(bb1.shape)
#    
#        aa1.columns = ['SideB.' + x  for x in aa1.columns] 
#        bb1.columns = ['SideA.' + x  for x in bb1.columns]
#    
#        cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
#        appended_data.append(cc1)
#        cancel_broker = pd.concat(appended_data)
#        cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
#        elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]
#        if elim1.shape[0]!=0:
#            id_listA = list(set(elim1['SideA.final_ID']))
#            c1 = dummy
#            c2 = dummy1[dummy1['final_ID'].isin(id_listA)]
#            c1['predicted category'] = 'Cancelled trade'
#            c2['predicted category'] = 'Original of Cancelled trade'
#            c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']),axis = 1)
#            c2['predicted comment'] = c2.apply(lambda x : cancelcomment3(x['ViewData.Settle Date1']),axis = 1)
#            cancel_fin = pd.concat([c1,c2])
#            cancel_fin = cancel_fin.reset_index()
#            cancel_fin = cancel_fin.drop('index', axis = 1)
#            sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#            cancel_fin = cancel_fin[sel_col_1]
#            comment_df_final_list.append(cancel_fin)
#            cancel_fin.to_csv('Comment file oaktree 2 sep testing p3.csv')
#            id_listB = list(set(c1['final_ID']))
#            comb = id_listB + id_listA
#            dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
#            
#            
#            
#   
#        
#    else:
#        c1 = dummy
#        c1['predicted category'] = 'Cancelled trade'
#        c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']),axis = 1)
#        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#        cancel_fin = c1[sel_col_1]
#        comment_df_final_list.append(cancel_fin)
#        cancel_fin.to_csv('Comment file oaktree 2 sep testing no original p4.csv')
#        id_listB = list(set(c1['final_ID']))
#        comb = id_listB
#        dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
#        
#else:
#    dffk2 = dffk2.copy()
#End change on 09-01-2021 


# #### Finding Pairs in Up and down
sel_col = ['ViewData.Currency', 
       'ViewData.Accounting Net Amount', 'ViewData.Age', 'ViewData.Asset Type Category1',
       
        'ViewData.Cust Net Amount',
       'ViewData.BreakID', 'ViewData.Business Date', 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag', 'ViewData.ClusterID', 'ViewData.Commission',
       'ViewData.CUSIP',  
       'ViewData.Description',  'ViewData.Fund',
        'ViewData.Has Attachments',
       'ViewData.InternalComment1', 'ViewData.InternalComment2',
       'ViewData.InternalComment3', 'ViewData.Investment ID',
       'ViewData.Investment Type1', 
       'ViewData.ISIN', 'ViewData.Keys', 
       'ViewData.Mapped Custodian Account', 'ViewData.Department',
       
        'ViewData.Portfolio ID',
       'ViewData.Portolio', 'ViewData.Price', 'ViewData.Prime Broker1',
        
       'ViewData.Quantity',  'ViewData.Rule And Key',
       'ViewData.SEDOL', 'ViewData.Settle Date', 
       'ViewData.Status', 'ViewData.Strategy', 'ViewData.System Comments',
       'ViewData.Ticker', 'ViewData.Trade Date', 'ViewData.Trade Expenses',
       'ViewData.Transaction ID',
       'ViewData.Transaction Type1', 'ViewData.Underlying Cusip',
       'ViewData.Underlying Investment ID', 'ViewData.Underlying ISIN',
       'ViewData.Underlying Sedol', 'ViewData.Underlying Ticker',
       'ViewData.UserTran1', 'ViewData.UserTran2', 
       'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
       'ViewData.Task Business Date', 'final_ID',
        'ViewData.Task Business Date1',
       'ViewData.Settle Date1', 'ViewData.Trade Date1',
       'ViewData.SettlevsTrade Date', 'ViewData.SettlevsTask Date',
       'ViewData.TaskvsTrade Date','new_desc_cat', 'ViewData.Custodian', 'ViewData.Net Amount Difference Absolute', 'new_pb1'
      ,'fut_date','full_call','rate_var','month_end_mark','new_pb'
        ]

#Change added on 09-01-2021 as per Abhijeet. dffk2 and dffk3 definition had to be regiven once Cancel trade code was commented out
#Begin change on 09-01-2021 
dffk2 = df2[df2['ViewData.Side0_UniqueIds']=='AA']
dffk3 = df2[df2['ViewData.Side1_UniqueIds']=='BB']
#End change on 09-01-2021 
dff4 = dffk2[sel_col]
dff5 = dffk3[sel_col]

#dff6 = dffk4[sel_col]
#dff7 = dffk5[sel_col]
#
#dff4 = pd.concat([dff4,dff6])
#dff4 = dff4.reset_index()
#dff4 = dff4.drop('index', axis = 1)
#
#dff5 = pd.concat([dff5,dff7])
#dff5 = dff5.reset_index()
#dff5 = dff5.drop('index', axis = 1)

# #### M cross N code

###################### loop 3 ###############################
pool =[]
key_index =[]
training_df =[]
call1 = []

appended_data = []

no_pair_ids = []
#max_rows = 5

k = list(set(list(set(dff5['ViewData.Task Business Date1'])) + list(set(dff4['ViewData.Task Business Date1']))))
k1 = k

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
    
    if (row['SideA.ViewData.Settle Date1'] == row['SideB.ViewData.Settle Date1']):
        e = 1
    else:
        e = 0
        
    if (row['SideA.ViewData.Transaction Type1'] == row['SideB.ViewData.Transaction Type1']):
        f = 1
    else:
        f = 0
        
    return a, b, c ,d, e,f
    
df_213_1[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")

df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['sd_match'] + df_213_1['curr_match']
elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]

# - putting updown comments

def updownat(a,b,c,d,e):
    if a == 0:
        k = 'mapped custodian account'
    elif b==0:
        k = 'currency'
    elif c ==0 :
        k = 'Settle Date'
    elif d == 0:
        k = 'fund'    
    elif e == 0:
        k = 'transaction type'
    else :
        k = 'Investment type'
        
    com = 'up/down at'+ ' ' + k
    return com

if elim1.shape[0]!=0:
    elim1['SideA.predicted category'] = 'Updown'
    elim1['SideB.predicted category'] = 'Updown'
    elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
    elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
    elim_col = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
    
    sideA_col = []
    sideB_col = []
#    elim_col = list(elim1.columns)

    for items in elim_col:
        item = 'SideA.'+items
        sideA_col.append(item)
        item = 'SideB.'+items
        sideB_col.append(item)
        
    elim2 = elim1[sideA_col]
    elim3 = elim1[sideB_col]
    
    elim2 = elim2.rename(columns= {'SideA.final_ID':'final_ID',
                              'SideA.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
                              'SideA.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',
                              'SideA.predicted category':'predicted category',
                              'SideA.predicted comment':'predicted comment'})
    elim3 = elim3.rename(columns= {'SideB.final_ID':'final_ID',
                              'SideB.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
                              'SideB.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',                                   
                              'SideB.predicted category':'predicted category',
                              'SideB.predicted comment':'predicted comment'})
    frames = [elim2,elim3]
    elim = pd.concat(frames)
    elim = elim.reset_index()
    elim = elim.drop('index', axis = 1)
    elim['predicted action'] = 'No-Pair'
    elim['predicted status'] = 'OB'


    elim = elim[output_col]
    elim.to_csv(base_dir + 'Comment file oaktree 2 sep testing p5.csv')
    comment_df_final_list.append(elim)
    
    id_listB = list(set(elim1['SideB.final_ID'])) 
    id_listA = list(set(elim1['SideA.final_ID']))
    
    df_213_1 = df_213_1[~df_213_1['SideB.final_ID'].isin(id_listB)]
    df_213_1 = df_213_1[~df_213_1['SideA.final_ID'].isin(id_listA)]
    
    id_listB = list(set(df_213_1['SideB.final_ID'])) 
    id_listA = list(set(df_213_1['SideA.final_ID']))
    
    dff4 = dff4[dff4['final_ID'].isin(id_listB)]
    dff5 = dff5[dff5['final_ID'].isin(id_listA)]
    
else:
    dff4 = dff4.copy()
    dff5 = dff5.copy()
    
frames = [dff4,dff5]

data = pd.concat(frames)
data = data.reset_index()
data = data.drop('index', axis = 1)
data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)

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
'ViewData.InternalComment2', 'ViewData.Description','new_pb2','new_pb1','ViewData.TaskvsTrade Date'
]


#data = data[Pre_final]

df_mod1 = data.copy()

df_mod1['ViewData.TaskvsTrade Date'] = df_mod1['ViewData.TaskvsTrade Date'].fillna(0)

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

df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].replace('nan','kkk')
df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].replace('None','kkk')
df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].replace('','kkk')

df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].replace('nan','bb')
df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].replace('None','bb')
df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].replace('','bb')

df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].replace('nan',0)
df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].replace('None',0)
df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].replace('',0)

df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].replace('nan',0)
df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].replace('None',0)
df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].replace('',0)

df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].replace('nan',0)
df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].replace('None',0)
df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].replace('',0)


df_mod1['ViewData.Asset Type Category1'] = df_mod1['ViewData.Asset Type Category1'].replace('nan','CC')
df_mod1['ViewData.Asset Type Category1'] = df_mod1['ViewData.Asset Type Category1'].replace('None','CC')
df_mod1['ViewData.Asset Type Category1'] = df_mod1['ViewData.Asset Type Category1'].replace('','CC')

df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].replace('nan','DD')
df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].replace('None','DD')
df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].replace('','DD')

df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].replace('nan','EE')
df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].replace('None','EE')
df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].replace('','EE')

df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].replace('nan','FF')
df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].replace('None','FF')
df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].replace('','FF')

df_mod1['ViewData.Investment Type1'] = df_mod1['ViewData.Investment Type1'].replace('nan','GG')
df_mod1['ViewData.Investment Type1'] = df_mod1['ViewData.Investment Type1'].replace('None','GG')
df_mod1['ViewData.Investment Type1'] = df_mod1['ViewData.Investment Type1'].replace('','GG')

df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].replace('nan',0)
df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].replace('None',0)
df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].replace('',0)

df_mod1['ViewData.Prime Broker1'] = df_mod1['ViewData.Prime Broker1'].replace('nan','HH')
df_mod1['ViewData.Prime Broker1'] = df_mod1['ViewData.Prime Broker1'].replace('None','HH')
df_mod1['ViewData.Prime Broker1'] = df_mod1['ViewData.Prime Broker1'].replace('','HH')

df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].replace('nan',0)
df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].replace('None',0)
df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].replace('',0)

df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].replace('nan',0)
df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].replace('None',0)
df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].replace('',0)

df_mod1['ViewData.Transaction Type1'] = df_mod1['ViewData.Transaction Type1'].replace('nan','kk')
df_mod1['ViewData.Transaction Type1'] = df_mod1['ViewData.Transaction Type1'].replace('None','kk')
df_mod1['ViewData.Transaction Type1'] = df_mod1['ViewData.Transaction Type1'].replace('','kk')

df_mod1['ViewData.ISIN'] = df_mod1['ViewData.ISIN'].replace('nan','mm')
df_mod1['ViewData.ISIN'] = df_mod1['ViewData.ISIN'].replace('None','mm')
df_mod1['ViewData.ISIN'] = df_mod1['ViewData.ISIN'].replace('','mm')

df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].replace('nan','nn')
df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].replace('None','nn')
df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].replace('','nn')

df_mod1['ViewData.Description'] = df_mod1['ViewData.Description'].replace('nan','nn')
df_mod1['ViewData.Description'] = df_mod1['ViewData.Description'].replace('None','nn')
df_mod1['ViewData.Description'] = df_mod1['ViewData.Description'].replace('','nn')

df_mod1['ViewData.Department'] = df_mod1['ViewData.Department'].replace('nan','nn')
df_mod1['ViewData.Department'] = df_mod1['ViewData.Department'].replace('None','nn')
df_mod1['ViewData.Department'] = df_mod1['ViewData.Department'].replace('','nn')


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)

data2 = df_mod1.copy()

# Change added on 20-12-2020 as per Abhijeet. 
data2['fut_date'] = data2['fut_date'].fillna(0)
data2['full_call'] = data2['full_call'].fillna(0)
data2['rate_var'] = data2['rate_var'].fillna(0)
data2['month_end_mark'] = data2['month_end_mark'].fillna(0)

data2['fut_date'] = data2['fut_date'].astype(int)
data2['full_call'] = data2['full_call'].astype(int)
data2['rate_var'] = data2['rate_var'].astype(int)
data2['month_end_mark'] = data2['month_end_mark'].astype(int)

# ### Separate Prediction of the Trade and Non trade

# #### 1st for Non Trade

# trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']

# data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]
#Change made on 12-02-2021 as per Abhijeet to change the .sav file as well as change cols going in model file
#cols = [
# 
#
# 
# 'ViewData.Transaction Type1','ViewData.Asset Type Category1', 'ViewData.Department',
#            'ViewData.Investment Type1','new_desc_cat','new_pb1','fut_date'
# 
#              
#             ]
#
#Change made on 12-02-2021 as per Abhijeet to change the .sav file as well as change cols going in model file
cols = [
 

 
 'ViewData.Transaction Type1','ViewData.Asset Type Category1','ViewData.Department','new_desc_cat','new_pb1','fut_date'
 
              
             ]

#cols = [ 
#  'ViewData.Transaction Type1',
# 'ViewData.Asset Type Category1',
#    'ViewData.Department',
#  'ViewData.Investment Type1',
#    'new_desc_cat',
#  
# 'new_pb1'
##     ,'new_pb2'
#,'fut_date'
#    ,'full_call','rate_var','month_end_mark'
#]

data211 = data2[cols]
#filename = 'finalized_model_oaktree_overall v8 Jan 9.sav'
#Change made on 12-02-2021 as per Abhijeet to change the .sav file as well as change cols going in model file
#filename = 'finalized_model_oaktree_overall v9 feb12 final.sav'
#Change made on 18-02-2021 as per Abhijeet to change the .sav file
filename = 'finalized_model_oaktree_overall v10 feb17 final.sav'
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



#Change added on 12-01-2021 as per Abhijeet. This code will apply final rules on comments
#Begin changes made on 12-01-2021



#sell = ['Sell','sell']
#redem = ['redemption','Redemption']
#forward = ['forwardfx','forward fx']
#interest = ['int','interest','Interest']
#term_loan = ['bank debt (term loan)','bank debt','term loan']
#hedging = ['forward']
#drawdown = ['drawdown']
#tender = ['tender']
#buy = ['buy','Buy']
#sink = ['sink','Sink']
#
#
#def autocom(x,y,z,k,a,b,c):
#    if ((x in sell) & (c < 0)):
#        return 'future dated sell'
#    elif ((x in buy) & (c < 0)):
#        return 'future dated buy'
#    elif x in buy:
#        return 'buy trade'
#    elif x in sell:
#        return 'sell trade'
#    elif x in drawdown:
#        return 'drawdown'
#    elif ((x in redem) & (k=='loan')):
#        return 'redemption'
#
#    elif ((x in interest) & (y in term_loan)):
#        return 'bank loan interest'
#    elif ((x in sink)):
#        return 'sink reschedule'
#    elif ((x in interest) & (a == 'DD') & (b == 'mm')):
#        return 'monthend interest'
#    
#    else:
#        return z
#
#result_non_trade['predicted category'] = result_non_trade.apply(lambda row: autocom(row['ViewData.Transaction Type1'],row['ViewData.Investment Type1'], row['predicted category'],row['new_desc_cat'], row['ViewData.CUSIP'],row['ViewData.ISIN'],row['ViewData.TaskvsTrade Date']), axis = 1)
#result_non_trade['predicted category'] = result_non_trade.apply(lambda row: autocom(row['ViewData.Transaction Type1'],row['ViewData.Investment Type1'], row['predicted category'],row['new_desc_cat']), axis = 1)

#End changes made on 12-01-2021

#Change made on 18-02-2021 as per Abhijeet. This change is applying rule sets on the different trasactions taking place which will change the prediction category, in return changing the predicted temlpate1
#Being change made on 18-02-2021
sell = ['Sell','sell']
redem = ['redemption','Redemption']
forward = ['forwardfx','forward fx']
interest = ['int','interest','Interest']
term_loan = ['bank debt (term loan)','bank debt','term loan']
hedging = ['forward']
drawdown = ['drawdown']
tender = ['tender']
buy = ['buy','Buy']
sink = ['sink','Sink']


def autocom(x,z,k):
    k1 = k.split() 
    if (z == 'capital activity'):
        if 'subscription' in k1:
            return 'subscription'
        elif 'redemption' in k1:
            return 'redemption'
        else:
            return z
    elif (('unadvised' in  k1) & ('cash' in  k1) & ('posting' in  k1)):
        return 'unadvised cash'
    elif x in sell:
        return 'sell trade'
    elif x in buy:
        return 'buy trade'
    elif x in drawdown:
        return 'drawdown'
    elif x=='paydown':
        return 'paydown'
    else:
        return z

result_non_trade['predicted category'] = result_non_trade.apply(lambda row: autocom(row['ViewData.Transaction Type1'], row['predicted category'],row['new_desc_cat']), axis = 1)

#End change made o 18-0202021


result_non_trade = result_non_trade.drop('predicted comment', axis = 1)

#com_temp = pd.read_csv('Soros comment template for delivery.csv')
#com_temp = pd.read_csv('oaktree Comment template for delivery new jan 10.csv')
com_temp = pd.read_csv('oaktree Comment template for delivery new feb 17.csv')
com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})

result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')

#Change made on 18-02-2021 as per Abhijeet to comment our the earlier comgen function which was used earlier
#def comgen(x,y,z,k):
#    if x == 'Geneva':
#        
#        com = k + ' ' +y + ' ' + str(z)
#    else:
#        com = "Geneva" + ' ' +y + ' ' + str(z)
#        
#    return com

def comgen(x,y,z,k,m,a,b,c,d,date_i_col_val):
    trade_ttype = ['buy trade','sell trade','sell short','cover short']
    pos_break = ['future dated trade']
    type1 = ['spot fx','forward fx' ,'Cash deposit']
    type2 = ['subscription','redemption']
    
    x = x.lower()
    if m in type2:
        if x != 'geneva':
            if ((c!= 0)):
                com = str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + " " + 'with fee'+' '+ str(round(d,1))+ '. Geneva yet to book' 
            else:
                com = str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + '. Geneva yet to book'
            
        else:
            if ((c!= 0)):
                com = str(date_i_col_val) + ' ' + '-' + 'Geneva' + ' ' +str(y) + ' ' + str(z) + " " + 'with fee'+' '+ str(round(d,1)) + '.'+ str(k)+ ' yet to book' 
            else:
                com = str(date_i_col_val) + ' ' + '-' + 'Geneva' + ' ' +str(y) + ' ' + str(z) + '.'+ str(k)+ ' yet to book'
                
    elif m in type1:
        if x != 'geneva':
        
            com = str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + ".Need to check with custody and Geneva yet to book"
        else:
            com = str(date_i_col_val) + ' ' + '-' + 'Geneva' + ' ' +str(y) + ' ' + str(z)+ '.' +'Need to check with custody and'+ ' ' + str(k) + 'yet to book'
           
    elif m in trade_ttype:
        if x != 'geneva':
            if ((a!= 0) & (b!= 0)):
                com = str(date_i_col_val) + ' ' + '-' +str(k) + ' ' +str(y) + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '. Geneva yet to book' 
            elif  ((a==0) & (b!=0)):
                com =str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
            elif  ((a!=0) & (b==0)):
                com =str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
            else:
                com =str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
        else:
            if ((a!= 0) & (b!= 0)):
                com = str(date_i_col_val) + ' ' + '-' +'Geneva' + ' ' +str(y) + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '.'+ str(k)+ ' yet to book' 
            elif  ((a==0) & (b!=0)):
                com = str(date_i_col_val) + ' ' + '-' +'Geneva' + ' ' +str(y) + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ str(k)+ ' yet to book'
            elif  ((a!=0) & (b==0)):
                com = str(date_i_col_val) + ' ' + '-' +'Geneva' + ' ' +str(y) + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ str(k)+ ' yet to book'
            else:
                com = str(date_i_col_val) + ' ' + '-' +'Geneva' + ' ' +str(y) + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ str(k)+ ' yet to book'
    
    elif m in pos_break:
        
        if x != 'geneva':
        
            com = str(date_i_col_val) + ' ' + '-' + 'Timing Difference.' + ' '+ str(k) + ' ' +str(y) + ' ' + str(z) + ". Geneva' yet to book"
        else:
            com = str(date_i_col_val) + ' ' + '-' + 'Timing Difference.' + ' '+ 'Geneva' + ' ' +str(y) + ' ' + str(z)+ '.' + str(k) + 'yet to book'
        
    else:
        if x != 'geneva':
        
            com = str(date_i_col_val) + ' ' + '-' + str(k) + ' ' +str(y) + ' ' + str(z) + ". Geneva' yet to book"
        else:
            com = str(date_i_col_val) + ' ' + '-' + 'Geneva' + ' ' +str(y) + ' ' + str(z)+ '.' + str(k) + 'yet to book'
        
    return com

result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
result_non_trade['ViewData.Settle Date'] = result_non_trade['ViewData.Settle Date'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)

result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
#result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].dt.date
result_non_trade['ViewData.Settle Date_mode'] = result_non_trade['ViewData.Settle Date'].mode()[0]
result_non_trade['ViewData.Settle Date'] = result_non_trade.apply(lambda row : fill_trade_date_equals_0_with_settle_date_apply_row_379(param_row = row,param_column_to_fill_for = 'ViewData.Settle Date', param_column_to_fill_with = 'ViewData.Trade Date'), axis = 1, result_type="expand")

result_non_trade['ViewData.Settle Date2'] = pd.to_datetime(result_non_trade['ViewData.Settle Date']).dt.date
result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date2'].astype(str)
#result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].dt.date

result_non_trade['ViewData.Trade Date_mode'] = result_non_trade['ViewData.Trade Date'].mode()[0]
result_non_trade['ViewData.Trade Date'] = result_non_trade.apply(lambda row : fill_trade_date_equals_0_with_settle_date_apply_row_379(param_row = row,param_column_to_fill_for = 'ViewData.Trade Date', param_column_to_fill_with = 'ViewData.Settle Date'), axis = 1, result_type="expand")

result_non_trade['ViewData.Trade Date2'] = pd.to_datetime(result_non_trade['ViewData.Trade Date']).dt.date
result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date2'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)
#result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : brokermap(x))
result_non_trade['Task_Business_Date_for_comgen_fun'] = date_i
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date2'],x['ViewData.Commission']), axis = 1)
result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date2'],x['ViewData.Commission'],x['Task_Business_Date_for_comgen_fun']), axis = 1)
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)

#result_non_trade = result_non_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]
result_non_trade['predicted status'] = 'OB'
result_non_trade['predicted action'] = 'No-pair'

result_non_trade = result_non_trade[output_col]

result_non_trade.to_csv(base_dir + 'Comment file soros 2 sep testing p6.csv')
comment_df_final_list.append(result_non_trade)

# #### For Trade Model

# data22 = data2[data2['ViewData.Transaction Type1'].isin(trade_types)]

# data222 = data22[cols]

# filename = 'finalized_model_oaktree_trade_v1.sav'

# clf = pickle.load(open(filename, 'rb'))

# # Actual class predictions
# cb_predictions = clf.predict(data222)#.astype(str)
# # Probabilities for each class
# #cb_probs = clf.predict_proba(X_test)[:, 1]

# demo = []
# for item in cb_predictions:
#     demo.append(item[0])

# result_trade =data22.copy()

# result_trade = result_trade.reset_index()

# result_trade['predicted category'] = pd.Series(demo)
# result_trade['predicted comment'] = 'NA'

# result_trade = result_trade.drop('predicted comment', axis = 1)

# com_temp = pd.read_csv('oaktree Comment template for delivery new.csv')

# com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})

# result_trade = pd.merge(result_trade,com_temp,on = 'predicted category',how = 'left')

# def comgen(x,y,z,k):
#     if x == 'Geneva':
        
#         com = k + ' ' +y + ' ' + str(z)
#     else:
#         com = "Geneva" + ' ' +y + ' ' + str(z)
        
#     return com

# result_trade['new_pb2'] = result_trade['new_pb2'].astype(str)
# result_trade['predicted template'] = result_trade['predicted template'].astype(str)
# result_trade['ViewData.Settle Date'] = result_trade['ViewData.Settle Date'].astype(str)
# result_trade['new_pb1'] = result_trade['new_pb1'].astype(str)

# result_trade['predicted comment'] = result_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)


# result_trade = result_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]

# result_trade.to_csv('Comment file oaktree 2 sep testing p7.csv')
# comment_df_final_list.append(result_trade)

comment_df_final = pd.concat(comment_df_final_list)

change_col_names_comment_df_final_dict = {
                                        'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                        'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                        'predicted category' : 'PredictedCategory',
                                        'predicted comment' : 'PredictedComment'
                                        }

comment_df_final.rename(columns = change_col_names_comment_df_final_dict, inplace = True)

filepaths_comment_df_final = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\comment_df_final_setup_' + setup_code + '_date_' + str(date_i) + '_abhijeet_comment_change_on_09_01_2021.csv'
comment_df_final.to_csv(filepaths_comment_df_final)

comment_df_final_side0 = comment_df_final[comment_df_final['Side1_UniqueIds'] == 'BB']
comment_df_final_side1 = comment_df_final[comment_df_final['Side0_UniqueIds'] == 'AA']

final_df = final_table_to_write.merge(comment_df_final_side0, on = 'Side0_UniqueIds', how = 'left')

if('probability_UMT' not in list(final_df.columns)):
	final_df['probability_UMT'] = ''
#111111
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']
final_df['PredictedComment'] = final_df['PredictedComment_y']
final_df['PredictedCategory'] = final_df['PredictedCategory_y']

final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].astype(str)
final_df['PredictedComment'] = final_df['PredictedComment'].astype(str)
final_df['PredictedCategory'] = final_df['PredictedCategory'].astype(str)

#Change made on 22-12-2020. Below commented code is wrong
#final_df['Side1_UniqueIds_y'] = final_df['Side1_UniqueIds_y'].astype(str)
#
#final_df.loc[final_df['PredictedComment_x']=='','PredictedComment'] = final_df['PredictedComment_y']
#final_df.loc[final_df['PredictedComment_y']=='','PredictedComment'] = final_df['PredictedComment_x']
#
#final_df.loc[final_df['PredictedComment_x']=='None','PredictedComment'] = final_df['PredictedComment_y']
#final_df.loc[final_df['PredictedComment_y']=='None','PredictedComment'] = final_df['PredictedComment_x']
#
#final_df.loc[final_df['PredictedComment_x']=='nan','PredictedComment'] = final_df['PredictedComment_y']
#final_df.loc[final_df['PredictedComment_y']=='nan','PredictedComment'] = final_df['PredictedComment_x']
#
#final_df.loc[final_df['PredictedCategory_x']=='','PredictedCategory'] = final_df['PredictedCategory_y']
#final_df.loc[final_df['PredictedCategory_y']=='','PredictedCategory'] = final_df['PredictedCategory_x']
#
#final_df.loc[final_df['PredictedCategory_x']=='None','PredictedCategory'] = final_df['PredictedCategory_y']
#final_df.loc[final_df['PredictedCategory_y']=='None','PredictedCategory'] = final_df['PredictedCategory_x']
#
#final_df.loc[final_df['PredictedCategory_x']=='nan','PredictedCategory'] = final_df['PredictedCategory_y']
#final_df.loc[final_df['PredictedCategory_y']=='nan','PredictedCategory'] = final_df['PredictedCategory_x']

#final_df.loc[final_df['Side1_UniqueIds_x']=='','Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
#final_df.loc[final_df['Side1_UniqueIds_y']=='','Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']
#
#final_df.loc[final_df['Side1_UniqueIds_x']=='None','Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
#final_df.loc[final_df['Side1_UniqueIds_y']=='None','Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']
#
#final_df.loc[final_df['Side1_UniqueIds_x']=='nan','Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
#final_df.loc[final_df['Side1_UniqueIds_y']=='nan','Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']
#
#final_df.loc[final_df['Side1_UniqueIds_x'].isnull(),'Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
#final_df.loc[final_df['Side1_UniqueIds_y'].isnull(),'Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']

def columns_to_drop(fun_df_to_remove_columns_from, fun_columns_to_remove_list):
    for col in fun_columns_to_remove_list:
        if(col in list(fun_df_to_remove_columns_from.columns)):
            fun_df_to_remove_columns_from.drop(col, axis = 1, inplace = True)
    return(fun_df_to_remove_columns_from)

final_df_columns_to_remove = ['PredictedCategory_x','PredictedComment_x','Side1_UniqueIds_x','SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideA.final_ID','SideA.predicted category','SideA.predicted comment','Side1_UniqueIds_y','final_ID','predicted action','PredictedCategory_y','PredictedComment_y','predicted status']
final_df = columns_to_drop(fun_df_to_remove_columns_from = final_df, fun_columns_to_remove_list = final_df_columns_to_remove)

filepaths_final_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df.to_csv(filepaths_final_df)

final_df_2 = final_df.merge(comment_df_final_side1, on = 'Side1_UniqueIds', how = 'left')

final_df_2['PredictedComment_y'] = final_df_2['PredictedComment_y'].astype(str)
final_df_2['PredictedComment_x'] = final_df_2['PredictedComment_x'].astype(str)

final_df_2['PredictedCategory_y'] = final_df_2['PredictedCategory_y'].astype(str)
final_df_2['PredictedCategory_x'] = final_df_2['PredictedCategory_x'].astype(str)

final_df_2['Side0_UniqueIds_x'] = final_df_2['Side0_UniqueIds_x'].astype(str)
final_df_2['Side0_UniqueIds_y'] = final_df_2['Side0_UniqueIds_y'].astype(str)

final_df_2.loc[final_df_2['PredictedComment_x']=='','PredictedComment'] = final_df_2['PredictedComment_y']
final_df_2.loc[final_df_2['PredictedComment_y']=='','PredictedComment'] = final_df_2['PredictedComment_x']

final_df_2.loc[final_df_2['PredictedComment_x']=='None','PredictedComment'] = final_df_2['PredictedComment_y']
final_df_2.loc[final_df_2['PredictedComment_y']=='None','PredictedComment'] = final_df_2['PredictedComment_x']

final_df_2.loc[final_df_2['PredictedComment_x']=='nan','PredictedComment'] = final_df_2['PredictedComment_y']
final_df_2.loc[final_df_2['PredictedComment_y']=='nan','PredictedComment'] = final_df_2['PredictedComment_x']

final_df_2.loc[final_df_2['PredictedCategory_x']=='','PredictedCategory'] = final_df_2['PredictedCategory_y']
final_df_2.loc[final_df_2['PredictedCategory_y']=='','PredictedCategory'] = final_df_2['PredictedCategory_x']

final_df_2.loc[final_df_2['PredictedCategory_x']=='None','PredictedCategory'] = final_df_2['PredictedCategory_y']
final_df_2.loc[final_df_2['PredictedCategory_y']=='None','PredictedCategory'] = final_df_2['PredictedCategory_x']

final_df_2.loc[final_df_2['PredictedCategory_x']=='nan','PredictedCategory'] = final_df_2['PredictedCategory_y']
final_df_2.loc[final_df_2['PredictedCategory_y']=='nan','PredictedCategory'] = final_df_2['PredictedCategory_x']

final_df_2.loc[final_df_2['Side0_UniqueIds_x']=='','Side0_UniqueIds'] = final_df_2['Side0_UniqueIds_y']
final_df_2.loc[final_df_2['Side0_UniqueIds_y']=='','Side0_UniqueIds'] = final_df_2['Side0_UniqueIds_x']

final_df_2.loc[final_df_2['Side0_UniqueIds_x']=='None','Side0_UniqueIds'] = final_df_2['Side0_UniqueIds_y']
final_df_2.loc[final_df_2['Side0_UniqueIds_y']=='None','Side0_UniqueIds'] = final_df_2['Side0_UniqueIds_x']

final_df_2.loc[final_df_2['Side0_UniqueIds_x']=='nan','Side0_UniqueIds'] = final_df_2['Side0_UniqueIds_y']
final_df_2.loc[final_df_2['Side0_UniqueIds_y']=='nan','Side0_UniqueIds'] = final_df_2['Side0_UniqueIds_x']


final_df_2.drop(['PredictedComment_x','PredictedComment_y', \
               'PredictedCategory_x','PredictedCategory_y', \
               'Side0_UniqueIds_x','Side0_UniqueIds_y'], axis = 1, inplace = True)

    
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

cols_to_remove_newline_char_from = ['Side1_UniqueIds','Side0_UniqueIds','BreakID']
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
final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('NaN','')

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

final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])

final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(float)
final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(np.int64)

final_df_2['ReconSetupName'] = 'SMA Custodian Cash Recon'
final_df_2['ClientShortCode'] = 'Oaktree'

today = date.today()
today_Y_m_d = today.strftime("%Y-%m-%d")

final_df_2['CreatedDate'] = today_Y_m_d
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
final_df_2['CreatedDate'] = final_df_2['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])

#UMR_One_to_One separation into UMT_One_to_One
#final_df_2.loc[final_df_2['Predicted_action'] == 'UMR_One_to_One', ''] =

#Fixing 'Not_Covered_by_ML' Statuses
Search_term = 'not_covered_by_ml'

final_df_2_Covered_by_ML_df = final_df_2[~final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

final_df_2_Not_Covered_by_ML_df = final_df_2[final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

def get_first_term_before_separator(single_string, separator):
    return(single_string.split(separator)[0])

final_df_2_Not_Covered_by_ML_df['Predicted_Status'] = final_df_2_Not_Covered_by_ML_df['Predicted_Status'].apply(lambda x : get_first_term_before_separator(x,'_'))
final_df_2_Not_Covered_by_ML_df['ML_flag'] = 'ML'

final_df_2 = final_df_2_Covered_by_ML_df.append(final_df_2_Not_Covered_by_ML_df)

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace('\.0','',regex = True)

umb_ids_to_convert_to_smb = list(final_df_2[((final_df_2['Final_predicted_break'] == '') & (final_df_2['Predicted_Status'] == 'UMB') & (final_df_2['PredictedComment'] == ''))]['BreakID'])

final_df_2.loc[(final_df_2['BreakID'].isin(umb_ids_to_convert_to_smb)),'Predicted_Status'] = 'SMB'
final_df_2.loc[(final_df_2['BreakID'].isin(umb_ids_to_convert_to_smb)),'Predicted_action'] = 'SMB'

######### For Oaktree #######################

def smb_comment(sd,pb,tt):
    #sd = pd.datetime(sd)
    tt = str(tt)
    if tt.lower() == 'dividend':
        comment = 'Difference in DVD booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
    elif tt.lower() == 'dividendincome':
        comment = 'Custody to advise on the negative dividend reflected.'
    elif tt.lower() in ['corp action - call','corp action']:
        comment = 'Custody to advise on the corp. action'
    elif tt.lower() =='interest':
        comment =  'Interest mismatch, checking with custody to clear this cash difference'
    else:
        comment = 'Difference in amount booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
    return comment

#####################################################################################################################
smb_breakids_str = list(final_df_2[final_df_2['Predicted_Status'] == 'SMB']['BreakID'])
smb_breakids_int = [int(x) for x in smb_breakids_str]

meo_df_smb_comment = meo_df[meo_df['ViewData.BreakID'].isin(smb_breakids_int)][['ViewData.BreakID','ViewData.Settle Date','ViewData.Prime Broker','ViewData.Transaction Type']]

meo_df_smb_comment['PredictedComment_SMB'] = meo_df_smb_comment.apply(lambda x: smb_comment(x['ViewData.Settle Date'],x['ViewData.Prime Broker'],x['ViewData.Transaction Type']),axis=1)
meo_df_smb_comment['ViewData.BreakID']  = meo_df_smb_comment['ViewData.BreakID'].astype(str)

meo_df_smb_comment.drop(columns = ['ViewData.Settle Date', 'ViewData.Prime Broker','ViewData.Transaction Type'], axis = 1, inplace = True)
meo_df_smb_comment.rename(columns = {'ViewData.BreakID' : 'BreakID'}, inplace = True)

final_df_2 = pd.merge(final_df_2,meo_df_smb_comment, on = 'BreakID', how = 'left')
final_df_2['PredictedComment_SMB'] = final_df_2['PredictedComment_SMB'].astype(str)

final_df_2['PredictedComment_SMB'] = final_df_2['PredictedComment_SMB'].replace('nan','',regex = True)
final_df_2['PredictedComment_SMB'] = final_df_2['PredictedComment_SMB'].replace('None','',regex = True)

def predicted_comment_smb_integrate_row_apply(fun_row):
	if(fun_row['Predicted_Status'] == 'SMB'):
		return(fun_row['PredictedComment_SMB'])
	else:
		return(fun_row['PredictedComment'])
		

final_df_2['PredictedComment'] = final_df_2.apply(lambda row : predicted_comment_smb_integrate_row_apply(row),axis = 1,result_type="expand")


#UMB commenting
not_umb_breakids_for_commenting_df = final_df_2[~((final_df_2['Predicted_Status'] == 'UMB') & (final_df_2['PredictedComment'] == ''))]

umb_breakids_for_commenting_df = final_df_2[((final_df_2['Predicted_Status'] == 'UMB') & (final_df_2['PredictedComment'] == ''))]

umb_breakids_for_commenting_df['Side0ID_to_use_in_commenting'] = umb_breakids_for_commenting_df['Side0_UniqueIds'].apply(lambda x : x.split(',')[0].replace('[','').replace(']','').replace('\'',''))
umb_breakids_for_commenting_df['Side1ID_to_use_in_commenting'] = umb_breakids_for_commenting_df['Side1_UniqueIds'].apply(lambda x : x.split(',')[0].replace('[','').replace(']','').replace('\'',''))

def breakid_corresponding_to_side01id(fun_side01_ids,fun_side,fun_meo_df):
	if(fun_side == 0):
		return(fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'] == fun_side01_ids]['ViewData.BreakID'])
	elif(fun_side == 1):
		return(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'] == fun_side01_ids]['ViewData.BreakID'])
		
umb_breakids_for_commenting_df['BreakID0_to_use_in_commenting'] = umb_breakids_for_commenting_df['Side0ID_to_use_in_commenting'].apply(lambda x : str(list(breakid_corresponding_to_side01id(x,0,meo_df))[0]).replace('[','').replace(']','').replace('\'',''))
umb_breakids_for_commenting_df['BreakID1_to_use_in_commenting'] = umb_breakids_for_commenting_df['Side1ID_to_use_in_commenting'].apply(lambda x : str(list(breakid_corresponding_to_side01id(x,1,meo_df))[0]).replace('[','').replace(']','').replace('\'',''))


#umb_breakids_for_commenting_df['BreakID_number_of_breaks'] = umb_breakids_for_commenting_df['BreakID'].apply(lambda x : x.count(',') + 1)
#umb_breakids_for_commenting_df['Final_predicted_break_number_of_breaks'] = umb_breakids_for_commenting_df['Final_predicted_break'].apply(lambda x : x.count(',') + 1)

#As per Pratik, UMB will either be oto or otm/mto. They wont be mtm

def umb_comment(bk0,bk1):
    #sd = pd.datetime(sd)
    dummy0 = meo[meo['ViewData.BreakID']==bk0]
    dummy1 = meo[meo['ViewData.BreakID']==bk1]
    
    tt = str(dummy0['ViewData.Transaction Type'].values[0])
    pb = dummy1['ViewData.Prime Broker'].values[0]
    sd = dummy1['ViewData.Settle Date'].values[0]
    
    if tt.lower() == 'dividend':
        comment = 'Difference in DVD booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
    elif tt.lower() == 'dividendincome':
        comment = 'Custody to advise on the negative dividend reflected.'
    elif tt.lower() in ['corp action - call','corp action']:
        comment = 'Custody to advise on the corp. action'
    elif tt.lower() =='interest':
        comment =  'Interest mismatch, checking with custody to clear this cash difference'
    else:
        comment = 'Difference in amount booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)  
    return comment

umb_breakids_for_commenting_df['PredictedComment'] = umb_breakids_for_commenting_df.apply(lambda x: umb_comment(int(x['BreakID0_to_use_in_commenting']),int(x['BreakID0_to_use_in_commenting'])),axis=1)

umb_breakids_for_commenting_df.drop(columns = ['Side0ID_to_use_in_commenting','Side1ID_to_use_in_commenting','BreakID1_to_use_in_commenting', 'BreakID0_to_use_in_commenting'], axis = 1, inplace = True)

final_df_2 = umb_breakids_for_commenting_df.append(not_umb_breakids_for_commenting_df)




#Change made by Rohit on 07-12-2020 to add action columns for UI
def ui_action_column(param_final_df):
    param_final_df.loc[((param_final_df['ML_flag'] == 'Not_Covered_by_ML')),'ActionType'] = 'No Prediction'    
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['OB','SMB'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'No Action'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['OB','SMB'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'COMMENT'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'CLOSE'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR WITH COMMENT'
    param_final_df['ActionType'] = param_final_df['ActionType'].astype(str)
    return(param_final_df)    

def ui_action_column(param_final_df):
    param_final_df.loc[((param_final_df['ML_flag'] == 'Not_Covered_by_ML')),'ActionType'] = 'No Prediction'    
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['OB','SMB'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'No Action'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['OB','SMB'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'COMMENT'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'CLOSE'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR WITH COMMENT'
    param_final_df.loc[(param_final_df['ActionType'].isnull()),'ActionType'] = 'Status not covered'
    param_final_df.loc[(param_final_df['ActionType'].isna()),'ActionType'] = 'Status not covered'
    param_final_df['ActionType'] = param_final_df['ActionType'].astype(str)

    param_final_df.loc[((param_final_df['ML_flag'] == 'Not_Covered_by_ML')),'ActionTypeCode'] = 7    
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['OB','SMB'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 6
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['OB','SMB'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 3
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 2
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 1
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 4
    param_final_df.loc[(param_final_df['ActionTypeCode'].isnull()),'ActionTypeCode'] = 0
    param_final_df.loc[(param_final_df['ActionTypeCode'].isna()),'ActionTypeCode'] = 0
    param_final_df['ActionTypeCode'] = param_final_df['ActionTypeCode'].astype(int)

    return(param_final_df)    

final_df_2= ui_action_column(final_df_2)

#Started at 17:42

#final_df_2[final_df_2['Predicted_action'] == 'UMR_Many_to_Many']['Final_predicted_break'].apply(lambda x : x.replace(' ','').split(','))

final_df_2_copy = final_df_2.copy()
def set_intersection_of_two_columns(row):
    return(row['set_BreakID'].intersection(row['set_Final_predicted_break']))

def set_union_of_two_columns(row):
    return(row['set_BreakID'].union(row['set_Final_predicted_break']))

def remove_single_breakid_from_string_column_value(fun_string_column_value,fun_string_single_breakid_to_remove):
    lst = fun_string_column_value.replace(' ','').split(',')
    if(fun_string_single_breakid_to_remove in lst):    
        lst.remove(fun_string_single_breakid_to_remove)
        lst_joined_string = ", ".join(lst)
        return(lst_joined_string)
    else:
        return(fun_string_column_value)

def clean_list_of_BreakID_to_form_insertable_into_db(fun_str_list):
    fun_str_list_to_str_values = str(fun_str_list)
    fun_str_list_to_str_values = fun_str_list_to_str_values.replace('{','').replace('}','').replace('\'','').replace('[','').replace(']','')
    return(fun_str_list_to_str_values)
    
def remove_or_not_remove_BreakIDs_from_BreakID_and_FinalPredictedBreakID_columns(row):
    if(row['len_intersection_set_of_BreakID_and_FinalPredictedBreak'] == 0):
        return(clean_list_of_BreakID_to_form_insertable_into_db(row['BreakID']),clean_list_of_BreakID_to_form_insertable_into_db(row['Final_predicted_break']))
    elif(row['len_intersection_set_of_BreakID_and_FinalPredictedBreak'] == 1):
        strig_value_of_intersection_set_of_BreakID_and_FinalPredictedBreak = str(row['intersection_set_of_BreakID_and_FinalPredictedBreak']).replace('{','').replace('}','').replace('\'','')
        print(row['Final_predicted_break'])
        row['Final_predicted_break'] = remove_single_breakid_from_string_column_value(fun_string_column_value = row['Final_predicted_break'],fun_string_single_breakid_to_remove = strig_value_of_intersection_set_of_BreakID_and_FinalPredictedBreak)
        return(clean_list_of_BreakID_to_form_insertable_into_db(row['BreakID']),clean_list_of_BreakID_to_form_insertable_into_db(row['Final_predicted_break']))
    elif(row['len_intersection_set_of_BreakID_and_FinalPredictedBreak'] > 1):
        row['BreakID'] = list(row['union_set_of_BreakID_and_FinalPredictedBreak'])[0]        
        row['Final_predicted_break'] = list(row['union_set_of_BreakID_and_FinalPredictedBreak'])[1:]        
        return(clean_list_of_BreakID_to_form_insertable_into_db(row['BreakID']),clean_list_of_BreakID_to_form_insertable_into_db(row['Final_predicted_break']))

def UMB_and_SMB_duplication_issue_in_BreakID_and_FinalPredictedBreakID_columns(fun_final_df_2):#, fun_meo_df = None):
    fun_final_df_2['set_Final_predicted_break'] = fun_final_df_2['Final_predicted_break'].apply(lambda x : set(x.replace(' ','').split(',')))
    fun_final_df_2['set_BreakID'] = fun_final_df_2['BreakID'].apply(lambda x : set(x.replace(' ','').split(',')))
    fun_final_df_2['intersection_set_of_BreakID_and_FinalPredictedBreak'] = fun_final_df_2.apply(lambda row : set_intersection_of_two_columns(row), axis = 1)
    fun_final_df_2['union_set_of_BreakID_and_FinalPredictedBreak'] = fun_final_df_2.apply(lambda row : set_union_of_two_columns(row), axis = 1)
    fun_final_df_2['len_intersection_set_of_BreakID_and_FinalPredictedBreak'] = fun_final_df_2['intersection_set_of_BreakID_and_FinalPredictedBreak'].apply(lambda x : len(x))
    fun_final_df_2[['BreakID_new','Final_predicted_break_new']] = fun_final_df_2.apply(lambda row : remove_or_not_remove_BreakIDs_from_BreakID_and_FinalPredictedBreakID_columns(row), axis = 1,result_type="expand")
    return(fun_final_df_2)

umb_smb_duplication_issue_df = UMB_and_SMB_duplication_issue_in_BreakID_and_FinalPredictedBreakID_columns(fun_final_df_2 = final_df_2_copy)
filepaths_umb_smb_duplication_issue_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_smb_duplication_issue_df_setup_' + setup_code + '_date_' + str(date_i) + '_for_Inzi_closed_analysis_5.csv'
umb_smb_duplication_issue_df.to_csv(filepaths_umb_smb_duplication_issue_df)

colums_for_final_df_2 = ['len_intersection_set_of_BreakID_and_FinalPredictedBreak','BreakID_new','BusinessDate','Final_predicted_break_new','ML_flag','Predicted_Status','Predicted_action','SetupID','SourceCombinationCode','TaskID','probability_No_pair','probability_UMB','probability_UMR','probability_UMT','Side1_UniqueIds','PredictedComment','PredictedCategory','Side0_UniqueIds','ActionType','ActionTypeCode']


final_df_2 = umb_smb_duplication_issue_df[colums_for_final_df_2]
final_df_2.rename(columns = {'BreakID_new' : 'BreakID', 'Final_predicted_break_new' : 'Final_predicted_break'}, inplace = True)



filepaths_final_df_2_before_making_umt_from_umr = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_before_making_umt_from_umr_setup_' + setup_code + '_date_' + str(date_i) + '_after_changes.csv'
final_df_2.to_csv(filepaths_final_df_2_before_making_umt_from_umr)

#final_df_2[['Predicted_Status_new','Predicted_action_new']] = final_df_2.apply(lambda row : get_NetAmountDifference_for_BreakIds_from_BreakID_and_FinalPredictedBreakID_column_apply_row(fun_row = row, fun_meo_df = meo_df), axis = 1,result_type="expand")


#final_df_2[['Predicted_Status_new','Predicted_action_new','Sum_of_Net_Amount_Difference']] = final_df_2.apply(lambda row : get_NetAmountDifference_for_BreakIds_from_BreakID_and_FinalPredictedBreakID_column_apply_row(fun_row = row, fun_meo_df = meo_df), axis = 1,result_type="expand")

#def get_NetAmountDifference_for_Side01UniqueIds_from_Side0UniqueId_and_Side0UniqueId_column_apply_row(fun_row, fun_meo_df):
#    side0_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side0_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
#    side1_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side1_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
#    lst_Net_Amount_Difference_for_side0_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin(side0_UniqueIds_list_str)]['ViewData.Accounting Net Amount'].unique())
#    lst_Net_Amount_Difference_for_side1_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(side1_UniqueIds_list_str)]['ViewData.B-P Net Amount'].unique())
#    amount_diff_of_sum_of_side_0_1_lists = sum(lst_Net_Amount_Difference_for_side0_UniqueIds_list_str) - sum(lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)
#    rounded_amount_diff_of_sum_of_side_0_1_lists = round(amount_diff_of_sum_of_side_0_1_lists,3)
#    if(fun_row['Predicted_Status'] == 'UMR'):
#        if((abs(rounded_amount_diff_of_sum_of_side_0_1_lists) >= 0.01) and (abs(rounded_amount_diff_of_sum_of_side_0_1_lists) <= 1.00)):
#            Predicted_Status_new = 'UMT'
#            Predicted_action_new = fun_row['Predicted_action'].replace('UMR','UMT')
#        else:
#            Predicted_Status_new = fun_row['Predicted_Status'] 
#            Predicted_action_new = fun_row['Predicted_action']
#    else:
#        Predicted_Status_new = fun_row['Predicted_Status'] 
#        Predicted_action_new = fun_row['Predicted_action']
#
#    return(Predicted_Status_new, Predicted_action_new, rounded_amount_diff_of_sum_of_side_0_1_lists,lst_Net_Amount_Difference_for_side0_UniqueIds_list_str,lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)
meo_df.rename(columns = {'ViewData.Cust Net Amount' : 'ViewData.B-P Net Amount'}, inplace = True)
def get_NetAmountDifference_for_Side01UniqueIds_from_Side0UniqueId_and_Side0UniqueId_column_apply_row(fun_row, fun_meo_df):
    side0_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side0_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
    side1_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side1_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
    lst_Net_Amount_Difference_for_side0_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin(side0_UniqueIds_list_str)]['ViewData.Accounting Net Amount'].unique())
    lst_Net_Amount_Difference_for_side1_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(side1_UniqueIds_list_str)]['ViewData.B-P Net Amount'].unique())
    amount_diff_of_sum_of_side_0_1_lists = sum(lst_Net_Amount_Difference_for_side0_UniqueIds_list_str) - sum(lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)
    rounded_amount_diff_of_sum_of_side_0_1_lists = round(amount_diff_of_sum_of_side_0_1_lists,3)
    if((fun_row['Predicted_Status'] == 'UMR') | (fun_row['Predicted_Status'] == 'UMB')):
        if((abs(rounded_amount_diff_of_sum_of_side_0_1_lists) > 0.00) and (abs(rounded_amount_diff_of_sum_of_side_0_1_lists) <= 0.25)):
            Predicted_Status_new = 'UMT'
            Predicted_action_new = fun_row['Predicted_action'].replace('UMR','UMT')
        else:
            Predicted_Status_new = fun_row['Predicted_Status'] 
            Predicted_action_new = fun_row['Predicted_action']
    else:
        Predicted_Status_new = fun_row['Predicted_Status'] 
        Predicted_action_new = fun_row['Predicted_action']

    return(Predicted_Status_new, Predicted_action_new, rounded_amount_diff_of_sum_of_side_0_1_lists,lst_Net_Amount_Difference_for_side0_UniqueIds_list_str,lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)

final_df_2[['Predicted_Status_new','Predicted_action_new','Sum_of_Net_Amount_Difference','lst_Net_Amount_Difference_for_side0_UniqueIds','lst_Net_Amount_Difference_for_side1_UniqueIds']] = final_df_2.apply(lambda row : get_NetAmountDifference_for_Side01UniqueIds_from_Side0UniqueId_and_Side0UniqueId_column_apply_row(fun_row = row, fun_meo_df = meo_df), axis = 1,result_type="expand")

filepaths_final_df_2_after_making_umt_from_umr = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_after_making_umt_from_umr_setup_' + setup_code + '_date_' + str(date_i) + '_after_changes2.csv'
final_df_2.to_csv(filepaths_final_df_2_after_making_umt_from_umr)

final_df_2.drop(columns = ['Predicted_Status','Predicted_action'], axis = 1, inplace = True)

final_df_2.rename(columns = {'Predicted_Status_new' : 'Predicted_Status','Predicted_action_new' : 'Predicted_action'}, inplace = True)

final_df_2.drop(columns = ['Sum_of_Net_Amount_Difference','lst_Net_Amount_Difference_for_side0_UniqueIds','lst_Net_Amount_Difference_for_side1_UniqueIds'], axis = 1, inplace = True)

final_df_2_copy_2 = final_df_2.copy()
final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].astype(str)

final_df_2.loc[final_df_2['ActionType'] == 'Status not covered','ML_flag'] = 'Not_Covered_by_ML'
filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_after_changes.csv'
final_df_2.to_csv(filepaths_final_df_2)
#
#
#
#coll_1_for_writing_prediction_data = db_for_MEO_data['MLPrediction_Cash']
#Changes asked by Shitanshu for db on 24-03-2021
coll_1_for_writing_prediction_data = db_for_writing_MEO_data['MLPrediction_' + setup_code]
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing = db_2_for_MEO_data_MLReconDB_Testing['MLPrediction_Cash']



coll_1_for_writing_prediction_data.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})

#Break up final_df_2 into two parts : 
#    Part 1. final_df_2_without_UMR_Many_One_to_One_Many since it will contain BreakId as int64
#    Part 2. final_df_2_with_UMR_Many_One_to_One_Many since it will contain BreakId as list for Many to One and Many to Many breaks

final_df_2_without_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'] != 'UMR_One-Many_to_Many-One']
if(final_df_2_without_UMR_Many_One_to_One_Many.shape[0] != 0):

	final_df_2_without_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_without_UMR_Many_One_to_One_Many['BreakID'].astype(str)
	final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)
	
	data_dict_without_UMR_Many_One_to_One_Many = final_df_2_without_UMR_Many_One_to_One_Many.to_dict("records_final_1")
	coll_1_for_writing_prediction_data.insert_many(data_dict_without_UMR_Many_One_to_One_Many) 
	
	data_dict_without_UMR_Many_One_to_One_Many_for_testingdb = final_df_2_without_UMR_Many_One_to_One_Many.to_dict("records_final_1_for_testingdb")
	coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.insert_many(data_dict_without_UMR_Many_One_to_One_Many_for_testingdb) 


final_df_2_with_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'].isin(['UMR_One-Many_to_Many-One','UMR_Many_to_Many'])]
if(final_df_2_with_UMR_Many_One_to_One_Many.shape[0] != 0):
	final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].astype(str)
	final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)
	final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
	final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))
	
	data_dict_with_UMR_Many_One_to_One_Many = final_df_2_with_UMR_Many_One_to_One_Many.to_dict("records_final_2")
	coll_1_for_writing_prediction_data.insert_many(data_dict_with_UMR_Many_One_to_One_Many) 
	
	data_dict_with_UMR_Many_One_to_One_Many_for_testingdb = final_df_2_with_UMR_Many_One_to_One_Many.to_dict("records_final_2_for_testingdb")
	coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.insert_many(data_dict_with_UMR_Many_One_to_One_Many_for_testingdb) 

print(setup_code)
print(date_i)
'''
os.chdir('D:\\ViteosModel')

mngdb_137_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_137_server.connect_with_or_without_ssh()
ReconDB_ML_137_server = mngdb_137_server.client['ReconDB_ML']
ReconDB_ML_Testing_137_server = mngdb_137_server.client['ReconDB_ML_Testing']

mngdb_prod_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb05.viteos.com', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_prod_server.connect_with_or_without_ssh()
ReconDB_prod_server = mngdb_prod_server.client['ReconDB']


#1. Drop the following collections
## i. Tasks in ReconDB_ML_Testing db
Tasks_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['Tasks']
Tasks_ReconDB_ML_Testing_137_server.drop()

## ii. RecData_<setup_code> in ReconDB_ML_Testing db
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]
RecData_Setup_Code_ReconDB_ML_Testing_137_server.drop()

## iii. RecData_<setup_code>_Audit in ReconDB_ML_Testing_db
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server.drop()

#2. Fixing Tasks collection in 137 Testing so as to reflect Tasks corresponding to client,setup_code and date
query_for_Task_collection = ReconDB_ML_137_server['Tasks'].find({ "BusinessDate": getDateTimeFromISO8601String(penultimate_date_to_analyze_ymd_iso_18_30_format), 
                                                                  "ReconSetupCode": setup_code },
                                                                 {'_createdBy' : 1,
                                                                  '_updatedBy' : 1,
                                                                  '_version' : 1,
                                                                  '_createdAt' : 1,
                                                                  '_updatedAt' : 1,
                                                                  '_IPAddress' : 1,
                                                                  '_MACAddress' : 1,
                                                                  'RequestId' : 1,
                                                                  'InstanceID' : 1,
                                                                  'SourceCombinationCode' : 1,
                                                                  'ReconSetupId' : 1,
                                                                  'ReconSetupCode' : 1,
                                                                  'ReconSetupForTask' : 1,
                                                                  'KnowledgeDate' : 1,
                                                                  'BusinessDate' : 1,
                                                                  'RunDate' : 1,
                                                                  'Frequency' : 1,
                                                                  'RecType' : 1,
                                                                  'SourceDataMappings' : 1,
                                                                  'SourceCombination' : 1,
                                                                  'SourceDataOperations' : 1,
                                                                  'PreAcctMapSourceDataOperations' : 1,
                                                                  'AccountMappings' : 1,
                                                                  'DBFileName' : 1,
                                                                  'ErrorCode' : 1,
                                                                  'Status' : 1,
                                                                  'ErrorMessage' : 1,
                                                                  'FileLoadStatus' : 1,
                                                                  'DataPreparationStatus' : 1,
                                                                  'ReconRunStatus' : 1,                                                                  
                                                                  'BreakManagementStatus' : 1,
                                                                  'PublishStatus' : 1,
                                                                  'FrequencyType' : 1,
                                                                  'IsUndone' : 1,
                                                                  'IsCashRec' : 1,
                                                                  'IsOTERec' : 1,
                                                                  'IsMigrationTask' : 1,
                                                                  'ParentTaskId' : 1,
                                                                  'IsFirstSourceCombination' : 1,
                                                                  'IsIncrementalRec' : 1,
                                                                  'IsManualRun' : 1,
                                                                  'ETLInfo' : 1,
                                                                  'Labels' : 1,
                                                                  'OTEDetails' : 1,
                                                                  'PublishData' : 1,
                                                                  'HostName' : 1,
                                                                  'ProcessID' : 1})
list_of_dicts_query_for_Task_collection = list(query_for_Task_collection)
list_instance_ids = [list_of_dicts_query_for_Task_collection[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_query_for_Task_collection))]
list_version = [list_of_dicts_query_for_Task_collection[i].get('_version',{}) for i in range(0,len(list_of_dicts_query_for_Task_collection))]

#Update all values of '_version' to 2 as the comparison report requires the value of '_version' column to be greater than 0. We have randomly chosen 2 to keep uniformity.     
for d in list_of_dicts_query_for_Task_collection:
    d.update((k, int(2)) for k, v in d.items() if k == '_version')

Tasks_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['Tasks']
Tasks_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_Task_collection) 

RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]

print(list_instance_ids)
#3. Getting AUA data from prod and dumping results into RecData_<setup_code>_Audit collection in 137 Testing corresponding to client,setup_code and date

query_for_AUA_data = ReconDB_prod_server['HST_RecData_' + setup_code].find({ 'TaskInstanceID': { '$in': list_instance_ids }, 'ViewData' : { '$ne': None}, 'LastPerformedAction' : { '$ne' : 31 }, 'MatchStatus': { '$nin' : [1,2,18,19,20,21] } },
                                                                           {'_createdBy' : 1,
                                                                            '_updatedBy' : 1,
                                                                            '_version' : 1,
                                                                            '_createdAt' : 1,
                                                                            '_updatedAt' : 1,
                                                                            '_isLocked' : 1,
                                                                            '_IPAddress' : 1,
                                                                            '_MACAddress' : 1,
                                                                            'DataSides' : 1,
                                                                            'MetaData' : 1,
                                                                            'MatchStatus' : 1,
                                                                            'Priority' : 1,
                                                                            'SystemComments' : 1,
                                                                            'BreakID' : 1,
                                                                            'CombiningData' : 1,
                                                                            'ClusterID' : 1,
                                                                            'SPMID' : 1,
                                                                            'KeySet' : 1,
                                                                            'RuleName' : 1,
                                                                            'Age' : 1,
                                                                            'InternalComment1' : 1,
                                                                            'InternalComment2' : 1,
                                                                            'InternalComment3' : 1,
                                                                            'ExternalComment1' : 1,
                                                                            'ExternalComment2' : 1,
                                                                            'ExternalComment3' : 1,
                                                                            'Differences' : 1,
                                                                            'TaskInstanceID' : 1,
                                                                            'ReviewData' : 1,
                                                                            'PublishData' : 1,
                                                                            'AssigmentData' : 1,
                                                                            'SourceCombinationCode' : 1,
                                                                            'LinkedBreaks' : 1,
                                                                            'WorkflowData' : 1,
                                                                            'LastPerformedAction' : 1,
                                                                            'AttributeTolerance' : 1,
                                                                            'Attachments' : 1,
                                                                            'ViewData' : 1,
                                                                            'Age2' : 1,
                                                                            'IsManualActionUploadData' : 1,
                                                                            'IsManualBreakUploadData' : 1,
                                                                            '_parentID' : 1})

list_of_dicts_query_for_AUA_data = list(query_for_AUA_data)
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_AUA_data)

#3. Getting AUA data from prod and dumping results into RecData_<setup_code>_Audit collection in 137 Testing corresponding to client,setup_code and date

query_for_MEO_data = ReconDB_ML_137_server['RecData_' + setup_code + '_Historic'].find({ 'TaskInstanceID': { '$in': list_instance_ids }},
                                                                           {'_createdBy' : 1,
                                                                            '_updatedBy' : 1,
                                                                            '_version' : 1,
                                                                            '_createdAt' : 1,
                                                                            '_updatedAt' : 1,
                                                                            '_isLocked' : 1,
                                                                            '_IPAddress' : 1,
                                                                            '_MACAddress' : 1,
                                                                            'DataSides' : 1,
                                                                            'MetaData' : 1,
                                                                            'MatchStatus' : 1,
                                                                            'Priority' : 1,
                                                                            'SystemComments' : 1,
                                                                            'BreakID' : 1,
                                                                            'CombiningData' : 1,
                                                                            'ClusterID' : 1,
                                                                            'SPMID' : 1,
                                                                            'KeySet' : 1,
                                                                            'RuleName' : 1,
                                                                            'Age' : 1,
                                                                            'InternalComment1' : 1,
                                                                            'InternalComment2' : 1,
                                                                            'InternalComment3' : 1,
                                                                            'ExternalComment1' : 1,
                                                                            'ExternalComment2' : 1,
                                                                            'ExternalComment3' : 1,
                                                                            'Differences' : 1,
                                                                            'TaskInstanceID' : 1,
                                                                            'ReviewData' : 1,
                                                                            'PublishData' : 1,
                                                                            'AssigmentData' : 1,
                                                                            'SourceCombinationCode' : 1,
                                                                            'LinkedBreaks' : 1,
                                                                            'WorkflowData' : 1,
                                                                            'LastPerformedAction' : 1,
                                                                            'AttributeTolerance' : 1,
                                                                            'Attachments' : 1,
                                                                            'ViewData' : 1,
                                                                            'Age2' : 1,
                                                                            'IsManualActionUploadData' : 1,
                                                                            'IsManualBreakUploadData' : 1,
                                                                            '_parentID' : 1})

list_of_dicts_query_for_MEO_data = list(query_for_MEO_data)
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]
RecData_Setup_Code_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_MEO_data)

 
#query_for_PredictionInfo_exists_in_Testing = ReconDB_ML_Testing_137_server['RecData_' + setup_code].find({'PredictionInfo' : { '$exists' : True}},{})
#PredictionInfo_count = query_for_PredictionInfo_exists_in_Testing.count_documents(True)
count_docs = ReconDB_ML_Testing_137_server['RecData_' + setup_code].count_documents({'PredictionInfo': {'$exists' : True}})
print(count_docs)

#meo_df = json_normalize(list_of_dicts_query_for_MEO_data)
#meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
#meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + str(client) + '/meo_df_setup_' + str(setup_code) +'_date_' + date_to_analyze_ymd_format + '.csv'
#meo_df.to_csv(meo_filename)
'''