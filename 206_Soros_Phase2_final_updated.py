#!/usr/bin/env python
# coding: utf-8

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 19:12:48 2020

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

cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
#'Activity Code',
'Task ID', 'Source Combination Code',
'Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount',
'B-P Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
'Derived Source','Description','Department','ExpiryDate','ExternalComment1','ExternalComment2',
'ExternalComment3','Fund',
#'Interest Amount',
'InternalComment1','InternalComment2',
'InternalComment3','Investment Type','Is Combined Data','ISIN','Keys',
'Mapped Custodian Account','Net Amount Difference','Net Amount Difference Absolute','Non Trade Description',
#'OTE Custodian Account',
#'Predicted Action','Predicted Status','Prediction Details',
'Price','Prime Broker',
'Quantity','SEDOL','Settle Date','SPM ID','Status',
'System Comments','Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


# In[3]:


new_cols = ['ViewData.' + x for x in cols] + add


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

trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']

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

# In[3]:


pd.set_option('display.max_columns',500)
pd.set_option('display.max_rows',500)


# In[4]:


pd.options.display.max_colwidth = 1000


# In[5]:


pd.options.display.float_format = '{:.5f}'.format


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

def cleaned_meo(#fun_filepath_meo, 
                fun_meo_df):
#    meo = pd.read_csv(fun_filepath_meo)           .drop_duplicates()           .reset_index()           .drop('index',1)
    meo = fun_meo_df
    meo = normalize_bp_acct_col_names(fun_df = meo)
    
#    Commened out below line on 26-11-2020 to exclude SPM from closed coverage, and added the line below the commened line
#    meo = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
    meo = meo[~meo['ViewData.Status'].isin(['SPM','SMT','HST', 'OC', 'CT', 'Archive','SMR'])] 
    meo = meo[~meo['ViewData.Status'].isnull()]           .reset_index()           .drop('index',1)
    
    meo['Date'] = pd.to_datetime(meo['ViewData.Task Business Date'])
    meo = meo[~meo['Date'].isnull()]           .reset_index()           .drop('index',1)
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
   
    meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(int)
    meo = meo[meo['ViewData.BreakID']!=-1]           .reset_index()           .drop('index',1)
          
    meo['Side_0_1_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str) +                                 meo['ViewData.Side1_UniqueIds'].astype(str)
                                
    meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
    return(meo)
    
def cleaned_aua(fun_filepath_aua):
    aua = pd.read_csv(fun_filepath_aua)       .drop_duplicates()       .reset_index()       .drop('index',1)       .sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)

    aua = normalize_bp_acct_col_names(fun_df = aua)

    
    aua['Side_0_1_UniqueIds'] = aua['ViewData.Side0_UniqueIds'].astype(str) +                                 aua['ViewData.Side1_UniqueIds'].astype(str)
    
    return(aua)

def Acct_MEO_combination_file(fun_side, fun_cleaned_meo_df):
    if(fun_side == 'PB' or fun_side == 'BP' or fun_side == 'B-P' or fun_side == 'Prime Broker'):
        side_meo = fun_cleaned_meo_df[(fun_cleaned_meo_df['flag_side1'] >= 1) & (fun_cleaned_meo_df['flag_side0'] == 0)]
#        Currency_col_name = 'ViewData.B-P Currency'
    elif(fun_side == 'Acct' or fun_side == 'Accounting'):
        side_meo = fun_cleaned_meo_df[(fun_cleaned_meo_df['flag_side1'] == 0) & (fun_cleaned_meo_df['flag_side0'] >= 1)]
#        Currency_col_name = 'ViewData.Accounting Currency'
    else:
        print('The only options for side are on of the following : ')
        print('For Prime Broker side, the options are PB or BP or B-P or Prime Broker')
        print('For Accounting side, the options are Acct or Accounting')
        raise ValueError('Exiting function because fun_side argument was not from the accepted set of parameter values')
    
    side_meo['filter_key'] = side_meo['ViewData.Source Combination'].astype(str) + \
                             side_meo['ViewData.Mapped Custodian Account'].astype(str) + \
                             side_meo['ViewData.Currency'].astype(str)
        
    side_meo_training_df =[]
    for key in (list(np.unique(np.array(list(side_meo['filter_key'].values))))):
        side_meo_filter_slice = side_meo[side_meo['filter_key']==key]
        if side_meo_filter_slice.empty == False:
    
            side_meo_filter_slice = side_meo_filter_slice.reset_index()
            side_meo_filter_slice = side_meo_filter_slice.drop('index', 1)
    
            side_meo_filter_joined = pd.merge(side_meo_filter_slice, side_meo_filter_slice, on='filter_key')
            side_meo_training_df.append(side_meo_filter_joined)
    return(pd.concat(side_meo_training_df))
    
def identifying_closed_breaks_from_Trans_type(fun_side, fun_transaction_type_list, fun_side_meo_combination_df, fun_setup_code_crucial):
    if(fun_side == 'PB' or fun_side == 'BP' or fun_side == 'B-P' or fun_side == 'Prime Broker'):
        Net_amount_col_name_list = ['ViewData.B-P Net Amount_' + x for x in ['x','y']]
        Side_0_1_UniqueIds_col_name_list = ['ViewData.Side1_UniqueIds_' + x for x in ['x','y']]
    elif(fun_side == 'Acct' or fun_side == 'Accounting'):
        Net_amount_col_name_list = ['ViewData.Accounting Net Amount_' + x for x in ['x','y']]
        Side_0_1_UniqueIds_col_name_list = ['ViewData.Side0_UniqueIds_' + x for x in ['x','y']]
    else:
        print('The only options for side are on of the following : ')
        print('For Prime Broker side, the options are PB or BP or B-P or Prime Broker')
        print('For Accounting side, the options are Acct or Accounting')
        raise ValueError('Exiting function because fun_side argument was not from the accepted set of parameter values')        

    if(fun_setup_code_crucial == '153' or fun_setup_code_crucial == '239' or fun_setup_code_crucial == '206' or fun_setup_code_crucial == '205' or fun_setup_code_crucial == '213' or fun_setup_code_crucial == '179' or fun_setup_code_crucial == '172' or fun_setup_code_crucial == '173' or fun_setup_code_crucial == '149'):
        Transaction_type_closed_break_df = \
            fun_side_meo_combination_df[ \
                    (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) & \
                    (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) & \
                    (abs(fun_side_meo_combination_df[Net_amount_col_name_list[0]]).astype(str) == abs(fun_side_meo_combination_df[Net_amount_col_name_list[1]]).astype(str)) & \
                    (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str)) \
                                        ]
        if(fun_setup_code_crucial == '153'):
            if(fun_transaction_type_list == ['JNL'] or fun_transaction_type_list == ['MTM']):
                Transaction_type_closed_break_df = \
                    Transaction_type_closed_break_df[ \
                            (Transaction_type_closed_break_df['ViewData.Custodian_x'].astype(str) == 'CS') & \
                            (Transaction_type_closed_break_df['ViewData.Custodian_y'].astype(str) == 'CS') \
                                                ]
                if(fun_transaction_type_list == ['JNL']):
                    Transaction_type_closed_break_df = \
                        Transaction_type_closed_break_df[ \
                                (Transaction_type_closed_break_df['ViewData.Transaction ID_x'].astype(str) == Transaction_type_closed_break_df['ViewData.Transaction ID_y'].astype(str)) \
                                                    ]

        else:
            if(fun_transaction_type_list == ['JNL'] or fun_transaction_type_list == ['MTM']):
                Transaction_type_closed_break_df = \
                    Transaction_type_closed_break_df[ \
                            (Transaction_type_closed_break_df['ViewData.Custodian_x'].astype(str) == 'CS') & \
                            (Transaction_type_closed_break_df['ViewData.Custodian_y'].astype(str) == 'CS') \
                                                ]
                if(fun_transaction_type_list == ['JNL']):
                    Transaction_type_closed_break_df = \
                        Transaction_type_closed_break_df[ \
                                (Transaction_type_closed_break_df['ViewData.Transaction ID_x'].astype(str) == Transaction_type_closed_break_df['ViewData.Transaction ID_y'].astype(str)) \
                                                    ]

    
    return(set(
                Transaction_type_closed_break_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
                Transaction_type_closed_break_df['ViewData.Side1_UniqueIds_x'].astype(str)
               ))

def closed_breaks_captured_mode(fun_aua_df, fun_transaction_type, fun_captured_closed_breaks_set, fun_mode):
    if(fun_transaction_type != 'All_Closed_Breaks'):
        aua_df = fun_aua_df[(fun_aua_df['ViewData.Status'] == 'UCB') &                             (fun_aua_df['ViewData.Transaction Type'] == fun_transaction_type)]
    else:
        aua_df = fun_aua_df[(fun_aua_df['ViewData.Status'] == 'UCB')]
        
    aua_side_0_1_UniqueIds_set = set(aua_df['ViewData.Side0_UniqueIds'].astype(str) +                                  aua_df['ViewData.Side1_UniqueIds'].astype(str))
    if(fun_mode == 'Correctly_Captured_In_AUA'):
        list_to_return = list(aua_side_0_1_UniqueIds_set & fun_captured_closed_breaks_set)
    elif(fun_mode == 'Not_Captured_In_AUA'):
        list_to_return = list(aua_side_0_1_UniqueIds_set - fun_captured_closed_breaks_set)
    elif(fun_mode == 'Over_Captured_In_AUA'):
        list_to_return = list(fun_captured_closed_breaks_set - aua_side_0_1_UniqueIds_set)
    return(list_to_return)

def update_dict_to_output_breakids_number_pct(fun_dict, fun_aua_df, fun_loop_transaction_type, fun_count, fun_Side_0_1_UniqueIds_list):
    mode_type_list = ['Correctly_Captured_In_AUA','Not_Captured_In_AUA','Over_Captured_In_AUA']
    for mode_type in mode_type_list:
#    if(fun_loop_transaction_type != 'All_Closed_Breaks'):
        fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'] = list(set(            fun_aua_df[fun_aua_df['Side_0_1_UniqueIds'].isin(                     closed_breaks_captured_mode(fun_aua_df = fun_aua_df,                                         fun_transaction_type = fun_loop_transaction_type,                                         fun_captured_closed_breaks_set = set(fun_Side_0_1_UniqueIds_list),                                         fun_mode = mode_type))]                    ['ViewData.BreakID']))
    
        fun_total_number = len(                             fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'])
        
        fun_dict[fun_loop_transaction_type][mode_type + '_Total_Number'] = len(                             fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'])
        
        if(fun_count != 0):
            
            fun_dict[fun_loop_transaction_type][mode_type + '_Percentage'] = fun_total_number/fun_count#\
#                                 fun_dict[fun_loop_transaction_type][mode_type + '_Total_Number']/fun_count
        
        else:
            fun_dict[fun_loop_transaction_type][mode_type + '_Percentage'] = fun_loop_transaction_type + ' not found in Closed breaks of AUA'
    return(fun_dict)

def closed_daily_run(fun_setup_code, 
                     fun_date, 
                     fun_meo_df_daily_run,
                     fun_side
#                     fun_main_filepath_meo, 
#                     fun_main_filepath_aua
                     ):
    setup_val = fun_setup_code
    main_meo = cleaned_meo(fun_meo_df = fun_meo_df_daily_run)#, fun_filepath_meo = fun_main_filepath_meo
    if(fun_side == 'Both'):
        BP_meo_training_df = Acct_MEO_combination_file(fun_side = 'PB', \
                                                       fun_cleaned_meo_df = main_meo)
        
        Acct_meo_training_df = Acct_MEO_combination_file(fun_side = 'Acct', \
                                                         fun_cleaned_meo_df = main_meo)
    elif(fun_side == 'PB' or fun_side == 'BP' or fun_side == 'B-P' or fun_side == 'Prime Broker'):
        BP_meo_training_df = Acct_MEO_combination_file(fun_side = 'PB', \
                                                       fun_cleaned_meo_df = main_meo)
    elif(fun_side == 'Acct' or fun_side == 'Accounting'):
        Acct_meo_training_df = Acct_MEO_combination_file(fun_side = 'Acct', \
                                                         fun_cleaned_meo_df = main_meo)
    else:
        BP_meo_training_df = Acct_MEO_combination_file(fun_side = 'PB', \
                                                       fun_cleaned_meo_df = main_meo)
        
        Acct_meo_training_df = Acct_MEO_combination_file(fun_side = 'Acct', \
                                                         fun_cleaned_meo_df = main_meo)
        
#    main_aua = cleaned_aua(fun_filepath_aua = fun_main_filepath_aua)
    
    if(fun_setup_code == '153'):
        Transaction_Type_dict = {
                                 'JNL' : {'side' : 'PB',
                                           'Transaction_Type' : ['JNL'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                 'MTM' : {'side' : 'PB',
                                          'Transaction_Type' : ['MTM'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'Collateral' : {'side' : 'PB',
                                          'Transaction_Type' : ['Collateral'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'DEB_CRED' : {'side' : 'PB',
                                          'Transaction_Type' : ['DEB','CRED'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'DEP_WDRL' : {'side' : 'PB',
                                          'Transaction_Type' : ['DEP','WDRL'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'Miscellaneous' : {'side' : 'PB',
                                          'Transaction_Type' : ['Miscellaneous'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'REORG' : {'side' : 'PB',
                                          'Transaction_Type' : ['REORG'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'Transfer' : {'side' : 'Acct',
                                          'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df }
                                 }
    elif(fun_setup_code == '239'):
        Transaction_Type_dict = {
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Cash BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Cash'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Cash Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Cash'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Buy BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'AccountingRelated BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Exchange Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Exchange Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Exchange Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Exchange Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'VEC(Execution Fee) BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['VEC(Execution Fee)'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'VEC(Execution Fee) Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['VEC(Execution Fee)'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Clearing Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Clearing Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Clearing Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Clearing Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'NFA Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['NFA Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'NFA Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['NFA Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Seg/Sec BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Seg/Sec'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Seg/Sec Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Seg/Sec'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'EFP Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['EFP Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'EFP Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['EFP Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'PSAJ BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['PSAJ'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'PSAJ Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['PSAJ'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Multiple BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Multiple'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Multiple Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Multiple'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'BUY BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['BUY'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'BUY Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['BUY'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'DEP BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['DEP'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'DEP Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['DEP'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }
    elif(fun_setup_code == '206'):
        Transaction_Type_dict = {
#                                'Transfer BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Transfer'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'ForwardFX BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['ForwardFX'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ForwardFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'AccountingRelated BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['AccountingRelated'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Buy BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Buy'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Sell BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Sell'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Interest BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Interest'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Repo Close BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Repo Close'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Repo Close Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Repo Close'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Repo Open BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Repo Open'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Repo Open Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Repo Open'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'ReverseRepo Open BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['ReverseRepo Open'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ReverseRepo Open Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ReverseRepo Open'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'ReverseRepo Close BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['ReverseRepo Close'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ReverseRepo Close Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ReverseRepo Close'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'CoverShort BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['CoverShort'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'CoverShort Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CoverShort'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Dividend BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Dividend'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Dividend Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Dividend'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SpotFX BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SpotFX'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SpotFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SpotFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SellShort BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SellShort'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SellShort Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SellShort'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Withdraw BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Withdraw'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Withdraw Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Withdraw'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Mature BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Mature'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Mature Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Mature'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Deposit BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Deposit'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Deposit Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Deposit'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Trade Not To Be Reported_Buy BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Trade Not To Be Reported_Buy'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Trade Not To Be Reported_Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Trade Not To Be Reported_Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'ReturnOfCap BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['ReturnOfCap'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ReturnOfCap Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ReturnOfCap'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }

    elif(fun_setup_code == '149'):
        Transaction_Type_dict = {
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'SEQ BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['SEQ'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SEQ Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SEQ'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'SEG BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['SEG'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SEG Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SEG'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Buy BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'SwapReset BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['SwapReset'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SwapReset Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SwapReset'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'AccountingRelated BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'ForwardFX BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ForwardFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df}  
                                }
    elif(fun_setup_code == '172'):
        Transaction_Type_dict = {
                                'Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }
    elif(fun_setup_code == '173'):
        Transaction_Type_dict = {
#                                'Transfer BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Transfer'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Interest BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Interest'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Drawdown BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Drawdown'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Drawdown Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Drawdown'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Sell BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Sell'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Buy BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Buy'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Revenue BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Revenue'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Revenue Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Expenses BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Expenses'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Expenses Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Paydown BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Paydown'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Paydown Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Paydown'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'AccountingRelated BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['AccountingRelated'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }
    elif(fun_setup_code == '179'):
        Transaction_Type_dict = {
#                                'CoverShort BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['CoverShort'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'CoverShort Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CoverShort'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Sell BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Sell'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Transfer BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Transfer'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SwapReset BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SwapReset'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SwapReset Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SwapReset'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'AccountingRelated BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['AccountingRelated'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'ForwardFX BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['ForwardFX'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ForwardFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Dividend BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Dividend'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Dividend Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Dividend'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SellShort BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SellShort'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SellShort Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SellShort'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Buy BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Buy'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Interest BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Interest'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SpotFX BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SpotFX'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SpotFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SpotFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Reorganization BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Reorganization'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Reorganization Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Reorganization'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                
                                }
    elif(fun_setup_code == '213'):
        Transaction_Type_dict = {
#                                'Sell BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Sell'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'CoverShort BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['CoverShort'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'CoverShort Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CoverShort'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'AccountingRelated BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['AccountingRelated'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Transfer BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Transfer'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SwapReset BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SwapReset'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SwapReset Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SwapReset'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Dividend BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Dividend'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Dividend Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Dividend'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                
                                }
    elif(fun_setup_code == '205'):
        Transaction_Type_dict = {
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'DataDetail Debits BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['DataDetail Debits'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'DataDetail Debits Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['DataDetail Debits'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'DataDetail Credits BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['DataDetail Credits'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'DataDetail Credits Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['DataDetail Credits'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'AccountingRelated Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'AccountingRelated BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['AccountingRelated'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SpotFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SpotFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'SpotFX BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['SpotFX'],
                                           'Side_meo_training_df' : BP_meo_training_df}
                                
                                }


    print(os.getcwd())
    os.chdir('D:\\ViteosModel\\Closed')
    print(os.getcwd())
    
    filepath_stdout = fun_setup_code + '_closed_run_date_' + str(fun_date) + '_timestamp_' + str(datetime.now().strftime("%d_%m_%Y_%H_%M")) + '.txt'
    orig_stdout = sys.stdout
    f = open(filepath_stdout, 'w')
    sys.stdout = f
    
    Side_0_1_UniqueIds_closed_all_list = []
    for Transaction_type in Transaction_Type_dict:

        Side_0_1_UniqueIds_for_Transaction_type = identifying_closed_breaks_from_Trans_type(fun_side = Transaction_Type_dict.get(Transaction_type).get('side'), \
                                                                                            fun_transaction_type_list = Transaction_Type_dict.get(Transaction_type).get('Transaction_Type'), \
                                                                                            fun_side_meo_combination_df = Transaction_Type_dict.get(Transaction_type).get('Side_meo_training_df'), \
                                                                                            fun_setup_code_crucial = setup_val)

#        count_closed_breaks_for_transaction_type = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB') & \
#                                                                    (main_aua['ViewData.Transaction Type'] == Transaction_type)]['Side_0_1_UniqueIds']))
#        
#        Transaction_Type_dict = update_dict_to_output_breakids_number_pct(fun_dict = Transaction_Type_dict, \
#                                                                          fun_aua_df = main_aua, \
#                                                                          fun_loop_transaction_type = Transaction_type, \
#                                                                          fun_count = count_closed_breaks_for_transaction_type, \
#                                                                          fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_for_Transaction_type)
            
        
        Side_0_1_UniqueIds_closed_all_list.extend(Side_0_1_UniqueIds_for_Transaction_type)
        print('\n' + Transaction_type + '\n')
#        pprint.pprint(dictionary_exclude_keys(fun_dict = Transaction_Type_dict.get(Transaction_type),                                      fun_keys_to_exclude = {'side','Transaction_Type','Side_meo_training_df'}),                      width = 4)
    
    sys.stdout = orig_stdout
    f.close()
    
#    count_all_closed_breaks = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB')]                                               ['Side_0_1_UniqueIds']))
    
#    aua_closed_dict = {'All_Closed_Breaks' : {}}
#    aua_closed_dict = update_dict_to_output_breakids_number_pct(fun_dict = aua_closed_dict,\
#                                                                fun_aua_df = main_aua, \
#                                                                fun_loop_transaction_type = 'All_Closed_Breaks', \
#                                                                fun_count = count_all_closed_breaks, \
#                                                                fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_closed_all_list)
    
#    write_dict_at_top(fun_filename = filepath_stdout, \
#                      fun_dict_to_add = aua_closed_dict)
    
    return(Side_0_1_UniqueIds_closed_all_list)

#### Closed break functions - End #### 


def return_int_list(list_x):
    return [int(i) for i in list_x]

#### Break Prediction functions - End #### 


date_numbers_list = [16]
                     #2,3,4,
                    # 7,8,9,10,11,
                    # 14,15,16,17,18,
                    # 21,22,23,24,25,
                    # 28,29,30]

client = 'Soros'

setup_code = '206'

setup_code_list = [ \
#                   '153'
#                   , \
#                   '239', \
#It seems that 206 is has only sigle sided obs for date 9/23/2020, so I skipped it
                   '206' \
#                   , \
#For 149, this statement has error
#umr_double_count_left = umr_double_count[~umr_double_count['SideB.ViewData.Side0_UniqueIds'].isin(umr_otm_table['SideB.ViewData.Side0_UniqueIds'].unique())]
#KeyError: 'SideB.ViewData.Side0_UniqueIds'
#Thats because umr_otm_table is empty. We need to write exception management for it

#                   '149'
#                   , \
#                   '172'
#                   , \
#                   '173'
#                   , \
#It seems that 179 is has only sigle sided obs for date 9/23/2020, so I skipped it
#                   '179'
#                   , \
#For 205, this statement has error
#umr_double_count_left = umr_double_count[~umr_double_count['SideB.ViewData.Side0_UniqueIds'].isin(umr_otm_table['SideB.ViewData.Side0_UniqueIds'].unique())]
#KeyError: 'SideB.ViewData.Side0_UniqueIds'
#Thats because umr_otm_table is empty. We need to write exception management for it
#                   '205'
#                   , \
#                   '213' \
                   ]

mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_Soros_ML']
today = date.today()
d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
desired_date = d1 - timedelta(days=4)
desired_date_str = desired_date.strftime("%Y-%m-%d")
date_input = desired_date_str


#for setup_code in setup_code_list:
print('Starting predictions for Soros, setup_code = ')
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
meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)
meo = meo_df[new_cols]
date_i = pd.to_datetime(pd.to_datetime(meo_df['ViewData.Task Business Date'])).dt.date.astype(str).mode()[0]
umb_carry_forward_df = meo_df[meo_df['ViewData.Status'] == 'UMB']

Side_0_1_UniqueIds_closed_all_dates_list = []

i = 0
for i in range(0,len(date_numbers_list)):

    Side_0_1_UniqueIds_closed_all_dates_list.append(
            closed_daily_run(fun_setup_code=setup_code,\
                             fun_date = i,\
                             fun_meo_df_daily_run = meo,
                             fun_side = 'Acct')
#                             fun_main_filepath_meo= filepaths_MEO[i],\
#                             fun_main_filepath_aua = filepaths_AUA[i])
            )

new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list[0]]
new_closed_keys = [i.replace('None','') for i in new_closed_keys]

# ## Read testing data 


df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)

## Output for Closed breaks

closed_df= df1[df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)]

## Output for Closed breaks

non_closed_df = df1[~df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)]


#Closed Begins
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
filepaths_final_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_closed_df.csv'
final_closed_df.to_csv(filepaths_final_closed_df)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)






#Closed Begins
non_closed_columns_for_updation = ['ViewData.BreakID','ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Source Combination Code','ViewData.Task ID']

final_non_closed_df = non_closed_df[non_closed_columns_for_updation]
final_non_closed_df['Predicted_Status'] = 'OB'
final_non_closed_df['Predicted_action'] = 'No-Pair'
final_non_closed_df['ML_flag'] = 'ML'
final_non_closed_df['SetupID'] = setup_code 
final_non_closed_df['Final_predicted_break'] = ''
final_non_closed_df['PredictedComment'] = ''
final_non_closed_df['PredictedCategory'] = ''
final_non_closed_df['probability_UMB'] = ''
final_non_closed_df['probability_No_pair'] = ''
final_non_closed_df['probability_UMR'] = ''

final_non_closed_df[non_closed_columns_for_updation] = final_non_closed_df[non_closed_columns_for_updation].astype(str)
change_names_of_final_non_closed_df_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID' : 'BreakID',
                                            'ViewData.Task ID' : 'TaskID',
                                            'ViewData.Task Business Date' : 'BusinessDate',
                                            'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                        }

final_non_closed_df.rename(columns = change_names_of_final_non_closed_df_mapping_dict, inplace = True)

final_non_closed_df['BusinessDate'] = pd.to_datetime(final_non_closed_df['BusinessDate'])
final_non_closed_df['BusinessDate'] = final_non_closed_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_non_closed_df['BusinessDate'] = pd.to_datetime(final_non_closed_df['BusinessDate'])

final_non_closed_df['Side0_UniqueIds'] = final_non_closed_df['Side0_UniqueIds'].astype(str)
final_non_closed_df['Side1_UniqueIds'] = final_non_closed_df['Side1_UniqueIds'].astype(str)
final_non_closed_df['Final_predicted_break'] = final_non_closed_df['Final_predicted_break'].astype(str)
final_non_closed_df['Predicted_action'] = final_non_closed_df['Predicted_action'].astype(str)
final_non_closed_df['probability_No_pair'] = final_non_closed_df['probability_No_pair'].astype(str)
final_non_closed_df['probability_UMB'] = final_non_closed_df['probability_UMB'].astype(str)
final_non_closed_df['probability_UMR'] = final_non_closed_df['probability_UMR'].astype(str)
final_non_closed_df['SourceCombinationCode'] = final_non_closed_df['SourceCombinationCode'].astype(str)
final_non_closed_df['Predicted_Status'] = final_non_closed_df['Predicted_Status'].astype(str)
final_non_closed_df['ML_flag'] = final_non_closed_df['ML_flag'].astype(str)


final_non_closed_df[['BreakID', 'TaskID']] = final_non_closed_df[['BreakID', 'TaskID']].astype(float)
final_non_closed_df[['BreakID', 'TaskID']] = final_non_closed_df[['BreakID', 'TaskID']].astype(np.int64)

final_non_closed_df[['SetupID']] = final_non_closed_df[['SetupID']].astype(int)
filepaths_final_non_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_non_closed_df.csv'
final_non_closed_df.to_csv(filepaths_final_non_closed_df)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)










final_table_to_write = final_non_closed_df.append(final_closed_df)
#final_table_to_write = final_table_to_write.append([final_closed_df, \
#                                                    final_oto_umb_table_new_to_write, \
#                                                    umr_mto_table_new_to_write, \
#                                                    umr_otm_table_final_new_to_write])

#Closed Ends

filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_table_to_write.to_csv(filepaths_final_table_to_write)

filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
meo_df.to_csv(filepaths_meo_df)

def unlist_comma_separated_single_quote_string_lst(list_obj):
    new_list = []
    for i in list_obj:
        list_i = list(i.replace('\'','').split(', '))
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

#Comment

#Start of Commenting
comment_df_final_list = []
brk = final_table_to_write.copy()

brk = brk[brk['Predicted_action'] == 'No-Pair']
#meo_df = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\meo_df.csv')

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

def fid1(a,b,c):
    if a=='No-Pair':
        if b =='AA':
            return c
        else:
            return b
    else:
        return '12345'


brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1 )




side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))



meo1 = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(side0_id)]
meo2 = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(side1_id)]

frames = [meo1, meo2]

df1 = pd.concat(frames)
df1 = df1.reset_index()
df1 = df1.drop('index', axis = 1)


# ### Duplicate OB removal

df1 = df1.drop_duplicates()



df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b
        

df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])

uni2 = df1.groupby(['final_ID','ViewData.Task Business Date']).last().reset_index()

uni2 = uni2.sort_values(['final_ID','ViewData.Task Business Date'], ascending = [True, True])

# #### Trade date vs Settle date and future dated trade

df2 = uni2.copy()


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


# ### Cleannig of the 4 variables in this

os.chdir('D:\\ViteosModel\\Abhijeet - Comment')

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

# ### Cleaning of Description

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
            
df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

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
        

df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))

com = com.drop(['var','Catogery'], axis = 1)

com = com.drop_duplicates()

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

df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))

comp = ['inc','stk','corp ','llc','pvt','plc']
df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

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

df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : desccat(x))


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


# #### Cancelled Trade Removal

trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']

dfkk = df2[df2['ViewData.Transaction Type1'].isin(trade_types)]

dfk_nontrade = df2[~df2['ViewData.Transaction Type1'].isin(trade_types)]

dffk2 = dfkk[dfkk['ViewData.Side0_UniqueIds']=='AA']
dffk3 = dfkk[dfkk['ViewData.Side1_UniqueIds']=='BB']

dffk4 = dfk_nontrade[dfk_nontrade['ViewData.Side0_UniqueIds']=='AA']
dffk5 = dfk_nontrade[dfk_nontrade['ViewData.Side1_UniqueIds']=='BB']


# #### Geneva side

def canceltrade(x,y):
    if x =='buy' and y>0:
        k = 1
    elif x =='sell' and y<0:
        k = 1
    else:
        k = 0
    return k

if(dffk3.shape[0] != 0):    
    dffk3['cancel_marker'] = dffk3.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Accounting Net Amount']), axis = 1)

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

if(dffk3.shape[0] != 0):    

    if dffk3[dffk3['cancel_marker'] == 1].shape[0]!=0:
        cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))
        if len(cancel_trade)>0:
            km = dffk3[dffk3['cancel_marker'] != 1]
            original = km[km['ViewData.Transaction ID'].isin(cancel_trade)]
            if(original.shape[0] != 0):
                original['predicted category'] = 'Original of Cancelled trade'
                original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)                
                cancellation = dffk3[dffk3['cancel_marker'] == 1]
                cancellation['predicted category'] = 'Cancelled trade'
                cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
                cancel_fin = pd.concat([original,cancellation])                
                sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
                cancel_fin = cancel_fin[sel_col_1]
                cancel_fin.to_csv('Comment file soros 2 sep testing p1.csv')
                comment_df_final_list.append(cancel_fin)
                dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
            
            else:
                cancellation = dffk3[dffk3['cancel_marker'] == 1]
                cancellation['predicted category'] = 'Cancelled trade'
                cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
                cancel_fin = cancellation.copy()
                sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
                cancel_fin = cancel_fin[sel_col_1]
                cancel_fin.to_csv('Comment file soros 2 sep testing no original p2.csv')
                comment_df_final_list.append(cancel_fin)
                dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
        else:
            dffk3 = dffk3.copy()
    else:
        dffk3 = dffk3.copy()
else:
    dffk3 = dffk3.copy()            
        


# #### Broker side
if(dffk2.shape[0]!=0):
    dffk2['cancel_marker'] = dffk2.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Cust Net Amount']), axis = 1)

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


# In[171]:


def cancelcomment3(y):
    com1 = 'This is cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' ' + com2 + ' ' + str(y)
    return com


# In[172]:

if(dffk2.shape[0]!=0):    
    if dffk2[dffk2['cancel_marker'] == 1].shape[0]!=0:
        dummy = dffk2[dffk2['cancel_marker']!=1]
        dummy1 = dffk2[dffk2['cancel_marker']==1]
    
    
        pool =[]
        key_index =[]
        training_df =[]
        call1 = []
    
        appended_data = []
    
        no_pair_ids = []
    #max_rows = 5
    
        k = list(set(list(set(dummy['ViewData.Task Business Date1']))))
        k1 = k
    
        for d in tqdm(k1):
            aa1 = dummy[dummy['ViewData.Task Business Date1']==d]
            bb1 = dummy1[dummy1['ViewData.Task Business Date1']==d]
            aa1['marker'] = 1
            bb1['marker'] = 1
        
            aa1 = aa1.reset_index()
            aa1 = aa1.drop('index',1)
            bb1 = bb1.reset_index()
            bb1 = bb1.drop('index', 1)
            #print(aa1.shape)
            #print(bb1.shape)
        
            aa1.columns = ['SideB.' + x  for x in aa1.columns] 
            bb1.columns = ['SideA.' + x  for x in bb1.columns]
        
            cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
            appended_data.append(cc1)
            cancel_broker = pd.concat(appended_data)
            cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
            elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]
            if elim1.shape[0]!=0:
                id_listA = list(set(elim1['SideA.final_ID']))
                c1 = dummy
                c2 = dummy1[dummy1['final_ID'].isin(id_listA)]
                c1['predicted category'] = 'Cancelled trade'
                c2['predicted category'] = 'Original of Cancelled trade'
                c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']),axis = 1)
                c2['predicted comment'] = c2.apply(lambda x : cancelcomment3(x['ViewData.Settle Date1']), axis = 1)
                cancel_fin = pd.concat([c1,c2])
                cancel_fin = cancel_fin.reset_index()
                cancel_fin = cancel_fin.drop('index', axis = 1)
                sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
                cancel_fin = cancel_fin[sel_col_1]
                comment_df_final_list.append(cancel_fin)
                cancel_fin.to_csv('Comment file soros 2 sep testing p3.csv')
                id_listB = list(set(c1['final_ID']))
                comb = id_listB + id_listA
                dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
                
                
                
       
            
        else:
            c1 = dummy
            c1['predicted category'] = 'Cancelled trade'
            c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']),axis = 1)
            sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
            cancel_fin = c1[sel_col_1]
            comment_df_final_list.append(cancel_fin)
            cancel_fin.to_csv('Comment file soros 2 sep testing no original p4.csv')
            id_listB = list(set(c1['final_ID']))
            comb = id_listB
            dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
            
    else:
        dffk2 = dffk2.copy()
else:
    dffk2 = dffk2.copy()

# #### Finding Pairs in Up and down

# In[173]:


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
      ]


# In[174]:


dff4 = dffk2[sel_col]
dff5 = dffk3[sel_col]


# In[175]:


dff6 = dffk4[sel_col]
dff7 = dffk5[sel_col]


# In[176]:


dff4 = pd.concat([dff4,dff6])
dff4 = dff4.reset_index()
dff4 = dff4.drop('index', axis = 1)


# In[177]:


dff5 = pd.concat([dff5,dff7])
dff5 = dff5.reset_index()
dff5 = dff5.drop('index', axis = 1)


# #### M cross N code

# In[178]:


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
 


# In[179]:


df_213_1 = pd.concat(appended_data)


# In[180]:


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
    
        
   
    


# In[181]:


df_213_1[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")


# In[182]:


df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['sd_match'] + df_213_1['curr_match']


# In[183]:


elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]


# - putting updown comments

# In[190]:


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


# In[191]:


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
    elim.to_csv('Comment file soros 2 sep testing p5.csv')
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
    


# In[192]:


frames = [dff4,dff5]


# In[193]:


data = pd.concat(frames)
data = data.reset_index()
data = data.drop('index', axis = 1)


# In[196]:


data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])


# In[197]:


days = [1,30,31,29]
data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[198]:


data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')


# In[199]:


def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k


# In[200]:


data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))


# In[201]:


data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)


# In[202]:


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
 
'ViewData.InternalComment2', 'ViewData.Description','new_pb2','new_pb1','comm_marker','monthend marker'
   
 
]


# In[203]:


data = data[Pre_final]


# In[204]:


df_mod1 = data.copy()


# In[205]:


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


# In[207]:


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[208]:


data2 = df_mod1.copy()


# ### Separate Prediction of the Trade and Non trade

# #### 1st for Non Trade

# In[209]:


trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[210]:


data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]


# In[211]:


cols = [
 


 'ViewData.Transaction Type1',
 
 

 'ViewData.Asset Type Category1',

 'new_desc_cat',

 'ViewData.Investment Type1',
 

 'new_pb2','new_pb1','comm_marker','monthend marker'
 
 
              
             ]


# In[212]:


data211 = data21[cols]


# In[213]:

filename = 'finalized_model_soros_non_trade_v9.sav'

clf = pickle.load(open(filename, 'rb'))


# In[216]:


# Actual class predictions
cb_predictions = clf.predict(data211)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]


# #### Testing of Model and final prediction file - Non Trade

# In[217]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[218]:


result_non_trade =data21.copy()


# In[219]:


result_non_trade = result_non_trade.reset_index()


# In[220]:


result_non_trade['predicted category'] = pd.Series(demo)
result_non_trade['predicted comment'] = 'NA'


# In[229]:


result_non_trade = result_non_trade.drop('predicted comment', axis = 1)


# In[249]:


com_temp = pd.read_csv('Soros comment template for delivery.csv')


# In[250]:


com_temp = com_temp.rename(columns = {'Category ':'predicted category','template':'predicted template'})


# In[252]:


result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')


# In[256]:


def comgen(x,y,z,k):
    if x == 'Geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "Geneva" + ' ' +y + ' ' + str(z)
        
    return com


# In[257]:


result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)


# In[ ]:


result_non_trade = result_non_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]


# In[260]:


result_non_trade.to_csv('Comment file soros 2 sep testing p6.csv')
comment_df_final_list.append(result_non_trade)

#TODO : Uncomment below code once asked Abhijeet why clf.predict is emtying the entire variable list
## #### For Trade Model
#
## In[261]:
#
#
#data22 = data2[data2['ViewData.Transaction Type1'].isin(trade_types)]
#
#
## In[262]:
#
#
#data222 = data22[cols]
#
#
## In[263]:
#
#
#filename = 'finalized_model_soros_trade_v9.sav'
#
#
## In[264]:
#
#
#clf = pickle.load(open(filename, 'rb'))
#
#
## In[265]:
#
#
## Actual class predictions
#TODO : Ask Abhijeet why the below line empties the variable list
#cb_predictions = clf.predict(data222)#.astype(str)
## Probabilities for each class
##cb_probs = clf.predict_proba(X_test)[:, 1]
#
#
## In[266]:
#
#
#demo = []
#for item in cb_predictions:
#    demo.append(item[0])
#
#
## In[267]:
#
#
#result_trade =data22.copy()
#
#result_trade = result_trade.reset_index()
#
#result_trade['predicted category'] = pd.Series(demo)
#result_trade['predicted comment'] = 'NA'
#
#result_trade = result_trade.drop('predicted comment', axis = 1)
#
#result_trade = pd.merge(result_trade,com_temp,on = 'predicted category',how = 'left')
#
#result_trade['predicted comment'] = result_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#
#
#result_trade = result_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]
#
#
#result_trade.to_csv('Comment file soros 2 sep testing p7.csv')
#comment_df_final_list.append(result_trade)

comment_df_final = pd.concat(comment_df_final_list)

change_col_names_comment_df_final_dict = {
                                        'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                        'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                        'predicted category' : 'PredictedCategory',
                                        'predicted comment' : 'PredictedComment'
                                        }

comment_df_final.rename(columns = change_col_names_comment_df_final_dict, inplace = True)

comment_df_final_side0 = comment_df_final[comment_df_final['Side1_UniqueIds'] == 'BB']
comment_df_final_side1 = comment_df_final[comment_df_final['Side0_UniqueIds'] == 'AA']

final_df = final_table_to_write.merge(comment_df_final_side0, on = 'Side0_UniqueIds', how = 'left')
final_df['PredictedComment'] = final_df['PredictedComment_y'].fillna(final_df['PredictedComment_x'])
final_df.drop(['final_ID','PredictedComment_x','PredictedComment_y','Side1_UniqueIds_y'], axis = 1, inplace = True)
final_df.rename(columns = {'Side1_UniqueIds_x' : 'Side1_UniqueIds'}, inplace = True)


final_df_2 = final_df.merge(comment_df_final_side1, on = 'Side1_UniqueIds', how = 'left')
final_df_2['PredictedComment'] = final_df_2['PredictedComment_y'].fillna(final_df_2['PredictedComment_x'])
final_df_2.drop(['final_ID','PredictedComment_x','PredictedComment_y','Side0_UniqueIds_y'], axis = 1, inplace = True)
final_df_2.rename(columns = {'Side0_UniqueIds_x' : 'Side0_UniqueIds'}, inplace = True)

#    Added more checks for database
final_df_2['Side1_UniqueIds'] = final_df_2['Side1_UniqueIds'].astype(str)
final_df_2['Side0_UniqueIds'] = final_df_2['Side0_UniqueIds'].astype(str)
final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].astype(str)

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

#final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('None','')
#final_df_2['probability_UMT'] = final_df_2['probability_UMT'].replace('nan','')

final_df_2['probability_UMR'] = final_df_2['probability_UMR'].replace('None','')
final_df_2['probability_UMR'] = final_df_2['probability_UMR'].replace('nan','')

final_df_2['probability_UMB'] = final_df_2['probability_UMB'].replace('None','')
final_df_2['probability_UMB'] = final_df_2['probability_UMB'].replace('nan','')

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\\n','',regex = True)

final_df_2['PredictedComment'] = final_df_2['PredictedComment'].astype(str)
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('nan','',regex = True)
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('None','',regex = True)
final_df_2['PredictedComment'] = final_df_2['PredictedComment'].replace('NA','',regex = True)

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)

final_df_2_UMR_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
if(final_df_2_UMR_record_with_predicted_comment.shape[0] != 0):
    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]

    Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side0_UniqueIds']
    Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side1_UniqueIds']

    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]

    final_df_2_UMR_record_with_predicted_comment['PredictedComment'] = ''       
    final_df_2 = final_df_2.append(final_df_2_UMR_record_with_predicted_comment)

#    final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])

final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(float)
final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(np.int64)


#Fixing 'Not_Covered_by_ML' Statuses
Search_term = 'not_covered_by_ml'

final_df_2_Covered_by_ML_df = final_df_2[~final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

final_df_2_Not_Covered_by_ML_df = final_df_2[final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

def get_first_term_before_separator(single_string, separator):
    return(single_string.split(separator)[0])

final_df_2_Not_Covered_by_ML_df['Predicted_Status'] = final_df_2_Not_Covered_by_ML_df['Predicted_Status'].apply(lambda x : get_first_term_before_separator(x,'_'))
final_df_2_Not_Covered_by_ML_df['ML_flag'] = 'Not_Covered_by_ML'

final_df_2 = final_df_2_Covered_by_ML_df.append(final_df_2_Not_Covered_by_ML_df)

def ui_action_column(param_final_df):
    param_final_df.loc[((param_final_df['ML_flag'] == 'Not_Covered_by_ML')),'ActionType'] = 'No Prediction'    
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'OB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'No Action'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'OB') & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'COMMENT'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'CLOSE'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'CLOSE WITH COMMENT'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR WITH COMMENT'
    param_final_df['ActionType'] = param_final_df['ActionType'].astype(str)
    return(param_final_df)    

final_df_2= ui_action_column(final_df_2)

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df_2.to_csv(filepaths_final_df_2)

#End of Commenting

#data_dict = final_table_to_write.to_dict("records")
data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
coll_1_for_writing_prediction_data.insert_many(data_dict) 

print(setup_code)
print(date_i)

#For remaining obs, fill probability as 60 - 90
#remaining_ob_df = pd.DataFrame()
#remaining_ob_df[]
#remaining_ob_df['Side0_UniqueIds']








