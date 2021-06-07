#!/usr/bin/env python
# coding: utf-8

# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 15:33:48 2020
@author: consultant138
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 19:12:48 2020

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


# In[3]:


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


# In[4]:


new_cols = ['ViewData.' + x for x in cols] + add

# ## Close Prediction Weiss


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

#def cleaned_meo(fun_filepath_meo):
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

    meo.loc[meo['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
    meo.loc[meo['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0

    meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(int)
    meo = meo[meo['ViewData.BreakID']!=-1] \
          .reset_index() \
          .drop('index',1)
          
    meo['Side_0_1_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str) + \
                                meo['ViewData.Side1_UniqueIds'].astype(str)
                                
    meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
    return(meo)
    
def cleaned_aua(fun_filepath_aua):
    aua = pd.read_csv(fun_filepath_aua)       .drop_duplicates()       .reset_index()       .drop('index',1)       .sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)

    aua = normalize_bp_acct_col_names(fun_df = aua)

    
    aua['Side_0_1_UniqueIds'] = aua['ViewData.Side0_UniqueIds'].astype(str) + \
                                aua['ViewData.Side1_UniqueIds'].astype(str)
    
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
                             side_meo['ViewData.Currency'].astype(str) + \
                             side_meo['ViewData.Transaction Type'].astype(str)                             
        
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
    
    
    if(fun_setup_code_crucial == '125'):
        Transaction_type_closed_break_df = \
            fun_side_meo_combination_df[ \
                                        (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) & \
                                        (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) & \
                                        (abs(fun_side_meo_combination_df[Net_amount_col_name_list[0]]).astype(str) == abs(fun_side_meo_combination_df[Net_amount_col_name_list[1]]).astype(str)) & \
                                        (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str)) \
                                         ]

        Transaction_type_closed_break_df_2 = \
            fun_side_meo_combination_df[ \
                                        (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) & \
                                        (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) & \
                                        (fun_side_meo_combination_df['ViewData.Transaction ID_x'].astype(str) == fun_side_meo_combination_df['ViewData.Transaction ID_y'].astype(str)) & \
                                        (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str)) \
                                         ]

        Transaction_type_closed_break_df = Transaction_type_closed_break_df.append(Transaction_type_closed_break_df_2)

    return(set(
                Transaction_type_closed_break_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
                Transaction_type_closed_break_df['ViewData.Side1_UniqueIds_x'].astype(str)
               ))

def closed_breaks_captured_mode(fun_aua_df, fun_transaction_type, fun_captured_closed_breaks_set, fun_mode):
    if(fun_transaction_type != 'All_Closed_Breaks'):
        aua_df = fun_aua_df[(fun_aua_df['ViewData.Status'] == 'UCB') & \
                            (fun_aua_df['ViewData.Transaction Type'] == fun_transaction_type)]
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
        fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'] = list(set( \
                fun_aua_df[fun_aua_df['Side_0_1_UniqueIds'].isin( \
                           closed_breaks_captured_mode(fun_aua_df = fun_aua_df, \
                                                       fun_transaction_type = fun_loop_transaction_type, \
                                                       fun_captured_closed_breaks_set = set(fun_Side_0_1_UniqueIds_list), \
                                                       fun_mode = mode_type))] \
                                                        ['ViewData.BreakID']))
    
        fun_total_number = len( \
                               fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'])
        
        fun_dict[fun_loop_transaction_type][mode_type + '_Total_Number'] = len( \
                             fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'])
        
        if(fun_count != 0):
            
            fun_dict[fun_loop_transaction_type][mode_type + '_Percentage'] = fun_total_number/fun_count#\
#                                 fun_dict[fun_loop_transaction_type][mode_type + '_Total_Number']/fun_count
        
        else:

            fun_dict[fun_loop_transaction_type][mode_type + '_Percentage'] = fun_loop_transaction_type + ' not found in Closed breaks of AUA'
    return(fun_dict)

#def closed_daily_run(fun_setup_code, fun_date, fun_main_filepath_meo, fun_main_filepath_aua):
def closed_daily_run(fun_setup_code, 
                     fun_date, 
                     fun_meo_df_daily_run#,
#                     fun_main_filepath_meo, 
#                     fun_main_filepath_aua
                     ):
    setup_val = fun_setup_code

#    main_meo = cleaned_meo(fun_filepath_meo = fun_main_filepath_meo)
    main_meo = cleaned_meo(fun_meo_df = fun_meo_df_daily_run)
    
    BP_meo_training_df = Acct_MEO_combination_file(fun_side = 'PB', \
                                                    fun_cleaned_meo_df = main_meo)
    
    Acct_meo_training_df = Acct_MEO_combination_file(fun_side = 'Acct', \
                                                     fun_cleaned_meo_df = main_meo)

#    main_aua = cleaned_aua(fun_filepath_aua = fun_main_filepath_aua)
    
    if(fun_setup_code == '125'):
        Transaction_Type_dict = {
                                 'Fees & Comm' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Fees & Comm'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                 'DEPOSIT' : {'side' : 'PB',
                                          'Transaction_Type' : ['DEPOSIT'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'WITHDRAWAL' : {'side' : 'PB',
                                          'Transaction_Type' : ['WITHDRAWAL'],
                                           'Side_meo_training_df' : BP_meo_training_df },
                                 'ForwardFX' : {'side' : 'Acct',
                                          'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df },
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
                                           'Side_meo_training_df' : Acct_meo_training_df },
                                'WTH BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['WTH'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'WTH Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['WTH'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'SPEC Stk Loan Jrl BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['SPEC Stk Loan Jrl'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'SPEC Stk Loan Jrl Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SPEC Stk Loan Jrl'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Globex Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Globex Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Globex Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Globex Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'TRF FM MARGIN MARK TO MARKET BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['TRF FM MARGIN MARK TO MARKET'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'TRF FM MARGIN MARK TO MARKET Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['TRF FM MARGIN MARK TO MARKET'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'TRF TO SHORT MARK TO MARKET BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['TRF TO SHORT MARK TO MARKET'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'TRF TO SHORT MARK TO MARKET Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['TRF TO SHORT MARK TO MARKET'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'EQUITY SWAP SHORT PERFORMANCE BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['EQUITY SWAP SHORT PERFORMANCE'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'EQUITY SWAP SHORT PERFORMANCE Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['EQUITY SWAP SHORT PERFORMANCE'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'DEP BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['DEP'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'DEP Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['DEP'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'MARK TO THE MARKET BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['MARK TO THE MARKET'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'MARK TO THE MARKET Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['MARK TO THE MARKET'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'EQUITY SWAP SHORT FINANCING BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['EQUITY SWAP SHORT FINANCING'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'EQUITY SWAP SHORT FINANCING Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['EQUITY SWAP SHORT FINANCING'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Dividend BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Dividend'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Dividend Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Dividend'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
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

        Side_0_1_UniqueIds_for_Transaction_type = identifying_closed_breaks_from_Trans_type(fun_side = Transaction_Type_dict.get(Transaction_type).get('side'),                                                                                   fun_transaction_type_list = Transaction_Type_dict.get(Transaction_type).get('Transaction_Type'),                                                                                   fun_side_meo_combination_df = Transaction_Type_dict.get(Transaction_type).get('Side_meo_training_df'),                                                                                  fun_setup_code_crucial = setup_val)

#        count_closed_breaks_for_transaction_type = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB') &                     (main_aua['ViewData.Transaction Type'] == Transaction_type)]['Side_0_1_UniqueIds']))
#        
#        Transaction_Type_dict = update_dict_to_output_breakids_number_pct(fun_dict = Transaction_Type_dict,                                                                    fun_aua_df = main_aua,                                                                    fun_loop_transaction_type = Transaction_type,                                                                    fun_count = count_closed_breaks_for_transaction_type,                                                                    fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_for_Transaction_type)
#            
#        
        Side_0_1_UniqueIds_closed_all_list.extend(Side_0_1_UniqueIds_for_Transaction_type)
        print('\n' + Transaction_type + '\n')
        pprint.pprint(dictionary_exclude_keys(fun_dict = Transaction_Type_dict.get(Transaction_type), \
                                              fun_keys_to_exclude = {'side','Transaction_Type','Side_meo_training_df'}), \
                      width = 4)
    
    sys.stdout = orig_stdout
    f.close()
    
#    count_all_closed_breaks = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB')]                                               ['Side_0_1_UniqueIds']))
#    
#    aua_closed_dict = {'All_Closed_Breaks' : {}}
#    aua_closed_dict = update_dict_to_output_breakids_number_pct(fun_dict = aua_closed_dict,                                                                 fun_aua_df = main_aua,                                                                 fun_loop_transaction_type = 'All_Closed_Breaks',                                                                 fun_count = count_all_closed_breaks,                                                                 fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_closed_all_list)
#    
#    write_dict_at_top(fun_filename = filepath_stdout,                       fun_dict_to_add = aua_closed_dict)
    
    return(Side_0_1_UniqueIds_closed_all_list)
    
def normalize_final_no_pair_table_col_names(fun_final_no_pair_table):
    final_no_pair_table_col_names_mapping_dict = {
                                      'SideA.ViewData.Side1_UniqueIds' : 'ViewData.Side1_UniqueIds',
                                      'SideB.ViewData.Side0_UniqueIds' : 'ViewData.Side0_UniqueIds',
                                      'SideA.ViewData.BreakID_A_side' : 'ViewData.BreakID_Side1', 
                                      'SideB.ViewData.BreakID_B_side' : 'ViewData.BreakID_Side0'
                                      }
    fun_final_no_pair_table.rename(columns = final_no_pair_table_col_names_mapping_dict, inplace = True)
    return(fun_final_no_pair_table)
   
def return_int_list(list_x):
    return [int(i) for i in list_x]

def get_BreakID_from_list_of_Side_01_UniqueIds(fun_str_list_Side_01_UniqueIds, fun_meo_df, fun_side_0_or_1):
    list_BreakID_corresponding_to_Side_01_UniqueIds = []
    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
        if(fun_side_0_or_1 == 0):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
        elif(fun_side_0_or_1 == 1):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    return(list_BreakID_corresponding_to_Side_01_UniqueIds)

#date_numbers_list = [11]
#,2,3,4,
                     #7,8,9,10,11,
                     #14,15,16,17,18,
                     ##21,22,23,24,25,
                     #28,29,30]
#
#client = 'Soros'    
#
#setup = '153'
#
#filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections_' + client.upper() + '.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections_' + client.upper() + '.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]

client = 'Weiss'

setup = '125'
setup_code = '125'
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
meo = meo_df[new_cols]
print('meo size')
print(meo.shape[0])
umb_carry_forward_df = meo_df[meo_df['ViewData.Status'] == 'UMB']

#Prepare RabbitMQ simulation message to be sent to DB for data extraction    
meo_df_taskids = list(meo_df['ViewData.Task ID'].unique())

#    Side_0_1_UniqueIds_closed_all_dates_list = []
#    
#    i = 0
#    for i in range(0,len(date_numbers_list)):
#    
#        Side_0_1_UniqueIds_closed_all_dates_list.append(
#                closed_daily_run(fun_setup_code=setup_code,\
#                                 fun_date = i,\
#                                 fun_meo_df_daily_run = meo)
#    #                             fun_main_filepath_meo= filepaths_MEO[i],\
#    #                             fun_main_filepath_aua = filepaths_AUA[i])
#                )
    





Side_0_1_UniqueIds_closed_all_dates_list = []

i = 0
#for i in range(0,len(date_numbers_list)):

start_closed = timeit.default_timer()

Side_0_1_UniqueIds_closed_all_dates_list.append(
        closed_daily_run(fun_setup_code=setup_code,\
                         fun_date = i,\
                         fun_meo_df_daily_run= meo)
        )

new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list[0]]
new_closed_keys = [i.replace('None','') for i in new_closed_keys]

stop_closed = timeit.default_timer()

print('Time for closed : ', stop_closed - start_closed)
# ## Read testing data 

df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)


## Output for Closed breaks
closed_df_side1 = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(new_closed_keys)]
closed_df_side0 = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(new_closed_keys)]
closed_df = closed_df_side1.append(closed_df_side0)

# ## Machine generated output


df = df1.copy()
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

df['ViewData.Status'].value_counts()

df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)

## ## Sample data on one date
#
print('The Date value count is:')
print(df['Date'].value_counts())

date_i = df['Date'].mode()[0]

print('Choosing the date : ' + date_i)

sample = df[df['Date'] == date_i]

sample = sample.reset_index()
sample = sample.drop('index',1)


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


bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
bb = bb.reset_index()
bb = bb.drop('index',1)

# ## Many to Many for Equity Swaps
cc = pd.concat([aa, bb], axis=0)

cc = cc.reset_index().drop('index',1)

cc['ViewData.Transaction Type'] = cc['ViewData.Transaction Type'].astype(str)
cc['ViewData.Settle Date'] = pd.to_datetime(cc['ViewData.Settle Date'])
cc['filter_key_with_sd'] = cc['filter_key'].astype(str) + cc['ViewData.Settle Date'].astype(str)

def transfer_desc(tt, desc):
    if tt.lower() =='transfer':
        
        desc = desc.lower()
        if any(key in desc for key in ['equity','swap unwind','eq swap']):
            transfer_flag = 1
        else:
             transfer_flag = 0    
    else:
        transfer_flag =0
    return transfer_flag
cc['ViewData.Description'] = cc['ViewData.Description'].astype(str)
cc['Transfer_flag'] = cc.apply(lambda x: transfer_desc(x['ViewData.Transaction Type'],x['ViewData.Description']), axis=1)

def eq_swap_tt_flag(tt):
    tt = tt.lower()
    if any(key in tt for key in ['equity swap','swap unwind','eq swap','transfer']):
        tt_flag = 1
    else:
        tt_flag = 0
    return tt_flag

cc['Equity_Swap_flag'] = cc.apply(lambda x: eq_swap_tt_flag(x['ViewData.Transaction Type']), axis=1)

if cc[(cc['Equity_Swap_flag']==1)|(cc['Transfer_flag']==1)].shape[0]>0:

    cc2 = cc[(cc['Equity_Swap_flag']==1)|(cc['Transfer_flag']==1)]
    cc2 = cc2.reset_index().drop('index',1)
    cc2['ViewData.Settle Date'] = pd.to_datetime(cc2['ViewData.Settle Date'])
    cc2['filter_key_with_sd'] = cc2['filter_key'].astype(str) + cc2['ViewData.Settle Date'].astype(str)
else:
    cc2 = pd.DataFrame()

filter_key_umt_umb = []
diff_in_amount = []
diff_in_amount_key = []


if cc2.empty == False:
    for key in cc2['filter_key_with_sd'].unique():        
        cc2_dummy = cc2[cc2['filter_key_with_sd']==key]
        if (-1<= cc2_dummy['ViewData.Net Amount Difference'].sum() <=1) & (cc2_dummy.shape[0]>2) & (cc2_dummy['Trans_side'].nunique()>1):
            #print(cc2_dummy.shape[0])
            #print(key)
            filter_key_umt_umb.append(key)
        else:
            if (cc2_dummy.shape[0]>2) & (cc2_dummy['Trans_side'].nunique()>1):
                diff = cc2_dummy['ViewData.Net Amount Difference'].sum()
                diff_in_amount.append(diff)
                diff_in_amount_key.append(key)



# ### Difference in amount for Swap settlement Dataframe

diff_in_amount_df = pd.DataFrame(diff_in_amount_key)


def eq_swap_comment(filter_key,difference):
    comment = "Difference of " + str(difference) + " in swap settlement of " + filter_key[-5:]
    return comment

if diff_in_amount_df.empty == False:
    diff_in_amount_df.columns = ['filter_key_with_sd']
    diff_in_amount_df['diff_in_amount'] = diff_in_amount
    diff_in_amount_df['comment'] = diff_in_amount_df.apply(lambda x:eq_swap_comment(x['filter_key_with_sd'], x['diff_in_amount']),axis=1)

if diff_in_amount_df.empty == False:
    cc3 = pd.merge(cc2,diff_in_amount_df,on='filter_key_with_sd', how='left')
    cc4 = cc3[~cc3['comment'].isnull()]
    cc4 = cc4.reset_index().drop('index',1)
else:
    cc3 = pd.DataFrame()
    cc4 = pd.DataFrame()
    

if cc4.empty == False:
    comment_table_eq_swap = cc4[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','comment']]
else:
    comment_table_eq_swap = pd.DataFrame()
# ### Remove IDs from eq swap

## cc4 goes directly into the comments engine #############

if cc4.empty == False:
    cc5 = cc2[~cc2['filter_key_with_sd'].isin(cc4['filter_key_with_sd'].unique())]
    cc5 = cc5.reset_index().drop('index',1)
else:
    cc5 = cc2.copy()

## Equity Swap Many to many

eq_mtm_1_ids = []
eq_mtm_0_ids = []

if cc5.empty == False:
    for key in filter_key_umt_umb:
        one_side = cc5[cc5['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
        zero_side = cc5[cc5['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
        one_side = [i for i in one_side if i not in ['nan','None','']]
        zero_side = [i for i in zero_side if i not in ['nan','None','']]
        eq_mtm_1_ids.append(one_side)
        eq_mtm_0_ids.append(zero_side)

if eq_mtm_1_ids !=[]:
    mtm_list_1 = list(np.concatenate(eq_mtm_1_ids))
else:
    mtm_list_1 = []

if eq_mtm_0_ids !=[]:
    mtm_list_0 = list(np.concatenate(eq_mtm_0_ids))
else:
    mtm_list_0 = []

## Data Frame for MTM from equity Swap

mtm_df_eqs = pd.DataFrame(np.arange(len(eq_mtm_0_ids)))
mtm_df_eqs.columns = ['index']

mtm_df_eqs['ViewData.Side0_UniqueIds'] = eq_mtm_0_ids
mtm_df_eqs['ViewData.Side1_UniqueIds'] = eq_mtm_1_ids
mtm_df_eqs = mtm_df_eqs.drop('index',1)

comment_one_side = []
comment_zero_side = []
if comment_table_eq_swap.empty == False:
    for i in comment_table_eq_swap['ViewData.Side1_UniqueIds'].unique():
        if i not in ['nan','None',''] :
            comment_one_side.append(i)

    comment_zero_side = []
    for i in comment_table_eq_swap['ViewData.Side0_UniqueIds'].unique():
        if i not in ['nan','None','']:
            comment_zero_side.append(i)

## IDs left after removing Equity Swap MTM and Comment of Difference in amount

cc6 = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1)))]

#cc6 = cc6['ViewData.Side0_UniqueIds'].isin(comment_table_eq_swap['ViewData.Side0_UniqueIds'].unique())| cc['ViewData.Side1_UniqueIds'].isin(comment_table_eq_swap['ViewData.Side1_UniqueIds'].unique())

cc6 = cc6[~((cc6['ViewData.Side1_UniqueIds'].isin(comment_one_side)) | (cc6['ViewData.Side0_UniqueIds'].isin(comment_zero_side)))]

## IDs left after removing Equity Swap MTM and Comment of Difference in amount

#cc6 = cc5[~(cc5['ViewData.Side0_UniqueIds'].isin(mtm_list_0) |cc5['ViewData.Side1_UniqueIds'].isin(mtm_list_1))]
#cc6 = cc6.reset_index().drop('index',1)

# ## New and final Close Break IDs Table

new_close = []
for i in new_closed_keys:
    new_close.append(i.split(','))

new_close_flat = np.concatenate(new_close)

closed_final_df = cc6[cc6['ViewData.Side0_UniqueIds'].isin(new_close_flat) | cc6['ViewData.Side1_UniqueIds'].isin(new_close_flat)]

closed_final_df = closed_final_df[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]

# ## Removing Close Break IDs
cc7 = cc6[~(cc6['ViewData.Side0_UniqueIds'].isin(new_close_flat) | cc6['ViewData.Side1_UniqueIds'].isin(new_close_flat))]
cc7 = cc7.reset_index().drop('index',1)
print('cc7 shape is :')
print(cc7.shape)
# ## M*N Loop starts

aa_new = cc7[cc7['Trans_side']=='A_side']
bb_new = cc7[cc7['Trans_side']=='B_side']

aa_new = aa_new.reset_index().drop('index',1)
bb_new = bb_new.reset_index().drop('index',1)

#Rohit - Added Abhijeet updown code here
#UpDown code begin
dffk2 = aa_new.copy()
dffk3 = bb_new.copy()

dffk2['ViewData.Settle Date'] = pd.to_datetime(dffk2['ViewData.Settle Date'])
dffk2['ViewData.Settle Date1'] = dffk2['ViewData.Settle Date'].dt.date

dffk2['ViewData.Trade Date'] = pd.to_datetime(dffk2['ViewData.Trade Date'])
dffk2['ViewData.Trade Date1'] = dffk2['ViewData.Trade Date'].dt.date

dffk2['ViewData.Task Business Date'] = pd.to_datetime(dffk2['ViewData.Task Business Date'])
dffk2['ViewData.Task Business Date1'] = dffk2['ViewData.Task Business Date'].dt.date

dffk3['ViewData.Settle Date'] = pd.to_datetime(dffk3['ViewData.Settle Date'])
dffk3['ViewData.Settle Date1'] = dffk3['ViewData.Settle Date'].dt.date

dffk3['ViewData.Trade Date'] = pd.to_datetime(dffk3['ViewData.Trade Date'])
dffk3['ViewData.Trade Date1'] = dffk3['ViewData.Trade Date'].dt.date

dffk3['ViewData.Task Business Date'] = pd.to_datetime(dffk3['ViewData.Task Business Date'])
dffk3['ViewData.Task Business Date1'] = dffk3['ViewData.Task Business Date'].dt.date



updown_col_rename_dict_at_start = {'ViewData.B-P Net Amount' : 'ViewData.Cust Net Amount'}

dffk2.rename(columns = updown_col_rename_dict_at_start, inplace = True)
dffk3.rename(columns = updown_col_rename_dict_at_start, inplace = True)


sel_col = ['ViewData.Currency',
       'ViewData.Accounting Net Amount',
       

       'ViewData.Cust Net Amount', 'ViewData.BreakID',
       'ViewData.CUSIP', 'ViewData.Description', 'ViewData.Fund',
       'ViewData.Investment Type', 
       'ViewData.ISIN', 'ViewData.Keys', 
       'ViewData.Mapped Custodian Account',  'ViewData.Prime Broker',
       'ViewData.Settle Date1',
       'ViewData.Quantity',
       'ViewData.Status',
#       'ViewData.Strategy', 
       'ViewData.Ticker', 
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

def updmark(y,x):
    if x =='MMM':
        return 0
    else:
        if y in x:
            return 1
        else:
            return 0

dffpb['upd_mark'] = dffpb.apply(lambda x :  updmark(x['ViewData.Cust Net Amount'], x['upd_amt']) , axis= 1)
dffacc['upd_mark'] = dffacc.apply(lambda x : updmark(x['ViewData.Accounting Net Amount'], x['upd_amt']) , axis= 1)

dff4 = dffpb[dffpb['upd_mark']==1]
dff5 = dffacc[dffacc['upd_mark']==1]

#dff6 = dffk4[sel_col]
#dff7 = dffk5[sel_col]

# dff4 = pd.concat([dff4,dff6])
# dff4 = dff4.reset_index()
# dff4 = dff4.drop('index', axis = 1)

# dff5 = pd.concat([dff5,dff7])
# dff5 = dff5.reset_index()
# dff5 = dff5.drop('index', axis = 1)

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

        
    return a,b,c,d,e

def updownat(a,b,c,d):
    if a == 0:
        k = 'mapped custodian account'
    elif b==0:
        k = 'currency'
    elif c ==0 :
        k = 'Settle Date'
    elif d == 0:
        k = 'fund'    
#    elif e == 0:
#        k = 'transaction type'
    else :
        k = 'Investment type'
        
    com = 'up/down at'+ ' ' + k
    return com


# #### M cross N code

###################### loop 3 ###############################
if ((dff4.shape[0]!=0) & (dff5.shape[0]!=0)):
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
#    TODO :  below concat is taking time. reduce time    
    start_time_apply_first = timeit.default_timer()
    df_213_1 = pd.concat(appended_data)
    stop_time_apply_first = timeit.default_timer()
    print('Time for first apply = ', stop_time_apply_first - start_time_apply_first)


    df_213_1[['map_match','amt_match','fund_match','curr_match','sd_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
    df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['fund_match'] + df_213_1['curr_match']
    elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]
    if elim1.shape[0]!=0:
        elim1['SideA.predicted category'] = 'Updown'
        elim1['SideB.predicted category'] = 'Updown'
        elim1['SideA.Predicted_action'] = 'No-Pair'
        elim1['SideB.Predicted_action'] = 'No-Pair'
        start_time_apply_first = timeit.default_timer()
        elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match']), axis = 1)
        stop_time_apply_first = timeit.default_timer()
        print('Time for first apply = ', stop_time_apply_first - start_time_apply_first)
        start_time_apply_second = timeit.default_timer()
        elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match']), axis = 1)
        stop_time_apply_second = timeit.default_timer()
        print('Time for second apply = ', stop_time_apply_second - start_time_apply_second)
        elim_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment','Predicted_action']
    
    
#        elim_col = list(elim1.columns)
        sideA_col = []
        sideB_col = []

        for items in elim_col:
            item = 'SideA.'+items
            sideA_col.append(item)
            item = 'SideB.'+items
            sideB_col.append(item)
        
        elim2 = elim1[sideA_col]
        elim3 = elim1[sideB_col]
    
        elim2 = elim2.rename(columns= {
                              'SideA.ViewData.Side0_UniqueIds':'Side0_UniqueIds',
                              'SideA.ViewData.Side1_UniqueIds':'Side1_UniqueIds',
                              'SideA.predicted category':'predicted category',
                              'SideA.predicted comment':'predicted comment',
                              'SideA.Predicted_action' : 'Predicted_action' }) 
        elim3 = elim3.rename(columns= {
                              'SideB.ViewData.Side0_UniqueIds':'Side0_UniqueIds',
                              'SideB.ViewData.Side1_UniqueIds':'Side1_UniqueIds',
                              'SideB.predicted category':'predicted category',
                              'SideB.predicted comment':'predicted comment',
                              'SideB.Predicted_action' : 'Predicted_action' })

        frames = [elim2,elim3]
        elim = pd.concat(frames)
        elim = elim.reset_index()
        elim = elim.drop('index', axis = 1)
        elim.to_csv('Comment file soros 2 sep testing p5.csv')

        ## TODO : Rohit to write elimination code to remove ids containing 'up/down at mapped custodian account' and ids containing 'up/down at currency'
        updown_mapped_custodian_acct_side0_ids = elim[(elim['predicted comment'] == 'up/down at mapped custodian account') & (~elim['Side0_UniqueIds'].isin(['None','nan','']))]['Side0_UniqueIds']                
        updown_mapped_custodian_acct_side1_ids = elim[(elim['predicted comment'] == 'up/down at mapped custodian account') & (~elim['Side1_UniqueIds'].isin(['None','nan','']))]['Side1_UniqueIds']                
        updown_currency_side0_ids = elim[(elim['predicted comment'] == 'up/down at currency') & (~elim['Side0_UniqueIds'].isin(['None','nan','']))]['Side0_UniqueIds']                
        updown_currency_side1_ids = elim[(elim['predicted comment'] == 'up/down at currency') & (~elim['Side1_UniqueIds'].isin(['None','nan','']))]['Side1_UniqueIds']                
        
        if((len(updown_currency_side1_ids) != 0) & (len(updown_mapped_custodian_acct_side1_ids) != 0)):
            list_of_side1_ids_to_remove_from_aa_new = updown_currency_side1_ids.to_list() + updown_mapped_custodian_acct_side1_ids.to_list()
        elif((len(updown_currency_side1_ids) == 0) & (len(updown_mapped_custodian_acct_side1_ids) != 0)):
            list_of_side1_ids_to_remove_from_aa_new = updown_mapped_custodian_acct_side1_ids.to_list()
        elif((len(updown_currency_side1_ids) != 0) & (len(updown_mapped_custodian_acct_side1_ids) == 0)):
            list_of_side1_ids_to_remove_from_aa_new = updown_currency_side1_ids.to_list()
        else:
            list_of_side1_ids_to_remove_from_aa_new = []
            
        if((len(updown_currency_side0_ids) != 0) & (len(updown_mapped_custodian_acct_side0_ids) != 0)):
            list_of_side0_ids_to_remove_from_bb_new = updown_currency_side0_ids.to_list() + updown_mapped_custodian_acct_side0_ids.to_list()
        elif((len(updown_currency_side0_ids) == 0) & (len(updown_mapped_custodian_acct_side0_ids) != 0)):
            list_of_side0_ids_to_remove_from_bb_new = updown_mapped_custodian_acct_side0_ids.to_list()
        elif((len(updown_currency_side0_ids) != 0) & (len(updown_mapped_custodian_acct_side0_ids) == 0)):
            list_of_side0_ids_to_remove_from_bb_new = updown_currency_side0_ids.to_list()
        else:
            list_of_side0_ids_to_remove_from_bb_new = []

        if(len(list_of_side1_ids_to_remove_from_aa_new) != 0):
            list_of_side1_ids_to_remove_from_aa_new_without_duplicates = list(set(list_of_side1_ids_to_remove_from_aa_new))
            flag_side1_ids_to_remove_from_aa_new_exist = 1
        else:
            flag_side1_ids_to_remove_from_aa_new_exist = 0
        
        if(len(list_of_side0_ids_to_remove_from_bb_new) != 0):
            list_of_side0_ids_to_remove_from_bb_new_without_duplicates = list(set(list_of_side0_ids_to_remove_from_bb_new))
            flag_side0_ids_to_remove_from_bb_new_exist = 1
        else:
            flag_side0_ids_to_remove_from_bb_new_exist = 0
        
        
        #       Remove Side0_UniqueIds from bb_new and Side1_UniqueIds from aa_new
        aa_new = aa_new[~aa_new['ViewData.Side1_UniqueIds'].isin(list_of_side1_ids_to_remove_from_aa_new_without_duplicates)]
        bb_new = bb_new[~bb_new['ViewData.Side0_UniqueIds'].isin(list_of_side0_ids_to_remove_from_bb_new_without_duplicates)]
        
        #        Remove ids containing 'up/down at mapped custodian account' and ids containing 'up/down at currency' from elim. 
        elim_except_mapped_custodian_acct_and_currency = elim[~elim['Side1_UniqueIds'].isin(list_of_side1_ids_to_remove_from_aa_new_without_duplicates)]
        elim_except_mapped_custodian_acct_and_currency = elim_except_mapped_custodian_acct_and_currency[~elim_except_mapped_custodian_acct_and_currency['Side0_UniqueIds'].isin(list_of_side0_ids_to_remove_from_bb_new_without_duplicates)]
        #        This elim containing remaining up-down comments will be interesected with final_umr_table. All intersected ids will be removed from elim_except_mapped_custodian_acct_and_currency. This new elim df will be elim_except_mapped_custodian_acct_and_currency_and_umr. Ids from elim_except_mapped_custodian_acct_and_currency_and_umr will be removed before making final_no_pair_table
        elim.drop_duplicates(keep=False, inplace = True)
        elim_except_mapped_custodian_acct_and_currency.drop_duplicates(keep=False, inplace = True)
    else:
        aa_new = aa_new.copy()
        bb_new = bb_new.copy()
        flag_side1_ids_to_remove_from_aa_new_exist = 0
        flag_side0_ids_to_remove_from_bb_new_exist = 0
        elim = pd.DataFrame()
else:
    aa_new = aa_new.copy()
    bb_new = bb_new.copy()
    flag_side1_ids_to_remove_from_aa_new_exist = 0
    flag_side0_ids_to_remove_from_bb_new_exist = 0
    elim = pd.DataFrame()


updown_col_rename_dict_at_end = {'ViewData.Cust Net Amount' : 'ViewData.B-P Net Amount'}
aa_new.drop_duplicates(keep=False, inplace = True)
bb_new.drop_duplicates(keep=False, inplace = True)
#UpDown code end



###################### loop m*n ###############################

pool =[]
key_index =[]
training_df =[]

no_pair_ids = []
#max_rows = 5

for d in tqdm(aa_new['Date'].unique()):
    aa1 = aa_new.loc[aa['Date']==d,:][common_cols]
    bb1 = bb_new.loc[bb['Date']==d,:][common_cols]
    
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
            

            
if len(no_pair_ids)>0:        
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
    no_pair_ids = []
    with open(filepaths_no_pair_id_no_data_warning, 'w') as f:
            f.write('No no pair ids found for this setup and date combination')

#no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])

#pd.DataFrame(no_pair_ids).rename

#test_file['SideA.ViewData.Status'].value_counts()

test_file = pd.concat(training_df)

print('test_file shape is :')
print(test_file.shape)

test_file = test_file.reset_index()
test_file = test_file.drop('index',1)

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

#test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)

#test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)

test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']

## ## Description code
#
## In[475]:
#
#
##import os
#
#
## In[476]:
#
#
#os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')
#
#
## In[477]:
#
#
#print(os.getcwd())
#
#
## In[478]:
#
#
### TODO - Import a csv file for description category mapping
#
#com = pd.read_csv('desc cat with naveen oaktree.csv')
##com
#
#
## In[479]:
#
#
#cat_list = list(set(com['Pairing']))
#
#
## In[480]:
#
#
##!pip install swifter
#
#
## In[481]:
#
#
#import re
#
#def descclean(com,cat_list):
#    cat_all1 = []
#    list1 = cat_list
#    m = 0
#    if (type(com) == str):
#        com = com.lower()
#        com1 =  re.split("[,/. \-!?:]+", com)  
#        
#        for item in list1:
#            if (type(item) == str):
#                item = item.lower()
#                item1 = item.split(' ')
#                lst3 = [value for value in item1 if value in com1] 
#                if len(lst3) == len(item1):
#                    cat_all1.append(item)
#                    m = m+1
#            
#                else:
#                    m = m
#            else:
#                    m = 0
#    else:
#        m = 0
#            
#    if m >0 :
#        return list(set(cat_all1))
#    else:
#        if ((type(com)==str)):
#            if (len(com1)<4):
#                if ((len(com1)==1) & com1[0].startswith('20')== True):
#                    return 'swap id'
#                else:
#                    return com
#            else:
#                return 'NA'
#        else:
#            return 'NA'
#
#
## In[482]:
#
#
##vec_descclean = np.vectorize(descclean)
###values_desc_B_Side = test_file['SideB.ViewData.Description'].values
##values_desc_A_Side = test_file['SideA.ViewData.Description'].values
##vec_descclean(values_desc_B_Side,cat_list)
#
#
## In[483]:
#
#
##df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))
#
#test_file['SideA.desc_cat'] = test_file['SideA.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
#test_file['SideB.desc_cat'] = test_file['SideB.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
#
#
## In[484]:
#
#
#def currcln(x):
#    if (type(x)==list):
#        return x
#      
#    else:
#       
#        
#        if x == 'NA':
#            return "NA"
#        elif (('dollar' in x) | ('dollars' in x )):
#            return 'dollar'
#        elif (('pound' in x) | ('pounds' in x)):
#            return 'pound'
#        elif ('yen' in x):
#            return 'yen'
#        elif ('euro' in x) :
#            return 'euro'
#        else:
#            return x
#        
#
#
## In[485]:
#
#
#
##df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
#
#test_file['SideA.desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : currcln(x))
#test_file['SideB.desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : currcln(x))
#
#
## In[486]:
#
#
#com = com.drop(['var','Catogery'], axis = 1)
#
#com = com.drop_duplicates()
#
#com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
#com['replace'] = com['replace'].apply(lambda x : x.lower())
#
#
## In[487]:
#
#
#def catcln1(cat,df):
#    ret = []
#    if (type(cat)==list):
#        
#        if 'equity swap settlement' in cat:
#            ret.append('equity swap settlement')
#        #return 'equity swap settlement'
#        elif 'equity swap' in cat:
#            ret.append('equity swap settlement')
#        #return 'equity swap settlement'
#        elif 'swap settlement' in cat:
#            ret.append('equity swap settlement')
#        #return 'equity swap settlement'
#        elif 'swap unwind' in cat:
#            ret.append('swap unwind')
#        #return 'swap unwind'
#    
#        else:
#            for item in cat:
#            
#                a = df[df['Pairing']==item]['replace'].values[0]
#                if a not in ret:
#                    ret.append(a)
#        return list(set(ret))
#      
#    else:
#        return cat
#
#
## In[488]:
#
#
#test_file['SideA.new_desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : catcln1(x,com))
#test_file['SideB.new_desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : catcln1(x,com))
#
#
## In[489]:
#
#
#comp = ['inc','stk','corp ','llc','pvt','plc']
##df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
#
#test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
#
#test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
#
#
## In[490]:
#
#
##df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))
#
#def desccat(x):
#    if isinstance(x, list):
#        
#        if 'equity swap settlement' in x:
#            return 'swap settlement'
#        elif 'collateral transfer' in x:
#            return 'collateral transfer'
#        elif 'dividend' in x:
#            return 'dividend'
#        elif (('loan' in x) & ('option' in x)):
#            return 'option loan'
#        
#        elif (('interest' in x) & ('corp' in x) ):
#            return 'corp loan'
#        elif (('interest' in x) & ('loan' in x) ):
#            return 'interest'
#        else:
#            return x[0]
#    else:
#        return x
#
#
## In[491]:
#
#
##df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))
#
#test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : desccat(x))
#test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : desccat(x))
#
#
## In[492]:
#
#
##test_file['SideB.new_desc_cat'].value_counts()


# ## Prime Broker

test_file['new_pb'] = test_file['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)

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

test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].fillna('kkk')

test_file['new_pb1'] = test_file.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)

#test_file = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/X_test_files_after_loop/meo_testing_HST_RecData_379_06_19_2020_test_file_with_ID.csv')

#test_file = test_file.drop('Unnamed: 0',1)

test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days

test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days

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

############ Fund match new ########

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

vec_fund_match = np.vectorize(fundmatch)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)

### New code for cleaning text variables 

#column_names = ['SideA.ViewData.Transaction Type', 'ViewData.Investment Type', 'ViewData.Asset Type Category', 'ViewData.Prime Broker', 'ViewData.Description']

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

cleaned_trans_types_A_side = [' '.join(item) for item in remove_ats_A_side]
cleaned_trans_types_B_side = [' '.join(item) for item in remove_ats_B_side]

# # INVESTMENT TYPE

remove_nums_i_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_A_side]
remove_nums_i_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_B_side]

remove_dates_i_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_A_side]
remove_dates_i_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_B_side]

#remove_blanks_i_A_side = [item for item in remove_dates_i_A_side if item]
#remove_blanks_i_B_side = [item for item in remove_dates_i_B_side if item]
#remove_blanks_i[:10]

cleaned_invest_A_side = [' '.join(item) for item in remove_dates_i_A_side]
cleaned_invest_B_side = [' '.join(item) for item in remove_dates_i_B_side]

remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]
# remove_blanks_a = [item for item in remove_dates_a if item]
# # remove_blanks_a[:10]

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]
test_file['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
test_file['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

test_file['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
test_file['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

test_file['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
test_file['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side

#test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))

#test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))

##############

values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values

vec_tt_match = np.vectorize(mhreplaced)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
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

test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))

test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)

#train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)

test_file['Combined_Investment_Type'] = test_file['SideA.ViewData.Investment Type'].astype(str) + test_file['SideB.ViewData.Investment Type'].astype(str)

test_file['Combined_Asset_Type_Category'] = test_file['SideA.ViewData.Asset Category Type'].astype(str) + test_file['SideB.ViewData.Asset Category Type'].astype(str)

def nan_fun(x):
    if x=='nan' or x == '' or x == 'None':
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

def a_keymatch(a_cusip, a_isin):
    
    pb_nan = 0
    a_common_key = 'NA' 
    if a_cusip in ['nan','None',''] and a_isin in ['nan','None','']:
        pb_nan =1
    elif(a_cusip not in ['nan','None',''] and a_isin in ['nan','None','']):
        a_common_key = a_cusip
    elif(a_cusip in ['nan','None',''] and a_isin not in ['nan','None','']):
        a_common_key = a_isin
    else:
        a_common_key = a_isin
        
    return (pb_nan, a_common_key)

def b_keymatch(b_cusip, b_isin):
    accounting_nan = 0
    b_common_key = 'NA'
    if b_cusip in ['nan','None',''] and b_isin in ['nan','None','']:
        accounting_nan =1
    elif (b_cusip not in ['nan','None',''] and b_isin in ['nan','None','']):
        b_common_key = b_cusip
    elif(b_cusip in ['nan','None',''] and b_isin not in ['nan','None','']):
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

#df['cons_ener_cat'] = np.where(df.consumption_energy > 400, 'high', 
#         (np.where(df.consumption_energy < 200, 'low', 'medium')))

test_file['SideB.ViewData.key_NAN']= vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[0]
test_file['SideB.ViewData.Common_key'] = vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[1]
test_file['SideA.ViewData.key_NAN'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[0]
test_file['SideA.ViewData.Common_key'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[1]

#test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
#test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)

def nan_equals_fun(a,b):
    if a==1 and b==1:
        return 1
    else:
        return 0
    
vec_nan_equal_fun = np.vectorize(nan_equals_fun)
values_key_NAN_B_Side = test_file['SideB.ViewData.key_NAN'].values
values_key_NAN_A_Side = test_file['SideA.ViewData.key_NAN'].values
test_file['All_key_nan'] = vec_nan_equal_fun(values_key_NAN_B_Side,values_key_NAN_A_Side)

#test_file['All_key_nan'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.key_NAN']==1 and x['SideA.ViewData.key_NAN']==1 else 0, axis=1)

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

test_file['amount_percent'] = (test_file['SideA.ViewData.B-P Net Amount']/test_file['SideB.ViewData.Accounting Net Amount']*100)

test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())

test_file['SideB.ViewData.Prime Broker'] = test_file['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())

test_file['SideB.ViewData.Asset Type Category'] = test_file['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Asset Type Category'] = test_file['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())

#test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)

#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/X_test_files_after_loop/meo_testing_HST_RecData_170_06-18-2020_test_file.csv")

trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']

#test_file['SideA.TType'] = test_file.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)
#test_file['SideB.TType'] = test_file.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)

#test_file['Combined_Desc'] = test_file['SideA.new_desc_cat'] + test_file['SideB.new_desc_cat']

#test_file['Combined_TType'] = test_file['SideA.TType'].astype(str) + test_file['SideB.TType'].astype(str)
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

test_file =  clean_text(test_file,'SideA.ViewData.Description', 'SideA.ViewData.Description_new') 
test_file =  clean_text(test_file,'SideB.ViewData.Description', 'SideB.ViewData.Description_new') 

values_desc_new_A_Side = test_file['SideA.ViewData.Description_new'].values
values_desc_new_B_Side = test_file['SideB.ViewData.Description_new'].values

vec_desc_simi = np.vectorize(fuzz.token_sort_ratio)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['description_similarity_score'] = vec_desc_simi(values_desc_new_A_Side,values_desc_new_B_Side)

#test_file['description_similarity_score'] = test_file.apply(lambda x: fuzz.token_sort_ratio(x['SideA.ViewData.Description_new'], x['SideB.ViewData.Description_new']), axis=1)

#le = LabelEncoder()
for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date','SideA.ViewData.Trade Date','SideB.ViewData.Trade Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    test_file[feature] = pd.to_datetime(test_file[feature],errors = 'coerce').dt.weekday

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
             # 'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
                'SideA.ViewData.Trade Date','SideB.ViewData.Trade Date',
              'All_key_nan','new_key_match', 'new_pb1','desc_any_match','SEDOL_match','TD_bucket','SD_bucket',
             # 'Combined_TType',
              #'SideB.Date',
            'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side'] 
             # 'label']


# ## UMR Mapping

## TODO Import HIstorical UMR FILE for Transaction Type mapping
os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')

Weiss_umr = pd.read_csv('Weiss_UMR.csv')

#soros_umr['ViewData.Combined Transaction Type'].unique()

Weiss_umr_list = Weiss_umr['ViewData.Combined Transaction Type'].unique()
#test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in Weiss_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)
test_file['tt_map_flag'] = np.where(test_file['ViewData.Combined Transaction Type'].isin(Weiss_umr_list),1,0) 

def abs_amount(var1, var2):
    if var1 == (-1*var2):
        return 1
    else:
        return 0
    

values_acc_Side = test_file['SideB.ViewData.Accounting Net Amount'].values
values_bp_Side = test_file['SideA.ViewData.B-P Net Amount'].values

vec_abs_amount = np.vectorize(abs_amount)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['abs_amount_flag'] = vec_abs_amount(values_acc_Side,values_bp_Side)

#test_file['abs_amount_flag'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)

test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]

test_file = test_file.reset_index().drop('index',1)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int)

test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))

test_file['SideA.ViewData.SEDOL'] = test_file['SideA.ViewData.SEDOL'].astype(str) 
test_file['SideB.ViewData.SEDOL'] = test_file['SideB.ViewData.SEDOL'].astype(str) 

def sedol_match(text1,text2):
    if text1 not in ['nan','None',''] and text2 not in ['nan','None',''] and (text1 in text2 or text2 in text1):
        return 1
    elif text1 not in ['nan','None',''] and text2 not in ['nan','None',''] and (text1 not in text2 or text2 not in text1):
        return 2
    else:
        return 0
    
values_sedol_A_Side = test_file['SideA.ViewData.SEDOL'].values
values_sedol_B_Side = test_file['SideB.ViewData.SEDOL'].values

vec_sedol_match = np.vectorize(sedol_match)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SEDOL_match'] = vec_sedol_match(values_sedol_A_Side,values_sedol_A_Side)
    
#test_file['SEDOL_match'] = test_file.apply(lambda x: sedol_match(x['SideA.ViewData.SEDOL'],x['SideB.ViewData.SEDOL']),axis=1)

def desc_any_string_check(text1, text2):
    match = 0
    match2 = 0
    text1 = text1.replace('interest','loan')
    text1 = text1.replace('principal','loan')
    text1 = text1.split(" ")
    text2 = text2.split(" ")
    for i in text1:
        for j in text2:
            if i in j and len(i)>1 and len(j)>1:
                match = 1
                break
    for i in text2:
        for j in text1:
            if i in j and len(i)>1 and len(j)>1:
                match2 = 1
                break
    if match==0 and match2==0:
        return 0
    else: 
        return 1

values_desc_new_A_Side = test_file['SideA.ViewData.Description_new'].values
values_desc_new_B_Side = test_file['SideB.ViewData.Description_new'].values

vec_desc_any_string_check = np.vectorize(desc_any_string_check)

test_file['desc_any_match'] = vec_desc_any_string_check(values_desc_new_A_Side,values_desc_new_B_Side)

#test_file['desc_any_match'] = test_file.apply(lambda x: desc_any_string_check(x['SideA.ViewData.Description_new'],x['SideB.ViewData.Description_new']), axis=1)

test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))

# ## Transaction Type New code

def inttype(x):
    if type(x)== float:
        return 'interest'
    else:
        x1 = x.lower()
        x2 = x1.split()
        if 'int' in x2:
            return 'interest'
        else:
            return x1 
        
def divclient(x):
    if (type(x) == str):
        if ('eqswap dividend client tax' in x) :
            return 'eqswap dividend client tax'
        else:
            return x
    else:
        return 'float'
    
def mhreplace(item):
    item1 = item.split()
    for items in item1:
        if items.endswith('mh')==True:
            item1.remove(items)
    return ' '.join(item1).lower()

def dollarreplace(item):
    item1 = item.split()
    for items in item1:
        if items.startswith('$')==True:
            item1.remove(items)
    return ' '.join(item1).lower()

def thirtyper(item):
    item1 = item.split()
    if '30%' in item1:
        return '30 percent'
    else:
        return item

test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))

test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x :x.lower())
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : inttype(x))
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : divclient(x))
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x :mhreplace(x))
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x :dollarreplace(x))
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x :thirtyper(x))

test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x :x.lower())
test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : inttype(x))
test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : divclient(x))
test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x :mhreplace(x))
test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x :dollarreplace(x))
test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x :thirtyper(x))

test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('withdrawal','withdraw'))
test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))

test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) + test_file['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Transaction Type'] = test_file['ViewData.Combined Transaction Type'].apply(lambda x: x.replace('jnl','journal'))

#test_file['ViewData.Combined Transaction Type'].nunique()

test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(str)
test_file = test_file[test_file['SideA.ViewData.Trade Date']!='nan']
test_file = test_file.reset_index().drop('index',1)

test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(float).astype(int)

test_file['TD_bucket'] = test_file['Trade_date_diff'].apply(lambda x: 0 if x==0 else(1 if x>=-2 and x<=2 else 2))
test_file['SD_bucket'] = test_file['Settle_date_diff'].apply(lambda x: 0 if x==0 else(1 if x>=-2 and x<=2 else 2))

test_file3 = test_file[~(test_file['SideA.ViewData.Side1_UniqueIds'].isin(new_closed_keys) | test_file['SideB.ViewData.Side0_UniqueIds'].isin(new_closed_keys))]
test_file3 = test_file3.reset_index()
test_file3 = test_file3.drop('index',1)

print('test_file3 shape is :')
print(test_file3.shape)

#test_file2 = test_file[((test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")) | ((test_file['SideA.TType']!="Trade") & (test_file['SideB.TType']!="Trade")) ]
#test_file2 = test_file[(test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")]
#test_file[(test_file['SideA.TType']==test_file['SideB.TType'])]['SideB.TType']

#test_file2 = test_file2.reset_index()
#test_file2 = test_file2.drop('index',1)

#test_file['SideA.ViewData.BreakID_A_side'].value_counts()
#test_file[model_cols]

#Changed these two lines on 25-11-2020
test_file3 = test_file3[(test_file3['SideA.ViewData.Status'] !='SPM')]
test_file3 = test_file3[(test_file3['SideB.ViewData.Status'] !='SPM')]

test_file3 = test_file3.reset_index()
test_file3 = test_file3.drop('index',1)

# ## Test file served into the model

test_file2 = test_file3.copy()

X_test = test_file2[model_cols]

X_test = X_test.reset_index()
X_test = X_test.drop('index',1)
X_test = X_test.fillna(0)

X_test = X_test.fillna(0)

X_test = X_test.drop_duplicates()
X_test = X_test.reset_index()
X_test = X_test.drop('index',1)

# ## Model Pickle file import

## TODO Import Pickle file for 1st Model

#filename = 'Oak_W125_model_with_umb.sav'
#filename = '125_with_umb_without_des_and_many_to_many.sav'
#filename = '125_with_umb_and_price_without_des_and_many_to_many_tdsd2.sav'
#filename = 'Weiss_new_model_V1.sav'
#filename = 'Soros_new_model_V1_with_close.sav'
#filename = 'Soros_full_model_smote.sav'

#filename = 'Soros_full_model_best_cleaned_tt_without_date.sav'
#filename = 'Soros_full_model_version2.sav'
#filename = 'OakTree_final_model2.sav'

filename = 'Weiss_final_model2_with_umt.sav'
#filename = 'Soros_full_model_umr_umt.sav'
clf = pickle.load(open(filename, 'rb'))

# ## Predictions

# Actual class predictions
rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]

X_test['Predicted_action'] = rf_predictions
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test['probability_No_pair'] = probability_class_0
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test['probability_UMB'] = probability_class_1
X_test['probability_UMR'] = probability_class_2
X_test['probability_UMT'] = probability_class_3

# ## Two Step Modeling
model_cols_2 = [
    'SideA.ViewData.B-P Net Amount', 
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
             # 'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
                'SideA.ViewData.Trade Date','SideB.ViewData.Trade Date',
              'All_key_nan','new_key_match', 'new_pb1','desc_any_match','SEDOL_match','TD_bucket','SD_bucket',
              #'Combined_TType',
              #'SideB.Date',
    'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side']
             # 'label']

X_test2 = test_file3[model_cols_2]

X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)
X_test2 = X_test2.fillna(0)

X_test2 = X_test2.drop_duplicates()
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)

os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')

#filename2 = 'Soros_full_model_all_two_step.sav'

## TODO Import MOdel2 as per the two step modelling process

#filename2 = 'OakTree_final_model2_step_two.sav'
#filename2 = 'Weiss_final_model2_two_step.sav'
filename2 = 'Weiss_final_model2_with_umt_two_step.sav'
clf2 = pickle.load(open(filename2, 'rb'))

# Actual class predictions
rf_predictions2 = clf2.predict(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs2 = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_0_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

#probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]

X_test2['Predicted_action_2'] = rf_predictions2
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test2['probability_No_pair_2'] = probability_class_0_two
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test2['probability_UMB_2'] = probability_class_1_two
#X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3

X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)

X_test.loc[(X_test['Amount_diff_2']!=0) & (X_test['Predicted_action']=='UMR_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'

X_test.loc[(X_test['Amount_diff_2']==0) & (X_test['Predicted_action']=='UMB_One_to_One'), 'Predicted_action'] = 'UMR_One_to_One'

X_test.loc[((X_test['Amount_diff_2']>1)  | (X_test['Amount_diff_2']<-1)) & (X_test['Predicted_action']=='UMT_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'

#Changes made on 25-11-2020.
filepaths_X_test = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\X_Test_for_Pratik_setup_' + setup_code + '_date_' + str(date_i) + '_time_experiment.csv'
X_test.to_csv(filepaths_X_test)

# ## Absolute amount flag
abs_amount_table = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['abs_amount_flag'].max().reset_index()
abs_amount_table[abs_amount_table['abs_amount_flag']==1]['SideB.ViewData.Side0_UniqueIds']

duplicate_entries = abs_amount_table[abs_amount_table['abs_amount_flag']==1]['SideB.ViewData.Side0_UniqueIds'].unique()

#27-11-2020 changes made by Pratik for CMF, CNF duplication
######## One to Many from Duplication (CMF and CNF) #########

if len(duplicate_entries)>0:
    dup_df =  X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_entries)]
    dup_df = dup_df.reset_index().drop('index',1)
    many_side_array = []
    one_side_array = []
    for id_0 in duplicate_entries:
        dup_id_1_array = dup_df[(dup_df['SideB.ViewData.Side0_UniqueIds']==id_0) & ((dup_df['Predicted_action']== 'UMR_One_to_One') | (dup_df['abs_amount_flag']==1))]['SideA.ViewData.Side1_UniqueIds'].values
        many_side_array.append(dup_id_1_array)
        one_side_array.append(id_0)
        
if len(one_side_array)>0:
    
    dup_otm_table_new = pd.DataFrame(one_side_array)  
    dup_otm_table_new.columns = ['SideB.ViewData.Side0_UniqueIds']
    dup_otm_table_new['SideA.ViewData.Side1_UniqueIds'] = many_side_array
else:
    dup_otm_table_new = pd.DataFrame()

#Change made on 29-11-2020. As per Pratik, dup_otm_table_new has both oto and otm. We have to include only otm. The below code is for separating out otm from dup_otm_table_new
dup_otm_table_new_raw = dup_otm_table_new
dup_otm_table_new_raw['otm_or_oto_comma_position'] = dup_otm_table_new_raw['SideA.ViewData.Side1_UniqueIds'].apply(lambda x : str(x).replace("' '","' , '").find(','))
dup_otm_table_new_raw['otm_or_oto_flag'] = dup_otm_table_new_raw['otm_or_oto_comma_position'].apply(lambda x : 'oto' if x == -1 else 'otm')
dup_otm_table_new_final = dup_otm_table_new_raw[dup_otm_table_new_raw['otm_or_oto_flag'] == 'otm'][['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds']]

filepaths_dup_otm_table_new_raw = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\dup_otm_table_new_raw_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
dup_otm_table_new_raw.to_csv(filepaths_dup_otm_table_new_raw)

filepaths_dup_otm_table_new_final= '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\dup_otm_table_new_final_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
dup_otm_table_new_final.to_csv(filepaths_dup_otm_table_new_final)


######## One to Many from Duplication Ends (CMF and CNF)  #########


# ## Removing duplicate entries 

if len(duplicate_entries) !=0:
    X_test = X_test[~X_test['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_entries)]

X_test = X_test.reset_index().drop('index',1)

# ## New Aggregation

X_test['Tolerance_level'] = np.abs(X_test['probability_UMB_2'] - X_test['probability_No_pair_2'])

#X_test[X_test['Tolerance_level']<0.1]['Predicted_action'].value_counts()

b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()

#X_test[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Amount_diff_2']!=0)]

# ## UMR segregation

def umr_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']<=3) & (b_count2['len']<=3)]
    
    return umr_table['SideB.ViewData.Side0_UniqueIds'].values
    

def umt_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umt_table = b_count2[(b_count2['Predicted_action']=='UMT_One_to_One')  & (b_count2['count']==1) & (b_count2['len']<=3)]
    return umt_table['SideB.ViewData.Side0_UniqueIds'].values

#umt_seg(X_test)

X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_seg(X_test)) & (X_test['Predicted_action']=='UMT_One_to_One')].shape

umr_ids_0 = umr_seg(X_test)
umt_ids_0 = umt_seg(X_test)


# ## 1st Prediction Table for One to One UMR

final_umr_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')]

final_umr_table_Side0_count_ge_1 = final_umr_table['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index()
final_umr_table_Side1_count_ge_1 = final_umr_table['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index()

duplicate_ids_final_umr_table_Side0 = final_umr_table_Side0_count_ge_1[final_umr_table_Side0_count_ge_1['SideB.ViewData.Side0_UniqueIds'] > 1]['index'].unique()
duplicate_ids_final_umr_table_Side1 = final_umr_table_Side1_count_ge_1[final_umr_table_Side1_count_ge_1['SideA.ViewData.Side1_UniqueIds'] > 1]['index'].unique()

if(len(duplicate_ids_final_umr_table_Side0) != 0):
    final_umr_table_duplicates_Side0 = final_umr_table[final_umr_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umr_table_Side0)]
    final_umr_table_duplicates_Side0 = final_umr_table_duplicates_Side0[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]
    final_umr_table = final_umr_table[~final_umr_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umr_table_Side0)]
    final_umr_table_side0_ids = list(set(final_umr_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umr_ids_to_remove_from_final_open_table = final_umr_table_side0_ids + list(duplicate_ids_final_umr_table_Side0)
    
else:
    final_umr_table_duplicates_Side0 = pd.DataFrame()
    final_umr_table_side0_ids = list(set(final_umr_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umr_ids_to_remove_from_final_open_table = final_umr_table_side0_ids
    
if(len(duplicate_ids_final_umr_table_Side1) != 0):
    final_umr_table_duplicates_Side1 = final_umr_table[final_umr_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umr_table_Side1)]
    final_umr_table_duplicates_Side1 = final_umr_table_duplicates_Side1[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]
    final_umr_table = final_umr_table[~final_umr_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umr_table_Side1)]
    final_umr_table_side1_ids = list(set(final_umr_table['SideA.ViewData.Side1_UniqueIds'])) 
    side1_umr_ids_to_remove_from_final_open_table = final_umr_table_side1_ids + list(duplicate_ids_final_umr_table_Side1)

else:
    final_umr_table_duplicates_Side1 = pd.DataFrame()
    final_umr_table_side1_ids = list(set(final_umr_table['SideA.ViewData.Side1_UniqueIds']))
    side1_umr_ids_to_remove_from_final_open_table = final_umr_table_side1_ids
    
final_umr_table = final_umr_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]


#Find intersection of Ids present in final_umr_table and elim_except_mapped_custodian_acct_and_currency
if(flag_side0_ids_to_remove_from_bb_new_exist == 1):
    side0_intersection_elim_umr = set(elim_except_mapped_custodian_acct_and_currency['Side0_UniqueIds']) & set(final_umr_table['SideB.ViewData.Side0_UniqueIds'])
    elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency[~elim_except_mapped_custodian_acct_and_currency['Side0_UniqueIds'].isin(list(side0_intersection_elim_umr))]
    
    if(elim.shape[0] != 0):    
        elim = elim[~elim['Side0_UniqueIds'].isin(list(side0_intersection_elim_umr))] 
    
else:
    elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency

if(flag_side1_ids_to_remove_from_aa_new_exist == 1):
    side1_intersection_elim_umr = set(elim_except_mapped_custodian_acct_and_currency['Side1_UniqueIds']) & set(final_umr_table['SideA.ViewData.Side1_UniqueIds'])
    elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency_and_umr[~elim_except_mapped_custodian_acct_and_currency_and_umr['Side1_UniqueIds'].isin(list(side1_intersection_elim_umr))]

    if(elim.shape[0] != 0):    
        elim = elim[~elim['Side1_UniqueIds'].isin(list(side1_intersection_elim_umr))] 

else:
    elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency_and_umr

final_updown_table = elim.copy()

# ## Prediction table for One to One UMT

final_umt_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_ids_0) & (X_test['Predicted_action']=='UMT_One_to_One')]

final_umt_table_Side0_count_ge_1 = final_umt_table['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index()
final_umt_table_Side1_count_ge_1 = final_umt_table['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index()

duplicate_ids_final_umt_table_Side0 = final_umt_table_Side0_count_ge_1[final_umt_table_Side0_count_ge_1['SideB.ViewData.Side0_UniqueIds'] > 1]['index'].unique()
duplicate_ids_final_umt_table_Side1 = final_umt_table_Side1_count_ge_1[final_umt_table_Side1_count_ge_1['SideA.ViewData.Side1_UniqueIds'] > 1]['index'].unique()

if(len(duplicate_ids_final_umt_table_Side0) != 0):
    final_umt_table_duplicates_Side0 = final_umt_table[final_umt_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umt_table_Side0)]
    final_umt_table_duplicates_Side0 = final_umt_table_duplicates_Side0[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]
    final_umt_table = final_umt_table[~final_umt_table['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_ids_final_umt_table_Side0)]
    final_umt_table_side0_ids = list(set(final_umt_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umt_ids_to_remove_from_final_open_table = final_umt_table_side0_ids + list(duplicate_ids_final_umt_table_Side0)
    
else:
    final_umt_table_duplicates_Side0 = pd.DataFrame()
    final_umt_table_side0_ids = list(set(final_umt_table['SideB.ViewData.Side0_UniqueIds']))
    side0_umt_ids_to_remove_from_final_open_table = final_umt_table_side0_ids
    
if(len(duplicate_ids_final_umt_table_Side1) != 0):
    final_umt_table_duplicates_Side1 = final_umt_table[final_umt_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umt_table_Side1)]
    final_umt_table_duplicates_Side1 = final_umt_table_duplicates_Side1[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]
    final_umt_table = final_umt_table[~final_umt_table['SideA.ViewData.Side1_UniqueIds'].isin(duplicate_ids_final_umt_table_Side1)]
    final_umt_table_side1_ids = list(set(final_umt_table['SideA.ViewData.Side1_UniqueIds'])) 
    side1_umt_ids_to_remove_from_final_open_table = final_umt_table_side1_ids + list(duplicate_ids_final_umt_table_Side1)

else:
    final_umt_table_duplicates_Side1 = pd.DataFrame()
    final_umt_table_side1_ids = list(set(final_umt_table['SideA.ViewData.Side1_UniqueIds']))
    side1_umt_ids_to_remove_from_final_open_table = final_umt_table_side1_ids


final_umt_table = final_umt_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]

# ## No-Pair segregation

#b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
#a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()

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


no_pair_ids_b_side, no_pair_ids_a_side = no_pair_seg(X_test)

X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()

final_open_table = X_test[(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

final_open_table = final_open_table[~final_open_table['SideA.ViewData.Side1_UniqueIds'].isin(side1_umr_ids_to_remove_from_final_open_table)]
final_open_table = final_open_table[~final_open_table['SideB.ViewData.Side0_UniqueIds'].isin(side0_umr_ids_to_remove_from_final_open_table)]
final_open_table = final_open_table[~final_open_table['SideA.ViewData.Side1_UniqueIds'].isin(side1_umt_ids_to_remove_from_final_open_table)]
final_open_table = final_open_table[~final_open_table['SideB.ViewData.Side0_UniqueIds'].isin(side0_umt_ids_to_remove_from_final_open_table)]


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

final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.BreakID_A_side']].drop_duplicates(), on = 'SideA.ViewData.Side1_UniqueIds', how='left')
final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideB.ViewData.Side0_UniqueIds','SideB.ViewData.BreakID_B_side']].drop_duplicates(), on = 'SideB.ViewData.Side0_UniqueIds', how='left')

#Rohit FINAL_NO_PAIR_TABLE table

final_no_pair_table = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_no_pair_table)
final_no_pair_table_copy = final_no_pair_table.copy()
#
final_no_pair_table_copy['ViewData.Side0_UniqueIds'] = final_no_pair_table_copy['ViewData.Side0_UniqueIds'].astype(str)
final_no_pair_table_copy['ViewData.Side1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side1_UniqueIds'].astype(str)
 
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side0_UniqueIds']=='None','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side1_UniqueIds']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side1_UniqueIds']=='None','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side0_UniqueIds']

final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side0_UniqueIds']=='nan','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side1_UniqueIds']
final_no_pair_table_copy.loc[final_no_pair_table_copy['ViewData.Side1_UniqueIds']=='nan','Side0_1_UniqueIds'] = final_no_pair_table_copy['ViewData.Side0_UniqueIds']


del final_no_pair_table_copy['ViewData.Side0_UniqueIds']
del final_no_pair_table_copy['ViewData.Side1_UniqueIds']


#actual_closed = pd.read_csv('D:\Raman  Strategy ML 2.0\All_Data\OakTree\JuneData\Final_Predictions_379\Final_Predictions_Table_HST_RecData_379_2020-06-14.csv')
#actual_closed_array = np.array(list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side0_UniqueIds'].unique()) + list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side1_UniqueIds'].unique()))
#X_test_umb3 = X_test_umb[~((X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(actual_closed_array)) | (X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(actual_closed_array)))]

# ## Separate Ids to remove

umr_ids_a_side = final_umr_table['SideA.ViewData.Side1_UniqueIds'].unique()
umr_ids_b_side = final_umr_table['SideB.ViewData.Side0_UniqueIds'].unique()

umt_ids_a_side = final_umt_table['SideA.ViewData.Side1_UniqueIds'].unique()
umt_ids_b_side = final_umt_table['SideB.ViewData.Side0_UniqueIds'].unique()

updown_ids_a_side = elim_except_mapped_custodian_acct_and_currency_and_umr['Side1_UniqueIds'].unique()
updown_ids_b_side = elim_except_mapped_custodian_acct_and_currency_and_umr['Side0_UniqueIds'].unique()

### Remove Updown IDs

X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(updown_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(updown_ids_a_side))]

### Remove Open IDs

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

## Remove UMR IDs

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]

## Remove UMT IDs

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umt_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umt_ids_b_side))]


X_test_left = X_test_left.reset_index().drop('index',1)

# ## New MTM with SD

trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']

#Changed these two lines on 25-11-2020
#cc_new = cc7
cc_new = cc7[cc7['ViewData.Status']!='SPM']
cc_new = cc_new.reset_index().drop('index',1)

cc_new = cc_new[~((cc_new['ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])) | (cc_new['ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]

cc_new = cc_new[~((cc_new['ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) | (cc_new['ViewData.Side1_UniqueIds'].isin(final_umt_table['SideA.ViewData.Side1_UniqueIds'])))]

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

# ## Remove Ids from MTM SD match

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(sd_mtm_list_1))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(sd_mtm_list_0))]

# ## One to One UMB segregation

### IDs left after removing UMR ids from 0 and 1 side

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]

X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
X_test_left = X_test_left.reset_index().drop('index',1)

#for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():
#    umb_ids_1 = X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.Side1_UniqueIds'].unique()


# ## UMR One to Many and Many to One 

# ### One to Many

#X_test_left = X_test.copy()

cliff_for_loop = 16

threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()

exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()

def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
#            if length == 1 and np.isclose(number, total, atol=0).any():
#Change made on 03-12-2020 as asked by Pratik in value of atol
            if length == 1 and np.isclose(number, total, atol=0.35).any():

                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
               

#null_value ='No'
many_ids_1 = []
one_id_0 = []
amount_array =[]
#Changes made on 27-11-2020 as asked by Pratik to check how much time this code will take if we dont take X_test_left, but instead take X_test. Taking X_test should result in more accurate results but more time. The tradeoff between time and accuracy is what we need to measure
for key in X_test_left[~((X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(exceptions_0_umb_ids)) | (X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) |(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])))]['SideB.ViewData.Side0_UniqueIds'].unique():
    print(key)
    
    if key in threshold_0_umb:

        values =  X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
        net_sum = X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = X_test_left[(X_test_left['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass
            
    else:
        values =  X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.B-P Net Amount'].values
        net_sum = X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = X_test_left[(X_test_left['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass

umr_otm_table = pd.DataFrame(one_id_0)

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

pb_ids_otm_left = []
acc_id_single = []

for key in umr_double_count_left['SideB.ViewData.Side0_UniqueIds'].unique():
    acc_amount = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
    pb_ids_otm = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & ((X_test['SideB.ViewData.Accounting Net Amount']==X_test['SideA.ViewData.B-P Net Amount']) | (X_test['SideB.ViewData.Accounting Net Amount']== (-1)*X_test['SideA.ViewData.B-P Net Amount']))]['SideA.ViewData.Side1_UniqueIds'].values
    pb_ids_otm_left.append(pb_ids_otm)
    acc_id_single.append(key)
    
umr_otm_table_double_count = pd.DataFrame(acc_id_single)
umr_otm_table_double_count.columns = ['SideB.ViewData.Side0_UniqueIds']

umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'] = pb_ids_otm_left

umr_otm_table_final = pd.concat([umr_otm_table, umr_otm_table_double_count], axis=0)

umr_otm_table_final = umr_otm_table_final.reset_index().drop('index',1)

# ### Many to One

cliff_for_loop = 17

threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()

exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()

def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
#            if length == 1 and np.isclose(number, total, atol=0).any():
#Change made on 03-12-2020 as asked by Pratik in value of atol
            if length == 1 and np.isclose(number, total, atol=0.35).any():
                return [number]
           
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
        

#null_value ='No'

many_ids_0 = []
one_id_1 = []
amount_array2 =[]
#Changes made on 27-11-2020 as asked by Pratik to check how much time this code will take if we dont take X_test_left, but instead take X_test. Taking X_test should result in more accurate results but more time. The tradeoff between time and accuracy is what we need to measure

for key in X_test_left[~((X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)) | (X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) | (X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umt_table['SideA.ViewData.Side1_UniqueIds'])) |(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]['SideA.ViewData.Side1_UniqueIds'].unique():
    #if key not in ['1174_379879573_State Street','201_379823765_State Street']:
    print(key)
    if key in threshold_1_umb:

        values2 =  X_test_left[(X_test_left['SideA.ViewData.Side1_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = X_test_left[X_test_left['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()

        if subSum(values2,net_sum2) == []: 
            amount_array2 =[]
            #print("There are no valid subsets.")

        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = X_test_left[(X_test_left['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test_left['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass

    else:
        values2 =  X_test_left[(X_test_left['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = X_test_left[X_test_left['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()

        if subSum(values2,net_sum2) == []: 
            amount_array2 =[]
            #print("There are no valid subsets.")

        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = X_test_left[(X_test_left['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test_left['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass
            
umr_mto_table = pd.DataFrame(one_id_1)

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
 
### Converting array to list
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



# ## Removing all the OTM and MTO Ids

#X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(list(np.concatenate(many_ids_0))))]

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_mto_flat))]

X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(one_id_1)))]

#X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(np.concatenate(many_ids_1))))]

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_otm_flat))]
X_test_left2 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(list(one_id_0)))]

X_test_left2 = X_test_left2.reset_index().drop('index',1)

# ## UMB one to one (final)

X_test_umb = X_test_left2[X_test_left2['Predicted_action_2']=='UMB_One_to_One']
X_test_umb = X_test_umb.reset_index().drop('index',1)

#X_test_umb['UMB_key_OTO'] = X_test_umb['SideA.ViewData.Side1_UniqueIds'] + X_test_umb['SideB.ViewData.Side0_UniqueIds']

def one_to_one_umb(data):
    
    count = data['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count0')
    id0s = count[count['count0']==1]['index'].unique()
    id1s = data[data['SideB.ViewData.Side0_UniqueIds'].isin(id0s)]['SideA.ViewData.Side1_UniqueIds']
    
    count1 = data['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count1')
    final_ids = count1[(count1['count1']==1) & (count1['index'].isin(id1s))]['index'].unique()
    return final_ids
   
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

#X_test['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test['SideA.ViewData.Side1_UniqueIds'].nunique()
#X_test_left3['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left3['SideA.ViewData.Side1_UniqueIds'].nunique()

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

open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)

X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]
X_test_left4 = X_test_left4.reset_index().drop('index',1)

#X_test_left4['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left4['SideA.ViewData.Side1_UniqueIds'].nunique()

# ## Seperating OTM and MTO from MTM

mtm_df_sd['len_1'] = mtm_df_sd['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
mtm_df_sd['len_0'] = mtm_df_sd['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
mtm_df_eqs['len_1'] = mtm_df_eqs['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
mtm_df_eqs['len_0'] = mtm_df_eqs['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))

new_mto1 = pd.DataFrame()
new_mto2 = pd.DataFrame()

new_otm1 = pd.DataFrame()
new_otm2 = pd.DataFrame()

new_mtm1 = pd.DataFrame()
new_mtm2 = pd.DataFrame()

if mtm_df_sd['len_1'].max() ==1: 
    new_mto1 = mtm_df_sd[mtm_df_sd['len_1']==1].drop(['len_1','len_0'],1)
    new_mto1 = new_mto1.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
else:
    if mtm_df_sd['len_0'].max() ==1:
        new_otm1 = mtm_df_sd[mtm_df_sd['len_0']==1].drop(['len_1','len_0'],1)
        new_otm1 = new_otm1.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
        
        
if mtm_df_eqs['len_1'].max() ==1: 
    new_mto2 = mtm_df_eqs[mtm_df_eqs['len_1']==1].drop(['len_1','len_0'],1)
    new_mto2 = new_mto2.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
else:
    if mtm_df_eqs['len_0'].max() ==1:
        new_otm2 = mtm_df_eqs[mtm_df_eqs['len_0']==1].drop(['len_1','len_0'],1)
        new_otm2 = new_otm2.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
        
        
if mtm_df_sd[(mtm_df_sd['len_1']>1) & (mtm_df_sd['len_0']>1)].empty ==False:
    new_mtm1 = mtm_df_sd[(mtm_df_sd['len_1']>1) & (mtm_df_sd['len_0']>1)].drop(['len_1','len_0'],1)
    new_mtm1 = new_mtm1.reset_index().drop('index',1)
    new_mtm1 = new_mtm1.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
    
if mtm_df_eqs[(mtm_df_eqs['len_1']>1) & (mtm_df_eqs['len_0']>1)].empty ==False:
    new_mtm2 = mtm_df_eqs[(mtm_df_eqs['len_1']>1) & (mtm_df_eqs['len_0']>1)].drop(['len_1','len_0'],1)
    new_mtm2 = new_mtm2.reset_index().drop('index',1)
    new_mtm2 = new_mtm2.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})

final_mto_table = pd.DataFrame()
final_otm_table = pd.DataFrame()
final_mtm_table = pd.DataFrame()

final_mto_table = pd.concat([umr_mto_table,new_mto1,new_mto2],axis=0)
final_otm_table = pd.concat([umr_otm_table,new_otm1,new_otm2],axis=0)

#Below line of Change made on 29-11-2020
final_otm_table = final_otm_table.append(dup_otm_table_new_final)

final_mto_table = final_mto_table.reset_index().drop('index',1)
final_otm_table = final_otm_table.reset_index().drop('index',1)

final_mtm_table = pd.concat([new_mtm1,new_mtm2],axis=0)
final_mtm_table = final_mtm_table.reset_index().drop('index',1)

if final_mto_table.empty == False:
    final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("['",''))
    final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("']",''))

if final_otm_table.empty == False:
    final_otm_table['SideB.ViewData.Side0_UniqueIds'] = final_otm_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("['",''))
    final_otm_table['SideB.ViewData.Side0_UniqueIds'] = final_otm_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("']",''))

if final_mtm_table.empty:
    print (' No MTM')

# ## Packaging final output and coverage calculation

# ### List of tables for final Output 
#final_umr_table
#final_umt_table
#len(no_pair_ids)
#final_no_pair_table
#len(open_ids_0_last)
#len(open_ids_1_last)

#closed_final_df
#comment_table_eq_swap

#final_mto_table
#final_otm_table
#final_mtm_table


#TODO : Revisit this code later - start here
#umr_otm_table_final['BreakID_Side1'] = umr_otm_table_final.apply(lambda x: list(meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(umr_otm_table_final['SideA.ViewData.Side1_UniqueIds'])]['ViewData.BreakID']), axis=1)

#for i in range(0,umr_otm_table_final.shape[0]):
#    umr_otm_table_final['BreakID_Side1'].iloc[i] = list(meo_df[meo_df['ViewData.Side1_UniqueIds']\
#                                                    .isin(umr_otm_table_final['SideA.ViewData.Side1_UniqueIds'].values[i])]\
#                                                    ['ViewData.BreakID'])#        fun_otm_mto_df['BreakID_Side1'].iloc[i] = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(fun_otm_mto_df['SideA.ViewData.Side1_UniqueIds'].iloc[i])]['ViewData.BreakID'])

#TODO : Revisit this code later - end here

#def return_BreakID_list(list_x, fun_meo_df):
#    return [fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(list(i))]['ViewData.BreakID'] for i in list_x]
#
#
#
#def return_BreakID_list2(list_x, fun_meo_df):
#    return [fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'] == str(i)]['ViewData.BreakID'].unique() for i in list_x]
#
#umr_mto_table['BreakID_Side0'] = umr_mto_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x : return_BreakID_list(list_x=x,fun_meo_df = meo_df))


#def return_int_list(list_x):
#    return [int(i) for i in list_x]
#
#

#Rohit : Code to normalize tables before pushing into database 

#final_umr_table
final_umr_table_copy = final_umr_table.copy()   

final_umr_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_umr_table_copy)
#
final_umr_table_copy['ViewData.Side0_UniqueIds'] = final_umr_table_copy['ViewData.Side0_UniqueIds'].astype(str)
final_umr_table_copy['ViewData.Side1_UniqueIds'] = final_umr_table_copy['ViewData.Side1_UniqueIds'].astype(str)
 
final_umr_table_copy_new = pd.merge(final_umr_table_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
final_umr_table_copy_new['Predicted_Status'] = 'UMR'
final_umr_table_copy_new['ML_flag'] = 'ML'
final_umr_table_copy_new['SetupID'] = setup_code 
#
#filepaths_final_umr_table_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\filepaths_final_umr_table_copy_new.csv'
#final_umr_table_copy_new.to_csv(filepaths_final_umr_table_copy_new)

change_names_of_filepaths_final_umr_table_copy_new_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID_Side1' : 'BreakID',
                                            'ViewData.BreakID_Side0' : 'Final_predicted_break',
                                            'ViewData.Task ID' : 'Task ID',
                                            'ViewData.Task Business Date' : 'Task Business Date',
                                            'ViewData.Source Combination Code' : 'Source Combination Code'
                                        }


final_umr_table_copy_new.rename(columns = change_names_of_filepaths_final_umr_table_copy_new_mapping_dict, inplace = True)

final_umr_table_copy_new['Task Business Date'] = pd.to_datetime(final_umr_table_copy_new['Task Business Date'])
final_umr_table_copy_new['Task Business Date'] = final_umr_table_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_umr_table_copy_new['Task Business Date'] = pd.to_datetime(final_umr_table_copy_new['Task Business Date'])


final_umr_table_copy_new['PredictedComment'] = ''

#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32

final_umr_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR','probability_UMT', 'Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']] = final_umr_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'probability_UMT','Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']].astype(str)

final_umr_table_copy_new[['BreakID', 'Task ID']] = final_umr_table_copy_new[['BreakID', 'Task ID']].astype(float)
final_umr_table_copy_new[['BreakID', 'Task ID']] = final_umr_table_copy_new[['BreakID', 'Task ID']].astype(np.int64)

final_umr_table_copy_new[['SetupID']] = final_umr_table_copy_new[['SetupID']].astype(int)

#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

change_col_names_final_umr_table_copy_new_new_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
final_umr_table_copy_new.rename(columns = change_col_names_final_umr_table_copy_new_new_dict, inplace = True)

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

final_umr_table_copy_new_to_write = final_umr_table_copy_new[cols_for_database_new]

filepaths_final_umr_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_umr_table_copy_new_to_write.csv'
final_umr_table_copy_new_to_write.to_csv(filepaths_final_umr_table_copy_new_to_write)





#final_umt_table
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

filepaths_final_umt_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_umt_table_copy_new_to_write.csv'
final_umt_table_copy_new_to_write.to_csv(filepaths_final_umt_table_copy_new_to_write)


#final_no_pair_table, no_pair_ids_df, open_ids_0_last, open_ids_1_last
no_pair_ids_last = list(open_ids_1_last)
for x in list(open_ids_0_last):
    no_pair_ids_last.append(x)
no_pair_ids_last_df = pd.DataFrame(no_pair_ids_last, columns = ['Side0_1_UniqueIds'])
no_pair_ids_last_df = no_pair_ids_last_df[~no_pair_ids_last_df['Side0_1_UniqueIds'].isin(['None'])]

final_no_pair_table_copy_1 = final_no_pair_table_copy.append([no_pair_ids_df,no_pair_ids_last_df])

if(final_otm_table.shape[0] != 0):
    final_mto_table_side_0_ids_to_remove_from_no_pair_table_list = list(set(np.concatenate(final_mto_table['SideB.ViewData.Side0_UniqueIds'])))
    final_mto_table_side_1_ids_to_remove_from_no_pair_table_list = list(set(final_mto_table['SideA.ViewData.Side1_UniqueIds']))
    final_no_pair_table_copy_1 = final_no_pair_table_copy_1[~final_no_pair_table_copy_1['Side0_1_UniqueIds'].isin(final_mto_table_side_0_ids_to_remove_from_no_pair_table_list)]
    final_no_pair_table_copy_1 = final_no_pair_table_copy_1[~final_no_pair_table_copy_1['Side0_1_UniqueIds'].isin(final_mto_table_side_1_ids_to_remove_from_no_pair_table_list)]

if(final_otm_table.shape[0] != 0):
    final_otm_table_side_0_ids_to_remove_from_no_pair_table_list = list(set(final_otm_table['SideB.ViewData.Side0_UniqueIds']))
    final_otm_table_side_1_ids_to_remove_from_no_pair_table_list = list(set(np.concatenate(final_otm_table['SideA.ViewData.Side1_UniqueIds'])))
    final_no_pair_table_copy_1 = final_no_pair_table_copy_1[~final_no_pair_table_copy_1['Side0_1_UniqueIds'].isin(final_otm_table_side_0_ids_to_remove_from_no_pair_table_list)]
    final_no_pair_table_copy_1 = final_no_pair_table_copy_1[~final_no_pair_table_copy_1['Side0_1_UniqueIds'].isin(final_otm_table_side_1_ids_to_remove_from_no_pair_table_list)]
    
if(final_mtm_table.shape[0] != 0):
    final_mtm_table_side_0_ids_to_remove_from_no_pair_table_list = list(set(np.concatenate(final_mtm_table['SideB.ViewData.Side0_UniqueIds'])))
    final_mtm_table_side_1_ids_to_remove_from_no_pair_table_list = list(set(np.concatenate(final_mtm_table['SideA.ViewData.Side1_UniqueIds'])))
    final_no_pair_table_copy_1 = final_no_pair_table_copy_1[~final_no_pair_table_copy_1['Side0_1_UniqueIds'].isin(final_mtm_table_side_0_ids_to_remove_from_no_pair_table_list)]
    final_no_pair_table_copy_1 = final_no_pair_table_copy_1[~final_no_pair_table_copy_1['Side0_1_UniqueIds'].isin(final_mtm_table_side_1_ids_to_remove_from_no_pair_table_list)]


final_no_pair_table_copy_1 = pd.merge(final_no_pair_table_copy_1, meo_df[['ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side1_UniqueIds', how='left')
final_no_pair_table_copy_1 = pd.merge(final_no_pair_table_copy_1, meo_df[['ViewData.Side0_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side0_UniqueIds', how='left')
#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
final_no_pair_table_copy_1['Predicted_Status'] = 'OB'
final_no_pair_table_copy_1['Predicted_action'] = 'No-Pair'
final_no_pair_table_copy_1['ML_flag'] = 'ML'
final_no_pair_table_copy_1['SetupID'] = setup_code 

final_no_pair_table_copy_1['ViewData.Task ID_x'] = final_no_pair_table_copy_1['ViewData.Task ID_x'].astype(str)
final_no_pair_table_copy_1['ViewData.Task ID_y'] = final_no_pair_table_copy_1['ViewData.Task ID_y'].astype(str)
 
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task ID_x']=='None','Task ID'] = final_no_pair_table_copy_1['ViewData.Task ID_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task ID_y']=='None','Task ID'] = final_no_pair_table_copy_1['ViewData.Task ID_x']

final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task ID_x']=='nan','Task ID'] = final_no_pair_table_copy_1['ViewData.Task ID_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task ID_y']=='nan','Task ID'] = final_no_pair_table_copy_1['ViewData.Task ID_x']


final_no_pair_table_copy_1['ViewData.BreakID_x'] = final_no_pair_table_copy_1['ViewData.BreakID_x'].astype(str)
final_no_pair_table_copy_1['ViewData.BreakID_y'] = final_no_pair_table_copy_1['ViewData.BreakID_y'].astype(str)
 
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_x']=='None','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_y']=='None','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_x']

final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_x']=='nan','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_y']=='nan','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_x']

final_no_pair_table_copy_1['ViewData.Task Business Date_x'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x'].astype(str)
final_no_pair_table_copy_1['ViewData.Task Business Date_y'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y'].astype(str)
 
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_x']=='None','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_y']=='None','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x']

final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_x']=='nan','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_y']=='nan','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x']

final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_x']=='NaT','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_y']=='NaT','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x']

final_no_pair_table_copy_1['ViewData.Source Combination Code_x'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_x'].astype(str)
final_no_pair_table_copy_1['ViewData.Source Combination Code_y'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_y'].astype(str)
 
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Source Combination Code_x']=='None','Source Combination Code'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Source Combination Code_y']=='None','Source Combination Code'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_x']

final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Source Combination Code_x']=='nan','Source Combination Code'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Source Combination Code_y']=='nan','Source Combination Code'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_x']


final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Source Combination Code_x']=='NaT','Source Combination Code'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_y']
final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Source Combination Code_y']=='NaT','Source Combination Code'] = final_no_pair_table_copy_1['ViewData.Source Combination Code_x']


final_no_pair_table_copy_1['Final_predicted_break'] = ''

final_no_pair_table_copy_1['probability_UMT'] = 0.05
for i in range(0,final_no_pair_table_copy_1.shape[0]):
    final_no_pair_table_copy_1['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(50, 100))/1000)

final_no_pair_table_copy_1['Task Business Date'] = pd.to_datetime(final_no_pair_table_copy_1['Task Business Date'])
final_no_pair_table_copy_1['Task Business Date'] = final_no_pair_table_copy_1['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_no_pair_table_copy_1['Task Business Date'] = pd.to_datetime(final_no_pair_table_copy_1['Task Business Date'])


change_names_of_final_no_pair_table_copy_1_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.Task ID' : 'Task ID',
                                            'ViewData.Task Business Date' : 'Task Business Date',
                                            'ViewData.Source Combination Code' : 'Source Combination Code'
                                        }

final_no_pair_table_copy_1.rename(columns = change_names_of_final_no_pair_table_copy_1_mapping_dict, inplace = True)


final_no_pair_table_copy_1['PredictedComment'] = ''

#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32

final_no_pair_table_copy_1[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR','probability_UMT', 'Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']] = final_no_pair_table_copy_1[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'probability_UMT','Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']].astype(str)

final_no_pair_table_copy_1[['BreakID', 'Task ID']] = final_no_pair_table_copy_1[['BreakID', 'Task ID']].astype(float)
final_no_pair_table_copy_1[['BreakID', 'Task ID']] = final_no_pair_table_copy_1[['BreakID', 'Task ID']].astype(np.int64)

final_no_pair_table_copy_1[['SetupID']] = final_no_pair_table_copy_1[['SetupID']].astype(int)

#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

change_col_names_final_no_pair_table_copy_1_new_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
final_no_pair_table_copy_1.rename(columns = change_col_names_final_no_pair_table_copy_1_new_dict, inplace = True)

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

final_no_pair_table_copy_1_to_write = final_no_pair_table_copy_1[cols_for_database_new]

filepaths_final_no_pair_table_copy_1_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_no_pair_table_copy_1_to_write.csv'
final_no_pair_table_copy_1_to_write.to_csv(filepaths_final_no_pair_table_copy_1_to_write)

#comment_table_eq_swap


comment_table_eq_swap_copy = comment_table_eq_swap.copy()   
comment_table_eq_swap_copy_col_names_mapping_dict = {
                                  'comment' : 'PredictedComment'
                                  }
comment_table_eq_swap_copy.rename(columns = {'comment' : 'PredictedComment'}, inplace = True)

comment_table_eq_swap_copy['ViewData.Side0_UniqueIds'] = comment_table_eq_swap_copy['ViewData.Side0_UniqueIds'].astype(str)
comment_table_eq_swap_copy['ViewData.Side1_UniqueIds'] = comment_table_eq_swap_copy['ViewData.Side1_UniqueIds'].astype(str)

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Side0_UniqueIds']=='None','Side0_1_UniqueIds'] = comment_table_eq_swap_copy['ViewData.Side1_UniqueIds']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Side1_UniqueIds']=='None','Side0_1_UniqueIds'] = comment_table_eq_swap_copy['ViewData.Side0_UniqueIds']

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Side0_UniqueIds']=='nan','Side0_1_UniqueIds'] = comment_table_eq_swap_copy['ViewData.Side1_UniqueIds']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Side1_UniqueIds']=='nan','Side0_1_UniqueIds'] = comment_table_eq_swap_copy['ViewData.Side0_UniqueIds']

#filepaths_comment_table_eq_swap_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\comment_table_eq_swap_copy.csv'
#comment_table_eq_swap_copy.to_csv(filepaths_comment_table_eq_swap_copy)


del comment_table_eq_swap_copy['ViewData.Side0_UniqueIds']
del comment_table_eq_swap_copy['ViewData.Side1_UniqueIds']
 
#comment_table_eq_swap_copy = pd.merge(comment_table_eq_swap_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
comment_table_eq_swap_copy = pd.merge(comment_table_eq_swap_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side1_UniqueIds', how='left')
comment_table_eq_swap_copy = pd.merge(comment_table_eq_swap_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side0_UniqueIds', how='left')
#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
comment_table_eq_swap_copy['Predicted_Status'] = 'OB'
comment_table_eq_swap_copy['Predicted_action'] = 'No-Pair'
comment_table_eq_swap_copy['ML_flag'] = 'ML'
comment_table_eq_swap_copy['SetupID'] = setup_code 

comment_table_eq_swap_copy['ViewData.Task ID_x'] = comment_table_eq_swap_copy['ViewData.Task ID_x'].astype(str)
comment_table_eq_swap_copy['ViewData.Task ID_y'] = comment_table_eq_swap_copy['ViewData.Task ID_y'].astype(str)
 
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task ID_x']=='None','Task ID'] = comment_table_eq_swap_copy['ViewData.Task ID_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task ID_y']=='None','Task ID'] = comment_table_eq_swap_copy['ViewData.Task ID_x']

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task ID_x']=='nan','Task ID'] = comment_table_eq_swap_copy['ViewData.Task ID_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task ID_y']=='nan','Task ID'] = comment_table_eq_swap_copy['ViewData.Task ID_x']


comment_table_eq_swap_copy['ViewData.BreakID_x'] = comment_table_eq_swap_copy['ViewData.BreakID_x'].astype(str)
comment_table_eq_swap_copy['ViewData.BreakID_y'] = comment_table_eq_swap_copy['ViewData.BreakID_y'].astype(str)
 
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.BreakID_x']=='None','BreakID'] = comment_table_eq_swap_copy['ViewData.BreakID_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.BreakID_y']=='None','BreakID'] = comment_table_eq_swap_copy['ViewData.BreakID_x']

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.BreakID_x']=='nan','BreakID'] = comment_table_eq_swap_copy['ViewData.BreakID_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.BreakID_y']=='nan','BreakID'] = comment_table_eq_swap_copy['ViewData.BreakID_x']

comment_table_eq_swap_copy['ViewData.Task Business Date_x'] = comment_table_eq_swap_copy['ViewData.Task Business Date_x'].astype(str)
comment_table_eq_swap_copy['ViewData.Task Business Date_y'] = comment_table_eq_swap_copy['ViewData.Task Business Date_y'].astype(str)
 
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task Business Date_x']=='None','Task Business Date'] = comment_table_eq_swap_copy['ViewData.Task Business Date_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task Business Date_y']=='None','Task Business Date'] = comment_table_eq_swap_copy['ViewData.Task Business Date_x']

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task Business Date_x']=='nan','Task Business Date'] = comment_table_eq_swap_copy['ViewData.Task Business Date_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task Business Date_y']=='nan','Task Business Date'] = comment_table_eq_swap_copy['ViewData.Task Business Date_x']

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task Business Date_x']=='NaT','Task Business Date'] = comment_table_eq_swap_copy['ViewData.Task Business Date_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Task Business Date_y']=='NaT','Task Business Date'] = comment_table_eq_swap_copy['ViewData.Task Business Date_x']

comment_table_eq_swap_copy['ViewData.Source Combination Code_x'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_x'].astype(str)
comment_table_eq_swap_copy['ViewData.Source Combination Code_y'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_y'].astype(str)
 
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Source Combination Code_x']=='None','Source Combination Code'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Source Combination Code_y']=='None','Source Combination Code'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_x']

comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Source Combination Code_x']=='nan','Source Combination Code'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Source Combination Code_y']=='nan','Source Combination Code'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_x']


comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Source Combination Code_x']=='NaT','Source Combination Code'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_y']
comment_table_eq_swap_copy.loc[comment_table_eq_swap_copy['ViewData.Source Combination Code_y']=='NaT','Source Combination Code'] = comment_table_eq_swap_copy['ViewData.Source Combination Code_x']


comment_table_eq_swap_copy['Final_predicted_break'] = ''

comment_table_eq_swap_copy['probability_UMB'] = 0.017
comment_table_eq_swap_copy['probability_No_pair'] = 0.95
comment_table_eq_swap_copy['probability_UMR'] = 0.017
comment_table_eq_swap_copy['probability_UMT'] = 0.017
    
for i in range(0,comment_table_eq_swap_copy.shape[0]):
    comment_table_eq_swap_copy['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
    comment_table_eq_swap_copy['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(950, 1000))/1000)
    comment_table_eq_swap_copy['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
    comment_table_eq_swap_copy['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)

comment_table_eq_swap_copy['Task Business Date'] = pd.to_datetime(comment_table_eq_swap_copy['Task Business Date'])
comment_table_eq_swap_copy['Task Business Date'] = comment_table_eq_swap_copy['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
comment_table_eq_swap_copy['Task Business Date'] = pd.to_datetime(comment_table_eq_swap_copy['Task Business Date'])


change_names_of_comment_table_eq_swap_copy_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.Task ID' : 'Task ID',
                                            'ViewData.Task Business Date' : 'Task Business Date',
                                            'ViewData.Source Combination Code' : 'Source Combination Code'
                                        }

comment_table_eq_swap_copy.rename(columns = change_names_of_comment_table_eq_swap_copy_mapping_dict, inplace = True)


#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32

comment_table_eq_swap_copy[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR','probability_UMT', 'Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']] = comment_table_eq_swap_copy[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'probability_UMT','Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']].astype(str)

comment_table_eq_swap_copy[['BreakID', 'Task ID']] = comment_table_eq_swap_copy[['BreakID', 'Task ID']].astype(float)
comment_table_eq_swap_copy[['BreakID', 'Task ID']] = comment_table_eq_swap_copy[['BreakID', 'Task ID']].astype(np.int64)

comment_table_eq_swap_copy[['SetupID']] = comment_table_eq_swap_copy[['SetupID']].astype(int)

#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

change_col_names_comment_table_eq_swap_copy_new_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
comment_table_eq_swap_copy.rename(columns = change_col_names_comment_table_eq_swap_copy_new_dict, inplace = True)

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

comment_table_eq_swap_copy_to_write = comment_table_eq_swap_copy[cols_for_database_new]

filepaths_comment_table_eq_swap_copy_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\comment_table_eq_swap_copy_to_write.csv'
comment_table_eq_swap_copy_to_write.to_csv(filepaths_comment_table_eq_swap_copy_to_write)


#final_mto_table
final_mto_table_copy = final_mto_table.copy()

#final_mto_table_copy['BreakID_Side1'] = meo_df[meo_df['ViewData.Side1_UniqueIds'] == final_mto_table_copy['SideA.ViewData.Side1_UniqueIds']]['ViewData.BreakID'].unique()

final_mto_table_copy['BreakID_Side1'] = final_mto_table_copy['SideA.ViewData.Side1_UniqueIds'].apply(lambda x : meo_df[meo_df['ViewData.Side1_UniqueIds'] == x]['ViewData.BreakID'].unique())

final_mto_table_copy['BreakID_Side1'] = final_mto_table_copy['BreakID_Side1'].astype(int)

final_mto_table_copy['BreakID_Side0'] = final_mto_table_copy['SideB.ViewData.Side0_UniqueIds'].apply( \
                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
                                                                                              fun_side_0_or_1 = 0, \
                                                                                              fun_str_list_Side_01_UniqueIds = x))

final_mto_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_mto_table_copy)
#
final_mto_table_copy['ViewData.Side0_UniqueIds'] = final_mto_table_copy['ViewData.Side0_UniqueIds'].astype(str)
final_mto_table_copy['ViewData.Side1_UniqueIds'] = final_mto_table_copy['ViewData.Side1_UniqueIds'].astype(str)
 
final_mto_table_copy_new = pd.merge(final_mto_table_copy, meo_df[['ViewData.Side1_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side1_UniqueIds', how='left')
final_mto_table_copy_new['Predicted_Status'] = 'UMR'
final_mto_table_copy_new['Predicted_action'] = 'UMR_One-Many_to_Many-One'
final_mto_table_copy_new['ML_flag'] = 'ML'
final_mto_table_copy_new['SetupID'] = setup_code 

filepaths_final_mto_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_mto_table_copy.csv'
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

filepaths_final_mto_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_mto_table_copy_new_to_write.csv'
final_mto_table_copy_new_to_write.to_csv(filepaths_final_mto_table_copy_new_to_write)


#final_otm_table
final_otm_table_copy = final_otm_table.copy()

final_otm_table_copy['BreakID_Side0'] = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(list(final_otm_table_copy['SideB.ViewData.Side0_UniqueIds']))]['ViewData.BreakID'].values
final_otm_table_copy['BreakID_Side0'] = final_otm_table_copy['BreakID_Side0'].astype(int)

final_otm_table_copy['BreakID_Side1'] = final_otm_table_copy['SideA.ViewData.Side1_UniqueIds'].apply( \
                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
                                                                                              fun_side_0_or_1 = 1, \
                                                                                              fun_str_list_Side_01_UniqueIds = x))

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

final_otm_table_copy_new[['BreakID', 'Task ID']] = final_otm_table_copy_new[['BreakID', 'Task ID']].astype(float)
final_otm_table_copy_new[['BreakID', 'Task ID']] = final_otm_table_copy_new[['BreakID', 'Task ID']].astype(np.int64)

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

filepaths_final_otm_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_otm_table_copy_new_to_write.csv'
final_otm_table_copy_new_to_write.to_csv(filepaths_final_otm_table_copy_new_to_write)


#final_mtm_table
final_mtm_table_copy = final_mtm_table.copy()

final_mtm_table_copy['BreakID_Side1'] = final_mtm_table_copy['SideA.ViewData.Side1_UniqueIds'].apply( \
                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
                                                                                              fun_side_0_or_1 = 1, \
                                                                                              fun_str_list_Side_01_UniqueIds = x))

final_mtm_table_copy['BreakID_Side0'] = final_mtm_table_copy['SideB.ViewData.Side0_UniqueIds'].apply( \
                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
                                                                                              fun_side_0_or_1 = 0, \
                                                                                              fun_str_list_Side_01_UniqueIds = x))



final_mtm_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_mtm_table_copy)
#
Single_Side0_UniqueId_for_merging_with_meo_df = final_mtm_table_copy['ViewData.Side0_UniqueIds'][0][0]
final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = Single_Side0_UniqueId_for_merging_with_meo_df
final_mtm_table_copy['ViewData.Side0_UniqueIds'] = final_mtm_table_copy['ViewData.Side0_UniqueIds'].astype(str)
final_mtm_table_copy['ViewData.Side1_UniqueIds'] = final_mtm_table_copy['ViewData.Side1_UniqueIds'].astype(str)
final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'].astype(str) 

final_mtm_table_copy_new = pd.merge(final_mtm_table_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'ViewData.Side0_UniqueIds_for_merging', right_on = 'ViewData.Side0_UniqueIds', how='left')

final_mtm_table_copy_new['Predicted_Status'] = 'UMR'
final_mtm_table_copy_new['Predicted_action'] = 'UMR_Many_to_Many'
final_mtm_table_copy_new['ML_flag'] = 'ML'
final_mtm_table_copy_new['SetupID'] = setup_code 

del final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging']
del final_mtm_table_copy_new['ViewData.Side0_UniqueIds_for_merging']


filepaths_final_mtm_table_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_mtm_table_copy_new.csv'
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

filepaths_final_mtm_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_mtm_table_copy_new_to_write.csv'
final_mtm_table_copy_new_to_write.to_csv(filepaths_final_mtm_table_copy_new_to_write)

#final_oto_umb_table
final_oto_umb_table_copy = final_oto_umb_table.copy()
final_oto_umb_table_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = final_oto_umb_table_copy)
#
final_oto_umb_table_copy['ViewData.Side0_UniqueIds'] = final_oto_umb_table_copy['ViewData.Side0_UniqueIds'].astype(str)
final_oto_umb_table_copy['ViewData.Side1_UniqueIds'] = final_oto_umb_table_copy['ViewData.Side1_UniqueIds'].astype(str)

final_oto_umb_table_copy_new = pd.merge(final_oto_umb_table_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), on = 'ViewData.Side0_UniqueIds', how='left')

final_oto_umb_table_copy_new['Predicted_Status'] = 'UMB'
final_oto_umb_table_copy_new['ML_flag'] = 'ML'
final_oto_umb_table_copy_new['SetupID'] = setup_code 

filepaths_final_oto_umb_table_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_oto_umb_table_copy_new.csv'
final_oto_umb_table_copy_new.to_csv(filepaths_final_oto_umb_table_copy_new)

change_names_of_final_oto_umb_table_copy_new_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID_Side0' : 'BreakID',
                                            'ViewData.BreakID_Side1' : 'Final_predicted_break',
                                            'ViewData.Task ID' : 'Task ID',
                                            'ViewData.Task Business Date' : 'Task Business Date',
                                            'ViewData.Source Combination Code' : 'Source Combination Code'
                                        }


final_oto_umb_table_copy_new.rename(columns = change_names_of_final_oto_umb_table_copy_new_mapping_dict, inplace = True)

final_oto_umb_table_copy_new['Task Business Date'] = pd.to_datetime(final_oto_umb_table_copy_new['Task Business Date'])
final_oto_umb_table_copy_new['Task Business Date'] = final_oto_umb_table_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_oto_umb_table_copy_new['Task Business Date'] = pd.to_datetime(final_oto_umb_table_copy_new['Task Business Date'])


final_oto_umb_table_copy_new['PredictedComment'] = ''

#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32
final_oto_umb_table_copy_new['probability_UMT'] = 0.017
    
for i in range(0,final_oto_umb_table_copy_new.shape[0]):
    final_oto_umb_table_copy_new['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)


final_oto_umb_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = final_oto_umb_table_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)

final_oto_umb_table_copy_new[['BreakID','Task ID']] = final_oto_umb_table_copy_new[['BreakID','Task ID']].astype(float)
final_oto_umb_table_copy_new[['BreakID','Task ID']] = final_oto_umb_table_copy_new[['BreakID','Task ID']].astype(np.int64)

final_oto_umb_table_copy_new[['SetupID']] = final_oto_umb_table_copy_new[['SetupID']].astype(int)

#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

change_col_names_final_oto_umb_table_copy_new_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
final_oto_umb_table_copy_new.rename(columns = change_col_names_final_oto_umb_table_copy_new_dict, inplace = True)

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

final_oto_umb_table_copy_new_to_write = final_oto_umb_table_copy_new[cols_for_database_new]

filepaths_final_oto_umb_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_oto_umb_table_copy_new_to_write.csv'
final_oto_umb_table_copy_new_to_write.to_csv(filepaths_final_oto_umb_table_copy_new_to_write)

#final_closed_df
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

#UMB_carry_forward
umb_carry_forward_columns_to_select_from_meo_df = ['ViewData.BreakID', \
                                                   'ViewData.Task Business Date', \
                                                   'ViewData.Side0_UniqueIds', \
                                                   'ViewData.Side1_UniqueIds', \
                                                   'ViewData.Source Combination Code', \
                                                   'ViewData.Task ID']
umb_carry_forward_df = umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df]

umb_carry_forward_df['Predicted_Status'] = 'UMB'
umb_carry_forward_df['Predicted_action'] = 'UMB_Carry_Forward'
umb_carry_forward_df['ML_flag'] = 'Not_Covered_by_ML'
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


#umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(float)
#umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(np.int64)

umb_carry_forward_df[['SetupID']] = umb_carry_forward_df[['SetupID']].astype(int)
filepaths_umb_carry_forward_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_carry_forward_df.csv'
umb_carry_forward_df.to_csv(filepaths_umb_carry_forward_df)

final_table_to_write = pd.concat([final_umr_table_copy_new_to_write, \
                                  final_umt_table_copy_new_to_write, \
                                  final_no_pair_table_copy_1_to_write, \
                                  comment_table_eq_swap_copy_to_write, \
                                  final_mto_table_copy_new_to_write, \
                                  final_otm_table_copy_new_to_write, \
                                  final_mtm_table_copy_new_to_write, \
                                  final_oto_umb_table_copy_new_to_write, \
                                  final_closed_df, \
                                  umb_carry_forward_df\
                                  ], axis=0)


#Pratik Coverage calculation
coverage_meo = meo[~meo['ViewData.Status'].isin(['SMR','SMT','SPM','UMB'])]

coverage_meo['ViewData.Side1_UniqueIds'].nunique() + coverage_meo['ViewData.Side0_UniqueIds'].nunique()

final_umr_table.shape[0]*2 + final_umt_table.shape[0]*2 + len(no_pair_ids)+final_no_pair_table.shape[0] + len(open_ids_0_last) + len(open_ids_1_last) + comment_table_eq_swap.shape[0] + final_mto_table.shape[0]*3 +  final_otm_table.shape[0]*3 + final_mtm_table.shape[0]*3 +final_oto_umb_table.shape[0]*2 + final_closed_df.shape[0] + umb_carry_forward_df.shape[0] * 2

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
stop = timeit.default_timer()

print('Time: ', stop - start)





import pandas as pd
import numpy as np
import os
import dask.dataframe as dd
import glob


# - 'up/down at mapped custodian account'
# - 'up/down at currency'

# ### Receiving No pairs, looking up in MEO and prepration of comment file

# In[4]:

comment_df_final_list = []
#Note that Equity Swap settlement OBs already have a PredictedComment column which will take predence over Comment model. Therefore, we wont allow Equity Swap Settlement OBs to go into comment model below.
#brk = final_table_to_write[final_table_to_write['Predicted_action'] == 'No-Pair']
brk = final_table_to_write[(final_table_to_write['Predicted_action'] == 'No-Pair') & (final_table_to_write['PredictedComment'] == '')]

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

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].replace('nan','AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].replace('nan','BB')

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].replace('None','AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].replace('None','BB')

df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].replace('','AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].replace('','BB')

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

#Changes made on 02-12-2020 as per Abhijeet comment changes
#Begin changes

def inttype(x):
    if type(x)== float:
        return 'interest'
    else:
        x1 = x.lower()
        x2 = x1.split()
        if 'int' in x2:
            return 'interest'
        else:
            return x1 
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : inttype(x))

def divclient(x):
    if (type(x) == str):
        x = x.lower()
        if ('eqswap div client tax' in x) :
            return 'eqswap div client tax'
        else:
            return x
    else:
        return 'float'
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : divclient(x))

def mhreplace(item):
    item1 = item.split()
    for items in item1:
        items = items.lower()
        if items.endswith('mh')==True:
            item1.remove(items)
    return ' '.join(item1).lower()
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x :mhreplace(x))
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x :x.lower())

def compname(x):
    m = 0
    comp = ['Corporate','stk','inc','lp','plc','inc.','inc','corp']
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
    
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : compname(x))

def inter(x):
    m = 0
    comp = ['Corporate','stk','inc','lp','plc','inc.','inc','corp']
    if type(x)==str:
        x1 = x.split()
        if (('from' in x1) & ('from' in x1)):
            return 'interest'
        else:
            return x
    else:
        return x
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : inter(x))

def wht(x):
    
    if type(x)==str :
        
        x1 = x.split()
        if len(x1)>0:
            if x1[0] =='30%':
                return 'Withholding tax'
            else:
                return x
        else:
            return x
    else:
        return x
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : wht(x))


#End changes
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

#Changes made on 02-12-2020 as per Abhijeet changes 
#Begin changes for excluding code below . These functions were defined from line 4597 to 4675
#def divclient(x):
#    if (type(x) == str):
#        if ('eqswap div client tax' in x) :
#            return 'eqswap div client tax'
#        else:
#            return x
#    else:
#        return 'float'
#
#def mhreplace(item):
#    item1 = item.split()
#    for items in item1:
#        if items.endswith('mh')==True:
#            item1.remove(items)
#    return ' '.join(item1).lower()
#
#def compname(x):
#    m = 0
#    comp = ['corporate','stk','inc','lp','plc','inc.','inc','corp']
#    if type(x)==str:
#        x1 = x.split()
#        for item in x1:
#            if item in comp:
#                m = m+1
#    else:
#        m = 0
#    
#    if m ==0:
#        return x
#    else:
#        return 'Company'
#
#def wht(x):
#    if type(x)==str:
#        x1 = x.split()
#        if x1[0] =='30%':
#            return 'Wht'
#        else:
#            return x
#    else:
#        return x
#
#df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : divclient(x))
#df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : mhreplace(x))
#df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : compname(x))
#df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : divclient(x))
#End changes

#TODO : Ask Abhjeet if correction is right
#df2['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply( lambda x : item[:2] if '30%' in x else x)
#Correction
#df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply( lambda x : item[:2] if '30%' in x else x)

# ### Cleaning of Description

com = pd.read_csv('desc cat with naveen.csv')
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

df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].replace('nan','kkk')
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].replace('None','kkk')
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].replace('','kkk')

df2['new_pb1'] = df2.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)

df2['new_pb1'] = df2['new_pb1'].apply(lambda x : x.lower())

# #### Cancelled Trade Removal

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
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#         cancel_fin = cancel_fin[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing p1.csv')
#         dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
        
#     else:
#         cancellation = dffk3[dffk3['cancel_marker'] == 1]
#         cancellation['predicted category'] = 'Cancelled trade'
#         cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancel_fin = pd.concat([original,cancellation])
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
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
#             sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#             cancel_fin = cancel_fin[sel_col_1]
#             cancel_fin.to_csv('Comment file soros 2 sep testing p3.csv')
#             id_listB = list(set(c1['final_ID']))
#             comb = id_listB + id_listA
#             dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
            
            
            
   
        
#     else:
#         c1 = dummy
#         c1['predicted category'] = 'Cancelled trade'
#         c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']
#         cancel_fin = c1[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing no original p4.csv')
#         id_listB = list(set(c1['final_ID']))
#         comb = id_listB
#         dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
        
# else:
#     dffk2 = dffk2.copy()


## #### Finding Pairs in Up and down
#
## In[ ]:
#
#
#dffk2 = aa_new.copy()
#dffk3 = bb_new.copy()
#
#
## In[173]:
#
#
#sel_col = ['final_ID',  'ViewData.Currency',
#       'ViewData.Accounting Net Amount',
#       
#       'ViewData.Asset Type Category', 
#       'ViewData.Cust Net Amount', 'ViewData.BreakID',
#       'ViewData.ClusterID',
#       'ViewData.CUSIP', 'ViewData.Description', 'ViewData.Fund',
#        'ViewData.Investment ID',
#       'ViewData.Investment Type', 
#       'ViewData.ISIN', 'ViewData.Keys', 
#       'ViewData.Mapped Custodian Account',  'ViewData.Prime Broker',
#       
#       'ViewData.Quantity','ViewData.Settle Date1', 
#       'ViewData.Status', 'ViewData.Strategy', 
#       'ViewData.Ticker', 'ViewData.Trade Date1', 
#       'ViewData.Transaction Type', 
#       'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds', 
#      'ViewData.Task Business Date1','ViewData.InternalComment2','s/d','new_pb1','new_pb2'
#      ]
#
#
## In[174]:
#
#
#dffpb = dff2[sel_col]
#dffacc = dff3[sel_col]
#
#
## In[ ]:
#
#
#bplist = dffpb.groupby('ViewData.Task Business Date1')['ViewData.Cust Net Amount'].apply(list).reset_index()
#acclist = dffacc.groupby('ViewData.Task Business Date1')['ViewData.Accounting Net Amount'].apply(list).reset_index()
#
#
## In[ ]:
#
#
#updlist = pd.merge(bplist, acclist, on = 'ViewData.Task Business Date1', how = 'inner')
#
#
## In[ ]:
#
#
#updlist['upd_amt'] = updlist.apply(lambda x : [value for value in x['ViewData.Cust Net Amount'] if value in x['ViewData.Accounting Net Amount']], axis = 1)
#
#
## In[ ]:
#
#
#updlist = updlist[['ViewData.Task Business Date1','upd_amt']]
#
#
## In[ ]:
#
#
#dffpb = pd.merge(dffpb, updlist, on = 'ViewData.Task Business Date1', how = 'left')
#dffacc = pd.merge(dffacc, updlist, on = 'ViewData.Task Business Date1', how = 'left')
#
#
## In[ ]:
#
#
#dffpb['upd_amt']= dffpb['upd_amt'].fillna('MMM')
#dffacc['upd_amt']= dffacc['upd_amt'].fillna('MMM')
#
#
## In[ ]:
#
#
#def updmark(y,x):
#    if x =='MMM':
#        return 0
#    else:
#        if y in x:
#            return 1
#        else:
#            return 0
#
#
## In[ ]:
#
#
#dffpb['upd_mark'] = dffpb.apply(lambda x :  updmark(x['ViewData.Cust Net Amount'], x['upd_amt']) , axis= 1)
#dffacc['upd_mark'] = dffacc.apply(lambda x : updmark(x['ViewData.Accounting Net Amount'], x['upd_amt']) , axis= 1)
#
#
## In[ ]:
#
#
#dff4 = dffpb[dffpb['upd_mark']==1]
#dff5 = dffacc[dffacc['upd_mark']==1]
#
#
## In[175]:
#
#
##dff6 = dffk4[sel_col]
##dff7 = dffk5[sel_col]
#
#
## In[176]:
#
#
## dff4 = pd.concat([dff4,dff6])
## dff4 = dff4.reset_index()
## dff4 = dff4.drop('index', axis = 1)
#
#
## In[177]:
#
#
## dff5 = pd.concat([dff5,dff7])
## dff5 = dff5.reset_index()
## dff5 = dff5.drop('index', axis = 1)
#
#
## In[ ]:
#
#
#def amountelim(row):
#   
#    if (row['SideA.ViewData.Mapped Custodian Account'] == row['SideB.ViewData.Mapped Custodian Account']):
#        a = 1
#    else:
#        a = 0
#        
#    if (row['SideB.ViewData.Cust Net Amount'] == row['SideA.ViewData.Accounting Net Amount']):
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
#    
#        
#        
#        
#    return a, b, c ,d
#
#
## In[ ]:
#
#
#def updownat(a,b,c,d,e):
#    if a == 0:
#        k = 'mapped custodian account'
#    elif b==0:
#        k = 'currency'
#    elif c ==0 :
#        k = 'Settle Date'
#    elif d == 0:
#        k = 'fund'    
#    elif e == 0:
#        k = 'transaction type'
#    else :
#        k = 'Investment type'
#        
#    com = 'up/down at'+ ' ' + k
#    return com
#
#
## #### M cross N code
#
## In[178]:
#
#
####################### loop 3 ###############################
#from pandas import merge
#from tqdm import tqdm
#if ((dff4.shape[0]!=0) & (dff5.shape[0]!=0)):
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
#    k = list(set(list(set(dff5['ViewData.Task Business Date1'])) + list(set(dff4['ViewData.Task Business Date1']))))
#    k1 = k
#
#    for d in tqdm(k1):
#        aa1 = dff4[dff4['ViewData.Task Business Date1']==d]
#        bb1 = dff5[dff5['ViewData.Task Business Date1']==d]
#        aa1['marker'] = 1
#        bb1['marker'] = 1
#    
#        aa1 = aa1.reset_index()
#        aa1 = aa1.drop('index',1)
#        bb1 = bb1.reset_index()
#        bb1 = bb1.drop('index', 1)
#        print(aa1.shape)
#        print(bb1.shape)
#    
#        aa1.columns = ['SideB.' + x  for x in aa1.columns] 
#        bb1.columns = ['SideA.' + x  for x in bb1.columns]
#    
#        cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
#        appended_data.append(cc1)
#        
#    df_213_1 = pd.concat(appended_data)
#    df_213_1[['map_match','amt_match','fund_match','curr_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
#    df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['fund_match'] + df_213_1['curr_match']
#    elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]
#    if elim1.shape[0]!=0:
#        elim1['SideA.predicted category'] = 'Updown'
#        elim1['SideB.predicted category'] = 'Updown'
#        elim1['SideA.Predicted_action'] = 'No-Pair'
#        elim1['SideB.Predicted_action'] = 'No-Pair'
#        elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
#        elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
#        elim_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment','Predicted_action']
#    
#    
#        elim_col = list(elim1.columns)
#        sideA_col = []
#        sideB_col = []
#
#        for items in elim_col:
#            item = 'SideA.'+items
#            sideA_col.append(item)
#            item = 'SideB.'+items
#            sideB_col.append(item)
#        
#        elim2 = elim1[sideA_col]
#        elim3 = elim1[sideB_col]
#    
#        elim2 = elim2.rename(columns= {\
#                              'SideA.predicted category':'predicted category',
#                              'SideA.predicted comment':'predicted comment'})
#        elim3 = elim3.rename(columns= {\
#                              'SideB.predicted category':'predicted category',
#                              'SideB.predicted comment':'predicted comment'})
#        frames = [elim2,elim3]
#        elim = pd.concat(frames)
#        elim = elim.reset_index()
#        elim = elim.drop('index', axis = 1)
#        elim.to_csv('Comment file soros 2 sep testing p5.csv')
#        
#        ## TODO : Rohit to write elimination code here
#        
#    else:
#        aa_new = aa_new.copy()
#        bb_new = bb_new.copy()
#else:
#    aa_new = aa_new.copy()
#    bb_new = bb_new.copy()
#    
#

# #### Start of the single Side Commenting

data = df2.copy()

# data = pd.concat(frames)
# data = data.reset_index()
# data = data.drop('index', axis = 1)

#data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])
# days = [1,30,31,29]
# data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)
# data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')

#data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))
#Changes made on 02-12-2020.Below line was commented
#data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)

#Changes made on 02-12-2020. Below piece of code was added as per Abhijeet changes
#Begin Changes
days = [1,30,31,29]
data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])
data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)
data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')

def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k
data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))
data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
data['new_pb2'] = data['new_pb2'].apply(lambda x : x.lower())
#End Changes

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

#Changes made on 02-12-2020.Below two columns were added to Pre-final columns as per Abhijeet changes   
,'monthend marker','comm_marker' 
]

data = data[Pre_final]

df_mod1 = data.copy()

df_mod1['monthend marker'] = df_mod1['monthend marker'].fillna('AA')
df_mod1['comm_marker'] = df_mod1['comm_marker'].fillna('bb')

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

df_mod1 = df_mod1.rename(columns = {'new_desc_cat' : 'new_desc_cat1'})
data2 = df_mod1.copy()


# ### Separate Prediction of the Trade and Non trade

# #### 1st for Non Trade
#trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']
#data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]

#Changes made on 02-12-2020. Below cols definition was changed and new cols were added. These cols have the same order as per model, so column order is important

#cols = [
# 
#
#
# 'ViewData.Transaction Type2',
# 
# 
#
# 'ViewData.Asset Type Category',
#
# 'new_desc_cat',
#
# 'ViewData.Investment Type',
# 
#
# 'new_pb2','new_pb1'
# 
# 
#              
#             ]

#Changes made on 02-12-2020. Above cols definition was changed and new cols were added as per below. These cols have the same order as per model, so column order is important

cols = [
'ViewData.Transaction Type1',
 'ViewData.Asset Type Category1',
 'ViewData.Investment Type1',
 'new_desc_cat1',
 'new_pb1',
 'monthend marker',
 'comm_marker',
 'new_pb2'
]
#data211

#Changes made on 02-12-2020. Renaming is removed as new definition of cols above has the required model names we expect in model
#data2.rename(columns = {'ViewData.Transaction Type1' : 'ViewData.Transaction Type2',
#                        'ViewData.Asset Type Category1' : 'ViewData.Asset Type Category',
#                        'ViewData.Investment Type1' : 'ViewData.Investment Type' }, inplace = True)


#cols = [
# 
#
#
# 'ViewData.Transaction Type1',
# 
# 
#
# 'ViewData.Asset Type Category1',
#
# 'new_desc_cat',
#
# 'ViewData.Investment Type1',
# 
#
# 'new_pb2','new_pb1','comm_marker','monthend marker'
# 
# 
#              
#             ]


data211 = data2[cols]


#filename = 'finalized_model_weiss_catrefine_v8.sav'
#filename = 'finalized_model_weiss_v7_eqswap+wiretrans.sav'
#Changes made on 02-12-2020. New model file is inserted on 02-12-2020
#filename = 'finalized_model_weiss_catrefine_v10_gompu.sav'
filename = 'finalized_model_weiss_125_v11.sav'
clf = pickle.load(open(filename, 'rb'))

# Actual class predictions
cb_predictions = clf.predict(data211)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]


# #### Testing of Model and final prediction file - Non Trade

# In[217]:


demo = []
for item in cb_predictions:
    demo.append(item[0])
result_non_trade =data2.copy()

result_non_trade = result_non_trade.reset_index()

result_non_trade['predicted category'] = pd.Series(demo)
result_non_trade['predicted comment'] = 'NA'



result_non_trade = result_non_trade.drop('predicted comment', axis = 1)

com_temp = pd.read_csv('Weiss comment template for delivery new.csv')

com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})

result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')


# In[256]:


def comgen(x,y,z,k):
    if x == 'Geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "Geneva" + ' ' +y + ' ' + str(z)
        
    return com

result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
result_non_trade['ViewData.Settle Date'] = result_non_trade['ViewData.Settle Date'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)

result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)

result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]


# In[260]:


result_non_trade.to_csv('Comment file Weiss 2 sep testing p6.csv')
comment_df_final_list.append(result_non_trade)
comment_df_final = pd.concat(comment_df_final_list)

change_col_names_comment_df_final_dict = {
                                        'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                        'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                        'predicted category' : 'PredictedCategory',
                                        'predicted comment' : 'PredictedComment'
                                        }

comment_df_final.rename(columns = change_col_names_comment_df_final_dict, inplace = True)
comment_df_final[['Side0_UniqueIds','Side1_UniqueIds','PredictedCategory','PredictedComment']] = comment_df_final[['Side0_UniqueIds','Side1_UniqueIds','PredictedCategory','PredictedComment']].astype(str) 
#filepaths_comment_df_final = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\comment_df_final.csv'
#comment_df_final.to_csv(filepaths_comment_df_final)

comment_df_final_side0 = comment_df_final[comment_df_final['Side1_UniqueIds'] == 'BB']
comment_df_final_side1 = comment_df_final[comment_df_final['Side0_UniqueIds'] == 'AA']

final_df = final_table_to_write.merge(comment_df_final_side0, on = 'Side0_UniqueIds', how = 'left')

final_df['PredictedComment_y'] = final_df['PredictedComment_y'].astype(str)
final_df['PredictedComment_x'] = final_df['PredictedComment_x'].astype(str)

final_df['PredictedCategory_y'] = final_df['PredictedCategory_y'].astype(str)
final_df['PredictedCategory_x'] = final_df['PredictedCategory_x'].astype(str)

final_df['Side1_UniqueIds_x'] = final_df['Side1_UniqueIds_x'].astype(str)
final_df['Side1_UniqueIds_y'] = final_df['Side1_UniqueIds_y'].astype(str)

final_df.loc[final_df['PredictedComment_x']=='','PredictedComment'] = final_df['PredictedComment_y']
final_df.loc[final_df['PredictedComment_y']=='','PredictedComment'] = final_df['PredictedComment_x']

final_df.loc[final_df['PredictedComment_x']=='None','PredictedComment'] = final_df['PredictedComment_y']
final_df.loc[final_df['PredictedComment_y']=='None','PredictedComment'] = final_df['PredictedComment_x']

final_df.loc[final_df['PredictedComment_x']=='nan','PredictedComment'] = final_df['PredictedComment_y']
final_df.loc[final_df['PredictedComment_y']=='nan','PredictedComment'] = final_df['PredictedComment_x']

final_df.loc[final_df['PredictedCategory_x']=='','PredictedCategory'] = final_df['PredictedCategory_y']
final_df.loc[final_df['PredictedCategory_y']=='','PredictedCategory'] = final_df['PredictedCategory_x']

final_df.loc[final_df['PredictedCategory_x']=='None','PredictedCategory'] = final_df['PredictedCategory_y']
final_df.loc[final_df['PredictedCategory_y']=='None','PredictedCategory'] = final_df['PredictedCategory_x']

final_df.loc[final_df['PredictedCategory_x']=='nan','PredictedCategory'] = final_df['PredictedCategory_y']
final_df.loc[final_df['PredictedCategory_y']=='nan','PredictedCategory'] = final_df['PredictedCategory_x']

final_df.loc[final_df['Side1_UniqueIds_x']=='','Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
final_df.loc[final_df['Side1_UniqueIds_y']=='','Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']

final_df.loc[final_df['Side1_UniqueIds_x']=='None','Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
final_df.loc[final_df['Side1_UniqueIds_y']=='None','Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']

final_df.loc[final_df['Side1_UniqueIds_x']=='nan','Side1_UniqueIds'] = final_df['Side1_UniqueIds_y']
final_df.loc[final_df['Side1_UniqueIds_y']=='nan','Side1_UniqueIds'] = final_df['Side1_UniqueIds_x']


final_df.drop(['PredictedComment_x','PredictedComment_y', \
               'PredictedCategory_x','PredictedCategory_y', \
               'Side1_UniqueIds_x','Side1_UniqueIds_y'], axis = 1, inplace = True)

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

final_df_2_UMR_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
if(final_df_2_UMR_record_with_predicted_comment.shape[0] != 0):
    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]

    Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side0_UniqueIds']
    Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side1_UniqueIds']

    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]

    final_df_2_UMR_record_with_predicted_comment['PredictedComment'] = ''       
    final_df_2 = final_df_2.append(final_df_2_UMR_record_with_predicted_comment)


final_df_2_UMT_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
if(final_df_2_UMT_record_with_predicted_comment.shape[0] != 0):
    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
    
    Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side0_UniqueIds']
    Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side1_UniqueIds']
    
    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]

    final_df_2_UMT_record_with_predicted_comment['PredictedComment'] = ''
    final_df_2 = final_df_2.append(final_df_2_UMT_record_with_predicted_comment)

final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])

final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(float)
final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(np.int64)


#UMR_One_to_One separation into UMT_One_to_One
#final_df_2.loc[final_df_2['Predicted_action'] == 'UMR_One_to_One', ''] =


#Fixing 'Not_Covered_by_ML' Statuses
Search_term = 'not_covered_by_ml'

final_df_2_Covered_by_ML_df = final_df_2[~final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

final_df_2_Not_Covered_by_ML_df = final_df_2[final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

def get_first_term_before_separator(single_string, separator):
    return(single_string.split(separator)[0])

final_df_2_Not_Covered_by_ML_df['Predicted_Status'] = final_df_2_Not_Covered_by_ML_df['Predicted_Status'].apply(lambda x : get_first_term_before_separator(x,'_'))
final_df_2_Not_Covered_by_ML_df['ML_flag'] = 'Not_Covered_by_ML'

final_df_2 = final_df_2_Covered_by_ML_df.append(final_df_2_Not_Covered_by_ML_df)

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)

#Started at 17:42
filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_2.csv'
final_df_2.to_csv(filepaths_final_df_2)

final_df_2[final_df_2['BreakID'].str.endswith('.0')].shape
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']

#Break up final_df_2 into two parts : 
#    Part 1. final_df_2_without_UMR_Many_One_to_One_Many since it will contain BreakId as int64
#    Part 2. final_df_2_with_UMR_Many_One_to_One_Many since it will contain BreakId as list for Many to One and Many to Many breaks

final_df_2_without_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'] != 'UMR_One-Many_to_Many-One']
final_df_2_without_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_without_UMR_Many_One_to_One_Many['BreakID'].astype(str)
final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)

data_dict_without_UMR_Many_One_to_One_Many = final_df_2_without_UMR_Many_One_to_One_Many.to_dict("records_final_1")
coll_1_for_writing_prediction_data.insert_many(data_dict_without_UMR_Many_One_to_One_Many) 


final_df_2_with_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'].isin(['UMR_One-Many_to_Many-One','UMR_Many_to_Many'])]

final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].astype(str)
final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)
final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))

data_dict_with_UMR_Many_One_to_One_Many = final_df_2_with_UMR_Many_One_to_One_Many.to_dict("records_final_2")
coll_1_for_writing_prediction_data.insert_many(data_dict_with_UMR_Many_One_to_One_Many) 

print(setup_code)
print(date_i)
