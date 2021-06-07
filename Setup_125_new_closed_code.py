# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 10:48:07 2020

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
    
#    Change added on 07-12-2020. As per business, in Fees and Commision, business is not matching Mapped Custodian Account anymore. Therefore, putting the condition that the first row value for Transaction Type matches 'Fees & Commision', then filter wont include Mapped Custodian Account. For all else, Mapped Custodian Account would be added in filter
    if(side_meo.iloc[1]['ViewData.Mapped Custodian Account'] == 'Fees & Comm'):
        side_meo['filter_key'] = side_meo['ViewData.Source Combination'].astype(str) + \
                                 side_meo['ViewData.Currency'].astype(str) + \
                                 side_meo['ViewData.Transaction Type'].astype(str)                             
    else:
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
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Including B-P Side for Fees & Comm just to be sure  
#                                 'Fees & Comm BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Fees & Comm'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                 'Fees & Comm Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['Fees & Comm'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                 'DEPOSIT WITHDRAWAL BP_side' : {'side' : 'PB',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Deposit and Withrawal interact with each other. Also, DEP means Deposit and WTH meand Withdrawal. 
##                                          'Transaction_Type' : ['DEPOSIT'],
#                                          'Transaction_Type' : ['DEPOSIT','DEP','WTH','WITHDRAWAL','WDRL'],
#                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'DEPOSIT WITHDRAWAL Acct_side' : {'side' : 'Acct',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Deposit and Withrawal interact with each other. Also, DEP means Deposit and WTH meand Withdrawal. 
##                                          'Transaction_Type' : ['DEPOSIT'],
#                                          'Transaction_Type' : ['DEPOSIT','DEP','WTH','WITHDRAWAL','WDRL'],
#                                           'Side_meo_training_df' : Acct_meo_training_df },
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Commenting out WITHDRAWAL as it is already covered above now 
##                                 'WITHDRAWAL' : {'side' : 'PB',
###                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Deposit and Withrawal interact with each other. Also, DEP means Deposit and WTH meand Withdrawal. 
###                                          'Transaction_Type' : ['WITHDRAWAL'],
##                                          'Transaction_Type' : ['DEPOSIT','DEP','WTH','WITHDRAWAL'],
##                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'Buy Sell BP_side' : {'side' : 'PB',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Buy and Sell sometimes interact with each other. 
#                                          'Transaction_Type' : ['Buy','Sell'],
#                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'Buy Sell Acct_side' : {'side' : 'Acct',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Buy and Sell sometimes interact with each other. 
#                                          'Transaction_Type' : ['Buy','Sell'],
#                                           'Side_meo_training_df' : Acct_meo_training_df },
#                                 '(CASH DEPOSIT)/(PAYMENT) BP_side' : {'side' : 'PB',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. (CASH DEPOSIT)/(PAYMENT) sometimes interact with each other. 
#                                          'Transaction_Type' : ['CASH DEPOSIT','PAYMENT'],
#                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 '(CASH DEPOSIT)/(PAYMENT) Acct_side' : {'side' : 'Acct',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Buy and Sell sometimes interact with each other. 
#                                          'Transaction_Type' : ['CASH DEPOSIT','PAYMENT'],
#                                           'Side_meo_training_df' : Acct_meo_training_df },
#                                 'SHORT POSITION (INTRST/DIVIDEND)/(CANCEL) BP_side' : {'side' : 'PB',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. SHORT POSITION INTRST/DIVIDEND and SHORT POSITION CANCEL interact with each other
#                                          'Transaction_Type' : ['SHORT POSITION INTRST/DIVIDEND','SHORT POSITION CANCEL'],
#                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'SHORT POSITION (INTRST/DIVIDEND)/(CANCEL) Acct_side' : {'side' : 'Acct',
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. Deposit and Withrawal interact with each other. Also, DEP means Deposit and WTH meand Withdrawal. 
##                                          'Transaction_Type' : ['DEPOSIT'],
#                                          'Transaction_Type' : ['SHORT POSITION INTRST/DIVIDEND','SHORT POSITION CANCEL'],
#                                           'Side_meo_training_df' : Acct_meo_training_df },
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. ARRANGING CASH COLLATERAL not covered previously 
                                 'ARRANGING CASH COLLATERAL BP_side' : {'side' : 'PB',
                                          'Transaction_Type' : ['ARRANGING CASH COLLATERAL'],
                                          'Side_meo_training_df' : BP_meo_training_df},
                                 'ARRANGING CASH COLLATERAL Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ARRANGING CASH COLLATERAL'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. ARRANGING CASH COLLATERAL not covered previously 
                                 'CASH BALANCE TYPE ADJUSTMENT BP_side' : {'side' : 'PB',
                                          'Transaction_Type' : ['CASH BALANCE TYPE ADJUSTMENT'],
                                          'Side_meo_training_df' : BP_meo_training_df},
                                 'CASH BALANCE TYPE ADJUSTMENT Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CASH BALANCE TYPE ADJUSTMENT'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. SECURITIES PURCHASED - CANCEL not covered previously 
                                 'SECURITIES PURCHASED - CANCEL BP_side' : {'side' : 'PB',
                                          'Transaction_Type' : ['SECURITIES PURCHASED - CANCEL'],
                                          'Side_meo_training_df' : BP_meo_training_df},
                                 'SECURITIES PURCHASED - CANCEL Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['SECURITIES PURCHASED - CANCEL'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                 'ForwardFX' : {'side' : 'Acct',
#                                          'Transaction_Type' : ['ForwardFX'],
#                                           'Side_meo_training_df' : Acct_meo_training_df },
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. DEP and WDRL category already covered above, so including again would make code catch duplicates
##                                 'DEP_WDRL' : {'side' : 'PB',
##                                          'Transaction_Type' : ['DEP','WDRL'],
##                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'Miscellaneous' : {'side' : 'PB',
#                                          'Transaction_Type' : ['Miscellaneous'],
#                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'REORG' : {'side' : 'PB',
#                                          'Transaction_Type' : ['REORG'],
#                                           'Side_meo_training_df' : BP_meo_training_df },
#                                 'Transfer' : {'side' : 'Acct',
#                                          'Transaction_Type' : ['Transfer'],
#                                           'Side_meo_training_df' : Acct_meo_training_df },
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. RECEIVE VS PAYMENT,DELIVER VS PAYMENT interact with each other
#                                 '(RECEIVE VS PAYMENT)/(DELIVER VS PAYMENT) BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['RECEIVE VS PAYMENT','DELIVER VS PAYMENT'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                 '(RECEIVE VS PAYMENT)/(DELIVER VS PAYMENT) Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['RECEIVE VS PAYMENT','DELIVER VS PAYMENT'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. WTH category already covered above, so including again would make code catch duplicates
##                                'WTH BP_side' : {'side' : 'PB',
##                                           'Transaction_Type' : ['WTH'],
##                                           'Side_meo_training_df' : BP_meo_training_df},
##                                'WTH Acct_side' : {'side' : 'Acct',
##                                           'Transaction_Type' : ['WTH'],
##                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'SPEC Stk Loan Jrl BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['SPEC Stk Loan Jrl'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'SPEC Stk Loan Jrl Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['SPEC Stk Loan Jrl'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Globex Fee BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Globex Fee'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'Globex Fee Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['Globex Fee'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'TRF (TO SHORT)/(FM MARGIN) MARK TO MARKET BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['TRF FM MARGIN MARK TO MARKET', 'TRF TO SHORT MARK TO MARKET'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'TRF (TO SHORT)/(FM MARGIN) MARK TO MARKET Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['TRF FM MARGIN MARK TO MARKET', 'TRF TO SHORT MARK TO MARKET'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. TRF TO SHORT MARK TO MARKET category already covered above, so including again would make code catch duplicates
##                                'TRF TO SHORT MARK TO MARKET BP_side' : {'side' : 'PB',
##                                           'Transaction_Type' : ['TRF TO SHORT MARK TO MARKET'],
##                                           'Side_meo_training_df' : BP_meo_training_df},
##                                'TRF TO SHORT MARK TO MARKET Acct_side' : {'side' : 'Acct',
##                                           'Transaction_Type' : ['TRF TO SHORT MARK TO MARKET'],
##                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'EQUITY SWAP SHORT PERFORMANCE BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['EQUITY SWAP SHORT PERFORMANCE'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'EQUITY SWAP SHORT PERFORMANCE Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['EQUITY SWAP SHORT PERFORMANCE'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
##                                              Change made on 07-12-2020 after talking with business to improve closed accuracy. DEP category already covered above, so including again would make code catch duplicates
##                                'DEP BP_side' : {'side' : 'PB',
##                                           'Transaction_Type' : ['DEP'],
##                                           'Side_meo_training_df' : BP_meo_training_df},
##                                'DEP Acct_side' : {'side' : 'Acct',
##                                           'Transaction_Type' : ['DEP'],
##                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'MARK TO THE MARKET BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['MARK TO THE MARKET'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'MARK TO THE MARKET Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['MARK TO THE MARKET'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'EQUITY SWAP SHORT FINANCING BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['EQUITY SWAP SHORT FINANCING'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'EQUITY SWAP SHORT FINANCING Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['EQUITY SWAP SHORT FINANCING'],
#                                           'Side_meo_training_df' : Acct_meo_training_df},
#                                'Dividend BP_side' : {'side' : 'PB',
#                                           'Transaction_Type' : ['Dividend'],
#                                           'Side_meo_training_df' : BP_meo_training_df},
#                                'Dividend Acct_side' : {'side' : 'Acct',
#                                           'Transaction_Type' : ['Dividend'],
#                                           'Side_meo_training_df' : Acct_meo_training_df}
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
meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/meo_df_setup_125_date_2020-12-08.csv'
meo_df = pd.read_csv(meo_filename)
#Comment out below three lines if running from db, in case they are commented
#meo_df = json_normalize(list_of_dicts_query_result_1)
#meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
#meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 

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
    for amount in smb_amount:
        ob = smb_ob_table[(smb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (smb_ob_table['ViewData.Status']=='OB')]
        smb = smb_ob_table[(smb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (smb_ob_table['ViewData.Status']=='SMB')]
#        if((ob.shape[0]==1) and (smb.shape[0]==1) and (ob['ViewData.Mapped Custodian Account'] == smb['ViewData.Mapped Custodian Account']) and (ob['ViewData.Currency'] == smb['ViewData.Currency']) and (ob['ViewData.Source Combination Code'] == smb['ViewData.Source Combination Code'])):
        if ob.shape[0]==1 and smb.shape[0]==1:
            ob_breakid.append(ob['ViewData.BreakID'].values)
            smb_breakid.append(smb['ViewData.BreakID'].values)
            
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

#Remove BreakIDs caught in final_smb_ob_table if final_smb_ob_table is not null
if(final_smb_ob_table.shape[0] != 0):
    final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].astype(np.int64)
    final_smb_ob_table['BreakID_OB'] = final_smb_ob_table['BreakID_OB'].astype(np.int64)
    
    final_smb_ob_table_BreakID_list =  list(final_smb_ob_table['BreakID_OB']) + list(final_smb_ob_table['BreakID_SMB'])
    meo = meo[~meo['ViewData.BreakID'].isin(final_smb_ob_table_BreakID_list)]
else:
    final_smb_ob_table_BreakID_list = []
#End change code made on 12-12-2020


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
#filepaths_final_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_closed_df.csv'
filepaths_final_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_closed_df_setup_' + setup_code + '_date_' + str(date_i) + '_2.csv'
final_closed_df.to_csv(filepaths_final_closed_df)
