# -*- coding: utf-8 -*-
"""
Created on Sun Feb 14 19:10:32 2021

@author: consultant138
"""

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
import memory_profiler


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

import subprocess
from subprocess import check_output, STDOUT, CalledProcessError

from RabbitMQ import RabbitMQ_Class as rb_mq
import pika

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

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



new_cols = ['ViewData.' + x for x in cols] + add

# ## Close Prediction Weiss

def equals_fun(a,b):
    if a == b:
        return 1
    else:
        return 0

vec_equals_fun = np.vectorize(equals_fun)

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
    print(fun_str_list_Side_01_UniqueIds)
    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
        if(fun_side_0_or_1 == 0):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
        elif(fun_side_0_or_1 == 1):
            element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([str_element_Side_01_UniqueIds])]['ViewData.BreakID'].unique()
            list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    return(list_BreakID_corresponding_to_Side_01_UniqueIds)

def get_first_non_null_value(string_of_values_separated_by_comma):
    if(string_of_values_separated_by_comma != '' and string_of_values_separated_by_comma != 'nan' and string_of_values_separated_by_comma != 'None' ):
        if(string_of_values_separated_by_comma.partition(',')[0] != '' and string_of_values_separated_by_comma.partition(',')[0] != 'nan' and string_of_values_separated_by_comma.partition(',')[0] != 'None'):
            return(string_of_values_separated_by_comma.partition(',')[0])
        else:
            return(get_first_non_null_value(string_of_values_separated_by_comma.partition(',')[2]))
    else:
        return('Blank value')        

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

def get_NetAmountDifference_for_BreakIds_from_BreakID_and_FinalPredictedBreakID_column_apply_row(fun_row, fun_meo_df):
    lst_Net_Amount_Difference_list_for_FinalPredictedBreak = list(fun_meo_df[fun_meo_df['ViewData.BreakID'].isin(fun_row['Final_predicted_break'].split(', '))]['ViewData.Net Amount Difference'].unique())
    lst_Net_Amount_Difference_list_for_BreakID = list(fun_meo_df[fun_meo_df['ViewData.BreakID'].isin(fun_row['BreakID'].split(', '))]['ViewData.Net Amount Difference'].unique())
    full_list_of_Net_Amount_Difference = lst_Net_Amount_Difference_list_for_FinalPredictedBreak + lst_Net_Amount_Difference_list_for_BreakID
#    full_list_of_Net_Amount_Difference_rounded_3_decimals = [round(num,3) for num in full_list_of_Net_Amount_Difference]
    full_list_of_Net_Amount_Difference_rounded_3_decimals = [num for num in full_list_of_Net_Amount_Difference]
#    sum_NetAmountDifference = sum(full_list_of_Net_Amount_Difference_rounded_3_decimals)
    sum_NetAmountDifference = round(sum(full_list_of_Net_Amount_Difference_rounded_3_decimals),3)

    if(abs(sum_NetAmountDifference) >= 0.01):
        Predicted_Status_new = 'UMT'
        Predicted_action_new = fun_row['Predicted_action'].replace('UMR','UMT')
    else:
        Predicted_Status_new = fun_row['Predicted_Status'] 
        Predicted_action_new = fun_row['Predicted_action']

#    return(Predicted_Status_new, Predicted_action_new)
    return(Predicted_Status_new, Predicted_action_new, sum_NetAmountDifference)

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

#Change added on New closed code 
mapping_dict_trans_type = {
                        'BUY_SELL' : ['Buy','Sell','buy','sell','BUY','SELL'],
                        'XSSHORT_Sell' : ['XSSHORT','Sell'],
                        'XSELL_Sell' : ['XSSHORT','Sell'],
                        'XBCOVER_Buy' : ['XBCOVER','Buy'],
                        'XBUY_Buy' : ['XBUY','Buy'],
                        'WITHDRAWAL_DEPOSIT' : ['WITHDRAWAL','DEPOSIT'],
                        'Withdraw_Deposit' : ['Withdraw','Deposit'],
                        'SPEC_Stk_Loan_Jrl_DEP' : ['SPEC Stk Loan Jrl','DEP'],
                        'SPEC_Stk_Loan_Jrl_WTH' : ['SPEC Stk Loan Jrl','WTH'],
                        'CASH_DEPOSIT_PAYMENT' : ['CASH DEPOSIT','PAYMENT'],
                        'DELIVER_RECEIVE_PAYMENT' : ['DELIVER VS PAYMENT','RECEIVE VS PAYMENT'],
                        'CANCEL_INTEREST' : ['CANCEL INTEREST','INTEREST'],
                        'TRF_FM_SHORT_MARK_TO_MARKET' : ['TRF FM MARGIN MARK TO MARKET','TRF TO SHORT MARK TO MARKET'],
                        'SHORT_POSITION_INTRST_DIVIDEND_CANCEL' : ['SHORT POSITION INTRST/DIVIDEND','SHORT POSITION CANCEL']
# Added on 27-12-2020 to catch non interacting transaction type.
                        ,'ARRANGING CASH COLLATERAL_non_interacting' : ['ARRANGING CASH COLLATERAL','ARRANGING CASH COLLATERAL']
                        ,'MARK TO THE MARKET_non_interacting' : ['MARK TO THE MARKET','MARK TO THE MARKET']
                        ,'CASH BALANCE TYPE ADJUSTMENT_non_interacting' : ['CASH BALANCE TYPE ADJUSTMENT','CASH BALANCE TYPE ADJUSTMENT']
                        ,'MARGIN TYPE JOURNAL_non_interacting' : ['MARGIN TYPE JOURNAL','MARGIN TYPE JOURNAL']
                        ,'JOURNAL_non_interacting' : ['JOURNAL','JOURNAL']
## Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
#                        ,'ForwardFX_UBS_UBFX_ON_OP' : ['ForwardFX','ForwardFX']
}

def assign_Transaction_Type_for_closing_apply_row(fun_row, fun_transaction_type_col_name = 'ViewData.Transaction Type'):
    if(fun_row[fun_transaction_type_col_name] in ['Buy','Sell','buy','sell','BUY','SELL']):
        Transaction_Type_for_closing = 'BUY_SELL'
    elif(fun_row[fun_transaction_type_col_name] in ['XSSHORT','Sell']):
        Transaction_Type_for_closing = 'XSSHORT_Sell'
    elif(fun_row[fun_transaction_type_col_name] in ['XSELL','Sell']):
        Transaction_Type_for_closing = 'XSELL_Sell'
    elif(fun_row[fun_transaction_type_col_name] in ['XBCOVER','Buy']):
        Transaction_Type_for_closing = 'XBCOVER_Buy'
    elif(fun_row[fun_transaction_type_col_name] in ['XBUY','Buy']):
        Transaction_Type_for_closing = 'XBUY_Buy'
    elif(fun_row[fun_transaction_type_col_name] in ['WITHDRAWAL','DEPOSIT']):
        Transaction_Type_for_closing = 'WITHDRAWAL_DEPOSIT'
    elif(fun_row[fun_transaction_type_col_name] in ['SPEC Stk Loan Jrl','DEP']):
        Transaction_Type_for_closing = 'SPEC_Stk_Loan_Jrl_DEP'
#Note that Transaction_Type_for_closing = 'SPEC_Stk_Loan_Jrl_WTH' will be covered in another column
#    elif(fun_row[fun_transaction_type_col_name] in ['SPEC Stk Loan Jrl','WTH']):
#        Transaction_Type_for_closing = 'SPEC_Stk_Loan_Jrl_WTH'        
    elif(fun_row[fun_transaction_type_col_name] in ['CASH DEPOSIT','PAYMENT']):
        Transaction_Type_for_closing = 'CASH_DEPOSIT_PAYMENT'
    elif(fun_row[fun_transaction_type_col_name] in ['DELIVER VS PAYMENT','RECEIVE VS PAYMENT']):
        Transaction_Type_for_closing = 'DELIVER_RECEIVE_PAYMENT'
    elif(fun_row[fun_transaction_type_col_name] in ['CANCEL INTEREST','INTEREST']):
        Transaction_Type_for_closing = 'CANCEL_INTEREST'
#     elif(fun_row[fun_transaction_type_col_name] in ['TRF FM MARGIN MARK TO MARKET','TRF TO SHORT MARK TO MARKET']):
#         Transaction_Type_for_closing = 'TRF_FM_SHORT_MARK_TO_MARKET'
    elif(fun_row[fun_transaction_type_col_name] in ['TRF FM MARGIN MARK TO MARKET','TRF TO SHORT MARK TO MARKET']):
        Transaction_Type_for_closing = 'TRF_FM_SHORT_MARK_TO_MARKET'
    elif(fun_row[fun_transaction_type_col_name] in ['SHORT POSITION INTRST/DIVIDEND','SHORT POSITION CANCEL']):
        Transaction_Type_for_closing = 'SHORT_POSITION_INTRST_DIVIDEND_CANCEL'
    elif(fun_row[fun_transaction_type_col_name] in ['Transfer','nan','None']):
        Transaction_Type_for_closing = 'TRANSFER_OR_NULL'
# Added on 27-12-2020 to catch non interacting transaction type.
    elif(fun_row[fun_transaction_type_col_name] in ['ARRANGING CASH COLLATERAL']):
        Transaction_Type_for_closing = 'ARRANGING CASH COLLATERAL_non_interacting'
    elif(fun_row[fun_transaction_type_col_name] in ['MARK TO THE MARKET']):
        Transaction_Type_for_closing = 'MARK TO THE MARKET_non_interacting'
    elif(fun_row[fun_transaction_type_col_name] in ['CASH BALANCE TYPE ADJUSTMENT']):
        Transaction_Type_for_closing = 'CASH BALANCE TYPE ADJUSTMENT_non_interacting'
    elif(fun_row[fun_transaction_type_col_name] in ['MARGIN TYPE JOURNAL']):
        Transaction_Type_for_closing = 'MARGIN TYPE JOURNAL_non_interacting'
    elif(fun_row[fun_transaction_type_col_name] in ['JOURNAL']):
        Transaction_Type_for_closing = 'JOURNAL_non_interacting'
# Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
#    elif((fun_row[fun_transaction_type_col_name] in ['ForwardFX']) & (fun_row['ViewData.Mapped Custodian Account'] in ['UBS_UBFX_ON','UBS_UBFX_OP'])):
#        Transaction_Type_for_closing = 'ForwardFX_UBS_UBFX_ON_OP'

    else:
         Transaction_Type_for_closing = fun_row[fun_transaction_type_col_name]
    return(Transaction_Type_for_closing)

def assign_Transaction_Type_for_closing_apply_row_2(fun_row, fun_transaction_type_col_name = 'ViewData.Transaction Type2'):
    if(fun_row[fun_transaction_type_col_name] in ['SPEC Stk Loan Jrl','WTH']):
        Transaction_Type_for_closing2 = 'SPEC_Stk_Loan_Jrl_WTH'
    else:
        Transaction_Type_for_closing2 = 'Not_SPEC_Stk_Loan_Jrl_WTH'
    return(Transaction_Type_for_closing2)


def assign_PB_Acct_side_row_apply(fun_row):
    if((fun_row['flag_side1'] >= 1) & (fun_row['flag_side0'] == 0)):
        PB_or_Acct_Side_Value = 'PB_Side'
    elif((fun_row['flag_side1'] == 0) & (fun_row['flag_side0'] >= 1)):
        PB_or_Acct_Side_Value = 'Acct_Side'
    else:
        PB_or_Acct_Side_Value = 'Non OB'

    return(PB_or_Acct_Side_Value)

def cleaned_meo(#fun_filepath_meo, 
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
    meo['Transaction_Type_for_closing'] = meo.apply(lambda row : assign_Transaction_Type_for_closing_apply_row(fun_row = row, fun_transaction_type_col_name = 'ViewData.Transaction Type'), axis = 1, result_type="expand")
    meo['ViewData.Transaction Type2'] = meo['ViewData.Transaction Type']
    meo['Transaction_Type_for_closing_2'] = meo.apply(lambda row : assign_Transaction_Type_for_closing_apply_row_2(fun_row = row, fun_transaction_type_col_name = 'ViewData.Transaction Type2'), axis = 1, result_type="expand")
    meo['abs_net_amount_difference'] = meo['ViewData.Net Amount Difference'].apply(lambda x : abs(x))
    meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
    return(meo)  

def interacting_closing_125(fun_df):
    fun_df['ViewData.Mapped Custodian Account'] = fun_df['ViewData.Mapped Custodian Account'].astype(str)
    fun_df['ViewData.Currency'] = fun_df['ViewData.Currency'].astype(str)    
    fun_df['ViewData.Source Combination Code'] = fun_df['ViewData.Source Combination Code'].astype(str)
    fun_df['abs_net_amount_difference'] = fun_df['abs_net_amount_difference'].astype(str)
    fun_df['filter'] = fun_df['ViewData.Source Combination Code'] + fun_df['ViewData.Mapped Custodian Account'] + fun_df['ViewData.Currency'] + fun_df['abs_net_amount_difference']
    grouped_by_filter_df = fun_df.groupby('filter').size().reset_index(name='counts_for_filter')
    merged_df_with_filter_counts = pd.merge(fun_df, grouped_by_filter_df, on = 'filter', how = 'left')
    merged_df_with_filter_counts_ge_1 = merged_df_with_filter_counts[merged_df_with_filter_counts['counts_for_filter'] > 1] 
    return(merged_df_with_filter_counts_ge_1)

def All_combination_file(fun_df):
    fun_df['filter_key'] = fun_df['ViewData.Source Combination Code'].astype(str) + \
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

def identifying_closed_breaks(fun_all_meo_combination_df, fun_setup_code_crucial, fun_trans_type_1, fun_trans_type_2):
    
    if(fun_all_meo_combination_df.shape[0] != 0):
        if(fun_setup_code_crucial == '125'):
    
            Matching_closed_break_df_1 = \
                fun_all_meo_combination_df[ \
                                            (fun_all_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin([fun_trans_type_1])) & \
                                            (fun_all_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin([fun_trans_type_2])) & \
    #                                         (fun_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
                                            (abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
                                            (fun_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != fun_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) \
                                             ]
            Matching_closed_break_df_2 = \
                fun_all_meo_combination_df[ \
                                            (fun_all_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin([fun_trans_type_2])) & \
                                            (fun_all_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin([fun_trans_type_1])) & \
    #                                         (fun_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
                                            (abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
                                            (fun_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != fun_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) \
                                             ]
    # Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
            if((fun_trans_type_1 == 'ForwardFX') & (fun_all_meo_combination_df['ViewData.Mapped Custodian Account_x'].iloc[0] in ['UBS_UBFX_ON','UBS_UBFX_OP'])):
                Matching_closed_break_df_forwardfx_UBS_UBFX_ON_OP = \
                    fun_all_meo_combination_df[ \
                                                (fun_all_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin([fun_trans_type_2])) & \
                                                (fun_all_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin([fun_trans_type_1])) & \
    #                                             ((fun_all_meo_combination_df['ViewData.Mapped Custodian Account_x'].astype(str) == fun_all_meo_combination_df['ViewData.Mapped Custodian Account_y'].astype(str) == 'UBS_UBFX_ON') | (fun_all_meo_combination_df['ViewData.Mapped Custodian Account_x'].astype(str) == fun_all_meo_combination_df['ViewData.Mapped Custodian Account_y'].astype(str) == 'UBS_UBFX_OP')) & \
    #                                             (fun_all_meo_combination_df['ViewData.PB_or_Acct_Side_x'].astype(str) == fun_all_meo_combination_df['PB_or_Acct_Side_y'].astype(str) == 'Acct_Side') & \
                                                (abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_x']).astype(str) == abs(fun_all_meo_combination_df['ViewData.Net Amount Difference_y']).astype(str)) & \
                                                (fun_all_meo_combination_df['ViewData.BreakID_x'].astype(str) != fun_all_meo_combination_df['ViewData.BreakID_y'].astype(str)) \
                                                 ]
            else:
                Matching_closed_break_df_forwardfx_UBS_UBFX_ON_OP = pd.DataFrame()
    
            closed_df_list = [ \
                              Matching_closed_break_df_1 \
                              , \
                              Matching_closed_break_df_2
    # Added on 27-12-202 to catch Tran Type = ForwardFX for Mapped Custodian Account values of UBS_UBFX_ON and UBS_UBFX_OP 
                              , \
                              Matching_closed_break_df_forwardfx_UBS_UBFX_ON_OP
    
                                 ]
            
            
            Transaction_type_closed_break_df = pd.concat(closed_df_list)
        if(Transaction_type_closed_break_df.shape[0] != 0):
            return(Transaction_type_closed_break_df)
        else:
            return(pd.DataFrame())
    else:
        return(pd.DataFrame())
#     return(set(
#                 Transaction_type_closed_break_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
#                 Transaction_type_closed_break_df['ViewData.Side1_UniqueIds_x'].astype(str)
#                ))

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
def make_Side0_Side1_columns_for_final_smb_ob_or_umb_ob_table_row_apply(row, fun_side, fun_umb_or_smb_flag):
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

def make_Side0_Side1_columns_for_final_smb_or_umb_ob_table(fun_final_smb_or_umb_ob_table, fun_meo_df, fun_umb_or_smb_flag):
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

    fun_final_smb_or_umb_ob_table['Side0_UniqueIds'] = fun_final_smb_or_umb_ob_table.apply(lambda row : make_Side0_Side1_columns_for_final_smb_ob_or_umb_ob_table_row_apply(row, fun_side = 0, fun_umb_or_smb_flag = flag_value),axis = 1,result_type="expand")
    fun_final_smb_or_umb_ob_table['Side1_UniqueIds'] = fun_final_smb_or_umb_ob_table.apply(lambda row : make_Side0_Side1_columns_for_final_smb_ob_or_umb_ob_table_row_apply(row, fun_side = 1, fun_umb_or_smb_flag = flag_value),axis = 1,result_type="expand")
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] == '', 'Side0_UniqueIds'] = fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name]
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] != '', 'Side0_UniqueIds'] = fun_final_smb_or_umb_ob_table['Side0_UniqueIds_OB'] + fun_final_smb_or_umb_ob_table[Side0_UniqueIds_col_name]
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] == '', 'Side1_UniqueIds'] = fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name]
#    fun_final_smb_or_umb_ob_table.iloc[fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] != '', 'Side1_UniqueIds'] = fun_final_smb_or_umb_ob_table['Side1_UniqueIds_OB'] + fun_final_smb_or_umb_ob_table[Side1_UniqueIds_col_name]

    fun_final_smb_or_umb_ob_table.drop(['Side0_UniqueIds_OB','Side1_UniqueIds_OB',Side0_UniqueIds_col_name,Side1_UniqueIds_col_name], axis = 1, inplace = True)

    return(fun_final_smb_or_umb_ob_table)



client = 'Weiss'

setup = '125'
setup_code = '125'
#filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
today = date.today()
#filepath_to_read_ReconDF_from = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\ReconDF_messages\\ReconDF_setup_' + str(setup_code) + '_date_' + str(today) + '_' + str(200) + '.csv'
#ReconDF = pd.read_csv(filepath_to_read_ReconDF_from)


mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
#                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
                 param_MONGO_HOST = '10.1.79.212', param_MONGO_PORT = 27017,

#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
db_for_reading_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB']
db_for_writing_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
#db_2_for_MEO_data_MLReconDB_Testing = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML_Testing']

#today = date.today()
#d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
#desired_date = d1 - timedelta(days=4)
#desired_date_str = desired_date.strftime("%Y-%m-%d")
#date_input = desired_date_str
#
#
##for setup_code in setup_code_list:
#print('Starting predictions for Weiss, setup_code = ')
#print(setup_code)
#
#
##filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
##filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_input) + '.csv'
#
#query_1_for_MEO_data = db_1_for_MEO_data['RecData_' + setup_code].find({ 
#                                                                     "LastPerformedAction": 31
#                                                             },
#                                                             {
#                                                                     "DataSides" : 1,
#                                                                     "BreakID" : 1,
#                                                                     "LastPerformedAction" : 1,
#                                                                     "TaskInstanceID" : 1,
#                                                                     "SourceCombinationCode" : 1,
#                                                                     "MetaData" : 1, 
#                                                                     "ViewData" : 1
#                                                             })
#list_of_dicts_query_result_1 = list(query_1_for_MEO_data)
rb_mq_obj_new_for_publish = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
#                 param_RABBITMQ_HOST_IP = 'vitblrmleng01.viteos.com', param_RABBITMQ_PORT = '5671', \vitblresbuat
                 param_RABBITMQ_HOST_IP = 'vu-uat', param_RABBITMQ_PORT = '5671', \
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
                 param_RABBITMQ_EXCHANGE = 'ReconPROD_PARALLELExchange_MLAck', \
                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML_Ack_PROD_PARALLEL', \
                 param_RABBITMQ_ROUTING_KEY = 'Recon2_ReadFromMLAck_PROD_PARALLEL', \
                 param_test_message_publishing = True, \
                 param_timeout = 10)

rb_mq_obj_new_for_acknowledgement = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
#                 param_RABBITMQ_HOST_IP = 'vitblrmleng01.viteos.com', param_RABBITMQ_PORT = '5671', \vitblresbuat
                 param_RABBITMQ_HOST_IP = 'vu-uat', param_RABBITMQ_PORT = '5671', \
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
                 param_RABBITMQ_EXCHANGE = 'ReconPROD_PARALLELExchange_MLAck', \
                 param_RABBITMQ_QUEUE = 'VNFRecon_WriteToML_Ack_PROD_PARALLEL', \
                 param_RABBITMQ_ROUTING_KEY = 'Recon2_ReadFromMLAck_PROD_PARALLEL', \
                 param_test_message_publishing = True, \
                 param_timeout = 10)

while_loop_iterator = 0
while True:
    try:
      s2_out = subprocess.check_output([sys.executable, 'D:/ViteosModel/Setup_' + setup_code + '_ML2_RMQ_Receive.py'])
    except Exception:
        data = None
    
#Note that message from .Net code is as follows:
#string message = string.Format("{0}|{1}|{2}|{3}|{4}|{5}|{6}", task.InstanceID, task.ReconSetupForTask.Client.ClientShortCode, task.ReconSetupForTask.ReconPurpose.ReconPurpose, ReconciliationDataRepository.GetReconDataCollection(task.ReconSetupCode), processID, "Recon Run Completed", task.ReconSetupCode) 
       
    # Decoding the output of rabbit MQ message
    s2_stout=str(s2_out, 'utf-8')
    stout_list = s2_stout.split("|")
    
    print (stout_list)
    if len(stout_list) > 1:
        while_loop_iterator = while_loop_iterator + 1

        print('Receiving method worked')
#        sys.exit(1)
        #Converting input message to send as aruguments in prediction script
        smallerlist = [l.split(',') for l in ','.join(stout_list).split('\n')]
        
        ReconDF = DataFrame.from_records(smallerlist)
        ReconDF = ReconDF.dropna(how='any',axis=0)
#        ReconDF.columns = ['TaskID', 'csc','ReconPurpose','collection_meo','RequestId']
        ReconDF.columns = ['TaskID', 'csc','ReconPurpose','collection_meo','ProcessID','Completed_Status','Setup_Code','MongoDB_TaskID']
        
        ReconDF['TaskID'] = ReconDF['TaskID'].str.lstrip("b'")
        ReconDF['ProcessID'] = ReconDF['ProcessID'].str.replace(r"[^0-9]"," ")
        ReconDF['Setup_Code'] = ReconDF['Setup_Code'].str.rstrip("'\r")
        ReconDF['MongoDB_TaskID'] = ReconDF['MongoDB_TaskID'].str.rstrip("'\r")
        
    
        print('ReconDF')
        print(ReconDF)
        ReconDF_filepath = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\ReconDF_messages\\ReconDF_setup_' + str(setup_code) + '_date_' + str(today) + '_' + str(while_loop_iterator) + '_100.csv'
        ReconDF.to_csv(ReconDF_filepath)

#        d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
#        desired_date = d1 - timedelta(days=4)
#        desired_date_str = desired_date.strftime("%Y-%m-%d")
#        date_input = desired_date_str
        
        for z in range(ReconDF.shape[0]):
        #for setup_code in setup_code_list:

    

            TaskID_z = ReconDF['TaskID'].iloc[z]
            print('Starting predictions for Weiss, setup_code = ' + str(setup_code) + ' and Task ID = ', str(TaskID_z))
            print(setup_code)
            
            csc_z = ReconDF['csc'].iloc[z]
            ReconPurpose_z = ReconDF['ReconPurpose'].iloc[z]
            collection_meo_z = ReconDF['collection_meo'].iloc[z]
            ProcessID_z = ReconDF['ProcessID'].iloc[z]
            Completed_Status_z = ReconDF['Completed_Status'].iloc[z]
            Setup_Code_z = ReconDF['Setup_Code'].iloc[z]
            MongoDB_TaskID_z = ReconDF['MongoDB_TaskID'].iloc[z]

            AckMessage_z = 'Prediction Message Received for' + str(TaskID_z)
            rb_mq_obj_new_for_acknowledgement.fun_publish_single_message(param_message_body = AckMessage_z)
            #filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
            #filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
            
            query_1_for_MEO_data = db_for_reading_MEO_data['RecData_' + setup_code].find({ 
                                                                                 "LastPerformedAction": 31,
                                                                                 "TaskInstanceID" : int(TaskID_z),
                                                                                 "MatchStatus": { "$nin" : [1,2,18,19,20,21]},
                                                                                 "ViewData" : { "$ne": None}
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
            
            date_to_analyze = '18012021'
            penultimate_date_to_analyze = '17012021'
            date_to_analyze_ymd_format = date_to_analyze[4:] + '-' + date_to_analyze[2:4] + '-' + date_to_analyze[:2]
            penultimate_date_to_analyze_ymd_format = penultimate_date_to_analyze[4:] + '-' + penultimate_date_to_analyze[2:4] + '-' + penultimate_date_to_analyze[:2]
            penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
            date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'
            
            #meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/meo_df_setup_' + setup_code + '_date_' + date_to_analyze_ymd_format + '.csv'
            #meo_df = pd.read_csv(meo_filename)
            #Comment out below three lines if running from db, in case they are commented
            if(len(list_of_dicts_query_result_1) != 0):
                meo_df = json_normalize(list_of_dicts_query_result_1)
                meo_df = meo_df.loc[:,meo_df.columns.str.startswith(('ViewData','_createdAt'))]
                meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
                meo_df.drop_duplicates(keep=False, inplace = True)
                meo_df = normalize_bp_acct_col_names(fun_df = meo_df)
                
                #Change added on 14-12-2020 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
                meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_df.apply(lambda row : contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row = row), axis = 1,result_type="expand")
                meo_df = meo_df[~(meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]
                meo = meo_df[new_cols]
            else:
                meo_df = pd.DataFrame()
                meo = pd.DataFrame()
            print('meo size')
            print(meo.shape[0])

            if(meo_df.shape[0] == 0):
                
                Message_z = str(TaskID_z) + '|' + csc_z + '|' + ReconPurpose_z + '|' + collection_meo_z + '|' + ProcessID_z + '|' + 'FAILURE' + '|' +  Setup_Code_z + '|' + MongoDB_TaskID_z
                rb_mq_obj_new_for_publish.fun_publish_single_message(param_message_body = Message_z)
                print(Message_z)
                print('meo has size = ' + str(meo_df.shape[0]) + ' therefore it failed')

#            meo_df_for_sending_message = meo_df[~meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
            elif(meo_df[~meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])].shape[0] == 0):
                
                Message_z = str(TaskID_z) + '|' + csc_z + '|' + ReconPurpose_z + '|' + collection_meo_z + '|' + ProcessID_z + '|' + 'FAILURE' + '|' +  Setup_Code_z + '|' + MongoDB_TaskID_z
                rb_mq_obj_new_for_publish.fun_publish_single_message(param_message_body = Message_z)
                print(Message_z)
                print('meo has size = ' + str(meo_df.shape[0]) + ' therefore it failed')
            else:
                ob_carry_forward_df = meo_df[meo_df['ViewData.Status'] == 'OB']                
                umb_carry_forward_df = meo_df[meo_df['ViewData.Status'] == 'UMB']
                meo_df['Date'] = pd.to_datetime(meo_df['ViewData.Task Business Date'])
                meo_df = meo_df.reset_index()
                meo_df = meo_df.drop('index',1)
                
                
                meo_df['Date'] = pd.to_datetime(meo_df['Date']).dt.date
                
                meo_df['Date'] = meo_df['Date'].astype(str)
                
                meo_df_taskids = list(meo_df['ViewData.Task ID'].unique())
                
                date_i = meo_df['Date'].mode()[0]

                    #UMB_carry_forward
                umb_carry_forward_columns_to_select_from_meo_df = ['ViewData.BreakID', \
                                                                   'ViewData.Task Business Date', \
                                                                   'ViewData.Side0_UniqueIds', \
                                                                   'ViewData.Side1_UniqueIds', \
                                                                   'ViewData.Source Combination Code', \
                                                                   'ViewData.Task ID']
                if(umb_carry_forward_df.shape[0] != 0): 
                    umb_carry_forward_df = umb_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df]
                    
                    umb_carry_forward_df['Predicted_Status'] = 'UMB'
                    umb_carry_forward_df['Predicted_action'] = 'UMB_Carry_Forward'
                    umb_carry_forward_df['ML_flag'] = 'ML'
                    umb_carry_forward_df['SetupID'] = setup_code 
                    umb_carry_forward_df['Final_predicted_break'] = ''
                    umb_carry_forward_df['PredictedComment'] = ''
                    umb_carry_forward_df['PredictedCategory'] = ''
                    umb_carry_forward_df['probability_UMB'] = ''
                    umb_carry_forward_df['probability_No_pair'] = ''
                    umb_carry_forward_df['probability_UMR'] = ''
                    umb_carry_forward_df['probability_UMT'] = ''
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
                    umb_carry_forward_df['probability_UMT'] = umb_carry_forward_df['probability_UMT'].astype(str)
                    umb_carry_forward_df['SourceCombinationCode'] = umb_carry_forward_df['SourceCombinationCode'].astype(str)
                    umb_carry_forward_df['Predicted_Status'] = umb_carry_forward_df['Predicted_Status'].astype(str)
                    umb_carry_forward_df['ML_flag'] = umb_carry_forward_df['ML_flag'].astype(str)
                    
                    
                    #umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(float)
                    #umb_carry_forward_df[['BreakID', 'TaskID']] = umb_carry_forward_df[['BreakID', 'TaskID']].astype(np.int64)
                    
                    umb_carry_forward_df[['SetupID']] = umb_carry_forward_df[['SetupID']].astype(int)
                    filepaths_umb_carry_forward_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_carry_forward_df_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                    umb_carry_forward_df.to_csv(filepaths_umb_carry_forward_df)
                else:
                    umb_carry_forward_df = pd.DataFrame()

                umb_carry_forward_columns_to_select_from_meo_df = ['ViewData.BreakID', \
                                                                   'ViewData.Task Business Date', \
                                                                   'ViewData.Side0_UniqueIds', \
                                                                   'ViewData.Side1_UniqueIds', \
                                                                   'ViewData.Source Combination Code', \
                                                                   'ViewData.Task ID']

                if(ob_carry_forward_df.shape[0] != 0): 
                    ob_carry_forward_df = ob_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df]
                    
                    ob_carry_forward_df['Predicted_Status'] = 'OB'
                    ob_carry_forward_df['Predicted_action'] = 'No-Pair'
                    ob_carry_forward_df['ML_flag'] = 'ML'
                    ob_carry_forward_df['SetupID'] = setup_code 
                    ob_carry_forward_df['Final_predicted_break'] = ''
                    ob_carry_forward_df['PredictedComment'] = ''
                    ob_carry_forward_df['PredictedCategory'] = ''
                    ob_carry_forward_df['probability_UMB'] = ''
                    ob_carry_forward_df['probability_No_pair'] = ''
                    ob_carry_forward_df['probability_UMR'] = ''
                    ob_carry_forward_df['probability_UMT'] = ''
                    
                    ob_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df] = ob_carry_forward_df[umb_carry_forward_columns_to_select_from_meo_df].astype(str)
                    change_names_of_ob_carry_forward_df_mapping_dict = {
                                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                                'ViewData.BreakID' : 'BreakID',
                                                                'ViewData.Task ID' : 'TaskID',
                                                                'ViewData.Task Business Date' : 'BusinessDate',
                                                                'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                                            }
                    
                    ob_carry_forward_df.rename(columns = change_names_of_ob_carry_forward_df_mapping_dict, inplace = True)
                    
                    ob_carry_forward_df['BusinessDate'] = pd.to_datetime(ob_carry_forward_df['BusinessDate'])
                    ob_carry_forward_df['BusinessDate'] = ob_carry_forward_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
                    ob_carry_forward_df['BusinessDate'] = pd.to_datetime(ob_carry_forward_df['BusinessDate'])
                    
                    ob_carry_forward_df['Side0_UniqueIds'] = ob_carry_forward_df['Side0_UniqueIds'].astype(str)
                    ob_carry_forward_df['Side1_UniqueIds'] = ob_carry_forward_df['Side1_UniqueIds'].astype(str)
                    ob_carry_forward_df['Final_predicted_break'] = ob_carry_forward_df['Final_predicted_break'].astype(str)
                    ob_carry_forward_df['Predicted_action'] = ob_carry_forward_df['Predicted_action'].astype(str)
                    ob_carry_forward_df['probability_No_pair'] = ob_carry_forward_df['probability_No_pair'].astype(str)
                    ob_carry_forward_df['probability_UMB'] = ob_carry_forward_df['probability_UMB'].astype(str)
                    ob_carry_forward_df['probability_UMR'] = ob_carry_forward_df['probability_UMR'].astype(str)
                    ob_carry_forward_df['probability_UMT'] = ob_carry_forward_df['probability_UMT'].astype(str)
                    ob_carry_forward_df['SourceCombinationCode'] = ob_carry_forward_df['SourceCombinationCode'].astype(str)
                    ob_carry_forward_df['Predicted_Status'] = ob_carry_forward_df['Predicted_Status'].astype(str)
                    ob_carry_forward_df['ML_flag'] = ob_carry_forward_df['ML_flag'].astype(str)
                    
                    
                    #ob_carry_forward_df[['BreakID', 'TaskID']] = ob_carry_forward_df[['BreakID', 'TaskID']].astype(float)
                    #ob_carry_forward_df[['BreakID', 'TaskID']] = ob_carry_forward_df[['BreakID', 'TaskID']].astype(np.int64)
                    
                    ob_carry_forward_df[['SetupID']] = ob_carry_forward_df[['SetupID']].astype(int)
                    filepaths_ob_carry_forward_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\ob_carry_forward_df_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                    ob_carry_forward_df.to_csv(filepaths_ob_carry_forward_df)
                else:
                    ob_carry_forward_df = pd.DataFrame()
                
                
                #Prepare RabbitMQ simulation message to be sent to DB for data extraction    
                meo_df_taskids = list(meo_df['ViewData.Task ID'].unique())
                m_after_reading_meo = memory_profiler.memory_usage()
    #            mem_diff_m_after_reading_meo = m_after_reading_meo[0] - m_after_function_definitions[0]
    #            print(f"It took {mem_diff_m_after_reading_meo} Mb to execute this method")
                #It took 626.6796875 Mb to execute this method
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
                #Change added on 17-12-2020 by Rohit to include filter on ob and smb. Below if statement is commented out and new if statement is included
                #        if ob.shape[0]==1 and smb.shape[0]==1:
                #            ob_breakid.append(ob['ViewData.BreakID'].values)
                #            smb_breakid.append(smb['ViewData.BreakID'].values)
                        if ob.shape[0]==1 and smb.shape[0]==1 :
                            if((ob['ViewData.Mapped Custodian Account'].iloc[0] == smb['ViewData.Mapped Custodian Account'].iloc[0]) and (ob['ViewData.Currency'].iloc[0] == smb['ViewData.Currency'].iloc[0]) and (ob['ViewData.Source Combination'].iloc[0] == smb['ViewData.Source Combination'].iloc[0])):
                
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
                    umb_amount = np.array([])
                
                #Remove BreakIDs caught in final_smb_ob_table if final_smb_ob_table is not null
                if(final_smb_ob_table.shape[0] != 0):
                    final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].astype(np.int64)
                    final_smb_ob_table['BreakID_OB'] = final_smb_ob_table['BreakID_OB'].astype(np.int64)
                    
                    final_smb_ob_table_BreakID_list =  list(final_smb_ob_table['BreakID_OB']) + list(final_smb_ob_table['BreakID_SMB'])
                    meo = meo[~meo['ViewData.BreakID'].isin(final_smb_ob_table_BreakID_list)]
                else:
                    final_smb_ob_table_BreakID_list = []
                #End change code made on 12-12-2020
                
                m_after_smb_ob_table = memory_profiler.memory_usage()
                mem_diff_m_after_smb_ob_table = m_after_smb_ob_table[0] - m_after_reading_meo[0]
                print(f"It took {mem_diff_m_after_smb_ob_table} Mb to execute this method")
                #It took 7.44140625 Mb to execute this method
                #Change made on 11-01-2021 as per Rohit to include umb_ob table, just as smb_ob table.
                ob_breakid = []
                umb_breakid = []
                for amount in umb_amount:
                    ob = umb_ob_table[(umb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (umb_ob_table['ViewData.Status']=='OB')]
                    umb = umb_ob_table[(umb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (umb_ob_table['ViewData.Status']=='UMB')]
                #         if((ob.shape[0]==1) and (smb.shape[0]==1) and (ob['ViewData.Mapped Custodian Account'] == smb['ViewData.Mapped Custodian Account']) and (ob['ViewData.Currency'] == smb['ViewData.Currency']) and (ob['ViewData.Source Combination Code'] == smb['ViewData.Source Combination Code'])):
                
                    if ob.shape[0]==1 and umb.shape[0]==1 :
                #Change added on 17-12-2020 by Rohit to include filter on ob and smb. Below if statement is commented out and new if statement is included
                        if(('ViewData.Source Combination Code' in list(ob.columns)) & ('ViewData.Source Combination Code' in list(umb.columns))):
                            if((ob['ViewData.Mapped Custodian Account'].iloc[0] == umb['ViewData.Mapped Custodian Account'].iloc[0]) and (ob['ViewData.Currency'].iloc[0] == umb['ViewData.Currency'].iloc[0]) and (ob['ViewData.Source Combination Code'].iloc[0] == umb['ViewData.Source Combination Code'].iloc[0])):
                    
                                ob_breakid.append(ob['ViewData.BreakID'].values)
                                umb_breakid.append(umb['ViewData.BreakID'].values)
                        else:
                            if((ob['ViewData.Mapped Custodian Account'].iloc[0] == umb['ViewData.Mapped Custodian Account'].iloc[0]) and (ob['ViewData.Currency'].iloc[0] == umb['ViewData.Currency'].iloc[0])):
                    
                                ob_breakid.append(ob['ViewData.BreakID'].values)
                                umb_breakid.append(umb['ViewData.BreakID'].values)
                            
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
                    final_umb_ob_table_copy = make_Side0_Side1_columns_for_final_smb_or_umb_ob_table(final_umb_ob_table_copy,meo_df,'UMB')
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
                    filepaths_final_umb_ob_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umb_ob_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                
                    final_umb_ob_table_copy.to_csv(filepaths_final_umb_ob_table_copy)
                
                
                else:
                    final_umb_ob_table_copy = pd.DataFrame()
                #End code change made on 12-12-2020 to incorporate final_smb_ob_table
                m_after_umb_ob_table = memory_profiler.memory_usage()
                mem_diff_m_after_umb_ob_table = m_after_umb_ob_table[0] - m_after_smb_ob_table[0]
                print(f"It took {mem_diff_m_after_umb_ob_table} Mb to execute this method")
                #It took 0.484375 Mb to execute this method
                
                start_closed = timeit.default_timer()
                
                normalized_meo_df = normalize_bp_acct_col_names(meo_df)
                meo_for_closed = cleaned_meo(normalized_meo_df)
                
                
                closed_df_list = []
                for transaction_type_for_closing_value in mapping_dict_trans_type:
                    if(meo_for_closed[meo_for_closed['Transaction_Type_for_closing'] == transaction_type_for_closing_value].shape[0] != 0):
                        meo_for_transaction_type_for_closing_value_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing'] == transaction_type_for_closing_value]
                        meo_for_transaction_type_for_closing_value = interacting_closing_125(meo_for_transaction_type_for_closing_value_input)
                        All_combination_df = All_combination_file(fun_df = meo_for_transaction_type_for_closing_value)
                        if(All_combination_df.shape[0] != 0):
                            closed_df_for_transaction_type_for_closing_value = identifying_closed_breaks(fun_all_meo_combination_df = All_combination_df, \
                                                                                     fun_setup_code_crucial = setup_code, \
                                                                                     fun_trans_type_1 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[0], \
                                                                                     fun_trans_type_2 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[1])
                            closed_df_list.append(closed_df_for_transaction_type_for_closing_value)
                        else:
                            closed_df_list.append(pd.DataFrame())
                        del(meo_for_transaction_type_for_closing_value_input)
                        del(meo_for_transaction_type_for_closing_value)
                        del(All_combination_df)
                
                if(meo_for_closed[meo_for_closed['Transaction_Type_for_closing_2'] == 'SPEC_Stk_Loan_Jrl_WTH'].shape[0] != 0):
                    meo_for_SPEC_Stk_Loan_Jrl_WTH_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing_2'] == 'SPEC_Stk_Loan_Jrl_WTH'] 
                    meo_for_SPEC_Stk_Loan_Jrl_WTH = interacting_closing_125(meo_for_SPEC_Stk_Loan_Jrl_WTH_input)
                    All_combination_df_SPEC_Stk_Loan_Jrl_WTH = All_combination_file(fun_df = meo_for_SPEC_Stk_Loan_Jrl_WTH)
                    closed_df_SPEC_Stk_Loan_Jrl_WTH = identifying_closed_breaks(fun_all_meo_combination_df = All_combination_df_SPEC_Stk_Loan_Jrl_WTH, \
                                                                             fun_setup_code_crucial = '125', \
                                                                             fun_trans_type_1 = 'SPEC Stk Loan Jrl', \
                                                                             fun_trans_type_2 = 'WTH')
                else:
                    closed_df_SPEC_Stk_Loan_Jrl_WTH = pd.DataFrame()
                    
                closed_df_list.append(closed_df_SPEC_Stk_Loan_Jrl_WTH)
                
                closed_df_interacting = pd.concat(closed_df_list)
                
                if(closed_df_interacting.shape[0]):
                    breakId_x = set(list(closed_df_interacting['ViewData.BreakID_x']))
                    breakId_y = set(list(closed_df_interacting['ViewData.BreakID_y']))
                else:
                    breakId_x = set()
                    breakId_y = set()
                
                all_interacting_closed_breakIds = list(breakId_x.union(breakId_y))
                
                all_breakids_fees_and_comm = list(meo_for_closed[meo_for_closed['ViewData.Transaction Type'] == 'Fees & Comm']['ViewData.BreakID'])
                
                all_breakids_beginning_with_X = list(meo_for_closed[meo_for_closed['ViewData.Transaction Type'].isin(['XBCOVER','XBUY','XSELL','XSSHORT'])]['ViewData.BreakID'])
                
                all_breakids_transfer_UBFX = list(meo_for_closed[((meo_for_closed['ViewData.Transaction Type'].isin(['Transfer'])) & (meo_for_closed['ViewData.Prime Broker'] == 'UBFX'))]['ViewData.BreakID'])
                
                all_predicted_close_breakids = all_interacting_closed_breakIds + all_breakids_fees_and_comm
                
                all_predicted_close_breakids = all_predicted_close_breakids + all_breakids_beginning_with_X + all_breakids_transfer_UBFX
                
                all_predicted_closed_nan = list(meo_for_closed[((meo_for_closed['ViewData.Transaction Type'].isin(['nan','None',''])) & (meo_for_closed['ViewData.Mapped Custodian Account'].isin(['UBS_UBFX_ON','UBS_UBFX_OP'])))]['ViewData.BreakID'])
                
                all_predicted_close_breakids = all_predicted_close_breakids + all_predicted_closed_nan
                
                int_all_predicted_close_breakids = [int(x) for x in all_predicted_close_breakids]
                
                
                #Side_0_1_UniqueIds_closed_all_dates_list.append(
                #        closed_daily_run(fun_setup_code=setup_code,\
                #                         fun_date = i,\
                #                         fun_meo_df_daily_run= meo)
                #        )
                
                def make_Side01_UniqueIds_apply_row(fun_row):
                    side0id = str(fun_row['ViewData.Side0_UniqueIds'])
                    side1id = str(fun_row['ViewData.Side1_UniqueIds'])
                    return(side0id + side1id)
                
                if(len(int_all_predicted_close_breakids) != 0):   
                    closed_new_breakid_side01_ids_df2 = meo_df[meo_df['ViewData.BreakID'].isin(int_all_predicted_close_breakids)][['ViewData.BreakID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]
                    closed_new_breakid_side01_ids_df2['Side0_1_UniqueIds'] = closed_new_breakid_side01_ids_df2.apply(lambda row : make_Side01_UniqueIds_apply_row(fun_row = row), axis = 1, result_type="expand")
                    Side_0_1_UniqueIds_closed_all_dates_list = closed_new_breakid_side01_ids_df2['Side0_1_UniqueIds'].tolist()
                else:
                    Side_0_1_UniqueIds_closed_all_dates_list = []
                
                new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list]
                new_closed_keys = [i.replace('None','') for i in new_closed_keys]
                
                stop_closed = timeit.default_timer()
                
                print('Time for closed : ', stop_closed - start_closed)
                # ## Read testing data 
                m_after_closed = memory_profiler.memory_usage()
                mem_diff_m_after_closed = m_after_closed[0] - m_after_umb_ob_table[0]
                print(f"It took {mem_diff_m_after_closed} Mb to execute this method")
                #It took 21.921875 Mb to execute this method
                
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
                
                #Start from here
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
                
                m_after_aa_bb_definitions = memory_profiler.memory_usage()
                mem_diff_m_after_aa_bb_definitions = m_after_aa_bb_definitions[0] - m_after_closed[0]
                print(f"It took {mem_diff_m_after_aa_bb_definitions} Mb to execute this method")
                #It took 22.9765625 Mb to execute this method
                
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
                
                #Change made on 22-12-2020 as per Pratik. This change is for catching mtm for revenue,expense and stock loan.
                #Begin changes made on 22-12-2020
                def revenue_stock(tt):
                    tt = tt.lower()
                    if tt == 'revenue' or tt =='expenses' or any(key in tt for key in ['stock loan fee','interest on short credit','interest charged on debit ','interest earned on credit']):
                        
                        revenue_flag = 1
                    else:
                         revenue_flag = 0    
                
                    return revenue_flag
                
                cc['Revenue_flag'] = cc.apply(lambda x: revenue_stock(x['ViewData.Transaction Type']), axis=1)
                
                ########################################################################################################
                if cc[(cc['Revenue_flag']==1)].shape[0]>0:
                    fff2 = cc[cc['Revenue_flag']==1]
                    fff2 = fff2.reset_index().drop('index',1)
                    fff2['ViewData.Settle Date'] = pd.to_datetime(fff2['ViewData.Settle Date'])
                    #fff2['filter_key_with_sd'] = fff2['filter_key'].astype(str) + fff2['ViewData.Settle Date'].astype(str)
                else:
                    fff2 = pd.DataFrame()
                
                ########################################################################################################
                
                filter_key_umt_umb_revenue = []
                
                if fff2.empty == False:
                    for key in fff2['filter_key'].unique():        
                        fff2_dummy = fff2[fff2['filter_key']==key]
                        #print(key)
                        if (-0.1 <= fff2_dummy['ViewData.Net Amount Difference'].sum() <0.1) & (fff2_dummy.shape[0]>2) & (fff2_dummy['Trans_side'].nunique()>1):
                            #print(cc2_dummy.shape[0])
                            #print(key)
                            filter_key_umt_umb_revenue.append(key)
                            
                            
                            
                ############################### Revenue Many to many ############################
                
                if filter_key_umt_umb_revenue ==[]:
                    filter_key_umt_umb_revenue = ['None']
                
                rv_mtm_1_ids = []
                rv_mtm_0_ids = []
                
                if fff2.empty == False:
                    for key in filter_key_umt_umb_revenue:
                        one_side_revenue = fff2[fff2['filter_key']== key]['ViewData.Side1_UniqueIds'].unique()
                        zero_side_revenue = fff2[fff2['filter_key']== key]['ViewData.Side0_UniqueIds'].unique()
                        one_side_revenue = [i for i in one_side_revenue if i not in ['nan','None','']]
                        zero_side_revenue = [i for i in zero_side_revenue if i not in ['nan','None','']]
                        rv_mtm_1_ids.append(one_side_revenue)
                        rv_mtm_0_ids.append(zero_side_revenue)
                
                if rv_mtm_1_ids !=[]:
                    mtm_list_1_rv = list(np.concatenate(rv_mtm_1_ids))
                else:
                    mtm_list_1_rv = []
                
                if rv_mtm_0_ids !=[]:
                    mtm_list_0_rv = list(np.concatenate(rv_mtm_0_ids))
                else:
                    mtm_list_0_rv = []
                    
                ##########################################################################################    
                ## Data Frame for MTM from Revenue
                
                if(len(rv_mtm_0_ids) != 0):
                    mtm_df_rv = pd.DataFrame(np.arange(len(rv_mtm_0_ids)))
                    mtm_df_rv.columns = ['index']
                    
                    mtm_df_rv['ViewData.Side0_UniqueIds'] = rv_mtm_0_ids
                    mtm_df_rv['ViewData.Side1_UniqueIds'] = rv_mtm_1_ids
                    #TODO : Make table for mtm_df_rv for this
                    mtm_df_rv = mtm_df_rv.drop('index',1)
                
                    mtm_df_rv['len0'] = mtm_df_rv['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
                    mtm_df_rv['len1'] = mtm_df_rv['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
                    
                    mtm_df_rv = mtm_df_rv[(mtm_df_rv['len0']!=0) | (mtm_df_rv['len1']!=0)]     
                    mtm_df_rv = mtm_df_rv.drop(['len0','len1'],1)
                else:
                    mtm_df_rv = pd.DataFrame()
                ##########################################################################################  
                
                cc = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0_rv)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1_rv)))]
                cc = cc.reset_index().drop('index',1)
                
                #End changes made on 22-12-2020
                m_after_mtm_rv = memory_profiler.memory_usage()
                mem_diff_m_after_mtm_rv = m_after_mtm_rv[0] - m_after_aa_bb_definitions[0]
                print(f"It took {mem_diff_m_after_mtm_rv} Mb to execute this method")
                #It took 9.5859375 Mb to execute this method
                
                #Change made on 11-12-2020 as per Pratik. This change is for catching otm-mto-mtm in forward-fx transaction type
                #Begin change made on 11-12-2020
                
                def forwardfx_tt_flag(tt):
                    tt = tt.lower()
                    if any(key in tt for key in ['forwardfx',' fx']):
                        tt_flag = 1
                    else:
                        tt_flag = 0
                    return tt_flag
                cc['Forward_fx_flag'] = cc.apply(lambda x: forwardfx_tt_flag(x['ViewData.Transaction Type']), axis=1)
                
                
                
                if cc[(cc['Forward_fx_flag']==1)].shape[0]>0:
                    dd2 = cc[cc['Forward_fx_flag']==1]
                    dd2 = dd2.reset_index().drop('index',1)
                    dd2['ViewData.Settle Date'] = pd.to_datetime(dd2['ViewData.Settle Date'])
                    dd2['filter_key_with_sd'] = dd2['filter_key'].astype(str) + dd2['ViewData.Settle Date'].astype(str)
                else:
                    dd2 = pd.DataFrame()
                
                filter_key_umt_umb_forward = []
                
                if dd2.empty == False:
                    for key in dd2['filter_key_with_sd'].unique():        
                        dd2_dummy = dd2[dd2['filter_key_with_sd']==key]
                        #print(key)
                        if (-0.2<= dd2_dummy['ViewData.Net Amount Difference'].sum() <0.2) & (dd2_dummy.shape[0]>2) & (dd2_dummy['Trans_side'].nunique()>1):
                            #print(cc2_dummy.shape[0])
                            #print(key)
                            filter_key_umt_umb_forward.append(key)
                
                
                
                ############################### ForwardFX Many to many ############################
                
                if filter_key_umt_umb_forward ==[]:
                    filter_key_umt_umb_forward = ['None']
                
                fx_mtm_1_ids = []
                fx_mtm_0_ids = []
                
                if dd2.empty == False:
                    for key in filter_key_umt_umb_forward:
                        one_side_forward = dd2[dd2['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
                        zero_side_forward = dd2[dd2['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
                        one_side_forward = [i for i in one_side_forward if i not in ['nan','None','']]
                        zero_side_forward = [i for i in zero_side_forward if i not in ['nan','None','']]
                        fx_mtm_1_ids.append(one_side_forward)
                        fx_mtm_0_ids.append(zero_side_forward)
                
                if fx_mtm_1_ids !=[]:
                    mtm_list_1_fx = list(np.concatenate(fx_mtm_1_ids))
                else:
                    mtm_list_1_fx = []
                
                if fx_mtm_0_ids !=[]:
                    mtm_list_0_fx = list(np.concatenate(fx_mtm_0_ids))
                else:
                    mtm_list_0_fx = []
                    
                    
                ##########################################################################################    
                ## Data Frame for MTM from ForwardFX
                if(len(fx_mtm_0_ids) != 0):
                    mtm_df_fx = pd.DataFrame(np.arange(len(fx_mtm_0_ids)))
                    mtm_df_fx.columns = ['index']
                    
                    mtm_df_fx['ViewData.Side0_UniqueIds'] = fx_mtm_0_ids
                    mtm_df_fx['ViewData.Side1_UniqueIds'] = fx_mtm_1_ids
                    
                    #TODO : Insert this table : mtm_df_fx into the results table for final_df_2
                    mtm_df_fx = mtm_df_fx.drop('index',1)
                
                    mtm_df_fx['len0'] = mtm_df_fx['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
                    mtm_df_fx['len1'] = mtm_df_fx['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
                    
                    mtm_df_fx = mtm_df_fx[(mtm_df_fx['len0']!=0) | (mtm_df_fx['len1']!=0)]     
                    mtm_df_fx = mtm_df_fx.drop(['len0','len1'],1)
                
                else:
                    mtm_df_fx = pd.DataFrame()
                
                
                cc = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0_fx)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1_fx)))]
                cc = cc.reset_index().drop('index',1)
                
                m_after_mtm_fx = memory_profiler.memory_usage()
                mem_diff_m_after_mtm_fx = m_after_mtm_fx[0] - m_after_mtm_rv[0]
                print(f"It took {mem_diff_m_after_mtm_fx} Mb to execute this method")
                #It took 0.4765625 Mb to execute this method
                
                # ## Expense
                
                
                def expense_tt_flag(tt):
                    tt = tt.lower()
                    if any(key in tt for key in ['expense','withdraw','deposit']) and tt!='cash deposit':
                        tt_flag = 1
                    else:
                        tt_flag = 0
                    return tt_flag
                cc['Expense_flag'] = cc.apply(lambda x: expense_tt_flag(x['ViewData.Transaction Type']), axis=1)
                
                
                
                if cc[(cc['Expense_flag']==1)].shape[0]>0:
                    ee2 = cc[cc['Expense_flag']==1]
                    ee2 = ee2.reset_index().drop('index',1)
                    ee2['ViewData.Settle Date'] = pd.to_datetime(ee2['ViewData.Settle Date'])
                    ee2['filter_key_with_sd'] = ee2['filter_key'].astype(str) + ee2['ViewData.Settle Date'].astype(str)
                else:
                    ee2 = pd.DataFrame()
                
                filter_key_umt_umb_expense = []
                
                if ee2.empty == False:
                    for key in ee2['filter_key_with_sd'].unique():        
                        ee2_dummy = ee2[ee2['filter_key_with_sd']==key]
                        #print(key)
                        if (-0.2<= ee2_dummy['ViewData.Net Amount Difference'].sum() <0.2) & (ee2_dummy.shape[0]>2) & (ee2_dummy['Trans_side'].nunique()>1):
                            #print(cc2_dummy.shape[0])
                            #print(key)
                            filter_key_umt_umb_expense.append(key)
                
                
                
                ############################### Expense Many to many ############################
                
                if filter_key_umt_umb_expense ==[]:
                    filter_key_umt_umb_expense = ['None']
                
                ex_mtm_1_ids = []
                ex_mtm_0_ids = []
                
                if ee2.empty == False:
                    for key in filter_key_umt_umb_expense:
                        one_side_expense = ee2[ee2['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
                        zero_side_expense = ee2[ee2['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
                        one_side_expense = [i for i in one_side_expense if i not in ['nan','None','']]
                        zero_side_expense = [i for i in zero_side_expense if i not in ['nan','None','']]
                        ex_mtm_1_ids.append(one_side_expense)
                        ex_mtm_0_ids.append(zero_side_expense)
                
                if ex_mtm_1_ids !=[]:
                    mtm_list_1_ex = list(np.concatenate(ex_mtm_1_ids))
                else:
                    mtm_list_1_ex = []
                
                if ex_mtm_0_ids !=[]:
                    mtm_list_0_ex = list(np.concatenate(ex_mtm_0_ids))
                else:
                    mtm_list_0_ex = []
                    
                
                ##########################################################################################    
                ## Data Frame for MTM from ForwardFX
                if(len(ex_mtm_0_ids) != 0):
                    mtm_df_ex = pd.DataFrame(np.arange(len(ex_mtm_0_ids)))
                    mtm_df_ex.columns = ['index']
                    
                    mtm_df_ex['ViewData.Side0_UniqueIds'] = ex_mtm_0_ids
                    mtm_df_ex['ViewData.Side1_UniqueIds'] = ex_mtm_1_ids
                    
                    #TODO : Insert this table : mtm_df_ex into the results table for final_df_2
                    mtm_df_ex = mtm_df_ex.drop('index',1)
                
                    mtm_df_ex['len0'] = mtm_df_ex['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
                    mtm_df_ex['len1'] = mtm_df_ex['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
                    
                    mtm_df_ex = mtm_df_ex[(mtm_df_ex['len0']!=0) | (mtm_df_ex['len1']!=0)]     
                    mtm_df_ex = mtm_df_ex.drop(['len0','len1'],1)
                
                else:
                    mtm_df_ex = pd.DataFrame()
                
                
                m_after_mtm_ex = memory_profiler.memory_usage()
                mem_diff_m_after_mtm_ex = m_after_mtm_ex[0] - m_after_mtm_fx[0]
                print(f"It took {mem_diff_m_after_mtm_ex} Mb to execute this method")
                #It took 0.01171875 Mb to execute this method
                    
                
                
                
                cc = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0_ex)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1_ex)))]
                cc = cc.reset_index().drop('index',1)
                
                
                
                
                #End change made on 11-12-2020
                
                
                def transfer_desc(tt, desc):
                    if tt.lower() =='transfer':
                        
                        desc = desc.lower()
                #Change made on 12-12-2020. Below line was commented and new line below that was added
                #        if any(key in desc for key in ['equity','swap unwind','eq swap']):
                        if any(key in desc for key in ['equity','swap unwind','eq swap','eqswap']):
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
                #Change made on 12-12-2020. Below line was commented and new line below that was added
                #    if any(key in tt for key in ['equity swap','swap unwind','eq swap','transfer']):
                    if any(key in tt for key in ['equity swap','swap unwind','eq swap','eqswap']):
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
                
                ##########################################################################################    
                ## Data Frame for MTM from ForwardFX
                if(len(eq_mtm_0_ids) != 0):
                    mtm_df_eqs = pd.DataFrame(np.arange(len(eq_mtm_0_ids)))
                    mtm_df_eqs.columns = ['index']
                    
                    mtm_df_eqs['ViewData.Side0_UniqueIds'] = eq_mtm_0_ids
                    mtm_df_eqs['ViewData.Side1_UniqueIds'] = eq_mtm_1_ids
                    
                    #TODO : Insert this table : mtm_df_eqs into the results table for final_df_2
                    mtm_df_eqs = mtm_df_eqs.drop('index',1)
                
                    mtm_df_eqs['len0'] = mtm_df_eqs['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
                    mtm_df_eqs['len1'] = mtm_df_eqs['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
                    
                    mtm_df_eqs = mtm_df_eqs[(mtm_df_eqs['len0']!=0) | (mtm_df_eqs['len1']!=0)]     
                    mtm_df_eqs = mtm_df_eqs.drop(['len0','len1'],1)
                
                else:
                    mtm_df_eqs = pd.DataFrame()
                
                
                
                
                
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
                
                if(len(new_close) != 0):
                    new_close_flat = np.concatenate(new_close)
                else:
                    new_close_flat = []
                
                if(len(new_close_flat) != 0):
                    closed_final_df = cc6[cc6['ViewData.Side0_UniqueIds'].isin(new_close_flat) | cc6['ViewData.Side1_UniqueIds'].isin(new_close_flat)]
                    closed_final_df = closed_final_df[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]
                else:
                    closed_final_df = pd.DataFrame()
                
                
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
                
                m_before_updown = memory_profiler.memory_usage()
                mem_diff_m_before_updown = m_before_updown[0] - m_after_mtm_ex[0]
                print(f"It took {mem_diff_m_before_updown} Mb to execute this method")
                #It took 26.328125 Mb to execute this method
                
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
                
                if(updlist.shape[0] != 0):
                    updlist['upd_amt'] = updlist.apply(lambda x : [value for value in x['ViewData.Cust Net Amount'] if value in x['ViewData.Accounting Net Amount']], axis = 1)
                    updlist = updlist[['ViewData.Task Business Date1','upd_amt']]
                    dffpb = pd.merge(dffpb, updlist, on = 'ViewData.Task Business Date1', how = 'left')
                    dffacc = pd.merge(dffacc, updlist, on = 'ViewData.Task Business Date1', how = 'left')

                elif((dffpb.shape[0] == 0) & (dffacc.shape[0] != 0)):
                    dffacc['upd_amt'] = None
                    updlist = pd.DataFrame()
                elif((dffpb.shape[0] != 0) & (dffacc.shape[0] == 0)):
                    dffpb['upd_amt'] = None
                    updlist = pd.DataFrame()
                else:
                    updlist = pd.DataFrame()
                    
                def updmark(y,x):
                    if x =='MMM':
                        return 0
                    else:
                        if y in x:
                            return 1
                        else:
                            return 0

                if(dffpb.shape[0] != 0):                
                    dffpb['upd_amt']= dffpb['upd_amt'].fillna('MMM')
                    dffpb['upd_mark'] = dffpb.apply(lambda x :  updmark(x['ViewData.Cust Net Amount'], x['upd_amt']) , axis= 1)
                    dff4 = dffpb[dffpb['upd_mark']==1]
                else:
                    dff4 = pd.DataFrame()
                if(dffacc.shape[0] != 0):
                    dffacc['upd_amt']= dffacc['upd_amt'].fillna('MMM')
                    dffacc['upd_mark'] = dffacc.apply(lambda x : updmark(x['ViewData.Accounting Net Amount'], x['upd_amt']) , axis= 1)
                    dff5 = dffacc[dffacc['upd_mark']==1]
                else:
                    dff5 = pd.DataFrame()
                
                
                
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
                
                    SideAMappedCustodianAccount_values = df_213_1['SideA.ViewData.Mapped Custodian Account'].values
                    SideBMappedCustodianAccount_values = df_213_1['SideB.ViewData.Mapped Custodian Account'].values
                    df_213_1['map_match'] = vec_equals_fun(SideAMappedCustodianAccount_values, SideBMappedCustodianAccount_values)
                    
                    SideAAccountingNetAmount_values = df_213_1['SideA.ViewData.Accounting Net Amount'].values
                    SideBCustNetAmount_values = df_213_1['SideB.ViewData.Cust Net Amount'].values
                    df_213_1['amt_match'] = vec_equals_fun(SideAAccountingNetAmount_values, SideBCustNetAmount_values)
                
                    SideAFund_values = df_213_1['SideA.ViewData.Fund'].values
                    SideBFund_values = df_213_1['SideB.ViewData.Fund'].values
                    df_213_1['fund_match'] = vec_equals_fun(SideAFund_values, SideBFund_values)
                    
                    SideACurrency_values = df_213_1['SideA.ViewData.Currency'].values
                    SideBCurrency_values = df_213_1['SideB.ViewData.Currency'].values
                    df_213_1['curr_match'] = vec_equals_fun(SideACurrency_values, SideBCurrency_values)
                    
                    SideASettleDate1_values = df_213_1['SideA.ViewData.Settle Date1'].values
                    SideBSettleDate1_values = df_213_1['SideB.ViewData.Settle Date1'].values
                    df_213_1['sd_match'] = vec_equals_fun(SideASettleDate1_values, SideBSettleDate1_values)
                
                    df_213_1[['map_match','amt_match','fund_match','curr_match','sd_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
                #    df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['fund_match'] + df_213_1['curr_match']
                    df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['curr_match']
                #    elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]
                    elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=1)]
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
                            list_of_side1_ids_to_remove_from_aa_new_without_duplicates = []
                        
                        if(len(list_of_side0_ids_to_remove_from_bb_new) != 0):
                            list_of_side0_ids_to_remove_from_bb_new_without_duplicates = list(set(list_of_side0_ids_to_remove_from_bb_new))
                            flag_side0_ids_to_remove_from_bb_new_exist = 1
                        else:
                            flag_side0_ids_to_remove_from_bb_new_exist = 0
                            list_of_side0_ids_to_remove_from_bb_new_without_duplicates = []
                        
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
                
                m_after_updown = memory_profiler.memory_usage()
                mem_diff_m_after_updown = m_after_updown[0] - m_before_updown[0]
                print(f"It took {mem_diff_m_after_updown} Mb to execute this method")
                #It took 530.53125 Mb to execute this method
                
                
                ###################### loop m*n ###############################
                filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_i) + '.csv'
                filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_i) + '.csv'
                
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
                    filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_i) + '.csv'
                    filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_i) + '.csv'

                    no_pair_ids_df.to_csv(filepaths_no_pair_id_data)
                else:
                    no_pair_ids_df = pd.DataFrame()
                    no_pair_ids = []
                    with open(filepaths_no_pair_id_no_data_warning, 'w') as f:
                            f.write('No no pair ids found for this setup and date combination')
                
                #no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])
                
                #pd.DataFrame(no_pair_ids).rename
                
                #test_file['SideA.ViewData.Status'].value_counts()
                
                m_after_m_cross_n = memory_profiler.memory_usage()
                mem_diff_m_after_m_cross_n = m_after_m_cross_n[0] - m_after_updown[0]
                print(f"It took {mem_diff_m_after_m_cross_n} Mb to execute this method")
                #It took 846.3828125 Mb to execute this method
                
                if(len(training_df) != 0):
                    test_file = pd.concat(training_df)
                
                
                    m_after_test_file_made_for_first_time = memory_profiler.memory_usage()
                    mem_diff_m_after_test_file_made_for_first_time = m_after_test_file_made_for_first_time[0] - m_after_m_cross_n[0]
                    print(f"It took {mem_diff_m_after_test_file_made_for_first_time} Mb to execute this method")
                    #It took 884.25390625 Mb to execute this method
                    
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
                    
                    #Change made on 14-12-2020 for Pratik to remove unnecessary filtering out of data in test_file in trade and settle date
                    test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].fillna(0)
                    test_file['SideB.ViewData.Trade Date'] = test_file['SideB.ViewData.Trade Date'].fillna(0)
                    test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].fillna(0)
                    test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].fillna(0) 
                    
                    m_after_test_file_all_cleaning = memory_profiler.memory_usage()
                    mem_diff_m_after_test_file_all_cleaning = m_after_test_file_all_cleaning[0] - m_after_test_file_made_for_first_time[0]
                    print(f"It took {mem_diff_m_after_test_file_all_cleaning} Mb to execute this method")
                    #It took 10080.828125 Mb to execute this method
                    
                    
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
                    
                    
                    #Change made on 14-12-2020 as per Pratik. He said that logic for not taking null is dicey and he sees users matching records with null values of Trade Data and Settle date with other records
                    #test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
                    #test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]
                    
                    
                    test_file = test_file.reset_index().drop('index',1)
                    
                    #Change made on 14-12-2020 as per Pratik. He said that logic for not taking null is dicey and he sees users matching records with null values of Trade Data and Settle date with other records
                    test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].fillna(0)
                    test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].fillna(0)
                    
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
                    
                    #test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(str)
                    
                    #Change made on 14-12-2020 as per Pratik. He said that logic for not taking null is dicey and he sees users matching records with null values of Trade Data and Settle date with other records
                    #test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].fillna(0)
                    #test_file['SideB.ViewData.Trade Date'] = test_file['SideB.ViewData.Trade Date'].fillna(0)
                    #test_file = test_file[test_file['SideA.ViewData.Trade Date']!='nan']
                    
                    test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(int)
                    test_file['SideB.ViewData.Trade Date'] = test_file['SideB.ViewData.Trade Date'].astype(int)
                    test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
                    test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int) 
                    
                    test_file = test_file.reset_index().drop('index',1)
                    
                    test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(float).astype(int)
                    
                    test_file['TD_bucket'] = test_file['Trade_date_diff'].apply(lambda x: 0 if x==0 else(1 if x>=-2 and x<=2 else 2))
                    test_file['SD_bucket'] = test_file['Settle_date_diff'].apply(lambda x: 0 if x==0 else(1 if x>=-2 and x<=2 else 2))
                    
                    m_before_test_file3 = memory_profiler.memory_usage()
                    mem_diff_m_before_test_file3 = m_before_test_file3[0] - m_after_test_file_all_cleaning[0]
                    print(f"It took {mem_diff_m_before_test_file3} Mb to execute this method")
                    #It took 1111.16796875 Mb to execute this method
                    
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
                    
                    m_before_X_test = memory_profiler.memory_usage()
                    mem_diff_m_before_X_test = m_before_X_test[0] - m_before_test_file3[0]
                    print(f"It took {mem_diff_m_before_X_test} Mb to execute this method")
                    #It took 2291.34375 Mb to execute this method
                    
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
                    print('Weiss_final_model2_with_umt.sav going to load')					
                    clf = pickle.load(open(filename, 'rb'))
                    print('Weiss_final_model2_with_umt.sav loaded')					
                    
                    # ## Predictions
                    
                    # Actual class predictions
                    print('shape of X_test is')
                    print(X_test.shape[0])
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
                    
                    m_after_X_test_first_prediction = memory_profiler.memory_usage()
                    mem_diff_m_after_X_test_first_prediction = m_after_X_test_first_prediction[0] - m_before_X_test[0]
                    print(f"It took {mem_diff_m_after_X_test_first_prediction} Mb to execute this method")
                    #It took 916.24609375 Mb to execute this method
                    
                    os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')
                    
                    #filename2 = 'Soros_full_model_all_two_step.sav'
                    
                    ## TODO Import MOdel2 as per the two step modelling process
                    
                    #filename2 = 'OakTree_final_model2_step_two.sav'
                    #filename2 = 'Weiss_final_model2_two_step.sav'

                    filename2 = 'Weiss_final_model2_with_umt_two_step.sav'
                    print('Weiss_final_model2_with_umt_two_step.sav going to load')					
                    clf2 = pickle.load(open(filename2, 'rb'))
                    print('Weiss_final_model2_with_umt_two_step.sav going to load')					
                                        
                    # Actual class predictions
                    print('shape of X_test2 is')
                    print(X_test2.shape[0])
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
                    
                    m_after_X_test_second_prediction = memory_profiler.memory_usage()
                    mem_diff_m_after_X_test_second_prediction = m_after_X_test_second_prediction[0] - m_after_X_test_first_prediction[0]
                    print(f"It took {mem_diff_m_after_X_test_second_prediction} Mb to execute this method")
                    #It took 115.390625 Mb to execute this method
                    X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)
                    
                    X_test.loc[(X_test['Amount_diff_2']!=0) & (X_test['Predicted_action']=='UMR_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'
                    
                    X_test.loc[(X_test['Amount_diff_2']==0) & (X_test['Predicted_action']=='UMB_One_to_One'), 'Predicted_action'] = 'UMR_One_to_One'
                    
                    X_test.loc[((X_test['Amount_diff_2']>1)  | (X_test['Amount_diff_2']<-1)) & (X_test['Predicted_action']=='UMT_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'
                    
                    #Changes made on 25-11-2020.
                    filepaths_X_test = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\X_Test_for_Pratik_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
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
                    else:
                        many_side_array = []
                        one_side_array = []
    
                    if len(one_side_array)>0:
                        
                        dup_otm_table_new = pd.DataFrame(one_side_array)  
                        dup_otm_table_new.columns = ['SideB.ViewData.Side0_UniqueIds']
                        dup_otm_table_new['SideA.ViewData.Side1_UniqueIds'] = many_side_array
                    else:
                        dup_otm_table_new = pd.DataFrame()
                    
                    #Change made on 29-11-2020. As per Pratik, dup_otm_table_new has both oto and otm. We have to include only otm. The below code is for separating out otm from dup_otm_table_new
                    if(dup_otm_table_new.shape[0] != 0):
                        dup_otm_table_new_raw = dup_otm_table_new
                        dup_otm_table_new_raw['otm_or_oto_comma_position'] = dup_otm_table_new_raw['SideA.ViewData.Side1_UniqueIds'].apply(lambda x : str(x).replace("' '","' , '").find(','))
                        dup_otm_table_new_raw['otm_or_oto_flag'] = dup_otm_table_new_raw['otm_or_oto_comma_position'].apply(lambda x : 'oto' if x == -1 else 'otm')
                        dup_otm_table_new_final = dup_otm_table_new_raw[dup_otm_table_new_raw['otm_or_oto_flag'] == 'otm'][['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds']]
                        
                        filepaths_dup_otm_table_new_raw = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\dup_otm_table_new_raw_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        dup_otm_table_new_raw.to_csv(filepaths_dup_otm_table_new_raw)
                        
                        filepaths_dup_otm_table_new_final= '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\dup_otm_table_new_final_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        dup_otm_table_new_final.to_csv(filepaths_dup_otm_table_new_final)
                    else:
                        dup_otm_table_new_final = pd.DataFrame()
                           
                    
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
                    
                    m_after_X_test_umt_seg = memory_profiler.memory_usage()
                    mem_diff_m_after_X_test_umt_seg = m_after_X_test_umt_seg[0] - m_after_X_test_second_prediction[0]
                    print(f"It took {mem_diff_m_after_X_test_umt_seg} Mb to execute this method")
                    #It took 64.3515625 Mb to execute this method
                    
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
                        if 'elim_except_mapped_custodian_acct_and_currency' in locals():
                            elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency
                        else:
                            elim_except_mapped_custodian_acct_and_currency = pd.DataFrame()
                            elim_except_mapped_custodian_acct_and_currency_and_umr = pd.DataFrame()
                    if(flag_side1_ids_to_remove_from_aa_new_exist == 1):
                        side1_intersection_elim_umr = set(elim_except_mapped_custodian_acct_and_currency['Side1_UniqueIds']) & set(final_umr_table['SideA.ViewData.Side1_UniqueIds'])
                        elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency_and_umr[~elim_except_mapped_custodian_acct_and_currency_and_umr['Side1_UniqueIds'].isin(list(side1_intersection_elim_umr))]
                    
                        if(elim.shape[0] != 0):    
                            elim = elim[~elim['Side1_UniqueIds'].isin(list(side1_intersection_elim_umr))] 
                    else:
                        if 'elim_except_mapped_custodian_acct_and_currency' in locals():
                            elim_except_mapped_custodian_acct_and_currency_and_umr = elim_except_mapped_custodian_acct_and_currency
                        else:
                            elim_except_mapped_custodian_acct_and_currency = pd.DataFrame()
                            elim_except_mapped_custodian_acct_and_currency_and_umr = pd.DataFrame()
                    
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
                    
                    m_before_X_test_no_pair_seg = memory_profiler.memory_usage()
                    mem_diff_m_before_X_test_no_pair_seg = m_before_X_test_no_pair_seg[0] - m_after_X_test_umt_seg[0]
                    print(f"It took {mem_diff_m_before_X_test_no_pair_seg} Mb to execute this method")
                    #It took 0.0 Mb to execute this method
                    
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
                    
                    if(elim_except_mapped_custodian_acct_and_currency_and_umr.shape[0] != 0):
                        updown_ids_a_side = elim_except_mapped_custodian_acct_and_currency_and_umr['Side1_UniqueIds'].unique()
                        updown_ids_b_side = elim_except_mapped_custodian_acct_and_currency_and_umr['Side0_UniqueIds'].unique()
                    else:
                        updown_ids_a_side = []
                        updown_ids_b_side = []
                        
                    m_before_X_test_left = memory_profiler.memory_usage()
                    mem_diff_m_before_X_test_left = m_before_X_test_left[0] - m_before_X_test_no_pair_seg[0]
                    print(f"It took {mem_diff_m_before_X_test_left} Mb to execute this method")
                    #It took 43.52734375 Mb to execute this method
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
                    
                    m_before_cc_new = memory_profiler.memory_usage()
                    mem_diff_m_before_cc_new = m_before_cc_new[0] - m_before_X_test_left[0]
                    print(f"It took {mem_diff_m_before_cc_new} Mb to execute this method")
                    #It took 213.328125 Mb to execute this method
                    
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
                    
                    m_after_cc_new = memory_profiler.memory_usage()
                    mem_diff_m_after_cc_new = m_after_cc_new[0] - m_before_cc_new[0]
                    print(f"It took {mem_diff_m_after_cc_new} Mb to execute this method")
                    #It took 6.20703125 Mb to execute this method
                    
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
                    #Change made on 04-12-2020 for Pratik. Below code is commented out and new code is added in place of it
                    #Begin comment change
                    
                    #cliff_for_loop = 16
                    #
                    #threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
                    #threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
                    #threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()
                    #
                    #exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
                    #exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()
                    #
                    #def subSum(numbers,total):
                    #    for length in range(1, 3):
                    #        if len(numbers) < length or length < 1:
                    #            return []
                    #        for index,number in enumerate(numbers):
                    ##            if length == 1 and np.isclose(number, total, atol=0).any():
                    ##Change made on 03-12-2020 as asked by Pratik in value of atol
                    #            if length == 1 and np.isclose(number, total, atol=0.35).any():
                    #
                    #                return [number]
                    #            subset = subSum(numbers[index+1:],total-number)
                    #            if subset: 
                    #                return [number] + subset
                    #        return []
                    #               
                    #
                    ##null_value ='No'
                    #many_ids_1 = []
                    #one_id_0 = []
                    #amount_array =[]
                    ##Changes made on 27-11-2020 as asked by Pratik to check how much time this code will take if we dont take X_test_left, but instead take X_test. Taking X_test should result in more accurate results but more time. The tradeoff between time and accuracy is what we need to measure
                    #for key in X_test_left[~((X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(exceptions_0_umb_ids)) | (X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) |(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])))]['SideB.ViewData.Side0_UniqueIds'].unique():
                    #    print(key)
                    #    
                    #    if key in threshold_0_umb:
                    #
                    #        values =  X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
                    #        net_sum = X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
                    #
                    #        #memo = dict()
                    #        #print(values)
                    #        #print(net_sum)
                    #
                    #        if subSum(values,net_sum) == []: 
                    #            #print("There are no valid subsets.")
                    #            amount_array = ['NULL']
                    #        else:
                    #            amount_array = subSum(values,net_sum)
                    #
                    #            id1_aggregation = X_test_left[(X_test_left['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
                    #            id0_unique = key       
                    #
                    #            if len(id1_aggregation)>1: 
                    #                many_ids_1.append(id1_aggregation)
                    #                one_id_0.append(id0_unique)
                    #            else:
                    #                pass
                    #            
                    #    else:
                    #        values =  X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.B-P Net Amount'].values
                    #        net_sum = X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
                    #
                    #        #memo = dict()
                    #        #print(values)
                    #        #print(net_sum)
                    #
                    #        if subSum(values,net_sum) == []: 
                    #            #print("There are no valid subsets.")
                    #            amount_array = ['NULL']
                    #        else:
                    #            amount_array = subSum(values,net_sum)
                    #
                    #            id1_aggregation = X_test_left[(X_test_left['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
                    #            id0_unique = key       
                    #
                    #            if len(id1_aggregation)>1: 
                    #                many_ids_1.append(id1_aggregation)
                    #                one_id_0.append(id0_unique)
                    #            else:
                    #                pass
                    #
                    #umr_otm_table = pd.DataFrame(one_id_0)
                    #End comment change
                    
                    #Change made on 04-12-2020 for Pratik. Above code is commented out and new code is added in place of it as below
                    #Begin replace change
                    
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
                                if length == 1 and np.isclose(number, total, atol=0.25).any():
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
                    #    print(loop_count)
                        if key in exceptions_0_umb_ids:
                            sort_data = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
                            sort_data = sort_data.reset_index().drop('index',1)
                    #        Change made on 08-12-2020. As per Pratik, we will take the first 8 values, not 10 in order to not overpredict otm and mto umrs
                    #        sort_data = sort_data.loc[0:10,:]
                            sort_data = sort_data.loc[0:8,:]
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
                            
                            sort_data2 = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
                            sort_data2 = sort_data2.reset_index().drop('index',1)
                    
                    #        Change made on 08-12-2020. As per Pratik, we will take the first 8 values, not 10 in order to not overpredict otm and mto umrs
                    #        sort_data2 = sort_data2.loc[0:10,:]
                            sort_data2 = sort_data2.loc[0:8,:]
                            
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
                    else:
                        acc_amount = pd.DataFrame()
                        pb_ids_otm = pd.DataFrame()
                        
                    umr_otm_table_double_count = pd.DataFrame(acc_id_single)
                    if(umr_otm_table_double_count.shape[0] != 0):
                        umr_otm_table_double_count.columns = ['SideB.ViewData.Side0_UniqueIds']
                        umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'] = pb_ids_otm_left
                    
                    umr_otm_table_final = pd.concat([umr_otm_table, umr_otm_table_double_count], axis=0)
                    
                    umr_otm_table_final = umr_otm_table_final.reset_index().drop('index',1)
                    
                    m_after_umr_otm_table_loop = memory_profiler.memory_usage()
                    mem_diff_m_after_umr_otm_table_loop = m_after_umr_otm_table_loop[0] - m_after_cc_new[0]
                    print(f"It took {mem_diff_m_after_umr_otm_table_loop} Mb to execute this method")
                    
                    # ### Many to One
                    #Change made on 04-12-2020 for Pratik. Below code is commented out and new code is added in place of it
                    #Begin comment change
                    
                    #cliff_for_loop = 17
                    #
                    #threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
                    #threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
                    #threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()
                    #
                    #exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
                    #exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()
                    #
                    #def subSum(numbers,total):
                    #    for length in range(1, 3):
                    #        if len(numbers) < length or length < 1:
                    #            return []
                    #        for index,number in enumerate(numbers):
                    ##            if length == 1 and np.isclose(number, total, atol=0).any():
                    ##Change made on 03-12-2020 as asked by Pratik in value of atol
                    #            if length == 1 and np.isclose(number, total, atol=0.35).any():
                    #                return [number]
                    #           
                    #            subset = subSum(numbers[index+1:],total-number)
                    #            if subset: 
                    #                return [number] + subset
                    #        return []
                    #        
                    #
                    ##null_value ='No'
                    #
                    #many_ids_0 = []
                    #one_id_1 = []
                    #amount_array2 =[]
                    ##Changes made on 27-11-2020 as asked by Pratik to check how much time this code will take if we dont take X_test_left, but instead take X_test. Taking X_test should result in more accurate results but more time. The tradeoff between time and accuracy is what we need to measure
                    #
                    #for key in X_test[~((X_test['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)) | (X_test['SideB.ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(final_umt_table['SideA.ViewData.Side1_UniqueIds'])) |(X_test['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]['SideA.ViewData.Side1_UniqueIds'].unique():
                    #    #if key not in ['1174_379879573_State Street','201_379823765_State Street']:
                    #    print(key)
                    #    if key in threshold_1_umb:
                    #
                    #        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].values
                    #        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()
                    #
                    #        #memo = dict()
                    #
                    #        if subSum(values2,net_sum2) == []: 
                    #            amount_array2 =[]
                    #            #print("There are no valid subsets.")
                    #
                    #        else:
                    #            amount_array2 = subSum(values2,net_sum2)
                    #
                    #            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
                    #            id1_unique = key       
                    #
                    #            if len(id0_aggregation)>1: 
                    #                many_ids_0.append(id0_aggregation)
                    #                one_id_1.append(id1_unique)
                    #            else:
                    #                pass
                    #
                    #    else:
                    #        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Accounting Net Amount'].values
                    #        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()
                    #
                    #        #memo = dict()
                    #
                    #        if subSum(values2,net_sum2) == []: 
                    #            amount_array2 =[]
                    #            #print("There are no valid subsets.")
                    #
                    #        else:
                    #            amount_array2 = subSum(values2,net_sum2)
                    #
                    #            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
                    #            id1_unique = key       
                    #
                    #            if len(id0_aggregation)>1: 
                    #                many_ids_0.append(id0_aggregation)
                    #                one_id_1.append(id1_unique)
                    #            else:
                    #                pass
                    #            
                    #umr_mto_table = pd.DataFrame(one_id_1)
                    
                    #Change made on 04-12-2020 for Pratik. Above code is commented out and new code is added in place of it as below
                    #Begin replace change
                    
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
                                if length == 1 and np.isclose(number, total, atol=0.25).any():
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
                            sort_data = X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
                            sort_data = sort_data.reset_index().drop('index',1)
                            
                    #        Change made on 08-12-2020. As per Pratik, we will take the first 8 values, not 10 in order to not overpredict otm and mto umrs
                    #        sort_data = sort_data.loc[0:10,:]
                            sort_data = sort_data.loc[0:8,:]
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
                            
                            sort_data2 = X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')].sort_values(by = ['probability_UMB_2'], ascending =[False])
                            sort_data2 = sort_data2.reset_index().drop('index',1)
                            
                    #        Change made on 08-12-2020. As per Pratik, we will take the first 8 values, not 10 in order to not overpredict otm and mto umrs
                    #        sort_data2 = sort_data2.loc[0:10,:]
                            sort_data2 = sort_data2.loc[0:8,:]
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
                    
                    m_after_umr_mto_table_loop = memory_profiler.memory_usage()
                    mem_diff_m_after_umr_mto_table_loop = m_after_umr_mto_table_loop[0] - m_after_umr_otm_table_loop[0]
                    print(f"It took {mem_diff_m_after_umr_mto_table_loop} Mb to execute this method")
                    
                    
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
                        if(X_test.shape[0] != 0):    
                            b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
                            a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()
                            
                            b_side_agg['len'] = b_side_agg['Predicted_action'].str.len()
                            b_side_agg['No_Pair_flag'] = b_side_agg['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
                        
                            a_side_agg['len'] = a_side_agg['Predicted_action'].str.len()
                            a_side_agg['No_Pair_flag'] = a_side_agg['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
                            
                            no_pair_ids_b_side = b_side_agg[(b_side_agg['len']==1) & (b_side_agg['No_Pair_flag']==1)]['SideB.ViewData.Side0_UniqueIds'].values
                        
                            no_pair_ids_a_side = a_side_agg[(a_side_agg['len']==1) & (a_side_agg['No_Pair_flag']==1)]['SideA.ViewData.Side1_UniqueIds'].values
                            
                            return no_pair_ids_b_side, no_pair_ids_a_side
                        else:
                            return list(), list()
                               
                    open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)
                    
                    X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]
                    X_test_left4 = X_test_left4.reset_index().drop('index',1)
                    
                    #X_test_left4['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left4['SideA.ViewData.Side1_UniqueIds'].nunique()
                    
                    # ## Seperating OTM and MTO from MTM
                    if(mtm_df_sd.shape[0] != 0):
                    
                        mtm_df_sd['len_1'] = mtm_df_sd['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
                        mtm_df_sd['len_0'] = mtm_df_sd['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
    
                    if(mtm_df_eqs.shape[0] != 0):
                        mtm_df_eqs['len_1'] = mtm_df_eqs['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
                        mtm_df_eqs['len_0'] = mtm_df_eqs['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
                    
                    new_mto1 = pd.DataFrame()
                    new_mto2 = pd.DataFrame()
                    
                    new_otm1 = pd.DataFrame()
                    new_otm2 = pd.DataFrame()
                    
                    new_mtm1 = pd.DataFrame()
                    new_mtm2 = pd.DataFrame()
                    
                    if(mtm_df_sd.shape[0] != 0):
    
                        if mtm_df_sd['len_1'].max() ==1: 
                            new_mto1 = mtm_df_sd[mtm_df_sd['len_1']==1].drop(['len_1','len_0'],1)
                            new_mto1 = new_mto1.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
                        else:
                            if mtm_df_sd['len_0'].max() ==1:
                                new_otm1 = mtm_df_sd[mtm_df_sd['len_0']==1].drop(['len_1','len_0'],1)
                                new_otm1 = new_otm1.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
                            
                    if(mtm_df_eqs.shape[0] != 0):
                            
                        if mtm_df_eqs['len_1'].max() ==1: 
                            new_mto2 = mtm_df_eqs[mtm_df_eqs['len_1']==1].drop(['len_1','len_0'],1)
                            new_mto2 = new_mto2.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
                        else:
                            if mtm_df_eqs['len_0'].max() ==1:
                                new_otm2 = mtm_df_eqs[mtm_df_eqs['len_0']==1].drop(['len_1','len_0'],1)
                                new_otm2 = new_otm2.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
                            
                    if(mtm_df_sd.shape[0] != 0):                
                            
                        if mtm_df_sd[(mtm_df_sd['len_1']>1) & (mtm_df_sd['len_0']>1)].empty ==False:
                            new_mtm1 = mtm_df_sd[(mtm_df_sd['len_1']>1) & (mtm_df_sd['len_0']>1)].drop(['len_1','len_0'],1)
                            new_mtm1 = new_mtm1.reset_index().drop('index',1)
                            new_mtm1 = new_mtm1.rename(columns = {'ViewData.Side0_UniqueIds':'SideB.ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds':'SideA.ViewData.Side1_UniqueIds'})
                        
                    if(mtm_df_eqs.shape[0] != 0):               
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
                    
                    
                    
                    #Change added on 15-12-2020 for Pratik. This change below will remove duplication of ids found in mto/otm and in mtm. Note that preference is given to mto/otm
                    
                    #Begin changes made on 15-12-2020
                    if final_otm_table.shape[0]>0:
                        otm_flat_new = [item for sublist in final_otm_table['SideA.ViewData.Side1_UniqueIds'].values for item in sublist]  + list(final_otm_table['SideB.ViewData.Side0_UniqueIds'].values)
                    else:
                        otm_flat_new = ['None','nan']
                        
                    if final_mto_table.shape[0]>0:
                        mto_flat_new = [item for sublist in final_mto_table['SideB.ViewData.Side0_UniqueIds'].values for item in sublist] + list(final_mto_table['SideA.ViewData.Side1_UniqueIds'].values)
                    else:
                        mto_flat_new = ['NONE','NAN']
                    
                        
                    total_flat_new = otm_flat_new + mto_flat_new  
                    
                    if final_mtm_table.shape[0]>0:
                    
                        final_mtm_table['zero_dup_flag'] = final_mtm_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: 1 if any(key in x for key in total_flat_new) else 0)
                        final_mtm_table['one_dup_flag'] = final_mtm_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: 1 if any(key in x for key in total_flat_new) else 0)
                        final_mtm_table = final_mtm_table[(final_mtm_table['zero_dup_flag']==0) & (final_mtm_table['one_dup_flag']==0)]
                        final_mtm_table = final_mtm_table.reset_index().drop('index',1)
                        final_mtm_table = final_mtm_table.drop(['zero_dup_flag','one_dup_flag'],1)
                    
                    #End changes made on 15-12-2020
                    
                    #Change made on 13-12-2020. The below piece of code had typo. Now the corrections have been made and typos deleted
                    #if final_mto_table.empty == False:
                    #    final_mto_table['SideB.ViewData.Side0_UniqueIds'] = final_mto_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("['",''))
                    #    final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("']",''))
                    #    final_mto_table['SideB.ViewData.Side0_UniqueIds'] = final_mto_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("['",''))
                    #    final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("']",''))
                    #
                    #if final_otm_table.empty == False:
                    #    final_otm_table['SideB.ViewData.Side0_UniqueIds'] = final_otm_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("['",''))
                    #    final_otm_table['SideA.ViewData.Side1_UniqueIds'] = final_otm_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("']",''))
                    #
                    #if final_mtm_table.empty:
                    #    print (' No MTM')
                    
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
                    
                    filepaths_final_umr_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umr_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                    
                    #filepaths_final_umr_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_umr_table_copy_new_to_write.csv'
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
                    filepaths_final_umt_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umt_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                    final_umt_table_copy_new_to_write.to_csv(filepaths_final_umt_table_copy_new_to_write)
                    
                    
                    #final_no_pair_table, no_pair_ids_df, open_ids_0_last, open_ids_1_last
                    no_pair_ids_last = list(open_ids_1_last)
                    for x in list(open_ids_0_last):
                        no_pair_ids_last.append(x)
                    no_pair_ids_last_df = pd.DataFrame(no_pair_ids_last, columns = ['Side0_1_UniqueIds'])
                    no_pair_ids_last_df = no_pair_ids_last_df[~no_pair_ids_last_df['Side0_1_UniqueIds'].isin(['None'])]
                    
                    final_no_pair_table_copy_1 = final_no_pair_table_copy.append([no_pair_ids_df,no_pair_ids_last_df])
                    if(final_mto_table.shape[0] != 0):
                        final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].astype(str).apply(lambda x : x.replace('[','').replace(']','').replace('\'','').strip())
                    if(final_otm_table.shape[0] != 0):
                        final_otm_table['SideB.ViewData.Side0_UniqueIds'] = final_otm_table['SideB.ViewData.Side0_UniqueIds'].astype(str).apply(lambda x : x.replace('[','').replace(']','').replace('\'','').strip())
                    
                    if(final_mto_table.shape[0] != 0):
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
                    
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task ID_x']=='NaN','Task ID'] = final_no_pair_table_copy_1['ViewData.Task ID_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task ID_y']=='NaN','Task ID'] = final_no_pair_table_copy_1['ViewData.Task ID_x']
                    
                    final_no_pair_table_copy_1['Task ID'] = final_no_pair_table_copy_1['Task ID'].replace('\.0','', regex = True)
                    
                    final_no_pair_table_copy_1['ViewData.BreakID_x'] = final_no_pair_table_copy_1['ViewData.BreakID_x'].astype(str)
                    final_no_pair_table_copy_1['ViewData.BreakID_y'] = final_no_pair_table_copy_1['ViewData.BreakID_y'].astype(str)
                     
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_x']=='None','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_y']=='None','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_x']
                    
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_x']=='nan','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_y']=='nan','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_x']
                    
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_x']=='NaN','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.BreakID_y']=='NaN','BreakID'] = final_no_pair_table_copy_1['ViewData.BreakID_x']
                    
                    final_no_pair_table_copy_1['BreakID'] = final_no_pair_table_copy_1['BreakID'].replace('\.0','', regex = True)
                    
                    final_no_pair_table_copy_1['ViewData.Task Business Date_x'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x'].astype(str)
                    final_no_pair_table_copy_1['ViewData.Task Business Date_y'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y'].astype(str)
                     
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_x']=='None','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_y']=='None','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x']
                    
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_x']=='nan','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_y']=='nan','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x']
                    
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_x']=='NaN','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_y']
                    final_no_pair_table_copy_1.loc[final_no_pair_table_copy_1['ViewData.Task Business Date_y']=='NaN','Task Business Date'] = final_no_pair_table_copy_1['ViewData.Task Business Date_x']
                    
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
                    
                    filepaths_final_no_pair_table_copy_1_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' +client + '\\final_no_pair_table_copy_1_to_write' + '_TaskID_' + str(TaskID_z) + '.csv'
                    final_no_pair_table_copy_1_to_write.to_csv(filepaths_final_no_pair_table_copy_1_to_write)
                    
                    #comment_table_eq_swap
                    
                    if(comment_table_eq_swap.shape[0] != 0):
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
                        
                        filepaths_comment_table_eq_swap_copy_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\comment_table_eq_swap_copy_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        comment_table_eq_swap_copy_to_write.to_csv(filepaths_comment_table_eq_swap_copy_to_write)
                    else:
                        comment_table_eq_swap_copy_to_write = pd.DataFrame()
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
                        final_smb_ob_table_copy = make_Side0_Side1_columns_for_final_smb_ob_table(final_smb_ob_table_copy,meo_df)
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
                        filepaths_final_smb_ob_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_smb_ob_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                    
                        final_smb_ob_table_copy.to_csv(filepaths_final_smb_ob_table_copy)
                    
                    
                    else:
                        final_smb_ob_table_copy = pd.DataFrame()
                    #End code change made on 12-12-2020 to incorporate final_smb_ob_table
                    
                    #Change made on 11-12-2020 to incorporate mtm_df_ex and mtm_df_fx into final_df_2
                    #Begin Change for incorporating mtm_df_fx and mto_df_ex into final_df_2
                    if(mtm_df_ex.shape[0] != 0):
                        if(mtm_df_fx.shape[0] != 0):
                            mtm_df_ex_and_fx = mtm_df_ex.append(mtm_df_fx)
                        else:
                            mtm_df_ex_and_fx = mtm_df_ex.copy()
                    elif(mtm_df_fx.shape[0] != 0):
                        mtm_df_ex_and_fx = mtm_df_fx.copy()
                    else:
                        mtm_df_ex_and_fx = pd.DataFrame()
                    
                    #Change added on 22-12-2020 to add mtm_df_rv to mtm_df_ex_and_fx
                    if(mtm_df_rv.shape[0] != 0):
                        mtm_df_ex_and_fx = mtm_df_ex_and_fx.append(mtm_df_rv)
                    
                    #Note that ideally df name should be mtm_df_ex_and_fx_and_rv, but since the change was made later on, I will keep the name same as before, which is mtm_df_ex_and_fx
                        
                    if(mtm_df_ex_and_fx.shape[0] != 0):
                        
                        mtm_df_ex_and_fx_copy = mtm_df_ex_and_fx.copy()
                        
                        mtm_df_ex_and_fx_copy['BreakID_Side1'] = mtm_df_ex_and_fx_copy['ViewData.Side1_UniqueIds'].apply( \
                                                                lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
                                                                                                                      fun_side_0_or_1 = 1, \
                                                                                                                      fun_str_list_Side_01_UniqueIds = x))
                        
                        mtm_df_ex_and_fx_copy['BreakID_Side0'] = mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds'].apply( \
                                                                lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
                                                                                                                      fun_side_0_or_1 = 0, \
                                                                                                                      fun_str_list_Side_01_UniqueIds = x))
                        
                        
                        
                        mtm_df_ex_and_fx_copy = normalize_final_no_pair_table_col_names(fun_final_no_pair_table = mtm_df_ex_and_fx_copy)
                        mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds'] = mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds'].astype(str)
                        mtm_df_ex_and_fx_copy['ViewData.Side1_UniqueIds'] = mtm_df_ex_and_fx_copy['ViewData.Side1_UniqueIds'].astype(str)
                        
                        #
                        #Change made on 10-12-2020. Below piece of code is wrong for merging. Commenting out below two lines of code and writing replacement code below it
                        #Single_Side0_UniqueId_for_merging_with_meo_df = final_mtm_table_copy['ViewData.Side0_UniqueIds'][0][0]
                        #final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = Single_Side0_UniqueId_for_merging_with_meo_df
                        #Change made on 10-12-2020. Above piece of code is wrong for merging. Commenting out above two lines of code and writing replacement code below it
                        mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds_for_merging'] = mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']')).apply(lambda x : get_first_non_null_value(str(x)))
                        
                        mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds_for_merging'] = mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds_for_merging'].astype(str) 
                        mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds_for_merging'] = mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds_for_merging'].replace('\'','',regex = True)
                        
                        mtm_df_ex_and_fx_copy_new = pd.merge(mtm_df_ex_and_fx_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'ViewData.Side0_UniqueIds_for_merging', right_on = 'ViewData.Side0_UniqueIds', how='left')
                        
                        mtm_df_ex_and_fx_copy_new['Predicted_Status'] = 'UMR'
                        mtm_df_ex_and_fx_copy_new['Predicted_action'] = 'UMR_Many_to_Many'
                        mtm_df_ex_and_fx_copy_new['ML_flag'] = 'ML'
                        mtm_df_ex_and_fx_copy_new['SetupID'] = setup_code 
                        
                        del mtm_df_ex_and_fx_copy['ViewData.Side0_UniqueIds_for_merging']
                        del mtm_df_ex_and_fx_copy_new['ViewData.Side0_UniqueIds_for_merging']
                        
                        
                        filepaths_mtm_df_ex_and_fx_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\mtm_df_ex_and_fx_copy_new.csv'
                        mtm_df_ex_and_fx_copy_new.to_csv(filepaths_mtm_df_ex_and_fx_copy_new)
                        
                        change_names_of_mtm_df_ex_and_fx_copy_new_mapping_dict = {
                                                                    'ViewData.Side0_UniqueIds_x' : 'Side0_UniqueIds',
                                                                    'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                                                    'BreakID_Side0' : 'BreakID',
                                                                    'BreakID_Side1' : 'Final_predicted_break',
                                                                    'ViewData.Task ID' : 'Task ID',
                                                                    'ViewData.Task Business Date' : 'Task Business Date',
                                                                    'ViewData.Source Combination Code' : 'Source Combination Code'
                                                                }
                        
                        
                        mtm_df_ex_and_fx_copy_new.rename(columns = change_names_of_mtm_df_ex_and_fx_copy_new_mapping_dict, inplace = True)
                        
                        mtm_df_ex_and_fx_copy_new['Task Business Date'] = pd.to_datetime(mtm_df_ex_and_fx_copy_new['Task Business Date'])
                        mtm_df_ex_and_fx_copy_new['Task Business Date'] = mtm_df_ex_and_fx_copy_new['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
                        mtm_df_ex_and_fx_copy_new['Task Business Date'] = pd.to_datetime(mtm_df_ex_and_fx_copy_new['Task Business Date'])
                        
                        
                        mtm_df_ex_and_fx_copy_new['PredictedComment'] = ''
                        
                    #    #Changing data types of columns as follows:
                    #    #Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
                    #    #BreakID, TaskID - int64
                    #    #SetupID - int32
                    #    final_mtm_table_copy_new['probability_UMB'] = 0.017
                    #    final_mtm_table_copy_new['probability_No_pair'] = 0.017
                    #    final_mtm_table_copy_new['probability_UMR'] = 0.95
                    #    final_mtm_table_copy_new['probability_UMT'] = 0.017
                    #        
                    #    for i in range(0,final_mtm_table_copy_new.shape[0]):
                    #        final_mtm_table_copy_new['probability_UMB'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
                    #        final_mtm_table_copy_new['probability_No_pair'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
                    #        final_mtm_table_copy_new['probability_UMR'].iloc[i] = float(decimal.Decimal(random.randrange(950, 1000))/1000)
                    #        final_mtm_table_copy_new['probability_UMT'].iloc[i] = float(decimal.Decimal(random.randrange(17, 100))/1000)
                        
                        mtm_df_ex_and_fx_copy_new['probability_UMB'] = ''
                        mtm_df_ex_and_fx_copy_new['probability_No_pair'] = ''
                        mtm_df_ex_and_fx_copy_new['probability_UMR'] = ''
                        mtm_df_ex_and_fx_copy_new['probability_UMT'] = ''
                        
                        mtm_df_ex_and_fx_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']] = mtm_df_ex_and_fx_copy_new[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'Source Combination Code', 'Predicted_Status', 'ML_flag']].astype(str)
                        
                        
                        #Note that BreakID now if more than one and in a list, so we cant convert it to float and int64
                        mtm_df_ex_and_fx_copy_new[['Task ID']] = mtm_df_ex_and_fx_copy_new[['Task ID']].astype(float)
                        mtm_df_ex_and_fx_copy_new[['Task ID']] = mtm_df_ex_and_fx_copy_new[['Task ID']].astype(np.int64)
                        
                        mtm_df_ex_and_fx_copy_new[['SetupID']] = mtm_df_ex_and_fx_copy_new[['SetupID']].astype(int)
                        
                        #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
                        #final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)
                        
                        change_col_names_mtm_df_ex_and_fx_copy_new_dict = {
                                                'Task ID' : 'TaskID',
                                                'Task Business Date' : 'BusinessDate',
                                                'Source Combination Code' : 'SourceCombinationCode'
                                                }
                        mtm_df_ex_and_fx_copy_new.rename(columns = change_col_names_mtm_df_ex_and_fx_copy_new_dict, inplace = True)
                        
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
                        
                        mtm_df_ex_and_fx_copy_new_to_write = mtm_df_ex_and_fx_copy_new[cols_for_database_new]
                        
                        filepaths_mtm_df_ex_and_fx_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\mtm_df_ex_and_fx_copy_new_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                    
                        mtm_df_ex_and_fx_copy_new_to_write.to_csv(filepaths_mtm_df_ex_and_fx_copy_new_to_write)
                    
                    else:
                        mtm_df_ex_and_fx_copy_new_to_write = pd.DataFrame()
                    #End Change for incorporating mtm_df_fx and mto_df_ex into final_df_2
                    #11111
                    #final_mto_table
                    if(final_mto_table.shape[0] != 0):
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
                        
                        #filepaths_final_mto_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\UAT_Run\\X_Test_' + setup_code +'\\final_mto_table_copy.csv'
                        filepaths_final_mto_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mto_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        
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
                        
                        filepaths_final_mto_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mto_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        
                        final_mto_table_copy_new_to_write.to_csv(filepaths_final_mto_table_copy_new_to_write)
                    else:
                        final_mto_table_copy_new_to_write = pd.DataFrame()
                    
                    #final_otm_table
                    if(final_otm_table.shape[0] != 0):
                        final_otm_table_copy = final_otm_table.copy()
                        
                        #final_otm_table_copy['BreakID_Side0'] = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(list(final_otm_table_copy['SideB.ViewData.Side0_UniqueIds']))]['ViewData.BreakID'].values
                        #final_otm_table_copy['BreakID_Side0'] = final_otm_table_copy['BreakID_Side0'].astype(int)
                        
                        final_otm_table_copy['BreakID_Side0'] = final_otm_table_copy['SideB.ViewData.Side0_UniqueIds'].apply(lambda x : meo_df[meo_df['ViewData.Side0_UniqueIds'] == x]['ViewData.BreakID'].unique())
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
                        
                        filepaths_final_otm_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_otm_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        final_otm_table_copy_new_to_write.to_csv(filepaths_final_otm_table_copy_new_to_write)
                    else:
                        final_otm_table_copy_new_to_write = pd.DataFrame()
                    
                    
                    #final_mtm_table
                    if(final_mtm_table.shape[0] != 0):
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
                        
                        filepaths_final_mtm_table_copy_new = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mtm_table_copy_new_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
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
                        
                        filepaths_final_mtm_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_mtm_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        
                        final_mtm_table_copy_new_to_write.to_csv(filepaths_final_mtm_table_copy_new_to_write)
                    else:
                        final_mtm_table_copy_new_to_write = pd.DataFrame()
                        
                    #final_oto_umb_table
                    if(final_oto_umb_table.shape[0] != 0):
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
                        
                        filepaths_final_oto_umb_table_copy_new_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_oto_umb_table_copy_new_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        final_oto_umb_table_copy_new_to_write.to_csv(filepaths_final_oto_umb_table_copy_new_to_write)
                    else:
                        final_oto_umb_table_copy_new_to_write = pd.DataFrame()
                               
                    #final_closed_df
                    #Closed Begins
                    #closed_columns_for_updation = ['ViewData.BreakID','ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Source Combination Code','ViewData.Task ID']
                    closed_columns_for_updation = ['ViewData.BreakID','ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Source Combination Code','ViewData.Task ID','ViewData.Accounting Net Amount','ViewData.B-P Net Amount','ViewData.Transaction Type']
                    
                    if(closed_df.shape[0] != 0):
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
                        filepaths_final_closed_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_closed_df_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                        final_closed_df.to_csv(filepaths_final_closed_df)
                    else:
                        final_closed_df = pd.DataFrame()
                        
                    
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
                    #Change added on 12-12-2020 to concat mtm_df_ex_and_fx_copy_new_to_write and final_smb_ob_table_copy dataframes
                                                      ,mtm_df_ex_and_fx_copy_new_to_write\
                                                      ,final_smb_ob_table_copy\
                                                      ], axis=0)
                else:
                    final_table_to_write = pd.concat([ob_carry_forward_df, \
                                                      umb_carry_forward_df\
                    #Change added on 12-12-2020 to concat mtm_df_ex_and_fx_copy_new_to_write and final_smb_ob_table_copy dataframes
                                                      ], axis=0)
                
                
                #Pratik Coverage calculation
#                coverage_meo = meo[~meo['ViewData.Status'].isin(['SMR','SMT','SPM','UMB'])]
#                
#                coverage_meo['ViewData.Side1_UniqueIds'].nunique() + coverage_meo['ViewData.Side0_UniqueIds'].nunique()
#                
#                final_umr_table.shape[0]*2 + final_umt_table.shape[0]*2 + len(no_pair_ids)+final_no_pair_table.shape[0] + len(open_ids_0_last) + len(open_ids_1_last) + comment_table_eq_swap.shape[0] + final_mto_table.shape[0]*3 +  final_otm_table.shape[0]*3 + final_mtm_table.shape[0]*3 +final_oto_umb_table.shape[0]*2 + final_closed_df.shape[0] + umb_carry_forward_df.shape[0] * 2
                
                filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                final_table_to_write.to_csv(filepaths_final_table_to_write)
                
                filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
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
                
                    fun_final_df_2['BreakID'] = fun_final_df_2['BreakID'].astype(str)
                    fun_final_df_2['Final_predicted_break'] = fun_final_df_2['Final_predicted_break'].astype(str)
                    fun_final_df_2['BreakID'] = fun_final_df_2['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
                    fun_final_df_2['Final_predicted_break'] = fun_final_df_2['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))
                
                
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
                
                filepaths_df_to_append = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\df_to_append_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
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
                    if(a=='No-Pair'):
                        if(b =='AA'):
                            return c
                        else:
                            return b
                    else:
                        return '12345'

                def fid1(a,b,c):
#                    if(a=='No-Pair'):
                        if(b =='AA'):
                            return c
                        else:
                            return b
#                    else:
#                        return '12345'
                
#                brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1,result_type="expand" )
                brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1,result_type="expand" )
                brk = brk.reset_index()
                brk = brk.drop('index', axis = 1)
                
                side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
                side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))
                
                
                meo1 = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(side0_id)]
                meo2 = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(side1_id)]
                
                meo1['ViewData.Side1_UniqueIds'] = ''
                meo2['ViewData.Side0_UniqueIds'] = ''
                
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
                print('finalized_model_weiss_125_v11.sav going to load')
                clf = pickle.load(open(filename, 'rb'))
                print('finalized_model_weiss_125_v11.sav loaded')
				
                # Actual class predictions
                print('shape of data211 is')
                print(data211.shape[0])
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
                
                
                
                result_non_trade = result_non_trade.drop('predicted comment', axis = 1)
                
                com_temp = pd.read_csv('Weiss comment template for delivery new.csv')
                
                com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
                
                result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
                
                #Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
                #def comgen(x,y,z,k):
                #    if x == 'Geneva':
                #        
                #        com = k + ' ' +y + ' ' + str(z)
                #    else:
                #        com = "Geneva" + ' ' +y + ' ' + str(z)
                #        
                #    return com
                #def comgen(x,y,z,k,m,a,b,c):
                #    trade_ttype = ['buy','sell','sell short','cover short','spot fx','forward','forward fx','spotfx','forwardfx']
                #    x = x.lower()
                #    if m in trade_ttype:
                #        if x == 'geneva':
                #        
                #            com = k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) 
                #        else:
                #            com = "Geneva" + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.' + k + 'booked the transaction'
                #    else:
                #        if x == 'geneva':
                #        
                #            com = k + ' ' +y + ' ' + str(z)
                #        else:
                #            com = "Geneva" + ' ' +y + ' ' + str(z)+ '.' + k + 'booked the transaction'
                #        
                #    return com
                #
                #
                #result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
                #result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
                #result_non_trade['ViewData.Settle Date'] = result_non_trade['ViewData.Settle Date'].astype(str)
                #result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)
                #
                ##result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                ##Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
                ##result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                #result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1'],x['ViewData.Transaction Type1'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date']), axis = 1)
                
                #Change made on 19-01-2021 as per Abhijeet. This change is for changing the language in Commenting
                #Begin change made on 19-01-2021
                def brokermap(x):
                    if x == 'barclays':
                        return 'BARC'
                    elif x == 'morgan stanely':
                        return 'MS'
                    elif x == 'jp morgan':
                        return 'JPM'
                    elif x == 'goldman sachs':
                        return 'GS'
                    elif x == 'us bank':
                        return 'USBK'
                    elif x == 'citi bank':
                        return 'CITI'
                    elif x == 'northern trust':
                        return 'NT'
                    elif x == 'deutsche bank':
                        return 'DB'
                    elif x=='state street':
                        return 'SST'
                    elif x == 'bn paribas':
                        return 'BNP'
                    elif x == 'credit suisse':
                        return 'CS'
                    else:
                        return x
                    
                
                
                
                def comgen(x,y,z,k,m,a,b,c):
                    trade_ttype = ['buy','sell','sell short','cover short','spot fx','forward','forward fx','spotfx','forwardfx']
                    pos_break = ['settlement amount , no pos break']
                    x = x.lower()
                    if m in trade_ttype:
                        if x != 'geneva':
                            if ((a!= 0) & (b!= 0)):
                                com = k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '. Geneva yet to book' 
                            elif  ((a==0) & (b!=0)):
                                com = k + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
                            elif  ((a!=0) & (b==0)):
                                com = k + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
                            else:
                                com = k + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
                        else:
                            if ((a!= 0) & (b!= 0)):
                                com = 'Geneva' + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '.'+ str(k) + ' yet to book' 
                            elif  ((a==0) & (b!=0)):
                                com = 'Geneva' + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ str(k) + ' yet to book'
                            elif  ((a!=0) & (b==0)):
                                com = 'Geneva' + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ str(k) + ' yet to book'
                            else:
                                com = 'Geneva' + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ str(k) + ' yet to book'
                    
                    elif m in pos_break:
                        
                        if ((a!= 0) & (b!= 0)):
                            com ='No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b)
                        elif  ((a==0) & (b!=0)):
                            com = 'No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for quantity' +' ' + str(b) 
                        elif  ((a!=0) & (b==0)):
                            com = 'No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for price' +' ' + str(a) 
                        else:
                            com = 'No position break, Geneva to reflect jpm trade on ' + str(z)
                        
                    else:
                        if x != 'geneva':
                        
                            com = k + ' ' +y + ' ' + str(z) + ". Geneva yet to book"
                        else:
                            com = 'Geneva' + ' ' +y + ' ' + str(z)+ '.' + k + ' yet to book the transaction'
                        
                    return com
                
                
                result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].apply(lambda x : str(x).split(' ')[0])
                result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].apply(lambda x : str(x).split(' ')[0])
                
                result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
                result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
                #result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].dt.date
                #result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date2'].astype(str)
                #result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].dt.date
                #result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date2'].astype(str)
                result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)
                result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : brokermap(x))
                
                result_non_trade['ViewData.Price'] = result_non_trade['ViewData.Price'].apply(lambda x : round(x,1))
                result_non_trade['ViewData.Quantity'] = result_non_trade['ViewData.Quantity'].apply(lambda x : round(x,1))
                
                
                #result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                #Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
                #result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
                result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date']), axis = 1)
                #End change made on 19-01-2021
                
                
                result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]
                
                
                
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
                
                #Change added on 14-12-2020 to rename ViewData.Cust Net Amount back to ViewData.B-P Net Amount
                meo_df.rename(columns = {'ViewData.Cust Net Amount' : 'ViewData.B-P Net Amount'}, inplace = True)
                
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
                final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].astype(str)
                
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
                final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace('\.0','',regex = True)

                umb_ids_to_convert_to_smb = list(final_df_2[((final_df_2['Final_predicted_break'] == '') & (final_df_2['Predicted_Status'] == 'UMB') & (final_df_2['PredictedComment'] == ''))]['BreakID'])
                
                final_df_2.loc[(final_df_2['BreakID'].isin(umb_ids_to_convert_to_smb)),'Predicted_Status'] = 'SMB'
                final_df_2.loc[(final_df_2['BreakID'].isin(umb_ids_to_convert_to_smb)),'Predicted_action'] = 'SMB'
                
                def smb_comment(sd,pb,tt):
                    #sd = pd.datetime(sd)
                    tt = str(tt)
                    if tt.lower() == 'dividend':
                        comment = 'Difference in DVD booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    elif tt.lower() == 'buy':
                        comment = 'Difference in fee and commision booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    elif tt.lower() == 'sell':
                        comment = 'Difference in sell trade booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    else:
                        comment = 'Difference in amount booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    return comment
                
                smb_breakids_str = list(final_df_2[final_df_2['Predicted_Status'] == 'SMB']['BreakID'])
                smb_breakids_int = [int(x) for x in smb_breakids_str]
                
                meo_df_smb_comment = meo_df[meo_df['ViewData.BreakID'].isin(smb_breakids_int)][['ViewData.BreakID','ViewData.Settle Date','ViewData.Prime Broker','ViewData.Transaction Type']]

                def predicted_comment_smb_integrate_row_apply(fun_row):
                    if(fun_row['Predicted_Status'] == 'SMB'):
                        return(fun_row['PredictedComment_SMB'])
                    else:
                        return(fun_row['PredictedComment'])
                
                if(meo_df_smb_comment.shape[0] != 0):
                    meo_df_smb_comment['PredictedComment_SMB'] = meo_df_smb_comment.apply(lambda x: smb_comment(x['ViewData.Settle Date'],x['ViewData.Prime Broker'],x['ViewData.Transaction Type']),axis=1)
                    meo_df_smb_comment['ViewData.BreakID']  = meo_df_smb_comment['ViewData.BreakID'].astype(str)
                    
                    meo_df_smb_comment.drop(columns = ['ViewData.Settle Date', 'ViewData.Prime Broker','ViewData.Transaction Type'], axis = 1, inplace = True)
                    meo_df_smb_comment.rename(columns = {'ViewData.BreakID' : 'BreakID'}, inplace = True)
                
                    final_df_2 = pd.merge(final_df_2,meo_df_smb_comment, on = 'BreakID', how = 'left')
                    final_df_2['PredictedComment_SMB'] = final_df_2['PredictedComment_SMB'].astype(str)
                
                    final_df_2['PredictedComment_SMB'] = final_df_2['PredictedComment_SMB'].replace('nan','',regex = True)
                    final_df_2['PredictedComment_SMB'] = final_df_2['PredictedComment_SMB'].replace('None','',regex = True)
                
                        
                
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
                    elif tt.lower() == 'buy':
                        comment = 'Difference in fee and commision booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    elif tt.lower() == 'sell':
                        comment = 'Difference in sell trade booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    elif tt.lower() in ['transfer','eq swap','equity swap']:
                        comment = 'Difference equity swap booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)   
                    else:
                        comment = 'Difference in amount booked between' + ' ' + str(pb) + ' & Geneva on ' + str(sd)
                    return comment
                
                if(umb_breakids_for_commenting_df.shape[0] != 0):
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
                    param_final_df.loc[(param_final_df['ActionType'].isnull()),'ActionType'] = 'Status not covered'
                    param_final_df.loc[(param_final_df['ActionType'].isna()),'ActionType'] = 'Status not covered'
                    param_final_df['ActionType'] = param_final_df['ActionType'].astype(str)
                
                    param_final_df.loc[((param_final_df['ML_flag'] == 'Not_Covered_by_ML')),'ActionTypeCode'] = 7    
                    param_final_df.loc[((param_final_df['Predicted_Status'] == 'OB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 6
                    param_final_df.loc[((param_final_df['Predicted_Status'] == 'OB') & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 3
                    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 2
                    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 1
                    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionTypeCode'] = 4
                    param_final_df.loc[(param_final_df['ActionTypeCode'].isnull()),'ActionTypeCode'] = 0
                    param_final_df.loc[(param_final_df['ActionTypeCode'].isna()),'ActionTypeCode'] = 0

                    param_final_df['ActionTypeCode'] = param_final_df['ActionTypeCode'].astype(int)
                
                    return(param_final_df)    
                
                final_df_2= ui_action_column(final_df_2)

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
                filepaths_umb_smb_duplication_issue_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\umb_smb_duplication_issue_df_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                umb_smb_duplication_issue_df.to_csv(filepaths_umb_smb_duplication_issue_df)
                
                colums_for_final_df_2 = ['len_intersection_set_of_BreakID_and_FinalPredictedBreak','BreakID_new','BusinessDate','Final_predicted_break_new','ML_flag','Predicted_Status','Predicted_action','SetupID','SourceCombinationCode','TaskID','probability_No_pair','probability_UMB','probability_UMR','probability_UMT','Side1_UniqueIds','PredictedComment','PredictedCategory','Side0_UniqueIds','ActionType','ActionTypeCode']
                
                
                final_df_2 = umb_smb_duplication_issue_df[colums_for_final_df_2]
                final_df_2.rename(columns = {'BreakID_new' : 'BreakID', 'Final_predicted_break_new' : 'Final_predicted_break'}, inplace = True)
                
                filepaths_final_df_2_before_making_umt_from_umr = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_before_making_umt_from_umr_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
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
                
                def get_NetAmountDifference_for_Side01UniqueIds_from_Side0UniqueId_and_Side0UniqueId_column_apply_row(fun_row, fun_meo_df):
                    side0_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side0_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
                    side1_UniqueIds_list_str = [x.lstrip().rstrip() for x in fun_row['Side1_UniqueIds'].replace('\' \'','\',\'').replace('\'','').split(',')]
                    lst_Net_Amount_Difference_for_side0_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin(side0_UniqueIds_list_str)]['ViewData.Accounting Net Amount'].unique())
                    lst_Net_Amount_Difference_for_side1_UniqueIds_list_str = list(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin(side1_UniqueIds_list_str)]['ViewData.B-P Net Amount'].unique())
                    amount_diff_of_sum_of_side_0_1_lists = sum(lst_Net_Amount_Difference_for_side0_UniqueIds_list_str) - sum(lst_Net_Amount_Difference_for_side1_UniqueIds_list_str)
                    rounded_amount_diff_of_sum_of_side_0_1_lists = round(amount_diff_of_sum_of_side_0_1_lists,3)
                    if(fun_row['Predicted_Status'] == 'UMR'):
                        if((abs(rounded_amount_diff_of_sum_of_side_0_1_lists) >= 0.01) and (abs(rounded_amount_diff_of_sum_of_side_0_1_lists) <= 1.00)):
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
                
                filepaths_final_df_2_after_making_umt_from_umr = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_after_making_umt_from_umr_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                final_df_2.to_csv(filepaths_final_df_2_after_making_umt_from_umr)
                
                final_df_2.drop(columns = ['Predicted_Status','Predicted_action'], axis = 1, inplace = True)
                
                final_df_2.rename(columns = {'Predicted_Status_new' : 'Predicted_Status','Predicted_action_new' : 'Predicted_action'}, inplace = True)
                
                final_df_2.drop(columns = ['Sum_of_Net_Amount_Difference','lst_Net_Amount_Difference_for_side0_UniqueIds','lst_Net_Amount_Difference_for_side1_UniqueIds'], axis = 1, inplace = True)
                
                final_df_2_copy_2 = final_df_2.copy()
                final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
                final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)
                
                final_df_2['ReconSetupName'] = 'Weiss Advisers Cash Recon'
                final_df_2['ClientShortCode'] = 'Weiss Advisors'
                
                today = date.today()
                today_Y_m_d = today.strftime("%Y-%m-%d")
                
                final_df_2['CreatedDate'] = today_Y_m_d
                final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
                final_df_2['CreatedDate'] = final_df_2['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
                final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
                
                
                
                
                filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_TaskID_' + str(TaskID_z) + '.csv'
                final_df_2.to_csv(filepaths_final_df_2)
                         
                if 'test_file' in locals():
                    del(test_file)
                if 'test_file2' in locals():
                    del(test_file2)
                if 'test_file3' in locals():
                    del(test_file3)
                if 'df1' in locals():
                    del(df1)
                if 'df' in locals():
                    del(df)
                if 'df2' in locals():
                    del(df2)
                if 'df_213_1' in locals():
                    del(df_213_1)
                if 'dff' in locals():
                    del(dff)
                if 'dff4' in locals():
                    del(dff4)
                if 'cc' in locals():
                    del(cc)
                if 'cc1' in locals():
                    del(cc1)
                if 'cc2' in locals():
                    del(cc2)
                if 'cc2_dummy' in locals():
                    del(cc2_dummy)
                if 'cc3' in locals():
                    del(cc3)
                if 'cc4' in locals():
                    del(cc4)
                if 'cc6' in locals():
                    del(cc6)
                if 'cc7' in locals():
                    del(cc7)
                if 'cc_new' in locals():
                    del(cc_new)
                if 'cc_dummy' in locals():
                    del(cc_dummy)
                if 'data' in locals():
                    del(data)
                if 'data2' in locals():
                    del(data2)
                if 'dff5' in locals():
                    del(dff5)
                if 'dffacc' in locals():
                    del(dffacc)
                if 'dffk2' in locals():
                    del(dffk2)


                if 'dffk3' in locals():
                    del(dffk3)
                if 'dffpb' in locals():
                    del(dffpb)
                if 'dup_df' in locals():
                    del(dup_df)
                if 'ee2' in locals():
                    del(ee2)
                if 'elim' in locals():
                    del(elim)
                if 'elim1' in locals():
                    del(elim1)
                if 'elim2' in locals():
                    del(elim2)
                if 'elim3' in locals():
                    del(elim3)
                if 'fff2' in locals():
                    del(fff2)
                if 'meo' in locals():
                    del(meo)
                if 'meo1' in locals():
                    del(meo1)
                if 'meo2' in locals():
                    del(meo2)
                if 'meo3' in locals():
                    del(meo3)
                if 'normalized_meo_df' in locals():
                    del(normalized_meo_df)
                if 'sample' in locals():
                    del(sample)
                if 'sample1' in locals():
                    del(sample1)
                if 'uni2' in locals():
                    del(uni2)
                if 'X_test' in locals():
                    del(X_test)
                if 'X_test2' in locals():
                    del(X_test2)
                if 'data211' in locals():
                    del(data211)


    #            del(test_file2)
    #            del(test_file3)
    #            del(df1)
    #            del(df)
    #            del(df2)
    #            del(df_213_1)
    #            del(dff)
    #            del(dff4)
    #            del(cc)
    #            del(cc1)
    #            del(cc2)
    #            del(cc2_dummy)
    #            del(cc3)
    #            del(cc4)
    #            del(cc6)
    #            del(cc7)
    #            del(cc_new)
    #            del(cc_dummy)
    #            del(data)
    #            del(data2)
    #            del(dff5)
    #            del(dffacc)
    #            del(dffk2)
    #            del(dffk3)
    #            del(dffpb)
    #            del(dup_df)
    #            del(ee2)
    #            del(elim)
    #            del(elim1)
    #            del(elim2)
    #            del(elim3)
    #            del(fff2)            
    #            del(meo)
    #            del(meo1)
    #            del(meo2)
    #            del(meo3)
    #            del(normalized_meo_df)
    #            del(sample)
    #            del(sample1)
    #            del(uni2)
                            
                coll_1_for_writing_prediction_data = db_for_writing_MEO_data['MLPrediction_' + setup_code]
                            
                final_df_2_without_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'] != 'UMR_One-Many_to_Many-One']
                if(final_df_2_without_UMR_Many_One_to_One_Many.shape[0] != 0):
                    final_df_2_without_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_without_UMR_Many_One_to_One_Many['BreakID'].astype(str)
                    final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_without_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)
                    final_df_2_without_UMR_Many_One_to_One_Many['SourceCombinationCode'] = final_df_2_without_UMR_Many_One_to_One_Many['SourceCombinationCode'].astype(str)
                            
                    data_dict_without_UMR_Many_One_to_One_Many = final_df_2_without_UMR_Many_One_to_One_Many.to_dict("records_final_1")
                    coll_1_for_writing_prediction_data.insert_many(data_dict_without_UMR_Many_One_to_One_Many) 
                
                final_df_2_with_UMR_Many_One_to_One_Many = final_df_2[final_df_2['Predicted_action'].isin(['UMR_One-Many_to_Many-One','UMR_Many_to_Many'])]
                if(final_df_2_with_UMR_Many_One_to_One_Many.shape[0] != 0):
                    
                    final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].astype(str)
                    final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].astype(str)
                    final_df_2_with_UMR_Many_One_to_One_Many['BreakID'] = final_df_2_with_UMR_Many_One_to_One_Many['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
                    final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'] = final_df_2_with_UMR_Many_One_to_One_Many['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))
                    final_df_2_with_UMR_Many_One_to_One_Many['SourceCombinationCode'] = final_df_2_with_UMR_Many_One_to_One_Many['SourceCombinationCode'].astype(str)
                    
                    data_dict_with_UMR_Many_One_to_One_Many = final_df_2_with_UMR_Many_One_to_One_Many.to_dict("records_final_2")
                    coll_1_for_writing_prediction_data.insert_many(data_dict_with_UMR_Many_One_to_One_Many) 
                
                print(setup_code)
                print(date_i)
                print('Following Task ID done')
                print(TaskID_z)
                Message_z = str(TaskID_z) + '|' + str(csc_z) + '|' + str(ReconPurpose_z) + '|' + str(collection_meo_z) + '|' + str(ProcessID_z) + '|' + 'SUCCESS' + '|' +  str(Setup_Code_z) + '|' + str(MongoDB_TaskID_z)
                rb_mq_obj_new_for_publish.fun_publish_single_message(param_message_body = Message_z)
                print(Message_z)
                   