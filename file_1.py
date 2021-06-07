# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 18:59:50 2021

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
#filepath_to_read_ReconDF_from = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) + '\\ReconDF_messages\\ReconDF_setup_' + str(setup_code) + '_date_' + str(today) + '_' + str(1) + '.csv'
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
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
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
print('This ran')