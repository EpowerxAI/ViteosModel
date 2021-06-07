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

import pandas as pd
import os
import sys
import numpy as np
from datetime import datetime, date
from difflib import SequenceMatcher
import pprint
import json



# In[2]:


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

def cleaned_meo(fun_filepath_meo):
    meo = pd.read_csv(fun_filepath_meo) \
          .drop_duplicates() \
          .reset_index() \
          .drop('index',1)
    
    meo = normalize_bp_acct_col_names(fun_df = meo)
    
    meo = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])] 
    meo = meo[~meo['ViewData.Status'].isnull()] \
          .reset_index() \
          .drop('index',1)
    
    meo['Date'] = pd.to_datetime(meo['ViewData.Task Business Date'])
    meo = meo[~meo['Date'].isnull()] \
          .reset_index() \
          .drop('index',1)
    meo['Date'] = pd.to_datetime(meo['Date']).dt.date
    meo['Date'] = meo['Date'].astype(str)

    meo['ViewData.Side0_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str)
    meo['ViewData.Side1_UniqueIds'] = meo['ViewData.Side1_UniqueIds'].astype(str)

    meo['flag_side0'] = meo.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
    meo['flag_side1'] = meo.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)

    meo.loc[meo['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
    meo.loc[meo['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

    meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(int)
    meo = meo[meo['ViewData.BreakID']!=-1] \
          .reset_index() \
          .drop('index',1)
          
    meo['Side_0_1_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str) + \
                                meo['ViewData.Side1_UniqueIds'].astype(str)
                                
    meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
    return(meo)
    
def cleaned_aua(fun_filepath_aua):
    aua = pd.read_csv(fun_filepath_aua) \
      .drop_duplicates() \
      .reset_index() \
      .drop('index',1) \
      .sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)

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
    
    
    if(fun_setup_code_crucial == '125' or fun_setup_code_crucial == '170' or fun_setup_code_crucial == '123' or fun_setup_code_crucial == '531' or fun_setup_code_crucial == '833' or fun_setup_code_crucial == '1200'):
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
        
    aua_side_0_1_UniqueIds_set = set(aua_df['ViewData.Side0_UniqueIds'].astype(str) + \
                                 aua_df['ViewData.Side1_UniqueIds'].astype(str))
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
        fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'] = list(set(\
            fun_aua_df[fun_aua_df['Side_0_1_UniqueIds'].isin(\
                     closed_breaks_captured_mode(fun_aua_df = fun_aua_df, \
                                        fun_transaction_type = fun_loop_transaction_type, \
                                        fun_captured_closed_breaks_set = set(fun_Side_0_1_UniqueIds_list), \
                                        fun_mode = mode_type))]\
                    ['ViewData.BreakID']))
    
        fun_total_number = len(\
                             fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'])
        
        fun_dict[fun_loop_transaction_type][mode_type + '_Total_Number'] = len(\
                             fun_dict[fun_loop_transaction_type][mode_type + '_BreakIDs_in_AUA'])
        
        if(fun_count != 0):
            
            fun_dict[fun_loop_transaction_type][mode_type + '_Percentage'] = fun_total_number/fun_count#\
#                                 fun_dict[fun_loop_transaction_type][mode_type + '_Total_Number']/fun_count
        
        else:
            fun_dict[fun_loop_transaction_type][mode_type + '_Percentage'] = fun_loop_transaction_type + ' not found in Closed breaks of AUA'
    return(fun_dict)

def closed_daily_run(fun_setup_code, fun_date, fun_main_filepath_meo, fun_main_filepath_aua):
    setup_val = fun_setup_code
    main_meo = cleaned_meo(fun_filepath_meo = fun_main_filepath_meo)
    
    BP_meo_training_df = Acct_MEO_combination_file(fun_side = 'PB', \
                                                   fun_cleaned_meo_df = main_meo)
    
    Acct_meo_training_df = Acct_MEO_combination_file(fun_side = 'Acct', \
                                                   fun_cleaned_meo_df = main_meo)

    main_aua = cleaned_aua(fun_filepath_aua = fun_main_filepath_aua)
    
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
    elif(fun_setup_code == '170'):
        Transaction_Type_dict = {
                                'Fees & Comm BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Fees & Comm'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Fees & Comm Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Fees & Comm'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Proceeds Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceeds Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Collateral BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Collateral'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Collateral Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Collateral'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Futures Collateral BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Futures Collateral'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Futures Collateral Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Futures Collateral'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Expenses BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Expenses Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Miscellaneous BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Miscellaneous'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Miscellaneous Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Miscellaneous'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Revenue BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Revenue Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Expiry BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Expiry'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Expiry Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Expiry'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Wires BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Wires'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Wires Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Wires'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }
    elif(fun_setup_code == '531'):
        Transaction_Type_dict = {
                                'Proceeds Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceeds Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Sell Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Buy Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Buy Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Proceeds Sell Adjustment BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceeds Sell Adjustment'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceeds Sell Adjustment Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceeds Sell Adjustment'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'DEP BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['DEP'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'DEP Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['DEP'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'WITH BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['WITH'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'WITH Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['WITH'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Expenses BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Expenses Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                }
    elif(fun_setup_code == '123'):
        Transaction_Type_dict = {
                                'Buy BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'RVLD BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['RVLD'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'RVLD Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['RVLD'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'RRVLD BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['RRVLD'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'RRVLD Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['RRVLD'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'JNL BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['JNL'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'JNL Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['JNL'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'RPLD BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['RPLD'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'RPLD Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['RPLD'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'RRPLD BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['RRPLD'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'RRPLD Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['RRPLD'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'ADJ BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['ADJ'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ADJ Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ADJ'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'ReverseRepo Close BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['ReverseRepo Close'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ReverseRepo Close Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ReverseRepo Close'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'RRPTM BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['RRPTM'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'RRPTM Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['RRPTM'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'RPTM BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['RPTM'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'RPTM Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['RPTM'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'ReverseRepo Open BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['ReverseRepo Open'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ReverseRepo Open Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ReverseRepo Open'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'CKP BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['CKP'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'CKP Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CKP'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'CKR BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['CKR'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'CKR Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CKR'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'CRI BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['CRI'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'CRI Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['CRI'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }
    elif(fun_setup_code == '833'):
        Transaction_Type_dict = {
                                'Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Exchange Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Exchange Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'VEC(Execution Fee) BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['VEC(Execution Fee)'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'VEC(Execution Fee) Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['VEC(Execution Fee)'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'NFA Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['NFA Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'NFA Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['NFA Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Clearing Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Clearing Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Clearing Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Clearing Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Proceeds Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceeds Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Realized P&L BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Realized P&L'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Realized P&L Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Realized P&L'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Sell Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Buy Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Buy Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Globex Fee BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Globex Fee'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Globex Fee Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Globex Fee'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Cash BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Cash'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Cash Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Cash'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Futures Collateral BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Futures Collateral'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Futures Collateral Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Futures Collateral'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Multiple BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Multiple'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Multiple Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Multiple'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Expenses BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Expenses Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Revenue BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Revenue Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Floor Brokerage BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Floor Brokerage'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Floor Brokerage Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Floor Brokerage'],
                                           'Side_meo_training_df' : Acct_meo_training_df}
                                                      
                                }
    elif(fun_setup_code == '1200'):
        Transaction_Type_dict = {
                                'Sell Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Sell Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Sell Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Sell Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Buy Commission BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Buy Commission'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Buy Commission Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Buy Commission'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Proceed Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceed Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceed Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceed Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Proceed Buy BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceed Buy'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceed Buy Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceed Buy'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Proceeds Sell BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Proceeds Sell Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Proceeds Sell'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Collateral Posted to ISDA Counterparties BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Collateral Posted to ISDA Counterparties'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Collateral Posted to ISDA Counterparties Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Collateral Posted to ISDA Counterparties'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Trf from SEC to SEG BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Trf from SEC to SEG'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Trf from SEC to SEG Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Trf from SEC to SEG'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Wire transferred BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Wire transferred'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Wire transferred Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Wire transferred'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Wire received BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Wire received'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Wire received Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Wire received'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Trf from SEG to SEC BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Trf from SEG to SEC'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Trf from SEG to SEC Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Trf from SEG to SEC'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Expenses BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Expenses Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Expenses'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Revenue BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Revenue Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Transfer to move PB balances BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer to move PB balances'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer to move PB balances Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer to move PB balances'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Transfer BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Trf within accounts BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Trf within accounts'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Trf within accounts Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Trf within accounts'],
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

        Side_0_1_UniqueIds_for_Transaction_type = identifying_closed_breaks_from_Trans_type(fun_side = Transaction_Type_dict.get(Transaction_type).get('side'), \
                                                                                  fun_transaction_type_list = Transaction_Type_dict.get(Transaction_type).get('Transaction_Type'), \
                                                                                  fun_side_meo_combination_df = Transaction_Type_dict.get(Transaction_type).get('Side_meo_training_df'),\
                                                                                  fun_setup_code_crucial = setup_val)

        count_closed_breaks_for_transaction_type = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB') & \
                    (main_aua['ViewData.Transaction Type'] == Transaction_type)]['Side_0_1_UniqueIds']))
        
        Transaction_Type_dict = update_dict_to_output_breakids_number_pct(fun_dict = Transaction_Type_dict, \
                                                                   fun_aua_df = main_aua, \
                                                                   fun_loop_transaction_type = Transaction_type, \
                                                                   fun_count = count_closed_breaks_for_transaction_type, \
                                                                   fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_for_Transaction_type)
            
        
        Side_0_1_UniqueIds_closed_all_list.extend(Side_0_1_UniqueIds_for_Transaction_type)
        print('\n' + Transaction_type + '\n')
        pprint.pprint(dictionary_exclude_keys(fun_dict = Transaction_Type_dict.get(Transaction_type),\
                                      fun_keys_to_exclude = {'side','Transaction_Type','Side_meo_training_df'}),\
                      width = 4)
    
    sys.stdout = orig_stdout
    f.close()
    
    count_all_closed_breaks = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB')]\
                                               ['Side_0_1_UniqueIds']))
    
    aua_closed_dict = {'All_Closed_Breaks' : {}}
    aua_closed_dict = update_dict_to_output_breakids_number_pct(fun_dict = aua_closed_dict, \
                                                                fun_aua_df = main_aua, \
                                                                fun_loop_transaction_type = 'All_Closed_Breaks', \
                                                                fun_count = count_all_closed_breaks, \
                                                                fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_closed_all_list)
    
    write_dict_at_top(fun_filename = filepath_stdout, \
                      fun_dict_to_add = aua_closed_dict)
    
    return(Side_0_1_UniqueIds_closed_all_list)
    
    

#date_numbers_list = [1,2,3,4,
#                     7,8,9,10,11,
#                     14,15,16,17,18,
#                     21,22,23,24,25,
#                     28,29,30] #for setups 125

date_numbers_list = [4,10,17,21] #for setups 531

#
#client = 'Soros'    
#
#setup = '153'
#
#filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections_' + client.upper() + '.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections_' + client.upper() + '.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]

client = 'Weiss'

setup = '531'

filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]


Side_0_1_UniqueIds_closed_all_dates_list = []

import timeit

i = 0
for i in range(0,len(date_numbers_list)):
    start = timeit.default_timer()

    Side_0_1_UniqueIds_closed_all_dates_list.append(
            closed_daily_run(fun_setup_code=setup,\
                             fun_date = i,\
                             fun_main_filepath_meo= filepaths_MEO[i],\
                             fun_main_filepath_aua = filepaths_AUA[i])
            )
    stop = timeit.default_timer()
    print('Date')
    print(date_numbers_list[i])
    print('Time: ', stop - start)