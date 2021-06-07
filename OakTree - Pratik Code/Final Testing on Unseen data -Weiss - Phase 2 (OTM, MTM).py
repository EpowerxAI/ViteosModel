#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE
import timeit
start = timeit.default_timer()




# In[2]:


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


# In[3]:


new_cols = ['ViewData.' + x for x in cols] + add


# In[4]:


#df_170.shape


# In[5]:


df_153_june_25_meo = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\JuneData\\MEO\\MeoCollections.MEO_HST_RecData_125_2020-06-8.csv')

df_153_june_25_meo = df_153_june_25_meo.drop_duplicates()
df_153_june_25_meo = df_153_june_25_meo.reset_index()
df_153_june_25_meo = df_153_june_25_meo.drop('index',1)


# In[396]:


df_153_june_25_meo[df_153_june_25_meo['ViewData.Status']=='SMR'][['ViewData.Keys','ViewData.Status', 'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]['ViewData.Keys'].value_counts()


# In[402]:


df_153_june_25_meo[df_153_june_25_meo['ViewData.Status']=='SMR'][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Keys']]['ViewData.Keys'].values[4]


# ## Close Prediction Weiss

# In[6]:


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
    meo = pd.read_csv(fun_filepath_meo)           .drop_duplicates()           .reset_index()           .drop('index',1)
    
    meo = normalize_bp_acct_col_names(fun_df = meo)
    
    meo = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])] 
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
        Transaction_type_closed_break_df =             fun_side_meo_combination_df[                     (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) &                     (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) &                     (abs(fun_side_meo_combination_df[Net_amount_col_name_list[0]]).astype(str) == abs(fun_side_meo_combination_df[Net_amount_col_name_list[1]]).astype(str)) &                     (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str))                                         ]
        Transaction_type_closed_break_df_2 =             fun_side_meo_combination_df[                     (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) &                     (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) &                     (fun_side_meo_combination_df['ViewData.Transaction ID_x'].astype(str) == fun_side_meo_combination_df['ViewData.Transaction ID_y'].astype(str)) &                     (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str))                                         ]
        Transaction_type_closed_break_df = Transaction_type_closed_break_df.append(Transaction_type_closed_break_df_2)

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

def closed_daily_run(fun_setup_code, fun_date, fun_main_filepath_meo, fun_main_filepath_aua):
    setup_val = fun_setup_code
    main_meo = cleaned_meo(fun_filepath_meo = fun_main_filepath_meo)
    
    BP_meo_training_df = Acct_MEO_combination_file(fun_side = 'PB',                                                    fun_cleaned_meo_df = main_meo)
    
    Acct_meo_training_df = Acct_MEO_combination_file(fun_side = 'Acct',                                                    fun_cleaned_meo_df = main_meo)

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

        count_closed_breaks_for_transaction_type = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB') &                     (main_aua['ViewData.Transaction Type'] == Transaction_type)]['Side_0_1_UniqueIds']))
        
        Transaction_Type_dict = update_dict_to_output_breakids_number_pct(fun_dict = Transaction_Type_dict,                                                                    fun_aua_df = main_aua,                                                                    fun_loop_transaction_type = Transaction_type,                                                                    fun_count = count_closed_breaks_for_transaction_type,                                                                    fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_for_Transaction_type)
            
        
        Side_0_1_UniqueIds_closed_all_list.extend(Side_0_1_UniqueIds_for_Transaction_type)
        print('\n' + Transaction_type + '\n')
        pprint.pprint(dictionary_exclude_keys(fun_dict = Transaction_Type_dict.get(Transaction_type),                                      fun_keys_to_exclude = {'side','Transaction_Type','Side_meo_training_df'}),                      width = 4)
    
    sys.stdout = orig_stdout
    f.close()
    
    count_all_closed_breaks = len(set(main_aua[(main_aua['ViewData.Status'] == 'UCB')]                                               ['Side_0_1_UniqueIds']))
    
    aua_closed_dict = {'All_Closed_Breaks' : {}}
    aua_closed_dict = update_dict_to_output_breakids_number_pct(fun_dict = aua_closed_dict,                                                                 fun_aua_df = main_aua,                                                                 fun_loop_transaction_type = 'All_Closed_Breaks',                                                                 fun_count = count_all_closed_breaks,                                                                 fun_Side_0_1_UniqueIds_list = Side_0_1_UniqueIds_closed_all_list)
    
    write_dict_at_top(fun_filename = filepath_stdout,                       fun_dict_to_add = aua_closed_dict)
    
    return(Side_0_1_UniqueIds_closed_all_list)
    
    

date_numbers_list = [28]
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

filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]


Side_0_1_UniqueIds_closed_all_dates_list = []

i = 0
for i in range(0,len(date_numbers_list)):

    Side_0_1_UniqueIds_closed_all_dates_list.append(
            closed_daily_run(fun_setup_code=setup,\
                             fun_date = i,\
                             fun_main_filepath_meo= filepaths_MEO[i],\
                             fun_main_filepath_aua = filepaths_AUA[i])
            )


# In[7]:


len(Side_0_1_UniqueIds_closed_all_dates_list[0])


# In[8]:


new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list[0]]


# In[9]:


len(new_closed_keys)


# ## Read testing data 

# In[10]:


#MeoCollections.MEO_HST_RecData_379_2020-06-18
meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_125_2020-06-28.csv",usecols=new_cols)


# In[11]:


#df['ViewData.Task Business Date']
meo.shape


# In[12]:


meo['ViewData.Status'].value_counts()


# In[13]:


df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)


# In[14]:


#df1[(df1['Date']=='2020-04-10') & (df1['ViewData.Side1_UniqueIds']=='996_125813417_Goldman Sachs')]
df1.shape


# In[15]:


#df1['close_key'] = df1['ViewData.Side0_UniqueIds'].astype(str) + df1['ViewData.Side1_UniqueIds'].astype(str)


# In[16]:



## Output for Closed breaks

#closed_df = df1[(df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys))]


# In[17]:


#closed_df.shape


# ## Machine generated output

# In[18]:


#df2 = df1[~df1['close_key'].isin(list(all_closed))]
#df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]


# In[19]:


df = df1.copy()


# In[20]:


df = df.reset_index()
df = df.drop('index',1)


# In[21]:


df.shape


# In[22]:


#pd.set_option('display.max_columns', 500)


# In[23]:


df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[24]:


#df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[25]:


df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[26]:


pd.to_datetime(df['Date'])


# In[27]:


df['Date'] = pd.to_datetime(df['Date']).dt.date


# In[28]:


df['Date'] = df['Date'].astype(str)


# In[29]:


#df['ViewData.Status'].value_counts()


# In[30]:


df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB','SPM'])]
df = df.reset_index()
df = df.drop('index',1)


# In[31]:


#df1[df1['ViewData.Status']=='SMB']
df['ViewData.Status'].value_counts()


# In[32]:


df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[33]:


#df_170[(df_170['ViewData.Status']=='UMR')]


# In[34]:


df['Date'].value_counts()


# ## Sample data on one date

# In[35]:


#df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})


# In[36]:


sample = df[df['Date'] =='2020-06-29']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[37]:


smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
smb = smb.drop('index',1)


# In[38]:


smb_pb = smb.copy()
smb_acc = smb.copy()


# In[39]:


smb_pb['ViewData.Accounting Net Amount'] = np.nan
smb_pb['ViewData.Side0_UniqueIds'] = np.nan
smb_pb['ViewData.Status'] ='SMB-OB'

smb_acc['ViewData.B-P Net Amount'] = np.nan
smb_acc['ViewData.Side1_UniqueIds'] = np.nan
smb_acc['ViewData.Status'] ='SMB-OB'


# In[40]:


sample = sample[sample['ViewData.Status']!='SMB']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[41]:


sample.shape


# In[42]:


sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[43]:


#sample['ViewData.Status'].value_counts()


# In[44]:


sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


# In[45]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0


# In[46]:


#sample['ViewData.Status'].value_counts()


# In[47]:


#sample['flag_side1'].value_counts()


# In[48]:


sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'

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


# In[49]:


sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)


# In[50]:


sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[51]:


sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[52]:


#sample1['ViewData.Status'].value_counts()


# In[53]:


aa = sample1[sample1['Trans_side']=='A_side']
bb = sample1[sample1['Trans_side']=='B_side']


# In[54]:


#bb['ViewData.Source Combination'].value_counts()


# In[55]:


aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)

bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)


# In[56]:


aa = aa.reset_index()
aa = aa.drop('index', 1)
bb = bb.reset_index()
bb = bb.drop('index', 1)


# In[57]:


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


# In[58]:


bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
bb = bb.reset_index()
bb = bb.drop('index',1)


# In[59]:


bb['ViewData.Status'].value_counts()


# In[60]:


bb.shape


# ## Many to Many for Equity Swaps

# In[374]:


cc = pd.concat([aa, bb], axis=0)

cc = cc.reset_index().drop('index',1)


# In[375]:


cc['ViewData.Transaction Type'] = cc['ViewData.Transaction Type'].astype(str)
cc['ViewData.Settle Date'] = pd.to_datetime(cc['ViewData.Settle Date'])
cc['filter_key_with_sd'] = cc['filter_key'].astype(str) + cc['ViewData.Settle Date'].astype(str)


# In[376]:


aa.shape


# In[377]:


cc.shape


# In[378]:


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

cc['Transfer_flag'] = cc.apply(lambda x: transfer_desc(x['ViewData.Transaction Type'],x['ViewData.Description']), axis=1)


# In[379]:


def eq_swap_tt_flag(tt):
    tt = tt.lower()
    if any(key in tt for key in ['equity swap','swap unwind','eq swap','transfer']):
        tt_flag = 1
    else:
        tt_flag = 0
    return tt_flag

cc['Equity_Swap_flag'] = cc.apply(lambda x: eq_swap_tt_flag(x['ViewData.Transaction Type']), axis=1)


# In[380]:


cc2 = cc[(cc['Equity_Swap_flag']==1)|(cc['Transfer_flag']==1)]
cc2 = cc2.reset_index().drop('index',1)
cc2['ViewData.Settle Date'] = pd.to_datetime(cc2['ViewData.Settle Date'])
cc2['filter_key_with_sd'] = cc2['filter_key'].astype(str) + cc2['ViewData.Settle Date'].astype(str)


# In[381]:


filter_key_umt_umb = []
diff_in_amount = []
diff_in_amount_key = []
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


# In[382]:


filter_key_umt_umb


# ### Difference in amount for Swap settlement Dataframe

# In[383]:


diff_in_amount_df = pd.DataFrame(diff_in_amount_key)
diff_in_amount_df.columns = ['filter_key_with_sd']
diff_in_amount_df['diff_in_amount'] = diff_in_amount

def eq_swap_comment(filter_key,difference):
    comment = "Difference of " + str(difference) + " in swap settlement of " + filter_key[-5:]
    return comment

diff_in_amount_df['comment'] = diff_in_amount_df.apply(lambda x:eq_swap_comment(x['filter_key_with_sd'], x['diff_in_amount']),axis=1)


# In[384]:


cc3 = pd.merge(cc2,diff_in_amount_df,on='filter_key_with_sd', how='left')

cc4 = cc3[~cc3['comment'].isnull()]
cc4 = cc4.reset_index().drop('index',1)


# In[385]:


comment_table_eq_swap = cc4[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','comment']]


# In[386]:


comment_table_eq_swap


# ### Remove IDs from eq swap

# In[392]:


## cc4 goes directly into the comments engine #############

cc5 = cc2[~cc2['filter_key_with_sd'].isin(cc4['filter_key_with_sd'].unique())]
cc5 = cc5.reset_index().drop('index',1)


# In[393]:


cc5.shape


# In[394]:


## Equity Swap Many to many

eq_mtm_1_ids = []
eq_mtm_0_ids = []

for key in filter_key_umt_umb:
    one_side = cc5[cc5['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
    zero_side = cc5[cc5['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
    one_side = [i for i in one_side if i!='nan']
    zero_side = [i for i in zero_side if i!='nan']
    eq_mtm_1_ids.append(one_side)
    eq_mtm_0_ids.append(zero_side)


# In[395]:


if eq_mtm_1_ids !=[]:
    mtm_list_1 = list(np.concatenate(eq_mtm_1_ids))
else:
    mtm_list_1 = []

if eq_mtm_0_ids !=[]:
    mtm_list_0 = list(np.concatenate(eq_mtm_0_ids))
else:
    mtm_list_0 = []


# In[396]:


## Data Frame for MTM from equity Swap

mtm_df_eqs = pd.DataFrame(np.arange(len(eq_mtm_0_ids)))
mtm_df_eqs.columns = ['index']

mtm_df_eqs['ViewData.Side0_UniqueIds'] = eq_mtm_0_ids
mtm_df_eqs['ViewData.Side1_UniqueIds'] = eq_mtm_1_ids
mtm_df_eqs = mtm_df_eqs.drop('index',1)

mtm_df_eqs


# In[411]:


comment_one_side = []
for i in comment_table_eq_swap['ViewData.Side1_UniqueIds'].unique():
    if i !='nan':
        comment_one_side.append(i)
        
comment_zero_side = []
for i in comment_table_eq_swap['ViewData.Side0_UniqueIds'].unique():
    if i !='nan':
        comment_zero_side.append(i)


# In[414]:


## IDs left after removing Equity Swap MTM and Comment of Difference in amount

cc6 = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1)))]

#cc6 = cc6['ViewData.Side0_UniqueIds'].isin(comment_table_eq_swap['ViewData.Side0_UniqueIds'].unique())| cc['ViewData.Side1_UniqueIds'].isin(comment_table_eq_swap['ViewData.Side1_UniqueIds'].unique())

cc6 = cc6[~((cc6['ViewData.Side1_UniqueIds'].isin(comment_one_side)) | (cc6['ViewData.Side0_UniqueIds'].isin(comment_zero_side)))]


# In[398]:


## IDs left after removing Equity Swap MTM and Comment of Difference in amount

#cc6 = cc5[~(cc5['ViewData.Side0_UniqueIds'].isin(mtm_list_0) |cc5['ViewData.Side1_UniqueIds'].isin(mtm_list_1))]
#cc6 = cc6.reset_index().drop('index',1)


# In[415]:


cc6.shape


# In[416]:


cc6['Trans_side'].value_counts()


# In[417]:


cc6['Trans_side'].value_counts()


# ## New and final Close Break IDs Table

# In[418]:


new_close = []
for i in new_closed_keys:
    new_close.append(i.split(','))


# In[419]:


new_close_flat = np.concatenate(new_close)


# In[420]:


closed_final_df = cc6[cc6['ViewData.Side0_UniqueIds'].isin(new_close_flat) | cc6['ViewData.Side1_UniqueIds'].isin(new_close_flat)]


# In[421]:


closed_final_df = closed_final_df[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]


# In[422]:


closed_final_df.shape


# ## Removing Close Break IDs

# In[423]:


cc7 = cc6[~(cc6['ViewData.Side0_UniqueIds'].isin(new_close_flat) | cc6['ViewData.Side1_UniqueIds'].isin(new_close_flat))]
cc7 = cc7.reset_index().drop('index',1)


# ## M*N Loop starts

# In[424]:


aa_new = cc7[cc7['Trans_side']=='A_side']
bb_new = cc7[cc7['Trans_side']=='B_side']


# In[425]:


aa_new = aa_new.reset_index().drop('index',1)
bb_new = bb_new.reset_index().drop('index',1)


# In[426]:


aa_new.shape


# In[427]:


bb_new.shape


# In[428]:


###################### loop m*n ###############################
from pandas import merge
from tqdm import tqdm

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
            no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
            no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
            

no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[429]:


#no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[430]:


#pd.DataFrame(no_pair_ids).rename


# In[431]:


len(no_pair_ids)


# In[432]:


#test_file['SideA.ViewData.Status'].value_counts()


# In[433]:


test_file = pd.concat(training_df)


# In[434]:


test_file = test_file.reset_index()
test_file = test_file.drop('index',1)


# In[435]:


test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')


# In[436]:


test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]


# In[437]:


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


# In[438]:


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


# In[439]:


#test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)


# In[440]:


#test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)


# In[441]:


test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']


# In[442]:


test_file.shape


## ## Description code
#
## In[475]:
#
#
#import os
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

# In[493]:


test_file['new_pb'] = test_file['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[494]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[495]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[496]:


test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].fillna('kkk')


# In[497]:


test_file['new_pb1'] = test_file.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)


# In[838]:


test_file['new_pb1'].unique()


# In[498]:


#test_file = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/X_test_files_after_loop/meo_testing_HST_RecData_379_06_19_2020_test_file_with_ID.csv')


# In[499]:


#test_file = test_file.drop('Unnamed: 0',1)


# In[500]:


test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days

test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days


# In[501]:


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


# In[502]:



############ Fund match new ########

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

vec_fund_match = np.vectorize(fundmatch)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)


# In[503]:


### New code for cleaning text variables 

import pandas as pd
import dask.dataframe as dd
import glob
import math
from sklearn.feature_extraction.text import TfidfVectorizer
from dateutil.parser import parse
import operator
import itertools
from sklearn.feature_extraction.text import CountVectorizer
import re
import os
import numpy as np


#column_names = ['SideA.ViewData.Transaction Type', 'ViewData.Investment Type', 'ViewData.Asset Type Category', 'ViewData.Prime Broker', 'ViewData.Description']

trans_type_A_side = test_file['SideA.ViewData.Transaction Type']
trans_type_B_side = test_file['SideB.ViewData.Transaction Type']

asset_type_cat_A_side = test_file['SideA.ViewData.Asset Type Category']
asset_type_cat_B_side = test_file['SideB.ViewData.Asset Type Category']

invest_type_A_side = test_file['SideA.ViewData.Investment Type']
invest_type_B_side = test_file['SideB.ViewData.Investment Type']

prime_broker_A_side = test_file['SideA.ViewData.Prime Broker']
prime_broker_B_side = test_file['SideB.ViewData.Prime Broker']


# In[504]:


# LOWER CASE
trans_type_A_side = [str(item).lower() for item in trans_type_A_side]
trans_type_B_side = [str(item).lower() for item in trans_type_B_side]

asset_type_cat_A_side = [str(item).lower() for item in asset_type_cat_A_side]
asset_type_cat_B_side = [str(item).lower() for item in asset_type_cat_B_side]

invest_type_A_side = [str(item).lower() for item in invest_type_A_side]
invest_type_B_side = [str(item).lower() for item in invest_type_B_side]

prime_broker_A_side = [str(item).lower() for item in prime_broker_A_side]
prime_broker_B_side = [str(item).lower() for item in prime_broker_B_side]


# In[505]:


split_trans_A_side = [item.split() for item in trans_type_A_side]
split_trans_B_side = [item.split() for item in trans_type_B_side]


split_asset_A_side = [item.split() for item in asset_type_cat_A_side]
split_asset_B_side = [item.split() for item in asset_type_cat_B_side]


split_invest_A_side = [item.split() for item in invest_type_A_side]
split_invest_B_side = [item.split() for item in invest_type_B_side]

split_prime_A_side = [item.split() for item in prime_broker_A_side]
split_prime_b_side = [item.split() for item in prime_broker_B_side]

# In[310]:


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


# In[506]:


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


# In[319]:

cleaned_trans_types_A_side = [' '.join(item) for item in remove_ats_A_side]
cleaned_trans_types_B_side = [' '.join(item) for item in remove_ats_B_side]


# In[507]:


# # INVESTMENT TYPE

# # In[322]:


remove_nums_i_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_A_side]
remove_nums_i_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_invest_B_side]

remove_dates_i_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_A_side]
remove_dates_i_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_i_B_side]

#remove_blanks_i_A_side = [item for item in remove_dates_i_A_side if item]
#remove_blanks_i_B_side = [item for item in remove_dates_i_B_side if item]
#remove_blanks_i[:10]


# # In[323]:

cleaned_invest_A_side = [' '.join(item) for item in remove_dates_i_A_side]
cleaned_invest_B_side = [' '.join(item) for item in remove_dates_i_B_side]


# In[508]:



remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]
# remove_blanks_a = [item for item in remove_dates_a if item]
# # remove_blanks_a[:10]


# # In[321]:

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]


# In[509]:


test_file['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
test_file['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

test_file['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
test_file['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

test_file['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
test_file['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side


# In[510]:


#test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[511]:


#test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[512]:


##############

values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values

vec_tt_match = np.vectorize(mhreplaced)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)


# In[513]:


test_file.loc[test_file['SideA.ViewData.Transaction Type']=='int','SideA.ViewData.Transaction Type'] = 'interest'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='wires','SideA.ViewData.Transaction Type'] = 'wire'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='dividends','SideA.ViewData.Transaction Type'] = 'dividend'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='miscellaneous','SideA.ViewData.Transaction Type'] = 'misc'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='div','SideA.ViewData.Transaction Type'] = 'dividend'


# In[514]:


test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[515]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[516]:


test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)


# In[517]:


#train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)


# In[518]:


test_file['Combined_Investment_Type'] = test_file['SideA.ViewData.Investment Type'].astype(str) + test_file['SideB.ViewData.Investment Type'].astype(str)


# In[519]:


test_file['Combined_Asset_Type_Category'] = test_file['SideA.ViewData.Asset Category Type'].astype(str) + test_file['SideB.ViewData.Asset Category Type'].astype(str)


# In[520]:


def nan_fun(x):
    if x=='nan':
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


# In[521]:


len(test_file['SideB.ViewData.CUSIP'].values)


# In[522]:


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


# In[523]:


#test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
#test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[524]:


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


# In[525]:


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


# In[526]:


test_file['amount_percent'] = (test_file['SideA.ViewData.B-P Net Amount']/test_file['SideB.ViewData.Accounting Net Amount']*100)


# In[527]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())


# In[528]:


test_file['SideB.ViewData.Prime Broker'] = test_file['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())


# In[529]:


test_file['SideB.ViewData.Asset Type Category'] = test_file['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Asset Type Category'] = test_file['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())


# In[ ]:





# In[530]:


#test_file


# In[531]:



#test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)


# In[532]:


#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/X_test_files_after_loop/meo_testing_HST_RecData_170_06-18-2020_test_file.csv")


# In[ ]:





# In[533]:


trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']


# In[534]:


test_file['SideA.TType'] = test_file.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)
test_file['SideB.TType'] = test_file.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)


# In[535]:


#test_file['Combined_Desc'] = test_file['SideA.new_desc_cat'] + test_file['SideB.new_desc_cat']


# In[536]:


test_file['Combined_TType'] = test_file['SideA.TType'].astype(str) + test_file['SideB.TType'].astype(str)


# In[537]:


from fuzzywuzzy import fuzz


# In[538]:


import re
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


# In[539]:


test_file =  clean_text(test_file,'SideA.ViewData.Description', 'SideA.ViewData.Description_new') 
test_file =  clean_text(test_file,'SideB.ViewData.Description', 'SideB.ViewData.Description_new') 


# In[839]:


values_desc_new_A_Side = test_file['SideA.ViewData.Description_new'].values
values_desc_new_B_Side = test_file['SideB.ViewData.Description_new'].values

vec_desc_simi = np.vectorize(fuzz.token_sort_ratio)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['description_similarity_score'] = vec_desc_simi(values_desc_new_A_Side,values_desc_new_B_Side)


# In[540]:


#test_file['description_similarity_score'] = test_file.apply(lambda x: fuzz.token_sort_ratio(x['SideA.ViewData.Description_new'], x['SideB.ViewData.Description_new']), axis=1)


# In[541]:


import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
import numpy as np

#le = LabelEncoder()
for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date','SideA.ViewData.Trade Date','SideB.ViewData.Trade Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    test_file[feature] = pd.to_datetime(test_file[feature],errors = 'coerce').dt.weekday


# In[542]:


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

# In[543]:



## TODO Import HIstorical UMR FILE for Transaction Type mapping
os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')

Weiss_umr = pd.read_csv('Weiss_UMR.csv')


# In[544]:


#soros_umr['ViewData.Combined Transaction Type'].unique()


# In[545]:


test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in Weiss_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)


# In[546]:


test_file['abs_amount_flag'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)


# In[547]:


test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]

test_file = test_file.reset_index().drop('index',1)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int)


# In[548]:


test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))


# In[ ]:





# In[549]:


test_file['SideA.ViewData.SEDOL'] = test_file['SideA.ViewData.SEDOL'].astype(str) 
test_file['SideB.ViewData.SEDOL'] = test_file['SideB.ViewData.SEDOL'].astype(str) 

def sedol_match(text1,text2):
    if text1 !='nan' and text2!='nan' and (text1 in text2 or text2 in text1):
        return 1
    elif text1 !='nan' and text2!='nan' and (text1 not in text2 or text2 not in text1):
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


# In[550]:


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


# In[ ]:


values_desc_new_A_Side = test_file['SideA.ViewData.Description_new'].values
values_desc_new_B_Side = test_file['SideB.ViewData.Description_new'].values

vec_desc_any_string_check = np.vectorize(desc_any_string_check)


# In[ ]:


test_file['desc_any_match'] = vec_desc_any_string_check(values_desc_new_A_Side,values_desc_new_B_Side)


# In[841]:


#test_file['desc_any_match'] = test_file.apply(lambda x: desc_any_string_check(x['SideA.ViewData.Description_new'],x['SideB.ViewData.Description_new']), axis=1)


# In[552]:


test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))


# ## Transaction Type New code

# In[553]:


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


# In[ ]:





# In[554]:


#test_file['ViewData.Combined Transaction Type'].nunique()


# In[555]:


test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(str)
test_file = test_file[test_file['SideA.ViewData.Trade Date'] !='nan']
test_file = test_file.reset_index().drop('index',1)


# In[556]:


test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(float).astype(int)


# In[557]:


test_file['TD_bucket'] = test_file['Trade_date_diff'].apply(lambda x: 0 if x==0 else(1 if x>=-2 and x<=2 else 2))
test_file['SD_bucket'] = test_file['Settle_date_diff'].apply(lambda x: 0 if x==0 else(1 if x>=-2 and x<=2 else 2))


# In[571]:


test_file3 = test_file[~(test_file['SideA.ViewData.Side1_UniqueIds'].isin(new_closed_keys) | test_file['SideB.ViewData.Side0_UniqueIds'].isin(new_closed_keys))]
test_file3 = test_file3.reset_index()
test_file3 = test_file3.drop('index',1)


# In[572]:


test_file3.shape


# In[560]:


#test_file2 = test_file[((test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")) | ((test_file['SideA.TType']!="Trade") & (test_file['SideB.TType']!="Trade")) ]


# In[561]:


#test_file2 = test_file[(test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")]


# In[204]:


#test_file[(test_file['SideA.TType']==test_file['SideB.TType'])]['SideB.TType']


# In[205]:


#test_file2 = test_file2.reset_index()
#test_file2 = test_file2.drop('index',1)


# In[568]:


test_file3.shape


# In[573]:


#test_file['SideA.ViewData.BreakID_A_side'].value_counts()
#test_file[model_cols]

test_file3 = test_file3[(test_file3['SideA.ViewData.Status'] !='SPM')]
test_file3 = test_file3[(test_file3['SideB.ViewData.Status'] !='SPM')]


# In[574]:


test_file3 = test_file3.reset_index()
test_file3 = test_file3.drop('index',1)
test_file3.shape


# ## Test file served into the model

# In[575]:


test_file2 = test_file3.copy()


# In[576]:


X_test = test_file2[model_cols]


# In[577]:


X_test = X_test.reset_index()
X_test = X_test.drop('index',1)
X_test = X_test.fillna(0)


# In[578]:


X_test = X_test.fillna(0)


# In[579]:


X_test.shape


# In[580]:


X_test = X_test.drop_duplicates()
X_test = X_test.reset_index()
X_test = X_test.drop('index',1)


# In[581]:


X_test.shape


# ## Model Pickle file import

# In[582]:



## TODO Import Pickle file for 1st Model

import pickle


# In[583]:


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


# In[584]:


X_test


# ## Predictions

# In[585]:


# Actual class predictions
rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[586]:


probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[587]:


X_test['Predicted_action'] = rf_predictions
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test['probability_No_pair'] = probability_class_0
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test['probability_UMB'] = probability_class_1
X_test['probability_UMR'] = probability_class_2
X_test['probability_UMT'] = probability_class_3


# In[588]:


X_test['Predicted_action'].value_counts()


# In[589]:


X_test['Predicted_action'].value_counts()


# ## Two Step Modeling

# In[590]:


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


# In[591]:


X_test2 = test_file3[model_cols_2]


# In[592]:


X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)
X_test2 = X_test2.fillna(0)


# In[593]:


X_test2.shape


# In[594]:


X_test2 = X_test2.drop_duplicates()
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)


# In[595]:


X_test2.shape


# In[596]:


os.chdir('D:\\ViteosModel\\OakTree - Pratik Code')


# In[597]:


#filename2 = 'Soros_full_model_all_two_step.sav'

## TODO Import MOdel2 as per the two step modelling process

#filename2 = 'OakTree_final_model2_step_two.sav'
#filename2 = 'Weiss_final_model2_two_step.sav'
filename2 = 'Weiss_final_model2_with_umt_two_step.sav'
clf2 = pickle.load(open(filename2, 'rb'))


# In[598]:


# Actual class predictions
rf_predictions2 = clf2.predict(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs2 = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[599]:


probability_class_0_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

#probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[600]:


X_test2['Predicted_action_2'] = rf_predictions2
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test2['probability_No_pair_2'] = probability_class_0_two
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test2['probability_UMB_2'] = probability_class_1_two
#X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[601]:


X_test2['Predicted_action_2'].value_counts()


# In[602]:


X_test.shape


# In[603]:


X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)


# In[604]:


X_test.loc[(X_test['Amount_diff_2']!=0) & (X_test['Predicted_action']=='UMR_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'


# In[605]:


X_test.loc[(X_test['Amount_diff_2']==0) & (X_test['Predicted_action']=='UMB_One_to_One'), 'Predicted_action'] = 'UMR_One_to_One'


# In[606]:


X_test.loc[((X_test['Amount_diff_2']>1)  | (X_test['Amount_diff_2']<-1)) & (X_test['Predicted_action']=='UMT_One_to_One'), 'Predicted_action'] = 'UMB_One_to_One'


# ## Absolute amount flag

# In[609]:


abs_amount_table = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['abs_amount_flag'].max().reset_index()


# In[611]:


abs_amount_table[abs_amount_table['abs_amount_flag']==1]['SideB.ViewData.Side0_UniqueIds']


# In[618]:


duplicate_entries = abs_amount_table[abs_amount_table['abs_amount_flag']==1]['SideB.ViewData.Side0_UniqueIds'].unique()


# ## Removing duplicate entries 

# In[619]:


if len(duplicate_entries) !=0:
    X_test = X_test[~X_test['SideB.ViewData.Side0_UniqueIds'].isin(duplicate_entries)]


# In[620]:


X_test = X_test.reset_index().drop('index',1)


# ## New Aggregation

# In[622]:


X_test['Tolerance_level'] = np.abs(X_test['probability_UMB_2'] - X_test['probability_No_pair_2'])


# In[623]:


#X_test[X_test['Tolerance_level']<0.1]['Predicted_action'].value_counts()


# In[624]:


b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[625]:


#X_test[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Amount_diff_2']!=0)]


# ## UMR segregation

# In[626]:


def umr_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']<=3) & (b_count2['len']<=3)]
    return umr_table['SideB.ViewData.Side0_UniqueIds'].values
    


# In[627]:


def umt_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umt_table = b_count2[(b_count2['Predicted_action']=='UMT_One_to_One')  & (b_count2['count']==1) & (b_count2['len']<=3)]
    return umt_table['SideB.ViewData.Side0_UniqueIds'].values


# In[628]:


#umt_seg(X_test)

X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_seg(X_test)) & (X_test['Predicted_action']=='UMT_One_to_One')].shape


# In[629]:


umr_ids_0 = umr_seg(X_test)
umt_ids_0 = umt_seg(X_test)


# ## 1st Prediction Table for One to One UMR

# In[630]:


final_umr_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')]


# In[631]:


final_umr_table = final_umr_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]


# In[632]:


final_umr_table.shape


# ## Prediction table for One to One UMT

# In[633]:


final_umt_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umt_ids_0) & (X_test['Predicted_action']=='UMT_One_to_One')]


# In[634]:


final_umt_table = final_umt_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR','probability_UMT']]


# In[635]:


final_umt_table.shape


# ## No-Pair segregation

# In[636]:


#b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
#a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[637]:


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


# In[638]:


no_pair_ids_b_side, no_pair_ids_a_side = no_pair_seg(X_test)


# In[639]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]['Predicted_action_2'].value_counts()


# In[640]:


X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[641]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]['Predicted_action_2'].value_counts()


# In[642]:


final_open_table = X_test[(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]


# In[643]:


final_open_table = final_open_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[644]:


final_open_table['probability_UMR'] = 0.00010
final_open_table = final_open_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[645]:


final_open_table.shape


# In[646]:


#final_open_table.head()

len(no_pair_ids_b_side)


# In[647]:


b_side_open_table = final_open_table.groupby('SideB.ViewData.Side0_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()
a_side_open_table = final_open_table.groupby('SideA.ViewData.Side1_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()


# In[648]:


a_side_open_table = a_side_open_table[a_side_open_table['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]
b_side_open_table = b_side_open_table[b_side_open_table['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)]


# In[649]:


b_side_open_table = b_side_open_table.reset_index().drop('index',1)
a_side_open_table = a_side_open_table.reset_index().drop('index',1)


# In[650]:


final_no_pair_table = pd.concat([a_side_open_table,b_side_open_table], axis=0)
final_no_pair_table = final_no_pair_table.reset_index().drop('index',1)


# In[651]:


final_open_table


# In[652]:


final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.BreakID_A_side']].drop_duplicates(), on = 'SideA.ViewData.Side1_UniqueIds', how='left')
final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideB.ViewData.Side0_UniqueIds','SideB.ViewData.BreakID_B_side']].drop_duplicates(), on = 'SideB.ViewData.Side0_UniqueIds', how='left')


# In[653]:


final_no_pair_table


# In[654]:


#actual_closed = pd.read_csv('D:\Raman  Strategy ML 2.0\All_Data\OakTree\JuneData\Final_Predictions_379\Final_Predictions_Table_HST_RecData_379_2020-06-14.csv')


# In[655]:


#actual_closed_array = np.array(list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side0_UniqueIds'].unique()) + list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side1_UniqueIds'].unique()))


# In[656]:


#X_test_umb3 = X_test_umb[~((X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(actual_closed_array)) | (X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(actual_closed_array)))]


# ## Remove Open Ids

# In[657]:


umr_ids_a_side = final_umr_table['SideA.ViewData.Side1_UniqueIds'].unique()
umr_ids_b_side = final_umr_table['SideB.ViewData.Side0_UniqueIds'].unique()

umt_ids_a_side = final_umt_table['SideA.ViewData.Side1_UniqueIds'].unique()
umt_ids_b_side = final_umt_table['SideB.ViewData.Side0_UniqueIds'].unique()

### Remove Open IDs

X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

## Remove UMR IDs

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]

## Remove UMT IDs

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umt_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umt_ids_b_side))]


X_test_left = X_test_left.reset_index().drop('index',1)


# In[658]:


X_test.shape


# In[659]:


X_test_left.shape


# In[660]:


X_test_left['Predicted_action_2'].value_counts()


# ## New MTM with SD

# In[661]:


trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']


# In[708]:


cc_new = cc7[cc7['ViewData.Status']!='SPM']
cc_new = cc_new.reset_index().drop('index',1)


# In[709]:


cc_new = cc_new[~((cc_new['ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds'])) | (cc_new['ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]


# In[710]:


cc_new = cc_new[~((cc_new['ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) | (cc_new['ViewData.Side1_UniqueIds'].isin(final_umt_table['SideA.ViewData.Side1_UniqueIds'])))]


# In[711]:


cc_new = cc_new.reset_index().drop('index',1)


# In[712]:


cc_new.shape


# In[716]:


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


# In[717]:


## Equity Swap Many to many

sd_mtm_1_ids = []
sd_mtm_0_ids = []

for key in filter_key_umt_umb_sd:
    one_side = cc_new[cc_new['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
    zero_side = cc_new[cc_new['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
    one_side = [i for i in one_side if i!='nan']
    zero_side = [i for i in zero_side if i!='nan']
    sd_mtm_1_ids.append(one_side)
    sd_mtm_0_ids.append(zero_side)


# In[718]:


if sd_mtm_1_ids !=[]:
    sd_mtm_list_1 = list(np.concatenate(sd_mtm_1_ids))
else:
    sd_mtm_list_1 = []

if sd_mtm_0_ids !=[]:
    sd_mtm_list_0 = list(np.concatenate(sd_mtm_0_ids))
else:
    sd_mtm_list_0 = []


# In[719]:


sd_mtm_list_0


# In[720]:


## Data Frame for MTM from equity Swap

mtm_df_sd = pd.DataFrame(np.arange(len(sd_mtm_0_ids)))
mtm_df_sd.columns = ['index']

mtm_df_sd['ViewData.Side0_UniqueIds'] = sd_mtm_0_ids
mtm_df_sd['ViewData.Side1_UniqueIds'] = sd_mtm_1_ids
mtm_df_sd = mtm_df_sd.drop('index',1)

mtm_df_sd


# ## Remove Ids from MTM SD match

# In[731]:


X_test_left.shape


# In[732]:


X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(sd_mtm_list_1))]

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(sd_mtm_list_0))]


# In[733]:


X_test_left.shape


# ## One to One UMB segregation

# In[734]:


X_test_left['Predicted_action_2'].value_counts()


# In[735]:


### IDs left after removing UMR ids from 0 and 1 side

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]


# In[736]:


X_test_left.shape


# In[737]:


X_test_left['Predicted_action_2'].value_counts()


# In[738]:


X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
X_test_left = X_test_left.reset_index().drop('index',1)


# In[739]:


for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():
    umb_ids_1 = X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.Side1_UniqueIds'].unique()


# ## UMR One to Many and Many to One 

# ### One to Many

# In[300]:


#X_test_left = X_test.copy()


# In[740]:


cliff_for_loop = 16


# In[741]:


threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()


# In[742]:


exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()


# In[744]:


import numpy as np


def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total, atol=0).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
               

#null_value ='No'
many_ids_1 = []
one_id_0 = []
amount_array =[]
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


# In[745]:


umr_otm_table = pd.DataFrame(one_id_0)

if umr_otm_table.empty == False:
    umr_otm_table.columns = ['SideB.ViewData.Side0_UniqueIds']
    umr_otm_table['SideA.ViewData.Side1_UniqueIds'] =many_ids_1
else:
    print('No One to Many found')


# In[746]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='2166_125891836_Advent Geneva']


# ## Removing duplicate IDs from side 1

# In[747]:


if len(many_ids_1)!=0:
    unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
else:
    unique_many_ids_1 = np.array(['None'])


# In[748]:


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


# In[749]:


#meo[meo['ViewData.Side0_UniqueIds'] =='162_153156748_Advent Geneva']

if len(one_id_0_final)!=0:
    #unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
    one_id_0_final = one_id_0_final
else:
    one_id_0_final = np.array(['None'])


    
if umr_otm_table.empty == False:    
    umr_otm_table = umr_otm_table[umr_otm_table['SideB.ViewData.Side0_UniqueIds'].isin(one_id_0_final)]


# In[750]:


umr_otm_table


# In[751]:


filtered_otm_flat = [item for sublist in filtered_otm for item in sublist]


# ## Including UMR double count into OTM

# In[752]:


umr_double_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
umr_double_count = umr_double_count[(umr_double_count['Predicted_action']=='UMR_One_to_One') & (umr_double_count['count']==2)]


# In[753]:


umr_double_count


# In[754]:


if umr_otm_table.empty == False:
    sideb_unique = umr_otm_table['SideB.ViewData.Side0_UniqueIds'].unique()
else:
    sideb_unique =['None']
if umr_double_count.empty == False:

    umr_double_count_left = umr_double_count[~umr_double_count['SideB.ViewData.Side0_UniqueIds'].isin(sideb_unique)]


# In[755]:


umr_double_count_left


# In[756]:


pb_ids_otm_left = []
acc_id_single = []

for key in umr_double_count_left['SideB.ViewData.Side0_UniqueIds'].unique():
    acc_amount = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()
    pb_ids_otm = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & ((X_test['SideB.ViewData.Accounting Net Amount']==X_test['SideA.ViewData.B-P Net Amount']) | (X_test['SideB.ViewData.Accounting Net Amount']== (-1)*X_test['SideA.ViewData.B-P Net Amount']))]['SideA.ViewData.Side1_UniqueIds'].values
    pb_ids_otm_left.append(pb_ids_otm)
    acc_id_single.append(key)
    


# In[757]:


umr_otm_table_double_count = pd.DataFrame(acc_id_single)
umr_otm_table_double_count.columns = ['SideB.ViewData.Side0_UniqueIds']

umr_otm_table_double_count['SideA.ViewData.Side1_UniqueIds'] = pb_ids_otm_left


# In[758]:


umr_otm_table_final = pd.concat([umr_otm_table, umr_otm_table_double_count], axis=0)


# In[759]:


umr_otm_table_final = umr_otm_table_final.reset_index().drop('index',1)


# In[760]:


umr_otm_table_final


# ### Many to One

# In[761]:


cliff_for_loop = 17


# In[762]:


threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()


# In[763]:


exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()


# In[764]:


import numpy as np


def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total, atol=0).any():
                return [number]
           
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
        

#null_value ='No'

many_ids_0 = []
one_id_1 = []
amount_array2 =[]
for key in X_test_left[~((X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)) | (X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umt_table['SideB.ViewData.Side0_UniqueIds'])) | (X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umt_table['SideA.ViewData.Side1_UniqueIds'])) |(X_test['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds'])))]['SideA.ViewData.Side1_UniqueIds'].unique():
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
            


# In[765]:


umr_mto_table = pd.DataFrame(one_id_1)

if umr_mto_table.empty == False:
    umr_mto_table.columns = ['SideA.ViewData.Side1_UniqueIds']
    umr_mto_table['SideB.ViewData.Side0_UniqueIds'] =many_ids_0
else:
    print('No Many to One found')


# In[766]:


umr_mto_table


# ## Removing duplicate IDs from side 0

# In[767]:


if len(many_ids_0)!=0:
    unique_many_ids_0 = np.unique(np.concatenate(many_ids_0))
else:
    unique_many_ids_0 = np.array(['None'])


# In[768]:


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


# In[769]:


#pd.set_option('max_columns',50)

if len(one_id_1_final)!=0:
    #unique_many_ids_1 = np.unique(np.concatenate(many_ids_1))
    one_id_1_final = one_id_1_final
else:
    one_id_1_final = np.array(['None'])

#umr_otm_table = umr_otm_table[umr_otm_table['SideB.ViewData.Side0_UniqueIds'].isin(one_id_0_final)]

umr_mto_table = umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(one_id_1_final)]


# In[770]:


umr_mto_table = umr_mto_table.reset_index().drop('index',1)


# In[771]:


umr_mto_table


# In[772]:


umr_mto_table


# In[773]:


filtered_mto_flat = [item for sublist in filtered_mto for item in sublist]


# In[774]:


filtered_mto_flat


# ## Removing all the OTM and MTO Ids

# In[775]:


X_test_left.shape


# In[776]:


#X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(list(np.concatenate(many_ids_0))))]

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_mto_flat))]


X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(one_id_1)))]

#X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(np.concatenate(many_ids_1))))]

X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(filtered_otm_flat))]
X_test_left2 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(list(one_id_0)))]

X_test_left2 = X_test_left2.reset_index().drop('index',1)


# In[777]:


X_test_left2.shape


# ## UMB one to one (final)

# In[778]:


X_test_left2.shape


# In[779]:


X_test_umb = X_test_left2[X_test_left2['Predicted_action_2']=='UMB_One_to_One']
X_test_umb = X_test_umb.reset_index().drop('index',1)


# In[780]:


X_test_umb.shape


# In[781]:


#X_test_umb['UMB_key_OTO'] = X_test_umb['SideA.ViewData.Side1_UniqueIds'] + X_test_umb['SideB.ViewData.Side0_UniqueIds']


# In[782]:


def one_to_one_umb(data):
    
    count = data['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count0')
    id0s = count[count['count0']==1]['index'].unique()
    id1s = data[data['SideB.ViewData.Side0_UniqueIds'].isin(id0s)]['SideA.ViewData.Side1_UniqueIds']
    
    count1 = data['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count1')
    final_ids = count1[(count1['count1']==1) & (count1['index'].isin(id1s))]['index'].unique()
    return final_ids
   


# In[783]:


one_side_unique_umb_ids = one_to_one_umb(X_test_umb)


# In[784]:


final_oto_umb_table = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]


# In[785]:


final_oto_umb_table = final_oto_umb_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[786]:


final_oto_umb_table['probability_UMR'] = 0.00010
final_oto_umb_table = final_oto_umb_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[787]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds']=='1024_125845307_Goldman Sachs']


# ## Removing IDs from OTO UMB

# In[788]:


X_test_left2.shape


# In[789]:


X_test_left3 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(final_oto_umb_table['SideB.ViewData.Side0_UniqueIds']))]
X_test_left3 = X_test_left3[~(X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(final_oto_umb_table['SideA.ViewData.Side1_UniqueIds']))]


X_test_left3 = X_test_left3.reset_index().drop('index',1)


# In[790]:


X_test_left3.shape


# ## UMB One to Many and Many to One

# In[791]:


## Total IDs 

X_test['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[792]:


X_test_left3['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left3['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[793]:


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


# In[794]:


open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)


# In[ ]:





# In[795]:


X_test_left3[~X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)]


# In[796]:


X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]


# In[797]:


X_test_left4 = X_test_left4.reset_index().drop('index',1)


# In[798]:


X_test_left4


# In[824]:


X_test_left4['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left4['SideA.ViewData.Side1_UniqueIds'].nunique()


# ## Seperating OTM and MTO from MTM

# In[857]:


mtm_df_sd['len_1'] = mtm_df_sd['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
mtm_df_sd['len_0'] = mtm_df_sd['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))
mtm_df_eqs['len_1'] = mtm_df_eqs['ViewData.Side1_UniqueIds'].apply(lambda x: len(x))
mtm_df_eqs['len_0'] = mtm_df_eqs['ViewData.Side0_UniqueIds'].apply(lambda x: len(x))


# In[898]:


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


# In[903]:


final_mto_table = pd.DataFrame()
final_otm_table = pd.DataFrame()
final_mtm_table = pd.DataFrame()

final_mto_table = pd.concat([umr_mto_table,new_mto1,new_mto2],axis=0)
final_otm_table = pd.concat([umr_otm_table,new_otm1,new_otm2],axis=0)
final_mto_table = final_mto_table.reset_index().drop('index',1)
final_otm_table = final_otm_table.reset_index().drop('index',1)

final_mtm_table = pd.concat([new_mtm1,new_mtm2],axis=0)
final_mtm_table = final_mtm_table.reset_index().drop('index',1)


# In[900]:


if final_mto_table.empty == False:
    final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("['",''))
    final_mto_table['SideA.ViewData.Side1_UniqueIds'] = final_mto_table['SideA.ViewData.Side1_UniqueIds'].apply(lambda x: str(x).replace("']",''))


# In[901]:


if final_otm_table.empty == False:
    final_otm_table['SideB.ViewData.Side0_UniqueIds'] = final_otm_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("['",''))
    final_otm_table['SideB.ViewData.Side0_UniqueIds'] = final_otm_table['SideB.ViewData.Side0_UniqueIds'].apply(lambda x: str(x).replace("']",''))


# In[904]:


if final_mtm_table.empty:
    print (' No MTM')


# ## Packaging final output and coverage calculation

# ### List of tables for final Output 

# In[842]:


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


# In[817]:


coverage_meo = meo[~meo['ViewData.Status'].isin(['SMR','SMT','SPM','UMB'])]


# In[819]:


coverage_meo['ViewData.Side1_UniqueIds'].nunique() + coverage_meo['ViewData.Side0_UniqueIds'].nunique()


# In[905]:


final_umr_table.shape[0]*2 + final_umt_table.shape[0]*2 + len(no_pair_ids)+final_no_pair_table.shape[0] + len(open_ids_0_last) + len(open_ids_1_last) + closed_final_df.shape[0]+comment_table_eq_swap.shape[0] + final_mto_table.shape[0]*3 +  final_otm_table.shape[0]*3 + final_mtm_table.shape[0]*3 +final_oto_umb_table.shape[0]*2 

stop = timeit.default_timer()

print('Time: ', stop - start) 
