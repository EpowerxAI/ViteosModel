#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE


# In[2]:


cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
'Activity Code','Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount','Bloomberg_Yellow_Key',
'Cust Net Amount',
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


# In[3]:


new_cols = ['ViewData.' + x for x in cols] + add


# In[4]:


#df_170.shape


# ## Close Prediction OakTree

# In[1074]:


# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 00:42:02 2020

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
    
    side_meo['filter_key'] = side_meo['ViewData.Source Combination'].astype(str) +                          side_meo['ViewData.Mapped Custodian Account'].astype(str) +                          side_meo['ViewData.Currency'].astype(str)
        
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
    
    if(fun_setup_code_crucial == '379'):
        Transaction_type_closed_break_df =             fun_side_meo_combination_df[                     (fun_side_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(fun_transaction_type_list)) &                     (fun_side_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(fun_transaction_type_list)) &                     (abs(fun_side_meo_combination_df[Net_amount_col_name_list[0]]).astype(str) == abs(fun_side_meo_combination_df[Net_amount_col_name_list[1]]).astype(str)) &                     (fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[0]].astype(str) != fun_side_meo_combination_df[Side_0_1_UniqueIds_col_name_list[1]].astype(str))                     ]
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
    
    if(fun_setup_code == '379'):
        Transaction_Type_dict = {
                                'Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'STIF Interest BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['STIF Interest'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'STIF Interest Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['STIF Interest'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
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
                                'ForwardFX BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'ForwardFX Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['ForwardFX'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Internal Trans' : {'side' : 'PB',
                                           'Transaction_Type' : ['Internal Trans'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Withdraw' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Withdraw'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Deposit' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Deposit'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Redemption' : {'side' : 'PB',
                                           'Transaction_Type' : ['Redemption'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Subscription' : {'side' : 'PB',
                                           'Transaction_Type' : ['Redemption'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Incoming Wire' : {'side' : 'PB',
                                           'Transaction_Type' : ['Incoming Wire'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Transfer' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Transfer'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Withdrawal BP_side' : {'side' : 'PB',
                                           'Transaction_Type' : ['Withdrawal'],
                                           'Side_meo_training_df' : BP_meo_training_df},
                                'Withdrawal Acct_side' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Withdrawal'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Revenue' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Revenue'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Pay Down' : {'side' : 'Acct',
                                           'Transaction_Type' : ['Pay Down'],
                                           'Side_meo_training_df' : Acct_meo_training_df},
                                'Over & Short' : {'side' : 'PB',
                                           'Transaction_Type' : ['Over & Short'],
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
    
    

date_numbers_list = [16]
                     #2,3,4,
                    # 7,8,9,10,11,
                    # 14,15,16,17,18,
                    # 21,22,23,24,25,
                    # 28,29,30]

client = 'OakTree'

setup = '379'

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


# In[1100]:


new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list[0]]


# ## Read testing data 

# In[1077]:


#MeoCollections.MEO_HST_RecData_379_2020-06-18
meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/MEO/MeoCollections.MEO_HST_RecData_379_2020-06-16.csv",usecols=new_cols)


# In[1078]:


#df['ViewData.Task Business Date']


# In[1079]:


meo['ViewData.Status'].value_counts()


# In[1080]:


df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)


# In[1081]:


#df1[(df1['Date']=='2020-04-10') & (df1['ViewData.Side1_UniqueIds']=='996_125813417_Goldman Sachs')]
df1.shape


# In[1082]:


df1['close_key'] = df1['ViewData.Side0_UniqueIds'].astype(str) + df1['ViewData.Side1_UniqueIds'].astype(str)


# In[1093]:



## Output for Closed breaks

closed_df = df1[df1['close_key'].isin(list(Side_0_1_UniqueIds_closed_all_dates_list[0]))]


# In[1107]:


closed_df.shape


# ## Machine generated output

# In[1105]:


#df2 = df1[~df1['close_key'].isin(list(all_closed))]
df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]


# In[1108]:


df = df2.copy()


# In[1109]:


df = df.reset_index()
df = df.drop('index',1)


# In[1110]:


df.shape


# In[1113]:


#pd.set_option('display.max_columns', 500)


# In[1114]:


df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[1115]:


#df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[1116]:


df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[1117]:


pd.to_datetime(df['Date'])


# In[1118]:


df['Date'] = pd.to_datetime(df['Date']).dt.date


# In[1119]:


df['Date'] = df['Date'].astype(str)


# In[1121]:


#df['ViewData.Status'].value_counts()


# In[1122]:


df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB'])]
df = df.reset_index()
df = df.drop('index',1)


# In[1123]:


#df1[df1['ViewData.Status']=='SMB']
df['ViewData.Status'].value_counts()


# In[1124]:


df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[1125]:


#df_170[(df_170['ViewData.Status']=='UMR')]


# In[1127]:


#df['Date'].value_counts()


# ## Sample data on one date

# In[1128]:


df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})


# In[1129]:


sample = df[df['Date'] =='2020-06-17']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[1130]:


smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
smb = smb.drop('index',1)


# In[1134]:


smb_pb = smb.copy()
smb_acc = smb.copy()


# In[1135]:


smb_pb['ViewData.Accounting Net Amount'] = np.nan
smb_pb['ViewData.Side0_UniqueIds'] = np.nan
smb_pb['ViewData.Status'] ='SMB-OB'

smb_acc['ViewData.B-P Net Amount'] = np.nan
smb_acc['ViewData.Side1_UniqueIds'] = np.nan
smb_acc['ViewData.Status'] ='SMB-OB'


# In[1136]:


sample = sample[sample['ViewData.Status']!='SMB']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[1137]:


sample.shape


# In[1138]:


sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[1140]:


#sample['ViewData.Status'].value_counts()


# In[1141]:


sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


# In[1142]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0


# In[1146]:


#sample['ViewData.Status'].value_counts()


# In[1145]:


#sample['flag_side1'].value_counts()


# In[1147]:


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


# In[1148]:


sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)


# In[1149]:


sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[1150]:


sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[1152]:


#sample1['ViewData.Status'].value_counts()


# In[1154]:


aa = sample1[sample1['Trans_side']=='A_side']
bb = sample1[sample1['Trans_side']=='B_side']


# In[1156]:


#bb['ViewData.Source Combination'].value_counts()


# In[1157]:


aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)

bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)


# In[1158]:


aa = aa.reset_index()
aa = aa.drop('index', 1)
bb = bb.reset_index()
bb = bb.drop('index', 1)


# In[1159]:


#'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds'
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


# In[1160]:


bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
bb = bb.reset_index()
bb = bb.drop('index',1)


# In[1161]:


bb['ViewData.Status'].value_counts()


# In[1163]:


bb.shape


# In[1165]:


###################### loop m*n ###############################
from pandas import merge
from tqdm import tqdm

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
            no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
            no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
            

no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[1166]:


#no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[1167]:


#pd.DataFrame(no_pair_ids).rename


# In[1168]:


len(no_pair_ids)


# In[1169]:


#test_file['SideA.ViewData.Status'].value_counts()


# In[1170]:


test_file = pd.concat(training_df)


# In[1171]:


test_file = test_file.reset_index()
test_file = test_file.drop('index',1)


# In[1172]:


test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')


# In[1173]:


test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]


# In[1174]:


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


# In[1175]:


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


# In[1176]:


#test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)


# In[1177]:


#test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)


# In[1178]:


test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']


# ## Description code

# In[1184]:


os.chdir('C:\\Users\\consultant136\\ML1.0')


# In[1185]:


print(os.getcwd())


# In[1187]:


## TODO - Import a csv file for description category mapping

com = pd.read_csv('desc cat with naveen oaktree.csv')
#com


# In[1188]:


cat_list = list(set(com['Pairing']))


# In[1189]:


import re

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


# In[1190]:


#df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

test_file['SideA.desc_cat'] = test_file['SideA.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
test_file['SideB.desc_cat'] = test_file['SideB.ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[1191]:


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
        


# In[1192]:



#df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))

test_file['SideA.desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : currcln(x))
test_file['SideB.desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : currcln(x))


# In[1193]:


com = com.drop(['var','Catogery'], axis = 1)

com = com.drop_duplicates()

com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[1194]:


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


# In[1195]:


test_file['SideA.new_desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : catcln1(x,com))
test_file['SideB.new_desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : catcln1(x,com))


# In[1196]:


comp = ['inc','stk','corp ','llc','pvt','plc']
#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[1197]:


#df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

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


# In[1198]:


#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : desccat(x))
test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : desccat(x))


# In[1200]:


#test_file['SideB.new_desc_cat'].value_counts()


# ## Prime Broker

# In[1201]:


test_file['new_pb'] = test_file['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[1202]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[1203]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[1204]:


test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].fillna('kkk')


# In[1205]:


test_file['new_pb1'] = test_file.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)


# In[1206]:


#test_file = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/X_test_files_after_loop/meo_testing_HST_RecData_379_06_19_2020_test_file_with_ID.csv')


# In[1207]:


#test_file = test_file.drop('Unnamed: 0',1)


# In[1208]:


test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days

test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days


# In[1209]:


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


# In[1210]:



############ Fund match new ########

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

vec_fund_match = np.vectorize(fundmatch)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)


# In[1211]:


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


# In[1212]:


# LOWER CASE
trans_type_A_side = [str(item).lower() for item in trans_type_A_side]
trans_type_B_side = [str(item).lower() for item in trans_type_B_side]

asset_type_cat_A_side = [str(item).lower() for item in asset_type_cat_A_side]
asset_type_cat_B_side = [str(item).lower() for item in asset_type_cat_B_side]

invest_type_A_side = [str(item).lower() for item in invest_type_A_side]
invest_type_B_side = [str(item).lower() for item in invest_type_B_side]

prime_broker_A_side = [str(item).lower() for item in prime_broker_A_side]
prime_broker_B_side = [str(item).lower() for item in prime_broker_B_side]


# In[1213]:


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


# In[1214]:


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


# In[1215]:


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


# In[1216]:



remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]
# remove_blanks_a = [item for item in remove_dates_a if item]
# # remove_blanks_a[:10]


# # In[321]:

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]


# In[1217]:


test_file['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
test_file['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

test_file['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
test_file['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

test_file['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
test_file['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side


# In[1218]:


#test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[1219]:


#test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[1220]:


##############

values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values

vec_tt_match = np.vectorize(mhreplaced)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)


# In[1221]:


test_file.loc[test_file['SideA.ViewData.Transaction Type']=='int','SideA.ViewData.Transaction Type'] = 'interest'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='wires','SideA.ViewData.Transaction Type'] = 'wire'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='dividends','SideA.ViewData.Transaction Type'] = 'dividend'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='miscellaneous','SideA.ViewData.Transaction Type'] = 'misc'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='div','SideA.ViewData.Transaction Type'] = 'dividend'


# In[1222]:


test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[1223]:


test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)


# In[1224]:


#train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)


# In[1225]:


test_file['Combined_Investment_Type'] = test_file['SideA.ViewData.Investment Type'].astype(str) + test_file['SideB.ViewData.Investment Type'].astype(str)


# In[1226]:


test_file['Combined_Asset_Type_Category'] = test_file['SideA.ViewData.Asset Category Type'].astype(str) + test_file['SideB.ViewData.Asset Category Type'].astype(str)


# In[1227]:


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


# In[1228]:


len(test_file['SideB.ViewData.CUSIP'].values)


# In[ ]:





# In[1229]:


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


# In[1230]:


#test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
#test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[1231]:


def nan_equals_fun(a,b):
    if a==1 and b==1:
        return 1
    else:
        return 0
    
vec_nan_equal_fun = np.vectorize(nan_equals_fun)
values_key_NAN_B_Side = test_file['SideB.ViewData.key_NAN'].values
values_key_NAN_A_Side = test_file['SideA.ViewData.key_NAN'].values
test_file['All_key_nan'] = vec_nan_equal_fun(values_key_NAN_B_Side,values_key_NAN_A_Side )

#test_file['All_key_nan'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.key_NAN']==1 and x['SideA.ViewData.key_NAN']==1 else 0, axis=1)


# In[1232]:


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


# In[1233]:


test_file['amount_percent'] = (test_file['SideA.ViewData.B-P Net Amount']/test_file['SideB.ViewData.Accounting Net Amount']*100)


# In[1234]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())


# In[1235]:


test_file['SideB.ViewData.Prime Broker'] = test_file['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())


# In[1236]:


test_file['SideB.ViewData.Asset Type Category'] = test_file['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Asset Type Category'] = test_file['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())


# In[1237]:


#test_file

test_file['ViewData.Combined Transaction Type'] = test_file['ViewData.Combined Transaction Type'].apply(lambda x: x.replace('jnl','journal'))


# In[1238]:



#test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)


# In[1239]:


#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/X_test_files_after_loop/meo_testing_HST_RecData_170_06-18-2020_test_file.csv")


# In[1240]:


test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))


# In[1241]:


trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']


# In[1242]:


test_file['SideA.TType'] = test_file.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)
test_file['SideB.TType'] = test_file.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)


# In[1243]:


test_file['Combined_Desc'] = test_file['SideA.new_desc_cat'] + test_file['SideB.new_desc_cat']


# In[1244]:


test_file['Combined_TType'] = test_file['SideA.TType'].astype(str) + test_file['SideB.TType'].astype(str)


# In[1245]:


import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
import numpy as np

#le = LabelEncoder()
for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    test_file[feature] = pd.to_datetime(test_file[feature],errors = 'coerce').dt.weekday


# In[1247]:


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


# ## UMR Mapping

# In[1248]:



## TODO Import HIstorical UMR FILE for Transaction Type mapping

oaktree_umr = pd.read_csv('OakTree_UMR.csv')


# In[1249]:


#soros_umr['ViewData.Combined Transaction Type'].unique()


# In[1250]:


test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in oaktree_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)


# In[1251]:


test_file['abs_amount_flag'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)


# In[1252]:


test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]

test_file = test_file.reset_index().drop('index',1)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int)


# In[765]:


#test_file2 = test_file[((test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")) | ((test_file['SideA.TType']!="Trade") & (test_file['SideB.TType']!="Trade")) ]


# In[766]:


#test_file2 = test_file[(test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")]


# In[767]:


#test_file[(test_file['SideA.TType']==test_file['SideB.TType'])]['SideB.TType']


# In[768]:


#test_file2 = test_file2.reset_index()
#test_file2 = test_file2.drop('index',1)


# In[1256]:


#test_file['SideA.ViewData.BreakID_A_side'].value_counts()
#test_file[model_cols]


# ## Test file served into the model

# In[1257]:


test_file2 = test_file.copy()


# In[1258]:


X_test = test_file2[model_cols]


# In[1259]:


X_test = X_test.reset_index()
X_test = X_test.drop('index',1)
X_test = X_test.fillna(0)


# In[1260]:


X_test = X_test.fillna(0)


# In[1261]:


X_test.shape


# In[1262]:


X_test = X_test.drop_duplicates()
X_test = X_test.reset_index()
X_test = X_test.drop('index',1)


# In[1263]:


X_test.shape


# ## Model Pickle file import

# In[1266]:



## TODO Import Pickle file for 1st Model

import pickle


# In[1265]:


#filename = 'Oak_W125_model_with_umb.sav'
#filename = '125_with_umb_without_des_and_many_to_many.sav'
#filename = '125_with_umb_and_price_without_des_and_many_to_many_tdsd2.sav'
#filename = 'Weiss_new_model_V1.sav'
#filename = 'Soros_new_model_V1_with_close.sav'
#filename = 'Soros_full_model_smote.sav'

#filename = 'Soros_full_model_best_cleaned_tt_without_date.sav'
#filename = 'Soros_full_model_version2.sav'
filename = 'OakTree_final_model2.sav'

#filename = 'Soros_full_model_umr_umt.sav'

clf = pickle.load(open(filename, 'rb'))


# In[1267]:


X_test


# ## Predictions

# In[1269]:


# Actual class predictions
rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[1270]:


probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[1271]:


X_test['Predicted_action'] = rf_predictions
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test['probability_No_pair'] = probability_class_0
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test['probability_UMB'] = probability_class_1
X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[1272]:


X_test['Predicted_action'].value_counts()


# In[1273]:


X_test


# ## Two Step Modeling

# In[1274]:


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


# In[1275]:


X_test2 = test_file[model_cols_2]


# In[1276]:


X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)
X_test2 = X_test2.fillna(0)


# In[1277]:


X_test2.shape


# In[1278]:


X_test2 = X_test2.drop_duplicates()
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)


# In[1279]:


X_test2.shape


# In[1281]:


#filename2 = 'Soros_full_model_all_two_step.sav'

## TODO Import MOdel2 as per the two step modelling process

filename2 = 'OakTree_final_model2_step_two.sav'
clf2 = pickle.load(open(filename2, 'rb'))


# In[1282]:


# Actual class predictions
rf_predictions2 = clf2.predict(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs2 = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[1283]:


probability_class_0_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

#probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[1284]:


X_test2['Predicted_action_2'] = rf_predictions2
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test2['probability_No_pair_2'] = probability_class_0_two
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test2['probability_UMB_2'] = probability_class_1_two
#X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[1285]:


X_test2['Predicted_action_2'].value_counts()


# In[1286]:


X_test.shape


# In[1287]:


X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)


# In[1289]:


X_test


# ## New Aggregation

# In[1292]:


X_test['Tolerance_level'] = np.abs(X_test['probability_UMB_2'] - X_test['probability_No_pair_2'])


# In[1293]:


#X_test[X_test['Tolerance_level']<0.1]['Predicted_action'].value_counts()


# In[1294]:


b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# ## UMR segregation

# In[1295]:


def umr_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']==1) & (b_count2['len']<=2)]
    return umr_table['SideB.ViewData.Side0_UniqueIds'].values
    


# In[1296]:


umr_ids_0 = umr_seg(X_test)


# ## 1st Prediction Table for One to One UMR

# In[1297]:


final_umr_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')]


# In[1298]:


final_umr_table = final_umr_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]


# In[1299]:


final_umr_table.shape


# ## No-Pair segregation

# In[1307]:


#b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
#a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[1308]:


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


# In[1309]:


no_pair_ids_b_side, no_pair_ids_a_side = no_pair_seg(X_test)


# In[1310]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]['Predicted_action_2'].value_counts()


# In[1311]:


X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[1312]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]['Predicted_action_2'].value_counts()


# In[1313]:


final_open_table = X_test[(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]


# In[1314]:


final_open_table = final_open_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[1315]:


final_open_table['probability_UMR'] = 0.00010
final_open_table = final_open_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[1316]:


final_open_table.shape


# In[1321]:


#final_open_table.head()

len(no_pair_ids_b_side)


# In[1327]:


b_side_open_table = final_open_table.groupby('SideB.ViewData.Side0_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()
a_side_open_table = final_open_table.groupby('SideA.ViewData.Side1_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()


# In[1330]:


a_side_open_table = a_side_open_table[a_side_open_table['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]
b_side_open_table = b_side_open_table[b_side_open_table['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)]


# In[1332]:


b_side_open_table = b_side_open_table.reset_index().drop('index',1)
a_side_open_table = a_side_open_table.reset_index().drop('index',1)


# In[1336]:


final_no_pair_table = pd.concat([a_side_open_table,b_side_open_table], axis=0)
final_no_pair_table = final_no_pair_table.reset_index().drop('index',1)


# In[1337]:


#final_no_pair_table


# ## One to One UMB segregation

# In[829]:


### IDs left after removing open ids from 0 and 1 side


X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

X_test_left = X_test_left.reset_index().drop('index',1)


# In[830]:


X_test_left['Predicted_action_2'].value_counts()


# In[831]:


### IDs left after removing UMR ids from 0 and 1 side

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]


# In[832]:


X_test_left.shape


# In[833]:


X_test_left['Predicted_action'].value_counts()


# In[834]:


X_test_left.groupby('SideB.ViewData.Side0_UniqueIds')['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()


# In[835]:


X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
X_test_left = X_test_left.reset_index().drop('index',1)


# In[836]:


X_test_umb = X_test_left[X_test_left['Predicted_action_2']=='UMB_One_to_One']
X_test_umb = X_test_umb.reset_index().drop('index',1)


# In[837]:


#X_test_umb['UMB_key_OTO'] = X_test_umb['SideA.ViewData.Side1_UniqueIds'] + X_test_umb['SideB.ViewData.Side0_UniqueIds']


# In[838]:


def one_to_one_umb(data):
    
    count = data['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count0')
    id0s = count[count['count0']==1]['index'].unique()
    id1s = data[data['SideB.ViewData.Side0_UniqueIds'].isin(id0s)]['SideA.ViewData.Side1_UniqueIds']
    
    count1 = data['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count1')
    final_ids = count1[(count1['count1']==1) & (count1['index'].isin(id1s))]['index'].unique()
    return final_ids
    
    


# In[839]:


one_side_unique_umb_ids = one_to_one_umb(X_test_umb)


# In[840]:


final_oto_umb_table = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]


# In[841]:


final_oto_umb_table = final_oto_umb_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[842]:


final_oto_umb_table['probability_UMR'] = 0.00010
final_oto_umb_table = final_oto_umb_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[843]:


X_test_umb2 = X_test_umb[~X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]
X_test_umb2 = X_test_umb2.reset_index().drop('index',1)


# In[845]:


X_test_umb2.shape


# In[844]:


X_test_umb2['SideB.ViewData.Side0_UniqueIds'].value_counts()


# In[846]:


X_test_umb2[X_test_umb2['SideB.ViewData.Side0_UniqueIds']=='432_379899569_Advent Geneva']['SideA.ViewData.Side1_UniqueIds'].values


# In[847]:


df = pd.DataFrame({
'Column1': [1, 1, 1, 2, 2, 3, 3, 3, 3,4,5,6,7],
'Column2': [14, 13, 16, 18, 19, 14, 11, 24, 15,22,33,33,33]})


# In[848]:


df


# In[849]:


#X_test_umb2[X_test_umb2['SideA.ViewData.Side1_UniqueIds']=='1175_379879573_State Street']['SideB.ViewData.Side0_UniqueIds'].unique()


# In[850]:


def sample_otm(data, col1, col2):
    ss =[]
    count_table = data[col2].value_counts().reset_index()
    count_table.columns = [col2, 'count']
    for i in data[col1].unique():
        ids1 = data[data[col1]==i][col2].unique()
        if count_table[count_table[col2].isin(ids1)]['count'].max() ==1:
            ss.append(i)
        else:
            pass
    return ss


def sample_mto(data, col1, col2):
    ss =[]
    count_table = data[col1].value_counts().reset_index()
    count_table.columns = [col1, 'count']
    for i in data[col2].unique():
        ids0 = data[data[col2]==i][col1].unique()
        if count_table[count_table[col1].isin(ids0)]['count'].max() ==1:
            ss.append(i)
        else:
            pass
    return ss


# In[851]:


sample_otm(df, 'Column1', 'Column2')


# In[852]:


sample_mto(df, 'Column1', 'Column2')


# In[853]:


sample_mto(X_test_umb2, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds')


# In[854]:


sample_otm(X_test_umb2, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds')


# In[857]:


X_test_umb2[X_test_umb2['SideB.ViewData.Side0_UniqueIds'].isin(sample_otm(X_test_umb2, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds'))]


# In[855]:


#for i in sample_mto(X_test_umb2, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds'):
X_test_umb2[X_test_umb2['SideA.ViewData.Side1_UniqueIds'].isin(sample_mto(X_test_umb2, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds'))]


# In[861]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds']=='20_379902880_BNP Paribas']


# In[874]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='93_379902880_Advent Geneva']


# In[929]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values


# In[941]:


import numpy as np 
import itertools
  
def find_closese_sum(numbers, targets):
    numbers = numbers[:]
    for t in targets:
        if not numbers:
            break
    combs = sum([list(itertools.combinations(numbers, r)) for r in range(1, len(numbers)+1)], [])
    sums = np.asarray(list(map(sum, combs)))
    bestcomb = combs[np.argmin(np.abs(np.asarray(sums) - t))]
    numbers = list(set(numbers).difference(bestcomb))
    print("Target: {},  combination: {}".format(t, bestcomb))
 


# In[953]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']


# In[943]:




find_closese_sum(numbers, targets)


# In[950]:


import itertools
def finding_closet(ls,target,depth):
    closest = []
    for i in itertools.combinations(ls, depth):

        if sum(i) == target:
            return i
        else:
            closest.append((abs(sum(i) - target), i))
    return min(closest)[1]


# In[955]:


finding_closet(numbers, targets,5)


# In[971]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']


# In[1073]:


#targets = list(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideB.ViewData.Accounting Net Amount'].max())
#numbers = list(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values)


# In[ ]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values


# In[ ]:





# ## UMR One to Many Summation Code

# In[1012]:


def f(v, i, S, memo):
    if i >= len(v): return 1 if S == 0 else 0
    if (i, S) not in memo:  # <-- Check if value has not been calculated.
        count = f(v, i + 1, S, memo)
        count += f(v, i + 1, S - v[i], memo)
        memo[(i, S)] = count  # <-- Memoize calculated result.
    return memo[(i, S)] # <-- Return memoized value.


def g(v, S, memo):
    subset = []
    for i, x in enumerate(v):
        # Check if there is still a solution if we include v[i]
        if f(v, i + 1, S - x, memo) > 0:
            subset.append(x)
            S -= x
    return subset


many_ids_1 = []
one_id_0 = []
for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():

    values =  X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].values
    net_sum = X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

    memo = dict()
    if f(v, 0, sum, memo) == 0: 
        print("There are no valid subsets.")
    else:
        amount_array = np.array((g(values, net_sum, memo)))

    id1_aggregation = X_test_left[(X_test_left['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
    id0_unique = key
    
    if key =='68_379899240_Advent Geneva':
        print(amount_array)
    
    if len(id1_aggregation)>0: 
        many_ids_1.append(id1_aggregation)
        one_id_0.append(id0_unique)
    else:
        pass
    #print(id1_aggregation)


# In[1010]:


#X_test_left


# In[1025]:


def f(v, i, S, memo):
    if i >= len(v): return 1 if S == 0 else 0
    if (i, S) not in memo:  # <-- Check if value has not been calculated.
        count = f(v, i + 1, S, memo)
        count += f(v, i + 1, S - v[i], memo)
        memo[(i, S)] = count  # <-- Memoize calculated result.
    return memo[(i, S)] # <-- Return memoized value.


def g(v, S, memo):
    subset = []
    for i, x in enumerate(v):
        # Check if there is still a solution if we include v[i]
        if f(v, i + 1, S - x, memo) > 0:
            subset.append(x)
            S = S-x
    return subset

def f_equal(a, b):
    return abs(a - b) < epsilon


many_ids_1 = []
one_id_0 = []
for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():

    values =  X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].values
    net_sum = X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

    memo = dict()
    if f(v, 0, sum, memo) == 0: 
        print("There are no valid subsets.")
    else:
        amount_array = np.array((g(values, net_sum, memo)))

    id1_aggregation = X_test_left[(X_test_left['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test_left['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
    id0_unique = key       
    
    if len(id1_aggregation)>0: 
        many_ids_1.append(id1_aggregation)
        one_id_0.append(id0_unique)
    else:
        pass


# In[1065]:


import numpy as np
array = np.array([11,22.22,3,4])
num = 33.33

def subsetsum(array,num):

    if np.isclose(num,0) or num < 1:
        return None
    elif len(array) == 0:
        return None
    else:
        if np.isclose(array[0], num):
            return [array[0]]
        else:
            with_v = subsetsum(array[1:],(num - array[0])) 
            if with_v:
                return [array[0]] + with_v
            else:
                return subsetsum(array[1:],num)

print('\nList of Values : ',array)
print('\nSum Desired : ',num)
print('\nValues that add up to sum : ',subsetsum(array,num))


# In[1071]:


import numpy as np
#array = np.array([11.11,22.22,3,4])
#num = 33.33

def subsetsum(array,num):

    if np.isclose(num,0) or num < 1:
        return None
    elif len(array) == 0:
        return None
    else:
        if np.isclose(array[0], num):
            return [array[0]]
        else:
            with_v = subsetsum(array[1:],(num - array[0])) 
            if with_v:
                return [array[0]] + with_v
            else:
                return subsetsum(array[1:],num)

#print('\nList of Values : ',array)
#print('\nSum Desired : ',num)
#print('\nValues that add up to sum : ',subsetsum(array,num))


many_ids_1 = []
one_id_0 = []
for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():

    values =  X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].values
    net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

    #memo = dict()
    if subsetsum(values,net_sum) == None: 
        #print("There are no valid subsets.")
        pass
    else:
        amount_array = np.array(subsetsum(values,net_sum))

    id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
    id0_unique = key       
    
    if len(id1_aggregation)>1: 
        many_ids_1.append(id1_aggregation)
        one_id_0.append(id0_unique)
    else:
        pass


# In[1072]:


many_ids_1


# In[ ]:





# In[ ]:





# In[ ]:





# In[1044]:


def f_equal(a, b):
    return abs(a - b) < epsilon
def find_sum(numbers, target):
    numbers = set(numbers)
    half = math.ceil(target / 2)
    print(half)
    partial_results = set(range(1, half))
    print(partial_results)
    for i in numbers.intersection(partial_results):
        print(i)
        #if f_equal(target, i) in numbers:
            
        if target - i in numbers:
            return True


# In[1059]:


approximate_subset_sum([11,22.22,3,4], 33.33,0)


# In[1060]:


#X_test_left['SideB.ViewData.Side0_UniqueIds'].unique()

def trim(data, delta):
    """Trims elements within `delta` of other elements in the list."""

    output = []
    last = 0

    for element in data:
        if element['value'] > last * (1 + delta):
            output.append(element)
            last = element['value']

    return output
import itertools
import operator

def merge_lists(m, n):
    """
    Merges two lists into one.

    We do *not* remove duplicates, since we'd like to see all possible
    item combinations for the given approximate subset sum instead of simply
    confirming that there exists a subset that satisfies the given conditions.

    """
    merged = itertools.chain(m, n)
    return sorted(merged, key=operator.itemgetter('value'))

def approximate_subset_sum(data, target, epsilon):
    """
    Calculates the approximate subset sum total in addition
    to the items that were used to construct the subset sum.

    Modified to track the elements that make up the partial
    sums to then identify which subset items were chosen
    for the solution.

    """

    # Intialize our accumulator with the trivial solution
    acc = [{'value': 0, 'partials': [0]}]

    count = len(data)

    # Prep data by turning it into a list of hashes
    data = [{'value': d, 'partials': [d]} for d in data]

    for key, element in enumerate(data, start=1):
        augmented_list = [{
            'value': element['value'] + a['value'],
            'partials': a['partials'] + [element['value']]
        } for a in acc]

        acc = merge_lists(acc, augmented_list)
        acc = trim(acc, delta=float(epsilon) / (2 * count))
        acc = [val for val in acc if val['value'] <= target]

    # The resulting list is in ascending order of partial sums; the
    # best subset will be the last one in the list.
    return acc[-1]


# In[1064]:





# In[1002]:


many_ids_1


# In[999]:


X_test_left[X_test_left['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']


# In[ ]:


for key in X_test['SideB.ViewData.Side0_UniqueIds'].unique()


# In[939]:


def binary_search(a, x, lo=0, hi=None): 
    if hi is None: 
        hi = len(a) 
    while lo < hi: 
        mid = (lo+hi)//2
        midval = a[mid] 
        if midval < x: 
            lo = mid+1
        elif midval > x:  
            hi = mid 
        else: 
            return mid 
    return -1
  
def printPairs(arr, n):  
  
    pair_exists = False
    # Sort the array  
    arr.sort()  

    # Traverse the array  
    for i in range(n): 
        # For every arr[i] < 0 element,  
        # do a binary search for arr[i] > 0.  
        if (arr[i] < 0):  
            # If found, print the pair.  
            if (binary_search(arr,-arr[i])):  
                print(arr[i] , ", " , -arr[i])  
  
                pair_exists = True
        else: 
            break
  
    if (pair_exists == False):  
        print("No such pair exists") 


# In[940]:


printPairs(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values,2)


# In[ ]:





# In[949]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='166_379903081_Advent Geneva']


# In[944]:


def findPairs(lst, K):  
    res = [] 
    while lst: 
        num = lst.pop() 
        diff = K - num 
        if diff in lst: 
            res.append((diff, num)) 
          
    res.reverse() 
    return res 
      
# Driver code 
lst = list(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='166_379903081_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values)


# In[945]:


lst


# In[946]:


K = X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='166_379903081_Advent Geneva']['SideB.ViewData.Accounting Net Amount'].max()


# In[947]:


K


# In[948]:


print(findPairs(lst, K))


# In[928]:





# In[900]:


lst = list(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='1_379903081_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values)


# In[867]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds']=='119_379903081_State Street']


# In[870]:


df


# In[873]:


meo[meo['ViewData.Side1_UniqueIds']=='501_379879573_State Street']



# In[604]:


X_test_umb2[X_test_umb2['SideB.ViewData.Side0_UniqueIds']=='18_379899486_Advent Geneva']


# In[ ]:





# In[ ]:



prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
prediction_table['UMR_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[335]:


from itertools import combinations
# set the number of closest combinations to show, the targeted number and the list
show = dummy.shape[0]
target = dummy['SideB.ViewData.Accounting Net Amount'].max()
#lis = [497.96, 10, 5084, 156.43, 381.3, 3298.85, 625.68]
lis = list(dummy['SideA.ViewData.B-P Net Amount'].values)

diffs = []
for n in range(1, len(lis)+1):
    numbers = combinations(lis, n)
    # list the combinations and their absolute difference to target
    for combi in numbers:
        diffs.append([combi, abs(target - sum(combi))])

diffs.sort(key=lambda x: x[1])

for item in diffs[:show]:
    print(item[0], round(item[1],10))


# In[345]:


from past.builtins import xrange
from itertools import combinations

show = dummy.shape[0]
target = dummy['SideB.ViewData.Accounting Net Amount'].max()
#lis = [497.96, 10, 5084, 156.43, 381.3, 3298.85, 625.68]

lis = list(dummy['SideA.ViewData.B-P Net Amount'].values)

#lis = [497.96, 10, 5084, 156.43, 381.3, 3298.85, 625.68]
predicted_status = list(X_test['Predicted_action'].values)
for i , (j,k) in enumerate(zip(lis,predicted_status)):
    for comb in combinations(lis, i):
        if sum(comb) == target:
            print (comb)


# In[258]:


umr_table = X_test[(X_test['Predicted_action']=='UMR_One_to_One')]
umr_table = umr_table.reset_index().drop('index',1)


X_test_left = X_test[~X_test['SideA.ViewData.Side1_UniqueIds'].isin(umr_table['SideA.ViewData.Side1_UniqueIds'].unique())]

X_test_left = X_test_left[~X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_table['SideB.ViewData.Side0_UniqueIds'].unique())]


# In[259]:


X_test_left


# In[260]:


#prediction_table = X_test_left.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[261]:


prediction_table = X_test_left.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()


# In[262]:


prediction_table['len'] = prediction_table['Predicted_action'].str.len()
prediction_table['No_Pair_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
prediction_table['UMR_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)


# In[263]:


open_table = prediction_table[(prediction_table['No_Pair_flag']==1) & (prediction_table['len']==1)]
open_table = open_table.reset_index().drop('index',1)


# In[264]:


X_test_left2 = X_test_left[~X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(open_table['SideB.ViewData.Side0_UniqueIds'].unique())]


# In[265]:


#X_test_left2


# In[ ]:





# In[266]:


prediction_table2 = X_test_left2.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()


# In[267]:


prediction_table2['len'] = prediction_table2['Predicted_action'].str.len()
prediction_table2['No_Pair_flag'] = prediction_table2['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
prediction_table2['UMB_flag'] = prediction_table2['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
prediction_table2['UMT_flag'] = prediction_table2['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
prediction_table2['UMR_flag'] = prediction_table2['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)


# In[268]:


open_table2 = prediction_table2[(prediction_table2['No_Pair_flag']==1) & (prediction_table2['len']==1)]
open_table2 = open_table2.reset_index().drop('index',1)


# In[269]:


X_test_left3 = X_test_left2[~X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(open_table2['SideA.ViewData.Side1_UniqueIds'].unique())]


# In[ ]:





# In[270]:


X_test_umb = X_test_left3[X_test_left3['Predicted_action']!='No-Pair']
X_test_umb = X_test_umb.reset_index()
X_test_umb = X_test_umb.drop('index',1)


# In[271]:


umb_table = X_test_umb.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].count().reset_index()


# In[272]:


umb_table


# In[273]:


X_test_umb


# In[274]:


check_ids = ['16_153157591_Advent Geneva','30_153157422_Advent Geneva','78_153156543_Advent Geneva',
             '182_153156543_Advent Geneva','170_153156543_Advent Geneva','567_153157547_Advent Geneva']


# In[275]:


for i, key in enumerate(check_ids):
    if i==4:
        print(key)
        dummy = X_test_umb[X_test_umb['SideB.ViewData.Side0_UniqueIds']=='134_153157549_Advent Geneva']


# In[276]:


#X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='182_153156543_Advent Geneva']

dummy


# In[763]:


X_test_mtm


# In[277]:


def get_relation(df, col1, col2):        
    first_max = df[[col1, col2]].groupby(col1).count().max()[0]
    second_max = df[[col1, col2]].groupby(col2).count().max()[0]
    if first_max==1:
        if second_max==1:
            return 'one-to-one'
        else:
            return 'one-to-many'
    else:
        if second_max==1:
            return 'many-to-one'
        else:
            return 'many-to-many'

#from itertools import product
#for col_i, col_j in product(df.columns, df.columns):
#    if col_i == col_j:
#        continue
#    print(col_i, col_j, get_relation(df, col_i, col_j))


# In[278]:


print(get_relation(X_test_umb, 'SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'))


# In[766]:


X_test_umb[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds']].groupby('SideB.ViewData.Side0_UniqueIds').count()


# In[767]:


X_test_umb[X_test_umb['SideB.ViewData.Side0_UniqueIds']=='9_153154447_Advent Geneva']


# In[768]:


X_test_umb[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds']].groupby('SideA.ViewData.Side1_UniqueIds').count()


# In[629]:


df = pd.DataFrame({
'Column1': [1, 1, 1, 2, 2, 3, 3, 3, 3,4],
'Column2': [14, 13, 16, 18, 19, 14, 11, 24, 15,22]})


# In[635]:


df


# In[639]:


df.groupby(['Column2'])['Column1'].nunique()


# In[279]:


def one_to_one(df, col1, col2):        
    grp = df[[col1, col2]].groupby(col1).count().reset_index()
    grp.columns = [col1, 'count']
    
    grp2 = df[[col1, col2]].groupby(col2).count().reset_index()
    grp2.columns = [col2, 'count']
    
    grp3 = grp[grp['count']==1]
    grp4 = grp2[grp2['count']==1]
    
 
    return grp3[col1].unique() , grp4[col2].unique()


# In[280]:


one_to_many(df, 'Column1','Column2')


# In[771]:


df.groupby(['Column1'])['Column2'].unique()


# In[772]:


df.groupby(['Column2']).count().reset_index()


# In[773]:


df


# In[281]:


def one_to_many(df, colB, colA):        
    grp = df[[colB, colA]].groupby(colA)[colB].nunique().reset_index()
    
    grp.columns = [colA, 'count']
    
    grp2 = grp[grp['count']==1]
 
    return grp2[colA].unique()


# In[775]:


one_to_many(X_test_umb, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds')


# In[776]:


X_test_otm = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_to_many(X_test_umb, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds'))]


# In[777]:


X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_to_many(X_test_umb, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds'))]


# In[652]:


X_test_otm.shape


# In[282]:


def one_to_one(df, col1, col2):        
    grp = df[[col1, col2]].groupby(col1).count().reset_index()
    grp.columns = [col1, 'count']
    
    grp2 = df[[col1, col2]].groupby(col2).count().reset_index()
    grp2.columns = [col2, 'count']
    
    grp3 = grp[grp['count']==1]
    grp4 = grp2[grp2['count']==1]
    
 
    return grp3[col1].unique() , grp4[col2].unique()


# In[283]:


def one_to_many(df, col1, col2):        
    grp = df[[col1, col2]].groupby(col2).count().reset_index()
    grp.columns = [col2, 'count']
    
    grp2 = grp[grp['count']==1]
 
    return grp2[col2].unique()


# In[284]:


def many_to_one(df, col1, col2):
    grp = df[[col1, col2]].groupby(col1).count().reset_index()
    grp.columns = [col1, 'count']
    
    grp2 = grp[grp['count']==1]
 
    return grp2[col1].unique()


# In[494]:


#X_test_umb['key'] = X_test_umb['SideB.ViewData.Side0_UniqueIds'] + X_test_umb['SideA.ViewData.Side1_UniqueIds']


# In[623]:


#X_test_umb['key'].value_counts()

b = X_test_umb[['SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds']].groupby('SideB.ViewData.Side0_UniqueIds')['SideA.ViewData.Side1_UniqueIds'].count().reset_index()


# In[624]:


b


# In[625]:


b[b['SideB.ViewData.Side0_UniqueIds']=='158_153147536_Advent Geneva']


# In[619]:


X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds']=='104_153157549_Credit suisse']


# In[600]:


X_test_mtm['SideB.ViewData.Side0_UniqueIds'].value_counts()


# In[601]:


X_test_umb[X_test_umb['SideB.ViewData.Side0_UniqueIds']=='158_153147536_Advent Geneva']


# In[602]:


X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds']=='11_153139764_Bank of America Merrill Lynch']


# In[607]:


#X_test_umb.to_csv('umb_example.csv')


# In[604]:


X_test_mto[X_test_mto['SideA.ViewData.Side1_UniqueIds']=='11_153139764_Bank of America Merrill Lynch']


# In[496]:


#dd = X_test_umb2[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds']].groupby('SideB.ViewData.Side0_UniqueIds').count().reset_index()
#dd[dd['SideA.ViewData.Side1_UniqueIds']==1]


# In[497]:


oto_ids_0,oto_ids_1  = one_to_one(X_test_umb,'SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds')
otm_ids = one_to_many(X_test_umb,'SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds')
mto_ids = many_to_one(X_test_umb,'SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds')


# In[498]:


X_test_oto = X_test_umb[(X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(oto_ids_0)) & (X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(oto_ids_1))]


# In[499]:


X_test_oto.shape


# In[500]:


X_test_umb2 = X_test_umb[~(X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(X_test_oto['SideB.ViewData.Side0_UniqueIds'].unique()))]
X_test_umb2 = X_test_umb2.reset_index()
X_test_umb2 = X_test_umb2.drop('index',1)


# In[501]:


X_test_umb2.shape


# In[502]:


#X_test_otm['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[503]:


#X_test_umb[X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(mto_ids)]

X_test_otm = X_test_umb2[X_test_umb2['SideA.ViewData.Side1_UniqueIds'].isin(otm_ids)]

X_test_umb3 = X_test_umb2[~((X_test_umb2['SideA.ViewData.Side1_UniqueIds'].isin(X_test_otm['SideA.ViewData.Side1_UniqueIds'].unique())) | (X_test_umb2['SideB.ViewData.Side0_UniqueIds'].isin(X_test_otm['SideB.ViewData.Side0_UniqueIds'].unique())))]
X_test_umb3 = X_test_umb3.reset_index()
X_test_umb3 = X_test_umb3.drop('index',1)


# In[504]:


X_test_umb3.shape


# In[505]:


X_test_mto = X_test_umb3[X_test_umb3['SideB.ViewData.Side0_UniqueIds'].isin(mto_ids)]


# In[506]:


X_test['ViewData.Combined Transaction Type'].nunique()


# In[507]:


print("The shape of one to one is ", X_test_oto.shape)
print("The shape of one to many is ", X_test_otm.shape)
print("The shape of many to one is ", X_test_mto.shape)


# In[508]:


X_test_mtm = X_test_umb3[~((X_test_umb3['SideB.ViewData.Side0_UniqueIds'].isin(X_test_mto['SideB.ViewData.Side0_UniqueIds'])) | (X_test_umb3['SideA.ViewData.Side1_UniqueIds'].isin(X_test_mto['SideA.ViewData.Side1_UniqueIds'])))]


# In[509]:


print("The shape of many to many is ", X_test_mtm.shape)


# In[510]:


print(get_relation(X_test_mtm, 'SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'))


# In[511]:


X_test_mtm['SideB.ViewData.Side0_UniqueIds'].value_counts()


# In[512]:


X_test_umb[X_test_umb['SideB.ViewData.Side0_UniqueIds']=='255_153157549_Advent Geneva']


# In[533]:


X_test[(X_test['SideB.ViewData.Side0_UniqueIds']=='170_153156543_Advent Geneva')]


# In[170]:


pd.pivot_table(X_test, values='ViewData.Combined Fund', index=['ViewData.Combined Transaction Type'],  columns=['Predicted_action'], aggfunc='count').reset_index().to_csv('trans_type_testing_distribution.csv')


# In[547]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds']=='1141_153157324_CITI')& (X_test['probability_UMB']>0.05)]


# In[ ]:





# In[296]:


X_test[(X_test['SideB.ViewData.Side0_UniqueIds']=='170_153156543_Advent Geneva') & (X_test['Predicted_action']=='UMB_One_to_One')]


# In[ ]:


100_153157424_Morgan Stanley,101_153157424_Morgan Stanley,45_153157424_Morgan Stanley,46_153157424_Morgan Stanley

62_153157356_JP Morgan,66_153157356_JP Morgan,64_153157356_JP Morgan

125_153157587_Advent Geneva,126_153157587_Advent Geneva,127_153157587_Advent Geneva

check_ids_0 = ['602_153153147_Credit suisse','635_153157324_CITI','1294_153157324_CITI',
               '738_153157324_CITI','953_153157324_CITI','778_153157324_CITI','402_153157324_CITI',
               '1018_153157324_CITI','579_153157324_CITI','878_153157324_CITI','656_153157324_CITI',
               '620_153157324_CITI','593_153157324_CITI','629_153157324_CITI','13_153157547_CITI',
               '880_153157324_CITI','1340_153157324_CITI','828_153157324_CITI','1025_153157324_CITI',
               '860_153157324_CITI','943_153157324_CITI','994_153157324_CITI','658_153157324_CITI',
               '1021_153157324_CITI','737_153157324_CITI','362_153157324_CITI','954_153157324_CITI',
               '981_153157324_CITI','1141_153157324_CITI']


# ## Prediction Table

# In[134]:


X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & ((X_test['Amount_diff_2']!=0) | (X_test['Amount_diff_2']!=0)),'Predicted_action'] = 'Unrecognized' 


# In[135]:


pd.set_option('max_columns', 100)


# In[175]:


X_test[X_test['Predicted_action']=='UMB_One_to_One']


# In[137]:


test_file[['SideA.ViewData.Price','SideA.ViewData.Quantity', 'SideA.ViewData.Transaction Type',
 'SideA.ViewData.Trade Date','SideA.ViewData.Settle Date', 'SideA.ISIN_NA','SideA.ViewData.CUSIP','SideA.ViewData.ISIN','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side']]


# In[138]:


###### Probability filter for UMT and UMB ################

#X_test.loc[(X_test['Predicted_action']=='UMT_One_to_One') & (X_test['probability_UMT']<0.90) & (X_test['probability_No_pair']>0.05),'Predicted_action'] = 'No-Pair' 

#X_test.loc[(X_test['Predicted_action']=='UMB_One_to_One') & (X_test['probability_UMB']<0.75) & (X_test['probability_No_pair']>0.2),'Predicted_action'] = 'No-Pair' 

#X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['probability_UMR']<0.90) & (X_test['probability_No_pair']>0.05),'Predicted_actionX_test.loc[(X_test['Predicted_action']=='No-Pair') & (X_test['probability_No_pair']<0.9) & (X_test['probability_UMB']>0.05),'Predicted_action'] = 'UMB_One_to_One' 


#X_test.loc[(X_test['Predicted_action']=='No-Pair') & (X_test['probability_No_pair']<0.95) & (X_test['probability_UMB']>0.05),'Predicted_action'] = 'UMB_One_to_One' 

#X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Settle_date_diff']>4),'Predicted_action'] = 'No-Pair' 
#X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Settle_date_diff']<-4),'Predicted_action'] = 'No-Pair' 


# In[139]:


#X_test.loc[(X_test['SideB.ViewData.Status']=='SDB') & (X_test['SideA.ViewData.Status']=='OB') & (X_test['Predicted_action']=='No-Pair'),'Predicted_action'] = 'SDB/Open Break'


# In[176]:


prediction_table =  X_test.groupby('SideB.ViewData.BreakID_B_side')['Predicted_action'].unique().reset_index()


# In[177]:


prediction_table


# In[178]:


#X_test[X_test['SideB.ViewData.BreakID_B_side']==82817946]


# In[179]:


#prob1 = X_test.groupby('SideB.ViewData.BreakID_B_side')['probability_No_pair'].mean().reset_index()


# In[180]:


prediction_table['len'] = prediction_table['Predicted_action'].str.len()


# In[181]:





# In[182]:


prediction_table['len'] = prediction_table['Predicted_action'].str.len()
prediction_table['No_Pair_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
prediction_table['UMR_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)


# In[183]:


prediction_table


# In[184]:


X_test[X_test['Predicted_action']=='UMB_One_to_One']


# In[190]:


X_test


# In[185]:


umr_array = X_test[X_test['Predicted_action']=='UMR_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
umt_array = X_test[X_test['Predicted_action']=='UMT_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
umb_array = X_test[X_test['Predicted_action']=='UMB_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()


# In[191]:


umr_array_ids = X_test[X_test['Predicted_action']=='UMR_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
umt_array_ids = X_test[X_test['Predicted_action']=='UMT_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
umb_array_ids = X_test[X_test['Predicted_action']=='UMB_One_to_One'].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()


# In[186]:


umr_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMR_array']
umt_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMT_array']
umb_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMB_array']


# In[192]:


umr_array_ids.columns = ['SideB.ViewData.Side0_UniqueIds', 'Predicted_UMR_ids']
umt_array_ids.columns = ['SideB.ViewData.Side0_UniqueIds', 'Predicted_UMT_ids']
umb_array_ids.columns = ['SideB.ViewData.Side0_UniqueIds', 'Predicted_UMB_ids']


# In[187]:


prediction_table = pd.merge(prediction_table,umr_array, on='SideB.ViewData.BreakID_B_side', how='left' )
prediction_table = pd.merge(prediction_table,umt_array, on='SideB.ViewData.BreakID_B_side', how='left' )
prediction_table = pd.merge(prediction_table,umb_array, on='SideB.ViewData.BreakID_B_side', how='left' )


# In[197]:


prediction_table_ids = X_test.groupby('SideB.ViewData.Side0_UniqueIds')['Predicted_action'].unique().reset_index()


# In[198]:


prediction_table_ids = pd.merge(prediction_table_ids,umr_array_ids, on='SideB.ViewData.Side0_UniqueIds', how='left')
prediction_table_ids = pd.merge(prediction_table_ids,umt_array_ids, on='SideB.ViewData.Side0_UniqueIds', how='left')
prediction_table_ids = pd.merge(prediction_table_ids,umb_array_ids, on='SideB.ViewData.Side0_UniqueIds', how='left')


# In[ ]:





# In[221]:


check_ids = ['16_153157591_Advent Geneva','30_153157422_Advent Geneva','78_153156543_Advent Geneva',
             '182_153156543_Advent Geneva','170_153156543_Advent Geneva','567_153157547_Advent Geneva']


check_ids_0 = ['602_153153147_Credit suisse','635_153157324_CITI','1294_153157324_CITI',
               '738_153157324_CITI','953_153157324_CITI','778_153157324_CITI','402_153157324_CITI',
               '1018_153157324_CITI','579_153157324_CITI','878_153157324_CITI','656_153157324_CITI',
               '620_153157324_CITI','593_153157324_CITI','629_153157324_CITI','13_153157547_CITI',
               '880_153157324_CITI','1340_153157324_CITI','828_153157324_CITI','1025_153157324_CITI',
               '860_153157324_CITI','943_153157324_CITI','994_153157324_CITI','658_153157324_CITI',
               '1021_153157324_CITI','737_153157324_CITI','362_153157324_CITI','954_153157324_CITI',
               '981_153157324_CITI','1141_153157324_CITI']


# In[224]:


#prediction_table_ids[prediction_table_ids['SideA.ViewData.Side1_UniqueIds'].isin(check_ids_0)]


# In[251]:


for i, ids in enumerate(check_ids_0):
    if i==9:
        dummy = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==ids]


# In[252]:


dummy[dummy['Predicted_action']!='No-Pair'][['SideA.ViewData.Side1_UniqueIds','SideB.ViewData.Side0_UniqueIds']]


# In[275]:


#prediction_table
X_test[(X_test['SideA.ViewData.Side1_UniqueIds']=='878_153157324_CITI') & (X_test['probability_UMB']>0.02)]


# In[189]:


prediction_table


# In[153]:


prediction_table['Final_prediction'] = prediction_table.apply(lambda x: 'UMR_One_to_One' if x['UMR_flag']==1 else('UMT_One_to_One' if x['len']==1 and x['UMT_flag']==1 else('UMB_One_to_UMB' if x['len']==1 and x['UMB_flag']==1 else('No-Pair' if x['len']==1 else 'Undecided'))), axis=1)


# In[154]:


prediction_table['Final_prediction'].value_counts()


# In[155]:


prediction_table['Final_prediction'].value_counts()


# In[156]:


prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)


# In[157]:


prediction_table.loc[(prediction_table['UMB_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMB_One_to_One'
prediction_table.loc[(prediction_table['UMT_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMT_One_to_One'


# In[158]:


prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['len']==2),'Final_prediction']='No-Pair/Unrecognized'


# In[159]:


prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMT_flag']==1),'Final_prediction']='UMT_One_to_One'


# In[160]:


prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMB_flag']==1),'Final_prediction']='UMB_One_to_One'


# In[161]:


prediction_table['Final_prediction'].value_counts()


# In[162]:


prediction_table[prediction_table['SideB.ViewData.BreakID_B_side'] == 1334352050]


# In[163]:


#X_test[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['SideB.ViewData.BreakID_B_side']==1346769635)]


# In[164]:


prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Predicted_UMR_array']


# In[165]:


prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Predicted_UMT_array']
prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Predicted_UMB_array']
#prediction_table.loc[prediction_table['Final_prediction']=='No-Pair', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='No-Pair', '']


# In[166]:


prediction_table['predicted_break_len'] = prediction_table['Final_predicted_break'].str.len()


# In[167]:


prediction_table[prediction_table['SideB.ViewData.BreakID_B_side'] == 1334352050]


# In[168]:


#prediction_table[(prediction_table['predicted_break_len']>1) & (prediction_table['Final_prediction']=='UMT_One_to_One')]


# In[169]:


#prediction_table[['SideB.ViewData.BreakID_B_side', 'Final_prediction', 'Final_predicted_break']]


# In[170]:


X_test['prob_key'] = X_test['SideB.ViewData.BreakID_B_side'].astype(str) + X_test['Predicted_action']
prediction_table['prob_key'] = prediction_table['SideB.ViewData.BreakID_B_side'].astype(str) + prediction_table['Final_prediction']


# In[171]:


user_prob = X_test.groupby('prob_key')[['probability_UMR','probability_UMT','probability_UMB']].max().reset_index()
open_prob = X_test.groupby('prob_key')['probability_No_pair'].mean().reset_index()


# In[172]:


#prediction_table = prediction_table.drop(,1)

prediction_table = pd.merge(prediction_table,user_prob, on='prob_key', how='left')
prediction_table = pd.merge(prediction_table,open_prob, on='prob_key', how='left')


# In[173]:


prediction_table = prediction_table.drop('prob_key',1)


# In[174]:


prediction_table[prediction_table['SideB.ViewData.BreakID_B_side'] == 1334352050]


# In[175]:


prediction_table


# In[176]:


prediction_table = pd.merge(prediction_table, X_test[['SideB.ViewData.BreakID_B_side','SideA.ViewData._ID','SideB.ViewData._ID']].drop_duplicates(['SideB.ViewData.BreakID_B_side','SideB.ViewData._ID']), on ='SideB.ViewData.BreakID_B_side', how='left')


# In[177]:


prediction_table[prediction_table['SideB.ViewData.BreakID_B_side'] ==1334352050]


# In[178]:


prediction_table.dtypes


# ## Merging PB side Break ID's

# In[5065]:


#pb_break_ids = prediction_table[~prediction_table['Final_predicted_break'].isnull()][['Final_prediction','Final_predicted_break']]


# In[4358]:


#pb_break_ids = pb_break_ids.reset_index()
#pb_break_ids = pb_break_ids.drop('index',1)


# In[1706]:


#pb_break_ids['Final_predicted_break'] = pb_break_ids['Final_predicted_break'].apply(lambda x: str(x).replace("[",''))
#pb_break_ids['Final_predicted_break'] = pb_break_ids['Final_predicted_break'].apply(lambda x: str(x).replace("]",''))


# In[2444]:


#pb_break_ids['Final_predicted_break'].unique()


# In[1708]:


#id_list = []
#id_list2 = []

#for i in pb_break_ids['Final_predicted_break'].unique():
#    id_list.append(i.split(' '))
#for j in np.concatenate(id_list,axis=0):
#    if j!='':
#        id_list2.append(j.replace("\n",''))


# In[1709]:


#new_ob_ids =[]
#
#for i in X_test['SideA.ViewData.BreakID_A_side'].astype(str).unique():
#    if i not in np.array(id_list2,dtype="O"):
#        new_ob_ids.append(i)


# In[1710]:


#prediction_table2 = pd.DataFrame(np.array(new_ob_ids))


# In[1711]:


#prediction_table2.columns = ['SideB.ViewData.BreakID_B_side']


# In[1712]:


#prediction_table2['Final_prediction'] = 'No-Pair'


# In[1713]:


#prediction_table2['Side'] = 'P-B Side'


# In[1714]:


#prediction_table['Side'] = 'Accounting Side'


# In[2074]:


#prediction_table3 = prediction_table


# In[2442]:


#prediction_table3 = pd.concat([prediction_table, prediction_table2], axis=0)


# In[1716]:


#prediction_table3 = prediction_table3.reset_index()
#prediction_table3 = prediction_table3.drop('index',1)


# In[1717]:


#prediction_table3 = prediction_table3[prediction_table.columns]


# In[2443]:


#prediction_table3[['SideB.ViewData.BreakID_B_side', 'Final_prediction', 'Final_predicted_break','Side']]


# In[1719]:


#ids_for_comment = prediction_table3[['SideB.ViewData.BreakID_B_side', 'Final_prediction', 'Final_predicted_break','Side']]


# In[2445]:


#ids_for_comment


# In[1721]:


#ids_for_comment.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/Input for comment prediction/prediction_table_testing_HST_RecData_125_1159652110_06-19-2020.csv')


# In[6161]:


prediction_table[prediction_table['SideB.ViewData.BreakID_B_side'] == 144895568]


# In[6169]:


data = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/MEO/MeoCollections_SOROS.MEO_HST_RecData_153_2020-06-11.csv')


# In[6175]:


data['ViewData.Status'].value_counts()


# In[6174]:


data[data['ViewData.Side1_UniqueIds']=='356_153155545_Credit suisse']['ViewData.Status']


# In[6173]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds']=='356_153155545_Credit suisse']

X_test[X_test['SideB.ViewData.BreakID_B_side']==144895379]


# ## Merging with User Action Data

# In[179]:


prediction_table3 = prediction_table


# In[180]:


aua = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/AUA/AUACollections_SOROS.AUA_HST_RecData_153_2020-06-25.csv')


# In[181]:


#aua = /Soros/JuneData/MEO/MeoCollections_SOROS.AUA_HST_RecData_153_2020-06-25.csv"


# In[182]:


#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test/x_test_2020-06-29.csv")


# In[183]:


aua.shape


# In[184]:


aua = aua[~((aua['LastPerformedAction']==0) & (aua['ViewData.Status']=='SDB'))]
aua = aua.reset_index()
aua = aua.drop('index',1)


# In[185]:


aua['ViewData.Status'].value_counts()


# In[186]:


aua = aua[aua['ViewData.Status'].isin(['UMR','UMB','UMT','OB','SDB','UCB'])]
aua = aua.reset_index()
aua = aua.drop('index',1)


# In[187]:


aua['ViewData.Status'].value_counts()


# In[188]:


prediction_table3


# In[189]:


aua_id_match = aua[['MetaData.0._ParentID','ViewData.Status','ViewData.Age','ViewData.BreakID','ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]

aua_id_match.columns = ['SideB.ViewData._ID','Actual_Status','ViewData.Age','ViewData.BreakID','AUA_ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']

aua_id_match = aua_id_match.drop_duplicates()
aua_id_match = aua_id_match.reset_index()
aua_id_match = aua_id_match.drop('index',1)

########################################################################################################
aua_open_status = aua[['ViewData.BreakID','ViewData.Status']]

aua_open_status.columns = ['SideB.ViewData.BreakID_B_side','Actual_Status_Open']
aua_open_status = aua_open_status.drop_duplicates()
aua_open_status = aua_open_status.reset_index()
aua_open_status = aua_open_status.drop('index',1)


# In[190]:


aua_id_match


# In[191]:


aua_open_status['SideB.ViewData.BreakID_B_side'] = aua_open_status['SideB.ViewData.BreakID_B_side'].astype(int).astype(str)
prediction_table3['SideB.ViewData.BreakID_B_side'] = prediction_table3['SideB.ViewData.BreakID_B_side'].astype(int).astype(str)


# In[192]:


aua_id_match.shape


# In[193]:


prediction_table3['SideB.ViewData._ID'] = prediction_table3['SideB.ViewData._ID'].fillna('Not_generated')
prediction_table3['SideA.ViewData._ID'] = prediction_table3['SideA.ViewData._ID'].fillna('Not_generated')


# In[194]:


prediction_table3[prediction_table3['SideB.ViewData.BreakID_B_side'] == '1334352050']


# In[195]:


#aua_id_match['len_side0'] = aua_id_match.apply(lambda x: len(x['Actual_Status'].split(',')), axis=1)
#aua_id_match['len_side1'] = aua_id_match.apply(lambda x: len(x['Actual_Status'].split(',')), axis=1)


# In[196]:


#aua_one_side = aua_id_match.groupby(['ViewData.Side1_UniqueIds'])['Actual_Status'].unique().reset_index()
#aua_zero_side = aua_id_match.groupby(['ViewData.Side0_UniqueIds'])['Actual_Status'].unique().reset_index()


# In[197]:


aua_id_match['combined_flag'] = aua_id_match.apply(lambda x: 1 if 'Combined' in x['AUA_ViewData._ID'] else 0,axis=1)


# In[198]:


#aua_id_match[''.sort_values(['ViewData.Side0_UniqueIds'])


# In[199]:


aua_id_match1  = aua_id_match[aua_id_match['combined_flag']!=1]
aua_id_match1 = aua_id_match1.reset_index()
aua_id_match1 = aua_id_match1.drop('index',1)


# In[200]:


side1_repeat = aua_id_match1['ViewData.Side1_UniqueIds'].value_counts().reset_index()
side0_repeat = aua_id_match1['ViewData.Side0_UniqueIds'].value_counts().reset_index()


# In[201]:


side1_repeat


# In[202]:


side1_repeat[side1_repeat['ViewData.Side1_UniqueIds']>1]


# In[203]:


aua_id_match1['1_repeat_flag'] = aua_id_match1.apply(lambda x: 1 if x['ViewData.Side1_UniqueIds'] in side1_repeat[side1_repeat['ViewData.Side1_UniqueIds']>1]['index'].values else 0, axis=1)
aua_id_match1['0_repeat_flag'] = aua_id_match1.apply(lambda x: 1 if x['ViewData.Side0_UniqueIds'] in side0_repeat[side0_repeat['ViewData.Side0_UniqueIds']>1]['index'].values else 0, axis=1)


# In[204]:


aua_id_match2 = aua_id_match1[~((aua_id_match1['1_repeat_flag']==1) & (aua_id_match1['Actual_Status']=='OB'))]
aua_id_match2 = aua_id_match2.reset_index()
aua_id_match2 = aua_id_match2.drop('index',1)


# In[205]:


aua_id_match3 = aua_id_match2[~((aua_id_match2['0_repeat_flag']==1) & (aua_id_match2['Actual_Status']=='OB'))]
aua_id_match3 = aua_id_match3.reset_index()
aua_id_match3 = aua_id_match3.drop('index',1)


# In[206]:



#aua_zero_side['len_side0'].value_counts()
#aua_open_status['SideB.ViewData.BreakID_B_side'].nunique()


# In[207]:


#aua_sub99[aua_sub99['ViewData.Side0_UniqueIds'] == '789_125897734_Advent Geneva']


# In[208]:


aua_id_match3[aua_id_match3['ViewData.Side0_UniqueIds'] == '789_125897734_Advent Geneva']


# In[209]:


aua_id_match3[aua_id_match3['SideB.ViewData._ID'] == '5ee721a315545830acc67099']


# In[210]:


aua_id_match3


# In[211]:


pb_side = X_test.groupby('SideA.ViewData.BreakID_A_side')['Predicted_action'].unique().reset_index()


# In[212]:


pb_side['len'] = pb_side['Predicted_action'].apply(lambda x: len(x))


# In[213]:


pb_side['No_Pair_flag'] = pb_side.apply(lambda x: 1 if 'No-Pair' in x['Predicted_action'] else 0, axis=1)


# In[214]:


pb_side_open_ids = pb_side[(pb_side['len']==1) & (pb_side['No_Pair_flag']==1)]['SideA.ViewData.BreakID_A_side']


# In[ ]:





# In[215]:


prediction_table3[prediction_table3['SideB.ViewData.BreakID_B_side']=='1335439926']


# In[216]:


aua_id_match3[aua_id_match3['ViewData.Side0_UniqueIds'].isnull()]


# In[217]:


prediction_table_new = pd.merge(prediction_table3, aua_id_match3, on='SideB.ViewData._ID', how='left')


# In[218]:


prediction_table_new[prediction_table_new['SideB.ViewData.BreakID_B_side'] == '1334352050']


# In[219]:


prediction_table_new[prediction_table_new['Actual_Status']=='OB']


# In[ ]:





# In[220]:


aua_id_match4 = aua_id_match3.rename(columns = {'ViewData.BreakID': 'SideB.ViewData.BreakID_B_side'})
aua_id_match4 = aua_id_match4.rename(columns = {'Actual_Status': 'Actual_Status_Open'})


# In[221]:


aua_id_match4['SideB.ViewData.BreakID_B_side'] = aua_id_match4['SideB.ViewData.BreakID_B_side'].astype(str)


# In[222]:


#prediction_table_new = pd.merge(prediction_table_new ,aua_open_status, on='SideB.ViewData.BreakID_B_side', how='left')
prediction_table_new = pd.merge(prediction_table_new ,aua_id_match4[['SideB.ViewData.BreakID_B_side','Actual_Status_Open']], on='SideB.ViewData.BreakID_B_side', how='left')


# In[223]:


#prediction_table_new


# In[224]:


prediction_table_new[prediction_table_new['Actual_Status_Open']=='OB']['Final_prediction'].value_counts()


# In[225]:


prediction_table_new.loc[prediction_table_new['Final_prediction']=='No-Pair/Unrecognized','Final_prediction'] = 'No-Pair'


# In[226]:


prediction_table_new.loc[prediction_table_new['Actual_Status'].isnull()]


# In[227]:


prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),'Actual_Status'] = prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),'Actual_Status_Open']


# In[228]:


prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),:]


# In[229]:


prediction_table_new[prediction_table_new['SideB.ViewData.BreakID_B_side'] =='1334352248']


# In[230]:


prediction_table_new.loc[prediction_table_new['Actual_Status']=='OB','Actual_Status'] = 'Open Break'


# In[231]:


prediction_table_new.loc[prediction_table_new['Final_prediction']=='No-Pair','Final_prediction'] = 'Open Break'
prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMR_One_to_One','Final_prediction'] = 'UMR'
prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMT_One_to_One','Final_prediction'] = 'UMT'
prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMB_One_to_One','Final_prediction'] = 'UMB'


# In[232]:


prediction_table_new['Actual_Status'].isnull().sum()


# In[233]:


prediction_table_new = prediction_table_new[~prediction_table_new['Actual_Status'].isnull()]
prediction_table_new = prediction_table_new.reset_index()
prediction_table_new = prediction_table_new.drop('index',1)


# In[234]:


prediction_table_new


# ## Final Actual vs Predicted Table - Process Initiation

# In[236]:


meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/MEO/MeoCollections_SOROS.MEO_HST_RecData_153_2020-06-25.csv",usecols=new_cols)


# In[237]:


meo = meo[['ViewData.BreakID','ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Age','ViewData.Status']].drop_duplicates()


# In[238]:


meo['key'] = meo['ViewData.Side0_UniqueIds'].astype(str) + meo['ViewData.Side1_UniqueIds'].astype(str)


# In[239]:


aua_id_match5 = aua_id_match3.rename(columns ={'Actual_Status': 'ViewData.Status'})


# In[240]:


aua_sub = aua_id_match5[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Age','ViewData.Status']].drop_duplicates()


# In[241]:


aua_sub['key'] = aua_sub['ViewData.Side0_UniqueIds'].astype(str) + aua_sub['ViewData.Side1_UniqueIds'].astype(str)


# In[242]:


prediction_table_new['ViewData.BreakID'] = prediction_table_new['SideB.ViewData.BreakID_B_side']
prediction_table_new['ViewData.BreakID'] = prediction_table_new['ViewData.BreakID'].astype(str)


# In[243]:


meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(str)


# In[244]:


prediction_table_new1 = pd.merge(prediction_table_new, meo[['ViewData.BreakID','key']], on='ViewData.BreakID', how='left')


# In[245]:


prediction_table_new1[prediction_table_new1['Final_prediction']=='UMR']


# In[246]:


prediction_table_new1[prediction_table_new1['ViewData.Side0_UniqueIds']=='789_125897734_Advent Geneva']


# In[247]:


aua_sub1 = pd.merge(aua_sub, prediction_table_new1[['key','Final_prediction','probability_UMR','probability_No_pair','probability_UMT','probability_UMB','Final_predicted_break']], on='key', how='left')


# In[248]:


aua_sub1[aua_sub1['ViewData.Side0_UniqueIds']=='789_125897734_Advent Geneva']


# In[249]:


aua_sub1


# In[250]:


no_open = prediction_table_new1[prediction_table_new1['Final_prediction']!='Open Break'].reset_index()
no_open = no_open.drop('index',1)

no_open['key'] = no_open['ViewData.Side0_UniqueIds'].astype(str) + no_open['ViewData.Side1_UniqueIds'].astype(str)


# In[251]:


#aua_sub1[aua_sub1['Final_prediction']=='UMR_One_to_One']
X_test['key'] = X_test['SideB.ViewData.Side0_UniqueIds'].astype(str) + X_test['SideA.ViewData.Side1_UniqueIds'].astype(str)


# In[252]:


aua_sub[aua_sub['ViewData.Side0_UniqueIds'] == '789_125897734_Advent Geneva']


# In[253]:


aua_sub = pd.merge(aua_sub1, no_open[['key','Final_prediction']], on='key', how='left')


# In[254]:


aua_sub11 = aua_sub1[aua_sub1['Final_prediction']=='Open Break']
aua_sub11 = aua_sub11.reset_index()
aua_sub11 = aua_sub11.drop('index',1)


# In[255]:


aua_sub11['probability_UMR'].fillna(0.00355,inplace=True)
aua_sub11['probability_UMB'].fillna(0.003124,inplace=True)
aua_sub11['probability_UMT'].fillna(0.00255,inplace=True)
aua_sub11['probability_No_pair'].fillna(0.99034,inplace=True)


# In[256]:


aua_sub22 = aua_sub1[aua_sub1['Final_prediction']!='Open Break'][['ViewData.Side1_UniqueIds', 'ViewData.Side0_UniqueIds', 'ViewData.Age','ViewData.Status', 'key']]

aua_sub22 = aua_sub22.reset_index()
aua_sub22 = aua_sub22.drop('index',1)
aua_sub22 = pd.merge(aua_sub22, no_open[['key','Final_prediction','probability_UMR','probability_No_pair','probability_UMT','probability_UMB','Final_predicted_break']], on='key', how='left')
aua_sub22 = aua_sub22.reset_index()
aua_sub22 = aua_sub22.drop('index',1)


# In[257]:


aua_sub33 = pd.concat([aua_sub11,aua_sub22], axis=0)
aua_sub33 = aua_sub33.reset_index()
aua_sub33 = aua_sub33.drop('index',1)


# In[258]:


aua_sub33['ViewData.Side0_UniqueIds'] = aua_sub33['ViewData.Side0_UniqueIds'].astype(str)
aua_sub33['ViewData.Side1_UniqueIds'] = aua_sub33['ViewData.Side1_UniqueIds'].astype(str)


# In[259]:


aua_sub33['len_side0'] = aua_sub33.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
aua_sub33['len_side1'] = aua_sub33.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[260]:


aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']==1) & (aua_sub33['ViewData.Status']=='OB') ,'Type'] = 'One_side_aggregation'
aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']==1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'One_to_Many'

aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']=='OB') ,'Type'] = 'One_side_aggregation'
aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'One_to_Many'
aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'Many_to_Many'

aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']==1) ,'Type'] = 'One_to_One/Open'


# In[261]:


aua_sub44 = aua_sub33[(aua_sub33['ViewData.Status']=='UMB') & (aua_sub33['ViewData.Age']>1)]
aua_sub44 = aua_sub44.reset_index()
aua_sub44 = aua_sub44.drop('index',1)


# In[262]:


aua_sub44['Final_prediction'].fillna('UMB-Carry-Forward',inplace= True)
aua_sub44['probability_UMR'].fillna(0.0001,inplace= True)
aua_sub44['probability_UMB'].fillna(0.9998,inplace= True)
aua_sub44['probability_UMT'].fillna(0.0000,inplace= True)
aua_sub44['probability_No_pair'].fillna(0.0000,inplace= True)


# In[263]:


aua_sub55 = aua_sub33[~((aua_sub33['ViewData.Status']=='UMB') & (aua_sub33['ViewData.Age']>1))]
aua_sub55 = aua_sub55.reset_index()
aua_sub55 = aua_sub55.drop('index',1)


# In[264]:


aua_sub66 = pd.concat([aua_sub55,aua_sub44], axis=0)
aua_sub66 = aua_sub66.reset_index()
aua_sub66 = aua_sub66.drop('index',1)


# In[265]:


aua_sub66.loc[(aua_sub66['ViewData.Status']=='UMB') & (aua_sub66['ViewData.Age']>1),'ViewData.Status'] = 'UMB-Carry-Forward'
aua_sub66.loc[(aua_sub66['ViewData.Status']=='OB'),'ViewData.Status'] = 'Open Break'


# In[266]:


aua_sub66


# ## Read No-Pair Id File

# In[5926]:


no_pair_id_data = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/X_Test/no_pair_ids_125_2020-06-8.csv")


# In[268]:


no_pair_ids


# In[5927]:


no_pair_ids = no_pair_id_data['filter_key'].unique()


# In[269]:


aua_sub66.loc[aua_sub66['ViewData.Side1_UniqueIds'].isin(no_pair_ids),'Final_prediction'] = aua_sub66.loc[aua_sub66['ViewData.Side1_UniqueIds'].isin(no_pair_ids),'ViewData.Status']
aua_sub66.loc[aua_sub66['ViewData.Side0_UniqueIds'].isin(no_pair_ids),'Final_prediction'] = aua_sub66.loc[aua_sub66['ViewData.Side0_UniqueIds'].isin(no_pair_ids),'ViewData.Status']


# In[270]:


aua_sub66['Type'].value_counts()


# In[271]:


#aua_sub66


# In[272]:


pb_side_grp = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()


# In[273]:


pb_side_grp


# In[274]:


pb_side_grp_status = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideA.ViewData.Status'].unique().reset_index()
pb_side_grp_status['SideA.ViewData.Status'] = pb_side_grp_status['SideA.ViewData.Status'].apply(lambda x: str(x).replace("[",""))
pb_side_grp_status['SideA.ViewData.Status'] = pb_side_grp_status['SideA.ViewData.Status'].apply(lambda x: str(x).replace("]",""))
pb_side_grp['len'] = pb_side_grp.apply(lambda x: len(x['Predicted_action']), axis=1)
pb_side_grp['No_pair_flag'] = pb_side_grp.apply(lambda x: 1 if x['len'] == 1 and "No-Pair" in x['Predicted_action'] else 0, axis=1)


# In[275]:


pb_side_grp = pd.merge(pb_side_grp,pb_side_grp_status, on='SideA.ViewData.Side1_UniqueIds', how='left')


# In[276]:


#pb_side_grp['SideA.ViewData.Status'].value_counts()


# In[277]:


#pb_side_grp = pd.merge(pb_side_grp,pb_side_grp_status, on='SideA.ViewData.Side1_UniqueIds', how='left')
pb_side_grp['Final_status'] = pb_side_grp.apply(lambda x: "Open Break" if x['SideA.ViewData.Status']=="'OB'" else("SDB" if x['SideA.ViewData.Status']=="'SDB'" else "NA"),axis=1)
pb_side_grp = pb_side_grp.rename(columns = {'SideA.ViewData.Side1_UniqueIds':'ViewData.Side1_UniqueIds'})



pb_side_grp1 = pb_side_grp[pb_side_grp['No_pair_flag']==1]
pb_side_grp1 = pb_side_grp1.reset_index()
pb_side_grp1 = pb_side_grp1.drop('index',1)


# In[278]:


aua_sub77 = pd.merge(aua_sub66 ,pb_side_grp1[['ViewData.Side1_UniqueIds','Final_status']], on ='ViewData.Side1_UniqueIds',how='left')


# In[279]:


aua_sub77.loc[(~aua_sub77['Final_status'].isnull()) & (aua_sub77['ViewData.Side0_UniqueIds']=='nan'),'Final_prediction'] = aua_sub77.loc[(~aua_sub77['Final_status'].isnull()) & (aua_sub77['ViewData.Side0_UniqueIds']=='nan'),'Final_status']


# In[280]:


pb_side_grp_B = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()


# In[281]:


pb_side_grp_B_status = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['SideB.ViewData.Status'].unique().reset_index()
pb_side_grp_B_status['SideB.ViewData.Status'] = pb_side_grp_B_status['SideB.ViewData.Status'].apply(lambda x: str(x).replace("[",""))
pb_side_grp_B_status['SideB.ViewData.Status'] = pb_side_grp_B_status['SideB.ViewData.Status'].apply(lambda x: str(x).replace("]",""))
pb_side_grp_B['len'] = pb_side_grp_B.apply(lambda x: len(x['Predicted_action']), axis=1)
pb_side_grp_B['No_pair_flag'] = pb_side_grp_B.apply(lambda x: 1 if x['len'] == 1 and "No-Pair" in x['Predicted_action'] else 0, axis=1)


# In[282]:


pb_side_grp_B = pd.merge(pb_side_grp_B,pb_side_grp_B_status, on='SideB.ViewData.Side0_UniqueIds', how='left')
pb_side_grp_B['Final_status_B'] = pb_side_grp_B.apply(lambda x: "Open Break" if x['SideB.ViewData.Status']=="'OB'" else("SDB" if x['SideB.ViewData.Status']=="'SDB'" else "NA"),axis=1)
pb_side_grp_B = pb_side_grp_B.rename(columns = {'SideB.ViewData.Side0_UniqueIds':'ViewData.Side0_UniqueIds'})



pb_side_grp2 = pb_side_grp_B[pb_side_grp_B['No_pair_flag']==1]
pb_side_grp2 = pb_side_grp2.reset_index()
pb_side_grp2 = pb_side_grp2.drop('index',1)


# In[283]:


aua_sub88 = pd.merge(aua_sub77 ,pb_side_grp2[['ViewData.Side0_UniqueIds','Final_status_B']], on ='ViewData.Side0_UniqueIds',how='left')


# In[284]:


aua_sub88.loc[(~aua_sub88['Final_status_B'].isnull()) & (aua_sub88['ViewData.Side1_UniqueIds']=='nan'),'Final_prediction'] = aua_sub88.loc[(~aua_sub88['Final_status_B'].isnull()) & (aua_sub88['ViewData.Side1_UniqueIds']=='nan'),'Final_status_B']


# In[285]:


aua_sub99 = aua_sub88[(aua_sub88['ViewData.Status']!='SDB')]
aua_sub99 = aua_sub99.reset_index()
aua_sub99 = aua_sub99.drop('index',1)


# In[286]:


aua_sub99['Final_prediction'] = aua_sub99['Final_prediction'].fillna('Open Break')
aua_sub99 = aua_sub99.reset_index()
aua_sub99 = aua_sub99.drop('index',1)


# In[287]:


aua_sub99['ViewData.Status'] = aua_sub99['ViewData.Status'].astype(str)
aua_sub99['Final_prediction'] = aua_sub99['Final_prediction'].astype(str)


# In[288]:


#X_test
aua_sub[aua_sub['ViewData.Side0_UniqueIds']=='789_125897734_Advent Geneva']


# In[289]:


#aua[aua['ViewData.Side0_UniqueIds'] == '789_125897734_Advent Geneva']


# ## Summary file 

# In[290]:


break_id_merge = meo[meo['ViewData.Status'].isin(['OB','SDB','UOB','UDB','SPM'])][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID']].drop_duplicates()
break_id_merge = break_id_merge.reset_index()
break_id_merge = break_id_merge.drop('index',1)


# In[291]:


break_id_merge


# In[292]:


break_id_merge['key'] = break_id_merge['ViewData.Side0_UniqueIds'].astype(str) + break_id_merge['ViewData.Side1_UniqueIds'].astype(str)


# In[293]:


final = pd.merge(aua_sub99,break_id_merge[['key','ViewData.BreakID']], on='key', how='left')


# In[294]:


aua_sub[aua_sub['ViewData.Side1_UniqueIds']=='233_125897734_Morgan Stanley']


# In[295]:


break_id_merge[break_id_merge['ViewData.BreakID']=='1334352248']


# In[296]:


break_id_merge.dtypes


# In[297]:


#final[final['ViewData.BreakID'].isnull()]

final = pd.merge(final,break_id_merge[['ViewData.Side0_UniqueIds','ViewData.BreakID']], on='ViewData.Side0_UniqueIds', how='left')


# In[298]:


final.loc[final['ViewData.BreakID_x'].isnull(),'ViewData.BreakID_x'] = final.loc[final['ViewData.BreakID_x'].isnull(),'ViewData.BreakID_y']


# In[299]:


final = final.rename(columns={'ViewData.BreakID_x':'ViewData.BreakID'})
final = final.drop('ViewData.BreakID_y',1)


# In[ ]:





# In[300]:


final1 = final[(final['Type']=='One_to_One/Open') & (final['probability_No_pair'].isnull())]
final1 = final1.reset_index()
final1 = final1.drop('index',1)


final2 = final[~((final['Type']=='One_to_One/Open') & (final['probability_No_pair'].isnull()))]
final2 = final2.reset_index()
final2 = final2.drop('index',1)


# In[301]:


final1['probability_UMR'].fillna(0.0024,inplace=True)
final1['probability_UMB'].fillna(0.004124,inplace=True)
final1['probability_UMT'].fillna(0.00155,inplace=True)
final1['probability_No_pair'].fillna(0.9922,inplace=True)


# In[302]:


final3 = pd.concat([final1, final2], axis=0)


# In[303]:


final3['ML_flag'] = final3.apply(lambda x: "ML" if x['Type']=='One_to_One/Open' else "Non-ML", axis=1)


# In[304]:


prediction_cols = ['ViewData.BreakID', 'ViewData.Side1_UniqueIds', 'ViewData.Side0_UniqueIds','ViewData.Age' ,
       'probability_No_pair', 'probability_UMR','probability_UMB', 'probability_UMT',
       'Final_predicted_break', 'Type', 'ML_flag','ViewData.Status', 'Final_prediction']


final4 = final3[prediction_cols]

final4 = final4.rename(columns ={'ViewData.Status':'Actual_Status', 'Final_prediction': 'Predicted_Status'})


# In[305]:


final4[final4['ML_flag']=='ML']


# In[306]:


#crosstab_table


# In[307]:


NA_status_file = final4[(final4['Type']=='One_to_One/Open') & (final4['Predicted_Status']=='NA')]
NA_status_file = NA_status_file.reset_index()
NA_status_file = NA_status_file.drop('index',1)


# In[308]:


final5 = final4[~((final4['Type']=='One_to_One/Open') & (final4['Predicted_Status']=='NA'))]
final5 = final5.reset_index()
final5 = final5.drop('index',1)


# In[309]:


NA_status_file_A_side = NA_status_file[NA_status_file['ViewData.Side0_UniqueIds']=='nan']
NA_status_file_B_side = NA_status_file[NA_status_file['ViewData.Side1_UniqueIds']=='nan']


# In[310]:


gg = X_test[X_test['SideA.ViewData.BreakID_A_side'].isin(NA_status_file_A_side['ViewData.BreakID'].unique())].groupby(['SideA.ViewData.BreakID_A_side'])['Predicted_action'].unique().reset_index()
gg.columns = ['ViewData.BreakID','Predicted_action']
gg['NA_prediction_A'] = 'Open Break'

kk = X_test[X_test['SideB.ViewData.BreakID_B_side'].isin(NA_status_file_B_side['ViewData.BreakID'].unique())].groupby(['SideB.ViewData.BreakID_B_side'])['Predicted_action'].unique().reset_index()
kk.columns = ['ViewData.BreakID','Predicted_action']
kk['NA_prediction_B'] = 'Open Break'


# In[311]:


gg['ViewData.BreakID'] = gg['ViewData.BreakID'].astype(str)
kk['ViewData.BreakID'] = kk['ViewData.BreakID'].astype(str)


# In[312]:


final6 = pd.merge(NA_status_file, gg[['ViewData.BreakID','NA_prediction_A']], on='ViewData.BreakID', how='left')
final6 = pd.merge(final6, kk[['ViewData.BreakID','NA_prediction_B']], on='ViewData.BreakID', how='left')


# In[313]:


final6.loc[final6['NA_prediction_A'].isnull(),'Predicted_Status'] = 'Open Break'
final6.loc[final6['NA_prediction_B'].isnull(),'Predicted_Status'] = 'Open Break'


# In[314]:


final6 = final6.drop(['NA_prediction_A','NA_prediction_B'],1)


# In[315]:


final5[final5['ViewData.Side0_UniqueIds']=='789_125897734_Advent Geneva']


# In[316]:


final7 = pd.concat([final5, final6], axis=0)
final7 = final7.reset_index()
final7 = final7.drop('index',1)


# In[317]:


X_test.head(3)


# In[318]:


pair_match = X_test[X_test['Predicted_action']!='No-Pair']
pair_match = pair_match.reset_index()
pair_match = pair_match.drop('index',1)


# In[319]:


pair_match = pair_match[['Predicted_action',
       'probability_No_pair', 'probability_UMB', 'probability_UMR',
       'probability_UMT', 'key']]
pair_match.columns = ['New_Predicted_action',
       'New_probability_No_pair', 'New_probability_UMB', 'New_probability_UMR',
       'New_probability_UMT','key']


# In[320]:


pair_match['New_Predicted_action'] = pair_match['New_Predicted_action'].apply(lambda x: 'UMR' if x=='UMR_One_to_One' else("UMT" if x=='UMT_One_to_One' else("UMB" if x== "UMB_One_to_One" else x)))


# In[321]:


final7['key'] = final7['ViewData.Side0_UniqueIds'].astype(str) + final7['ViewData.Side1_UniqueIds'].astype(str)


# In[322]:


final8 = pd.merge(final7,pair_match, on='key', how='left')


# In[323]:


final8[~final8['New_Predicted_action'].isnull()]['Predicted_Status'].value_counts()


# In[324]:


final8[~final8['New_Predicted_action'].isnull()]['Actual_Status'].value_counts()


# In[325]:


final8[~final8['New_Predicted_action'].isnull()]['New_Predicted_action'].value_counts()


# In[326]:


final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'Predicted_Status'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_Predicted_action']
final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_No_pair'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_No_pair']
final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_UMB'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_UMB']
final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_UMR'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_UMR']
final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_UMT'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_UMT']


# In[327]:


final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'probability_UMT'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'New_probability_UMT']
final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'probability_UMR'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'New_probability_UMR']
final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'probability_UMB'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'New_probability_UMB']
#final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='Open Break'),'probability_UMB'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'New_probability_UMB']


final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'probability_No_Pair'] = 0.002
final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'probability_No_Pair'] = 0.002
final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'probability_No_Pair'] = 0.002


# In[328]:


umr_break_array_match = prediction_table[prediction_table['Final_prediction']=='UMR_One_to_One'][['SideB.ViewData.BreakID_B_side','Final_predicted_break']]
umt_break_array_match = prediction_table[prediction_table['Final_prediction']=='UMT_One_to_One'][['SideB.ViewData.BreakID_B_side','Final_predicted_break']]

umb_break_array_match = prediction_table[prediction_table['Final_prediction']=='UMB_One_to_One'][['SideB.ViewData.BreakID_B_side','Final_predicted_break']]

umr_break_array_match.columns = np.array(['ViewData.BreakID','New_Final_predicted_break_UMR'])
umt_break_array_match.columns = np.array(['ViewData.BreakID','New_Final_predicted_break_UMT'])
umb_break_array_match.columns = np.array(['ViewData.BreakID','New_Final_predicted_break_UMB'])


# In[329]:


#umr_break_array_match['New_Final_predicted_break_UMR'] = umr_break_array_match['New_Final_predicted_break_UMR'].astype(str) 
#umb_break_array_match['New_Final_predicted_break_UMB'] = umb_break_array_match['New_Final_predicted_break_UMB'].astype(str) 
#umt_break_array_match['New_Final_predicted_break_UMT'] = umt_break_array_match['New_Final_predicted_break_UMT'].astype(str) 


# In[330]:


final9 = pd.merge(final8, umr_break_array_match, on ='ViewData.BreakID', how='left')
final9 = pd.merge(final9, umt_break_array_match, on ='ViewData.BreakID', how='left')
final9 = pd.merge(final9, umb_break_array_match, on ='ViewData.BreakID', how='left')


# In[331]:


final9.loc[(final9['Final_predicted_break'].isnull()) & (final9['Predicted_Status']=='UMT'),'Final_predicted_break'] = final9.loc[(final9['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'New_Final_predicted_break_UMT']
final9.loc[(final9['Final_predicted_break'].isnull()) & (final9['Predicted_Status']=='UMR'),'Final_predicted_break'] = final9.loc[(final9['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'New_Final_predicted_break_UMR']
final9.loc[(final9['Final_predicted_break'].isnull()) & (final9['Predicted_Status']=='UMB'),'Final_predicted_break'] = final9.loc[(final9['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'New_Final_predicted_break_UMB']


# In[332]:


#final9[(final9['Actual_Status']=='UMB') & (final9['Predicted_Status']=='UMB') & (final9['ML_flag']=='ML')]['Final_predicted_break']


# In[333]:


final9.columns


# In[334]:


final9 = final9.drop(['key','New_Predicted_action',
       'New_probability_No_pair', 'New_probability_UMB', 'New_probability_UMR',
       'New_probability_UMT','New_Final_predicted_break_UMR',
       'New_Final_predicted_break_UMT', 'New_Final_predicted_break_UMB'], 1)



# In[335]:


final9


# In[336]:


#final8['Type'].value_counts()


# In[337]:


#meo1 = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_125_2020-06-8.csv",usecols=new_cols)


# In[338]:


#meo1[meo1['ViewData.Side1_UniqueIds']=='6_125858636_Goldman Sachs']


# ## Merging columns from the transaction table

# In[344]:


import pandas as pd

cols_to_show = [
'Account Type',
'Accounting Net Amount',
#'Accounting Net Amount Difference',
#'Activity Code',
'Age',
'Alt ID 1',
'Asset Type Category',
#'Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference',
#'B-P Net Amount Difference Absolute',
'BreakID',
'Business Date',
#'Call Put Indicator',
'Cancel Amount',
'Cancel Flag',
'Commission',
'Currency',
'CUSIP',
'Custodian',
#'Custodian Account',
'Department',
'Description',
'ExpiryDate',
'ExternalComment2',
'Fund',
#'FX Rate',
#'Interest Amount',
'InternalComment2',
'Investment ID',
'Investment Type',
'Is Combined Data',
'ISIN',
'Keys',
'Knowledge Date',
'Mapped Custodian Account',
'Net Amount Difference',
'Non Trade Description',
#'OTE Custodian Account',
#'OTE Ticker',
'PB Account Numeric',
'Portfolio ID',
'Portolio',
'Price',
'Prime Broker',
#'Principal Amount',
'Quantity',
#'Sec Fees',
'SEDOL',
'Settle Date',
'Status',
#'Strike Price',
'System Comments',
'Ticker',
'Trade Date',
'Trade Expenses',
'Transaction Category',
'Transaction ID',
'Transaction Type',
'Underlying Cusip',
'Underlying Investment ID',
'Underlying ISIN',
'Underlying Sedol',
'Underlying Ticker',
'UserTran1',
'UserTran2',
'Value Date',
] 
add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds']
viewdata_cols_to_show = ['ViewData.' + x for x in cols_to_show] + add


# In[347]:


meo_final = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/MEO/MeoCollections_SOROS.MEO_HST_RecData_153_2020-06-25.csv',usecols = viewdata_cols_to_show)
aua_final = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/AUA/AUACollections_SOROS.AUA_HST_RecData_153_2020-06-25.csv',usecols = viewdata_cols_to_show)


# In[348]:


#final_predictions = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Final_Predictions_Table_HST_RecData_125_2020-06-1.csv')


# In[349]:


final_predictions = final9.copy()


# In[350]:


#final_predictions.groupby(['Actual_Status'])['Predicted_Status'].value_counts()


# In[351]:


#final_predictions[(final_predictions['Actual_Status'] == 'Open Break') & (final_predictions['Predicted_Status'] == 'UMR')][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]


# In[352]:


#final_predictions.groupby(['ViewData.Side0_UniqueIds'])['ViewData.Side1_UniqueIds'].value_counts()


# In[353]:


#final12[final12['Actual_Status']=='UMT']


# In[354]:


final_predictions_both_present = final_predictions[(final_predictions['ViewData.Side0_UniqueIds'] !='nan') & (final_predictions['ViewData.Side1_UniqueIds']!='nan')]
final_predictions_side0_only = final_predictions[(final_predictions['ViewData.Side0_UniqueIds']!='nan') & (final_predictions['ViewData.Side1_UniqueIds'] =='nan')]
final_predictions_side1_only = final_predictions[(final_predictions['ViewData.Side0_UniqueIds']=='nan') & (final_predictions['ViewData.Side1_UniqueIds'] != 'nan')]
final_predictions_both_null = final_predictions[(final_predictions['ViewData.Side0_UniqueIds']=='nan') & (final_predictions['ViewData.Side1_UniqueIds']=='nan')]


# In[355]:


final_predictions_both_present.shape


# In[356]:


aua_final = aua_final.drop_duplicates()
aua_final = aua_final.reset_index()
aua_final = aua_final.drop('index',1)


# In[357]:


final_predictions_both_present_aua_merge = pd.merge(final_predictions_both_present,aua_final, on=['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds'], how='left' )
final_predictions_side0_only_aua_merge = pd.merge(final_predictions_side0_only,aua_final, on='ViewData.Side0_UniqueIds', how='left' )
final_predictions_side1_only_aua_merge = pd.merge(final_predictions_side1_only,aua_final, on='ViewData.Side1_UniqueIds', how='left' )


# In[358]:


#final_predictions_side1_only_aua_merge


# In[359]:


final_predictions_side0_only_aua_merge = final_predictions_side0_only_aua_merge.drop(['ViewData.BreakID_y', 'ViewData.Side1_UniqueIds_y', 'ViewData.Age_y'], 1)

final_predictions_side0_only_aua_merge = final_predictions_side0_only_aua_merge.rename(columns={'ViewData.BreakID_x': 'ViewData.BreakID'})
final_predictions_side0_only_aua_merge = final_predictions_side0_only_aua_merge.rename(columns={'ViewData.Side1_UniqueIds_x': 'ViewData.Side1_UniqueIds'})
final_predictions_side0_only_aua_merge = final_predictions_side0_only_aua_merge.rename(columns={'ViewData.Age_x': 'ViewData.Age'})


final_predictions_side1_only_aua_merge = final_predictions_side1_only_aua_merge.drop(['ViewData.BreakID_y', 'ViewData.Side0_UniqueIds_y', 'ViewData.Age_y'], 1)

final_predictions_side1_only_aua_merge = final_predictions_side1_only_aua_merge.rename(columns={'ViewData.BreakID_x': 'ViewData.BreakID'})
final_predictions_side1_only_aua_merge = final_predictions_side1_only_aua_merge.rename(columns={'ViewData.Side0_UniqueIds_x': 'ViewData.Side0_UniqueIds'})
final_predictions_side1_only_aua_merge = final_predictions_side1_only_aua_merge.rename(columns={'ViewData.Age_x': 'ViewData.Age'})



final_predictions_both_present_aua_merge = final_predictions_both_present_aua_merge.drop(['ViewData.BreakID_y', 'ViewData.Age_y'], 1)

final_predictions_both_present_aua_merge = final_predictions_both_present_aua_merge.rename(columns={'ViewData.BreakID_x': 'ViewData.BreakID'})
final_predictions_both_present_aua_merge = final_predictions_both_present_aua_merge.rename(columns={'ViewData.Age_x': 'ViewData.Age'})
                                                                                                   
                                                                                                                                                                                             


# In[ ]:





# In[ ]:





# In[360]:


#final_prediction_show_cols = final_predictions_both_present_aua_merge.append([final_predictions_side0_only_aua_merge,final_predictions_side1_only_aua_merge])


# In[361]:


final11 = pd.concat([final_predictions_both_present_aua_merge, final_predictions_side0_only_aua_merge,final_predictions_side1_only_aua_merge], axis=0)


# In[362]:


final11 = final11.reset_index()
final11 = final11.drop('index',1)


# In[363]:


final12 = final11.drop_duplicates(['ViewData.BreakID', 'ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds', 'ViewData.Age'])


# In[364]:


final12.loc[(final12['Actual_Status']=='UCB'), 'ML_flag'] ='Non-ML'
final12.loc[(final12['Actual_Status']=='UCB'), 'Type'] = 'Closed Breaks'


# In[365]:


final12.loc[final12['Actual_Status']=='UCB','Predicted_Status'] = 'No-Prediction'


# In[366]:


from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
print(classification_report(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status']))


# In[5675]:


report = classification_report(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status'], output_dict=True)
accuracy_table = pd.DataFrame(report).transpose()


# In[5676]:


from sklearn.metrics import confusion_matrix
crosstab_table = pd.crosstab(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status'])


# In[5678]:


crosstab_table


# ## Save results

# In[ ]:


final12.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/test2_show_cols.csv')


# In[ ]:


accuracy_table.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Accuracy_table_all_june.csv')


# In[ ]:


crosstab_table.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Crosstab_table_all_june.csv')


# ## Enitre month prediction

# In[15]:


import pandas as pd 
import glob
df_list = []

path = "//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_125/*.csv"
for fname in glob.glob(path):
    
    if "Accuracy_" not in fname and "Crosstab" not in fname and "All_june" not in fname:
        print(fname)
        df_list.append(pd.read_csv(fname))


# In[21]:


all_june_data = pd.concat(df_list)


# In[22]:


#all_june_data = all_june_data[final12.columns]


# In[23]:


all_june_data.columns


# In[19]:


##################### For Soros #######################

all_june_data = all_june_data[['ViewData.BreakID', 'ViewData.Side1_UniqueIds',
       'ViewData.Side0_UniqueIds', 'ViewData.Age', 'probability_No_pair',
       'probability_UMR', 'probability_UMB', 'probability_UMT',
       'Final_predicted_break', 'Type', 'ML_flag', 'Actual_Status',
       'Predicted_Status', 'ViewData.Account Type',
       'ViewData.Accounting Net Amount',
       'ViewData.Alt ID 1', 'ViewData.Asset Type Category',
       'ViewData.B-P Net Amount',
       'ViewData.Business Date', 'ViewData.CUSIP',
    'ViewData.Cancel Amount',
       'ViewData.Cancel Flag', 'ViewData.Commission', 'ViewData.Currency',
       'ViewData.Custodian', 'ViewData.Custodian Account',
       'ViewData.Department', 'ViewData.Description', 'ViewData.ExpiryDate',
       'ViewData.ExternalComment2', 'ViewData.Fund',
       'ViewData.ISIN',
       'ViewData.InternalComment2', 'ViewData.Investment ID',
       'ViewData.Investment Type', 'ViewData.Is Combined Data',
       'ViewData.Keys', 'ViewData.Knowledge Date',
       'ViewData.Mapped Custodian Account', 'ViewData.Net Amount Difference',
       'ViewData.Non Trade Description',
        'ViewData.PB Account Numeric',
       'ViewData.Portfolio ID', 'ViewData.Portolio', 'ViewData.Price',
       'ViewData.Prime Broker',
       'ViewData.Quantity', 'ViewData.SEDOL',
       'ViewData.Settle Date', 'ViewData.Status',
       'ViewData.System Comments', 'ViewData.Ticker', 'ViewData.Trade Date',
       'ViewData.Trade Expenses', 'ViewData.Transaction Category',
       'ViewData.Transaction ID', 'ViewData.Transaction Type',
       'ViewData.Underlying Cusip', 'ViewData.Underlying ISIN',
       'ViewData.Underlying Investment ID', 'ViewData.Underlying Sedol',
       'ViewData.Underlying Ticker', 'ViewData.UserTran1',
       'ViewData.UserTran2', 'ViewData.Value Date']]


# In[25]:


all_june_data = all_june_data[['ViewData.BreakID', 'ViewData.Side1_UniqueIds',
       'ViewData.Side0_UniqueIds', 'ViewData.Age', 'probability_No_pair',
       'probability_UMR', 'probability_UMB', 'probability_UMT',
       'Final_predicted_break', 'Type', 'ML_flag', 'Actual_Status',
       'Predicted_Status', 'ViewData.Account Type',
       'ViewData.Accounting Net Amount', 'ViewData.Activity Code',
       'ViewData.Alt ID 1', 'ViewData.Asset Type Category',
       'ViewData.B-P Net Amount', 'ViewData.Bloomberg_Yellow_Key',
       'ViewData.Business Date', 'ViewData.CUSIP',
       'ViewData.Call Put Indicator', 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag', 'ViewData.Commission', 'ViewData.Currency',
       'ViewData.Custodian', 'ViewData.Custodian Account',
       'ViewData.Department', 'ViewData.Description', 'ViewData.ExpiryDate',
       'ViewData.ExternalComment2', 'ViewData.FX Rate', 'ViewData.Fund',
       'ViewData.ISIN', 'ViewData.Interest Amount',
       'ViewData.InternalComment2', 'ViewData.Investment ID',
       'ViewData.Investment Type', 'ViewData.Is Combined Data',
       'ViewData.Keys', 'ViewData.Knowledge Date',
       'ViewData.Mapped Custodian Account', 'ViewData.Net Amount Difference',
       'ViewData.Non Trade Description', 'ViewData.OTE Custodian Account',
       'ViewData.OTE Ticker', 'ViewData.PB Account Numeric',
       'ViewData.Portfolio ID', 'ViewData.Portolio', 'ViewData.Price',
       'ViewData.Prime Broker', 'ViewData.Principal Amount',
       'ViewData.Quantity', 'ViewData.SEDOL', 'ViewData.Sec Fees',
       'ViewData.Settle Date', 'ViewData.Status', 'ViewData.Strike Price',
       'ViewData.System Comments', 'ViewData.Ticker', 'ViewData.Trade Date',
       'ViewData.Trade Expenses', 'ViewData.Transaction Category',
       'ViewData.Transaction ID', 'ViewData.Transaction Type',
       'ViewData.Underlying Cusip', 'ViewData.Underlying ISIN',
       'ViewData.Underlying Investment ID', 'ViewData.Underlying Sedol',
       'ViewData.Underlying Ticker', 'ViewData.UserTran1',
       'ViewData.UserTran2', 'ViewData.Value Date']]


# In[26]:


#all_june_data = all_june_data[final12.columns]


# In[27]:


all_june_data


# In[6552]:


all_june_data = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/Final_Predictions_239/All_june_data_239.csv')


# In[11]:


all_june_data


# In[28]:


from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
print(classification_report(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status']))


# In[29]:


from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
print(classification_report(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status']))


# In[30]:


report_all_june = classification_report(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status'], output_dict=True)
accuracy_table_all_june = pd.DataFrame(report_all_june).transpose()


# In[31]:


accuracy_table_all_june


# In[32]:


from sklearn.metrics import confusion_matrix
crosstab_all_june =  pd.crosstab(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status'])


# In[6545]:


accuracy_table_all_june.to_csv('accuracy_all_j.csv')
crosstab_all_june.to_csv('cross_tab_soros.csv')


# In[33]:


crosstab_all_june


# In[35]:


accuracy_table_all_june.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_125/accuracy_table_all_june_125.csv')
crosstab_all_june.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_125/crosstab_all_june_125.csv')


# In[6517]:


all_june_data = all_june_data.reset_index()
all_june_data = all_june_data.drop('index',1)


# In[34]:


all_june_data


# ## Save Results (Entire Month)

# In[6187]:


all_june_data.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_123/All_june_data_123.csv')


# In[19]:


accuracy_table_all_june.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions_379/Accuracy_table_all_june_379.csv')


# In[18]:


crosstab_all_june.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions_379/Crosstab_table_all_june_379.csv')


# In[6406]:


comment_file = pd.read_csv('1 month merged file prediction brk_com for weiss123.csv')

#comment_file = pd.read_csv('1 month merged file prediction brk_com for oak tree.csv')


# In[6407]:


comment_file = comment_file.drop('Unnamed: 0', 1)


# In[6393]:


#all_june_data
import glob
meo_list = []

path = "//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/*.csv"
for fname in glob.glob(path):
    
    if "RecData_123" in fname:
        print(fname)
        meo_list.append(pd.read_csv(fname))


# In[6394]:


meo_123 = pd.concat(meo_list)


# In[6396]:


meo_123= meo_123.reset_index()
meo_123 = meo_123.drop('index', 1)


# In[6403]:


meo_123.drop_duplicates()[['ViewData.Side1_UniqueIds']]


# In[6401]:


meo_123[meo_123['ViewData.Side1_UniqueIds'].isin(all_june_data[all_june_data['Actual_Status']=='Open Break']['ViewData.Side1_UniqueIds'].unique())]


# In[6410]:


comment_file[(comment_file['Actual_Status']=='Open Break') & (comment_file['Predicted_comment'].isnull())]['ViewData.Side1_UniqueIds']


# In[6412]:


#meo_123[meo_123['ViewData.Side1_UniqueIds']=='70_123882606_CITI']


# In[6399]:


all_june_data[all_june_data['Actual_Status']=='Open Break']['ViewData.Side1_UniqueIds'].unique()


# In[6386]:


all_june_data[all_june_data['Predicted_Status']=='Open Break']['ViewData.InternalComment2'].value_counts()


# In[6314]:


#comment_file[comment_file['Actual_Status']=='Open Break']


# In[6357]:


all_june_data['Predicted_Status'].value_counts()


# In[6487]:


all_june_data.shape


# In[6359]:


#comment_file.loc[comment_file['ViewData.Side0_UniqueIds']=='AA','ViewData.Side0_UniqueIds'] = np.nan


# In[6360]:


#comment_file.loc[comment_file['ViewData.Side1_UniqueIds']=='BB','ViewData.Side1_UniqueIds'] = np.nan


# In[6519]:


all_june_data['key'] = all_june_data['ViewData.Side0_UniqueIds'].astype(str) + all_june_data['ViewData.Side1_UniqueIds'].astype(str)

#comment_file['key'] = comment_file['ViewData.Side0_UniqueIds'].astype(str) + comment_file['ViewData.Side1_UniqueIds'].astype(str)


# In[6520]:


#comment_file = comment_file.drop_duplicates()
#comment_file = comment_file.reset_index()
#comment_file = comment_file.drop('index',1)


# In[6521]:


all_june_data = all_june_data.drop_duplicates()
all_june_data = all_june_data.reset_index()
all_june_data = all_june_data.drop('index',1)


# In[6522]:


jj = all_june_data[all_june_data['key'].isin(all_june_data['key'].value_counts().reset_index()[all_june_data['key'].value_counts().reset_index()['key']>1]['index'])]


# In[6523]:


jj['Actual_Status'].value_counts()


# In[6524]:


all_june_data1 = all_june_data[~((all_june_data['key'].isin(jj['key'].unique())) & (all_june_data['Actual_Status']=='Open Break'))]
all_june_data1 = all_june_data1.reset_index()
all_june_data1 = all_june_data1.drop('index',1)


# In[6525]:


#comment_file1 = comment_file[~((comment_file['key'].isin(jj['key'].unique())) & (comment_file['Actual_Status']=='Open Break'))]
#comment_file1 = comment_file1.reset_index()
#comment_file1 = comment_file1.drop('index',1)


# In[6526]:


all_june_data1['key'].value_counts()


# In[6527]:


all_june_data2 = all_june_data1[~((all_june_data1['Actual_Status']=='UCB') & (all_june_data1['ViewData.BreakID'].isnull()))]
all_june_data2 = all_june_data2.reset_index()
all_june_data2 = all_june_data2.drop('index',1)


# In[6528]:


all_june_data2['key'].value_counts()


# In[6529]:


all_june_data2[all_june_data2['key']=='11_239149101_Advent Geneva19_239149101_JP Morgan']


# In[6530]:


all_june_data2


# In[6531]:


#all_june_data2 = all_june_data1[~((all_june_data1['ViewData.BreakID'].isnull()) & (all_june_data1['Actual_Status']=='UCB'))]
#comment_file2 = comment_file1[~((comment_file1['ViewData.BreakID'].isnull()) & (comment_file1['Actual_Status']=='UCB'))]

#all_june_data2 = all_june_data2.reset_index()
#all_june_data2 = all_june_data2.drop('index',1)

#comment_file2 = comment_file2.reset_index()
#comment_file2 = comment_file2.drop('index',1)


# In[6501]:


#comment_file2[comment_file2['key']=='nan88_123895430_CITI'].drop_duplicates()


# In[6532]:


all_june_data2.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/Final_Predictions_239/All_june_data_239.csv')


# In[6350]:


#new_comment = pd.merge(all_june_data2, comment_file2[['key', 'final_ID', 's/d', 'ViewData.InternalComment2', 'ViewData.Description',
#       'ViewData.Transaction Type', 'new_desc_cat', 'Created_cat_predicted',
#       'Created_cat_actual', 'error', 'Category', 'Predicted_comment']], on='key', how='left')


# In[369]:


final12['Type'].value_counts()


# In[371]:


final12[final12['Type'] == 'One_to_Many']


# In[382]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds']=='1141_153157324_CITI')]['Predicted_action'].value_counts()


# In[ ]:




