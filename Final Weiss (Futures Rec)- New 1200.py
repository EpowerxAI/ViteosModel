#!/usr/bin/env python
# coding: utf-8

import timeit
start = timeit.default_timer()


import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE
import dateutil.parser


import os
os.chdir('D:\\ViteosModel')

from datetime import datetime,date,timedelta
import datetime as dt
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from pandas.io.json import json_normalize

def clean_list_of_BreakID_to_form_insertable_into_db(fun_str_list):
    fun_str_list_to_str_values = str(fun_str_list)
    fun_str_list_to_str_values = fun_str_list_to_str_values.replace('{','').replace('}','').replace('\'','').replace('[','').replace(']','')
    return(fun_str_list_to_str_values)


def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d


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

# M X M and N X N architecture for closed break prediction

#Change added on New closed code on 10-01-2021
mapping_dict_trans_type = {
                        'WITH_Transfer_interacting' : ['WITH','Transfer'],
                        'WITH_DEP_interacting' : ['WITH','DEP'],
                        'DEP_Transfer_interacting' : ['DEP','Transfer'],
                        'Same' : ['everthing_else','everthing_else']
                        }

def assign_Transaction_Type_for_closing_apply_row(fun_row, fun_transaction_type_col_name = 'ViewData.Transaction Type'):
    if(fun_row[fun_transaction_type_col_name] in ['WITH','Transfer']):
        Transaction_Type_for_closing = 'WITH_Transfer_interacting'
    else:
         Transaction_Type_for_closing = 'Same'
    return(Transaction_Type_for_closing)

def assign_Transaction_Type_for_closing_apply_row_2(fun_row, fun_transaction_type_col_name = 'ViewData.Transaction Type2'):
    if(fun_row[fun_transaction_type_col_name] in ['WITH','DEP']):
        Transaction_Type_for_closing2 = 'WITH_DEP_interacting'
    else:
        Transaction_Type_for_closing2 = 'Not_WITH_DEP_interacting'
    return(Transaction_Type_for_closing2)

def assign_Transaction_Type_for_closing_apply_row_3(fun_row, fun_transaction_type_col_name = 'ViewData.Transaction Type3'):
    if(fun_row[fun_transaction_type_col_name] in ['DEP','Transfer']):
        Transaction_Type_for_closing3 = 'DEP_Transfer_interacting'
    else:
        Transaction_Type_for_closing3 = 'Not_DEP_Transfer_interacting'
    return(Transaction_Type_for_closing3)

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

    if(fun_meo_df.shape[0] != 0):
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
        meo['abs_net_amount_difference'] = meo['ViewData.Net Amount Difference'].apply(lambda x : abs(x))
        meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)
        return(meo)  
    else:
        return(pd.DataFrame())
        
def interacting_closing_futures_weiss(fun_df): #The name of the function contains 125 but it is also being used in 379
    if(fun_df.shape[0] != 0):
        fun_df['ViewData.Settle Date'] = fun_df['ViewData.Settle Date'].astype(str)
        fun_df['ViewData.Mapped Custodian Account'] = fun_df['ViewData.Mapped Custodian Account'].astype(str)
        fun_df['ViewData.Currency'] = fun_df['ViewData.Currency'].astype(str)    
        fun_df['ViewData.Source Combination Code'] = fun_df['ViewData.Source Combination Code'].astype(str)
        fun_df['abs_net_amount_difference'] = fun_df['abs_net_amount_difference'].astype(str)
        fun_df['filter'] = fun_df['ViewData.Source Combination Code'] + fun_df['ViewData.Mapped Custodian Account'] + fun_df['ViewData.Currency'] + fun_df['abs_net_amount_difference']
        grouped_by_filter_df = fun_df.groupby('filter').size().reset_index(name='counts_for_filter')
        merged_df_with_filter_counts = pd.merge(fun_df, grouped_by_filter_df, on = 'filter', how = 'left')
        merged_df_with_filter_counts_ge_1 = merged_df_with_filter_counts[merged_df_with_filter_counts['counts_for_filter'] > 1] 
        return(merged_df_with_filter_counts_ge_1)
    else:
        return(pd.DataFrame())
        
def All_combination_file(fun_df):
    if(fun_df.shape[0] != 0):
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
    else:
        return(pd.DataFrame())

def identifying_closed_breaks_897(fun_all_meo_combination_df, fun_setup_code_crucial, fun_trans_type_1, fun_trans_type_2):

    if(fun_all_meo_combination_df.shape[0] != 0):    
        print(fun_trans_type_2)
        print(fun_trans_type_2)

        if(fun_setup_code_crucial == '897'):
    
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
    else:
        return(pd.DataFrame())
#     return(set(
#                 Transaction_type_closed_break_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
#                 Transaction_type_closed_break_df['ViewData.Side1_UniqueIds_x'].astype(str)
#                ))
    


#### Closed break functions - End #### 

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
'Quantity','SEDOL','Settle Date','SPM ID','Status','Task ID',
#'Strike Price',
'System Comments',
'Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type','Investment ID',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID','Source Combination Code']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']

new_cols = ['ViewData.' + x for x in cols] + add

client = 'Weiss'
setup = '1200'
setup_code = '1200'

mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
db_2_for_MEO_data_MLReconDB_Testing = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML_Testing']

#for setup_code in setup_code_list:
print('Starting predictions for Weiss, setup_code = ')
print(setup_code)

query_1_for_MEO_data = db_1_for_MEO_data['RecData_' + setup_code].find({ 
                                                                     "LastPerformedAction": 31
                                                             },
                                                             {       "DataSides" : 1,
                                                                     "BreakID" : 1,
                                                                     "LastPerformedAction" : 1,
                                                                     "TaskInstanceID" : 1,
                                                                     "SourceCombinationCode" : 1,
                                                                     "MetaData" : 1, 
                                                                     "ViewData" : 1
                                                             })
list_of_dicts_query_result_1 = list(query_1_for_MEO_data)

date_to_analyze = '27012021'
penultimate_date_to_analyze = '26012021'
date_to_analyze_ymd_format = date_to_analyze[4:] + '-' + date_to_analyze[2:4] + '-' + date_to_analyze[:2]
penultimate_date_to_analyze_ymd_format = penultimate_date_to_analyze[4:] + '-' + penultimate_date_to_analyze[2:4] + '-' + penultimate_date_to_analyze[:2]
penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'

#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/meo_df_setup_' + setup_code + '_date_' + date_to_analyze_ymd_format + '.csv'
#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/meo_df_setup_1200_date_2021-01-25_for_10_day_predictions.csv'

#meo_df = pd.read_csv(meo_filename)
#Comment out below three lines if running from db, in case they are commented
meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)

date_i = pd.to_datetime(pd.to_datetime(meo_df['ViewData.Task Business Date'])).dt.date.astype(str).mode()[0]

#Change added on 14-12-2020 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_df.apply(lambda row : contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row = row), axis = 1,result_type="expand")
meo_df = meo_df[~(meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]


meo = meo_df[new_cols]
print('meo size')
print(meo.shape[0])


# ## Read testing data 
print('meo shape is')
print(meo.shape[0])

#start_closed = timeit.default_timer()
#
#normalized_meo_df = normalize_bp_acct_col_names(meo_df)
#meo_for_closed = cleaned_meo(normalized_meo_df)
#
#closed_df_list = []
#for transaction_type_for_closing_value in mapping_dict_trans_type:
#    meo_for_transaction_type_for_closing_value_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing'] == transaction_type_for_closing_value]
#    meo_for_transaction_type_for_closing_value = interacting_closing_futures_weiss(meo_for_transaction_type_for_closing_value_input)
#    All_combination_df = All_combination_file(fun_df = meo_for_transaction_type_for_closing_value)
#    if(All_combination_df.shape[0] != 0):
#        closed_df_for_transaction_type_for_closing_value = identifying_closed_breaks_897(fun_all_meo_combination_df = All_combination_df, \
#                                                                 fun_setup_code_crucial = setup_code, \
#                                                                 fun_trans_type_1 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[0], \
#                                                                 fun_trans_type_2 = mapping_dict_trans_type.get(transaction_type_for_closing_value)[1])
#        closed_df_list.append(closed_df_for_transaction_type_for_closing_value)
#    else:
#        closed_df_list.append(pd.DataFrame())
#    del(meo_for_transaction_type_for_closing_value_input)
#    del(meo_for_transaction_type_for_closing_value)
#    del(All_combination_df)
#
#meo_for_WITH_DEP_interacting_input = meo_for_closed[meo_for_closed['Transaction_Type_for_closing_2'] == 'WITH_DEP_interacting'] 
#meo_for_WITH_DEP_interacting = interacting_closing_futures_weiss(meo_for_WITH_DEP_interacting_input)
#All_combination_df_WITH_DEP_interacting = All_combination_file(fun_df = meo_for_WITH_DEP_interacting)
#closed_df_WITH_DEP_interacting = identifying_closed_breaks_897(fun_all_meo_combination_df = All_combination_df_WITH_DEP_interacting, \
#                                                         fun_setup_code_crucial = '125', \
#                                                         fun_trans_type_1 = 'WITH', \
#                                                         fun_trans_type_2 = 'DEP')
#
#
#closed_df_list.append(closed_df_WITH_DEP_interacting)
#
#closed_df_interacting = pd.concat(closed_df_list)
#
#breakId_x = set(list(closed_df_interacting['ViewData.BreakID_x']))
#breakId_y = set(list(closed_df_interacting['ViewData.BreakID_y']))
#
#all_interacting_closed_breakIds = list(breakId_x.union(breakId_y))
#
#meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_531_2020-06-18.csv",usecols=new_cols)

df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)

#df1['close_key'] = df1['ViewData.Side0_UniqueIds'].astype(str) + df1['ViewData.Side1_UniqueIds'].astype(str)

df1['ViewData.Transaction Type'] = df1['ViewData.Transaction Type'].apply(lambda x: str(x).lower())

df1.loc[df1['ViewData.Transaction Type'] =='proceeds sell', 'ViewData.Transaction Type'] = 'proceed sell'
df1.loc[df1['ViewData.Transaction Type'] =='realized p&l', 'ViewData.Transaction Type'] = 'proceed sell'
df1.loc[df1['ViewData.Transaction Type'] =='wire received', 'ViewData.Transaction Type'] = 'wire'
df1.loc[df1['ViewData.Transaction Type'] =='trf from sec to seg', 'ViewData.Transaction Type'] = 'wire'
df1.loc[df1['ViewData.Transaction Type'] =='wire transferred', 'ViewData.Transaction Type'] = 'wire'
df1.loc[df1['ViewData.Transaction Type'] =='collateral posted to isda counterparties', 'ViewData.Transaction Type'] = 'wire'

df1['TT_fee_commission_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['fee','commission']) else 0)
df1['TT_fee_commission_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['fee','commission']) else 0)
df1['TT_fee_sell_commission_flag'] = df1.apply(lambda x: 1 if x['ViewData.Transaction Type']!='buy commission' and x['TT_fee_commission_flag']==1 else 0, axis=1)
df1['TT_fee_buy_commission_flag'] = df1.apply(lambda x: 1 if x['ViewData.Transaction Type']!='sell commission' and x['TT_fee_commission_flag']==1 else 0, axis=1)
df1['TT_proceed_sell_pnl_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['realized p&l', 'proceed sell','proceeds sell adjustment']) else 0)

## Output for Closed breaks

#closed_df = df1[(df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys))]

#df1[df1['ViewData.OTE Ticker']!=df1['ViewData.Investment ID']]

# ## Machine generated output

#df2 = df1[~df1['close_key'].isin(list(all_closed))]
#df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]

df = df1.copy()
df = df.reset_index()
df = df.drop('index',1)

df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
#df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])

df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)

df['Date'] = pd.to_datetime(df['Date']).dt.date

df['Date'] = df['Date'].astype(str)

df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB','SPM'])]
df = df.reset_index()
df = df.drop('index',1)

#df1[df1['ViewData.Status']=='SMB']
df['ViewData.Status'].value_counts()

df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)

# ## Sample data on one date
df['ViewData.Settle Date'] = pd.to_datetime(df['ViewData.Settle Date'],errors='coerce').dt.date

df = df.sort_values(by = ['ViewData.Settle Date'],ascending =[False])
df = df.reset_index().drop('index',1)

df['ViewData.Settle Date'] = df['ViewData.Settle Date'].astype(str)

#df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})

#sample = df[df['ViewData.Settle Date'] ==df.loc[0,'ViewData.Settle Date']]
sample = df.copy()
sample = sample.reset_index()
sample = sample.drop('index',1)

#smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
#smb = smb.drop('index',1)

#smb_pb = smb.copy()
#smb_acc = smb.copy()

#smb_pb['ViewData.Accounting Net Amount'] = np.nan
#smb_pb['ViewData.Side0_UniqueIds'] = np.nan
#smb_pb['ViewData.Status'] ='SMB-OB'

#smb_acc['ViewData.B-P Net Amount'] = np.nan
#smb_acc['ViewData.Side1_UniqueIds'] = np.nan
#smb_acc['ViewData.Status'] ='SMB-OB'

#sample = sample[sample['ViewData.Status']!='SMB']
#sample = sample.reset_index()
#sample = sample.drop('index',1)

#sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
#sample = sample.reset_index()
#sample = sample.drop('index',1)

sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)

sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
sample.loc[(sample['ViewData.Side0_UniqueIds']!='nan') &(sample['ViewData.Side1_UniqueIds']!='nan') ,'Trans_side'] = 'Both_side'

sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 

sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
#sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)

#sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)

sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else(x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'] if x['Trans_side']=='B_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Currency']), axis=1)

sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','SMB-OB','CNF','CMF','SMB']))]


sample1 = sample1.reset_index()
sample1 = sample1.drop('index', 1)

sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)

sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)

sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)

sample1[['filter_key','ViewData.Mapped Custodian Account','ViewData.B-P Currency','ViewData.Accounting Currency','Trans_side']]

# ## Fresh daily code 

sample1['filter_key'] = sample1['ViewData.Source Combination'].astype(str) + sample1['filter_key'].astype(str) + sample1['ViewData.Fund'].astype(str) + sample1['ViewData.Settle Date'].astype(str)

sample1.loc[sample1['ViewData.Transaction Type']=='Proceeds Sell','ViewData.Transaction Type'] = 'Proceed Sell'
sample1['ViewData.Transaction Type'] = sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower())

sample1.loc[sample1['ViewData.Transaction Type']=='proceeds sell adjustment','ViewData.Transaction Type'] = 'proceed sell'
sample1.loc[sample1['ViewData.Transaction Type']=='transfer','ViewData.Transaction Type'] = 'trans_with_dep'
sample1.loc[sample1['ViewData.Transaction Type']=='with','ViewData.Transaction Type'] = 'trans_with_dep'
sample1.loc[sample1['ViewData.Transaction Type']=='dep','ViewData.Transaction Type'] = 'trans_with_dep'

#############for BNP set up ##############
sample1.loc[sample1['ViewData.Transaction Type'] =='wire received', 'ViewData.Transaction Type'] = 'wire'
#df1.loc[df1['ViewData.Transaction Type'] =='trf from sec to seg', 'ViewData.Transaction Type'] = 'wire'
sample1.loc[sample1['ViewData.Transaction Type'] =='wire transferred', 'ViewData.Transaction Type'] = 'wire'
sample1.loc[sample1['ViewData.Transaction Type'] =='collateral posted to isda counterparties', 'ViewData.Transaction Type'] = 'wire'


#############for MS set up ##############
sample1.loc[sample1['ViewData.Transaction Type'] =='collateral', 'ViewData.Transaction Type'] = 'trans_with_dep'
#df1.loc[df1['ViewData.Transaction Type'] =='trf from sec to seg', 'ViewData.Transaction Type'] = 'wire'
sample1.loc[sample1['ViewData.Transaction Type'] =='futures collateral', 'ViewData.Transaction Type'] = 'trans_with_dep'
sample1.loc[sample1['ViewData.Transaction Type'] =='transfer', 'ViewData.Transaction Type'] = 'trans_with_dep'

sample1['filter_key_with_tt'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str)

table1 = sample1.groupby(['filter_key_with_tt'])['ViewData.Price'].value_counts().reset_index(name ='amount_count')

table2 = sample1.groupby(['filter_key_with_tt','ViewData.Price'])['Trans_side'].nunique().reset_index(name='side_count')

table1['key'] = table1['filter_key_with_tt'] + table1['ViewData.Price'].astype(str)
table2['key'] = table2['filter_key_with_tt'] + table2['ViewData.Price'].astype(str)

table3 = pd.merge(table1, table2[['key','side_count']], on='key', how='left')
table3

keys_to_consider = table3[table3['side_count']>1]['key'].unique()

sample1['key'] = sample1['filter_key_with_tt'] + sample1['ViewData.Price'].astype(str)

unique_id_0 =[]
unique_id_1 =[]
break_id_0 = []
break_id_1 = []
net_amount = []
business_date = []
source_combination_code = []
task_id =[]

for i in keys_to_consider:
    sample2 = sample1[sample1['key']==i]
    unique_id_0.append([k for k in list(sample2['ViewData.Side0_UniqueIds'].values) if 'nan'!= k])
    unique_id_1.append([k for k in list(sample2['ViewData.Side1_UniqueIds'].values) if 'nan'!= k])
    break_id_0.append(list(sample2[sample2['Trans_side'].isin(['B_side','Both_side'])]['ViewData.BreakID'].values))
    break_id_1.append(list(sample2[sample2['Trans_side'].isin(['A_side','Both_side'])]['ViewData.BreakID'].values))
    net_amount.append(sample2['ViewData.Net Amount Difference'].sum())
    source_combination_code.append(sample2['ViewData.Source Combination Code'].unique()[0])
    business_date.append(sample2['ViewData.Task Business Date'].unique()[0])
    task_id.append(sample2['ViewData.Task ID'].unique()[0])
    
if(len(unique_id_0) != 0):
	final_df = pd.DataFrame(np.array(unique_id_0))
	final_df.columns = ['ViewData.Side0_UniqueIds']
	final_df['ViewData.Side1_UniqueIds'] = np.array(unique_id_1)
	
	final_df['break_id_0'] = np.array(break_id_0)
	final_df['break_id_1'] = np.array(break_id_1)
	final_df['net_amount'] = np.array(net_amount)
	
	final_df['ViewData.Source Combination Code'] = source_combination_code
	final_df['ViewData.Task Business Date'] = business_date
	final_df['ViewData.Task ID'] = task_id
	
	final_df.loc[final_df['net_amount'] !=0, 'Predicted_Status'] = 'UMB'
	final_df.loc[final_df['net_amount'] ==0, 'Predicted_Status'] = 'UMR'
else:
	final_df = pd.DataFrame()
	
if final_df.shape[0]>0:
    umb_0_ids = np.concatenate(final_df['ViewData.Side0_UniqueIds'].values)
    umb_1_ids = np.concatenate(final_df['ViewData.Side1_UniqueIds'].values)
#    umb_0_ids = list(final_df['ViewData.Side0_UniqueIds'].values)
#    umb_1_ids = list(final_df['ViewData.Side1_UniqueIds'].values)

else:
    umb_0_ids =[]
    umb_1_ids =[]
    
ob_table = sample1[~(sample1['ViewData.Side0_UniqueIds'].isin(umb_0_ids) | sample1['ViewData.Side1_UniqueIds'].isin(umb_1_ids))]
ob_table = ob_table.reset_index().drop('index',1)

final_open_table = ob_table[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Source Combination Code','ViewData.Task Business Date','ViewData.Task ID','ViewData.Status']]

final_open_table = final_open_table.rename(columns={'ViewData.Status':'Predicted_Status'})
final_open_table
# ## Code Ends

#Table for final_open_table
change_names_of_final_open_table_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.BreakID' : 'BreakID',
											'ViewData.Task ID' : 'TaskID',
                                            'ViewData.Task Business Date' : 'BusinessDate',
                                            'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                        }

final_open_table.rename(columns = change_names_of_final_open_table_mapping_dict, inplace = True)
final_open_table['PredictedComment'] = ''
final_open_table['PredictedCategory'] = ''
final_open_table['Final_predicted_break'] = ''
final_open_table['Predicted_action'] = 'No-Pair'
final_open_table['ML_flag'] = 'ML'
final_open_table['SetupID'] = setup_code

final_open_table['probability_UMT'] = ''
final_open_table['probability_UMR'] = ''
final_open_table['probability_UMB'] = ''
final_open_table['probability_No_pair'] = ''

final_open_table['BusinessDate'] = pd.to_datetime(final_open_table['BusinessDate'])
final_open_table['BusinessDate'] = final_open_table['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_open_table['BusinessDate'] = pd.to_datetime(final_open_table['BusinessDate'])

final_open_table['ReconSetupName'] = '3011 BNP Future Cash Recon'
final_open_table['ClientShortCode'] = 'Weiss Advisors'

today = date.today()
today_Y_m_d = today.strftime("%Y-%m-%d")

final_open_table['CreatedDate'] = today_Y_m_d
final_open_table['CreatedDate'] = pd.to_datetime(final_open_table['CreatedDate'])
final_open_table['CreatedDate'] = final_open_table['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_open_table['CreatedDate'] = pd.to_datetime(final_open_table['CreatedDate'])

#Changing dtypes to required dtypes
final_open_table['BreakID'] = final_open_table['BreakID'].astype(str)
#BusinessDate is already datetime, so wont change it as it will throw an error
final_open_table['Final_predicted_break'] = final_open_table['Final_predicted_break'].astype(str)
final_open_table['ML_flag'] = final_open_table['ML_flag'].astype(str)
final_open_table['SetupID'] = final_open_table['SetupID'].astype(int)
final_open_table['SourceCombinationCode'] = final_open_table['SourceCombinationCode'].astype(str)
final_open_table['TaskID'] = final_open_table['TaskID'].astype(float)
final_open_table['TaskID'] = final_open_table['TaskID'].astype(np.int64)
final_open_table['probability_UMT'] = final_open_table['probability_UMT'].astype(str)
final_open_table['probability_UMR'] = final_open_table['probability_UMR'].astype(str)
final_open_table['probability_UMB'] = final_open_table['probability_UMB'].astype(str)
final_open_table['probability_No_pair'] = final_open_table['probability_No_pair'].astype(str)
final_open_table['Side1_UniqueIds'] = final_open_table['Side1_UniqueIds'].astype(str)
final_open_table['Side1_UniqueIds'] = final_open_table['Side1_UniqueIds'].replace('None','')            
final_open_table['Side1_UniqueIds'] = final_open_table['Side1_UniqueIds'].replace('nan','')            

final_open_table['Side0_UniqueIds'] = final_open_table['Side0_UniqueIds'].astype(str)
final_open_table['Side0_UniqueIds'] = final_open_table['Side0_UniqueIds'].replace('None','')            
final_open_table['Side0_UniqueIds'] = final_open_table['Side0_UniqueIds'].replace('nan','')            

final_open_table['PredictedComment'] = final_open_table['PredictedComment'].astype(str)
final_open_table['PredictedCategory'] = final_open_table['PredictedCategory'].astype(str)
final_open_table['Predicted_Status'] = final_open_table['Predicted_Status'].astype(str)
final_open_table['Predicted_action'] = final_open_table['Predicted_action'].astype(str)
final_open_table['ReconSetupName'] = final_open_table['ReconSetupName'].astype(str)
final_open_table['ClientShortCode'] = final_open_table['ClientShortCode'].astype(str)
#CreatedDate is already datetime, so wont change it as it will throw an error


#Table for final_df
if(final_df.shape[0] != 0):
	change_names_of_final_df_mapping_dict = {
	                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
	                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
	                                            'break_id_0' : 'BreakID',
												'break_id_1' : 'Final_predicted_break',
	                                            'ViewData.Task ID' : 'TaskID',
												'ViewData.Task Business Date' : 'BusinessDate',
	                                            'ViewData.Source Combination Code' : 'SourceCombinationCode'
	                                        }
	
	final_df.rename(columns = change_names_of_final_df_mapping_dict, inplace = True)
	final_df['PredictedComment'] = ''
	final_df['PredictedCategory'] = ''
	final_df['Predicted_action'] = 'Pair'
	final_df['ML_flag'] = 'ML'
	final_df['SetupID'] = setup_code
	
	final_df['probability_UMT'] = ''
	final_df['probability_UMR'] = ''
	final_df['probability_UMB'] = ''
	final_df['probability_No_pair'] = ''
	
	final_df['BusinessDate'] = pd.to_datetime(final_df['BusinessDate'])
	final_df['BusinessDate'] = final_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
	final_df['BusinessDate'] = pd.to_datetime(final_df['BusinessDate'])
	
	final_df['ReconSetupName'] = '3011 BNP Future Cash Recon'
	final_df['ClientShortCode'] = 'Weiss Advisors'
	
	today = date.today()
	today_Y_m_d = today.strftime("%Y-%m-%d")
	
	final_df['CreatedDate'] = today_Y_m_d
	final_df['CreatedDate'] = pd.to_datetime(final_df['CreatedDate'])
	final_df['CreatedDate'] = final_df['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
	final_df['CreatedDate'] = pd.to_datetime(final_df['CreatedDate'])
	
	#Changing dtypes to required dtypes
	final_df['BreakID'] = final_df['BreakID'].astype(str)
	final_df['BreakID'] = final_df['BreakID'].replace('{','',regex = True)
	final_df['BreakID'] = final_df['BreakID'].replace('}','',regex = True)
	final_df['BreakID'] = final_df['BreakID'].replace('\'','',regex = True)
	final_df['BreakID'] = final_df['BreakID'].replace('\[','',regex = True)
	final_df['BreakID'] = final_df['BreakID'].replace('\]','',regex = True)
	final_df['BreakID'] = final_df['BreakID'].replace(', ',',',regex = True)
	#BusinessDate is already datetime, so wont change it as it will throw an error
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].astype(str)
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].replace('{','',regex = True)
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].replace('}','',regex = True)
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].replace('\'','',regex = True)
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].replace('\[','',regex = True)
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].replace('\]','',regex = True)
	final_df['Final_predicted_break'] = final_df['Final_predicted_break'].replace(', ',',',regex = True)

	final_df['ML_flag'] = final_df['ML_flag'].astype(str)
	final_df['SetupID'] = final_df['SetupID'].astype(int)
	final_df['SourceCombinationCode'] = final_df['SourceCombinationCode'].astype(str)
	final_df['TaskID'] = final_df['TaskID'].astype(float)
	final_df['TaskID'] = final_df['TaskID'].astype(np.int64)
	final_df['probability_UMT'] = final_df['probability_UMT'].astype(str)
	final_df['probability_UMR'] = final_df['probability_UMR'].astype(str)
	final_df['probability_UMB'] = final_df['probability_UMB'].astype(str)
	final_df['probability_No_pair'] = final_df['probability_No_pair'].astype(str)

	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].astype(str)
	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('{','',regex = True)
	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('}','',regex = True)
	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('\'','',regex = True)
	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('\[','',regex = True)
	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('\]','',regex = True)
	final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace(', ',',',regex = True)

	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].astype(str)
	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('{','',regex = True)
	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('}','',regex = True)
	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('\'','',regex = True)
	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('\[','',regex = True)
	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('\]','',regex = True)
	final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace(', ',',',regex = True)

	final_df['PredictedComment'] = final_df['PredictedComment'].astype(str)
	final_df['PredictedCategory'] = final_df['PredictedCategory'].astype(str)
	final_df['Predicted_Status'] = final_df['Predicted_Status'].astype(str)
	final_df['Predicted_action'] = final_df['Predicted_action'].astype(str)
	final_df['ReconSetupName'] = final_df['ReconSetupName'].astype(str)
	final_df['ClientShortCode'] = final_df['ClientShortCode'].astype(str)
	#CreatedDate is already datetime, so wont change it as it will throw an error
else:
	final_df = pd.DataFrame()

final_df_2 = final_open_table.append(final_df)
final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace('\.0','',regex = True)

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

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_rerun_for_business.csv'
final_df_2.to_csv(filepaths_final_df_2)

#data_dict = final_table_to_write.to_dict("records")
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing = db_2_for_MEO_data_MLReconDB_Testing['MLPrediction_Cash']

coll_1_for_writing_prediction_data.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})

data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data.insert_many(data_dict) 

data_dict_for_testingdb = final_df_2.to_dict("records_final_for_testingdb")
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.insert_many(data_dict_for_testingdb) 

print(setup_code)
print(date_i)

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

 
    
#meo_df = json_normalize(list_of_dicts_query_for_MEO_data)
#meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
#meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 

    
