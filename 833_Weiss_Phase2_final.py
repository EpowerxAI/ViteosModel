#!/usr/bin/env python
# coding: utf-8

import os
os.chdir('D:\\ViteosModel')
import ast
import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE
import datetime as dt
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from pandas.io.json import json_normalize
from datetime import datetime,date,timedelta
#import timeit

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
'Quantity','SEDOL','Settle Date','SPM ID','Status',
#'Strike Price',
'System Comments',
'Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type','Investment ID',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


new_cols = ['ViewData.' + x for x in cols] + add


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

def select_multiple_col_values_and(fun_string_Side0_UniqueIds, fun_string_Side1_UniqueIds):
    if(fun_string_Side0_UniqueIds == 'None'):
        return 'B_side'
    elif(fun_string_Side1_UniqueIds == 'None'):
        return 'A_side'
#    if(fun_string_Side0_UniqueIds != 'nan' and fun_string_Side1_UniqueIds != 'nan'):
#        return 'Both_side'
    elif(fun_string_Side0_UniqueIds != 'None' and fun_string_Side1_UniqueIds != 'None'):
        return 'Both_side'
#    elif(fun_string_Side0_UniqueIds != '' and fun_string_Side1_UniqueIds != ''):
#        return 'Both_side'
#    else:
#        return 'A_side'
# ## Read testing data 

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

def get_TaskID_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((len(fun_row['Side0_UniqueIds']) != 0) & (len(fun_row['Side1_UniqueIds']) == 0)):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds'][0]])]['ViewData.Task ID'].unique()
    elif((len(fun_row['Side1_UniqueIds']) != 0) & (len(fun_row['Side0_UniqueIds']) == 0)):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Task ID'].unique()
    elif((fun_row['Side0_UniqueIds'][0] != '') & (fun_row['Side1_UniqueIds'][0] == '')):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds'][0]])]['ViewData.Task ID'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'][0] != '') & (fun_row['Side0_UniqueIds'][0] == '')):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Task ID'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'][0] != '') & (fun_row['Side0_UniqueIds'][0] != '')):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Task ID'].unique()
    return(element_TaskID_corresponding_to_Side_01_UniqueIds )

def get_SourceCombinationCode_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((len(fun_row['Side0_UniqueIds']) != 0) & (len(fun_row['Side1_UniqueIds']) == 0)):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds'][0]])]['ViewData.Source Combination Code'].unique()
    elif((len(fun_row['Side1_UniqueIds']) != 0) & (len(fun_row['Side0_UniqueIds']) == 0)):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Source Combination Code'].unique()
    elif((fun_row['Side0_UniqueIds'][0] != '') & (fun_row['Side1_UniqueIds'][0] == '')):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds'][0]])]['ViewData.Source Combination Code'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'][0] != '') & (fun_row['Side0_UniqueIds'][0] == '')):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Source Combination Code'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'][0] != '') & (fun_row['Side0_UniqueIds'][0] != '')):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Source Combination Code'].unique()
    return(element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds )

def get_BusinessDate_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((len(fun_row['Side0_UniqueIds']) != 0) & (len(fun_row['Side1_UniqueIds']) == 0)):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds'][0]])]['ViewData.Task Business Date'].unique()
    elif((len(fun_row['Side1_UniqueIds']) != 0) & (len(fun_row['Side0_UniqueIds']) == 0)):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Task Business Date'].unique()

    elif((fun_row['Side0_UniqueIds'][0] != '') & (fun_row['Side1_UniqueIds'][0] == '')):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds'][0]])]['ViewData.Task Business Date'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'][0] != '') & (fun_row['Side0_UniqueIds'][0] == '')):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Task Business Date'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'][0] != '') & (fun_row['Side0_UniqueIds'][0] != '')):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds'][0]])]['ViewData.Task Business Date'].unique()
    return(element_BusinessDate_corresponding_to_Side_01_UniqueIds)




client = 'Weiss'

setup = '833'
setup_code = '833'
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

meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df['ViewData.Side0_UniqueIds'] = meo_df['ViewData.Side0_UniqueIds'].astype(str)
meo_df['ViewData.Side1_UniqueIds'] = meo_df['ViewData.Side1_UniqueIds'].astype(str)

meo_df.loc[meo_df['ViewData.Side0_UniqueIds'] == 'nan', 'ViewData.Side0_UniqueIds'] = 'None'
meo_df.loc[meo_df['ViewData.Side1_UniqueIds'] == 'nan', 'ViewData.Side1_UniqueIds'] = 'None'

meo_df.loc[meo_df['ViewData.Side0_UniqueIds'] == '', 'ViewData.Side0_UniqueIds'] = 'None'
meo_df.loc[meo_df['ViewData.Side1_UniqueIds'] == '', 'ViewData.Side1_UniqueIds'] = 'None'

meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)
meo = meo_df[new_cols]
print('meo size')
print(meo.shape[0])
umb_carry_forward_df = meo_df[meo_df['ViewData.Status'] == 'UMB']

meo_df_copy = meo_df.copy()
meo_df_copy['Date'] = pd.to_datetime(meo_df_copy['ViewData.Task Business Date'])

meo_df_copy['Date'] = pd.to_datetime(meo_df_copy['Date']).dt.date
meo_df_copy['Date'] = meo_df_copy['Date'].astype(str)

meo_date_i = meo_df_copy['Date'].mode()[0]
meo_df_date_i = meo_df_copy[meo_df_copy['Date'] == meo_date_i]
meo_df_date_i = meo_df_date_i.reset_index()
meo_df_date_i = meo_df_date_i.drop('index',1)
    


#Side_0_1_UniqueIds_closed_all_dates_list = []
#
#i = 0
##for i in range(0,len(date_numbers_list)):
#
#start_closed = timeit.default_timer()
#
#Side_0_1_UniqueIds_closed_all_dates_list.append(
#        closed_daily_run(fun_setup_code=setup_code,\
#                         fun_date = i,\
#                         fun_meo_df_daily_run= meo)
#        )
#
#new_closed_keys = [i.replace('nan','') for i in Side_0_1_UniqueIds_closed_all_dates_list[0]]
#new_closed_keys = [i.replace('None','') for i in new_closed_keys]
#
#stop_closed = timeit.default_timer()

#print('Time for closed : ', stop_closed - start_closed)
# ## Read testing data 

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

df1['TT_proceed_sell_pnl_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['realized p&l', 'proceed sell']) else 0)

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

df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)

#df_170[(df_170['ViewData.Status']=='UMR')]
# ## Sample data on one date
#df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})


print('The Date value count is:')
print(df['Date'].value_counts())

date_i = df['Date'].mode()[0]

print('Choosing the date : ' + date_i)

sample = df[df['Date'] == date_i]

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

#sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
#sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0

sample.loc[sample['ViewData.Side0_UniqueIds']=='None','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='None','flag_side1'] = 0

#sample.loc[sample['ViewData.Side0_UniqueIds']=='','flag_side0'] = 0
#sample.loc[sample['ViewData.Side1_UniqueIds']=='','flag_side1'] = 0

#sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
#sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
#
#sample.loc[sample['ViewData.Side1_UniqueIds']=='None','Trans_side'] = 'B_side'
#sample.loc[sample['ViewData.Side0_UniqueIds']=='None','Trans_side'] = 'A_side'
#
#sample.loc[sample['ViewData.Side1_UniqueIds']=='','Trans_side'] = 'B_side'
#sample.loc[sample['ViewData.Side0_UniqueIds']=='','Trans_side'] = 'A_side'
#
#sample.loc[((sample['ViewData.Side0_UniqueIds']!='nan') & (sample['ViewData.Side1_UniqueIds']!='nan')) ,'Trans_side'] = 'Both_side'

#sample.loc[(\
#             (sample['ViewData.Side0_UniqueIds'].any('nan','None',''])) & (sample['ViewData.Side1_UniqueIds'].any(['nan','None','']))\
#             ) \
#    ,'Trans_side'] = 'Both_side'
sample['Trans_side'] = sample.apply(lambda x : select_multiple_col_values_and(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)
#sample.loc[((sample['ViewData.Side0_UniqueIds']!='') & (sample['ViewData.Side1_UniqueIds']!='')) ,'Trans_side'] = 'Both_side'

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

# ## New code

sample1['filter_key'] = sample1['ViewData.Source Combination'].astype(str) + sample1['filter_key'].astype(str) + sample1['ViewData.Fund'].astype(str)

sample1.loc[sample1['ViewData.Transaction Type']=='Proceeds Sell','ViewData.Transaction Type'] = 'Proceed Sell'
sample1['ViewData.Transaction Type'] = sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower())

sample1['filter_key_with_tt'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str)

sample1['filter_key_with_sd'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Settle Date'].astype(str)

sample1['filter_key_with_tt_sd'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str) +  sample1['ViewData.Settle Date'].astype(str)

sample1['filter_key_with_tt_inid'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str)

sample1['filter_key_with_inid'] = sample1['filter_key'].astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str)

sample1['filter_key_with_inid_qt'] = sample1['filter_key'].astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str) + sample1['ViewData.Quantity'].astype(str)

sample1['filter_key_with_inid_price'] = sample1['filter_key'].astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str) + sample1['ViewData.Price'].astype(str)

aa_new = sample1[sample1['Trans_side']=='A_side']
bb_new = sample1[sample1['Trans_side']=='B_side']
zz_new = sample1[sample1['Trans_side']=='Both_side']

#aa_new2 = aa_new[aa_new['Date']==dt]
#bb_new2 = bb_new[bb_new['Date']==dt]
cc_new2 = pd.concat([aa_new, bb_new,zz_new], axis=0)
cc_new2 = cc_new2.reset_index().drop('index',1)
cc_new2 = cc_new2.drop_duplicates()
cc_new2 = cc_new2.reset_index().drop('index',1)

cc_new2['Amount_diff2'] = cc_new2['ViewData.Accounting Net Amount'].fillna(0) - cc_new2['ViewData.B-P Net Amount'].fillna(0)

cc_new2['Amount_diff2_absolute'] = np.abs(cc_new2['ViewData.Accounting Net Amount'].fillna(0) - cc_new2['ViewData.B-P Net Amount'].fillna(0))

cc_new2.loc[cc_new2['ViewData.Transaction Type']=='Proceeds Sell','ViewData.Transaction Type'] = 'Proceed Sell'
cc_new2['ViewData.Transaction Type'] = cc_new2['ViewData.Transaction Type'].apply(lambda x: str(x).lower())

# ## One sided Open Ids

dd = cc_new2.groupby('filter_key')['Trans_side'].nunique().reset_index()
ee = cc_new2.groupby('filter_key')['Trans_side'].unique().reset_index()

#ee.columns = ['filter_key','Unique_side']
dd['Unique_side'] = ee['Trans_side']

dd['Both_side_flag'] = dd['Unique_side'].apply(lambda x: 1 if 'Both_side' in x else 0)

dd[(dd['Trans_side']==1) &(dd['Both_side_flag']==0)]

open_filter_key = []
if dd[(dd['Trans_side']==1) &(dd['Both_side_flag']==0)].shape[0] !=0:
    open_filter_key = dd[(dd['Trans_side']==1) &(dd['Both_side_flag']==0)]['filter_key'].unique()
open_table = cc_new2[cc_new2['filter_key'].isin(open_filter_key)][['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds']]
open_table = open_table.reset_index().drop('index',1)

if open_table.empty == False:
    open_table['Predicted_Status'] = 'Open'
else:
    open_table = pd.DataFrame()

if open_table.empty == False:
#    open_one_side = [key for key in list(open_table['ViewData.Side1_UniqueIds'].unique()) if key!='nan']
#    open_zero_side = [key for key in list(open_table['ViewData.Side0_UniqueIds'].unique()) if key!='nan']
    open_one_side = [key for key in list(open_table['ViewData.Side1_UniqueIds'].unique()) if key!='None']
    open_zero_side = [key for key in list(open_table['ViewData.Side0_UniqueIds'].unique()) if key!='None']

else:
    open_one_side = []
    open_zero_side = []
    
if len(open_one_side) >0:
    cc_new2 = cc_new2[~cc_new2['ViewData.Side1_UniqueIds'].isin(open_one_side)]
if len(open_zero_side) >0:
    cc_new2 = cc_new2[~cc_new2['ViewData.Side0_UniqueIds'].isin(open_zero_side)]
    
cc_new2 = cc_new2.reset_index().drop('index',1)    

# ## 1. UpDown

cc_new2['Updown_flag'] = cc_new2['ViewData.InternalComment2'].apply(lambda x: 1 if 'Up' in str(x) else 0)

ud = cc_new2[cc_new2['Updown_flag']==1]
ud = ud.reset_index().drop('index',1)

ud_amount = []
umr_id0 = []
umr_id1 = []

for key in ud['filter_key'].unique():
#print(key)
    ud2 = ud[(ud['filter_key'] ==key)]
    #ud3 = ud2.groupby(['Amount_diff2_absolute'])['Trans_side'].nunique().reset_index()
    #amount_array = ud3[ud3['Trans_side']==2]['Amount_diff2_absolute'].unique()
    #amount_array = [9999]
    amount_array = ud2['Amount_diff2_absolute'].unique()
   
    for i in amount_array:
        #print(i)
#        id1 = ud2[(ud2['Amount_diff2_absolute']==i) & (ud2['ViewData.Side1_UniqueIds']!='nan')]['ViewData.Side1_UniqueIds'].values
#        id0 = ud2[(ud2['Amount_diff2_absolute']==i) & (ud2['ViewData.Side0_UniqueIds']!='nan')]['ViewData.Side0_UniqueIds'].values
        id1 = ud2[(ud2['Amount_diff2_absolute']==i) & (ud2['ViewData.Side1_UniqueIds'] != 'None')]['ViewData.Side1_UniqueIds'].values
        id0 = ud2[(ud2['Amount_diff2_absolute']==i) & (ud2['ViewData.Side0_UniqueIds'] != 'None')]['ViewData.Side0_UniqueIds'].values
        
        
        if len(id1)>=1 and len(id0)>=1:
            umr_id0.append(id0)
            umr_id1.append(id1)

if len(umr_id1)!=0: 
    pair_table = pd.DataFrame(np.array(umr_id0))
    pair_table.columns =['ViewData.Side0_UniqueIds']
    pair_table['ViewData.Side1_UniqueIds'] = np.array(umr_id1)
else:
    pair_table = pd.DataFrame()

pair_table['Predicted_Status'] = 'Pair-UMR'
pair_table['diff'] = 0

if len(umr_id1)>0:
    umr_id1 = np.concatenate(umr_id1)
else:
    umr_id1 = ['Not-defined']
if len(umr_id0)>0:
    umr_id0 = np.concatenate(umr_id0)
else:
    umr_id0 =['Not-defined']

if pair_table.empty ==False:
    cc_new3 = cc_new2[~cc_new2['ViewData.Side0_UniqueIds'].isin(umr_id0)]
    cc_new3 = cc_new3[~cc_new3['ViewData.Side1_UniqueIds'].isin(umr_id1)]
    cc_new3 = cc_new3.reset_index().drop('index',1)
else:
    cc_new3 = cc_new2.copy()
    cc_new3 = cc_new3.reset_index().drop('index',1)


# ## 2. Filter on trans type and settle date
cc_new3[(cc_new3['TT_fee_commission_flag'] ==0) & (cc_new3['TT_proceed_sell_pnl_flag']==0)]['filter_key_with_tt_sd'].nunique()

tt_sd_ids_0 = []
tt_sd_ids_1 = []

for key in cc_new3[(cc_new3['TT_fee_commission_flag'] ==0)]['filter_key_with_tt_sd'].unique():
    dff =  cc_new3[(cc_new3['TT_fee_commission_flag'] ==0) &(cc_new3['filter_key_with_tt_sd']==key)]
    
    if dff['Trans_side'].nunique() >=2:
        sum_a = dff[dff['Trans_side']!='B_side']['Amount_diff2_absolute'].sum()
        sum_b = dff[dff['Trans_side']!='A_side']['Amount_diff2_absolute'].sum()
        
        if sum_a== sum_b:
            print(sum_a,sum_b)
            tt_sd_ids_0.append(dff[dff['Trans_side']!='B_side']['ViewData.Side1_UniqueIds'].unique())
            tt_sd_ids_1.append(dff[dff['Trans_side']!='A_side']['ViewData.Side0_UniqueIds'].unique())

#cc_new2[cc_new2['filter_key_with_tt_sd']=='Advent Geneva,Morgan StanleyMS_PB_MFUSDMFproceed sell06-02-2020'][['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Status','Trans_side','Amount_diff2_absolute']]

tt_sd_table = pd.DataFrame()
if len(tt_sd_ids_0)!=0:
    tt_sd_table['ViewData.Side0_UniqueIds'] = tt_sd_ids_0
    tt_sd_table['ViewData.Side1_UniqueIds'] = tt_sd_ids_1
    tt_sd_table['Predicted_Status'] = 'Pair-UMR'
    tt_sd_table['diff'] = 0

# ## Fee and Sell commission transaction type

# & (cc_new3['ViewData.SPM ID'].isnull()

fc = cc_new3[(cc_new3['TT_fee_sell_commission_flag']==1)]
fc = fc.reset_index().drop('index',1)

fc['ViewData.Investment ID'] = fc['ViewData.Investment ID'].apply(lambda x: str(x).replace(' ',''))
fc['ViewData.Investment ID'] = fc['ViewData.Investment ID'].apply(lambda x: x.lower())

pb_ids = fc.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side1_UniqueIds'].unique().reset_index()
acc_ids = fc.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side0_UniqueIds'].unique().reset_index()

#aggfunc=lambda x: len(x.unique()

#final_amount_table = pd.concat([acc_amount,pb_amount['ViewData.B-P Net Amount']], axis=1)
final_amount_table = pd.pivot_table(fc,\
                                    index=['filter_key','ViewData.Settle Date','ViewData.Investment ID'],\
                                    aggfunc={'ViewData.B-P Net Amount':np.sum,'ViewData.Accounting Net Amount':np.sum,'Trans_side':lambda x: len(x.unique()),'ViewData.Status':lambda x: str(x.unique())}).reset_index()
if(final_amount_table.empty == False):
    final_amount_table['diff'] = final_amount_table['ViewData.Accounting Net Amount'] - final_amount_table['ViewData.B-P Net Amount']
    #final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x).replace("[",''))
    #final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x).replace("]",''))
    #final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x).replace(" ",','))
    #final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x))
    
    final_amount_table['SMB_flag'] = final_amount_table['ViewData.Status'].apply(lambda x: 1 if 'SMB' in x else 0)
    final_amount_table['OB_flag'] = final_amount_table['ViewData.Status'].apply(lambda x: 1 if 'OB' in x else 0)
    
    final_amount_table['ViewData.Side1_UniqueIds'] = pb_ids['ViewData.Side1_UniqueIds']
    final_amount_table['ViewData.Side0_UniqueIds'] = acc_ids['ViewData.Side0_UniqueIds']
    #final_amount_table = final_amount_table[final_amount_table['Trans_side']==2]
    final_amount_table = final_amount_table.reset_index().drop('index',1)
    
    final_amount_table['ViewData.Side0_UniqueIds'] = final_amount_table['ViewData.Side0_UniqueIds'].apply(lambda x: [key for key in x if key != 'None'])
    final_amount_table['ViewData.Side1_UniqueIds'] = final_amount_table['ViewData.Side1_UniqueIds'].apply(lambda x: [key for key in x if key != 'None'])
    
pair_table2 = pd.DataFrame()
smb_table = pd.DataFrame()
if final_amount_table.shape[0] !=0:
    smb_table = final_amount_table[(final_amount_table['Trans_side']==1) & (final_amount_table['SMB_flag']==1)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table2 =  final_amount_table[(final_amount_table['Trans_side']==2)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table2['Predicted_Status'] = 'Pair-UMB'
    smb_table['Predicted_Status'] = 'SMB'
    
    

all_umb_ids1 = []
all_umb_ids0 = []

final_pair_table_sell = pd.concat([smb_table,pair_table2],axis=0)
final_pair_table_sell = final_pair_table_sell.reset_index().drop('index',1)
if final_pair_table_sell.shape[0] !=0:

    all_umb_ids1 =list(np.concatenate(final_pair_table_sell['ViewData.Side1_UniqueIds'].values))
    all_umb_ids0 =list(np.concatenate(final_pair_table_sell['ViewData.Side0_UniqueIds'].values))

if len(all_umb_ids0)==0:
    all_umb_ids0 = ['Not-defined']
if len(all_umb_ids1)==0:
    all_umb_ids1 = ['Not-defined']

# ## Fee and Buy commission transaction type

cc_new3 = cc_new3[~cc_new3['ViewData.Side1_UniqueIds'].isin(all_umb_ids1)]
cc_new3 = cc_new3[~cc_new3['ViewData.Side0_UniqueIds'].isin(all_umb_ids0)]
cc_new3 = cc_new3.reset_index().drop('index',1)

#cc_new3[(cc_new3['TT_fee_buy_commission_flag']==1)
fc_buy = cc_new3[(cc_new3['TT_fee_buy_commission_flag']==1)]
fc_buy = fc_buy.reset_index().drop('index',1)

fc_buy['ViewData.Investment ID'] = fc_buy['ViewData.Investment ID'].apply(lambda x: str(x).replace(' ',''))
fc_buy['ViewData.Investment ID'] = fc_buy['ViewData.Investment ID'].apply(lambda x: x.lower())

pb_ids_buy = fc_buy.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side1_UniqueIds'].unique().reset_index()
acc_ids_buy = fc_buy.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side0_UniqueIds'].unique().reset_index()

#aggfunc=lambda x: len(x.unique()

#final_amount_table = pd.concat([acc_amount,pb_amount['ViewData.B-P Net Amount']], axis=1)

final_amount_table_buy = pd.pivot_table(fc_buy,index=['filter_key','ViewData.Settle Date','ViewData.Investment ID'],aggfunc={'ViewData.B-P Net Amount':np.sum,'ViewData.Accounting Net Amount':np.sum,'Trans_side':lambda x: len(x.unique()),'ViewData.Status':lambda x: str(x.unique())}).reset_index()

final_amount_table_buy['diff'] = final_amount_table_buy['ViewData.Accounting Net Amount'] - final_amount_table_buy['ViewData.B-P Net Amount']

final_amount_table_buy['SMB_flag'] = final_amount_table_buy['ViewData.Status'].apply(lambda x: 1 if 'SMB' in x else 0)
final_amount_table_buy['OB_flag'] = final_amount_table_buy['ViewData.Status'].apply(lambda x: 1 if 'OB' in x else 0)

final_amount_table_buy['ViewData.Side1_UniqueIds'] = pb_ids_buy['ViewData.Side1_UniqueIds']
final_amount_table_buy['ViewData.Side0_UniqueIds'] = acc_ids_buy['ViewData.Side0_UniqueIds']
#final_amount_table_buy = final_amount_table_buy[final_amount_table_buy['Trans_side']==2]
final_amount_table_buy = final_amount_table_buy.reset_index().drop('index',1)

final_amount_table_buy['ViewData.Side0_UniqueIds'] = final_amount_table_buy['ViewData.Side0_UniqueIds'].apply(lambda x: [key for key in x if key != 'None'])
final_amount_table_buy['ViewData.Side1_UniqueIds'] = final_amount_table_buy['ViewData.Side1_UniqueIds'].apply(lambda x: [key for key in x if key != 'None'])

pair_table3 = pd.DataFrame()

if final_amount_table_buy.shape[0] !=0:
    smb_table2 = final_amount_table_buy[(final_amount_table_buy['Trans_side']==1) & (final_amount_table_buy['SMB_flag']==1)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table3 =  final_amount_table_buy[(final_amount_table_buy['Trans_side']==2)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table3['Predicted_Status'] = 'Pair-UMB'
    smb_table2['Predicted_Status'] = 'SMB'

final_pair_table_buy = pd.concat([smb_table2,pair_table3],axis=0)
final_pair_table_buy = final_pair_table_buy.reset_index().drop('index',1)

all_umb_ids1_buy = []
all_umb_ids0_buy = []
if final_pair_table_buy.empty == False:
    all_umb_ids1_buy =list(np.concatenate(final_pair_table_buy['ViewData.Side1_UniqueIds'].values))
    all_umb_ids0_buy =list(np.concatenate(final_pair_table_buy['ViewData.Side0_UniqueIds'].values))

if len(all_umb_ids0_buy)==0:
    all_umb_ids0_buy = ['None']
if len(all_umb_ids1_buy)==0:
    all_umb_ids1_buy = ['None']

# ## IDs left after removing UMBs

cc_new4 = cc_new3[~cc_new3['ViewData.Side0_UniqueIds'].isin(all_umb_ids0)]

cc_new4 = cc_new4[~cc_new4['ViewData.Side1_UniqueIds'].isin(all_umb_ids1)]
cc_new4 = cc_new4[~cc_new4['ViewData.Side0_UniqueIds'].isin(all_umb_ids0_buy)]
cc_new4 = cc_new4[~cc_new4['ViewData.Side1_UniqueIds'].isin(all_umb_ids1_buy)]

# ## FInal Output

final_pair_table = pd.concat([pair_table, pair_table2, pair_table3,tt_sd_table], axis=0)
final_pair_table = final_pair_table.reset_index().drop('index',1)

final_smb_table = pd.concat([smb_table,smb_table2], axis=0)
final_smb_table = final_smb_table.reset_index().drop('index',1)

#final_pair_table
#
#final_smb_table


open_one_side = [key for key in cc_new4['ViewData.Side1_UniqueIds'].unique() if key!='None']
open_zero_side = [key for key in cc_new4['ViewData.Side0_UniqueIds'].unique() if key!='None']

final_open_table = pd.DataFrame()
open_table1 = pd.DataFrame()
open_table2 = pd.DataFrame()

open_table1['ViewData.Side0_UniqueIds'] = open_zero_side
open_table2['ViewData.Side1_UniqueIds'] = open_one_side

final_open_table = pd.concat([open_table1,open_table2],axis=0)

final_open_table['Predicted_Status']= 'OB'
final_open_table['Predicted_action']= 'No-Pair'

final_open_table = final_open_table.reset_index().drop('index',1)

final_open_table2 = pd.concat([final_open_table,open_table],axis=0)
final_open_table2 = final_open_table2.reset_index().drop('index',1)

final_open_table2['Predicted_Status']= 'OB'

#Rohit Open Table Begins
##final_open_table2
#
final_open_table2['ViewData.Side0_UniqueIds'] = final_open_table2['ViewData.Side0_UniqueIds'].astype(str)
final_open_table2['ViewData.Side1_UniqueIds'] = final_open_table2['ViewData.Side1_UniqueIds'].astype(str)

final_open_table2['ViewData.Side0_UniqueIds'] = final_open_table2['ViewData.Side0_UniqueIds'].str.strip()
final_open_table2['ViewData.Side1_UniqueIds'] = final_open_table2['ViewData.Side1_UniqueIds'].str.strip()
 
final_open_table2.loc[final_open_table2['ViewData.Side0_UniqueIds']=='None','Side0_1_UniqueIds'] = final_open_table2['ViewData.Side1_UniqueIds']
final_open_table2.loc[final_open_table2['ViewData.Side1_UniqueIds']=='None','Side0_1_UniqueIds'] = final_open_table2['ViewData.Side0_UniqueIds']

final_open_table2.loc[final_open_table2['ViewData.Side0_UniqueIds']=='nan','Side0_1_UniqueIds'] = final_open_table2['ViewData.Side1_UniqueIds']
final_open_table2.loc[final_open_table2['ViewData.Side1_UniqueIds']=='nan','Side0_1_UniqueIds'] = final_open_table2['ViewData.Side0_UniqueIds']


del final_open_table2['ViewData.Side0_UniqueIds']
del final_open_table2['ViewData.Side1_UniqueIds']

#final_open_table2['BreakID_Side0'] = final_open_table2['ViewData.Side0_UniqueIds'].apply(lambda x : meo_df[meo_df['ViewData.Side0_UniqueIds'] == x]['ViewData.BreakID'].unique())
#final_open_table2['BreakID_Side1'] = final_open_table2['ViewData.Side1_UniqueIds'].apply(lambda x : meo_df[meo_df['ViewData.Side1_UniqueIds'] == x]['ViewData.BreakID'].unique())
#
#final_open_table2['BreakID_Side0'] = final_open_table2['BreakID_Side0'].astype(str)
#final_open_table2['BreakID_Side1'] = final_open_table2['BreakID_Side1'].astype(str)
#
#final_open_table2.loc[final_open_table2['BreakID_Side0']=='[]','BreakID'] = final_open_table2['BreakID_Side1']
#final_open_table2.loc[final_open_table2['BreakID_Side1']=='[]','BreakID'] = final_open_table2['BreakID_Side0']
#
#final_open_table2['BreakID'] = final_open_table2['BreakID'].astype(str)
#
#final_open_table2['BreakID'] = final_open_table2['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))

final_open_table2 = pd.merge(final_open_table2 , meo_df[['ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side1_UniqueIds', how='left')
final_open_table2 = pd.merge(final_open_table2 , meo_df[['ViewData.Side0_UniqueIds','ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'Side0_1_UniqueIds',right_on = 'ViewData.Side0_UniqueIds', how='left')


#    #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
final_open_table2['ML_flag'] = 'ML'
final_open_table2['SetupID'] = setup_code 

final_open_table2['ViewData.Task ID_x'] = final_open_table2['ViewData.Task ID_x'].astype(str)
final_open_table2['ViewData.Task ID_y'] = final_open_table2['ViewData.Task ID_y'].astype(str)
 
final_open_table2.loc[final_open_table2['ViewData.Task ID_x']=='None','Task ID'] = final_open_table2['ViewData.Task ID_y']
final_open_table2.loc[final_open_table2['ViewData.Task ID_y']=='None','Task ID'] = final_open_table2['ViewData.Task ID_x']

final_open_table2.loc[final_open_table2['ViewData.Task ID_x']=='nan','Task ID'] = final_open_table2['ViewData.Task ID_y']
final_open_table2.loc[final_open_table2['ViewData.Task ID_y']=='nan','Task ID'] = final_open_table2['ViewData.Task ID_x']

final_open_table2['ViewData.BreakID_x'] = final_open_table2['ViewData.BreakID_x'].astype(str)
final_open_table2['ViewData.BreakID_y'] = final_open_table2['ViewData.BreakID_y'].astype(str)
 
final_open_table2.loc[final_open_table2['ViewData.BreakID_x']=='None','BreakID'] = final_open_table2['ViewData.BreakID_y']
final_open_table2.loc[final_open_table2['ViewData.BreakID_y']=='None','BreakID'] = final_open_table2['ViewData.BreakID_x']

final_open_table2.loc[final_open_table2['ViewData.BreakID_x']=='nan','BreakID'] = final_open_table2['ViewData.BreakID_y']
final_open_table2.loc[final_open_table2['ViewData.BreakID_y']=='nan','BreakID'] = final_open_table2['ViewData.BreakID_x']


final_open_table2['ViewData.Task Business Date_x'] = final_open_table2['ViewData.Task Business Date_x'].astype(str)
final_open_table2['ViewData.Task Business Date_y'] = final_open_table2['ViewData.Task Business Date_y'].astype(str)
 
final_open_table2.loc[final_open_table2['ViewData.Task Business Date_x']=='None','Task Business Date'] = final_open_table2['ViewData.Task Business Date_y']
final_open_table2.loc[final_open_table2['ViewData.Task Business Date_y']=='None','Task Business Date'] = final_open_table2['ViewData.Task Business Date_x']

final_open_table2.loc[final_open_table2['ViewData.Task Business Date_x']=='nan','Task Business Date'] = final_open_table2['ViewData.Task Business Date_y']
final_open_table2.loc[final_open_table2['ViewData.Task Business Date_y']=='nan','Task Business Date'] = final_open_table2['ViewData.Task Business Date_x']

final_open_table2.loc[final_open_table2['ViewData.Task Business Date_x']=='NaT','Task Business Date'] = final_open_table2['ViewData.Task Business Date_y']
final_open_table2.loc[final_open_table2['ViewData.Task Business Date_y']=='NaT','Task Business Date'] = final_open_table2['ViewData.Task Business Date_x']

final_open_table2['ViewData.Source Combination Code_x'] = final_open_table2['ViewData.Source Combination Code_x'].astype(str)
final_open_table2['ViewData.Source Combination Code_y'] = final_open_table2['ViewData.Source Combination Code_y'].astype(str)
 
final_open_table2.loc[final_open_table2['ViewData.Source Combination Code_x']=='None','Source Combination Code'] = final_open_table2['ViewData.Source Combination Code_y']
final_open_table2.loc[final_open_table2['ViewData.Source Combination Code_y']=='None','Source Combination Code'] = final_open_table2['ViewData.Source Combination Code_x']

final_open_table2.loc[final_open_table2['ViewData.Source Combination Code_x']=='nan','Source Combination Code'] = final_open_table2['ViewData.Source Combination Code_y']
final_open_table2.loc[final_open_table2['ViewData.Source Combination Code_y']=='nan','Source Combination Code'] = final_open_table2['ViewData.Source Combination Code_x']


final_open_table2.loc[final_open_table2['ViewData.Source Combination Code_x']=='NaT','Source Combination Code'] = final_open_table2['ViewData.Source Combination Code_y']
final_open_table2.loc[final_open_table2['ViewData.Source Combination Code_y']=='NaT','Source Combination Code'] = final_open_table2['ViewData.Source Combination Code_x']


final_open_table2['Final_predicted_break'] = ''

task_business_date_mod = final_open_table2['Task Business Date'].mode()[0]
final_open_table2.loc[final_open_table2['Task Business Date']=='NaT','Task Business Date'] = task_business_date_mod 
final_open_table2['Task Business Date'] = final_open_table2['Task Business Date'].fillna(task_business_date_mod)

final_open_table2['Task Business Date'] = pd.to_datetime(final_open_table2['Task Business Date'])
final_open_table2['Task Business Date'] = final_open_table2['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_open_table2['Task Business Date'] = pd.to_datetime(final_open_table2['Task Business Date'])


change_names_of_final_open_table2_mapping_dict = {
                                            'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                            'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                                            'ViewData.Task ID' : 'Task ID',
                                            'ViewData.Task Business Date' : 'Task Business Date',
                                            'ViewData.Source Combination Code' : 'Source Combination Code'
                                        }

final_open_table2.rename(columns = change_names_of_final_open_table2_mapping_dict, inplace = True)

final_open_table2['Predicted_action'] = 'No-Pair'
final_open_table2['PredictedComment'] = ''
final_open_table2['probability_No_pair'] = ''
final_open_table2['probability_UMB'] = ''
final_open_table2['probability_UMR'] = ''
final_open_table2['probability_UMT'] = ''

#Changing data types of columns as follows:
#Side0_UniqueIds, Side1_UniqueIds, Final_predicted_break, Predicted_action, probability_No_pair, probability_UMB, probability_UMR, BusinessDate, SourceCombinationCode, Predicted_Status, ML_flag - string
#BreakID, TaskID - int64
#SetupID - int32

final_open_table2[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR','probability_UMT', 'Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']] = final_open_table2[['Side0_UniqueIds', 'Side1_UniqueIds', 'Final_predicted_break', 'Predicted_action', 'probability_No_pair', 'probability_UMB', 'probability_UMR', 'probability_UMT','Source Combination Code', 'Predicted_Status', 'ML_flag','PredictedComment']].astype(str)


final_open_table2['Task ID'].fillna(0, inplace=True)

final_open_table2[['Task ID']] = final_open_table2[['Task ID']].astype(float)
final_open_table2[['Task ID']] = final_open_table2[['Task ID']].astype(np.int64)

final_open_table2[['SetupID']] = final_open_table2[['SetupID']].astype(int)

#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(float)
#final_table_to_write['Task ID'] = final_table_to_write['Task ID'].astype(np.int64)

change_col_names_final_open_table2_dict = {
                        'Task ID' : 'TaskID',
                        'Task Business Date' : 'BusinessDate',
                        'Source Combination Code' : 'SourceCombinationCode'
                        }
final_open_table2.rename(columns = change_col_names_final_open_table2_dict, inplace = True)

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

final_open_table2_to_write = final_open_table2[cols_for_database_new]

filepaths_final_open_table2_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_open_table2_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
#final_open_table2_to_write.to_csv(filepaths_final_open_table2_to_write)

#Rohit Open Table Ends




#Rohit code



del final_pair_table['diff']
del final_smb_table['diff']
#del final_open_table2['Predicted_action']

#final_table_list = [final_pair_table, final_smb_table, final_open_table2] 
final_table_list = [final_pair_table, final_smb_table] 
final_table_to_write = pd.concat(final_table_list)

filepaths_final_table_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_table_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
#final_table_to_write.to_csv(filepaths_final_table_to_write)

final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].astype(str)
final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].astype(str)

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].replace('\\n','',regex = True)
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].replace('\\n','',regex = True)

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].replace('BB','')
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].replace('AA','')

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].replace('None','')
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].replace('None','')

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].replace('nan','')
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].replace('nan','')

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].replace("' '","', '", regex = True)
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].replace("' '","', '", regex = True)

final_table_to_write['Side1_UniqueIds_contains_single_quote_True_False'] = final_table_to_write['ViewData.Side1_UniqueIds'].str.contains("'", regex=False)
final_table_to_write['Side0_UniqueIds_contains_single_quote_True_False'] = final_table_to_write['ViewData.Side0_UniqueIds'].str.contains("'", regex=False)

def add_single_quote_Side1_UniqueIds(row):
   if row['Side1_UniqueIds_contains_single_quote_True_False'] == False :
      return "'" + row['ViewData.Side1_UniqueIds'] + "'"
   else:
       return row['ViewData.Side1_UniqueIds']

def add_single_quote_Side0_UniqueIds(row):
   if row['Side0_UniqueIds_contains_single_quote_True_False'] == False :
      return "'" + row['ViewData.Side0_UniqueIds'] + "'"
   else:
       return row['ViewData.Side0_UniqueIds']

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write.apply(lambda row: add_single_quote_Side1_UniqueIds(row), axis=1)
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write.apply(lambda row: add_single_quote_Side0_UniqueIds(row), axis=1)

final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].replace("''","")
final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].replace("''","")

final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].apply(lambda x: list(x.replace("'","").split(',')))
final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].apply(lambda x: list(x.replace("'","").split(',')))

final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].apply(lambda x: [x_element.strip() for x_element in x])
final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].apply(lambda x: [x_element.strip() for x_element in x])

final_table_to_write['ViewData.Side0_UniqueIds'] = final_table_to_write['ViewData.Side0_UniqueIds'].apply(lambda x: [x_element for x_element in x if x_element])
final_table_to_write['ViewData.Side1_UniqueIds'] = final_table_to_write['ViewData.Side1_UniqueIds'].apply(lambda x: [x_element for x_element in x if x_element])

#final_pair_table['ViewData.Side1_UniqueIds'] = 
#final_pair_table['ViewData.Side1_UniqueIds'].apply(lambda x: x.lstrip().split(',')))

final_table_to_write['count_side0'] = final_table_to_write['ViewData.Side0_UniqueIds'].apply(lambda x : len(x))
final_table_to_write['count_side1'] = final_table_to_write['ViewData.Side1_UniqueIds'].apply(lambda x : len(x))
final_table_to_write['count_side_total'] = final_table_to_write['count_side0'] + final_table_to_write['count_side1']  

final_table_to_write['BreakID_Side1'] = final_table_to_write['ViewData.Side1_UniqueIds'].apply( \
                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df_date_i, \
                                                                                              fun_side_0_or_1 = 1, \
                                                                                              fun_str_list_Side_01_UniqueIds = x))

final_table_to_write['BreakID_Side0'] = final_table_to_write['ViewData.Side0_UniqueIds'].apply( \
                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df_date_i, \
                                                                                              fun_side_0_or_1 = 0, \
                                                                                              fun_str_list_Side_01_UniqueIds = x))

#final_pair_table['Predicted_Status_New'] = 
final_table_to_write.rename(columns = {
                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds'}, inplace = True)

#final_table_to_write['BusinessDate'] = final_table_to_write.apply(lambda x: get_BusinessDate_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
final_table_to_write['BusinessDate'] = date_i
final_table_to_write['TaskID'] = final_table_to_write.apply(lambda x: get_TaskID_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df_date_i, fun_row = x), axis=1)
final_table_to_write['SourceCombinationCode'] = final_table_to_write.apply(lambda x: get_SourceCombinationCode_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df_date_i, fun_row = x), axis=1)


final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMR') & (final_table_to_write['count_side0'] == 1) & (final_table_to_write['count_side1'] == 1)), 'Predicted_action'] = 'UMR_One_to_One'
#final_pair_table_umr_one_to_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] == 1))]

final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMR') & (final_table_to_write['count_side0'] > 1) & (final_table_to_write['count_side1'] == 1)), 'Predicted_action'] = 'UMR_One-Many_to_Many-One'
#final_pair_table_umr_side0_many_to_side1_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] == 1))]


final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMR') & (final_table_to_write['count_side0'] == 1) & (final_table_to_write['count_side1'] > 1)), 'Predicted_action'] = 'UMR_One-Many_to_Many-One'
#final_pair_table_umr_side0_one_to_side1_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] > 1))]

final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMR') & (final_table_to_write['count_side0'] > 1) & (final_table_to_write['count_side1'] > 1)), 'Predicted_action'] = 'UMR_Many_to_Many'
#final_pair_table_umr_many_to_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] > 1))]


final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMB') & (final_table_to_write['count_side0'] == 1) & (final_table_to_write['count_side1'] == 1)), 'Predicted_action'] = 'UMB_One_to_One'
#final_pair_table_umb_one_to_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] == 1))]

final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMB') & (final_table_to_write['count_side0'] > 1) & (final_table_to_write['count_side1'] == 1)), 'Predicted_action'] = 'UMB_One-Many_to_Many-One'
#final_pair_table_umb_side0_many_to_side1_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] == 1))]


final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMB') & (final_table_to_write['count_side0'] == 1) & (final_table_to_write['count_side1'] > 1)), 'Predicted_action'] = 'UMB_One-Many_to_Many-One'
#final_pair_table_umb_side0_one_to_side1_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] > 1))]

final_table_to_write.loc[((final_table_to_write['Predicted_Status'] == 'Pair-UMB') & (final_table_to_write['count_side0'] > 1) & (final_table_to_write['count_side1'] > 1)), 'Predicted_action'] = 'UMB_Many_to_Many'
#final_pair_table_umb_many_to_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] > 1))]

final_table_to_write.loc[(final_table_to_write['Predicted_Status'] == 'OB'), 'Predicted_action'] = 'No-Pair'
final_table_to_write.loc[(final_table_to_write['Predicted_Status'] == 'SMB'), 'Predicted_action'] = 'SMB'

final_table_to_write.loc[(final_table_to_write['Predicted_Status'] == 'Pair-UMR'), 'Predicted_Status_New'] = 'UMR'
final_table_to_write.loc[(final_table_to_write['Predicted_Status'] == 'Pair-UMB'), 'Predicted_Status_New'] = 'UMB'
final_table_to_write.loc[(final_table_to_write['Predicted_Status'] == 'SMB'), 'Predicted_Status_New'] = 'SMB'
final_table_to_write.loc[(final_table_to_write['Predicted_Status'] == 'OB'), 'Predicted_Status_New'] = 'OB'

final_table_to_write['ML_flag'] = 'ML'
final_table_to_write['SetupID'] = setup_code 
final_table_to_write['PredictedComment'] = ''
final_table_to_write['PredictedCategory'] = ''
final_table_to_write['probability_No_pair'] = ''
final_table_to_write['probability_UMR'] = ''
final_table_to_write['probability_UMT'] = ''
final_table_to_write['probability_UMB'] = ''


#final_table_to_write['BreakID_Side0'] = final_table_to_write['BreakID_Side0'].replace('None','')
#final_table_to_write['BreakID_Side1'] = final_table_to_write['BreakID_Side1'].replace('None','')
#filepaths_final_df_2= '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
#final_table_to_write.to_csv(filepaths_final_df_2)
#
#
#final_table_to_write.loc[((final_table_to_write['Predicted_Status_New'] == 'OB') & (final_table_to_write['BreakID_Side1'] != []), 'BreakID'] = final_table_to_write['BreakID_Side1']
#final_table_to_write.loc[((final_table_to_write['Predicted_Status_New'] == 'OB') & (final_table_to_write['BreakID_Side0'] != []), 'BreakID'] = final_table_to_write['BreakID_Side0']

final_table_to_write.rename(columns = {
                                                'BreakID_Side1' : 'BreakID',
                                                'BreakID_Side0' : 'Final_predicted_break'}, inplace = True)

final_table_to_write['TaskID'] = final_table_to_write['TaskID'].apply(lambda x: ['{:.1f}'.format(x_element) for x_element in x])

final_table_to_write['Side0_UniqueIds'] = final_table_to_write['Side0_UniqueIds'].astype(str)
final_table_to_write['Side1_UniqueIds'] = final_table_to_write['Side1_UniqueIds'].astype(str)
final_table_to_write['BreakID'] = final_table_to_write['BreakID'].astype(str)
final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].astype(str)
final_table_to_write['BusinessDate'] = final_table_to_write['BusinessDate'].astype(str)
final_table_to_write['TaskID'] = final_table_to_write['TaskID'].astype(str)
final_table_to_write['SourceCombinationCode'] = final_table_to_write['SourceCombinationCode'].astype(str)

final_table_to_write['Side1_UniqueIds'] = final_table_to_write['Side1_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['Side0_UniqueIds'] = final_table_to_write['Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['BreakID'] = final_table_to_write['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['BusinessDate'] = final_table_to_write['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['TaskID'] = final_table_to_write['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_table_to_write['SourceCombinationCode'] = final_table_to_write['SourceCombinationCode'].map(lambda x:x.lstrip('[').rstrip(']'))

final_table_to_write['Side1_UniqueIds'] = final_table_to_write['Side1_UniqueIds'].replace('\\n','',regex = True)
final_table_to_write['Side0_UniqueIds'] = final_table_to_write['Side0_UniqueIds'].replace('\\n','',regex = True)
final_table_to_write['BreakID'] = final_table_to_write['BreakID'].replace('\\n','',regex = True)
final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].replace('\\n','',regex = True)

final_table_to_write['Side1_UniqueIds'] = final_table_to_write['Side1_UniqueIds'].replace('None','')
final_table_to_write['Side0_UniqueIds'] = final_table_to_write['Side0_UniqueIds'].replace('None','')
final_table_to_write['BreakID'] = final_table_to_write['BreakID'].replace('None','')
final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].replace('None','')

final_table_to_write['Side1_UniqueIds'] = final_table_to_write['Side1_UniqueIds'].replace('nan','')
final_table_to_write['Side0_UniqueIds'] = final_table_to_write['Side0_UniqueIds'].replace('nan','')
final_table_to_write['BreakID'] = final_table_to_write['BreakID'].replace('nan','')
final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].replace('nan','')

final_table_to_write['Side1_UniqueIds'] = final_table_to_write['Side1_UniqueIds'].replace("', '","' '", regex = True)
final_table_to_write['Side0_UniqueIds'] = final_table_to_write['Side0_UniqueIds'].replace("', '","' '", regex = True)

final_table_to_write['BreakID'] = final_table_to_write['BreakID'].replace('\.0','',regex = True)
final_table_to_write['Final_predicted_break'] = final_table_to_write['Final_predicted_break'].replace('\.0','',regex = True)

final_table_to_write['BusinessDate'] = pd.to_datetime(final_table_to_write['BusinessDate'])
final_table_to_write['BusinessDate'] = final_table_to_write['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_table_to_write['BusinessDate'] = pd.to_datetime(final_table_to_write['BusinessDate'])

final_table_to_write['TaskID'] = final_table_to_write['TaskID'].map(lambda x:x.lstrip("'").rstrip("'"))
final_table_to_write['SourceCombinationCode'] = final_table_to_write['SourceCombinationCode'].map(lambda x:x.lstrip("'").rstrip("'"))

final_table_to_write[['SetupID']] = final_table_to_write[['SetupID']].astype(int)

final_table_to_write[['TaskID']] = final_table_to_write[['TaskID']].astype(float)
final_table_to_write[['TaskID']] = final_table_to_write[['TaskID']].astype(np.int64)

del final_table_to_write['Predicted_Status']

final_table_to_write.rename(columns = {'Predicted_Status_New' : 'Predicted_Status'}, inplace = True)

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
#if(setup_code == '125' or setup_code == '123'):
df_to_append['probability_UMT'] = ''
df_to_append['PredictedComment'] = ''
df_to_append['PredictedCategory'] = ''

filepaths_df_to_append = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\df_to_append_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
df_to_append.to_csv(filepaths_df_to_append)

final_table_to_write = final_table_to_write.append(df_to_append)

cols_for_database = ['BreakID', \
                     'BusinessDate', \
                     'Final_predicted_break', \
                     'ML_flag', \
                     'Predicted_Status', \
                     'Predicted_action', \
                     'SetupID', \
                     'Side0_UniqueIds', \
                     'Side1_UniqueIds', \
                     'SourceCombinationCode', \
                     'TaskID', \
                     'probability_No_pair', \
                     'probability_UMB', \
                     'probability_UMR', \
                     'PredictedCategory', \
                     'PredictedComment' \
                     ]

final_df = final_table_to_write[cols_for_database]

final_df_2 = final_df.append(final_open_table2_to_write)

#Fixing 'Not_Covered_by_ML' Statuses
Search_term = 'not_covered_by_ml'

final_df_2_Covered_by_ML_df = final_df_2[~final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

final_df_2_Not_Covered_by_ML_df = final_df_2[final_df_2['Predicted_Status'].str.lower().str.endswith(Search_term)]

def get_first_term_before_separator(single_string, separator):
    return(single_string.split(separator)[0])

final_df_2_Not_Covered_by_ML_df['Predicted_Status'] = final_df_2_Not_Covered_by_ML_df['Predicted_Status'].apply(lambda x : get_first_term_before_separator(x,'_'))
final_df_2_Not_Covered_by_ML_df['ML_flag'] = 'Not_Covered_by_ML'

final_df_2 = final_df_2_Covered_by_ML_df.append(final_df_2_Not_Covered_by_ML_df)


final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2['BreakID'] = final_df_2['BreakID'].replace('\.0','',regex = True)
final_df_2[['BreakID']] = final_df_2[['BreakID']].astype(str)

final_df_2['probability_UMT'] = ''

def ui_action_column_weiss_futures_4_setups(param_final_df):
    param_final_df.loc[((param_final_df['ML_flag'] == 'Not_Covered_by_ML')),'ActionType'] = 'No Prediction'    
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'OB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'No Action'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'OB') & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'COMMENT'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'CLOSE'
    param_final_df.loc[((param_final_df['Predicted_Status'] == 'UCB') & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'CLOSE WITH COMMENT'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT','SMB'])) & (param_final_df['PredictedComment'] == '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR'
    param_final_df.loc[((param_final_df['Predicted_Status'].isin(['UMB','UMR','UMT','SMB'])) & (param_final_df['PredictedComment'] != '') & (param_final_df['ML_flag'] == 'ML')),'ActionType'] = 'PAIR WITH COMMENT'
    param_final_df['ActionType'] = param_final_df['ActionType'].astype(str)
    return(param_final_df)    

final_df_2= ui_action_column_weiss_futures_4_setups(final_df_2)

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df_2.to_csv(filepaths_final_df_2)

data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
coll_1_for_writing_prediction_data.insert_many(data_dict) 

print(setup_code)
print(date_i)

#
#
##TODO from here
##final_pair_table
##final_pair_table to be bifurcated into 6 parts
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].astype(str)
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].astype(str)
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].map(lambda x:x.lstrip('[').rstrip(']'))
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].replace('\\n','',regex = True)
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].replace('\\n','',regex = True)
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].replace('BB','')
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].replace('AA','')
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].replace('None','')
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].replace('None','')
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].replace('nan','')
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].replace('nan','')
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].replace("' '","', '", regex = True)
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].replace("' '","', '", regex = True)
#
#final_pair_table['Side1_UniqueIds_contains_single_quote_True_False'] = final_pair_table['ViewData.Side1_UniqueIds'].str.contains("'", regex=False)
#final_pair_table['Side0_UniqueIds_contains_single_quote_True_False'] = final_pair_table['ViewData.Side0_UniqueIds'].str.contains("'", regex=False)
#
#def add_single_quote_Side1_UniqueIds(row):
#   if row['Side1_UniqueIds_contains_single_quote_True_False'] == False :
#      return "'" + row['ViewData.Side1_UniqueIds'] + "'"
#   else:
#       return row['ViewData.Side1_UniqueIds']
#
#def add_single_quote_Side0_UniqueIds(row):
#   if row['Side0_UniqueIds_contains_single_quote_True_False'] == False :
#      return "'" + row['ViewData.Side0_UniqueIds'] + "'"
#   else:
#       return row['ViewData.Side0_UniqueIds']
#
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table.apply(lambda row: add_single_quote_Side1_UniqueIds(row), axis=1)
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table.apply(lambda row: add_single_quote_Side0_UniqueIds(row), axis=1)
#
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].apply(lambda x: list(x.replace("'","").split(',')))
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].apply(lambda x: list(x.replace("'","").split(',')))
#
#final_pair_table['ViewData.Side0_UniqueIds'] = final_pair_table['ViewData.Side0_UniqueIds'].apply(lambda x: [x_element.strip() for x_element in x])
#final_pair_table['ViewData.Side1_UniqueIds'] = final_pair_table['ViewData.Side1_UniqueIds'].apply(lambda x: [x_element.strip() for x_element in x])
##final_pair_table['ViewData.Side1_UniqueIds'] = 
##final_pair_table['ViewData.Side1_UniqueIds'].apply(lambda x: x.lstrip().split(',')))
#
#final_pair_table['count_side0'] = final_pair_table['ViewData.Side0_UniqueIds'].apply(lambda x : len(x))
#final_pair_table['count_side1'] = final_pair_table['ViewData.Side1_UniqueIds'].apply(lambda x : len(x))
#final_pair_table['count_side_total'] = final_pair_table['count_side0'] + final_pair_table['count_side1']  
#
#final_pair_table['BreakID_Side1'] = final_pair_table['ViewData.Side1_UniqueIds'].apply( \
#                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
#                                                                                              fun_side_0_or_1 = 1, \
#                                                                                              fun_str_list_Side_01_UniqueIds = x))
#
#final_pair_table['BreakID_Side0'] = final_pair_table['ViewData.Side0_UniqueIds'].apply( \
#                                        lambda x : get_BreakID_from_list_of_Side_01_UniqueIds(fun_meo_df = meo_df, \
#                                                                                              fun_side_0_or_1 = 0, \
#                                                                                              fun_str_list_Side_01_UniqueIds = x))
#
##final_pair_table['Predicted_Status_New'] = 
#final_pair_table.rename(columns = {
#                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
#                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds'}, inplace = True)
#
#final_pair_table['BusinessDate'] = final_pair_table.apply(lambda x: get_BusinessDate_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_pair_table['TaskID'] = final_pair_table.apply(lambda x: get_TaskID_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_pair_table['SourceCombinationCode'] = final_pair_table.apply(lambda x: get_SourceCombinationCode_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] == 1)), 'Predicted_Status_New'] = 'UMR_One_to_One'
##final_pair_table_umr_one_to_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] == 1))]
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] == 1)), 'Predicted_Status_New'] = 'UMR_One-Many_to_Many-One'
##final_pair_table_umr_side0_many_to_side1_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] == 1))]
#
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] > 1)), 'Predicted_Status_New'] = 'UMR_One-Many_to_Many-One'
##final_pair_table_umr_side0_one_to_side1_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] > 1))]
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] > 1)), 'Predicted_Status_New'] = 'UMR_Many_to_Many'
##final_pair_table_umr_many_to_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMR') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] > 1))]
#
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] == 1)), 'Predicted_Status_New'] = 'UMB_One_to_One'
##final_pair_table_umb_one_to_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] == 1))]
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] == 1)), 'Predicted_Status_New'] = 'UMB_One-Many_to_Many-One'
##final_pair_table_umb_side0_many_to_side1_one = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] == 1))]
#
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] > 1)), 'Predicted_Status_New'] = 'UMB_One-Many_to_Many-One'
##final_pair_table_umb_side0_one_to_side1_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] == 1) & (final_pair_table['count_side1'] > 1))]
#
#final_pair_table.loc[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] > 1)), 'Predicted_Status_New'] = 'UMB_Many_to_Many'
##final_pair_table_umb_many_to_many = final_pair_table[((final_pair_table['Predicted_Status'] == 'Pair-UMB') & (final_pair_table['count_side0'] > 1) & (final_pair_table['count_side1'] > 1))]
#
###final_pair_table_umr_one_to_one
##final_pair_table_umr_one_to_one.rename(columns = {
##                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
##                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds'}, inplace = True)
##
##final_pair_table_umr_one_to_one['BusinessDate'] = final_pair_table_umr_one_to_one.apply(lambda x: get_BusinessDate_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
##final_pair_table_umr_one_to_one['TaskID'] = final_pair_table_umr_one_to_one.apply(lambda x: get_TaskID_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
##final_pair_table_umr_one_to_one['SourceCombinationCode'] = final_pair_table_umr_one_to_one.apply(lambda x: get_SourceCombinationCode_from_list_first_element_as_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
##filepaths_final_pair_table_umr_one_to_one = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_pair_table_umr_one_to_one_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
##final_pair_table_umr_one_to_one.to_csv(filepaths_final_pair_table_umr_one_to_one)
#
#
#filepaths_final_pair_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_pair_table_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
#final_pair_table.to_csv(filepaths_final_pair_table)
#
#filepaths_final_smb_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_smb_table_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
#final_smb_table.to_csv(filepaths_final_smb_table)
##final_pair_table['ViewData.Side0_UniqueIds'].apply(lambda x : list(ast.literal_eval(x)))
##filepaths_final_open_table2_to_write = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_open_table2_to_write_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
##final_open_table2_to_write.to_csv(filepaths_final_open_table2_to_write)
##
##final_pair_table['ViewData.Side0_UniqueIds'].head().str.split(',').tolist()[0]
##import json
##final_pair_table['ViewData.Side0_UniqueIds'].apply(lambda x: json.loads(x))
#
#
#
##filepaths_final_smb_table = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_smb_table_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
##final_smb_table.to_csv(filepaths_final_smb_table)
##
##
##
##
##
##
##
##Single_Side0_UniqueId_for_merging_with_meo_df = final_mtm_table_copy['ViewData.Side0_UniqueIds'][0][0]
##final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = Single_Side0_UniqueId_for_merging_with_meo_df
##final_mtm_table_copy['ViewData.Side0_UniqueIds'] = final_mtm_table_copy['ViewData.Side0_UniqueIds'].astype(str)
##final_mtm_table_copy['ViewData.Side1_UniqueIds'] = final_mtm_table_copy['ViewData.Side1_UniqueIds'].astype(str)
##final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'] = final_mtm_table_copy['ViewData.Side0_UniqueIds_for_merging'].astype(str) 
##
##final_mtm_table_copy_new = pd.merge(final_mtm_table_copy, meo_df[['ViewData.Side0_UniqueIds','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'ViewData.Side0_UniqueIds_for_merging', right_on = 'ViewData.Side0_UniqueIds', how='left')
