#!/usr/bin/env python
# coding: utf-8
import os
os.chdir('D:\\ViteosModel')

import pandas as pd
import numpy as np
import datetime as dt
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from pandas.io.json import json_normalize

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

def get_BreakID_from_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((fun_row['Side0_UniqueIds'] != '') & (fun_row['Side1_UniqueIds'] == '')):
        element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds']])]['ViewData.BreakID'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] == '')):
        element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.BreakID'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] != '')):
#        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.BreakID'].unique()
#        if((len(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Side0_UniqueIds'].unique()) == 1 ) & \
#           (fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Side0_UniqueIds'].unique() == fun_row['Side0_UniqueIds']) & \
#            (len(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.BreakID'].unique()) == 1 )):
#            print(fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Side0_UniqueIds'].unique())
        element_BreakID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.BreakID'].unique()
#        else:
#            element_BreakID_corresponding_to_Side_01_UniqueIds = 'BreakID_Ambiguous'
#        element_BreakID_corresponding_to_Side_01_UniqueIds = 'BreakID_WIP'
    return(element_BreakID_corresponding_to_Side_01_UniqueIds )

def get_TaskID_from_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((fun_row['Side0_UniqueIds'] != '') & (fun_row['Side1_UniqueIds'] == '')):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds']])]['ViewData.Task ID'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] == '')):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Task ID'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] != '')):
        element_TaskID_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Task ID'].unique()
    return(element_TaskID_corresponding_to_Side_01_UniqueIds )

def get_SourceCombinationCode_from_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((fun_row['Side0_UniqueIds'] != '') & (fun_row['Side1_UniqueIds'] == '')):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds']])]['ViewData.Source Combination Code'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] == '')):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Source Combination Code'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] != '')):
        element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Source Combination Code'].unique()
    return(element_SourceCombinationCode_corresponding_to_Side_01_UniqueIds )

def get_BusinessDate_from_single_string_of_Side_01_UniqueIds(fun_row, fun_meo_df):
#    value_TaskID_corresponding_to_Side_01_UniqueIds = []
#    for str_element_Side_01_UniqueIds in fun_str_list_Side_01_UniqueIds:
    if((fun_row['Side0_UniqueIds'] != '') & (fun_row['Side1_UniqueIds'] == '')):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side0_UniqueIds'].isin([fun_row['Side0_UniqueIds']])]['ViewData.Task Business Date'].unique()
#        value_TaskID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] == '')):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Task Business Date'].unique()
#        list_BreakID_corresponding_to_Side_01_UniqueIds.append(element_BreakID_corresponding_to_Side_01_UniqueIds[0])
    elif((fun_row['Side1_UniqueIds'] != '') & (fun_row['Side0_UniqueIds'] != '')):
        element_BusinessDate_corresponding_to_Side_01_UniqueIds = fun_meo_df[fun_meo_df['ViewData.Side1_UniqueIds'].isin([fun_row['Side1_UniqueIds']])]['ViewData.Task Business Date'].unique()
    return(element_BusinessDate_corresponding_to_Side_01_UniqueIds)

# ### Reading comments file
client = 'Schonfeld'

setup = '85'
setup_code = '85'
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

#for setup_code in setup_code_list:
print('Starting predictions for Weiss, setup_code = ')
print(setup_code)
#filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/no_pair_ids_' + setup_code + '_' + str(date_input) + '.csv'
#filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/UAT_Run/X_Test_' + setup_code + '/WARNING_no_pair_ids_' + setup_code + str(date_input) + '.csv'

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
print('meo size')
print(meo_df.shape[0])
date_i = pd.to_datetime(pd.to_datetime(meo_df['ViewData.Task Business Date'])).dt.date.astype(str).mode()[0]

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
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID',
#'UnMapped']
'Investment ID','Call Put Indicator']

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']

new_cols = ['ViewData.' + x for x in cols] + add

uni2 = meo_df.copy()

def futureacc(x):
    if type(x)==str:
        x1 = x.split('_')
        if len(x1)>=2:
            k = x1[1]
            k1 = x1[0]
            
            if k =='WHITNEY':
                return 1
            elif k1.endswith('FU'):
                return 2
            elif k1.endswith('COLL'):
                return 3
            else:
                return 0
    else:
        return 0

uni2['map_marker'] = uni2['ViewData.Mapped Custodian Account'].apply(lambda x : futureacc(x))

uni3 = uni2[uni2['map_marker']==2.0]

uni3 = uni3.rename(columns = {'ViewData.Net Amount Difference':'Net Amount Difference1',
                            'ViewData.Mapped Custodian Account':'Custodian Account',
                             'ViewData.Currency':'Currency',
                             'ViewData.Ticker':'Ticker1'})

def subSum(numbers,total):
    length = len(numbers)
    
    
    
    if length <20:
      
            
      
        
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=5).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers

def remove_mark(x,z,k):
    
   
    if ((x>1) & (x<20)):
        if ((k<6.0) & (z==0)):
            return 1
        elif ((k==0.0) & (z!=0)):
            return 1
        else:
            return 0
    else:
        return 0

if uni3.shape[0]!=0:
    dummy = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()
    dummy1 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'])['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'], how = 'left')
    dummy2 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'])['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'], how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    dummy = dummy[dummy['sel_mark']==1]
    dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
    dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))
    dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
    dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
    dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'] ,x['diff_len'], x['zero_list_sum']),axis = 1)
    dummy = dummy[['ViewData.Task Business Date','Custodian Account', 'Currency', 'Ticker1', 'zero_list',  'diff_len', 'remove_mark','sel_mark']]
    df3 = pd.merge(uni3, dummy, on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1'], how = 'left')
    df4 = df3[(df3['sel_mark']==1) & (df3['sel_mark']==1)]
    
    if df4.shape[0]!=0:
        df4['Predicted Category'] = 'Match'
        df4['Predicted Comment'] = 'Match'
        df4 = df4[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df4.to_csv('Schonfield 85 p1.csv')
        df5 = df3[~((df3['sel_mark']==1) & (df3['sel_mark']==1))]
        
        df5['Predicted Category'] = 'OTE'
        df5['Predicted Comment'] = 'Yet to be commented'
        df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df5.to_csv('Schonfield 85 p2.csv')
    else:
        df5 = df3.copy()
        
        df5['Predicted Category'] = 'OTE'
        df5['Predicted Comment'] = 'Yet to be commented'
        df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df5.to_csv('Schonfield 85 p3.csv')

uni3 = uni2[uni2['map_marker']==0.0]

uni3 = uni3.rename(columns = {'ViewData.Net Amount Difference':'Net Amount Difference1',
                            'ViewData.Mapped Custodian Account':'Custodian Account',
                             'ViewData.Currency':'Currency',
                             'ViewData.Ticker':'Ticker1'})

if uni3.shape[0]!=0:
    dummy = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date'])['Net Amount Difference1'].apply(list).reset_index()
    dummy1 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date'])['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date'], how = 'left')
    dummy2 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date'])['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date'], how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    dummy = dummy[dummy['sel_mark']==1]
    dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
    dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))
    dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
    dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
    dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'] ,x['diff_len'], x['zero_list_sum']),axis = 1)
    dummy = dummy[['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date', 'zero_list',  'diff_len', 'remove_mark','sel_mark']]
    df3 = pd.merge(uni3, dummy, on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Transaction Type','ViewData.Settle Date'], how = 'left')
    df4 = df3[(df3['sel_mark']==1) & (df3['sel_mark']==1)]
    
    if df4.shape[0]!=0:
        df4['Predicted Category'] = 'Match'
        df4['Predicted Comment'] = 'Match'
        df4 = df4[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df4.to_csv('Schonfield 85 p4.csv')
        df5 = df3[~((df3['sel_mark']==1) & (df3['sel_mark']==1))]
        uni3 = df5.copy()
        dummy = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency'])['Net Amount Difference1'].apply(list).reset_index()
        dummy1 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency'])['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
        dummy2 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency'])['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
        dummy = dummy[dummy['sel_mark']==1]
        
        dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
        dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        df3 = pd.DataFrame()
        df4 = pd.DataFrame()
        df5 = pd.DataFrame()
        
        uni3.drop(['zero_list', 'diff_len', 'remove_mark', 'sel_mark'], axis = 1, inplace = True)
        
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'] ,x['diff_len'], x['zero_list_sum']),axis = 1)
        dummy = dummy[['ViewData.Task Business Date','Custodian Account','Currency','zero_list',  'diff_len', 'remove_mark','sel_mark']]
        df3 = pd.merge(uni3, dummy, on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
        df4 = df3[(df3['sel_mark']==1) & (df3['sel_mark']==1)]
        
        
        if df4.shape[0]!=0:
            
            df4['Predicted Category'] = 'Match'
            df4['Predicted Comment'] = 'Match'
            df4 = df4[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
            df4.to_csv('Schonfield 85 p5.csv')
        
        
            df5 = df3[~((df3['sel_mark']==1) & (df3['sel_mark']==1))]
            df5['Predicted Category'] = 'OTE'
            df5['Predicted Comment'] = 'Yet to be commented'
            df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
            df5.to_csv('Schonfield 85 p6.csv')
        else:
            df5 = df3.copy()
        
            df5['Predicted Category'] = 'OTE'
            df5['Predicted Comment'] = 'Yet to be commented'
            df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
            df5.to_csv('Schonfield 85 p7.csv')
    else:
        df5 = df3.copy()
        
        df5['Predicted Category'] = 'OTE'
        df5['Predicted Comment'] = 'Yet to be commented'
        df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df5.to_csv('Schonfield 85 p8.csv')
        
uni3 = uni2[uni2['map_marker']==1.0]

uni3 = uni3.rename(columns = {'ViewData.Net Amount Difference':'Net Amount Difference1',
                            'ViewData.Mapped Custodian Account':'Custodian Account',
                             'ViewData.Currency':'Currency',
                             'ViewData.Ticker':'Ticker1'})

if uni3.shape[0]!=0:
    dummy = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date'])['Net Amount Difference1'].apply(list).reset_index()
    dummy1 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date'])['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date'], how = 'left')
    dummy2 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date'])['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date'], how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    dummy = dummy[dummy['sel_mark']==1]
    dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
    dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))
    dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
    dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
    dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
    dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'] ,x['diff_len'], x['zero_list_sum']),axis = 1)
    dummy = dummy[['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date', 'zero_list',  'diff_len', 'remove_mark','sel_mark']]
    df3 = pd.merge(uni3, dummy, on = ['ViewData.Task Business Date','Custodian Account','Currency','Ticker1','ViewData.Settle Date'], how = 'left')
    df4 = df3[(df3['sel_mark']==1) & (df3['sel_mark']==1)]
    
    if df4.shape[0]!=0:
        df4['Predicted Category'] = 'Match'
        df4['Predicted Comment'] = 'Match'
        df4 = df4[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df4.to_csv('Schonfield 85 p9.csv')
        df5 = df3[~((df3['sel_mark']==1) & (df3['sel_mark']==1))]
        uni3 = df5.copy()
        dummy = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency'])['Net Amount Difference1'].apply(list).reset_index()
        dummy1 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency'])['ViewData.Side0_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy1 , on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
        dummy2 = uni3.groupby(['ViewData.Task Business Date','Custodian Account','Currency'])['ViewData.Side1_UniqueIds'].count().reset_index()
        dummy = pd.merge(dummy, dummy2 , on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
        dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
        dummy = dummy[dummy['sel_mark']==1]
        
        dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))
        dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
        
        df3 = pd.DataFrame()
        df4 = pd.DataFrame()
        df5 = pd.DataFrame()
        
        uni3.drop(['zero_list', 'diff_len', 'remove_mark', 'sel_mark'], axis = 1, inplace = True)
        
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'] ,x['diff_len'], x['zero_list_sum']),axis = 1)
        dummy = dummy[['ViewData.Task Business Date','Custodian Account','Currency','zero_list',  'diff_len', 'remove_mark','sel_mark']]
        df3 = pd.merge(uni3, dummy, on = ['ViewData.Task Business Date','Custodian Account','Currency'], how = 'left')
        df4 = df3[(df3['sel_mark']==1) & (df3['sel_mark']==1)]
        
        
        if df4.shape[0]!=0:
            
            df4['Predicted Category'] = 'Match'
            df4['Predicted Comment'] = 'Match'
            df4 = df4[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
            df4.to_csv('Schonfield 85 p10.csv')
        
        
            df5 = df3[~((df3['sel_mark']==1) & (df3['sel_mark']==1))]
            df5['Predicted Category'] = 'OTE'
            df5['Predicted Comment'] = 'Yet to be commented'
            df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
            df5.to_csv('Schonfield 85 p11.csv')
        else:
            df5 = df3.copy()
        
            df5['Predicted Category'] = 'OTE'
            df5['Predicted Comment'] = 'Yet to be commented'
            df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
            df5.to_csv('Schonfield 85 p12.csv')
    else:
        df5 = df3.copy()
        
        df5['Predicted Category'] = 'OTE'
        df5['Predicted Comment'] = 'Yet to be commented'
        df5 = df5[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','Predicted Category','Predicted Comment']]
        df5.to_csv('Schonfield 85 p13.csv')
        
    
def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
    frames = []
    current_folder = os.getcwd()
    full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
    for full_filepath in full_filepath_list :
        if os.path.isfile(full_filepath) == True:
            frames.append(pd.read_csv(full_filepath))
    return pd.concat(frames)


# #### Combining all the files
final_df_filename_list = ['Schonfield 85 p' + str(x) + '.csv' for x in [1,2,3,4,5,6,7,8,9,10,11,12,13]]
final_df = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(final_df_filename_list)
cols_to_rename_final_df_mapping_dict = {'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                                        'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',	
                                        'Predicted Category' : 'PredictedCategory',	
                                        'Predicted Comment' : 'PredictedComment'}
final_df.rename(columns = cols_to_rename_final_df_mapping_dict, inplace = True)

#TODO : Below columns need to be filled with values
#Begin
final_df['Final_predicted_break'] = ''


final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].astype(str)
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].astype(str)

final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('\\n','',regex = True)
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('\\n','',regex = True)
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('BB','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('AA','')
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('None','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('None','')
final_df['Side1_UniqueIds'] = final_df['Side1_UniqueIds'].replace('nan','')
final_df['Side0_UniqueIds'] = final_df['Side0_UniqueIds'].replace('nan','')


#Rough Begin
if((final_df['Side0_UniqueIds'].iloc[0] != '') & (final_df['Side1_UniqueIds'].iloc[0] == '')):
    element_BreakID_corresponding_to_Side_01_UniqueIds = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin([final_df['Side0_UniqueIds'].iloc[0]])]['ViewData.BreakID'].unique()

if((final_df['Side0_UniqueIds'].iloc[0] == '') & (final_df['Side1_UniqueIds'].iloc[0] != '')):
    element_BreakID_corresponding_to_Side_01_UniqueIds = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin([final_df['Side1_UniqueIds'].iloc[0]])]['ViewData.BreakID'].unique()

#Rough End



final_df['BreakID'] = final_df.apply(lambda x: get_BreakID_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
final_df['BusinessDate'] = ''
final_df['SourceCombinationCode'] = ''
final_df['TaskID'] = ''
#End

final_df['Final_predicted_break'] = final_df['Final_predicted_break'].astype(str)

final_df['ML_flag'] = 'ML'

final_df['SetupID'] = setup_code

final_df['probability_No_pair'] = ''
final_df['probability_UMB'] = ''
final_df['probability_UMR'] = ''
final_df['probability_UMT'] = ''

final_df.loc[final_df['PredictedComment'] != 'Match','Predicted_Status'] = 'OB'
final_df.loc[final_df['PredictedComment'] != 'Match','Predicted_action'] = 'No-Pair'    

final_df.loc[final_df['PredictedComment'] != 'Match','Predicted_Status'] = ''
final_df.loc[final_df['PredictedComment'] != 'Match','Predicted_action'] = ''
    
cols_for_database = ['BreakID', 'BusinessDate', 'Final_predicted_break', 'ML_flag',
       'Predicted_Status', 'Predicted_action', 'SetupID',
       'SourceCombinationCode', 'TaskID', 'probability_No_pair',
       'probability_UMB', 'probability_UMR', 'probability_UMT',
       'Side1_UniqueIds', 'PredictedComment', 'PredictedCategory',
       'Side0_UniqueIds']    
    
    

final_df_2 = final_df[cols_for_database]

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

final_df_2['BusinessDate'] = final_df_2.apply(lambda x: get_BusinessDate_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
final_df_2['TaskID'] = final_df_2.apply(lambda x: get_TaskID_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
final_df_2['SourceCombinationCode'] = final_df_2.apply(lambda x: get_SourceCombinationCode_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)


final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df_2['BusinessDate'] = date_i
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])


final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2['TaskID'] = final_df_2['TaskID'].astype(str)
final_df_2['TaskID'] = final_df_2['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].astype(str)



final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(float)
final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(np.int64)

final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(float)
final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(np.int64)
final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(str)

final_df_2['Predicted_Status'] = ''
final_df_2['Predicted_action'] = ''

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_2.csv'
final_df_2.to_csv(filepaths_final_df_2)

filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
meo_df.to_csv(filepaths_meo_df)

#data_dict = final_table_to_write.to_dict("records")
#data_dict = final_df_2.to_dict("records_final")
#coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
#coll_1_for_writing_prediction_data.insert_many(data_dict) 
#

