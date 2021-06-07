# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:14:08 2020

@author: consultant138
"""

#[‎6/‎29/‎2020 22:18]  Ritesh Kumar Patra:  
# <add name="mongodbServer" connectionString="mongodb://appuser:V!teo$@vitblrrecdb01.viteos.com:27017,vitblrrecdb02.viteos.com:27017,vitblrrecdb03.viteos.com:27017/ReconDB?replicaSet=reconReplSet;authSource=Users" />
  
import numpy as np
import pandas as pd
#from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
from tqdm import tqdm
import pickle
import sys
import os

   
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import pprint
import json

import dateutil.parser

print(os.getcwd())
os.chdir('D:\\ViteosModel')
print(os.getcwd())

orig_stdout = sys.stdout
f = open('OakTree_379_model_run_' + str(datetime.now().strftime("%d_%m_%Y_%H_%M")) + '.txt', 'w')
sys.stdout = f

print(datetime.now())
# In[4709]:


def equals_fun(a,b):
    if a == b:
        return 1
    else:
        return 0

vec_equals_fun = np.vectorize(equals_fun)

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
    
vec_tt_match = np.vectorize(mhreplaced)

def fundmatch(item):
    items = item.lower()
    items = item.replace(' ','') 
    return items

vec_fund_match = np.vectorize(fundmatch)

def nan_fun(x):
    if x=='nan':
        return 1
    else:
        return 0
    
vec_nan_fun = np.vectorize(nan_fun)

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

def nan_equals_fun(a,b):
    if a==1 and b==1:
        return 1
    else:
        return 0
    
vec_nan_equal_fun = np.vectorize(nan_equals_fun)

def new_key_match_fun(a,b,c):
    if a==b and c==0:
        return 1
    else:
        return 0
    
vec_new_key_match_fun = np.vectorize(new_key_match_fun)


cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
'Activity Code','Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount','Bloomberg_Yellow_Key',
'Cust Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
'Derived Source','Description','ExpiryDate','ExternalComment1','ExternalComment2',
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


# In[4710]:


new_cols = ['ViewData.' + x for x in cols] + add

cols_to_show = [
'Account Type',
'Accounting Net Amount',
#'Accounting Net Amount Difference',
'Activity Code',
'Age',
'Alt ID 1',
'Asset Type Category',
'Bloomberg_Yellow_Key',
'Cust Net Amount',
#'B-P Net Amount Difference',
#'B-P Net Amount Difference Absolute',
'BreakID',
'Business Date',
'Call Put Indicator',
'Cancel Amount',
'Cancel Flag',
'Commission',
'Currency',
'CUSIP',
'Custodian',
'Custodian Account',
'Department',
'Description',
'ExpiryDate',
'ExternalComment2',
'Fund',
'FX Rate',
'Interest Amount',
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
'OTE Custodian Account',
'OTE Ticker',
'PB Account Numeric',
'Portfolio ID',
'Portolio',
'Price',
'Prime Broker',
'Principal Amount',
'Quantity',
'Sec Fees',
'SEDOL',
'Settle Date',
'Status',
'Strike Price',
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
add_cols_to_show = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds']
viewdata_cols_to_show = ['ViewData.' + x for x in cols_to_show] + add_cols_to_show

common_cols = ['ViewData.Accounting Net Amount', 'ViewData.Age',
'ViewData.Age WK', 'ViewData.Asset Type Category',
'ViewData.B-P Net Amount', 'ViewData.Base Net Amount','ViewData.CUSIP', 
 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag',
#'ViewData.Commission',
        'ViewData.Currency', 'ViewData.Custodian',
       'ViewData.Custodian Account',
       'ViewData.Description', 'ViewData.ExpiryDate', 'ViewData.Fund',
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




def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d



#setup_code_input_list = ['379']
setup_code_input_list = ['125']
#setup_code_input_list = ['123']
# connection 1
mngdb_obj_1_for_instance_ids = mngdb(param_without_ssh = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb01.viteos.com', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
mngdb_obj_1_for_instance_ids.connect_with_or_without_ssh()
db_1_for_instance_ids = mngdb_obj_1_for_instance_ids.client['ReconDB']

# connection 2

mngdb_obj_2_for_data_in_list_instance_ids = mngdb(param_without_ssh = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb05.viteos.com', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
mngdb_obj_2_for_data_in_list_instance_ids.connect_with_or_without_ssh()
db_2_for_data_in_list_instance_ids = mngdb_obj_2_for_data_in_list_instance_ids.client['ReconDB']

mngdb_obj_3_for_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '192.168.170.158', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_3_for_writing_in_uat_server.connect_with_or_without_ssh()
db_3_for_writing_in_ml_server = mngdb_obj_3_for_writing_in_uat_server.client['MEO_AUA_Collections']
#db_4_for_MEO_data = mngdb_obj_3_for_writing_in_uat_server.client['MeoCollections']
db_4_for_MEO_data = mngdb_obj_3_for_writing_in_uat_server.client['ReconDB_ML']
db_5_for_AUA_data = mngdb_obj_3_for_writing_in_uat_server.client['AUACollections']
#db_6_for_prediction = mngdb_obj_3_for_writing_in_uat_server.client['MLPrediction_Cash']

today = date.today()
d1 = datetime.strptime(today.strftime("%Y-%m-%d"),"%Y-%m-%d")
desired_date = d1 - timedelta(days=6)
desired_date_str = desired_date.strftime("%Y-%m-%d")
date_input_list = [desired_date_str]

# loop start
for setup_code in setup_code_input_list:
    print('INITIATED : Setup wise extraction for following setup :')
    print(setup_code)
    coll_1_for_instance_ids = db_1_for_instance_ids['Tasks']
    coll_2_for_data_in_list_instance_ids = db_2_for_data_in_list_instance_ids['HST_RecData_' + setup_code]
    
    for date_input in date_input_list:
        filepaths_X_test = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test_379/x_test_379_' + str(date_input) + '.csv'
        filepaths_no_pair_id_data = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test_379/no_pair_ids_379_' + str(date_input) + '.csv'
        filepaths_no_pair_id_no_data_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test_379/WARNING_no_pair_ids_379_' + str(date_input) + '.csv'
        filepaths_AUA = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/AUA/AUACollections.AUA_HST_RecData_379_' + str(date_input) + '.csv'
        filepaths_MEO = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/MEO/MeoCollections.MEO_HST_RecData_379_' + str(date_input) + '.csv'
        filepaths_final_prediction_table = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions_379/Final_Predictions_Table_HST_RecData_379_' + str(date_input) + '.csv'
        filepaths_accuracy_table = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions_379/Accuracy_Table_HST_RecData_379_' + str(date_input) + '.csv'
        filepaths_crosstab_table = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions_379/Crosstab_Table_HST_RecData_379_' + str(date_input) + '.csv'


        print('INITIATED : Date wise extraction for following date :')
        print(date_input)
        date_input_iso = date_input + 'T18:30:00.000+0000'
        
        query_for_instance_id_setup_and_date = coll_1_for_instance_ids.find({ 
                   "ReconSetupCode" : setup_code, 
#                   "BusinessDate": {"$gte" : datetime(2020, 6, 24), "$lt": datetime(2015, 6, 26)},
                   "BusinessDate" : getDateTimeFromISO8601String(date_input_iso), 
                   "SourceCombinationCode" : {"$ne" : "UNMAPPED"}, 
                   "IsUndone" : False, 
                   "Status" : "Completed"
                   }, 
                   { "InstanceID" : 1}
                   ) 
#        df_instance_ids = json_normalize(query_for_instance_id_setup_and_date)
#        list_instance_ids = list(df_instance_ids['InstanceID'])
        list_of_dicts_instance_ids = list(query_for_instance_id_setup_and_date)
        list_instance_ids = [list_of_dicts_instance_ids[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_instance_ids))]
        
        instance_ids_temp_path = 'C:\\Data\\ML Data\\logs\\' + setup_code + '_' + date_input + '.txt'
        with open(instance_ids_temp_path, 'w') as f:
            for item in list_instance_ids:
                f.write("%s\n" % item)
        
        if(len(list_instance_ids) != 0):
            query_2_for_data_in_list_instance_ids = coll_2_for_data_in_list_instance_ids.find({ 
                        "ViewData": { "$ne": None },
                        "ViewData.Status": { "$nin": ["HST","Archive","OC"] },
                        "TaskInstanceID" : {"$in" : list_instance_ids}
                       },{ 
                        "DataSides" : 1,
                        "BreakID" : 1,
                        "LastPerformedAction" : 1,
                        "TaskInstanceID" : 1,
                        "SourceCombinationCode" : 1,
                        "MetaData" : 1, 
                        "ViewData" : 1
                                     })
            #df_query_result_2 = json_normalize(query2)

            list_of_dicts_query_result_2 = list(query_2_for_data_in_list_instance_ids)
            if(len(list_of_dicts_query_result_2) != 0):
                coll_3_for_writing_in_ml_server = db_3_for_writing_in_ml_server['MEO_AUA_HST_RecData_' + setup_code + '_' + date_input]
                coll_3_for_writing_in_ml_server.insert_many(list_of_dicts_query_result_2)
                query_4_for_MEO_data = coll_3_for_writing_in_ml_server.find({ 
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
                list_of_dicts_query_result_4 = list(query_4_for_MEO_data)
                query_5_for_AUA_data = coll_3_for_writing_in_ml_server.find({ 
                                                                                     "LastPerformedAction": { "$ne": 31 }
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
                list_of_dicts_query_result_5 = list(query_5_for_AUA_data)
                
                if('RecData_' + setup_code in db_4_for_MEO_data.list_collection_names()):
                    print('RecData_' + setup_code + ' exists in ReconDB_ML')
                    if(db_4_for_MEO_data['RecData_' + setup_code].count != 0):
                        query_6_for_MEO_historic_data = db_4_for_MEO_data['RecData_' + setup_code].find({ 
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
                        list_of_dicts_query_result_6 = list(query_6_for_MEO_historic_data)
                        db_4_for_MEO_data['RecData_' + setup_code].drop()
                        coll_6_for_writing_MEO_historic_data = db_4_for_MEO_data['RecData_' +  setup_code + '_HistroricPrediction']
                        coll_6_for_writing_MEO_historic_data.insert_many(list_of_dicts_query_result_6)
                        
                if('RecData_' + setup_code in db_5_for_AUA_data.list_collection_names()):
                    print('RecData_' + setup_code + ' exists in ReconDB_ML')
                    if(db_5_for_AUA_data['RecData_' + setup_code].count != 0):
                        query_7_for_AUA_historic_data = db_5_for_AUA_data['RecData_' + setup_code].find({ 
                                                                                             "LastPerformedAction": { "$ne": 31 }
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
                        list_of_dicts_query_result_7 = list(query_7_for_AUA_historic_data)
                        db_5_for_AUA_data['RecData_' + setup_code].drop()
                        coll_7_for_writing_AUA_historic_data = db_5_for_AUA_data['RecData_' +  setup_code + '_HistroricPrediction']
                        coll_7_for_writing_AUA_historic_data.insert_many(list_of_dicts_query_result_7) 
                        
                if(len(list_of_dicts_query_result_4) != 0):
#                    coll_4_for_writing_MEO_data = db_4_for_MEO_data['MEO_HST_RecData_' + setup_code + '_' + date_input]
                    coll_4_for_writing_MEO_data = db_4_for_MEO_data['RecData_' + setup_code]
                    coll_4_for_writing_MEO_data.insert_many(list_of_dicts_query_result_4)
                if(len(list_of_dicts_query_result_5) != 0):
                    coll_5_for_writing_AUA_data = db_5_for_AUA_data['RecData_' + setup_code]
                    coll_5_for_writing_AUA_data.insert_many(list_of_dicts_query_result_5)
                
                
        else:
             instance_ids_temp_path_warning = 'C:\Data\ML Data\logs\WARNING_' + setup_code + '_' + date_input + '.txt'
             with open(instance_ids_temp_path_warning, 'w') as f:
                 f.write('No instance ID found for this setup')
        
        print('DONE : Date wise extraction for following date :')
        print(date_input)
        meo_df = json_normalize(list_of_dicts_query_result_4)
        meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
        meo = meo_df[new_cols]
        df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
        df1 = df1[~df1['ViewData.Status'].isnull()]
        df1 = df1.reset_index()
        df1 = df1.drop('index',1)
        
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
        
        df = df[df['ViewData.Status'].isin(['OB','SPM','SDB','UOB','UDB','SMB'])]
        df = df.reset_index()
        df = df.drop('index',1)
        df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
        df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
        df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
        df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
        
        print('The Date value count is:')
        print(df['Date'].value_counts())
        
        date_i = df['Date'].mode()[0]
        
        print('Choosing the date : ' + date_i)
        
        df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})
        
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
        
        sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
        sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
        
        sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
        sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 
        
        sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
        sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
        sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
        sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)
        
        sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','SMB-OB']))]
        
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
        
        bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
        bb = bb.reset_index()
        bb = bb.drop('index',1)
        pool =[]
        key_index =[]
        training_df =[]
        
        no_pair_ids = []
        
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
        
                    df1 = df1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
                    df2 = df2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
        
                    df1 = df1.reset_index()
                    df2 = df2.reset_index()
                    df1 = df1.drop('index', 1)
                    df2 = df2.drop('index', 1)
        
                    df1.columns = ['SideA.' + x  for x in df1.columns] 
                    df2.columns = ['SideB.' + x  for x in df2.columns]
        
                    df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
                    df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})
                    dff = pd.merge(df1, df2, on='filter_key')
                    training_df.append(dff)
        
                else:
                    no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
                    no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
        
        if len(no_pair_ids) != 0:
            no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])
            no_pair_ids_df = pd.DataFrame(no_pair_ids)
            #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
            no_pair_ids_df.columns = ['filter_key']
            no_pair_ids_df.to_csv(filepaths_no_pair_id_data)
        else:
             with open(filepaths_no_pair_id_no_data_warning, 'w') as f:
                 f.write('No no pair ids found for this setup and date combination')
        
        test_file = pd.concat(training_df)
        
        test_file = test_file.reset_index()
        test_file = test_file.drop('index',1)
        
        test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
        test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')
        
        model_cols = [
        'SideA.ViewData.Accounting Net Amount', 
        'SideA.ViewData.B-P Net Amount', 
        'SideA.ViewData.CUSIP', 
        'SideA.ViewData.Currency', 
        #'SideA.ViewData.Description',
        'SideA.ViewData.ISIN', 
        'SideB.ViewData.Accounting Net Amount',
        'SideB.ViewData.B-P Net Amount',
        'SideB.ViewData.CUSIP',
        'SideB.ViewData.Currency',
        #'SideB.ViewData.Description', 
        'SideB.ViewData.ISIN',
        'SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 
        'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side',
        'label']
        
        
        y_col = ['label']
        
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
        
        values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
        values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
        
        values_CUSIP_A_Side = test_file['SideA.ViewData.CUSIP'].values
        values_CUSIP_B_Side = test_file['SideB.ViewData.CUSIP'].values
        
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
        
        test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
        test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']
        
        test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days
        
        test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days
        
        values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
        values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values
        test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
        test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)
        
        values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
        values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values
        test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
        test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)
        
        test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)
        test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)
        
        values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
        values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
        test_file['SideA.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)
        test_file['SideB.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)
        
        values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
        values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
        
        values_CUSIP_A_Side = test_file['SideA.ViewData.CUSIP'].values
        values_CUSIP_B_Side = test_file['SideB.ViewData.CUSIP'].values
        
        test_file['SideB.ViewData.key_NAN']= vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[0]
        test_file['SideB.ViewData.Common_key'] = vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[1]
        test_file['SideA.ViewData.key_NAN'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[0]
        test_file['SideA.ViewData.Common_key'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[1]
        
        values_key_NAN_B_Side = test_file['SideB.ViewData.key_NAN'].values
        values_key_NAN_A_Side = test_file['SideA.ViewData.key_NAN'].values
        test_file['All_key_nan'] = vec_nan_equal_fun(values_key_NAN_B_Side,values_key_NAN_A_Side )
        
        test_file['SideB.ViewData.Common_key'] = test_file['SideB.ViewData.Common_key'].astype(str)
        test_file['SideA.ViewData.Common_key'] = test_file['SideA.ViewData.Common_key'].astype(str)
        
        
        values_Common_key_B_Side = test_file['SideB.ViewData.Common_key'].values
        values_Common_key_A_Side = test_file['SideA.ViewData.Common_key'].values
        values_All_key_NAN = test_file['All_key_nan'].values
        
        test_file['new_key_match']= vec_new_key_match_fun(values_Common_key_B_Side,values_Common_key_A_Side,values_All_key_NAN)
        
        model_cols = [
         #   'SideA.ViewData.Accounting Net Amount',
         'SideA.ViewData.B-P Net Amount',
         'SideA.ViewData.Price',
         'SideA.ViewData.Quantity',
         'SideB.ViewData.Accounting Net Amount',
        # 'SideB.ViewData.B-P Net Amount',
         'SideB.ViewData.Price',
         'SideB.ViewData.Quantity',  
         'Trade_Date_match',
         'Settle_Date_match',
        # 'Fund_match',
         'Amount_diff_2',
         'Trade_date_diff',
         'Settle_date_diff',
        
        'SideA.ISIN_NA',
         'SideB.ISIN_NA',
        
         'ViewData.Combined Fund',
         'ViewData.Combined Transaction Type',
         'All_key_nan',
         'new_key_match',
        
        'SideA.ViewData._ID',
        'SideB.ViewData._ID',
        
        'SideB.ViewData.Status',
         'SideB.ViewData.BreakID_B_side',
         'SideA.ViewData.Status',
         'SideA.ViewData.BreakID_A_side',
         'SideB.ViewData.Side0_UniqueIds',
        'SideA.ViewData.Side1_UniqueIds']
        test_file.to_csv(filepaths_X_test)
        print('Done till X_Test creation')
        print(datetime.now())
        
        X_test = test_file[model_cols]
        
        X_test = X_test.reset_index()
        X_test = X_test.drop('index',1)
        
        X_test = X_test.fillna(0)
        X_test = X_test.drop_duplicates()
        X_test = X_test.reset_index()
        X_test = X_test.drop('index',1)
        
        # ## Model Pickle file import
        
        filename = 'Oak_new_model_V7.sav'
        clf = pickle.load(open(filename, 'rb'))
        rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
        rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]
        
        probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
        probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]
        
        probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
        probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]
        
        X_test['Predicted_action'] = rf_predictions
        X_test['probability_No_pair'] = probability_class_0
        X_test['probability_UMB'] = probability_class_1
        X_test['probability_UMR'] = probability_class_2
        X_test['probability_UMT'] = probability_class_3
        
        X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & ((X_test['Amount_diff_2']!=0) | (X_test['Amount_diff_2']!=0)),'Predicted_action'] = 'Unrecognized' 
        
        prediction_table =  X_test.groupby('SideB.ViewData.BreakID_B_side')['Predicted_action'].unique().reset_index()
        
        prediction_table['len'] = prediction_table['Predicted_action'].str.len()
        
        prediction_table['No_Pair_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
        
        prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
        prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
        prediction_table['UMR_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)
        
        umr_array = X_test[X_test['Predicted_action']=='UMR_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
        umt_array = X_test[X_test['Predicted_action']=='UMT_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
        umb_array = X_test[X_test['Predicted_action']=='UMB_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
        
        umr_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMR_array']
        umt_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMT_array']
        umb_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMB_array']
        
        prediction_table = pd.merge(prediction_table,umr_array, on='SideB.ViewData.BreakID_B_side', how='left' )
        prediction_table = pd.merge(prediction_table,umt_array, on='SideB.ViewData.BreakID_B_side', how='left' )
        prediction_table = pd.merge(prediction_table,umb_array, on='SideB.ViewData.BreakID_B_side', how='left' )
        
        prediction_table['Final_prediction'] = prediction_table.apply(lambda x: 'UMR_One_to_One' if x['UMR_flag']==1 else('UMT_One_to_One' if x['len']==1 and x['UMT_flag']==1 else('UMB_One_to_UMB' if x['len']==1 and x['UMB_flag']==1 else('No-Pair' if x['len']==1 else 'Undecided'))), axis=1)
        
        prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
        prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
        
        prediction_table.loc[(prediction_table['UMB_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMB_One_to_One'
        prediction_table.loc[(prediction_table['UMT_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMT_One_to_One'
        
        prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['len']==2),'Final_prediction']='No-Pair/Unrecognized'
        
        prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMT_flag']==1),'Final_prediction']='UMT_One_to_One'
        
        prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMB_flag']==1),'Final_prediction']='UMB_One_to_One'
        
        prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Predicted_UMR_array']
        
        prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Predicted_UMT_array']
        prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Predicted_UMB_array']
        
        prediction_table['predicted_break_len'] = prediction_table['Final_predicted_break'].str.len()
        
        X_test['prob_key'] = X_test['SideB.ViewData.BreakID_B_side'].astype(str) + X_test['Predicted_action']
        prediction_table['prob_key'] = prediction_table['SideB.ViewData.BreakID_B_side'].astype(str) + prediction_table['Final_prediction']
        
        user_prob = X_test.groupby('prob_key')[['probability_UMR','probability_UMT','probability_UMB']].max().reset_index()
        open_prob = X_test.groupby('prob_key')['probability_No_pair'].mean().reset_index()
        
        prediction_table = pd.merge(prediction_table,user_prob, on='prob_key', how='left')
        prediction_table = pd.merge(prediction_table,open_prob, on='prob_key', how='left')
        
        prediction_table = prediction_table.drop('prob_key',1)
        
        prediction_table = pd.merge(prediction_table, X_test[['SideB.ViewData.BreakID_B_side','SideA.ViewData._ID','SideB.ViewData._ID']].drop_duplicates(['SideB.ViewData.BreakID_B_side','SideB.ViewData._ID']), on ='SideB.ViewData.BreakID_B_side', how='left')
        
        prediction_table3 = prediction_table
        col_rename_dict = {'SideB.ViewData.BreakID_B_side' : 'BreakID'}
        
        prediction_table3.rename(columns = col_rename_dict, inplace = True)
        
        pred_table_deliver = prediction_table3[['BreakID','Predicted_action','probability_UMR','probability_UMT','probability_UMB','probability_No_pair']]        
        pred_table_deliver['Predicted_action'] = pred_table_deliver['Predicted_action'].apply(lambda x: ','.join(map(str, x)))
        pred_table_deliver.reset_index(inplace=True)
        data_dict = pred_table_deliver.to_dict("records")
        coll_8_for_writing_prediction_data = db_4_for_MEO_data['MLPrediction_Cash_2']
        coll_8_for_writing_prediction_data.insert_many(data_dict) 

    print('DONE : Setup wise extraction for following setup :')
    print(setup_code)

##### Adding MEO and AUA query
#for setup_code in setup_code_input_list:
#    print('INITIATED : Setup wise MEO and AUA extraction for following setup :')
#    print(setup_code)
#    coll_1_for_instance_ids = db_1_for_instance_ids['Tasks']
#    coll_2_for_data_in_list_instance_ids = db_2_for_data_in_list_instance_ids['HST_RecData_' + setup_code]
#    
#    for date_input in date_input_list:
#        print('INITIATED : Date wise extraction for following date :')
#        print(date_input)
#        date_input_iso = date_input + 'T18:30:00.000+0000'
#        
#        if(len(list_instance_ids) != 0):
#            meo_query = 
