#!/usr/bin/env python
# coding: utf-8

import os
os.chdir('D:\\ViteosModel')

import pandas as pd
import numpy as np
import datetime as dt
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from pandas.io.json import json_normalize
import datetime
import math
# ### Reading comments file
import dateutil.parser

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

client = 'Schonfeld'

setup = '85'
setup_code = '85'
#filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/AUA/AUACollections.AUA_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/' + client + '/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + setup + '_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]

base_dir_containing_client = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\'

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

date_to_analyze = '21012021'
penultimate_date_to_analyze = '20012021'
date_to_analyze_ymd_format = date_to_analyze[4:] + '-' + date_to_analyze[2:4] + '-' + date_to_analyze[:2]
penultimate_date_to_analyze_ymd_format = penultimate_date_to_analyze[4:] + '-' + penultimate_date_to_analyze[2:4] + '-' + penultimate_date_to_analyze[:2]
penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'

#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Schonfeld/meo_df_setup_897_date_' + date_to_analyze_ymd_format + '.csv'
#meo_df = pd.read_csv(meo_filename)

#meo_df = pd.read_csv(base_dir_containing_client + 'meo_df_setup_897_date_2021-01-04.csv')


meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df.drop_duplicates(keep=False, inplace = True)
print('meo size')
print(meo_df.shape[0])

date_i = pd.to_datetime(pd.to_datetime(meo_df['ViewData.Task Business Date'])).dt.date.astype(str).mode()[0]

#df1 = pd.read_csv('Schonfield\meo_df_879.csv')
df1 = meo_df.copy()
df1 = df1[~df1['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR','SPM'])]
days = df1['ViewData.Task Business Date'].value_counts().reset_index()
date = list(days[days['ViewData.Task Business Date']>50]['index'])[0]


date1 = pd.to_datetime(date)

day = date1.day
mon = date1.month
yr = date1.year

#path = 'Schonfield\daily files\\'
path = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\Files and Rules for Lookup\\Rules delivery from Ronson\\Schonfeld\\Schonfeld ML final\\57 Position Recon\\'
if((day > 0) & (day < 10)):
    filename = '57 Position recon - '+ str(mon) + '0' + str(day) + str(yr)+'_copy.xlsx'
else:
    filename = '57 Position recon - '+ str(mon) + str(day) + str(yr)+'_copy.xlsx'
filename_pos_file = path +filename

def read_pos_file_and_concat_to_single_pos_df(fun_filepath):
    xlsx_obj = pd.ExcelFile(fun_filepath)
    xlsx_sheet_names_list = xlsx_obj.sheet_names
    True_False_list_for_Document_Map_substring_in_xlsx_sheet_names_list = ['Document Map' in x for x in xlsx_sheet_names_list]
    Document_Map_substring_in_xlsx_sheet_names_list = [x for x,y in zip(xlsx_sheet_names_list,True_False_list_for_Document_Map_substring_in_xlsx_sheet_names_list) if y == True]
    string_name_for_Document_Map_substring_in_xlsx_sheet_names_list = Document_Map_substring_in_xlsx_sheet_names_list[0]
    
    xlsx_sheet_names_list_without_Document_Map = [x for x in xlsx_sheet_names_list if x != string_name_for_Document_Map_substring_in_xlsx_sheet_names_list]
    df_sheet_list = []
    for sheet_name in xlsx_sheet_names_list_without_Document_Map:
        df_sheet = xlsx_obj.parse(sheet_name,skiprows = 3)
        df_sheet_list.append(df_sheet)
    pos_df = pd.concat(df_sheet_list)
    return(pos_df)
    
pos = read_pos_file_and_concat_to_single_pos_df(fun_filepath = filename_pos_file)

os.chdir('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client +'\\output_files')
#uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')

#Change made by Rohit on 09-12-2020 to make dynamic directories
# base dir
base_dir = os.getcwd()       

# create dynamic name with date as folder
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code +'\\BD_of_' + str(date_i) + 'For_10_days_predictions_2')
# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

os.chdir(base_dir)

# create dynamic name with date as folder
base_dir_plus_client = os.path.join(base_dir, client)

# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir_plus_client):
    os.makedirs(base_dir_plus_client)

# create dynamic name with date as folder
base_dir_plus_client_plus_setup = os.path.join(base_dir_plus_client, setup_code)

# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir_plus_client_plus_setup):
    os.makedirs(base_dir_plus_client_plus_setup)

# Function1
def subSum(numbers,total):
    length = len(numbers)
    
    if length <16:
      
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=0.05).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers

def subSum1(numbers,total):
    length = len(numbers)
    
    if length <16:
      
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=5.0).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers

def amt_marker(x,y,z):
    if type(y)==list:
        if ((x in y) & ((z<16) & (z>=2))) :
            return 1
        else:
            return 0
    else:
        return 0

def remove_mark(x,z,k):
    if ((x>1) & (x<16)):
        if ((k<6.0)):
            return 1
#         elif ((k==0.0) & (z!=0)):
#             return 1
        else:
            return 0
    else:
        return 0

def mtm(x,y):
    if ((pd.isnull(x)==False) & (pd.isnull(y)==False)):
        y1 = y.split(',')
        x1 = x.split(',')
        return pd.Series([len(x1),len(y1)], index=['len_0', 'len_1'])
    elif ((pd.isnull(x)==False) & (pd.isnull(y)==True)):
        x1 = x.split(',')
        
        return pd.Series([len(x1),0], index=['len_0', 'len_1'])
    elif ((pd.isnull(x)==True) & (pd.isnull(y)==False)):
        y1 = y.split(',')
        
        return pd.Series([0,len(y1)], index=['len_0', 'len_1'])
        
    else:
        return pd.Series([0,0], index=['len_0', 'len_1'])

def mtm_mark(x,y):
    if ((x>1) &(y>1)):
        return 'MTM'
    elif((x==1) &(y==1)):
        return 'OTO'
    elif((x>1) &(y==1)):
        return 'MTO'
    elif((x==1) &(y>1)):
        return 'OTM'
    else:
        return 'OB'

#appended_data = []
#for infile in glob.glob('schonfield/UMF 85/schon_85_UMF*.csv'):
#    data = pd.read_csv(infile)
#    # store DataFrame in list
#    appended_data.append(data)
## see pd.concat documentation for more info
#uni2 = pd.concat(appended_data)

#uni2.to_csv('schonfield/UMF 85/combined UCB UMF of 85.csv')
def futureacc(x):
    if type(x)==str:
        x1 = x.split('_')
        if len(x1)>=2:
            k = x1[1]
            k1 = x1[0]
            
            if k1.endswith('FU'):
                return 1
            elif k =='WHITNEY':
                return 2
            
            elif k1.endswith('COLL'):
                return 3
            else:
                return 0
    else:
        return 0

uni2 = meo_df.copy()
uni2['map_marker'] = uni2['ViewData.Mapped Custodian Account'].apply(lambda x : futureacc(x))

filter_col = ['ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account','ViewData.BreakID','ViewData.Fund',
 'ViewData.Currency',
 'ViewData.Asset Type Category',
 'ViewData.Transaction Type',
 'ViewData.Investment Type',
 'ViewData.Prime Broker',
 'ViewData.Ticker',
 'ViewData.Sec Fees',
 'ViewData.Settle Date',
 'ViewData.Trade Date',
 'ViewData.Description',
 'ViewData.CUSIP',
 'ViewData.Call Put Indicator',
 'ViewData.Cancel Flag',
 'ViewData.Commission',
 'ViewData.ISIN',
 'ViewData.Investment ID',
 
 'ViewData.Interest Amount',
 'ViewData.InternalComment1',
 'ViewData.InternalComment2',
 'ViewData.InternalComment3',
             
'ViewData.Accounting Net Amount',
'ViewData.B-P Net Amount',
              'ViewData.Net Amount Difference','ViewData.Status','map_marker','ViewData.Source Combination Code', 'ViewData.Task ID'
             ]


uni2 = uni2[filter_col]

#uni2.to_csv('Schonfield/Meo file for 85 with needed columns.csv')
#uni2['ViewData.Side1_UniqueIds'] = uni2['ViewData.Side1_UniqueIds'].fillna('BB')
#uni2['ViewData.Side0_UniqueIds'] = uni2['ViewData.Side0_UniqueIds'].fillna('AA')
#uni2[['len_0','len_1']] = uni2.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)
#uni2['MTM_mark'] = uni2.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)

# #### Others : Map-Mark 0 to be processed first

# We preserve Actual copy of the file and move to make changes on copy
uni3 = uni2.copy()

# Function for reconciliation involving single sides only
def common_matching_engine_single1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_sum_list'] = dummy[amount_column].apply(lambda x : sum(x))
        #dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        #dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        #dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy['zero_sum_list'].apply(lambda x :1 if abs(x)<=5.0 else 0)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#             k = pd.merge(k1, k2 , on = filters, how = 'left')
#             k = pd.merge(k, k3 , on = filters, how = 'left')
#             k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'pair'
            df4['predicted action'] = 'UMR'
            df4['predicted category'] = 'match'
            df4['predicted comment'] = 'match'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfeld setup 85 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# In[596]:


def common_matching_engine_single2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#             k = pd.merge(k1, k2 , on = filters, how = 'left')
#             k = pd.merge(k, k3 , on = filters, how = 'left')
#             k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'pair'
            df4['predicted action'] = 'UMR'
            df4['predicted category'] = 'match'
            df4['predicted comment'] = 'match'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfeld setup 85 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# In[597]:


def common_matching_engine_single3(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 1 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 0, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum1(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#             k = pd.merge(k1, k2 , on = filters, how = 'left')
#             k = pd.merge(k, k3 , on = filters, how = 'left')
#             k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'pair'
            df4['predicted action'] = 'UMR'
            df4['predicted category'] = 'match'
            df4['predicted comment'] = 'match'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfeld setup 85 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# In[598]:


def common_matching_engine_double1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_sum_list'] = dummy[amount_column].apply(lambda x : sum(x))
        #dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        #dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        #dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy['zero_sum_list'].apply(lambda x :1 if abs(x)<=5.0 else 0)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#             k = pd.merge(k1, k2 , on = filters, how = 'left')
#             k = pd.merge(k, k3 , on = filters, how = 'left')
#             k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'pair'
            df4['predicted action'] = 'UMR'
            df4['predicted category'] = 'match'
            df4['predicted comment'] = 'match'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfeld setup 85 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# In[599]:


def common_matching_engine_double2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#             k = pd.merge(k1, k2 , on = filters, how = 'left')
#             k = pd.merge(k, k3 , on = filters, how = 'left')
#             k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'pair'
            df4['predicted action'] = 'UMR'
            df4['predicted category'] = 'match'
            df4['predicted comment'] = 'match'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfeld setup 85 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# In[600]:


# Function for reconciliation involving both sides
def common_matching_engine_double3(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
    if dummy[dummy['sel_mark']==1].shape[0]!=0:
    
        dummy['len_amount'] = dummy[amount_column].apply(lambda x : len(x))
    
        dummy['zero_list'] = dummy[amount_column].apply(lambda x : subSum1(x,0))
        dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#             k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#             k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#             k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#             k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#             k = pd.merge(k1, k2 , on = filters, how = 'left')
#             k = pd.merge(k, k3 , on = filters, how = 'left')
#             k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'pair'
            df4['predicted action'] = 'UMR'
            df4['predicted category'] = 'match'
            df4['predicted comment'] = 'match'
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfeld setup 85 ' + string_name + '.csv'
            df4.to_csv(filename)
        
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            #print(df5.columns)
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
        else:    
            df5 = df3[~((df3['remove_mark']==1) & (df3['sel_mark']==1))]
            df5.drop(dummy_filter, axis = 1, inplace = True)
            df5 = df5.reset_index()
            df5.drop('index', axis = 1, inplace = True)
            print(df5.columns)
    else:
        df5 = df.copy()
        
    return df5


# ### Ticker and description needs to be cleaned mostly, with currency conversion

# #### Currency conversion 
abhijeet_comment_soros_folder_filepath = 'D:\\ViteosModel\\Abhijeet - Comment\\'
conv = pd.read_csv(abhijeet_comment_soros_folder_filepath + 'Currency conversion for 85.csv')

#conv = pd.read_csv('Schonfield/Currency conversion for 85.csv')

uni3 = pd.merge(uni3, conv, left_on = 'ViewData.Currency', right_on = 'Currency', how = 'left')

uni3['ViewData.Net Amount Difference'] = uni3['ViewData.Net Amount Difference']*uni3['Conversion']

# #### description cleaning for stars

def stardesc(x):
    if type(x)==str:
        a = x.replace('*','')
    else:
        a = x
    return a

# ### Matching code starts here

dummy_filter = ['remove_mark','sel_mark']
columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment','ViewData.Task Business Date','ViewData.Task ID','ViewData.Source Combination Code']
amount_column = 'ViewData.Net Amount Difference'

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']
serial_num_list = []
serial_num = 1

df1 = common_matching_engine_double1(uni3,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 2
#df2 = common_matching_engine_double2(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 3
#df3 = common_matching_engine_double3(df2,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 4
df4 = common_matching_engine_single1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

#serial_num = 5
#df5 = common_matching_engine_single2(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#serial_num_list.append(serial_num)
#serial_num = 6
#df6 = common_matching_engine_single3(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#serial_num_list.append(serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']

serial_num = 5
df5 = common_matching_engine_double1(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

#serial_num = 8
#df8 = common_matching_engine_double2(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#serial_num_list.append(serial_num)

#serial_num = 9
#df9 = common_matching_engine_double3(df8,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#serial_num_list.append(serial_num)

serial_num = 6
df6 = common_matching_engine_single1(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

#serial_num = 11
#df11 = common_matching_engine_single2(df10,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#serial_num_list.append(serial_num)

#serial_num = 12
#df12 = common_matching_engine_single3(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#serial_num_list.append(serial_num)

# #### Basic ticker cleaning

def ticker_get_first_element_apply(fun_ticker_value):
    if(type(fun_ticker_value) == str):
        if(fun_ticker_value == ''):
            return('')
        else:
            return(fun_ticker_value.split()[0])
    else:
        return(fun_ticker_value)
    
#df6['ViewData.Ticker'] = df6['ViewData.Ticker'].apply(lambda x : x.split()[0] if type(x)==str else x)
df6['ViewData.Ticker'] = df6['ViewData.Ticker'].apply(lambda x : ticker_get_first_element_apply(x))

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Settle Date']

serial_num = 7
df7 = common_matching_engine_single1(df6,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 8
df8 = common_matching_engine_double1(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']

serial_num = 9
df9 = common_matching_engine_single1(df8,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 10
df10 = common_matching_engine_double1(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

# #### Cleaning of description

df10['ViewData.Description'] = df10['ViewData.Description'].apply(lambda x : stardesc(x))

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']

serial_num = 11
df11 = common_matching_engine_single1(df10,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 12
df12 = common_matching_engine_double1(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']

serial_num = 22
df22 = common_matching_engine_single1(df12,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 23
df23 = common_matching_engine_double1(df22,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

df23['ViewData.Description'] = df23['ViewData.Description'].apply(lambda x : x.lower() if type(x)==str else x)

# #### Standardization of currency

usd = ['us dollar','us dollars','u.s. dollar','u.s. dollars','united states dollars','united states dollar']
aud = ['austrailian dollar (a$)','austrailian dollar','austrailian dollars']
pound = ['british pound (bp)','british pound','pound sterling','uk pounds','uk pounds']

def curr_stn(x):
    if type(x)==str:
        if x in usd:
            return 'usd'
        elif x in aud:
            return 'aud'
        elif x in pound:
            return 'pound'
        else:
            return x
        
df23['ViewData.Description'] = df23['ViewData.Description'].apply(lambda x : curr_stn(x))

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']

serial_num = 25
df24 = common_matching_engine_single1(df23,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 26
df25 = common_matching_engine_double1(df24,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

# #### Analysis of the remaining

itype1 = ['EQSW','Cash','PortfolioSwap','Swap Cash Transfer','Swap Cash Flow','Currency']
itype2 = ['Cash','Cash/Money']

ttype1 = ['Swap Cash Flow','Swap Cash Transfer','SwapReset','Transfer','Buy','Buy(CAT)_','Sell','Sell(CAT)_']
ttype2 = ['General P&L']

ttype3 = ['Mark to Market']
ttype4=['Spot fx','fx movement journal', 'accounting related']

df12 = df25.copy()

df11_1 = df12[df12['ViewData.Investment Type'].isin(itype2)]
df11_2 = df12[~(df12['ViewData.Investment Type'].isin(itype2))]

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account']

if df11_1.shape[0]!=0:
    serial_num = 14
    df11_11 = common_matching_engine_single1(df11_1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)
    serial_num = 15
    df11_12 = common_matching_engine_double1(df11_11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)
    df13 = pd.concat([df11_2,df11_12] , axis = 0)
else:
    df13 = df11_2.copy()

df13_1 = df13[df13['ViewData.Transaction Type'].isin(ttype2)]
df13_2 = df13[~(df13['ViewData.Transaction Type'].isin(ttype2))]
filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account']
if df13_1.shape[0]!=0:
    serial_num = 16
    df13_11 = common_matching_engine_single1(df13_1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)
    serial_num = 17
    df13_12 = common_matching_engine_double1(df13_11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)
    df14 = pd.concat([df13_2,df13_12] , axis = 0)
else:
    df14 = df13_2.copy()

df14_1 = df14[df14['ViewData.Investment Type'].isin(itype1)]
df14_2 = df14[~(df14['ViewData.Investment Type'].isin(itype1))]
filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency']

if df14_1.shape[0]!=0:

    serial_num = 18
    df14_11 = common_matching_engine_single1(df14_1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)

    serial_num = 19
    df14_12 = common_matching_engine_double1(df14_11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)

    df15 = pd.concat([df14_2,df14_12] , axis = 0)
else:
    df15 = df14_2.copy()
    
filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency']

serial_num = 28
df16 = common_matching_engine_single1(df15,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

serial_num = 29
df17 = common_matching_engine_double1(df16,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num_list.append(serial_num)

df17_1 = df17[df17['ViewData.Transaction Type'].isin(ttype4)]
df17_2 = df17[~(df17['ViewData.Transaction Type'].isin(ttype4))]
filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency']

if df17_1.shape[0]!=0:

    serial_num = 30
    df17_11 = common_matching_engine_single1(df17_1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)

    serial_num = 31
    df17_12 = common_matching_engine_double1(df17_11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num_list.append(serial_num)

    df18 = pd.concat([df17_2,df17_12] , axis = 0)
    
else:
    df18 = df17_2.copy()

# #### Finding additional rules

# - For foreign exchange : ttype : Spot fx, fx movement journal/spot fx with accounting related
# - Mark to market only
# - Investment Type : Cash/ money settles with custodian and currency only
# - Try General Pnl in ttype.

# #### Amendment in description

# - Remove astricks from the beginning of desc.
# - Standardize currency name

# #### Matching code ends here

# #### Commenting code

serial_num = 100

df18['predicted status'] = 'No-pair'
df18['predicted action'] = 'OB'
df18['predicted category'] = ''
df18['predicted comment'] = ''
df18 = df18[columns_to_output]
        
string_name = 'p'+str(serial_num)
filename = 'Schonfeld setup 85 ' + string_name + '.csv'
df18.to_csv(filename)
serial_num_list.append(serial_num)

def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
    frames = []
    current_folder = os.getcwd()
    full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
    for full_filepath in full_filepath_list :
        if os.path.isfile(full_filepath) == True:
            frames.append(pd.read_csv(full_filepath))
    return pd.concat(frames)


# #### Combining all the files
final_df_filename_list = ['Schonfeld setup 85 p' + str(x) + '.csv' for x in serial_num_list]
final_df = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(final_df_filename_list)
final_df = final_df.reset_index()
final_df = final_df.drop('index', axis = 1)

#### While renaming ticker1 will be viewdata.ticker and 

# final_df = final_df.rename(columns = {'Custodian Account':'ViewData.Mapped Custodian Account',
#                            'Currency':'ViewData.Currency',
#                             'Ticker1':'ViewData.Ticker',
#                           'Net Amount Difference1':'ViewData.Net Amount Difference',
#                           'Settle Date': 'ViewData.Settle Date',
#                            'Trade Date':'ViewData.Trade Date',
#                            'Description':'ViewData.Description'})

#### While renaming ticker1 will be viewdata.ticker and 

final_df = final_df.rename(columns = {'Custodian Account':'ViewData.Mapped Custodian Account',
                           'Currency':'ViewData.Currency',
                            'Ticker1':'ViewData.Ticker',
                          'Net Amount Difference1':'ViewData.Net Amount Difference',
                          'Settle Date': 'ViewData.Settle Date',
                           'Trade Date':'ViewData.Trade Date',
                           'Description':'ViewData.Description',
                           'ViewData.Task Business Date' : 'BusinessDate',
                           'ViewData.BreakID' : 'BreakID',
#                           'final_BreakID_to_insert_in_db' : 'BreakID',
#                           'Predicted_BreakID_to_insert_in_db' : 'Final_predicted_break',
#                           'final_Status_to_insert_in_db' : 'Predicted_Status',
#                           'Predicted_action_to_insert_in_db' : 'Predicted_action',
                           
                           'ViewData.Source Combination Code' : 'SourceCombinationCode',
                           'ViewData.Task ID' : 'TaskID',
                           'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                           'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                           'predicted action' : 'Predicted_action',
                           'predicted comment' : 'PredictedComment',
                           'predicted category' : 'PredictedCategory',
                           'predicted status' : 'Predicted_Status'})
#As per Abhijeet, Ticker will be Ticker1 and Net Amount Difference will be Net Amount Difference1 now

final_df.to_csv('Schonfield concatenated file for all predictions.csv')

final_df['BreakID'] = final_df['BreakID'].astype(str)

final_df['BusinessDate'] = pd.to_datetime(final_df['BusinessDate'])
final_df['BusinessDate'] = final_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df['BusinessDate'] = pd.to_datetime(final_df['BusinessDate'])

final_df['Final_predicted_break'] = ''
final_df['Final_predicted_break'] = final_df['Final_predicted_break'].astype(str)
final_df['BreakID'] = final_df['BreakID'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df['Final_predicted_break'] = final_df['Final_predicted_break'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df['ML_flag'] = 'ML'

final_df['SetupID'] = setup_code

final_df['probability_No_pair'] = ''
final_df['probability_UMB'] = ''
final_df['probability_UMR'] = ''
final_df['probability_UMT'] = ''


    
    
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

cols_to_remove_newline_char_from = ['Side1_UniqueIds','Side0_UniqueIds','BreakID','Final_predicted_break']
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

#final_df_2_UMR_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
#if(final_df_2_UMR_record_with_predicted_comment.shape[0] != 0):
#    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMR'))]
#
#    Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side0_UniqueIds']
#    Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment = final_df_2_UMR_record_with_predicted_comment['Side1_UniqueIds']
#
#    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMR_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#
#    final_df_2_UMR_record_with_predicted_comment['PredictedComment'] = ''       
#    final_df_2 = final_df_2.append(final_df_2_UMR_record_with_predicted_comment)
#
#
#final_df_2_UMT_record_with_predicted_comment = final_df_2[((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
#if(final_df_2_UMT_record_with_predicted_comment.shape[0] != 0):
#    final_df_2 = final_df_2[~((final_df_2['PredictedComment']!='') & (final_df_2['Predicted_Status'] == 'UMT'))]
#    
#    Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side0_UniqueIds']
#    Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment = final_df_2_UMT_record_with_predicted_comment['Side1_UniqueIds']
#    
#    final_df_2 = final_df_2[~((final_df_2['Side0_UniqueIds'].isin(Side0_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#    final_df_2 = final_df_2[~((final_df_2['Side1_UniqueIds'].isin(Side1_id_of_OB_record_to_remove_corresponding_to_UMT_record_with_predicted_comment)) & (final_df_2['Predicted_Status'] == 'OB'))]
#
#    final_df_2_UMT_record_with_predicted_comment['PredictedComment'] = ''
#    final_df_2 = final_df_2.append(final_df_2_UMT_record_with_predicted_comment)
#
#final_df_2['BusinessDate'] = final_df_2.apply(lambda x: get_BusinessDate_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_df_2['TaskID'] = final_df_2.apply(lambda x: get_TaskID_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)
#final_df_2['SourceCombinationCode'] = final_df_2.apply(lambda x: get_SourceCombinationCode_from_single_string_of_Side_01_UniqueIds(fun_meo_df = meo_df, fun_row = x), axis=1)


final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])


final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)

final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(float)
final_df_2[['TaskID']] = final_df_2[['TaskID']].astype(np.int64)

final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(str)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('\'').rstrip('\''))

final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)

def apply_ui_action_column_897(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionType = 'No Prediction'
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'

        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE'
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE WITH COMMENT'
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'FORCE MATCH'
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'FORCE MATCH WITH COMMENT'
        else:
            ActionType = 'Status not covered'
    return ActionType

def apply_ui_action_column_897_final(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionType = 'No Prediction'
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE'
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'FORCE MATCH'
        else:
            ActionType = 'Status not covered'
    return ActionType

def apply_ui_action_column_897_final_ActionTypeCode(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionTypeCode = 7
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 6
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 3
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 6
        elif((fun_row['Predicted_Status'] == 'SMB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 3
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 2
        elif((fun_row['Predicted_Status'] == 'UMF') & (fun_row['ML_flag'] == 'ML')):
            ActionTypeCode = 5
        else:
            ActionTypeCode = 0
    return ActionTypeCode

final_df_2['ActionType'] = final_df_2.apply(lambda row : apply_ui_action_column_897_final(fun_row = row), axis = 1,result_type="expand")            

final_df_2['ActionTypeCode'] = final_df_2.apply(lambda row : apply_ui_action_column_897_final_ActionTypeCode(fun_row = row), axis = 1,result_type="expand")            
final_df_2['ActionTypeCode'] = final_df_2['ActionTypeCode'].astype(int)
final_df_2.loc[final_df_2['Predicted_Status']=='UMF','PredictedComment'] = ''
final_df_2.loc[final_df_2['Predicted_Status']=='UCB','PredictedComment'] = ''

final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].astype(str)

final_df_2['ReconSetupName'] = 'Schonfeld Cash - 57'
final_df_2['ClientShortCode'] = 'Schonfeld'

from datetime import datetime,date,timedelta
today = date.today()
today_Y_m_d = today.strftime("%Y-%m-%d")

final_df_2['CreatedDate'] = today_Y_m_d
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
final_df_2['CreatedDate'] = final_df_2['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_for_10_day_predictions.csv'
final_df_2.to_csv(filepaths_final_df_2)

filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '_for_10_day_predictions.csv'
meo_df.to_csv(filepaths_meo_df)

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