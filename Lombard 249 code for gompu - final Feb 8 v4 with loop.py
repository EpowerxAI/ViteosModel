#!/usr/bin/env python
# coding: utf-8

# In[2373]:
import timeit
start = timeit.default_timer()

import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE

import os
os.chdir('D:\\ViteosModel')

#from imblearn.over_sampling import SMOTE
import pickle
import datetime as dt
import sys
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import dateutil.parser
import re
from dateutil.parser import parse
import re


client = 'Lombard'

setup = '249'
setup_code = '249'

ReconSetupName = 'Lombard Cash Recon'

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

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

def comgen(x,y,z,k):
    if x == 'geneva':
        
        com = str(k) + ' ' + str(y) + ' ' + str(z)
    else:
        com = "Integrata" + ' ' + str(y) + ' ' + str(z)
        
    return com

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

def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys

def common_matching_engine_single1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num, param_client = client, param_setup_code = setup_code):
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
    
        dummy['remove_mark'] = dummy.apply(lambda x :1 if ((abs(x['zero_sum_list'])<=0.05) & (x['len_amount']>1)) else 0, axis =1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
            k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
            k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
            k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
            k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
            k = pd.merge(k1, k2 , on = filters, how = 'left')
            k = pd.merge(k, k3 , on = filters, how = 'left')
            k = pd.merge(k, k4 , on = filters, how = 'left')
        
            k['predicted status'] = 'UCB'
            k['predicted action'] = 'Close'
            k['predicted category'] = 'Close'
            k['predicted comment'] = 'Close'
            k = k[columns_to_output]
        
        
            string_name = 'p'+str(serial_num) + ' ' + str(item)
#            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
            filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
#            filename = 'Lombard setup 249 ' + string_name + '.csv'

            k.to_csv(filename)
        
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


def common_matching_engine_double1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num, param_client = client, param_setup_code = setup_code):
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
    
        dummy['remove_mark'] = dummy.apply(lambda x :1 if ((abs(x['zero_sum_list'])<=0.05) & (x['len_amount']>1)) else 0, axis =1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
            k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
            k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
            k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
            k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
            k = pd.merge(k1, k2 , on = filters, how = 'left')
            k = pd.merge(k, k3 , on = filters, how = 'left')
            k = pd.merge(k, k4 , on = filters, how = 'left')
        
            k['predicted status'] = 'UMR'
            k['predicted action'] = 'UMR'
            k['predicted category'] = 'match'
            k['predicted comment'] = 'match'
            k = k[columns_to_output]
        
        
            string_name = 'p'+str(serial_num) + ' ' + str(item)
            filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
#            filename = 'Lombard setup 249 ' + string_name + '.csv'
            k.to_csv(filename)
        
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

def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x

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

# ### Reading of MEO files

# In[2404]:

columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
#df = pd.read_csv('Lombard/249/meo_df_setup_249_penultimate_date_time_2020-12-15.csv')
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
#
#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/meo_df_setup_125_date_2020-11-25.csv'
#meo_df = pd.read_csv(meo_filename)
meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
meo_df.drop_duplicates(keep=False, inplace = True)
meo_df = normalize_bp_acct_col_names(fun_df = meo_df)

meo_df['Date'] = pd.to_datetime(meo_df['ViewData.Task Business Date'])

meo_df = meo_df[~meo_df['Date'].isnull()]
meo_df = meo_df.reset_index()
meo_df = meo_df.drop('index',1)

meo_df['Date'] = pd.to_datetime(meo_df['Date']).dt.date

meo_df['Date'] = meo_df['Date'].astype(str)

## ## Sample data on one date
#
print('The Date value count is:')
print(meo_df['Date'].value_counts())

date_i = meo_df['Date'].mode()[0]

print('Choosing the date : ' + date_i)

os.chdir('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files')
#uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')

#Change made by Rohit on 09-12-2020 to make dynamic directories
# base dir
base_dir = os.getcwd()       

# create dynamic name with date as folder
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code +'\\BD_of_' + str(date_i))
# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

os.chdir(base_dir)

# create dynamic name with date as folder
base_dir_plus_Lombard = os.path.join(base_dir, client)

# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir_plus_Lombard):
    os.makedirs(base_dir_plus_Lombard)

# create dynamic name with date as folder
base_dir_plus_client_plus_setup_code = os.path.join(base_dir_plus_Lombard, setup_code)

# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir_plus_client_plus_setup_code):
    os.makedirs(base_dir_plus_client_plus_setup_code)

df = meo_df.copy()
meo_df_copy = meo_df.copy()
del(meo_df)

df = df[df['ViewData.Status']!='Archive']

df[['len_0','len_1']] = df.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']), axis = 1)

df1 = df[(df['len_0']==0) | (df['len_1']==0) ]

df2 = df[~((df['len_0']==0) | (df['len_1']==0)) ]

side0 = df['ViewData.Side0_UniqueIds'].value_counts().reset_index()
side1 = df['ViewData.Side1_UniqueIds'].value_counts().reset_index()

side0_id = list(set(side0[side0['ViewData.Side0_UniqueIds']==1]['index']))
side1_id = list(set(side1[side1['ViewData.Side1_UniqueIds']==1]['index']))

df11 = df1[(df1['ViewData.Side0_UniqueIds'].isin(side0_id)) |(df1['ViewData.Side1_UniqueIds'].isin(side1_id)) ]
df2_umb = df2[((df2['ViewData.Status'] == 'UMB') & (df['len_0']>1) & (df['len_1']>1))]

df2_umb['predicted status'] = 'UMB'
df2_umb['predicted action'] = 'UMB_Carry_Forward'
df2_umb['predicted category'] = 'UMB'
#df2_umb['predicted comment'] = 'difference in amount'
df2_umb.rename(columns = {'ViewData.InternalComment2' : 'predicted comment'}, inplace =  True)
df2_umb = df2_umb[columns_to_output]
        
serial_num_umb = 0        
string_name_umb = 'p'+str(serial_num_umb) 
#filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
#filename = str(client) + '/' + str(setup_code) + '/setup ' + str(setup_code) + ' ' + string_name + '.csv'
filename_umb = str(client) +' setup ' + str(setup_code) + ' ' + string_name_umb + '.csv'
df2_umb.to_csv(filename_umb)

meo_df = pd.concat([df11,df2], axis = 0)
meo_df = meo_df.reset_index()
meo_df.drop('index', inplace = True, axis = 1)

# ### Coding approach to find UMR and UMT

# meo_df1= meo_df[meo_df['ViewData.Source Combination']=='Integrata,Goldman Sachs']

# dummy_filter = ['remove_mark','sel_mark']
# columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.ISIN','ViewData.Investment Type','ViewData.Investment ID','ViewData.Transaction Type','ViewData.Description','ViewData.Settle Date','ViewData.Trade Date','ViewData.Net Amount Difference','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
# amount_column = 'ViewData.Net Amount Difference

# filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
# serial_num = 1
# df1 = common_matching_engine_single1(meo_df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)

# filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
# serial_num = 2
# df2 = common_matching_engine_double1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)

#meo_df1 = meo_df[meo_df['ViewData.Source Combination']=='Integrata,Goldman Sachs']


src_comb_code = list(set(meo_df['ViewData.Source Combination']))

for item in src_comb_code:
    meo_df1 = meo_df[meo_df['ViewData.Source Combination']==item]

#vital_cols = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.ISIN','ViewData.Investment Type','ViewData.Investment ID','ViewData.Transaction Type','ViewData.Description','ViewData.Settle Date','ViewData.Trade Date','ViewData.Net Amount Difference','ViewData.Status']


    dummy_filter = ['remove_mark','sel_mark']
    columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
    amount_column = 'ViewData.Net Amount Difference'


# In[2425]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
    serial_num = 1
    df1 = common_matching_engine_single1(meo_df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2426]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
    serial_num = 2
    df2 = common_matching_engine_double1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2427]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
    serial_num = 3
    df3 = common_matching_engine_single1(df2,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2428]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
    serial_num = 4
    df4 = common_matching_engine_double1(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2429]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']
    serial_num = 5
    df5 = common_matching_engine_single1(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 6
    df6 = common_matching_engine_double1(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2430]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
    serial_num = 7
    df7 = common_matching_engine_single1(df6,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 8
    df8 = common_matching_engine_double1(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2431]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Settle Date']
    serial_num = 9
    df9 = common_matching_engine_single1(df8,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 10
    df10 = common_matching_engine_double1(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2432]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']
    serial_num = 11
    df11 = common_matching_engine_single1(df10,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 12
    df12 = common_matching_engine_double1(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)





    df12_1 = df12[((df12['ViewData.ISIN'].isna()) & (df12['ViewData.Investment ID'].isna())) ]
    df12_2 = df12[~((df12['ViewData.ISIN'].isna()) & (df12['ViewData.Investment ID'].isna())) ]





    df12_2['ViewData.ISIN'] = df12_2['ViewData.ISIN'].fillna('AAAA')



    df12_2['ID'] = df12_2.apply(lambda x : x['ViewData.Investment ID'] if x['ViewData.ISIN']=='AAAA' else x['ViewData.ISIN'], axis =1 )


# In[2448]:


    columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']


# In[2442]:


    filter_umb = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ID']


# In[2443]:


    dummy = df12_2.groupby(filter_umb)[amount_column].apply(list).reset_index()
    dummy1 = df12_2.groupby(filter_umb)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filter_umb, how = 'left')
    dummy2 = df12_2.groupby(filter_umb)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filter_umb, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )





    dummy0 = dummy[['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account',
       'ViewData.Currency', 'ID', 
       'sel_mark']]



    dfk = pd.merge(df12_2, dummy0, on = filter_umb, how = 'left')



    dfk4 = dfk[(dfk['sel_mark']==1)]
  
    serial_num = 13  

    if dfk4.shape[0]!=0:
        k1 = dfk4.groupby(filter_umb)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
        k2 = dfk4.groupby(filter_umb)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
        k3 = dfk4.groupby(filter_umb)['ViewData.BreakID'].apply(list).reset_index()
        k4 = dfk4.groupby(filter_umb)['ViewData.Status'].apply(list).reset_index()
        k = pd.merge(k1, k2 , on = filter_umb, how = 'left')
        k = pd.merge(k, k3 , on = filter_umb, how = 'left')
        k = pd.merge(k, k4 , on = filter_umb, how = 'left')
        
        k['predicted status'] = 'UMB'
        k['predicted action'] = 'UMB'
        k['predicted category'] = 'UMB'
        k['predicted comment'] = 'difference in amount'
        k = k[columns_to_output]
        
        
        string_name = 'p'+str(serial_num) + ' ' + str(item)
#        filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
        filename = str(client) +' setup ' + str(setup_code) + ' ' + string_name + '.csv'
        k.to_csv(filename)
    
        dfk5 = dfk[(dfk['sel_mark']!=1)]
    else:
        dfk5 = dfk.copy()


    serial_num = 14
    if dfk5[((dfk5['ViewData.Status'] == 'UMB') | (dfk5['ViewData.Status'] == 'SMB'))].shape[0]!=0:
        string_name = 'p'+str(serial_num) + ' ' + str(item)
#        filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
        filename = str(client) +' setup ' + str(setup_code) + ' ' + string_name + '.csv'
        dfk6 = dfk5[dfk5['ViewData.Status'] == 'UMB'][columns_to_output]
        dfk6.to_csv(filename)
        ob = dfk5[dfk5['ViewData.Status'] != 'UMB']
    else:
        ob = dfk5.copy()

    df3 = ob.copy()
	
    df = pd.read_excel('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Mapping variables for variable cleaning.xlsx', sheet_name='General')
#    df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')

    df['tuple'] = df.apply(make_dict, axis=1)
    clean_map_dict = df.set_index('tuple')['Value'].to_dict()

    df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
    df3['ViewData.Asset Type Category'] = df3['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
    df3['ViewData.Investment Type'] = df3['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
    df3['ViewData.Prime Broker'] = df3['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)


    df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
    df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
    df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
    df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)


    df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
    df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
    df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
    df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)

    com = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\desc cat with naveen oaktree.csv')
#    com = pd.read_csv('desc cat with naveen oaktree.csv')
    cat_list = list(set(com['Pairing']))


    df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


    df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
    com = com.drop(['var','Catogery'], axis = 1)
    com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
    com['replace'] = com['replace'].apply(lambda x : x.lower())

    
    df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

    comp = ['inc','stk','corp ','llc','pvt','plc']
    df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
        
    df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

    df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
    new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}

    df3['new_pb'] = df3['new_pb'].apply(lambda x : new_pf_mapping(x))
    df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].fillna('kkk')
    df3['new_pb1'] = df3.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)
    df3['new_pb1'] = df3['new_pb1'].apply(lambda x : x.lower())

    df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])
    days = [1,30,31,29]
    df3['monthend marker'] = df3['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)

    df3['comm_marker'] = 'zero'
    df3['new_pb2'] = df3.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
    df3['new_pb2'] = df3['new_pb2'].apply(lambda x : x.lower())

    cols = ['ViewData.Transaction Type1','ViewData.Asset Type Category1','ViewData.Investment Type1','new_desc_cat','new_pb2','new_pb1','comm_marker','monthend marker']

    df4 = df3[cols]

    df4['ViewData.Transaction Type1'] = df4['ViewData.Transaction Type1'].fillna('aa')
    df4['ViewData.Asset Type Category1'] = df4['ViewData.Asset Type Category1'].fillna('aa')
    df4['ViewData.Investment Type1'] = df4['ViewData.Investment Type1'].fillna('aa')
    df4['new_desc_cat'] = df4['new_desc_cat'].fillna('aa')
    df4['new_pb2'] = df4['new_pb2'].fillna('aa')
    df4['new_pb1'] = df4['new_pb1'].fillna('aa')
    df4['comm_marker'] = df4['comm_marker'].fillna('aa')
    df4['monthend marker'] = df4['monthend marker'].fillna('aa')


#    filename = 'finalized_model_lombard_249_v1.sav'
    filename = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\finalized_model_' + str.lower(str('Lombard')) + '_' + str(setup_code) + '_v1.sav'
    clf = pickle.load(open(filename, 'rb'))

    cb_predictions = clf.predict(df4)

    demo = []
    for item_cb_predict in cb_predictions:
        demo.append(item_cb_predict[0])
    df3['predicted category'] = pd.Series(demo)

    com_temp = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Lombard comment template for delivery.csv')

    com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
    result_non_trade = df3.copy()
    result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')

    result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
    result_non_trade['predicted status'] = 'OB'
    result_non_trade['predicted action'] = 'No-pair'
    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
    path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
	
    print('comment path')
    print(path)
#    path = 'Lombard/249/' + str(item) + 'Comment file for lombard 249.csv'
   
    result_non_trade.to_csv(path)

serial_num_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14] 
path_serial_num_list = [str(client) +' setup ' + str(setup_code) + ' p' + str(x) for x in serial_num_list]

all_paths_list = []
for path_serial_num_list_element in path_serial_num_list:
	path_serial_num_Source_Combination_list = [path_serial_num_list_element + ' ' + str(src_comb_code_element) + '.csv' for src_comb_code_element in src_comb_code]
	all_paths_list.append(path_serial_num_Source_Combination_list)
comment_filepath_list = ['Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(src_comb_code_element) + '.csv' for src_comb_code_element in src_comb_code]
all_paths_list.append(comment_filepath_list)

flatten=lambda l: sum(map(flatten,l),[]) if isinstance(l,list) else [l]	
all_paths_list_flattened = flatten(all_paths_list)
all_paths_list_flattened.append(filename_umb)


def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
    frames = []
    current_folder = os.getcwd()
    full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
    for full_filepath in full_filepath_list :
        if os.path.isfile(full_filepath) == True:
            frames.append(pd.read_csv(full_filepath))
    return pd.concat(frames)

final_df = check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(all_paths_list_flattened)

stop = timeit.default_timer()

print('Time: ', stop - start)

final_df = final_df.reset_index()
final_df = final_df.drop('index', axis = 1)

if('Unnamed: 0' in list(final_df.columns)):
    final_df.drop(['Unnamed: 0'], axis = 1, inplace = True)

final_df = final_df.rename(columns = {'ViewData.BreakID' : 'BreakID',
                           'ViewData.Source Combination Code' : 'SourceCombinationCode',
                           'ViewData.Task ID' : 'TaskID',
                           'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
                           'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
                           'Predicted Comment' : 'PredictedComment',
                           'Predicted Category' : 'PredictedCategory'})



	
	
	
	
	
