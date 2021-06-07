#!/usr/bin/env python
# coding: utf-8

# In[1]:

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
client = 'Schonfeld'

setup = '897'
setup_code = '897'
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

meo_df = json_normalize(list_of_dicts_query_result_1)
meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
#meo_df = pd.read_csv(base_dir_containing_client + 'meo_df_setup_897_date_2020-12-17.csv')
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
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code +'\\BD_of_' + str(date_i))
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

# ### Reading comments file

# #### Establish connection with DB, read meo_df and make a copy to df1
# #### Examination of Df1 for duplication

# #### Extract by date, combines sheets of position file and read position file in pos

# In[10]:

# ### Start of operation

# #### Standardization of ticker of both position and cash file

rem_word = ['comdty','index','indx','elec']

def tickerclean(x):
    if ((x!= None) & (type(x)== str)):
        x = x.lower()
        x1 = x.split()
        k = []
        for item in x1:
            if item not in rem_word:
                k.append(item)
        return ' '.join(k)

pos = pos[['Fund', 'Custodian Account','Investment ID', 'Strategy', 'Security Type', 'Currency',
        'Description', 'Quantity Diff','Local Price Diff', 'Price dif %','Local MV Diff']]

pos = pos.rename(columns = {'Description':'Pos_Desc','Security Type':'Pos_security','Quantity Diff':'pos_qnt_diff','Investment ID':'Ticker'})

pos = pos[~pos['Ticker'].isna()]


pos['Ticker1'] = pos['Ticker'].apply(lambda x : tickerclean(x) )


# In[21]:


df1 = df1.rename(columns = {'ViewData.Mapped Custodian Account':'Custodian Account',
                           'ViewData.Currency':'Currency',
                            'ViewData.Ticker':'Ticker',
                          'ViewData.Net Amount Difference':'Net Amount Difference',
                           'ViewData.Settle Date':'Settle Date',
                           'ViewData.Trade Date':'Trade Date',
                           'ViewData.Description':'Description'})


# In[22]:


df1['Ticker1'] = df1['Ticker'].apply(lambda x : tickerclean(x))


# In[23]:


df1.drop('Ticker', axis =1 , inplace = True)
pos.drop('Ticker', axis =1 , inplace = True)


# In[24]:


df1 = pd.merge(df1, pos, on = ['Custodian Account','Currency','Ticker1'], how = 'left')


# In[25]:


df1 = df1.reset_index()
df1 =df1.drop('index',1)


# In[26]:


df1.count()


# In[27]:


pd.set_option('display.max_columns', 500)


# ### Cleaning of Variables and feature Engineering

# - cleaning of dates

# In[28]:


df2= df1.copy()


# In[29]:


df2.shape


# In[30]:


import datetime


# In[31]:


df2['Settle Date'] = pd.to_datetime(df2['Settle Date'])
df2['Trade Date'] = pd.to_datetime(df2['Trade Date'])
#df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])


# In[32]:


#df2['ViewData.Task Business Date1'] = df2['ViewData.Task Business Date'].dt.date


# In[33]:


df2['Settle Date1'] = df2['Settle Date'].dt.date
df2['Trade Date1'] = df2['Trade Date'].dt.date


# - Cleaning of description

# In[34]:


def desc_clean(x):
    if type(x)== str:
        x = x.lower()
        x1 = x.split()
        if 'interest' in x1:
            return 'interest'
        elif (('fx' in x1) & ('conv' in x1)):
            return 'fx conv'
        elif (('comm' in x1) & ('adj' in x1)):
            return 'commission'
        elif(('fee' in x1) & ('adj' in x1)):
            return 'fee'
        elif (('aps' in x1) & ('adj' in x1)):
            return 'aps'
        elif (('internal' in x1) & ('wire' in x1) & ('transfer' in x1)):
            return 'internal wire transfer'
        elif (('incoming' in x1) & ('fedwire(s)' in x1)):
            return 'incoming fedwire'
        else:
            return x
        


# In[35]:


df2['Description'] = df2['Description'].apply(lambda x :desc_clean(x))


# - Taking care of the amount variable

# In[36]:


pd.set_option('display.max_columns', 500)


# In[37]:


df2.shape


# In[38]:


def amountcleaning(x):
    if((type(x) == int) or (type(x) == float)):
        return float(x)
    elif type(x)==str:
        
        if x.startswith('('):
            x1 = x.strip('(,)')
            x2 = x1.split(',')
            x3 = []
            for item in x2:
                if item!=',':
                    x3.append(item)
            x4 = ''.join(x3)
                
            x4 = 0 - float(x4)
            return x4
        else:
            x2 = x.split(',')
            x3 = []
            for item in x2:
                if item!=',':
                    x3.append(item)
            x4 = ''.join(x3)
            return float(x4)
    else:
        return 1234567


# In[39]:


df2['Net Amount Difference1']=df2['Net Amount Difference']


# In[40]:


#df2['Net Amount Difference1'] = df2['Net Amount Difference'].apply(lambda x : amountcleaning(x))
#df2['Local MV Diff'] = df2['Local MV Diff'].apply(lambda x :amountcleaning(x) )
df2['Local MV Diff'] = df2['Local MV Diff'].fillna(123456)


# In[41]:


df2.shape


# In[42]:


dummyk = df2.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].sum().reset_index()


# In[43]:


dummyk = dummyk.rename(columns = {'Net Amount Difference1':'Cash Standing'})


# In[44]:


def remove_mark_pre(y,k):
    
   
   
    if ((abs(k)<0.5) &  (y==0)):
            return 1
     
    else:
        return 0


# In[45]:


dummyk = dummyk[['Custodian Account', 'Currency', 'Ticker1','Cash Standing']]


# In[46]:


df2 = pd.merge(df2,dummyk, on = ['Custodian Account','Currency','Ticker1'], how = 'left')


# In[47]:


df2['pos_qnt_diff'] = df2['pos_qnt_diff'].fillna(0)


# In[48]:


df2.shape


# In[49]:


df2['remove_mark_fin'] = df2.apply(lambda x :remove_mark_pre(x['pos_qnt_diff'] ,x['Cash Standing']),axis = 1)


# In[50]:


df2.columns


# In[51]:


df2['cash difference'] = df2.apply(lambda x : x['Cash Standing']+x['Local MV Diff'] if x['Local MV Diff']!=1234567.0 else 1234567, axis =1)


# In[52]:


df2['zero_list'] = 'am'
df2['diff_len'] = 10
df2['remove_mark'] = 1
df2['remove_mark_new'] = 10
df2['day'] = 10


# In[53]:


dfn = df2[df2['remove_mark_fin'] ==1]


# In[54]:


dfn.shape


# In[55]:


if dfn.shape[0]!=0:
    dfn['predicted action'] = 'pair'
    dfn['predicted category'] = 'match'
    dfn['predicted comment'] = 'Match'
    dfn['predicted status'] = dfn['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
    dfn.to_csv('Schonfield ' + setup_code +' Meo Prediction P0.csv')
    df2 = df2[df2['remove_mark_fin'] !=1]
else:
    df2 = df2.copy()


# In[56]:


df2 = df2.drop(['zero_list', 'diff_len', 'remove_mark',
       'remove_mark_fin','remove_mark_new','day'], axis = 1)
df2 = df2.reset_index()
df2.drop('index', axis = 1,inplace = True)


# In[57]:


df2.shape


# ### Elimination of Matched

# #### Absolute Zero : No tolerance

# In[58]:


dummy = df2.groupby(['Custodian Account','Currency','Ticker1'])['Net Amount Difference1'].apply(list).reset_index()


# In[59]:


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


# In[60]:


dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))


# In[61]:


dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))


# In[62]:


dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))


# In[63]:


dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']


# In[64]:


dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))


# In[65]:


#dummy['remain_amt'] = dummy.apply(lambda x : list(set(x['Net Amount Difference1'])-set(x['zero_list'])) if x['remove_mark'] == 1 else "AA", axis =1)


# In[66]:


dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')


# In[67]:


dummy.count()


# In[68]:


dummy['pos_qnt_diff'] = dummy['pos_qnt_diff'].fillna(0)


# In[69]:


def remove_mark(x,y,z,k):
   
  
   if ((x>1) & (x<20)):
       if ((abs(k)<0.5) &  (y==0)):
           return 1
       elif ((abs(k)<5.1) & (z==0) & (y==0)):
           return 1
       elif ((abs(k)<5.1) & (z<3) & (y==0)):
           return 1
       else:
           return 0
   else:
       return 0


# In[70]:


dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['pos_qnt_diff'],x['diff_len'],x['zero_list_sum']),axis = 1)


# In[71]:


dummy = dummy[['Custodian Account', 'Currency', 'Ticker1', 'zero_list',  'diff_len', 'remove_mark']]


# In[72]:


df3 = pd.merge(df2, dummy, on = ['Custodian Account','Currency','Ticker1'], how = 'left')


# In[73]:


df3.shape


# In[74]:


def remover(x,y,z):
    if x==1:
        if y in z:
            return 1
        else:
            return 0
    else:
        return 0
   


# In[75]:


df3['remove_mark_fin'] = df3.apply(lambda x : remover(x['remove_mark'],x['Net Amount Difference1'],x['zero_list']),axis =1)


# In[76]:


df4 = df3[df3['remove_mark_fin']==1]
df5 = df3[df3['remove_mark_fin']!=1]


# In[77]:


df4.shape


# In[78]:


def date_remover(x,y,z):
    if isinstance(x,list):
        for item in x:
            if ((item>y) & (item<z)):
                return 0
            else:
                return 1
    else:
        return 1


# In[79]:


df5.shape


# In[80]:


if df4.shape[0] !=0:
    df4['day'] = df4['Trade Date'].dt.day
    df5['day'] = df5['Trade Date'].dt.day
    agg = df4.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg = agg.rename(columns = {'day':'zero_day'})
    agg1 = df5.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
    agg2 = agg2[['Custodian Account','Currency','Ticker1','remove_mark_new']]
    df4 = pd.merge(df4, agg2, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    if df4[df4['remove_mark_new']==1].shape[0]!=0:
        df4_new = df4[df4['remove_mark_new']==1]
        df4_new['predicted action'] = 'pair'
        df4_new['predicted category'] = 'match'
        df4_new['predicted comment'] = 'Match'
        df4_new['predicted status'] = df4['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
        df4_new.to_csv('Schonfield ' + setup_code +' Meo Prediction P1.csv')
        df5_new = df4[df4['remove_mark_new']!=1]
        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
        
        df5 = pd.concat([df5_new,df5], axis = 0)
        df5 = df5.reset_index()
        df5.drop(['index','day'], axis = 1, inplace = True)
    else:
        df5 = df3.copy()
else:
    
    df5 = df5.copy()
    


# In[81]:


df5_new.shape


# In[82]:


df5.shape


# #### Without Ticker

# In[83]:


df5.drop(['zero_list', 'diff_len', 'remove_mark',
       'remove_mark_fin'], axis =1 , inplace = True)


# In[84]:


dummy = df5.groupby(['Custodian Account','Currency'])['Net Amount Difference1'].apply(list).reset_index()


# In[85]:


dummy['len_amount'] = dummy['Net Amount Difference1'].apply(lambda x : len(x))


# In[86]:


dummy['zero_list'] = dummy['Net Amount Difference1'].apply(lambda x : subSum(x,0))


# In[87]:


dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))


# In[88]:


dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']


# In[89]:


dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))


# In[90]:


def remove_mark2(x,z,k):
    
   
    if ((x>1) & (x<20)):
        if ((abs(k)<0.5)):
            return 1
        elif ((abs(k)<5.1) & (z==0)):
            return 1
        elif ((abs(k)<5.1) & (z<4)):
            return 1
        else:
            return 0
    else:
        return 0


# In[91]:


dummy['remove_mark'] = dummy.apply(lambda x :remove_mark2(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)


# In[92]:


dummy = dummy[['Custodian Account', 'Currency', 'zero_list',  'diff_len', 'remove_mark']]


# In[93]:


df3 = pd.merge(df5, dummy, on = ['Custodian Account','Currency'], how = 'left')


# In[94]:


df3['remove_mark_fin'] = df3.apply(lambda x : remover(x['remove_mark'],x['Net Amount Difference1'],x['zero_list']),axis =1)


# In[95]:


df4 = df3[df3['remove_mark_fin']==1]
df5 = df3[df3['remove_mark_fin']!=1]


# In[96]:


if df4.shape[0] !=0:
    df4['day'] = df4['Trade Date'].dt.day
    df5['day'] = df5['Trade Date'].dt.day
    agg = df4.groupby(['Custodian Account','Currency'])['day'].apply(list).reset_index()
    agg = agg.rename(columns = {'day':'zero_day'})
    agg1 = df5.groupby(['Custodian Account','Currency'])['day'].apply(list).reset_index()
    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency'], how = 'left' )
    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
    agg2 = agg2[['Custodian Account','Currency','remove_mark_new']]
    df4 = pd.merge(df4, agg2, on = ['Custodian Account','Currency'], how = 'left' )
    if df4[df4['remove_mark_new']==1].shape[0]!=0:
        df4_new = df4[df4['remove_mark_new']==1]
        df4_new['predicted action'] = 'pair'
        df4_new['predicted category'] = 'match'
        df4_new['predicted comment'] = 'Match'
        df4_new['predicted status'] = df4['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
        df4_new.to_csv('Schonfield ' + setup_code +' Meo Prediction P2.csv')
        df5_new = df4[df4['remove_mark_new']!=1]
        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
        df5 = pd.concat([df5_new,df5], axis = 0)
        df5 = df5.reset_index()
        df5.drop(['index','day'], axis = 1, inplace = True)
    else:
        df5 = df3.copy()
else:
    
    df5 = df5.copy()


# In[97]:


df5.shape


# ### Cases of Currency conversion - New Stuff

# In[98]:


df5.drop(['zero_list', 'diff_len', 'remove_mark','remove_mark_fin'], axis =1 , inplace = True)


# In[99]:

abhijeet_comment_viteos_folder_filepath = 'D:\\ViteosModel\\Abhijeet - Comment\\'
conv = pd.read_csv(abhijeet_comment_viteos_folder_filepath + 'currency conversion.csv')



# In[100]:


df5 = pd.merge(df5, conv, on = 'Currency', how = 'left')


# In[101]:


df5.shape


# In[102]:


df5['USD conv amount'] = round(df5['Net Amount Difference1']* df5['conversion'],2)


# #### With Ticker

# In[103]:


dummy = df5.groupby(['Custodian Account','Currency','Ticker1'])['USD conv amount'].apply(list).reset_index()
dummy['len_amount'] = dummy['USD conv amount'].apply(lambda x : len(x))
dummy['zero_list'] = dummy['USD conv amount'].apply(lambda x : subSum(x,0))
dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
dummy['pos_qnt_diff'] = dummy['pos_qnt_diff'].fillna(0)
dummy['remove_mark'] = dummy.apply(lambda x :remove_mark(x['zero_list_len'],x['pos_qnt_diff'],x['diff_len'],x['zero_list_sum']),axis = 1)
dummy = dummy[['Custodian Account', 'Currency', 'Ticker1', 'zero_list',  'diff_len', 'remove_mark']]
df3 = pd.merge(df5, dummy, on = ['Custodian Account','Currency','Ticker1'], how = 'left')
df3['remove_mark_fin'] = df3.apply(lambda x : remover(x['remove_mark'],x['USD conv amount'],x['zero_list']),axis =1)


# In[104]:


df4 = df3[df3['remove_mark_fin']==1]
df5 = df3[df3['remove_mark_fin']!=1]


# In[105]:


df4.drop(['USD conv amount','conversion'], axis = 1, inplace = True)


# In[106]:


if df4.shape[0] !=0:
    df4['day'] = df4['Trade Date'].dt.day
    df5['day'] = df5['Trade Date'].dt.day
    agg = df4.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg = agg.rename(columns = {'day':'zero_day'})
    agg1 = df5.groupby(['Custodian Account','Currency','Ticker1'])['day'].apply(list).reset_index()
    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
    agg2 = agg2[['Custodian Account','Currency','Ticker1','remove_mark_new']]
    df4 = pd.merge(df4, agg2, on = ['Custodian Account','Currency','Ticker1'], how = 'left' )
    if df4[df4['remove_mark_new']==1].shape[0]!=0:
        df4_new = df4[df4['remove_mark_new']==1]
        df4_new['predicted action'] = 'pair'
        df4_new['predicted category'] = 'match'
        df4_new['predicted comment'] = 'Match'
        df4_new['predicted status'] = df4['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
        df4_new.to_csv('Schonfield ' + setup_code +' Meo Prediction P3.csv')
        df5_new = df4[df4['remove_mark_new']!=1]
        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
        df5 = pd.concat([df5_new,df5], axis = 0)
        df5 = df5.reset_index()
        df5.drop(['index','day'], axis = 1, inplace = True)
    else:
        df5 = df3.copy()
else:
    
    df5 = df5.copy()


# In[107]:


df5.shape


# #### When Ticker is missing

# In[108]:


df5.drop(['zero_list', 'diff_len', 'remove_mark','remove_mark_fin'], axis =1 , inplace = True)


# In[109]:


dummy = df5.groupby(['Custodian Account','Currency'])['USD conv amount'].apply(list).reset_index()
dummy['len_amount'] = dummy['USD conv amount'].apply(lambda x : len(x))
dummy['zero_list'] = dummy['USD conv amount'].apply(lambda x : subSum(x,0))
dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))
dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
dummy['remove_mark'] = dummy.apply(lambda x :remove_mark2(x['zero_list_len'],x['diff_len'],x['zero_list_sum']),axis = 1)
dummy = dummy[['Custodian Account', 'Currency', 'zero_list',  'diff_len', 'remove_mark']]
df3 = pd.merge(df5, dummy, on = ['Custodian Account','Currency'], how = 'left')
df3['remove_mark_fin'] = df3.apply(lambda x : remover(x['remove_mark'],x['USD conv amount'],x['zero_list']),axis =1)


# In[110]:


df4 = df3[df3['remove_mark_fin']==1]
df5 = df3[df3['remove_mark_fin']!=1]


# In[111]:


df4.shape


# In[112]:


df4.drop(['USD conv amount','conversion'], axis = 1, inplace = True)


# In[113]:


if df4.shape[0] !=0:
    df4['day'] = df4['Trade Date'].dt.day
    df5['day'] = df5['Trade Date'].dt.day
    agg = df4.groupby(['Custodian Account','Currency'])['day'].apply(list).reset_index()
    agg = agg.rename(columns = {'day':'zero_day'})
    agg1 = df5.groupby(['Custodian Account','Currency'])['day'].apply(list).reset_index()
    agg2 = pd.merge(agg, agg1, on = ['Custodian Account','Currency'], how = 'left' )
    agg2['zero_max'] = agg2['zero_day'].apply(lambda x : max(x))
    agg2['zero_min'] = agg2['zero_day'].apply(lambda x : min(x))
    agg2['remove_mark_new'] = agg2.apply(lambda x : date_remover(x['zero_day'],x['zero_min'],x['zero_max']), axis = 1)
    agg2 = agg2[['Custodian Account','Currency','remove_mark_new']]
    df4 = pd.merge(df4, agg2, on = ['Custodian Account','Currency'], how = 'left' )
    if df4[df4['remove_mark_new']==1].shape[0]!=0:
        df4_new = df4[df4['remove_mark_new']==1]
        df4_new['predicted action'] = 'pair'
        df4_new['predicted category'] = 'match'
        df4_new['predicted comment'] = 'Match'
        df4_new['predicted status'] = df4['ViewData.Status'].apply(lambda x : 'UMF' if x =='SMB' else 'UCB')
        df4_new.to_csv('Schonfield ' + setup_code +' Meo Prediction P4.csv')
#        df4_new.to_csv('Schonfield/tweak_test_897/14 dec file schonfield prediction p2.csv')
        df5_new = df4[df4['remove_mark_new']!=1]
        df5_new.drop(['remove_mark_new'], axis = 1, inplace = True)
        df5 = pd.concat([df5_new,df5], axis = 0)
        df5 = df5.reset_index()
        df5.drop(['index','day'], axis = 1, inplace = True)
    else:
        df5 = df3.copy()
else:
    
    df5 = df5.copy()


# ### Final Commenting Section

# In[114]:


df5.drop(['USD conv amount','conversion'], axis = 1, inplace = True)


# In[115]:

#TODO : When taking data from DB, check if df6 and df7 are forming or not. This execution is for data from csv
df6 = df5[df5['ViewData.InternalComment2'].isna()]
df7 = df5[~df5['ViewData.InternalComment2'].isna()]


# In[116]:


if df7.shape[0]!=0:
    df7['predicted action'] = 'No-pair'
    df7['predicted category'] = 'OB'
    df7['predicted comment'] = df7['ViewData.InternalComment2']
    df7['predicted status'] = df7['ViewData.Status']
else:
    df6 = df6.copy()


# In[117]:


df6.shape


# In[118]:


df6['Local Price Diff'] = df6['Local Price Diff'].fillna(0)


# In[119]:


def commentschon(pos,amt,accamt, pbamt,cash_diff):
    if ((pos==0) & (amt==0)):
        if((cash_diff<6.0) & (cash_diff>-6.0)):
            com = 'MV Swing'
        else:
            
            com = 'Commission & fee difference,SFA to advise'
    elif(pos!=0):
#        if (accamt==None):
        if ((accamt==None) | (math.isnan(accamt))):
#        if (math.isnan(accamt)):
            print('found null')
            com = 'GVA missing the trade, viteos to check and book'
        else:
            com = 'PB to report missing the trade.'
    else:
        com = 'MV Swing'
        
    return com
            


# In[120]:


df6['predicted comment'] = df6.apply(lambda x : commentschon(x['pos_qnt_diff'],x['Local Price Diff'],x['ViewData.Accounting Net Amount'],x['ViewData.Cust Net Amount'],x['cash difference']),axis = 1)


# In[121]:


df6['predicted comment'].value_counts()


# In[122]:


df6['predicted status'] = df6['ViewData.Status']
df6['predicted action'] = 'No-pair'
df6['predicted category'] = 'OB'


# In[123]:

df7.to_csv('Schonfield ' + setup_code +' Meo Prediction P5.csv')
df6.to_csv('Schonfield ' + setup_code +' Meo Prediction P6.csv')



# In[124]:


def check_if_file_exist_in_cwd_and_append_to_df_list_if_exists(fun_only_filename_with_csv_list):
    frames = []
    current_folder = os.getcwd()
    full_filepath_list = [current_folder + '\\' + x for x in fun_only_filename_with_csv_list]
    for full_filepath in full_filepath_list :
        if os.path.isfile(full_filepath) == True:
            frames.append(pd.read_csv(full_filepath))
    return pd.concat(frames)


# #### Combining all the files
final_df_filename_list = ['Schonfield 897 Meo Prediction P' + str(x) + '.csv' for x in [0,1,2,3,4,5,6]]
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

filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '_after_business_consent_2_abhijeet_ask_6.csv'
final_df_2.to_csv(filepaths_final_df_2)

filepaths_meo_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + str(date_i) + '_abhijeet_ask_6.csv'
meo_df.to_csv(filepaths_meo_df)

#data_dict = final_table_to_write.to_dict("records")
coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing = db_2_for_MEO_data_MLReconDB_Testing['MLPrediction_Cash']


data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data.insert_many(data_dict) 

data_dict_for_testingdb = final_df_2.to_dict("records_final_for_testingdb")
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.insert_many(data_dict_for_testingdb) 

print(setup_code)
print(date_i)
    

