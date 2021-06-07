#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from datetime import datetime 
#from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
from tqdm import tqdm
import pickle
import os
import sys
#from sklearn.metrics import confusion_matrix



# In[4709]:

print(os.getcwd())
os.chdir('C:\\Users\\consultant138\\Downloads\\Viteos_Rohit\\ViteosModel')
print(os.getcwd())

orig_stdout = sys.stdout
f = open('1105_model_run_' + str(datetime.now().strftime("%d_%m_%Y_%H_%M")) + '.txt', 'w')
sys.stdout = f

print(datetime.now())

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


cols = ['Currency','Account Type','PMSVendor Net Amount',
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
'Derived Source','Description','ExpiryDate','ExternalComment1','ExternalComment2',
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


# In[4710]:


new_cols = ['ViewData.' + x for x in cols] + add

cols_to_show = [
'Account Type',
'PMSVendor Net Amount',
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
'Custodian Account',
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


date_numbers_list = [1,3,4,
                     7,9,
                     
                     23,25,
                     29,30]

dates_with_error_1105 = [2,8,10,11,14,15,16,17,18,21,22,24]
# Error for dates 14,18,28,
#Statement : print(classification_report(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status']))
#ValueError: max() arg is an empty sequence

# Error for dates 2,11,17,
#Statement : test_file = pd.concat(training_df)
#ValueError: No objects to concatenate

# Problem with dates 8,10,15,16,21,22,24
# No AUA data

# Problem with dates 15,16,18,28
# No MEO data

filepaths_X_test = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/X_Test_1105/x_test_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_no_pair_id_data = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/X_Test_1105/no_pair_ids_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_no_pair_id_no_data_warning = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/X_Test_1105/WARNING_no_pair_ids_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/AUA/AUACollections.AUA_HST_RecData_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/MEO/MeoCollections.MEO_HST_RecData_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_final_prediction_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/Final_Predictions_1105/Final_Predictions_Table_HST_RecData_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_accuracy_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/Final_Predictions_1105/Accuracy_Table_HST_RecData_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_crosstab_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/JuneData/Final_Predictions_1105/Crosstab_Table_HST_RecData_1105_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]

# In[4711]:


#df_170.shape


# ## Read testing data 

# In[4712]:

i = 0
for i in range(0,len(date_numbers_list)):
    meo = pd.read_csv(filepaths_MEO[i],usecols=new_cols)
    meo = meo.rename(columns={'ViewData.PMSVendor Net Amount':'ViewData.Accounting Net Amount'})
    meo = meo.rename(columns={'ViewData.B-P Net Amount':'ViewData.B-P Net Amount'})


    df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
    #df = df[df['MatchStatus'] != 21]
    df1 = df1[~df1['ViewData.Status'].isnull()]
    df1 = df1.reset_index()
    df1 = df1.drop('index',1)
    

# ## Machine generated output

# In[4716]:


    df = df1.copy()
    
    
    # In[4717]:
    
    
    df = df.reset_index()
    df = df.drop('index',1)
    
    



    # In[4720]:
    
    
    df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
    
    
    # In[4721]:
    
    
    #df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
    
    
    # In[4722]:
    
    
    df = df[~df['Date'].isnull()]
    df = df.reset_index()
    df = df.drop('index',1)
    

    # In[4723]:
    
    
    pd.to_datetime(df['Date'])
    
    
    # In[4724]:
    
    
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    
    
    # In[4725]:
    
    
    df['Date'] = df['Date'].astype(str)
    

# In[4726]:



    # In[4727]:
    
    
    df = df[df['ViewData.Status'].isin(['OB','SPM','SDB','UOB','UDB','SMB'])]
    df = df.reset_index()
    df = df.drop('index',1)
    
    
    # In[4728]:
    
    
    df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
    df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
    df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
    df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
    
    
    # In[4729]:
    
    
    #df_170[(df_170['ViewData.Status']=='UMR')]
    
    
    # In[4730]:
    


    print('The Date value count is:')
    print(df['Date'].value_counts())
    
    date_i = df['Date'].mode()[0]
    
    print('Choosing the date : ' + date_i)
    
    df = df.rename(columns= {'ViewData.B-P Net Amount':'ViewData.B-P Net Amount'})
    
    sample = df[df['Date'] == date_i]

    sample = sample.reset_index()
    sample = sample.drop('index',1)
    
    
    # In[4945]:
    
    
    smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
    smb = smb.drop('index',1)


# In[4946]:


#import glob
#df_list = []

#path = "//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/AUA/*.csv"
#for fname in glob.glob(path):
#    print(fname)
#    if "125" in fname:
#        df_list.append(pd.read_csv(fname))


# In[4947]:


#dfff = pd.concat(df_list, axis=0)
#dfff['Date'] = pd.to_datetime(dfff['ViewData.Task Business Date'])
#dfff[dfff['ViewData.Status']=='UCB'].shape


# In[4948]:


#dfff[dfff['ViewData.Status']=='UCB'].groupby('Date')['ViewData.Age'].size()


# In[4949]:


#dfff[(dfff['ViewData.Status']=='UCB') & (dfff['Date']=='2020-06-30')]['ViewData.Side1_UniqueIds'].nunique()


# In[4950]:


    smb_pb = smb.copy()
    smb_acc = smb.copy()
    
    
    # In[4951]:
    
    
    smb_pb['ViewData.Accounting Net Amount'] = np.nan
    smb_pb['ViewData.Side0_UniqueIds'] = np.nan
    smb_pb['ViewData.Status'] ='SMB-OB'
    
    smb_acc['ViewData.B-P Net Amount'] = np.nan
    smb_acc['ViewData.Side1_UniqueIds'] = np.nan
    smb_acc['ViewData.Status'] ='SMB-OB'
    
    
    # In[4952]:
    
    
    sample = sample[sample['ViewData.Status']!='SMB']
    sample = sample.reset_index()
    sample = sample.drop('index',1)
    
    
    # In[4953]:


# In[4954]:


    sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
    sample = sample.reset_index()
    sample = sample.drop('index',1)
    
    
    # In[4955]:




# In[4958]:
    
    
    sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
    sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[4959]:
    
    
    sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
    sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
    
    
    # In[4960]:



# In[4961]:



# In[4962]:


    sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
    sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
    
    sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
    sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 
    
    sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
    sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
    sample['ViewData.Source Combination'] = sample['ViewData.Source Combination'].astype(str)
    sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
    #sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
    sample['filter_key'] = sample.apply(lambda x: x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)
    
    
    sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB','SMB-OB']))]
    
    sample1 = sample1.reset_index()
    sample1 = sample1.drop('index', 1)
    
    
    # In[4963]:
    
    
    sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)
    
    
    # In[4964]:
    
    
    sample1 = sample1[sample1['ViewData.BreakID']!=-1]
    sample1 = sample1.reset_index()
    sample1 = sample1.drop('index',1)
    
    
    # In[4965]:
    
    
    sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
    sample1 = sample1.reset_index()
    sample1 = sample1.drop('index',1)


# In[4966]:



# In[5252]:


#sample1[sample1['ViewData.Status']=='SMB-OB']


# In[4968]:


    aa = sample1[sample1['Trans_side']=='A_side']
    bb = sample1[sample1['Trans_side']=='B_side']
    
    if aa.empty:
        print('aa is empty')
    if bb.empty:
        print('bb is empty')
    
# In[4969]:



    aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)
    
    bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)
    
    
    # In[4971]:
    
    
    aa = aa.reset_index()
    aa = aa.drop('index', 1)
    bb = bb.reset_index()
    bb = bb.drop('index', 1)
    
    
    # In[4972]:


#'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds'


# In[4973]:


    bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
    bb = bb.reset_index()
    bb = bb.drop('index',1)
    
    
    # In[4974]:



# In[4975]:


###################### loop 3 ###############################


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
                dff = pd.merge(df1, df2, on='filter_key')
                training_df.append(dff)
                    #key_index.append(i)
                #else:
                #no_pair_ids.append([aa1[(aa1['filter_key']=='key') & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0]])
                   # no_pair_ids.append(aa1[(aa1['filter_key']== key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0])
        
            else:
                no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
                no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
            

# In[4976]:


#no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[4977]:


#pd.DataFrame(no_pair_ids).rename


# In[4978]:


    
    if len(no_pair_ids) != 0:
        no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])
        no_pair_ids_df = pd.DataFrame(no_pair_ids)
        #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
        no_pair_ids_df.columns = ['filter_key']
        no_pair_ids_df.to_csv(filepaths_no_pair_id_data[i])
    else:
         with open(filepaths_no_pair_id_no_data_warning[i], 'w') as f:
             f.write('No no pair ids found for this setup and date combination')

    # In[4981]:
    
    
    # In[4980]:
    
    
    test_file = pd.concat(training_df)
    
    
    # In[4982]:
    
    
    test_file = test_file.reset_index()
    test_file = test_file.drop('index',1)
    
    
    # In[4983]:
    
    
    test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
    test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')
    
    
    # In[4984]:


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


# In[4985]:
    
    
    test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].astype(str).str.split(".",expand=True)[0]
    test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].astype(str).str.split(".",expand=True)[0]
    
    
    # In[4986]:
    
    
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
    

# In[4987]:


    #test_file[['SideA.ViewData.ISIN','SideB.ViewData.ISIN']]
    
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


# In[4988]:


#test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)


# In[4989]:


#test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)


# In[4990]:


    test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
    test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']
    

# In[4991]:


#test_file = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/X_test_files_after_loop/meo_testing_HST_RecData_379_06_19_2020_test_file_with_ID.csv')


# In[4992]:


#test_file = test_file.drop('Unnamed: 0',1)


# In[4993]:


    test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days
    
    test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days


# In[4994]:



# In[4995]:




############ Fund match new ########

    values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
    values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values


    #test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
    #test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
    test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
    #test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
    test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)


# In[ ]:





# In[4996]:


#test_file['SideA.ViewData.Fund']


# In[4997]:


#test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[4998]:


#test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[4999]:


##############

    values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].astype(str).values
    values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].astype(str).values


    #test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
    #test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
    test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
    #test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
    test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)


# In[5000]:


    test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)
    
    
    # In[5001]:
    
    
    #train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
    test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)
    
    
    # In[ ]:
    
    
    
    
    
    # In[5002]:
    

    values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
    values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
    test_file['SideA.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)
    test_file['SideB.ISIN_NA'] = vec_nan_fun(values_ISIN_A_Side)
    
    #test_file['SideA.ISIN_NA'] =  test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']=='nan' else 0, axis=1)
    #test_file['SideB.ISIN_NA'] =  test_file.apply(lambda x: 1 if x['SideB.ViewData.ISIN']=='nan' else 0, axis=1)
    
    
    # In[5666]:



# In[ ]:





# In[5669]:



    values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
    values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
    
    values_CUSIP_A_Side = test_file['SideA.ViewData.CUSIP'].values
    values_CUSIP_B_Side = test_file['SideB.ViewData.CUSIP'].values
    
    test_file['SideB.ViewData.key_NAN']= vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[0]
    test_file['SideB.ViewData.Common_key'] = vec_a_key_match_fun(values_CUSIP_B_Side,values_ISIN_B_Side)[1]
    test_file['SideA.ViewData.key_NAN'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[0]
    test_file['SideA.ViewData.Common_key'] = vec_b_key_match_fun(values_CUSIP_A_Side,values_ISIN_A_Side)[1]


# In[ ]:


#test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
#test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[ ]:


    values_key_NAN_B_Side = test_file['SideB.ViewData.key_NAN'].values
    values_key_NAN_A_Side = test_file['SideA.ViewData.key_NAN'].values
    test_file['All_key_nan'] = vec_nan_equal_fun(values_key_NAN_B_Side,values_key_NAN_A_Side )
    
    #test_file['All_key_nan'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.key_NAN']==1 and x['SideA.ViewData.key_NAN']==1 else 0, axis=1)
    
    
    # In[5005]:
    
    
    test_file['SideB.ViewData.Common_key'] = test_file['SideB.ViewData.Common_key'].astype(str)
    test_file['SideA.ViewData.Common_key'] = test_file['SideA.ViewData.Common_key'].astype(str)


    values_Common_key_B_Side = test_file['SideB.ViewData.Common_key'].values
    values_Common_key_A_Side = test_file['SideA.ViewData.Common_key'].values
    values_All_key_NAN = test_file['All_key_nan'].values
    
    #values_accounting_nan = np.where((values_CUSIP_B_Side == 'nan') & (values_ISIN_B_Side == 'nan'),1,0)
    #values_b_common_key = np.where((values_CUSIP_B_Side == 'nan') & (values_ISIN_B_Side == 'nan'),'NA',
    #         np.where((values_CUSIP_B_Side != 'nan') & (values_ISIN_B_Side == 'nan'), values_CUSIP_B_Side,
    #                  np.where((values_CUSIP_B_Side == 'nan') & (values_ISIN_B_Side != 'nan'),values_ISIN_B_Side,values_ISIN_B_Side)))
    
    
    test_file['new_key_match']= vec_new_key_match_fun(values_Common_key_B_Side,values_Common_key_A_Side,values_All_key_NAN)


# In[5006]:



#test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)


# In[5007]:



# In[5008]:


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
    
    
    # In[4933]:
    

    test_file.to_csv(filepaths_X_test[i])


# In[930]:


#test_file['SideA.ViewData.BreakID_A_side'].value_counts()


# In[4299]:
    print('Done till X_Test creation')
    print(datetime.now())
    
    #test_file = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/X_Test/x_test_125_2020-06-8.csv")
    #
    #
    ## In[4300]:
    #
    #
    #test_file = test_file.drop('Unnamed: 0',1)
    
    
    # ## Test file served into the model
    
    # In[5009]:
    
    X_test = test_file[model_cols]
    
    
    # In[5010]:
    
    
    X_test = X_test.reset_index()
    X_test = X_test.drop('index',1)
    
    
    # In[5011]:
    
    
    X_test = X_test.fillna(0)


# In[5012]:



# In[5013]:


    X_test = X_test.drop_duplicates()
    X_test = X_test.reset_index()
    X_test = X_test.drop('index',1)
    
    
    # In[5014]:
    


# ## Model Pickle file import

# In[5015]:




# In[5016]:


#filename = 'Oak_W125_model_with_umb.sav'
#filename = '125_with_umb_without_des_and_many_to_many.sav'
#filename = '125_with_umb_and_price_without_des_and_many_to_many_tdsd2.sav'
    filename = 'HIG_new_model_V1.sav'
    clf = pickle.load(open(filename, 'rb'))


# In[5017]:


# In[5018]:




# ## Predictions

# In[5019]:


# Actual class predictions
    rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
    # Probabilities for each class
    rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]
    
    
    # In[5020]:
    
    
    probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
    probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]
    
    probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
    #probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]
    
    #probability_class_4 = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]
    
    
    # In[5021]:
    
    
    X_test['Predicted_action'] = rf_predictions
    #X_test['Predicted_action_probabilty'] = rf_probs
    X_test['probability_No_pair'] = probability_class_0
    #X_test['probability_Partial_match'] = probability_class_1
    #X_test['probability_UMB'] = probability_class_1
    #X_test['probability_UMB'] = probability_class_1
    X_test['probability_UMR'] = probability_class_1
    X_test['probability_UMT'] = probability_class_2
    
    
    # In[5022]:


    
    
    # ## Prediction Table
    
    # In[5023]:
    
    
    X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & ((X_test['Amount_diff_2']!=0) | (X_test['Amount_diff_2']!=0)),'Predicted_action'] = 'Unrecognized' 
    
    
    # In[5024]:
    
    
    
    # In[5025]:
    
    
    
    
    # In[5026]:
    
    
    ###### Probability filter for UMT and UMB ################
    
    #X_test.loc[(X_test['Predicted_action']=='UMT_One_to_One') & (X_test['probability_UMT']<0.90) & (X_test['probability_No_pair']>0.05),'Predicted_action'] = 'No-Pair' 
    
    #X_test.loc[(X_test['Predicted_action']=='UMB_One_to_One') & (X_test['probability_UMB']<0.75) & (X_test['probability_No_pair']>0.2),'Predicted_action'] = 'No-Pair' 
    
    #X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['probability_UMR']<0.90) & (X_test['probability_No_pair']>0.05),'Predicted_actionX_test.loc[(X_test['Predicted_action']=='No-Pair') & (X_test['probability_No_pair']<0.9) & (X_test['probability_UMB']>0.05),'Predicted_action'] = 'UMB_One_to_One' 
    
    
    #X_test.loc[(X_test['Predicted_action']=='No-Pair') & (X_test['probability_No_pair']<0.95) & (X_test['probability_UMB']>0.05),'Predicted_action'] = 'UMB_One_to_One' 
    
    #X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Settle_date_diff']>4),'Predicted_action'] = 'No-Pair' 
    #X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Settle_date_diff']<-4),'Predicted_action'] = 'No-Pair' 
    
    
    # In[5027]:
    
    
    #X_test.loc[(X_test['SideB.ViewData.Status']=='SDB') & (X_test['SideA.ViewData.Status']=='OB') & (X_test['Predicted_action']=='No-Pair'),'Predicted_action'] = 'SDB/Open Break'
    
    
    # In[5028]:
    
    
    prediction_table =  X_test.groupby('SideB.ViewData.BreakID_B_side')['Predicted_action'].unique().reset_index()
    
    
    # In[5029]:
    
    
    #prob1 = X_test.groupby('SideB.ViewData.BreakID_B_side')['probability_No_pair'].mean().reset_index()
    
    
    # In[5030]:
    
    
    prediction_table['len'] = prediction_table['Predicted_action'].str.len()
    
    
    # In[5031]:
    
    
    prediction_table['No_Pair_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
    
    
    # In[5032]:
    
    
#    prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
    prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
    prediction_table['UMR_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)
    
    
    # In[5033]:
    
    
    # In[5034]:
    
    
    # In[5035]:
    
    
    umr_array = X_test[X_test['Predicted_action']=='UMR_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
    umt_array = X_test[X_test['Predicted_action']=='UMT_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
#    umb_array = X_test[X_test['Predicted_action']=='UMB_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
    
    
    # In[5036]:
    
    
    umr_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMR_array']
    umt_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMT_array']
#    umb_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMB_array']
    
    
    # In[5037]:
    
    
    prediction_table = pd.merge(prediction_table,umr_array, on='SideB.ViewData.BreakID_B_side', how='left' )
    prediction_table = pd.merge(prediction_table,umt_array, on='SideB.ViewData.BreakID_B_side', how='left' )
#    prediction_table = pd.merge(prediction_table,umb_array, on='SideB.ViewData.BreakID_B_side', how='left' )
    
    
    # In[5038]:
    
    
    #prediction_table
    #X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='2495_125897734_Advent Geneva']
    
    
    
    # In[5039]:
    
    
    prediction_table['Final_prediction'] = prediction_table.apply(lambda x: 'UMR_One_to_One' if x['UMR_flag']==1 else('UMT_One_to_One' if x['len']==1 and x['UMT_flag']==1 else('No-Pair' if x['len']==1 else 'Undecided')), axis=1)
    
    
    # In[5040]:
    
    
    
    
    # In[5042]:
    
    
    prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
#    prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
    
    
    # In[5043]:
    
    
#    prediction_table.loc[(prediction_table['UMB_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMB_One_to_One'
    prediction_table.loc[(prediction_table['UMT_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMT_One_to_One'
    
    
    # In[5044]:
    
    
    prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['len']==2),'Final_prediction']='No-Pair/Unrecognized'
    
    
    # In[5045]:
    
    
    prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMT_flag']==1),'Final_prediction']='UMT_One_to_One'
    
    
    # In[5046]:
    
    
#    prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMB_flag']==1),'Final_prediction']='UMB_One_to_One'
    
    
    # In[5047]:
    
    
    
    
    # In[5049]:
    
    
    #X_test[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['SideB.ViewData.BreakID_B_side']==1346769635)]
    
    
    # In[5050]:
    
    
    prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Predicted_UMR_array']
    
    
    # In[5051]:
    
    
    prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Predicted_UMT_array']
#    prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Predicted_UMB_array']
    #prediction_table.loc[prediction_table['Final_prediction']=='No-Pair', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='No-Pair', '']
    
    
    # In[5052]:
    
    
    prediction_table['predicted_break_len'] = prediction_table['Final_predicted_break'].astype(str).str.len()
    
    
    # In[5053]:
    
    
    
    # In[5054]:
    
    
    #prediction_table[(prediction_table['predicted_break_len']>1) & (prediction_table['Final_prediction']=='UMT_One_to_One')]
    
    
    # In[5055]:
    
    
    #prediction_table[['SideB.ViewData.BreakID_B_side', 'Final_prediction', 'Final_predicted_break']]
    
    
    # In[5056]:
    
    
    X_test['prob_key'] = X_test['SideB.ViewData.BreakID_B_side'].astype(str) + X_test['Predicted_action']
    prediction_table['prob_key'] = prediction_table['SideB.ViewData.BreakID_B_side'].astype(str) + prediction_table['Final_prediction']
    
    
    # In[5057]:
    
    
    user_prob = X_test.groupby('prob_key')[['probability_UMR','probability_UMT']].max().reset_index()
    open_prob = X_test.groupby('prob_key')['probability_No_pair'].mean().reset_index()
    
    
    # In[5058]:
    
    
    #prediction_table = prediction_table.drop(,1)
    
    prediction_table = pd.merge(prediction_table,user_prob, on='prob_key', how='left')
    prediction_table = pd.merge(prediction_table,open_prob, on='prob_key', how='left')
    
    
    # In[5059]:
    
    
    prediction_table = prediction_table.drop('prob_key',1)
    
    
    # In[5060]:
    
    
    
    
    # In[5062]:
    
    
    prediction_table = pd.merge(prediction_table, X_test[['SideB.ViewData.BreakID_B_side','SideA.ViewData._ID','SideB.ViewData._ID']].drop_duplicates(['SideB.ViewData.BreakID_B_side','SideB.ViewData._ID']), on ='SideB.ViewData.BreakID_B_side', how='left')
    
    
    # In[5063]:
    
    
    
    
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
    
    
    # ## Merging with User Action Data
    
    # In[5474]:
    
    
    prediction_table3 = prediction_table
    
    
    # In[5475]:


    aua = pd.read_csv(filepaths_AUA[i])


# In[5476]:


#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test/x_test_2020-06-29.csv")


# In[5477]:


    
    # In[5478]:
    
    
    aua = aua[~((aua['LastPerformedAction']==0) & (aua['ViewData.Status']=='SDB'))]
    aua = aua.reset_index()
    aua = aua.drop('index',1)
    
    
    # In[5479]:
    
    
    
    # In[5480]:
    
    
    aua = aua[aua['ViewData.Status'].isin(['UMR','UMT','OB','SDB','UCB'])]
    aua = aua.reset_index()
    aua = aua.drop('index',1)
    
    
    # In[5481]:
    
    
    
    
    # In[5483]:
    
    if 'MetaData.0._ParentID' in aua.columns:
        aua_id_match = aua[['MetaData.0._ParentID','ViewData.Status','ViewData.Age','ViewData.BreakID','ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]
        print('MetaData.0._ParentID is present')
    else:
        aua_id_match = aua[['ViewData.Status','ViewData.Age','ViewData.BreakID','ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]
        aua_id_match['MetaData.0._ParentID'] = np.nan
        print('MetaData.0._ParentID is absent')
    # Set the order of columns
    aua_id_match['MetaData.0._ParentID'] = aua_id_match['MetaData.0._ParentID'].astype(str)
    aua_id_match = aua_id_match[['MetaData.0._ParentID','ViewData.Status','ViewData.Age','ViewData.BreakID','ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]
       
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
    
    
    # In[5484]:
    
    
    
    
    # In[5485]:
    
    
    aua_open_status['SideB.ViewData.BreakID_B_side'] = aua_open_status['SideB.ViewData.BreakID_B_side'].astype(int).astype(str)
    prediction_table3['SideB.ViewData.BreakID_B_side'] = prediction_table3['SideB.ViewData.BreakID_B_side'].astype(int).astype(str)
    
    
    # In[5486]:
    
    
    
    # In[5487]:
    
    
    prediction_table3['SideB.ViewData._ID'] = prediction_table3['SideB.ViewData._ID'].fillna('Not_generated')
    prediction_table3['SideA.ViewData._ID'] = prediction_table3['SideA.ViewData._ID'].fillna('Not_generated')
    
    
    # In[5488]:
    
    
    
    
    # In[5489]:
    
    
    #aua_id_match['len_side0'] = aua_id_match.apply(lambda x: len(x['Actual_Status'].split(',')), axis=1)
    #aua_id_match['len_side1'] = aua_id_match.apply(lambda x: len(x['Actual_Status'].split(',')), axis=1)
    
    
    # In[5490]:
    
    
    #aua_one_side = aua_id_match.groupby(['ViewData.Side1_UniqueIds'])['Actual_Status'].unique().reset_index()
    #aua_zero_side = aua_id_match.groupby(['ViewData.Side0_UniqueIds'])['Actual_Status'].unique().reset_index()
    
    
    # In[5491]:
    
    
    aua_id_match['combined_flag'] = aua_id_match.apply(lambda x: 1 if 'Combined' in x['AUA_ViewData._ID'] else 0,axis=1)
    
    
    # In[5492]:
    
    
    #aua_id_match[''.sort_values(['ViewData.Side0_UniqueIds'])
    
    
    # In[5493]:
    
    
    aua_id_match1  = aua_id_match[aua_id_match['combined_flag']!=1]
    aua_id_match1 = aua_id_match1.reset_index()
    aua_id_match1 = aua_id_match1.drop('index',1)
    
    
    # In[5494]:
    
    
    side1_repeat = aua_id_match1['ViewData.Side1_UniqueIds'].value_counts().reset_index()
    side0_repeat = aua_id_match1['ViewData.Side0_UniqueIds'].value_counts().reset_index()
    
    
    # In[5495]:
    
    
    
    
    # In[5497]:
    
    
    aua_id_match1['1_repeat_flag'] = aua_id_match1.apply(lambda x: 1 if x['ViewData.Side1_UniqueIds'] in side1_repeat[side1_repeat['ViewData.Side1_UniqueIds']>1]['index'].values else 0, axis=1)
    aua_id_match1['0_repeat_flag'] = aua_id_match1.apply(lambda x: 1 if x['ViewData.Side0_UniqueIds'] in side0_repeat[side0_repeat['ViewData.Side0_UniqueIds']>1]['index'].values else 0, axis=1)
    
    
    # In[5498]:
    
    
    aua_id_match2 = aua_id_match1[~((aua_id_match1['1_repeat_flag']==1) & (aua_id_match1['Actual_Status']=='OB'))]
    aua_id_match2 = aua_id_match2.reset_index()
    aua_id_match2 = aua_id_match2.drop('index',1)
    
    
    # In[5499]:
    
    
    aua_id_match3 = aua_id_match2[~((aua_id_match2['0_repeat_flag']==1) & (aua_id_match2['Actual_Status']=='OB'))]
    aua_id_match3 = aua_id_match3.reset_index()
    aua_id_match3 = aua_id_match3.drop('index',1)
    
    
    # In[5500]:
    
    
    
    #aua_zero_side['len_side0'].value_counts()
    #aua_open_status['SideB.ViewData.BreakID_B_side'].nunique()
    
    
    # In[5501]:
    
    
    #aua_sub99[aua_sub99['ViewData.Side0_UniqueIds'] == '789_125897734_Advent Geneva']
    
    
    # In[5502]:
    
    
    
    
    # In[5505]:
    
    
    pb_side = X_test.groupby('SideA.ViewData.BreakID_A_side')['Predicted_action'].unique().reset_index()
    
    
    # In[5506]:
    
    
    pb_side['len'] = pb_side['Predicted_action'].apply(lambda x: len(x))
    
    
    # In[5507]:
    
    
    pb_side['No_Pair_flag'] = pb_side.apply(lambda x: 1 if 'No-Pair' in x['Predicted_action'] else 0, axis=1)
    
    
    # In[5508]:
    
    
    pb_side_open_ids = pb_side[(pb_side['len']==1) & (pb_side['No_Pair_flag']==1)]['SideA.ViewData.BreakID_A_side']
    
    
    # In[ ]:
    
    
    
    
    
    # In[5509]:
    
    
    
    # In[5510]:
    
    
    
    
    # In[5511]:
    
    
    prediction_table_new = pd.merge(prediction_table3, aua_id_match3, on='SideB.ViewData._ID', how='left')
    
    
    # In[5512]:
    
    
    
    
    # In[ ]:
    
    
    
    
    
    # In[5514]:
    
    
    aua_id_match4 = aua_id_match3.rename(columns = {'ViewData.BreakID': 'SideB.ViewData.BreakID_B_side'})
    aua_id_match4 = aua_id_match4.rename(columns = {'Actual_Status': 'Actual_Status_Open'})
    
    
    # In[5515]:
    
    
    aua_id_match4['SideB.ViewData.BreakID_B_side'] = aua_id_match4['SideB.ViewData.BreakID_B_side'].astype(str)
    
    
    # In[5516]:
    
    
    #prediction_table_new = pd.merge(prediction_table_new ,aua_open_status, on='SideB.ViewData.BreakID_B_side', how='left')
    prediction_table_new = pd.merge(prediction_table_new ,aua_id_match4[['SideB.ViewData.BreakID_B_side','Actual_Status_Open']], on='SideB.ViewData.BreakID_B_side', how='left')
    
    
    # In[5517]:
    
    
    #prediction_table_new
    
    
    # In[5518]:
    
    
    
    # In[5519]:
    
    
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='No-Pair/Unrecognized','Final_prediction'] = 'No-Pair'
    
    
    # In[5520]:
    
    
    prediction_table_new.loc[prediction_table_new['Actual_Status'].isnull()]
    
    
    # In[5521]:
    
    
    prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),'Actual_Status'] = prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),'Actual_Status_Open']
    
    
    # In[5522]:
    
    
    prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),:]
    
    
    # In[5523]:
    
    
    
    # In[5524]:
    
    
    prediction_table_new.loc[prediction_table_new['Actual_Status']=='OB','Actual_Status'] = 'Open Break'
    
    
    # In[5525]:
    
    
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='No-Pair','Final_prediction'] = 'Open Break'
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMR_One_to_One','Final_prediction'] = 'UMR'
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMT_One_to_One','Final_prediction'] = 'UMT'
#    prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMB_One_to_One','Final_prediction'] = 'UMB'
    
    
    # In[5526]:
    
    
    # In[5527]:
    
    
    prediction_table_new = prediction_table_new[~prediction_table_new['Actual_Status'].isnull()]
    prediction_table_new = prediction_table_new.reset_index()
    prediction_table_new = prediction_table_new.drop('index',1)
    
    
    # ## Final Actual vs Predicted Table - Process Initiation
    
    # In[5528]:
    
    
    meo = pd.read_csv(filepaths_MEO[i],usecols=new_cols)
    
    
    # In[5529]:
    
    
    meo = meo[['ViewData.BreakID','ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Age','ViewData.Status']].drop_duplicates()
    
    
    # In[5530]:
    
    
    meo['key'] = meo['ViewData.Side0_UniqueIds'].astype(str) + meo['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5531]:
    
    
    aua_id_match5 = aua_id_match3.rename(columns ={'Actual_Status': 'ViewData.Status'})
    
    
    # In[5532]:
    
    
    aua_sub = aua_id_match5[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Age','ViewData.Status']].drop_duplicates()
    
    
    # In[5533]:
    
    
    aua_sub['key'] = aua_sub['ViewData.Side0_UniqueIds'].astype(str) + aua_sub['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5534]:
    
    
    prediction_table_new['ViewData.BreakID'] = prediction_table_new['SideB.ViewData.BreakID_B_side']
    prediction_table_new['ViewData.BreakID'] = prediction_table_new['ViewData.BreakID'].astype(str)
    
    
    # In[5535]:
    
    
    meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(str)
    
    
    # In[5536]:
    
    
    prediction_table_new1 = pd.merge(prediction_table_new, meo[['ViewData.BreakID','key']], on='ViewData.BreakID', how='left')
    
    
    # In[5537]:
    
    
    
    # In[5538]:
    
    
    
    # In[5539]:
    
    
    aua_sub1 = pd.merge(aua_sub, prediction_table_new1[['key','Final_prediction','probability_UMR','probability_No_pair','probability_UMT','Final_predicted_break']], on='key', how='left')
    
    
    # In[5540]:
    
    
    
    # In[5541]:
    
    
    no_open = prediction_table_new1[prediction_table_new1['Final_prediction']!='Open Break'].reset_index()
    no_open = no_open.drop('index',1)
    
    no_open['key'] = no_open['ViewData.Side0_UniqueIds'].astype(str) + no_open['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5542]:
    
    
    #aua_sub1[aua_sub1['Final_prediction']=='UMR_One_to_One']
    X_test['key'] = X_test['SideB.ViewData.Side0_UniqueIds'].astype(str) + X_test['SideA.ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5543]:
    
    
    
    
    # In[5544]:
    
    
    aua_sub = pd.merge(aua_sub1, no_open[['key','Final_prediction']], on='key', how='left')
    
    
    # In[5545]:
    
    
    aua_sub11 = aua_sub1[aua_sub1['Final_prediction']=='Open Break']
    aua_sub11 = aua_sub11.reset_index()
    aua_sub11 = aua_sub11.drop('index',1)
    
    
    # In[5546]:
    
    
    aua_sub11['probability_UMR'].fillna(0.00455,inplace=True)
#    aua_sub11['probability_UMB'].fillna(0.003124,inplace=True)
    aua_sub11['probability_UMT'].fillna(0.00355,inplace=True)
    aua_sub11['probability_No_pair'].fillna(0.99034,inplace=True)
    
    
    # In[5547]:
    
    
    aua_sub22 = aua_sub1[aua_sub1['Final_prediction']!='Open Break'][['ViewData.Side1_UniqueIds', 'ViewData.Side0_UniqueIds', 'ViewData.Age','ViewData.Status', 'key']]
    
    aua_sub22 = aua_sub22.reset_index()
    aua_sub22 = aua_sub22.drop('index',1)
    aua_sub22 = pd.merge(aua_sub22, no_open[['key','Final_prediction','probability_UMR','probability_No_pair','probability_UMT','Final_predicted_break']], on='key', how='left')
    aua_sub22 = aua_sub22.reset_index()
    aua_sub22 = aua_sub22.drop('index',1)
    
    
    # In[5548]:
    
    
    aua_sub33 = pd.concat([aua_sub11,aua_sub22], axis=0)
    aua_sub33 = aua_sub33.reset_index()
    aua_sub33 = aua_sub33.drop('index',1)
    
    
    # In[5549]:
    
    
    aua_sub33['ViewData.Side0_UniqueIds'] = aua_sub33['ViewData.Side0_UniqueIds'].astype(str)
    aua_sub33['ViewData.Side1_UniqueIds'] = aua_sub33['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5550]:
    
    
    aua_sub33['len_side0'] = aua_sub33.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
    aua_sub33['len_side1'] = aua_sub33.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
    
    
    # In[5551]:
    
    
    aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']==1) & (aua_sub33['ViewData.Status']=='OB') ,'Type'] = 'One_side_aggregation'
    aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']==1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'One_to_Many'
    
    aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']=='OB') ,'Type'] = 'One_side_aggregation'
    aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'One_to_Many'
    aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'Many_to_Many'
    
    aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']==1) ,'Type'] = 'One_to_One/Open'
    
    
    # In[5552]:
    
    
    #aua_sub44 = aua_sub33[(aua_sub33['ViewData.Status']=='UMB') & (aua_sub33['ViewData.Age']>1)]
    #aua_sub44 = aua_sub33
    #aua_sub44 = aua_sub44.reset_index()
    #aua_sub44 = aua_sub44.drop('index',1)
    
    
    # In[5553]:
    
    
    #aua_sub44['Final_prediction'].fillna('UMB-Carry-Forward',inplace= True)
    #aua_sub44['probability_UMR'].fillna(0.0001,inplace= True)
    #aua_sub44['probability_UMB'].fillna(0.9998,inplace= True)
    #aua_sub44['probability_UMT'].fillna(0.0000,inplace= True)
    #aua_sub44['probability_No_pair'].fillna(0.0000,inplace= True)
    
    
    # In[5554]:
    
    
    #aua_sub55 = aua_sub33[~((aua_sub33['ViewData.Status']=='UMB') & (aua_sub33['ViewData.Age']>1))]
    #aua_sub55 = aua_sub33
    #aua_sub55 = aua_sub55.reset_index()
    #aua_sub55 = aua_sub55.drop('index',1)
    
    
    # In[5555]:
    
    
    #aua_sub66 = pd.concat([aua_sub55,aua_sub44], axis=0)
    aua_sub66 = aua_sub33
    aua_sub66 = aua_sub66.reset_index()
    aua_sub66 = aua_sub66.drop('index',1)
    
    
    # In[5556]:
    
    
    #aua_sub66.loc[(aua_sub66['ViewData.Status']=='UMB') & (aua_sub66['ViewData.Age']>1),'ViewData.Status'] = 'UMB-Carry-Forward'
    aua_sub66.loc[(aua_sub66['ViewData.Status']=='OB'),'ViewData.Status'] = 'Open Break'
    
    
    # In[5557]:
    
    
    
    # In[ ]:
    
    
    
    
    
    # ## Read No-Pair Id File
    
    # In[5558]:


    #no_pair_id_data = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/X_Test/no_pair_ids_125_2020-06-8.csv")


# In[5559]:


#    no_pair_ids = no_pair_ids_df['filter_key'].unique()
    
    
    # In[5560]:
    
    
    aua_sub66.loc[aua_sub66['ViewData.Side1_UniqueIds'].isin(no_pair_ids),'Final_prediction'] = aua_sub66.loc[aua_sub66['ViewData.Side1_UniqueIds'].isin(no_pair_ids),'ViewData.Status']
    aua_sub66.loc[aua_sub66['ViewData.Side0_UniqueIds'].isin(no_pair_ids),'Final_prediction'] = aua_sub66.loc[aua_sub66['ViewData.Side0_UniqueIds'].isin(no_pair_ids),'ViewData.Status']
    
    
    # In[5561]:
    
    
    
    
    # In[5672]:
    
    
    #aua_sub66
    
    
    # In[5563]:
    
    
    pb_side_grp = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    
    # In[5564]:
    
    
    
    
    # In[5565]:
    
    
    pb_side_grp_status = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideA.ViewData.Status'].unique().reset_index()
    pb_side_grp_status['SideA.ViewData.Status'] = pb_side_grp_status['SideA.ViewData.Status'].apply(lambda x: str(x).replace("[",""))
    pb_side_grp_status['SideA.ViewData.Status'] = pb_side_grp_status['SideA.ViewData.Status'].apply(lambda x: str(x).replace("]",""))
    pb_side_grp['len'] = pb_side_grp.apply(lambda x: len(x['Predicted_action']), axis=1)
    pb_side_grp['No_pair_flag'] = pb_side_grp.apply(lambda x: 1 if x['len'] == 1 and "No-Pair" in x['Predicted_action'] else 0, axis=1)
    
    
    # In[5566]:
    
    
    pb_side_grp = pd.merge(pb_side_grp,pb_side_grp_status, on='SideA.ViewData.Side1_UniqueIds', how='left')
    
    
    # In[5567]:
    
    
    #pb_side_grp['SideA.ViewData.Status'].value_counts()
    
    
    # In[5568]:
    
    
    #pb_side_grp = pd.merge(pb_side_grp,pb_side_grp_status, on='SideA.ViewData.Side1_UniqueIds', how='left')
    pb_side_grp['Final_status'] = pb_side_grp.apply(lambda x: "Open Break" if x['SideA.ViewData.Status']=="'OB'" else("SDB" if x['SideA.ViewData.Status']=="'SDB'" else "NA"),axis=1)
    pb_side_grp = pb_side_grp.rename(columns = {'SideA.ViewData.Side1_UniqueIds':'ViewData.Side1_UniqueIds'})
    
    
    
    pb_side_grp1 = pb_side_grp[pb_side_grp['No_pair_flag']==1]
    pb_side_grp1 = pb_side_grp1.reset_index()
    pb_side_grp1 = pb_side_grp1.drop('index',1)
    
    
    # In[5569]:
    
    
    aua_sub77 = pd.merge(aua_sub66 ,pb_side_grp1[['ViewData.Side1_UniqueIds','Final_status']], on ='ViewData.Side1_UniqueIds',how='left')
    
    
    # In[5570]:
    
    
    aua_sub77.loc[(~aua_sub77['Final_status'].isnull()) & (aua_sub77['ViewData.Side0_UniqueIds']=='nan'),'Final_prediction'] = aua_sub77.loc[(~aua_sub77['Final_status'].isnull()) & (aua_sub77['ViewData.Side0_UniqueIds']=='nan'),'Final_status']
    
    
    # In[5571]:
    
    
    pb_side_grp_B = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    
    # In[5572]:
    
    
    pb_side_grp_B_status = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['SideB.ViewData.Status'].unique().reset_index()
    pb_side_grp_B_status['SideB.ViewData.Status'] = pb_side_grp_B_status['SideB.ViewData.Status'].apply(lambda x: str(x).replace("[",""))
    pb_side_grp_B_status['SideB.ViewData.Status'] = pb_side_grp_B_status['SideB.ViewData.Status'].apply(lambda x: str(x).replace("]",""))
    pb_side_grp_B['len'] = pb_side_grp_B.apply(lambda x: len(x['Predicted_action']), axis=1)
    pb_side_grp_B['No_pair_flag'] = pb_side_grp_B.apply(lambda x: 1 if x['len'] == 1 and "No-Pair" in x['Predicted_action'] else 0, axis=1)
    
    
    # In[5573]:
    
    
    pb_side_grp_B = pd.merge(pb_side_grp_B,pb_side_grp_B_status, on='SideB.ViewData.Side0_UniqueIds', how='left')
    pb_side_grp_B['Final_status_B'] = pb_side_grp_B.apply(lambda x: "Open Break" if x['SideB.ViewData.Status']=="'OB'" else("SDB" if x['SideB.ViewData.Status']=="'SDB'" else "NA"),axis=1)
    pb_side_grp_B = pb_side_grp_B.rename(columns = {'SideB.ViewData.Side0_UniqueIds':'ViewData.Side0_UniqueIds'})
    
    
    
    pb_side_grp2 = pb_side_grp_B[pb_side_grp_B['No_pair_flag']==1]
    pb_side_grp2 = pb_side_grp2.reset_index()
    pb_side_grp2 = pb_side_grp2.drop('index',1)
    
    
    # In[5574]:
    
    
    aua_sub88 = pd.merge(aua_sub77 ,pb_side_grp2[['ViewData.Side0_UniqueIds','Final_status_B']], on ='ViewData.Side0_UniqueIds',how='left')
    
    
    # In[5575]:
    
    
    aua_sub88.loc[(~aua_sub88['Final_status_B'].isnull()) & (aua_sub88['ViewData.Side1_UniqueIds']=='nan'),'Final_prediction'] = aua_sub88.loc[(~aua_sub88['Final_status_B'].isnull()) & (aua_sub88['ViewData.Side1_UniqueIds']=='nan'),'Final_status_B']
    
    
    # In[5576]:
    
    
    aua_sub99 = aua_sub88[(aua_sub88['ViewData.Status']!='SDB')]
    aua_sub99 = aua_sub99.reset_index()
    aua_sub99 = aua_sub99.drop('index',1)
    
    
    # In[5577]:
    
    
    aua_sub99['Final_prediction'] = aua_sub99['Final_prediction'].fillna('Open Break')
    aua_sub99 = aua_sub99.reset_index()
    aua_sub99 = aua_sub99.drop('index',1)
    
    
    # In[5578]:
    
    
    aua_sub99['ViewData.Status'] = aua_sub99['ViewData.Status'].astype(str)
    aua_sub99['Final_prediction'] = aua_sub99['Final_prediction'].astype(str)
    
    
    # In[5579]:
    
    
    #X_test
    
    # In[5580]:
    
    
    #aua[aua['ViewData.Side0_UniqueIds'] == '789_125897734_Advent Geneva']
    
    
    # ## Summary file 
    
    # In[5581]:
    
    
    break_id_merge = meo[meo['ViewData.Status'].isin(['OB','SDB','UOB','UDB','SPM'])][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID']].drop_duplicates()
    break_id_merge = break_id_merge.reset_index()
    break_id_merge = break_id_merge.drop('index',1)
    
    
    # In[5582]:
    
    
    
    # In[5583]:
    
    
    break_id_merge['key'] = break_id_merge['ViewData.Side0_UniqueIds'].astype(str) + break_id_merge['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5584]:
    
    
    final = pd.merge(aua_sub99,break_id_merge[['key','ViewData.BreakID']], on='key', how='left')
    
    
    # In[5585]:
    
    

# In[5586]:




# In[5587]:


    
    
    # In[5588]:
    
    
    #final[final['ViewData.BreakID'].isnull()]
    
    final = pd.merge(final,break_id_merge[['ViewData.Side0_UniqueIds','ViewData.BreakID']], on='ViewData.Side0_UniqueIds', how='left')
    
    
    # In[5589]:
    
    
    final.loc[final['ViewData.BreakID_x'].isnull(),'ViewData.BreakID_x'] = final.loc[final['ViewData.BreakID_x'].isnull(),'ViewData.BreakID_y']
    
    
    # In[5590]:
    
    
    final = final.rename(columns={'ViewData.BreakID_x':'ViewData.BreakID'})
    final = final.drop('ViewData.BreakID_y',1)
    
    
    # In[ ]:
    
    
    
    
    
    # In[5591]:
    
    
    final1 = final[(final['Type']=='One_to_One/Open') & (final['probability_No_pair'].isnull())]
    final1 = final1.reset_index()
    final1 = final1.drop('index',1)
    
    
    final2 = final[~((final['Type']=='One_to_One/Open') & (final['probability_No_pair'].isnull()))]
    final2 = final2.reset_index()
    final2 = final2.drop('index',1)
    
    
    # In[5592]:
    
    
    final1['probability_UMR'].fillna(0.0044,inplace=True)
    #final1['probability_UMB'].fillna(0.004124,inplace=True)
    final1['probability_UMT'].fillna(0.00355,inplace=True)
    final1['probability_No_pair'].fillna(0.9922,inplace=True)
    
    
    # In[5593]:
    
    
    final3 = pd.concat([final1, final2], axis=0)
    
    
    # In[5594]:
    
    
    final3['ML_flag'] = final3.apply(lambda x: "ML" if x['Type']=='One_to_One/Open' else "Non-ML", axis=1)
    
    
    # In[5595]:
    
    
    prediction_cols = ['ViewData.BreakID', 'ViewData.Side1_UniqueIds', 'ViewData.Side0_UniqueIds','ViewData.Age' ,
           'probability_No_pair', 'probability_UMR', 'probability_UMT',
           'Final_predicted_break', 'Type', 'ML_flag','ViewData.Status', 'Final_prediction']
    
    
    final4 = final3[prediction_cols]
    
    final4 = final4.rename(columns ={'ViewData.Status':'Actual_Status', 'Final_prediction': 'Predicted_Status'})
    
    
    # In[5596]:
    
    
    
    # In[5597]:
    
    
    #crosstab_table
    
    
    # In[5598]:
    
    
    NA_status_file = final4[(final4['Type']=='One_to_One/Open') & (final4['Predicted_Status']=='NA')]
    NA_status_file = NA_status_file.reset_index()
    NA_status_file = NA_status_file.drop('index',1)
    
    
    # In[5599]:
    
    
    final5 = final4[~((final4['Type']=='One_to_One/Open') & (final4['Predicted_Status']=='NA'))]
    final5 = final5.reset_index()
    final5 = final5.drop('index',1)
    
    
    # In[5600]:
    
    
    NA_status_file_A_side = NA_status_file[NA_status_file['ViewData.Side0_UniqueIds']=='nan']
    NA_status_file_B_side = NA_status_file[NA_status_file['ViewData.Side1_UniqueIds']=='nan']
    
    
    # In[5601]:
    
    
    gg = X_test[X_test['SideA.ViewData.BreakID_A_side'].isin(NA_status_file_A_side['ViewData.BreakID'].unique())].groupby(['SideA.ViewData.BreakID_A_side'])['Predicted_action'].unique().reset_index()
    gg.columns = ['ViewData.BreakID','Predicted_action']
    gg['NA_prediction_A'] = 'Open Break'
    
    kk = X_test[X_test['SideB.ViewData.BreakID_B_side'].isin(NA_status_file_B_side['ViewData.BreakID'].unique())].groupby(['SideB.ViewData.BreakID_B_side'])['Predicted_action'].unique().reset_index()
    kk.columns = ['ViewData.BreakID','Predicted_action']
    kk['NA_prediction_B'] = 'Open Break'
    
    
    # In[5602]:
    
    
    gg['ViewData.BreakID'] = gg['ViewData.BreakID'].astype(str)
    kk['ViewData.BreakID'] = kk['ViewData.BreakID'].astype(str)
    
    
    # In[5603]:
    
    
    final6 = pd.merge(NA_status_file, gg[['ViewData.BreakID','NA_prediction_A']], on='ViewData.BreakID', how='left')
    final6 = pd.merge(final6, kk[['ViewData.BreakID','NA_prediction_B']], on='ViewData.BreakID', how='left')
    
    
    # In[5604]:
    
    
    final6.loc[final6['NA_prediction_A'].isnull(),'Predicted_Status'] = 'Open Break'
    final6.loc[final6['NA_prediction_B'].isnull(),'Predicted_Status'] = 'Open Break'
    
    
    # In[5605]:
    
    
    final6 = final6.drop(['NA_prediction_A','NA_prediction_B'],1)
    
    
    # In[5606]:
    
    
    #final5[final5['ViewData.Side0_UniqueIds']=='789_125897734_Advent Geneva']
    
    
    # In[5607]:
    
    
    final7 = pd.concat([final5, final6], axis=0)
    final7 = final7.reset_index()
    final7 = final7.drop('index',1)
    
    
    # In[5609]:
    
    
    # In[5610]:
    
    
    pair_match = X_test[X_test['Predicted_action']!='No-Pair']
    pair_match = pair_match.reset_index()
    pair_match = pair_match.drop('index',1)


# In[5611]:


    pair_match = pair_match[['Predicted_action',
           'probability_No_pair', 'probability_UMR',
           'probability_UMT', 'key']]
    pair_match.columns = ['New_Predicted_action',
           'New_probability_No_pair', 'New_probability_UMR',
           'New_probability_UMT','key']
    
    
    # In[5612]:
    
    
    pair_match['New_Predicted_action'] = pair_match['New_Predicted_action'].apply(lambda x: 'UMR' if x=='UMR_One_to_One' else "UMT")
    
    
    # In[5613]:
    
    
    final7['key'] = final7['ViewData.Side0_UniqueIds'].astype(str) + final7['ViewData.Side1_UniqueIds'].astype(str)
    
    
    # In[5614]:
    
    
    final8 = pd.merge(final7,pair_match, on='key', how='left')
    

# In[5615]:



# In[5617]:



# In[5618]:

    
    final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'Predicted_Status'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_Predicted_action']
    final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_No_pair'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_No_pair']
    #final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_UMB'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_UMB']
    final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_UMR'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_UMR']
    final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'probability_UMT'] = final8.loc[(~final8['New_Predicted_action'].isnull()) & (final8['New_Predicted_action']!= final8['Predicted_Status']),'New_probability_UMT']
    
    
    # In[5619]:
    
    
    final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'probability_UMT'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'New_probability_UMT']
    final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'probability_UMR'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'New_probability_UMR']
#    final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'probability_UMB'] = final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'New_probability_UMB']
    
    final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'probability_No_pair'] = 0.003
    final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'probability_No_pair'] = 0.003
#    final8.loc[(final8['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'probability_No_pair'] = 0.002

    # In[5620]:
    
    
    umr_break_array_match = prediction_table[prediction_table['Final_prediction']=='UMR_One_to_One'][['SideB.ViewData.BreakID_B_side','Final_predicted_break']]
    umt_break_array_match = prediction_table[prediction_table['Final_prediction']=='UMT_One_to_One'][['SideB.ViewData.BreakID_B_side','Final_predicted_break']]
    
#    umb_break_array_match = prediction_table[prediction_table['Final_prediction']=='UMB_One_to_One'][['SideB.ViewData.BreakID_B_side','Final_predicted_break']]
    
    umr_break_array_match.columns = np.array(['ViewData.BreakID','New_Final_predicted_break_UMR'])
    umt_break_array_match.columns = np.array(['ViewData.BreakID','New_Final_predicted_break_UMT'])
#    umb_break_array_match.columns = np.array(['ViewData.BreakID','New_Final_predicted_break_UMB'])
    
    
    # In[5621]:
    
    
    #umr_break_array_match['New_Final_predicted_break_UMR'] = umr_break_array_match['New_Final_predicted_break_UMR'].astype(str) 
    #umb_break_array_match['New_Final_predicted_break_UMB'] = umb_break_array_match['New_Final_predicted_break_UMB'].astype(str) 
    #umt_break_array_match['New_Final_predicted_break_UMT'] = umt_break_array_match['New_Final_predicted_break_UMT'].astype(str) 
    
    
    # In[5622]:
    
    
    final9 = pd.merge(final8, umr_break_array_match, on ='ViewData.BreakID', how='left')
    final9 = pd.merge(final9, umt_break_array_match, on ='ViewData.BreakID', how='left')
#    final9 = pd.merge(final9, umb_break_array_match, on ='ViewData.BreakID', how='left')
    
    
    # In[5623]:
    
    
    final9.loc[(final9['Final_predicted_break'].isnull()) & (final9['Predicted_Status']=='UMT'),'Final_predicted_break'] = final9.loc[(final9['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMT'),'New_Final_predicted_break_UMT']
    final9.loc[(final9['Final_predicted_break'].isnull()) & (final9['Predicted_Status']=='UMR'),'Final_predicted_break'] = final9.loc[(final9['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMR'),'New_Final_predicted_break_UMR']
#    final9.loc[(final9['Final_predicted_break'].isnull()) & (final9['Predicted_Status']=='UMB'),'Final_predicted_break'] = final9.loc[(final9['Final_predicted_break'].isnull()) & (final8['Predicted_Status']=='UMB'),'New_Final_predicted_break_UMB']
    
    
    # In[5624]:
    
    
    #final9[(final9['Actual_Status']=='UMB') & (final9['Predicted_Status']=='UMB') & (final9['ML_flag']=='ML')]['Final_predicted_break']
    
    
    # In[5625]:
    
    
    
    
    # In[5626]:
    
    
    final9 = final9.drop(['key','New_Predicted_action',
           'New_probability_No_pair', 'New_probability_UMR',
           'New_probability_UMT','New_Final_predicted_break_UMR',
           'New_Final_predicted_break_UMT'], 1)
    
    
    
    # In[5627]:





# In[5628]:


#final8['Type'].value_counts()


# In[5629]:


#meo1 = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_125_2020-06-8.csv",usecols=new_cols)


# In[5630]:


#meo1[meo1['ViewData.Side1_UniqueIds']=='6_125858636_Goldman Sachs']


# ## Merging columns from the transaction table

# In[5631]:




# In[5632]:


    aua_final = pd.read_csv(filepaths_AUA[i],usecols = viewdata_cols_to_show)
    
    aua_final = aua_final.rename(columns={'ViewData.PMSVendor Net Amount':'ViewData.Accounting Net Amount'})
    aua_final = aua_final.rename(columns={'ViewData.B-P Net Amount':'ViewData.B-P Net Amount'})


# In[5633]:


#final_predictions = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Final_Predictions_Table_HST_RecData_125_2020-06-1.csv')


# In[5634]:


    final_predictions = final9.copy()


# In[5635]:


#final_predictions.groupby(['Actual_Status'])['Predicted_Status'].value_counts()


# In[5636]:


#final_predictions[(final_predictions['Actual_Status'] == 'Open Break') & (final_predictions['Predicted_Status'] == 'UMR')][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]


# In[5637]:


#final_predictions.groupby(['ViewData.Side0_UniqueIds'])['ViewData.Side1_UniqueIds'].value_counts()


# In[5638]:


# In[5639]:


    final_predictions_both_present = final_predictions[(final_predictions['ViewData.Side0_UniqueIds'] !='nan') & (final_predictions['ViewData.Side1_UniqueIds']!='nan')]
    final_predictions_side0_only = final_predictions[(final_predictions['ViewData.Side0_UniqueIds']!='nan') & (final_predictions['ViewData.Side1_UniqueIds'] =='nan')]
    final_predictions_side1_only = final_predictions[(final_predictions['ViewData.Side0_UniqueIds']=='nan') & (final_predictions['ViewData.Side1_UniqueIds'] != 'nan')]
    final_predictions_both_null = final_predictions[(final_predictions['ViewData.Side0_UniqueIds']=='nan') & (final_predictions['ViewData.Side1_UniqueIds']=='nan')]
    
    
    # In[5640]:
    



    aua_final = aua_final.drop_duplicates()
    aua_final = aua_final.reset_index()
    aua_final = aua_final.drop('index',1)
    
    
    # In[5642]:
    
    
    final_predictions_both_present_aua_merge = pd.merge(final_predictions_both_present,aua_final, on=['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds'], how='left' )
    final_predictions_side0_only_aua_merge = pd.merge(final_predictions_side0_only,aua_final, on='ViewData.Side0_UniqueIds', how='left' )
    final_predictions_side1_only_aua_merge = pd.merge(final_predictions_side1_only,aua_final, on='ViewData.Side1_UniqueIds', how='left' )


# In[5643]:


#final_predictions_side1_only_aua_merge


# In[5644]:


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
                                                                                                       
                                                                                                                                                                                             


# In[5645]:


#final_prediction_show_cols = final_predictions_both_present_aua_merge.append([final_predictions_side0_only_aua_merge,final_predictions_side1_only_aua_merge])


# In[5655]:


    final11 = pd.concat([final_predictions_both_present_aua_merge, final_predictions_side0_only_aua_merge,final_predictions_side1_only_aua_merge], axis=0)
    
    
    # In[5656]:
    
    
    final11 = final11.reset_index()
    final11 = final11.drop('index',1)
    
    
    # In[5657]:
    
    
    final12 = final11.drop_duplicates(['ViewData.BreakID', 'ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds', 'ViewData.Age'])
    

# In[5658]:


    final12.loc[(final12['Actual_Status']=='UCB'), 'ML_flag'] ='Non-ML'
    final12.loc[(final12['Actual_Status']=='UCB'), 'Type'] = 'Closed Breaks'
    
    
    # In[5673]:
    
    
    final12.loc[final12['Actual_Status']=='UCB','Predicted_Status'] = 'No-Prediction'
    
    
    # In[5246]:
    
    
    
    
    
    # In[5674]:
    
    print('classification_report')
    print(classification_report(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status']))


# In[5675]:


    report = classification_report(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status'], output_dict=True)
    accuracy_table = pd.DataFrame(report).transpose()
    
    print('accuracy_table')
    print(accuracy_table)

# In[5676]:


    crosstab_table = pd.crosstab(final12[final12['Type']=='One_to_One/Open']['Actual_Status'], final12[final12['Type']=='One_to_One/Open']['Predicted_Status'])
    
    
    # In[5678]:
    
    print('crosstab_table')
    print(crosstab_table)
    print(datetime.now())

# ## Save results

# In[ ]:

#    filepaths_final_prediction_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_2/Final_Predictions_Table_HST_RecData_125_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#    filepaths_accuracy_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_2/Accuracy_Table_HST_RecData_125_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
#    filepaths_crosstab_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions_2/Crosstab_Table_HST_RecData_125_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
    
    final12.to_csv(filepaths_final_prediction_table[i])
    
    
    # In[ ]:
    
    
    accuracy_table.to_csv(filepaths_accuracy_table[i])
    
    
    # In[ ]:
    
    
    crosstab_table.to_csv(filepaths_crosstab_table[i])
    i = i+1


sys.stdout = orig_stdout
f.close()
# ## Enitre month prediction

# In[4747]:


#all_june_data = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/All_June_predictions_123.csv')
#
#
## In[4694]:
#
#
#from sklearn.metrics import accuracy_score 
#from sklearn.metrics import classification_report
#print(classification_report(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status']))
#
#
## In[4748]:
#
#
#from sklearn.metrics import accuracy_score 
#from sklearn.metrics import classification_report
#print(classification_report(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status']))
#
#
## In[4695]:
#
#
#report_all_june = classification_report(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status'], output_dict=True)
#accuracy_table_all_june = pd.DataFrame(report_all_june).transpose()
#
#
## In[4696]:
#
#
#accuracy_table_all_june
#
#
## In[4697]:
#
#
#from sklearn.metrics import confusion_matrix
#crosstab_all_june =  pd.crosstab(all_june_data[all_june_data['Type']=='One_to_One/Open']['Actual_Status'], all_june_data[all_june_data['Type']=='One_to_One/Open']['Predicted_Status'])
#
#
## In[4698]:
#
#
#crosstab_all_june
#
#
## ## Save Results (Entire Month)
#
## In[4702]:
#
#
#accuracy_table_all_june.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Accuracy_table_all_june.csv')
#
#
## In[4703]:
#
#
#crosstab_all_june.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Crosstab_table_all_june.csv')
#
