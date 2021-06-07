#!/usr/bin/env python
# coding: utf-8

# In[379]:


import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE


# In[380]:


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


# In[381]:


new_cols = ['ViewData.' + x for x in cols] + add


# In[18]:


#df_170.shape


# ## Close Prediction Soros

# In[382]:


cols_for_closed = ['Status','Source Combination','Mapped Custodian Account','Accounting Currency','B-P Currency',                   'Transaction ID','Transaction Type','Description','Investment ID',                   'Accounting Net Amount','B-P Net Amount',                   'InternalComment2','Custodian','Fund']
cols_for_closed = ['ViewData.' + x for x in cols_for_closed]
cols_for_closed_x = [x + '_x' for x in cols_for_closed] + ['ViewData.Side0_UniqueIds_x','ViewData.Side1_UniqueIds_x']
cols_for_closed_y = [x + '_y' for x in cols_for_closed] + ['ViewData.Side0_UniqueIds_y','ViewData.Side1_UniqueIds_y']
cols_for_closed_x_y = cols_for_closed_x + cols_for_closed_y

df_153_june_25_meo = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Soros\\JuneData\\MEO\\MeoCollections_SOROS.MEO_HST_RecData_153_2020-06-24.csv')

df_153_june_25_meo = df_153_june_25_meo.drop_duplicates()
df_153_june_25_meo = df_153_june_25_meo.reset_index()
df_153_june_25_meo = df_153_june_25_meo.drop('index',1)



#df_153_june_25_meo = df_153_june_25_meo.rename(columns ={'ViewData.Cust Net Amount': 'ViewData.B-P Net Amount'})

meo = df_153_june_25_meo[~df_153_june_25_meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
meo = meo[~meo['ViewData.Status'].isnull()]
meo = meo.reset_index()
meo = meo.drop('index',1)
del df_153_june_25_meo

meo['Date'] = pd.to_datetime(meo['ViewData.Task Business Date'])
meo = meo[~meo['Date'].isnull()]
meo = meo.reset_index()
meo = meo.drop('index',1)
meo['Date'] = pd.to_datetime(meo['Date']).dt.date
meo['Date'] = meo['Date'].astype(str)

meo['ViewData.Side0_UniqueIds'] = meo['ViewData.Side0_UniqueIds'].astype(str)
meo['ViewData.Side1_UniqueIds'] = meo['ViewData.Side1_UniqueIds'].astype(str)
meo['flag_side0'] = meo.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
meo['flag_side1'] = meo.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
meo.loc[meo['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
meo.loc[meo['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
meo.loc[meo['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
meo.loc[meo['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(int)
meo = meo[meo['ViewData.BreakID']!=-1]
meo = meo.reset_index()
meo = meo.drop('index',1)

meo = meo.sort_values(by=['ViewData.Transaction ID','ViewData.Transaction Type'],ascending = False)

BP_meo = meo[(meo['flag_side1'] >= 1) & (meo['flag_side0'] == 0)]
Acct_meo = meo[(meo['flag_side1'] == 0) & (meo['flag_side0'] >= 1)]



#BP_meo = BP_meo.rename(columns={'ViewData.Currency':'ViewData.B-P Currency'})
#Acct_meo = Acct_meo.rename(columns={'ViewData.Currency':'ViewData.Accounting Currency'})
#BP_meo = BP_meo.rename(columns ={'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})

BP_meo['filter_key'] = BP_meo['ViewData.Source Combination'].astype(str) + BP_meo['ViewData.Mapped Custodian Account'].astype(str) + BP_meo['ViewData.B-P Currency'].astype(str)

Acct_meo['filter_key'] = Acct_meo['ViewData.Source Combination'].astype(str) + Acct_meo['ViewData.Mapped Custodian Account'].astype(str) + Acct_meo['ViewData.Accounting Currency'].astype(str)




# BP_meo Side M X M architecture
BP_meo_training_df =[]
for key in (list(np.unique(np.array(list(BP_meo['filter_key'].values))))):
    BP_meo_filter_slice = BP_meo[BP_meo['filter_key']==key]
    if BP_meo_filter_slice.empty == False:

        BP_meo_filter_slice = BP_meo_filter_slice.reset_index()
        BP_meo_filter_slice = BP_meo_filter_slice.drop('index', 1)


#         df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
#         df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

        BP_meo_filter_joined = pd.merge(BP_meo_filter_slice, BP_meo_filter_slice, on='filter_key')
        BP_meo_training_df.append(BP_meo_filter_joined)

BP_meo_combination_df =pd.concat(BP_meo_training_df)

JNL_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Custodian_x'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Custodian_y'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'JNL') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'JNL') &  (BP_meo_combination_df['ViewData.Transaction ID_x'].astype(str) == BP_meo_combination_df['ViewData.Transaction ID_y'].astype(str)) &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

MTM_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Custodian_x'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Custodian_y'].astype(str) == 'CS') &  (BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'MTM') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'MTM') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

Collateral_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'Collateral') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'Collateral') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

DEB_CRED_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(['DEB','CRED'])) &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(['DEB','CRED'])) &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

DEP_WDRL_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str).isin(['DEP','WDRL'])) &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str).isin(['DEP','WDRL'])) &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

Miscellaneous_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'Miscellaneous') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'Miscellaneous') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

REORG_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'REORG') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'REORG') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]




P_and_I_closed_breaks = BP_meo_combination_df[(BP_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'P&I') &  (BP_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'P&I') &  (abs(BP_meo_combination_df['ViewData.B-P Net Amount_x']).astype(str) == abs(BP_meo_combination_df['ViewData.B-P Net Amount_y']).astype(str)) &  (BP_meo_combination_df['ViewData.Side1_UniqueIds_x'].astype(str) != BP_meo_combination_df['ViewData.Side1_UniqueIds_y'].astype(str))] [cols_for_closed_x_y]

Acct_meo_training_df =[]
for key in (list(np.unique(np.array(list(Acct_meo['filter_key'].values))))):
    Acct_meo_filter_slice = Acct_meo[Acct_meo['filter_key']==key]
    if Acct_meo_filter_slice.empty == False:

        Acct_meo_filter_slice = Acct_meo_filter_slice.reset_index()
        Acct_meo_filter_slice = Acct_meo_filter_slice.drop('index', 1)


#         df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
#         df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

        Acct_meo_filter_joined = pd.merge(Acct_meo_filter_slice, Acct_meo_filter_slice, on='filter_key')
        Acct_meo_training_df.append(Acct_meo_filter_joined)

Acct_meo_combination_df =pd.concat(Acct_meo_training_df)

Transfer_closed_breaks = Acct_meo_combination_df[(Acct_meo_combination_df['ViewData.Transaction Type_x'].astype(str) == 'Transfer') &  (Acct_meo_combination_df['ViewData.Transaction Type_y'].astype(str) == 'Transfer') & # (Acct_meo_combination_df['ViewData.Transaction ID_x'].astype(str) == Acct_meo_combination_df['ViewData.Transaction ID_y'].astype(str)) & \
 (abs(Acct_meo_combination_df['ViewData.Accounting Net Amount_x']).astype(str) == abs(Acct_meo_combination_df['ViewData.Accounting Net Amount_y']).astype(str)) & \
 (Acct_meo_combination_df['ViewData.Side0_UniqueIds_x'].astype(str) != Acct_meo_combination_df['ViewData.Side0_UniqueIds_y'].astype(str))] \
[cols_for_closed_x_y]





all_closed_df = pd.concat([JNL_closed_breaks,
                MTM_closed_breaks,
                Collateral_closed_breaks,
                DEB_CRED_closed_breaks,
                DEP_WDRL_closed_breaks,
                Miscellaneous_closed_breaks,
                REORG_closed_breaks,
                Transfer_closed_breaks], axis=0)

all_closed_df

 #list(all_closed_df['ViewData.Side0_UniqueIds_y']) + list(all_closed_df['ViewData.Side1_UniqueIds_y'])

closed_x = set(
                all_closed_df['ViewData.Side0_UniqueIds_x'].astype(str) + \
                all_closed_df['ViewData.Side1_UniqueIds_x'].astype(str) 
               )

closed_y = set(all_closed_df['ViewData.Side0_UniqueIds_y'].astype(str) +                 all_closed_df['ViewData.Side1_UniqueIds_y'].astype(str) 
               )

all_closed = closed_x.union(closed_y)

## Read testing data 


# In[383]:


all_closed


# In[384]:


new_closed_keys = [i.replace('nan','') for i in all_closed]


# ## Read testing data 

# In[385]:


#MeoCollections.MEO_HST_RecData_379_2020-06-18
meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/MEO/MeoCollections_SOROS.MEO_HST_RecData_153_2020-06-24.csv",usecols=new_cols)


# In[20]:


#df['ViewData.Task Business Date']


# In[386]:


meo['ViewData.Status'].value_counts()


# In[387]:


df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)


# In[388]:


#df1[(df1['Date']=='2020-04-10') & (df1['ViewData.Side1_UniqueIds']=='996_125813417_Goldman Sachs')]
df1.shape


# In[389]:


#df1['close_key'] = df1['ViewData.Side0_UniqueIds'].astype(str) + df1['ViewData.Side1_UniqueIds'].astype(str)


# In[390]:



## Output for Closed breaks

closed_df = df1[(df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys))]


# In[391]:


closed_df.shape


# ## Machine generated output

# In[392]:


#df2 = df1[~df1['close_key'].isin(list(all_closed))]
df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]


# In[393]:


df = df2.copy()


# In[394]:


df = df.reset_index()
df = df.drop('index',1)


# In[395]:


df.shape


# In[396]:


#pd.set_option('display.max_columns', 500)


# In[397]:


df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[398]:


#df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[399]:


df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[400]:


pd.to_datetime(df['Date'])


# In[401]:


df['Date'] = pd.to_datetime(df['Date']).dt.date


# In[402]:


df['Date'] = df['Date'].astype(str)


# In[403]:


#df['ViewData.Status'].value_counts()


# In[404]:


df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB'])]
df = df.reset_index()
df = df.drop('index',1)


# In[405]:


#df1[df1['ViewData.Status']=='SMB']
df['ViewData.Status'].value_counts()


# In[406]:


df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[407]:


#df_170[(df_170['ViewData.Status']=='UMR')]


# In[408]:


df['Date'].value_counts()


# ## Sample data on one date

# In[51]:


#df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})


# In[409]:


sample = df[df['Date'] =='2020-06-25']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[410]:


smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
smb = smb.drop('index',1)


# In[411]:


smb_pb = smb.copy()
smb_acc = smb.copy()


# In[412]:


smb_pb['ViewData.Accounting Net Amount'] = np.nan
smb_pb['ViewData.Side0_UniqueIds'] = np.nan
smb_pb['ViewData.Status'] ='SMB-OB'

smb_acc['ViewData.B-P Net Amount'] = np.nan
smb_acc['ViewData.Side1_UniqueIds'] = np.nan
smb_acc['ViewData.Status'] ='SMB-OB'


# In[413]:


sample = sample[sample['ViewData.Status']!='SMB']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[414]:


sample.shape


# In[415]:


sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[416]:


#sample['ViewData.Status'].value_counts()


# In[417]:


sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


# In[418]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0


# In[419]:


#sample['ViewData.Status'].value_counts()


# In[420]:


#sample['flag_side1'].value_counts()


# In[421]:


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


# In[422]:


sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)


# In[423]:


sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[424]:


sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[425]:


#sample1['ViewData.Status'].value_counts()


# In[426]:


aa = sample1[sample1['Trans_side']=='A_side']
bb = sample1[sample1['Trans_side']=='B_side']


# In[427]:


#bb['ViewData.Source Combination'].value_counts()


# In[428]:


aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)

bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)


# In[429]:


aa = aa.reset_index()
aa = aa.drop('index', 1)
bb = bb.reset_index()
bb = bb.drop('index', 1)


# In[430]:


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


# In[431]:


bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
bb = bb.reset_index()
bb = bb.drop('index',1)


# In[432]:


bb['ViewData.Status'].value_counts()


# In[433]:


bb.shape


# ## Experiment

# In[1028]:


cc = pd.concat([aa, bb], axis=0)


# In[1058]:


cc= cc[(cc['ViewData.Side0_UniqueIds'].isin(X_test_left3['SideB.ViewData.Side0_UniqueIds'].unique())) | (cc['ViewData.Side1_UniqueIds'].isin(X_test_left3['SideA.ViewData.Side1_UniqueIds'].unique()))]


# In[1059]:


#cc = cc.sort_values(['filter_key'])
cc = cc.reset_index().drop('index',1)


# In[1060]:


cc.shape


# In[1050]:


cc[cc['ViewData.Side1_UniqueIds']=='921_153157324_CITI']


# In[1041]:


exp_cols = ['ViewData.Accounting Net Amount', 'ViewData.Asset Type Category',
'ViewData.B-P Net Amount', 'ViewData.CUSIP', 

#'ViewData.Commission',
        'ViewData.Currency', 
       'ViewData.Description', 
              # 'ViewData.ExpiryDate', 
               'ViewData.Fund',
       'ViewData.ISIN',
       'ViewData.Investment Type',
      # 'ViewData.Keys',
       #'ViewData.Mapped Custodian Account',
       #'ViewData.Net Amount Difference',
       #'ViewData.Net Amount Difference Absolute',
        #'ViewData.OTE Ticker',
       # 'ViewData.Price',
       'ViewData.Prime Broker', 'ViewData.Quantity',
       'ViewData.SEDOL', 'ViewData.SPM ID', 'ViewData.Settle Date',
       
  #  'ViewData.Strike Price',
               'Date',
       'ViewData.Ticker', 'ViewData.Trade Date',
       'ViewData.Transaction Category',
       'ViewData.Transaction Type',
 #'ViewData.Underlying Cusip',
      # 'ViewData.Underlying ISIN',
      # 'ViewData.Underlying Sedol',
 'filter_key','ViewData.Status','ViewData.BreakID',
              'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData._ID','Trans_side']


# In[1064]:


for key in cc['filter_key'].unique():
    if key =='Advent Geneva,CITIQP_LP_INT_CIPBUSD':
        subset = cc[cc['filter_key']==key]
        subset = subset.sort_values(['ViewData.Settle Date','ViewData.ISIN','ViewData.Transaction Type','Trans_side'])[exp_cols]
        for isin in subset['ViewData.ISIN'].unique():
            subset2 = subset[subset['ViewData.ISIN']==isin]   


# In[1074]:


X_test_left3[(X_test_left3['SideB.ViewData.Side0_UniqueIds']=='12_153148410_Advent Geneva')&(X_test_left3['Predicted_action_2']=='UMB_One_to_One')]


# In[1079]:


X_test[(X_test['Predicted_action']=='UMR_One_to_One')&(X_test['Settle_date_diff']>5)]


# In[ ]:





# In[1070]:


subset[subset['ViewData.ISIN']=='US22540VSC71']


# In[1069]:


subset['ViewData.ISIN'].unique()


# In[1066]:


subset2['ViewData.Accounting Net Amount'].sum()


# In[1067]:


subset2['ViewData.B-P Net Amount'].sum()


# In[ ]:





# In[ ]:





# ## Experiment Ends

# In[434]:


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


# In[435]:


#no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[436]:


#pd.DataFrame(no_pair_ids).rename


# In[437]:


len(no_pair_ids)


# In[438]:


#test_file['SideA.ViewData.Status'].value_counts()


# In[439]:


test_file = pd.concat(training_df)


# In[440]:


test_file = test_file.reset_index()
test_file = test_file.drop('index',1)


# In[441]:


test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')


# In[442]:


test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]


# In[443]:


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


# In[444]:


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


# In[445]:


#test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)


# In[446]:


#test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)


# In[447]:


test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']


# ## Description code

# In[448]:


import os


# In[449]:


os.chdir('C:\\Users\\consultant136\\ML1.0')


# In[450]:


print(os.getcwd())


# In[451]:


## TODO - Import a csv file for description category mapping

com = pd.read_csv('desc cat with naveen oaktree.csv')
#com


# In[452]:


cat_list = list(set(com['Pairing']))


# In[453]:


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


# In[454]:


#df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

test_file['SideA.desc_cat'] = test_file['SideA.ViewData.Description'].apply(lambda x : descclean(x,cat_list))
test_file['SideB.desc_cat'] = test_file['SideB.ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[455]:


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
        


# In[456]:



#df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))

test_file['SideA.desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : currcln(x))
test_file['SideB.desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : currcln(x))


# In[457]:


com = com.drop(['var','Catogery'], axis = 1)

com = com.drop_duplicates()

com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[458]:


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


# In[459]:


test_file['SideA.new_desc_cat'] = test_file['SideA.desc_cat'].apply(lambda x : catcln1(x,com))
test_file['SideB.new_desc_cat'] = test_file['SideB.desc_cat'].apply(lambda x : catcln1(x,com))


# In[460]:


comp = ['inc','stk','corp ','llc','pvt','plc']
#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)

test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[461]:


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


# In[462]:


#df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

test_file['SideA.new_desc_cat'] = test_file['SideA.new_desc_cat'].apply(lambda x : desccat(x))
test_file['SideB.new_desc_cat'] = test_file['SideB.new_desc_cat'].apply(lambda x : desccat(x))


# In[463]:


#test_file['SideB.new_desc_cat'].value_counts()


# ## Prime Broker

# In[464]:


test_file['new_pb'] = test_file['SideA.ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[465]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[466]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[467]:


test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].fillna('kkk')


# In[468]:


test_file['new_pb1'] = test_file.apply(lambda x : x['new_pb'] if x['SideA.ViewData.Prime Broker']=='kkk' else x['SideA.ViewData.Prime Broker'],axis = 1)


# In[469]:


#test_file = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/X_test_files_after_loop/meo_testing_HST_RecData_379_06_19_2020_test_file_with_ID.csv')


# In[470]:


#test_file = test_file.drop('Unnamed: 0',1)


# In[471]:


test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days

test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days


# In[472]:


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


# In[473]:



############ Fund match new ########

values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values

vec_fund_match = np.vectorize(fundmatch)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Fund'] = vec_fund_match(values_Fund_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Fund'] = vec_fund_match(values_Fund_match_B_Side)


# In[474]:


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


# In[475]:


# LOWER CASE
trans_type_A_side = [str(item).lower() for item in trans_type_A_side]
trans_type_B_side = [str(item).lower() for item in trans_type_B_side]

asset_type_cat_A_side = [str(item).lower() for item in asset_type_cat_A_side]
asset_type_cat_B_side = [str(item).lower() for item in asset_type_cat_B_side]

invest_type_A_side = [str(item).lower() for item in invest_type_A_side]
invest_type_B_side = [str(item).lower() for item in invest_type_B_side]

prime_broker_A_side = [str(item).lower() for item in prime_broker_A_side]
prime_broker_B_side = [str(item).lower() for item in prime_broker_B_side]


# In[476]:


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


# In[477]:


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


# In[478]:


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


# In[479]:



remove_nums_a_A_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_A_side]
remove_nums_a_B_side = [[item for item in sublist if not is_num(item)] for sublist in split_asset_B_side]

remove_dates_a_A_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_A_side]
remove_dates_a_B_side = [[item for item in sublist if not is_date_format(item)] for sublist in remove_nums_a_B_side]
# remove_blanks_a = [item for item in remove_dates_a if item]
# # remove_blanks_a[:10]


# # In[321]:

cleaned_asset_A_side = [' '.join(item) for item in remove_dates_a_A_side]
cleaned_asset_B_side = [' '.join(item) for item in remove_dates_a_B_side]


# In[480]:


test_file['SideA.ViewData.Transaction Type'] = cleaned_trans_types_A_side
test_file['SideB.ViewData.Transaction Type'] = cleaned_trans_types_B_side

test_file['SideA.ViewData.Investment Type'] = cleaned_invest_A_side
test_file['SideB.ViewData.Investment Type'] = cleaned_invest_B_side

test_file['SideA.ViewData.Asset Category Type'] = cleaned_asset_A_side
test_file['SideB.ViewData.Asset Category Type'] = cleaned_asset_B_side


# In[481]:


#test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[482]:


#test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[483]:


##############

values_transaction_type_match_A_Side = test_file['SideA.ViewData.Transaction Type'].values
values_transaction_type_match_B_Side = test_file['SideB.ViewData.Transaction Type'].values

vec_tt_match = np.vectorize(mhreplaced)

#test_file['ISIN_match'] = vec_(values_ISIN_A_Side,values_ISIN_B_Side)
#test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
test_file['SideA.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_A_Side)
#test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
test_file['SideB.ViewData.Transaction Type'] = vec_tt_match(values_transaction_type_match_B_Side)


# In[484]:


test_file.loc[test_file['SideA.ViewData.Transaction Type']=='int','SideA.ViewData.Transaction Type'] = 'interest'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='wires','SideA.ViewData.Transaction Type'] = 'wire'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='dividends','SideA.ViewData.Transaction Type'] = 'dividend'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='miscellaneous','SideA.ViewData.Transaction Type'] = 'misc'
test_file.loc[test_file['SideA.ViewData.Transaction Type']=='div','SideA.ViewData.Transaction Type'] = 'dividend'


# In[485]:


test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[ ]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('options','option'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqt','equity'))
test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: x.replace('eqty','equity'))


# In[486]:


test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)


# In[487]:


#train_full_new1['ViewData.Combined Transaction Type'] = train_full_new1['SideA.ViewData.Transaction Type'].astype(str) + train_full_new1['SideB.ViewData.Transaction Type'].astype(str)
test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)


# In[488]:


test_file['Combined_Investment_Type'] = test_file['SideA.ViewData.Investment Type'].astype(str) + test_file['SideB.ViewData.Investment Type'].astype(str)


# In[489]:


test_file['Combined_Asset_Type_Category'] = test_file['SideA.ViewData.Asset Category Type'].astype(str) + test_file['SideB.ViewData.Asset Category Type'].astype(str)


# In[490]:


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


# In[491]:


len(test_file['SideB.ViewData.CUSIP'].values)


# In[ ]:





# In[492]:


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


# In[493]:


#test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
#test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)


# In[494]:


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


# In[495]:


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


# In[496]:


test_file['amount_percent'] = (test_file['SideA.ViewData.B-P Net Amount']/test_file['SideB.ViewData.Accounting Net Amount']*100)


# In[497]:


test_file['SideB.ViewData.Investment Type'] = test_file['SideB.ViewData.Investment Type'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Investment Type'] = test_file['SideA.ViewData.Investment Type'].apply(lambda x: str(x).lower())


# In[498]:


test_file['SideB.ViewData.Prime Broker'] = test_file['SideB.ViewData.Prime Broker'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Prime Broker'] = test_file['SideA.ViewData.Prime Broker'].apply(lambda x: str(x).lower())


# In[499]:


test_file['SideB.ViewData.Asset Type Category'] = test_file['SideB.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())
test_file['SideA.ViewData.Asset Type Category'] = test_file['SideA.ViewData.Asset Type Category'].apply(lambda x: str(x).lower())


# In[ ]:


test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x: x.replace('cover short','covershort'))


# In[500]:


#test_file

test_file['ViewData.Combined Transaction Type'] = test_file['ViewData.Combined Transaction Type'].apply(lambda x: x.replace('jnl','journal'))


# In[501]:



#test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)


# In[502]:


#test_file.to_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/X_test_files_after_loop/meo_testing_HST_RecData_170_06-18-2020_test_file.csv")


# In[503]:





# In[504]:


trade_types_A = ['buy', 'sell', 'covershort','sellshort',
       'fx', 'fx settlement', 'sell short',
       'trade not to be reported_buy', 'covershort','ptbl','ptss', 'ptcs', 'ptcl']
trade_types_B = ['trade not to be reported_buy','buy', 'sellshort', 'sell', 'covershort',
       'spotfx', 'forwardfx',
       'trade not to be reported_sell',
       'trade not to be reported_sellshort',
       'trade not to be reported_covershort']


# In[505]:


test_file['SideA.TType'] = test_file.apply(lambda x: "Trade" if x['SideA.ViewData.Transaction Type'] in trade_types_A else "Non-Trade", axis=1)
test_file['SideB.TType'] = test_file.apply(lambda x: "Trade" if x['SideB.ViewData.Transaction Type'] in trade_types_B else "Non-Trade", axis=1)


# In[506]:


test_file['Combined_Desc'] = test_file['SideA.new_desc_cat'] + test_file['SideB.new_desc_cat']


# In[507]:


test_file['Combined_TType'] = test_file['SideA.TType'].astype(str) + test_file['SideB.TType'].astype(str)


# In[508]:


from fuzzywuzzy import fuzz


# In[509]:


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


# In[510]:


test_file =  clean_text(test_file,'SideA.ViewData.Description', 'SideA.ViewData.Description_new') 
test_file =  clean_text(test_file,'SideB.ViewData.Description', 'SideB.ViewData.Description_new') 


# In[511]:


test_file['description_similarity_score'] = test_file.apply(lambda x: fuzz.token_sort_ratio(x['SideA.ViewData.Description_new'], x['SideB.ViewData.Description_new']), axis=1)


# In[512]:


import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
import numpy as np

#le = LabelEncoder()
for feature in ['SideA.Date','SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date']:
    #train_full_new12[feature] = le.fit_transform(train_full_new12[feature])
    test_file[feature] = pd.to_datetime(test_file[feature],errors = 'coerce').dt.weekday


# In[513]:


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
              'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
              'All_key_nan','new_key_match', 'new_pb1','Combined_TType',
              #'SideB.Date',
                 'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',          
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side']
             # 'label']


# ## UMR Mapping

# In[514]:



## TODO Import HIstorical UMR FILE for Transaction Type mapping

Soros_umr = pd.read_csv('Soros_UMR.csv')


# In[515]:


#soros_umr['ViewData.Combined Transaction Type'].unique()


# In[516]:


test_file['tt_map_flag'] = test_file.apply(lambda x: 1 if x['ViewData.Combined Transaction Type'] in Soros_umr['ViewData.Combined Transaction Type'].unique() else 0, axis=1)


# In[517]:


test_file['abs_amount_flag'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Accounting Net Amount'] == x['SideA.ViewData.B-P Net Amount']*(-1) else 0, axis=1)


# In[518]:


test_file = test_file[~test_file['SideB.ViewData.Settle Date'].isnull()]
test_file = test_file[~test_file['SideA.ViewData.Settle Date'].isnull()]

test_file = test_file.reset_index().drop('index',1)
test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(int)
test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(int)


# In[994]:


test_file['new_pb1'] = test_file['new_pb1'].apply(lambda x: x.replace('Citi','CITI'))


# In[519]:


#test_file2 = test_file[((test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")) | ((test_file['SideA.TType']!="Trade") & (test_file['SideB.TType']!="Trade")) ]


# In[520]:


#test_file2 = test_file[(test_file['SideA.TType']=="Trade") & (test_file['SideB.TType']=="Trade")]


# In[141]:


#test_file[(test_file['SideA.TType']==test_file['SideB.TType'])]['SideB.TType']


# In[142]:


#test_file2 = test_file2.reset_index()
#test_file2 = test_file2.drop('index',1)


# In[143]:


#test_file['SideA.ViewData.BreakID_A_side'].value_counts()
#test_file[model_cols]


# In[521]:


test_file


# ## Test file served into the model

# In[698]:


test_file2 = test_file.copy()


# In[699]:


X_test = test_file2[model_cols]


# In[700]:


X_test = X_test.reset_index()
X_test = X_test.drop('index',1)
X_test = X_test.fillna(0)


# In[701]:


X_test = X_test.fillna(0)


# In[702]:


X_test.shape


# In[703]:


X_test = X_test.drop_duplicates()
X_test = X_test.reset_index()
X_test = X_test.drop('index',1)


# In[704]:


X_test.shape


# ## Model Pickle file import

# In[705]:



## TODO Import Pickle file for 1st Model

import pickle


# In[706]:


#filename = 'Oak_W125_model_with_umb.sav'
#filename = '125_with_umb_without_des_and_many_to_many.sav'
#filename = '125_with_umb_and_price_without_des_and_many_to_many_tdsd2.sav'
#filename = 'Weiss_new_model_V1.sav'
#filename = 'Soros_new_model_V1_with_close.sav'
#filename = 'Soros_full_model_smote.sav'

#filename = 'Soros_full_model_best_cleaned_tt_without_date.sav'
#filename = 'Soros_full_model_version2.sav'
#filename = 'OakTree_final_model2.sav'
filename = 'Soros_final_model2.sav'

#filename = 'Soros_full_model_umr_umt.sav'

clf = pickle.load(open(filename, 'rb'))


# In[707]:


X_test


# ## Predictions

# In[708]:


# Actual class predictions
rf_predictions = clf.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[709]:


probability_class_0 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[710]:


X_test['Predicted_action'] = rf_predictions
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test['probability_No_pair'] = probability_class_0
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test['probability_UMB'] = probability_class_1
X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[711]:


X_test['Predicted_action'].value_counts()


# In[712]:


X_test['Predicted_action'].value_counts()


# ## Two Step Modeling

# In[713]:


model_cols_2 =[
    #'SideA.ViewData.B-P Net Amount', 
              #'SideA.ViewData.Cancel Flag', 
             # 'SideA.new_desc_cat',
              #'SideA.ViewData.Description',
            # 'SideA.ViewData.Investment Type', 
              #'SideA.ViewData.Asset Type Category', 
              
             # 'SideB.ViewData.Accounting Net Amount', 
              #'SideB.ViewData.Cancel Flag', 
              #'SideB.ViewData.Description',
             # 'SideB.new_desc_cat',
             # 'SideB.ViewData.Investment Type', 
              #'SideB.ViewData.Asset Type Category', 
              'Trade_Date_match', 'Settle_Date_match', 
           # 'Amount_diff_2', 
              'Trade_date_diff', 
            'Settle_date_diff', 'SideA.ISIN_NA', 'SideB.ISIN_NA', 
             'ViewData.Combined Fund',
              'ViewData.Combined Transaction Type', 'Combined_Investment_Type','Combined_Asset_Type_Category',
              'Combined_Desc',
             # 'ViewData.Combined Investment Type',
             # 'SideA.TType', 'SideB.TType',
              'abs_amount_flag', 'tt_map_flag', 'description_similarity_score',
              'SideB.Date','SideA.ViewData.Settle Date','SideB.ViewData.Settle Date',
              'All_key_nan','new_key_match', 'new_pb1','Combined_TType',
              #'SideB.Date',
                 'SideA.ViewData._ID', 'SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds',          
              'SideB.ViewData.Status', 'SideB.ViewData.BreakID_B_side',
              'SideA.ViewData.Status', 'SideA.ViewData.BreakID_A_side']
             # 'label']
         


# In[714]:


X_test2 = test_file[model_cols_2]


# In[715]:


X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)
X_test2 = X_test2.fillna(0)


# In[716]:


X_test2.shape


# In[717]:


X_test2 = X_test2.drop_duplicates()
X_test2 = X_test2.reset_index()
X_test2 = X_test2.drop('index',1)


# In[718]:


X_test2.shape


# In[719]:


#filename2 = 'Soros_full_model_all_two_step.sav'

## TODO Import MOdel2 as per the two step modelling process

#filename2 = 'OakTree_final_model2_step_two.sav'
filename2 = 'Soros_final_model2_step_two.sav'
clf2 = pickle.load(open(filename2, 'rb'))


# In[720]:


# Actual class predictions
rf_predictions2 = clf2.predict(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
# Probabilities for each class
rf_probs2 = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]


# In[721]:


probability_class_0_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
probability_class_1_two = clf2.predict_proba(X_test2.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]

#probability_class_2 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
#probability_class_3 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

#probability_class_4 = clf.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]


# In[722]:


X_test2['Predicted_action_2'] = rf_predictions2
#X_test['Predicted_action_probabilty'] = rf_probs

#X_test['probability_Close'] = probability_class_0
X_test2['probability_No_pair_2'] = probability_class_0_two
#X_test['probability_Partial_match'] = probability_class_0
#X_test['probability_UMB'] = probability_class_1
X_test2['probability_UMB_2'] = probability_class_1_two
#X_test['probability_UMR'] = probability_class_2
#X_test['probability_UMT'] = probability_class_3


# In[723]:


X_test2['Predicted_action_2'].value_counts()


# In[724]:


X_test.shape


# In[725]:


X_test = pd.concat([X_test, X_test2[['Predicted_action_2','probability_No_pair_2','probability_UMB_2']]],axis=1)


# In[726]:


X_test


# ## Removing positive and negative pair trades

# In[795]:


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
    final_list =[]
    pair_exists = False
    # Sort the array  
    arr.sort()  

    # Traverse the array  
    for i in range(n): 
        # For every arr[i] < 0 element,  
        # do a binary search for arr[i] > 0.
        
        if (arr[i] < 0):
            print(arr[i])
            print(i)
            
            # If found, print the pair.  
            if (binary_search(arr,-arr[i])):  
                #print(arr[i] , ", " , -arr[i]) 
                final_list.append(arr[i])
                final_list.append(-arr[i])
                        
                pair_exists = True
        else: 
            break
  
    if (pair_exists == False):  
        print("No such pair exists")
    return final_list


# In[815]:


def printPairs(arr, n): 
    v = [] 
    final_list =[]
  
    # For each element of array. 
    for i in range(n):  
  
        # Try to find the negative value  
        # of arr[i] from i + 1 to n 
        for j in range( i + 1,n) : 
            if len(arr)>1:
  
            # If absolute values are  
            # equal print pair.
            
            
                if (abs(arr[i]) == abs(arr[j])) : 
                    v.append(abs(arr[i]))
            else:
                break
  
    # If size of vector is 0, therefore  
    # there is no element with positive 
    # negative value, print "0" 
    if (len(v) == 0): 
        return np.array(['NULL']); 
  
    # Sort the vector 
    v.sort() 
  
    # Print the pair with negative  
    # positive value. 
    for i in range(len( v)): 
        print(-v[i], "" , v[i], end = " ") 
        final_list.append(v[i])
        final_list.append(-v[i])
        
    return final_list


# In[823]:


#duplicate_ids_1_list = []
#for key in X_test['SideB.ViewData.Side0_UniqueIds'].unique():
#    amount_list = printPairs(X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].values,2)
#    duplicate_ids_1 = X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['SideA.ViewData.B-P Net Amount'].isin(amount_list))]['SideA.ViewData.Side1_UniqueIds'].values
#    duplicate_ids_1_list.append(duplicate_ids_1)


# In[821]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds'].isin(np.concatenate(duplicate_ids_1_list)))]['SideA.ViewData.B-P Net Amount'].unique()


# In[789]:


#printPairs(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='1063_153157324_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values,2)


# ## New Aggregation

# In[727]:


X_test['Tolerance_level'] = np.abs(X_test['probability_UMB_2'] - X_test['probability_No_pair_2'])


# In[728]:


#X_test[X_test['Tolerance_level']<0.1]['Predicted_action'].value_counts()


# In[729]:


b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# ## UMR segregation

# In[730]:


def umr_seg(X_test):
    b_count = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].value_counts().reset_index(name='count')
    b_unique = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    b_unique['len'] = b_unique['Predicted_action'].str.len()
    b_count2 = pd.merge(b_count, b_unique.drop('Predicted_action',1), on='SideB.ViewData.Side0_UniqueIds', how='left')
    umr_table = b_count2[(b_count2['Predicted_action']=='UMR_One_to_One') & (b_count2['count']==1) & (b_count2['len']<=3)]
    return umr_table['SideB.ViewData.Side0_UniqueIds'].values
    


# In[731]:


umr_ids_0 = umr_seg(X_test)


# ## 1st Prediction Table for One to One UMR

# In[732]:


final_umr_table = X_test[X_test['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_0) & (X_test['Predicted_action']=='UMR_One_to_One')]


# In[733]:


final_umr_table = final_umr_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action','probability_No_pair','probability_UMB','probability_UMR']]


# In[734]:


final_umr_table.shape


# In[735]:


final_umr_table


# ## No-Pair segregation

# In[736]:


#b_side_agg = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action_2'].unique().reset_index()
#a_side_agg = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[737]:


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


# In[738]:


no_pair_ids_b_side, no_pair_ids_a_side = no_pair_seg(X_test)


# In[739]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]['Predicted_action_2'].value_counts()


# In[740]:


X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action_2'].unique().reset_index()


# In[741]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]['Predicted_action_2'].value_counts()


# In[742]:


final_open_table = X_test[(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)) | (X_test['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]


# In[743]:


final_open_table = final_open_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[744]:


final_open_table['probability_UMR'] = 0.00010
final_open_table = final_open_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[745]:


final_open_table.shape


# In[746]:


#final_open_table.head()

len(no_pair_ids_b_side)


# In[747]:


b_side_open_table = final_open_table.groupby('SideB.ViewData.Side0_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()
a_side_open_table = final_open_table.groupby('SideA.ViewData.Side1_UniqueIds')[['probability_No_pair','probability_UMB','probability_UMR']].mean().reset_index()


# In[748]:


a_side_open_table = a_side_open_table[a_side_open_table['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side)]
b_side_open_table = b_side_open_table[b_side_open_table['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side)]


# In[749]:


b_side_open_table = b_side_open_table.reset_index().drop('index',1)
a_side_open_table = a_side_open_table.reset_index().drop('index',1)


# In[750]:


final_no_pair_table = pd.concat([a_side_open_table,b_side_open_table], axis=0)
final_no_pair_table = final_no_pair_table.reset_index().drop('index',1)


# In[751]:


final_open_table


# In[752]:


final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.BreakID_A_side']].drop_duplicates(), on = 'SideA.ViewData.Side1_UniqueIds', how='left')
final_no_pair_table = pd.merge(final_no_pair_table, final_open_table[['SideB.ViewData.Side0_UniqueIds','SideB.ViewData.BreakID_B_side']].drop_duplicates(), on = 'SideB.ViewData.Side0_UniqueIds', how='left')


# In[753]:


final_no_pair_table


# In[754]:


#actual_closed = pd.read_csv('D:\Raman  Strategy ML 2.0\All_Data\OakTree\JuneData\Final_Predictions_379\Final_Predictions_Table_HST_RecData_379_2020-06-14.csv')


# In[755]:


#actual_closed_array = np.array(list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side0_UniqueIds'].unique()) + list(actual_closed[actual_closed['Type']=='Closed Breaks']['ViewData.Side1_UniqueIds'].unique()))


# In[756]:


#X_test_umb3 = X_test_umb[~((X_test_umb['SideB.ViewData.Side0_UniqueIds'].isin(actual_closed_array)) | (X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(actual_closed_array)))]


# ## Remove Open Ids

# In[878]:


umr_ids_a_side = final_umr_table['SideA.ViewData.Side1_UniqueIds'].unique()
umr_ids_b_side = final_umr_table['SideB.ViewData.Side0_UniqueIds'].unique()

### Remove Open IDs

X_test_left = X_test[~(X_test['SideB.ViewData.Side0_UniqueIds'].isin(no_pair_ids_b_side))]
X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(no_pair_ids_a_side))]

## Remove UMR IDs

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(umr_ids_a_side))]
X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(umr_ids_b_side))]



X_test_left = X_test_left.reset_index().drop('index',1)


# In[879]:


X_test.shape


# In[880]:


X_test_left.shape


# In[881]:


X_test_left['Predicted_action_2'].value_counts()


# ## One to One UMB segregation

# In[579]:


X_test_left['Predicted_action_2'].value_counts()


# In[580]:


### IDs left after removing UMR ids from 0 and 1 side

X_test_left = X_test_left[~(X_test_left['SideA.ViewData.Side1_UniqueIds'].isin(final_umr_table['SideA.ViewData.Side1_UniqueIds']))]

X_test_left = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(final_umr_table['SideB.ViewData.Side0_UniqueIds']))]


# In[581]:


X_test_left.shape


# In[582]:


X_test_left['Predicted_action_2'].value_counts()


# In[583]:


X_test_left = X_test_left.drop(['SideB.ViewData._ID','SideA.ViewData._ID'],1).drop_duplicates()
X_test_left = X_test_left.reset_index().drop('index',1)


# In[584]:


for key in X_test_left['SideB.ViewData.Side0_UniqueIds'].unique():
    umb_ids_1 = X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds']==key) & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.Side1_UniqueIds'].unique()


# In[585]:


X_test_left['SideB.ViewData.Side0_UniqueIds'].value_counts()


# ## UMR One to Many and Many to One 

# ### One to Many

# In[601]:


#X_test_left = X_test.copy()


# In[768]:


cliff_for_loop = 16


# In[769]:


threshold_0 = X_test['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
threshold_0_umb = threshold_0[threshold_0['count']>cliff_for_loop]['index'].unique()
threshold_0_without_umb = threshold_0[threshold_0['count']<=cliff_for_loop]['index'].unique()


# In[770]:


exceptions_0_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count')
exceptions_0_umb_ids = exceptions_0_umb[exceptions_0_umb['count']>cliff_for_loop]['index'].unique()


# In[771]:


import numpy as np

def subSum(numbers,total):
    for length in range(1, 3):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total,atol=0.25).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
        

        
        
#null_value ='No'
many_ids_1 = []
one_id_0 = []
amount_array =[]
for key in X_test[~X_test['SideB.ViewData.Side0_UniqueIds'].isin(exceptions_0_umb_ids)]['SideB.ViewData.Side0_UniqueIds'].unique():
    print(key)
    
    if key in threshold_0_umb:

        values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideA.ViewData.B-P Net Amount'].values
        net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass
            
    else:
        values =  X_test[(X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.B-P Net Amount'].values
        net_sum = X_test[X_test['SideB.ViewData.Side0_UniqueIds']==key]['SideB.ViewData.Accounting Net Amount'].max()

        #memo = dict()
        #print(values)
        #print(net_sum)

        if subSum(values,net_sum) == []: 
            #print("There are no valid subsets.")
            amount_array = ['NULL']
        else:
            amount_array = subSum(values,net_sum)

            id1_aggregation = X_test[(X_test['SideA.ViewData.B-P Net Amount'].isin(amount_array)) & (X_test['SideB.ViewData.Side0_UniqueIds']==key)]['SideA.ViewData.Side1_UniqueIds'].values
            id0_unique = key       

            if len(id1_aggregation)>1: 
                many_ids_1.append(id1_aggregation)
                one_id_0.append(id0_unique)
            else:
                pass


# In[772]:


umr_otm_table = pd.DataFrame(one_id_0)
umr_otm_table.columns = ['SideB.ViewData.Side0_UniqueIds']

umr_otm_table['SideA.ViewData.Side1_UniqueIds'] =many_ids_1


# In[774]:


umr_otm_table


# In[778]:


X_test_left[(X_test_left['SideB.ViewData.Side0_UniqueIds'] =='1146_153157324_Advent Geneva') & (X_test_left['SideA.ViewData.Side1_UniqueIds']=='1374_153157324_CITI')][['SideA.ViewData.Side1_UniqueIds','SideA.ViewData.B-P Net Amount']]


# ### Many to One

# In[ ]:


cliff_for_loop = 16


# In[824]:


threshold_1 = X_test['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
threshold_1_umb = threshold_1[threshold_1['count']>cliff_for_loop]['index'].unique()
threshold_1_without_umb = threshold_1[threshold_1['count']<=cliff_for_loop]['index'].unique()


# In[825]:


exceptions_1_umb = X_test[X_test['Predicted_action_2']=='UMB_One_to_One']['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count')
exceptions_1_umb_ids = exceptions_1_umb[exceptions_1_umb['count']>cliff_for_loop]['index'].unique()


# In[826]:


import numpy as np

def subSum(numbers,total):
    for length in range(1, 4):
        if len(numbers) < length or length < 1:
            return []
        for index,number in enumerate(numbers):
            if length == 1 and np.isclose(number, total,atol=0.25).any():
                return [number]
            subset = subSum(numbers[index+1:],total-number)
            if subset: 
                return [number] + subset
        return []
        

#null_value ='No'
many_ids_0 = []
one_id_1 = []
amount_array2 =[]
for key in X_test[~X_test['SideA.ViewData.Side1_UniqueIds'].isin(exceptions_1_umb_ids)]['SideA.ViewData.Side1_UniqueIds'].unique():
    #if key not in ['1174_379879573_State Street','201_379823765_State Street']:
    print(key)
    if key in threshold_1_umb:

        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key) & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()

        if subSum(values2,net_sum2) == []: 
            amount_array2 =[]
            #print("There are no valid subsets.")

        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass

    else:
        values2 =  X_test[(X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Accounting Net Amount'].values
        net_sum2 = X_test[X_test['SideA.ViewData.Side1_UniqueIds']==key]['SideA.ViewData.B-P Net Amount'].max()

        #memo = dict()

        if subSum(values2,net_sum2) == []: 
            amount_array2 =[]
            #print("There are no valid subsets.")

        else:
            amount_array2 = subSum(values2,net_sum2)

            id0_aggregation = X_test[(X_test['SideB.ViewData.Accounting Net Amount'].isin(amount_array2)) & (X_test['SideA.ViewData.Side1_UniqueIds']==key)]['SideB.ViewData.Side0_UniqueIds'].values
            id1_unique = key       

            if len(id0_aggregation)>1: 
                many_ids_0.append(id0_aggregation)
                one_id_1.append(id1_unique)
            else:
                pass
            


# In[827]:


#many_ids_0


# In[828]:


one_id_1


# In[829]:


umr_mto_table = pd.DataFrame(one_id_1)
umr_mto_table.columns = ['SideA.ViewData.Side1_UniqueIds']

umr_mto_table['SideB.ViewData.Side0_UniqueIds'] =many_ids_0 


# In[830]:


umr_mto_table


# In[831]:


sample_array = ['192_153157324_CITI','543_153157324_CITI','561_153157324_CITI','705_153157324_CITI','719_153157324_CITI',
 '775_153157324_CITI','1179_153157324_CITI','294_153157324_CITI','989_153157324_CITI','697_153157324_CITI',
 '1027_153157324_CITI','756_153157324_CITI','949_153157324_CITI','963_153157324_CITI','925_153157324_CITI',
 '395_153157324_CITI','362_153157324_CITI','413_153157324_CITI','1331_153157324_CITI','1077_153157324_CITI',
 '1092_153157324_CITI','1170_153157324_CITI','1066_153157324_CITI','793_153157324_CITI','1116_153157324_CITI',
 '1098_153157324_CITI','1176_153157324_CITI','1237_153157324_CITI','1354_153157324_CITI','1061_153157324_CITI',
 '1361_153157324_CITI','1347_153157324_CITI','842_153157324_CITI','1200_153157324_CITI','732_153157324_CITI',
 '857_153157324_CITI','679_153157324_CITI','779_153157324_CITI','967_153157324_CITI','776_153157324_CITI','843_153157324_CITI',
 '809_153157324_CITI','1139_153157324_CITI','662_153157324_CITI','789_153157324_CITI','741_153157324_CITI',
 '748_153157324_CITI','1005_153157324_CITI','916_153157324_CITI','847_153157324_CITI','483_153157324_CITI',
 '567_153157324_CITI','995_153157324_CITI','904_153157324_CITI','858_153157324_CITI','1070_153157324_CITI',
 '1181_153157324_CITI','549_153157324_CITI','1108_153157324_CITI','747_153157324_CITI','480_153157324_CITI',
 '890_153157324_CITI','1022_153157324_CITI','112_153157324_CITI','765_153157324_CITI','838_153157324_CITI',
 '1172_153157324_CITI','1057_153157324_CITI','999_153157324_CITI','976_153157324_CITI','1351_153157324_CITI',
 '1338_153157324_CITI','1352_153157324_CITI','416_153156564_Credit suisse']


# In[832]:


len(sample_array)


# In[833]:


X_test[X_test['SideA.ViewData.Side1_UniqueIds'].isin(sample_array)]['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[834]:


umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds'].isin(sample_array)].shape


# In[942]:


umr_mto_table


# In[957]:


pd.set_option('max_columns',50)


# In[960]:


X_test_left[(X_test_left['SideA.ViewData.Side1_UniqueIds']=='842_153157324_CITI') & (X_test_left['Predicted_action_2']=='UMB_One_to_One')]


# In[962]:


X_test_left[(X_test_left['SideA.ViewData.Side1_UniqueIds']=='842_153157324_CITI') & (X_test_left['Predicted_action_2']!='UMB_One_to_One')]['new_key_match'].value_counts()


# In[965]:


X_test[X_test['Predicted_action']!='No-Pair']['new_key_match'].value_counts()


# In[969]:


X_test.groupby(['Predicted_action'])['new_key_match'].value_counts(normalize=True)


# In[981]:


columns = ['Combined_Investment_Type']
for key in columns:
    print(X_test.groupby(['Predicted_action'])[key].value_counts(normalize=True))


# In[985]:


X_test[X_test['Predicted_action']=='UMB_One_to_One']['Combined_Investment_Type'].value_counts(normalize=True)


# ## Removing all the OTM and MTO Ids

# In[882]:


X_test_left.shape


# In[883]:


X_test_left2 = X_test_left[~(X_test_left['SideB.ViewData.Side0_UniqueIds'].isin(list(np.concatenate(many_ids_0))))]
X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(one_id_1)))]
X_test_left2 = X_test_left2[~(X_test_left2['SideA.ViewData.Side1_UniqueIds'].isin(list(np.concatenate(many_ids_1))))]
X_test_left2 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(list(one_id_0)))]

X_test_left2 = X_test_left2.reset_index().drop('index',1)


# In[884]:


X_test_left2.shape


# ## UMB one to one (final)

# In[885]:


X_test_left2.shape


# In[886]:


X_test_umb = X_test_left2[X_test_left2['Predicted_action_2']=='UMB_One_to_One']
X_test_umb = X_test_umb.reset_index().drop('index',1)


# In[887]:


X_test_umb.shape


# In[888]:


#X_test_umb['UMB_key_OTO'] = X_test_umb['SideA.ViewData.Side1_UniqueIds'] + X_test_umb['SideB.ViewData.Side0_UniqueIds']


# In[889]:


def one_to_one_umb(data):
    
    count = data['SideB.ViewData.Side0_UniqueIds'].value_counts().reset_index(name='count0')
    id0s = count[count['count0']==1]['index'].unique()
    id1s = data[data['SideB.ViewData.Side0_UniqueIds'].isin(id0s)]['SideA.ViewData.Side1_UniqueIds']
    
    count1 = data['SideA.ViewData.Side1_UniqueIds'].value_counts().reset_index(name='count1')
    final_ids = count1[(count1['count1']==1) & (count1['index'].isin(id1s))]['index'].unique()
    return final_ids
    
    


# In[890]:


one_side_unique_umb_ids = one_to_one_umb(X_test_umb)


# In[891]:


final_oto_umb_table = X_test_umb[X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]


# In[892]:


final_oto_umb_table = final_oto_umb_table[['SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideB.ViewData.BreakID_B_side','SideA.ViewData.BreakID_A_side','Predicted_action_2','probability_No_pair_2','probability_UMB_2','probability_UMR']]


# In[893]:


final_oto_umb_table['probability_UMR'] = 0.00010
final_oto_umb_table = final_oto_umb_table.rename(columns = {'Predicted_action_2':'Predicted_action','probability_No_pair_2':'probability_No_pair','probability_UMB_2':'probability_UMB'})


# In[898]:


final_oto_umb_table


# ## Removing IDs from OTO UMB

# In[901]:


X_test_left2.shape


# In[902]:


X_test_left3 = X_test_left2[~(X_test_left2['SideB.ViewData.Side0_UniqueIds'].isin(final_oto_umb_table['SideB.ViewData.Side0_UniqueIds']))]
X_test_left3 = X_test_left3[~(X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(final_oto_umb_table['SideA.ViewData.Side1_UniqueIds']))]


X_test_left3 = X_test_left3.reset_index().drop('index',1)


# In[903]:


X_test_left3.shape


# In[1026]:


umr_mto_table[umr_mto_table['SideA.ViewData.Side1_UniqueIds']=='921_153157324_CITI']['SideB.ViewData.Side0_UniqueIds'].values[0]


# In[1021]:


X_test[(X_test['SideA.ViewData.Side1_UniqueIds']=='921_153157324_CITI') & (X_test['Predicted_action_2']=='UMB_One_to_One')]['SideB.ViewData.Side0_UniqueIds']


# ## UMB One to Many and Many to One

# In[858]:


## Total IDs 

X_test['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[904]:


X_test_left3['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left3['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[915]:


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


# In[917]:


open_ids_0_last , open_ids_1_last = no_pair_seg2(X_test_left3)


# In[918]:


X_test_left3[~X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)]


# In[ ]:





# In[921]:


X_test_left4 = X_test_left3[~((X_test_left3['SideB.ViewData.Side0_UniqueIds'].isin(open_ids_0_last)) | (X_test_left3['SideA.ViewData.Side1_UniqueIds'].isin(open_ids_1_last)))]


# In[923]:


X_test_left4 = X_test_left4.reset_index().drop('index',1)


# In[ ]:


X_test_left4


# In[924]:


X_test_left4['SideB.ViewData.Side0_UniqueIds'].nunique() + X_test_left4['SideA.ViewData.Side1_UniqueIds'].nunique()


# In[941]:


X_test[X_test['Predicted_action']=='UMB_One_to_One']['Settle_Date_match'].value_counts()


# In[ ]:





# In[909]:


X_test_left3.groupby('SideB.ViewData.Side0_UniqueIds')['Predicted_action'].nunique()


# In[914]:


X_test_left3[X_test_left3['SideB.ViewData.Side0_UniqueIds']=='999_153157324_Advent Geneva']['Predicted_action'].value_counts()


# In[ ]:





# In[871]:


ff = X_test_umb.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].nunique().reset_index(name='count')
ff[ff['count']>1]


# In[876]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds'] =='101_153157422_Advent Geneva']


# In[877]:


umr_ids_a_side


# In[855]:


X_test_umb2 = X_test_umb[~X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]
X_test_umb2 = X_test_umb2.reset_index().drop('index',1)


# In[692]:


X_test_umb.shape


# In[693]:


X_test_umb2.shape


# In[694]:


X_test_umb2[X_test_umb2['Tolerance_level']<0.45].shape


# In[695]:


X_test_umb2['Predicted_action'].value_counts()


# In[352]:


X_test_umb2['Predicted_action'].value_counts()


# In[696]:


rr2 = X_test_umb2.groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
rr2['SideA.ViewData.Side1_UniqueIds'] = rr2['SideA.ViewData.Side1_UniqueIds'].apply(tuple)


# In[697]:


rr2.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()


# In[309]:


X_test[(X_test['SideB.ViewData.Side0_UniqueIds']=='31_153156570_Goldman Sachs')]['Predicted_action_2'].value_counts()


# In[ ]:





# In[ ]:





# In[ ]:





# In[355]:


X_test_umb2[X_test_umb2['Tolerance_level']>0.9].shape


# In[356]:


rr2 = X_test_umb2[X_test_umb2['Tolerance_level']>0.9].groupby(['SideB.ViewData.Side0_UniqueIds'])['SideA.ViewData.Side1_UniqueIds'].unique().reset_index()
rr2['SideA.ViewData.Side1_UniqueIds'] = rr2['SideA.ViewData.Side1_UniqueIds'].apply(tuple)


# In[357]:


rr2.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideB.ViewData.Side0_UniqueIds'].unique().reset_index()


# ## One to Many and Many to One Initiation

# In[358]:


X_test_umb2 = X_test_umb[~X_test_umb['SideA.ViewData.Side1_UniqueIds'].isin(one_side_unique_umb_ids)]
X_test_umb2 = X_test_umb2.reset_index().drop('index',1)


# In[369]:


X_test_umb2['Predicted_action'].value_counts()


# In[ ]:





# In[360]:


X_test_umb2['SideB.ViewData.Side0_UniqueIds'].value_counts()


# In[361]:


#X_test_umb2[X_test_umb2['SideA.ViewData.Side1_UniqueIds']=='1175_379879573_State Street']['SideB.ViewData.Side0_UniqueIds'].unique()


# In[370]:


X_test_umb3 =  X_test_umb2[X_test_umb2['Predicted_action']!='No-Pair']
X_test_umb3 = X_test_umb3.reset_index().drop('index',1)


# In[371]:


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


# In[372]:


sample_mto(X_test_umb3, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds')


# In[373]:


sample_otm(X_test_umb3, 'SideB.ViewData.Side0_UniqueIds', 'SideA.ViewData.Side1_UniqueIds')


# In[378]:


X_test_umb3[X_test_umb3['SideB.ViewData.Side0_UniqueIds']=='14_153156729_Advent Geneva']


# In[953]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']


# In[1073]:


#targets = list(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideB.ViewData.Accounting Net Amount'].max())
#numbers = list(X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values)


# In[ ]:


X_test[X_test['SideB.ViewData.Side0_UniqueIds']=='68_379899240_Advent Geneva']['SideA.ViewData.B-P Net Amount'].values


# In[ ]:





# ## UMR One to Many Summation Code

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




