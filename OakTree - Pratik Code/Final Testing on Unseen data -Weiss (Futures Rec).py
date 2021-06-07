#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE


# In[350]:


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


# In[351]:


new_cols = ['ViewData.' + x for x in cols] + add


# In[352]:


#df_170.shape


# ## Read testing data 

# In[1787]:


meo = pd.read_csv("//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_1200_2020-06-18.csv",usecols=new_cols)


# In[1788]:


#df['ViewData.Task Business Date']
meo.shape


# In[1789]:


meo['ViewData.Status'].value_counts()


# In[1790]:


df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
#df = df[df['MatchStatus'] != 21]
df1 = df1[~df1['ViewData.Status'].isnull()]
df1 = df1.reset_index()
df1 = df1.drop('index',1)


# In[1791]:


#df1['close_key'] = df1['ViewData.Side0_UniqueIds'].astype(str) + df1['ViewData.Side1_UniqueIds'].astype(str)
df1.shape


# In[1792]:


df1['ViewData.Transaction Type'].unique()


# In[1793]:


df1['ViewData.Fund'].unique()


# In[1794]:


df1['ViewData.Transaction Type'] = df1['ViewData.Transaction Type'].apply(lambda x: str(x).lower())


# In[1795]:


df1.loc[df1['ViewData.Transaction Type'] =='proceeds sell', 'ViewData.Transaction Type'] = 'proceed sell'

df1.loc[df1['ViewData.Transaction Type'] =='realized p&l', 'ViewData.Transaction Type'] = 'proceed sell'


df1.loc[df1['ViewData.Transaction Type'] =='wire received', 'ViewData.Transaction Type'] = 'wire'
df1.loc[df1['ViewData.Transaction Type'] =='trf from sec to seg', 'ViewData.Transaction Type'] = 'wire'
df1.loc[df1['ViewData.Transaction Type'] =='wire transferred', 'ViewData.Transaction Type'] = 'wire'
df1.loc[df1['ViewData.Transaction Type'] =='collateral posted to isda counterparties', 'ViewData.Transaction Type'] = 'wire'


# In[1796]:


df1['TT_fee_commission_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['fee','commission']) else 0)
df1['TT_fee_commission_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['fee','commission']) else 0)


# In[1797]:


df1['TT_fee_sell_commission_flag'] = df1.apply(lambda x: 1 if x['ViewData.Transaction Type']!='buy commission' and x['TT_fee_commission_flag']==1 else 0, axis=1)
df1['TT_fee_buy_commission_flag'] = df1.apply(lambda x: 1 if x['ViewData.Transaction Type']!='sell commission' and x['TT_fee_commission_flag']==1 else 0, axis=1)


# In[1798]:


df1['TT_proceed_sell_pnl_flag'] = df1['ViewData.Transaction Type'].apply(lambda x: 1 if any(key in x for key in ['realized p&l', 'proceed sell']) else 0)


# In[1799]:



## Output for Closed breaks

#closed_df = df1[(df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys))]


# In[1800]:


#closed_df.shape
#df1[df1['ViewData.OTE Ticker']!=df1['ViewData.Investment ID']]


# ## Machine generated output

# In[1801]:


#df2 = df1[~df1['close_key'].isin(list(all_closed))]
#df2 = df1[~((df1['ViewData.Side1_UniqueIds'].isin(new_closed_keys)) | (df1['ViewData.Side0_UniqueIds'].isin(new_closed_keys)))]


# In[1802]:


df = df1.copy()


# In[1803]:


df = df.reset_index()
df = df.drop('index',1)


# In[1804]:


df.shape


# In[1805]:


#pd.set_option('display.max_columns', 500)


# In[1806]:


df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[1807]:


#df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])


# In[1808]:


df = df[~df['Date'].isnull()]
df = df.reset_index()
df = df.drop('index',1)


# In[1809]:


pd.to_datetime(df['Date'])


# In[1810]:


df['Date'] = pd.to_datetime(df['Date']).dt.date


# In[1811]:


df['Date'] = df['Date'].astype(str)


# In[1812]:


#df['ViewData.Status'].value_counts()


# In[1813]:


df = df[df['ViewData.Status'].isin(['OB','SDB','UOB','UDB','CMF','CNF','SMB','SPM'])]
df = df.reset_index()
df = df.drop('index',1)


# In[1814]:


#df1[df1['ViewData.Status']=='SMB']
df['ViewData.Status'].value_counts()


# In[1815]:


df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)


# In[1816]:


#df_170[(df_170['ViewData.Status']=='UMR')]


# In[1817]:


df['Date'].value_counts()


# ## Sample data on one date

# In[1818]:


#df = df.rename(columns= {'ViewData.Cust Net Amount':'ViewData.B-P Net Amount'})


# In[1819]:


sample = df[df['Date'] =='2020-06-19']
sample = sample.reset_index()
sample = sample.drop('index',1)


# In[1820]:


#smb = sample[sample['ViewData.Status']=='SMB'].reset_index()
#smb = smb.drop('index',1)


# In[1821]:


#smb_pb = smb.copy()
#smb_acc = smb.copy()


# In[1822]:


#smb_pb['ViewData.Accounting Net Amount'] = np.nan
#smb_pb['ViewData.Side0_UniqueIds'] = np.nan
#smb_pb['ViewData.Status'] ='SMB-OB'

#smb_acc['ViewData.B-P Net Amount'] = np.nan
#smb_acc['ViewData.Side1_UniqueIds'] = np.nan
#smb_acc['ViewData.Status'] ='SMB-OB'


# In[1823]:


#sample = sample[sample['ViewData.Status']!='SMB']
#sample = sample.reset_index()
#sample = sample.drop('index',1)


# In[1824]:




sample.shape


# In[1825]:


#sample = pd.concat([sample,smb_pb,smb_acc],axis=0)
#sample = sample.reset_index()
#sample = sample.drop('index',1)


# In[1826]:


#sample['ViewData.Status'].value_counts()


# In[1827]:


sample['ViewData.Side0_UniqueIds'] = sample['ViewData.Side0_UniqueIds'].astype(str)
sample['ViewData.Side1_UniqueIds'] = sample['ViewData.Side1_UniqueIds'].astype(str)


# In[1828]:


sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0


# In[1829]:


#sample['ViewData.Status'].value_counts()


# In[1830]:


#sample['flag_side1'].value_counts()


# In[1831]:


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


# In[1832]:


sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)


# In[1833]:


sample1 = sample1[sample1['ViewData.BreakID']!=-1]
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[1834]:


sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
sample1 = sample1.reset_index()
sample1 = sample1.drop('index',1)


# In[1835]:


sample1[['filter_key','ViewData.Mapped Custodian Account','ViewData.B-P Currency','ViewData.Accounting Currency','Trans_side']]


# ## New code

# In[1836]:


sample1['filter_key'] = sample1['ViewData.Source Combination'].astype(str) + sample1['filter_key'].astype(str) + sample1['ViewData.Fund'].astype(str)


# In[1837]:


sample1.loc[sample1['ViewData.Transaction Type']=='Proceeds Sell','ViewData.Transaction Type'] = 'Proceed Sell'
sample1['ViewData.Transaction Type'] = sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower())


# In[1838]:


sample1['filter_key_with_tt'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str)

sample1['filter_key_with_sd'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Settle Date'].astype(str)

sample1['filter_key_with_tt_sd'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str) +  sample1['ViewData.Settle Date'].astype(str)

sample1['filter_key_with_tt_inid'] = sample1['filter_key'].astype(str) +  sample1['ViewData.Transaction Type'].apply(lambda x: str(x).lower()).astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str)

sample1['filter_key_with_inid'] = sample1['filter_key'].astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str)

sample1['filter_key_with_inid_qt'] = sample1['filter_key'].astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str) + sample1['ViewData.Quantity'].astype(str)

sample1['filter_key_with_inid_price'] = sample1['filter_key'].astype(str) + sample1['ViewData.Investment ID'].apply(lambda x: str(x).lower()).astype(str) + sample1['ViewData.Price'].astype(str)


# In[1839]:


sample1['Trans_side'].value_counts()


# In[1840]:


aa_new = sample1[sample1['Trans_side']=='A_side']
bb_new = sample1[sample1['Trans_side']=='B_side']
zz_new = sample1[sample1['Trans_side']=='Both_side']


# In[1841]:


#aa_new2 = aa_new[aa_new['Date']==dt]
#bb_new2 = bb_new[bb_new['Date']==dt]
cc_new2 = pd.concat([aa_new, bb_new,zz_new], axis=0)
cc_new2 = cc_new2.reset_index().drop('index',1)
cc_new2 = cc_new2.drop_duplicates()
cc_new2 = cc_new2.reset_index().drop('index',1)

cc_new2['Amount_diff2'] = cc_new2['ViewData.Accounting Net Amount'].fillna(0) - cc_new2['ViewData.B-P Net Amount'].fillna(0)


# In[1842]:


cc_new2['Amount_diff2_absolute'] = np.abs(cc_new2['ViewData.Accounting Net Amount'].fillna(0) - cc_new2['ViewData.B-P Net Amount'].fillna(0))


# In[1843]:


cc_new2['ViewData.Transaction Type'].unique()


# In[1844]:


cc_new2.loc[cc_new2['ViewData.Transaction Type']=='Proceeds Sell','ViewData.Transaction Type'] = 'Proceed Sell'
cc_new2['ViewData.Transaction Type'] = cc_new2['ViewData.Transaction Type'].apply(lambda x: str(x).lower())


# In[1845]:


cc_new2['ViewData.Transaction Type'].unique()


# In[1846]:


cc_new2


# ## One sided Open Ids

# In[1847]:


dd = cc_new2.groupby('filter_key')['Trans_side'].nunique().reset_index()
ee = cc_new2.groupby('filter_key')['Trans_side'].unique().reset_index()

#ee.columns = ['filter_key','Unique_side']
dd['Unique_side'] = ee['Trans_side']


# In[1848]:


dd['Both_side_flag'] = dd['Unique_side'].apply(lambda x: 1 if 'Both_side' in x else 0)


# In[1849]:


dd[(dd['Trans_side']==1) &(dd['Both_side_flag']==0)]


# In[1850]:


open_filter_key = []
if dd[(dd['Trans_side']==1) &(dd['Both_side_flag']==0)].shape[0] !=0:
    open_filter_key = dd[(dd['Trans_side']==1) &(dd['Both_side_flag']==0)]['filter_key'].unique()


# In[1851]:


open_table = cc_new2[cc_new2['filter_key'].isin(open_filter_key)][['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds']]
open_table = open_table.reset_index().drop('index',1)


# In[1852]:


if open_table.empty == False:
    open_table['Predicted_status'] = 'Open'
else:
    open_table = pd.DataFrame()


# In[1853]:


open_table


# In[1854]:


if open_table.empty == False:
    open_one_side = [key for key in list(open_table['ViewData.Side1_UniqueIds'].unique()) if key!='nan']
    open_zero_side = [key for key in list(open_table['ViewData.Side0_UniqueIds'].unique()) if key!='nan']
    


# In[1855]:


if len(open_one_side) >0:
    cc_new2 = cc_new2[~cc_new2['ViewData.Side1_UniqueIds'].isin(open_one_side)]
if len(open_zero_side) >0:
    cc_new2 = cc_new2[~cc_new2['ViewData.Side0_UniqueIds'].isin(open_zero_side)]
    

cc_new2 = cc_new2.reset_index().drop('index',1)    


# In[1856]:


cc_new2.shape


# ## 1. UpDown

# In[1857]:


#cc_new2['ViewData.InternalComment2']


# In[1858]:


cc_new2['Updown_flag'] = cc_new2['ViewData.InternalComment2'].apply(lambda x: 1 if 'Up' in str(x) else 0)


# In[1859]:


ud = cc_new2[cc_new2['Updown_flag']==1]
ud = ud.reset_index().drop('index',1)


# In[1860]:


cc_new2[cc_new2['Updown_flag']==1][['ViewData.InternalComment2','Amount_diff2_absolute','filter_key','ViewData.Status','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']].sort_values(by='filter_key')


# In[1861]:


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
        id1 = ud2[(ud2['Amount_diff2_absolute']==i) & (ud2['ViewData.Side1_UniqueIds']!='nan')]['ViewData.Side1_UniqueIds'].values
        id0 = ud2[(ud2['Amount_diff2_absolute']==i) & (ud2['ViewData.Side0_UniqueIds']!='nan')]['ViewData.Side0_UniqueIds'].values
        
        
        if len(id1)>=1 and len(id0)>=1:
            umr_id0.append(id0)
            umr_id1.append(id1)


# In[ ]:





# In[1862]:


if len(umr_id1)!=0: 
    pair_table = pd.DataFrame(np.array(umr_id0))
    pair_table.columns =['ViewData.Side0_UniqueIds']
    pair_table['ViewData.Side1_UniqueIds'] = np.array(umr_id1)
else:
    pair_table = pd.DataFrame()


# In[1863]:


pair_table['Predicted_status'] = 'Pair-UMR'
pair_table['diff'] = 0


# In[1864]:


pair_table


# In[1865]:


if len(umr_id1)>0:
    umr_id1 = np.concatenate(umr_id1)
else:
    umr_id1 = ['None']
if len(umr_id0)>0:
    umr_id0 = np.concatenate(umr_id0)
else:
    umr_id0 =['None']


# In[1866]:


if pair_table.empty ==False:
    cc_new3 = cc_new2[~cc_new2['ViewData.Side0_UniqueIds'].isin(umr_id0)]
    cc_new3 = cc_new3[~cc_new3['ViewData.Side1_UniqueIds'].isin(umr_id1)]
    cc_new3 = cc_new3.reset_index().drop('index',1)
else:
    cc_new3 = cc_new2.copy()
    cc_new3 = cc_new3.reset_index().drop('index',1)


# ## 2. Filter on trans type and settle date

# In[1867]:


cc_new3[(cc_new3['TT_fee_commission_flag'] ==0) & (cc_new3['TT_proceed_sell_pnl_flag']==0)]['filter_key_with_tt_sd'].nunique()


# In[1868]:


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


# In[1869]:


#cc_new2[cc_new2['filter_key_with_tt_sd']=='Advent Geneva,Morgan StanleyMS_PB_MFUSDMFproceed sell06-02-2020'][['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Status','Trans_side','Amount_diff2_absolute']]


# In[1870]:


tt_sd_table = pd.DataFrame()
if len(tt_sd_ids_0)!=0:
    tt_sd_table['ViewData.Side0_UniqueIds'] = tt_sd_ids_0
    tt_sd_table['ViewData.Side1_UniqueIds'] = tt_sd_ids_1
    tt_sd_table['Predicted_status'] = 'Pair-UMR'
    tt_sd_table['diff'] = 0


# In[1871]:


tt_sd_table


# In[1872]:


cc_new3['ViewData.Transaction Type'].unique()


# In[1873]:


#'realized p&l', 'proceed sell'
   


# In[1874]:


cc_new3[['ViewData.Transaction Type','ViewData.Fund','ViewData.Accounting Net Amount','ViewData.B-P Net Amount','ViewData.Settle Date']]


# ## Fee and Sell commission transaction type

# In[1875]:


cc_new3[~cc_new3['ViewData.SPM ID'].isnull()].shape


# In[1876]:


# & (cc_new3['ViewData.SPM ID'].isnull()

fc = cc_new3[(cc_new3['TT_fee_sell_commission_flag']==1)]
fc = fc.reset_index().drop('index',1)


# In[1877]:


fc.shape


# In[1878]:


fc['ViewData.Investment ID'] = fc['ViewData.Investment ID'].apply(lambda x: str(x).replace(' ',''))
fc['ViewData.Investment ID'] = fc['ViewData.Investment ID'].apply(lambda x: x.lower())


# In[1879]:


pb_ids = fc.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side1_UniqueIds'].unique().reset_index()
acc_ids = fc.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side0_UniqueIds'].unique().reset_index()

#aggfunc=lambda x: len(x.unique()

#final_amount_table = pd.concat([acc_amount,pb_amount['ViewData.B-P Net Amount']], axis=1)

final_amount_table = pd.pivot_table(fc,index=['filter_key','ViewData.Settle Date','ViewData.Investment ID'],aggfunc={'ViewData.B-P Net Amount':np.sum,'ViewData.Accounting Net Amount':np.sum,'Trans_side':lambda x: len(x.unique()),'ViewData.Status':lambda x: str(x.unique())}).reset_index()


# In[1880]:


final_amount_table['diff'] = final_amount_table['ViewData.Accounting Net Amount'] - final_amount_table['ViewData.B-P Net Amount']


# In[1881]:


final_amount_table


# In[1882]:


#final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x).replace("[",''))
#final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x).replace("]",''))
#final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x).replace(" ",','))
#final_amount_table['ViewData.Status'] = final_amount_table['ViewData.Status'].apply(lambda x: str(x))


# In[1883]:


final_amount_table['SMB_flag'] = final_amount_table['ViewData.Status'].apply(lambda x: 1 if 'SMB' in x else 0)
final_amount_table['OB_flag'] = final_amount_table['ViewData.Status'].apply(lambda x: 1 if 'OB' in x else 0)


# In[1884]:


final_amount_table['ViewData.Side1_UniqueIds'] = pb_ids['ViewData.Side1_UniqueIds']
final_amount_table['ViewData.Side0_UniqueIds'] = acc_ids['ViewData.Side0_UniqueIds']
#final_amount_table = final_amount_table[final_amount_table['Trans_side']==2]
final_amount_table = final_amount_table.reset_index().drop('index',1)


# In[1885]:


final_amount_table['ViewData.Side0_UniqueIds'] = final_amount_table['ViewData.Side0_UniqueIds'].apply(lambda x: [key for key in x if key != 'nan'])
final_amount_table['ViewData.Side1_UniqueIds'] = final_amount_table['ViewData.Side1_UniqueIds'].apply(lambda x: [key for key in x if key != 'nan'])


# In[1886]:


pair_table2 = pd.DataFrame()

if final_amount_table.shape[0] !=0:
    smb_table = final_amount_table[(final_amount_table['Trans_side']==1) & (final_amount_table['SMB_flag']==1)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table2 =  final_amount_table[(final_amount_table['Trans_side']==2)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table2['Predicted_status'] = 'Pair-UMB'
    smb_table['Predicted_status'] = 'SMB'


# In[1887]:


all_umb_ids1 = []
all_umb_ids0 = []

final_pair_table = pd.concat([smb_table,pair_table2],axis=0)
final_pair_table = final_amount_table.reset_index().drop('index',1)
if final_pair_table.shape[0] !=0:

    all_umb_ids1 =list(np.concatenate(final_pair_table['ViewData.Side1_UniqueIds'].values))
    all_umb_ids0 =list(np.concatenate(final_pair_table['ViewData.Side0_UniqueIds'].values))
    


# In[1888]:


if len(all_umb_ids0)==0:
    all_umb_ids0 = ['None']
if len(all_umb_ids1)==0:
    all_umb_ids1 = ['None']


# In[1889]:


pair_table2


# In[1890]:


smb_table


# ## Fee and Buy commission transaction type

# In[1891]:


cc_new3 = cc_new3[~cc_new3['ViewData.Side1_UniqueIds'].isin(all_umb_ids1)]
cc_new3 = cc_new3[~cc_new3['ViewData.Side0_UniqueIds'].isin(all_umb_ids0)]
cc_new3 = cc_new3.reset_index().drop('index',1)


# In[1892]:


#cc_new3[(cc_new3['TT_fee_buy_commission_flag']==1)


# In[1893]:


fc_buy = cc_new3[(cc_new3['TT_fee_buy_commission_flag']==1)]
fc_buy = fc_buy.reset_index().drop('index',1)


# In[1894]:


fc_buy.shape


# In[1895]:


fc_buy['ViewData.Investment ID'] = fc_buy['ViewData.Investment ID'].apply(lambda x: str(x).replace(' ',''))
fc_buy['ViewData.Investment ID'] = fc_buy['ViewData.Investment ID'].apply(lambda x: x.lower())


# In[1896]:


pb_ids_buy = fc_buy.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side1_UniqueIds'].unique().reset_index()
acc_ids_buy = fc_buy.groupby(['filter_key','ViewData.Settle Date','ViewData.Investment ID'])['ViewData.Side0_UniqueIds'].unique().reset_index()

#aggfunc=lambda x: len(x.unique()

#final_amount_table = pd.concat([acc_amount,pb_amount['ViewData.B-P Net Amount']], axis=1)

final_amount_table_buy = pd.pivot_table(fc_buy,index=['filter_key','ViewData.Settle Date','ViewData.Investment ID'],aggfunc={'ViewData.B-P Net Amount':np.sum,'ViewData.Accounting Net Amount':np.sum,'Trans_side':lambda x: len(x.unique()),'ViewData.Status':lambda x: str(x.unique())}).reset_index()


# In[1897]:


final_amount_table_buy['diff'] = final_amount_table_buy['ViewData.Accounting Net Amount'] - final_amount_table_buy['ViewData.B-P Net Amount']


# In[1898]:


final_amount_table_buy['SMB_flag'] = final_amount_table_buy['ViewData.Status'].apply(lambda x: 1 if 'SMB' in x else 0)
final_amount_table_buy['OB_flag'] = final_amount_table_buy['ViewData.Status'].apply(lambda x: 1 if 'OB' in x else 0)


# In[1899]:


final_amount_table_buy


# In[1900]:


final_amount_table_buy['ViewData.Side1_UniqueIds'] = pb_ids_buy['ViewData.Side1_UniqueIds']
final_amount_table_buy['ViewData.Side0_UniqueIds'] = acc_ids_buy['ViewData.Side0_UniqueIds']
#final_amount_table_buy = final_amount_table_buy[final_amount_table_buy['Trans_side']==2]
final_amount_table_buy = final_amount_table_buy.reset_index().drop('index',1)


# In[1901]:


final_amount_table_buy['ViewData.Side0_UniqueIds'] = final_amount_table_buy['ViewData.Side0_UniqueIds'].apply(lambda x: [key for key in x if key != 'nan'])
final_amount_table_buy['ViewData.Side1_UniqueIds'] = final_amount_table_buy['ViewData.Side1_UniqueIds'].apply(lambda x: [key for key in x if key != 'nan'])


# In[1902]:


pair_table3 = pd.DataFrame()

if final_amount_table_buy.shape[0] !=0:
    smb_table2 = final_amount_table_buy[(final_amount_table_buy['Trans_side']==1) & (final_amount_table_buy['SMB_flag']==1)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table3 =  final_amount_table_buy[(final_amount_table_buy['Trans_side']==2)].loc[:,['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','diff']]
    pair_table3['Predicted_status'] = 'Pair-UMB'
    smb_table2['Predicted_status'] = 'SMB'


# In[1903]:


smb_table2


# In[1904]:


pair_table3


# In[1905]:


all_umb_ids1_buy = []
all_umb_ids0_buy = []
if final_amount_table_buy.empty == False:
    all_umb_ids1_buy =list(np.concatenate(final_amount_table_buy['ViewData.Side1_UniqueIds'].values))
    all_umb_ids0_buy =list(np.concatenate(final_amount_table_buy['ViewData.Side0_UniqueIds'].values))


# In[1906]:


if len(all_umb_ids0_buy)==0:
    all_umb_ids0_buy = ['None']
if len(all_umb_ids1_buy)==0:
    all_umb_ids1_buy = ['None']


# ## IDs left after removing UMBs

# In[1907]:


cc_new3.shape


# In[1908]:


cc_new4 = cc_new3[~cc_new3['ViewData.Side0_UniqueIds'].isin(all_umb_ids0)]

cc_new4 = cc_new4[~cc_new4['ViewData.Side1_UniqueIds'].isin(all_umb_ids1)]
cc_new4 = cc_new4[~cc_new4['ViewData.Side1_UniqueIds'].isin(all_umb_ids0_buy)]
cc_new4 = cc_new4[~cc_new4['ViewData.Side1_UniqueIds'].isin(all_umb_ids1_buy)]


# In[1909]:


cc_new4.shape


# ## FInal Output

# In[1910]:


#pair_table2

final_pair_table = pd.concat([pair_table, pair_table2, pair_table3,tt_sd_table], axis=0)
final_pair_table = final_pair_table.reset_index().drop('index',1)

final_smb_table = pd.concat([smb_table,smb_table2], axis=0)
final_smb_table = final_smb_table.reset_index().drop('index',1)


# In[1911]:


final_pair_table


# In[1912]:


final_smb_table


# In[1913]:


open_one_side = [key for key in cc_new4['ViewData.Side1_UniqueIds'].unique() if key!='nan']
open_zero_side = [key for key in cc_new4['ViewData.Side0_UniqueIds'].unique() if key!='nan']


# In[1914]:


final_open_table = pd.DataFrame()
open_table1 = pd.DataFrame()
open_table2 = pd.DataFrame()

open_table1['ViewData.Side0_UniqueIds'] = open_zero_side
open_table2['ViewData.Side1_UniqueIds'] = open_one_side


# In[1915]:


final_open_table = pd.concat([open_table1,open_table2],axis=0)


# In[1916]:


final_open_table['Predicted_status']= 'Open'


# In[1917]:


final_open_table = final_open_table.reset_index().drop('index',1)


# In[1918]:


final_open_table


# In[ ]:




