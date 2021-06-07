#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os


# In[ ]:


import dask.dataframe as dd
import glob


# In[ ]:


df1 = pd.read_csv('oaktree/meo_df_setup_379_date_2020-12-10.csv')


# In[ ]:


output_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']


# In[ ]:


sel_col = [

 

 
 'ViewData.BreakID',
 'ViewData.Status',
 'ViewData.SPM ID',
 'ViewData.Cancel Amount',
 'ViewData.Currency',
 'ViewData.Custodian Account',
 'ViewData.Trade Date',
 'ViewData.Transaction ID',
 'ViewData.Investment ID',
 'ViewData.CUSIP',
 'ViewData.Description',
 'ViewData.ISIN',
 'ViewData.SEDOL',
 'ViewData.Ticker',
 'ViewData.Underlying Cusip',
 'ViewData.Underlying Investment ID',
 'ViewData.Underlying ISIN',
 'ViewData.Underlying Sedol',
 'ViewData.Underlying Ticker',
 'ViewData.Settle Date',
 'ViewData.Fund',
 'ViewData.Transaction Type',
 'ViewData.Non Trade Description',
 'ViewData.Value Date',
 'ViewData.Accounting Net Amount',
 'ViewData.Cust Net Amount',
 'ViewData.Net Amount Difference',
 'ViewData.Net Amount Difference Absolute',
 'ViewData.Account Type',
 'ViewData.Portolio',
 'ViewData.Portfolio ID',
 'ViewData.Custodian',
 'ViewData.PB Account Numeric',
 'ViewData.Asset Type Category',
 'ViewData.Business Date',
 'ViewData.Cancel Flag',
 'ViewData.Investment Type',
 'ViewData.Prime Broker',
 'ViewData.Quantity',
 'ViewData.Strategy',
 'ViewData.Price',
 'ViewData.Commission',
 'ViewData.Transaction Category',
 'ViewData.ExpiryDate',
 'ViewData.UserTran1',
 'ViewData.UserTran2',
 'ViewData.Knowledge Date',
 'ViewData.Prior Knowledge Date',
 'ViewData.Base Net Amount',
 'ViewData.Base Currency',
 'ViewData.Break Tag',
 'ViewData.Department',
 'ViewData.Group',
 'ViewData.Strike Price',

 'ViewData.Activity Code',
 'ViewData.Sec Fees',
 'ViewData.FX Rate',
 'ViewData.OTE Ticker',
 'ViewData.OTEIncludeFlag',
 'ViewData.Call Put Indicator',
 'ViewData.Bloomberg_Yellow_Key',

 'ViewData.Principal Amount',
 'ViewData.Interest Amount',
 'ViewData.Trade Expenses',
 'ViewData.OTE Custodian Account',
 'ViewData.LoanX ID',
 'ViewData.Legal Entity',
 'ViewData.Derived Source',


 'ViewData.Base Closing Balance',
 'ViewData.Closing Balance',
 'ViewData.Closing Date',
 'ViewData.PB Account',
 'ViewData.Balance FX Rate',

 'ViewData.Principal Balance',
 'ViewData.Interest Balance',
 'ViewData.PB Account Number',
 'ViewData.Account Name',
 'ViewData.Fund Structure',
 
 'ViewData.Rule And Key',
 'ViewData.Keys',
 'ViewData.Age',
 'ViewData.Age WK',
 'ViewData.Priority',
 'ViewData.ClusterID',
 'ViewData.System Comments',
 'ViewData.InternalComment1',
 'ViewData.InternalComment2',
 'ViewData.InternalComment3',

 'ViewData.Workflow Status',
 'ViewData.Workflow Remark',
 'ViewData.Assigned To',
 'ViewData.Reviewer',
 'ViewData.Recon Purpose',
 'ViewData.Recon Data Model',
 'ViewData.Recon Setup',
 'ViewData.Task ID',
 'ViewData.Recon Setup Code',
 'ViewData.Client',
 'ViewData.ClientShortCode',
 'ViewData.Task Business Date',
 'ViewData.Task Knowledge Date',
 'ViewData.Source Combination',
 'ViewData.Source Combination Code',
 'ViewData.CombinedAndIsPaired',
 'ViewData.SourceTypeCode',
 'ViewData.Source',
 'ViewData.Source Type Code Combination',
 'ViewData.Source Type Combination',

 'ViewData.Type',
 'ViewData.Mapped Custodian Account',
 'ViewData.BalanceAcc',
 'ViewData.BalancePB',
 'ViewData.BaseBalanceAcc',
 'ViewData.BaseBalancePB',
 'ViewData.Balances Fund',
 'ViewData.Balances Account Name',
 'ViewData.Balances PB Account Number',
 'ViewData.Balances Fund Structure',
 'ViewData.Balances Alt ID 1',
 'ViewData.Balances Balance FX Rate',
 
 'ViewData.Side0_UniqueIds',
 'ViewData.Side1_UniqueIds',
 'ViewData.Is Combined Data',
 'ViewData.Accounting Custodian Account',
 'ViewData.Cust Custodian Account',
 'ViewData.Custodian Account Difference',
 'ViewData.Accounting Quantity',
 'ViewData.Cust Quantity',
 'ViewData.Quantity Difference',
 'ViewData.Accounting Strategy',
 'ViewData.Cust Strategy',
 'ViewData.Strategy Difference',
 'ViewData.Accounting Legal Entity',
 'ViewData.Cust Legal Entity',
 'ViewData.Legal Entity Difference'
]


# In[ ]:


df1 = df1[sel_col]


# In[ ]:


df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')


# In[ ]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[ ]:


df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[ ]:


df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])


# In[ ]:


def subSum(numbers,total):
    length = len(numbers)

    if length <18:
        
        for index,number in enumerate(numbers):
            if np.isclose(number, total, atol=1).any():
                return [number]
                print(34567)
            subset = subSum(numbers[index+1:],total-number)
            if subset:
                #print(12345)
                return [number] + subset
        return []
    else:
        return numbers


# In[ ]:


fullcal = df1.groupby(['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Fund'])['ViewData.Quantity'].apply(list).reset_index()


# In[ ]:


fullcal['set_Qn_list'] = fullcal['ViewData.Quantity'].apply(lambda x : list(set([value for value in x if str(value) != 'nan'])))


# In[ ]:


fullcal['zero_list'] =fullcal['set_Qn_list'].apply(lambda x : subSum(x,0))


# In[ ]:


def list_filter(x):
    if ((len(x)>1) & (len(x)<11)):
        if ((0.0 not in x) & (sum(x)<0.5)):
            return 1
        else:
            return 0
    else:
        return 0


# In[ ]:


fullcal['fcmark1'] = fullcal['zero_list'].apply(lambda x : list_filter(x))


# In[ ]:


fullcal.drop(['ViewData.Quantity','set_Qn_list'], axis = 1, inplace = True)


# In[ ]:


df1 = pd.merge(df1, fullcal, on = ['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Fund'], how = 'left')


# In[ ]:


def fc_remover(x,y,z,m):
    ttype = ['corporate action','interest','int','corp-red payment','corprd']
    if ((isinstance(y,list)) & (type(m)==str)):
        m = m.lower()
        if ((x in y) & (z == 1.0)  & (m in ttype)):
            return 1
        else:
            return 0
    else:
        return 0


# In[ ]:


df1['final_fc'] = df1.apply(lambda row: fc_remover(row['ViewData.Quantity'],row['zero_list'],row['fcmark1'],row['ViewData.Transaction Type']), axis =1 )


# In[ ]:


fc1 =df1[df1['final_fc']==1]


# In[ ]:


fc1['predicted status'] = 'No-pair'
fc1['predicted action'] = 'OB'
fc1['predicted category'] = 'full call corp action'


# In[ ]:


def comgen(x,y,z,k):
    if x == None:
        com = 'Geneva to book full call. Corporate action team to post it' + ' ' + k + " " + 'already booked it.'
    else:
        com = k + ' ' + 'to book full call. Corporate action team to post it' + " " +  'Geneva already booked it.'
        
    return com


# In[ ]:


fc1['predicted comment'] = fc1.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds'],x['ViewData.Settle Date'],x['ViewData.Prime Broker']), axis = 1)


# In[ ]:


fc1 = fc1[output_col]


# In[ ]:


fc1.to_csv('oaktree/oaktree final prediction p1.csv')


# In[ ]:


df1 = df1[df1['final_fc']!=1]
df1.drop(['zero_list', 'fcmark1', 'final_fc'], axis =1 , inplace = True)


# In[ ]:


req_desc = ['third','party','fx','transaction']


# In[ ]:


def netting(x,req_desc):
    if type(x)==str:
        x = x.lower()
        x1 = x.split()
        lst3 = [value for value in req_desc if value in x1]
        if len(lst3) == 4:
            return 1
        else:
            return 0
    else:
        return 0


# In[ ]:


df1['Netting_var'] = df1['ViewData.Description'].apply(lambda x : netting(x,req_desc))


# In[ ]:


def netting_com(a,b):
    com = 'Netting is' + ' ' + str(a) + 'for' + " " + str(b) + " " + 'We have escalted this to client.'
    return com


# In[ ]:


if (df1[df1['Netting_var']==1].shape[0]!=0):
    netting = df1[df1['Netting_var']==1]
    net = netting.groupby(['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Settle Date'])['ViewData.Net Amount Difference'].sum().reset_index()
    net = net.rename(columns = {'ViewData.Net Amount Difference':'netting amount'})
    netting = pd.merge(netting, net, on = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Settle Date'], how = 'left')
    netting['predicted comment'] = netting.apply(lambda row: netting_com(row['netting amount'], row['ViewData.Settle Date']), axis =1)
    netting['predicted status'] = 'No-pair'
    netting['predicted action'] = 'OB'
    netting['predicted category'] = 'full call corp action'
    netting = netting[output_col]
    netting.to_csv('oaktree/oaktree final prediction p2.csv')
else:
    df1 = df1.copy()


# In[ ]:


df3 = df1.copy()


# In[ ]:


df3['ViewData.Task Business Date'] = pd.to_datetime(df3['ViewData.Task Business Date'])
df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])


# In[ ]:


from datetime import datetime, timezone

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


# In[ ]:


df3['ViewData.Task Business Date'] = df3['ViewData.Task Business Date'].apply(lambda x :utc_to_local(x) )


# In[ ]:


df3['fut_date'] = round(((df3['ViewData.Settle Date'] - df3['ViewData.Task Business Date']).astype('timedelta64[h]'))/24,1)


# In[ ]:


import re


# In[ ]:


def fullcall(x):
    if type(x)==str:
        y = re.search("[0-9]{2}[A-Z]{3}[0-9]{2}", x)
        if y==None:
            return 0
        else:
            return 1
    else:
        return 0


# In[ ]:


df3['full_call'] = df3['ViewData.Description'].apply(lambda x :fullcall(x))


# In[ ]:


def hasdate(x):
    if type(x)==str:
        y = re.search("[0-9]+/[A-Za-z]+/[0-9]+", x)
        if y==None:
            return 0
        else: 
            y = re.search("[0-9]+-[A-Za-z]+-[0-9]+", x)
            if y==None:
                return 1
            else:
                return 0
    else:
        return 0


# In[ ]:


df3['has_date'] = df3['ViewData.Description'].apply(lambda x :hasdate(x))


# In[ ]:


def rate(x):
    if type(x)==str:
        item1 = x.split()
        for item in item1:
            if item.endswith('%'):
                return 1
            else:
                return 0
    else:
        return 0


# In[ ]:


df3['rate_var'] = df3['ViewData.Description'].apply(lambda x :rate(x))
df3['rate_var_itype'] = df3['ViewData.Investment Type'].apply(lambda x :rate(x))


# In[ ]:


month_end =[29,30,31,1]
df3['month_end_mark'] = df3['ViewData.Settle Date'].apply(lambda x : 1 if x.day in month_end else 0)


# In[ ]:


cols = [
 

 
 'ViewData.Transaction Type1','ViewData.Asset Type Category1', 'ViewData.Department',
            'ViewData.Investment Type1','new_desc_cat','new_pb1','fut_date'
 
              
             ]


# In[ ]:


data2['ViewData.Description'] = data2['ViewData.Description'].fillna('nn')


# In[ ]:


data2['fut_date'] = data2['fut_date'].astype(int)
data2['full_call'] = data2['full_call'].astype(int)
data2['rate_var'] = data2['rate_var'].astype(int)
data2['month_end_mark'] = data2['month_end_mark'].astype(int)

