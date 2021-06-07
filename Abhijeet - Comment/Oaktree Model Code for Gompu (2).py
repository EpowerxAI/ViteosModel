#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os


# #### addition to pipeline

# - probable updown at fund with wire transfer
# - Removal of duplicate trades
# - Failed trade has is already settled then failed trade has to be rejected.
# - some overlap of failed trade and duplicate trade, yet to be figured out.

# - Soros 
# - Allocation needs understanding. But we will let it be as we are predicting it fine.
# - timing issue is a new category. It comes with buy trade. Still don't know what to do about it.
# - forward fx and forward are same

# In[2]:


pwd


# In[3]:


import dask.dataframe as dd
import glob


# ### Receiving No pairs, looking up in MEO and prepration of comment file

# In[4]:


#brk = pd.read_csv('1 Month Data for testing/soros/All_june_data_153.csv')


# In[54]:


final_no_pair_table_copy =pd.read_csv('final_no_pair_table.csv')


# In[55]:


brk = final_no_pair_table_copy[final_no_pair_table_copy['Predicted_action'] == 'No-Pair']


# In[56]:


meo_df = pd.read_csv('meo_df.csv')


# In[57]:


final_no_pair_table_copy.columns


# In[58]:


brk = brk.rename(columns ={'Side0_UniqueIds':'ViewData.Side0_UniqueIds',
                         'Side1_UniqueIds':'ViewData.Side1_UniqueIds'})


# In[59]:


brk.columns


# In[60]:


brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].fillna('AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].fillna('BB')


# In[61]:


brk['ViewData.Side0_UniqueIds'].count()


# In[ ]:





# In[62]:


def fid1(a,b,c):
    if a=='No-Pair':
        if b =='AA':
            return c
        else:
            return b
    else:
        return '12345'


# In[63]:


brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1 )


# In[64]:


brk['final_ID'].value_counts()


# In[65]:


side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))


# In[67]:


len(side0_id)


# In[68]:


meo1 = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(side0_id)]
meo2 = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(side1_id)]


# In[69]:


meo1.shape


# In[70]:


meo2.shape


# In[71]:


frames = [meo1, meo2]


# In[72]:


df1 = pd.concat(frames)
df1 = df1.reset_index()
df1 = df1.drop('index', axis = 1)


# ### Duplicate OB removal

# In[73]:


df1 = df1.drop_duplicates()


# In[75]:


df1.shape


# In[76]:


df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')


# In[77]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b
        


# In[78]:


df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[79]:


pd.set_option('display.max_columns', 500)


# In[80]:


df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])


# In[81]:


uni2 = df1.groupby(['final_ID','ViewData.Task Business Date']).last().reset_index()


# In[82]:


uni2 = uni2.sort_values(['final_ID','ViewData.Task Business Date'], ascending = [True, True])


# #### Trade date vs Settle date and future dated trade

# In[83]:


df2 = uni2.copy()


# In[84]:


df2.shape


# In[85]:


import datetime


# In[86]:


df2['ViewData.Settle Date'] = pd.to_datetime(df2['ViewData.Settle Date'])
df2['ViewData.Trade Date'] = pd.to_datetime(df2['ViewData.Trade Date'])
df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])


# In[87]:


df2['ViewData.Task Business Date1'] = df2['ViewData.Task Business Date'].dt.date


# In[88]:


df2['ViewData.Settle Date1'] = df2['ViewData.Settle Date'].dt.date
df2['ViewData.Trade Date1'] = df2['ViewData.Trade Date'].dt.date


# In[89]:


df2['ViewData.SettlevsTrade Date'] = (df2['ViewData.Settle Date1'] - df2['ViewData.Trade Date1']).dt.days
df2['ViewData.SettlevsTask Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Settle Date1']).dt.days
df2['ViewData.TaskvsTrade Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Trade Date1']).dt.days


# ### Cleannig of the 4 variables in this

# In[90]:


df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')


# In[91]:


def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys


# In[92]:


df['tuple'] = df.apply(make_dict, axis=1)


# In[93]:


clean_map_dict = df.set_index('tuple')['Value'].to_dict()


# In[94]:


clean_map_dict


# In[95]:


df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Asset Type Category'] = df2['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Investment Type'] = df2['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Prime Broker'] = df2['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)


# In[96]:


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
        


# In[97]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)


# In[98]:


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


# In[99]:



import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[100]:


def comb_clean(x):
    k = []
    for item in x.split():
        if ((is_num(item)==False) and (is_date_format(item)==False) and (date_edge_cases(item)==False)):
            k.append(item)
    return ' '.join(k)


# In[101]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[102]:


df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[103]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : 'paydown' if x=='pay down' else x)


# In[104]:


pd.set_option('display.max_columns', 500)


# ### Cleaning of Description

# In[105]:


com = pd.read_csv('desc cat with naveen oaktree.csv')


# In[106]:


cat_list = list(set(com['Pairing']))


# In[107]:


import re


# In[108]:



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
            


# In[111]:


df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[112]:


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
        


# In[113]:


df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))


# In[114]:


com = com.drop(['var','Catogery'], axis = 1)


# In[115]:


com = com.drop_duplicates()


# In[116]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[117]:


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


# In[118]:


df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))


# In[119]:


comp = ['inc','stk','corp ','llc','pvt','plc']
df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[120]:


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


# In[121]:


df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : desccat(x))


# #### Prime Broker Creation

# In[124]:


df2['new_pb'] = df2['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[125]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[126]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[127]:


df2['new_pb'] = df2['new_pb'].apply(lambda x : new_pf_mapping(x))


# In[128]:


df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].fillna('kkk')


# In[129]:


df2['new_pb1'] = df2.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)


# In[130]:


df2['new_pb1'] = df2['new_pb1'].apply(lambda x : x.lower())


# #### Cancelled Trade Removal

# In[173]:


trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[174]:


dfkk = df2[df2['ViewData.Transaction Type1'].isin(trade_types)]


# In[216]:


dfk_nontrade = df2[~df2['ViewData.Transaction Type1'].isin(trade_types)]


# In[217]:


dfk_nontrade.shape


# In[175]:


dffk2 = dfkk[dfkk['ViewData.Side0_UniqueIds']=='AA']
dffk3 = dfkk[dfkk['ViewData.Side1_UniqueIds']=='BB']


# In[218]:


dffk4 = dfk_nontrade[dfk_nontrade['ViewData.Side0_UniqueIds']=='AA']
dffk5 = dfk_nontrade[dfk_nontrade['ViewData.Side1_UniqueIds']=='BB']


# In[219]:


dffk4.shape


# #### Geneva side

# In[176]:


def canceltrade(x,y):
    if x =='buy' and y>0:
        k = 1
    elif x =='sell' and y<0:
        k = 1
    else:
        k = 0
    return k


# In[177]:


dffk3['cancel_marker'] = dffk3.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Accounting Net Amount']), axis = 1)


# In[178]:


dffk3['cancel_marker'].value_counts()


# In[179]:


def cancelcomment(x,y):
    com1 = 'This is original of cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[180]:


def cancelcomment1(x,y):
    com1 = 'This is cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[181]:


if dffk3[dffk3['cancel_marker'] == 1].shape[0]!=0:
    cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))
    if len(cancel_trade)>0:
        km = dffk3[dffk3['cancel_marker'] != 1]
        original = km[km['ViewData.Transaction ID'].isin(cancel_trade)]
        original['predicted category'] = 'Original of Cancelled trade'
        original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
        cancellation = dffk3[dffk3['cancel_marker'] == 1]
        cancellation['predicted category'] = 'Cancelled trade'
        cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
        cancel_fin = pd.concat([original,cancellation])
        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
        cancel_fin = cancel_fin[sel_col_1]
        cancel_fin.to_csv('Comment file oaktree 2 sep testing p1.csv')
        dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
        
    else:
        cancellation = dffk3[dffk3['cancel_marker'] == 1]
        cancellation['predicted category'] = 'Cancelled trade'
        cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
        cancel_fin = pd.concat([original,cancellation])
        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
        cancel_fin = cancel_fin[sel_col_1]
        cancel_fin.to_csv('Comment file oaktree 2 sep testing no original p2.csv')
        dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
else:
    dffk3 = dffk3.copy()
        
        


# #### Broker side

# In[182]:


dffk2['cancel_marker'] = dffk2.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Cust Net Amount']), axis = 1)


# In[183]:


dffk2['cancel_marker'].value_counts()


# In[184]:


def amountelim(row):
   
   
   
    if (row['SideA.ViewData.Mapped Custodian Account'] == row['SideB.ViewData.Mapped Custodian Account']):
        a = 1
    else:
        a = 0
        
    if ((row['SideB.ViewData.Cust Net Amount']) == -(row['SideA.ViewData.Cust Net Amount'])):
        b = 1
    else:
        b = 0
    
    if (row['SideA.ViewData.Fund'] == row['SideB.ViewData.Fund']):
        c = 1
    else:
        c = 0
        
    if (row['SideA.ViewData.Currency'] == row['SideB.ViewData.Currency']):
        d = 1
    else:
        d = 0
    
    if (row['SideA.ViewData.Settle Date1'] == row['SideB.ViewData.Settle Date1']):
        e = 1
    else:
        e = 0
        
    if (row['SideA.ViewData.Transaction Type1'] == row['SideB.ViewData.Transaction Type1']):
        f = 1
    else:
        f = 0
        
    if (row['SideB.ViewData.Quantity'] == row['SideA.ViewData.Quantity']):
        g = 1
    else:
        g = 0
        
    if (row['SideB.ViewData.ISIN'] == row['SideA.ViewData.ISIN']):
        h = 1
    else:
        h = 0
        
    if (row['SideB.ViewData.CUSIP'] == row['SideA.ViewData.CUSIP']):
        i = 1
    else:
        i = 0
        
    if (row['SideB.ViewData.Ticker'] == row['SideA.ViewData.Ticker']):
        j = 1
    else:
        j = 0
        
    if (row['SideB.ViewData.Investment ID'] == row['SideA.ViewData.Investment ID']):
        k = 1
    else:
        k = 0
        
    return a, b, c ,d, e,f,g,h,i,j,k
    
        


# In[185]:


from pandas import merge
from tqdm import tqdm


# In[186]:


def cancelcomment2(y):
    com1 = 'This is original of cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' '  + com2 +' ' + str(y)
    return com


# In[187]:


def cancelcomment3(y):
    com1 = 'This is cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' ' + com2 + ' ' + str(y)
    return com


# In[188]:


if dffk2[dffk2['cancel_marker'] == 1].shape[0]!=0:
    dummy1 = dffk2[dffk2['cancel_marker']!=1]
    dummy1 = dffk2[dffk2['cancel_marker']==1]


    pool =[]
    key_index =[]
    training_df =[]
    call1 = []

    appended_data = []

    no_pair_ids = []
#max_rows = 5

    k = list(set(list(set(dummy['ViewData.Task Business Date1']))))
    k1 = k

    for d in tqdm(k1):
        aa1 = dummy[dummy['ViewData.Task Business Date1']==d]
        bb1 = dummy1[dummy1['ViewData.Task Business Date1']==d]
        aa1['marker'] = 1
        bb1['marker'] = 1
    
        aa1 = aa1.reset_index()
        aa1 = aa1.drop('index',1)
        bb1 = bb1.reset_index()
        bb1 = bb1.drop('index', 1)
        #print(aa1.shape)
        #print(bb1.shape)
    
        aa1.columns = ['SideB.' + x  for x in aa1.columns] 
        bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
        cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
        appended_data.append(cc1)
        cancel_broker = pd.concat(appended_data)
        cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
        elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]
        if elim1.shape[0]!=0:
            id_listA = list(set(elim1['SideA.final_ID']))
            c1 = dummy
            c2 = dummy1[dummy1['final_ID'].isin(id_listA)]
            c1['predicted category'] = 'Cancelled trade'
            c2['predicted category'] = 'Original of Cancelled trade'
            c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
            c2['predicted comment'] = c2.apply(lambda x : cancelcomment3(x['ViewData.Settle Date1']))
            cancel_fin = pd.concat([c1,c2])
            cancel_fin = cancel_fin.reset_index()
            cancel_fin = cancel_fin.drop('index', axis = 1)
            sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
            cancel_fin = cancel_fin[sel_col_1]
            cancel_fin.to_csv('Comment file oaktree 2 sep testing p3.csv')
            id_listB = list(set(c1['final_ID']))
            comb = id_listB + id_listA
            dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
            
            
            
   
        
    else:
        c1 = dummy
        c1['predicted category'] = 'Cancelled trade'
        c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
        sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
        cancel_fin = c1[sel_col_1]
        cancel_fin.to_csv('Comment file oaktree 2 sep testing no original p4.csv')
        id_listB = list(set(c1['final_ID']))
        comb = id_listB
        dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
        
else:
    dffk2 = dffk2.copy()


# #### Finding Pairs in Up and down

# In[272]:


sel_col = ['ViewData.Currency', 
       'ViewData.Accounting Net Amount', 'ViewData.Age', 'ViewData.Asset Type Category1',
       
        'ViewData.Cust Net Amount',
       'ViewData.BreakID', 'ViewData.Business Date', 'ViewData.Cancel Amount',
       'ViewData.Cancel Flag', 'ViewData.ClusterID', 'ViewData.Commission',
       'ViewData.CUSIP',  
       'ViewData.Description',  'ViewData.Fund',
        'ViewData.Has Attachments',
       'ViewData.InternalComment1', 'ViewData.InternalComment2',
       'ViewData.InternalComment3', 'ViewData.Investment ID',
       'ViewData.Investment Type1', 
       'ViewData.ISIN', 'ViewData.Keys', 
       'ViewData.Mapped Custodian Account', 'ViewData.Department',
       
        'ViewData.Portfolio ID',
       'ViewData.Portolio', 'ViewData.Price', 'ViewData.Prime Broker1',
        
       'ViewData.Quantity',  'ViewData.Rule And Key',
       'ViewData.SEDOL', 'ViewData.Settle Date', 
       'ViewData.Status', 'ViewData.Strategy', 'ViewData.System Comments',
       'ViewData.Ticker', 'ViewData.Trade Date', 'ViewData.Trade Expenses',
       'ViewData.Transaction ID',
       'ViewData.Transaction Type1', 'ViewData.Underlying Cusip',
       'ViewData.Underlying Investment ID', 'ViewData.Underlying ISIN',
       'ViewData.Underlying Sedol', 'ViewData.Underlying Ticker',
       'ViewData.UserTran1', 'ViewData.UserTran2', 
       'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
       'ViewData.Task Business Date', 'final_ID',
        'ViewData.Task Business Date1',
       'ViewData.Settle Date1', 'ViewData.Trade Date1',
       'ViewData.SettlevsTrade Date', 'ViewData.SettlevsTask Date',
       'ViewData.TaskvsTrade Date','new_desc_cat', 'ViewData.Custodian', 'ViewData.Net Amount Difference Absolute', 'new_pb1'
      ]


# In[273]:


dff4 = dffk2[sel_col]
dff5 = dffk3[sel_col]


# In[274]:


dff6 = dffk4[sel_col]
dff7 = dffk5[sel_col]


# In[275]:


dff4 = pd.concat([dff4,dff6])
dff4 = dff4.reset_index()
dff4 = dff4.drop('index', axis = 1)


# In[276]:


dff5 = pd.concat([dff5,dff7])
dff5 = dff5.reset_index()
dff5 = dff5.drop('index', axis = 1)


# In[ ]:





# #### M cross N code

# In[277]:


###################### loop 3 ###############################
from pandas import merge
from tqdm import tqdm

pool =[]
key_index =[]
training_df =[]
call1 = []

appended_data = []

no_pair_ids = []
#max_rows = 5

k = list(set(list(set(dff5['ViewData.Task Business Date1'])) + list(set(dff4['ViewData.Task Business Date1']))))
k1 = k

for d in tqdm(k1):
    aa1 = dff4[dff4['ViewData.Task Business Date1']==d]
    bb1 = dff5[dff5['ViewData.Task Business Date1']==d]
    aa1['marker'] = 1
    bb1['marker'] = 1
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    print(aa1.shape)
    print(bb1.shape)
    
    aa1.columns = ['SideB.' + x  for x in aa1.columns] 
    bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
    cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
    appended_data.append(cc1)
 


# In[278]:


df_213_1 = pd.concat(appended_data)


# In[279]:


def amountelim(row):
   
   
   
    if (row['SideA.ViewData.Mapped Custodian Account'] == row['SideB.ViewData.Mapped Custodian Account']):
        a = 1
    else:
        a = 0
        
    if (row['SideB.ViewData.Cust Net Amount'] == row['SideA.ViewData.Accounting Net Amount']):
        b = 1
    else:
        b = 0
    
    if (row['SideA.ViewData.Fund'] == row['SideB.ViewData.Fund']):
        c = 1
    else:
        c = 0
        
    if (row['SideA.ViewData.Currency'] == row['SideB.ViewData.Currency']):
        d = 1
    else:
        d = 0
    
    if (row['SideA.ViewData.Settle Date1'] == row['SideB.ViewData.Settle Date1']):
        e = 1
    else:
        e = 0
        
    if (row['SideA.ViewData.Transaction Type1'] == row['SideB.ViewData.Transaction Type1']):
        f = 1
    else:
        f = 0
        
    return a, b, c ,d, e,f
    
        
   
    


# In[280]:


df_213_1[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")


# In[281]:


df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['sd_match'] + df_213_1['curr_match']


# In[282]:


elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]


# In[283]:


elim1.shape


# - putting updown comments

# In[ ]:





# In[284]:


def updownat(a,b,c,d,e):
    if a == 0:
        k = 'mapped custodian account'
    elif b==0:
        k = 'currency'
    elif c ==0 :
        k = 'Settle Date'
    elif d == 0:
        k = 'fund'    
    elif e == 0:
        k = 'transaction type'
    else :
        k = 'Investment type'
        
    com = 'up/down at'+ ' ' + k
    return com


# In[285]:


if elim1.shape[0]!=0:
    elim1['SideA.predicted category'] = 'Updown'
    elim1['SideB.predicted category'] = 'Updown'
    elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
    elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
    elim_col = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
    
    sideA_col = []
    sideB_col = []
    elim_col = list(elim1.columns)

    for items in elim_col:
        item = 'SideA.'+items
        sideA_col.append(item)
        item = 'SideB.'+items
        sideB_col.append(item)
        
    elim2 = elim1[sideA_col]
    elim3 = elim1[sideA_col]
    
    elim2 = elim2.rename(columns= {'SideA.final_ID':'final_ID',
                              'SideA.predicted category':'predicted category',
                              'SideA.predicted comment':'predicted comment'})
    elim3 = elim3.rename(columns= {'SideB.final_ID':'final_ID',
                              'SideB.predicted category':'predicted category',
                              'SideB.predicted comment':'predicted comment'})
    frames = [elim2,elim3]
    elim = pd.concat(frames)
    elim = elim.reset_index()
    elim = elim.drop('index', axis = 1)
    elim.to_csv('Comment file oaktree 2 sep testing p5.csv')
    
    id_listB = list(set(elim1['SideB.final_ID'])) 
    id_listA = list(set(elim1['SideA.final_ID']))
    
    df_213_1 = df_213_1[~df_213_1['SideB.final_ID'].isin(id_listB)]
    df_213_1 = df_213_1[~df_213_1['SideA.final_ID'].isin(id_listA)]
    
    id_listB = list(set(df_213_1['SideB.final_ID'])) 
    id_listA = list(set(df_213_1['SideA.final_ID']))
    
    dff4 = dff4[dff4['final_ID'].isin(id_listB)]
    dff5 = dff5[dff5['final_ID'].isin(id_listA)]
    
else:
    dff4 = dff4.copy()
    dff5 = dff5.copy()
    


# In[286]:


frames = [dff4,dff5]


# In[287]:


data = pd.concat(frames)
data = data.reset_index()
data = data.drop('index', axis = 1)


# In[288]:


data.shape


# In[340]:


data['ViewData.InternalComment2'].count()


# In[289]:


data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)


# In[294]:


Pre_final = [
    
'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID',
 


 'ViewData.Currency',
 'ViewData.Custodian',
     'ViewData.ISIN',

 'ViewData.Mapped Custodian Account',
 
 'ViewData.Net Amount Difference Absolute',
 
 'ViewData.Portolio',
 'ViewData.Settle Date',
 
 'ViewData.Trade Date',
 'ViewData.Transaction Type1',
'new_desc_cat',
    'ViewData.Department',

 
 
 
 'ViewData.Accounting Net Amount',
 'ViewData.Asset Type Category1',
 
 
 'ViewData.CUSIP',
 'ViewData.Commission',
 
 'ViewData.Fund',
 
 
 'ViewData.Investment ID',
 'ViewData.Investment Type1',
 
 
 'ViewData.Price',
 'ViewData.Prime Broker1',

 'ViewData.Quantity',
 
'ViewData.InternalComment2', 'ViewData.Description','new_pb2','new_pb1'
   
 
]


# In[295]:


data = data[Pre_final]


# In[296]:


df_mod1 = data.copy()


# In[297]:


df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].fillna('AA')
df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].fillna('bb')
df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].fillna(0)
df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].fillna(0)
df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].fillna(0)
df_mod1['ViewData.Asset Type Category1'] = df_mod1['ViewData.Asset Type Category1'].fillna('CC')
df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].fillna('DD')
df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].fillna('EE')
df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].fillna('FF')
df_mod1['ViewData.Investment Type1'] = df_mod1['ViewData.Investment Type1'].fillna('GG')
#df_mod1['ViewData.Knowledge Date'] = df_mod1['ViewData.Knowledge Date'].fillna(0)
df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].fillna(0)
df_mod1['ViewData.Prime Broker1'] = df_mod1['ViewData.Prime Broker1'].fillna("HH")
df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].fillna(0)
#df_mod1['ViewData.Sec Fees'] = df_mod1['ViewData.Sec Fees'].fillna(0)
#df_mod1['ViewData.Strike Price'] = df_mod1['ViewData.Strike Price'].fillna(0)
df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].fillna(0)
df_mod1['ViewData.Transaction Type1'] = df_mod1['ViewData.Transaction Type1'].fillna('kk')
df_mod1['ViewData.ISIN'] = df_mod1['ViewData.ISIN'].fillna('mm')
df_mod1['new_desc_cat'] = df_mod1['new_desc_cat'].fillna('nn')
#df_mod1['Category'] = df_mod1['Category'].fillna('NA')
df_mod1['ViewData.Description'] = df_mod1['ViewData.Description'].fillna('nn')
df_mod1['ViewData.Department'] = df_mod1['ViewData.Department'].fillna('nn')


# In[298]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[299]:


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[300]:


data2 = df_mod1.copy()


# ### Separate Prediction of the Trade and Non trade

# #### 1st for Non Trade

# In[301]:


trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[302]:


data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]


# In[306]:


cols = [
 

 
  'ViewData.Transaction Type1',
 
 

 'ViewData.Asset Type Category1',
 
 'new_desc_cat',

 'ViewData.Investment Type1',
 
'new_pb1','new_pb2','ViewData.Department'
 
 
              
             ]


# In[307]:


data211 = data21[cols]


# In[308]:


data211


# In[326]:


filename = 'finalized_model_oaktree_non trade_v1.sav'


# In[327]:


import pickle


# In[328]:


clf = pickle.load(open(filename, 'rb'))


# In[329]:


# Actual class predictions
cb_predictions = clf.predict(data211)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]


# #### Testing of Model and final prediction file - Non Trade

# In[330]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[341]:


result_non_trade =data21.copy()


# In[342]:


result_non_trade = result_non_trade.reset_index()


# In[343]:


result_non_trade['predicted category'] = pd.Series(demo)
result_non_trade['predicted comment'] = 'NA'


# In[ ]:


result_non_trade = result_non_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']]


# In[ ]:


result_non_trade.to_csv('Comment file oaktree 2 sep testing p6.csv')


# #### For Trade Model

# In[347]:


data22 = data2[data2['ViewData.Transaction Type1'].isin(trade_types)]


# In[348]:


data222 = data22[cols]


# In[349]:


filename = 'finalized_model_oaktree_trade_v1.sav'


# In[350]:


clf = pickle.load(open(filename, 'rb'))


# In[351]:


# Actual class predictions
cb_predictions = clf.predict(data222)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]


# In[352]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[353]:


result_trade =data22.copy()


# In[354]:


result_trade = result_trade.reset_index()


# In[355]:


result_trade['predicted category'] = pd.Series(demo)
result_trade['predicted comment'] = 'NA'


# In[356]:


result_trade.shape


# In[ ]:


result.to_csv('Prefile of Comment prediction.csv')


# In[ ]:


result_trade = result_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']]


# In[ ]:


result_trade.to_csv('Comment file oaktree 2 sep testing p7.csv')


# ### Code Ends Here

# In[202]:


itemlist1 = []
onerow1 = []
for i, row in sideA1.iterrows():
    onerow = []
    itemlist = []
    id1 = row['ViewData.Side0_UniqueIds']
    mca = row['ViewData.Mapped Custodian Account']
    #print(mca)
    fund = row['ViewData.Fund']
    td = row['ViewData.Trade Date']
    sd = row['ViewData.Settle Date']
    price = row['ViewData.Price']
    cusip = row['ViewData.CUSIP']
    isin = row['ViewData.ISIN']
    qn = row['ViewData.Quantity']
    asset = row['ViewData.Asset Type Category']
    ttype = row['ViewData.Transaction Type']
    amt = row['ViewData.Net Amount Difference Absolute']
    
    #qn = row['ViewData.Quantity']
    shape = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')].shape[0]
    k = list(sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td ) & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')].values)
    if shape==1:
        rowlist = list(row.values)
        onerow.append(rowlist)
        onerow.append(k)
        onerow1.append(onerow)
        itemlist.append(id1)
        itemlist.append(shape)
        itemlist1.append(itemlist)
    else: 
        shape1 = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')].shape[0]
        dfk = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')]
        
        if shape1==1:
            itemlist.append(id1)
            itemlist.append(shape1)
            itemlist1.append(itemlist)
        else:
            
            shape2 = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')& (sideB['ViewData.Transaction Type'] == ttype)].shape[0]
            if shape2 == 1:
                itemlist.append(id1)
                itemlist.append(shape2)
                itemlist1.append(itemlist)
            else:
                shape3 = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')& (sideB['ViewData.Transaction Type'] == ttype) & (sideB['ViewData.Net Amount Difference Absolute'] == amt)].shape[0]
                itemlist.append(id1)
                itemlist.append(shape3)
                itemlist1.append(itemlist) 
#             ViewData.Net Amount Difference
#             k1 = assetmatch(asset,asscat1,dfk)
#             k2 = ttypematch(ttype,ttype2,dfk)            
#             if (k1>k2):
#                 itemlist.append(id1)
#                 itemlist.append(k1)
#                 itemlist1.append(itemlist)
#             else:
#                 itemlist.append(id1)
#                 itemlist.append(k2)
#                 itemlist1.append(itemlist)
   
        else:
           
    


# In[346]:


itemlist1 = []
onerow1 = []
for i, row in sideA1.iterrows():
    onerow = []
    itemlist = []
    id1 = row['ViewData.Side0_UniqueIds']
    mca = row['ViewData.Mapped Custodian Account']
    #print(mca)
    fund = row['ViewData.Fund']
    td = row['ViewData.Trade Date']
    sd = row['ViewData.Settle Date']
    price = row['ViewData.Price']
    cusip = row['ViewData.CUSIP']
    isin = row['ViewData.ISIN']
    qn = row['ViewData.Quantity']
    asset = row['ViewData.Asset Type Category']
    ttype = row['ViewData.Transaction Type']
    amt = row['ViewData.Net Amount Difference Absolute']
    
    #qn = row['ViewData.Quantity']
    shape = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & (sideB['updattr'] == 'settle')].shape[0]
    k = list(sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td ) & (sideB['updattr'] == 'settle')].values)
    if shape==1:
        rowlist = list(row.values)
        onerow.append(rowlist)
        onerow.append(k)
        onerow1.append(onerow)
        itemlist.append(id1)
        itemlist.append(shape)
        itemlist1.append(itemlist)
    else: 
        shape1 = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['updattr'] == 'settle')].shape[0]
        dfk = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['updattr'] == 'settle')]
        
        if shape1==1:
            itemlist.append(id1)
            itemlist.append(shape1)
            itemlist1.append(itemlist)
        else:
            
            shape2 = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['updattr'] == 'settle')].shape[0]
            if shape2 == 1:
                itemlist.append(id1)
                itemlist.append(shape2)
                itemlist1.append(itemlist)
            else:
                shape3 = sideB[(sideB['ViewData.Mapped Custodian Account'] == mca) & (sideB['ViewData.Fund'] == fund) & (sideB['ViewData.Trade Date'] == td) & ((sideB['ViewData.Price'] == price) | (sideB['ViewData.ISIN'] == isin) | (sideB['ViewData.Quantity'] == qn) | (sideB['ViewData.CUSIP'] == cusip) ) & (sideB['updattr'] == 'settle') & (sideB['ViewData.Net Amount Difference Absolute'] == amt)].shape[0]
                itemlist.append(id1)
                itemlist.append(shape3)
                itemlist1.append(itemlist) 


# In[347]:


onerow


# In[348]:


side = pd.DataFrame(itemlist1)


# In[339]:


side[side[1]==5]


# In[155]:


sideA1.columns


# In[340]:


pd.set_option('display.max_columns', 500)


# In[200]:


sideA1[sideA1['final_ID']=='55_531727393_Advent Geneva']


# In[201]:


sideB[(sideB['ViewData.Mapped Custodian Account'] == 'GSI_FUT_LF') & (sideB['ViewData.Fund'] == 'LF') & (sideB['ViewData.Trade Date'] == '01-31-2020')   & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')].head(5)


# In[186]:


sideB[(sideB['ViewData.Mapped Custodian Account'] == 'GSI_FUT_LF') & (sideB['ViewData.Fund'] == 'LF') & (sideB['ViewData.Trade Date'] == '04-24-2020')   & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')]['ViewData.Transaction Type'].value_counts()


# In[ ]:


sideB[(sideB['ViewData.Mapped Custodian Account'] == 'GSI_FUT_LF') & (sideB['ViewData.Fund'] == 'LF') & (sideB['ViewData.Trade Date'] == '04-24-2020')   & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')]['ViewData.Transaction Type'].value_counts()


# In[189]:


sideB[(sideB['ViewData.Mapped Custodian Account'] == 'GSI_FUT_LF') & (sideB['ViewData.Fund'] == 'LF') & (sideB['ViewData.Trade Date'] == '04-24-2020')   & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')]['ViewData.Quantity'].value_counts()


# In[188]:


sideB[(sideB['ViewData.Mapped Custodian Account'] == 'GSI_FUT_LF') & (sideB['ViewData.Fund'] == 'LF') & (sideB['ViewData.Trade Date'] == '04-24-2020')   & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')]['ViewData.Price'].value_counts()


# In[187]:


sideB[(sideB['ViewData.Mapped Custodian Account'] == 'GSI_FUT_LF') & (sideB['ViewData.Fund'] == 'LF') & (sideB['ViewData.Trade Date'] == '04-24-2020')   & (sideB['ViewData.InternalComment2'] == 'Up/Down at Investment ID')]['ViewData.CUSIP'].value_counts()


# In[177]:


sideA1[sideA1['final_ID']=='1015_125719350_Advent Geneva']


# #### For Investment ID

# In[206]:


side[1].value_counts()


# #### Settle Date

# In[344]:


side[1].value_counts()


# In[192]:


raman.to_csv('for double side cheking.csv')


# In[154]:


df3 = df2.drop(['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds'], axis = 1)


# ### Looking at all the keys in detail

# In[9]:


df2 = df_833_0.copy()


# In[10]:


df2['ViewData.Status'].value_counts()


# In[11]:


df2.shape


# In[13]:


col = list(df2.columns)


# In[17]:


for item in col:
    x = item.split('.')
    if 'Transaction Type' in x:
        print(item)


# In[18]:


asscat = df2[['CombiningData.CombinedData.Asset Type Category',
'DataSides.0.Asset Type Category','DataSides.1.Asset Type Category','ViewData.Asset Type Category']]

ttype = df2[['CombiningData.CombinedData.Transaction Type',
'DataSides.0.Transaction Type',
'DataSides.1.Transaction Type','KeySet.Keys.Transaction Type',
'ViewData.Transaction Type']]


# In[19]:


asscat.head(4)


# In[20]:


asscat['acc/pb'] = asscat.apply(lambda row: 1 if row['DataSides.0.Asset Type Category']==row['ViewData.Asset Type Category'] else 0 , axis =1)


# In[21]:


asscat['acc/pb'].value_counts()


# In[22]:


asscat[asscat['acc/pb'] == 0]


# In[28]:


asscat1 = asscat[~asscat['DataSides.0.Asset Type Category'].isna()].groupby(['DataSides.0.Asset Type Category','DataSides.1.Asset Type Category'])['acc/pb'].count().reset_index()


# In[29]:


asscat1.shape


# In[106]:


asscat1.sort_values('acc/pb', ascending  = [False]).head(30)


# In[31]:


asscat1['acc/pb'].value_counts()


# In[33]:


ttype.head(4)


# In[34]:


ttype['acc/pb'] = ttype.apply(lambda row: 1 if row['DataSides.0.Transaction Type']==row['DataSides.1.Transaction Type'] else 0 , axis =1)


# In[35]:


ttype['acc/pb'].value_counts()


# In[36]:


ttype1 = ttype[~ttype['DataSides.0.Transaction Type'].isna()].groupby(['DataSides.0.Transaction Type','DataSides.1.Transaction Type'])['acc/pb'].count().reset_index()


# In[37]:


ttype1.shape


# In[43]:


ttype[ttype['DataSides.0.Transaction Type']=='div charge']


# In[87]:


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
            


# In[88]:


ttype['side0'] = ttype['DataSides.0.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[89]:


ttype['side1'] = ttype['DataSides.1.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[90]:


ttype2 = ttype[~ttype['DataSides.0.Transaction Type'].isna()].groupby(['side0','side1'])['acc/pb'].count().reset_index()


# In[102]:


ttype2.sort_values('acc/pb', ascending = [False]).tail(60)


# In[107]:


ttype2 = ttype2[ttype['acc/pb']>5]


# In[94]:


side1ttype = ttype['side1'].value_counts().reset_index()


# In[96]:


side1ttype[(side1ttype['side1']>0) & (side1ttype['side1']<10)] 


# In[69]:


st1 = 'DIV CHRG ON 576'
for item in st1.split(' '):
    if item.isdigit() == True:
        print(item)
    


# In[ ]:





# In[12]:


df2['ViewData.Asset Type Category'].value_counts()


# In[55]:


ttype2.nunique()


# In[351]:


sideA['ViewData.Asset Type Category'].value_counts()


# In[ ]:





# In[188]:


df3.sort_values(['ViewData.Fund','ViewData.Mapped Custodian Account','ViewData.Trade Date'], ascending = [False, True, False]).head(50)


# In[161]:


df3[(df3['ViewData.Mapped Custodian Account'] == 'GSIL_PB_PO') & (df3['ViewData.CUSIP'] == 'G96629103') & (df3['ViewData.Settle Date'] == '01-15-2020') ].head(4)


# In[165]:


df3[(df3['ViewData.Mapped Custodian Account'] == 'GSIL_PB_PO') & (df3['ViewData.CUSIP'] == 'G96629103') & (df3['ViewData.Settle Date'] == '01-15-2020') ]['ViewData.InternalComment2'].value_counts()


# In[172]:


df3[(df3['ViewData.Mapped Custodian Account'] == 'MS_PB_PO') & (df3['ViewData.Transaction Type'] == 'Collateral Posted to ISDA Counterparties') & (df3['ViewData.Settle Date'] == '11-20-2019') ].sort_values('ViewData.Trade Date')


# In[175]:


df3[(df3['ViewData.Mapped Custodian Account'] == 'MS_PB_PO') & (df3['ViewData.Transaction Type'] == 'Collateral Posted to ISDA Counterparties') & (df3['ViewData.Task Business Date'] == '2019-11-20T00:00:00.000Z') ].sort_values('ViewData.Trade Date')


# In[174]:


df3[(df3['ViewData.Mapped Custodian Account'] == 'MS_PB_PO') & (df3['ViewData.Transaction Type'] == 'Collateral Posted to ISDA Counterparties') & (df3['ViewData.Settle Date'] == '11-20-2019') ]['ViewData.Fund']


# In[173]:


'ViewData.Internalremark' in list(df_833.columns)


# In[166]:


df3[(df3['ViewData.Mapped Custodian Account'] == 'MS_PB_PO') & (df3['ViewData.Investment Type'] == 'Cash and Equivalents') & (df3['ViewData.Settle Date'] == '11-20-2019') ]['ViewData.InternalComment2'].value_counts()


# In[163]:


df2[df2['ViewData.InternalComment2']=='Up/Down at Investment ID']


# In[114]:


df2.count()


# In[113]:


df2.sort_values(['ViewData.Fund','ViewData.Mapped Custodian Account','ViewData.Trade Date']).tail(5)


# In[83]:


list(df2.columns)


# In[85]:


columns1 = [
 'ViewData.Currency',
 'ViewData.Account Type',
 'ViewData.Accounting Net Amount',
 
 'ViewData.Age',
 
 'ViewData.Asset Type Category',


 'ViewData.B-P Net Amount',
 
 'ViewData.BreakID',
 'ViewData.Business Date',
 'ViewData.Call Put Indicator',

 'ViewData.Commission',
 'ViewData.CUSIP',
 
 'ViewData.Custodian Account',

 'ViewData.ExpiryDate',
 'ViewData.Fund',
 
 'ViewData.InternalComment2',

 'ViewData.Investment ID',
 'ViewData.Investment Type',

 'ViewData.ISIN',

 'ViewData.Mapped Custodian Account',

 'ViewData.Price',

 'ViewData.Quantity',

 'ViewData.SEDOL',
 'ViewData.Settle Date',
 'ViewData.SPM ID',
 'ViewData.Status',

 'ViewData.Trade Date',
 'ViewData.Trade Expenses',
 
 'ViewData.Transaction Type',

 
 'ViewData.Value Date',
 
 'ViewData.Side0_UniqueIds',
 'ViewData.Side1_UniqueIds',
 'ViewData.Task Business Date',
 'setup',
 'final_ID']


# In[86]:


df4 = df2[columns1]


# In[87]:


df4.head(4)


# In[88]:


df4.sort_values(['ViewData.Fund','ViewData.Mapped Custodian Account','ViewData.Trade Date']).head(5)


# In[ ]:





# In[13]:


comk.count()


# In[14]:


comk = comk[~comk['Pairing'].isna()]


# In[15]:


comk['Single/double'] = comk['Single/double'].fillna('u')
comk['Up/down'] = comk['Up/down'].fillna(2)


# In[62]:


com1 = df_mod1['Cat_y'].value_counts().reset_index()


# In[63]:


com1.head(4)


# In[65]:


comk = pd.merge(comk, com1 , left_on = 'Cat_y', right_on = 'index', how = 'left')


# In[67]:


comk


# In[68]:


comk[comk['Single/double']== 's']['Cat_y_y'].sum()


# In[72]:


comk[comk['Single/double']== 's']['Pairing'].nunique()


# In[73]:


comk[comk['Single/double']== 'd']['Pairing'].nunique()


# In[69]:


comk['Cat_y_y'].sum()


# In[71]:


comk[~comk['Pairing'].isna()]['Cat_y_y'].sum()


# In[74]:


comk1 = comk[comk['Single/double']== 's']


# In[81]:


comk1.head(4)


# In[76]:


comk1 = comk1.drop(['Unnamed: 0','ViewData.InternalComment2','index','Cat_y_y'], axis =1)


# In[78]:


comk1 = comk1.rename(columns= {'Cat_y_x':'Cat_y'})


# In[80]:


comk1 = comk1.reset_index()


# In[82]:


comk1 = comk1.drop(['index'], axis =1)


# In[83]:


df_mod1 = pd.merge( df_mod1 ,comk1 , on = 'Cat_y', how = 'left')


# In[84]:


df_mod1.count()


# #### Double sided transactions other than Up/Down

# In[85]:


df_mod1 = df_mod1[~df_mod1['Up/down'].isna()]


# In[86]:


df_mod1.shape


# In[20]:


comk[(comk['Single/double']=='d') & (comk['Up/down']==0.0)]['Pairing'].nunique()


# In[21]:


comk[(comk['Single/double']=='d') & (comk['Up/down']==0.0)]['Pairing'].shape


# In[31]:


list(set(comk[(comk['Single/double']=='d') & (comk['Up/down']==0.0)]['Pairing']))


# In[43]:


pd.set_option('display.max_columns', 500)


# In[46]:


df1[df1['ViewData.InternalComment2'] == 'Difference of 82659.62 Equity swap settlement booked between MS and Geneva on 11/29'].head(2)


# In[24]:


list(df1.columns)


# In[28]:


pd.set_option('display.max_columns', 500)


# In[29]:


df1[(df1['ViewData.InternalComment2'] == 'Difference of 82659.62 Equity swap settlement booked between MS and Geneva on 11/29') & (~df1['ViewData.Side0_UniqueIds'].isna())].head(3)


# In[22]:


list(set(df1[df1['ViewData.InternalComment2'] == 'Difference of 82659.62 Equity swap settlement booked between MS and Geneva on 11/29']['ViewData.Settle Date']))


# - Unable to understand why comments on the all the transactions are same

# #### lets find match for up/down side

# In[78]:


df1[df1['ViewData.InternalComment2'] == 'Up/Down at Mapped Custodian Account'].shape


# In[81]:


df1[df1['ViewData.InternalComment2'] == 'Up/Down at Mapped Custodian Account']['ViewData.Side0_UniqueIds'].value_counts()


# In[82]:


df1[(df1['ViewData.InternalComment2'] == 'Up/Down at Mapped Custodian Account') & (df1['ViewData.Side0_UniqueIds'] == '415_125689995_Advent Geneva')]


# In[85]:


df1[(df1['ViewData.Price'] == 45.3301)  & (df1['ViewData.Settle Date'] == '01-02-2020') & (df1['ViewData.Transaction Type'] == 'Buy')].shape


# In[69]:


df1[df1['ViewData.InternalComment2'] == 'Up/Down at CUSIP'].head(8)


# In[52]:


df1[(df1['ViewData.Price'] == 3234.000000)  & (df1['ViewData.Settle Date'] == '01-03-2020') & (df1['ViewData.Transaction Type'] == 'Proceeds Sell')].shape


# In[54]:


df1[(df1['ViewData.Price'] == 3234.000000)  & (df1['ViewData.Settle Date'] == '01-03-2020') & (df1['ViewData.Transaction Type'] == 'Proceeds Sell')]['ViewData.Custodian Account'].value_counts()


# In[55]:


df1[(df1['ViewData.Price'] == 3234.000000)  & (df1['ViewData.Settle Date'] == '01-03-2020') & (df1['ViewData.Transaction Type'] == 'Proceeds Sell') & (df1['ViewData.Transaction Type'] == 'Proceeds Sell') & (df1['ViewData.Custodian Account'] == 'GSI_FUT_LF')]


# ### Transaction type categorisation

# In[13]:


df1['ViewData.Transaction Type'].nunique()


# In[19]:


df1['ViewData.Transaction Type'] = df1['ViewData.Transaction Type'].apply(lambda x : str(x).lower())


# In[20]:


ttype = df1['ViewData.Transaction Type'].value_counts().reset_index()


# In[21]:


ttype.to_csv('ttypecleaning.csv')


# ### Model Building

# In[25]:


df = df1.copy()


# In[42]:


pd.set_option('display.max_columns', 500)


# In[41]:


df.head(2)


# In[10]:


df.shape


# In[13]:


df.groupby(['setup','ViewData.Prime Broker'])['InternalComment2'].count().reset_index().sort_values('InternalComment2' , ascending = [False])


# In[20]:


df.groupby(['setup','ViewData.Prime Broker','ViewData.Task Business Date'])['InternalComment2'].count().reset_index().sort_values('InternalComment2' , ascending = [False]).head(50)


# - As an architecture, We must put one sided transactions, all OBs with all important variables selected from View data keys, transaction type and investment type. We will put categories as classes. We take classes with high volume only right now. 
# 
# - Take OBs with comment only
# - Map comments to categories
# - Take only one sided variables
# - Treat all the variables for tree based model
# - Run the model

# - Since alrtchitecture is already in place this time and I have only taken OBs

# #### Comment categories creation and refining

# In[35]:


com = df['comment'].value_counts().reset_index()


# In[36]:


com.head(8)


# -cleaning to categorise comments is a continuous process. right now we are aleardy with aprrox 1200 categories.
# - We will correct spelling mistake related to break category
# - We will combine expenses in opposite direction category
# - We will put all catgories with value less than 10 as unknows.
# - And then we all rest categories we will move to prediction

# In[11]:


text = ['break cleared','ViewData.Prime Broker']


# In[ ]:





# In[450]:


words = text.split(' ')


# In[451]:


breaks = ['brteak','breaks','break','cleared']


# In[452]:


len(list(set(words) & set(breaks)))


# In[39]:


def mapping_cat(row):
    text = row['index']
    number = row['comment']
    words = text.split(' ')
    breaks = ['brteak','breaks','break','cleared']
    expenses = ['expenses', 'expense']
    opposite = ['opposite']
    if len(list(set(words) & set(breaks)))>0:
        category = 'Breaks cleared'
    elif (number<10):
        category = 'Unknown'
    else:
        category = text
    return category


# In[40]:


com['Cat_y'] = com.apply(lambda x :mapping_cat(x), axis = 1)


# In[38]:


com = com.drop('comment', axis = 1)


# In[40]:


df = pd.merge(df, com, left_on = 'comment', right_on ='index', how = 'left')


# In[41]:


list(df.columns)


# In[42]:


columns = df.count().reset_index()


# In[43]:


columns.columns = ['col','count']


# In[44]:


man_col = list(columns[columns['count']>550000]['col'])


# In[45]:


man_col


# In[30]:


sel_man_col = [


 'ViewData.BalancePB',
 'ViewData.BaseBalanceAcc',
 'ViewData.BaseBalancePB',

 'ViewData.Business Date',
 
 'ViewData.Currency',
 'ViewData.Custodian',
 'ViewData.Custodian Account',
 
 'ViewData.Mapped Custodian Account',
 'ViewData.Net Amount Difference',
 'ViewData.Net Amount Difference Absolute',
 'ViewData.Portfolio ID',
 'ViewData.Portolio',
 'ViewData.Settle Date',
 
 'ViewData.Task Business Date',
 'ViewData.Task Knowledge Date',
 'ViewData.Trade Date',
 
 'ViewData.Transaction Type',
 'ViewData.Type',
 'ViewData.Description',
 'ViewData.InternalComment2',
 
 'Cat_y']


# In[31]:


non_man_col = list(columns[columns['count']<=550000]['col'])


# In[32]:


non_man_col


# In[33]:


sel_non_man_col =[
 'ViewData.Account Name',
 'ViewData.Account Type',
 'ViewData.Accounting Net Amount',
 'ViewData.Asset Type Category',

 'ViewData.Base Currency',
 'ViewData.Base Net Amount',
 'ViewData.Bloomberg_Yellow_Key',
 'ViewData.CUSIP',

 'ViewData.Commission',

 'ViewData.FX Rate',
 'ViewData.Fund',

 'ViewData.ISIN',
 'ViewData.Interest Amount',
 'ViewData.Interest Balance',
 'ViewData.Investment ID',
 'ViewData.Investment Type',
 'ViewData.Knowledge Date',
 'ViewData.Legal Entity',

 'ViewData.Price',
 'ViewData.Prime Broker',
 'ViewData.Principal Amount',
 'ViewData.Principal Balance',
 
 'ViewData.Quantity',
 'ViewData.SEDOL',
 'ViewData.Sec Fees',

 'ViewData.Strike Price',
 'ViewData.Ticker',
 'ViewData.Trade Expenses',

 'ViewData.Value Date']


# In[34]:


df_mod = df[sel_man_col + sel_non_man_col]


# In[51]:


df_mod.shape


# In[77]:


df_mod.count()


# In[102]:


list(df_mod.columns)


# In[52]:


Pre_final = ['ViewData.Base Net Amount','ViewData.BalancePB',
 'ViewData.BaseBalanceAcc',
 'ViewData.BaseBalancePB',
 'ViewData.Business Date',
 'ViewData.Currency',
 'ViewData.Custodian',

 'ViewData.Mapped Custodian Account',
 'ViewData.Net Amount Difference',
 'ViewData.Net Amount Difference Absolute',
 
 'ViewData.Portolio',
 'ViewData.Settle Date',
 'ViewData.Task Business Date',
 'ViewData.Task Knowledge Date',
 'ViewData.Trade Date',
 'ViewData.Transaction Type',
'ViewData.Description',
'ViewData.InternalComment2',
 
 'Cat_y',
 
 
 'ViewData.Accounting Net Amount',
 'ViewData.Asset Type Category',
 
 
 'ViewData.CUSIP',
 'ViewData.Commission',
 
 'ViewData.Fund',
 
 
 'ViewData.Investment ID',
 'ViewData.Investment Type',
 'ViewData.Knowledge Date',
 
 'ViewData.Price',
 'ViewData.Prime Broker',

 'ViewData.Quantity',
 
 'ViewData.Sec Fees',
 'ViewData.Strike Price'
 
]


# In[350]:


cat_features = [
 'ViewData.Business Date',
 'ViewData.Currency',
 'ViewData.Custodian',

 'ViewData.Mapped Custodian Account',
 
 
 'ViewData.Portolio',
 'ViewData.Settle Date',
 'ViewData.Task Business Date',
 'ViewData.Task Knowledge Date',
 'ViewData.Trade Date',
 'ViewData.Transaction Type',
 

 
 

 'ViewData.Asset Type Category',
 
 
 'ViewData.CUSIP',
 
 
 'ViewData.Fund',
 
 
 'ViewData.Investment ID',
 'ViewData.Investment Type',
 'ViewData.Knowledge Date',
 
 
 'ViewData.Prime Broker',

 ]


# In[53]:


df_mod1 = df_mod[Pre_final]


# In[54]:


df_mod1.shape


# In[353]:


pd.set_option('display.max_columns', 100)


# In[354]:


df_mod1.head(5)


# In[314]:


'Abhijeet'.lower()


# In[318]:


list1 = []
sentence = "You are a bad boy"


# In[321]:


sentence.lower()


# In[319]:


list1.append(sentence.lower())


# In[320]:


list1


# In[356]:


df_mod1['up'] = df_mod1['ViewData.InternalComment2'].apply(lambda x: 1 if 'up/down' in  x.lower().split(' ') else 0 )


# In[357]:


df_mod1['up'].value_counts()


# In[324]:


df_mod1[df_mod1['up'] == 1]


# In[358]:


def updown(x):
    emp = ''
    a = ['up/down','at']
    x1 = x.lower().split(',')[0]
    for item in x1.split(' '):
        if item not in a:
            emp = emp + " " + item
    return emp     


# In[359]:


df_mod1['upcat'] = df_mod1['ViewData.InternalComment2'].apply(lambda x: updown(x))


# In[360]:


df_mod1['upcat'].value_counts()


# In[329]:


df_mod1[df_mod1['up']==1]['upcat'].value_counts()


# In[ ]:


dfk2 = df_mod1[df_mod1['Cat_y']==


# In[307]:


val = df_mod1.groupby(['ViewData.Currency','ViewData.Mapped Custodian Account','ViewData.Trade Date'])['ViewData.Settle Date'].count().reset_index()


# In[311]:


val.shape


# In[310]:


val[val['ViewData.Settle Date']>10].shape


# In[288]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') ].count()


# In[289]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') ]['ViewData.Trade Date'].value_counts()


# In[291]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020')].count()


# In[292]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020')]['ViewData.Investment Type'].value_counts()


# In[293]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020')]['ViewData.Prime Broker'].value_counts()


# In[294]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020')]['ViewData.Transaction Type'].value_counts()


# In[297]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020') & (df_mod1['ViewData.Transaction Type']=='RRPLD')]['ViewData.InternalComment2'].value_counts()


# In[298]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020') & (df_mod1['ViewData.Transaction Type']=='RRPLD')]['ViewData.Quantity'].value_counts()


# In[300]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020') & (df_mod1['ViewData.Transaction Type']=='RRPLD')]['ViewData.Fund'].value_counts()


# In[302]:


df_mod1[(df_mod1['ViewData.Mapped Custodian Account']=='CITIGM_FI_R1') & (df_mod1['ViewData.Currency']=='USD') & (df_mod1['ViewData.Trade Date']=='01-06-2020') & (df_mod1['ViewData.Transaction Type']=='RRPLD')]['ViewData.BalancePB'].value_counts()


# In[305]:


df_mod1.groupby(['ViewData.Currency','ViewData.Mapped Custodian Account','ViewData.CUSIP','ViewData.Trade Date'])['ViewData.Quantity'].count().reset_index()


# In[333]:


df_mod1[(df_mod1['up'] == 1) & (df_mod1['upcat'] == 'investment id')]


# In[338]:


dfk3 = df_mod1[(df_mod1['up'] == 1) & (df_mod1['upcat'] == ' investment id')] .groupby(['ViewData.Currency','ViewData.Mapped Custodian Account','ViewData.CUSIP','ViewData.Trade Date','ViewData.Investment ID'])['ViewData.Quantity'].count().reset_index().sort_values('ViewData.Quantity',ascending = [False])


# In[ ]:





# In[339]:


dfk3.head(5)


# In[361]:


df_mod1[(df_mod1['up'] == 1) & (df_mod1['upcat'] == ' investment id')] .groupby(['ViewData.Currency','ViewData.Mapped Custodian Account','ViewData.Trade Date','ViewData.Investment ID','ViewData.CUSIP','ViewData.Base Net Amount'])['ViewData.Quantity'].count().reset_index().sort_values('ViewData.Quantity',ascending = [False]).head(15)


# In[344]:


df_mod1[(df_mod1['ViewData.Currency']=='GBP') & (df_mod1['ViewData.Mapped Custodian Account']=='GSI_FUT_LF') & (df_mod1['ViewData.Trade Date']=='01-31-2020') & (df_mod1['ViewData.Investment ID']=='Z H0') & (df_mod1['ViewData.CUSIP']=='00F7D1RN6') & (df_mod1['ViewData.BalancePB']==-42508.08)]


# In[362]:


df[(df['ViewData.Currency']=='GBP') & (df['ViewData.Mapped Custodian Account']=='GSI_FUT_LF') & (df['ViewData.Trade Date']=='01-31-2020') & (df['ViewData.Investment ID']=='Z H0') & (df['ViewData.CUSIP']=='00F7D1RN6') & (df['ViewData.Base Net Amount']==69.5)]


# In[378]:


list(df_mod1.columns)


# In[ ]:





# In[130]:


df_mod1['Cat_y'].nunique()


# ### Some EDA on Variables that go into the model

# In[182]:


ttype = pd.pivot_table(df_mod1, values= 'ViewData.Sec Fees', index= 'ViewData.Transaction Type', columns='Cat_y', aggfunc='count', fill_value=0, margins=False, dropna=True, margins_name='All').reset_index()


# In[184]:


ttype.head(4)


# In[190]:


ttype['#ttype']= (ttype == 0).astype(int).sum(axis=1)


# In[192]:


ttype['ViewData.Transaction Type'].nunique()


# In[195]:


df_mod1['Cat_y'].nunique()


# In[191]:


ttype['#ttype'].value_counts()


# In[197]:


ttype[ttype['#ttype']==550]['ViewData.Transaction Type']


# In[205]:


cat_list =list(set(df_mod1['ViewData.Transaction Type']))


# In[211]:


df_mod1['Cat_y'].value_counts()


# In[212]:


df_mod1 = df_mod1[df_mod1['Cat_y'] != 'Breaks cleared']


# In[213]:


dff2 = []
for item in cat_list:
    dff1 = []
    a = len(list(set(df_mod1[df_mod1['ViewData.Transaction Type']==item]['Cat_y'])))
    dff1.append(item)
    dff1.append(a)
    dff2.append(dff1)


# In[214]:


dff3 = pd.DataFrame(dff2)


# In[215]:


dff3[1].value_counts()


# In[216]:


dff3.columns = ['ttype', '#cat']


# In[243]:


dff3[dff3['#cat']>6].sort_values('#cat', ascending = [False])


# In[282]:


df_mod1[df_mod1['Cat_y']=='reflecting cash moment']['ViewData.InternalComment2'].iat[1]


# In[283]:


df_mod1[df_mod1['Cat_y']=='reflecting cash moment']['ViewData.Description'].iat[1]


# In[284]:


df_mod1[df_mod1['Cat_y']=='reflecting cash moment']['ViewData.Transaction Type'].iat[1]


# In[280]:


df_mod1[df_mod1['Cat_y']=='difference transfer wire usbk']


# In[247]:


list(set(df_mod1[df_mod1['ViewData.Transaction Type']=='Deposit']['Cat_y']))


# In[233]:


pd.set_option('display.max_columns', 100)


# In[242]:


df_mod1[(df_mod1['ViewData.Transaction Type']=='EQUITY SWAP LONG FINANCING') & (df_mod1['Cat_y']=='expenses opposite')].head(5)


# In[239]:


df_mod1[(df_mod1['ViewData.Transaction Type']=='EQUITY SWAP LONG FINANCING') & (df_mod1['Cat_y']=='difference equity swap settlement')]['ViewData.InternalComment2'].iat[1]


# In[234]:


df_mod1[(df_mod1['ViewData.Transaction Type']=='EQUITY SWAP LONG FINANCING') & (df_mod1['Cat_y']=='currency')]


# In[248]:


ttype.head(4)


# In[263]:


cat_list = list(set(ttype.columns))


# In[264]:


dff2 = []
for item in cat_list:
    dff1 = []
    a = len(list(set(df_mod1[df_mod1['Cat_y']==item]['ViewData.Transaction Type'])))
    dff1.append(item)
    dff1.append(a)
    dff2.append(dff1)


# In[265]:


dff3 = pd.DataFrame(dff2)


# In[266]:


dff3.columns = ['col','count']


# In[267]:


dff3['count'].value_counts()


# In[268]:


dff3[dff3['count']>10]


# In[272]:


list(df_mod1.columns)


# In[271]:


list(set(df_mod1[df_mod1['Cat_y']=='trade date']['ViewData.Transaction Type']))


# In[273]:


list(set(df_mod1[df_mod1['Cat_y']=='trade date']['ViewData.Currency']))


# In[274]:


list(set(df_mod1[df_mod1['Cat_y']=='trade date']['ViewData.Mapped Custodian Account']))


# In[277]:


list(set(df_mod1[df_mod1['Cat_y']=='trade date']['ViewData.Description']))


# In[ ]:





# In[255]:


ttype.astype(bool).sum(axis=0).reset_index()[0].value_counts()


# In[ ]:





# #### Files to be shared with Raghu : One time code

# In[131]:


com_w_cat = df_mod1.groupby('Cat_y').head(1)


# In[132]:


com_w_cat.shape


# In[133]:


com_w_cat.head(4)


# In[134]:


com_w_cat.columns


# In[135]:


com_w_cat = com_w_cat[['ViewData.InternalComment2','Cat_y']]


# In[136]:


com_w_cat.to_csv('category with comment example.csv')


# In[137]:


des = df_mod1['ViewData.Description'].value_counts().reset_index()


# In[138]:


des.head(4)


# In[139]:


des.to_csv('unique description weiss.csv')


# In[140]:


Ttype = df_mod1['ViewData.Transaction Type'].value_counts().reset_index()


# In[141]:


Ttype.head(4)


# In[142]:


Ttype.to_csv('unique transaction type weiss.csv')


# #### Actual code starts here

# In[81]:


df_mod1['Cat_y'].value_counts().reset_index().shape


# In[668]:


df_mod1['Cat_y'].value_counts().reset_index().head(25)


# In[373]:


df_mod1['ViewData.Transaction Type'].value_counts().reset_index()


# In[397]:


pd.set_option('display.max_columns', 100)


# In[398]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'SPEC Journal']


# In[399]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'SPEC Journal']['ViewData.Description'].value_counts()


# In[375]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'SPEC Journal']['Cat_y'].value_counts()


# In[376]:


df_mod1[(df_mod1['ViewData.Transaction Type']== 'DEPOSIT') | (df_mod1['ViewData.Transaction Type']== 'WITHDRAWAL')]


# In[408]:


df_mod1[(df_mod1['ViewData.Transaction Type']== 'DEPOSIT') | (df_mod1['ViewData.Transaction Type']== 'WITHDRAWAL')]['ViewData.Description'].value_counts()


# In[401]:


df_mod1[(df_mod1['ViewData.Transaction Type']== 'DEPOSIT') | (df_mod1['ViewData.Transaction Type']== 'WITHDRAWAL')]['Cat_y'].value_counts()


# In[701]:


Eq_swap = ["EQUITY SWAP LONG FINANCING", "EQUITY SWAP LONG PERFORMANCE", "EQUITY SWAP SHORT FINANCING", "EQUITY SWAP SHORT PERFORMANCE", "EQUITY SWAP SHORT DIVIDEND", "EQUITY SWAP LONG DIVIDEND", "EQUITY SWAP RESET PAYMENT", "EQUITY SWAP LONG FEE", "EQUITY SWAP" ]


# In[702]:


df_mod2 = df_mod1[df_mod1['ViewData.Transaction Type'].isin(Eq_swap)]


# In[703]:


df_mod2.shape


# In[704]:


df_mod2['ViewData.Description'].value_counts()


# In[705]:


df_mod2['Cat_y'].value_counts()


# In[411]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'SpotFX']['Cat_y'].value_counts()


# In[413]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'SpotFX']['ViewData.Description'].value_counts()


# In[412]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'ForwardFX']['Cat_y'].value_counts()


# In[414]:


df_mod1[df_mod1['ViewData.Transaction Type']== 'ForwardFX']['ViewData.Description'].value_counts()


# In[706]:


pd.pivot_table(df_mod2, values= 'ViewData.BalancePB', index= 'ViewData.Description', columns='Cat_y', aggfunc='count', fill_value= 0, margins=False, dropna=True, margins_name='All').reset_index().head(3)


# In[369]:


df_mod1['Cat_y'].value_counts().reset_index()


# In[ ]:





# In[362]:


df_mod1.count()


# - Removal of NA values

# In[87]:


df_mod1['ViewData.Business Date'] = df_mod1['ViewData.Business Date'].fillna(0)
df_mod1['ViewData.Custodian'] = df_mod1['ViewData.Custodian'].fillna('AA')
df_mod1['ViewData.Portolio'] = df_mod1['ViewData.Portolio'].fillna('bb')
df_mod1['ViewData.Settle Date'] = df_mod1['ViewData.Settle Date'].fillna(0)
df_mod1['ViewData.Trade Date'] = df_mod1['ViewData.Trade Date'].fillna(0)
df_mod1['ViewData.Accounting Net Amount'] = df_mod1['ViewData.Accounting Net Amount'].fillna(0)
df_mod1['ViewData.Asset Type Category'] = df_mod1['ViewData.Asset Type Category'].fillna('CC')
df_mod1['ViewData.CUSIP'] = df_mod1['ViewData.CUSIP'].fillna('DD')
df_mod1['ViewData.Fund'] = df_mod1['ViewData.Fund'].fillna('EE')
df_mod1['ViewData.Investment ID'] = df_mod1['ViewData.Investment ID'].fillna('FF')
df_mod1['ViewData.Investment Type'] = df_mod1['ViewData.Investment Type'].fillna('GG')
df_mod1['ViewData.Knowledge Date'] = df_mod1['ViewData.Knowledge Date'].fillna(0)
df_mod1['ViewData.Price'] = df_mod1['ViewData.Price'].fillna(0)
df_mod1['ViewData.Prime Broker'] = df_mod1['ViewData.Prime Broker'].fillna("HH")
df_mod1['ViewData.Quantity'] = df_mod1['ViewData.Quantity'].fillna(0)
df_mod1['ViewData.Sec Fees'] = df_mod1['ViewData.Sec Fees'].fillna(0)
df_mod1['ViewData.Strike Price'] = df_mod1['ViewData.Strike Price'].fillna(0)
df_mod1['ViewData.Commission'] = df_mod1['ViewData.Commission'].fillna(0)
df_mod1['ViewData.Transaction Type'] = df_mod1['ViewData.Transaction Type'].fillna('kk')


# In[88]:


df_mod1.to_csv('comment_training_fileV4.csv')


# In[364]:


from catboost import Pool, CatBoostClassifier, cv
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


# In[365]:


x = df_mod1.drop('Cat_y',axis=1)
y = df_mod1['Cat_y']


# In[366]:


xtrain,xtest,ytrain,ytest = train_test_split(x,y,train_size=.9,random_state=1234)


# In[367]:


model = CatBoostClassifier(eval_metric='Accuracy',use_best_model=True,random_seed=42)


# In[368]:


model.fit(xtrain,ytrain,cat_features=cat_features,eval_set=(xtest,ytest))


# In[218]:


mod1 = df[~df['ViewData.InternalComment2'].isna()]


# In[219]:


mod1.shape


# In[27]:


allcom = pd.read_csv('allcomments.csv')


# In[29]:


pd.set_option('display.max_rows', 500)


# In[34]:


allcom['index'].iloc[10]


# In[222]:


allcom = allcom.drop(['sno','ViewData.InternalComment2'], axis = 1)


# In[223]:


allcom.columns = ['ViewData.InternalComment2','Category']


# In[224]:


mod1 = pd.merge(mod1, allcom, on ='ViewData.InternalComment2', how = 'left')


# In[225]:


mod1.columns


# In[226]:


mod1['Category'].value_counts()


# In[227]:


mod1['ViewData.Status'].value_counts()


# In[228]:


mod1 = mod1[mod1['ViewData.Status']=='OB']


# In[229]:


mod1.shape


# In[230]:


mod1['ViewData.Side1_UniqueIds'].count()


# In[231]:


mod1['ViewData.Side0_UniqueIds'].count()


# In[232]:


mod1 = mod1[~mod1['ViewData.Side1_UniqueIds'].isna()]


# In[233]:


mod1.shape


# In[235]:


df['ViewData.Keys'].value_counts()


# In[209]:


df2 = df1[df1['Category'].isin(cat)]


# - Know your columns program and choose basis counts

# In[252]:


forspm = list(df.columns)


# In[320]:


des = []
for item in forspm:
    x = item.split('.')
    if 'Description' in x:
        des.append(item)


# In[321]:


col_cnt1 = []
for item in des:
    col_cnt = []
    cnt = mod1[item].count()
    col_cnt.append(item)
    col_cnt.append(cnt)
    col_cnt1.append(col_cnt)
    


# In[322]:


pd.DataFrame(col_cnt1)


# In[296]:


mod1['DataSides.1.Settle Date'].value_counts()


# In[261]:


mod1['ViewData.CUSIP'].value_counts()


# - Custodian account is missing values everywhere.

# In[323]:


sel_col = ['ViewData.Currency','ViewData.Settle Date','ViewData.CUSIP',
           'ViewData.Transaction Type','ViewData.Trade Date','ViewData.Investment Type',
           'DataSides.1.Net Amount','Differences.1.Net Amount','ViewData.Quantity','ViewData.Description','Category']


# In[324]:


mod2 = mod1[sel_col]


# In[325]:


mod2


# In[327]:


mod2[mod2['Category']=='up/down']['ViewData.Description'].value_counts()


# In[ ]:


mod2[mod2['Category']=='up/down']['ViewData.Description'].value_counts()


# In[307]:


mod2['ViewData.CUSIP'].value_counts()


# In[308]:


mod2['ViewData.Quantity'].value_counts()


# In[33]:


mod2[mod2['Category']=='Buy Trade']


# In[309]:


mod2['Category'].value_counts()


# In[314]:


category = list(set(mod2['Category']))


# In[315]:


category


# In[316]:


category = ['Buy Trade',
 'Billing Fee',
 'up/down',
 'Non Sec USD',
 'CRI Trade',
 
 'Sell Trade',

 'CPN Trade',
 'Cash Repo',

 'CKR Trade',
 'CKP Trade',
 
 'Wire Transfer',
 ]


# In[317]:


mod2 = mod2[mod2['Category'].isin(category)]


# In[318]:


mod2.shape


# In[319]:


mod2.to_csv('commentfile.csv')


# In[213]:


df3.count()


# In[208]:


df3['ViewData.Status'].value_counts()


# In[ ]:





# ## The Fucking Audit trail code

# In[22]:


aua['ViewData.Side0_UniqueIds'].iat[0]


# In[23]:


from tqdm import tqdm 
print('Applying training labels directly..')
for i in tqdm(range(aua.shape[0])):
    s0 = aua['ViewData.Side0_UniqueIds'].iat[i]
    s1 = aua['ViewData.Side1_UniqueIds'].iat[i]
    st = aua['ViewData.Status'].iat[i]
    if (pd.isnull(s0) == False) & (pd.isnull(s1) == False):
           meo.loc[(meo['ViewData.Side0_UniqueIds'] == s0) & (meo['ViewData.Side1_UniqueIds'] == s1), 'training_label'] = st


# In[80]:


meo['ViewData.Status']


# In[81]:


meo['ViewData.Status'].value_counts()


# In[34]:


aua['ViewData.Side0_UniqueIds']


# In[26]:


aua[aua['ViewData.Side0_UniqueIds']=='89_123778915_Advent Geneva']


# In[27]:


meo[meo['ViewData.Side0_UniqueIds']=='89_123778915_Advent Geneva']


# In[28]:


print('Applying training labels after splitting..')
for i in tqdm(range(aua.shape[0])):
    s0 = aua['ViewData.Side0_UniqueIds'].iat[i]
    s1 = aua['ViewData.Side1_UniqueIds'].iat[i]
    st = aua['ViewData.Status'].iat[i]

    if pd.isnull(s0) == False:
        
        ss0=s0.split(',')
        for st1 in ss0:
            #print('**',st1)
            meo.loc[(meo['ViewData.Side0_UniqueIds'] == st1) & (meo['training_label'].isnull()), 'training_label'] = st

        temp_s0 = meo[(meo['ViewData.Side0_UniqueIds'] == s0)]['ViewData.Side0_UniqueIds']
        if  len(ss0) > 1:
            #print(s0, temp_s0.sum())
            meo.loc[(meo['ViewData.Side0_UniqueIds'] == s0) & (meo['training_label'].isnull()), 'training_label'] = st
            #_ = input()

    if pd.isnull(s1) == False:

        ss1=s1.split(',')
        for st2 in ss1:
            #print('**',st2)
            meo.loc[(meo['ViewData.Side1_UniqueIds'] == st2) & (meo['training_label'].isnull()), 'training_label'] = st

        temp_s1 = meo[(meo['ViewData.Side1_UniqueIds'] == s1)]['ViewData.Side1_UniqueIds']
        if len(ss1) > 1:
            #print(s1, temp_s1.sum())
            meo.loc[(meo['ViewData.Side1_UniqueIds'] == s1) & (meo['training_label'].isnull()), 'training_label'] = st


# In[29]:


meo['training_label']


# In[30]:


meo.head(4)


# ## Code for audit trail generation

# In[36]:


meo = meo.reset_index()
meo = meo.drop('index', 1)
aua = aua.reset_index()
aua = aua.drop('index', 1)


# In[37]:


pd.set_option('display.max_columns', 500)


# In[38]:


aua[aua['MetaData.1._ParentID']=='5dd75f2c1554580508dd646e']


# In[124]:


aua[aua['MetaData.0._ParentID']=='5dd7ab364333822d70d69215']


# In[125]:


meo.nunique()


# In[126]:


meo.shape


# In[62]:


auak[(auak['ViewData.Side0_UniqueIds'] == '88_123615210_Advent Geneva') & (auak['ViewData.Side1_UniqueIds'] == '175_123615210_CITI')]


# 

# In[ ]:





# In[ ]:





# In[ ]:





# ### Generation of Audit Trail

# In[96]:


from tqdm import tqdm 


# In[99]:


aua.shape


# In[100]:


meo.shape


# In[127]:


meo.count()


# In[128]:


meo1 = meo.copy()


# In[129]:


meo1['MetaData.1._RecordID'] = meo1['MetaData.1._RecordID'].fillna(0.0)
meo1['MetaData.0._RecordID'] = meo1['MetaData.0._RecordID'].fillna(0.0)


# In[136]:


meo1['Key'] = meo1['MetaData.0._RecordID'].astype(int).astype(str) + '_' + meo1['TaskInstanceID'].astype(str) + '_' + meo1['MetaData.1._RecordID'].astype(int).astype(str)


# In[137]:


k2 = meo1['Key'].value_counts().reset_index()


# In[139]:


k2.columns = ['Key', 'Key_val']


# In[140]:


meo1 = pd.merge(meo1, k2, on = 'Key', how = 'left')


# In[143]:


meo11 = meo1[meo1['Key_val']>1].reset_index()
meo12 = meo1[meo1['Key_val']==1].reset_index()


# In[103]:


final_df = []

for i in tqdm(range(meo.shape[0])):
    first_id = meo.loc[i,'ViewData._ID']
    
    id_array = []
    
    id1 = aua.loc[(aua['MetaData.0._ParentID'] ==first_id), 'ViewData._ID']
    
    id2 = aua.loc[(aua['MetaData.1._ParentID'] ==first_id), 'ViewData._ID']
    if id1.isnull().all() ==False:
        id1 = id1.values[0]
    else:
        id1 ='NAN'
        
    if id2.isnull().all() ==False:
        id2 = id2.values[0]
    else:
        id2 ='NAN'

    #print(id1)
    #print(id2)
    id_array.append(id1)
    id_array.append(id2)
    #print(id_array)
    
    id_array = [word for word in id_array if word!='NAN']
    if id_array ==[]:
        print('Single MEO')
    else:
    
        for j in range(200):
            if ((aua[aua['MetaData.0._ParentID'] ==id_array[j]])).empty and ((aua[aua['MetaData.1._ParentID'] ==id_array[j]])).empty :
                

            else:
                if ((aua[aua['MetaData.0._ParentID'] ==id_array[j]])).empty:
                    
                    value = aua[aua['MetaData.1._ParentID'] ==id_array[j]]['ViewData._ID'].values[0]
                else:
                    value = aua[aua['MetaData.0._ParentID'] ==id_array[j]]['ViewData._ID'].values[0]
            id_array.append(value)

    df = pd.concat([meo[meo['ViewData._ID']==first_id],aua[aua['ViewData._ID'].isin(id_array)]], axis=0)
    df['example_num'] = i
    final_df.append(df)
    
final_df = pd.concat(final_df)    
#final_array = [first_id] + id_array


# In[104]:


final_df.head(4)


# In[108]:


final_df.columns


# In[105]:


count_table = final_df['example_num'].value_counts().reset_index()


# In[106]:


count_table.columns = ['example_num', 'freq']


# In[107]:


count_table[count_table['freq']>1]


# In[109]:


final_df[final_df['example_num']==0]


# - SPM why one sided?
# - Why UMR is getting disconnected?
# - why comments on all OB?
# - Settle date and trade date diffference
# 
# 
# - Update on the Pairing
# - Update on commenting
# - Update on engineering
# 
# 
# 

# In[34]:


get_ipython().system('pip install catboost')


# ### Analysis of all kinds of source files

# #### BARC Files

# In[7]:


df1 = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\Raman Weiss Data\BARC\vitftp01_PM0120C2-3_S_430965_20200207.csv')
df2 = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\Raman Weiss Data\BARC\vitftp01_PM0120C2-3_S_430965_20200210.csv')
df3 = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\Raman Weiss Data\BARC\vitftp01_PM0120C2-3_S_430965_20200211.csv')
df4 = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\Raman Weiss Data\BARC\vitftp01_PM0120C2-3_S_430965_20200212.csv')
df5 = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\Raman Weiss Data\BARC\vitftp01_PM0120C2-3_S_430965_20200213.csv')
df6 = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\Raman Weiss Data\BARC\vitftp01_PM0120C2-3_S_430965_20200225.csv')


# In[8]:


frames = [df1,df2,df3,df4,df5,df6]


# In[9]:


df = pd.concat(frames)


# In[10]:


df.head(4)


# In[11]:


df.shape


# In[12]:


list(df.columns)


# In[13]:


dfk = df[['Activity Description','Journal Code']]


# In[14]:


dfk.head(4)


# In[18]:


Ad = list(set(dfk['Journal Code']))


# In[26]:


Ad


# In[24]:


'DIV_FEE' in Ad


# In[25]:


'CPN_FEE' in Ad


# - For BARC,Final Remark is to used Journal Code and Activity Description
# - For CITI and CITI FI, columns AD and Q, Trans2 and Extract Trans
# - For GS file,Citerion is not clear
# - If Tran Type from CITI begins with words "CASH DIV ON" and Investment Type from CITI has "EQUITY", comment added "CITI booked the DVD on <Trade Date> for pattern based Dividend commnent
# - Better Undeerstand the lookup criteria for Dividend.

# - Could not find comments related to difference in equity swap settlement
# - Suppose Interest is overlapping between lookup and ML based then what is the criteria of this overall. Is it broker to broker different?
# - How to map Journal code and Activity Description to main file, from BARC?

# In[ ]:




