#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import os


# In[3]:


import dask.dataframe as dd
import glob


# - 'up/down at mapped custodian account'
# - 'up/down at currency'

# ### Receiving No pairs, looking up in MEO and prepration of comment file

# In[4]:


final_no_pair_table_copy =pd.read_csv('Arjun/final_no_pair_table_soros.csv')


# In[5]:


brk = final_no_pair_table_copy[final_no_pair_table_copy['Predicted_action'] == 'No-Pair']


# In[277]:


brk.shape


# In[6]:


meo_df = pd.read_csv('Arjun/meo_df_soros.csv')


# In[8]:


brk = brk.rename(columns ={'Side0_UniqueIds':'ViewData.Side0_UniqueIds',
                         'Side1_UniqueIds':'ViewData.Side1_UniqueIds'})


# In[92]:


meo_df = meo_df.rename(columns ={'ViewData.B-P Net Amount':'ViewData.Cust Net Amount'
                         })


# In[93]:


brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].fillna('AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].fillna('BB')


# In[94]:


def fid1(a,b,c):
    if a=='No-Pair':
        if b =='AA':
            return c
        else:
            return b
    else:
        return '12345'


# In[95]:


brk['final_ID'] = brk.apply(lambda row : fid1(row['Predicted_action'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1 )


# In[96]:


side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))


# In[97]:


meo1 = meo_df[meo_df['ViewData.Side0_UniqueIds'].isin(side0_id)]
meo2 = meo_df[meo_df['ViewData.Side1_UniqueIds'].isin(side1_id)]


# In[98]:


frames = [meo1, meo2]


# In[99]:


df1 = pd.concat(frames)
df1 = df1.reset_index()
df1 = df1.drop('index', axis = 1)


# ### Duplicate OB removal

# In[100]:


df1 = df1.drop_duplicates()


# In[102]:


df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')


# In[103]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b
        


# In[104]:


df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[105]:


pd.set_option('display.max_columns', 500)


# In[106]:


df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])


# In[107]:


uni2 = df1.groupby(['final_ID','ViewData.Task Business Date']).last().reset_index()


# In[108]:


uni2 = uni2.sort_values(['final_ID','ViewData.Task Business Date'], ascending = [True, True])


# #### Trade date vs Settle date and future dated trade

# In[109]:


df2 = uni2.copy()


# In[276]:


df2.shape


# In[111]:


import datetime


# In[112]:


df2['ViewData.Settle Date'] = pd.to_datetime(df2['ViewData.Settle Date'])
df2['ViewData.Trade Date'] = pd.to_datetime(df2['ViewData.Trade Date'])
df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])


# In[113]:


df2['ViewData.Task Business Date1'] = df2['ViewData.Task Business Date'].dt.date


# In[114]:


df2['ViewData.Settle Date1'] = df2['ViewData.Settle Date'].dt.date
df2['ViewData.Trade Date1'] = df2['ViewData.Trade Date'].dt.date


# In[115]:


df2['ViewData.SettlevsTrade Date'] = (df2['ViewData.Settle Date1'] - df2['ViewData.Trade Date1']).dt.days
df2['ViewData.SettlevsTask Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Settle Date1']).dt.days
df2['ViewData.TaskvsTrade Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Trade Date1']).dt.days


# ### Cleannig of the 4 variables in this

# In[116]:


df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')


# In[117]:


def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys


# In[118]:


df['tuple'] = df.apply(make_dict, axis=1)


# In[119]:


clean_map_dict = df.set_index('tuple')['Value'].to_dict()


# In[121]:


df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Asset Type Category'] = df2['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Investment Type'] = df2['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Prime Broker'] = df2['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)


# In[122]:


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
        


# In[123]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)


# In[124]:


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


# In[125]:



import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[126]:


def comb_clean(x):
    k = []
    for item in x.split():
        if ((is_num(item)==False) and (is_date_format(item)==False) and (date_edge_cases(item)==False)):
            k.append(item)
    return ' '.join(k)


# In[127]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[128]:


df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[129]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : 'paydown' if x=='pay down' else x)


# In[ ]:


def divclient(x):
    if (type(x) == str):
        if ('eqswap div client tax' in x) :
            return 'eqswap div client tax'
        else:
            return x
    else:
        return 'float'


# In[ ]:



def mhreplace(item):
    item1 = item.split()
    for items in item1:
        if items.endswith('mh')==True:
            item1.remove(items)
    return ' '.join(item1).lower()


# In[ ]:


def compname(x):
    m = 0
    comp = ['corporate','stk','inc','lp','plc','inc.','inc','corp']
    if type(x)==str:
        x1 = x.split()
        for item in x1:
            if item in comp:
                m = m+1
    else:
        m = 0
    
    if m ==0:
        return x
    else:
        return 'Company'


# In[ ]:


def wht(x):
    if type(x)==str:
        x1 = x.split()
        if x1[0] =='30%':
            return 'Wht'
        else:
            return x
    else:
        return x


# In[ ]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : divclient(x))
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : mhreplace(x))
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : compname(x))
df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : divclient(x))


# In[ ]:





# In[ ]:


df2['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply( lambda x : item[:2] if '30%' in x else x)


# ### Cleaning of Description

# In[131]:


com = pd.read_csv('desc cat with naveen.csv')


# In[132]:


cat_list = list(set(com['Pairing']))


# In[133]:


import re


# In[134]:



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
            


# In[135]:


df2['desc_cat'] = df2['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[136]:


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
        


# In[137]:


df2['desc_cat'] = df2['desc_cat'].apply(lambda x : currcln(x))


# In[138]:


com = com.drop(['var','Catogery'], axis = 1)


# In[139]:


com = com.drop_duplicates()


# In[140]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[141]:


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


# In[142]:


df2['new_desc_cat'] = df2['desc_cat'].apply(lambda x : catcln1(x,com))


# In[143]:


comp = ['inc','stk','corp ','llc','pvt','plc']
df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[144]:


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


# In[145]:


df2['new_desc_cat'] = df2['new_desc_cat'].apply(lambda x : desccat(x))


# #### Prime Broker Creation

# In[146]:


df2['new_pb'] = df2['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[147]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[148]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[149]:


df2['new_pb'] = df2['new_pb'].apply(lambda x : new_pf_mapping(x))


# In[150]:


df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].fillna('kkk')


# In[151]:


df2['new_pb1'] = df2.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)


# In[152]:


df2['new_pb1'] = df2['new_pb1'].apply(lambda x : x.lower())


# #### Cancelled Trade Removal

# In[153]:


#trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[154]:


#dfkk = df2[df2['ViewData.Transaction Type1'].isin(trade_types)]


# In[155]:


#dfk_nontrade = df2[~df2['ViewData.Transaction Type1'].isin(trade_types)]


# In[157]:


#dffk2 = dfkk[dfkk['ViewData.Side0_UniqueIds']=='AA']
#dffk3 = dfkk[dfkk['ViewData.Side1_UniqueIds']=='BB']


# In[158]:


#dffk4 = dfk_nontrade[dfk_nontrade['ViewData.Side0_UniqueIds']=='AA']
#dffk5 = dfk_nontrade[dfk_nontrade['ViewData.Side1_UniqueIds']=='BB']


# #### Geneva side

# In[160]:


def canceltrade(x,y):
    if x =='buy' and y>0:
        k = 1
    elif x =='sell' and y<0:
        k = 1
    else:
        k = 0
    return k


# In[161]:


#dffk3['cancel_marker'] = dffk3.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Accounting Net Amount']), axis = 1)


# In[163]:


def cancelcomment(x,y):
    com1 = 'This is original of cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[164]:


def cancelcomment1(x,y):
    com1 = 'This is cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[165]:


# if dffk3[dffk3['cancel_marker'] == 1].shape[0]!=0:
#     cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))
#     if len(cancel_trade)>0:
#         km = dffk3[dffk3['cancel_marker'] != 1]
#         original = km[km['ViewData.Transaction ID'].isin(cancel_trade)]
#         original['predicted category'] = 'Original of Cancelled trade'
#         original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancellation = dffk3[dffk3['cancel_marker'] == 1]
#         cancellation['predicted category'] = 'Cancelled trade'
#         cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancel_fin = pd.concat([original,cancellation])
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#         cancel_fin = cancel_fin[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing p1.csv')
#         dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
        
#     else:
#         cancellation = dffk3[dffk3['cancel_marker'] == 1]
#         cancellation['predicted category'] = 'Cancelled trade'
#         cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)
#         cancel_fin = pd.concat([original,cancellation])
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#         cancel_fin = cancel_fin[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing no original p2.csv')
#         dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]
# else:
#     dffk3 = dffk3.copy()
        
        


# #### Broker side

# In[166]:


#dffk2['cancel_marker'] = dffk2.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Cust Net Amount']), axis = 1)


# In[168]:


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
    
        


# In[169]:


from pandas import merge
from tqdm import tqdm


# In[170]:


def cancelcomment2(y):
    com1 = 'This is original of cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' '  + com2 +' ' + str(y)
    return com


# In[171]:


def cancelcomment3(y):
    com1 = 'This is cancelled trade'
    com2 = 'on settle date'
    com = com1 + ' ' + com2 + ' ' + str(y)
    return com


# In[172]:


# if dffk2[dffk2['cancel_marker'] == 1].shape[0]!=0:
#     dummy1 = dffk2[dffk2['cancel_marker']!=1]
#     dummy1 = dffk2[dffk2['cancel_marker']==1]


#     pool =[]
#     key_index =[]
#     training_df =[]
#     call1 = []

#     appended_data = []

#     no_pair_ids = []
# #max_rows = 5

#     k = list(set(list(set(dummy['ViewData.Task Business Date1']))))
#     k1 = k

#     for d in tqdm(k1):
#         aa1 = dummy[dummy['ViewData.Task Business Date1']==d]
#         bb1 = dummy1[dummy1['ViewData.Task Business Date1']==d]
#         aa1['marker'] = 1
#         bb1['marker'] = 1
    
#         aa1 = aa1.reset_index()
#         aa1 = aa1.drop('index',1)
#         bb1 = bb1.reset_index()
#         bb1 = bb1.drop('index', 1)
#         #print(aa1.shape)
#         #print(bb1.shape)
    
#         aa1.columns = ['SideB.' + x  for x in aa1.columns] 
#         bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
#         cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
#         appended_data.append(cc1)
#         cancel_broker = pd.concat(appended_data)
#         cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
#         elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]
#         if elim1.shape[0]!=0:
#             id_listA = list(set(elim1['SideA.final_ID']))
#             c1 = dummy
#             c2 = dummy1[dummy1['final_ID'].isin(id_listA)]
#             c1['predicted category'] = 'Cancelled trade'
#             c2['predicted category'] = 'Original of Cancelled trade'
#             c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
#             c2['predicted comment'] = c2.apply(lambda x : cancelcomment3(x['ViewData.Settle Date1']))
#             cancel_fin = pd.concat([c1,c2])
#             cancel_fin = cancel_fin.reset_index()
#             cancel_fin = cancel_fin.drop('index', axis = 1)
#             sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#             cancel_fin = cancel_fin[sel_col_1]
#             cancel_fin.to_csv('Comment file soros 2 sep testing p3.csv')
#             id_listB = list(set(c1['final_ID']))
#             comb = id_listB + id_listA
#             dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
            
            
            
   
        
#     else:
#         c1 = dummy
#         c1['predicted category'] = 'Cancelled trade'
#         c1['predicted comment'] =  c1.apply(lambda x : cancelcomment2(x['ViewData.Settle Date1']))
#         sel_col_1 = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']
#         cancel_fin = c1[sel_col_1]
#         cancel_fin.to_csv('Comment file soros 2 sep testing no original p4.csv')
#         id_listB = list(set(c1['final_ID']))
#         comb = id_listB
#         dffk2 = dffk2[~dffk2['final_ID'].isin(comb)]
        
# else:
#     dffk2 = dffk2.copy()


# #### Finding Pairs in Up and down

# In[ ]:


dffk2 = aa_new.copy()
dffk3 = bb_new.copy()


# In[173]:


sel_col = ['final_ID',  'ViewData.Currency',
       'ViewData.Accounting Net Amount',
       
       'ViewData.Asset Type Category', 
       'ViewData.Cust Net Amount', 'ViewData.BreakID',
       'ViewData.ClusterID',
       'ViewData.CUSIP', 'ViewData.Description', 'ViewData.Fund',
        'ViewData.Investment ID',
       'ViewData.Investment Type', 
       'ViewData.ISIN', 'ViewData.Keys', 
       'ViewData.Mapped Custodian Account',  'ViewData.Prime Broker',
       
       'ViewData.Quantity','ViewData.Settle Date1', 
       'ViewData.Status', 'ViewData.Strategy', 
       'ViewData.Ticker', 'ViewData.Trade Date1', 
       'ViewData.Transaction Type', 
       'ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds', 
      'ViewData.Task Business Date1','ViewData.InternalComment2','s/d','new_pb1','new_pb2'
      ]


# In[174]:


dffpb = dff2[sel_col]
dffacc = dff3[sel_col]


# In[ ]:


bplist = dffpb.groupby('ViewData.Task Business Date1')['ViewData.Cust Net Amount'].apply(list).reset_index()
acclist = dffacc.groupby('ViewData.Task Business Date1')['ViewData.Accounting Net Amount'].apply(list).reset_index()


# In[ ]:


updlist = pd.merge(bplist, acclist, on = 'ViewData.Task Business Date1', how = 'inner')


# In[ ]:


updlist['upd_amt'] = updlist.apply(lambda x : [value for value in x['ViewData.Cust Net Amount'] if value in x['ViewData.Accounting Net Amount']], axis = 1)


# In[ ]:


updlist = updlist[['ViewData.Task Business Date1','upd_amt']]


# In[ ]:


dffpb = pd.merge(dffpb, updlist, on = 'ViewData.Task Business Date1', how = 'left')
dffacc = pd.merge(dffacc, updlist, on = 'ViewData.Task Business Date1', how = 'left')


# In[ ]:


dffpb['upd_amt']= dffpb['upd_amt'].fillna('MMM')
dffacc['upd_amt']= dffacc['upd_amt'].fillna('MMM')


# In[ ]:


def updmark(y,x):
    if x =='MMM':
        return 0
    else:
        if y in x:
            return 1
        else:
            return 0


# In[ ]:


dffpb['upd_mark'] = dffpb.apply(lambda x :  updmark(x['ViewData.Cust Net Amount'], x['upd_amt']) , axis= 1)
dffacc['upd_mark'] = dffacc.apply(lambda x : updmark(x['ViewData.Accounting Net Amount'], x['upd_amt']) , axis= 1)


# In[ ]:


dff4 = dffpb[dffpb['upd_mark']==1]
dff5 = dffacc[dffacc['upd_mark']==1]


# In[175]:


#dff6 = dffk4[sel_col]
#dff7 = dffk5[sel_col]


# In[176]:


# dff4 = pd.concat([dff4,dff6])
# dff4 = dff4.reset_index()
# dff4 = dff4.drop('index', axis = 1)


# In[177]:


# dff5 = pd.concat([dff5,dff7])
# dff5 = dff5.reset_index()
# dff5 = dff5.drop('index', axis = 1)


# In[ ]:


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
        
    
        
        
        
    return a, b, c ,d


# In[ ]:


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


# #### M cross N code

# In[178]:


###################### loop 3 ###############################
from pandas import merge
from tqdm import tqdm
if ((dff4.shape[0]!=0) & (dff5.shape[0]!=0)):
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
        
    df_213_1 = pd.concat(appended_data)
    df_213_1[['map_match','amt_match','fund_match','curr_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")
    df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['fund_match'] + df_213_1['curr_match']
    elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]
    if elim1.shape[0]!=0:
        elim1['SideA.predicted category'] = 'Updown'
        elim1['SideB.predicted category'] = 'Updown'
        elim1['SideA.Predicted_action'] = 'No-Pair'
        elim1['SideB.Predicted_action'] = 'No-Pair'
        elim1['SideA.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
        elim1['SideB.predicted comment'] = elim1.apply(lambda x : updownat(x['map_match'],x['curr_match'],x['sd_match'],x['fund_match'],x['ttype_match']), axis = 1)
        elim_col = ['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment','Predicted_action']
    
    
        elim_col = list(elim1.columns)

        for items in elim_col:
            item = 'SideA.'+items
            sideA_col.append(item)
            item = 'SideB.'+items
            sideB_col.append(item)
        
        elim2 = elim1[sideA_col]
        elim3 = elim1[sideB_col]
    
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
        elim.to_csv('Comment file soros 2 sep testing p5.csv')
        
        ## TODO : Rohit to write elimination code here
        
    else:
        aa_new = aa_new.copy()
        bb_new = bb_new.copy()
else:
    aa_new = aa_new.copy()
    bb_new = bb_new.copy()
    


# #### Start of the single Side Commenting

# In[192]:


data = df2.copy()


# In[193]:


# data = pd.concat(frames)
# data = data.reset_index()
# data = data.drop('index', axis = 1)


# In[196]:


#data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])


# In[197]:


# days = [1,30,31,29]
# data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[198]:


# data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')


# In[199]:


def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k


# In[200]:


#data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))


# In[201]:


data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)


# In[202]:


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


# In[203]:


data = data[Pre_final]


# In[204]:


df_mod1 = data.copy()


# In[205]:


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


# In[206]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[207]:


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[208]:


data2 = df_mod1.copy()


# ### Separate Prediction of the Trade and Non trade

# #### 1st for Non Trade

# In[209]:


#trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[210]:


#data21 = data2[~data2['ViewData.Transaction Type1'].isin(trade_types)]


# In[211]:


cols = [
 


 'ViewData.Transaction Type1',
 
 

 'ViewData.Asset Type Category1',

 'new_desc_cat',

 'ViewData.Investment Type1',
 

 'new_pb2','new_pb1'
 
 
              
             ]


# In[212]:


data211 = data2[cols]


# In[213]:


filename = 'finalized_model_weiss_catrefine_v8.sav'


# In[214]:


import pickle


# In[215]:


clf = pickle.load(open(filename, 'rb'))


# In[216]:


# Actual class predictions
cb_predictions = clf.predict(data211)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]


# #### Testing of Model and final prediction file - Non Trade

# In[217]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[218]:


result_non_trade =data2.copy()


# In[219]:


result_non_trade = result_non_trade.reset_index()


# In[220]:


result_non_trade['predicted category'] = pd.Series(demo)
result_non_trade['predicted comment'] = 'NA'


# In[229]:


#result_non_trade = result_non_trade.drop('predicted comment', axis = 1)


# In[249]:


#com_temp = pd.read_csv('Soros comment template for delivery.csv')


# In[250]:


#com_temp = com_temp.rename(columns = {'Category ':'predicted category','template':'predicted template'})


# In[252]:


#result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')


# In[256]:


def comgen(x,y,z,k):
    if x == 'Geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "Geneva" + ' ' +y + ' ' + str(z)
        
    return com


# In[257]:


#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)


# In[ ]:


result_non_trade = result_non_trade[['final_ID','ViewData.Side0_UniqueIds','ViewData.Side0_UniqueIds','predicted category','predicted comment']]


# In[260]:


result_non_trade.to_csv('Comment file Weiss 2 sep testing p6.csv')


# In[ ]:




