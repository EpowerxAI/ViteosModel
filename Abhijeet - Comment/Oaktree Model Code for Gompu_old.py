#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
os.chdir('D:\\ViteosModel\\Abhijeet - Comment')

# #### addition to pipeline

# - probable updown at fund with wire transfer
# - Removal of duplicate trades
# - Failed trade has is already settled then failed trade has to be rejected.
# - some overlap of failed trade and duplicate trade, yet to be figured out.

# In[2]:


pwd


# In[3]:


import dask.dataframe as dd
import glob


# ### Receiving No pairs, looking up in MEO and prepration of comment file

# In[ ]:


brk = pd.read_csv('1 Month Data for testing/soros/All_june_data_153.csv')


# In[ ]:


brk = brk[brk['Actual_Status'] == 'Open Break']


# In[ ]:


meo_file = pd.read_csv('placeholder.csv')


# In[ ]:


brk['ViewData.Side0_UniqueIds'] = brk['ViewData.Side0_UniqueIds'].fillna('AA')
brk['ViewData.Side1_UniqueIds'] = brk['ViewData.Side1_UniqueIds'].fillna('BB')


# In[ ]:


def fid1(a,b,c):
    if a=='Open Break':
        if b =='AA':
            return c
        else:
            return b
    else:
        return '12345'


# In[ ]:


brk['final_ID'] = brk.apply(lambda row : fid1(row['Actual_Status'],row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1 )


# In[ ]:


side0_id = list(set(brk[brk['ViewData.Side1_UniqueIds'] =='BB']['ViewData.Side0_UniqueIds']))
side1_id = list(set(brk[brk['ViewData.Side0_UniqueIds'] =='AA']['ViewData.Side1_UniqueIds']))


# In[ ]:


meo1 = meo_file[meo_file['ViewData.Side0_UniqueIds'].isin(side0_id)]
meo2 = meo_file[meo_file['ViewData.Side1_UniqueIds'].isin(side1_id)]


# In[ ]:


frames = [meo1, meo2]


# In[ ]:


df1 = pd.concat(frames)


# In[ ]:


df1 = df1.reset_index()


# In[ ]:


df1 = df1.drop('index', axis = 1)


# ### Reading comments file

# In[236]:


df1 = pd.read_csv('combined setup oaktree for commenting  v2.csv')


# In[177]:


#df1 = pd.read_csv('Input for comment prediction/Trail of breaks Only/testaud1906v2.csv')


# In[178]:


df1.info(memory_usage = 'deep')


# In[7]:


##dfk = pd.read_csv(r'\\vitblrdevcons01\Raman  Strategy ML 2.0\ReconDB.HST_RecData_123.csv')


# In[8]:


df1.head(4)


# In[237]:


df1.shape


# In[238]:


df1.head(4)


# #### For 1 month testing column selection

# In[238]:


df1.columns


# In[239]:


df1['ViewData.Status'].value_counts()


# In[1301]:


df1 = df1[df1['Actual_Status'] == 'Open Break']


# In[1302]:


month = pd.read_csv('1 Month Data for testing/soros/1 month soros setup read.csv')


# In[1303]:


month.columns


# In[1304]:


month = month.drop(['Unnamed: 0'],axis =1 )


# In[1305]:


month = month.drop_duplicates()


# In[1306]:


month['ViewData.Side0_UniqueIds'] = month['ViewData.Side0_UniqueIds'].fillna('AA')
month['ViewData.Side1_UniqueIds'] = month['ViewData.Side1_UniqueIds'].fillna('BB')


# In[1307]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[1308]:


month['final_ID'] = month.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[1309]:


month['final_ID'].nunique()


# In[1310]:


month1 = month[['final_ID','ViewData.Net Amount Difference','ViewData.Net Amount Difference Absolute']].drop_duplicates()


# In[1324]:


df1 = pd.merge(df1, month1, on = 'final_ID', how = 'left')


# In[1328]:


df1.count()


# In[1326]:


df1 = df1.drop('ViewData.Net Amount Difference_x' , axis = 1)


# In[1327]:


df1 = df1.rename(columns = {'ViewData.Net Amount Difference_y':'ViewData.Net Amount Difference'})


# ### Duplicate OB removal

# In[405]:


#df1 =dfk1.copy()


# In[239]:


df1.columns


# In[240]:


df1 = df1.drop(['Unnamed: 0'],axis =1 )


# In[241]:


df1 = df1.drop(['Unnamed: 0.1'],axis =1 )


# In[242]:


df1.shape


# In[243]:


df1 = df1.drop_duplicates()


# In[244]:


df1.shape


# In[245]:


df1['ViewData.Side0_UniqueIds'].count()


# In[247]:


df1['ViewData.Side0_UniqueIds'].nunique()


# In[248]:


df1['ViewData.Side1_UniqueIds'].count()


# In[249]:


df1['ViewData.Side1_UniqueIds'].nunique()


# In[246]:


df1['ViewData.Side0_UniqueIds'] = df1['ViewData.Side0_UniqueIds'].fillna('AA')
df1['ViewData.Side1_UniqueIds'] = df1['ViewData.Side1_UniqueIds'].fillna('BB')


# In[15]:


df1.count()


# In[247]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b
        


# In[248]:


df1['final_ID'] = df1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[249]:


df1.shape


# In[94]:


df1['final_ID'].count()


# In[256]:


df1['final_ID'].nunique()


# In[25]:


pd.set_option('display.max_columns', 500)


# In[250]:


df1 = df1.sort_values(['final_ID','ViewData.Business Date'], ascending = [True, True])


# In[251]:


df1.head(5)


# In[32]:


df1.shape


# In[149]:


df1['final_ID'].count()


# In[252]:


uni2 = df1.groupby(['final_ID','ViewData.Task Business Date']).last().reset_index()


# In[253]:


uni2 = uni2.sort_values(['final_ID','ViewData.Task Business Date'], ascending = [True, True])


# #### Trade date vs Settle date and future dated trade

# In[ ]:


df2 = uni2.copy()


# In[40]:


df2.columns


# In[258]:


import datetime


# In[259]:


df2['ViewData.Settle Date'] = pd.to_datetime(df2['ViewData.Settle Date'])
df2['ViewData.Trade Date'] = pd.to_datetime(df2['ViewData.Trade Date'])
df2['ViewData.Task Business Date'] = pd.to_datetime(df2['ViewData.Task Business Date'])


# In[260]:


df2['ViewData.Task Business Date1'] = df2['ViewData.Task Business Date'].dt.date


# In[261]:


df2['ViewData.Settle Date1'] = df2['ViewData.Settle Date'].dt.date
df2['ViewData.Trade Date1'] = df2['ViewData.Trade Date'].dt.date


# In[262]:


df2['ViewData.SettlevsTrade Date'] = (df2['ViewData.Settle Date1'] - df2['ViewData.Trade Date1']).dt.days
df2['ViewData.SettlevsTask Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Settle Date1']).dt.days
df2['ViewData.TaskvsTrade Date'] = (df2['ViewData.Task Business Date1'] - df2['ViewData.Trade Date1']).dt.days


# ### Cleannig of the 4 variables in this

# In[263]:


df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')


# In[264]:


def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys


# In[265]:


df['tuple'] = df.apply(make_dict, axis=1)


# In[266]:


clean_map_dict = df.set_index('tuple')['Value'].to_dict()


# In[267]:


clean_map_dict


# In[270]:


df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Asset Type Category'] = df2['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Investment Type'] = df2['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df2['ViewData.Prime Broker'] = df2['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)


# In[271]:


#ttype_soros['SideA.ViewData.Transaction Type'] = ttype_soros['SideA.ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)


# In[272]:


#ttype_soros['SideB.ViewData.Transaction Type'] = ttype_soros['SideB.ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)


# In[273]:


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
        


# In[274]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)


# In[275]:


#ttype_soros['SideA.ViewData.Transaction Type1'] = ttype_soros['SideA.ViewData.Transaction Type'].apply(lambda x :  clean_mapping(x) if type(x)==str else x)


# In[276]:


#ttype_soros['SideB.ViewData.Transaction Type1'] = ttype_soros['SideB.ViewData.Transaction Type'].apply(lambda x :  clean_mapping(x) if type(x)==str else x)


# In[277]:


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


# In[278]:



import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[279]:


def comb_clean(x):
    k = []
    for item in x.split():
        if ((is_num(item)==False) and (is_date_format(item)==False) and (date_edge_cases(item)==False)):
            k.append(item)
    return ' '.join(k)


# In[280]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[281]:


df2['ViewData.Asset Type Category1'] = df2['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Investment Type1'] = df2['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df2['ViewData.Prime Broker1'] = df2['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[282]:


#ttype_soros['SideA.ViewData.Transaction Type1'] = ttype_soros['SideA.ViewData.Transaction Type'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[283]:


#ttype_soros['SideB.ViewData.Transaction Type1'] = ttype_soros['SideB.ViewData.Transaction Type'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[284]:


#ttype_soros['combined'] = ttype_soros['SideB.ViewData.Transaction Type1'] + ttype_soros['SideA.ViewData.Transaction Type1']


# In[223]:


#k1 = ttype_soros['combined'].value_counts().reset_index()


# In[224]:


#k1.head(5)


# In[198]:


df2['ViewData.Transaction Type1'].value_counts().iloc[0:25]


# In[285]:


df2['ViewData.Transaction Type1'] = df2['ViewData.Transaction Type1'].apply(lambda x : 'paydown' if x=='pay down' else x)


# In[78]:


pd.set_option('display.max_columns', 500)


# ### Cleaning of Description

# In[ ]:


com = pd.read_csv('desc cat with naveen oaktree.csv')


# In[ ]:


cat_list = list(set(com['Pairing']))


# In[ ]:


import re


# In[ ]:



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
            


# In[ ]:


df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[ ]:


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
        


# In[ ]:


df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))


# In[ ]:


com = com.drop(['var','Catogery'], axis = 1)


# In[ ]:


com = com.drop_duplicates()


# In[ ]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[ ]:


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


# In[ ]:


df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))


# In[ ]:


comp = ['inc','stk','corp ','llc','pvt','plc']
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[ ]:


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


# In[ ]:


df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))


# #### Prime Broker Creation

# In[ ]:


df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)


# In[ ]:


new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}


# In[ ]:


def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x


# In[ ]:


df3['new_pb'] = df3['new_pb'].apply(lambda x : new_pf_mapping(x))


# In[ ]:


df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].fillna('kkk')


# In[ ]:


df3['new_pb1'] = df3.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)


# In[ ]:


df3['new_pb1'] = df3['new_pb1'].apply(lambda x : x.lower())


# ### Comment Cleaning - Identification of Single sided and double sided too

# #### Cleaned DF2 for double sided comments

# In[292]:


import pandas as pd


# In[293]:


#df2 = pd.read_csv('comment_training_125_123.csv')


# In[294]:


df2.shape


# In[295]:


import re


# In[296]:


def doubleside(com):
    if type(com)!= str:
        return 'Float'
    else:
        com = com.lower()
    #x = com.split('[(\s+).]')
        x = re.split("[,/. \- !?:]+", com)
        if (('opposite' in x) & ('sign' in x)):
            return 'oppo'
        elif (('opposite' in x) & ('direction' in x)):
            return 'oppo'
        elif ('opposite' in x) :
            return 'oppo'
        elif ('difference' in x):
            return 'diff'
        elif ((('up' in x) & (('down' in x)| ('dow' in x))) | ('up/down' in x)) :
            return 'upd'
        
        elif ((('matching' in x) | ('netting' in x)  )):
            return "oppo"
        elif 'close' in x:
            return "close"
        elif (('viteos' in x) & ('working' in x)):
            return "viteos working on it"
        elif (('matched' in x)):
            return "matched"
        elif (('no' in x) & ('action' in x)):
            return "no action"
    
        else:
            return 'single'
        
    
    


# In[297]:


df2['ViewData.InternalComment2'].count()


# In[298]:


df2['ViewData.InternalComment2'] = df2['ViewData.InternalComment2'].apply(lambda x  : x.replace(u'\xa0', u' ') )
#df2['ViewData.InternalComment2_first'] = df2['ViewData.InternalComment2_first'].apply(lambda x  : x.replace(u'\xa0', u' ') )


# In[299]:


df2['s/d'] = df2['ViewData.InternalComment2'].apply(lambda x : doubleside(x))
#df2['s/d2'] = df2['ViewData.InternalComment2_first'].apply(lambda x : doubleside(x))


# In[309]:


df2['s/d'].value_counts()


# In[25]:


pd.set_option('display.max_columns', 500)


# In[26]:


df2[df2['s/d']=='upd'].head(1)


# In[310]:


df2['ViewData.InternalComment1'] = df2['ViewData.InternalComment1'].fillna('KK')


# In[302]:


pd.pivot_table(df2, values='ViewData.InternalComment2', index='ViewData.InternalComment1', columns='s/d', aggfunc='count', fill_value=0, margins=False, dropna=True, margins_name='All').reset_index()


# ### Cancelled trade removal

# In[343]:


trade_types = ['buy','sell','cover short', 'sell short', 'forward', 'forwardfx', 'spotfx']


# In[338]:


dfkk = df2[df2['ViewData.Transaction Type1'].isin(trade_types)]


# In[339]:


dffk2 = dfkk[dfkk['ViewData.Side0_UniqueIds']=='AA']
dffk3 = dfkk[dfkk['ViewData.Side1_UniqueIds']=='BB']


# #### Geneva side

# In[340]:


dffk3.shape


# In[ ]:


def canceltrade(x,y):
    if x =='buy' and y>0:
        k = 1
    elif x =='sell' and y<0:
        k = 1
    else:
        k = 0
    return k


# In[344]:


dffk3['cancel_marker'] = dffk3.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Accounting Net Amount']), axis = 1)


# In[345]:


dffk3['cancel_marker'].value_counts()


# In[347]:


dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID'].nunique()


# In[349]:


cancel_trade = list(set(dffk3[dffk3['cancel_marker'] == 1]['ViewData.Transaction ID']))


# - Segegation of canceltrade with comment

# In[ ]:


original = dffk3[dffk3['ViewData.Transaction ID'].isin(cancel_trade)]


# In[ ]:


original['predicted category'] = 'Original of Cancelled trade'


# In[ ]:


def cancelcomment(x,y):
    com1 = 'This is original of cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[ ]:


original['predicted comment'] = original.apply(lambda x : cancelcomment(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)


# In[ ]:


cancellation = dffk3[dffk3['cancel_marker'] == 1]


# In[ ]:


cancellation['predicted category'] = 'Cancelled trade'


# In[ ]:


def cancelcomment1(x,y):
    com1 = 'This is cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[ ]:


cancellation['predicted comment'] =  cancellation.apply(lambda x : cancelcomment1(x['ViewData.Transaction ID'],x['ViewData.Settle Date1']), axis = 1)


# In[ ]:


cancel_fin = pd.concat([original,cancellation])


# In[ ]:


sel_col_1 = ['final_ID','predicted category','predicted comment']


# In[ ]:


cancel_fin = cancel_fin[sel_col_1]


# In[ ]:


cancel_fin.to_csv('Comment file oaktree 2 sep testing p1.csv')


# - Moving Forward

# In[350]:


dffk3 = dffk3[~dffk3['ViewData.Transaction ID'].isin(cancel_trade)]


# In[351]:


dffk3.shape


# #### Broker side

# In[352]:


dffk2['cancel_marker'] = dffk2.apply(lambda x : canceltrade(x['ViewData.Transaction Type1'],x['ViewData.Cust Net Amount']), axis = 1)


# In[353]:


dffk2['cancel_marker'].value_counts()


# In[383]:


dffk2['ViewData.Transaction ID'].count()


# In[354]:


dummy = dffk2[dffk2['cancel_marker']==1]


# In[369]:


dummy.head(4)


# In[361]:


dummy1 = dffk2[dffk2['cancel_marker']!=1]


# In[355]:


dummy.shape


# In[362]:


from pandas import merge
from tqdm import tqdm

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
    print(aa1.shape)
    print(bb1.shape)
    
    aa1.columns = ['SideB.' + x  for x in aa1.columns] 
    bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
    cc1 = pd.merge(aa1,bb1, left_on = 'SideB.marker', right_on = 'SideA.marker', how = 'outer')
    appended_data.append(cc1)
    print(cc1.shape)


# In[363]:


cancel_broker = pd.concat(appended_data)


# In[364]:


cancel_broker.shape


# In[423]:


cancel_broker.head(4)


# In[437]:


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
    
        
    


# In[438]:


cancel_broker[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match','Qnt_match','isin_match','cusip_match','ticker_match','Invest_id']] = cancel_broker.apply(lambda row : amountelim(row), axis = 1,result_type="expand")


# In[430]:


cancel_broker['key_match_sum'] = cancel_broker['map_match'] + cancel_broker['sd_match'] + cancel_broker['curr_match'] + cancel_broker['ttype_match'] + cancel_broker['Qnt_match'] + + cancel_broker['isin_match'] + + cancel_broker['cusip_match']


# In[431]:


cancel_broker['key_match_sum'].value_counts()


# In[439]:


elim1 = cancel_broker[(cancel_broker['map_match']==1) & (cancel_broker['curr_match']==1)  & ((cancel_broker['isin_match']==1) |(cancel_broker['cusip_match']==1)| (cancel_broker['ticker_match']==1) | (cancel_broker['Invest_id']==1))]


# In[440]:


elim1.shape


# In[434]:


elim1.head(4)


# In[ ]:


id_listB = list(set(elim1['SideB.final_ID'])) 
id_listA = list(set(elim1['SideA.final_ID']))


# In[ ]:


c1 = dummy
c2 = dummy1[dummy1['final_ID'].isin(id_listA)]


# In[ ]:


original['predicted category'] = 'Original of Cancelled trade'


# In[ ]:


def cancelcomment(x,y):
    com1 = 'This is original of cancelled trade with tran id'
    com2 = 'on settle date'
    com = com1 + ' ' +  str(x) + ' ' + com2 + str(y)
    return com


# In[ ]:


cancel_fin = pd.concat([c1,c2])


# In[ ]:


sel_col_1 = ['final_ID','predicted category','predicted comment']


# In[ ]:


cancel_fin = cancel_fin[sel_col_1]


# In[441]:


elim1['comment'] ='custody booked reversal of this trade'


# In[442]:


elim1.to_csv('cancelled trade oaktree v2.csv')


# In[443]:


id_listB = list(set(elim1['SideB.final_ID'])) 
id_listA = list(set(elim1['SideA.final_ID']))


# In[444]:


cancel_broker = cancel_broker[~cancel_broker['SideB.final_ID'].isin(id_listB)]
cancel_broker = cancel_broker[~cancel_broker['SideA.final_ID'].isin(id_listA)]


# In[445]:


cancel_broker.shape


# #### partial elimination

# In[451]:


id_listA = list(set(cancel_broker['SideA.final_ID']))


# In[452]:


len(id_listA)


# In[453]:


dffkm = dummy1[dummy1['final_ID'].isin(id_listA)]


# In[454]:


dffkm.shape


# #### Finding Pairs in Up and down

# In[316]:


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
       'ViewData.Mapped Custodian Account', 
       
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
       'ViewData.TaskvsTrade Date'
      ]


# In[455]:


dff4 = dffkm[sel_col]
dff5 = dffk3[sel_col]


# In[533]:


dff4.shape


# In[539]:


dff4.head(4)


# In[536]:


dff4['final_ID'].nunique()


# In[537]:


dff5['final_ID'].nunique()


# In[534]:


dff5.shape


# In[280]:


#dff4 = dff4.head(100)
#dff5 = dff5.head(100)


# In[248]:


dff4.columns


# #### M cross N code

# In[499]:


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
    print(cc1.shape)
    #print(cc1.columns)
    
#     #bb1 = bb1.sort_values(by='filter_key',ascending =True)
#     for i, item in aa1.iterrows():
#         for k, item1 in bb1.iterrows():
#             call= []
#             for col in sel_col:
#                 s1 = item[col]
#                 s2 = item1[col]
           
#                 call.append(s1)
#                 call.append(s2)
#             call1.append(call)
            
    
#     for key in (list(np.unique(np.array(list(aa1['filter_key'].values) + list(bb1['filter_key'].values))))):
        
#         df1 = aa1[aa1['filter_key']==key]
#         df2 = bb1[bb1['filter_key']==key]

#         if df1.empty == False and df2.empty == False:
#             #aa_df = pd.concat([aa1[aa1.index==i]]*repeat_num, ignore_index=True)
#             #bb_df = bb1.loc[pool[len(pool)-1],:][common_cols].reset_index()
#             #bb_df = bb_df.drop('index', 1)

#             df1 = df1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
#             df2 = df2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})

#             #dff  = pd.concat([aa[aa.index==i],bb.loc[pool[i],:][accounting_vars]],axis=1)

#             df1 = df1.reset_index()
#             df2 = df2.reset_index()
#             df1 = df1.drop('index', 1)
#             df2 = df2.drop('index', 1)

#             df1.columns = ['SideA.' + x  for x in df1.columns] 
#             df2.columns = ['SideB.' + x  for x in df2.columns]

#             df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
#             df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})

#             #dff = pd.concat([aa_df,bb_df],axis=1)
#             dff = merge(df1, df2, on='filter_key')
#             training_df.append(dff)
#                 #key_index.append(i)
#             #else:
#             #no_pair_ids.append([aa1[(aa1['filter_key']=='key') & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0]])
#                # no_pair_ids.append(aa1[(aa1['filter_key']== key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0])
    
#         else:
#             no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
#             no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
            

# no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])


# In[500]:


df_213_1 = pd.concat(appended_data)


# In[501]:


df_213_1.shape


# ### Elimination Round 1 : Important key with Amount

# In[452]:


df_213_1.columns


# In[502]:


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
    
        
   
    


# In[503]:


df_213_1[['map_match','amt_match','fund_match','curr_match','sd_match','ttype_match']] = df_213_1.apply(lambda row : amountelim(row), axis = 1,result_type="expand")


# In[504]:


df_213_1['key_match_sum'] = df_213_1['map_match'] + df_213_1['sd_match'] + df_213_1['curr_match']


# In[505]:


df_213_1['key_match_sum'].value_counts()


# #### Inclusion of the fuzz matches

# In[82]:


ttype_soros = pd.read_csv('Soros for preprocess1/inzi training file/UMR ttype oaktree.csv')


# In[83]:


#ttype_combo_soros = list(ttype_soros['combined'])


# In[330]:


def ttype_match(a,b):
    if ((type(a)==float) | (type(b)==float)):
        if a==b:
            return 1
        else:
            return 0
    else:
        a = a.lower()
        b = b.lower()
        if a==b:
            return 1
        else:
            d = str(a)+str(b)
            e = str(b)+str(a)
        #print(d)
            if ((d in ttype_combo_soros) | (e in ttype_combo_soros)):
                return 1
            else:
                return 0
            return 0


# In[331]:


df_213_1['ttype_match_bool'] = df_213_1.apply(lambda x : ttype_match(x['SideA.ViewData.Transaction Type1'],x['SideB.ViewData.Transaction Type1']), axis = 1)


# In[332]:


df_213_1['ttype_match_bool'].value_counts()


# In[87]:


itype_soros = pd.read_csv('Soros for preprocess1/inzi training file/UMR Itype oaktree.csv')


# In[88]:


itype_combo_soros = list(set(itype_soros['combined']))


# In[89]:


a = []
for item in itype_combo_soros:
    if type(item) == float:
        item = item
    else:
        item = item.lower()
    a.append(item)
    


# In[90]:


def itype_match(a,b):
    if ((type(a)==float) | (type(b)==float)):
        if a==b:
            return 1
        else:
            return 0
    else:
        a = a.lower()
        b = b.lower()
        if a==b:
            return 1
        else:
            d = str(a)+str(b)
            e = str(b)+str(a)
        #print(d)
            if ((d in itype_combo_soros) | (e in itype_combo_soros)):
                return 1
            else:
                return 0


# In[91]:


df_213_1['itype_match_bool'] = df_213_1.apply(lambda x : itype_match(x['SideA.ViewData.Investment Type'],x['SideB.ViewData.Investment Type']), axis = 1)


# In[92]:


df_213_1['itype_match_bool'].value_counts()


# In[93]:


asset_soros = pd.read_csv('Soros for preprocess1/inzi training file/UMR Asset oaktree.csv')


# In[94]:


asset_combo_soros = list(set(asset_soros['combined']))


# In[95]:


def asset_match(a,b):
    if ((type(a)==float) | (type(b)==float)):
        if a==b:
            return 1
        else:
            return 0
    else:
        a = a.lower()
        b = b.lower()
        if a==b:
            return 1
        else:
            d = str(a)+str(b)
            e = str(b)+str(a)
        #print(d)
            if ((d in asset_combo_soros) | (e in asset_combo_soros)):
                return 1
            else:
                return 0


# In[96]:


df_213_1['asset_match_bool'] = df_213_1.apply(lambda x : asset_match(x['SideA.ViewData.Asset Type Category'],x['SideB.ViewData.Asset Type Category']), axis = 1)


# In[97]:


df_213_1['asset_match_bool'].value_counts()


# In[506]:


elim1 = df_213_1[(df_213_1['amt_match']==1) & (df_213_1['key_match_sum']>=2)]


# In[511]:


elim1['SideA.final_ID'].nunique()


# In[512]:


elim1['SideB.final_ID'].nunique()


# In[514]:


elim1.to_csv('updown oaktree.csv')


# In[515]:


id_listB = list(set(elim1['SideB.final_ID'])) 
id_listA = list(set(elim1['SideA.final_ID']))


# In[516]:


df_213_1 = df_213_1[~df_213_1['SideB.final_ID'].isin(id_listB)]
df_213_1 = df_213_1[~df_213_1['SideA.final_ID'].isin(id_listA)]


# In[517]:


df_213_1.shape


# In[518]:


df_213_1['SideB.final_ID'].nunique()


# In[519]:


df_213_1['SideA.final_ID'].nunique()


# #### Round 2 Elimination : Opposite Sign

# In[523]:


def opposignelim(row):
   
    if (abs(row['SideB.ViewData.Cust Net Amount']) == abs(row['SideA.ViewData.Accounting Net Amount'])):
        b = 1
    else:
        b = 0
    return b


# In[524]:


df_213_1['amt_abs_match'] = df_213_1.apply(lambda row : opposignelim(row), axis = 1)


# In[526]:


dummy = df_213_1[(df_213_1['amt_abs_match']==1) & (df_213_1['key_match_sum']>=2) & (df_213_1['ttype_match']==1)]


# In[527]:


dummy.shape


# In[177]:


dummy = df_213_1[(df_213_1['amt_abs_match']==1) & (df_213_1['key_match_sum']>=2)]


# In[178]:


dummy.shape


# In[179]:


dummy['SideA.final_ID'].nunique()


# In[180]:


dummy['SideB.final_ID'].nunique()


# In[182]:


dummy.columns


# In[189]:


def itype_match(a,b):
    if ((type(a)==float) | (type(b)==float)):
        if a==b:
            return 1
        else:
            return 0
    else:
        a = a.lower()
        b = b.lower()
        if a==b:
            return 1
        else:
            d = str(a)+str(b)
            e = str(b)+str(a)
        #print(d)
            if ((d in a) | (e in a)):
                return 1
            else:
                return 0


# In[190]:


dummy['new_itype_match'] = dummy.apply(lambda x : itype_match(x['SideA.ViewData.Investment Type'],x['SideB.ViewData.Investment Type']), axis = 1)


# In[510]:


dummy = df_213_1[(df_213_1['amt_elim']==1)]


# In[192]:


id_listB = list(set(dummy['SideB.final_ID'])) 
id_listA = list(set(dummy['SideA.final_ID']))


# In[193]:


df_213_1 = df_213_1[~df_213_1['SideB.final_ID'].isin(id_listB)]
df_213_1 = df_213_1[~df_213_1['SideA.final_ID'].isin(id_listA)]


# In[194]:


df_213_1.shape


# In[195]:


df_213_1['SideA.final_ID'].nunique()


# In[196]:


df_213_1['SideB.final_ID'].nunique()


# ### Segregation of the single sided files

# In[538]:


id_listB = list(set(df_213_1['SideB.final_ID'])) 
id_listA = list(set(df_213_1['SideA.final_ID']))


# In[540]:


dff4 = dff4[dff4['final_ID'].isin(id_listB)]
dff5 = dff5[dff5['final_ID'].isin(id_listA)]


# In[541]:


dff4.shape


# In[542]:


dff5.shape


# ### Comment Categorisation

# In[ ]:


com = pd.read_csv('comment cat with naveen oaktree.csv')


# In[ ]:


com = com.drop(['var','Catogery','Client'] ,axis = 1)


# In[ ]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[ ]:


com = com.drop_duplicates()


# In[ ]:


cat_list = list(set(com['Pairing']))


# In[ ]:


def comclean(com,cat_list):
    cat_all1 = []
    m = 0
    
    if type(com)==str:
        com = com.lower()
    
    
        list1 = cat_list
    
        com1 = re.split("[-,.() /!_?:]+", com)
        com2 = []
        for itemk in com1:
            itemk1 = itemk.split(' - ')
            com2= com2+ itemk1
            
    
    
        for item in list1:
            item = item.lower()
            item1 = item.split(' ')
            lst3 = [value for value in item1 if value in com2] 
            if len(lst3) == len(item1):
                cat_all1.append(item)
                m = m+1
            
            else:
                m = m
            
        if m >0 :
            return cat_all1
        else:
            return "NA"
    else:
        return 'Float'
    
            


# In[ ]:


df3['Com_cat'] = df3['ViewData.InternalComment2'].apply(lambda row : comclean(row,cat_list))# if row['s/d']=='single' else row['s/d'], axis = 1 )


# In[ ]:


def catcln1(cat,df):
    ret = []
    if cat == 'NA':
        return 'NA'
    elif 'equity swap settlement' in cat:
        ret.append('equity swap settlement')
    elif 'equity swap' in cat:
        ret.append('equity swap settlement')
    elif 'swap settlement' in cat:
        ret.append('equity swap settlement')
    elif (('follow up email' in cat)|('email sent' in cat) ):
        if (('follow up email' in cat)& ('email sent' in cat) & (len(cat)==2) ):
            ret.append('follow up email')
            cat.remove('email sent')
            cat.remove('follow up email')
        elif (('follow up email' in cat) & (len(cat)==1)):
            ret.append('follow up email')
            cat.remove('follow up email')
        elif (('email sent' in cat) & (len(cat)==1)):
            ret.append('follow up email')
            cat = cat.remove('email sent')
            
                
        else:
            if len(cat)==0:
                ret.append('email sent')
            else:
                for item in cat:
                    item = str(item)
        
                    if item == 'sd':
                        ret.append('sd1')
                    else:
            
                        a = df[df['Pairing']==item]['replace'].values[0]
                        if ((a!= 'sd') | (a not in ret)):
                            ret.append(a)
                
           
        
       
            
          
                
        
        
    else:
        for item in cat:
            item = str(item)
        
            if item == 'sd':
                ret.append('sd1')
            else:
            
                a = df[df['Pairing']==item]['replace'].values[0]
                if ((a!= 'sd') | (a not in ret)):
                    ret.append(a)
    
    return list(set(ret))


# In[ ]:


df3['Com_cat1'] = df3['Com_cat'].apply(lambda row : catcln1(row,com)) #if row['s/d']=='single' else row['Com_cat'], axis = 1)


# In[ ]:


remove_keyword = ['break cleared','breaks cleared','break clear next run','future dated trade','trade','follow up email','currency', 'security','mapped custodian account','allocleg','cusip','corp']


# In[ ]:


def comcat1(x):
    if x == 'NA':
        k = x
   
    elif len(x)==1:
        k = x
    else:
        k = []
        for item in x:
            if item not in remove_keyword:
                k.append(item)
    return k
            


# In[ ]:


df3['Com_cat2'] =df3['Com_cat1'].apply(lambda row : comcat1(row))


# In[ ]:


def wiretsf(x):
    if (x == 'NA') :
        k = 'NA'
    elif(('wire transfer' in x) and len(x)>1):
        k = []
        for item in x:
            if item !='wire transfer':
                k.append(item)
        
    
   
            
    else:
        k = x
    
    return k


# In[ ]:


df3['Com_cat3'] =df3['Com_cat2'].apply(lambda row : wiretsf(row))


# In[ ]:


df3['Com_cat2'] = df3['Com_cat2'].apply(lambda x : ['break will clear next run'] if len(x)==0 else x)


# In[ ]:


remove_keyword2 = ['cash','fees','loan','capital activity']


# In[ ]:


def comcat2(x):
    if x == 'NA':
        k = x
   
    elif len(x)==1:
        k = x
    else:
        k = []
        for item in x:
            if item not in remove_keyword2:
                k.append(item)
    return k


# In[ ]:


df3['Com_cat4'] =df3['Com_cat3'].apply(lambda row : comcat2(row))


# In[ ]:


def finalcal(x):
    if (x == 'NA'):
        k = 'NA'
    elif len(x)==0:
        k = 'NA'
    
    elif('failed trade' in x) :
        k = 'failed trade'
    elif('break will clear next run' in x) :
        k = 'break will clear next run'
    
    elif(('cancelled' in x) | ('trade cancellation' in x)) :
        k = 'cancelled'
    elif('closed' in x)  :
        k = 'closed'
    elif(('forward' in x) and ('forward fx' in x)) :
        k = 'forward fx'
    elif('bank debt' in x):
        k = 'bank debt interest'
    elif(('corporate action team' in x) and ('corporate action' in x)):
        k1 = []
        for item in x:
            if ((item!= 'corporate action team') and (item!= 'corporate action') and len(x)>2):
                k1.append(item)
            else:
                if len(x)==2:
                    k1.append('corporate action') 
        k = ' '.join(k1)
    else:
        k = ' '.join(x)
     
        
    return k


# In[ ]:


df3['Category'] =df3['Com_cat4'].apply(lambda row : finalcal(row))


# ### Comment Categorisation using model

# In[ ]:


df3 = data.copy()


# In[ ]:


data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)


# In[ ]:


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
 
'ViewData.InternalComment2', 'ViewData.Description','new_pb2','new_pb1',
    'Category'
 
]


# In[ ]:


data = data[Pre_final]


# In[ ]:


df_mod1 = data.copy()


# In[ ]:


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
df_mod1['Category'] = df_mod1['Category'].fillna('NA')
df_mod1['ViewData.Description'] = df_mod1['ViewData.Description'].fillna('nn')


# In[ ]:


def fid(a,b):
   
    if ( b=='BB'):
        return a
    else:
        return b


# In[ ]:


df_mod1['final_ID'] = df_mod1.apply(lambda row: fid(row['ViewData.Side0_UniqueIds'],row['ViewData.Side1_UniqueIds']),axis =1)


# In[ ]:


data2 = df_mod1.copy()


# #### Separate Prediction of the Trade and Non trade

# In[ ]:


trade_ttype = ['sell trade','sell short', 'buy trade', 'forward fx', 'spotfx']


# In[ ]:


data21 = data2[~data2['Category'].isin(trade_ttype)]


# In[ ]:


cols = [
 

 
 'ViewData.Transaction Type1',
 
 

 'ViewData.Asset Type Category1',

 'new_desc_cat',

 'ViewData.Investment Type1',
 

 'new_pb2','new_pb1'
 
 
              
             ]


# In[ ]:


data211 = data21[cols]


# In[ ]:


clf = pickle.load(open(filename, 'rb'))


# In[ ]:


X_test = data211.drop(['Category'],axis = 1)
y_test = data211['Category']   


# In[ ]:


# Actual class predictions
cb_predictions = clf.predict(X_test)#.astype(str)
# Probabilities for each class
#cb_probs = clf.predict_proba(X_test)[:, 1]


# #### Testing of Model and final prediction file

# In[ ]:


print(metrics.classification_report(y_test,cb_predictions, digits=3))


# In[ ]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[ ]:


list1 = list(y_test)


# In[ ]:


X_test1 = data21.drop(['Category'],axis = 1)
y_test1 = data21['Category'] 


# In[ ]:


result = X_test.copy()


# In[ ]:


result = result.reset_index()


# In[ ]:


result['predicted'] = pd.Series(demo)
result['Actual'] = pd.Series(list1)


# ### Code Ends Here

# In[134]:


both_final = ['ViewData.Currency',
 'ViewData.Account Type',
 'ViewData.Accounting Net Amount',

 'ViewData.Activity Code',
 'ViewData.Age',
 'ViewData.Age WK',
 'ViewData.Alt ID 1',
 'ViewData.Asset Type Category',
 'ViewData.Assigned To',
 'ViewData.Base Currency',
 'ViewData.Base Net Amount',
 'ViewData.Bloomberg_Yellow_Key',
 'ViewData.Cust Net Amount',
 
 
 'ViewData.BreakID',
 'ViewData.Business Date',
 'ViewData.Call Put Indicator',
 'ViewData.Cancel Amount',
 'ViewData.Cancel Flag',
 'ViewData.ClusterID',
 'ViewData.Commission',
 'ViewData.CUSIP',
 'ViewData.Custodian',
 'ViewData.Custodian Account',
 
 'ViewData.Department',
 'ViewData.Derived Source',
 'ViewData.Description',
 'ViewData.ETL File Code',
 'ViewData.ETL Package Code',
 'ViewData.ExpiryDate',

 'ViewData.Fund',
 'ViewData.FX Rate',
 'ViewData.Group',
 'ViewData.Has Attachments',
 'ViewData.Interest Amount',
 'ViewData.InternalComment1',
 'ViewData.InternalComment2',
 'ViewData.InternalComment3',
 'ViewData.Investment ID',
 'ViewData.Investment Type',
 'ViewData.Is Combined Data',
 'ViewData.ISIN',
 'ViewData.Keys',
 'ViewData.Knowledge Date',
 'ViewData.Legal Entity',
 
 'ViewData.Mapped Custodian Account',
 'ViewData.Net Amount Difference',
 'ViewData.Net Amount Difference Absolute',
 'ViewData.Non Trade Description',
 'ViewData.OTE Custodian Account',
 'ViewData.OTE Ticker',
 'ViewData.OTEIncludeFlag',
 'ViewData.PB Account Numeric',
 'ViewData.Portfolio ID',
 'ViewData.Portolio',
 'ViewData.Price',
 'ViewData.Prime Broker',
 'ViewData.Principal Amount',
 'ViewData.Prior Knowledge Date',
 'ViewData.Priority',
 'ViewData.Quantity',
 'ViewData.Reviewer',
 'ViewData.Rule And Key',
 'ViewData.Sec Fees',
 'ViewData.SEDOL',
 'ViewData.Settle Date',
 'ViewData.SPM ID',
 'ViewData.Status',
 'ViewData.Strategy',
 'ViewData.Strike Price',
 'ViewData.System Comments',
 'ViewData.Ticker',
 'ViewData.Trade Date',
 'ViewData.Trade Expenses',
 'ViewData.Transaction Category',
 'ViewData.Transaction ID',
 'ViewData.Transaction Type',
 'ViewData.Underlying Cusip',
 'ViewData.Underlying Investment ID',
 'ViewData.Underlying ISIN',
 'ViewData.Underlying Sedol',
 'ViewData.Underlying Ticker',
 'ViewData.UserTran1',
 'ViewData.UserTran2',
 'ViewData.Value Date',
 'ViewData.Workflow Remark',
 'ViewData.Workflow Status','ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds','ViewData.Task Business Date','ViewData.InternalComment2_last',
 'ViewData.InternalComment1_last',
 'ViewData.Task Business Date last','s/d1','s/d2']


# #### Column selection for merge 1 month files

# In[135]:


list(df2.columns)


# In[136]:


remove = ['ViewData.Interest Amount', 'ViewData.OTE Ticker', 'ViewData.Activity Code', 'ViewData.Principal Amount', 'ViewData.Derived Source', 'ViewData.Sec Fees', 'ViewData.Call Put Indicator', 'ViewData.OTE Custodian Account', 'ViewData.FX Rate', 'ViewData.Legal Entity', 'ViewData.Strike Price', 'ViewData.Bloomberg_Yellow_Key']


# In[137]:


both_final1 = []

for item in both_final:
    if item not in remove:
        both_final1.append(item)
        
    


# #### Normal preprocessing starts here

# In[138]:


df3 = df2[both_final1]


# In[139]:


df3 = df3[df3['s/d2']=='single']


# In[110]:


df3['ViewData.InternalComment2'].iat[4]


# In[71]:


com.head(4)


# In[140]:


cat_list = list(set(com['Pairing']))


# In[141]:


cat_list


# In[142]:



def comclean(com,cat_list):
    cat_all1 = []
    m = 0
    
    if type(com)==str:
        com = com.lower()
    
    
        list1 = cat_list
    
        com1 = re.split("[-,.() /!_?:]+", com)
        com2 = []
        for itemk in com1:
            itemk1 = itemk.split(' - ')
            com2= com2+ itemk1
            
    
    
        for item in list1:
            item = item.lower()
            item1 = item.split(' ')
            lst3 = [value for value in item1 if value in com2] 
            if len(lst3) == len(item1):
                cat_all1.append(item)
                m = m+1
            
            else:
                m = m
            
        if m >0 :
            return cat_all1
        else:
            return "NA"
    else:
        return 'Float'
    
            
        
        
        
        
    


# In[174]:


df3['Com_cat'] = df3['ViewData.InternalComment2_last'].apply(lambda row : comclean(row,cat_list))# if row['s/d']=='single' else row['s/d'], axis = 1 )


# In[175]:


df3.shape


# In[176]:


df3['Com_cat'].count()


# In[177]:


df3['Com_cat'].value_counts().iloc[0:25]


# In[117]:


df3[df3['Com_cat']=='NA']['ViewData.InternalComment2_last'].value_counts().iloc[25:50]


# In[159]:


#df3 = df3[df3['Com_cat']!='Float']


# In[178]:


def catcln1(cat,df):
    ret = []
    if cat == 'NA':
        return 'NA'
    elif 'equity swap settlement' in cat:
        ret.append('equity swap settlement')
    elif 'equity swap' in cat:
        ret.append('equity swap settlement')
    elif 'swap settlement' in cat:
        ret.append('equity swap settlement')
    elif (('follow up email' in cat)|('email sent' in cat) ):
        if (('follow up email' in cat)& ('email sent' in cat) & (len(cat)==2) ):
            ret.append('follow up email')
            cat.remove('email sent')
            cat.remove('follow up email')
        elif (('follow up email' in cat) & (len(cat)==1)):
            ret.append('follow up email')
            cat.remove('follow up email')
        elif (('email sent' in cat) & (len(cat)==1)):
            ret.append('follow up email')
            cat = cat.remove('email sent')
            
                
        else:
            if len(cat)==0:
                ret.append('email sent')
            else:
                for item in cat:
                    item = str(item)
        
                    if item == 'sd':
                        ret.append('sd1')
                    else:
            
                        a = df[df['Pairing']==item]['replace'].values[0]
                        if ((a!= 'sd') | (a not in ret)):
                            ret.append(a)
                
           
        
       
            
          
                
        
        
    else:
        for item in cat:
            item = str(item)
        
            if item == 'sd':
                ret.append('sd1')
            else:
            
                a = df[df['Pairing']==item]['replace'].values[0]
                if ((a!= 'sd') | (a not in ret)):
                    ret.append(a)
    
    return list(set(ret))


# In[179]:


df3['Com_cat1'] = df3['Com_cat'].apply(lambda row : catcln1(row,com)) #if row['s/d']=='single' else row['Com_cat'], axis = 1)


# In[180]:


df3['Com_cat1'].value_counts().iloc[0:25]


# In[82]:


df3.columns


# In[181]:


df3['Com_cat_len1'] = df3['Com_cat1'].apply(lambda x : len(x))


# In[182]:


df3['Com_cat_len1'].value_counts()


# In[183]:


pd.set_option('display.max_columns', 500)


# In[184]:


df3[df3['Com_cat_len1']==0].head(3)


# In[793]:


df3[df3['Com_cat_len1']==0]['ViewData.InternalComment2'].value_counts().iloc[1:25]


# In[185]:


df3 = df3[(df3['Com_cat_len1']==1) | (df3['Com_cat_len1']==2) | (df3['Com_cat_len1']==3) | (df3['Com_cat_len1']==4)]


# In[186]:


df3.shape


# In[206]:


df3.columns


# #### Checking whether right keyword extraction happened or not

# In[871]:


pd.set_option('display.max_columns', 500)


# In[453]:


df3[df3['ViewData.Transaction Type']=='Interest'].head(5)


# In[199]:


x.remove('break cleared')


# In[198]:


x


# In[196]:


x = ['break cleared','interest']
#y = x.remove('break cleared') if 'break cleared' in x else x


# In[194]:


y


# In[207]:


def comcat(x):
    if x =='NA':
        k = "NA"
    
    elif (len(x)==1) & ('breaks cleared' in x):
        k = 'break cleared'
    else:
        if ((len(x)>1) & ('breaks cleared' in x)):
            k = x.remove('breaks cleared')
        else:
            k = x
    return k


# In[208]:


df3['Com_cat2'] =df3['Com_cat1'].apply(lambda row : comcat(row))


# In[209]:


df3.shape


# In[217]:


df3['Com_cat2'].count()


# In[215]:


df3.count()


# In[218]:


df3['Com_cat2'].value_counts().iloc[0:50]


# In[216]:


df3['Com_cat2'] = df3['Com_cat2'].fillna('NA')


# In[219]:


def forcorpact(x):
    y = []
    if x =='NA':
        return 'NA'
    
    elif ('full call' in x):
        y.append('full call')
    elif ('partial call' in x):
        y.append('partial call')
    else:
        y = x
    return y


# In[220]:


df3['Com_cat3'] =df3['Com_cat2'].apply(lambda row : forcorpact(row))


# In[221]:


df3['Com_cat3'].value_counts().iloc[0:25]


# In[222]:


def catcln22(x):
    if x == 'NA':
        return 'NA'
    elif 'interest' in x:
        return 'interest'
    else:
        return x[0]


# In[224]:


df3['Category'] =df3['Com_cat3'].apply(lambda x : catcln22(x) )


# In[225]:


df3['Category'].value_counts()


# ### Making Sure One category is one template

# - Notes for amendement
# 
# - Fund Category should be amended with Money market fund and keyword will be same
# - margin has only keyword - margin
# - Do I need to find why cancellation happend?
# - Is deposit same as deposit trade? It is the query related to deposit ttype
# - Income from securities has only one keywrd, same.
# - cpn/coupan should be category with coupan/cpn fee.
# - If ticket fees and ticket charges are same, this one catgory.
# - corporate action category is rigged. This can be further split.
# - Sell short needs no breaking. Why 2 comments in one?
# -

# In[181]:


df3[df3['Category'] == 'spec journal']['ViewData.InternalComment2'].value_counts()


# In[171]:


df3[(df3['Category'] == 'cancelled') & (df3['ViewData.InternalComment2'] == 'JPM cancelled DIV Receive on 03/09. Viteos investigating on this')]['ViewData.Transaction Type'].value_counts()


# In[893]:


df3.columns


# #### Inclusion of non categorised comments

# In[480]:


df3[df3['Category']=='follow up email']['ViewData.InternalComment2'].value_counts().iloc[15:25]


# In[481]:


df3[df3['Com_cat']==['email sent']]['ViewData.InternalComment2'].value_counts().iloc[15:25]


# In[170]:


df3[df3['ViewData.InternalComment2']=='Only at PB - SFM to provide more information on cash activity']['ViewData.Transaction Type'].value_counts().iloc[0:5]


# In[171]:


df3[df3['ViewData.Transaction Type']=='Cash Journal']['ViewData.InternalComment2'].value_counts().iloc[0:5]


# ### Examination of discrepencies in ttype and desc

# In[222]:


df3['Category'].value_counts()


# - From Here On I will Check each category 1 by one

# #### 1st one is interest

# #### Interest
# -Questions related to Interest : Verify all the extractions you have done and then go one by one to wrong prediction.
# - category splitting :
#  1.  Month End Interest, break will clear in next run, different comment template?
#  2. Bank loan interest, different from just interest for business? Do we need what kind of interest
#  3. PIK/Interest to be reproted at custody, different?
# - Rational For wrong prediction:
# - Wrong user comments:
# 
# #### Followup Email
# - How to eliminate follow up email and breaks cleared. Why the even appear.Thay are predicted nicely because they are correted with forward Fx and Redemption in data. This should not be the actual logic.
# 
# #### Buy trade 
# - Since buy trade is prefectly getting predicted. So we only look at the comment types in buy trade to make sure if we need further categorisation
# - Category Splitting:
#  1. What is future dated trade comment template, why it is coming under 'buy' ttype?
# - Can cause bad prediction:
#  2. Why Dividend comment template in buy?
#  3. Is failed to report comment template different?
#  
# #### Common Conundrum of Interest, corporate action and Callable Interest bond.

# In[225]:


df3[df3['Category']=='corporate action']['ViewData.Transaction Type'].value_counts().reset_index()


# In[226]:


df3[df3['ViewData.Transaction Type']=='corporate action']['Category'].value_counts()


# In[220]:


df3[df3['Category']=='corporate action']['ViewData.InternalComment2'].value_counts().iloc[0:30]


# In[228]:


df3[(df3['ViewData.Transaction Type']=='corporate action') & (df3['Category']=='corporate action')]['ViewData.Description'].value_counts().iloc[0:30]


# In[179]:


df3[(df3['ViewData.Transaction Type']=='buy') & (df3['Category']=='dividend')]['ViewData.InternalComment2'].value_counts().iloc[0:30]


# In[184]:


df3.columns


# In[189]:


df3[(df3['ViewData.Transaction Type']=='equity swap') & (df3['Category']=='equity swap settlement')]['ViewData.Asset Type Category'].value_counts().iloc[0:25]


# In[127]:


df3[(df3['ViewData.Transaction Type']=='withdrawal') & (df3['Category']=='pb variation margin')]['ViewData.Description'].value_counts()


# In[114]:


eqwire.to_csv('testing/Problem of bad comments.csv')


# In[ ]:


df3[(df3['Category']=='wire transfer') & (df3['ViewData.Transaction Type']=='interest')]


# In[736]:


df3[(df3['Category']=='equity swap settlement') & (df3['ViewData.Transaction Type']=='transfer')]['ViewData.Description'].value_counts()


# In[732]:


df3[(df3['Category']=='wire transfer') & (df3['ViewData.Transaction Type']=='laredo petroleum inc')]['ViewData.Description'].value_counts()


# In[373]:


df3[df3['Category']=='expenses']['ViewData.Investment Type'].value_counts().iloc[0:25]


# In[375]:


df3[df3['ViewData.Investment Type']=='Cash and Equivalents']['ViewData.Transaction Type'].value_counts()


# In[343]:


df3['ViewData.Description'].count()


# - Is cash movement same as wire transfer??
# - Lets Look at each category and what leads to differentiation
# - Why not predict equity swap settlement when desc is swap unwind
# - Spec Journal and Expenses are 2 problematic trnasaction type

# In[352]:


df3[df3['Category']=='wire transfer']['ViewData.InternalComment2'].value_counts().iloc[75:78]


# In[320]:


pd.set_option('display.max_columns', 500)


# In[329]:


df3[df3['ViewData.InternalComment2']=='Difference of 14858.87 transfer wire booked between Geneva and USBK for SD 3/02.'].head(6)


# In[333]:


df3[df3['ViewData.InternalComment2']=='Difference of 14858.87 transfer wire booked between Geneva and USBK for SD 3/02.']['ViewData.Description'].iat[2]


# In[216]:


'EQSWAP DIV CLIENT TAX' in 'EQUITY SWAP LONG FEE'


# In[366]:


df3[(df3['ViewData.Transaction Type']== 'Expenses') & (df3['Category']=='wire transfer')]['new_desc_cat'].value_counts()


# In[378]:


df3[(df3['ViewData.Transaction Type']== 'Expenses') & (df3['Category']=='expenses')]['new_desc_cat'].value_counts().iloc[0:25]


# In[380]:


df3[(df3['ViewData.Transaction Type']== 'Expenses') & (df3['Category']=='expenses')]['ViewData.Description'].value_counts().iloc[0:25]


# In[364]:


df3[(df3['ViewData.Transaction Type']== 'SPEC Journal') & (df3['Category']=='spec journal')]['ViewData.InternalComment2'].value_counts().iloc[0:25]


# In[259]:


df3[(df3['ViewData.Transaction Type']=='transfer') & (df3['Category']=='N')]['new_desc_cat'].value_counts()


# In[260]:


df3[(df3['ViewData.Transaction Type']=='transfer') & (df3['Category']=='internal transfer')]['new_desc_cat'].value_counts()


# In[262]:


df3[(df3['ViewData.Transaction Type']=='datadetail') & (df3['Category']=='mo')]['ViewData.Description'].value_counts()


# In[263]:


df3[(df3['ViewData.Transaction Type']=='datadetail') & (df3['Category']=='N')]['ViewData.InternalComment2'].value_counts()


# In[255]:


df3[(df3['ViewData.Transaction Type']=='transfer') & (df3['Category']=='mo')]['ViewData.Prime Broker'].value_counts()


# In[1018]:


df3[(df3['ViewData.Transaction Type']=='interest') & (df3['Category']=='follow up email')]['new_desc_cat'].value_counts()


# In[235]:


df3[(df3['ViewData.Transaction Type']=='transfer') & (df3['Category']=='mo')]['ViewData.InternalComment2'].value_counts()


# In[1021]:


df3[(df3['ViewData.Transaction Type']=='interest') & (df3['Category']=='wht')]['ViewData.InternalComment2'].value_counts()


# #### Looking at Pay ttype

# In[1023]:


df3[df3['ViewData.Transaction Type']=='pay']['Category'].value_counts()


# In[1024]:


df3[(df3['ViewData.Transaction Type']=='pay') & (df3['Category']=='N')]['ViewData.InternalComment2'].value_counts()


# #### checking the int transaction type

# In[1025]:


df3[df3['ViewData.Transaction Type']=='int']['Category'].value_counts()


# #### 2nd is wire transfer

# In[1004]:


df3[df3['Category']=='wire transfer']['ViewData.Transaction Type'].value_counts()


# In[878]:


df3[df3['Category']=='interest']['ViewData.Transaction Type'].value_counts()


# In[883]:


df3[df3['Category']=='corporate action']['ViewData.Transaction Type'].value_counts()


# In[885]:


df3[df3['ViewData.Transaction Type']=='transfer']['Category'].value_counts()


# In[1003]:


df3[df3['ViewData.Transaction Type']=='internal']['Category'].value_counts()


# In[1005]:


df3[(df3['ViewData.Transaction Type']=='internal') & (df3['Category']=='expenses ')]['new_desc_cat'].value_counts()


# In[1006]:


df3[(df3['ViewData.Transaction Type']=='internal') & (df3['Category']=='wire transfer')]['new_desc_cat'].value_counts()


# In[910]:


'20BQKBB2VMC'.startswith('20')


# In[284]:


df3[(df3['Category']=='N') & (df3['ViewData.Transaction Type']=='Futures Collateral') ].head(4)


# In[1009]:


df3.sort_values('ViewData.Task Business Date').tail(4)


# In[279]:


df3[(df3['Category']=='buy trade') & (df3['ViewData.Transaction Type']=='Sell') ].head(4)


# #### Checking the sell trade category

# In[1026]:


df3[df3['Category']=='sell trade']['ViewData.Transaction Type'].value_counts()


# In[1028]:


df3[df3['ViewData.Transaction Type']=='sell']['Category'].value_counts()


# In[1031]:


df3[(df3['Category']=='future dated trade') & (df3['ViewData.Transaction Type']=='sell') ]['ViewData.InternalComment2'].value_counts()


# #### Checking for corporate action category

# In[1032]:


df3[df3['Category']=='corporate action']['ViewData.Transaction Type'].value_counts()


# In[1033]:


df3[df3['ViewData.Transaction Type']=='corporate']['Category'].value_counts()


# In[1034]:


df3[df3['ViewData.Transaction Type']=='corp']['Category'].value_counts()


# In[1035]:


df3[df3['ViewData.Transaction Type']=='interest']['Category'].value_counts()


# ### Transaction type cleaning

# In[232]:


df3['Category'].value_counts()


# In[243]:


df3[df3['Category']=='wht']['ViewData.Transaction Type1'].value_counts().iloc[0:25]


# In[592]:


k = 'MONEY TRANSFER 10/02/20MH'
item = k.split()
for item1 in item:
    if item1.endswith('MH') ==True:
        print(item1)


# In[439]:


df3.shape


# In[468]:


df3.head(4)


# ### Functions for cleaning category wise

# #### 1. Interest for oaktree

# In[158]:


df3[df3['Category']=='interest']['ViewData.Transaction Type'].value_counts()


# In[156]:


def inttype(x):
    if type(x)== float:
        return 'interest'
    else:
        x1 = x.lower()
        x2 = x1.split()
        if 'int' in x2:
            return 'interest'
        else:
            return x1 
    


# In[157]:


df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : inttype(x))


# #### For Equity Swap Settlement

# In[226]:


def divclient(x):
    if (type(x) == str):
        if ('EQSWAP DIV CLIENT TAX' in x) :
            return 'EQSWAP DIV CLIENT TAX'
        else:
            return x
    else:
        return 'float'


# In[227]:



df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : divclient(x))


# In[228]:



def mhreplace(item):
    item1 = item.split()
    for items in item1:
        if items.endswith('MH')==True:
            item1.remove(items)
    return ' '.join(item1).lower()


# In[229]:


df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x :mhreplace(x))


# In[230]:


df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x :x.lower())


# In[231]:


df3['ViewData.Transaction Type'].nunique() 


# #### For wire transfer

# In[223]:


def compname(x):
    m = 0
    comp = ['Corporate','stk','inc','lp','plc','inc.','inc','corp']
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
        


# In[227]:


df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : compname(x))


# #### For Interest 

# In[229]:


def inter(x):
    m = 0
    comp = ['Corporate','stk','inc','lp','plc','inc.','inc','corp']
    if type(x)==str:
        x1 = x.split()
        if (('from' in x1) & ('from' in x1)):
            return 'interest'
        else:
            return x
    else:
        return x
            


# In[234]:


df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : inter(x))


# #### For withholding taxes

# In[241]:


def wht(x):
    if type(x)==str:
        x1 = x.split()
        if x1[0] =='30%':
            return 'Wht'
        else:
            return x
    else:
        return x


# In[242]:


df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : wht(x))


# ### Cleaning of Description varibale

# #### TF IDF Approach 

# In[1040]:


df3 = pd.read_csv('commentprediction_125_123v6.csv')


# In[1041]:


df3.shape


# In[1042]:


df3.head(4)


# In[24]:


df4 = df3['ViewData.Description'].value_counts().reset_index()


# In[25]:


df4['Desc'] = df4.apply(lambda row : row['index'] if row['ViewData.Description']>10 else 'CAT', axis = 1)


# In[26]:


df4.head(4)


# In[27]:


df4 = df4.rename(columns = {'ViewData.Description':'Desc_count','Desc':'New_desc'})


# In[18]:


df4['Desc'].value_counts()


# In[28]:


df3 = pd.merge( df3 , df4, left_on = 'ViewData.Description', right_on = 'index', how = 'left')


# In[50]:


month = ['jan','feb','mar', 'apr', 'may', 'jun', 'july', 'aug', 'sep', 'oct', 'nov', 'dec', 'january', 'february', 'march', 'april',
        'may','june','july','august','september','october','november','december']


# In[112]:


def descclean(x):
    month = ['jan','feb','mar', 'apr', 'may', 'jun', 'july', 'aug', 'sep', 'oct', 'nov', 'dec', 'january', 'february', 'march', 'april',
        'may','june','july','august','september','october','november','december']
    if (type(x)==str):
        list1 = x.split()
        list2 = ' '.join(item for item in list1 if item.replace('.','',1).isdigit()== False)
    
        list3 = ' '.join(item for item in list2.split(' ') if item.replace(',','',1).isdigit()== False)
        list4 = ' '.join(item for item in list3.split(' ') if item.lower() not in month)
        list5 = ' '.join(item for item in list4.split(' ') if item.replace('.','').replace('-',''))
        list6 = ' '.join(item for item in list5.split(' ') if item.replace('.','').replace('-',''))
        return list5
    else:
        return x
    #list6 = ' '.join(item for item in list5.split(' ') if item.isalnum() == False)
   
        
    


# In[113]:


df3['Description'] = df3['ViewData.Description'].apply(lambda x : descclean(x))


# In[132]:


df3[['ViewData.Description','Description']].head(4)


# In[108]:


descclean('GSRPEMUL CUSTOM BASKET RIC-.GSRPEMUL CFD OTC EX Jun 28,28 1304.488595 USD X1')


# In[110]:


'BASKET'.isalnum()


# In[95]:


name = 'GSRPEMUL CUSTOM BASKET RIC-.GSRPEMUL CFD OTC EX Jun 28,28 1304.488595 USD X1'


# In[98]:


name.translate(".-",None)


# In[29]:


df3.count()


# ### Keyword Extraction Approach

# #### looking at the desc of a category

# In[296]:


df2['Description'].value_counts().iloc[550:600]


# In[287]:


def descclean(x):
    month = ['jan','feb','mar', 'apr', 'may', 'jun', 'july', 'aug', 'sep', 'oct', 'nov', 'dec', 'january', 'february', 'march', 'april',
        'may','june','july','august','september','october','november','december']
    if (type(x)==str):
        list1 = x.split()
        list2 = ' '.join(item for item in list1 if item.replace('.','',1).isdigit()== False)
    
        list3 = ' '.join(item for item in list2.split(' ') if item.replace(',','',1).isdigit()== False)
        list4 = ' '.join(item for item in list3.split(' ') if item.lower() not in month)
        #list5 = ' '.join(item for item in list4.split(' ') if item.replace('.','').replace('-',''))
        #list6 = ' '.join(item for item in list5.split(' ') if item.replace('.','').replace('-',''))
        return list4
    else:
        return x


# In[288]:


df2['Description'] = df2['ViewData.Description'].apply(lambda x : descclean(x))


# In[257]:


bad_char = ['#','?','-','&']


# In[263]:


def desc_cleaner1(x):
    if type(x)==float:
        return x
    else:
        x = x.lower()
        x1 = x.split()
        for item in x1:
            if ((item.endswith('%')) | (type(item)== float) | (item.startswith('$')) | (item.startswith(',')) | (item.startswith('(')) | (item.endswith(')')) | (item not in bad_char) ):
                x1.remove(item)
        return x1
    


# In[264]:


def desc_cleaner2(x):
    if (type(x) == float):
        return 'Float'
    else:
        for item in x:
            if ((len(item.split('/'))==3) | (len(item.split('-'))==3)):
                x.remove(item)
        return x


# In[265]:


len(' 03/01/2033'.split('/'))


# In[307]:


df2['ViewData.Description1'] = df2['ViewData.Description'].apply(lambda x : desc_cleaner1(x))


# In[308]:


df2['ViewData.Description2'] = df2['ViewData.Description1'].apply(lambda x : desc_cleaner2(x))


# In[309]:


df2['ViewData.Description2'].value_counts().iloc[0:25]


# In[310]:


dfk = df2[(df2['ViewData.Description2']!='Float')]


# In[311]:


dfk['len_desc'] = df2['ViewData.Description2'].apply(lambda x : len(x))


# In[312]:


dfk['len_desc'].value_counts()


# In[313]:


dfk = dfk[dfk['len_desc']!=0]


# In[314]:


from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from sklearn.feature_extraction.text import TfidfVectorizer


# In[315]:


tokenized_list_of_sentences = list(dfk['ViewData.Description2'])

def identity_tokenizer(text):
    return text

tfidf = TfidfVectorizer(tokenizer=identity_tokenizer, stop_words='english', lowercase=False,  ngram_range=(1,3))    
x = tfidf.fit_transform(tokenized_list_of_sentences)


# In[316]:


x = pd.DataFrame(x.toarray(), columns = tfidf.get_feature_names())


# In[317]:


y = pd.DataFrame(dfk['ViewData.Description2'])


# In[318]:


y


# In[319]:


x = pd.concat([x,y], axis = 1)


# In[320]:


x


# In[322]:


x['ViewData.Description2'].iat[178]


# In[283]:


'$1,588,249.93'.startswith('$')


# In[282]:


list(x.columns)


# In[325]:


x['islands'].iat[178]


# #### start of keyword approach

# In[232]:


com = pd.read_csv('desc cat with naveen oaktree.csv')


# In[233]:


com.head(3)


# In[234]:


cat_list = list(set(com['Pairing']))


# In[235]:


import re


# In[236]:



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
            


# In[237]:


df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))


# In[238]:


df3['desc_cat'].value_counts().reset_index().shape


# In[239]:


df3['desc_cat'].value_counts().iloc[0:25]


# In[240]:


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
        


# In[241]:


df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))


# In[242]:


com.head(4)


# In[299]:


com['Pairing'].value_counts()


# In[264]:


isinstance(['eurforward'], list)


# In[243]:


com = com.drop(['var','Catogery'], axis = 1)


# In[244]:


com = com.drop_duplicates()


# In[245]:


com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())


# In[246]:


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


# In[247]:


df3['desc_cat'].iat[0]


# In[248]:


df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))


# In[249]:


df3['new_desc_cat'].value_counts().iloc[0:60]


# In[250]:


comp = ['inc','stk','corp ','llc','pvt','plc']
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)


# In[251]:


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


# In[252]:


df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))


# In[253]:


df3['new_desc_cat'].value_counts().iloc[0:20]


# In[ ]:





# In[254]:


df3.to_csv('oaktree improv/Preprocessed Oaktree with broker right v3.csv')


# ### Filtering only valid transaction type

# In[357]:


tran = pd.read_csv('comment tran desc mapping for clients.csv')


# In[361]:


tran.head(4)


# In[360]:


tran = tran.drop(['desc','Client'], axis = 1)


# In[363]:


categ = list(set(tran['Comment Cat']))
ttype = list(set(tran['ttype']))


# In[364]:


dfk3 = df3[df3['Category'].isin(categ)]
dfm3 = df3[~df3['Category'].isin(categ)]


# In[365]:


dfk3.shape


# In[366]:


dfm3.shape


# In[370]:


dfk31 = dfk3[dfk3['ViewData.Transaction Type'].isin(ttype)]


# In[371]:


dfk31.shape


# In[372]:


frames = [dfm3,dfk31]
df31 = pd.concat(frames)


# In[374]:


df31 = df31.reset_index()


# In[375]:


df31.shape


# In[376]:


df31.to_csv('preprocessed weiss 125 v2 with cat refined.csv')


# ### This is the finishing line

# #### Up down at extraction from the comment

# In[180]:


def updown(x):
    m = x.split()
    for item in m:
        if item.lower() == 'up/down':
            return 1
        elif ((item== 'up') | (item =='down')):
            return 1
        else:
            return 0
                
            


# In[181]:


df2[df2['InternalComment1'] == 'UpDown'].shape


# In[182]:


df2[df2['comment']=='mapped custodian account'].sort_values(['ViewData.Fund','ViewData.Mapped Custodian Account','ViewData.Trade Date']).head(5)


# In[294]:


df2[(df2['up/down']==1)]['comment'].value_counts().reset_index().head(10)


# In[288]:


df2['up/down'] = df2['ViewData.InternalComment2'].apply(lambda x : updown(x))


# In[289]:


df2['up/down'].value_counts()


# In[300]:


df2[(df2['up/down']==1) & (df2['InternalComment1'] != 'UpDown')].head(3)


# In[306]:


df2[df2['up/down'] ==1]['ViewData.InternalComment2'].value_counts().reset_index()


# In[319]:


def updownattr(str,b,c):
    if c ==1:
        your_string = str.lower()
        list_of_words = your_string.split()
        your_search_word = b
        next_word = list_of_words[list_of_words.index(your_search_word) + 2]
        return next_word.replace('(', '').replace(')', '').replace('.', '').replace(',', '')
    else:
        return 'NA'


# In[320]:


df2['updattr'] = df2.apply(lambda row: updownattr(row['ViewData.InternalComment2'],'up/down',row['up/down']), axis = 1)


# In[321]:


df2['updattr'].value_counts()


# In[303]:


next_word


# In[ ]:


sideA = df2[df2['ViewData.Side0_UniqueIds'] !='AA']
sideB = df2[df2['ViewData.Side1_UniqueIds'] !='BB']


# In[326]:





# In[231]:


sideA1.shape


# In[239]:


sideA.count()


# In[237]:


sideB.count()


# In[ ]:


sideB['']


# In[209]:


sideA1['ViewData.Transaction Type'].value_counts()


# In[210]:


sideB['ViewData.Transaction Type'].value_counts()


# In[160]:


sideA1['ViewData.Transaction Type'].value_counts()


# In[161]:


sideB['ViewData.Transaction Type'].value_counts()


# In[173]:


asscat1[asscat1['DataSides.0.Asset Type Category'] =='Cash and Equivalents']


# In[176]:


ttype2[ttype2['side0'] =='futures']


# In[109]:


asscat1.head(4)


# In[110]:


ttype2.head(4)


# In[328]:


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


# In[329]:


sideA['ViewData.Transaction Type'] = sideA['ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
sideB['ViewData.Transaction Type'] = sideB['ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))


# In[335]:


df2['updattr'].value_counts()


# In[345]:


sideA1 =sideA[sideA['updattr']=='value']


# In[163]:


def assetmatch(a, asscat1,dfk):
    df = asscat1[asscat1['DataSides.0.Asset Type Category']==a]
    m = 0
    if dfk[dfk['ViewData.Asset Type Category']==a].shape[0]>0:
        return dfk[dfk['ViewData.Asset Type Category']==a].shape[0]
    elif df.shape[0] == 0:
        return 0
    else:
        ass = list(df['DataSides.1.Asset Type Category'])
        for item in ass:
            if (dfk[dfk['ViewData.Asset Type Category'] == item].shape[0]>0):
                m = m+dfk[dfk['ViewData.Asset Type Category'] == item].shape[0]
            else :
                m = m
        if m >0:
            return m
        else:
            return 0
    


# In[164]:


def ttypematch(a,ttype2,dfk):
    df = ttype2[ttype2['side0']==a]
    m = 0
    if dfk[dfk['ViewData.Transaction Type']==a].shape[0]>0:
        return dfk[dfk['ViewData.Transaction Type']==a].shape[0]
    
    
    elif df.shape[0] == 0:
        return 0
    else:
        ass = list(df['side1'])
        for item in ass:
            if (dfk[dfk['ViewData.Transaction Type'] == item].shape[0]>0):
                m = m+dfk[dfk['ViewData.Transaction Type'] == item].shape[0]
            else :
                m = m
        if m >0:
            return m
        else:
            return 0


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




