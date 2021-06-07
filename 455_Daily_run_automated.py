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
pd.set_option('mode.chained_assignment', None)


client = 'Lombard'

setup = '455'
setup_code = '455'

ReconSetupName = 'Lombard Caceis Cash Recon'


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

#def comgen(x,y,z,k):
#    if x == 'geneva':
#        
#        com = str(k) + ' ' + str(y) + ' ' + str(z)
#    else:
#        com = "Integrata" + ' ' + str(y) + ' ' + str(z)
#        
#    return com

def comgen(x,y,z,k,m,a,b,c,d):
    trade_ttype = ['buy','sell','sell short','cover short','spot fx','forward','forward fx','spotfx','forwardfx']
    pos_break = ['settlement amount , no pos break']
    x = x.lower()
    d = d.lower()

    if('aum' in d):
        y = 'booked the AUM plug for SD'
    elif('net sub/red' in d):
        y = 'booked the Net Sub/Red for SD'	
    elif('miscellaneous' in d):
        y = 'booked the Miscellaneous for SD'
    elif('subscription' in d):
        y = 'booked the subscription for SD'
    else:
        y = y

    if m in trade_ttype:
        if x != 'geneva':
            if ((a!= 0) & (b!= 0)):
                com = k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '. Integrata yet to book' 
            elif  ((a==0) & (b!=0)):
                com = k + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
            elif  ((a!=0) & (b==0)):
                com = k + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
            else:
                com = k + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
        else:
            if ((a!= 0) & (b!= 0)):
                com = 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '.'+ k+ ' yet to book' 
            elif  ((a==0) & (b!=0)):
                com = 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ k+ ' yet to book'
            elif  ((a!=0) & (b==0)):
                com = 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ k+ ' yet to book'
            else:
                com = 'Integrata' + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ k+ ' yet to book'
    
    elif m in pos_break:
        
        if ((a!= 0) & (b!= 0)):
            com ='No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b)
        elif  ((a==0) & (b!=0)):
            com = 'No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for quantity' +' ' + str(b) 
        elif  ((a!=0) & (b==0)):
            com = 'No position break, Geneva to reflect jpm trade on ' + str(z) + " " + 'for price' +' ' + str(a) 
        else:
            com = 'No position break, Geneva to reflect jpm trade on ' + str(z)
        
    else:
        if x != 'geneva':
        
            com = k + ' ' +y + ' ' + str(z) + ". Integrata yet to book"
        else:
            com = 'Integrata' + ' ' +y + ' ' + str(z)+ '.' + k + ' yet to book.'
        
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
    
        dummy['zero_sum_list'] = dummy[amount_column].apply(lambda x : len(set(x)))
        #dummy['zero_list_len'] = dummy['zero_list'].apply(lambda x : len(x))

        #dummy['diff_len'] = dummy['len_amount'] - dummy['zero_list_len']
        #dummy['zero_list_sum'] = dummy['zero_list'].apply(lambda x : round(abs(sum(x)),2))
    
    #dummy = pd.merge(dummy, pos , on = ['Custodian Account','Currency','Ticker1'], how = 'left')
        final_cols = filters + dummy_filter
    
        dummy['remove_mark'] = dummy.apply(lambda x :1 if ((x['len_amount'])/(x['zero_sum_list'])==2) else 0, axis =1)

        dummy = dummy[final_cols]
        df3 = pd.merge(df, dummy, on = filters, how = 'left')
        #print(df3.columns)
    
        df4 = df3[(df3['remove_mark']==1) & (df3['sel_mark']==1)]
    #print(df4.columns)
    
   
        if df4.shape[0]!=0:
#            k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#            k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#            k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
#            k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
#            k = pd.merge(k1, k2 , on = filters, how = 'left')
#            k = pd.merge(k, k3 , on = filters, how = 'left')
#            k = pd.merge(k, k4 , on = filters, how = 'left')
        
            df4['predicted status'] = 'UCB'
            df4['predicted action'] = 'Close'
            df4['predicted category'] = 'Close'
            df4['predicted comment'] = ''
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num) + ' ' + str(item)
#            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
            filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
#            filename = 'Lombard setup 249 ' + string_name + '.csv'

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
### Change 1:

def common_matching_engine_single2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num,param_client = client, param_setup_code = setup_code):
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
        
            df4['predicted status'] = 'UCB'
            df4['predicted action'] = 'Close'
            df4['predicted category'] = 'Close'
            df4['predicted comment'] = ''
            df4 = df4[columns_to_output]
        
        
            string_name = 'p'+str(serial_num) + ' ' + str(item)
#            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
            filename = str(param_client) +' setup ' + str(param_setup_code) + ' ' + string_name + '.csv'
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


# In[2285]:


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
            filename = 'Schonfield/pair result/setup 85 ' + string_name + '.csv'
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


# In[2402]:




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

def common_matching_engine_double2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num, param_client =  client, param_setup_code = setup_code):
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
            k1 = df4.groupby(filters)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
            k2 = df4.groupby(filters)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
            k3 = df4.groupby(filters)['ViewData.BreakID'].apply(list).reset_index()
            k4 = df4.groupby(filters)['ViewData.Status'].apply(list).reset_index()
            k = pd.merge(k1, k2 , on = filters, how = 'left')
            k = pd.merge(k, k3 , on = filters, how = 'left')
            k = pd.merge(k, k4 , on = filters, how = 'left')
        
            k['predicted status'] = 'pair'
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

def contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row):
    
    if(',' in str(fun_row['ViewData.Side0_UniqueIds'])):
        Side_0_contains_comma = 1
    else:
        Side_0_contains_comma = 0

    if(',' in str(fun_row['ViewData.Side1_UniqueIds'])):
        Side_1_contains_comma = 1
    else:
        Side_1_contains_comma = 0
    
    if((str(fun_row['ViewData.Status']) in ['OB','SDB','UOB','CNF','CMF']) and ((Side_0_contains_comma == 1) or (Side_1_contains_comma == 1))):
        return('remove')
    else:
        return('keep')

def apply_cancel_in_transaction_type(param_transaction_type_val):
	if('cancel' in str(param_transaction_type_val).lower()):
		return 1
	else:
		return 0
vec_apply_cancel_in_transaction_type = np.vectorize(apply_cancel_in_transaction_type)

def apply_closed_mark(param_col_val_to_extract_closed_mark_from, param_col_val_for_existing_closed_mark):
	if(param_col_val_to_extract_closed_mark_from == 1):
		return 1
	else:
		return param_col_val_for_existing_closed_mark
	
vec_apply_closed_mark = np.vectorize(apply_closed_mark)

def apply_Forward_swap_mark(param_transaction_type_val, param_currency_val, param_swap_transaction_types_list):
	if(str(param_transaction_type_val) in param_swap_transaction_types_list):
		if(str(param_currency_val) not in ['USD','']):
			return 1
		else:
			return 0
	else:
		return 0
	
vec_apply_Forward_swap_mark = np.vectorize(apply_Forward_swap_mark)
# ### Reading of MEO files

#Changes asked by Shitanshu for db on 24-03-2021
#Use 137 server
#Reading RecData collections (MEO data) DB name : ReconDB
#Writing ML Data DB name : ReconDB_ML
#Writing ML Data Collection Name : MLPrediction_ReconSetupCode. For eg. MLPrediction_125

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
#db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
#Changes asked by Shitanshu for db on 24-03-2021
db_for_reading_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB']
db_for_writing_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
db_2_for_MEO_data_MLReconDB_Testing = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML_Testing']


#for setup_code in setup_code_list:
print('Starting predictions for Weiss, setup_code = ')
print(setup_code)


query_1_for_MEO_data = db_for_reading_MEO_data['RecData_' + setup_code].find({ 
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
#meo_filename = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Lombard/meo_data/meo_df_setup_455_penultimate_date_time_' + str(penultimate_date_to_analyze_ymd_format) + '.csv'
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

#Change added on 01-03-2021 to remove records with multiple values of Side0 and Side1 UniqueIds for statuses like OB,UOB,SDB,CNF and CMF. Typically, these statuses should have single values in Side0 and Side1 UniqueIds. So records not following expected behviour are removed
meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = meo_df.apply(lambda row : contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(fun_row = row), axis = 1,result_type="expand")
meo_df = meo_df[~(meo_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]

meo_df['Date'] = pd.to_datetime(meo_df['Date']).dt.date

meo_df['Date'] = meo_df['Date'].astype(str)

## ## Sample data on one date
#
print('The Date value count is:')
print(meo_df['Date'].value_counts())

date_i = meo_df['Date'].mode()[0]

print('Choosing the date : ' + date_i)

date_to_analyze_ymd_format = date_i
penultimate_date_to_analyze_ymd_format = str((pd.to_datetime(date_i,format='%Y-%m-%d') - timedelta(1)).strftime('%Y-%m-%d'))
penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'

#os.chdir('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files')
##uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')
#
##Change made by Rohit on 09-12-2020 to make dynamic directories
## base dir
#base_dir = os.getcwd()       
#
## create dynamic name with date as folder
#base_dir = os.path.join(base_dir + '\\Setup_' + setup_code +'\\BD_of_' + str(date_i))
## create 'dynamic' dir, if it does not exist
#if not os.path.exists(base_dir):
#    os.makedirs(base_dir)
#
#os.chdir(base_dir)
#
## create dynamic name with date as folder
#base_dir_plus_Lombard = os.path.join(base_dir, client)
#
## create 'dynamic' dir, if it does not exist
#if not os.path.exists(base_dir_plus_Lombard):
#    os.makedirs(base_dir_plus_Lombard)
#
## create dynamic name with date as folder
#base_dir_plus_client_plus_setup_code = os.path.join(base_dir_plus_Lombard, setup_code)
#
## create 'dynamic' dir, if it does not exist
#if not os.path.exists(base_dir_plus_client_plus_setup_code):
#    os.makedirs(base_dir_plus_client_plus_setup_code)


os.chdir('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Lombard\\output_files')
#uni2 = pd.read_csv('Lombard/249/ReconDB.HST_RecData_249_01_10.csv')

#Change made by Rohit on 09-12-2020 to make dynamic directories
# base dir
base_dir = os.getcwd()       

# create dynamic name with date as folder
base_dir = os.path.join(base_dir + '\\Setup_' + setup_code)
# create 'dynamic' dir, if it does not exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)
os.chdir(base_dir)

recon_done_for_dates_folder_names = [name for name in os.listdir(".") if os.path.isdir(name)]

def get_date_subfolder_suffix(param_date, param_subfolder_list):
	greatest_element_number_suffix = 0
	for element in param_subfolder_list:
		if(param_date in element):
			if(element.split('_')[-1].isnumeric() == True):
				if(int(element.split('_')[-1]) > greatest_element_number_suffix):
					greatest_element_number_suffix = int(element.split('_')[-1])
				else:
					greatest_element_number_suffix = greatest_element_number_suffix
			else:
				greatest_element_number_suffix = greatest_element_number_suffix
		else:
			greatest_element_number_suffix = greatest_element_number_suffix
	
	return(greatest_element_number_suffix + 1)

suffix_for_BD_folder = get_date_subfolder_suffix(param_date = date_to_analyze_ymd_format, param_subfolder_list = recon_done_for_dates_folder_names)
base_dir = os.path.join(base_dir + '\\BD_of_' + str(date_i) + '_' + str(suffix_for_BD_folder))
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

os.chdir(base_dir)
'''
#Closed Code by Rohit Begins
meo_df['Closed_mark'] = 0

Transaction_type_values = meo_df['ViewData.Transaction Type'].values
meo_df['Cancel_mark'] = vec_apply_cancel_in_transaction_type(Transaction_type_values)
closed_df_cancel_mark_eq_1 = meo_df[meo_df['Cancel_mark'] == 1]

Cancel_mark_values = meo_df['Cancel_mark'].values
Closed_mark_values = meo_df['Closed_mark'].values

meo_df['Closed_mark'] = vec_apply_closed_mark(param_col_val_to_extract_closed_mark_from = Cancel_mark_values, param_col_val_for_existing_closed_mark = Closed_mark_values)

meo_df['ViewData.Transaction_Type_for_closing'] = meo_df['ViewData.Transaction Type']
meo_df['ViewData.Currency_for_closing'] = meo_df['ViewData.Currency']

swap_transaction_types_list = ['SPOT SWAP','FOREX (SALE)', 'FOREX (PURCHASE)','']

meo_df['ViewData.Transaction_Type_for_closing'] = meo_df['ViewData.Transaction_Type_for_closing'].astype(str)
meo_df['ViewData.Transaction_Type_for_closing'] = meo_df['ViewData.Transaction_Type_for_closing'].replace('Nan','')
meo_df['ViewData.Transaction_Type_for_closing'] = meo_df['ViewData.Transaction_Type_for_closing'].replace('None','')
meo_df['ViewData.Transaction_Type_for_closing'] = meo_df['ViewData.Transaction_Type_for_closing'].replace('nan','')

meo_df['ViewData.Currency_for_closing'] = meo_df['ViewData.Currency_for_closing'].astype(str)
meo_df['ViewData.Currency_for_closing'] = meo_df['ViewData.Currency_for_closing'].replace('Nan','')
meo_df['ViewData.Currency_for_closing'] = meo_df['ViewData.Currency_for_closing'].replace('None','')
meo_df['ViewData.Currency_for_closing'] = meo_df['ViewData.Currency_for_closing'].replace('nan','')

Transaction_Type_for_closing_values = meo_df['ViewData.Transaction_Type_for_closing'].values
Currency_for_closing_values = meo_df['ViewData.Currency_for_closing'].values

meo_df['Forward_swap_mark'] = vec_apply_Forward_swap_mark(param_transaction_type_val = Transaction_Type_for_closing_values, param_currency_val = Currency_for_closing_values, param_swap_transaction_types_list = swap_transaction_types_list)

#meo_df['Forward_swap_mark'] = meo_df['ViewData.Transaction_Type_for_closing'].apply(lambda x : 1 if x in swap_transaction_types_list else 0)

meo_df['Forward_swap_filter'] = meo_df['ViewData.Mapped Custodian Account'] + meo_df['ViewData.Currency']

#Closed Code by Rohit Ends
'''
df = meo_df.copy()
meo_df_copy = meo_df.copy()
del(meo_df)

df = df[df['ViewData.Status']!='Archive']

df[['len_0','len_1']] = df.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']), axis = 1)

df1 = df[(df['len_0']==0) | (df['len_1']==0) ]



side0 = df['ViewData.Side0_UniqueIds'].value_counts().reset_index()
side1 = df['ViewData.Side1_UniqueIds'].value_counts().reset_index()

side0_id = list(set(side0[side0['ViewData.Side0_UniqueIds']==1]['index']))
side1_id = list(set(side1[side1['ViewData.Side1_UniqueIds']==1]['index']))

df11 = df1[(df1['ViewData.Side0_UniqueIds'].isin(side0_id)) |(df1['ViewData.Side1_UniqueIds'].isin(side1_id)) ]
df2_umb = df[((df['ViewData.Status'] == 'UMB') & (df['len_0']>1) & (df['len_1']>1))]

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

df2 = df[(df['len_0']==1) & (df['len_1']==1)]

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
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
#     serial_num = 2
#     df2 = common_matching_engine_single2(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)

     

    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
    serial_num = 2
    df2 = common_matching_engine_double1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
#     serial_num = 4
#     df4 = common_matching_engine_double2(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
    
    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
    serial_num = 3
    df3 = common_matching_engine_single1(df2,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
#     serial_num = 6
#     df6 = common_matching_engine_single2(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)

     

    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
    serial_num = 4
    df4 = common_matching_engine_double1(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID','ViewData.Settle Date']
#     serial_num = 8
#     df8 = common_matching_engine_double2(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2427]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
    serial_num = 5
    df5 = common_matching_engine_single1(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
#     serial_num = 10
#     df10 = common_matching_engine_single2(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2428]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
    serial_num = 6
    df6 = common_matching_engine_double1(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
#     serial_num = 12
#     df12 = common_matching_engine_double2(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
    serial_num = 7
    df7 = common_matching_engine_single1(df6,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
#     serial_num = 14
#     df14 = common_matching_engine_single2(df13,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2428]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
    serial_num = 8
    df8 = common_matching_engine_double1(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
#     filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Investment ID']
#     serial_num = 16
#     df16 = common_matching_engine_double2(df15,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2429]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']
    serial_num = 9
    df9 = common_matching_engine_single1(df8,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#     serial_num = 18
#     df19 = common_matching_engine_single2(df18,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 10
    df10 = common_matching_engine_double1(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#     serial_num = 20
#     df21 = common_matching_engine_double2(df20,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2430]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
    serial_num = 11
    df11 = common_matching_engine_single1(df10,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#     serial_num = 22
#     df23 = common_matching_engine_single2(df22,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    
    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
    serial_num = 12
    df12 = common_matching_engine_double1(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)
#     serial_num = 24
#     df25 = common_matching_engine_double2(df24,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2431]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Settle Date']
    serial_num = 13
    df13 = common_matching_engine_single1(df12,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 14
    df14 = common_matching_engine_double1(df13,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[2432]:


    filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']
    serial_num = 15
    df15 = common_matching_engine_single1(df14,filters,columns_to_output, amount_column, dummy_filter,serial_num)
    serial_num = 16
    df16 = common_matching_engine_double1(df15,filters,columns_to_output, amount_column, dummy_filter,serial_num)





#     df12_1 = df16[((df16['ViewData.ISIN'].isna()) & (df16['ViewData.Investment ID'].isna())) ]
#     df12_2 = df30[~((df30['ViewData.ISIN'].isna()) & (df30['ViewData.Investment ID'].isna())) ]





#     df12_2['ViewData.ISIN'] = df12_2['ViewData.ISIN'].fillna('AAAA')



#     df12_2['ID'] = df12_2.apply(lambda x : x['ViewData.Investment ID'] if x['ViewData.ISIN']=='AAAA' else x['ViewData.ISIN'], axis =1 )


# # In[2448]:


#     columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']


# # In[2442]:


#     filter_umb = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ID']


# # In[2443]:


#     dummy = df12_2.groupby(filter_umb)[amount_column].apply(list).reset_index()
#     dummy1 = df12_2.groupby(filter_umb)['ViewData.Side0_UniqueIds'].count().reset_index()
#     dummy = pd.merge(dummy, dummy1 , on = filter_umb, how = 'left')
#     dummy2 = df12_2.groupby(filter_umb)['ViewData.Side1_UniqueIds'].count().reset_index()
#     dummy = pd.merge(dummy, dummy2 , on = filter_umb, how = 'left')
#     dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )





#     dummy0 = dummy[['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account',
#        'ViewData.Currency', 'ID', 
#        'sel_mark']]



#     dfk = pd.merge(df12_2, dummy0, on = filter_umb, how = 'left')



#     dfk4 = dfk[(dfk['sel_mark']==1)]
  
#     serial_num = 13  

#     if dfk4.shape[0]!=0:
#         k1 = dfk4.groupby(filter_umb)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
#         k2 = dfk4.groupby(filter_umb)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
#         k3 = dfk4.groupby(filter_umb)['ViewData.BreakID'].apply(list).reset_index()
#         k4 = dfk4.groupby(filter_umb)['ViewData.Status'].apply(list).reset_index()
#         k = pd.merge(k1, k2 , on = filter_umb, how = 'left')
#         k = pd.merge(k, k3 , on = filter_umb, how = 'left')
#         k = pd.merge(k, k4 , on = filter_umb, how = 'left')
        
#         k['predicted status'] = 'UMB'
#         k['predicted action'] = 'UMB'
#         k['predicted category'] = 'UMB'
#         k['predicted comment'] = 'difference in amount'
#         k = k[columns_to_output]
        
        
#         string_name = 'p'+str(serial_num) + ' ' + str(item)
# #        filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
#         filename = str(client) +' setup ' + str(setup_code) + ' ' + string_name + '.csv'
#         k.to_csv(filename)
    
#         dfk5 = dfk[(dfk['sel_mark']!=1)]
#     else:
#         dfk5 = dfk.copy()


#     serial_num = 14
#     if dfk5[((dfk5['ViewData.Status'] == 'UMB') | (dfk5['ViewData.Status'] == 'SMB'))].shape[0]!=0:
#         string_name = 'p'+str(serial_num) + ' ' + str(item)
# #        filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
#         filename = str(client) +' setup ' + str(setup_code) + ' ' + string_name + '.csv'
#         dfk6 = dfk5[dfk5['ViewData.Status'] == 'UMB'][columns_to_output]
#         dfk6.to_csv(filename)
#         ob = dfk5[dfk5['ViewData.Status'] != 'UMB']
#     else:
#         ob = dfk5.copy()

    df3 = df16.copy()
	
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

    df3['ViewData.Side0_UniqueIds'] = df3['ViewData.Side0_UniqueIds'].fillna('AA')
    df3['ViewData.Side1_UniqueIds'] = df3['ViewData.Side1_UniqueIds'].fillna('BB')

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

#    demo = []
#    for item_cb_predict in cb_predictions:
#        demo.append(item_cb_predict[0])
#    df3['predicted category'] = pd.Series(demo)
    df3['predicted category'] = np.concatenate(cb_predictions)
#    com_temp = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Lombard comment template for delivery.csv')
    com_temp = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + str(client) +'\\output_files\\Setup_' + str(setup_code) + '\\Lombard 455 comment template for delivery.csv')
    com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
#Change made on 19-02-2021 as per Pratik
#Begin change made on 19-02-2021
#    result_non_trade = df3.copy()
#    result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
#
#    result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#    result_non_trade['predicted status'] = 'OB'
#    result_non_trade['predicted action'] = 'No-pair'
#    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
#    path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
#
##    path = 'Lombard/249/' + str(item) + 'Comment file for lombard 249.csv'
#   
#    result_non_trade.to_csv(path)
#Begin change made on 19-02-2021
    result_non_trade = df3.copy()
    result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')

    result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : x.split('-')[0] if type(x)== str else x)
    result_non_trade['new_pb1'] = result_non_trade['new_pb1'].fillna('ms')

#Change made on 17-02-2021 as per Abhijeet. 
    result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
    result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
    result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].dt.date
    result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date2'].astype(str)
#    result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].dt.date
    result_non_trade['ViewData.Trade Date2'] = pd.to_datetime(result_non_trade['ViewData.Trade Date']).dt.date
    result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date2'].astype(str)
    result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)
	
    result_non_trade['new_pb1'] = result_non_trade['new_pb1'].replace('caceis','Caceis')
    result_non_trade['new_pb1'] = result_non_trade['new_pb1'].replace('','Caceis')
    #result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : brokermap(x))
	
	#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
	#Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
	#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
    result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date'],x['ViewData.Description']), axis = 1)
    result_non_trade['predicted status'] = 'OB'
    result_non_trade['predicted action'] = 'No-pair'
    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
    path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
    result_non_trade.to_csv(path)
	
#    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]



#    result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
    result_non_trade['predicted status'] = 'OB'
    result_non_trade['predicted action'] = 'No-pair'
    result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
    path = 'Comment file for ' + str(client) + ' ' + str(setup_code) + ' ' + str(item) + '.csv'
	
	
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

final_df.to_csv('final_df.csv')

def remove_duplicate_commas_from_string(param_str):
    if(param_str == ''):
        return('')
    else:
        result = (re.findall(r"([\w ]+)",param_str))
        expected_param_str = result[0]
        for i in range(1, len(result)):
            expected_param_str = expected_param_str + ',' + result[i]
        return(expected_param_str) 
		
	
def replace_leading_trailing_comma_if_str_startswith_or_endswith_comma(param_str):
	if(param_str.startswith(',')):
		param_str = param_str[1:]
	if(param_str.startswith('\',')):
		param_str = param_str[2:]
	if(param_str.endswith(',')):
		param_str = param_str[:-1]
	if(param_str.endswith(',\'')):
		param_str = param_str[:-2]
	if(set(param_str) == set(',')):
		param_str = ''
	return(param_str)

	
def clean_list_columns(param_df, param_list_col_name):
	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : str(x).replace('[','').replace(']','').replace('\'','').replace(', ',',').replace('None','').replace('nan','').replace('Nan','').replace('NaN','').replace('AA','').replace('BB','').replace('\\n','').replace('.0',''))
	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : str(x).replace(',,',',').replace('\',','').replace(',\'',''))
	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : replace_leading_trailing_comma_if_str_startswith_or_endswith_comma(str(x)))
	param_df[param_list_col_name] = param_df[param_list_col_name].apply(lambda x : remove_duplicate_commas_from_string(str(x)))
	return(param_df)	
		
#final_df['Side0_UniqueIds'].apply(lambda x : str(x).replace('[','').replace(']','').replace('\'','').replace(', ',',').replace('None',''))
final_df = clean_list_columns(param_df = final_df, param_list_col_name = 'Side0_UniqueIds')
final_df = clean_list_columns(param_df = final_df, param_list_col_name = 'Side1_UniqueIds')
final_df = clean_list_columns(param_df = final_df, param_list_col_name = 'BreakID')

def unlist_comma_separated_single_quote_string_lst(list_obj):
    new_list = []
    for i in list_obj:
        list_i = list(i.replace('\'','').split(', '))
        for j in list_i:
            new_list.append(j)
    return new_list


#BreakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['BreakID'].astype(str).unique().tolist())

def get_first_non_null_value(string_of_values_separated_by_comma):
    if(string_of_values_separated_by_comma != '' and string_of_values_separated_by_comma != 'nan' and string_of_values_separated_by_comma != 'None' ):
        if(string_of_values_separated_by_comma.partition(',')[0] != '' and string_of_values_separated_by_comma.partition(',')[0] != 'nan' and string_of_values_separated_by_comma.partition(',')[0] != 'None'):
            return(string_of_values_separated_by_comma.partition(',')[0])
        else:
            return(get_first_non_null_value(string_of_values_separated_by_comma.partition(',')[2]))
    else:
        return('Blank value')        


final_df['first_non_null_breakid_in_breakid_columns'] = final_df['BreakID'].apply(lambda x : get_first_non_null_value(str(x)))
final_df['first_non_null_breakid_in_breakid_columns'] = final_df['first_non_null_breakid_in_breakid_columns'].map(lambda x:x.lstrip('').rstrip(''))
final_df.to_csv('final_df_v2.csv')

meo_df_copy['ViewData.BreakID'] = meo_df_copy['ViewData.BreakID'].astype(str)
final_df_copy = pd.merge(final_df, meo_df_copy[['ViewData.BreakID','ViewData.Task ID','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'first_non_null_breakid_in_breakid_columns', right_on = 'ViewData.BreakID', how='left')

final_df_copy['BusinessDate'] = date_i
final_df_copy.to_csv('final_df_copy.csv')

final_df_copy['BusinessDate'] = pd.to_datetime(final_df_copy['BusinessDate'])
final_df_copy['BusinessDate'] = final_df_copy['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_copy['BusinessDate'] = pd.to_datetime(final_df_copy['BusinessDate'])

final_df_copy['BreakID_list'] = final_df_copy['BreakID'].apply(lambda x : x.split(','))

final_df_copy['BreakID_to_insert_in_db'],final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['BreakID_list'].apply(lambda x : x[0]),final_df_copy['BreakID_list'].apply(lambda x : x[1:])

final_df_copy['BreakID_to_insert_in_db'] = final_df_copy['BreakID_to_insert_in_db'].astype(str)
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].astype(str)

final_df_copy['BreakID_to_insert_in_db'] = final_df_copy['BreakID_to_insert_in_db'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace('\' ','\'', regex = True)
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace(', ',',', regex = True)
final_df_copy['Predicted_BreakID_to_insert_in_db'] = final_df_copy['Predicted_BreakID_to_insert_in_db'].replace('\'','', regex = True)

final_df_copy['ML_flag'] = 'ML'

final_df_copy['SetupID'] = setup_code

final_df_copy['probability_No_pair'] = ''
final_df_copy['probability_UMB'] = ''
final_df_copy['probability_UMR'] = ''
final_df_copy['probability_UMT'] = ''

#cols_for_database = ['BreakID', 'BusinessDate', 'Final_predicted_break', 'ML_flag',
#       'Predicted_Status', 'Predicted_action', 'SetupID',
#       'SourceCombinationCode', 'TaskID', 'probability_No_pair',
#       'probability_UMB', 'probability_UMR', 'probability_UMT',
#       'Side1_UniqueIds', 'PredictedComment', 'PredictedCategory',
#       'Side0_UniqueIds']    

filepaths_final_df_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'
final_df_copy.to_csv(filepaths_final_df_copy)

cols_for_database = ['Side1_UniqueIds','Side0_UniqueIds',
'predicted status', 
#'Predicted_Status',
'predicted action', 'predicted category',
'predicted comment',
'ViewData.Task ID',
'ViewData.Source Combination Code', 'BusinessDate',
'BreakID_to_insert_in_db', 'Predicted_BreakID_to_insert_in_db',
'ML_flag', 'SetupID', 'probability_No_pair', 'probability_UMB',
'probability_UMR', 'probability_UMT']    


final_df_2 = final_df_copy[cols_for_database]

cols_for_database_rename_dict = {'predicted status' : 'Predicted_Status',
                                 'predicted action' : 'Predicted_action',
                                 'predicted category' : 'PredictedCategory',
                                 'predicted comment' : 'PredictedComment',
                                 'ViewData.Task ID' : 'TaskID',
                                 'ViewData.Source Combination Code' : 'SourceCombinationCode',
                                 'BreakID_to_insert_in_db' : 'BreakID',
                                 'Predicted_BreakID_to_insert_in_db' : 'Final_predicted_break'}

final_df_2 = final_df_2.rename(columns = cols_for_database_rename_dict)
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

final_df_2['BreakID'] = final_df_2['BreakID'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].astype(str)
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x:x.lstrip('[').rstrip(']'))

final_df_2_copy_2 = final_df_2.copy()
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])
final_df_2['BusinessDate'] = final_df_2['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['BusinessDate'] = pd.to_datetime(final_df_2['BusinessDate'])

final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)
#def find_taskid_for_breakid_value_apply_function(fun_breakid_string_value,fun_meo_df):
#    return(fun_meo_df[fun_meo_df['ViewData.BreakID'] == int(fun_breakid_string_value)]['ViewData.Task ID'].unique())

meo_df['ViewData.BreakID'] = meo_df['ViewData.BreakID'].astype(str)
single_TaskID_value_for_455 = meo_df['ViewData.Task ID'].mode()[0]
if(setup_code == '455'):
	final_df_2['TaskID'] = single_TaskID_value_for_455 
	#final_df_2['TaskID'] = final_df_2['BreakID'].apply(lambda x : meo_df[meo_df['ViewData.BreakID'] == x]['ViewData.Task ID'].unique())
	#
	#final_df_2['TaskID'] = final_df_2['TaskID'].astype(str)
	#final_df_2['TaskID'] = final_df_2['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))
	#
	##final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
	##
	##final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
	#
	##final_df_2 =  final_df_2[~(final_df_2['TaskID'] == '')]
	#final_df_2 = final_df_2[final_df_2['TaskID'] != '']
	#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))
	
	final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)
elif(setup_code == '249'):
	final_df_2['TaskID'] = single_TaskID_value_for_455 
	final_df_2['TaskID'] = final_df_2['BreakID'].apply(lambda x : meo_df[meo_df['ViewData.BreakID'] == x]['ViewData.Task ID'].unique())
	
	final_df_2['TaskID'] = final_df_2['TaskID'].astype(str)
	final_df_2['TaskID'] = final_df_2['TaskID'].map(lambda x:x.lstrip('[').rstrip(']'))
	
	#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
	#
	#final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : np.int64(x)[0])
	
	#final_df_2 =  final_df_2[~(final_df_2['TaskID'] == '')]
	final_df_2 = final_df_2[final_df_2['TaskID'] != '']
	final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))
	
	final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)
	


final_df_2[['SourceCombinationCode']] = final_df_2[['SourceCombinationCode']].astype(str)
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('[').rstrip(']'))
final_df_2['SourceCombinationCode'] = final_df_2['SourceCombinationCode'].map(lambda x:x.lstrip('\'').rstrip('\''))

final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)

final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace('\'','',regex = True)
final_df_2['BreakID'] = final_df_2['BreakID'].replace('\'','',regex = True)
final_df_2['BreakID'] = final_df_2['BreakID'].replace(', ',',',regex = True)
final_df_2['Final_predicted_break'] = final_df_2['Final_predicted_break'].replace(', ',',',regex = True)

final_df_2['ReconSetupName'] = ReconSetupName
final_df_2['ClientShortCode'] = client

today = date.today()
today_Y_m_d = today.strftime("%Y-%m-%d")

final_df_2['CreatedDate'] = today_Y_m_d
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])
final_df_2['CreatedDate'] = final_df_2['CreatedDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
final_df_2['CreatedDate'] = pd.to_datetime(final_df_2['CreatedDate'])

final_df_2 = final_df_2[final_df_2['TaskID'] != '']
final_df_2['TaskID'] = final_df_2['TaskID'].apply(lambda x : float(x))

final_df_2['TaskID'] = final_df_2['TaskID'].astype(np.int64)

final_df_2[['SetupID']] = final_df_2[['SetupID']].astype(int)
#final_df_2['PredictedComment'] = final_df_2['PredictedComment'].apply(lambda x : x.replace('nan','test'))
final_df_2[['Predicted_Status']] = final_df_2[['Predicted_Status']].astype(str)
final_df_2[['Predicted_action']] = final_df_2[['Predicted_action']].astype(str)

def apply_ui_action_column_Lombard(fun_row):
    if(fun_row['ML_flag'] == 'Not_Covered_by_ML'):
        ActionType = 'No Prediction'
        ActionTypeCode = 7
    else:
        if((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'No Action'
            ActionTypeCode = 6 
        elif((fun_row['Predicted_Status'] == 'OB') & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'COMMENT'
            ActionTypeCode = 3
        elif((fun_row['Predicted_Status'] == 'UCB') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'CLOSE'
            ActionTypeCode = 2 
        elif(((fun_row['Predicted_Status'] == 'UMB many to many') or (fun_row['Predicted_action'] == 'UMB one to many') or (fun_row['Predicted_action'] == 'UMB one to one') or (fun_row['Predicted_action'] == 'UMR') or (fun_row['Predicted_action'] == 'UMB')) & (fun_row['PredictedComment'] == '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'PAIR'
            ActionTypeCode = 1 
        elif(((fun_row['Predicted_action'] == 'UMB many to many') or (fun_row['Predicted_action'] == 'UMB one to many') or (fun_row['Predicted_action'] == 'UMB one to one') or (fun_row['Predicted_action'] == 'UMR') or (fun_row['Predicted_action'] == 'UMB')) & (fun_row['PredictedComment'] != '') & (fun_row['ML_flag'] == 'ML')):
            ActionType = 'PAIR WITH COMMENT'
            ActionTypeCode = 4 
        else:
            ActionType = 'Status not covered'
            ActionTypeCode = 0
    return ActionType,int(ActionTypeCode)

final_df_2[['ActionType','ActionTypeCode']] = final_df_2.apply(lambda row : apply_ui_action_column_Lombard(fun_row = row), axis = 1,result_type="expand")            
final_df_2['ActionTypeCode'] = final_df_2['ActionTypeCode'].astype(int)
filepaths_final_df_2 = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_df_2_setup_' + setup_code + '_date_' + str(date_i) + '2.csv'
final_df_2.to_csv(filepaths_final_df_2)

#coll_1_for_writing_prediction_data = db_for_MEO_data['MLPrediction_Cash']
#Changes asked by Shitanshu for db on 24-03-2021
coll_1_for_writing_prediction_data = db_for_writing_MEO_data['MLPrediction_' + setup_code]
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing = db_2_for_MEO_data_MLReconDB_Testing['MLPrediction_Cash']

#coll_1_for_writing_prediction_data = db_1_for_MEO_data['MLPrediction_Cash']
#coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing = db_2_for_MEO_data_MLReconDB_Testing['MLPrediction_Cash']

#coll_1_for_writing_prediction_data.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
#coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.remove({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})

coll_1_for_writing_prediction_data.delete_many({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
#coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.delete_many({ "BusinessDate": getDateTimeFromISO8601String(date_to_analyze_ymd_iso_00_00_format), "SetupID": int(setup_code)})
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.drop()

data_dict = final_df_2.to_dict("records_final")
coll_1_for_writing_prediction_data.insert_many(data_dict) 

data_dict_for_testingdb = final_df_2.to_dict("records_final_for_testingdb")
coll_2_for_writing_prediction_data_in_ReconDB_ML_Testing.insert_many(data_dict_for_testingdb) 

print(setup_code)
print(date_i)
'''
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

#meo_df = json_normalize(list_of_dicts_query_for_MEO_data)
#meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
#meo_df['ViewData.Task Business Date'] = meo_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
#meo_df.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Lombard/meo_data/meo_df_setup_455_penultimate_date_time_' + str(penultimate_date_to_analyze_ymd_format) + '.csv')

#aua_df = json_normalize(list_of_dicts_query_for_AUA_data)
#aua_df = aua_df.loc[:,aua_df.columns.str.startswith('ViewData')]
#aua_df['ViewData.Task Business Date'] = aua_df['ViewData.Task Business Date'].apply(dt.datetime.isoformat) 
#aua_df.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Lombard/aua_data/aua_df_setup_455_penultimate_date_time_' + str(penultimate_date_to_analyze_ymd_format) + '.csv')

    
print(meo_df_copy.shape[0])
count_docs = ReconDB_ML_Testing_137_server['RecData_' + setup_code].count_documents({'PredictionInfo': {'$exists' : True}})
print(count_docs)
'''
