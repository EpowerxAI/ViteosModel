#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import numpy as np
import os
import dask.dataframe as dd
import glob

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

def common_matching_engine_single1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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
        
            k['predicted status'] = 'pair'
            k['predicted action'] = 'UMR'
            k['predicted category'] = 'match'
            k['predicted comment'] = 'match'
            k = k[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
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

def common_matching_engine_single2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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

def common_matching_engine_double1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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
        
            k['predicted status'] = 'pair'
            k['predicted action'] = 'UMR'
            k['predicted category'] = 'match'
            k['predicted comment'] = 'match'
            k = k[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
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

def common_matching_engine_double2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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

# Function for reconciliation involving both sides
def common_matching_engine_double3(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
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
            k['predicted comment'] = 'match'
            k = k[columns_to_output]
        
        
            string_name = 'p'+str(serial_num)
            filename = 'Schonfield/pair result/setup 85 ' + string_name + '.csv'
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


# ### Reading of MEO files

df = pd.read_csv('Lombard/249/meo_df_setup_249_penultimate_date_time_2020-12-15.csv')

# #### Treating th duplication issues.
df[['len_0','len_1']] = df.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']), axis = 1)
df1 = df[(df['len_0']==0) | (df['len_1']==0) ]
df2 = df[~((df['len_0']==0) | (df['len_1']==0)) ]

side0 = df['ViewData.Side0_UniqueIds'].value_counts().reset_index()
side1 = df['ViewData.Side1_UniqueIds'].value_counts().reset_index()

side0_id = list(set(side0[side0['ViewData.Side0_UniqueIds']==1]['index']))
side1_id = list(set(side1[side1['ViewData.Side1_UniqueIds']==1]['index']))

df11 = df1[(df1['ViewData.Side0_UniqueIds'].isin(side0_id)) |(df1['ViewData.Side1_UniqueIds'].isin(side1_id)) ]

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

meo_df1 = meo_df.copy()

#vital_cols = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.ISIN','ViewData.Investment Type','ViewData.Investment ID','ViewData.Transaction Type','ViewData.Description','ViewData.Settle Date','ViewData.Trade Date','ViewData.Net Amount Difference','ViewData.Status']

dummy_filter = ['remove_mark','sel_mark']
columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
amount_column = 'ViewData.Net Amount Difference'

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
serial_num = 1
df1 = common_matching_engine_single1(meo_df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN','ViewData.Settle Date']
serial_num = 2
df2 = common_matching_engine_double1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
serial_num = 3
df3 = common_matching_engine_single1(df2,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.ISIN']
serial_num = 4
df4 = common_matching_engine_double1(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']
serial_num = 5
df5 = common_matching_engine_single1(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 6
df6 = common_matching_engine_double1(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']
serial_num = 7
df7 = common_matching_engine_single1(df6,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 8
df8 = common_matching_engine_double1(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Settle Date']
serial_num = 9
df9 = common_matching_engine_single1(df8,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 10
df10 = common_matching_engine_double1(df9,filters,columns_to_output, amount_column, dummy_filter,serial_num)

filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']
serial_num = 11
df11 = common_matching_engine_single1(df10,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 12
df12 = common_matching_engine_double1(df11,filters,columns_to_output, amount_column, dummy_filter,serial_num)

df12_1 = df12[((df12['ViewData.ISIN'].isna()) & (df12['ViewData.Investment ID'].isna())) ]
df12_2 = df12[~((df12['ViewData.ISIN'].isna()) & (df12['ViewData.Investment ID'].isna())) ]

df12_2['ViewData.ISIN'] = df12_2['ViewData.ISIN'].fillna('AAAA')

df12_2['ID'] = df12_2.apply(lambda x : x['ViewData.Investment ID'] if x['ViewData.ISIN']=='AAAA' else x['ViewData.ISIN'], axis =1 )

columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']

filter_umb = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ID']

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
    #print(df4.columns)
serial_num = 13  

if dfk4.shape[0]!=0:
    k1 = dfk4.groupby(filter_umb)['ViewData.Side0_UniqueIds'].apply(list).reset_index()
    k2 = dfk4.groupby(filter_umb)['ViewData.Side1_UniqueIds'].apply(list).reset_index()
    k3 = dfk4.groupby(filter_umb)['ViewData.BreakID'].apply(list).reset_index()
    k4 = dfk4.groupby(filter_umb)['ViewData.Status'].apply(list).reset_index()
    k = pd.merge(k1, k2 , on = filter_umb, how = 'left')
    k = pd.merge(k, k3 , on = filter_umb, how = 'left')
    k = pd.merge(k, k4 , on = filter_umb, how = 'left')
        
    k['predicted status'] = 'No pair'
    k['predicted action'] = 'UMB'
    k['predicted category'] = 'UMB'
    k['predicted comment'] = 'difference in amount'
    k = k[columns_to_output]
        
        
    string_name = 'p'+str(serial_num)
    filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
    k.to_csv(filename)
    
    dfk5 = dfk[(dfk['sel_mark']!=1)]
else:
    dfk5 = dfk.copy()

serial_num = 14
if dfk5[dfk5['ViewData.Status'] == 'UMB'].shape[0]!=0:
    string_name = 'p'+str(serial_num)
    filename = 'Lombard/249/setup 249 ' + string_name + '.csv'
    dfk6 = dfk5[dfk5['ViewData.Status'] == 'UMB'][columns_to_output]
    dfk6.to_csv(filename)
    ob = dfk5[dfk5['ViewData.Status'] != 'UMB']
else:
    ob = dfk5.copy()

import pandas as pd
import math

from dateutil.parser import parse
import operator
import itertools

import re
import os

df3 = ob.copy()

df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')
def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys
df['tuple'] = df.apply(make_dict, axis=1)
clean_map_dict = df.set_index('tuple')['Value'].to_dict()

df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Asset Type Category'] = df3['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Investment Type'] = df3['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Prime Broker'] = df3['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)

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

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)

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

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)

import re

com = pd.read_csv('desc cat with naveen oaktree.csv')
cat_list = list(set(com['Pairing']))

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

df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

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

df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
com = com.drop(['var','Catogery'], axis = 1)
com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())

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
    
df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

comp = ['inc','stk','corp ','llc','pvt','plc']
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
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
        
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}
def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x
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

cols = ['ViewData.Transaction Type1',
 'ViewData.Asset Type Category1',
 'ViewData.Investment Type1','new_desc_cat', 'new_pb2',
 'new_pb1',
 'comm_marker',
 'monthend marker']

df4 = df3[cols]

df4['ViewData.Transaction Type1'] = df4['ViewData.Transaction Type1'].fillna('aa')
df4['ViewData.Asset Type Category1'] = df4['ViewData.Asset Type Category1'].fillna('aa')
df4['ViewData.Investment Type1'] = df4['ViewData.Investment Type1'].fillna('aa')
df4['new_desc_cat'] = df4['new_desc_cat'].fillna('aa')
df4['new_pb2'] = df4['new_pb2'].fillna('aa')
df4['new_pb1'] = df4['new_pb1'].fillna('aa')
df4['comm_marker'] = df4['comm_marker'].fillna('aa')
df4['monthend marker'] = df4['monthend marker'].fillna('aa')

import pickle
filename = 'finalized_model_lombard_249_v1.sav'
clf = pickle.load(open(filename, 'rb'))

cb_predictions = clf.predict(df4)

demo = []
for item in cb_predictions:
    demo.append(item[0])
df3['predicted category'] = pd.Series(demo)

com_temp = pd.read_csv('lobard 249 comment template for delivery.csv')
com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
result_non_trade = df3.copy()
result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
def comgen(x,y,z,k):
    if x == 'geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "geneva" + ' ' +y + ' ' + str(z)
        
    return com

result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
result_non_trade['predicted status'] = 'comment'
result_non_trade['predicted action'] = 'OB'
result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
result_non_trade.to_csv('Lombard/249/Comment file for lombard 249.csv')


# ### Training for UMR starts here
uni2 = pd.read_csv('Lombard/249/ReconDB_ML_Testing.249_Abhijeet_ask_by_Rohit_5Jan_to_25Jan_audit_trail_without_OB.csv')

#uni2.to_csv('Lombard/249/ReconDB_ML_Testing.249_Abhijeet_ask_by_Rohit_5Jan_to_25Jan_audit_trail_without_OB.csv')

# #### Removal of multiple ID phenomenon
uni2[['len_0','len_1']] = uni2.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']), axis = 1)

side0_single = uni2[(uni2['len_0']==1) & (uni2['len_1']==0)]
side1_single = uni2[(uni2['len_0']==0) & (uni2['len_1']==1)]

side0_double = uni2[(uni2['len_0']==1) & (uni2['len_1']==1)]

# #### Many to Many UMB

umbmtm = uni2[(uni2['ViewData.Status']=='UMB') & (uni2['len_0']>1) & (uni2['len_1']>1) ][pair_col]

umbmtm1 = umbmtm[(~umbmtm['ViewData.Accounting ISIN'].isna()) & (~umbmtm['ViewData.B-P ISIN'].isna())]

umbmtm1['isin'] = umbmtm1.apply(lambda x : 1 if x['ViewData.Accounting ISIN'] == x['ViewData.B-P ISIN'] else 0, axis =1 )

uni2 = pd.concat([side0_single, side1_single, side0_double], axis = 0)

uni2 = uni2.reset_index()
uni2.drop('index', axis = 1, inplace = True)

# #### Duplication in SDB, UDB and UOB

dup = uni2.groupby('ViewData.Side0_UniqueIds')['ViewData.Status'].apply(list).reset_index()

dup['set_st'] = dup['ViewData.Status'].apply(lambda x : list(set(x)))

dup['len_set_st'] = dup['set_st'].apply(lambda x : len(x))

zero_id = list(set(dup[dup['len_set_st']==1]['ViewData.Side0_UniqueIds']))

dup = uni2.groupby('ViewData.Side1_UniqueIds')['ViewData.Status'].apply(list).reset_index()
dup['set_st'] = dup['ViewData.Status'].apply(lambda x : list(set(x)))
dup['len_set_st'] = dup['set_st'].apply(lambda x : len(x))

one_id = list(set(dup[dup['len_set_st']==1]['ViewData.Side1_UniqueIds']))

uni2 = uni2[(uni2['ViewData.Side0_UniqueIds'].isin(zero_id)) | (uni2['ViewData.Side1_UniqueIds'].isin(one_id))]

pair_col = ['DataSides.0.Mapped Custodian Account','ViewData.Accounting Asset Type Category','ViewData.Accounting CUSIP','ViewData.Accounting Currency',
            'ViewData.Accounting Fund','ViewData.Accounting ISIN','ViewData.Accounting Investment ID','ViewData.Accounting Investment Type','ViewData.Accounting Net Amount', 'ViewData.Accounting Prime Broker',
            'ViewData.Accounting Quantity','ViewData.Accounting Ticker','ViewData.Accounting Transaction Type','ViewData.Accounting Settle Date','ViewData.Accounting Trade Date','DataSides.1.Mapped Custodian Account',
            'ViewData.B-P Asset Type Category','ViewData.B-P CUSIP','ViewData.B-P Currency',
            'ViewData.B-P Fund','ViewData.B-P ISIN','ViewData.B-P Investment ID','ViewData.B-P Investment Type','ViewData.B-P Net Amount', 'ViewData.B-P Prime Broker',
            'ViewData.B-P Quantity','ViewData.B-P Ticker','ViewData.B-P Transaction Type','ViewData.B-P Settle Date','ViewData.B-P Trade Date','ViewData.Status']

pair_st = ['UMR']

pair = uni2[uni2['ViewData.Status'].isin(pair_st)][pair_col]

pair = pair.rename(columns = {'DataSides.0.Mapped Custodian Account':'ViewData.Accounting Mapped Custodian Account',
                             'DataSides.1.Mapped Custodian Account':'ViewData.B-P Mapped Custodian Account'})

# #### Finding UMB

umb_st = ['UMB']

umb = uni2[uni2['ViewData.Status'].isin(umb_st)][pair_col]

umb = umb.rename(columns = {'DataSides.0.Mapped Custodian Account':'ViewData.Accounting Mapped Custodian Account',
                             'DataSides.1.Mapped Custodian Account':'ViewData.B-P Mapped Custodian Account'})

# #### Creation of No-pair

side0_col = ['DataSides.0.Mapped Custodian Account','ViewData.Accounting Asset Type Category','ViewData.Accounting CUSIP','ViewData.Accounting Currency',
            'ViewData.Accounting Fund','ViewData.Accounting ISIN','ViewData.Accounting Investment ID','ViewData.Accounting Investment Type','ViewData.Accounting Net Amount', 'ViewData.Accounting Prime Broker',
            'ViewData.Accounting Quantity','ViewData.Accounting Ticker','ViewData.Accounting Transaction Type','ViewData.Accounting Settle Date','ViewData.Accounting Trade Date','ViewData.Task Business Date'
           ]

side1_col = ['DataSides.1.Mapped Custodian Account',
            'ViewData.B-P Asset Type Category','ViewData.B-P CUSIP','ViewData.B-P Currency',
            'ViewData.B-P Fund','ViewData.B-P ISIN','ViewData.B-P Investment ID','ViewData.B-P Investment Type','ViewData.B-P Net Amount', 'ViewData.B-P Prime Broker',
            'ViewData.B-P Quantity','ViewData.B-P Ticker','ViewData.B-P Transaction Type','ViewData.B-P Settle Date','ViewData.B-P Trade Date','ViewData.Task Business Date']

nopair_st = ['SDB','UDB']

nopair0 = uni2[(uni2['ViewData.Status'].isin(nopair_st)) & (uni2['ViewData.Side1_UniqueIds'].isna())][side0_col]
nopair1 = uni2[(uni2['ViewData.Status'].isin(nopair_st)) & (uni2['ViewData.Side0_UniqueIds'].isna())][side1_col]

nopair0 = nopair0.rename(columns = {'DataSides.0.Mapped Custodian Account':'ViewData.Accounting Mapped Custodian Account'})
nopair1 = nopair1.rename(columns = {'DataSides.1.Mapped Custodian Account':'ViewData.B-P Mapped Custodian Account'})
                            
from pandas import merge
from tqdm import tqdm

pool =[]
key_index =[]
training_df =[]
call1 = []

appended_data = []

no_pair_ids = []
#max_rows = 5

k = list(set(list(set(nopair0['ViewData.Task Business Date'])) + list(set(nopair1['ViewData.Task Business Date']))))
k1 = k

for d in tqdm(k1):
    aa1 = nopair0[nopair0['ViewData.Task Business Date']==d]
    bb1 = nopair1[nopair1['ViewData.Task Business Date']==d]
#     aa1['marker'] = 1
#     bb1['marker'] = 1
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    print(aa1.shape)
    print(bb1.shape)
    
#     aa1.columns = ['SideB.' + x  for x in aa1.columns] 
#     bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
    cc1 = pd.merge(aa1,bb1, left_on = ['ViewData.Accounting Mapped Custodian Account','ViewData.Accounting Currency'], right_on = ['ViewData.B-P Mapped Custodian Account','ViewData.B-P Currency'], how = 'outer')
    appended_data.append(cc1)
   
nopair = pd.concat(appended_data)

nopair = nopair.drop_duplicates()

# #### Training File creation

# - UMT/UMR training file creation

nopair1 = nopair.head(1000)
nopair1['ViewData.Status'] = "OB"
nopair1.drop(['ViewData.Task Business Date_x','ViewData.Task Business Date_y'], axis = 1,inplace = True)

data = pd.concat([pair,nopair1], axis = 0)
data = data.reset_index()
data.drop('index', axis =1 , inplace = True)

# - Umb dataframe for training

nopair1 = nopair.head(150000)
nopair1['ViewData.Status'] = "OB"
nopair1.drop(['ViewData.Task Business Date_x','ViewData.Task Business Date_y'], axis = 1,inplace = True)

data = pd.concat([umb,nopair1], axis = 0)
data = data.reset_index()
data.drop('index', axis =1 , inplace = True)

# #### Feature creation

# - For UMT and UMR
import random

data['Amount_diff'] = data['ViewData.Accounting Net Amount'] - data['ViewData.B-P Net Amount']

#data['Amount_diff'] = data.apply(lambda x : random.uniform(1.0,-1.0) if ((x['Amount_diff']==0) & (x['ViewData.Status']=='UMT')) else x['Amount_diff'], axis = 1)

data['ViewData.Accounting Settle Date'] = pd.to_datetime(data['ViewData.Accounting Settle Date'])
data['ViewData.B-P Settle Date'] = pd.to_datetime(data['ViewData.B-P Settle Date'])
data['ViewData.Accounting Trade Date'] = pd.to_datetime(data['ViewData.Accounting Trade Date'])
data['ViewData.B-P Trade Date'] = pd.to_datetime(data['ViewData.B-P Trade Date'])

data['Trade date diff'] = (data['ViewData.Accounting Trade Date'] - data['ViewData.B-P Trade Date'])
data['Settle date diff'] = (data['ViewData.Accounting Settle Date'] - data['ViewData.B-P Settle Date'])

data['Trade date diff'] = data['Trade date diff'].apply(lambda x : round(x.total_seconds()/(3600*24),0))

data['Settle date diff'] = data['Settle date diff'].apply(lambda x : round(x.total_seconds()/(3600*24),0))

# #### filling of nullvalue

non_cat = ['ViewData.B-P Quantity','ViewData.Accounting Quantity','ViewData.Accounting Net Amount','ViewData.B-P Net Amount','Amount_diff','Settle date diff','Trade date diff']

for item in data.columns:
    if item not in non_cat:
        data[item] = data[item].fillna('AAA')
    else:
        data[item] = data[item].fillna(0)

# #### Basic EDA of all the columns

data = data[(data['ViewData.Accounting Mapped Custodian Account'] == data['ViewData.Accounting Mapped Custodian Account']) & (data['ViewData.Accounting Currency']==data['ViewData.B-P Currency'])]

# #### UMR and UMT model Training

col = [
       'ViewData.Accounting Transaction Type',
       'ViewData.B-P Transaction Type', 'ViewData.Status',
'Amount_diff',
 'Trade date diff',
 'Settle date diff']

col1 = ['ViewData.Accounting Mapped Custodian Account',
       'ViewData.Accounting Currency', 
       'ViewData.B-P Mapped Custodian Account',
       'ViewData.B-P Currency',   'ViewData.Status','Trade date diff',
 'Settle date diff',
 'Amount_diff']

data = data[col]

cat_vars = [  'ViewData.Accounting Transaction Type','ViewData.B-P Transaction Type']

cat_vars1 = [ 'ViewData.Accounting Mapped Custodian Account',
 'ViewData.Accounting Currency',
 'ViewData.B-P Mapped Custodian Account',
 'ViewData.B-P Currency',
       ]

from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix

from catboost import CatBoostClassifier

from sklearn import metrics

X_temp, X_test, y_temp, y_test = train_test_split(data.drop((['ViewData.Status']),axis=1), 
           data['ViewData.Status'], test_size=0.20, 
            random_state=88, shuffle=True,stratify=data['ViewData.Status'])

X_train, X_val, y_train, y_val = train_test_split(X_temp, 
           y_temp, test_size=0.1, 
            random_state=87, shuffle=True,stratify=y_temp)

SEED =88
params ={'loss_function' : 'MultiClass',
        'eval_metric' : 'Accuracy',
        'learning_rate':0.055,
        'iterations':1200,
        'depth':5,
        'random_seed':SEED,
        'od_type':'Iter',
        'od_wait':200,
        'cat_features':cat_vars,
        'task_type':'CPU'}

clf = CatBoostClassifier(**params)

clf.fit(X_train, 
            y_train,eval_set=(X_val, y_val),
       use_best_model=True,plot=True)

cb_predictions = clf.predict(X_test)

cb_prediction_prob = clf.predict_proba(X_test)

print(metrics.classification_report(y_test,cb_predictions, digits=3))

feature_names = X_train.columns
importances = clf.feature_importances_

import matplotlib.pyplot as plt

indices = np.argsort(importances)

# Plot the feature importances of the forest
plt.figure()
plt.title("Feature importances")
plt.barh(range(X_train.shape[1]), importances[indices],
       color="r", align="center")
# If you want to define your own labels,
# change indices to a list of labels on the following line.
plt.yticks(range(X_train.shape[1]),[feature_names[i] for i in indices])
plt.ylim([-1, X_train.shape[1]])
plt.show()

import pickle
filename = 'finalized_model_lombard 249_v8 feb 4 umtob.sav'
pickle.dump(clf, open(filename, 'wb'))

# #### UMB model
col = ['ViewData.Accounting Mapped Custodian Account',
       'ViewData.Accounting Asset Type Category', 
       'ViewData.Accounting Currency', 
       'ViewData.Accounting Investment Type', 
       'ViewData.Accounting Prime Broker', 'ViewData.Accounting Quantity',
       'ViewData.Accounting Ticker', 'ViewData.Accounting Transaction Type',
       'ViewData.B-P Mapped Custodian Account',
       'ViewData.B-P Asset Type Category', 
       'ViewData.B-P Currency',  'ViewData.B-P Investment Type',
        'ViewData.B-P Prime Broker',
       'ViewData.B-P Quantity', 'ViewData.B-P Ticker',
       'ViewData.B-P Transaction Type', 'ViewData.Status']

data = data[col]

pd.options.display.max_columns = 999

cat_vars = [ 'ViewData.Accounting Mapped Custodian Account',
 'ViewData.Accounting Asset Type Category',
 'ViewData.Accounting Currency',
 'ViewData.Accounting Investment Type',
 'ViewData.Accounting Prime Broker',
 'ViewData.Accounting Ticker',
 'ViewData.Accounting Transaction Type',
 'ViewData.B-P Mapped Custodian Account',
 'ViewData.B-P Asset Type Category',
 'ViewData.B-P Currency',
 'ViewData.B-P Investment Type',
 'ViewData.B-P Prime Broker',
 'ViewData.B-P Ticker',
 'ViewData.B-P Transaction Type',
       ]

from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix

from catboost import CatBoostClassifier

from sklearn import metrics

X_temp, X_test, y_temp, y_test = train_test_split(data.drop((['ViewData.Status']),axis=1), 
           data['ViewData.Status'], test_size=0.20, 
            random_state=88, shuffle=True,stratify=data['ViewData.Status'])

X_train, X_val, y_train, y_val = train_test_split(X_temp, 
           y_temp, test_size=0.1, 
            random_state=87, shuffle=True,stratify=y_temp)

SEED =88
params ={'loss_function' : 'MultiClass',
        'eval_metric' : 'Accuracy',
        'learning_rate':0.055,
        'iterations':1200,
        'depth':5,
        'random_seed':SEED,
        'od_type':'Iter',
        'od_wait':200,
        'cat_features':cat_vars,
        'task_type':'CPU'}

clf = CatBoostClassifier(**params)

clf.fit(X_train, 
            y_train,eval_set=(X_val, y_val),
       use_best_model=True,plot=True)

cb_predictions = clf.predict(X_test)

cb_prediction_prob = clf.predict_proba(X_test)

print(metrics.classification_report(y_test,cb_predictions, digits=3))

feature_names = X_train.columns
importances = clf.feature_importances_

import matplotlib.pyplot as plt

indices = np.argsort(importances)

# Plot the feature importances of the forest
plt.figure()
plt.title("Feature importances")
plt.barh(range(X_train.shape[1]), importances[indices],
       color="r", align="center")
# If you want to define your own labels,
# change indices to a list of labels on the following line.
plt.yticks(range(X_train.shape[1]),[feature_names[i] for i in indices])
plt.ylim([-1, X_train.shape[1]])
plt.show()

# #### Predicting probability

pred_st = []
for item in cb_predictions:
    item1 = item[0]
    pred_st.append(item1)

pred_Prob_ob = []
pred_Prob_umb = []
for item in cb_prediction_prob:
    item1 = item[0]
    item2 = item[1]
    pred_Prob_ob.append(item1)
    pred_Prob_umb.append(item2)

X_test = X_test.reset_index()
X_test.drop('index', inplace = True, axis = 1)

X_test['predicted'] = pd.Series(pred_st)
X_test['ob prob'] = pd.Series(pred_Prob_ob)
X_test['umb prob'] = pd.Series(pred_Prob_umb)

y_test = y_test.reset_index()

actual = list(y_test['ViewData.Status'])
X_test['actual'] = pd.Series(actual)

pd.set_option('display.max_columns', 500)


# #### Saving the model

import pickle
filename = 'finalized_model_lombard 249_v1 feb 1 umb.sav'
pickle.dump(clf, open(filename, 'wb'))


# ### Testing on MEO files

# In[1740]:


df = pd.read_csv('Lombard/249/meo_df_7BD.csv')


# In[1741]:


df['ViewData.Status'].value_counts()


# In[1712]:


df[side1_col].count()


# In[1742]:


df[['len_0','len_1']] = df.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']), axis = 1)


# In[1752]:


col_to_exam = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account',
       'ViewData.Asset Type Category', 
       'ViewData.Currency', 'ViewData.Settle Date','ViewData.Trade Date',
       
       'ViewData.Investment Type', 'ViewData.Investment ID','ViewData.ISIN',
       'ViewData.Prime Broker', 'ViewData.Quantity',
       'ViewData.Ticker', 'ViewData.Transaction Type','ViewData.Accounting Net Amount','len_0','len_1','ViewData.Description','ViewData.Status'
      ]


# In[1753]:


df1 = df[col_to_exam]


# In[1754]:


df1.to_csv('Meo lombard to examine.csv')


# In[1710]:


side0_col = ['ViewData.Side0_UniqueIds','ViewData.Mapped Custodian Account',
       'ViewData.Asset Type Category', 
       'ViewData.Currency', 'ViewData.Settle Date','ViewData.Trade Date',
       
       'ViewData.Investment Type', 
       'ViewData.Prime Broker', 'ViewData.Quantity',
       'ViewData.Ticker', 'ViewData.Transaction Type','ViewData.Accounting Net Amount'
      ]


# In[1675]:


side1_col = ['ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account',
       'ViewData.Asset Type Category', 
       'ViewData.Currency', 'ViewData.Settle Date','ViewData.Trade Date',
       
       'ViewData.Investment Type', 
       'ViewData.Prime Broker', 'ViewData.Quantity',
       'ViewData.Ticker', 'ViewData.Transaction Type','ViewData.B-P Net Amount'
      ]


# - Take only one ID one side.

# In[1676]:


side0_single = df[(df['len_0']==1) & (df['len_1']==0)][side0_col]
side1_single = df[(df['len_0']==0) & (df['len_1']==1)][side1_col]


# In[1677]:


side1_single.shape


# In[1678]:


side0_double = df[(df['len_0']==1) & (df['len_1']==1)][side0_col]
side1_double = df[(df['len_0']==1) & (df['len_1']==1)][side1_col]


# In[1679]:


side1_double.shape


# In[1680]:


side0_single = side0_single.rename(columns = {'ViewData.Accounting Net Amount':'ViewData.Net Amount'})
side0_double = side0_double.rename(columns = {'ViewData.Accounting Net Amount':'ViewData.Net Amount'})
                    


# In[1681]:


side1_single = side1_single.rename(columns = {'ViewData.B-P Net Amount':'ViewData.Net Amount'})
side1_double = side1_double.rename(columns = {'ViewData.B-P Net Amount':'ViewData.Net Amount'})


# In[1682]:


side0 = pd.concat([side0_single,side0_double], axis =0)
side0 = side0.reset_index()
side0.drop('index',inplace = True, axis = 1)


# In[1683]:


side1 = pd.concat([side1_single,side1_double], axis =0)
side1 = side1.reset_index()
side1.drop('index',inplace = True, axis = 1)


# In[1684]:


for item in list(side0.columns):
    if item!= 'ViewData.Side0_UniqueIds':
        item1 = item.split('.')[1]
        item1 = 'ViewData.Accounting' + ' ' + item1
        side0 = side0.rename(columns = {item: item1})


# In[1685]:


for item in list(side1.columns):
    if item!= 'ViewData.Side1_UniqueIds':
        item1 = item.split('.')[1]
        item1 = 'ViewData.B-P' + ' ' + item1
        side1 = side1.rename(columns = {item: item1})


# In[1473]:


side1.head(4)


# In[1686]:


side1.shape


# In[1687]:


from pandas import merge
from tqdm import tqdm

pool =[]
key_index =[]
training_df =[]
call1 = []

appended_data = []

no_pair_ids = []
#max_rows = 5

#k = list(set(list(set(nopair0['ViewData.Task Business Date'])) + list(set(nopair1['ViewData.Task Business Date']))))
k1 = [1]

for d in tqdm(k1):
    aa1 = side0.copy()#[side0['ViewData.Task Business Date']==d]
    bb1 = side1.copy()#[side1['ViewData.Task Business Date']==d]
#     aa1['marker'] = 1
#     bb1['marker'] = 1
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    print(aa1.shape)
    print(bb1.shape)
    
#     aa1.columns = ['SideB.' + x  for x in aa1.columns] 
#     bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
    cc1 = pd.merge(aa1,bb1, left_on = ['ViewData.Accounting Mapped Custodian Account','ViewData.Accounting Currency'], right_on = ['ViewData.B-P Mapped Custodian Account','ViewData.B-P Currency'], how = 'outer')
    appended_data.append(cc1)
 


# In[1688]:


test_file = pd.concat(appended_data) 


# In[1689]:


test_file.shape


# In[1478]:


test_file.count()


# In[1690]:


test_file['Amount_diff'] = test_file['ViewData.Accounting Net Amount'] - test_file['ViewData.B-P Net Amount']


# In[1691]:


test_file['ViewData.Accounting Settle Date'] = pd.to_datetime(test_file['ViewData.Accounting Settle Date'])
test_file['ViewData.B-P Settle Date'] = pd.to_datetime(test_file['ViewData.B-P Settle Date'])
test_file['ViewData.Accounting Trade Date'] = pd.to_datetime(test_file['ViewData.Accounting Trade Date'])
test_file['ViewData.B-P Trade Date'] = pd.to_datetime(test_file['ViewData.B-P Trade Date'])

# test_file['Trade date diff'] = (test_file['ViewData.Accounting Trade Date'] - test_file['ViewData.B-P Trade Date'])
# test_file['Settle date diff'] = (test_file['ViewData.Accounting Settle Date'] - test_file['ViewData.B-P Settle Date'])

# test_file['Trade date diff'] = (test_file['ViewData.Accounting Trade Date'] - test_file['ViewData.B-P Trade Date'])
# test_file['Settle date diff'] = (test_file['ViewData.Accounting Settle Date'] - test_file['ViewData.B-P Settle Date'])

# test_file['Settle date diff'] = test_file['Settle date diff'].apply(lambda x : str(x).split()[0])
# test_file['Trade date diff'] = test_file['Trade date diff'].apply(lambda x : str(x).split()[0])


# In[1692]:


test_file['Trade date diff'] = (test_file['ViewData.Accounting Trade Date'] -test_file['ViewData.B-P Trade Date'])
test_file['Settle date diff'] = (test_file['ViewData.Accounting Settle Date'] - test_file['ViewData.B-P Settle Date'])

test_file['Trade date diff'] = test_file['Trade date diff'].apply(lambda x : round(x.total_seconds()/(3600*24),0))

test_file['Settle date diff'] = test_file['Settle date diff'].apply(lambda x : round(x.total_seconds()/(3600*24),0))


# In[1487]:


test_file.count()


# In[1693]:


test_file.shape


# In[1694]:


test_file_ob = test_file[(test_file['ViewData.Side0_UniqueIds'].isna()) | (test_file['ViewData.Side1_UniqueIds'].isna())]
test_file = test_file[~((test_file['ViewData.Side0_UniqueIds'].isna()) | (test_file['ViewData.Side1_UniqueIds'].isna()))]


# In[1695]:


test_file.shape


# In[1697]:


model_col_new = [ 'ViewData.Accounting Transaction Type',
       
       'ViewData.B-P Transaction Type','Trade date diff',
 'Settle date diff', 'Amount_diff',]


# In[1424]:


model_col = ['ViewData.Accounting Mapped Custodian Account',
       'ViewData.Accounting Asset Type Category',
       'ViewData.Accounting Currency', 'ViewData.Accounting Investment Type',
       'ViewData.Accounting Prime Broker', 'ViewData.Accounting Quantity',
       'ViewData.Accounting Ticker', 'ViewData.Accounting Transaction Type',
       'ViewData.B-P Mapped Custodian Account',
       'ViewData.B-P Asset Type Category', 'ViewData.B-P Currency',
       'ViewData.B-P Investment Type', 'ViewData.B-P Prime Broker',
       'ViewData.B-P Quantity', 'ViewData.B-P Ticker',
       'ViewData.B-P Transaction Type','Trade date diff',
 'Settle date diff', 'Amount_diff',]


# In[1085]:


model_col1 = ['ViewData.Accounting Mapped Custodian Account',
      
       'ViewData.Accounting Currency',
       'ViewData.B-P Mapped Custodian Account',
       'ViewData.B-P Currency',
       'Trade date diff',
 'Settle date diff', 'Amount_diff',]


# In[1698]:


non_model_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Accounting Net Amount','ViewData.B-P Net Amount']


# In[1699]:


test_file1 = test_file[model_col_new]
test_file2 = test_file[non_model_col]


# In[1701]:


test_file1.columns


# In[1700]:


for item in test_file1.columns:
    if item not in non_cat:
        test_file1[item] = test_file1[item].fillna('AAA')
    else:
        test_file1[item] = test_file1[item].fillna(0)


# In[1702]:


import pickle
filename = 'finalized_model_lombard 249_v8 feb 4 umtob.sav'
clf = pickle.load(open(filename, 'rb'))


# In[1703]:


cb_predictions = clf.predict(test_file1)


# In[1704]:


np.concatenate(cb_predictions)


# In[1705]:


test_file1['predicted'] = np.concatenate(cb_predictions)


# In[1706]:


test_file1['predicted'].value_counts()


# In[1707]:


final_file = pd.concat([test_file2, test_file1], axis = 1)


# In[1708]:


final_file[(final_file['predicted']=='UMR') &(final_file['Amount_diff']==0)].shape


# In[1099]:


final_file[(final_file['predicted']=='OB')]['Settle date diff'].value_counts().iloc[0:20]


# In[1100]:


final_file[final_file['predicted'] == 'UMT'].to_csv('UMT predicted from lombard 249 v1.csv')


# In[827]:


x = final_file[final_file['predicted'] == 'UMT'].head(50000)


# In[829]:


x.to_csv('UMT predicted from lombard 249 v2.csv')


# ### M cross n creation n MEO file

# In[ ]:


uni2 = pd.read_csv('Lombard/249/meo_df_7BD.csv')


# In[74]:


uni2.shape


# In[75]:


uni2['ViewData.Status'].value_counts()


# In[76]:


filter_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account','ViewData.BreakID','ViewData.Fund','ViewData.Task Business Date',
 'ViewData.Currency',
 'ViewData.Asset Type Category',
 'ViewData.Transaction Type',
 'ViewData.Investment Type',
 'ViewData.Prime Broker',
 'ViewData.Ticker',
 'ViewData.Sec Fees',
 'ViewData.Settle Date',
 'ViewData.Trade Date',
 'ViewData.Description',
 'ViewData.CUSIP',
 'ViewData.Call Put Indicator',
 'ViewData.Cancel Flag',
 'ViewData.Commission',
 'ViewData.ISIN',
 'ViewData.Investment ID',
 
 'ViewData.Interest Amount',
 'ViewData.InternalComment1',
 'ViewData.InternalComment2',
 'ViewData.InternalComment3',
             
'ViewData.Accounting Net Amount',
'ViewData.B-P Net Amount',
              'ViewData.Net Amount Difference','ViewData.Status'
             ]


# In[77]:


uni2 = uni2[filter_col]


# In[78]:


uni2.shape


# In[309]:


#uni2['ViewData.Side1_UniqueIds'] = uni2['ViewData.Side1_UniqueIds'].fillna('BB')
#uni2['ViewData.Side0_UniqueIds'] = uni2['ViewData.Side0_UniqueIds'].fillna('AA')


# In[310]:


#uni2[['len_0','len_1']] = uni2.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)


# In[311]:


#uni2['MTM_mark'] = uni2.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)


# In[34]:


# We preserve Actual copy of the file and move to make changes on copy
uni3 = uni2.copy()


# In[35]:


def common_matching_engine_single1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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
    
        dummy['remove_mark'] = dummy['zero_sum_list'].apply(lambda x :1 if abs(x)<=5.0 else 0)

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
            filename = 'Lombard/249/pair result/setup 249 ' + string_name + '.csv'
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


# In[36]:


def common_matching_engine_single2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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


# In[37]:


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


# In[38]:


def common_matching_engine_double1(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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
    
        dummy['remove_mark'] = dummy['zero_sum_list'].apply(lambda x :1 if abs(x)<=5.0 else 0)

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
            filename = 'Lombard/249/pair result/setup 249 ' + string_name + '.csv'
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


# In[39]:


def common_matching_engine_double2(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
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


# In[40]:


def common_matching_engine_double3(df,filters,columns_to_output, amount_column, dummy_filter,serial_num):
    dummy = df.groupby(filters)[amount_column].apply(list).reset_index()
    dummy1 = df.groupby(filters)['ViewData.Side0_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy1 , on = filters, how = 'left')
    dummy2 = df.groupby(filters)['ViewData.Side1_UniqueIds'].count().reset_index()
    dummy = pd.merge(dummy, dummy2 , on = filters, how = 'left')
    dummy['sel_mark'] = dummy.apply(lambda x : 0 if ((x['ViewData.Side0_UniqueIds']==0) | (x['ViewData.Side1_UniqueIds']==0)) else 1, axis =1 )
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


# In[41]:


dummy_filter = ['remove_mark','sel_mark']
columns_to_output = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']
amount_column = 'ViewData.Net Amount Difference'


# In[ ]:


filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description','ViewData.Settle Date']


# In[42]:





# In[45]:


serial_num = 1
df1 = common_matching_engine_double1(uni3,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 2
df2 = common_matching_engine_single1(df1,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[48]:


filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description']


# In[49]:


serial_num = 3
df3 = common_matching_engine_double1(df2,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 4
df4 = common_matching_engine_single1(df3,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[50]:


filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker','ViewData.Settle Date']


# In[51]:


serial_num = 5
df5 = common_matching_engine_double1(df4,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 6
df6 = common_matching_engine_single1(df5,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[52]:


filters = ['ViewData.Task Business Date','ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Ticker']


# In[53]:


serial_num = 7
df7 = common_matching_engine_double1(df6,filters,columns_to_output, amount_column, dummy_filter,serial_num)
serial_num = 8
df8 = common_matching_engine_single1(df7,filters,columns_to_output, amount_column, dummy_filter,serial_num)


# In[54]:


df8['ViewData.Status'].value_counts()


# In[57]:


df8.shape


# In[56]:


df8['ViewData.Asset Type Category'].count()


# In[58]:


df8['ViewData.Investment Type'].count()


# In[59]:


df8['ViewData.Transaction Type'].count()


# In[60]:


df8['ViewData.Ticker'].count()


# In[61]:


df8['ViewData.Description'].count()


# In[64]:


df8['ViewData.Description'].value_counts().iloc[0:20]


# ### M cross n architecture for UMB finding using desc

# In[338]:


import re


# In[339]:


def desc_match(x,y):
    if ((type(x) == str) & (type(y)==str)):
        x = x.lower()
        y = y.lower()
        x1 =  re.split("[,/. \- !?:]+", x)
        y1 =  re.split("[,/. \- !?:]+", y)
        if len(x1)<len(y1):
            lst3 = [value for value in x1 if value in y1]
            
            if len(lst3)>0:
                score = len(lst3)/len(x1)
            else:
                score = 0
        else:
            lst3 = [value for value in y1 if value in x1]
            
            if len(lst3)>0:
                
                score = len(lst3)/len(y1)
            else:
                score = 0
    else:
        score = 2
    return score


# #### Finding OB for the architecture

# In[340]:


double6.columns


# In[341]:


# this copying is done so that we can add as many filters we can. We start code for UMB here.
uni5 = double6.copy()


# In[342]:


# filter_col = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Mapped Custodian Account','ViewData.Fund','ViewData.Task Business Date',
#  'ViewData.Currency',
#  'ViewData.Asset Type Category',
#  'ViewData.Transaction Type',
#  'ViewData.Investment Type',
#  'ViewData.Prime Broker',
#  'ViewData.Ticker',
#  'ViewData.Sec Fees',
#  'ViewData.Settle Date',
#  'ViewData.Trade Date',
#  'ViewData.Description']


# In[343]:


#uni5 = uni5[filter_col]


# In[344]:


uni5[['len_0','len_1']] = uni5.apply(lambda x : mtm(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds']),axis = 1)


# In[345]:


uni5['MTM_mark'] = uni5.apply(lambda x : mtm_mark(x['len_0'],x['len_1']),axis =1)


# In[346]:


uni5['MTM_mark'].value_counts()


# In[347]:


ob_df= uni5[uni5['MTM_mark'] == 'OB']
side_0 = ob_df[ob_df['ViewData.Side1_UniqueIds'].isna()]
side_0['final_id'] = side_0['ViewData.Side0_UniqueIds']
side_1 = ob_df[ob_df['ViewData.Side0_UniqueIds'].isna()]
side_1['final_id'] = side_1['ViewData.Side1_UniqueIds']
    


# In[348]:


side0_otm= uni5[uni5['MTM_mark'] != 'OB']
side0_otm['final_id'] = side0_otm['ViewData.Side0_UniqueIds'].astype(str) + '|' + side0_otm['ViewData.Side1_UniqueIds'].astype(str) 


# In[349]:


umb_0 = pd.concat([side_0,side0_otm], axis = 0)
umb_0 = umb_0.reset_index()
umb_0.drop('index', axis = 1, inplace =True )


# In[350]:


umb_0.shape


# In[351]:


umb_1 = side_1.copy()


# In[352]:


from pandas import merge
from tqdm import tqdm

pool =[]
key_index =[]
training_df =[]
call1 = []

appended_data = []

no_pair_ids = []
#max_rows = 5

k = list(set(list(set(umb_0['ViewData.Task Business Date'])) + list(set(umb_1['ViewData.Task Business Date']))))
k1 = k

for d in tqdm(k1):
    aa1 = umb_0.copy()#[umb_0['ViewData.Task Business Date']==d]
    bb1 = umb_1.copy()#[umb_1['ViewData.Task Business Date']==d]
#     aa1['marker'] = 1
#     bb1['marker'] = 1
    
    aa1 = aa1.reset_index()
    aa1 = aa1.drop('index',1)
    bb1 = bb1.reset_index()
    bb1 = bb1.drop('index', 1)
    print(aa1.shape)
    print(bb1.shape)
    
    aa1.columns = ['SideB.' + x  for x in aa1.columns] 
    bb1.columns = ['SideA.' + x  for x in bb1.columns]
    
    cc1 = pd.merge(aa1,bb1, left_on = ['SideB.ViewData.Mapped Custodian Account','SideB.ViewData.Currency'], right_on = ['SideA.ViewData.Mapped Custodian Account','SideA.ViewData.Currency'], how = 'outer')
    appended_data.append(cc1)
    
umbmn = pd.concat(appended_data)
 


# In[353]:


umbmn = pd.concat(appended_data)


# In[354]:


umbmn['desc_score'] =  umbmn.apply(lambda x : desc_match(x['SideA.ViewData.Description'],x['SideB.ViewData.Description']), axis = 1)


# ### Input for UMB model

# In[355]:


### Now remove those pair where one side is absent
ab_a = umbmn[(umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna())]
ab_b = umbmn[~((umbmn['SideB.final_id'].isna()) | (umbmn['SideA.final_id'].isna()))]


# In[356]:


ab_b.columns


# In[357]:


umbk = ab_b[['SideB.ViewData.Side0_UniqueIds', 'SideB.ViewData.Side1_UniqueIds','SideB.final_id','SideB.ViewData.BreakID','SideB.ViewData.Status','SideA.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds','SideA.final_id','SideA.ViewData.BreakID','SideA.ViewData.Status']]


# In[358]:


col_umb = ['SideB.ViewData.Asset Type Category',
        'SideB.ViewData.Fund',
       'SideB.ViewData.Investment Type',
        'SideB.ViewData.Ticker','SideB.ViewData.Transaction Type',
      'SideB.ViewData.Mapped Custodian Account','SideB.ViewData.Currency', 'SideA.ViewData.Asset Type Category',
        'SideA.ViewData.Fund','SideA.ViewData.Investment Type',
        'SideA.ViewData.Ticker','SideA.ViewData.Transaction Type',
      'SideA.ViewData.Mapped Custodian Account','SideA.ViewData.Currency', 'desc_score']


# In[359]:


umb_file = ab_b[col_umb]


# In[360]:


# cat_vars = [ 
      
#       'SideB.ViewData.Asset Type Category',
#         'SideB.ViewData.Fund',
#        'SideB.ViewData.Investment Type',
#         'SideB.ViewData.Ticker','SideB.ViewData.Transaction Type',
#       'SideB.ViewData.Currency', 'SideB.ViewData.Mapped Custodian Account','SideA.ViewData.Asset Type Category',
#         'SideA.ViewData.Fund','SideA.ViewData.Investment Type',
#         'SideA.ViewData.Ticker','SideA.ViewData.Transaction Type',
#       'SideA.ViewData.Currency', 'SideA.ViewData.Mapped Custodian Account'
#        ]


# In[361]:


umb_file['SideB.ViewData.Asset Type Category'] = umb_file['SideB.ViewData.Asset Type Category'].fillna('AA')
umb_file['SideB.ViewData.Fund'] = umb_file['SideB.ViewData.Fund'].fillna('BB')
umb_file['SideB.ViewData.Investment Type'] = umb_file['SideB.ViewData.Investment Type'].fillna('CC')
umb_file['SideB.ViewData.Ticker'] = umb_file['SideB.ViewData.Ticker'].fillna('DD')
umb_file['SideB.ViewData.Transaction Type'] = umb_file['SideB.ViewData.Transaction Type'].fillna('EE')
umb_file['SideB.ViewData.Currency'] = umb_file['SideB.ViewData.Currency'].fillna('FF')
umb_file['SideB.ViewData.Mapped Custodian Account'] = umb_file['SideB.ViewData.Mapped Custodian Account'].fillna('GG')


# In[362]:


umb_file['SideA.ViewData.Asset Type Category'] = umb_file['SideA.ViewData.Asset Type Category'].fillna('aa')
umb_file['SideA.ViewData.Fund'] = umb_file['SideA.ViewData.Fund'].fillna('bb')
umb_file['SideA.ViewData.Investment Type'] = umb_file['SideA.ViewData.Investment Type'].fillna('cc')
umb_file['SideA.ViewData.Ticker'] = umb_file['SideA.ViewData.Ticker'].fillna('dd')
umb_file['SideA.ViewData.Transaction Type'] = umb_file['SideA.ViewData.Transaction Type'].fillna('ee')
umb_file['SideA.ViewData.Currency'] = umb_file['SideA.ViewData.Currency'].fillna('ff')
umb_file['SideA.ViewData.Mapped Custodian Account'] = umb_file['SideA.ViewData.Mapped Custodian Account'].fillna('gg')


# In[363]:


for item in list(umb_file.columns):
    
    x1 = item.split('.')
    if 'desc_score' not in x1:
    
        if x1[0]=='SideB':
            m = 'ViewData.' + 'Accounting'+ " " + x1[2]
            umb_file = umb_file.rename(columns = {item:m})
        else:
            m = 'ViewData.' + 'B-P'+ " " + x1[2]
            umb_file =umb_file.rename(columns = {item:m})


# In[ ]:





# In[364]:


import pickle
filename = 'finalized_model_lombard_249_umb_v3.sav'
clf = pickle.load(open(filename, 'rb'))


# In[365]:


cb_predictions = clf.predict(umb_file)


# In[ ]:





# In[366]:


demo = []
for item in cb_predictions:
    demo.append(item[0])


# In[367]:


umb_file['predicted'] = pd.Series(demo)
#result['Actual'] = pd.Series(list1)


# In[368]:


umb_file['predicted'].value_counts()


# In[369]:


umb_file.columns


# In[370]:


umbpred = pd.concat([umbk,umb_file], axis = 1)


# In[371]:


umbpred.shape


# In[372]:


ab_a.shape


# In[373]:


# We first segragate ab_a, then next
side0_ab_a = ab_a[ab_a['SideB.final_id'].isna()]
side1_ab_a = ab_a[~ab_a['SideB.final_id'].isna()]

if ((side0_ab_a.shape[0]!=0) & (side1_ab_a.shape[0]!=0)):
    list_id_0_ab_a = list(set(ab_a['SideA.final_id']))
    list_id_1_ab_a = list(set(ab_a['SideB.final_id']))
    side0_ob = umb_0[umb_0['final_id'].isin(list_id_1_ab_a)]
    side1_ob = umb_1[umb_1['final_id'].isin(list_id_0_ab_a)]
    ob_1st_set = pd.concat([side0_ob,side1_ob], axis = 0)
    ob_1st_set = ob_1st_set.reset_index()
    ob_1st_set = ob_1st_set.drop('index', axis = 1)
elif (side0_ab_a.shape[0]!=0):
    list_id_0_ab_a = list(set(ab_a['SideA.final_id']))
    
    side0_ob = umb_1[umb_1['final_id'].isin(list_id_0_ab_a)]
    
    ob_1st_set = side0_ob.copy()
    ob_1st_set = ob_1st_set.reset_index()
    ob_1st_set = ob_1st_set.drop('index', axis = 1)
else:
    list_id_1_ab_a = list(set(ab_a['SideB.final_id']))
    
    side1_ob = umb_0[umb_0['final_id'].isin(list_id_1_ab_a)]
    
    ob_1st_set = side1_ob.copy()
    ob_1st_set = ob_1st_set.reset_index()
    ob_1st_set = ob_1st_set.drop('index', axis = 1)
    
    


# In[374]:


ob_1st_set.shape


# In[375]:


ob_1st_set = ob_1st_set.drop_duplicates()


# In[376]:


# We will segragate IDs on both sides which were just OB.
k1 = umbpred.groupby('SideB.final_id')['predicted'].apply(list).reset_index()
k2 = umbpred.groupby('SideA.final_id')['predicted'].apply(list).reset_index()


# In[377]:


def ob_umb(x):
    x1 = list(set(x))
    if ((len(x1)==1) & ('OB' in x1)):
        return 'OB'
    elif ((len(x1)==1) & ('UMB' in x1)):
        return 'UMB'
    else:
        return 'Both'


# In[378]:


k1['State'] = k1['predicted'].apply(lambda x : ob_umb(x) )
k2['State'] = k2['predicted'].apply(lambda x : ob_umb(x) )


# In[379]:


k1['State'].value_counts()


# In[380]:


list_id_0_k1 = list(set(k1[k1['State']=='OB']['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['State']=='OB']['SideA.final_id']))
side0_ob = umb_0[umb_0['final_id'].isin(list_id_0_k1)]
side1_ob = umb_1[umb_1['final_id'].isin(list_id_1_k2)]


# In[381]:


ob_2nd_set = pd.concat([side0_ob,side1_ob], axis = 0)
ob_2nd_set = ob_2nd_set.reset_index()
ob_2nd_set = ob_2nd_set.drop('index', axis = 1)


# In[382]:


ob_2nd_set.shape


# In[383]:


if ((ob_1st_set.shape[0]!=0) & (ob_2nd_set.shape[0]!=0)):
    ob_for_comment = pd.concat([ob_1st_set,ob_2nd_set], axis = 0)
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
elif ((ob_1st_set.shape[0]!=0)):
    ob_for_comment = ob_1st_set.copy()
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
else:
    
    ob_for_comment = ob_2nd_set.copy()
    ob_for_comment = ob_for_comment.reset_index()
    ob_for_comment = ob_for_comment.drop('index', axis = 1)
            


# In[384]:


umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]


# In[385]:


k1 = umbpred.groupby('SideB.final_id')['predicted'].apply(list).reset_index()
k2 = umbpred.groupby('SideA.final_id')['predicted'].apply(list).reset_index()


# In[386]:


# Difficult part segragation of UMBs in OTO, OTM, MTO and MTM
# OTO

import collections

def umb_counter(x):
    counter=collections.Counter(x)
    if counter['UMB'] == 1:
        return 1
    else:
        return counter['UMB']
        
k1['umb_counter'] = k1['predicted'].apply(lambda x : umb_counter(x) )
k2['umb_counter'] = k2['predicted'].apply(lambda x : umb_counter(x) )


# In[387]:


list_id_0_k1 = list(set(k1[k1['umb_counter']==1]['SideB.final_id']))
list_id_1_k2 = list(set(k2[k2['umb_counter']==1]['SideA.final_id']))
if ((len(list_id_0_k1)>0) & (len(list_id_1_k2)>0)):
    him = umbpred[(umbpred['predicted']=='UMB') & (umbpred['SideB.final_id'].isin(list_id_0_k1))]['SideA.final_id']
    him1 = list(set(him))
    lst3 = [value for value in him1 if value in list_id_1_k2]
    umb_oto = umbpred[(umbpred['predicted']=='UMB') &(umbpred['SideA.final_id'].isin(lst3))]
    umbpred = umbpred[~((umbpred['predicted']=='UMB') &(umbpred['SideA.final_id'].isin(lst3)))]
#     umb_oto = umbpred[(umbpred['SideB.final_id'].isin(list_id_0_k1)) & (umbpred['SideA.final_id'].isin(list_id_1_k2))]
#     umbpred = umbpred[~umbpred['SideB.final_id'].isin(list_id_0_k1)]
#     umbpred = umbpred[~umbpred['SideA.final_id'].isin(list_id_1_k2)]


# In[388]:


def breakid(x,y):
    
    brk = []
    for item in x:
        brk.append(item)
    brk.append(y)
    
    return brk


# In[389]:


# Now We write the hierarchy for many to one
k3 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
k4 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
mn = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.BreakID']]
mn = mn.drop_duplicates()
k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
stat = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

mn1 = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.Status']]
mn1 = mn1.drop_duplicates()
k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )


# In[390]:


def intersection_(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return round((len(lst3)/len(lst1)),1)


# In[391]:


def Diff(li1, li2):
    li_dif = [value for value in li1 if value not in li2]
    return li_dif


# In[392]:


from functools import reduce


# In[393]:


ob_stage_df = pd.DataFrame()
umb_mtm = pd.DataFrame()
umb_mtm_list = []
umb_otm_list = []

for i, row in k3.iterrows():
    if k3.shape[0]!=0:
        #print(k3.shape)
    
        k5 = k3.copy()
        mid = row['SideB.final_id']
        midlist = row['SideA.final_id']
        midlen = row['id_len']
        all_brk = list(set(row['ViewData.BreakID']))
        all_sts = row['ViewData.Status']
    
        k6 = k5[(k5['id_len']<midlen+3) & (k5['id_len']>midlen-4)]
        
        k6['match'] = k6['SideA.final_id'].apply(lambda x:intersection_(x,midlist) )
    
    
        k7 =list(set(k6[k6['match']>0.8]['SideB.final_id']))
        if len(k7)>1:
        
            set_for_int = list((k6[k6['match']>0.8]['SideA.final_id']))
           
            k8 = list(reduce(set.intersection, [set(item) for item in set_for_int]))
          
        
            int1 = umbpred[umbpred['SideB.final_id'].isin(k7)]
            br7 =list(set(int1['SideB.ViewData.BreakID']))
            br8 =list(set((umbpred[umbpred['SideA.final_id'].isin(k8)]['SideA.ViewData.BreakID'])))
            br7_8 = br7 + br8
        
            vi7 =list(set(int1['SideB.ViewData.Status']))
            vi8 =list(set(umbpred[umbpred['SideA.final_id'].isin(k8)]['SideA.ViewData.Status']))
            vi7_8 = vi7 + vi8
            k9 =list(set(int1['SideA.final_id']))
#             if ((len(k7)>0) & (len(k8)>0)):
            umb_mtm_list_temp = []
            umb_mtm_list_temp.append(k7)
            umb_mtm_list_temp.append(k8)
            umb_mtm_list_temp.append(br7_8)
            umb_mtm_list_temp.append(vi7_8)
            umb_mtm_list.append(umb_mtm_list_temp)
            
            k10 = Diff(k9,k8)
            
            umbpred = umbpred[~umbpred['SideB.final_id'].isin(k7)]
            
            k11 = list(set(umbpred['SideA.final_id']))
            k12 = Diff(k10,k11)
            ob_3rd_set = umb_1[umb_1['final_id'].isin(k12)]
            ob_stage_df = pd.concat([ob_stage_df,ob_3rd_set], axis = 0)
            ob_stage_df = ob_stage_df.reset_index()
            ob_stage_df = ob_stage_df.drop('index', axis = 1)
            
            int2 = umbpred[umbpred['SideA.final_id'].isin(k8)]
            k21 = list(set(int2['SideB.final_id']))
            k22 = Diff(k21,k7)
            
            umbpred = umbpred[~umbpred['SideA.final_id'].isin(k8)]
            k23 = list(set(umbpred['SideB.final_id']))
            k24 = Diff(k22,k23)
            ob_3rd_set_n = umb_0[umb_0['final_id'].isin(k24)]
            ob_stage_df = pd.concat([ob_stage_df,ob_3rd_set_n], axis = 0)
            ob_stage_df = ob_stage_df.reset_index()
            ob_stage_df = ob_stage_df.drop('index', axis = 1)
            
            rem_brk = umbpred['SideA.ViewData.BreakID'].nunique() + umbpred['SideB.ViewData.BreakID'].nunique()
            #print('break after mtm')
            #print(rem_brk)
            
            
            
            

            if (umbpred[umbpred['predicted']=='UMB'].shape[0]!=0):
                k3 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
                #print('mtm k3 shape')
                #print(k3.shape)
                k4 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
                k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
                k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
                mn = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.BreakID']]
                mn = mn.drop_duplicates()
                k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
                k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
                stat = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
                k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

                mn1 = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.Status']]
                mn1 = mn1.drop_duplicates()
                k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
                k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )
            else:
                break
            
        else:
            k3 = k5.copy()
            umbpred = umbpred.copy()

    else:
        break
            


# In[394]:


k3 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
k4 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
mn = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.BreakID']]
mn = mn.drop_duplicates()
k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
stat = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

mn1 = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.Status']]
mn1 = mn1.drop_duplicates()
k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )


# In[395]:



umb_otm_list = []
for i, row in k3.iterrows():
    k5 = k3.copy()
    mid = row['SideB.final_id']
    midlist = row['SideA.final_id']
    mk = []
    mk.append(mid)
        
    all_brk = list(set(row['ViewData.BreakID']))
    all_sts = row['ViewData.Status']
    
    umb_otm_list_temp = []
    umb_otm_list_temp.append(mid)
    umb_otm_list_temp.append(midlist)
    umb_otm_list_temp.append(all_brk)
    umb_otm_list_temp.append(all_sts)
    umb_otm_list.append(umb_otm_list_temp)
    
    m11 = list(set(umbpred[umbpred['SideB.final_id']!=mid]['SideA.final_id']))
    m12 = Diff(m11,midlist)
    
    umbpred = umbpred[umbpred['SideB.final_id']!=mid]
    m13 = list(set(umbpred[umbpred['SideB.final_id']!=mid]['SideA.final_id']))
    m14 = Diff(m12,m13)
    ob_4th_set_n = umb_1[umb_1['final_id'].isin(m14)]
    ob_stage_df = pd.concat([ob_stage_df,ob_4th_set_n], axis = 0)
    ob_stage_df = ob_stage_df.reset_index()
    ob_stage_df = ob_stage_df.drop('index', axis = 1)
    #print(k1.shape)
    #print(len(midlist))
    
    int4 = umbpred[umbpred['SideA.final_id'].isin(midlist)]
    m21 = list(set(int4['SideB.final_id']))
    m22 = Diff(m21,mk)
            
    umbpred = umbpred[~umbpred['SideA.final_id'].isin(midlist)]
    m23 = list(set(umbpred['SideB.final_id']))
    m24 = Diff(m22,m23)
    ob_4th_set = umb_0[umb_0['final_id'].isin(m24)]
    ob_stage_df = pd.concat([ob_stage_df,ob_4th_set], axis = 0)
    ob_stage_df = ob_stage_df.reset_index()
    ob_stage_df = ob_stage_df.drop('index', axis = 1)
    
    #print(len(all_brk))       
    rem_brk = umbpred['SideA.ViewData.BreakID'].nunique() + umbpred['SideB.ViewData.BreakID'].nunique()
    #print('break after mtm')
    #print(rem_brk)
    
    if ((umbpred[umbpred['predicted']=='UMB'].shape[0]!=0) & (k3.shape[0]!=0)):
        k3 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.final_id'].apply(list).reset_index()
                #print('mtm k3 shape')
                #print(k3.shape)
        k4 = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.BreakID'].apply(list).reset_index()
        k3 = pd.merge(k3, k4, on = 'SideB.final_id', how = 'left')
        k3['id_len'] = k3['SideA.final_id'].apply(lambda x : len(x) )
        mn = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.BreakID']]
        mn = mn.drop_duplicates()
        k3 = pd.merge(k3, mn, on = 'SideB.final_id', how = 'left')
        k3['ViewData.BreakID'] = k3.apply(lambda x : breakid(x['SideA.ViewData.BreakID'],x['SideB.ViewData.BreakID']), axis = 1 )
        stat = umbpred[umbpred['predicted']=='UMB'].groupby('SideB.final_id')['SideA.ViewData.Status'].apply(list).reset_index()
        k3 = pd.merge(k3, stat, on = 'SideB.final_id', how = 'left')

        mn1 = umbpred[umbpred['predicted']=='UMB'][['SideB.final_id','SideB.ViewData.Status']]
        mn1 = mn1.drop_duplicates()
        k3 = pd.merge(k3, mn1, on = 'SideB.final_id', how = 'left')
        k3['ViewData.Status'] = k3.apply(lambda x : breakid(x['SideA.ViewData.Status'],x['SideB.ViewData.Status']), axis = 1 )
    else:
        break
    


# In[396]:


k3.shape


# In[397]:


umb_mtm = pd.DataFrame(umb_mtm_list)


# In[398]:


umb_otm = pd.DataFrame(umb_otm_list)


# In[399]:


umb_otm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status']
umb_mtm.columns = ['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status']


# In[400]:


umb_otm['predicted status'] = 'UMB'
umb_otm['predicted action'] = 'UMB one to many'
umb_otm['predicted category'] = 'UMB'
umb_otm['predicted comment'] = 'Difference in amount'


# In[401]:


umb_mtm['predicted status'] = 'UMB'
umb_mtm['predicted action'] = 'UMB many to many'
umb_mtm['predicted category'] = 'UMB'
umb_mtm['predicted comment'] = 'Difference in amount'


# In[402]:


umb_mtm = umb_mtm.reset_index()
umb_mtm = umb_mtm.drop('index', axis = 1)

umb_otm = umb_otm.reset_index()
umb_otm = umb_otm.drop('index', axis = 1)


# In[403]:


umb_oto1 = umb_oto[['SideB.final_id','SideB.ViewData.BreakID','SideB.ViewData.Status','SideA.final_id','SideA.ViewData.BreakID','SideA.ViewData.Status']]


# In[404]:


umb_oto1 = umb_oto1.rename(columns = {'SideB.final_id':'ViewData.Side0_UniqueIds',
                                     'SideA.final_id':'ViewData.Side1_UniqueIds'})


# In[405]:


def combining(x,y):
    blank_list = []
    blank_list.append(x)
    blank_list.append(y)
    
    return blank_list


# In[406]:


umb_oto1['ViewData.BreakID'] = umb_oto1.apply(lambda x : combining(x['SideB.ViewData.BreakID'], x['SideA.ViewData.BreakID']), axis = 1)
umb_oto1['ViewData.Status'] = umb_oto1.apply(lambda x : combining(x['SideB.ViewData.Status'], x['SideA.ViewData.Status']), axis = 1)


# In[407]:


umb_oto1 = umb_oto1.reset_index()
umb_oto1.drop(['index','SideB.ViewData.BreakID','SideA.ViewData.BreakID','SideB.ViewData.Status','SideA.ViewData.Status'], axis = 1, inplace = True)


# In[408]:


umb_oto1['predicted status'] = 'UMB'
umb_oto1['predicted action'] = 'UMB one to one'
umb_oto1['predicted category'] = 'UMB'
umb_oto1['predicted comment'] = 'Difference in amount'


# In[409]:


umb_oto1.to_csv('Lombard/249/umb lombard 249 oto.csv')
umb_mtm.to_csv('Lombard/249/umb lombard 249 mtm.csv')
umb_otm.to_csv('Lombard/249/umb lombard 249 otm.csv')


# In[410]:


k = ob_stage_df.drop_duplicates()


# In[411]:


li1 = list(set(umbpred[umbpred['SideB.ViewData.Status']=='UMB']['SideB.final_id']))
li2 = list(set(umbpred[umbpred['SideB.ViewData.Status']!='UMB']['SideB.final_id']))
li3 = list(set(umbpred[umbpred['SideA.ViewData.Status']=='UMB']['SideA.final_id']))
li4 = list(set(umbpred[umbpred['SideA.ViewData.Status']!='UMB']['SideA.final_id']))


# In[412]:


df1 = umb_0[umb_0['final_id'].isin(li1)]
df2 = umb_0[umb_0['final_id'].isin(li2)]
df3 = umb_1[umb_1['final_id'].isin(li3)]
df4 = umb_1[umb_1['final_id'].isin(li4)]


# In[413]:


umb_car = pd.concat([df1,df3], axis = 0)
umb_car = umb_car.reset_index()
umb_car.drop('index', axis = 1, inplace = True)


# In[414]:


umb_car['predicted status'] = 'UMB'
umb_car['predicted action'] = 'UMB carry forwad'
umb_car['predicted category'] = 'UMB'
umb_car['predicted comment'] = 'Difference in amount'


# In[415]:


umb_car.columns


# In[416]:


col_sel = [ 'ViewData.Side0_UniqueIds',
       'ViewData.Side1_UniqueIds',
       'ViewData.BreakID','ViewData.Status','predicted status', 'predicted action',
       'predicted category', 'predicted comment']


# In[417]:


umb_car1 = umb_car[col_sel]


# In[418]:


umb_car1.to_csv('Lombard/249/umb lombard 249 carryforward.csv')


# In[419]:


ob_car = pd.concat([df2,df4], axis = 0)
ob_car = ob_car.reset_index()
ob_car.drop('index', axis = 1, inplace = True)


# In[420]:


ob_for_comment.drop('ViewData.Task Business Date', axis =1 , inplace = True)
k.drop('ViewData.Task Business Date', axis =1 , inplace = True)
ob_car.drop('ViewData.Task Business Date', axis =1 , inplace = True)


# In[421]:


ob_for_comment_model = pd.concat([ob_for_comment,k,ob_car], axis = 0)


# In[422]:


ob_for_comment_model = ob_for_comment_model.reset_index()
ob_for_comment_model = ob_for_comment_model.drop('index', axis = 1)


# In[423]:


# Now we take all OBs to single side model


# In[ ]:





# In[424]:


import pandas as pd
import math

from dateutil.parser import parse
import operator
import itertools

import re
import os


# In[425]:


df3 = ob_for_comment_model.copy()


# In[426]:


df3.columns


# In[427]:





df = pd.read_excel('Mapping variables for variable cleaning.xlsx', sheet_name='General')
def make_dict(row):
    keys_l = str(row['Keys']).lower()
    keys_s = keys_l.split(', ')
    keys = tuple(keys_s)
    return keys
df['tuple'] = df.apply(make_dict, axis=1)
clean_map_dict = df.set_index('tuple')['Value'].to_dict()

df3['ViewData.Transaction Type'] = df3['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Asset Type Category'] = df3['ViewData.Asset Type Category'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Investment Type'] = df3['ViewData.Investment Type'].apply(lambda x : x.lower() if type(x)==str else x)
df3['ViewData.Prime Broker'] = df3['ViewData.Prime Broker'].apply(lambda x : x.lower() if type(x)==str else x)

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

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type'].apply(lambda x : clean_mapping(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker'].apply(lambda x : clean_mapping(x) if type(x)==str else x)

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

df3['ViewData.Transaction Type1'] = df3['ViewData.Transaction Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Asset Type Category1'] = df3['ViewData.Asset Type Category1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Investment Type1'] = df3['ViewData.Investment Type1'].apply(lambda x : comb_clean(x) if type(x)==str else x)
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].apply(lambda x : comb_clean(x) if type(x)==str else x)


# In[428]:


import re

com = pd.read_csv('desc cat with naveen oaktree.csv')
cat_list = list(set(com['Pairing']))

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

df3['desc_cat'] = df3['ViewData.Description'].apply(lambda x : descclean(x,cat_list))

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





# In[429]:


df3['desc_cat'] = df3['desc_cat'].apply(lambda x : currcln(x))
com = com.drop(['var','Catogery'], axis = 1)
com['Pairing'] = com['Pairing'].apply(lambda x : x.lower())
com['replace'] = com['replace'].apply(lambda x : x.lower())

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
    
df3['new_desc_cat'] = df3['desc_cat'].apply(lambda x : catcln1(x,com))

comp = ['inc','stk','corp ','llc','pvt','plc']
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : 'Company' if x in comp else x)
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
        
df3['new_desc_cat'] = df3['new_desc_cat'].apply(lambda x : desccat(x))

df3['new_pb'] = df3['ViewData.Mapped Custodian Account'].apply(lambda x : x.split('_')[0] if type(x)==str else x)
new_pb_mapping = {'GSIL':'GS','CITIGM':'CITI','JPMNA':'JPM'}
def new_pf_mapping(x):
    if x=='GSIL':
        return 'GS'
    elif x == 'CITIGM':
        return 'CITI'
    elif x == 'JPMNA':
        return 'JPM'
    else:
        return x
df3['new_pb'] = df3['new_pb'].apply(lambda x : new_pf_mapping(x))
df3['ViewData.Prime Broker1'] = df3['ViewData.Prime Broker1'].fillna('kkk')
df3['new_pb1'] = df3.apply(lambda x : x['new_pb'] if x['ViewData.Prime Broker1']=='kkk' else x['ViewData.Prime Broker1'],axis = 1)
df3['new_pb1'] = df3['new_pb1'].apply(lambda x : x.lower())


# In[430]:


df3['ViewData.Settle Date'] = pd.to_datetime(df3['ViewData.Settle Date'])
days = [1,30,31,29]
df3['monthend marker'] = df3['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)


# In[431]:


df3['comm_marker'] = 'zero'
df3['new_pb2'] = df3.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
df3['new_pb2'] = df3['new_pb2'].apply(lambda x : x.lower())


# In[432]:


df3.columns


# In[433]:


cols = ['ViewData.Transaction Type1',
 'ViewData.Asset Type Category1',
 'ViewData.Investment Type1',
 'new_desc_cat',
 'new_pb2',
 'new_pb1',
 'comm_marker',
 'monthend marker']


# In[434]:


df4 = df3[cols]


# In[435]:


df4['ViewData.Transaction Type1'] = df4['ViewData.Transaction Type1'].fillna('aa')
df4['ViewData.Asset Type Category1'] = df4['ViewData.Asset Type Category1'].fillna('aa')
df4['ViewData.Investment Type1'] = df4['ViewData.Investment Type1'].fillna('aa')
df4['new_desc_cat'] = df4['new_desc_cat'].fillna('aa')
df4['new_pb2'] = df4['new_pb2'].fillna('aa')
df4['new_pb1'] = df4['new_pb1'].fillna('aa')
df4['comm_marker'] = df4['comm_marker'].fillna('aa')
df4['monthend marker'] = df4['monthend marker'].fillna('aa')


# In[436]:


import pickle
filename = 'finalized_model_lombard_249_v1.sav'
clf = pickle.load(open(filename, 'rb'))


# In[437]:


df4.count()


# In[438]:


cb_predictions = clf.predict(df4)


# In[ ]:


demo = []
for item in cb_predictions:
    demo.append(item[0])
df3['predicted category'] = pd.Series(demo)


# In[439]:





# In[440]:


com_temp = pd.read_csv('lobard 249 comment template for delivery.csv')
com_temp = com_temp.rename(columns = {'Category':'predicted category','template':'predicted template'})
result_non_trade = df3.copy()
result_non_trade = pd.merge(result_non_trade,com_temp,on = 'predicted category',how = 'left')
def comgen(x,y,z,k):
    if x == 'Geneva':
        
        com = k + ' ' +y + ' ' + str(z)
    else:
        com = "Geneva" + ' ' +y + ' ' + str(z)
        
    return com

result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
result_non_trade['predicted status'] = 'comment'
result_non_trade['predicted action'] = 'OB'
result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID','ViewData.Status','predicted status','predicted action','predicted category','predicted comment']]
result_non_trade.to_csv('Lombard/249/Comment file for lombard 249.csv')


# In[ ]:





# In[ ]:





# In[ ]:




