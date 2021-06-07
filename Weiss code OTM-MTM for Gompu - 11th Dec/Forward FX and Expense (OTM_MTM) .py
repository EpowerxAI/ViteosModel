#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd


# ## Forward FX

# In[ ]:


def forwardfx_tt_flag(tt):
    tt = tt.lower()
    if any(key in tt for key in ['forwardfx',' fx']):
        tt_flag = 1
    else:
        tt_flag = 0
    return tt_flag
cc['Forward_fx_flag'] = cc.apply(lambda x: forwardfx_tt_flag(x['ViewData.Transaction Type']), axis=1)



if cc[(cc['Forward_fx_flag']==1)].shape[0]>0:
    dd2 = cc[cc['Forward_fx_flag']==1]
    dd2 = dd2.reset_index().drop('index',1)
    dd2['ViewData.Settle Date'] = pd.to_datetime(dd2['ViewData.Settle Date'])
    dd2['filter_key_with_sd'] = dd2['filter_key'].astype(str) + dd2['ViewData.Settle Date'].astype(str)
else:
    dd2 = pd.DataFrame()

filter_key_umt_umb_forward = []

if dd2.empty == False:
    for key in dd2['filter_key_with_sd'].unique():        
        dd2_dummy = dd2[dd2['filter_key_with_sd']==key]
        #print(key)
        if (-0.2<= dd2_dummy['ViewData.Net Amount Difference'].sum() <0.2) & (dd2_dummy.shape[0]>2) & (dd2_dummy['Trans_side'].nunique()>1):
            #print(cc2_dummy.shape[0])
            #print(key)
            filter_key_umt_umb_forward.append(key)


# In[ ]:


############################### ForwardFX Many to many ############################

if filter_key_umt_umb_forward ==[]:
    filter_key_umt_umb_forward = ['None']

fx_mtm_1_ids = []
fx_mtm_0_ids = []

if dd2.empty == False:
    for key in filter_key_umt_umb_forward:
        one_side_forward = dd2[dd2['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
        zero_side_forward = dd2[dd2['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
        one_side_forward = [i for i in one_side_forward if i not in ['nan','None','']]
        zero_side_forward = [i for i in zero_side_forward if i not in ['nan','None','']]
        fx_mtm_1_ids.append(one_side_forward)
        fx_mtm_0_ids.append(zero_side_forward)

if fx_mtm_1_ids !=[]:
    mtm_list_1_fx = list(np.concatenate(fx_mtm_1_ids))
else:
    mtm_list_1_fx = []

if fx_mtm_0_ids !=[]:
    mtm_list_0_fx = list(np.concatenate(fx_mtm_0_ids))
else:
    mtm_list_0_fx = []
    
    
##########################################################################################    
## Data Frame for MTM from ForwardFX

mtm_df_fx = pd.DataFrame(np.arange(len(fx_mtm_0_ids)))
mtm_df_fx.columns = ['index']

mtm_df_fx['ViewData.Side0_UniqueIds'] = fx_mtm_0_ids
mtm_df_fx['ViewData.Side1_UniqueIds'] = fx_mtm_1_ids
mtm_df_fx = mtm_df_fx.drop('index',1)


# In[ ]:


cc = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0_fx)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1_fx)))]
cc = cc.reset_index().drop('index',1)


# ## Expense

# In[ ]:


def expense_tt_flag(tt):
    tt = tt.lower()
    if any(key in tt for key in ['expense','withdraw','deposit']) and tt!='cash deposit':
        tt_flag = 1
    else:
        tt_flag = 0
    return tt_flag
cc['Expense_flag'] = cc.apply(lambda x: expense_tt_flag(x['ViewData.Transaction Type']), axis=1)


# In[ ]:


if cc[(cc['Expense_flag']==1)].shape[0]>0:
    ee2 = cc[cc['Expense_flag']==1]
    ee2 = ee2.reset_index().drop('index',1)
    ee2['ViewData.Settle Date'] = pd.to_datetime(ee2['ViewData.Settle Date'])
    ee2['filter_key_with_sd'] = ee2['filter_key'].astype(str) + ee2['ViewData.Settle Date'].astype(str)
else:
    ee2 = pd.DataFrame()

filter_key_umt_umb_expense = []

if ee2.empty == False:
    for key in ee2['filter_key_with_sd'].unique():        
        ee2_dummy = ee2[ee2['filter_key_with_sd']==key]
        #print(key)
        if (-0.2<= ee2_dummy['ViewData.Net Amount Difference'].sum() <0.2) & (ee2_dummy.shape[0]>2) & (ee2_dummy['Trans_side'].nunique()>1):
            #print(cc2_dummy.shape[0])
            #print(key)
            filter_key_umt_umb_expense.append(key)


# In[ ]:


############################### ForwardFX Many to many ############################

if filter_key_umt_umb_expense ==[]:
    filter_key_umt_umb_expense = ['None']

ex_mtm_1_ids = []
ex_mtm_0_ids = []

if ee2.empty == False:
    for key in filter_key_umt_umb_expense:
        one_side_expense = ee2[ee2['filter_key_with_sd']== key]['ViewData.Side1_UniqueIds'].unique()
        zero_side_expense = ee2[ee2['filter_key_with_sd']== key]['ViewData.Side0_UniqueIds'].unique()
        one_side_expense = [i for i in one_side_expense if i not in ['nan','None','']]
        zero_side_expense = [i for i in zero_side_expense if i not in ['nan','None','']]
        ex_mtm_1_ids.append(one_side_expense)
        ex_mtm_0_ids.append(zero_side_expense)

if ex_mtm_1_ids !=[]:
    mtm_list_1_ex = list(np.concatenate(ex_mtm_1_ids))
else:
    mtm_list_1_ex = []

if ex_mtm_0_ids !=[]:
    mtm_list_0_ex = list(np.concatenate(ex_mtm_0_ids))
else:
    mtm_list_0_ex = []
    
    
##########################################################################################    
## Data Frame for MTM from ForwardFX

mtm_df_ex = pd.DataFrame(np.arange(len(ex_mtm_0_ids)))
mtm_df_ex.columns = ['index']

mtm_df_ex['ViewData.Side0_UniqueIds'] = ex_mtm_0_ids
mtm_df_ex['ViewData.Side1_UniqueIds'] = ex_mtm_1_ids
mtm_df_ex = mtm_df_ex.drop('index',1)


# In[ ]:


cc = cc[~((cc['ViewData.Side0_UniqueIds'].isin(mtm_list_0_ex)) |(cc['ViewData.Side1_UniqueIds'].isin(mtm_list_1_ex)))]
cc = cc.reset_index().drop('index',1)

