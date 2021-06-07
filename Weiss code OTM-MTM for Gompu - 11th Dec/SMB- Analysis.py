#!/usr/bin/env python
# coding: utf-8

# In[382]:


import numpy as np
import pandas as pd


# In[395]:


meo = pd.read_csv('meo_df_setup_125_date_2020-12-07.csv')


# In[396]:


meo = meo.drop('Unnamed: 0',1)


# In[397]:


meo2 = meo[meo['ViewData.Status'].isin(['OB','SMB','SPM','UMB'])]
meo2 = meo2.reset_index().drop('index',1)


# In[398]:


meo2['ViewData.Net Amount Difference Absolute'] = np.round(meo2['ViewData.Net Amount Difference Absolute'],2)


# In[399]:


abs_amount_count = meo2['ViewData.Net Amount Difference Absolute'].value_counts().reset_index()


# In[400]:


duplicate_amount = abs_amount_count[abs_amount_count['ViewData.Net Amount Difference Absolute']==2]
duplicate_amount.columns = ['ViewData.Net Amount Difference Absolute','count']
duplicate_amount = duplicate_amount.reset_index().drop('index',1)


# In[401]:


if duplicate_amount.shape[0]>0:
    meo3 = meo2[meo2['ViewData.Net Amount Difference Absolute'].isin(duplicate_amount['ViewData.Net Amount Difference Absolute'].unique())]
    meo3 = meo3.reset_index().drop('index',1)
    meo3 = meo3.sort_values(by='ViewData.Net Amount Difference Absolute')
    meo3 = meo3.reset_index().drop('index',1)
    
    smb_amount = meo3[meo3['ViewData.Status'].isin(['SMB'])]['ViewData.Net Amount Difference Absolute'].unique()
    umb_amount = meo3[meo3['ViewData.Status'].isin(['UMB'])]['ViewData.Net Amount Difference Absolute'].unique()
    
    smb_ob_table = meo3[meo3['ViewData.Net Amount Difference Absolute'].isin(smb_amount)]
    umb_ob_table = meo3[meo3['ViewData.Net Amount Difference Absolute'].isin(umb_amount)]
    
    ob_breakid = []
    smb_breakid = []
    for amount in smb_amount:
        ob = smb_ob_table[(smb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (smb_ob_table['ViewData.Status']=='OB')]
        smb = smb_ob_table[(smb_ob_table['ViewData.Net Amount Difference Absolute']==amount) & (smb_ob_table['ViewData.Status']=='SMB')]
        if ob.shape[0]==1 and smb.shape[0]==1:
            ob_breakid.append(ob['ViewData.BreakID'].values)
            smb_breakid.append(smb['ViewData.BreakID'].values)
            
    if len(ob_breakid)>0:
        final_smb_ob_table = pd.DataFrame(ob_breakid)
        final_smb_ob_table.columns = ['BreakID_OB']
        final_smb_ob_table['BreakID_SMB'] = smb_breakid
        final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].apply(lambda x: str(x).replace("[",''))
        final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].apply(lambda x: str(x).replace("]",''))
        final_smb_ob_table['BreakID_SMB'] = final_smb_ob_table['BreakID_SMB'].astype(int)
    else:
        final_smb_ob_table = pd.DataFrame()
else:
    final_smb_ob_table = pd.DataFrame()


# In[402]:


final_smb_ob_table


# In[ ]:





# In[ ]:





# In[ ]:




