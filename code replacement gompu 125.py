#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : inttype(x))

def divclient(x):
    if (type(x) == str):
        x = x.lower()
        if ('eqswap div client tax' in x) :
            return 'eqswap div client tax'
        else:
            return x
    else:
        return 'float'
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : divclient(x))

def mhreplace(item):
    item1 = item.split()
    for items in item1:
        items = items.lower()
        if items.endswith('mh')==True:
            item1.remove(items)
    return ' '.join(item1).lower()
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x :mhreplace(x))
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x :x.lower())
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
    
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : compname(x))

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
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : inter(x))

def wht(x):
    if type(x)==str:
        x1 = x.split()
        if x1[0] =='30%':
            return 'Withholding tax'
        else:
            return x
    else:
        return x
df2['ViewData.Transaction Type'] = df2['ViewData.Transaction Type'].apply(lambda x : wht(x))


# In[ ]:


days = [1,30,31,29]
data['ViewData.Settle Date'] = pd.to_datetime(data['ViewData.Settle Date'])
data['monthend marker'] = data['ViewData.Settle Date'].apply(lambda x : 1 if x.day in days else 0)
data['ViewData.Commission'] = data['ViewData.Commission'].fillna('NA')
def comfun(x):
    if x=="NA":
        k = 'NA'
       
    elif x == 0.0:
        k = 'zero'
    else:
        k = 'positive'
   
    return k
data['comm_marker'] = data['ViewData.Commission'].apply(lambda x : comfun(x))
data['new_pb2'] = data.apply(lambda x : 'Geneva' if x['ViewData.Side0_UniqueIds'] != 'AA' else x['new_pb1'], axis = 1)
data['new_pb2'] = data['new_pb2'].apply(lambda x : x.lower())


# In[1]:


import pickle
filename = 'finalized_model_weiss_catrefine_v10_gompu.sav'
clf = pickle.load(open(filename, 'rb'))


# In[2]:


clf.feature_names_


# In[ ]:


'ViewData.Transaction Type1',
 'ViewData.Asset Type Category1',
 'ViewData.Investment Type1',
 'new_desc_cat1',
 'new_pb1',
 'monthend marker',
 'comm_marker',
 'new_pb2'

