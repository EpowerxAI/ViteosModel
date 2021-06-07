#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def autocom(x,y,z,k,a,b):
    if x in sell:
        return 'sell'
    elif x in buy:
        return 'buy'
    elif x in drawdown:
        return 'drawdown'
    elif ((x in redem) & (k=='loan')):
        return 'redemption'

    elif ((x in interest) & (y in term_loan)):
        return 'bank loan interest'
    elif ((x in sink)):
        return 'sink reschedule'
    elif ((x in interest) & (a.isnull()==True) & (b.isnull()==True)):
        return 'monthend interest'
    
    else:
        return z


# In[ ]:


result['predicted'] = result.apply(lambda row: autocom(row['ViewData.Transaction Type1'],row['ViewData.Investment Type1'], row['predicted'],row['new_desc_cat'], row['ViewData.CUSIP'],row['ViewData.ISIN']), axis = 1)

