#!/usr/bin/env python
# coding: utf-8

import pandas as pd


# In[14]:


def brokermap(x):
    if x == 'barclays':
        return 'BARC'
    elif x == 'morgan stanely':
        return 'MS'
    elif x == 'jp morgan':
        return 'jpm'
    elif x == 'goldman sachs':
        return 'gs'
    elif x == 'us bank':
        return 'usbk'
    elif x == 'citi bank':
        return 'citi'
    elif x == 'northern trust':
        return 'NT'
    elif x == 'deutsche bank':
        return 'db'
    elif x=='state street':
        return 'sst'
    elif x == 'bn paribas':
        return 'bnp'
    elif x == 'credit suisse':
        return 'cs'
    else:
        return x
    


# In[ ]:


def comgen(x,y,z,k,m,a,b,c):
    trade_ttype = ['buy','sell','sell short','cover short','spot fx','forward','forward fx','spotfx','forwardfx']
    pos_break = ['settlement amount , no pos break']
    x = x.lower()
    if m in trade_ttype:
        if x != 'geneva':
            if ((a!= 0) & (b!= 0)):
                com = k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '. Geneva yet to book' 
            elif  ((a==0) & (b!=0)):
                com = k + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
            elif  ((a!=0) & (b==0)):
                com = k + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
            else:
                com = k + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) +  '. Geneva yet to book'
        else:
            if ((a!= 0) & (b!= 0)):
                com = 'Geneva' + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '.'+ 'k'+ ' yet to book' 
            elif  ((a==0) & (b!=0)):
                com = 'Geneva' + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ 'k'+ ' yet to book'
            elif  ((a!=0) & (b==0)):
                com = 'Geneva' + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ 'k'+ ' yet to book'
            else:
                com = 'Geneva' + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ 'k'+ ' yet to book'
    
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
        
            com = k + ' ' +y + ' ' + str(z) + ". Geneva yet to book"
        else:
            com = 'Geneva' + ' ' +y + ' ' + str(z)+ '.' + k + 'booked the transaction'
        
    return com


result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].dt.date
result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date2'].astype(str)
result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].dt.date
result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date2'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : brokermap(x))

#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date']), axis = 1)

result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]


# In[2]:


template = pd.read_csv('weiss files for testing gompu/Weiss Comment template for delivery new.csv')


# In[12]:


template.head(4)


# In[7]:


def commentgen(x):
    com = 'booked the' + ' ' +str(x) +' '+'transaction on SD'
    return com


# In[8]:


template['Template'] = template['Category'].apply(lambda x :commentgen(x))


# In[10]:


template.drop('template', axis = 1, inplace = True)


# In[11]:


template = template.rename(columns = {'Template':'template'})


# In[13]:


template.to_csv('weiss files for testing gompu/Weiss Comment template for delivery new.csv')


# In[ ]:




