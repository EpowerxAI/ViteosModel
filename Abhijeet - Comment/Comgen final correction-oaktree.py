#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[ ]:


def comgen(x,y,z,k,m,a,b,c,d):
    trade_ttype = ['buy','sell','sell short','cover short']
    pos_break = ['future dated trade']
    type1 = ['spot fx','forward fx' ,'Cash deposit']
    type2 = ['subscription','redemption']
    
    
    x = x.lower()
    if m in type2:
        if x != 'geneva':
            if ((c!= 0)):
                com = z + ' ' + '-' + k + ' ' +y + ' ' + str(z) + " " + 'with fee'+' '+ str(round(d,1))+ '. Geneva yet to book' 
            else:
                com = z + ' ' + '-' + k + ' ' +y + ' ' + str(z) + '. Geneva yet to book'
            
        else:
            if ((c!= 0)):
                com = z + ' ' + '-' + 'Integrata' + ' ' +y + ' ' + str(z) + " " + 'with fee'+' '+ str(round(d,1)) + '.'+ k+ ' yet to book' 
            else:
                com = z + ' ' + '-' + 'Integrata' + ' ' +y + ' ' + str(z) + '.'+ k+ ' yet to book'
                
    if m in type1:
        if x != 'geneva':
        
            com = z + ' ' + '-' + k + ' ' +y + ' ' + str(z) + ".Need to check with custody and Integrata yet to book"
        else:
            com = z + ' ' + '-' + 'Integrata' + ' ' +y + ' ' + str(z)+ '.' +'Need to check with custody and'+ ' ' + k + 'yet to book'
           
    elif m in trade_ttype:
        if x != 'geneva':
            if ((a!= 0) & (b!= 0)):
                com = z + ' ' + '-' +k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '. Integrata yet to book' 
            elif  ((a==0) & (b!=0)):
                com =z + ' ' + '-' + k + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
            elif  ((a!=0) & (b==0)):
                com =z + ' ' + '-' + k + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
            else:
                com =z + ' ' + '-' + k + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) +  '. Integrata yet to book'
        else:
            if ((a!= 0) & (b!= 0)):
                com = z + ' ' + '-' +'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c)+ '.'+ 'k'+ ' yet to book' 
            elif  ((a==0) & (b!=0)):
                com = z + ' ' + '-' +'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ 'k'+ ' yet to book'
            elif  ((a!=0) & (b==0)):
                com = z + ' ' + '-' +'Integrata' + ' ' +y + ' ' + str(z) + " " + 'for price' +' ' + str(a) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ 'k'+ ' yet to book'
            else:
                com = z + ' ' + '-' +'Integrata' + ' ' +y + ' ' + str(z) + ' ' + 'on trade date' + ' ' + str(c) + '.'+ 'k'+ ' yet to book'
    
    elif m in pos_break:
        
        if x != 'geneva':
        
            com = z + ' ' + '-' + 'Timing Difference.' + ' '+ k + ' ' +y + ' ' + str(z) + ". Integrata' yet to book"
        else:
            com = z + ' ' + '-' + 'Timing Difference.' + ' '+ 'Integrata' + ' ' +y + ' ' + str(z)+ '.' + k + 'yet to book'
        
    else:
        if x != 'geneva':
        
            com = k + ' ' +y + ' ' + str(z) + ". Integrata' yet to book"
        else:
            com = 'Integrata' + ' ' +y + ' ' + str(z)+ '.' + k + 'yet to book'
        
    return com


result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date'].dt.date
result_non_trade['ViewData.Settle Date2'] = result_non_trade['ViewData.Settle Date2'].astype(str)
result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date'].dt.date
result_non_trade['ViewData.Trade Date2'] = result_non_trade['ViewData.Trade Date2'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)
#result_non_trade['new_pb1'] = result_non_trade['new_pb1'].apply(lambda x : brokermap(x))

#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date2'],x['new_pb1'],x['predicted category'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date']), axis = 1)

result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]

