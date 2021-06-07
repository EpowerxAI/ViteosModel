# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 20:16:43 2021

@author: consultant138
"""

def comgen(x,y,z,k,m,a,b,c):
    trade_ttype = ['buy','sell','sell short','cover short','spot fx','forward','forward fx','spotfx','forwardfx']
    x = x.lower()
    if m in trade_ttype:
        if x == 'geneva':
        
            com = k + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) 
        else:
            com = "Geneva" + ' ' +y + ' ' + str(z) + " " + 'for price'+' '+ str(a) + ' and quantity' +' ' + str(b) + ' ' + 'on trade date' + ' ' + str(c) + '.' + k + 'booked the transaction'
    else:
        if x == 'geneva':
        
            com = k + ' ' +y + ' ' + str(z)
        else:
            com = "Geneva" + ' ' +y + ' ' + str(z)+ '.' + k + 'booked the transaction'
        
    return com


result_non_trade['new_pb2'] = result_non_trade['new_pb2'].astype(str)
result_non_trade['predicted template'] = result_non_trade['predicted template'].astype(str)
result_non_trade['ViewData.Settle Date'] = result_non_trade['ViewData.Settle Date'].astype(str)
result_non_trade['new_pb1'] = result_non_trade['new_pb1'].astype(str)

#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
#Change made on 24-12-2020 as per Abhijeet. The comgen function below was commented out and a new, more elaborate comgen function was coded in. Also, corresponding to the comgen function, predicted_comment apply function was also changed.
#result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1']), axis = 1)
result_non_trade['predicted comment'] = result_non_trade.apply(lambda x : comgen(x['new_pb2'],x['predicted template'],x['ViewData.Settle Date'],x['new_pb1'],x['ViewData.Transaction Type1'],x['ViewData.Price'],x['ViewData.Quantity'],x['ViewData.Trade Date']), axis = 1)

result_non_trade = result_non_trade[['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','predicted category','predicted comment']]

