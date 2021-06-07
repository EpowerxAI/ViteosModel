#!/usr/bin/env python
# coding: utf-8
df1['ViewData.Transaction Type'] = df1['ViewData.Transaction Type'].apply(lambda x : x.lower() if type(x)==str else x)
fullcall2 = df1.groupby(['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'])['ViewData.Transaction Type'].apply(list).reset_index()
def fullcallcheck(x):
    if (('corp red payment' in x) & ('interest' in x)):
        return 1
    else:
        return 0
fullcall2['fcmark2'] = fullcall2['ViewData.Transaction Type'].apply(lambda x : fullcallcheck(x))
fullcall2.drop('ViewData.Transaction Type', axis = 1, inplace = True)
df1 = pd.merge(df1, fullcall2, on = ['ViewData.Task Business Date', 'ViewData.Mapped Custodian Account','ViewData.Currency','ViewData.Description'], how = 'left' )
rem_ttype = ['corp red payment','interest']

def fc_remove(x,y):
    if ((x ==1) &  (y in rem_ttype)):
        return 1
    else:
        return 0
    
if df1[df1['fcmark2']==1].shape[0]!=0:
    df1['fc_remove'] = df1.apply(lambda row: fc_remove(row['fcmark2'],row['ViewData.Transaction Type']), axis =1)
    if df1[df1['fc_remove']==1].shape[0]!=0:
        fc2 = df1[df1['fc_remove']==1]
        fc2['predicted comment'] = fc2.apply(lambda x : comgen(x['ViewData.Side0_UniqueIds'],x['ViewData.Side1_UniqueIds'],x['ViewData.Settle Date'],x['ViewData.Prime Broker']), axis = 1)
        fc2['predicted status'] = 'No-pair'
        fc2['predicted action'] = 'OB'
        fc2['predicted category'] = 'full call corp action'
        fc2 = fc2[output_col]
        fc2.to_csv('oaktree/oaktree final prediction fullcal p2.csv')
        df1 = df1[df1['fc_remove']!=1]
        df1.drop(['fcmark2','fc_remove'], axis = 1, inplace = True)
    else:
        df1 = df1.copy()
        df1.drop(['fcmark2','fc_remove'], axis = 1, inplace = True)
else:
    df1 = df1.copy()
    df1.drop(['fcmark2'], axis = 1, inplace = True)
    

