# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 22:45:19 2020

@author: consultant138
"""
cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
'Activity Code','Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount','Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference','B-P Net Amount Difference Absolute',
'BreakID',
'Business Date','Cancel Amount','Cancel Flag','CUSIP','Custodian',
'Custodian Account',
'Derived Source','Description','ExpiryDate','ExternalComment1','ExternalComment2',
'ExternalComment3','Fund','FX Rate','Interest Amount','InternalComment1','InternalComment2',
'InternalComment3','Investment Type','Is Combined Data','ISIN','Keys',
'Mapped Custodian Account','Net Amount Difference','Net Amount Difference Absolute','Non Trade Description',
'OTE Custodian Account',
#'Predicted Action','Predicted Status','Prediction Details',
'Price','Prime Broker',
'Quantity','SEDOL','Settle Date','SPM ID','Status','Strike Price',
'System Comments','Ticker','Trade Date','Trade Expenses','Transaction Category','Transaction ID','Transaction Type',
'Underlying Cusip','Underlying Investment ID','Underlying ISIN','Underlying Sedol','Underlying Ticker','Source Combination','_ID']
#'UnMapped']


cols_to_show = [
'Account Type',
'Accounting Net Amount',
'Accounting Net Amount Difference',
'Activity Code',
'Age',
'Alt ID 1',
'Asset Type Category',
'Bloomberg_Yellow_Key',
'B-P Net Amount',
'B-P Net Amount Difference',
'B-P Net Amount Difference Absolute',
'BreakID',
'Business Date',
'Call Put Indicator',
'Cancel Amount',
'Cancel Flag',
'Commission',
'Currency',
'CUSIP',
'Custodian',
'Custodian Account',
'Department',
'Description',
'ExpiryDate',
'ExternalComment2',
'Fund',
'FX Rate',
'Interest Amount',
'InternalComment2',
'Investment ID',
'Investment Type',
'Is Combined Data',
'ISIN',
'Keys',
'Knowledge Date',
'Mapped Custodian Account',
'Net Amount Difference',
'Non Trade Description',
'OTE Custodian Account',
'OTE Ticker',
'PB Account Numeric',
'Portfolio ID',
'Portolio',
'Price',
'Prime Broker',
'Principal Amount',
'Quantity',
'Sec Fees',
'SEDOL',
'Settle Date',
'Status',
'Strike Price',
'System Comments',
'Ticker',
'Trade Date',
'Trade Expenses',
'Transaction Category',
'Transaction ID',
'Transaction Type',
'Underlying Cusip',
'Underlying Investment ID',
'Underlying ISIN',
'Underlying Sedol',
'Underlying Ticker',
'UserTran1',
'UserTran2',
'Value Date',
]



import numpy as np
import pandas as pd
from tqdm import tqdm

def mhreplaced(item):
    word1 = []
    word2 = []
    if (type(item) == str):
    
        for items in item.split(' '):
            if (type(items) == str):
                items = items.lower()
                if items.isdigit() == False:
                    word1.append(items)
        
            
                for c in word1:
                    if c.endswith('MH')==False:
                        word2.append(c)
    
                words = ' '.join(word2)
                return words
    else:
        return item
    

def fundmatch(item):
    items = item.lower()
    items = item.replace(' ','') 
    return items

def a_keymatch(a_cusip, a_isin):
    
    pb_nan = 0
    a_common_key = 'NA' 
    if a_cusip=='nan' and a_isin =='nan':
        pb_nan =1
    elif(a_cusip!='nan' and a_isin == 'nan'):
        a_common_key = a_cusip
    elif(a_cusip =='nan' and a_isin !='nan'):
        a_common_key = a_isin
    else:
        a_common_key = a_isin
        
    return pd.Series([pb_nan, a_common_key])

def b_keymatch(b_cusip, b_isin):
    accounting_nan = 0
    b_common_key = 'NA'
    if b_cusip =='nan' and b_isin =='nan':
        accounting_nan =1
    elif (b_cusip!='nan' and b_isin == 'nan'):
        b_common_key = b_cusip
    elif(b_cusip =='nan' and b_isin !='nan'):
        b_common_key = b_isin
    else:
        b_common_key = b_isin
    return pd.Series([accounting_nan, b_common_key])

def equals_fun(a,b):
    if a == b:
        return 1
    else:
        return 0

vec_equals_fun = np.vectorize(equals_fun)



add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


new_cols = ['ViewData.' + x for x in cols] + add
date_numbers_list = [1,2,3,4,
                     7,8,9,10,11,
                     14,15,16,17,18,
                     21,22,23,24,25,
                     28,29,30]
setup_list = [123,125,170,531,833,1200]



for setup in setup_list:
    filepaths = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_' + str(setup) + '_2020-06-' + str(i) + '.csv' for i in date_numbers_list]
    i = 0
    for filepath in filepaths:
        print(filepath)
        
#        i = i+1
        meo = pd.read_csv(filepath,usecols=new_cols)
        df1 = meo[~meo['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR'])]
        #df = df[df['MatchStatus'] != 21]
        df1 = df1[~df1['ViewData.Status'].isnull()]
        df1 = df1.reset_index()
        df1 = df1.drop('index',1)
        df = df1.copy()
        df = df.reset_index()
        df = df.drop('index',1)
        df['Date'] = pd.to_datetime(df['ViewData.Task Business Date'])
        df = df[~df['Date'].isnull()]
        df = df.reset_index()
        df = df.drop('index',1)
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df['Date'] = df['Date'].astype(str)
        df = df[df['ViewData.Status'].isin(['OB','SPM','SDB','UOB','UDB'])]
        df = df.reset_index()
        df = df.drop('index',1)
        df['ViewData.Side0_UniqueIds'] = df['ViewData.Side0_UniqueIds'].astype(str)
        df['ViewData.Side1_UniqueIds'] = df['ViewData.Side1_UniqueIds'].astype(str)
        df['flag_side0'] = df.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
        df['flag_side1'] = df.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
        
        print('For ' + filepath + ' the Date value count is:')
        print(df['Date'].value_counts())
        
        date_i = df['Date'].mode()[0]
        
        print('For ' + filepath + ' choosing the date : ' + date_i)
        
        df = df.rename(columns= {'ViewData.B-P Net Amount':'ViewData.B-P Net Amount'})
        
        sample = df[df['Date'] == date_i]
        sample = sample.reset_index()
        sample = sample.drop('index',1)
        
        sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','flag_side0'] = 0
        sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','flag_side1'] = 0
        
        sample.loc[sample['ViewData.Side1_UniqueIds']=='nan','Trans_side'] = 'B_side'
        sample.loc[sample['ViewData.Side0_UniqueIds']=='nan','Trans_side'] = 'A_side'
    
        sample.loc[sample['Trans_side']=='A_side','ViewData.B-P Currency'] = sample.loc[sample['Trans_side']=='A_side','ViewData.Currency']
        sample.loc[sample['Trans_side']=='B_side','ViewData.Accounting Currency'] = sample.loc[sample['Trans_side']=='B_side','ViewData.Currency'] 
    
        sample['ViewData.B-P Currency'] = sample['ViewData.B-P Currency'].astype(str)
        sample['ViewData.Accounting Currency'] = sample['ViewData.Accounting Currency'].astype(str)
        sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
        sample['ViewData.Source Combination'] = sample['ViewData.Source Combination'].astype(str)
        
        #sample['ViewData.Mapped Custodian Account'] = sample['ViewData.Mapped Custodian Account'].astype(str)
        
        sample['filter_key'] = sample.apply(lambda x: x['ViewData.Source Combination'] + x['ViewData.Mapped Custodian Account'] + x['ViewData.B-P Currency'] if x['Trans_side']=='A_side' else x['ViewData.Source Combination'] + x['ViewData.Mapped Custodian Account'] + x['ViewData.Accounting Currency'], axis=1)
    
    
        sample1 = sample[(sample['flag_side0']<=1) & (sample['flag_side1']<=1) & (sample['ViewData.Status'].isin(['OB','SPM','SDB','UDB','UOB']))]
    
        sample1 = sample1.reset_index()
        sample1 = sample1.drop('index', 1)
        
        sample1['ViewData.BreakID'] = sample1['ViewData.BreakID'].astype(int)
        
        sample1 = sample1[sample1['ViewData.BreakID']!=-1]
        sample1 = sample1.reset_index()
        sample1 = sample1.drop('index',1)
        
        sample1 = sample1.sort_values(['ViewData.BreakID','Date'], ascending =[True, False])
        sample1 = sample1.reset_index()
        sample1 = sample1.drop('index',1)
        
        aa = sample1[sample1['Trans_side']=='A_side']
        bb = sample1[sample1['Trans_side']=='B_side']
        
        bb['ViewData.Source Combination'].value_counts()
        
        aa['filter_key'] = aa['ViewData.Source Combination'].astype(str) + aa['ViewData.Mapped Custodian Account'].astype(str) + aa['ViewData.B-P Currency'].astype(str)
    
        bb['filter_key'] = bb['ViewData.Source Combination'].astype(str) + bb['ViewData.Mapped Custodian Account'].astype(str) + bb['ViewData.Accounting Currency'].astype(str)
        
        aa = aa.reset_index()
        aa = aa.drop('index', 1)
        bb = bb.reset_index()
        bb = bb.drop('index', 1)
        
        common_cols = ['ViewData.Accounting Net Amount', 'ViewData.Age',
         'ViewData.Age WK', 'ViewData.Asset Type Category',
         'ViewData.B-P Net Amount', 'ViewData.Base Net Amount','ViewData.CUSIP', 
         'ViewData.Cancel Amount',
           'ViewData.Cancel Flag',
         #'ViewData.Commission',
            'ViewData.Currency', 'ViewData.Custodian',
           'ViewData.Custodian Account',
           'ViewData.Description', 'ViewData.ExpiryDate', 'ViewData.Fund',
           'ViewData.ISIN',
           'ViewData.Investment Type',
          # 'ViewData.Keys',
           'ViewData.Mapped Custodian Account',
           'ViewData.Net Amount Difference',
           'ViewData.Net Amount Difference Absolute',
            #'ViewData.OTE Ticker',
            'ViewData.Price',
           'ViewData.Prime Broker', 'ViewData.Quantity',
           'ViewData.SEDOL', 'ViewData.SPM ID', 'ViewData.Settle Date',
           
         #  'ViewData.Strike Price',
                   'Date',
           'ViewData.Ticker', 'ViewData.Trade Date',
           'ViewData.Transaction Category',
           'ViewData.Transaction Type', 'ViewData.Underlying Cusip',
           'ViewData.Underlying ISIN',
           'ViewData.Underlying Sedol','filter_key','ViewData.Status','ViewData.BreakID',
                  'ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData._ID']
        
        bb = bb[~bb['ViewData.Accounting Net Amount'].isnull()]
        bb = bb.reset_index()
        bb = bb.drop('index',1)
    
        ###################### loop 3 ###############################
        
        
        
        pool =[]
        key_index =[]
        training_df =[]
        
        no_pair_ids = []
        #max_rows = 5
        
        for d in tqdm(aa['Date'].unique()):
            aa1 = aa.loc[aa['Date']==d,:][common_cols]
            bb1 = bb.loc[bb['Date']==d,:][common_cols]
            
            aa1 = aa1.reset_index()
            aa1 = aa1.drop('index',1)
            bb1 = bb1.reset_index()
            bb1 = bb1.drop('index', 1)
            
            bb1 = bb1.sort_values(by='filter_key',ascending =True)
            
            for key in (list(np.unique(np.array(list(aa1['filter_key'].values) + list(bb1['filter_key'].values))))):
                
                df1 = aa1[aa1['filter_key']==key]
                df2 = bb1[bb1['filter_key']==key]
        
                if df1.empty == False and df2.empty == False:
                    #aa_df = pd.concat([aa1[aa1.index==i]]*repeat_num, ignore_index=True)
                    #bb_df = bb1.loc[pool[len(pool)-1],:][common_cols].reset_index()
                    #bb_df = bb_df.drop('index', 1)
        
                    df1 = df1.rename(columns={'ViewData.BreakID':'ViewData.BreakID_A_side'})
                    df2 = df2.rename(columns={'ViewData.BreakID':'ViewData.BreakID_B_side'})
        
                    #dff  = pd.concat([aa[aa.index==i],bb.loc[pool[i],:][accounting_vars]],axis=1)
        
                    df1 = df1.reset_index()
                    df2 = df2.reset_index()
                    df1 = df1.drop('index', 1)
                    df2 = df2.drop('index', 1)
        
                    df1.columns = ['SideA.' + x  for x in df1.columns] 
                    df2.columns = ['SideB.' + x  for x in df2.columns]
        
                    df1 = df1.rename(columns={'SideA.filter_key':'filter_key'})
                    df2 = df2.rename(columns={'SideB.filter_key':'filter_key'})
        
                    #dff = pd.concat([aa_df,bb_df],axis=1)
                    dff = pd.merge(df1, df2, on='filter_key')
                    training_df.append(dff)
                        #key_index.append(i)
                    #else:
                    #no_pair_ids.append([aa1[(aa1['filter_key']=='key') & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0]])
                       # no_pair_ids.append(aa1[(aa1['filter_key']== key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values[0])
            
                else:
                    no_pair_ids.append([aa1[(aa1['filter_key']==key) & (aa1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side1_UniqueIds'].values])
                    no_pair_ids.append([bb1[(bb1['filter_key']==key) & (bb1['ViewData.Status'].isin(['OB','SDB']))]['ViewData.Side0_UniqueIds'].values])
                    
        if len(no_pair_ids) != 0:
            no_pair_ids = np.unique(np.concatenate(no_pair_ids,axis=1)[0])
            no_pair_ids_df = pd.DataFrame(no_pair_ids)
            #no_pair_ids_df = no_pair_ids_df.rename(columns={'0':'filter_key'})
            no_pair_ids_df.columns = ['filter_key']
            no_pair_ids_df_filepath = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/X_Test/no_pair_ids_' +str(setup) + '_2020-06-' + str(date_numbers_list[i]) + '.csv'
            no_pair_ids_df.to_csv(no_pair_ids_df_filepath)
        else:
             no_pair_ids_warning_path = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/X_Test/WARNING_no_pair_ids_' +str(setup) + '_2020-06-' + str(date_numbers_list[i]) + '.txt'
             with open(no_pair_ids_warning_path, 'w') as f:
                 f.write('No no pair ids found for this setup and date combination')

        test_file = pd.concat(training_df)
        test_file = test_file.reset_index()
        test_file = test_file.drop('index',1)
        test_file['SideB.ViewData.BreakID_B_side'] = test_file['SideB.ViewData.BreakID_B_side'].astype('int64')
        test_file['SideA.ViewData.BreakID_A_side'] = test_file['SideA.ViewData.BreakID_A_side'].astype('int64')
        
        model_cols = [
        'SideA.ViewData.Accounting Net Amount', 
        'SideA.ViewData.B-P Net Amount', 
        'SideA.ViewData.CUSIP', 
        'SideA.ViewData.Currency', 
        #'SideA.ViewData.Description',
        'SideA.ViewData.ISIN', 
        'SideB.ViewData.Accounting Net Amount',
        'SideB.ViewData.B-P Net Amount',
        'SideB.ViewData.CUSIP',
        'SideB.ViewData.Currency',
        #'SideB.ViewData.Description', 
        'SideB.ViewData.ISIN',
        'SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 
        'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side',
        'label']
        
        y_col = ['label']
         
        test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].str.split(".",expand=True)[0]
        test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].str.split(".",expand=True)[0]
        
        test_file['SideA.ViewData.ISIN'] = test_file['SideA.ViewData.ISIN'].astype(str)
        test_file['SideB.ViewData.ISIN'] = test_file['SideB.ViewData.ISIN'].astype(str)
        test_file['SideA.ViewData.CUSIP'] = test_file['SideA.ViewData.CUSIP'].astype(str)
        test_file['SideB.ViewData.CUSIP'] = test_file['SideB.ViewData.CUSIP'].astype(str)
        test_file['SideA.ViewData.Currency'] = test_file['SideA.ViewData.Currency'].astype(str)
        test_file['SideB.ViewData.Currency'] = test_file['SideB.ViewData.Currency'].astype(str)
    
    
        test_file['SideA.ViewData.Trade Date'] = test_file['SideA.ViewData.Trade Date'].astype(str)
        test_file['SideB.ViewData.Trade Date'] = test_file['SideB.ViewData.Trade Date'].astype(str)
        test_file['SideA.ViewData.Settle Date'] = test_file['SideA.ViewData.Settle Date'].astype(str)
        test_file['SideB.ViewData.Settle Date'] = test_file['SideB.ViewData.Settle Date'].astype(str)
        test_file['SideA.ViewData.Fund'] = test_file['SideA.ViewData.Fund'].astype(str)
        test_file['SideB.ViewData.Fund'] = test_file['SideB.ViewData.Fund'].astype(str)
        
#        test_file['ISIN_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']==x['SideB.ViewData.ISIN'] else 0, axis=1)
#        test_file['CUSIP_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.CUSIP']==x['SideB.ViewData.CUSIP'] else 0, axis=1)
#        test_file['Currency_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Currency']==x['SideB.ViewData.Currency'] else 0, axis=1)
#    
#        test_file['Trade_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Trade Date']==x['SideB.ViewData.Trade Date'] else 0, axis=1)
#        test_file['Settle_Date_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Settle Date']==x['SideB.ViewData.Settle Date'] else 0, axis=1)
#        test_file['Fund_match'] = test_file.apply(lambda x: 1 if x['SideA.ViewData.Fund']==x['SideB.ViewData.Fund'] else 0, axis=1)
        values_ISIN_A_Side = test_file['SideA.ViewData.ISIN'].values
        values_ISIN_B_Side = test_file['SideB.ViewData.ISIN'].values
        
        values_CUSIP_A_Side = test_file['SideA.ViewData.CUSIP'].values
        values_CUSIP_B_Side = test_file['SideB.ViewData.CUSIP'].values
#
#        values_CUSIP_A_Side = test_file['SideA.ViewData.Currency'].values
#        values_CUSIP_B_Side = test_file['SideB.ViewData.Currency'].values

        values_Currency_match_A_Side = test_file['SideA.ViewData.Currency'].values
        values_Currency_match_B_Side = test_file['SideA.ViewData.Currency'].values

        values_Trade_Date_match_A_Side = test_file['SideA.ViewData.Trade Date'].values
        values_Trade_Date_match_B_Side = test_file['SideB.ViewData.Trade Date'].values

        values_Settle_Date_match_A_Side = test_file['SideA.ViewData.Settle Date'].values
        values_Settle_Date_match_B_Side = test_file['SideB.ViewData.Settle Date'].values

        values_Fund_match_A_Side = test_file['SideA.ViewData.Fund'].values
        values_Fund_match_B_Side = test_file['SideB.ViewData.Fund'].values
        
        test_file['ISIN_match'] = vec_equals_fun(values_ISIN_A_Side,values_ISIN_B_Side)
        test_file['CUSIP_match'] = vec_equals_fun(values_CUSIP_A_Side,values_CUSIP_B_Side)
        test_file['Currency_match'] = vec_equals_fun(values_Currency_match_A_Side,values_Currency_match_B_Side)
        test_file['Trade_Date_match'] = vec_equals_fun(values_Trade_Date_match_A_Side,values_Trade_Date_match_B_Side)
        test_file['Settle_Date_match'] = vec_equals_fun(values_Settle_Date_match_A_Side,values_Settle_Date_match_B_Side)
        test_file['Fund_match'] = vec_equals_fun(values_Fund_match_A_Side,values_Fund_match_B_Side)


        
        
        test_file['Amount_diff_1'] = test_file['SideA.ViewData.Accounting Net Amount'] - test_file['SideB.ViewData.B-P Net Amount']
        test_file['Amount_diff_2'] = test_file['SideB.ViewData.Accounting Net Amount'] - test_file['SideA.ViewData.B-P Net Amount']
        
        test_file['Trade_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Trade Date']) - pd.to_datetime(test_file['SideB.ViewData.Trade Date'])).dt.days
    
        test_file['Settle_date_diff'] = (pd.to_datetime(test_file['SideA.ViewData.Settle Date']) - pd.to_datetime(test_file['SideB.ViewData.Settle Date'])).dt.days
        
        test_file['SideA.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideA.ViewData.Fund']), axis=1)
        test_file['SideB.ViewData.Fund'] = test_file.apply(lambda x : fundmatch(x['SideB.ViewData.Fund']), axis=1)
    
        test_file['SideB.ViewData.Transaction Type'] = test_file['SideB.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
        
        test_file['SideA.ViewData.Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].apply(lambda x : mhreplaced(x))
        
        test_file['ViewData.Combined Transaction Type'] = test_file['SideA.ViewData.Transaction Type'].astype(str) +  test_file['SideB.ViewData.Transaction Type'].astype(str)
        
        test_file['ViewData.Combined Fund'] = test_file['SideA.ViewData.Fund'].astype(str) + test_file['SideB.ViewData.Fund'].astype(str)
        
        test_file['SideA.ISIN_NA'] =  test_file.apply(lambda x: 1 if x['SideA.ViewData.ISIN']=='nan' else 0, axis=1)
        test_file['SideB.ISIN_NA'] =  test_file.apply(lambda x: 1 if x['SideB.ViewData.ISIN']=='nan' else 0, axis=1)
        
        test_file[['SideB.ViewData.key_NAN','SideB.ViewData.Common_key']] = test_file.apply(lambda x: b_keymatch(x['SideB.ViewData.CUSIP'], x['SideB.ViewData.ISIN']), axis=1)
        test_file[['SideA.ViewData.key_NAN','SideA.ViewData.Common_key']] = test_file.apply(lambda x: a_keymatch(x['SideA.ViewData.CUSIP'],x['SideA.ViewData.ISIN']), axis=1)
        
        
        test_file['All_key_nan'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.key_NAN']==1 and x['SideA.ViewData.key_NAN']==1 else 0, axis=1)
        
        test_file['SideB.ViewData.Common_key'] = test_file['SideB.ViewData.Common_key'].astype(str)
        test_file['SideA.ViewData.Common_key'] = test_file['SideA.ViewData.Common_key'].astype(str)
    
        test_file['new_key_match'] = test_file.apply(lambda x: 1 if x['SideB.ViewData.Common_key']==x['SideA.ViewData.Common_key'] and x['All_key_nan']==0 else 0, axis=1)
    
        test_file_filepath = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/X_Test/x_test_' + str(setup) + '_2020-06-' + str(date_numbers_list[i]) + '.csv'
    
        
        test_file.to_csv(test_file_filepath)
        i = i+1
        