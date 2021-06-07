# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 17:52:33 2020

@author: consultant138
"""
import numpy as np
import pandas as pd
#from tqdm import tqdm
import pickle

from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report

model_cols = ['SideA.ViewData.B-P Net Amount',
 'SideA.ViewData.Price',
 'SideA.ViewData.Quantity',
 'SideB.ViewData.Accounting Net Amount',
 'SideB.ViewData.Price',
 'SideB.ViewData.Quantity',
 'SideB.ViewData.Status',
 'SideB.ViewData.BreakID_B_side',
 'SideA.ViewData.Status',
 'SideA.ViewData.BreakID_A_side',
              
'SideA.ViewData._ID',
'SideB.ViewData._ID',
              
 'Trade_Date_match',
 'Settle_Date_match',
 'Amount_diff_2',
 'Trade_date_diff',
 'Settle_date_diff',
 'SideA.ISIN_NA',
 'SideB.ISIN_NA',
 'ViewData.Combined Fund',
 'ViewData.Combined Transaction Type',
 'All_key_nan',
 'new_key_match',
'SideB.ViewData.Side0_UniqueIds',
'SideA.ViewData.Side1_UniqueIds']
 #'label']
 

cols = ['Currency','Account Type','Accounting Net Amount',
#'Accounting Net Amount Difference','Accounting Net Amount Difference Absolute ',
'Activity Code','Age','Age WK',
'Asset Type Category','Base Currency','Base Net Amount','Bloomberg_Yellow_Key',
'Cust Net Amount',
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

add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds',
      # 'MetaData.0._RecordID','MetaData.1._RecordID',
       'ViewData.Task Business Date']


new_cols = ['ViewData.' + x for x in cols] + add

prediction_cols = ['ViewData.BreakID', 'ViewData.Side1_UniqueIds', 'ViewData.Side0_UniqueIds','ViewData.Age' ,
       'probability_No_pair', 'probability_UMR','probability_UMB', 'probability_UMT',
       'Final_predicted_break', 'Type', 'ML_flag','ViewData.Status', 'Final_prediction']



date_numbers_list = [1,2,3,4,
                     7,8,9,10,11,
                     14,15,16,17,18,
                     21,22,23,24,25,
                     28,29,30]
filepaths_X_test = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test/x_test_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_no_pair_id_data = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/X_Test/no_pair_ids_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_AUA = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/AUA/AUACollections.AUA_HST_RecData_379_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_MEO = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/MEO/MeoCollections.MEO_HST_RecData_379_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_final_prediction_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions/Final_Predictions_Table_HST_RecData_379_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_accuracy_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions/Accuracy_Table_HST_RecData_379_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
filepaths_crosstab_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/OakTree/JuneData/Final_Predictions/Crosstab_Table_HST_RecData_379_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]

#filename = 'Oak_new_model_V7.sav'
#clf1 = pickle.load(open(filename, 'rb'))

i = 21
for i in range(0,len(date_numbers_list)):
    print("date : " + str(date_numbers_list[i]))
    filename = 'Oak_new_model_V7.sav'
    clf1 = pickle.load(open(filename, 'rb'))
    test_file = pd.read_csv(filepaths_X_test[i])
    test_file = test_file.drop('Unnamed: 0',1)
    X_test = test_file[model_cols]
    X_test = X_test.reset_index()
    X_test = X_test.drop('index',1)
    X_test = X_test.fillna(0)
    # Actual class predictions
    
    rf_predictions = clf1.predict(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))
    # Probabilities for each class
    
    rf_probs = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]
    
    probability_class_0 = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 0]
    probability_class_1 = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 1]
    
    probability_class_2 = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side','SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 2]
    probability_class_3 = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 3]

    #probability_class_4 = clf1.predict_proba(X_test.drop(['SideB.ViewData.Status','SideB.ViewData.BreakID_B_side', 'SideA.ViewData.Status','SideA.ViewData.BreakID_A_side','SideA.ViewData._ID','SideB.ViewData._ID','SideB.ViewData.Side0_UniqueIds','SideA.ViewData.Side1_UniqueIds'],1))[:, 4]
    
    X_test['Predicted_action'] = rf_predictions
    #X_test['Predicted_action_probabilty'] = rf_probs
    X_test['probability_No_pair'] = probability_class_0
    #X_test['probability_Partial_match'] = probability_class_1
    #X_test['probability_UMB'] = probability_class_1
    X_test['probability_UMB'] = probability_class_1
    X_test['probability_UMR'] = probability_class_2
    X_test['probability_UMT'] = probability_class_3
    
    X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & ((X_test['Amount_diff_2']!=0) | (X_test['Amount_diff_2']!=0)),'Predicted_action'] = 'Unrecognized' 
    
    ###### Probability filter for UMT and UMB ################

    #X_test.loc[(X_test['Predicted_action']=='UMT_One_to_One') & (X_test['probability_UMT']<0.90) & (X_test['probability_No_pair']>0.05),'Predicted_action'] = 'No-Pair' 

    #X_test.loc[(X_test['Predicted_action']=='UMB_One_to_One') & (X_test['probability_UMB']<0.75) & (X_test['probability_No_pair']>0.2),'Predicted_action'] = 'No-Pair' 

    #X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['probability_UMR']<0.90) & (X_test['probability_No_pair']>0.05),'Predicted_actionX_test.loc[(X_test['Predicted_action']=='No-Pair') & (X_test['probability_No_pair']<0.9) & (X_test['probability_UMB']>0.05),'Predicted_action'] = 'UMB_One_to_One' 


    #X_test.loc[(X_test['Predicted_action']=='No-Pair') & (X_test['probability_No_pair']<0.95) & (X_test['probability_UMB']>0.05),'Predicted_action'] = 'UMB_One_to_One' 

    #X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Settle_date_diff']>4),'Predicted_action'] = 'No-Pair' 
    #X_test.loc[(X_test['Predicted_action']=='UMR_One_to_One') & (X_test['Settle_date_diff']<-4),'Predicted_action'] = 'No-Pair' 
    
    #X_test.loc[(X_test['SideB.ViewData.Status']=='SDB') & (X_test['SideA.ViewData.Status']=='OB') & (X_test['Predicted_action']=='No-Pair'),'Predicted_action'] = 'SDB/Open Break'
    
    prediction_table =  X_test.groupby('SideB.ViewData.BreakID_B_side')['Predicted_action'].unique().reset_index()
    
    #prob1 = X_test.groupby('SideB.ViewData.BreakID_B_side')['probability_No_pair'].mean().reset_index()
    prediction_table['len'] = prediction_table['Predicted_action'].str.len()
    
    prediction_table['No_Pair_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'No-Pair' in x else 0)
    prediction_table['UMR_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMR_One_to_One' in x else 0)
    
    umr_array = X_test[X_test['Predicted_action']=='UMR_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
    umt_array = X_test[X_test['Predicted_action']=='UMT_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
    umb_array = X_test[X_test['Predicted_action']=='UMB_One_to_One'].groupby(['SideB.ViewData.BreakID_B_side'])['SideA.ViewData.BreakID_A_side'].unique().reset_index()
    
    umr_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMR_array']
    umt_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMT_array']
    umb_array.columns = ['SideB.ViewData.BreakID_B_side', 'Predicted_UMB_array']
    
    prediction_table = pd.merge(prediction_table,umr_array, on='SideB.ViewData.BreakID_B_side', how='left' )
    prediction_table = pd.merge(prediction_table,umt_array, on='SideB.ViewData.BreakID_B_side', how='left' )
    prediction_table = pd.merge(prediction_table,umb_array, on='SideB.ViewData.BreakID_B_side', how='left' )
    
    prediction_table['Final_prediction'] = prediction_table.apply(lambda x: 'UMR_One_to_One' if x['UMR_flag']==1 else('No-Pair' if x['len']==1 else 'Undecided'), axis=1)
    
    prediction_table['UMT_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMT_One_to_One' in x else 0)
    prediction_table['UMB_flag'] = prediction_table['Predicted_action'].apply(lambda x: 1 if 'UMB_One_to_One' in x else 0)
    
    prediction_table.loc[(prediction_table['UMT_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMT_One_to_One'
    prediction_table.loc[(prediction_table['UMB_flag']==1) & (prediction_table['len']==2),'Final_prediction']='UMB_One_to_One'
    
    prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['len']==2),'Final_prediction']='No-Pair/Unrecognized'
    
    prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMT_flag']==1),'Final_prediction']='UMT_One_to_One'
    
    prediction_table.loc[(prediction_table['Final_prediction']=='Undecided') & (prediction_table['UMB_flag']==1),'Final_prediction']='UMB_One_to_One'
    
    prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMR_One_to_One', 'Predicted_UMR_array']
    
    prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMT_One_to_One', 'Predicted_UMT_array']
    prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='UMB_One_to_One', 'Predicted_UMB_array']
    #prediction_table.loc[prediction_table['Final_prediction']=='No-Pair', 'Final_predicted_break'] = prediction_table.loc[prediction_table['Final_prediction']=='No-Pair', '']
    
    prediction_table['predicted_break_len'] = prediction_table['Final_predicted_break'].str.len()
    
    X_test['prob_key'] = X_test['SideB.ViewData.BreakID_B_side'].astype(str) + X_test['Predicted_action']
    prediction_table['prob_key'] = prediction_table['SideB.ViewData.BreakID_B_side'].astype(str) + prediction_table['Final_prediction']
    
    user_prob = X_test.groupby('prob_key')[['probability_UMR','probability_UMT','probability_UMB']].max().reset_index()
    open_prob = X_test.groupby('prob_key')['probability_No_pair'].mean().reset_index()
    
    #prediction_table = prediction_table.drop(,1)

    prediction_table = pd.merge(prediction_table,user_prob, on='prob_key', how='left')
    prediction_table = pd.merge(prediction_table,open_prob, on='prob_key', how='left')
    
    prediction_table = prediction_table.drop('prob_key',1)
    prediction_table = pd.merge(prediction_table, X_test[['SideB.ViewData.BreakID_B_side','SideA.ViewData._ID','SideB.ViewData._ID']].drop_duplicates(['SideB.ViewData.BreakID_B_side','SideB.ViewData._ID']), on ='SideB.ViewData.BreakID_B_side', how='left')
    
    #Merging with User Action Data
    prediction_table3 = prediction_table
    
    aua = pd.read_csv(filepaths_AUA[i])
    
    aua = aua[~((aua['LastPerformedAction']==0) & (aua['ViewData.Status']=='SDB'))]
    aua = aua.reset_index()
    aua = aua.drop('index',1)
    
    aua = aua[aua['ViewData.Status'].isin(['UMR','UMB','UMT','OB','SDB'])]
    aua = aua.reset_index()
    aua = aua.drop('index',1)
    
    aua_id_match = aua[['MetaData.0._ParentID','ViewData.Status','ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]

    aua_id_match.columns = ['SideB.ViewData._ID','Actual_Status','AUA_ViewData._ID','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']

    aua_id_match = aua_id_match.drop_duplicates()
    aua_id_match = aua_id_match.reset_index()
    aua_id_match = aua_id_match.drop('index',1)

    ########################################################################################################
    aua_open_status = aua[['ViewData.BreakID','ViewData.Status']]

    aua_open_status.columns = ['SideB.ViewData.BreakID_B_side','Actual_Status_Open']
    aua_open_status = aua_open_status.drop_duplicates()
    aua_open_status = aua_open_status.reset_index()
    aua_open_status = aua_open_status.drop('index',1)
    
    aua_open_status['SideB.ViewData.BreakID_B_side'] = aua_open_status['SideB.ViewData.BreakID_B_side'].astype(int).astype(str)
    prediction_table3['SideB.ViewData.BreakID_B_side'] = prediction_table3['SideB.ViewData.BreakID_B_side'].astype(int).astype(str)
    
    prediction_table3['SideB.ViewData._ID'] = prediction_table3['SideB.ViewData._ID'].fillna('Not_generated')
    prediction_table3['SideA.ViewData._ID'] = prediction_table3['SideA.ViewData._ID'].fillna('Not_generated')
    
    prediction_table_new = pd.merge(prediction_table3, aua_id_match, on='SideB.ViewData._ID', how='left')
    
    prediction_table_new = pd.merge(prediction_table_new ,aua_open_status, on='SideB.ViewData.BreakID_B_side', how='left')
    
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='No-Pair/Unrecognized','Final_prediction'] = 'No-Pair'
    
    prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),'Actual_Status'] = prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),'Actual_Status_Open']
    
    prediction_table_new.loc[~prediction_table_new['Actual_Status_Open'].isnull(),:]
    
    prediction_table_new.loc[prediction_table_new['Actual_Status']=='OB','Actual_Status'] = 'Open Break'
    
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='No-Pair','Final_prediction'] = 'Open Break'
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMR_One_to_One','Final_prediction'] = 'UMR'
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMT_One_to_One','Final_prediction'] = 'UMT'
    prediction_table_new.loc[prediction_table_new['Final_prediction']=='UMB_One_to_One','Final_prediction'] = 'UMB'
    
    prediction_table_new = prediction_table_new[~prediction_table_new['Actual_Status'].isnull()]
    prediction_table_new = prediction_table_new.reset_index()
    prediction_table_new = prediction_table_new.drop('index',1)
    
    meo = pd.read_csv(filepaths_MEO[i],usecols=new_cols)
    
    meo = meo[['ViewData.BreakID','ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Age','ViewData.Status']].drop_duplicates()
    
    meo['key'] = meo['ViewData.Side0_UniqueIds'].astype(str) + meo['ViewData.Side1_UniqueIds'].astype(str)
    
    aua_sub = aua[['ViewData.Side1_UniqueIds','ViewData.Side0_UniqueIds','ViewData.Age','ViewData.Status']].drop_duplicates()
    
    aua_sub['key'] = aua_sub['ViewData.Side0_UniqueIds'].astype(str) + aua_sub['ViewData.Side1_UniqueIds'].astype(str)
    
    prediction_table_new['ViewData.BreakID'] = prediction_table_new['SideB.ViewData.BreakID_B_side']
    prediction_table_new['ViewData.BreakID'] = prediction_table_new['ViewData.BreakID'].astype(str)

    meo['ViewData.BreakID'] = meo['ViewData.BreakID'].astype(str)
    
    prediction_table_new1 = pd.merge(prediction_table_new, meo[['ViewData.BreakID','key']], on='ViewData.BreakID', how='left')
    
    aua_sub1 = pd.merge(aua_sub, prediction_table_new1[['key','Final_prediction','probability_UMR','probability_No_pair','probability_UMT','probability_UMB','Final_predicted_break']], on='key', how='left')
    
    no_open = prediction_table_new1[prediction_table_new1['Final_prediction']!='Open Break'].reset_index()
    no_open = no_open.drop('index',1)

    no_open['key'] = no_open['ViewData.Side0_UniqueIds'].astype(str) + no_open['ViewData.Side1_UniqueIds'].astype(str)
    
    #aua_sub1[aua_sub1['Final_prediction']=='UMR_One_to_One']
    X_test['key'] = X_test['SideB.ViewData.Side0_UniqueIds'].astype(str) + X_test['SideA.ViewData.Side1_UniqueIds'].astype(str)

    aua_sub = pd.merge(aua_sub1, no_open[['key','Final_prediction']], on='key', how='left')
    
    aua_sub11 = aua_sub1[aua_sub1['Final_prediction']=='Open Break']
    aua_sub11 = aua_sub11.reset_index()
    aua_sub11 = aua_sub11.drop('index',1)
    
    aua_sub11['probability_UMR'].fillna(0.00355,inplace=True)
    aua_sub11['probability_UMB'].fillna(0.003124,inplace=True)
    aua_sub11['probability_UMT'].fillna(0.00255,inplace=True)
    aua_sub11['probability_No_pair'].fillna(0.99034,inplace=True)
    
    aua_sub22 = aua_sub1[aua_sub1['Final_prediction']!='Open Break'][['ViewData.Side1_UniqueIds', 'ViewData.Side0_UniqueIds', 'ViewData.Age','ViewData.Status', 'key']]

    aua_sub22 = aua_sub22.reset_index()
    aua_sub22 = aua_sub22.drop('index',1)
    aua_sub22 = pd.merge(aua_sub22, no_open[['key','Final_prediction','probability_UMR','probability_No_pair','probability_UMT','probability_UMB','Final_predicted_break']], on='key', how='left')
    aua_sub22 = aua_sub22.reset_index()
    aua_sub22 = aua_sub22.drop('index',1)
    
    aua_sub33 = pd.concat([aua_sub11,aua_sub22], axis=0)
    aua_sub33 = aua_sub33.reset_index()
    aua_sub33 = aua_sub33.drop('index',1)
    
    aua_sub33['ViewData.Side0_UniqueIds'] = aua_sub33['ViewData.Side0_UniqueIds'].astype(str)
    aua_sub33['ViewData.Side1_UniqueIds'] = aua_sub33['ViewData.Side1_UniqueIds'].astype(str)
    
    aua_sub33['len_side0'] = aua_sub33.apply(lambda x: len(x['ViewData.Side0_UniqueIds'].split(',')), axis=1)
    aua_sub33['len_side1'] = aua_sub33.apply(lambda x: len(x['ViewData.Side1_UniqueIds'].split(',')), axis=1)
    
    aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']==1) & (aua_sub33['ViewData.Status']=='OB') ,'Type'] = 'One_side_aggregation'
    aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']==1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'One_to_Many'

    aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']=='OB') ,'Type'] = 'One_side_aggregation'
    aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'One_to_Many'
    aua_sub33.loc[(aua_sub33['len_side0']>1) & (aua_sub33['len_side1']>1) & (aua_sub33['ViewData.Status']!='OB') ,'Type'] = 'Many_to_Many'

    aua_sub33.loc[(aua_sub33['len_side0']==1) & (aua_sub33['len_side1']==1) ,'Type'] = 'One_to_One/Open'
    
    aua_sub44 = aua_sub33[(aua_sub33['ViewData.Status']=='UMB') & (aua_sub33['ViewData.Age']>1)]
    aua_sub44 = aua_sub44.reset_index()
    aua_sub44 = aua_sub44.drop('index',1)
    
    aua_sub44['Final_prediction'].fillna('UMB-Carry-Forward',inplace= True)
    aua_sub44['probability_UMR'].fillna(0.0001,inplace= True)
    aua_sub44['probability_UMB'].fillna(0.9998,inplace= True)
    aua_sub44['probability_UMT'].fillna(0.0000,inplace= True)
    aua_sub44['probability_No_pair'].fillna(0.0000,inplace= True)

    aua_sub55 = aua_sub33[~((aua_sub33['ViewData.Status']=='UMB') & (aua_sub33['ViewData.Age']>1))]
    aua_sub55 = aua_sub55.reset_index()
    aua_sub55 = aua_sub55.drop('index',1)
    
    aua_sub66 = pd.concat([aua_sub55,aua_sub44], axis=0)
    aua_sub66 = aua_sub66.reset_index()
    aua_sub66 = aua_sub66.drop('index',1)
    
    aua_sub66.loc[(aua_sub66['ViewData.Status']=='UMB') & (aua_sub66['ViewData.Age']>1),'ViewData.Status'] = 'UMB-Carry-Forward'
    aua_sub66.loc[(aua_sub66['ViewData.Status']=='OB'),'ViewData.Status'] = 'Open Break'


    # Read no_pair_id file
    no_pair_id_data = pd.read_csv(filepaths_no_pair_id_data[i])
    
    no_pair_ids = no_pair_id_data['filter_key'].unique()
    
    aua_sub66.loc[aua_sub66['ViewData.Side1_UniqueIds'].isin(no_pair_ids),'Final_prediction'] = aua_sub66.loc[aua_sub66['ViewData.Side1_UniqueIds'].isin(no_pair_ids),'ViewData.Status']
    aua_sub66.loc[aua_sub66['ViewData.Side0_UniqueIds'].isin(no_pair_ids),'Final_prediction'] = aua_sub66.loc[aua_sub66['ViewData.Side0_UniqueIds'].isin(no_pair_ids),'ViewData.Status']
    
    pb_side_grp = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    pb_side_grp_status = X_test.groupby(['SideA.ViewData.Side1_UniqueIds'])['SideA.ViewData.Status'].unique().reset_index()
    pb_side_grp_status['SideA.ViewData.Status'] = pb_side_grp_status['SideA.ViewData.Status'].apply(lambda x: str(x).replace("[",""))
    pb_side_grp_status['SideA.ViewData.Status'] = pb_side_grp_status['SideA.ViewData.Status'].apply(lambda x: str(x).replace("]",""))
    pb_side_grp['len'] = pb_side_grp.apply(lambda x: len(x['Predicted_action']), axis=1)
    pb_side_grp['No_pair_flag'] = pb_side_grp.apply(lambda x: 1 if x['len'] == 1 and "No-Pair" in x['Predicted_action'] else 0, axis=1)
    
    pb_side_grp = pd.merge(pb_side_grp,pb_side_grp_status, on='SideA.ViewData.Side1_UniqueIds', how='left')
    
#    pb_side_grp = pd.merge(pb_side_grp,pb_side_grp_status, on='SideA.ViewData.Side1_UniqueIds', how='left')
    pb_side_grp['Final_status'] = pb_side_grp.apply(lambda x: "Open Break" if x['SideA.ViewData.Status']=="'OB'" else("SDB" if x['SideA.ViewData.Status']=="'SDB'" else "NA"),axis=1)
    pb_side_grp = pb_side_grp.rename(columns = {'SideA.ViewData.Side1_UniqueIds':'ViewData.Side1_UniqueIds'})

    pb_side_grp1 = pb_side_grp[pb_side_grp['No_pair_flag']==1]
    pb_side_grp1 = pb_side_grp1.reset_index()
    pb_side_grp1 = pb_side_grp1.drop('index',1)
    
    aua_sub77 = pd.merge(aua_sub66 ,pb_side_grp1[['ViewData.Side1_UniqueIds','Final_status']], on ='ViewData.Side1_UniqueIds',how='left')
    
    aua_sub77.loc[(~aua_sub77['Final_status'].isnull()) & (aua_sub77['ViewData.Side0_UniqueIds']=='nan'),'Final_prediction'] = aua_sub77.loc[(~aua_sub77['Final_status'].isnull()) & (aua_sub77['ViewData.Side0_UniqueIds']=='nan'),'Final_status']
    
    pb_side_grp_B = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['Predicted_action'].unique().reset_index()
    
    pb_side_grp_B_status = X_test.groupby(['SideB.ViewData.Side0_UniqueIds'])['SideB.ViewData.Status'].unique().reset_index()
    pb_side_grp_B_status['SideB.ViewData.Status'] = pb_side_grp_B_status['SideB.ViewData.Status'].apply(lambda x: str(x).replace("[",""))
    pb_side_grp_B_status['SideB.ViewData.Status'] = pb_side_grp_B_status['SideB.ViewData.Status'].apply(lambda x: str(x).replace("]",""))
    pb_side_grp_B['len'] = pb_side_grp_B.apply(lambda x: len(x['Predicted_action']), axis=1)
    pb_side_grp_B['No_pair_flag'] = pb_side_grp_B.apply(lambda x: 1 if x['len'] == 1 and "No-Pair" in x['Predicted_action'] else 0, axis=1)
    
    
    pb_side_grp_B = pd.merge(pb_side_grp_B,pb_side_grp_B_status, on='SideB.ViewData.Side0_UniqueIds', how='left')
    pb_side_grp_B['Final_status_B'] = pb_side_grp_B.apply(lambda x: "Open Break" if x['SideB.ViewData.Status']=="'OB'" else("SDB" if x['SideB.ViewData.Status']=="'SDB'" else "NA"),axis=1)
    pb_side_grp_B = pb_side_grp_B.rename(columns = {'SideB.ViewData.Side0_UniqueIds':'ViewData.Side0_UniqueIds'})



    pb_side_grp2 = pb_side_grp_B[pb_side_grp_B['No_pair_flag']==1]
    pb_side_grp2 = pb_side_grp2.reset_index()
    pb_side_grp2 = pb_side_grp2.drop('index',1)
    
    aua_sub88 = pd.merge(aua_sub77 ,pb_side_grp2[['ViewData.Side0_UniqueIds','Final_status_B']], on ='ViewData.Side0_UniqueIds',how='left')
    
    aua_sub88.loc[(~aua_sub88['Final_status_B'].isnull()) & (aua_sub88['ViewData.Side1_UniqueIds']=='nan'),'Final_prediction'] = aua_sub88.loc[(~aua_sub88['Final_status_B'].isnull()) & (aua_sub88['ViewData.Side1_UniqueIds']=='nan'),'Final_status_B']
    
    aua_sub99 = aua_sub88[(aua_sub88['ViewData.Status']!='SDB')]
    aua_sub99 = aua_sub99.reset_index()
    aua_sub99 = aua_sub99.drop('index',1)
    
    aua_sub99['Final_prediction'] = aua_sub99['Final_prediction'].fillna('Open Break')
    aua_sub99 = aua_sub99.reset_index()
    aua_sub99 = aua_sub99.drop('index',1)
    
    aua_sub99['ViewData.Status'] = aua_sub99['ViewData.Status'].astype(str)
    aua_sub99['Final_prediction'] = aua_sub99['Final_prediction'].astype(str)
    
    #Summary file
    
    break_id_merge = meo[meo['ViewData.Status'].isin(['OB','SDB','UOB','UDB','SPM'])][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.BreakID']].drop_duplicates()
    break_id_merge = break_id_merge.reset_index()
    break_id_merge = break_id_merge.drop('index',1)
    
    break_id_merge['key'] = break_id_merge['ViewData.Side0_UniqueIds'].astype(str) + break_id_merge['ViewData.Side1_UniqueIds'].astype(str)
    
    final = pd.merge(aua_sub99,break_id_merge[['key','ViewData.BreakID']], on='key', how='left')
    
    #final[final['ViewData.BreakID'].isnull()]

    final = pd.merge(final,break_id_merge[['ViewData.Side0_UniqueIds','ViewData.BreakID']], on='ViewData.Side0_UniqueIds', how='left')
    
    final.loc[final['ViewData.BreakID_x'].isnull(),'ViewData.BreakID_x'] = final.loc[final['ViewData.BreakID_x'].isnull(),'ViewData.BreakID_y']
    
    final = final.rename(columns={'ViewData.BreakID_x':'ViewData.BreakID'})
    final = final.drop('ViewData.BreakID_y',1)
    
    final1 = final[(final['Type']=='One_to_One/Open') & (final['probability_No_pair'].isnull())]
    final1 = final1.reset_index()
    final1 = final1.drop('index',1)


    final2 = final[~((final['Type']=='One_to_One/Open') & (final['probability_No_pair'].isnull()))]
    final2 = final2.reset_index()
    final2 = final2.drop('index',1)
    
    final1['probability_UMR'].fillna(0.0024,inplace=True)
    final1['probability_UMB'].fillna(0.004124,inplace=True)
    final1['probability_UMT'].fillna(0.00155,inplace=True)
    final1['probability_No_pair'].fillna(0.9922,inplace=True)
    
    final3 = pd.concat([final1, final2], axis=0)
    
    final3['ML_flag'] = final3.apply(lambda x: "ML" if x['Type']=='One_to_One/Open' else "Non-ML", axis=1)
    
    final4 = final3[prediction_cols]

    final4 = final4.rename(columns ={'ViewData.Status':'Actual_Status', 'Final_prediction': 'Predicted_Status'})
    
    print("Accuracy_table")
    print(classification_report(final4[final4['Type']=='One_to_One/Open']['Actual_Status'], final4[final4['Type']=='One_to_One/Open']['Predicted_Status']))
    
    report = classification_report(final4[final4['Type']=='One_to_One/Open']['Actual_Status'], final4[final4['Type']=='One_to_One/Open']['Predicted_Status'], output_dict=True)
    accuracy_table = pd.DataFrame(report).transpose()
    crosstab_table = pd.crosstab(final4[final4['Type']=='One_to_One/Open']['Actual_Status'], final4[final4['Type']=='One_to_One/Open']['Predicted_Status'])
    print("Crosstab Table")
    print(crosstab_table)
    ### Save Results
    final4.to_csv(filepaths_final_prediction_table[i])
    accuracy_table.to_csv(filepaths_accuracy_table[i])
    crosstab_table.to_csv(filepaths_crosstab_table[i])
#    i = i+1
    
    


#Bad Predictions
#
#Date 1
#UMB       1.00      0.43      0.60         7
#
#Date 2
#UMB       1.00      0.12      0.21        51
#
#Date 7
#UMB       1.00      0.44      0.62         9
#
#
#
#
#Good Predictions
#
#Date 3 All decent
#Date 4 All good except i UMT wrong
#Date 8 All Decent, UMB 66% right
#Date 9 Good,UMB 1 out 3 correct
#Date 10 Very Good, UMB 1 out 3 correct
#Date 11 Very Good, UMT 1 out of 1 wrong
#Date 14 Very Good
#Date 15 Very Good, UMB 1 out 3 correct
#Date 16, Decent UMB  3 wrong 1 correct
#Date 17 : 3rd Best
#
#Crosstab Table
#Predicted_Status   Open Break  UMB  UMB-Carry-Forward  UMR
#Actual_Status                                             
#Open Break                183    0                  0    0
#UMB                         2   73                  0    0
#UMB-Carry-Forward           0    0                141    0
#UMR                         0    0                  0   18
#
#Date 18 Very Good
#Date 21 Very good
#Date 22 All results perfect
#Date 23 Very Good, 2 out of 2 UMT wrong
#
#Date 24 : Good, 4 out of 10 UMRs went to Open Break. All else good, including UMB all right
#Date 25, Very Good
#Date 28, Good








    
    
    

    
    

    
