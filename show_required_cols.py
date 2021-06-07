# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 12:42:49 2020

@author: consultant138
"""
import pandas as pd

cols_to_show = [
'Account Type',
'Accounting Net Amount',
#'Accounting Net Amount Difference',
'Activity Code',
'Age',
'Alt ID 1',
'Asset Type Category',
'Bloomberg_Yellow_Key',
'B-P Net Amount',
#'B-P Net Amount Difference',
#'B-P Net Amount Difference Absolute',
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
add = ['ViewData.Side0_UniqueIds', 'ViewData.Side1_UniqueIds']
viewdata_cols_to_show = ['ViewData.' + x for x in cols_to_show] + add

meo = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/MEO/MeoCollections.MEO_HST_RecData_125_2020-06-1.csv',usecols = viewdata_cols_to_show)
aua = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/AUA/AUACollections.AUA_HST_RecData_125_2020-06-1.csv',usecols = viewdata_cols_to_show)

final_predictions = pd.read_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Final_Predictions_Table_HST_RecData_125_2020-06-1.csv')


final_predictions.groupby(['Actual_Status'])['Predicted_Status'].value_counts()
final_predictions[(final_predictions['Actual_Status'] == 'Open Break') & (final_predictions['Predicted_Status'] == 'UMR')][['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds']]

final_predictions.groupby(['ViewData.Side0_UniqueIds'])['ViewData.Side1_UniqueIds'].value_counts()


final_predictions_both_present = final_predictions[(final_predictions['ViewData.Side0_UniqueIds'].notnull()) & (final_predictions['ViewData.Side1_UniqueIds'].notnull())]
final_predictions_side0_only = final_predictions[(final_predictions['ViewData.Side0_UniqueIds'].notnull()) & (final_predictions['ViewData.Side1_UniqueIds'].isnull())]
final_predictions_side1_only = final_predictions[(final_predictions['ViewData.Side0_UniqueIds'].isnull()) & (final_predictions['ViewData.Side1_UniqueIds'].notnull())]
final_predictions_both_null = final_predictions[(final_predictions['ViewData.Side0_UniqueIds'].isnull()) & (final_predictions['ViewData.Side1_UniqueIds'].isnull())]

final_predictions_both_present_aua_merge = pd.merge(final_predictions_both_present,aua, on=['ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds'], how='left' )
final_predictions_side0_only_aua_merge = pd.merge(final_predictions_side0_only,aua, on='ViewData.Side0_UniqueIds', how='left' )
final_predictions_side1_only_aua_merge = pd.merge(final_predictions_side1_only,aua, on='ViewData.Side1_UniqueIds', how='left' )

final_prediction_show_cols = final_predictions_both_present_aua_merge.append([final_predictions_side0_only_aua_merge,final_predictions_side1_only_aua_merge])
final_prediction_show_cols.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/test_show_cols.csv')
