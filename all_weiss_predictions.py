# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 18:51:51 2020

@author: consultant138
"""
# 125
import pandas as pd
date_numbers_list = [1,3,4,
                     7,8,9,10,11,
                     14,15,16,17,18,
                     21,22,23,24,25,
                     28,29]
filepaths_final_prediction_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Final_Predictions_Table_HST_RecData_125_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
df_all = pd.DataFrame()
i = 0
for i in range(0,len(date_numbers_list)):
    if (i != len(date_numbers_list) - 1):
        df_pred_i = pd.read_csv(filepaths_final_prediction_table[i])
        df_all = df_all.append(df_pred_i)
    
df_all.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/All_June_predictions_125.csv')

#123   
import pandas as pd
date_numbers_list = [1,3,4,
                     7,8,9,10,11,
                     14,15,16,17,18,
                     21,22,23,24,25,
                     28,29]
filepaths_final_prediction_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/Final_Predictions_Table_HST_RecData_123_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
df_all = pd.DataFrame()
i = 0
for i in range(0,len(date_numbers_list)):
    if (i != len(date_numbers_list) - 1):
        df_pred_i = pd.read_csv(filepaths_final_prediction_table[i])
        df_all = df_all.append(df_pred_i)
    
df_all.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/JuneData/Final_Predictions/All_June_predictions_123.csv')

 