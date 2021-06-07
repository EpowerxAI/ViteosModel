# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 18:51:51 2020

@author: consultant138
"""
# 153
import pandas as pd
#date_numbers_list = [1,3,4,
#                     7,8,9,10,11,
#                     14,15,16,17,18,
#                     21,22,23,24,25,
#                     28,29]
date_numbers_list = [1,2,3,4,
                     7,8,9,10,11,
                     14,15,16,17,18,
                     21,22,23,24,25,
                     29,30]

filepaths_final_prediction_table = ['//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/Final_Predictions_205/Final_Predictions_Table_HST_RecData_205_2020-06-' + str(date_numbers_list[i]) + '.csv' for i in range(0,len(date_numbers_list))]
df_all = pd.DataFrame()
i = 0
for i in range(0,len(date_numbers_list)):
    if (i != len(date_numbers_list) - 1):
        df_pred_i = pd.read_csv(filepaths_final_prediction_table[i])
        df_all = df_all.append(df_pred_i)
    
df_all.to_csv('//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Soros/JuneData/Final_Predictions_205/All_June_predictions_205.csv')

