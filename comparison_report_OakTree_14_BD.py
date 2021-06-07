# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 10:02:26 2020

@author: consultant138
"""

import pandas as pd
final_df_2 = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\OakTree\\final_df_2_setup_379_date_2020-10-14.csv')

#Ronson Observation 1
BreakIds_mentioned_obs_1 = [\
                            1553354606,\
                            1489620754,\
                            1553435277,\
                            1548457408,\
                            1553435284,\
                            1548457411,\
                            1553435280,\
                            1537794234,\
                            1553435282,\
                            1550485118,\
                            1553435276,\
                            1550485112,\
                            1553782803,\
                            1551035201]

cols_to_show_in_mail = [ 'BreakID', 'BusinessDate', 'Final_predicted_break','Predicted_Status','Predicted_action']
final_df_2[final_df_2['BreakID'].isin(BreakIds_mentioned_obs_1)][cols_to_show_in_mail].to_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Coparison_Report\\Ronson_obs_1_14_BD.csv')

#Ronson Observation 2
BreakIds_mentioned_obs_2 = [1553782829,\
                            1553782814]
final_df_2[final_df_2['BreakID'].isin(BreakIds_mentioned_obs_2)][cols_to_show_in_mail].to_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Coparison_Report\\Ronson_obs_2_14_BD.csv')

#Ronson Observation 3
BreakIds_mentioned_obs_3 = [1528038817,\
                            1528039080,\
                            1528039092,\
                            1535721717,\
                            1548139052,\
                            1551035177,\
                            1551035202,\
                            1551035208,\
                            1551035212,\
                            1551035208,\
                            1553354067,\
                            1551035208,\
                            1553369474,\
                            1553369481,\
                            1553369486,\
                            1553369489,\
                            1553369496,\
                            1553369497,\
                            1553369498,\
                            1553420139,\
                            1553420144,\
                            1553420146\
                            ]

final_df_2[final_df_2['BreakID'].isin(BreakIds_mentioned_obs_3)][cols_to_show_in_mail].to_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Coparison_Report\\Ronson_obs_3_14_BD.csv')
