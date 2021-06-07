# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 12:35:58 2021

@author: consultant138
"""

import numpy as np
import pandas as pd
import datetime as dt


class Viteos_Generate_DB_Table_Class:

    
    def __init__(self, BreakID_col_name,
					   Final_predicted_break_col_name,
					   Side0_UniqueIds_col_name,
					   Side1_UniqueIds_col_name,
					   TaskID_col_name,
					   SourceCombinationCode_col_name,
				       BusinessDate_col_name,
					   SetupID_col_name,
					   ML_flag_col_name,
					   Predicted_Status_col_name,
					   Predicted_action_col_name,
					   PredictedComment_col_name,
					   PredictedCategory_col_name,
					   ActionType_col_name,
					   ActionTypeCode_col_name,
					   probability_No_pair_col_name,
					   probability_UMB_col_name,
					   probability_UMR_col_name,
					   probability_UMT_col_name
					   ):
		
        self.cols_in_db = ['BreakID',
					  'Final_predicted_break',
					  'Side0_UniqueIds',
					  'Side1_UniqueIds',
					  'TaskID',
					  'SourceCombinationCode',
				      'BusinessDate',
					  'SetupID',
					  'ML_flag',
					  'Predicted_Status',
					  'Predicted_action',
					  'PredictedComment'
					  'PredictedCategory',
					  'ActionType',
					  'ActionTypeCode'
					  'probability_No_pair',
					  'probability_UMB',
					  'probability_UMR',
					  'probability_UMT'
					  ]
        		


    final_umb_ob_table_copy = pd.merge(final_umb_ob_table,meo_df[['ViewData.BreakID','ViewData.Task ID','ViewData.Task Business Date','ViewData.Source Combination Code']].drop_duplicates(), left_on = 'BreakID_OB',right_on = 'ViewData.BreakID', how='left')
    final_umb_ob_table_copy.drop('ViewData.BreakID', axis = 1, inplace = True)
    
    final_umb_ob_table_copy['Predicted_Status'] = 'UMR'
    final_umb_ob_table_copy['Predicted_action'] = 'UMR_One-Many_to_Many-One'
    final_umb_ob_table_copy['ML_flag'] = 'ML'
    final_umb_ob_table_copy['SetupID'] = setup_code 
    final_umb_ob_table_copy['ViewData.Task Business Date'] = pd.to_datetime(final_umb_ob_table_copy['ViewData.Task Business Date'])
    final_umb_ob_table_copy['ViewData.Task Business Date'] = final_umb_ob_table_copy['ViewData.Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
    final_umb_ob_table_copy['ViewData.Task Business Date'] = pd.to_datetime(final_umb_ob_table_copy['ViewData.Task Business Date'])
    final_umb_ob_table_copy = make_Side0_Side1_columns_for_final_smb_or_umb_ob_table(final_umb_ob_table_copy,meo_df,'UMB')
    final_umb_ob_table_copy['probability_No_pair'] = ''
    final_umb_ob_table_copy['probability_UMB'] = ''
    final_umb_ob_table_copy['probability_UMR'] = ''
    final_umb_ob_table_copy['probability_UMT'] = ''
    final_umb_ob_table_copy['PredictedComment'] = ''
    final_umb_ob_table_copy['PredictedCategory'] = ''
    columns_rename_for_umb_ob_table_dict = {'BreakID_OB' : 'BreakID',
                                       'BreakID_UMB' : 'Final_predicted_break',
                                       'ViewData.Task ID' : 'TaskID',
                                       'ViewData.Task Business Date' : 'BusinessDate',
                                       'ViewData.Source Combination Code' : 'SourceCombinationCode'
                                       }
    final_umb_ob_table_copy.rename(columns = columns_rename_for_umb_ob_table_dict, inplace = True)
    filepaths_final_umb_ob_table_copy = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\final_umb_ob_table_copy_setup_' + setup_code + '_date_' + str(date_i) + '.csv'

    final_umb_ob_table_copy.to_csv(filepaths_final_umb_ob_table_copy)


else:
    final_umb_ob_table_copy = pd.DataFrame()
