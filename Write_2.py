# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 13:45:20 2021

@author: consultant138
"""

import pandas as pd

class Write_Class:
	dict_columns_expected_in_db_with_datatype = {'BreakID' : 'str',
											     'Final_predicted_break' : 'str',
												 'Predicted_Status' : 'str',
												 'Predicted_action' : 'str',
												 'ML_flag' : 'str',
												 'SetupID' : 'str',
                                                 'Final_predicted_break' : 'str',
												 'PredictedComment' : 'str',
												 'PredictedCategory' : 'str',
												 'probability_UMB' : 'str',
												 'probability_No_pair' : 'str',
												 'probability_UMR' : 'str',
												 'TaskID' : 'int64',
												 
    fun_df['PredictedComment'] = ''
    fun_df['PredictedCategory'] = ''
    fun_df['probability_UMB'] = ''
    fun_df['probability_No_pair'] = ''
    fun_df['probability_UMR'] = ''
	def __init__(self, param_columns_present_in_df):
		return 0
	
    def final_structure(param_df : DataFrame) -> DataFrame :      
		columns_for_updation = ['ViewData.BreakID','ViewData.Task Business Date','ViewData.Side0_UniqueIds','ViewData.Side1_UniqueIds','ViewData.Source Combination Code','ViewData.Task ID','ViewData.Accounting Net Amount','ViewData.B-P Net Amount','ViewData.Transaction Type']
		if(fun_df.shape[0] != 0):
		    fun_df = closed_df[closed_columns_for_updation]
		    fun_df['Predicted_Status'] = 'UCB'
		    fun_df['Predicted_action'] = 'Closed'
		    fun_df['ML_flag'] = 'ML'
		    fun_df['SetupID'] = setup_code 
		    fun_df['Final_predicted_break'] = ''
		    fun_df['PredictedComment'] = ''
		    fun_df['PredictedCategory'] = ''
		    fun_df['probability_UMB'] = ''
		    fun_df['probability_No_pair'] = ''
		    fun_df['probability_UMR'] = ''
		    
		    fun_df[closed_columns_for_updation] = fun_df[closed_columns_for_updation].astype(str)
		    change_names_of_fun_df_mapping_dict = {
		                                                'ViewData.Side0_UniqueIds' : 'Side0_UniqueIds',
		                                                'ViewData.Side1_UniqueIds' : 'Side1_UniqueIds',
		                                                'ViewData.BreakID' : 'BreakID',
		                                                'ViewData.Task ID' : 'TaskID',
		                                                'ViewData.Task Business Date' : 'BusinessDate',
		                                                'ViewData.Source Combination Code' : 'SourceCombinationCode'
		                                            }
		    
		    fun_df.rename(columns = change_names_of_fun_df_mapping_dict, inplace = True)
		    
		    fun_df['BusinessDate'] = pd.to_datetime(fun_df['BusinessDate'])
		    fun_df['BusinessDate'] = fun_df['BusinessDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
		    fun_df['BusinessDate'] = pd.to_datetime(fun_df['BusinessDate'])
		    
		    fun_df['Side0_UniqueIds'] = fun_df['Side0_UniqueIds'].astype(str)
		    fun_df['Side1_UniqueIds'] = fun_df['Side1_UniqueIds'].astype(str)
		    fun_df['Final_predicted_break'] = fun_df['Final_predicted_break'].astype(str)
		    fun_df['Predicted_action'] = fun_df['Predicted_action'].astype(str)
		    fun_df['probability_No_pair'] = fun_df['probability_No_pair'].astype(str)
		    fun_df['probability_UMB'] = fun_df['probability_UMB'].astype(str)
		    fun_df['probability_UMR'] = fun_df['probability_UMR'].astype(str)
		    fun_df['SourceCombinationCode'] = fun_df['SourceCombinationCode'].astype(str)
		    fun_df['Predicted_Status'] = fun_df['Predicted_Status'].astype(str)
		    fun_df['ML_flag'] = fun_df['ML_flag'].astype(str)
		    
		    
		    fun_df[['BreakID', 'TaskID']] = fun_df[['BreakID', 'TaskID']].astype(float)
		    fun_df[['BreakID', 'TaskID']] = fun_df[['BreakID', 'TaskID']].astype(np.int64)
		    
		    fun_df[['SetupID']] = fun_df[['SetupID']].astype(int)
		    #filepaths_fun_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\fun_df.csv'
		    filepaths_fun_df = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\fun_df_setup_' + setup_code + '_date_' + str(date_i) + '_3.csv'
		    fun_df.to_csv(filepaths_fun_df)
		else:
		    fun_df = pd.DataFrame()

def date_col_change_to_datetime(param_df : DataFrame, param_col_name):
fun_df['Task Business Date'] = pd.to_datetime(fun_df['Task Business Date'])
fun_df['Task Business Date'] = fun_df['Task Business Date'].fillna(fun_df['Task Business Date'].mode()[0])
fun_df['Task Business Date'] = fun_df['Task Business Date'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%SZ'))
fun_df['Task Business Date'] = pd.to_datetime(fun_df['Task Business Date'])
