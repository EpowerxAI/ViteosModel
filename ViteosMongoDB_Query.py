# -*- coding: utf-8 -*-
"""
Created on Sat Feb 20 12:15:23 2021

@author: consultant138
"""
import os
os.chdir('D:\\ViteosModel')
from pandas.io.json import json_normalize
import pandas as pd
import datetime as dt
from Viteos_Miscellaneous_Functions import contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status,reset_index_for_df
import Viteos_Miscellaneous_Functions
import dateutil
from dateutil.parser import parse
from datetime import datetime,timedelta

data_projection_meo_columns = {"DataSides" : 1,
                               "BreakID" : 1,
                               "LastPerformedAction" : 1,
                               "TaskInstanceID" : 1,
                               "SourceCombinationCode" : 1,
                               "MetaData" : 1, 
                               "ViewData" : 1
                               }

task_projection_all_columns = {'_createdBy' : 1,
                              '_updatedBy' : 1,
                              '_version' : 1,
                              '_createdAt' : 1,
                              '_updatedAt' : 1,
                              '_IPAddress' : 1,
                              '_MACAddress' : 1,
                              'RequestId' : 1,
                              'InstanceID' : 1,
                              'SourceCombinationCode' : 1,
                              'ReconSetupId' : 1,
                              'ReconSetupCode' : 1,
                              'ReconSetupForTask' : 1,
                              'KnowledgeDate' : 1,
                              'BusinessDate' : 1,
                              'RunDate' : 1,
                              'Frequency' : 1,
                              'RecType' : 1,
                              'SourceDataMappings' : 1,
                              'SourceCombination' : 1,
                              'SourceDataOperations' : 1,
                              'PreAcctMapSourceDataOperations' : 1,
                              'AccountMappings' : 1,
                              'DBFileName' : 1,
                              'ErrorCode' : 1,
                              'Status' : 1,
                              'ErrorMessage' : 1,
                              'FileLoadStatus' : 1,
                              'DataPreparationStatus' : 1,
                              'ReconRunStatus' : 1,                                                                  
                              'BreakManagementStatus' : 1,
                              'PublishStatus' : 1,
                              'FrequencyType' : 1,
                              'IsUndone' : 1,
                              'IsCashRec' : 1,
                              'IsOTERec' : 1,
                              'IsMigrationTask' : 1,
                              'ParentTaskId' : 1,
                              'IsFirstSourceCombination' : 1,
                              'IsIncrementalRec' : 1,
                              'IsManualRun' : 1,
                              'ETLInfo' : 1,
                              'Labels' : 1,
                              'OTEDetails' : 1,
                              'PublishData' : 1,
                              'HostName' : 1,
                              'ProcessID' : 1}

data_projection_all_columns = {'_createdBy' : 1,
                               '_updatedBy' : 1,
                               '_version' : 1,
                               '_createdAt' : 1,
                               '_updatedAt' : 1,
                               '_isLocked' : 1,
                               '_IPAddress' : 1,
                               '_MACAddress' : 1,
                               'DataSides' : 1,
                               'MetaData' : 1,
                               'MatchStatus' : 1,
                               'Priority' : 1,
                               'SystemComments' : 1,
                               'BreakID' : 1,
                               'CombiningData' : 1,
                               'ClusterID' : 1,
                               'SPMID' : 1,
                               'KeySet' : 1,
                               'RuleName' : 1,
                               'Age' : 1,
                               'InternalComment1' : 1,
                               'InternalComment2' : 1,
                               'InternalComment3' : 1,
                               'ExternalComment1' : 1,
                               'ExternalComment2' : 1,
                               'ExternalComment3' : 1,
                               'Differences' : 1,
                               'TaskInstanceID' : 1,
                               'ReviewData' : 1,
                               'PublishData' : 1,
                               'AssigmentData' : 1,
                               'SourceCombinationCode' : 1,
                               'LinkedBreaks' : 1,
                               'WorkflowData' : 1,
                               'LastPerformedAction' : 1,
                               'AttributeTolerance' : 1,
                               'Attachments' : 1,
                               'ViewData' : 1,
                               'Age2' : 1,
                               'IsManualActionUploadData' : 1,
                               'IsManualBreakUploadData' : 1,
                               '_parentID' : 1}

class ViteosMongoDB_Query_Class:
	
	def __init__(self,param_remove_statuses_from_data = ['SMT','HST', 'OC', 'CT', 'Archive','SMR','SPM'], param_setup_code = None, param_TaskID_server = None, param_Meo_data_server = None):
		self.remove_statuses_from_data = param_remove_statuses_from_data
		self.setup_code = param_setup_code
		self.TaskID_server = param_TaskID_server
		self.Meo_data_server = param_Meo_data_server
		return None

	def getDateTimeFromISO8601String(self,s):
	    d = dateutil.parser.parse(s)
	    return d

	def normalize_list_of_dicts_from_mongodb_query(self,param_list_of_dicts_from_query):
		if(len(param_list_of_dicts_from_query) != 0):
			fun_return_df = json_normalize(param_list_of_dicts_from_query)
			fun_return_df = fun_return_df.loc[:,fun_return_df.columns.str.startswith('ViewData')]
		else:
			fun_return_df = pd.DataFrame()
		return(fun_return_df)
	
	def change_date_col_to_iso_format(self,param_df, param_date_col_name = 'ViewData.Task Business Date'):
		if(str(param_date_col_name) in list(param_df.columns)):
			param_df[param_date_col_name] = param_df[param_date_col_name].apply(dt.datetime.isoformat) 
		else:
			print(str(param_date_col_name) + ' not present in dataframe')
		return(param_df)
	
	def preprocess_extracted_data(self, param_df, param_status_col_name = 'ViewData.Status', param_remove_or_keep_for_multiple_uniqueids_in_ob_issue = True,param_create_date_col = True):
		if(param_df.shape[0] != 0):
			return_df = param_df.drop_duplicates(keep=False)
			return_df = return_df[~return_df[param_status_col_name].isin(self.remove_statuses_from_data)]
			if(param_remove_or_keep_for_multiple_uniqueids_in_ob_issue == True):
				return_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] = return_df.apply(lambda row : contains_multiple_values_in_either_Side_0_or_1_UniqueIds_for_expected_single_sided_status(param_row = row), axis = 1,result_type="expand")
				return_df = return_df[~(return_df['remove_or_keep_for_multiple_uniqueids_in_ob_issue'] == 'remove')]
				return_df = reset_index_for_df(param_df = return_df)
				
			if(param_create_date_col == True):
				return_df['Date'] = pd.to_datetime(return_df['ViewData.Task Business Date'])
				return_df = reset_index_for_df(param_df = return_df)
				return_df['Date'] = pd.to_datetime(return_df['Date']).dt.date
				return_df['Date'] = return_df['Date'].astype(str)
			
		else:
			return_df = pd.DataFrame()

		return(return_df)

	def task_id_list(self,param_db_name, param_colleciton_name, param_BD, param_setupCode):
		query_for_Task_colleciton = param_db_name[param_colleciton_name].find({ "BusinessDate": param_BD,"ReconSetupCode": param_setupCode},task_projection_all_columns)
		list_of_dicts_for_Task_collection_for_penultimte_date = list(query_for_Task_colleciton)
		list_instance_ids_for_penultimte_date = [list_of_dicts_for_Task_collection_for_penultimte_date[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_for_Task_collection_for_penultimte_date))]
	
		return(list_instance_ids_for_penultimte_date)


#	def get_past_n_days_meo_df(self,param_number_of_days_to_go_behind, param_str_date_in_ddmmyyyy_format, param_mongodb_server_to_get_taskid_from = self.param_TaskID_collection, param_collection_to_get_taskid_from = self.TaskID_collection):
	def get_past_n_days_meo_df(self,param_number_of_days_to_go_behind, param_str_date_in_ddmmyyyy_format, param_mongodb_server_to_get_taskid_from = None, param_collection_to_get_taskid_from = None,param_mongodb_server_to_get_meo_from = None):		
		num_range = range(1,param_number_of_days_to_go_behind+1)
		num_list = list(num_range)
		penultimate_date_to_analyze_ymd_format,penultimate_date_to_analyze_ymd_iso_18_30_format,penultimate_date_to_analyze_ymd_iso_00_00_format = Viteos_Miscellaneous_Functions.date_various_format(param_str_date_in_ddmmyyyy_format = str(parse(param_str_date_in_ddmmyyyy_format, dayfirst = True) - timedelta(days = 1))[0:10])

		penultimate_dates_ymd_iso_18_30_format_list = [parse(penultimate_date_to_analyze_ymd_iso_18_30_format) - timedelta(days = x) for x in num_list]

		days_got_data_for = 1
		all_list_instance_ids_for_penultimate_date = []
			
		while (days_got_data_for <= param_number_of_days_to_go_behind):
			for penultimate_date in penultimate_dates_ymd_iso_18_30_format_list:

				if(param_mongodb_server_to_get_taskid_from is not None):
					list_instance_ids_for_penultimte_date = self.task_id_list(param_db_name = param_mongodb_server_to_get_taskid_from, param_colleciton_name = param_collection_to_get_taskid_from, param_BD = penultimate_date, param_setupCode = self.setup_code)
				else:
					list_instance_ids_for_penultimte_date = self.task_id_list(param_db_name = self.TaskID_server, param_colleciton_name = param_collection_to_get_taskid_from, param_BD = penultimate_date, param_setupCode = self.setup_code)

				if(len(list_instance_ids_for_penultimte_date) != 0):
					days_got_data_for = days_got_data_for + 1
					all_list_instance_ids_for_penultimate_date.append(list_instance_ids_for_penultimte_date)
				else:
					days_got_data_for = days_got_data_for
				
		all_list_instance_ids_for_penultimate_date_flattened = Viteos_Miscellaneous_Functions.flatten_list(param_2d_list = all_list_instance_ids_for_penultimate_date)
		if(param_mongodb_server_to_get_meo_from is not None):
			query_for_appended_MEO_data = param_mongodb_server_to_get_meo_from['RecData_' + self.setup_code + '_Historic'].find({ 'TaskInstanceID': { '$in': all_list_instance_ids_for_penultimate_date_flattened}},data_projection_all_columns)

		else:
			query_for_appended_MEO_data = self.Meo_data_server['RecData_' + self.setup_code + '_Historic'].find({ 'TaskInstanceID': { '$in': all_list_instance_ids_for_penultimate_date_flattened}},data_projection_all_columns)

		list_of_dicts_query_for_appended_MEO_data = list(query_for_appended_MEO_data)
		appended_meo_df = self.normalize_list_of_dicts_from_mongodb_query(param_list_of_dicts_from_query = list_of_dicts_query_for_appended_MEO_data)	
		appended_meo_df = self.change_date_col_to_iso_format(param_df = appended_meo_df, param_date_col_name = 'ViewData.Task Business Date')
		appended_meo_df = self.preprocess_extracted_data(param_df = appended_meo_df, param_status_col_name = 'ViewData.Status', param_remove_or_keep_for_multiple_uniqueids_in_ob_issue = True,param_create_date_col = True)
		return(appended_meo_df)						

				