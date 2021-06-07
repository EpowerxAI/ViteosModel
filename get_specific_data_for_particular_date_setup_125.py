# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 21:32:16 2020

@author: consultant138
"""
import os
print(os.getcwd())
os.chdir('D:\\ViteosModel')
print(os.getcwd())


import numpy as np
import pandas as pd
#from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report
from tqdm import tqdm
import pickle
import sys
import os

   
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import pprint
import json

import dateutil.parser

read_TaskID_from_final_df_2 = 'no'
read_TaskID_from_db = 'yes'

setup_code_input_list = ['249']
setup_code = '249'
#setup_code_input_list = ['123']
# connection 1
mngdb_obj_1_for_instance_ids = mngdb(param_without_ssh = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb01.viteos.com', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
mngdb_obj_1_for_instance_ids.connect_with_or_without_ssh()
db_1_for_instance_ids = mngdb_obj_1_for_instance_ids.client['ReconDB']

# connection 2

mngdb_obj_2_for_data_in_list_instance_ids = mngdb(param_without_ssh = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb05.viteos.com', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
mngdb_obj_2_for_data_in_list_instance_ids.connect_with_or_without_ssh()
db_2_for_data_in_list_instance_ids = mngdb_obj_2_for_data_in_list_instance_ids.client['ReconDB']

mngdb_obj_3_for_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_3_for_writing_in_uat_server.connect_with_or_without_ssh()
extract_for_meo_or_aua = 'meo'

db_3_for_writing_in_ml_server = mngdb_obj_3_for_writing_in_uat_server.client['ReconDB_ML_Testing']
#db_4_for_MEO_data = mngdb_obj_3_for_writing_in_uat_server.client['MeoCollections']
#db_4_for_MEO_data = mngdb_obj_3_for_writing_in_uat_server.client['ReconDB_ML']
#db_5_for_AUA_data = mngdb_obj_3_for_writing_in_uat_server.client['AUACollections']
#db_6_for_prediction = mngdb_obj_3_for_writing_in_uat_server.client['MLPrediction_Cash']



coll_1_for_instance_ids = db_1_for_instance_ids['Tasks']
coll_2_for_data_in_list_instance_ids = db_2_for_data_in_list_instance_ids['HST_RecData_' + setup_code]


def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

date_input = '2020-12-16'
#date_input_iso = date_input + 'T18:30:00.000+0000'
#
#
#query_for_instance_id_setup_and_date = coll_1_for_instance_ids.find({ 
#           "ReconSetupCode" : setup_code, 
##                   "BusinessDate": {"$gte" : datetime(2020, 6, 24), "$lt": datetime(2015, 6, 26)},
#           "BusinessDate" : getDateTimeFromISO8601String(date_input_iso), 
#           "SourceCombinationCode" : {"$ne" : "UNMAPPED"}, 
#           "IsUndone" : False, 
#           "Status" : "Completed"
#           }, 
#           { "InstanceID" : 1}
#           ) 
#
#list_of_dicts_instance_ids = list(query_for_instance_id_setup_and_date)
#list_instance_ids = [list_of_dicts_instance_ids[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_instance_ids))]
if(read_TaskID_from_final_df_2 == 'yes'):
	filepath_full = '\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\final_df_2_setup_125_date_2020-10-28.csv'
	final_df_2 = pd.read_csv(filepath_full)
	list_instance_ids = final_df_2['TaskID'].unique().tolist()

if(read_TaskID_from_db == 'yes'):
	date_input_iso = date_input + 'T18:30:00.000+0000'
	        
	query_for_instance_id_setup_and_date = coll_1_for_instance_ids.find({ 
	                   "ReconSetupCode" : setup_code, 
	#                   "BusinessDate": {"$gte" : datetime(2020, 6, 24), "$lt": datetime(2015, 6, 26)},
	                   "BusinessDate" : getDateTimeFromISO8601String(date_input_iso), 
	                   "SourceCombinationCode" : {"$ne" : "UNMAPPED"}, 
	                   "IsUndone" : False, 
	                   "Status" : "Completed"
	                   }, 
	                   { "InstanceID" : 1}
	                   ) 
	#        df_instance_ids = json_normalize(query_for_instance_id_setup_and_date)
	#        list_instance_ids = list(df_instance_ids['InstanceID'])
	list_of_dicts_instance_ids = list(query_for_instance_id_setup_and_date)
	list_instance_ids = [list_of_dicts_instance_ids[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_instance_ids))]


if(len(list_instance_ids) != 0):
    query_2_for_data_in_list_instance_ids = coll_2_for_data_in_list_instance_ids.find({ 
                "ViewData": { "$ne": None },
                "ViewData.Status": { "$nin": ["HST","Archive","OC"] },
                "TaskInstanceID" : {"$in" : list_instance_ids}
               },{ 
                "DataSides" : 1,
                "BreakID" : 1,
                "LastPerformedAction" : 1,
                "TaskInstanceID" : 1,
                "SourceCombinationCode" : 1,
                "MetaData" : 1, 
                "ViewData" : 1
                             })
    #df_query_result_2 = json_normalize(query2)

list_of_dicts_query_result_2 = list(query_2_for_data_in_list_instance_ids)
if(len(list_of_dicts_query_result_2) != 0):
	if(extract_for_meo_or_aua == 'meo'):

	    coll_3_for_writing_in_ml_server = db_3_for_writing_in_ml_server['MEO_HST_RecData_' + setup_code + '_' + date_input]
	#    coll_3_for_writing_in_ml_server.insert_many(list_of_dicts_query_result_2)
	    query_4_for_data = coll_3_for_writing_in_ml_server.find({ 
	#                                                                         "LastPerformedAction": {"$ne" : 31} #AUA
	                                                                         "LastPerformedAction": 31 #MEO
	                                    
	                                                                 },
	                                                                 {
	                                                                         "DataSides" : 1,
	                                                                         "BreakID" : 1,
	                                                                         "LastPerformedAction" : 1,
	                                                                         "TaskInstanceID" : 1,
	                                                                         "SourceCombinationCode" : 1,
	                                                                         "MetaData" : 1, 
	                                                                         "ViewData" : 1
	                                                                 })
	    list_of_dicts_query_result_4 = list(query_4_for_data)
		meo_df = json_normalize(list_of_dicts_query_result_4)
		meo_df = meo_df.loc[:,meo_df.columns.str.startswith('ViewData')]
		
		meo_df.to_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\meo_df_setup_' + setup_code + '_date_' + date_input + '.csv')

	elif(extract_for_meo_or_aua == 'aua'):

	    coll_3_for_writing_in_ml_server = db_3_for_writing_in_ml_server['AUA_HST_RecData_' + setup_code + '_' + date_input]
	#    coll_3_for_writing_in_ml_server.insert_many(list_of_dicts_query_result_2)
	    query_4_for_data = coll_3_for_writing_in_ml_server.find({ 
	                                                                         "LastPerformedAction": {"$ne" : 31} #AUA
#	                                                                         "LastPerformedAction": 31 #MEO
	                                    
	                                                                 },
	                                                                 {
	                                                                         "DataSides" : 1,
	                                                                         "BreakID" : 1,
	                                                                         "LastPerformedAction" : 1,
	                                                                         "TaskInstanceID" : 1,
	                                                                         "SourceCombinationCode" : 1,
	                                                                         "MetaData" : 1, 
	                                                                         "ViewData" : 1
	                                                                 })
	    list_of_dicts_query_result_4 = list(query_4_for_data)
		aua_df = json_normalize(list_of_dicts_query_result_4)
		aua_df = aua_df.loc[:,aua_df.columns.str.startswith('ViewData')]
		
		aua_df.to_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\' + client + '\\aua_df_setup_' + setup_code + '_date_' + date_input + '.csv')













meo_df = pd.read_csv('\\\\vitblrdevcons01\\Raman  Strategy ML 2.0\\All_Data\\Weiss\\meo_df_setup_125_date_2020_10_23.csv')

def get_status_for_breakid(fun_breakid, fun_meo_df):
    return(fun_meo_df[fun_meo_df['ViewData.BreakID'] == fun_breakid]['ViewData.Status'])


def unlist_comma_separated_single_quote_string_lst(list_obj):
    new_list = []
    for i in list_obj:
        list_i = list(i.replace('\'','').split(', '))
        for j in list_i:
            new_list.append(j)
    return new_list

def get_remaining_ids(fun_meo_df, fun_final_df_2):
    Side1_UniqueIds_final_df_2 = unlist_comma_separated_single_quote_string_lst(list_obj = \
                                                                                final_df_2['Side1_UniqueIds'].\
                                                                                astype(str).\
                                                                                unique().\
                                                                                tolist())
    Side0_UniqueIds_final_df_2 = unlist_comma_separated_single_quote_string_lst(list_obj = \
                                                                                final_df_2['Side0_UniqueIds'].\
                                                                                astype(str).\
                                                                                unique().\
                                                                                tolist())
    BreakId_final_df_2_UMR_mtm =  final_df_2[final_df_2['Predicted_action'] == 'UMR_Many_to_Many']['BreakID'].astype(str).unique().tolist()
#    Side01_UniqueIds_final_df_2 = Side1_UniqueIds_final_df_2.append(Side0_UniqueIds_final_df_2)
#    Side01_UniqueIds_final_df_2 = [str(x) for x in Side01_UniqueIds_final_df_2]
#    Side01_UniqueIds_final_df_2_ge_1 = [x if x.find(',') != -1) for x in Side01_UniqueIds_final_df_2]

#    Side01_UniqueIds_final_df_2 = [re.split(r", ", x) for x in Side01_UniqueIds_final_df_2]
#    Side01_UniqueIds_final_df_2 = [x.replace("\'",'') for x in Side01_UniqueIds_final_df_2]
    
    newStr = [x.replace("\'",'') for x in newStr]

import csv
RESULT = ['apple','cherry','orange','pineapple','strawberry']
with open('output.csv','wb') as result_file:
    wr = csv.writer(result_file, dialect='excel')
    wr.writerow(Side0_UniqueIds_final_df_2)


    Side1_UniqueIds_meo_df = unlist_comma_separated_single_quote_string_lst(list_obj = \
                                                                                meo_df['ViewData.Side1_UniqueIds'].\
                                                                                astype(str).\
                                                                                unique().\
                                                                                tolist())
    Side0_UniqueIds_meo_df = unlist_comma_separated_single_quote_string_lst(list_obj = \
                                                                                meo_df['ViewData.Side0_UniqueIds'].\
                                                                                astype(str).\
                                                                                unique().\
                                                                                tolist())
Side1_ids_diff = set(Side1_UniqueIds_meo_df) - set(Side1_UniqueIds_final_df_2)

ids_left = meo_df[~((meo_df['ViewData.Side0_UniqueIds'].isin(Side0_UniqueIds_final_df_2)) or (meo_df['ViewData.Side1_UniqueIds'].isin(Side1_UniqueIds_final_df_2)))]
ids_left = meo_df[~meo_df['ViewData.Side0_UniqueIds'].isin(Side0_UniqueIds_final_df_2)]

import re

cStr = '"aaaa","bbbb","ccc,ddd"'
newStr = re.split(r",(?=')", cStr)

print(newStr)


import csv
cStr = '"aaaa","bbbb","ccc,ddd"'
newStr = [ '"{}"'.format(x) for x in list(csv.reader([cStr], delimiter=',', quotechar='"'))[0] ]



meo_df = meo_df[~meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR','SPM'])]

def get_remaining_breakids(fun_meo_df, fun_final_df_2):
    
    fun_meo_df = fun_meo_df[~fun_meo_df['ViewData.Status'].isin(['SMT','HST', 'OC', 'CT', 'Archive','SMR','SPM'])]

    BreakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['BreakID'].astype(str).unique().tolist())
#    BreakId_final_df_2 =  final_df_2['BreakID'].astype(str).unique().tolist()
    
    Final_predicted_breakId_final_df_2 =  unlist_comma_separated_single_quote_string_lst(fun_final_df_2['Final_predicted_break'].astype(str).unique().tolist())
#    for i in final_df_2['Final_predicted_break'].astype(str).unique().tolist():
#        if(',' in i):
#            print(i)
    BreakId_meo_df =  unlist_comma_separated_single_quote_string_lst(fun_meo_df['ViewData.BreakID'].astype(str).unique().tolist())
    all_breakids_in_final_df_2 = set(BreakId_final_df_2).union(set(Final_predicted_breakId_final_df_2))        
    unpredicted_breakids = set(BreakId_meo_df) - set(all_breakids_in_final_df_2) 
#    meo_df[meo_df['ViewData.BreakID'].isin(unpredicted_breakids)]['ViewData.Status'].value_counts()
    return(unpredicted_breakids)

