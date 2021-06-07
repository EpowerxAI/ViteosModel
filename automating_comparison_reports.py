# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 16:58:57 2020

@author: consultant138
"""

import timeit
start = timeit.default_timer()


import numpy as np
import pandas as pd
#from imblearn.over_sampling import SMOTE


import os
os.chdir('D:\\ViteosModel')

#from imblearn.over_sampling import SMOTE
import sys
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime,date,timedelta
from pandas.io.json import json_normalize
import json

client = 'OakTree'

setup_code = '379'

import dateutil.parser

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

date_to_analyze = '10122020'
penultimate_date_to_analyze = '09122020'
date_to_analyze_ymd_format = date_to_analyze[4:] + '-' + date_to_analyze[2:4] + '-' + date_to_analyze[:2]
penultimate_date_to_analyze_ymd_format = penultimate_date_to_analyze[4:] + '-' + penultimate_date_to_analyze[2:4] + '-' + penultimate_date_to_analyze[:2]
penultimate_date_to_analyze_ymd_iso_18_30_format = penultimate_date_to_analyze_ymd_format + 'T18:30:00.000+0000'
date_to_analyze_ymd_iso_00_00_format = date_to_analyze_ymd_format + 'T00:00:00.000+0000'

mngdb_137_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_137_server.connect_with_or_without_ssh()
ReconDB_ML_137_server = mngdb_137_server.client['ReconDB_ML']
ReconDB_ML_Testing_137_server = mngdb_137_server.client['ReconDB_ML_Testing']

mngdb_prod_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = 'vitblrrecdb05.viteos.com', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_prod_server.connect_with_or_without_ssh()
ReconDB_prod_server = mngdb_prod_server.client['ReconDB']


#1. Drop the following collections
## i. Tasks in ReconDB_ML_Testing db
Tasks_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['Tasks']
Tasks_ReconDB_ML_Testing_137_server.drop()

## ii. RecData_<setup_code> in ReconDB_ML_Testing db
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]
RecData_Setup_Code_ReconDB_ML_Testing_137_server.drop()

## iii. RecData_<setup_code>_Audit in ReconDB_ML_Testing_db
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server.drop()

#2. Fixing Tasks collection in 137 Testing so as to reflect Tasks corresponding to client,setup_code and date
query_for_Task_collection = ReconDB_ML_137_server['Tasks'].find({ "BusinessDate": getDateTimeFromISO8601String(penultimate_date_to_analyze_ymd_iso_18_30_format), 
                                                                  "ReconSetupCode": setup_code },
                                                                 {'_createdBy' : 1,
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
                                                                  'ProcessID' : 1})
list_of_dicts_query_for_Task_collection = list(query_for_Task_collection)
list_instance_ids = [list_of_dicts_query_for_Task_collection[i].get('InstanceID',{}) for i in range(0,len(list_of_dicts_query_for_Task_collection))]
list_version = [list_of_dicts_query_for_Task_collection[i].get('_version',{}) for i in range(0,len(list_of_dicts_query_for_Task_collection))]

#Update all values of '_version' to 2 as the comparison report requires the value of '_version' column to be greater than 0. We have randomly chosen 2 to keep uniformity.     
for d in list_of_dicts_query_for_Task_collection:
    d.update((k, int(2)) for k, v in d.items() if k == '_version')

Tasks_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['Tasks']
Tasks_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_Task_collection) 

#3. Getting AUA data from prod and dumping results into RecData_<setup_code>_Audit collection in 137 Testing corresponding to client,setup_code and date

query_for_AUA_data = ReconDB_prod_server['HST_RecData_' + setup_code].find({ 'TaskInstanceID': { '$in': list_instance_ids }, 'ViewData' : { '$ne': None}, 'LastPerformedAction' : { '$ne' : 31 }, 'MatchStatus': { '$nin' : [1,2,18,19,20,21] } },
                                                                           {'_createdBy' : 1,
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
                                                                            '_parentID' : 1})

list_of_dicts_query_for_AUA_data = list(query_for_AUA_data)
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code + '_Audit']
RecData_Setup_Code_Audit_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_AUA_data)

#3. Getting AUA data from prod and dumping results into RecData_<setup_code>_Audit collection in 137 Testing corresponding to client,setup_code and date

query_for_MEO_data = ReconDB_ML_137_server['RecData_' + setup_code + '_Historic'].find({ 'TaskInstanceID': { '$in': list_instance_ids }},
                                                                           {'_createdBy' : 1,
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
                                                                            '_parentID' : 1})

list_of_dicts_query_for_MEO_data = list(query_for_MEO_data)
RecData_Setup_Code_ReconDB_ML_Testing_137_server = ReconDB_ML_Testing_137_server['RecData_' + setup_code]
RecData_Setup_Code_ReconDB_ML_Testing_137_server.insert_many(list_of_dicts_query_for_MEO_data)

 
    
 
    
