# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:14:08 2020

@author: consultant138
"""

#[‎6/‎29/‎2020 22:18]  Ritesh Kumar Patra:  
# <add name="mongodbServer" connectionString="mongodb://appuser:V!teo$@vitblrrecdb01.viteos.com:27017,vitblrrecdb02.viteos.com:27017,vitblrrecdb03.viteos.com:27017/ReconDB?replicaSet=reconReplSet;authSource=Users" />
     
from ViteosMongoDB import  ViteosMongoDB_Class as mngdb
from datetime import datetime 
from pandas.io.json import json_normalize

import dateutil.parser

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d

#date_input_1 =  '2020-06-01'
#date_input_iso_1 = date_input_1 + 'T18:30:00.000+0000'
#
#date_input_2 =  '2020-06-02'
#date_input_iso_2 = date_input_2 + 'T18:30:00.000+0000'
#
#date_input_3 =  '2020-06-03'
#date_input_iso_3 = date_input_3 + 'T18:30:00.000+0000'
#
#date_input_4 =  '2020-06-04'
#date_input_iso_4 = date_input_4 + 'T18:30:00.000+0000'
#
#date_input_5 =  '2020-06-05'
#date_input_iso_5 = date_input_5 + 'T18:30:00.000+0000'
#
#date_input_6 =  '2020-06-06'
#date_input_iso_6 = date_input_6 + 'T18:30:00.000+0000'
#
#date_input_7 =  '2020-06-07'
#date_input_iso_7 = date_input_7 + 'T18:30:00.000+0000'
#
#date_input_8 =  '2020-06-08'
#date_input_iso_8 = date_input_8 + 'T18:30:00.000+0000'
#
#date_input_9 =  '2020-06-09'
#date_input_iso_9 = date_input_9 + 'T18:30:00.000+0000'
#
#date_input_10 =  '2020-06-10'
#date_input_iso_10 = date_input_10 + 'T18:30:00.000+0000'
#
#date_input_11 =  '2020-06-11'
#date_input_iso_11 = date_input_11 + 'T18:30:00.000+0000'
#
#date_input_12 =  '2020-06-12'
#date_input_iso_12 = date_input_12 + 'T18:30:00.000+0000'
#
#date_input_13 =  '2020-06-13'
#date_input_iso_13 = date_input_13 + 'T18:30:00.000+0000'
#
#date_input_14 =  '2020-06-14'
#date_input_iso_14 = date_input_14 + 'T18:30:00.000+0000'
#
#date_input_15 =  '2020-06-15'
#date_input_iso_15 = date_input_15 + 'T18:30:00.000+0000'
#
#date_input_16 =  '2020-06-16'
#date_input_iso_16 = date_input_16 + 'T18:30:00.000+0000'
#
#date_input_17 =  '2020-06-17'
#date_input_iso_17 = date_input_17 + 'T18:30:00.000+0000'
#
#date_input_18 =  '2020-06-18'
#date_input_iso_18 = date_input_18 + 'T18:30:00.000+0000'
#
#date_input_19 =  '2020-06-19'
#date_input_iso_19 = date_input_19 + 'T18:30:00.000+0000'
#
#date_input_20 =  '2020-06-20'
#date_input_iso_20 = date_input_20 + 'T18:30:00.000+0000'
#
#date_input_21 =  '2020-06-21'
#date_input_iso_21 = date_input_21 + 'T18:30:00.000+0000'
#
#date_input_22 =  '2020-06-22'
#date_input_iso_22 = date_input_22 + 'T18:30:00.000+0000'
#
#date_input_23 =  '2020-06-23'
#date_input_iso_23 = date_input_23 + 'T18:30:00.000+0000'
#
#date_input_24 =  '2020-06-24'
#date_input_iso_24 = date_input_24 + 'T18:30:00.000+0000'
#
#date_input_25 =  '2020-06-25'
#date_input_iso_25 = date_input_25 + 'T18:30:00.000+0000'
#
#date_input_26 =  '2020-06-26'
#date_input_iso_26 = date_input_26 + 'T18:30:00.000+0000'
#
#date_input_27 =  '2020-06-27'
#date_input_iso_27 = date_input_27 + 'T18:30:00.000+0000'
#
#date_input_28 =  '2020-06-28'
#date_input_iso_28 = date_input_28 + 'T18:30:00.000+0000'
#
#date_input_29 =  '2020-06-29'
#date_input_iso_29 = date_input_29 + 'T18:30:00.000+0000'
#
#date_input_30 =  '2020-06-30'
#date_input_iso_30 = date_input_30 + 'T18:30:00.000+0000'

date_input_list = ['2020-06-' + str(i) for i in range(1,31)]
date_iso_input_list = [date_input + 'T18:30:00.000+0000' for date_input in date_input_list]
#setup_code_input_123 = '123'
#setup_code_input_125 = '125'
#setup_code_input_170 = '170'
#setup_code_input_531 = '531'
#setup_code_input_833 = '833'
#setup_code_input_1200 = '1200'

setup_code_input_list = ['123','125','170','531','833','1200']
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

mngdb_obj_3_for_writing_in_ml_server = mngdb(param_without_ssh = False, param_without_RabbitMQ_pipeline = True)
mngdb_obj_3_for_writing_in_ml_server.connect_with_or_without_ssh()
db_3_for_writing_in_ml_server = mngdb_obj_3_for_writing_in_ml_server.client['MeoCollections']

# loop start
for setup_code in setup_code_input_list:
    print('INITIATED : Setup wise extraction for following setup :')
    print(setup_code)
    coll_1_for_instance_ids = db_1_for_instance_ids['Tasks']
    coll_2_for_data_in_list_instance_ids = db_2_for_data_in_list_instance_ids['HST_RecData_' + setup_code]
    
    for date_input in date_input_list:
        print('INITIATED : Date wise extraction for following date :')
        print(date_input)
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
        
        instance_ids_temp_path = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/Setup_and_Date_wise_Task_Instance_Ids/' + setup_code + '_' + date_input + '.txt'
        with open(instance_ids_temp_path, 'w') as f:
            for item in list_instance_ids:
                f.write("%s\n" % item)
        
        if(len(list_instance_ids) != 0):
            query_2_for_data_in_list_instance_ids = coll_2_for_data_in_list_instance_ids.find({ 
                        "TaskInstanceID" : {"$in" : list_instance_ids}
                       },{ 
                        "MetaData" : 1, 
                        "ViewData" : 1
                                     })
            #df_query_result_2 = json_normalize(query2)

            list_of_dicts_query_result_2 = list(query_2_for_data_in_list_instance_ids)
            if(len(list_of_dicts_query_result_2) != 0):
                coll_3_for_writing_in_ml_server = db_3_for_writing_in_ml_server['HST_RecData_' + setup_code + '_' + date_input]
                coll_3_for_writing_in_ml_server.insert_many(list_of_dicts_query_result_2)
        else:
             instance_ids_temp_path_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/Weiss/Setup_and_Date_wise_Task_Instance_Ids/WARNING_' + setup_code + '_' + date_input + '.txt'
             with open(instance_ids_temp_path_warning, 'w') as f:
                 f.write('No instance ID found for this setup')
        
        print('DONE : Date wise extraction for following date :')
        print(date_input)
    print('DONE : Setup wise extraction for following setup :')
    print(setup_code)

##### Adding MEO and AUA query
for setup_code in setup_code_input_list:
    print('INITIATED : Setup wise MEO and AUA extraction for following setup :')
    print(setup_code)
    coll_1_for_instance_ids = db_1_for_instance_ids['Tasks']
    coll_2_for_data_in_list_instance_ids = db_2_for_data_in_list_instance_ids['HST_RecData_' + setup_code]
    
    for date_input in date_input_list:
        print('INITIATED : Date wise extraction for following date :')
        print(date_input)
        date_input_iso = date_input + 'T18:30:00.000+0000'
        

