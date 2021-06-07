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


setup_code_input_list = ['123']

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

mngdb_obj_3_for_writing_in_uat_server = mngdb(param_without_ssh = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '192.168.170.158', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_3_for_writing_in_uat_server.connect_with_or_without_ssh()
db_3_for_writing_in_ml_server = mngdb_obj_3_for_writing_in_uat_server.client['MEO_AUA_Collections']
db_4_for_MEO_data = mngdb_obj_3_for_writing_in_uat_server.client['MeoCollections']
db_5_for_AUA_data = mngdb_obj_3_for_writing_in_uat_server.client['AUACollections']

date_input_list = ['2020-12-10']

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
        
        instance_ids_temp_path = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/Setup_and_Date_wise_Task_Instance_Ids/' + setup_code + '_' + date_input + '.txt'
        with open(instance_ids_temp_path, 'w') as f:
            for item in list_instance_ids:
                f.write("%s\n" % item)
        
        if(len(list_instance_ids) != 0):
            query_2_for_data_in_list_instance_ids = coll_2_for_data_in_list_instance_ids.find({ 
                        "ViewData": { "$ne": None },
                        "ViewData.Status": { "$nin": ["HST","Archive","OC"] },
                        "TaskInstanceID" : {"$in" : list_instance_ids}
                       },{ 
                        "LastPerformedAction" : 1,
                        "TaskInstanceID" : 1,
                        "SourceCombinationCode" : 1,
                        "MetaData" : 1, 
                        "ViewData" : 1
                                     })
            #df_query_result_2 = json_normalize(query2)

            list_of_dicts_query_result_2 = list(query_2_for_data_in_list_instance_ids)
            if(len(list_of_dicts_query_result_2) != 0):
                coll_3_for_writing_in_ml_server = db_3_for_writing_in_ml_server['MEO_AUA_HST_RecData_' + setup_code + '_' + date_input]
                coll_3_for_writing_in_ml_server.insert_many(list_of_dicts_query_result_2)
                query_4_for_MEO_data = coll_3_for_writing_in_ml_server.find({ 
                                                                                     "LastPerformedAction": 31
                                                                             },
                                                                             {
                                                                                     "LastPerformedAction" : 1,
                                                                                     "TaskInstanceID" : 1,
                                                                                     "SourceCombinationCode" : 1,
                                                                                     "MetaData" : 1, 
                                                                                     "ViewData" : 1
                                                                             })
                list_of_dicts_query_result_4 = list(query_4_for_MEO_data)
                query_5_for_AUA_data = coll_3_for_writing_in_ml_server.find({ 
                                                                                     "LastPerformedAction": { "$ne": 31 }
                                                                             },
                                                                            {
                                                                                     "LastPerformedAction" : 1,
                                                                                     "TaskInstanceID" : 1,
                                                                                     "SourceCombinationCode" : 1,
                                                                                     "MetaData" : 1, 
                                                                                     "ViewData" : 1
                                                                            })
                list_of_dicts_query_result_5 = list(query_5_for_AUA_data)
                if(len(list_of_dicts_query_result_4) != 0):
                    coll_4_for_writing_MEO_data = db_4_for_MEO_data['MEO_HST_RecData_' + setup_code + '_' + date_input]
                    coll_4_for_writing_MEO_data.insert_many(list_of_dicts_query_result_4)
                if(len(list_of_dicts_query_result_5) != 0):
                    coll_5_for_writing_AUA_data = db_5_for_AUA_data['AUA_HST_RecData_' + setup_code + '_' + date_input]
                    coll_5_for_writing_AUA_data.insert_many(list_of_dicts_query_result_5)
                
                
        else:
             instance_ids_temp_path_warning = '//vitblrdevcons01/Raman  Strategy ML 2.0/All_Data/HIG_Capital/Setup_and_Date_wise_Task_Instance_Ids/WARNING_' + setup_code + '_' + date_input + '.txt'
             with open(instance_ids_temp_path_warning, 'w') as f:
                 f.write('No instance ID found for this setup')
        
        print('DONE : Date wise extraction for following date :')
        print(date_input)
    print('DONE : Setup wise extraction for following setup :')
    print(setup_code)

##### Adding MEO and AUA query
#for setup_code in setup_code_input_list:
#    print('INITIATED : Setup wise MEO and AUA extraction for following setup :')
#    print(setup_code)
#    coll_1_for_instance_ids = db_1_for_instance_ids['Tasks']
#    coll_2_for_data_in_list_instance_ids = db_2_for_data_in_list_instance_ids['HST_RecData_' + setup_code]
#    
#    for date_input in date_input_list:
#        print('INITIATED : Date wise extraction for following date :')
#        print(date_input)
#        date_input_iso = date_input + 'T18:30:00.000+0000'
#        
#        if(len(list_instance_ids) != 0):
#            meo_query = 
