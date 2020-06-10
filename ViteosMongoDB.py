#!/usr/bin/env python
# coding: utf-8
"""
Created on Tue May 12 17:23:09 2020

@author: consultant138
"""

import sshtunnel
import pymongo
import logging
import sys
sys.path.append('..')
import pandas as pd
from Read import Read_Class as rd_cl
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
from pandas.io.json import json_normalize



#DEFAULT_SSH_HOST = '10.1.15.138'
#DEFAULT_SSH_PORT = 27017
#DEFAULT_SSH_USERNAME = 'vurecml2'
#DEFAULT_SSH_PASSWORD = 'Viteos!23'
#
#DEFAULT_MONGO_HOST = '10.1.15.138'
#DEFAULT_MONGO_PORT = 27017
#DEFAULT_MONGO_USERNAME = 'mluser'
#DEFAULT_MONGO_PASSWORD = 'Viteos123'

# We will get the default ssh and mongo credentails from the Read_Class
rd_cl_obj1 = rd_cl()


class ViteosMongoDB_Class:
    
    def __init__(self, param_without_ssh = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = None, param_MONGO_PORT = None,
                 param_MONGO_USERNAME = None, param_MONGO_PASSWORD = None
#		 ,
#                 param_MONGO_DB = None, 
#		 param_MONGO_COLLECTION = None
		 ):
        self.rd_cl_obj = rd_cl()
        self.without_ssh = param_without_ssh
        self.without_RabbitMQ_pipeline = param_without_RabbitMQ_pipeline
        
        if param_SSH_HOST is None:
            self.SSH_HOST = self.rd_cl_obj.ssh_host()
        else:
            self.SSH_HOST = param_SSH_HOST
        
        if param_SSH_PORT is None:
            self.SSH_PORT = self.rd_cl_obj.ssh_port()
        else:
            self.SSH_PORT = param_SSH_PORT

        if param_SSH_USERNAME is None:
            self.SSH_USERNAME = self.rd_cl_obj.ssh_username()
        else:
            self.SSH_USERNAME = param_SSH_USERNAME

        if param_SSH_PASSWORD is None:
            self.SSH_PASSWORD = self.rd_cl_obj.ssh_password()
        else:
            self.SSH_PASSWORD = param_SSH_PASSWORD

        if param_MONGO_HOST is None:
            self.MONGO_HOST = self.rd_cl_obj.mongo_host()
        else:
            self.MONGO_HOST = param_MONGO_HOST
        
        if param_MONGO_PORT is None:
            self.MONGO_PORT = self.rd_cl_obj.mongo_port()
        else:
            self.MONGO_PORT = param_MONGO_PORT

        if param_MONGO_USERNAME is None:
            self.MONGO_USERNAME = self.rd_cl_obj.mongo_username()
        else:
            self.MONGO_USERNAME = param_MONGO_USERNAME
        
        if param_MONGO_PASSWORD is None:
            self.MONGO_PASSWORD = self.rd_cl_obj.mongo_password()
        else:
            self.MONGO_PASSWORD = param_MONGO_PASSWORD
        
        self.df_test_message_to_RabbitMQ = self.rd_cl_obj.fun_df_test_messages_to_RabbitMQ()
        
#        # Note: We will get the MONGO_DB and MONGO_COLLECTION later in the code from functions self.DB_to_use() and self.Collection_to_use()
#        self.MONGO_DB = param_MONGO_DB
#        self.MONGO_COLLECTION = param_MONGO_COLLECTION
        
    def connect_without_ssh(self):
        
        LOGGER.info('Invoking function connect_without_ssh')
        LOGGER.info('Connecting to ' + self.MONGO_HOST)
        
        print('Invoking function connect_without_ssh')
        print('Connecting to ' + self.MONGO_HOST)
        
        self.client_without_ssh = pymongo.MongoClient(host = self.MONGO_HOST, port = self.MONGO_PORT, username = self.MONGO_USERNAME, password = self.MONGO_PASSWORD) 

        LOGGER.info('Mongo Client without ssh created')
        print('Mongo Client without ssh created')

        LOGGER.info('\n Databases present in server ' + self.MONGO_HOST + '\n')        
        LOGGER.info(self.client_without_ssh.list_database_names())
        
        print('\n Databases present in server ' + self.MONGO_HOST + '\n')
        print(*self.client_without_ssh.list_database_names(), sep = '\n')
        
        
    def connect_with_ssh(self):

        LOGGER.info('Invoking function connect_with_ssh')
        LOGGER.info('Connecting to ' + self.MONGO_HOST)
        
        print('Invoking function connect_with_ssh')
        print('Connecting to ' + self.MONGO_HOST)
        
        self.server = sshtunnel.SSHTunnelForwarder(
                ssh_address_or_host = self.SSH_HOST,
                ssh_username = self.SSH_USERNAME,
                ssh_password = self.SSH_PASSWORD,
                remote_bind_address = ('127.0.0.1', self.SSH_PORT)
                )
        
        LOGGER.info('ssh tunnel created')
        print('ssh tunnel created')
        
        self.server.start()
        
        LOGGER.info('Server started')
        print('Server started')
        
        self.client_with_ssh = pymongo.MongoClient('127.0.0.1', self.server.local_bind_port, username = self.MONGO_USERNAME, password = self.MONGO_PASSWORD) 
        
        LOGGER.info('Mongo Client with ssh created')
        print('Mongo Client with ssh created')
        
        LOGGER.info('\n Databases present in server ' + self.MONGO_HOST + '\n')
        LOGGER.info(self.client_with_ssh.list_database_names())
        
        print('\n Databases present in server ' + self.MONGO_HOST + '\n')
        print(*self.client_with_ssh.list_database_names(), sep = '\n')
        

    def connect_with_or_without_ssh(self):
        if(self.without_ssh == True):
            self.connect_without_ssh()
            self.client = self.client_without_ssh
        else:
            self.connect_with_ssh()
            self.client = self.client_with_ssh
        
    def get_data_for_collection(self, param_collection, param_taskinstance_id = None, param_client_setup_flag_name):
        try:
            collection = param_collection
            common_inner_pipe = {"ViewData":{"$ne":None},
                                 "ViewData.Status": { "$nin": ['HST', 'OC', 'CT'] }, 
                                 "MatchStatus":{"$ne":21}, 
                                 "ViewData.CombinedAndIsPaired" : False
                                }
            if(self.without_RabbitMQ_pipeline == False):
                common_inner_pipe['TaskInstanceID'] = param_taskinstance_id
                
            cursor = collection.find( 
                                                common_inner_pipe,
                                                #self.rd_cl.all_columns_query()
                                                self.rd_cl_obj.common_columns_query() 
                                    )

            df_to_return = json_normalize(cursor)
            print ('\n Cash data - {} rows,cols loaded from mongodb\n'.format(df_cursor.shape))

        except Exception as e:
            print( str(e))
        
        if df_to_return.shape[0] == 0:
            raise ValueError('empty dataframe - no data to make predictions on for collection ' + param_collection)
	    
        print("Starting to make ViewData columns from _id columns")
        
        df_to_return['Client_Setup_Flag'] = param_client_setup_flag_name
        print("ViewData columns making process - Done")
        return df_to_return

    def df_to_evaluate_without_RabbitMQ_pipeline(self, param_setup_prefix = 'HST_RecData_'):
        clients_list = list(self.rd_cl_obj.client_info().keys())
        df = pd.DataFrame()
        for client in clients_list:
                database_for_client = self.rd_cl_obj.client_info().get(client, {}).get('DB')
                setups_list = list(self.rd_cl_obj.client_info().get(client, {}).get('Setups').keys())
                setups_to_evaluate_list_temp = []
		
                for setup in setups_list:
                       if self.rd_cl_obj.client_info().get(client, {}).get('Setups', {}).get(setup) == 1 :
				
                            setups_to_evaluate_list_temp.append(setup)
                            db_to_use = self.client[database_for_client]
                            db_to_use = self.client[database_for_client]
				
                            collection_to_use_name = param_setup_prefix + setup
                            client_setup_flag_name = self.rd_cl_obj.fun_client_for_setup_dict().get(collection_to_use_name)
                            collection_to_use = db_to_use[collection_to_use_name]
                            df = df.append(self.get_data_for_collection(param_collection = collection_to_use, param_client_setup_flag_name = client_setup_flag_name))	
				
					 		
        setups_to_evaluate_list_temp = [param_setup_prefix + element for element in setups_to_evaluate_list_temp]
        self.setups_to_evaluate_list = setups_to_evaluate_list_temp
        
        self.df_main = df
    
    def df_to_evaluate_with_RabbitMQ_pipeline(self):
        df = pd.DataFrame()	    
        for i in (self.df_test_message_to_RabbitMQ.shape[0]):
            message_i = self.df_test_message_to_RabbitMQ.loc[[i]]
            task_instance_id_i = message_i[['Task_Instance_ID']]
            collection_name_i = message_i[['Collection']] 
            database_name_i = self.rd_cl_obj.fun_db_for_setup_dict().get(collection_name_i)
            client_setup_flag_name = self.rd_cl_obj.fun_client_for_setup_dict().get(collection_name_i)
            db_to_use = self.client[database_name_i]
            collection_to_use = db_to_use[collection_name_i]
            df = df.append(self.get_data_for_collection(param_collection = collection_to_use, param_taskinstance_id = task_instance_id_i, param_client_setup_flag_name = client_setup_flag_name))
        self.df_main = df

    
    
#    def gather_TaskInstanceIDs(number_of_taskIID = 5)
#        self.TaskInstanceID_list = self.mongo_db.self.mongo_collection.distinct('TaskInstanceID')[1:number_of_taskIID + 1]
        
    
#if __name__ == '__main__':
#    test_client = ViteosMongoDB_Class()
#    test_client.connect_with_or_without_ssh()
#    test_client.df_to_evaluate_without_RabbitMQ_pipeline()
#    test_client.make_df()
#    print(test_client.df_main.shape)
#        
                                     
