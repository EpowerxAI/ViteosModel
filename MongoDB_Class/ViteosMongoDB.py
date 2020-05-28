# -*- coding: utf-8 -*-
"""
Created on Tue May 12 17:23:09 2020

@author: consultant138
"""
import sshtunnel
import pymongo
import logging
import sys
import pandas as pd
import Read_Class as rd_cl
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


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
rd_cl_obj = rd_cl()


class ViteosMongoDB:
    
    def __init__(self, param_without_ssh = True, 
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = None, param_MONGO_PORT = None,
                 param_MONGO_USERNAME = None, param_MONGO_PASSWORD = None,
                 param_MONGO_DB = None, param_MONGO_COLLECTION = None):
        
        self.without_ssh = param_without_ssh
        
        if param_SSH_HOST is None:
            self.SSH_HOST = rd_cl.ssh_host()
        else:
            self.SSH_HOST = param_SSH_HOST
        
        if param_SSH_PORT is None:
            self.SSH_PORT = rd_cl.ssh_port()
        else:
            self.SSH_PORT = param_SSH_PORT

        if param_SSH_USERNAME is None:
            self.SSH_USERNAME = rd_cl.ssh_username()
        else:
            self.SSH_USERNAME = param_SSH_USERNAME

        if param_SSH_PASSWORD is None:
            self.SSH_PASSWORD = rd_cl.ssh_password()
        else:
            self.SSH_PASSWORD = param_SSH_PASSWORD

        if param_MONGO_HOST is None:
            self.MONGO_HOST = rd_cl.mongo_host()
        else:
            self.MONGO_HOST = param_MONGO_HOST
        
        if param_MONGO_PORT is None:
            self.MONGO_PORT = rd_cl.mongo_port()
        else:
            self.MONGO_PORT = param_MONGO_PORT

        if param_MONGO_USERNAME is None:
            self.MONGO_USERNAME = rd_cl.mongo_username()
        else:
            self.MONGO_USERNAME = param_MONGO_USERNAME
        
        if param_MONGO_PASSWORD is None:
            self.MONGO_PASSWORD = rd_cl.mongo_password()
        else:
            self.MONGO_PASSWORD = param_MONGO_PASSWORD
    
        # Note: We will get the MONGO_DB and MONGO_COLLECTION later in the code from functions self.DB_to_use() and self.Collection_to_use()
        self.MONGO_DB = param_MONGO_DB
        self.MONGO_COLLECTION = param_MONGO_COLLECTION
        
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
        print(*self.client_ssh.list_database_names(), sep = '\n')
        
        
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
        
    def DB_to_use(self):
        if self.MONGO_DB is None:
            self.MONGO_DB = input("\nEnter the DB you wish to use : ") 
            if self.MONGO_DB == '':
                LOGGER.error('\nSince there is no database mentioned in the MONGO_DB parameter or the input, ending the program run')
                print('\nERROR : Since there is no database mentioned in the MONGO_DB parameter or the input, ending the program run')
                sys.exit();
            else:
                LOGGER.info('\nSwitching to DB = ' + self.MONGO_DB)
                print('\nSwitching to DB = ' + self.MONGO_DB)
        else:      
            LOGGER.info('\nSwitching to DB = ' + self.MONGO_DB)
            print('\nSwitching to DB = ' + self.MONGO_DB)
        
        self.database = self.client[self.MONGO_DB]
    
    def collection_to_use(self):
        if self.MONGO_COLLECTION is None:
            self.MONGO_COLLECTION = input("\nEnter the Collection you wish to use : ") 
            if self.MONGO_COLLECTION == '':
                LOGGER.error('\nSince there is no Collection mentioned in the MONGO_COLLECTION parameter or the input, ending the program run')
                print('\nERROR : Since there is no Collection mentioned in the MONGO_COLLECTION parameter or the input, ending the program run')
                sys.exit();
            else:
                LOGGER.info('\nSwitching to Collection = ' + self.MONGO_COLLECTION)
                print('\nSwitching to Collection = ' + self.MONGO_COLLECTION)
        else:      
            LOGGER.info('\nSwitching to Collection = ' + self.MONGO_COLLECTION)
            print('\nSwitching to Collection = ' + self.MONGO_COLLECTION)
        self.collection = self.database[self.MONGO_COLLECTION]

    
    def make_connection(self):
        
        self.connect_with_or_without_ssh()
        self.DB_to_use()
        self.collection_to_use()
    
    def get_data_for_TaskID(self,param_TaskID):
        try:
            cursor = self.collection.find( {"TaskInstanceID": param_TaskID, "ViewData":{"$ne":None}, "MatchStatus":{"$ne":21}, "ViewData.CombinedAndIsPaired" : False}, {'ViewData' : 1.0})
            self.df_cursor = pd.DataFrame(list(cursor))
            print ('\n Cash data - {} rows,cols loaded from mongodb\n'.format(self.df_cursor.shape))

        except Exception as e:
            print( str(e))
        
        if self.df_cursor.shape[0] == 0:
            raise ValueError('empty dataframe - no data to make predictions on!')

        self.df = self.df_cursor['ViewData'].apply(pd.Series)

    
    
#    def gather_TaskInstanceIDs(number_of_taskIID = 5)
#        self.TaskInstanceID_list = self.mongo_db.self.mongo_collection.distinct('TaskInstanceID')[1:number_of_taskIID + 1]
        
    
if __name__ == '__main__':
    test_client = ViteosMongoDB(param_without_ssh = False,param_MONGO_DB = 'ML_ReconDB', param_MONGO_COLLECTION = 'HST_RecData_123')
    test_client.make_connection()
    test_client.get_data_for_TaskID(param_TaskID = 123726192)
        
                                     