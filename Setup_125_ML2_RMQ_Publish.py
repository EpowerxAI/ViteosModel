# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 15:57:52 2020

@author: consultant138
"""

#!/usr/bin/env python
# coding: utf-8

# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 15:33:48 2020
@author: consultant138
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 19:12:48 2020

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
from pandas.io.json import json_normalize
import json
from pandas import merge
import re

from RabbitMQ import RabbitMQ_Class as rb_mq

client = 'Weiss'

setup = '125'
setup_code = '125'

mngdb_obj_1_for_reading_and_writing_in_uat_server = mngdb(param_without_ssh  = True, param_without_RabbitMQ_pipeline = True,
                 param_SSH_HOST = None, param_SSH_PORT = None,
                 param_SSH_USERNAME = None, param_SSH_PASSWORD = None,
                 param_MONGO_HOST = '10.1.15.137', param_MONGO_PORT = 27017,
#                 param_MONGO_HOST = '192.168.170.50', param_MONGO_PORT = 27017,
                 param_MONGO_USERNAME = 'mongouseradmin', param_MONGO_PASSWORD = '@L0ck&Key')
#                 param_MONGO_USERNAME = '', param_MONGO_PASSWORD = '')
mngdb_obj_1_for_reading_and_writing_in_uat_server.connect_with_or_without_ssh()
db_1_for_MEO_data = mngdb_obj_1_for_reading_and_writing_in_uat_server.client['ReconDB_ML']
#for setup_code in setup_code_list:
print('Sending Task Ids for Weiss, setup_code = ')
print(setup_code)

query_1_for_TaskID = db_1_for_MEO_data['RecData_' + setup_code].find({ 
                                                                     "LastPerformedAction": 31
                                                             },
                                                             {
                                                                     "TaskInstanceID" : 1,
                                                             })
list_of_dicts_query_result_1 = list(query_1_for_TaskID)

TaskID_df = json_normalize(list_of_dicts_query_result_1)
#Prepare RabbitMQ simulation message to be sent to DB for data extraction    
TaskID_list = list(TaskID_df['TaskInstanceID'].unique())

Message_list = [str(x) + '|csc|Recon_Purpose_Test|HST_RecData_' + str(setup_code) + '|Request_Id_Test' for x in TaskID_list]

rb_mq_obj_new = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
                 param_RABBITMQ_HOST_IP = 'Vitblresbuat.viteos.com', param_RABBITMQ_PORT = '5671', \
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
                 param_RABBITMQ_EXCHANGE = 'ReconUATExchange_ML_test_ch01', \
                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML2_UAT_test_ch01', \
                 param_RABBITMQ_ROUTING_KEY = 'VNFRecon_ReadFromML2_UAT_test_binding_ch01', \
                 param_test_message_publishing = True, \
                 param_timeout = 10)
for Message_i in Message_list:
    rb_mq_obj_new.fun_publish_single_message(param_message_body = Message_i)
