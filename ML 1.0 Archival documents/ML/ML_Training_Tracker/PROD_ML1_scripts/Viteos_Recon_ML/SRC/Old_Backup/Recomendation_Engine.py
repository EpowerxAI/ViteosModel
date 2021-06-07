# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 19:32:40 2018

@author: rangenine.chandar
"""

import sys
import subprocess
import re
import pika
#import ipdb
#=========================================== Run script to receive Messages from Rabbit MQ===========================================
#ipdb.set_trace()

try:
  s2_out = subprocess.check_output([sys.executable, '/home/reconadmin/Viteos_Recon_ML/SRC/RMQReceive.py'])
except Exception:
    data = None

# Decoding the output of rabbit MQ message
s2_stout=str(s2_out, 'utf-8')
stout_list = s2_stout.split("|")

#Validating the ouput of rabbit MQ message
def print_usage():
    print('Script to make predictions on given MEO data using a recon model for a specific client')
    print('\nUsage: python3 {} {} {} {} {}\n'.format(sys.argv[0],'<TaskId>', '<ClientShortCode>', '<ReconPurpose>', '<meocollectionname>', '<RequestNumber>'))
    print('<TaskId>\t: TaskId of the data')
    print('<ClientShortCode>\t: ClientShortCode of the client')
    print('<ReconPurpose>\t\t: ReconPurpose for MEO data')
    print('<collection_meo>\t\t: collection_meo for MEO data')
    print('<Model>\t\t\t: Full path to the model file to be used for prediction')



#new_tuple= tuple(s2_stout.split('|'))
print (stout_list)
if len(stout_list) != 5:
    print_usage()
    sys.exit(1)
#Converting input message to send as aruguments in prediction script

taskid=stout_list[0]
TaskId= re.sub('[^0-9]', '', taskid)
csc=stout_list[1]
ReconPurpose=stout_list[2]
collection_meo=stout_list[3]
requestid=stout_list[4]
RequestId= re.sub('[^0-9]', '', requestid)
Model=stout_list[1]+stout_list[2]+'.model'

# Creating messages to trigger rabbit mq
Message1 = 'PredictionsTriggered'+"|"+TaskId+"|"+ csc+"|"+ReconPurpose+"|"+collection_meo+"|"+RequestId
Message2 = 'PredictionsCompleted'+"|"+TaskId+"|"+ csc+"|"+ReconPurpose+"|"+collection_meo+"|"+RequestId

#Sending prediction started  message
connection = pika.BlockingConnection(pika.connection.URLParameters('amqp://recon2:recon2@vitblrauth02.viteos.com/viteos'))
channel = connection.channel()

channel.basic_publish(exchange='ReconPRODExchange_ML',
                      routing_key='Recon2_ReadFromML_PROD',
                      body=Message1)

print(" [x] Sent Message1")
#connection.close()

#Running the prediction script
subprocess.check_output([sys.executable, '/home/reconadmin/Viteos_Recon_ML/SRC/h2o_gbm_All_Client_Predict.py',TaskId, csc, ReconPurpose, collection_meo, Model, RequestId])
#subprocess.check_call(pass_arg)

#Sending prediction completed message to rabbit mq 
channel.basic_publish(exchange='ReconPRODExchange_ML',
                      routing_key='Recon2_ReadFromML_PROD',
                      body=Message2)

print(" [x] Sent Message2")
connection.close()




