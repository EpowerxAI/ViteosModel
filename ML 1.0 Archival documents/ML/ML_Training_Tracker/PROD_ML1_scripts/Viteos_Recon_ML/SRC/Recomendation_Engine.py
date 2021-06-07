# -*- coding: utf-8 -*-
"""
Created on Mon Oct  8 19:32:40 2018

@author: rangenine.chandar
"""

import sys
import subprocess
from subprocess import check_output, STDOUT, CalledProcessError
import re
import pika
from pandas import DataFrame
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
if len(stout_list) < 5:
    print_usage()
    sys.exit(1)
#Converting input message to send as aruguments in prediction script
smallerlist = [l.split(',') for l in ','.join(stout_list).split('\n')]

RecomDF = DataFrame.from_records(smallerlist)
RecomDF = RecomDF.dropna(how='any',axis=0)
RecomDF.columns = ['TaskId', 'csc','ReconPurpose','collection_meo','RequestId']

RecomDF['TaskId'] = RecomDF['TaskId'].str.lstrip("b'")
#RecomDF['RequestId'] = RecomDF['RequestId'].str.rstrip("'")
RecomDF['RequestId'] = RecomDF['RequestId'].str.replace(r"[^0-9]"," ")	


for i, row in RecomDF.iterrows():    
    
    row = row.values.tolist() 
    
    TaskId=row[0]
    #TaskId= re.sub('[^0-9]', '', taskid)
    csc=row[1]
    ReconPurpose=row[2]
    collection_meo=row[3]
    RequestId=row[4]
    #RequestId= re.sub('[^0-9]', '', requestid)
    Model=row[1]+row[2]+'.model'
    
    # Creating messages to trigger rabbit mq
    Message1 = 'PredictionsTriggered'+"|"+TaskId+"|"+ csc+"|"+ReconPurpose+"|"+collection_meo+"|"+RequestId
    Message2 = 'PredictionsCompleted'+"|"+TaskId+"|"+ csc+"|"+ReconPurpose+"|"+collection_meo+"|"+RequestId
    Message3 = 'PredictionsFailed|Wrong Data'+"|"+TaskId+"|"+ csc+"|"+ReconPurpose+"|"+collection_meo+"|"+RequestId
    #Sending prediction started  message
    connection = pika.BlockingConnection(pika.connection.URLParameters('amqps://recon2:recon2@vitblrauth02.viteos.com/viteos'))
    channel = connection.channel()

    channel.basic_publish(exchange='ReconPRODExchange_ML',
                      routing_key='Recon2_ReadFromML_PROD',
                      body=Message1)

    print(" [x] Sent"+ Message1)


    try:
        grepOut = subprocess.check_output([sys.executable, '/home/reconadmin/Viteos_Recon_ML/SRC/h2o_gbm_All_Client_Predict.py',TaskId, csc, ReconPurpose, collection_meo, Model, RequestId],stderr=STDOUT)
    except subprocess.CalledProcessError as grepexc:
     print("error code", grepexc.returncode, grepexc.output)
     
     channel.basic_publish(exchange='ReconPRODExchange_ML',
                                  routing_key='Recon2_ReadFromML_PROD',
                                  body=Message3)
     print(" [x] Sent "+ Message3)
     connection.close()
    else:
         channel.basic_publish(exchange='ReconPRODExchange_ML',
                     routing_key='Recon2_ReadFromML_PROD',
                      body=Message2)
         print(" [x] Sent"+ Message2)
         connection.close()


