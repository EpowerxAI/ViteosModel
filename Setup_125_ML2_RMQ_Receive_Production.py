# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 16:23:01 2020

@author: consultant138
"""
import os
os.chdir('D:\\ViteosModel')

#print('Rabbit MQ called')

from RabbitMQ import RabbitMQ_Class as rb_mq
import pika
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    
#rb_mq_obj_new_2 = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
#                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
##                 param_RABBITMQ_HOST_IP = 'vitblrmleng01.viteos.com', param_RABBITMQ_PORT = '5671', \vitblresbuat
#                 param_RABBITMQ_HOST_IP = 'Vitblresbuat.viteos.com', param_RABBITMQ_PORT = '5671', \
#                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
#                 param_RABBITMQ_EXCHANGE = 'ReconPROD_PARALLELExchange_ML', \
#                 param_RABBITMQ_QUEUE = 'VNFRecon_WriteToML_PROD_PARALLEL', \
#                 param_RABBITMQ_ROUTING_KEY = 'Recon2_WriteToML_PROD_PARALLEL', \
#                 param_test_message_publishing = True, \
#                 param_timeout = 10)

rb_mq_obj_new = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
#                 param_RABBITMQ_HOST_IP = 'vitblrmleng01.viteos.com', param_RABBITMQ_PORT = '5671', \vitblresbuat
                 param_RABBITMQ_HOST_IP = 'vu-uat', param_RABBITMQ_PORT = '5671', \
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
                 param_RABBITMQ_EXCHANGE = 'ReconPROD_PARALLELExchange_ML', \
                 param_RABBITMQ_QUEUE = 'VNFRecon_WriteToML_PROD_PARALLEL', \
                 param_RABBITMQ_ROUTING_KEY = 'Recon2_WriteToML_PROD_PARALLEL', \
                 param_test_message_publishing = True, \
                 param_timeout = 10)


#rb_mq_obj_new = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
#                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
#                 param_RABBITMQ_HOST_IP = 'Vitblresbuat.viteos.com', param_RABBITMQ_PORT = '5671', \
#                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
#                 param_RABBITMQ_EXCHANGE = 'ReconUATExchange_ML_test_ch01', \
#                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML2_UAT_test_ch01', \
#                 param_RABBITMQ_ROUTING_KEY = 'VNFRecon_ReadFromML2_UAT_test_binding_ch01', \
#                 param_test_message_publishing = True, \
#                 param_timeout = 10)

#rb_mq_obj_new = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', 
#                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', 
#                 param_RABBITMQ_HOST_IP = '10.1.15.153', param_RABBITMQ_PORT = '5671', 
#                 param_RABBITMQ_VIRTUAL_HOST = 'viteos',
#                 param_RABBITMQ_EXCHANGE = 'ReconUATExchange_ML_test_ch01',
#                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML2_UAT_test_ch01',
#                 param_RABBITMQ_ROUTING_KEY = 'VNFRecon_ReadFromML2_UAT_test_binding_ch01',
#                 param_test_message_publishing = False,
#                 param_timeout = 10)
connection = pika.BlockingConnection(pika.connection.URLParameters(rb_mq_obj_new.connection_string))
channel = connection.channel()

def add_callback_threadsafe(ch, method, properties, body):
   print("%r" % body)


timeout = 30

def on_timeout():
  global connection
  connection.close()

connection.add_timeout(timeout, on_timeout)
#connection.add_callback_threadsafe()
	
channel.basic_consume(add_callback_threadsafe,
#                      queue='VNFRecon_ReadFromML2_UAT_test_ch01',
                      queue='VNFRecon_WriteToML_PROD_PARALLEL',
#                      queue='VNFRecon_ReadFromML2_UAT_test_ch01',
                      no_ack=True)
                     # exchange='ReconDEVExchange_ML',
                     # queue='VNFRecon_WriteToML_DEV',
                      #no_ack=True)

#print('channel going to start')
channel.start_consuming()
#print('channel started')