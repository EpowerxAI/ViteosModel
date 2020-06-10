#!/usr/bin/env python
# coding: utf-8
"""
Created on Mon Jun  8 13:59:10 2020

@author: consultant138
"""
# This is the Rabbit MQ connection class which will be used to make an asynchronous connections to and from the server on which our model will run
import pika

class RabbitMQ_class:
    def __init__(self, param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', 
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', 
                 param_RABBITMQ_HOST_IP = '10.1.15.153', param_RABBITMQ_PORT = '5671', 
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos',
                 param_RABBITMQ_EXCHANGE = 'ReconUATExchange_ML_test_ch01',
                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML2_UAT_test_ch01',
                 param_RABBITMQ_ROUTING_KEY = 'VNFRecon_ReadFromML2_UAT_test_binding_ch01'):
        self.connection_string = param_RABBITMQ_QUEUEING_PROTOCOL + '://' + param_RABBITMQ_USERNAME + ':' + param_RABBITMQ_PASSWORD + '@' + param_RABBITMQ_HOST_IP + ':' + param_RABBITMQ_PORT + '/' + param_RABBITMQ_VIRTUAL_HOST
        self.exchange = param_RABBITMQ_EXCHANGE
        self.queue = param_RABBITMQ_QUEUE
        self.routing_key = param_RABBITMQ_ROUTING_KEY
        self.queue_bind(exchange = param_RABBITMQ_EXCHANGE, 
                        queue = param_RABBITMQ_QUEUE, 
                        routing_key = param_RABBITMQ_ROUTING_KEY)
    def fun_publish(self, param_message_body):
        connection = pika.BlockingConnection(pika.connection.URLParameters(self.connection_string))
        channel = connection.channel()
        channel.queue_bind(exchange = self.exchange, queue = self.queue, routing_key = self.routing_key)
        channel.basic_publish(exchange = self.exchange, routing_key = self.routing_key, 
                              body = param_message_body)
        connection.close()
    
    

        
