#!/usr/bin/env python
# coding: utf-8
"""
Created on Mon Jun  8 13:59:10 2020

@author: consultant138
"""
# This is the Rabbit MQ connection class which will be used to make an asynchronous connections to and from the server on which our model will run
import pika
#import time
from Read import Read_Class as rd_cl
from ViteosDecorator import logging_decorator

#def timeit(method):
#    def timed(*args, **kw):
#        ts = time.time()
#        result = method(*args, **kw)
#        te = time.time()
#        
#        print('%r (%r, %r) %2.2f sec' % \
#             (method.__name__, args, kw, te - ts))
#        return result
#
#    return timed

class RabbitMQ_Class:
    
    @logging_decorator
    def __init__(self, param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', 
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', 
                 param_RABBITMQ_HOST_IP = '10.1.15.153', param_RABBITMQ_PORT = '5671', 
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos',
                 param_RABBITMQ_EXCHANGE = 'ReconUATExchange_ML_test_ch01',
                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML2_UAT_test_ch01',
                 param_RABBITMQ_ROUTING_KEY = 'VNFRecon_ReadFromML2_UAT_test_binding_ch01',
                 param_test_message_publishing = False,
                 param_timeout = 10):
        self.rd_cl_obj = rd_cl()
        self.df_test_message_to_RabbitMQ = self.rd_cl_obj.fun_df_test_messages_to_RabbitMQ()
        self.test_message_publishing = param_test_message_publishing
        self.connection_string = param_RABBITMQ_QUEUEING_PROTOCOL + '://' + param_RABBITMQ_USERNAME + ':' + param_RABBITMQ_PASSWORD + '@' + param_RABBITMQ_HOST_IP + ':' + param_RABBITMQ_PORT + '/' + param_RABBITMQ_VIRTUAL_HOST
        self.exchange = param_RABBITMQ_EXCHANGE
        self.queue = param_RABBITMQ_QUEUE
        self.routing_key = param_RABBITMQ_ROUTING_KEY
        self.timeout = param_timeout
    
    @logging_decorator   
    def fun_publish_single_message(self, param_message_body):
        connection = pika.BlockingConnection(pika.connection.URLParameters(self.connection_string))
        channel = connection.channel()
        channel.queue_bind(exchange = self.exchange, queue = self.queue, routing_key = self.routing_key)
        channel.basic_publish(exchange = self.exchange, routing_key = self.routing_key, 
                              body = param_message_body)
        channel.close()
        connection.close()
    
    @logging_decorator 
    def fun_publish_muliple_messages(self):
        if(self.test_message_publishing == True):
            for i in range(self.df_test_message_to_RabbitMQ.shape[0]):
                message_i_list = list(self.df_test_message_to_RabbitMQ.iloc[i,:])
                message_i_str = ','.join([str(element) for element in message_i_list])
                self.fun_publish_single_message(param_message_body = message_i_str)
    
    @logging_decorator
    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)

    
    def on_timeout(self, param_connection):
        global connection
        #local connection
        #connection = param_connection
        connection.close()
    
    @logging_decorator    
    def fun_consume_messages(self):
        connection = pika.BlockingConnection(pika.connection.URLParameters(self.connection_string))
        channel = connection.channel()
        # The below code was not able to put messages inside a variable to be analyzes. So instead, I will try out channel.consume(queue) as shown in pika docs here : https://pika.readthedocs.io/en/stable/examples/blocking_consumer_generator.html#:~:text=The%20BlockingChannel.,to%20return%20any%20unprocessed%20messages.
#        connection.add_timeout(self.timeout, self.on_timeout(param_connection = connection))
#        channel.basic_consume(consumer_callback=self.callback, queue=self.queue, no_ack=True)
#        try:
#            channel.start_consuming()
#        except KeyboardInterrupt:
#            channel.stop_consuming()
        self.body_list = []
        for method_frame, properties, body in channel.consume(self.queue):
            # Display the message parts
            
#            method_frame_fun = method_frame
#            print(method_frame_fun)
#            properties_fun = properties
#            print(properties_fun)
            body_fun = body
#            print(body_fun)
            self.body_list.append(body_fun)

            # Acknowledge the message
            channel.basic_ack(method_frame.delivery_tag)

            # Escape out of the loop after 10 messages
            if method_frame.delivery_tag == self.df_test_message_to_RabbitMQ.shape[0]:
#            if method_frame.delivery_tag is not None:
                break

        # Cancel the consumer and return any pending messages
        requeued_messages = channel.cancel()
        print('Requeued %i messages' % requeued_messages)
        print('body_list')
        print(self.body_list)
        
        channel.close()
        connection.close()
        
        
    

        
