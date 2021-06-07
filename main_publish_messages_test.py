# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 09:07:22 2020

@author: consultant138
"""

from RabbitMQ import RabbitMQ_Class as rb_mq

#For Testing with reading messages from a dataframe instead of receiving messages from RabbitMQ directly
# Execute in a separate console - Console 1
rb_mq_obj = rb_mq(param_test_message_publishing = True)
rb_mq_obj.fun_publish_muliple_messages()


#After Ritesh/Shintanshu gave new RabbitMQ credentials
rb_mq_obj_new = rb_mq(param_RABBITMQ_QUEUEING_PROTOCOL = 'amqps', \
                 param_RABBITMQ_USERNAME = 'recon2', param_RABBITMQ_PASSWORD = 'recon2', \
                 param_RABBITMQ_HOST_IP = 'Vitblresbuat.viteos.com', param_RABBITMQ_PORT = '5671', \
                 param_RABBITMQ_VIRTUAL_HOST = 'viteos', \
                 param_RABBITMQ_EXCHANGE = 'ReconUATExchange_ML_test_ch01', \
                 param_RABBITMQ_QUEUE = 'VNFRecon_ReadFromML2_UAT_test_ch01', \
                 param_RABBITMQ_ROUTING_KEY = 'VNFRecon_ReadFromML2_UAT_test_binding_ch01', \
                 param_test_message_publishing = True, \
                 param_timeout = 10)

rb_mq_obj_new.fun_publish_muliple_messages()

